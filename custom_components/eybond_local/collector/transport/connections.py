"""Framed and AT collector socket connection implementations."""

from __future__ import annotations

import asyncio
import logging
from time import monotonic
from typing import Any, Callable

from ..at import CollectorAtResponse, build_at_query, build_at_write, parse_at_response
from ..cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_endpoint,
)
from ...collector_identity import reconcile_pn
from ...models import CollectorInfo
from ..protocol import (
    EybondHeader,
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    HEADER_SIZE,
    TIDCounter,
    build_collector_request,
    build_heartbeat_request,
    decode_header,
    parse_heartbeat_pn,
)
from .common import (
    _AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN,
    _AT_TEXT_MIXED_FRAME_FCODES,
    _AT_TEXT_MIXED_FRAME_READ_TIMEOUT,
    _PrefixedAsyncReader,
    _cancel_and_join_task,
    _close_writer_bounded,
    _copy_collector_info,
    _disconnect_reason_from_exception,
    _looks_like_plain_raw_response_start,
    _looks_like_uart_passthrough_value,
    _parse_fc2_collector_pn,
    _short_ascii,
)

logger = logging.getLogger(__name__)


def _modbus_rtu_response_length(prefix: bytes) -> int | None:
    """Return the complete RTU response length for a three-byte prefix.

    This is framing only; ``ModbusSession`` remains the authority that validates
    slave id, function, payload length and CRC.  The parser is enabled solely
    while a typed ``raw_serial/modbus_rtu`` request is pending, so arbitrary AT
    or mixed EyeBond traffic can never be reclassified as Modbus.
    """

    if len(prefix) < 3:
        return None
    function = prefix[1]
    if function & 0x80:
        return 5
    if function in (0x03, 0x04):
        byte_count = prefix[2]
        if byte_count > 250:
            return None
        return 3 + byte_count + 2
    if function in (0x06, 0x10):
        return 8
    return None


class _CollectorConnection:
    def __init__(
        self,
        *,
        remote_ip_hint: str = "",
        heartbeat_interval: float,
        write_timeout: float,
    ) -> None:
        self._heartbeat_interval = float(heartbeat_interval)
        self._write_timeout = float(write_timeout)
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = asyncio.Event()
        self._pending: dict[int, asyncio.Future[tuple[EybondHeader, bytes]]] = {}
        self._pending_at_response: asyncio.Future[CollectorAtResponse] | None = None
        self._request_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._tid = TIDCounter()
        self._collector = CollectorInfo(remote_ip=remote_ip_hint)
        self._last_heartbeat_monotonic: float | None = None
        self._last_liveness_monotonic: float | None = None
        self._session_id = ""
        self._session_identity_callback: Callable[[str, str, str], None] | None = None
        self._run_epoch = 0

    @property
    def connected(self) -> bool:
        writer = self._writer
        if writer is None or writer.is_closing():
            return False
        reader_task = self._reader_task
        return reader_task is None or not reader_task.done()

    @property
    def collector_info(self) -> CollectorInfo:
        self._collector.heartbeat_age_seconds = self._heartbeat_age_seconds()
        self._collector.heartbeat_fresh = self._has_fresh_heartbeat()
        return _copy_collector_info(self._collector)

    def set_heartbeat_interval(self, interval: float) -> None:
        self._heartbeat_interval = float(interval)

    def set_write_timeout(self, timeout: float) -> None:
        self._write_timeout = float(timeout)

    def _heartbeat_age_seconds(self) -> float | None:
        if self._last_heartbeat_monotonic is None:
            return None
        return max(0.0, monotonic() - self._last_heartbeat_monotonic)

    def _heartbeat_freshness_window(self) -> float:
        return max(self._heartbeat_interval * 2.0, 5.0)

    def _has_fresh_heartbeat(self) -> bool:
        age = self._heartbeat_age_seconds()
        return age is not None and age <= self._heartbeat_freshness_window()

    def _liveness_age_seconds(self) -> float | None:
        if self._last_liveness_monotonic is None:
            return None
        return max(0.0, monotonic() - self._last_liveness_monotonic)

    def _has_fresh_liveness(self) -> bool:
        age = self._liveness_age_seconds()
        return age is not None and age <= self._heartbeat_freshness_window()

    async def wait_until_connected(self, timeout: float) -> bool:
        if self.connected:
            return True
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return self.connected

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        if self._has_fresh_heartbeat():
            return True

        deadline = monotonic() + max(timeout, 0.0)
        while True:
            if self._has_fresh_heartbeat():
                return True
            if not self.connected:
                return False
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def wait_until_liveness(self, timeout: float) -> bool:
        """Wait for recent correlated traffic on this exact framed session."""

        if self._has_fresh_liveness():
            return True

        deadline = monotonic() + max(timeout, 0.0)
        while True:
            if self._has_fresh_liveness():
                return True
            if not self.connected:
                return False
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
        request_timeout: float,
    ) -> bytes:
        _, response_payload = await self.async_send_collector(
            fcode=FC_FORWARD_TO_DEVICE,
            payload=payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=request_timeout,
        )
        return response_payload

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
        request_timeout: float,
    ) -> tuple[EybondHeader, bytes]:
        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            writer = self._writer
            if writer is None or writer.is_closing():
                raise ConnectionError("collector_not_connected")

            tid = self._tid.next()
            frame = build_collector_request(
                tid,
                payload,
                devcode=devcode,
                collector_addr=collector_addr,
                fcode=fcode,
            )

            loop = asyncio.get_running_loop()
            future: asyncio.Future[tuple[EybondHeader, bytes]] = loop.create_future()
            self._pending[tid] = future

            try:
                await self._async_write(frame)
                logger.debug(
                    "TX collector remote=%s tid=%d fc=%d devcode=0x%04X devaddr=0x%02X payload=%s",
                    self._collector.remote_ip,
                    tid,
                    fcode,
                    devcode,
                    collector_addr,
                    payload.hex(),
                )
                return await asyncio.wait_for(future, timeout=request_timeout)
            finally:
                self._pending.pop(tid, None)

    async def async_query(self, command: str, *, request_timeout: float) -> CollectorAtResponse:
        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[CollectorAtResponse] = loop.create_future()
            self._pending_at_response = future
            try:
                await self._async_write(build_at_query(command))
                response = await asyncio.wait_for(future, timeout=request_timeout)
            finally:
                if self._pending_at_response is future:
                    self._pending_at_response = None
            self._apply_at_response_metadata(response)
            return response

    async def async_write(
        self,
        command: str,
        value: str,
        *,
        request_timeout: float,
    ) -> CollectorAtResponse:
        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[CollectorAtResponse] = loop.create_future()
            self._pending_at_response = future
            try:
                await self._async_write(build_at_write(command, value))
                response = await asyncio.wait_for(future, timeout=request_timeout)
            finally:
                if self._pending_at_response is future:
                    self._pending_at_response = None
            self._apply_at_response_metadata(response)
            return response

    async def run(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initial_bytes: bytes = b"",
        session_id: str = "",
        session_identity_callback: Callable[[str, str, str], None] | None = None,
        session_closed_callback: Callable[[str, object], None] | None = None,
        disconnect_callback: Callable[[object], None] | None = None,
    ) -> None:
        # The epoch marks THIS session as the connection's current owner.  A
        # replacing run() bumps it before tearing the old session down, so the
        # replaced session's ``finally`` below sees a stale epoch and must not
        # touch shared state: its writer/tasks/pending futures were already
        # torn down by the replacement, and running its disconnect_callback
        # would drop the listener indexes the new session just registered
        # (observed in the field as a live collector "vanishing" until redial).
        self._run_epoch += 1
        epoch = self._run_epoch
        if self.connected:
            self._collector.connection_replace_count += 1
            logger.warning("Replacing active collector connection for %s", self._collector.remote_ip)
            await self._disconnect(reason="replaced_active_connection")

        peer = writer.get_extra_info("peername") or ("", None)
        self._collector.remote_ip = peer[0] or self._collector.remote_ip
        self._collector.remote_port = peer[1]
        self._collector.connection_count += 1
        self._collector.last_disconnect_reason = ""
        self._last_heartbeat_monotonic = None
        self._last_liveness_monotonic = None
        self._reader = reader
        self._writer = writer
        self._session_id = str(session_id or "").strip()
        self._session_identity_callback = session_identity_callback
        self._connected.set()

        logger.info("Collector connected from %s:%s", self._collector.remote_ip, self._collector.remote_port)

        current_task = asyncio.current_task()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name=f"eybond_heartbeat_{self._collector.remote_ip}")
        prefixed_reader = _PrefixedAsyncReader(reader, initial_bytes)
        self._reader_task = asyncio.create_task(self._read_loop(prefixed_reader), name=f"eybond_reader_{self._collector.remote_ip}")
        try:
            await self._reader_task
        finally:
            # EOF/reset is the physical session-lifetime boundary. Publish it
            # before bounded writer/task cleanup: wait_closed() may legitimately
            # consume another five seconds on a rebooting collector, but recovery
            # must not misreport that already-observed disconnect as a timeout.
            # Session ids are socket-scoped, and the listener callback removes
            # only this exact id, so an overlapping successor remains untouched.
            if session_id and session_closed_callback is not None:
                session_closed_callback(session_id, self)
            if self._run_epoch == epoch:
                await self._disconnect(skip_task=current_task)
            # Re-check: a replacement may have started while the disconnect
            # above was awaiting; the callback must not fire for it then.
            if self._run_epoch == epoch and disconnect_callback is not None:
                disconnect_callback(self)

    async def _heartbeat_loop(self) -> None:
        try:
            while self.connected:
                tid = self._tid.next()
                interval = int(self._heartbeat_interval)
                frame = build_heartbeat_request(tid, interval)
                await self._async_write(frame)
                logger.debug("TX FC=1 remote=%s tid=%d interval=%d", self._collector.remote_ip, tid, interval)
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Heartbeat loop stopped for %s: %s", self._collector.remote_ip, exc)

    async def _async_write(self, frame: bytes) -> None:
        async with self._write_lock:
            writer = self._writer
            if writer is None or writer.is_closing():
                raise ConnectionError("collector_not_connected")
            writer.write(frame)
            try:
                await asyncio.wait_for(writer.drain(), timeout=self._write_timeout)
            except asyncio.TimeoutError as exc:
                raise ConnectionError("collector_write_timeout") from exc

    def _apply_at_response_metadata(self, response: CollectorAtResponse) -> None:
        if response.command == "DTUPN" and response.value:
            self._collector.collector_pn = reconcile_pn(
                self._collector.collector_pn,
                response.value,
            )
            self._record_session_identity(response.value, "at_dtupn")
        elif response.command == "FWVER" and response.value:
            self._collector.smartess_collector_version = response.value
        elif response.command == "CLDSRVHOST1" and response.value:
            self._collector.collector_server_endpoint = response.value
            apply_collector_cloud_family_observation(
                self._collector,
                collector_cloud_family_observation_from_endpoint(response.value),
            )

    def _record_session_identity(self, collector_pn: str, source: str) -> None:
        callback = self._session_identity_callback
        session_id = self._session_id
        if callback is None or not session_id or not collector_pn:
            return
        callback(session_id, collector_pn, source)

    def _handle_at_response(self, payload: bytes) -> None:
        try:
            response = parse_at_response(payload)
        except Exception:
            logger.debug(
                "Unhandled collector mixed payload remote=%s payload=%r",
                self._collector.remote_ip,
                payload,
            )
            return

        future = self._pending_at_response
        if future is not None and not future.done():
            self._last_liveness_monotonic = monotonic()
            future.set_result(response)
            return

        self._apply_at_response_metadata(response)
        logger.debug(
            "Unsolicited collector AT response remote=%s command=%s value=%s",
            self._collector.remote_ip,
            response.command,
            response.value,
        )

    async def _read_loop(
        self,
        reader: asyncio.StreamReader | _PrefixedAsyncReader,
    ) -> None:
        if not isinstance(reader, _PrefixedAsyncReader):
            reader = _PrefixedAsyncReader(reader)
        try:
            while True:
                prefix = await reader.readexactly(3)
                if prefix == b"AT+":
                    line = prefix + await reader.read_at_response()
                    self._handle_at_response(line)
                    continue

                header_bytes = prefix + await reader.readexactly(HEADER_SIZE - len(prefix))
                header = decode_header(header_bytes)
                payload = b""
                if header.payload_len > 0:
                    payload = await reader.readexactly(header.payload_len)

                self._collector.last_devcode = header.devcode
                logger.debug(
                    "RX header remote=%s tid=%d devcode=0x%04X devaddr=0x%02X fc=%d payload=%d",
                    self._collector.remote_ip,
                    header.tid,
                    header.devcode,
                    header.devaddr,
                    header.fcode,
                    header.payload_len,
                )

                observed_at = monotonic()
                if header.fcode == FC_HEARTBEAT:
                    pn = parse_heartbeat_pn(payload)
                    if pn:
                        self._collector.collector_pn = reconcile_pn(
                            self._collector.collector_pn,
                            pn,
                        )
                        self._record_session_identity(pn, "framed_heartbeat")
                    self._collector.heartbeat_devcode = header.devcode
                    self._collector.heartbeat_payload_hex = payload.hex()
                    self._last_heartbeat_monotonic = observed_at
                elif header.fcode == FC_QUERY_COLLECTOR:
                    pn = _parse_fc2_collector_pn(payload)
                    if pn:
                        self._collector.collector_pn = reconcile_pn(
                            self._collector.collector_pn,
                            pn,
                        )
                        self._record_session_identity(pn, "fc2_parameter_2")
                future = self._pending.get(header.tid)
                if header.fcode == FC_HEARTBEAT or (
                    future is not None and not future.done()
                ):
                    self._last_liveness_monotonic = observed_at
                if future and not future.done():
                    future.set_result((header, payload))
                    continue

                if header.fcode == FC_HEARTBEAT:
                    continue

                logger.debug(
                    "Unhandled collector frame remote=%s fc=%d payload=%s",
                    self._collector.remote_ip,
                    header.fcode,
                    payload.hex(),
                )
        except asyncio.IncompleteReadError:
            self._collector.last_disconnect_reason = "collector_eof"
            logger.info("Collector disconnected: %s", self._collector.remote_ip)
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            self._collector.last_disconnect_reason = _disconnect_reason_from_exception(exc)
            logger.info("Collector disconnected %s: %s", self._collector.remote_ip, exc)
        except asyncio.CancelledError:
            raise

    async def disconnect(self) -> None:
        await self._disconnect(reason="manual_disconnect")

    async def _disconnect(
        self,
        skip_task: asyncio.Task[Any] | None = None,
        *,
        reason: str = "",
    ) -> None:
        pending_drop_count = sum(1 for future in self._pending.values() if not future.done())
        had_session = (
            self._reader is not None
            or self._writer is not None
            or self._connected.is_set()
            or pending_drop_count > 0
        )
        if pending_drop_count:
            self._collector.pending_request_drop_count += pending_drop_count
        if had_session:
            self._collector.disconnect_count += 1
            self._collector.last_disconnect_reason = (
                reason
                or self._collector.last_disconnect_reason
                or "collector_disconnected"
            )

        # Detach the session from shared state and close the writer BEFORE
        # cancelling the reader: cancelling the reader wakes the session's
        # run() coroutine, and anything observing the connection at that
        # moment (the replaced run's finally, a concurrent waiter) must
        # already see the old session fully torn down — not a half-open
        # writer that only closes a few event-loop steps later.
        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        reader_task = self._reader_task
        self._reader_task = None
        writer = self._writer
        self._reader = None
        self._writer = None
        self._connected.clear()
        self._last_heartbeat_monotonic = None
        self._last_liveness_monotonic = None
        self._session_id = ""
        self._session_identity_callback = None

        if heartbeat_task and heartbeat_task is not skip_task:
            await _cancel_and_join_task(heartbeat_task)

        if writer:
            await _close_writer_bounded(writer)

        if reader_task and reader_task is not skip_task:
            await _cancel_and_join_task(reader_task)

        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("collector_disconnected"))
        self._pending.clear()

        at_future = self._pending_at_response
        self._pending_at_response = None
        if at_future is not None and not at_future.done():
            at_future.set_exception(ConnectionError("collector_disconnected"))


class _CollectorAtConnection:
    def __init__(
        self,
        *,
        remote_ip_hint: str = "",
        write_timeout: float,
        raw_passthrough_bootstrap: str = "",
        raw_passthrough_frame_format: str = "",
        raw_passthrough_min_interval_ms: int = 0,
    ) -> None:
        self._write_timeout = float(write_timeout)
        self._reader_task: asyncio.Task[None] | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = asyncio.Event()
        self._request_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._pending_response: asyncio.Future[CollectorAtResponse] | None = None
        self._pending_raw_response: asyncio.Future[bytes] | None = None
        self._pending_raw_protocol = ""
        self._pending_framed_response: dict[int, asyncio.Future[tuple[EybondHeader, bytes]]] = {}
        self._tid = TIDCounter()
        self._collector = CollectorInfo(remote_ip=remote_ip_hint)
        self._session_id = ""
        self._session_identity_callback: Callable[[str, str, str], None] | None = None
        self._raw_passthrough_bootstrap = str(raw_passthrough_bootstrap or "").strip().lower()
        self._raw_passthrough_frame_format = (
            str(raw_passthrough_frame_format or "").strip().lower()
        )
        self._raw_passthrough_min_interval = max(
            0.0,
            float(raw_passthrough_min_interval_ms or 0) / 1000.0,
        )
        self._raw_passthrough_last_write_monotonic = 0.0
        self._raw_passthrough_bootstrapped = False
        self._run_epoch = 0

    @property
    def connected(self) -> bool:
        writer = self._writer
        if writer is None or writer.is_closing():
            return False
        reader_task = self._reader_task
        return reader_task is None or not reader_task.done()

    @property
    def collector_info(self) -> CollectorInfo:
        return _copy_collector_info(self._collector)

    def set_write_timeout(self, timeout: float) -> None:
        self._write_timeout = float(timeout)

    def set_raw_passthrough_bootstrap(self, mode: str) -> None:
        normalized = str(mode or "").strip().lower()
        if normalized == self._raw_passthrough_bootstrap:
            return
        self._raw_passthrough_bootstrap = normalized
        self._raw_passthrough_bootstrapped = False

    def set_raw_passthrough_frame_format(self, mode: str) -> None:
        self._raw_passthrough_frame_format = str(mode or "").strip().lower()

    def set_raw_passthrough_min_interval_ms(self, value: int) -> None:
        self._raw_passthrough_min_interval = max(0.0, float(value or 0) / 1000.0)

    async def wait_until_connected(self, timeout: float) -> bool:
        if self.connected:
            return True
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return self.connected

    async def async_query(self, command: str, *, request_timeout: float) -> CollectorAtResponse:
        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            return await self._async_query_locked(
                build_at_query(command),
                request_timeout=request_timeout,
            )

    async def async_send_raw_payload(
        self,
        payload: bytes,
        *,
        request_timeout: float,
        payload_protocol: str = "",
    ) -> bytes:
        """Send one raw inverter payload over a plain AT callback stream.

        Some DTU/AT collectors do not wrap inverter traffic in the legacy EyeBond
        FC=4 tunnel. The cloud writes typed raw payloads (for example PI ASCII or
        Modbus RTU) directly to the same TCP stream after the AT bootstrap.
        """

        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            total_started = asyncio.get_running_loop().time()
            await self._async_bootstrap_raw_passthrough_locked(
                request_timeout=min(float(request_timeout), 2.0),
            )
            loop = asyncio.get_running_loop()
            future: asyncio.Future[bytes] = loop.create_future()
            self._pending_raw_response = future
            self._pending_raw_protocol = str(payload_protocol or "").strip().lower()
            self._collector.raw_request_count += 1
            self._collector.raw_last_request_hex = payload.hex()
            self._collector.raw_last_request_ascii = _short_ascii(payload)
            self._collector.raw_last_frame_format = self._raw_passthrough_frame_format
            spacing_wait_ms = 0
            try:
                logger.debug(
                    "EyeBond raw passthrough write remote=%s frame=%s payload=%r",
                    self._collector.remote_ip,
                    self._raw_passthrough_frame_format or "default",
                    payload,
                )
                spacing_wait_ms = await self._async_wait_raw_passthrough_spacing_locked()
                response_started = asyncio.get_running_loop().time()
                await self._async_write(payload)
                self._raw_passthrough_last_write_monotonic = (
                    asyncio.get_running_loop().time()
                )
                response = await asyncio.wait_for(future, timeout=request_timeout)
                finished = asyncio.get_running_loop().time()
                self._collector.raw_response_count += 1
                self._collector.raw_last_response_hex = response.hex()
                self._collector.raw_last_response_ascii = _short_ascii(response)
                self._collector.raw_last_spacing_wait_ms = spacing_wait_ms
                self._collector.raw_last_response_duration_ms = int(
                    round((finished - response_started) * 1000.0)
                )
                self._collector.raw_last_total_duration_ms = int(
                    round((finished - total_started) * 1000.0)
                )
                logger.debug(
                    "EyeBond raw passthrough response remote=%s parser=%s payload=%r",
                    self._collector.remote_ip,
                    self._collector.raw_last_parser or "unknown",
                    response,
                )
                return response
            except asyncio.TimeoutError:
                finished = asyncio.get_running_loop().time()
                self._collector.raw_timeout_count += 1
                self._collector.raw_last_timeout_request_ascii = _short_ascii(payload)
                self._collector.raw_last_spacing_wait_ms = spacing_wait_ms
                self._collector.raw_last_total_duration_ms = int(
                    round((finished - total_started) * 1000.0)
                )
                logger.debug(
                    "EyeBond raw passthrough timeout remote=%s frame=%s payload=%r last_parser=%s last_response=%r",
                    self._collector.remote_ip,
                    self._raw_passthrough_frame_format or "default",
                    payload,
                    self._collector.raw_last_parser or "",
                    self._collector.raw_last_response_ascii,
                )
                raise
            finally:
                if self._pending_raw_response is future:
                    self._pending_raw_response = None
                    self._pending_raw_protocol = ""

    async def async_send_bridge_identity_probe(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
        request_timeout: float,
    ) -> tuple[EybondHeader, bytes]:
        """Send one narrow framed collector identity request on a mixed AT stream.

        ESP EyeBond Collector bridges can expose the SmartESS AT callback shape
        before their bridge identity has been confirmed. Their positive bridge
        token, however, is intentionally stored in FC=2 parameter 6. Supporting
        this narrow framed request path lets runtime bootstrap that token without
        reintroducing legacy PN or AT fallbacks.
        """

        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            writer = self._writer
            if writer is None or writer.is_closing():
                raise ConnectionError("collector_not_connected")

            tid = self._tid.next()
            frame = build_collector_request(
                tid,
                payload,
                devcode=devcode,
                collector_addr=collector_addr,
                fcode=fcode,
            )
            loop = asyncio.get_running_loop()
            future: asyncio.Future[tuple[EybondHeader, bytes]] = loop.create_future()
            self._pending_framed_response[tid] = future
            try:
                await self._async_write(frame)
                return await asyncio.wait_for(future, timeout=request_timeout)
            finally:
                self._pending_framed_response.pop(tid, None)

    async def _async_wait_raw_passthrough_spacing_locked(self) -> int:
        interval = self._raw_passthrough_min_interval
        if interval <= 0:
            return 0
        elapsed = asyncio.get_running_loop().time() - self._raw_passthrough_last_write_monotonic
        remaining = interval - elapsed
        if remaining > 0:
            started = asyncio.get_running_loop().time()
            await asyncio.sleep(remaining)
            return int(round((asyncio.get_running_loop().time() - started) * 1000.0))
        return 0

    async def _async_query_locked(
        self,
        payload: bytes,
        *,
        request_timeout: float,
    ) -> CollectorAtResponse:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[CollectorAtResponse] = loop.create_future()
        self._pending_response = future
        try:
            await self._async_write(payload)
            response = await asyncio.wait_for(future, timeout=request_timeout)
        finally:
            if self._pending_response is future:
                self._pending_response = None
        self._apply_response_metadata(response)
        return response

    async def async_write(
        self,
        command: str,
        value: str,
        *,
        request_timeout: float,
    ) -> CollectorAtResponse:
        if not self.connected or not self._writer:
            raise ConnectionError("collector_not_connected")

        async with self._request_lock:
            return await self._async_query_locked(
                build_at_write(command, value),
                request_timeout=request_timeout,
            )

    async def _async_bootstrap_raw_passthrough_locked(self, *, request_timeout: float) -> None:
        """Mirror SmartESS AT bootstrap before direct inverter ASCII traffic.

        Legacy dtu_ess collectors send PI30 traffic as raw serial bytes on the
        AT callback stream, but the cloud first confirms the current UART mode
        with an ``AT+UART=<same value>`` write. Some older collectors appear not
        to forward raw inverter bytes reliably until this step is performed.
        """

        if self._raw_passthrough_bootstrapped:
            return
        if self._raw_passthrough_bootstrap == "none":
            self._raw_passthrough_bootstrapped = True
            return

        try:
            response = await self._async_query_locked(
                build_at_query("UART"),
                request_timeout=request_timeout,
            )
            uart_value = str(response.value or "").strip()
            if not _looks_like_uart_passthrough_value(uart_value):
                logger.debug(
                    "Skipping raw passthrough UART bootstrap remote=%s value=%r",
                    self._collector.remote_ip,
                    uart_value,
                )
                self._raw_passthrough_bootstrapped = True
                return
            await self._async_query_locked(
                build_at_write("UART", uart_value),
                request_timeout=request_timeout,
            )
        except Exception as exc:
            logger.debug(
                "Raw passthrough UART bootstrap failed remote=%s error=%s",
                self._collector.remote_ip,
                exc,
            )
        finally:
            self._raw_passthrough_bootstrapped = True

    async def run(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initial_bytes: bytes = b"",
        session_id: str = "",
        session_identity_callback: Callable[[str, str, str], None] | None = None,
        session_closed_callback: Callable[[str, object], None] | None = None,
        disconnect_callback: Callable[[object], None] | None = None,
    ) -> None:
        # Same epoch discipline as _CollectorConnection.run: a replaced
        # session's ``finally`` must not tear down or unindex its successor.
        self._run_epoch += 1
        epoch = self._run_epoch
        if self.connected:
            self._collector.connection_replace_count += 1
            logger.warning("Replacing active AT collector connection for %s", self._collector.remote_ip)
            await self._disconnect(reason="replaced_active_connection")

        peer = writer.get_extra_info("peername") or ("", None)
        self._collector.remote_ip = peer[0] or self._collector.remote_ip
        self._collector.remote_port = peer[1]
        self._collector.connection_count += 1
        self._collector.last_disconnect_reason = ""
        self._reader = reader
        self._writer = writer
        self._session_id = str(session_id or "").strip()
        self._session_identity_callback = session_identity_callback
        self._raw_passthrough_bootstrapped = False
        self._connected.set()

        logger.info(
            "Collector AT connection from %s:%s session=%s",
            self._collector.remote_ip,
            self._collector.remote_port,
            self._session_id or "unknown",
        )

        current_task = asyncio.current_task()
        prefixed_reader = _PrefixedAsyncReader(reader, initial_bytes)
        self._reader_task = asyncio.create_task(
            self._read_loop(prefixed_reader),
            name=f"collector_at_reader_{self._collector.remote_ip}",
        )
        try:
            await self._reader_task
        finally:
            # Same physical-session boundary as the framed connection: publish
            # EOF/reset before bounded writer cleanup, never after it.
            if session_id and session_closed_callback is not None:
                session_closed_callback(session_id, self)
            if self._run_epoch == epoch:
                await self._disconnect(skip_task=current_task)
            if self._run_epoch == epoch and disconnect_callback is not None:
                disconnect_callback(self)

    async def disconnect(self) -> None:
        await self._disconnect(reason="manual_disconnect")

    async def _async_write(self, payload: bytes) -> None:
        async with self._write_lock:
            writer = self._writer
            if writer is None or writer.is_closing():
                raise ConnectionError("collector_not_connected")
            writer.write(payload)
            try:
                await asyncio.wait_for(writer.drain(), timeout=self._write_timeout)
            except asyncio.TimeoutError as exc:
                raise ConnectionError("collector_write_timeout") from exc

    async def _read_loop(
        self,
        reader: asyncio.StreamReader | _PrefixedAsyncReader,
    ) -> None:
        if not isinstance(reader, _PrefixedAsyncReader):
            reader = _PrefixedAsyncReader(reader)
        try:
            buffered_prefix = b""
            while True:
                if buffered_prefix:
                    first = buffered_prefix[:1]
                    buffered_prefix = buffered_prefix[1:]
                else:
                    first = await reader.readexactly(1)

                if first == b"A":
                    prefix = first + await reader.readexactly(2)
                    if prefix == b"AT+":
                        line = prefix + await reader.read_at_response()
                        self._handle_at_response_line(line)
                        continue
                    buffered_prefix = prefix + buffered_prefix

                raw_future = self._pending_raw_response
                raw_protocol = self._pending_raw_protocol
                if (
                    raw_future is not None
                    and not raw_future.done()
                    and raw_protocol == "modbus_rtu"
                ):
                    prefix = buffered_prefix or first
                    buffered_prefix = b""
                    if len(prefix) < 3:
                        prefix += await reader.readexactly(3 - len(prefix))
                    frame_length = _modbus_rtu_response_length(prefix[:3])
                    if frame_length is None or len(prefix) > frame_length:
                        self._record_unhandled_raw_fragment(
                            prefix,
                            parser="raw_modbus_rtu_prefix_invalid",
                        )
                        continue
                    frame = prefix
                    if len(frame) < frame_length:
                        frame += await reader.readexactly(frame_length - len(frame))
                    self._collector.raw_last_parser = "raw_modbus_rtu"
                    raw_future.set_result(frame)
                    continue

                if (
                    raw_future is not None
                    and not raw_future.done()
                    and (
                        _looks_like_plain_raw_response_start(first)
                        or (
                            buffered_prefix
                            and _looks_like_plain_raw_response_start(buffered_prefix[:1])
                        )
                    )
                ):
                    if buffered_prefix:
                        line = buffered_prefix + await reader.readuntil(b"\r")
                        buffered_prefix = b""
                    else:
                        line = first + await reader.readuntil(b"\r")
                    self._handle_raw_ascii_line(line, parser="raw_pending_or_plain_line")
                    continue

                if not buffered_prefix:
                    buffered_prefix = first + buffered_prefix

                if len(buffered_prefix) >= 3:
                    prefix = buffered_prefix[:3]
                    buffered_prefix = buffered_prefix[3:]
                else:
                    prefix = buffered_prefix + await reader.readexactly(3 - len(buffered_prefix))
                    buffered_prefix = b""
                if prefix.startswith((b"(", b"^")):
                    terminator = prefix.find(b"\r")
                    if terminator >= 0:
                        line = prefix[: terminator + 1]
                        buffered_prefix = prefix[terminator + 1 :] + buffered_prefix
                    else:
                        line = prefix + await reader.readuntil(b"\r")
                    self._handle_raw_ascii_line(line, parser="raw_prefix_ascii")
                    continue

                if prefix in {b"NAK", b"NOA", b"ERC"}:
                    line = prefix + await reader.readuntil(b"\r")
                    self._handle_raw_ascii_line(line, parser="raw_negative")
                    continue

                if (
                    self._raw_passthrough_frame_format == "plain_line"
                    and prefix.startswith(b"BL")
                ):
                    line = prefix + await reader.readuntil(b"\r")
                    self._handle_raw_ascii_line(line, parser="raw_plain_line_bare_token")
                    continue

                if prefix != b"AT+":
                    header_tail = await self._read_mixed_frame_tail(
                        reader,
                        HEADER_SIZE - len(prefix),
                    )
                    if header_tail is None:
                        self._record_unhandled_raw_fragment(
                            prefix,
                            parser="mixed_frame_header_timeout",
                        )
                        continue
                    header_bytes = prefix + header_tail
                    header = decode_header(header_bytes)
                    if not self._looks_like_mixed_frame_header(header):
                        if (
                            self._raw_passthrough_frame_format == "plain_line"
                            and _looks_like_plain_raw_response_start(header_bytes[:1])
                        ):
                            try:
                                line = header_bytes + await asyncio.wait_for(
                                    reader.readuntil(b"\r"),
                                    timeout=_AT_TEXT_MIXED_FRAME_READ_TIMEOUT,
                                )
                            except asyncio.TimeoutError:
                                self._record_unhandled_raw_fragment(
                                    header_bytes,
                                    parser="raw_plain_line_stale_timeout",
                                )
                                continue
                            self._handle_raw_ascii_line(line, parser="raw_plain_line_stale")
                            continue
                        self._record_unhandled_raw_fragment(
                            header_bytes,
                            parser="mixed_frame_header_invalid",
                        )
                        continue
                    payload = b""
                    if header.payload_len > 0:
                        payload = await self._read_mixed_frame_tail(
                            reader,
                            header.payload_len,
                        )
                        if payload is None:
                            self._record_unhandled_raw_fragment(
                                header_bytes,
                                parser="mixed_frame_payload_timeout",
                            )
                            continue
                    if header.fcode == FC_HEARTBEAT:
                        pn = parse_heartbeat_pn(payload)
                        if pn:
                            self._collector.collector_pn = reconcile_pn(
                                self._collector.collector_pn,
                                pn,
                            )
                            self._record_session_identity(pn, "framed_heartbeat")
                        self._collector.heartbeat_devcode = header.devcode
                        self._collector.heartbeat_payload_hex = payload.hex()
                    elif header.fcode == FC_QUERY_COLLECTOR:
                        future = self._pending_framed_response.get(header.tid)
                        if future is not None and not future.done():
                            future.set_result((header, payload))
                            continue
                        pn = _parse_fc2_collector_pn(payload)
                        if pn:
                            self._collector.collector_pn = reconcile_pn(
                                self._collector.collector_pn,
                                pn,
                            )
                            self._record_session_identity(pn, "fc2_parameter_2")
                    else:
                        logger.debug(
                            "Unhandled collector mixed frame on AT connection remote=%s fc=%d payload=%s",
                            self._collector.remote_ip,
                            header.fcode,
                            payload.hex(),
                        )
                    continue

                line = prefix + await reader.readuntil(b"\n")
                try:
                    response = parse_at_response(line)
                except Exception:
                    logger.debug(
                        "Unhandled collector AT payload remote=%s payload=%r",
                        self._collector.remote_ip,
                        line,
                    )
                    continue
                future = self._pending_response
                if future is not None and not future.done():
                    future.set_result(response)
                    continue

                self._apply_response_metadata(response)
                logger.debug(
                    "Unsolicited collector AT response remote=%s command=%s value=%s",
                    self._collector.remote_ip,
                    response.command,
                    response.value,
                )
        except asyncio.IncompleteReadError:
            self._collector.last_disconnect_reason = "collector_eof"
            logger.info("Collector AT disconnected: %s", self._collector.remote_ip)
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            self._collector.last_disconnect_reason = _disconnect_reason_from_exception(exc)
            logger.info("Collector AT disconnected %s: %s", self._collector.remote_ip, exc)
        except asyncio.CancelledError:
            raise

    async def _read_mixed_frame_tail(
        self,
        reader: asyncio.StreamReader,
        size: int,
    ) -> bytes | None:
        if size <= 0:
            return b""
        if self._raw_passthrough_frame_format != "plain_line":
            return await reader.readexactly(size)
        try:
            return await asyncio.wait_for(
                reader.readexactly(size),
                timeout=_AT_TEXT_MIXED_FRAME_READ_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return None

    def _looks_like_mixed_frame_header(self, header: EybondHeader) -> bool:
        if header.payload_len < 0:
            return False
        if header.payload_len > _AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN:
            return False
        if header.fcode not in _AT_TEXT_MIXED_FRAME_FCODES:
            return False
        return True

    def _record_unhandled_raw_fragment(self, payload: bytes, *, parser: str) -> None:
        self._collector.raw_unhandled_line_count += 1
        self._collector.raw_last_parser = parser
        self._collector.raw_last_response_hex = payload.hex()
        self._collector.raw_last_response_ascii = _short_ascii(payload)
        logger.debug(
            "Unhandled collector mixed/raw fragment remote=%s parser=%s payload=%r",
            self._collector.remote_ip,
            parser,
            payload,
        )

    def _handle_raw_ascii_line(self, line: bytes, *, parser: str) -> None:
        future = self._pending_raw_response
        if future is not None and not future.done():
            self._collector.raw_last_parser = parser
            future.set_result(line)
            return

        self._collector.raw_unhandled_line_count += 1
        self._collector.raw_last_parser = f"{parser}_unhandled"
        self._collector.raw_last_response_hex = line.hex()
        self._collector.raw_last_response_ascii = _short_ascii(line)
        logger.debug(
            "Unhandled collector raw ASCII payload remote=%s parser=%s payload=%r",
            self._collector.remote_ip,
            parser,
            line,
        )

    def _handle_at_response_line(self, line: bytes) -> None:
        try:
            response = parse_at_response(line)
        except Exception:
            logger.debug(
                "Unhandled collector AT payload remote=%s payload=%r",
                self._collector.remote_ip,
                line,
            )
            return
        future = self._pending_response
        if future is not None and not future.done():
            future.set_result(response)
            return

        self._apply_response_metadata(response)
        logger.debug(
            "Unsolicited collector AT response remote=%s command=%s value=%s",
            self._collector.remote_ip,
            response.command,
            response.value,
        )

    def _apply_response_metadata(self, response: CollectorAtResponse) -> None:
        if response.command == "DTUPN" and response.value:
            self._collector.collector_pn = reconcile_pn(
                self._collector.collector_pn,
                response.value,
            )
            self._record_session_identity(response.value, "at_dtupn")
        elif response.command == "FWVER" and response.value:
            self._collector.smartess_collector_version = response.value
        elif response.command == "CLDSRVHOST1" and response.value:
            self._collector.collector_server_endpoint = response.value
            apply_collector_cloud_family_observation(
                self._collector,
                collector_cloud_family_observation_from_endpoint(response.value),
            )

    def _record_session_identity(self, collector_pn: str, source: str) -> None:
        callback = self._session_identity_callback
        session_id = self._session_id
        if callback is None or not session_id or not collector_pn:
            return
        callback(session_id, collector_pn, source)

    async def _disconnect(
        self,
        skip_task: asyncio.Task[Any] | None = None,
        *,
        reason: str = "",
    ) -> None:
        had_session = self._reader is not None or self._writer is not None or self._connected.is_set()
        if had_session:
            self._collector.disconnect_count += 1
            self._collector.last_disconnect_reason = (
                reason
                or self._collector.last_disconnect_reason
                or "collector_disconnected"
            )

        # Same ordering rule as _CollectorConnection._disconnect: detach and
        # close the writer before the reader cancellation wakes the session.
        reader_task = self._reader_task
        self._reader_task = None
        writer = self._writer
        self._reader = None
        self._writer = None
        self._connected.clear()
        self._session_id = ""
        self._session_identity_callback = None

        if writer:
            await _close_writer_bounded(writer)

        if reader_task and reader_task is not skip_task:
            await _cancel_and_join_task(reader_task)

        future = self._pending_response
        self._pending_response = None
        if future is not None and not future.done():
            future.set_exception(ConnectionError("collector_disconnected"))

        raw_future = self._pending_raw_response
        self._pending_raw_response = None
        self._pending_raw_protocol = ""
        if raw_future is not None and not raw_future.done():
            raw_future.set_exception(ConnectionError("collector_disconnected"))
