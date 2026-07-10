"""Shared collector transport primitives for single- and multi-collector listeners."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable, Protocol

from .at import CollectorAtResponse, build_at_query, build_at_write, parse_at_response
from .cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_endpoint,
)
from ..link_models import EybondLinkRoute, LinkRoute, RawSerialLinkRoute
from ..link_transport import PayloadLinkTransport
from ..models import CollectorInfo
from .profile import apply_collector_profile
from .protocol import (
    EybondHeader,
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    FC_SET_COLLECTOR,
    FC_SET_DEVICE_REG,
    FC_TRIGGER_QUERY_HISTORY,
    FC_TRIGGER_QUERY_REAL_TIME,
    HEADER_SIZE,
    TIDCounter,
    build_collector_request,
    build_heartbeat_request,
    decode_header,
    parse_heartbeat_pn,
)

logger = logging.getLogger(__name__)

_COLLECTOR_PN_PREFIX_MATCH_MIN_LEN = 14


class CollectorListenerBindError(RuntimeError):
    """Raised when the shared collector listener cannot bind its socket."""

    def __init__(self, host: str, port: int, error: OSError) -> None:
        self.host = str(host)
        self.port = int(port)
        self.error = error
        self.errno = getattr(error, "errno", None)
        super().__init__(
            f"collector_listener_bind_failed:{self.host}:{self.port}:{error}"
        )


# Strong references to session/sniff tasks: asyncio keeps only weak ones, so
# an unreferenced task can be garbage-collected mid-flight, and a crash in an
# unobserved task surfaces only as a contextless "exception was never
# retrieved" at GC time.
_BACKGROUND_TASKS: set["asyncio.Task[Any]"] = set()


def _reap_tracked_task(task: "asyncio.Task[Any]") -> None:
    _BACKGROUND_TASKS.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "Collector background task %s crashed: %s",
            task.get_name(),
            exc,
            exc_info=exc,
        )


def _spawn_tracked_task(coro: Any, *, name: str) -> "asyncio.Task[Any]":
    task = asyncio.create_task(coro, name=name)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_reap_tracked_task)
    return task


# Bounds every writer teardown: wait_closed() on a peer that vanished with
# unflushed data (collector rebooting mid-frame) otherwise blocks until the
# OS-level TCP timeout — minutes, observed hanging Home Assistant shutdown.
_WRITER_CLOSE_TIMEOUT = 5.0


async def _cancel_and_join_task(task: "asyncio.Task[Any]") -> None:
    """Cancel a session task and wait for it, re-cancelling until it dies.

    A single cancel() is not enough: on Python < 3.12 ``asyncio.wait_for``
    swallows a cancellation that races its inner future completing
    (gh-86296), so a task cancelled mid-write keeps running and a bare
    ``await task`` then blocks for as long as the task stays alive — the
    heartbeat loop, for one, never exits on a healthy socket.
    """

    attempts = 0
    while not task.done():
        task.cancel()
        await asyncio.wait({task}, timeout=0.25)
        attempts += 1
        if attempts >= 20 and not task.done():
            # A task that survives 20 cancellations is swallowing
            # CancelledError; waiting longer would recreate the very hang
            # this helper exists to prevent.
            logger.error(
                "Session task %s ignored %d cancellations; abandoning join",
                task.get_name(),
                attempts,
            )
            return
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


async def _close_writer_bounded(writer: Any) -> None:
    """Close a stream writer without inheriting a dead peer's TCP timeout."""

    try:
        writer.close()
    except Exception:
        return
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=_WRITER_CLOSE_TIMEOUT)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


async def _finish_cleanup_on_cancel(awaitable: Awaitable[Any]) -> Any:
    """Finish critical cleanup even if the caller is already being cancelled."""

    future = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        try:
            await future
        except Exception:
            raise
        raise


def _looks_like_at_traffic(chunk: bytes) -> bool:
    return chunk.lstrip().startswith(b"AT+")


def _looks_like_uart_passthrough_value(value: str) -> bool:
    normalized = str(value or "").strip()
    if not normalized.isascii():
        return False
    parts = [part.strip() for part in normalized.split(",")]
    if len(parts) != 4:
        return False
    baud, data_bits, stop_bits, parity = parts
    if not baud.isdigit() or not data_bits.isdigit() or not stop_bits.isdigit():
        return False
    if parity.upper() not in {"NONE", "N", "ODD", "EVEN", "O", "E"}:
        return False
    return True


def _looks_like_plain_raw_response_start(chunk: bytes) -> bool:
    if not chunk:
        return False
    first = chunk[:1]
    if first in {b"(", b"^"}:
        return True
    value = first[0]
    return 0x20 <= value <= 0x7E


def _short_ascii(value: bytes, *, limit: int = 160) -> str:
    text = "".join(chr(byte) if 0x20 <= byte <= 0x7E else "." for byte in value[:limit])
    if len(value) > limit:
        text += "..."
    return text


_AT_TEXT_MIXED_FRAME_READ_TIMEOUT = 0.05
_AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN = 4096
_AT_TEXT_MIXED_FRAME_FCODES = {
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    FC_SET_COLLECTOR,
    FC_FORWARD_TO_DEVICE,
    FC_TRIGGER_QUERY_REAL_TIME,
    FC_SET_DEVICE_REG,
    FC_TRIGGER_QUERY_HISTORY,
}


def _mask_identity_token(value: str) -> str:
    token = str(value or "").strip()
    if len(token) <= 6:
        return "*" * len(token)
    return f"{token[:3]}{'*' * max(len(token) - 6, 3)}{token[-3:]}"


def _prefer_more_complete_identity(current: str, candidate: str) -> str:
    normalized_current = str(current or "").strip()
    normalized_candidate = str(candidate or "").strip()
    if not normalized_candidate:
        return normalized_current
    if not normalized_current:
        return normalized_candidate
    if normalized_candidate == normalized_current:
        return normalized_candidate
    if normalized_candidate.startswith(normalized_current):
        return normalized_candidate
    if normalized_current.startswith(normalized_candidate):
        return normalized_current
    return normalized_current


_AT_DTUPN_RE = re.compile(rb"AT\+DTUPN\s*[:=]\s*([A-Za-z0-9][A-Za-z0-9._-]{5,})")


def _parse_fc2_collector_pn(payload: bytes) -> str:
    if len(payload) < 2:
        return ""
    if payload[1] != 2:
        return ""
    return payload[2:].decode("ascii", errors="ignore").strip("\x00").strip()


def _collector_pn_from_initial_chunk(chunk: bytes) -> tuple[str, str]:
    payload = bytes(chunk or b"")
    if not payload:
        return "", ""

    match = _AT_DTUPN_RE.search(payload)
    if match:
        return match.group(1).decode("ascii", errors="ignore").strip(), "at_dtupn"

    if len(payload) < HEADER_SIZE:
        return "", ""
    try:
        header = decode_header(payload[:HEADER_SIZE])
    except Exception:
        return "", ""
    available_payload = payload[HEADER_SIZE : HEADER_SIZE + max(header.payload_len, 0)]
    if header.fcode == FC_HEARTBEAT:
        return parse_heartbeat_pn(available_payload), "framed_heartbeat"
    if header.fcode == FC_QUERY_COLLECTOR:
        return _parse_fc2_collector_pn(available_payload), "fc2_parameter_2"
    return "", ""


def _identity_probe_payload_for_session_protocol(session_protocol: str) -> bytes:
    normalized = str(session_protocol or "").strip().lower()
    if normalized == "at_text":
        return build_at_query("DTUPN")
    if normalized == "eybond_framed":
        return build_collector_request(
            1,
            b"\x02",
            devcode=1,
            collector_addr=1,
            fcode=FC_QUERY_COLLECTOR,
        )
    return b""


def _bounded_write_timeout(request_timeout: float) -> float:
    return max(0.5, min(float(request_timeout), 1.5))


def _parse_ip_address(value: str) -> ipaddress._BaseAddress | None:
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def _is_hairpin_alias_candidate(expected_ip: str, remote_ip: str) -> bool:
    expected = _parse_ip_address(expected_ip)
    remote = _parse_ip_address(remote_ip)
    if expected is None or remote is None:
        return False
    return bool(expected.is_global and not remote.is_global)


def _is_default_broadcast_alias_candidate(expected_ip: str, remote_ip: str) -> bool:
    expected = _parse_ip_address(expected_ip)
    remote = _parse_ip_address(remote_ip)
    if not isinstance(expected, ipaddress.IPv4Address) or not isinstance(remote, ipaddress.IPv4Address):
        return False
    if expected == ipaddress.IPv4Address("255.255.255.255"):
        return True
    return expected == ipaddress.IPv4Address(int(remote) | 0xFF)


def _is_ipv4_broadcast_placeholder(value: str) -> bool:
    parsed = _parse_ip_address(value)
    return isinstance(parsed, ipaddress.IPv4Address) and (
        parsed == ipaddress.IPv4Address("255.255.255.255")
        or int(parsed) & 0xFF == 0xFF
    )


def _disconnect_reason_from_exception(exc: BaseException) -> str:
    if isinstance(exc, ConnectionResetError):
        return "collector_connection_reset"
    if isinstance(exc, BrokenPipeError):
        return "collector_broken_pipe"
    if isinstance(exc, OSError):
        return "collector_os_error"
    return "collector_disconnected"


def _copy_collector_info(collector: CollectorInfo) -> CollectorInfo:
    return apply_collector_profile(
        CollectorInfo(
            remote_ip=collector.remote_ip,
            remote_port=collector.remote_port,
            connection_count=collector.connection_count,
            connection_replace_count=collector.connection_replace_count,
            disconnect_count=collector.disconnect_count,
            pending_request_drop_count=collector.pending_request_drop_count,
            last_disconnect_reason=collector.last_disconnect_reason,
            discovery_restart_count=collector.discovery_restart_count,
            last_discovery_reason=collector.last_discovery_reason,
            collector_pn=collector.collector_pn,
            last_devcode=collector.last_devcode,
            heartbeat_devcode=collector.heartbeat_devcode,
            heartbeat_payload_hex=collector.heartbeat_payload_hex,
            last_udp_reply=collector.last_udp_reply,
            last_udp_reply_from=collector.last_udp_reply_from,
            profile_key=collector.profile_key,
            profile_name=collector.profile_name,
            heartbeat_ascii=collector.heartbeat_ascii,
            heartbeat_payload_len=collector.heartbeat_payload_len,
            heartbeat_format_key=collector.heartbeat_format_key,
            heartbeat_suffix_ascii=collector.heartbeat_suffix_ascii,
            heartbeat_suffix_kind=collector.heartbeat_suffix_kind,
            heartbeat_suffix_uint=collector.heartbeat_suffix_uint,
            devcode_major=collector.devcode_major,
            devcode_minor=collector.devcode_minor,
            collector_pn_prefix=collector.collector_pn_prefix,
            collector_pn_digits=collector.collector_pn_digits,
            heartbeat_age_seconds=collector.heartbeat_age_seconds,
            heartbeat_fresh=collector.heartbeat_fresh,
            raw_request_count=collector.raw_request_count,
            raw_response_count=collector.raw_response_count,
            raw_timeout_count=collector.raw_timeout_count,
            raw_unhandled_line_count=collector.raw_unhandled_line_count,
            raw_last_request_ascii=collector.raw_last_request_ascii,
            raw_last_request_hex=collector.raw_last_request_hex,
            raw_last_response_ascii=collector.raw_last_response_ascii,
            raw_last_response_hex=collector.raw_last_response_hex,
            raw_last_timeout_request_ascii=collector.raw_last_timeout_request_ascii,
            raw_last_parser=collector.raw_last_parser,
            raw_last_frame_format=collector.raw_last_frame_format,
            raw_last_spacing_wait_ms=collector.raw_last_spacing_wait_ms,
            raw_last_response_duration_ms=collector.raw_last_response_duration_ms,
            raw_last_total_duration_ms=collector.raw_last_total_duration_ms,
            collector_cloud_family=collector.collector_cloud_family,
            collector_cloud_family_source=collector.collector_cloud_family_source,
            collector_cloud_family_confidence=collector.collector_cloud_family_confidence,
            collector_server_endpoint=collector.collector_server_endpoint,
            collector_cloud_profile_key=collector.collector_cloud_profile_key,
            collector_cloud_profile_label=collector.collector_cloud_profile_label,
            collector_cloud_profile_source=collector.collector_cloud_profile_source,
            collector_cloud_profile_confidence=collector.collector_cloud_profile_confidence,
            smartess_collector_version=collector.smartess_collector_version,
            smartess_protocol_raw_id=collector.smartess_protocol_raw_id,
            smartess_protocol_asset_id=collector.smartess_protocol_asset_id,
            smartess_protocol_asset_name=collector.smartess_protocol_asset_name,
            smartess_protocol_suffix=collector.smartess_protocol_suffix,
            smartess_protocol_profile_key=collector.smartess_protocol_profile_key,
            smartess_protocol_name=collector.smartess_protocol_name,
            smartess_device_address=collector.smartess_device_address,
            collector_virtual_bridge=collector.collector_virtual_bridge,
            collector_bridge_kind=collector.collector_bridge_kind,
            collector_bridge_version=collector.collector_bridge_version,
        )
    )


class _PrefixedAsyncReader:
    def __init__(self, reader: asyncio.StreamReader, initial_bytes: bytes = b"") -> None:
        self._reader = reader
        self._buffer = bytearray(initial_bytes)

    async def readexactly(self, size: int) -> bytes:
        if size <= 0:
            return b""
        if len(self._buffer) >= size:
            data = bytes(self._buffer[:size])
            del self._buffer[:size]
            return data

        data = bytes(self._buffer)
        self._buffer.clear()
        if len(data) == size:
            return data
        data += await self._reader.readexactly(size - len(data))
        return data

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        index = self._buffer.find(separator)
        if index >= 0:
            end = index + len(separator)
            data = bytes(self._buffer[:end])
            del self._buffer[:end]
            return data

        data = bytes(self._buffer)
        self._buffer.clear()
        return data + await self._reader.readuntil(separator)


class CollectorTransport(PayloadLinkTransport, Protocol):
    @property
    def connected(self) -> bool:
        ...

    @property
    def collector_info(self) -> CollectorInfo:
        ...

    async def wait_until_connected(self, timeout: float) -> bool:
        ...

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        ...

    async def disconnect(self) -> None:
        ...

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        ...

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
    ) -> bytes:
        ...

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
        ...


class CollectorAtTransport(Protocol):
    @property
    def connected(self) -> bool:
        ...

    @property
    def collector_info(self) -> CollectorInfo:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    async def wait_until_connected(self, timeout: float) -> bool:
        ...

    async def async_query(self, command: str) -> CollectorAtResponse:
        ...

    async def async_write(self, command: str, value: str) -> CollectorAtResponse:
        ...


class _CollectorConnection:
    def __init__(self, *, remote_ip_hint: str = "", heartbeat_interval: float, write_timeout: float) -> None:
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
            self._collector.collector_pn = response.value
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
            future.set_result(response)
            return

        self._apply_at_response_metadata(response)
        logger.debug(
            "Unsolicited collector AT response remote=%s command=%s value=%s",
            self._collector.remote_ip,
            response.command,
            response.value,
        )

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                prefix = await reader.readexactly(3)
                if prefix == b"AT+":
                    line = prefix + await reader.readuntil(b"\n")
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

                if header.fcode == FC_HEARTBEAT:
                    pn = parse_heartbeat_pn(payload)
                    if pn:
                        self._collector.collector_pn = pn
                        self._record_session_identity(pn, "framed_heartbeat")
                    self._collector.heartbeat_devcode = header.devcode
                    self._collector.heartbeat_payload_hex = payload.hex()
                    self._last_heartbeat_monotonic = monotonic()
                elif header.fcode == FC_QUERY_COLLECTOR:
                    pn = _parse_fc2_collector_pn(payload)
                    if pn:
                        self._collector.collector_pn = pn
                        self._record_session_identity(pn, "fc2_parameter_2")
                future = self._pending.get(header.tid)
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

    async def async_send_raw_payload(self, payload: bytes, *, request_timeout: float) -> bytes:
        """Send one raw inverter payload over a plain AT callback stream.

        Some DTU/AT collectors do not wrap inverter traffic in the legacy EyeBond
        FC=4 tunnel. The cloud writes PI-style ASCII commands directly to the same
        TCP stream after the AT bootstrap and receives raw ``(...\r`` responses.
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

        logger.info("Collector AT connection from %s:%s", self._collector.remote_ip, self._collector.remote_port)

        current_task = asyncio.current_task()
        prefixed_reader = _PrefixedAsyncReader(reader, initial_bytes)
        self._reader_task = asyncio.create_task(
            self._read_loop(prefixed_reader),
            name=f"collector_at_reader_{self._collector.remote_ip}",
        )
        try:
            await self._reader_task
        finally:
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

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
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
                        line = prefix + await reader.readuntil(b"\n")
                        self._handle_at_response_line(line)
                        continue
                    buffered_prefix = prefix + buffered_prefix

                raw_future = self._pending_raw_response
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
                            self._collector.collector_pn = pn
                            self._record_session_identity(pn, "framed_heartbeat")
                        self._collector.heartbeat_devcode = header.devcode
                        self._collector.heartbeat_payload_hex = payload.hex()
                    elif header.fcode == FC_QUERY_COLLECTOR:
                        pn = _parse_fc2_collector_pn(payload)
                        if pn:
                            self._collector.collector_pn = pn
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
            self._collector.collector_pn = response.value
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
        if raw_future is not None and not raw_future.done():
            raw_future.set_exception(ConnectionError("collector_disconnected"))


@dataclass(slots=True)
class _PendingCollectorSocket:
    remote_ip: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    session_id: str = ""
    remote_port: int | None = None
    sniff_task: asyncio.Task[None] | None = None
    initial_bytes: bytes = b""
    parked: bool = False


@dataclass(slots=True)
class _CollectorSessionInventoryEntry:
    session_id: str
    remote_ip: str
    remote_port: int | None
    state: str = "pending"
    protocol_shape: str = "unknown"
    first_bytes_len: int = 0
    first_bytes_prefix_hex: str = ""
    collector_pn: str = ""
    collector_identity_source: str = ""

    def diagnostics(self) -> dict[str, object]:
        result: dict[str, object] = {
            "session_id": self.session_id,
            "peer_ip": self.remote_ip,
            "state": self.state,
            "protocol_shape": self.protocol_shape,
            "first_bytes_len": self.first_bytes_len,
        }
        if self.remote_port is not None:
            result["peer_port"] = self.remote_port
        if self.first_bytes_prefix_hex:
            result["first_bytes_prefix_hex"] = self.first_bytes_prefix_hex
        if self.collector_pn:
            result["collector_identity_masked"] = _mask_identity_token(self.collector_pn)
        if self.collector_identity_source:
            result["collector_identity_source"] = self.collector_identity_source
        return result


class _SharedEybondListener:
    _MAX_SESSION_INVENTORY = 20
    # Unclaimed collector callbacks are parked (held open passively) instead
    # of being closed: closing makes the collector firmware redial within
    # seconds, producing a permanent connect/close loop for collectors that
    # have no config entry. Parked sockets stay claimable by a later scan or
    # a newly added entry.
    _MAX_PARKED_SOCKETS = 8
    _PARKED_SOCKET_TTL_SECONDS = 900.0
    _PARKED_IDENTITY_BUFFER_LIMIT = 512

    def __init__(self, *, host: str, port: int) -> None:
        self._host = host
        self._port = int(port)
        self._server: asyncio.Server | None = None
        self._ref_count = 0
        self._connections: dict[str, _CollectorConnection] = {}
        self._at_connections: dict[str, _CollectorAtConnection] = {}
        self._connections_by_pn: dict[str, _CollectorConnection] = {}
        self._at_connections_by_pn: dict[str, _CollectorAtConnection] = {}
        self._session_payload_connections: dict[str, _CollectorConnection] = {}
        self._session_at_connections: dict[str, _CollectorAtConnection] = {}
        self._pending_sockets: dict[str, _PendingCollectorSocket] = {}
        self._last_connection_ip = ""
        self._last_at_connection_ip = ""
        self._last_pending_ip = ""
        self._payload_owner_counts: dict[str, int] = {}
        self._at_owner_counts: dict[str, int] = {}
        self._payload_pn_owner_counts: dict[str, int] = {}
        self._at_pn_owner_counts: dict[str, int] = {}
        self._session_protocol_owner_counts: dict[str, int] = {}
        self._session_seq = 0
        self._session_inventory: dict[str, _CollectorSessionInventoryEntry] = {}
        self._pending_route_lock = asyncio.Lock()
        self._connection_watcher_seq = 0
        self._connection_watchers: dict[int, tuple[str, Callable[[str], None]]] = {}

    def add_connection_watcher(
        self,
        collector_ip: str,
        callback: Callable[[str], None],
    ) -> int:
        """Register a callback fired when a collector socket arrives.

        ``collector_ip`` scopes the watcher to one collector; an empty value
        matches any incoming connection. The callback runs on the event loop
        and must not block.
        """

        self._connection_watcher_seq += 1
        token = self._connection_watcher_seq
        self._connection_watchers[token] = (str(collector_ip or "").strip(), callback)
        return token

    def remove_connection_watcher(self, token: int) -> None:
        self._connection_watchers.pop(token, None)

    def _notify_connection_watchers(self, remote_ip: str) -> None:
        for watched_ip, callback in tuple(self._connection_watchers.values()):
            if watched_ip and watched_ip != remote_ip:
                continue
            try:
                callback(remote_ip)
            except Exception:
                logger.debug("Collector connection watcher failed", exc_info=True)

    async def acquire(self) -> None:
        self._ref_count += 1
        if self._server is None:
            try:
                self._server = await asyncio.start_server(
                    self._handle_connection,
                    self._host,
                    self._port,
                )
            except OSError as exc:
                self._ref_count = max(0, self._ref_count - 1)
                raise CollectorListenerBindError(self._host, self._port, exc) from exc
            logger.info("Shared EyeBond listener listening on %s:%d", self._host, self._port)

    async def release(self) -> bool:
        self._ref_count = max(0, self._ref_count - 1)
        if self._ref_count != 0:
            return False

        for pending in tuple(self._pending_sockets.values()):
            await self._close_pending_socket(pending)
        self._pending_sockets.clear()

        for connection in self._unique_connections():
            await connection.disconnect()
        self._connections.clear()
        self._connections_by_pn.clear()
        for connection in self._unique_at_connections():
            await connection.disconnect()
        self._at_connections.clear()
        self._at_connections_by_pn.clear()
        self._session_payload_connections.clear()
        self._session_at_connections.clear()
        self._last_connection_ip = ""
        self._last_at_connection_ip = ""
        self._last_pending_ip = ""
        self._payload_owner_counts.clear()
        self._at_owner_counts.clear()
        self._payload_pn_owner_counts.clear()
        self._at_pn_owner_counts.clear()
        self._session_protocol_owner_counts.clear()
        self._session_inventory.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        return True

    def register_payload_owner(self, collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        self._payload_owner_counts[owner] = self._payload_owner_counts.get(owner, 0) + 1

    def register_payload_pn_owner(self, collector_pn: str) -> None:
        owner = str(collector_pn or "").strip()
        if not owner:
            return
        self._payload_pn_owner_counts[owner] = self._payload_pn_owner_counts.get(owner, 0) + 1

    def unregister_payload_owner(self, collector_ip: str) -> None:
        self._decrement_owner_count(self._payload_owner_counts, collector_ip)

    def unregister_payload_pn_owner(self, collector_pn: str) -> None:
        self._decrement_owner_count(self._payload_pn_owner_counts, collector_pn)

    def register_at_owner(self, collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        self._at_owner_counts[owner] = self._at_owner_counts.get(owner, 0) + 1

    def register_at_pn_owner(self, collector_pn: str) -> None:
        owner = str(collector_pn or "").strip()
        if not owner:
            return
        self._at_pn_owner_counts[owner] = self._at_pn_owner_counts.get(owner, 0) + 1

    def unregister_at_owner(self, collector_ip: str) -> None:
        self._decrement_owner_count(self._at_owner_counts, collector_ip)

    def unregister_at_pn_owner(self, collector_pn: str) -> None:
        self._decrement_owner_count(self._at_pn_owner_counts, collector_pn)

    def register_session_protocol_owner(self, session_protocol: str) -> None:
        owner = str(session_protocol or "").strip().lower()
        if not owner:
            return
        self._session_protocol_owner_counts[owner] = (
            self._session_protocol_owner_counts.get(owner, 0) + 1
        )

    def unregister_session_protocol_owner(self, session_protocol: str) -> None:
        self._decrement_owner_count(self._session_protocol_owner_counts, session_protocol)

    def _decrement_owner_count(self, owner_counts: dict[str, int], owner_value: str) -> None:
        owner = str(owner_value or "").strip()
        count = owner_counts.get(owner, 0)
        if count <= 1:
            owner_counts.pop(owner, None)
            return
        owner_counts[owner] = count - 1

    def ensure_connection(
        self,
        collector_ip: str,
        heartbeat_interval: float,
        write_timeout: float,
        collector_pn: str = "",
    ) -> _CollectorConnection | None:
        if collector_ip:
            connection = self._connections.get(collector_ip)
            if connection is None:
                connection = _CollectorConnection(
                    remote_ip_hint=collector_ip,
                    heartbeat_interval=heartbeat_interval,
                    write_timeout=write_timeout,
                )
                self._connections[collector_ip] = connection
            else:
                connection.set_heartbeat_interval(heartbeat_interval)
                connection.set_write_timeout(write_timeout)
            return connection

        if collector_pn:
            connection = self._connection_by_collector_pn(
                collector_pn,
                self._connections_by_pn,
            )
            if connection is not None:
                connection.set_heartbeat_interval(heartbeat_interval)
                connection.set_write_timeout(write_timeout)
            return connection

        connection = self.current_connection(
            heartbeat_interval=heartbeat_interval,
            write_timeout=write_timeout,
        )
        return connection

    def current_connection(self, *, heartbeat_interval: float, write_timeout: float) -> _CollectorConnection | None:
        connected = tuple(
            connection
            for connection in self._unique_connections()
            if connection.connected
        )
        if len(connected) != 1:
            return None

        connection = connected[0]
        connection.set_heartbeat_interval(heartbeat_interval)
        connection.set_write_timeout(write_timeout)
        return connection

    def ensure_at_connection(
        self,
        collector_ip: str,
        write_timeout: float,
        collector_pn: str = "",
        raw_passthrough_bootstrap: str = "",
        raw_passthrough_frame_format: str = "",
        raw_passthrough_min_interval_ms: int = 0,
    ) -> _CollectorAtConnection | None:
        if collector_ip:
            connection = self._at_connections.get(collector_ip)
            if connection is None:
                connection = _CollectorAtConnection(
                    remote_ip_hint=collector_ip,
                    write_timeout=write_timeout,
                    raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                    raw_passthrough_frame_format=raw_passthrough_frame_format,
                    raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
                )
                self._at_connections[collector_ip] = connection
            else:
                connection.set_write_timeout(write_timeout)
                connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
                connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
                connection.set_raw_passthrough_min_interval_ms(
                    raw_passthrough_min_interval_ms
                )
            return connection

        if collector_pn:
            connection = self._connection_by_collector_pn(
                collector_pn,
                self._at_connections_by_pn,
            )
            if connection is not None:
                connection.set_write_timeout(write_timeout)
                connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
                connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
                connection.set_raw_passthrough_min_interval_ms(
                    raw_passthrough_min_interval_ms
                )
            return connection

        connection = self.current_at_connection(write_timeout=write_timeout)
        if connection is not None:
            connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
            connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
            connection.set_raw_passthrough_min_interval_ms(raw_passthrough_min_interval_ms)
        return connection

    def _connection_by_collector_pn(
        self,
        collector_pn: str,
        connections_by_pn: dict[str, object],
    ) -> object | None:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return None
        exact = connections_by_pn.get(normalized_pn)
        if exact is not None:
            return exact

        candidates: list[object] = []
        for known_pn, connection in connections_by_pn.items():
            if self._collector_pn_matches(normalized_pn, known_pn):
                candidates.append(connection)
        unique_candidates = {id(candidate): candidate for candidate in candidates}
        if len(unique_candidates) != 1:
            return None
        return next(iter(unique_candidates.values()))

    def _single_registered_session_protocol(self) -> str:
        protocols = tuple(
            protocol
            for protocol, count in self._session_protocol_owner_counts.items()
            if protocol and count > 0
        )
        if len(protocols) != 1:
            return ""
        return protocols[0]

    def current_at_connection(self, *, write_timeout: float) -> _CollectorAtConnection | None:
        connected = tuple(
            connection
            for connection in self._unique_at_connections()
            if connection.connected
        )
        if len(connected) != 1:
            return None

        connection = connected[0]
        connection.set_write_timeout(write_timeout)
        return connection

    def _unique_connections(self) -> tuple[_CollectorConnection, ...]:
        seen: set[int] = set()
        unique: list[_CollectorConnection] = []
        for connection in self._connections.values():
            identity = id(connection)
            if identity in seen:
                continue
            seen.add(identity)
            unique.append(connection)
        return tuple(unique)

    def _unique_at_connections(self) -> tuple[_CollectorAtConnection, ...]:
        seen: set[int] = set()
        unique: list[_CollectorAtConnection] = []
        for connection in self._at_connections.values():
            identity = id(connection)
            if identity in seen:
                continue
            seen.add(identity)
            unique.append(connection)
        return tuple(unique)

    def session_inventory_diagnostics(self) -> dict[str, object]:
        entries = tuple(self._session_inventory.values())
        pending_ids = {pending.session_id for pending in self._pending_sockets.values()}
        peer_counts: dict[str, int] = {}
        for entry in entries:
            if not entry.remote_ip:
                continue
            peer_counts[entry.remote_ip] = peer_counts.get(entry.remote_ip, 0) + 1
        duplicate_peer_ips = sorted(
            peer_ip for peer_ip, count in peer_counts.items() if count > 1
        )
        return {
            "pending_session_count": len(pending_ids),
            "recent_session_count": len(entries),
            "duplicate_peer_ip_count": len(duplicate_peer_ips),
            "duplicate_peer_ips": duplicate_peer_ips,
            "sessions": [entry.diagnostics() for entry in entries],
        }

    def discovered_collector_sessions(self) -> tuple[dict[str, object], ...]:
        """Return raw collector identities observed by this listener.

        This is intentionally separate from ``session_inventory_diagnostics``:
        diagnostics mask collector PN values for support bundles, while onboarding
        needs the raw PN to materialize multiple collectors that call back from
        the same NAT peer IP.
        """

        sessions: list[dict[str, object]] = []
        for entry in self._session_inventory.values():
            collector_pn = str(entry.collector_pn or "").strip()
            remote_ip = str(entry.remote_ip or "").strip()
            if not collector_pn or not remote_ip:
                continue
            sessions.append(
                {
                    "session_id": entry.session_id,
                    "peer_ip": remote_ip,
                    "peer_port": entry.remote_port,
                    "state": entry.state,
                    "protocol_shape": entry.protocol_shape,
                    "collector_pn": collector_pn,
                    "collector_identity_source": entry.collector_identity_source,
                }
            )
        return tuple(sessions)

    def _next_session_id(self) -> str:
        self._session_seq += 1
        return f"listener-{self._port}-{self._session_seq}"

    def _remember_session(
        self,
        *,
        session_id: str,
        remote_ip: str,
        remote_port: int | None,
    ) -> None:
        self._session_inventory[session_id] = _CollectorSessionInventoryEntry(
            session_id=session_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
        )
        while len(self._session_inventory) > self._MAX_SESSION_INVENTORY:
            oldest = next(iter(self._session_inventory))
            self._session_inventory.pop(oldest, None)

    def _mark_session_state(self, session_id: str, state: str) -> None:
        entry = self._session_inventory.get(session_id)
        if entry is not None:
            entry.state = state

    def _mark_session_first_bytes(self, session_id: str, chunk: bytes) -> None:
        entry = self._session_inventory.get(session_id)
        if entry is None:
            return
        entry.first_bytes_len = len(chunk)
        entry.first_bytes_prefix_hex = chunk[:4].hex()
        if _looks_like_at_traffic(chunk):
            entry.protocol_shape = "at_text"
            return
        if len(chunk) >= 3:
            entry.protocol_shape = "eybond_framed_or_binary"
            return
        entry.protocol_shape = "unknown"

    def _mark_session_identity(
        self,
        session_id: str,
        collector_pn: str,
        source: str,
    ) -> None:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return
        entry = self._session_inventory.get(session_id)
        if entry is not None:
            entry.collector_pn = _prefer_more_complete_identity(
                entry.collector_pn,
                normalized_pn,
            )
            if source:
                entry.collector_identity_source = str(source)

        payload_connection = self._session_payload_connections.get(session_id)
        if payload_connection is not None:
            self._connections_by_pn[normalized_pn] = payload_connection
        at_connection = self._session_at_connections.get(session_id)
        if at_connection is not None:
            self._at_connections_by_pn[normalized_pn] = at_connection

    def matching_callback_ips(self, collector_ip: str) -> tuple[str, ...]:
        if not collector_ip:
            return ()

        is_broadcast_placeholder = _is_ipv4_broadcast_placeholder(collector_ip)
        ordered: list[str] = []
        seen: set[str] = set()

        def _matches(remote_ip: str) -> bool:
            if not remote_ip:
                return False
            if is_broadcast_placeholder and remote_ip == collector_ip:
                return False
            if remote_ip == collector_ip:
                return True
            return bool(
                _is_hairpin_alias_candidate(collector_ip, remote_ip)
                or _is_default_broadcast_alias_candidate(collector_ip, remote_ip)
            )

        def _remember(remote_ip: str) -> None:
            if not _matches(remote_ip) or remote_ip in seen:
                return
            seen.add(remote_ip)
            ordered.append(remote_ip)

        _remember(self._last_connection_ip)
        _remember(self._last_pending_ip)
        _remember(self._last_at_connection_ip)

        for pending in self._pending_sockets.values():
            _remember(pending.remote_ip)

        for remote_ip, connection in self._connections.items():
            if connection.connected:
                _remember(remote_ip)

        for remote_ip, connection in self._at_connections.items():
            if connection.connected:
                _remember(remote_ip)

        return tuple(ordered)

    def _resolve_public_placeholder_alias(
        self,
        remote_ip: str,
        connections: dict[str, object] | None = None,
    ) -> object | None:
        connection_map = connections if connections is not None else self._connections
        if not remote_ip or remote_ip in connection_map:
            return connection_map.get(remote_ip)

        candidates: list[tuple[str, object]] = []
        for expected_ip, connection in connection_map.items():
            if getattr(connection, "connected", False):
                continue
            if not (
                _is_hairpin_alias_candidate(expected_ip, remote_ip)
                or _is_default_broadcast_alias_candidate(expected_ip, remote_ip)
            ):
                continue
            candidates.append((expected_ip, connection))

        unique_candidates: list[tuple[str, object]] = []
        seen: set[int] = set()
        for expected_ip, connection in candidates:
            identity = id(connection)
            if identity in seen:
                continue
            seen.add(identity)
            unique_candidates.append((expected_ip, connection))

        if len(unique_candidates) != 1:
            return None

        expected_ip, connection = unique_candidates[0]
        logger.info(
            "Aliasing collector callback from %s to pending unresolved target %s",
            remote_ip,
            expected_ip,
        )
        connection_map[remote_ip] = connection
        return connection

    def has_pending_socket(self, collector_ip: str = "") -> bool:
        return self._select_pending_socket(collector_ip) is not None

    def pop_pending_socket(self, collector_ip: str = "") -> _PendingCollectorSocket | None:
        pending = self._select_pending_socket(collector_ip)
        if pending is None:
            return None
        return self._claim_pending_socket(pending)

    async def pop_pending_socket_for_route(
        self,
        *,
        collector_ip: str = "",
        collector_pn: str = "",
        session_protocol: str = "",
    ) -> _PendingCollectorSocket | None:
        async with self._pending_route_lock:
            normalized_pn = str(collector_pn or "").strip()
            if not normalized_pn:
                pending = self._select_pending_socket(collector_ip)
                if pending is None:
                    return None
                await self._pause_pending_sniff(pending)
                if not self._pending_socket_still_registered(pending):
                    return None
                return self._claim_pending_socket(pending)

            matched = self._select_pending_socket_by_collector_pn(normalized_pn)
            if matched is not None:
                return self._claim_pending_socket(matched)

            candidates = self._route_identity_candidates(collector_ip)
            if not candidates:
                return None

            for pending in candidates:
                if not self._pending_socket_still_registered(pending):
                    continue
                await self._pause_pending_sniff(pending)
                if not self._pending_socket_still_registered(pending):
                    continue
                pending_pn = await self._identify_pending_socket_for_route(
                    pending,
                    session_protocol=session_protocol,
                )
                if not self._pending_socket_still_registered(pending):
                    continue
                if self._collector_pn_matches(normalized_pn, pending_pn):
                    return self._claim_pending_socket(pending)
                if pending_pn:
                    self._mark_session_state(pending.session_id, "route_identity_mismatch")
                else:
                    self._mark_session_state(pending.session_id, "waiting_for_route_identity")
                # The pause above cancelled the sniff task; the socket stays
                # registered for another claimant, so it needs a watcher again.
                self._resume_pending_watch(pending)
            return None

    def _claim_pending_socket(self, pending: _PendingCollectorSocket) -> _PendingCollectorSocket:
        self._remove_pending_socket(pending)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is not None:
            sniff_task.cancel()
        self._mark_session_state(pending.session_id, "claimed")
        return pending

    def _pending_socket_key(self, pending: _PendingCollectorSocket) -> str:
        for key, candidate in self._pending_sockets.items():
            if candidate is pending:
                return key
        return pending.session_id or pending.remote_ip

    def _remove_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        self._pending_sockets.pop(self._pending_socket_key(pending), None)

    def _pending_socket_still_registered(self, pending: _PendingCollectorSocket) -> bool:
        return any(candidate is pending for candidate in self._pending_sockets.values())

    def _pending_sockets_for_remote_ip(self, remote_ip: str) -> tuple[_PendingCollectorSocket, ...]:
        return tuple(
            pending
            for pending in self._pending_sockets.values()
            if pending.remote_ip == remote_ip
        )

    def _select_pending_socket_by_collector_pn(
        self,
        collector_pn: str,
    ) -> _PendingCollectorSocket | None:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return None
        candidates: list[_PendingCollectorSocket] = []
        for pending in self._pending_sockets.values():
            entry = self._session_inventory.get(pending.session_id)
            pending_pn = str(getattr(entry, "collector_pn", "") or "").strip()
            if self._collector_pn_matches(normalized_pn, pending_pn):
                candidates.append(pending)
        unique_candidates = {id(candidate): candidate for candidate in candidates}
        if len(unique_candidates) != 1:
            return None
        return next(iter(unique_candidates.values()))

    def _route_identity_candidates(self, collector_ip: str) -> tuple[_PendingCollectorSocket, ...]:
        if not collector_ip:
            return tuple(self._pending_sockets.values())

        exact = self._pending_sockets_for_remote_ip(collector_ip)
        if exact:
            return exact

        return tuple(
            pending
            for pending in self._pending_sockets.values()
            if _is_hairpin_alias_candidate(collector_ip, pending.remote_ip)
            or _is_default_broadcast_alias_candidate(collector_ip, pending.remote_ip)
        )

    async def _pause_pending_sniff(self, pending: _PendingCollectorSocket) -> None:
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is None or sniff_task.done():
            return
        sniff_task.cancel()
        try:
            await sniff_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def _collector_pn_matches(self, expected_pn: str, observed_pn: str) -> bool:
        expected = str(expected_pn or "").strip()
        observed = str(observed_pn or "").strip()
        if not expected or not observed:
            return False
        if expected == observed:
            return True
        if len(expected) < _COLLECTOR_PN_PREFIX_MATCH_MIN_LEN:
            return False
        if len(observed) < _COLLECTOR_PN_PREFIX_MATCH_MIN_LEN:
            return False
        return bool(expected.startswith(observed) or observed.startswith(expected))

    def _select_pending_socket(self, collector_ip: str) -> _PendingCollectorSocket | None:
        if collector_ip:
            exact = self._pending_sockets_for_remote_ip(collector_ip)
            if len({id(pending) for pending in exact}) == 1:
                return exact[0]
            if len(exact) > 1:
                return None

            candidates: list[_PendingCollectorSocket] = []
            for candidate in self._pending_sockets.values():
                remote_ip = candidate.remote_ip
                if not (
                    _is_hairpin_alias_candidate(collector_ip, remote_ip)
                    or _is_default_broadcast_alias_candidate(collector_ip, remote_ip)
                ):
                    continue
                candidates.append(candidate)

            unique_candidates = {id(candidate): candidate for candidate in candidates}
            if len(unique_candidates) == 1:
                return next(iter(unique_candidates.values()))
            if unique_candidates and _is_ipv4_broadcast_placeholder(collector_ip):
                preferred = tuple(
                    pending
                    for pending in self._pending_sockets.values()
                    if pending.remote_ip == self._last_pending_ip
                    and id(pending) in unique_candidates
                )
                if len(preferred) == 1:
                    return preferred[0]
                return next(iter(unique_candidates.values()))
            return None

        unique_candidates = tuple({id(pending): pending for pending in self._pending_sockets.values()}.values())
        if len(unique_candidates) != 1:
            return None
        return unique_candidates[0]

    async def _close_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is not None:
            sniff_task.cancel()
            try:
                await sniff_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        try:
            await _close_writer_bounded(pending.writer)
        except asyncio.CancelledError:
            pass

    def _evict_parked_sockets(self, new_pending: _PendingCollectorSocket) -> None:
        """Bound parked sockets: replace same-IP parks, cap the total count."""

        def _close(parked: _PendingCollectorSocket, state: str) -> None:
            self._remove_pending_socket(parked)
            task = parked.sniff_task
            parked.sniff_task = None
            if task is not None and task is not asyncio.current_task():
                task.cancel()
            self._mark_session_state(parked.session_id, state)
            parked.writer.close()

        for candidate in tuple(self._pending_sockets.values()):
            if (
                candidate.parked
                and candidate is not new_pending
                and candidate.remote_ip == new_pending.remote_ip
            ):
                _close(candidate, "parked_replaced")
        parked_sockets = [
            candidate
            for candidate in self._pending_sockets.values()
            if candidate.parked and candidate is not new_pending
        ]
        while len(parked_sockets) >= self._MAX_PARKED_SOCKETS:
            _close(parked_sockets.pop(0), "parked_evicted")

    async def _park_unclaimed_pending_socket(
        self,
        pending: _PendingCollectorSocket,
        chunk: bytes,
        *,
        session_state: str,
    ) -> None:
        """Hold an ownerless collector callback open instead of dropping it.

        The already-sniffed bytes stay buffered so a later claim replays them;
        the watch loop keeps a bounded identity buffer, notices a peer close,
        and closes the socket after the TTL as a natural refresh point.
        """

        pending.initial_bytes = chunk + pending.initial_bytes
        pending.parked = True
        self._evict_parked_sockets(pending)
        if pending.session_id:
            self._pending_sockets[pending.session_id] = pending
        self._last_pending_ip = pending.remote_ip
        self._mark_session_state(pending.session_id, session_state)
        logger.debug(
            "Parked unclaimed collector callback from %s (%s)",
            pending.remote_ip,
            session_state,
        )
        await self._watch_parked_pending_socket(pending)

    def _resume_pending_watch(self, pending: _PendingCollectorSocket) -> None:
        """Re-arm the park watch after a paused sniff left the socket registered.

        A route-identity attempt pauses (cancels) the sniff task; when the
        socket turns out to belong to another collector it stays registered —
        without a watcher it would never notice a peer close, and a dead
        socket blocks same-IP routing as a phantom duplicate.
        """

        if not self._pending_socket_still_registered(pending):
            return
        if pending.sniff_task is not None and not pending.sniff_task.done():
            return
        pending.parked = True
        pending.sniff_task = _spawn_tracked_task(
            self._watch_parked_pending_socket(pending),
            name=f"collector_parked_watch_{pending.remote_ip}",
        )

    async def _watch_parked_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._PARKED_SOCKET_TTL_SECONDS
        close_state = "parked_expired"
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                data = await asyncio.wait_for(
                    pending.reader.read(256),
                    timeout=min(30.0, remaining),
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                # Claimed or listener shutdown: the socket is not ours to close.
                raise
            except Exception:
                close_state = "parked_read_failed"
                break
            if not data:
                close_state = "parked_peer_closed"
                break
            if len(pending.initial_bytes) < self._PARKED_IDENTITY_BUFFER_LIMIT:
                pending.initial_bytes = (
                    pending.initial_bytes + data
                )[: self._PARKED_IDENTITY_BUFFER_LIMIT]

        if not self._pending_socket_still_registered(pending):
            return
        self._remove_pending_socket(pending)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""
        self._mark_session_state(pending.session_id, close_state)
        await _close_writer_bounded(pending.writer)

    def _callback_ip_matches_collector(self, collector_ip: str, remote_ip: str) -> bool:
        if not collector_ip or not remote_ip:
            return False
        if remote_ip == collector_ip:
            return True
        return bool(
            _is_hairpin_alias_candidate(collector_ip, remote_ip)
            or _is_default_broadcast_alias_candidate(collector_ip, remote_ip)
        )

    def _has_owner_for_remote_ip(self, owner_counts: dict[str, int], remote_ip: str) -> bool:
        for collector_ip, count in owner_counts.items():
            if count <= 0:
                continue
            if not collector_ip:
                return True
            if self._callback_ip_matches_collector(collector_ip, remote_ip):
                return True
        return False

    def _has_owner_for_collector_pn(
        self,
        owner_counts: dict[str, int],
        collector_pn: str,
    ) -> bool:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return False
        for owner_pn, count in owner_counts.items():
            if count <= 0:
                continue
            if self._collector_pn_matches(normalized_pn, owner_pn):
                return True
        return False

    def _drop_connection_indexes_for_connection(self, connection: object) -> None:
        """Remove every listener index that still points at a disconnected connection."""

        selected_id = id(connection)
        payload_removed = False
        at_removed = False
        for mapping, is_payload in (
            (self._connections, True),
            (self._connections_by_pn, True),
            (self._session_payload_connections, True),
            (self._at_connections, False),
            (self._at_connections_by_pn, False),
            (self._session_at_connections, False),
        ):
            for key, candidate in tuple(mapping.items()):
                if id(candidate) == selected_id:
                    mapping.pop(key, None)
                    if is_payload:
                        payload_removed = True
                    else:
                        at_removed = True

        if payload_removed and not any(
            id(candidate) == selected_id for candidate in self._connections.values()
        ):
            self._last_connection_ip = ""
        if at_removed and not any(
            id(candidate) == selected_id for candidate in self._at_connections.values()
        ):
            self._last_at_connection_ip = ""

    def _connection_keys_for_collector(
        self,
        collector_ip: str,
        connections: dict[str, object],
    ) -> tuple[str, ...]:
        if not collector_ip:
            return ()

        selected_ids: set[int] = set()
        for remote_ip, connection in connections.items():
            if self._callback_ip_matches_collector(collector_ip, remote_ip):
                selected_ids.add(id(connection))

        if not selected_ids:
            return ()

        return tuple(
            remote_ip
            for remote_ip, connection in connections.items()
            if id(connection) in selected_ids
        )

    def _connection_keys_for_collector_pn(
        self,
        collector_pn: str,
        connections_by_pn: dict[str, object],
        connections: dict[str, object],
    ) -> tuple[str, ...]:
        connection = self._connection_by_collector_pn(collector_pn, connections_by_pn)
        if connection is None:
            return ()
        selected_id = id(connection)
        return tuple(
            key
            for key, candidate in connections.items()
            if id(candidate) == selected_id
        )

    def _drop_connection_pn_indexes(
        self,
        connections_by_pn: dict[str, object],
        selected_connections: list[object],
    ) -> None:
        if not selected_connections:
            return
        selected_ids = {id(connection) for connection in selected_connections}
        for collector_pn, connection in tuple(connections_by_pn.items()):
            if id(connection) in selected_ids:
                connections_by_pn.pop(collector_pn, None)

    def _drop_connection_session_indexes(
        self,
        session_connections: dict[str, object],
        selected_connections: list[object],
    ) -> None:
        if not selected_connections:
            return
        selected_ids = {id(connection) for connection in selected_connections}
        for session_id, connection in tuple(session_connections.items()):
            if id(connection) in selected_ids:
                session_connections.pop(session_id, None)

    async def _disconnect_connection_keys(
        self,
        connections: dict[str, object],
        keys: tuple[str, ...],
    ) -> None:
        if not keys:
            return

        selected_connections: list[object] = []
        seen: set[int] = set()
        for key in keys:
            connection = connections.pop(key, None)
            if connection is None:
                continue
            identity = id(connection)
            if identity in seen:
                continue
            seen.add(identity)
            selected_connections.append(connection)

        for connection in selected_connections:
            disconnect = getattr(connection, "disconnect", None)
            if callable(disconnect):
                await disconnect()
        self._drop_connection_pn_indexes(self._connections_by_pn, selected_connections)
        self._drop_connection_pn_indexes(self._at_connections_by_pn, selected_connections)
        self._drop_connection_session_indexes(
            self._session_payload_connections,
            selected_connections,
        )
        self._drop_connection_session_indexes(
            self._session_at_connections,
            selected_connections,
        )

    async def release_collector_connections(
        self,
        collector_ip: str,
        collector_pn: str = "",
        *,
        close_payload: bool = False,
        close_at: bool = False,
        close_pending: bool = False,
    ) -> None:
        if not collector_ip and not collector_pn:
            return

        if (
            close_payload
            and not collector_pn
            and self._has_owner_for_remote_ip(self._payload_owner_counts, collector_ip)
        ):
            close_payload = False
            close_pending = False
        if close_payload and self._has_owner_for_collector_pn(
            self._payload_pn_owner_counts,
            collector_pn,
        ):
            close_payload = False
            close_pending = False
        if (
            close_at
            and not collector_pn
            and self._has_owner_for_remote_ip(self._at_owner_counts, collector_ip)
        ):
            close_at = False
        if close_at and self._has_owner_for_collector_pn(
            self._at_pn_owner_counts,
            collector_pn,
        ):
            close_at = False

        if close_pending and collector_ip:
            for pending in tuple(self._pending_sockets.values()):
                remote_ip = pending.remote_ip
                if not self._callback_ip_matches_collector(collector_ip, remote_ip):
                    continue
                self._remove_pending_socket(pending)
                await self._close_pending_socket(pending)

        if close_payload:
            payload_keys = set()
            if collector_pn:
                payload_keys.update(
                    self._connection_keys_for_collector_pn(
                        collector_pn,
                        self._connections_by_pn,
                        self._connections,
                    )
                )
            if not payload_keys:
                payload_keys.update(
                    self._connection_keys_for_collector(collector_ip, self._connections)
                )
            await self._disconnect_connection_keys(
                self._connections,
                tuple(payload_keys),
            )
            if collector_ip and self._callback_ip_matches_collector(collector_ip, self._last_connection_ip):
                self._last_connection_ip = ""

        if close_at:
            at_keys = set()
            if collector_pn:
                at_keys.update(
                    self._connection_keys_for_collector_pn(
                        collector_pn,
                        self._at_connections_by_pn,
                        self._at_connections,
                    )
                )
            if not at_keys:
                at_keys.update(
                    self._connection_keys_for_collector(collector_ip, self._at_connections)
                )
            await self._disconnect_connection_keys(
                self._at_connections,
                tuple(at_keys),
            )
            if collector_ip and self._callback_ip_matches_collector(collector_ip, self._last_at_connection_ip):
                self._last_at_connection_ip = ""

        if close_pending and collector_ip and self._callback_ip_matches_collector(collector_ip, self._last_pending_ip):
            self._last_pending_ip = ""

    async def activate_pending_at_connection(
        self,
        pending: _PendingCollectorSocket,
        *,
        collector_ip: str,
        write_timeout: float,
        raw_passthrough_bootstrap: str = "",
        raw_passthrough_frame_format: str = "",
        raw_passthrough_min_interval_ms: int = 0,
    ) -> _CollectorAtConnection:
        remote_ip = pending.remote_ip
        connection = self._at_connections.get(remote_ip)
        if connection is None:
            connection = self._resolve_public_placeholder_alias(
                remote_ip,
                connections=self._at_connections,
            )
        if connection is None:
            connection = _CollectorAtConnection(
                remote_ip_hint=remote_ip,
                write_timeout=write_timeout,
                raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                raw_passthrough_frame_format=raw_passthrough_frame_format,
                raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
            )
        else:
            connection.set_write_timeout(write_timeout)
            connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
            connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
            connection.set_raw_passthrough_min_interval_ms(raw_passthrough_min_interval_ms)

        self._at_connections[remote_ip] = connection
        if collector_ip and collector_ip not in self._at_connections:
            self._at_connections[collector_ip] = connection
        if pending.session_id:
            self._session_at_connections[pending.session_id] = connection
        self._last_at_connection_ip = remote_ip
        self._mark_session_state(pending.session_id, "routed_at_text")
        initial_bytes = pending.initial_bytes
        pending.initial_bytes = b""
        _spawn_tracked_task(
            connection.run(
                pending.reader,
                pending.writer,
                initial_bytes=initial_bytes,
                session_id=pending.session_id,
                session_identity_callback=self._mark_session_identity,
                disconnect_callback=self._drop_connection_indexes_for_connection,
            ),
            name=f"collector_at_{remote_ip}",
        )
        await connection.wait_until_connected(timeout=0.1)
        return connection

    async def activate_pending_connection(
        self,
        pending: _PendingCollectorSocket,
        *,
        collector_ip: str,
        heartbeat_interval: float,
        write_timeout: float,
    ) -> _CollectorConnection:
        remote_ip = pending.remote_ip
        connection = self._connections.get(remote_ip)
        if connection is None:
            connection = self._resolve_public_placeholder_alias(remote_ip)
        if connection is None:
            connection = _CollectorConnection(
                remote_ip_hint=remote_ip,
                heartbeat_interval=heartbeat_interval,
                write_timeout=write_timeout,
            )
        else:
            connection.set_heartbeat_interval(heartbeat_interval)
            connection.set_write_timeout(write_timeout)

        self._connections[remote_ip] = connection
        if collector_ip and collector_ip not in self._connections:
            self._connections[collector_ip] = connection
        if pending.session_id:
            self._session_payload_connections[pending.session_id] = connection
        self._last_connection_ip = remote_ip
        self._mark_session_state(pending.session_id, "routed_framed")
        initial_bytes = pending.initial_bytes
        pending.initial_bytes = b""
        _spawn_tracked_task(
            connection.run(
                pending.reader,
                pending.writer,
                initial_bytes=initial_bytes,
                session_id=pending.session_id,
                session_identity_callback=self._mark_session_identity,
                disconnect_callback=self._drop_connection_indexes_for_connection,
            ),
            name=f"collector_framed_{remote_ip}",
        )
        await connection.wait_until_connected(timeout=0.1)
        return connection

    async def _read_pending_initial_chunk(
        self,
        pending: _PendingCollectorSocket,
    ) -> tuple[bytes, bool]:
        """Return initial bytes and whether the socket is exhausted/closed."""

        if pending.initial_bytes:
            chunk = pending.initial_bytes
            pending.initial_bytes = b""
            return chunk, False

        try:
            chunk = await asyncio.wait_for(pending.reader.read(64), timeout=0.25)
        except asyncio.TimeoutError:
            chunk = await self._async_probe_pending_identity(pending)
            return chunk, False
        except Exception:
            return b"", True
        return chunk, not chunk

    async def _async_probe_pending_identity(self, pending: _PendingCollectorSocket) -> bytes:
        session_protocol = self._single_registered_session_protocol()
        probe = _identity_probe_payload_for_session_protocol(session_protocol)
        if not probe:
            self._mark_session_state(pending.session_id, "waiting_for_identity")
            return b""

        self._mark_session_state(pending.session_id, f"probing_identity_{session_protocol}")
        try:
            pending.writer.write(probe)
            await asyncio.wait_for(pending.writer.drain(), timeout=1.5)
            return await asyncio.wait_for(pending.reader.read(64), timeout=1.5)
        except asyncio.TimeoutError:
            self._mark_session_state(pending.session_id, "identity_probe_timeout")
            return b""
        except Exception:
            self._mark_session_state(pending.session_id, "identity_probe_failed")
            return b""

    async def _identify_pending_socket_for_route(
        self,
        pending: _PendingCollectorSocket,
        *,
        session_protocol: str = "",
    ) -> str:
        if not self._pending_socket_still_registered(pending):
            return ""

        entry = self._session_inventory.get(pending.session_id)
        known_pn = str(getattr(entry, "collector_pn", "") or "").strip()
        if known_pn:
            return known_pn

        try:
            chunk = await asyncio.wait_for(pending.reader.read(64), timeout=0.25)
        except asyncio.TimeoutError:
            chunk = b""
        except Exception:
            self._mark_session_state(pending.session_id, "route_identity_read_failed")
            return ""

        if chunk:
            pending.initial_bytes += chunk
            self._mark_session_first_bytes(pending.session_id, chunk)
            collector_pn, source = _collector_pn_from_initial_chunk(chunk)
            if collector_pn:
                self._mark_session_identity(pending.session_id, collector_pn, source)
                return collector_pn
            return ""

        protocol = str(session_protocol or "").strip().lower()
        if not protocol:
            protocol = self._single_registered_session_protocol()
        probe = _identity_probe_payload_for_session_protocol(protocol)
        if not probe:
            return ""

        self._mark_session_state(pending.session_id, f"probing_route_identity_{protocol}")
        try:
            pending.writer.write(probe)
            await asyncio.wait_for(pending.writer.drain(), timeout=1.5)
            response = await asyncio.wait_for(pending.reader.read(64), timeout=1.5)
        except asyncio.TimeoutError:
            self._mark_session_state(pending.session_id, "route_identity_probe_timeout")
            return ""
        except Exception:
            self._mark_session_state(pending.session_id, "route_identity_probe_failed")
            return ""

        collector_pn, source = _collector_pn_from_initial_chunk(response)
        if collector_pn:
            self._mark_session_first_bytes(pending.session_id, response)
            self._mark_session_identity(pending.session_id, collector_pn, source)
            return collector_pn
        return ""

    async def _sniff_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        chunk, exhausted = await self._read_pending_initial_chunk(pending)

        if not self._pending_socket_still_registered(pending):
            return

        if not chunk:
            if not exhausted:
                # No identity yet, but the socket must stay WATCHED: an
                # unwatched registered socket never notices a peer close, and
                # a dead entry blocks same-IP routing as a phantom duplicate.
                await self._park_unclaimed_pending_socket(
                    pending,
                    b"",
                    session_state="parked_waiting_for_identity",
                )
                return
            self._remove_pending_socket(pending)
            if self._last_pending_ip == pending.remote_ip:
                self._last_pending_ip = ""
            self._mark_session_state(pending.session_id, "closed_no_payload")
            await _close_writer_bounded(pending.writer)
            return

        self._remove_pending_socket(pending)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""

        self._mark_session_first_bytes(pending.session_id, chunk)
        initial_pn, initial_pn_source = _collector_pn_from_initial_chunk(chunk)
        if initial_pn:
            self._mark_session_identity(
                pending.session_id,
                initial_pn,
                initial_pn_source,
            )

        route_at = _looks_like_at_traffic(chunk)
        # The byte-shape guess must not overrule a registered owner: the entry
        # that owns this collector (by PN, or by IP when no PN was seen)
        # already knows its session protocol.  A framed frame can begin with
        # the bytes "AT+", and an at_text collector in raw-passthrough mode
        # can open with non-AT bytes — shape only decides when ownership is
        # absent or ambiguous.
        framed_owner = self._has_owner_for_collector_pn(
            self._payload_pn_owner_counts, initial_pn
        ) or (
            not initial_pn
            and self._has_owner_for_remote_ip(
                self._payload_owner_counts, pending.remote_ip
            )
        )
        at_owner = self._has_owner_for_collector_pn(
            self._at_pn_owner_counts, initial_pn
        ) or (
            not initial_pn
            and self._has_owner_for_remote_ip(self._at_owner_counts, pending.remote_ip)
        )
        if route_at and framed_owner and not at_owner:
            logger.debug(
                "Sniffed AT-shaped bytes from %s but a framed owner is registered; routing framed",
                pending.remote_ip,
            )
            route_at = False
        elif not route_at and at_owner and not framed_owner:
            logger.debug(
                "Sniffed non-AT bytes from %s but an AT owner is registered; routing at_text",
                pending.remote_ip,
            )
            route_at = True

        if route_at:
            connection = None
            if initial_pn:
                connection = self._connection_by_collector_pn(
                    initial_pn,
                    self._at_connections_by_pn,
                )
            if connection is None and not initial_pn:
                connection = self._at_connections.get(pending.remote_ip)
            if connection is None and not initial_pn:
                connection = self._resolve_public_placeholder_alias(
                    pending.remote_ip,
                    connections=self._at_connections,
                )
            if connection is None:
                has_ip_owner = self._has_owner_for_remote_ip(
                    self._at_owner_counts,
                    pending.remote_ip,
                )
                has_pn_owner = self._has_owner_for_collector_pn(
                    self._at_pn_owner_counts,
                    initial_pn,
                )
                if not has_ip_owner and not has_pn_owner:
                    await self._park_unclaimed_pending_socket(
                        pending,
                        chunk,
                        session_state="parked_no_at_owner",
                    )
                    return
                connection = _CollectorAtConnection(
                    remote_ip_hint=pending.remote_ip,
                    write_timeout=1.5,
                )
            else:
                connection.set_write_timeout(1.5)
            self._at_connections[pending.remote_ip] = connection
            if pending.session_id:
                self._session_at_connections[pending.session_id] = connection
                if initial_pn:
                    self._mark_session_identity(
                        pending.session_id,
                        initial_pn,
                        initial_pn_source,
                    )
            self._last_at_connection_ip = pending.remote_ip
            self._mark_session_state(pending.session_id, "routed_at_text")
            await connection.run(
                pending.reader,
                pending.writer,
                initial_bytes=chunk,
                session_id=pending.session_id,
                session_identity_callback=self._mark_session_identity,
                disconnect_callback=self._drop_connection_indexes_for_connection,
            )
            return

        connection = None
        if initial_pn:
            connection = self._connection_by_collector_pn(
                initial_pn,
                self._connections_by_pn,
            )
        if connection is None and not initial_pn:
            connection = self._connections.get(pending.remote_ip)
        if connection is None and not initial_pn:
            connection = self._resolve_public_placeholder_alias(pending.remote_ip)
        if connection is None:
            has_ip_owner = self._has_owner_for_remote_ip(
                self._payload_owner_counts,
                pending.remote_ip,
            )
            has_pn_owner = self._has_owner_for_collector_pn(
                self._payload_pn_owner_counts,
                initial_pn,
            )
            if not has_ip_owner and not has_pn_owner:
                await self._park_unclaimed_pending_socket(
                    pending,
                    chunk,
                    session_state="parked_no_payload_owner",
                )
                return
            connection = _CollectorConnection(
                remote_ip_hint=pending.remote_ip,
                heartbeat_interval=60.0,
                write_timeout=1.5,
            )
        else:
            connection.set_heartbeat_interval(60.0)
            connection.set_write_timeout(1.5)
        self._connections[pending.remote_ip] = connection
        if pending.session_id:
            self._session_payload_connections[pending.session_id] = connection
            if initial_pn:
                self._mark_session_identity(
                    pending.session_id,
                    initial_pn,
                    initial_pn_source,
                )
        self._last_connection_ip = pending.remote_ip
        self._mark_session_state(pending.session_id, "routed_framed")
        await connection.run(
            pending.reader,
            pending.writer,
            initial_bytes=chunk,
            session_id=pending.session_id,
            session_identity_callback=self._mark_session_identity,
            disconnect_callback=self._drop_connection_indexes_for_connection,
        )

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername") or ("", None)
        remote_ip = peer[0] or ""
        remote_port = peer[1]
        if not remote_ip:
            await _close_writer_bounded(writer)
            return

        session_id = self._next_session_id()
        self._remember_session(
            session_id=session_id,
            remote_ip=remote_ip,
            remote_port=remote_port if isinstance(remote_port, int) else None,
        )
        pending = _PendingCollectorSocket(
            session_id=session_id,
            remote_ip=remote_ip,
            remote_port=remote_port if isinstance(remote_port, int) else None,
            reader=reader,
            writer=writer,
        )
        self._pending_sockets[session_id] = pending
        self._last_pending_ip = remote_ip
        pending.sniff_task = _spawn_tracked_task(
            self._sniff_pending_socket(pending),
            name=f"collector_pending_sniff_{remote_ip}",
        )
        self._notify_connection_watchers(remote_ip)


_LISTENERS: dict[tuple[str, int], _SharedEybondListener] = {}
_LISTENERS_LOCK = asyncio.Lock()
_WILDCARD_BIND_HOSTS = ("0.0.0.0", "")


def _resolve_registered_listener(host: str, port: int) -> _SharedEybondListener | None:
    """Return a registered listener that already serves ``host:port``.

    A wildcard listener (bound on 0.0.0.0) accepts connections for every local
    address, so a request for a specific host on the same port must REUSE it:
    binding the specific address while the wildcard socket holds the port fails
    with EADDRINUSE. The runtime binds its callback listener on 0.0.0.0 while
    options-flow helpers historically ask for the entry's server IP — without
    this fallback those helpers cannot run while the runtime is up (the
    collector Wi-Fi change regression).
    """

    listener = _LISTENERS.get((host, int(port)))
    if listener is not None:
        return listener
    if host not in _WILDCARD_BIND_HOSTS:
        for wildcard in _WILDCARD_BIND_HOSTS:
            listener = _LISTENERS.get((wildcard, int(port)))
            if listener is not None:
                return listener
    return None


async def _acquire_listener_locked(host: str, port: int) -> _SharedEybondListener:
    """Get-or-create + acquire one shared listener. Caller holds _LISTENERS_LOCK."""

    listener = _resolve_registered_listener(host, port)
    if listener is None:
        listener = _SharedEybondListener(host=host, port=port)
        _LISTENERS[(host, int(port))] = listener
    try:
        await listener.acquire()
    except Exception:
        if listener._server is None and listener._ref_count == 0:
            _LISTENERS.pop((listener._host, listener._port), None)
        raise
    return listener


async def _acquire_shared_listener(host: str, port: int) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        return await _acquire_listener_locked(host, port)


async def _acquire_shared_payload_listener(
    host: str,
    port: int,
    collector_ip: str,
    collector_pn: str = "",
    collector_session_protocol: str = "",
) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        listener = await _acquire_listener_locked(host, port)
        listener.register_payload_owner(collector_ip)
        listener.register_payload_pn_owner(collector_pn)
        listener.register_session_protocol_owner(collector_session_protocol)
        return listener


async def _acquire_shared_at_listener(
    host: str,
    port: int,
    collector_ip: str,
    collector_pn: str = "",
    collector_session_protocol: str = "",
) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        listener = await _acquire_listener_locked(host, port)
        listener.register_at_owner(collector_ip)
        listener.register_at_pn_owner(collector_pn)
        listener.register_session_protocol_owner(collector_session_protocol)
        return listener


async def _release_shared_listener(
    listener: _SharedEybondListener,
    *,
    collector_ip: str = "",
    collector_pn: str = "",
    collector_session_protocol: str = "",
    close_payload: bool = False,
    close_at: bool = False,
    close_pending: bool = False,
    unregister_payload_owner: bool = False,
    unregister_payload_pn_owner: bool = False,
    unregister_at_owner: bool = False,
    unregister_at_pn_owner: bool = False,
    unregister_session_protocol_owner: bool = False,
) -> None:
    async def _release() -> None:
        async with _LISTENERS_LOCK:
            key = (listener._host, listener._port)
            if unregister_payload_owner:
                listener.unregister_payload_owner(collector_ip)
            if unregister_payload_pn_owner:
                listener.unregister_payload_pn_owner(collector_pn)
            if unregister_at_owner:
                listener.unregister_at_owner(collector_ip)
            if unregister_at_pn_owner:
                listener.unregister_at_pn_owner(collector_pn)
            if unregister_session_protocol_owner:
                listener.unregister_session_protocol_owner(collector_session_protocol)
            await listener.release_collector_connections(
                collector_ip,
                collector_pn,
                close_payload=close_payload,
                close_at=close_at,
                close_pending=close_pending,
            )
            closed = await listener.release()
            if closed:
                _LISTENERS.pop(key, None)

    await _finish_cleanup_on_cancel(_release())


class SharedProxyCaptureRoute:
    """Route one collector callback accepted by the shared listener into a proxy handler."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
    ) -> None:
        self._host = str(host)
        self._port = int(port)
        self._collector_ip = str(collector_ip or "").strip()
        self._collector_pn = str(collector_pn or "").strip()
        self._collector_session_protocol = str(collector_session_protocol or "").strip().lower()
        self._handler = handler
        self._listener: _SharedEybondListener | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_listener(self._host, self._port)
        self._running = True
        self._task = asyncio.create_task(
            self._route_loop(),
            name=f"shared_proxy_capture_route_{self._collector_ip or self._port}",
        )

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        listener = self._listener
        self._listener = None
        if listener is not None:
            await _release_shared_listener(listener)

    async def _route_loop(self) -> None:
        try:
            while self._running:
                listener = self._listener
                if listener is None:
                    return
                pending = await listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                )
                if pending is None:
                    await asyncio.sleep(0.1)
                    continue
                reader, pump_task = _reader_with_initial_bytes(
                    pending.initial_bytes,
                    pending.reader,
                )
                pending.initial_bytes = b""
                try:
                    await self._handler(reader, pending.writer)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A handler crash must not kill the route loop and leave
                    # the claimed socket dangling open.
                    logger.exception(
                        "Proxy capture handler failed for %s; closing the claimed socket",
                        pending.remote_ip,
                    )
                    await _close_writer_bounded(pending.writer)
                finally:
                    if pump_task is not None:
                        pump_task.cancel()
                        try:
                            await pump_task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
        except asyncio.CancelledError:
            raise


def _reader_with_initial_bytes(
    initial_bytes: bytes,
    source: asyncio.StreamReader,
) -> tuple[asyncio.StreamReader, asyncio.Task[None] | None]:
    prefix = bytes(initial_bytes or b"")
    if not prefix:
        return source, None

    replay = asyncio.StreamReader()
    replay.feed_data(prefix)

    async def _pump() -> None:
        try:
            while True:
                chunk = await source.read(4096)
                if not chunk:
                    replay.feed_eof()
                    return
                replay.feed_data(chunk)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            replay.set_exception(exc)

    return replay, asyncio.create_task(_pump(), name="collector_proxy_replay_reader")


class SharedEybondTransport:
    """One per-entry transport facade backed by a shared TCP listener."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        request_timeout: float,
        heartbeat_interval: float,
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        collector_identity_strategy: str = "",
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._request_timeout = request_timeout
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._collector_ip = collector_ip
        self._collector_pn = str(collector_pn or "").strip()
        self._collector_session_protocol = str(collector_session_protocol or "").strip().lower()
        self._collector_identity_strategy = str(collector_identity_strategy or "").strip().lower()
        self._collector_raw_passthrough_bootstrap = (
            str(collector_raw_passthrough_bootstrap or "").strip().lower()
        )
        self._collector_raw_passthrough_frame_format = (
            str(collector_raw_passthrough_frame_format or "").strip().lower()
        )
        self._collector_raw_passthrough_min_interval_ms = max(
            0,
            int(collector_raw_passthrough_min_interval_ms or 0),
        )
        self._listener: _SharedEybondListener | None = None
        self._connection_watcher_callback: Callable[[str], None] | None = None
        self._connection_watcher_token: int | None = None

    def set_connection_watcher(self, callback: Callable[[str], None] | None) -> None:
        """Fire ``callback(remote_ip)`` whenever this collector dials back in.

        Registered on the shared listener once the transport starts; safe to
        call before ``start()``.
        """

        if self._listener is not None and self._connection_watcher_token is not None:
            self._listener.remove_connection_watcher(self._connection_watcher_token)
            self._connection_watcher_token = None
        self._connection_watcher_callback = callback
        if callback is not None and self._listener is not None:
            self._connection_watcher_token = self._listener.add_connection_watcher(
                self._collector_ip,
                callback,
            )

    @property
    def connected(self) -> bool:
        connection = self._connection(create_placeholder=False)
        return connection.connected if connection is not None else False

    @property
    def collector_info(self) -> CollectorInfo:
        connection = self._connection(create_placeholder=False)
        if connection is not None:
            return connection.collector_info
        return _copy_collector_info(
            CollectorInfo(remote_ip=self._collector_ip, collector_pn=self._collector_pn)
        )

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_payload_listener(
            self._host,
            self._port,
            self._collector_ip,
            self._collector_pn,
            self._collector_session_protocol,
        )
        if self._connection_watcher_callback is not None and self._connection_watcher_token is None:
            self._connection_watcher_token = self._listener.add_connection_watcher(
                self._collector_ip,
                self._connection_watcher_callback,
            )
        self._connection(create_placeholder=bool(self._collector_ip))

    async def stop(self) -> None:
        if self._listener is None:
            return
        listener = self._listener
        self._listener = None
        if self._connection_watcher_token is not None:
            listener.remove_connection_watcher(self._connection_watcher_token)
            self._connection_watcher_token = None
        await _release_shared_listener(
            listener,
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            collector_session_protocol=self._collector_session_protocol,
            close_payload=True,
            close_pending=True,
            unregister_payload_owner=True,
            unregister_payload_pn_owner=True,
            unregister_session_protocol_owner=True,
        )

    async def async_snapshot_shared_connection(self) -> _CollectorConnection | None:
        if not self._collector_ip and not self._collector_pn:
            return None
        async with _LISTENERS_LOCK:
            listener = _LISTENERS.get((self._host, self._port))
            if listener is None:
                return None
            if self._collector_ip:
                connection = listener._connections.get(self._collector_ip)
            else:
                connection = listener._connection_by_collector_pn(
                    self._collector_pn,
                    listener._connections_by_pn,
                )
            if connection is None or not connection.connected:
                return None
            return connection

    def session_inventory_diagnostics(self) -> dict[str, object]:
        if self._listener is None:
            return {
                "pending_session_count": 0,
                "recent_session_count": 0,
                "duplicate_peer_ip_count": 0,
                "duplicate_peer_ips": [],
                "sessions": [],
            }
        return self._listener.session_inventory_diagnostics()

    async def async_disconnect_if_new_shared_connection(
        self,
        snapshot: _CollectorConnection | None,
    ) -> None:
        if not self._collector_ip and not self._collector_pn:
            return
        async with _LISTENERS_LOCK:
            listener = _LISTENERS.get((self._host, self._port))
            if listener is None:
                return
            if self._collector_ip:
                connection = listener._connections.get(self._collector_ip)
            else:
                connection = listener._connection_by_collector_pn(
                    self._collector_pn,
                    listener._connections_by_pn,
                )
        if connection is None or connection is snapshot:
            return
        await connection.disconnect()

    async def disconnect(self) -> None:
        connection = self._connection(create_placeholder=False)
        if connection is None:
            return
        await connection.disconnect()

    def set_collector_ip(self, collector_ip: str) -> None:
        self._collector_ip = collector_ip
        self._connection(create_placeholder=bool(self._collector_ip))

    async def wait_until_connected(self, timeout: float) -> bool:
        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            connection = self._connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                return True

            if self._collector_ip or self._collector_pn:
                pending = await self._listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                )
                if pending is not None:
                    connection = await self._listener.activate_pending_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        heartbeat_interval=self._heartbeat_interval,
                        write_timeout=self._write_timeout,
                    )
                    if connection.connected:
                        return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            if connection is not None and self._collector_ip:
                ok = await connection.wait_until_connected(timeout=min(0.1, remaining))
                if ok:
                    return True
                continue

            await asyncio.sleep(min(0.1, remaining))

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            connection = self._connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False
                return await connection.wait_until_heartbeat(timeout=remaining)

            if self._collector_ip or self._collector_pn:
                pending = await self._listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                )
                if pending is not None:
                    connection = await self._listener.activate_pending_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        heartbeat_interval=self._heartbeat_interval,
                        write_timeout=self._write_timeout,
                    )
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        return False
                    return await connection.wait_until_heartbeat(timeout=remaining)

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        connection = await self._active_connection_for_send()
        return await connection.async_send_forward(
            payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=self._request_timeout,
        )

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
    ) -> bytes:
        if not isinstance(route, EybondLinkRoute):
            raise TypeError(f"unsupported_link_route:{route.family}")
        return await self.async_send_forward(
            payload,
            devcode=route.devcode,
            collector_addr=route.collector_addr,
        )

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
        connection = await self._active_connection_for_send()
        return await connection.async_send_collector(
            fcode=fcode,
            payload=payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=self._request_timeout,
        )

    async def _active_connection_for_send(self) -> _CollectorConnection:
        connection = self._connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return connection

        if self._listener is None:
            raise ConnectionError("collector_not_connected")

        if self._collector_ip or self._collector_pn:
            pending = await self._listener.pop_pending_socket_for_route(
                collector_ip=self._collector_ip,
                collector_pn=self._collector_pn,
                session_protocol=self._collector_session_protocol,
            )
            if pending is not None:
                return await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    heartbeat_interval=self._heartbeat_interval,
                    write_timeout=self._write_timeout,
                )

        if connection is None or not connection.connected:
            raise ConnectionError("collector_not_connected")

        return connection

    def _connection(self, *, create_placeholder: bool) -> _CollectorConnection | None:
        if self._listener is None:
            return None
        if self._collector_pn:
            connection = self._listener.ensure_connection(
                "",
                self._heartbeat_interval,
                self._write_timeout,
                self._collector_pn,
            )
            if connection is not None:
                return connection
        if create_placeholder:
            return self._listener.ensure_connection(
                self._collector_ip,
                self._heartbeat_interval,
                self._write_timeout,
                self._collector_pn,
            )
        if self._collector_ip:
            return self._listener.ensure_connection(
                self._collector_ip,
                self._heartbeat_interval,
                self._write_timeout,
                self._collector_pn,
            )
        return self._listener.current_connection(
            heartbeat_interval=self._heartbeat_interval,
            write_timeout=self._write_timeout,
        )


class SharedCollectorAtTransport:
    """One per-entry plain-AT transport facade backed by the shared TCP listener."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        request_timeout: float,
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        collector_identity_strategy: str = "",
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._request_timeout = float(request_timeout)
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._collector_ip = collector_ip
        self._collector_pn = str(collector_pn or "").strip()
        self._collector_session_protocol = str(collector_session_protocol or "").strip().lower()
        self._collector_identity_strategy = str(collector_identity_strategy or "").strip().lower()
        self._collector_raw_passthrough_bootstrap = (
            str(collector_raw_passthrough_bootstrap or "").strip().lower()
        )
        self._collector_raw_passthrough_frame_format = (
            str(collector_raw_passthrough_frame_format or "").strip().lower()
        )
        self._collector_raw_passthrough_min_interval_ms = max(
            0,
            int(collector_raw_passthrough_min_interval_ms or 0),
        )
        self._listener: _SharedEybondListener | None = None

    @property
    def connected(self) -> bool:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return True

        connection = self._at_connection(create_placeholder=False)
        return connection.connected if connection is not None else False

    @property
    def collector_info(self) -> CollectorInfo:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return framed.collector_info

        connection = self._at_connection(create_placeholder=False)
        if connection is not None:
            return connection.collector_info
        if self._listener is not None:
            pending = self._listener._select_pending_socket(self._collector_ip)
            if pending is not None:
                return _copy_collector_info(CollectorInfo(remote_ip=pending.remote_ip))
        return _copy_collector_info(
            CollectorInfo(remote_ip=self._collector_ip, collector_pn=self._collector_pn)
        )

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_at_listener(
            self._host,
            self._port,
            self._collector_ip,
            self._collector_pn,
            self._collector_session_protocol,
        )
        self._at_connection(create_placeholder=bool(self._collector_ip))

    async def stop(self) -> None:
        if self._listener is None:
            return
        listener = self._listener
        self._listener = None
        await _release_shared_listener(
            listener,
            collector_ip=self._collector_ip,
            collector_pn=self._collector_pn,
            collector_session_protocol=self._collector_session_protocol,
            close_at=True,
            unregister_at_owner=True,
            unregister_at_pn_owner=True,
            unregister_session_protocol_owner=True,
        )

    async def disconnect(self) -> None:
        connection = self._at_connection(create_placeholder=False)
        if connection is not None:
            await connection.disconnect()

    async def wait_until_connected(self, timeout: float) -> bool:
        if self._listener is None:
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if not self._uses_at_text_session():
                framed = self._framed_connection(create_placeholder=False)
                if framed is not None and framed.connected:
                    return True

            connection = self._at_connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                return True

            if self._collector_ip or self._collector_pn:
                pending = await self._listener.pop_pending_socket_for_route(
                    collector_ip=self._collector_ip,
                    collector_pn=self._collector_pn,
                    session_protocol=self._collector_session_protocol,
                )
                if pending is not None:
                    if self._uses_at_text_session():
                        connection = await self._listener.activate_pending_at_connection(
                            pending,
                            collector_ip=self._collector_ip,
                            write_timeout=self._write_timeout,
                            raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                            raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                            raw_passthrough_min_interval_ms=(
                                self._collector_raw_passthrough_min_interval_ms
                            ),
                        )
                        if connection.connected:
                            return True
                    else:
                        framed = await self._listener.activate_pending_connection(
                            pending,
                            collector_ip=self._collector_ip,
                            heartbeat_interval=60.0,
                            write_timeout=self._write_timeout,
                        )
                        if framed.connected:
                            return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    async def async_query(self, command: str) -> CollectorAtResponse:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return await framed.async_query(command, request_timeout=self._request_timeout)

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_query(command, request_timeout=self._request_timeout)

        if self._listener is None:
            raise ConnectionError("collector_not_connected")

        if self._collector_ip or self._collector_pn:
            pending = await self._listener.pop_pending_socket_for_route(
                collector_ip=self._collector_ip,
                collector_pn=self._collector_pn,
                session_protocol=self._collector_session_protocol,
            )
            if pending is not None:
                if self._uses_at_text_session():
                    connection = await self._listener.activate_pending_at_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        write_timeout=self._write_timeout,
                        raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                        raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                        raw_passthrough_min_interval_ms=(
                            self._collector_raw_passthrough_min_interval_ms
                        ),
                    )
                    return await connection.async_query(command, request_timeout=self._request_timeout)
                framed = await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    heartbeat_interval=60.0,
                    write_timeout=self._write_timeout,
                )
                return await framed.async_query(command, request_timeout=self._request_timeout)

        if connection is None:
            raise ConnectionError("collector_not_connected")

        if not connection.connected:
            raise ConnectionError("collector_not_connected")

        return await connection.async_query(command, request_timeout=self._request_timeout)

    async def async_write(self, command: str, value: str) -> CollectorAtResponse:
        if not self._uses_at_text_session():
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return await framed.async_write(
                    command,
                    value,
                    request_timeout=self._request_timeout,
                )

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_write(
                command,
                value,
                request_timeout=self._request_timeout,
            )

        if self._listener is None:
            raise ConnectionError("collector_not_connected")

        if self._collector_ip or self._collector_pn:
            pending = await self._listener.pop_pending_socket_for_route(
                collector_ip=self._collector_ip,
                collector_pn=self._collector_pn,
                session_protocol=self._collector_session_protocol,
            )
            if pending is not None:
                if self._uses_at_text_session():
                    connection = await self._listener.activate_pending_at_connection(
                        pending,
                        collector_ip=self._collector_ip,
                        write_timeout=self._write_timeout,
                        raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                        raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                        raw_passthrough_min_interval_ms=(
                            self._collector_raw_passthrough_min_interval_ms
                        ),
                    )
                    return await connection.async_write(
                        command,
                        value,
                        request_timeout=self._request_timeout,
                    )
                framed = await self._listener.activate_pending_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    heartbeat_interval=60.0,
                    write_timeout=self._write_timeout,
                )
                return await framed.async_write(
                    command,
                    value,
                    request_timeout=self._request_timeout,
                )

        if connection is None:
            raise ConnectionError("collector_not_connected")

        if not connection.connected:
            raise ConnectionError("collector_not_connected")

        return await connection.async_write(
            command,
            value,
            request_timeout=self._request_timeout,
        )

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route: LinkRoute,
        request_timeout: float | None = None,
    ) -> bytes:
        """Send one raw inverter payload over the active AT stream."""

        if not isinstance(route, RawSerialLinkRoute):
            raise TypeError(f"unsupported_link_route:{route.family}")
        if not self._uses_at_text_session():
            raise TypeError("raw_serial_route_requires_at_text_session")
        effective_request_timeout = (
            float(request_timeout)
            if request_timeout is not None
            else self._request_timeout
        )

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_send_raw_payload(
                payload,
                request_timeout=effective_request_timeout,
            )

        if self._listener is None:
            raise ConnectionError("collector_not_connected")

        if self._collector_ip or self._collector_pn:
            pending = await self._listener.pop_pending_socket_for_route(
                collector_ip=self._collector_ip,
                collector_pn=self._collector_pn,
                session_protocol=self._collector_session_protocol,
            )
            if pending is not None:
                connection = await self._listener.activate_pending_at_connection(
                    pending,
                    collector_ip=self._collector_ip,
                    write_timeout=self._write_timeout,
                    raw_passthrough_bootstrap=self._collector_raw_passthrough_bootstrap,
                    raw_passthrough_frame_format=self._collector_raw_passthrough_frame_format,
                    raw_passthrough_min_interval_ms=(
                        self._collector_raw_passthrough_min_interval_ms
                    ),
                )
                return await connection.async_send_raw_payload(
                    payload,
                    request_timeout=effective_request_timeout,
                )

        raise ConnectionError("collector_not_connected")

    def select_payload_route(
        self,
        route: LinkRoute,
        *,
        payload_family: str = "",
    ) -> LinkRoute:
        if self._uses_at_text_session():
            return RawSerialLinkRoute(protocol=payload_family)
        return route

    def _uses_at_text_session(self) -> bool:
        return self._collector_session_protocol == "at_text"

    def _at_connection(self, *, create_placeholder: bool) -> _CollectorAtConnection | None:
        if self._listener is None:
            return None
        if self._collector_pn:
            connection = self._listener.ensure_at_connection(
                "",
                self._write_timeout,
                self._collector_pn,
                self._collector_raw_passthrough_bootstrap,
                self._collector_raw_passthrough_frame_format,
                self._collector_raw_passthrough_min_interval_ms,
            )
            if connection is not None:
                return connection
        if create_placeholder:
            return self._listener.ensure_at_connection(
                self._collector_ip,
                self._write_timeout,
                self._collector_pn,
                self._collector_raw_passthrough_bootstrap,
                self._collector_raw_passthrough_frame_format,
                self._collector_raw_passthrough_min_interval_ms,
            )
        if self._collector_ip:
            return self._listener.ensure_at_connection(
                self._collector_ip,
                self._write_timeout,
                self._collector_pn,
                self._collector_raw_passthrough_bootstrap,
                self._collector_raw_passthrough_frame_format,
                self._collector_raw_passthrough_min_interval_ms,
            )
        connection = self._listener.current_at_connection(write_timeout=self._write_timeout)
        if connection is not None:
            connection.set_raw_passthrough_bootstrap(self._collector_raw_passthrough_bootstrap)
            connection.set_raw_passthrough_frame_format(self._collector_raw_passthrough_frame_format)
            connection.set_raw_passthrough_min_interval_ms(
                self._collector_raw_passthrough_min_interval_ms
            )
        return connection

    def _framed_connection(self, *, create_placeholder: bool) -> _CollectorConnection | None:
        if self._listener is None:
            return None
        if self._collector_pn:
            connection = self._listener.ensure_connection(
                "",
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
                collector_pn=self._collector_pn,
            )
            if connection is not None:
                return connection
        if create_placeholder:
            return self._listener.ensure_connection(
                self._collector_ip,
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
                collector_pn=self._collector_pn,
            )
        if self._collector_ip:
            return self._listener.ensure_connection(
                self._collector_ip,
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
                collector_pn=self._collector_pn,
            )
        return self._listener.current_connection(
            heartbeat_interval=60.0,
            write_timeout=self._write_timeout,
        )
