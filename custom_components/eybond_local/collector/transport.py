"""Shared collector transport primitives for single- and multi-collector listeners."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable, Protocol

from .at import CollectorAtResponse, build_at_query, parse_at_response
from ..link_models import EybondLinkRoute, LinkRoute
from ..link_transport import PayloadLinkTransport
from ..models import CollectorInfo
from .profile import apply_collector_profile
from .protocol import (
    EybondHeader,
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    HEADER_SIZE,
    TIDCounter,
    build_collector_request,
    build_heartbeat_request,
    decode_header,
    parse_heartbeat_pn,
)

logger = logging.getLogger(__name__)


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
            smartess_collector_version=collector.smartess_collector_version,
            smartess_protocol_raw_id=collector.smartess_protocol_raw_id,
            smartess_protocol_asset_id=collector.smartess_protocol_asset_id,
            smartess_protocol_asset_name=collector.smartess_protocol_asset_name,
            smartess_protocol_suffix=collector.smartess_protocol_suffix,
            smartess_protocol_profile_key=collector.smartess_protocol_profile_key,
            smartess_protocol_name=collector.smartess_protocol_name,
            smartess_device_address=collector.smartess_device_address,
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

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

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

    async def run(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initial_bytes: bytes = b"",
    ) -> None:
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
        self._connected.set()

        logger.info("Collector connected from %s:%s", self._collector.remote_ip, self._collector.remote_port)

        current_task = asyncio.current_task()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name=f"eybond_heartbeat_{self._collector.remote_ip}")
        prefixed_reader = _PrefixedAsyncReader(reader, initial_bytes)
        self._reader_task = asyncio.create_task(self._read_loop(prefixed_reader), name=f"eybond_reader_{self._collector.remote_ip}")
        try:
            await self._reader_task
        finally:
            await self._disconnect(skip_task=current_task)

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
        elif response.command == "FWVER" and response.value:
            self._collector.smartess_collector_version = response.value

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
                    self._collector.heartbeat_devcode = header.devcode
                    self._collector.heartbeat_payload_hex = payload.hex()
                    self._last_heartbeat_monotonic = monotonic()
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

        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None

        if heartbeat_task and heartbeat_task is not skip_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        reader_task = self._reader_task
        self._reader_task = None

        if reader_task and reader_task is not skip_task:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        writer = self._writer
        self._reader = None
        self._writer = None
        self._connected.clear()
        self._last_heartbeat_monotonic = None

        if writer:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("collector_disconnected"))
        self._pending.clear()

        at_future = self._pending_at_response
        self._pending_at_response = None
        if at_future is not None and not at_future.done():
            at_future.set_exception(ConnectionError("collector_disconnected"))


class _CollectorAtConnection:
    def __init__(self, *, remote_ip_hint: str = "", write_timeout: float) -> None:
        self._write_timeout = float(write_timeout)
        self._reader_task: asyncio.Task[None] | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = asyncio.Event()
        self._request_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._pending_response: asyncio.Future[CollectorAtResponse] | None = None
        self._collector = CollectorInfo(remote_ip=remote_ip_hint)

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    @property
    def collector_info(self) -> CollectorInfo:
        return _copy_collector_info(self._collector)

    def set_write_timeout(self, timeout: float) -> None:
        self._write_timeout = float(timeout)

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
            loop = asyncio.get_running_loop()
            future: asyncio.Future[CollectorAtResponse] = loop.create_future()
            self._pending_response = future
            try:
                await self._async_write(build_at_query(command))
                response = await asyncio.wait_for(future, timeout=request_timeout)
            finally:
                if self._pending_response is future:
                    self._pending_response = None
            self._apply_response_metadata(response)
            return response

    async def run(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        initial_bytes: bytes = b"",
    ) -> None:
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
            await self._disconnect(skip_task=current_task)

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
            while True:
                line = await reader.readuntil(b"\n")
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

    def _apply_response_metadata(self, response: CollectorAtResponse) -> None:
        if response.command == "DTUPN" and response.value:
            self._collector.collector_pn = response.value
        elif response.command == "FWVER" and response.value:
            self._collector.smartess_collector_version = response.value

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

        reader_task = self._reader_task
        self._reader_task = None
        if reader_task and reader_task is not skip_task:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        writer = self._writer
        self._reader = None
        self._writer = None
        self._connected.clear()

        if writer:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        future = self._pending_response
        self._pending_response = None
        if future is not None and not future.done():
            future.set_exception(ConnectionError("collector_disconnected"))


@dataclass(slots=True)
class _PendingCollectorSocket:
    remote_ip: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    sniff_task: asyncio.Task[None] | None = None


class _SharedEybondListener:
    def __init__(self, *, host: str, port: int) -> None:
        self._host = host
        self._port = int(port)
        self._server: asyncio.Server | None = None
        self._ref_count = 0
        self._connections: dict[str, _CollectorConnection] = {}
        self._at_connections: dict[str, _CollectorAtConnection] = {}
        self._pending_sockets: dict[str, _PendingCollectorSocket] = {}
        self._last_connection_ip = ""
        self._last_at_connection_ip = ""
        self._last_pending_ip = ""
        self._payload_owner_counts: dict[str, int] = {}
        self._at_owner_counts: dict[str, int] = {}

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
        for connection in self._unique_at_connections():
            await connection.disconnect()
        self._at_connections.clear()
        self._last_connection_ip = ""
        self._last_at_connection_ip = ""
        self._last_pending_ip = ""
        self._payload_owner_counts.clear()
        self._at_owner_counts.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        return True

    def register_payload_owner(self, collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        self._payload_owner_counts[owner] = self._payload_owner_counts.get(owner, 0) + 1

    def unregister_payload_owner(self, collector_ip: str) -> None:
        self._decrement_owner_count(self._payload_owner_counts, collector_ip)

    def register_at_owner(self, collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        self._at_owner_counts[owner] = self._at_owner_counts.get(owner, 0) + 1

    def unregister_at_owner(self, collector_ip: str) -> None:
        self._decrement_owner_count(self._at_owner_counts, collector_ip)

    def _decrement_owner_count(self, owner_counts: dict[str, int], collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        count = owner_counts.get(owner, 0)
        if count <= 1:
            owner_counts.pop(owner, None)
            return
        owner_counts[owner] = count - 1

    def ensure_connection(self, collector_ip: str, heartbeat_interval: float, write_timeout: float) -> _CollectorConnection | None:
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

    def ensure_at_connection(self, collector_ip: str, write_timeout: float) -> _CollectorAtConnection | None:
        if collector_ip:
            connection = self._at_connections.get(collector_ip)
            if connection is None:
                connection = _CollectorAtConnection(
                    remote_ip_hint=collector_ip,
                    write_timeout=write_timeout,
                )
                self._at_connections[collector_ip] = connection
            else:
                connection.set_write_timeout(write_timeout)
            return connection

        connection = self.current_at_connection(write_timeout=write_timeout)
        return connection

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

        for remote_ip in self._pending_sockets:
            _remember(remote_ip)

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
        self._pending_sockets.pop(pending.remote_ip, None)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is not None:
            sniff_task.cancel()
        return pending

    def _select_pending_socket(self, collector_ip: str) -> _PendingCollectorSocket | None:
        if collector_ip:
            pending = self._pending_sockets.get(collector_ip)
            if pending is not None:
                return pending

            candidates: list[_PendingCollectorSocket] = []
            for remote_ip, candidate in self._pending_sockets.items():
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
                preferred = self._pending_sockets.get(self._last_pending_ip)
                if preferred in unique_candidates.values():
                    return preferred
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
        pending.writer.close()
        try:
            await pending.writer.wait_closed()
        except Exception:
            pass

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

    async def release_collector_connections(
        self,
        collector_ip: str,
        *,
        close_payload: bool = False,
        close_at: bool = False,
        close_pending: bool = False,
    ) -> None:
        if not collector_ip:
            return

        if close_payload and self._has_owner_for_remote_ip(self._payload_owner_counts, collector_ip):
            close_payload = False
            close_pending = False
        if close_at and self._has_owner_for_remote_ip(self._at_owner_counts, collector_ip):
            close_at = False

        if close_pending:
            for remote_ip, pending in tuple(self._pending_sockets.items()):
                if not self._callback_ip_matches_collector(collector_ip, remote_ip):
                    continue
                self._pending_sockets.pop(remote_ip, None)
                await self._close_pending_socket(pending)

        if close_payload:
            await self._disconnect_connection_keys(
                self._connections,
                self._connection_keys_for_collector(collector_ip, self._connections),
            )
            if self._callback_ip_matches_collector(collector_ip, self._last_connection_ip):
                self._last_connection_ip = ""

        if close_at:
            await self._disconnect_connection_keys(
                self._at_connections,
                self._connection_keys_for_collector(collector_ip, self._at_connections),
            )
            if self._callback_ip_matches_collector(collector_ip, self._last_at_connection_ip):
                self._last_at_connection_ip = ""

        if close_pending and self._callback_ip_matches_collector(collector_ip, self._last_pending_ip):
            self._last_pending_ip = ""

    async def activate_pending_at_connection(
        self,
        pending: _PendingCollectorSocket,
        *,
        collector_ip: str,
        write_timeout: float,
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
            )
        else:
            connection.set_write_timeout(write_timeout)

        self._at_connections[remote_ip] = connection
        if collector_ip and collector_ip not in self._at_connections:
            self._at_connections[collector_ip] = connection
        self._last_at_connection_ip = remote_ip
        asyncio.create_task(
            connection.run(pending.reader, pending.writer),
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
        self._last_connection_ip = remote_ip
        asyncio.create_task(
            connection.run(pending.reader, pending.writer),
            name=f"collector_framed_{remote_ip}",
        )
        await connection.wait_until_connected(timeout=0.1)
        return connection

    async def _sniff_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        try:
            chunk = await pending.reader.read(16)
        except Exception:
            chunk = b""

        current = self._pending_sockets.get(pending.remote_ip)
        if current is not pending:
            return

        self._pending_sockets.pop(pending.remote_ip, None)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""

        if not chunk:
            pending.writer.close()
            try:
                await pending.writer.wait_closed()
            except Exception:
                pass
            return

        if _looks_like_at_traffic(chunk):
            connection = self._at_connections.get(pending.remote_ip)
            if connection is None:
                connection = self._resolve_public_placeholder_alias(
                    pending.remote_ip,
                    connections=self._at_connections,
                )
            if connection is None:
                if not self._has_owner_for_remote_ip(self._at_owner_counts, pending.remote_ip):
                    pending.writer.close()
                    try:
                        await pending.writer.wait_closed()
                    except Exception:
                        pass
                    return
                connection = _CollectorAtConnection(
                    remote_ip_hint=pending.remote_ip,
                    write_timeout=1.5,
                )
            else:
                connection.set_write_timeout(1.5)
            self._at_connections[pending.remote_ip] = connection
            self._last_at_connection_ip = pending.remote_ip
            await connection.run(pending.reader, pending.writer, initial_bytes=chunk)
            return

        connection = self._connections.get(pending.remote_ip)
        if connection is None:
            connection = self._resolve_public_placeholder_alias(pending.remote_ip)
        if connection is None:
            if not self._has_owner_for_remote_ip(self._payload_owner_counts, pending.remote_ip):
                pending.writer.close()
                try:
                    await pending.writer.wait_closed()
                except Exception:
                    pass
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
        self._last_connection_ip = pending.remote_ip
        await connection.run(pending.reader, pending.writer, initial_bytes=chunk)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername") or ("", None)
        remote_ip = peer[0] or ""
        if not remote_ip:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        existing_pending = self._pending_sockets.get(remote_ip)
        if existing_pending is not None:
            await self._close_pending_socket(existing_pending)

        pending = _PendingCollectorSocket(
            remote_ip=remote_ip,
            reader=reader,
            writer=writer,
        )
        self._pending_sockets[remote_ip] = pending
        self._last_pending_ip = remote_ip
        pending.sniff_task = asyncio.create_task(
            self._sniff_pending_socket(pending),
            name=f"collector_pending_sniff_{remote_ip}",
        )


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


async def _acquire_shared_payload_listener(host: str, port: int, collector_ip: str) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        listener = await _acquire_listener_locked(host, port)
        listener.register_payload_owner(collector_ip)
        return listener


async def _acquire_shared_at_listener(host: str, port: int, collector_ip: str) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        listener = await _acquire_listener_locked(host, port)
        listener.register_at_owner(collector_ip)
        return listener


async def _release_shared_listener(
    listener: _SharedEybondListener,
    *,
    collector_ip: str = "",
    close_payload: bool = False,
    close_at: bool = False,
    close_pending: bool = False,
    unregister_payload_owner: bool = False,
    unregister_at_owner: bool = False,
) -> None:
    async def _release() -> None:
        async with _LISTENERS_LOCK:
            key = (listener._host, listener._port)
            if unregister_payload_owner:
                listener.unregister_payload_owner(collector_ip)
            if unregister_at_owner:
                listener.unregister_at_owner(collector_ip)
            await listener.release_collector_connections(
                collector_ip,
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
        handler: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]],
    ) -> None:
        self._host = str(host)
        self._port = int(port)
        self._collector_ip = str(collector_ip or "").strip()
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
                pending = listener.pop_pending_socket(self._collector_ip)
                if pending is None:
                    await asyncio.sleep(0.1)
                    continue
                await self._handler(pending.reader, pending.writer)
        except asyncio.CancelledError:
            raise


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
    ) -> None:
        self._host = host
        self._port = int(port)
        self._request_timeout = request_timeout
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._collector_ip = collector_ip
        self._listener: _SharedEybondListener | None = None

    @property
    def connected(self) -> bool:
        connection = self._connection(create_placeholder=False)
        return connection.connected if connection is not None else False

    @property
    def collector_info(self) -> CollectorInfo:
        connection = self._connection(create_placeholder=False)
        if connection is not None:
            return connection.collector_info
        return _copy_collector_info(CollectorInfo(remote_ip=self._collector_ip))

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_payload_listener(
            self._host,
            self._port,
            self._collector_ip,
        )
        self._connection(create_placeholder=bool(self._collector_ip))

    async def stop(self) -> None:
        if self._listener is None:
            return
        listener = self._listener
        self._listener = None
        await _release_shared_listener(
            listener,
            collector_ip=self._collector_ip,
            close_payload=True,
            close_pending=True,
            unregister_payload_owner=True,
        )

    async def async_snapshot_shared_connection(self) -> _CollectorConnection | None:
        if not self._collector_ip:
            return None
        async with _LISTENERS_LOCK:
            listener = _LISTENERS.get((self._host, self._port))
            if listener is None:
                return None
            connection = listener._connections.get(self._collector_ip)
            if connection is None or not connection.connected:
                return None
            return connection

    async def async_disconnect_if_new_shared_connection(
        self,
        snapshot: _CollectorConnection | None,
    ) -> None:
        if not self._collector_ip:
            return
        async with _LISTENERS_LOCK:
            listener = _LISTENERS.get((self._host, self._port))
            if listener is None:
                return
            connection = listener._connections.get(self._collector_ip)
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

            if self._collector_ip:
                pending = self._listener.pop_pending_socket(self._collector_ip)
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

            if self._collector_ip:
                pending = self._listener.pop_pending_socket(self._collector_ip)
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

        pending = self._listener.pop_pending_socket(self._collector_ip)
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
        if create_placeholder:
            return self._listener.ensure_connection(
                self._collector_ip,
                self._heartbeat_interval,
                self._write_timeout,
            )
        if self._collector_ip:
            return self._listener.ensure_connection(
                self._collector_ip,
                self._heartbeat_interval,
                self._write_timeout,
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
    ) -> None:
        self._host = host
        self._port = int(port)
        self._request_timeout = float(request_timeout)
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._collector_ip = collector_ip
        self._listener: _SharedEybondListener | None = None

    @property
    def connected(self) -> bool:
        framed = self._framed_connection(create_placeholder=False)
        if framed is not None and framed.connected:
            return True

        connection = self._at_connection(create_placeholder=False)
        return connection.connected if connection is not None else False

    @property
    def collector_info(self) -> CollectorInfo:
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
        return _copy_collector_info(CollectorInfo(remote_ip=self._collector_ip))

    async def start(self) -> None:
        if self._listener is not None:
            return
        self._listener = await _acquire_shared_at_listener(
            self._host,
            self._port,
            self._collector_ip,
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
            close_at=True,
            unregister_at_owner=True,
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
            framed = self._framed_connection(create_placeholder=False)
            if framed is not None and framed.connected:
                return True

            connection = self._at_connection(create_placeholder=bool(self._collector_ip))
            if connection is not None and connection.connected:
                return True

            if self._collector_ip:
                pending = self._listener.pop_pending_socket(self._collector_ip)
                if pending is not None:
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
        framed = self._framed_connection(create_placeholder=False)
        if framed is not None and framed.connected:
            return await framed.async_query(command, request_timeout=self._request_timeout)

        connection = self._at_connection(create_placeholder=bool(self._collector_ip))
        if connection is not None and connection.connected:
            return await connection.async_query(command, request_timeout=self._request_timeout)

        if self._listener is None:
            raise ConnectionError("collector_not_connected")

        pending = self._listener.pop_pending_socket(self._collector_ip)
        if pending is not None:
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

    def _at_connection(self, *, create_placeholder: bool) -> _CollectorAtConnection | None:
        if self._listener is None:
            return None
        if create_placeholder:
            return self._listener.ensure_at_connection(
                self._collector_ip,
                self._write_timeout,
            )
        if self._collector_ip:
            return self._listener.ensure_at_connection(
                self._collector_ip,
                self._write_timeout,
            )
        return self._listener.current_at_connection(write_timeout=self._write_timeout)

    def _framed_connection(self, *, create_placeholder: bool) -> _CollectorConnection | None:
        if self._listener is None:
            return None
        if create_placeholder:
            return self._listener.ensure_connection(
                self._collector_ip,
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
            )
        if self._collector_ip:
            return self._listener.ensure_connection(
                self._collector_ip,
                heartbeat_interval=60.0,
                write_timeout=self._write_timeout,
            )
        return self._listener.current_connection(
            heartbeat_interval=60.0,
            write_timeout=self._write_timeout,
        )
