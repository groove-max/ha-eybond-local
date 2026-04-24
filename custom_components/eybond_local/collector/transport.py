"""Shared collector transport primitives for single- and multi-collector listeners."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from time import monotonic
from typing import Any, Protocol

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

    async def run(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
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
        self._reader_task = asyncio.create_task(self._read_loop(reader), name=f"eybond_reader_{self._collector.remote_ip}")
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

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                header_bytes = await reader.readexactly(HEADER_SIZE)
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


class _SharedEybondListener:
    def __init__(self, *, host: str, port: int) -> None:
        self._host = host
        self._port = int(port)
        self._server: asyncio.Server | None = None
        self._ref_count = 0
        self._connections: dict[str, _CollectorConnection] = {}
        self._last_connection_ip = ""

    async def acquire(self) -> None:
        self._ref_count += 1
        if self._server is None:
            self._server = await asyncio.start_server(self._handle_connection, self._host, self._port)
            logger.info("Shared EyeBond listener listening on %s:%d", self._host, self._port)

    async def release(self) -> bool:
        self._ref_count = max(0, self._ref_count - 1)
        if self._ref_count != 0:
            return False

        for connection in self._unique_connections():
            await connection.disconnect()
        self._connections.clear()
        self._last_connection_ip = ""

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        return True

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
        if self._last_connection_ip:
            connection = self._connections.get(self._last_connection_ip)
            if connection is not None:
                connection.set_heartbeat_interval(heartbeat_interval)
                connection.set_write_timeout(write_timeout)
                return connection

        for connection in self._unique_connections():
            if connection.connected:
                connection.set_heartbeat_interval(heartbeat_interval)
                connection.set_write_timeout(write_timeout)
                return connection
        return None

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

    def _resolve_public_placeholder_alias(self, remote_ip: str) -> _CollectorConnection | None:
        if not remote_ip or remote_ip in self._connections:
            return self._connections.get(remote_ip)

        candidates: list[tuple[str, _CollectorConnection]] = []
        for expected_ip, connection in self._connections.items():
            if connection.connected:
                continue
            if not (
                _is_hairpin_alias_candidate(expected_ip, remote_ip)
                or _is_default_broadcast_alias_candidate(expected_ip, remote_ip)
            ):
                continue
            candidates.append((expected_ip, connection))

        unique_candidates: list[tuple[str, _CollectorConnection]] = []
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
            "Aliasing collector callback from %s to pending public target %s",
            remote_ip,
            expected_ip,
        )
        self._connections[remote_ip] = connection
        return connection

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

        connection = self._connections.get(remote_ip)
        if connection is None:
            connection = self._resolve_public_placeholder_alias(remote_ip)
        if connection is None:
            connection = _CollectorConnection(
                remote_ip_hint=remote_ip,
                heartbeat_interval=60.0,
                write_timeout=1.5,
            )
            self._connections[remote_ip] = connection
        self._last_connection_ip = remote_ip
        await connection.run(reader, writer)


_LISTENERS: dict[tuple[str, int], _SharedEybondListener] = {}
_LISTENERS_LOCK = asyncio.Lock()


async def _acquire_shared_listener(host: str, port: int) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        key = (host, int(port))
        listener = _LISTENERS.get(key)
        if listener is None:
            listener = _SharedEybondListener(host=host, port=port)
            _LISTENERS[key] = listener
        await listener.acquire()
        return listener


async def _release_shared_listener(listener: _SharedEybondListener) -> None:
    async with _LISTENERS_LOCK:
        key = (listener._host, listener._port)
        closed = await listener.release()
        if closed:
            _LISTENERS.pop(key, None)


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
        self._listener = await _acquire_shared_listener(self._host, self._port)
        self._connection(create_placeholder=bool(self._collector_ip))

    async def stop(self) -> None:
        if self._listener is None:
            return
        listener = self._listener
        self._listener = None
        await _release_shared_listener(listener)

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

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            if connection is not None and self._collector_ip:
                ok = await connection.wait_until_connected(timeout=remaining)
                if ok:
                    return True
                return False

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
        connection = self._connection(create_placeholder=bool(self._collector_ip))
        if connection is None:
            raise ConnectionError("collector_not_connected")
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
        connection = self._connection(create_placeholder=bool(self._collector_ip))
        if connection is None:
            raise ConnectionError("collector_not_connected")
        return await connection.async_send_collector(
            fcode=fcode,
            payload=payload,
            devcode=devcode,
            collector_addr=collector_addr,
            request_timeout=self._request_timeout,
        )

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
