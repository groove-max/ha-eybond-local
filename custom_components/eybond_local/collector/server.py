"""Reverse TCP server for EyeBond collectors."""

from __future__ import annotations

import asyncio
import logging
from time import monotonic
from typing import Any

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


class EybondServer:
    """Accepts a reverse TCP connection from the collector and forwards requests."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        request_timeout: float,
        heartbeat_interval: float,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._request_timeout = request_timeout
        self._write_timeout = _bounded_write_timeout(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._server: asyncio.Server | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = asyncio.Event()
        self._pending: dict[int, asyncio.Future[tuple[EybondHeader, bytes]]] = {}
        self._request_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._tid = TIDCounter()
        self._collector = CollectorInfo()
        self._last_heartbeat_monotonic: float | None = None

    @property
    def connected(self) -> bool:
        """Whether a collector is currently connected."""

        return self._writer is not None and not self._writer.is_closing()

    @property
    def collector_info(self) -> CollectorInfo:
        """Return a snapshot of the last known collector metadata."""

        return apply_collector_profile(CollectorInfo(
            remote_ip=self._collector.remote_ip,
            remote_port=self._collector.remote_port,
            connection_count=self._collector.connection_count,
            connection_replace_count=self._collector.connection_replace_count,
            disconnect_count=self._collector.disconnect_count,
            pending_request_drop_count=self._collector.pending_request_drop_count,
            last_disconnect_reason=self._collector.last_disconnect_reason,
            discovery_restart_count=self._collector.discovery_restart_count,
            last_discovery_reason=self._collector.last_discovery_reason,
            collector_pn=self._collector.collector_pn,
            last_devcode=self._collector.last_devcode,
            heartbeat_devcode=self._collector.heartbeat_devcode,
            heartbeat_payload_hex=self._collector.heartbeat_payload_hex,
            last_udp_reply=self._collector.last_udp_reply,
            last_udp_reply_from=self._collector.last_udp_reply_from,
            profile_key=self._collector.profile_key,
            profile_name=self._collector.profile_name,
            heartbeat_ascii=self._collector.heartbeat_ascii,
            heartbeat_payload_len=self._collector.heartbeat_payload_len,
            heartbeat_format_key=self._collector.heartbeat_format_key,
            heartbeat_suffix_ascii=self._collector.heartbeat_suffix_ascii,
            heartbeat_suffix_kind=self._collector.heartbeat_suffix_kind,
            heartbeat_suffix_uint=self._collector.heartbeat_suffix_uint,
            devcode_major=self._collector.devcode_major,
            devcode_minor=self._collector.devcode_minor,
            collector_pn_prefix=self._collector.collector_pn_prefix,
            collector_pn_digits=self._collector.collector_pn_digits,
            heartbeat_age_seconds=self._heartbeat_age_seconds(),
            heartbeat_fresh=self._has_fresh_heartbeat(),
        ))

    def _heartbeat_age_seconds(self) -> float | None:
        if self._last_heartbeat_monotonic is None:
            return None
        return max(0.0, monotonic() - self._last_heartbeat_monotonic)

    def _heartbeat_freshness_window(self) -> float:
        return max(self._heartbeat_interval * 2.0, 5.0)

    def _has_fresh_heartbeat(self) -> bool:
        age = self._heartbeat_age_seconds()
        return age is not None and age <= self._heartbeat_freshness_window()

    async def start(self) -> None:
        """Start listening for collector connections."""

        if self._server:
            return
        self._server = await asyncio.start_server(self._handle_connection, self._host, self._port)
        logger.info("EyeBond server listening on %s:%d", self._host, self._port)

    async def stop(self) -> None:
        """Stop the server and close the active connection."""

        await self._disconnect()
        if not self._server:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def wait_until_connected(self, timeout: float) -> bool:
        """Wait for the collector to connect."""

        if self.connected:
            return True
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return self.connected

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        """Wait for at least one collector heartbeat frame."""

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
    ) -> bytes:
        """Send a collector FC=4 request and wait for its payload response."""

        _, response_payload = await self.async_send_collector(
            fcode=FC_FORWARD_TO_DEVICE,
            payload=payload,
            devcode=devcode,
            collector_addr=collector_addr,
        )
        return response_payload

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
        """Send a raw collector frame and wait for the matching response."""

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
                    "TX collector tid=%d fc=%d devcode=0x%04X devaddr=0x%02X payload=%s",
                    tid,
                    fcode,
                    devcode,
                    collector_addr,
                    payload.hex(),
                )
                return await asyncio.wait_for(future, timeout=self._request_timeout)
            finally:
                self._pending.pop(tid, None)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        if self.connected:
            logger.warning("Replacing active collector connection")
            await self._disconnect()

        peer = writer.get_extra_info("peername") or ("", None)
        self._collector.remote_ip = peer[0] or ""
        self._collector.remote_port = peer[1]
        self._last_heartbeat_monotonic = None
        self._reader = reader
        self._writer = writer
        self._connected.set()

        logger.info("Collector connected from %s:%s", self._collector.remote_ip, self._collector.remote_port)

        current_task = asyncio.current_task()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="eybond_heartbeat")
        self._reader_task = asyncio.create_task(self._read_loop(reader), name="eybond_reader")
        try:
            await self._reader_task
        finally:
            await self._disconnect(skip_task=current_task)

    async def _heartbeat_loop(self) -> None:
        try:
            while self.connected:
                tid = self._tid.next()
                frame = build_heartbeat_request(tid, int(self._heartbeat_interval))
                await self._async_write(frame)
                logger.debug("TX FC=1 tid=%d interval=%d", tid, int(self._heartbeat_interval))
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Heartbeat loop stopped: %s", exc)

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
                    "RX header tid=%d devcode=0x%04X devaddr=0x%02X fc=%d payload=%d",
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

                logger.debug("Unhandled collector frame fc=%d payload=%s", header.fcode, payload.hex())
        except asyncio.IncompleteReadError:
            logger.info("Collector disconnected")
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            logger.info("Collector disconnected: %s", exc)
        except asyncio.CancelledError:
            raise

    async def _disconnect(self, skip_task: asyncio.Task[Any] | None = None) -> None:
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
