"""Minimal EyeBond/SmartESS collector emulator for unit tests."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.at import parse_at_command
from custom_components.eybond_local.collector.protocol import (
    FC_FORWARD_TO_DEVICE,
    FC_HEARTBEAT,
    FC_QUERY_COLLECTOR,
    FC_SET_COLLECTOR,
    HEADER_SIZE,
    TIDCounter,
    build_collector_request,
    decode_header,
)
from fake_collector_lib import (
    CollectorScenario,
    build_at_reply,
    build_forward_response,
    build_query_collector_response,
    build_set_collector_response,
    build_udp_reply,
    build_unsolicited_heartbeat,
    parse_discovery_redirect,
)

_LOG = logging.getLogger("fake_collector")


class FakeCollectorService:
    """Async UDP discovery listener plus reverse TCP callback client."""

    def __init__(
        self,
        *,
        listen_ip: str,
        udp_port: int,
        tcp_bind_ip: str,
        heartbeat_interval: float,
        connect_timeout: float,
        udp_reply: str,
        scenario: CollectorScenario,
        nat_peer_scenarios: tuple[CollectorScenario, ...] = (),
    ) -> None:
        self._listen_ip = listen_ip
        self._udp_port = int(udp_port)
        self._tcp_bind_ip = tcp_bind_ip
        self._heartbeat_interval = float(heartbeat_interval)
        self._connect_timeout = float(connect_timeout)
        self._udp_reply = udp_reply
        self._scenario = scenario
        self._profile = scenario.profile
        self._nat_peer_services: tuple[FakeCollectorService, ...] = tuple(
            FakeCollectorService(
                listen_ip=listen_ip,
                udp_port=udp_port,
                tcp_bind_ip=tcp_bind_ip,
                heartbeat_interval=heartbeat_interval,
                connect_timeout=connect_timeout,
                udp_reply="",
                scenario=peer_scenario,
            )
            for peer_scenario in nat_peer_scenarios
        )
        self._udp_transport: asyncio.DatagramTransport | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._endpoint_lock = asyncio.Lock()
        self._tid = TIDCounter()
        self._unsolicited_tid = 0x8000
        self._last_discovery: tuple[str, int] | None = None
        self._cloud_endpoint = ""
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._heartbeat_ready = asyncio.Event()

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _UdpProtocol(self),
            local_addr=(self._listen_ip, self._udp_port),
        )
        self._udp_transport = transport
        _LOG.info("UDP discovery listener started on %s:%d", self._listen_ip, self._udp_port)

    async def stop(self) -> None:
        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        reader_task = self._reader_task
        self._reader_task = None
        if reader_task is not None:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None

        background_tasks = tuple(self._background_tasks)
        self._background_tasks.clear()
        for task in background_tasks:
            task.cancel()
        for task in background_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        for peer in self._nat_peer_services:
            await peer.stop()

    def create_background_task(self, coro: Any, *, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def handle_discovery(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            redirect = parse_discovery_redirect(data)
        except ValueError:
            _LOG.debug("Ignoring non-discovery UDP from=%s:%d payload=%r", addr[0], addr[1], data)
            return

        _LOG.info(
            "Discovery RX from=%s:%d callback=%s:%d raw=%s",
            addr[0],
            addr[1],
            redirect.server_ip,
            redirect.server_port,
            redirect.raw,
        )

        if self._udp_transport is not None and self._udp_reply:
            payload = build_udp_reply(self._udp_reply)
            self._udp_transport.sendto(payload, addr)
            _LOG.info(
                "Discovery TX to=%s:%d reply=%s",
                addr[0],
                addr[1],
                payload.decode("ascii", errors="replace"),
            )

        await self._ensure_reverse_tcp(redirect.server_ip, redirect.server_port)
        for peer in self._nat_peer_services:
            await peer._ensure_reverse_tcp(redirect.server_ip, redirect.server_port)

    async def _ensure_reverse_tcp(self, server_ip: str, server_port: int) -> None:
        endpoint = (server_ip, server_port)
        async with self._endpoint_lock:
            if self._connection_alive() and self._last_discovery == endpoint:
                return

            await self._close_tcp_only()
            self._last_discovery = endpoint
            self._cloud_endpoint = f"{server_ip},{server_port},TCP"

            local_addr = (self._tcp_bind_ip, 0) if self._tcp_bind_ip else None
            try:
                if self._scenario.reverse_connect_delay > 0:
                    _LOG.info(
                        "Reverse TCP delayed by %.3fs before connect target=%s:%d",
                        self._scenario.reverse_connect_delay,
                        server_ip,
                        server_port,
                    )
                    await asyncio.sleep(self._scenario.reverse_connect_delay)
                connect_coro = asyncio.open_connection(server_ip, server_port, local_addr=local_addr)
                reader, writer = await asyncio.wait_for(connect_coro, timeout=self._connect_timeout)
            except Exception as exc:
                _LOG.warning(
                    "Reverse TCP connect failed target=%s:%d error=%s",
                    server_ip,
                    server_port,
                    exc,
                )
                return

            self._reader = reader
            self._writer = writer
            local = writer.get_extra_info("sockname") or ("", 0)
            peer = writer.get_extra_info("peername") or ("", 0)
            _LOG.info(
                "Reverse TCP connected local=%s:%s remote=%s:%s",
                local[0],
                local[1],
                peer[0],
                peer[1],
            )

            self._heartbeat_ready = asyncio.Event()
            if self._scenario.first_heartbeat_delay <= 0:
                self._heartbeat_ready.set()
                await self._send_heartbeat(reason="connect")
            else:
                self.create_background_task(
                    self._delayed_first_heartbeat(),
                    name="fake_collector_first_heartbeat",
                )
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="fake_collector_heartbeat")
            self._reader_task = asyncio.create_task(self._reader_loop(), name="fake_collector_reader")

    async def _close_tcp_only(self) -> None:
        current_task = asyncio.current_task()
        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat_task is not None and heartbeat_task is not current_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        reader_task = self._reader_task
        self._reader_task = None
        if reader_task is not None and reader_task is not current_task:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        writer = self._writer
        self._reader = None
        self._writer = None
        self._heartbeat_ready = asyncio.Event()
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _connection_alive(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def _delayed_first_heartbeat(self) -> None:
        try:
            await asyncio.sleep(self._scenario.first_heartbeat_delay)
            if not self._connection_alive():
                return
            self._heartbeat_ready.set()
            await self._send_heartbeat(reason="delayed_connect")
        except asyncio.CancelledError:
            raise

    async def _heartbeat_loop(self) -> None:
        try:
            await self._heartbeat_ready.wait()
            while self._connection_alive():
                await asyncio.sleep(self._heartbeat_interval)
                await self._send_heartbeat(reason="interval")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOG.debug("Heartbeat loop stopped error=%s", exc)

    def _next_unsolicited_tid(self) -> int:
        self._unsolicited_tid = (self._unsolicited_tid + 1) & 0xFFFF
        if self._unsolicited_tid < 0x8000:
            self._unsolicited_tid = 0x8000
        return self._unsolicited_tid

    async def _send_heartbeat(self, *, reason: str, tid: int | None = None) -> None:
        frame = build_unsolicited_heartbeat(
            tid=self._next_unsolicited_tid() if tid is None else int(tid) & 0xFFFF,
            pn=self._profile.pn,
            devcode=self._scenario.heartbeat_devcode,
            collector_addr=self._scenario.collector_addr,
        )
        await self._send_raw(frame)
        _LOG.info(
            "Heartbeat TX reason=%s pn=%s devcode=0x%04X",
            reason,
            self._profile.pn[:14],
            self._scenario.heartbeat_devcode,
        )

    async def _reader_loop(self) -> None:
        reader = self._reader
        if reader is None:
            return

        try:
            while True:
                prefix = await reader.readexactly(3)
                if prefix == b"AT+":
                    line = prefix + await reader.readuntil(b"\n")
                    await self._handle_at(line)
                    continue

                header_bytes = prefix + await reader.readexactly(HEADER_SIZE - len(prefix))
                header = decode_header(header_bytes)
                payload = b""
                if header.payload_len:
                    payload = await reader.readexactly(header.payload_len)

                _LOG.info(
                    "Frame RX tid=%d fc=%d devcode=0x%04X devaddr=0x%02X payload_hex=%s",
                    header.tid,
                    header.fcode,
                    header.devcode,
                    header.devaddr,
                    payload.hex(),
                )

                if header.fcode == FC_HEARTBEAT:
                    if not self._heartbeat_ready.is_set():
                        _LOG.info(
                            "Heartbeat response delayed wait_remaining=%.3fs",
                            self._scenario.first_heartbeat_delay,
                        )
                        continue
                    await self._send_heartbeat(reason="server_fc1", tid=header.tid)
                    continue

                if header.fcode == FC_QUERY_COLLECTOR:
                    parameter = payload[0] if payload else 0
                    response_payload = build_query_collector_response(parameter, self._scenario)
                    if response_payload is None:
                        _LOG.info("Frame DROP fc=2 parameter=%d behavior=timeout", parameter)
                        continue
                    await self._send_framed_response(
                        tid=header.tid,
                        fcode=header.fcode,
                        devcode=header.devcode,
                        collector_addr=header.devaddr,
                        payload=response_payload,
                    )
                    _LOG.info(
                        "Frame TX fc=2 parameter=%d status=%d payload_hex=%s",
                        parameter,
                        response_payload[0] if response_payload else -1,
                        response_payload.hex(),
                    )
                    continue

                if header.fcode == FC_SET_COLLECTOR:
                    parameter = payload[0] if payload else 0
                    response_payload = build_set_collector_response(parameter, success=False)
                    await self._send_framed_response(
                        tid=header.tid,
                        fcode=header.fcode,
                        devcode=header.devcode,
                        collector_addr=header.devaddr,
                        payload=response_payload,
                    )
                    _LOG.info(
                        "Frame TX fc=3 parameter=%d status=%d",
                        parameter,
                        response_payload[0] if response_payload else -1,
                    )
                    continue

                if header.fcode == FC_FORWARD_TO_DEVICE:
                    response_payload = build_forward_response(payload, self._scenario)
                    if response_payload is None:
                        _LOG.info("Frame DROP fc=4 behavior=timeout")
                        continue
                    await self._send_framed_response(
                        tid=header.tid,
                        fcode=header.fcode,
                        devcode=header.devcode,
                        collector_addr=header.devaddr,
                        payload=response_payload,
                    )
                    _LOG.info(
                        "Frame TX fc=4 response_hex=%s",
                        response_payload.hex(),
                    )
                    continue

                _LOG.debug("Ignoring unsupported function code fc=%d", header.fcode)
        except asyncio.IncompleteReadError:
            _LOG.info("Reverse TCP disconnected by peer")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOG.warning("Reverse TCP reader failed error=%s", exc)
        finally:
            await self._close_tcp_only()

    async def _handle_at(self, payload: bytes) -> None:
        text = payload.decode("ascii", errors="replace").strip()
        _LOG.info("AT RX %s", text)
        try:
            command = parse_at_command(payload)
        except Exception as exc:
            _LOG.debug("Ignoring invalid AT payload error=%s raw=%r", exc, payload)
            return

        write_ack = command.operation == "write"
        if write_ack and command.command == "CLDSRVHOST1":
            self._cloud_endpoint = command.value
        response = build_at_reply(
            command.command,
            profile=self._profile,
            cloud_endpoint=self._cloud_endpoint,
            write_ack=write_ack,
        )
        await self._send_raw(response)
        _LOG.info("AT TX %s", response.decode("ascii", errors="replace").strip())

    async def _send_framed_response(
        self,
        *,
        tid: int,
        fcode: int,
        devcode: int,
        collector_addr: int,
        payload: bytes,
    ) -> None:
        frame = build_collector_request(
            tid,
            payload,
            devcode=devcode,
            collector_addr=collector_addr,
            fcode=fcode,
        )
        await self._send_raw(frame)

    async def _send_raw(self, payload: bytes) -> None:
        writer = self._writer
        if writer is None or writer.is_closing():
            raise ConnectionError("reverse_tcp_not_connected")
        writer.write(payload)
        await writer.drain()


class _UdpProtocol(asyncio.DatagramProtocol):
    """Thin UDP adapter that forwards datagrams into the service."""

    def __init__(self, service: FakeCollectorService) -> None:
        self._service = service

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self._service.create_background_task(
            self._service.handle_discovery(data, addr),
            name=f"fake_collector_discovery_{addr[0]}_{addr[1]}",
        )

    def error_received(self, exc: Exception) -> None:
        _LOG.debug("UDP listener error=%s", exc)
