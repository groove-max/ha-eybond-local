from __future__ import annotations

import asyncio
import socket
import sys
from time import monotonic
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.transport import (
    CollectorListenerBindError,
    SharedCollectorAtTransport,
    SharedEybondTransport,
    SharedProxyCaptureRoute,
    _LISTENERS,
    _CollectorAtConnection,
    _CollectorConnection,
    _PendingCollectorSocket,
    _SharedEybondListener,
)
from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE,
    build_collector_request,
    decode_header,
)


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class _FakeWriter:
    def __init__(self) -> None:
        self.closed = False
        self.buffer = bytearray()

    def is_closing(self) -> bool:
        return self.closed

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.closed = True

    def get_extra_info(self, name: str, default=None):
        if name == "peername":
            return ("203.0.113.10", 41000)
        return default


class SharedTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_collector_connection_wait_until_heartbeat_requires_fresh_sample(self) -> None:
        connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        class _OpenWriter:
            def is_closing(self) -> bool:
                return False

        connection._writer = _OpenWriter()  # type: ignore[assignment]
        connection._last_heartbeat_monotonic = monotonic() - 999.0

        self.assertFalse(await connection.wait_until_heartbeat(0.02))
        self.assertFalse(connection.collector_info.heartbeat_fresh)

    async def test_collector_connection_wait_until_heartbeat_accepts_fresh_sample(self) -> None:
        connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        connection._last_heartbeat_monotonic = monotonic()

        self.assertTrue(await connection.wait_until_heartbeat(0.02))
        self.assertTrue(connection.collector_info.heartbeat_fresh)
        self.assertIsNotNone(connection.collector_info.heartbeat_age_seconds)

    async def test_collector_connection_passively_reports_at_dtupn_identity(self) -> None:
        seen: list[tuple[str, str, str]] = []
        connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        connection._session_id = "session-1"
        connection._session_identity_callback = lambda session_id, pn, source: seen.append(
            (session_id, pn, source)
        )
        reader = asyncio.StreamReader()
        reader.feed_data(b"AT+DTUPN:E5000020000000\r\n")
        reader.feed_eof()

        await connection._read_loop(reader)

        self.assertEqual(
            seen,
            [("session-1", "E5000020000000", "at_dtupn")],
        )

    async def test_collector_connection_passively_reports_heartbeat_identity(self) -> None:
        seen: list[tuple[str, str, str]] = []
        connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        connection._session_id = "session-2"
        connection._session_identity_callback = lambda session_id, pn, source: seen.append(
            (session_id, pn, source)
        )
        reader = asyncio.StreamReader()
        reader.feed_data(
            build_collector_request(
                7,
                b"E5000020000000",
                devcode=2376,
                collector_addr=1,
                fcode=1,
            )
        )
        reader.feed_eof()

        await connection._read_loop(reader)

        self.assertEqual(
            seen,
            [("session-2", "E5000020000000", "framed_heartbeat")],
        )

    async def test_collector_connection_passively_reports_fc2_parameter_2_identity(self) -> None:
        seen: list[tuple[str, str, str]] = []
        connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        connection._session_id = "session-3"
        connection._session_identity_callback = lambda session_id, pn, source: seen.append(
            (session_id, pn, source)
        )
        reader = asyncio.StreamReader()
        reader.feed_data(
            build_collector_request(
                8,
                b"\x00\x02E50000200000000001",
                devcode=2376,
                collector_addr=1,
                fcode=2,
            )
        )
        reader.feed_eof()

        await connection._read_loop(reader)

        self.assertEqual(
            seen,
            [("session-3", "E50000200000000001", "fc2_parameter_2")],
        )

    async def test_collector_connection_write_timeout_raises_connection_error(self) -> None:
        connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.01,
        )

        class _BlockingWriter:
            def __init__(self) -> None:
                self.frames: list[bytes] = []

            def is_closing(self) -> bool:
                return False

            def write(self, frame: bytes) -> None:
                self.frames.append(frame)

            async def drain(self) -> None:
                await asyncio.Future()

        connection._writer = _BlockingWriter()  # type: ignore[assignment]

        with self.assertRaisesRegex(ConnectionError, "collector_write_timeout"):
            await connection._async_write(b"abc")

    async def test_two_transports_share_one_listener(self) -> None:
        port = _free_tcp_port()
        first = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )
        second = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )

        await first.start()
        await second.start()

        reader = writer = None
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"\x00")
            await writer.drain()
            self.assertTrue(await first.wait_until_connected(1.0))
            self.assertTrue(await second.wait_until_connected(1.0))
            self.assertTrue(first.connected)
            self.assertTrue(second.connected)
            self.assertEqual(first.collector_info.remote_ip, "127.0.0.1")
            self.assertEqual(second.collector_info.remote_ip, "127.0.0.1")
        finally:
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            await first.stop()
            await second.stop()

    async def test_specific_host_request_reuses_wildcard_listener(self) -> None:
        # The runtime binds its callback listener on 0.0.0.0; options-flow
        # helpers (collector Wi-Fi change, restart) historically request the
        # entry's server IP on the same port. The registry must hand back the
        # wildcard listener instead of binding the specific address — that bind
        # fails with EADDRINUSE while the wildcard socket holds the port
        # (the "collector_listener_bind_failed ... address in use" regression).
        port = _free_tcp_port()
        runtime_like = SharedEybondTransport(
            host="0.0.0.0",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )
        options_like = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )

        await runtime_like.start()
        try:
            # Must NOT raise CollectorListenerBindError: the wildcard listener
            # already serves this port.
            await options_like.start()
            self.assertIs(options_like._listener, runtime_like._listener)
            self.assertEqual(len(_LISTENERS), 1)
        finally:
            await options_like.stop()
            await runtime_like.stop()
        self.assertEqual(len(_LISTENERS), 0)

    async def test_listener_session_inventory_keeps_multiple_same_ip_pending_sessions(self) -> None:
        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )

        first_writer = None
        second_writer = None
        await transport.start()
        try:
            listener = transport._listener
            self.assertIsNotNone(listener)
            assert listener is not None

            _first_reader, first_writer = await asyncio.open_connection("127.0.0.1", port)
            await asyncio.sleep(0.05)
            _second_reader, second_writer = await asyncio.open_connection("127.0.0.1", port)
            await asyncio.sleep(0.05)

            diagnostics = listener.session_inventory_diagnostics()
            self.assertEqual(diagnostics["pending_session_count"], 2)
            self.assertEqual(diagnostics["recent_session_count"], 2)
            self.assertEqual(diagnostics["duplicate_peer_ip_count"], 1)
            self.assertEqual(diagnostics["duplicate_peer_ips"], ["127.0.0.1"])
            states = {
                item["state"]
                for item in diagnostics["sessions"]
                if isinstance(item, dict)
            }
            self.assertEqual(states, {"pending"})
        finally:
            for writer in (first_writer, second_writer):
                if writer is not None:
                    writer.close()
                    await writer.wait_closed()
            await transport.stop()

    async def test_transport_can_select_connected_session_by_collector_pn(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())

        class _OpenWriter:
            def is_closing(self) -> bool:
                return False

        first = _CollectorConnection(
            remote_ip_hint="203.0.113.10",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        first._writer = _OpenWriter()  # type: ignore[assignment]
        first._collector.remote_ip = "203.0.113.10"
        first._collector.collector_pn = "PN-ONE"

        second = _CollectorConnection(
            remote_ip_hint="203.0.113.10",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        second._writer = _OpenWriter()  # type: ignore[assignment]
        second._collector.remote_ip = "203.0.113.10"
        second._collector.collector_pn = "PN-TWO"

        listener._connections["203.0.113.10:first"] = first
        listener._connections["203.0.113.10:second"] = second
        listener._connections_by_pn["PN-ONE"] = first
        listener._connections_by_pn["PN-TWO"] = second

        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=listener._port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
            collector_pn="PN-TWO",
        )
        transport._listener = listener

        self.assertTrue(transport.connected)
        self.assertEqual(transport.collector_info.collector_pn, "PN-TWO")

    async def test_listener_indexes_passive_identity_for_pn_routing(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        connection = _CollectorConnection(
            remote_ip_hint="203.0.113.10",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        listener._session_payload_connections["session-1"] = connection

        listener._mark_session_identity("session-1", "E5000020000000", "framed_heartbeat")

        self.assertIs(
            listener.ensure_connection(
                "",
                heartbeat_interval=60.0,
                write_timeout=0.5,
                collector_pn="E5000020000000",
            ),
            connection,
        )

    async def test_release_collector_connections_drops_session_identity_indexes(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        connection = _CollectorConnection(
            remote_ip_hint="203.0.113.10",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        connection._writer = _FakeWriter()  # type: ignore[assignment]
        listener._connections["203.0.113.10"] = connection
        listener._connections_by_pn["PN-ONE"] = connection
        listener._session_payload_connections["session-one"] = connection

        await listener.release_collector_connections(
            "",
            "PN-ONE",
            close_payload=True,
        )

        self.assertNotIn("203.0.113.10", listener._connections)
        self.assertNotIn("PN-ONE", listener._connections_by_pn)
        self.assertNotIn("session-one", listener._session_payload_connections)

    async def test_listener_routes_initial_framed_identity_to_pn_owner(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.register_payload_pn_owner("E50000200000000001")
        listener._remember_session(
            session_id="session-1",
            remote_ip="203.0.113.10",
            remote_port=41000,
        )
        reader = asyncio.StreamReader()
        reader.feed_data(
            build_collector_request(
                7,
                b"E5000020000000",
                devcode=2376,
                collector_addr=1,
                fcode=1,
            )
        )
        reader.feed_eof()
        pending = _PendingCollectorSocket(
            remote_ip="203.0.113.10",
            remote_port=41000,
            session_id="session-1",
            reader=reader,
            writer=_FakeWriter(),  # type: ignore[arg-type]
        )
        listener._pending_sockets[pending.remote_ip] = pending

        await listener._sniff_pending_socket(pending)

        diagnostics = listener.session_inventory_diagnostics()
        self.assertEqual(diagnostics["pending_session_count"], 0)
        self.assertEqual(diagnostics["sessions"][0]["collector_identity_source"], "framed_heartbeat")
        self.assertEqual(
            diagnostics["sessions"][0]["collector_identity_masked"],
            "E50********000",
        )
        self.assertIn("E5000020000000", listener._connections_by_pn)

    async def test_listener_active_probe_routes_silent_at_session_to_pn_owner(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.register_session_protocol_owner("at_text")
        listener.register_at_pn_owner("E5000020000000")
        listener._remember_session(
            session_id="session-1",
            remote_ip="203.0.113.10",
            remote_port=41000,
        )
        reader = asyncio.StreamReader()

        class _ProbeWriter(_FakeWriter):
            async def drain(self) -> None:
                reader.feed_data(b"AT+DTUPN:E5000020000000\r\n")
                reader.feed_eof()

        writer = _ProbeWriter()
        pending = _PendingCollectorSocket(
            remote_ip="203.0.113.10",
            remote_port=41000,
            session_id="session-1",
            reader=reader,
            writer=writer,  # type: ignore[arg-type]
        )
        listener._pending_sockets[pending.remote_ip] = pending

        await listener._sniff_pending_socket(pending)

        self.assertEqual(bytes(writer.buffer), b"AT+DTUPN?\r\n")
        diagnostics = listener.session_inventory_diagnostics()
        self.assertEqual(diagnostics["pending_session_count"], 0)
        self.assertEqual(diagnostics["sessions"][0]["collector_identity_source"], "at_dtupn")
        self.assertIn("E5000020000000", listener._at_connections_by_pn)

    async def test_listener_active_probe_routes_silent_framed_session_to_pn_owner(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.register_session_protocol_owner("eybond_framed")
        listener.register_payload_pn_owner("E5000020000000")
        listener._remember_session(
            session_id="session-1",
            remote_ip="203.0.113.10",
            remote_port=41000,
        )
        reader = asyncio.StreamReader()

        class _ProbeWriter(_FakeWriter):
            async def drain(self) -> None:
                reader.feed_data(
                    build_collector_request(
                        1,
                        b"\x00\x02E5000020000000",
                        devcode=2376,
                        collector_addr=1,
                        fcode=2,
                    )
                )
                reader.feed_eof()

        writer = _ProbeWriter()
        pending = _PendingCollectorSocket(
            remote_ip="203.0.113.10",
            remote_port=41000,
            session_id="session-1",
            reader=reader,
            writer=writer,  # type: ignore[arg-type]
        )
        listener._pending_sockets[pending.remote_ip] = pending

        await listener._sniff_pending_socket(pending)

        written = bytes(writer.buffer)
        header = decode_header(written[:HEADER_SIZE])
        self.assertEqual(header.fcode, 2)
        self.assertEqual(written[HEADER_SIZE:header.total_len], b"\x02")
        diagnostics = listener.session_inventory_diagnostics()
        self.assertEqual(diagnostics["pending_session_count"], 0)
        self.assertEqual(diagnostics["sessions"][0]["collector_identity_source"], "fc2_parameter_2")
        self.assertIn("E5000020000000", listener._connections_by_pn)

    async def test_listener_does_not_active_probe_when_registered_protocols_are_mixed(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.register_session_protocol_owner("at_text")
        listener.register_session_protocol_owner("eybond_framed")
        listener._remember_session(
            session_id="session-1",
            remote_ip="203.0.113.10",
            remote_port=41000,
        )
        reader = asyncio.StreamReader()
        writer = _FakeWriter()
        pending = _PendingCollectorSocket(
            remote_ip="203.0.113.10",
            remote_port=41000,
            session_id="session-1",
            reader=reader,
            writer=writer,  # type: ignore[arg-type]
        )
        listener._pending_sockets[pending.remote_ip] = pending

        await listener._sniff_pending_socket(pending)

        self.assertEqual(bytes(writer.buffer), b"")
        self.assertIn(pending.remote_ip, listener._pending_sockets)
        self.assertEqual(
            listener.session_inventory_diagnostics()["sessions"][0]["state"],
            "waiting_for_identity",
        )

    async def test_listener_routes_two_silent_at_collectors_from_same_peer_ip_by_pn(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.register_session_protocol_owner("at_text")
        listener.register_at_pn_owner("PN-ONE")
        listener.register_at_pn_owner("PN-TWO")

        async def _run_pending(session_id: str, pn: str, port: int) -> _FakeWriter:
            listener._remember_session(
                session_id=session_id,
                remote_ip="203.0.113.10",
                remote_port=port,
            )
            reader = asyncio.StreamReader()

            class _ProbeWriter(_FakeWriter):
                async def drain(self) -> None:
                    reader.feed_data(f"AT+DTUPN:{pn}\r\n".encode("ascii"))
                    reader.feed_eof()

            writer = _ProbeWriter()
            pending = _PendingCollectorSocket(
                remote_ip="203.0.113.10",
                remote_port=port,
                session_id=session_id,
                reader=reader,
                writer=writer,  # type: ignore[arg-type]
            )
            listener._pending_sockets[session_id] = pending
            await listener._sniff_pending_socket(pending)
            return writer

        first_writer = await _run_pending("session-1", "PN-ONE", 41001)
        second_writer = await _run_pending("session-2", "PN-TWO", 41002)

        self.assertEqual(bytes(first_writer.buffer), b"AT+DTUPN?\r\n")
        self.assertEqual(bytes(second_writer.buffer), b"AT+DTUPN?\r\n")
        self.assertIn("PN-ONE", listener._at_connections_by_pn)
        self.assertIn("PN-TWO", listener._at_connections_by_pn)
        self.assertIsNot(
            listener._at_connections_by_pn["PN-ONE"],
            listener._at_connections_by_pn["PN-TWO"],
        )
        diagnostics = listener.session_inventory_diagnostics()
        self.assertEqual(diagnostics["pending_session_count"], 0)
        self.assertEqual(diagnostics["recent_session_count"], 2)
        self.assertEqual(diagnostics["duplicate_peer_ip_count"], 1)

    async def test_bind_failure_rolls_back_shared_listener_registry(self) -> None:
        port = 19099
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )
        key = ("127.0.0.1", port)

        with patch(
            "custom_components.eybond_local.collector.transport.asyncio.start_server",
            new=AsyncMock(side_effect=OSError("could not bind on any address")),
        ):
            with self.assertRaises(CollectorListenerBindError):
                await transport.start()

        self.assertIsNone(transport._listener)
        self.assertNotIn(key, _LISTENERS)

    async def test_transport_stop_releases_listener_even_when_cancelled(self) -> None:
        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )

        await transport.start()
        listener = transport._listener
        self.assertIsNotNone(listener)
        assert listener is not None

        release_started = asyncio.Event()
        original_release = listener.release

        async def _slow_release() -> bool:
            release_started.set()
            await asyncio.sleep(0.01)
            return await original_release()

        key = ("127.0.0.1", port)
        try:
            with patch.object(listener, "release", new=_slow_release):
                stop_task = asyncio.create_task(transport.stop())
                await asyncio.wait_for(release_started.wait(), timeout=1.0)
                stop_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await stop_task

            self.assertIsNone(listener._server)
            self.assertNotIn(key, _LISTENERS)
            self.assertIsNone(transport._listener)
        finally:
            leaked = _LISTENERS.get(key)
            if leaked is not None:
                await leaked.release()
                _LISTENERS.pop(key, None)

    async def test_transport_routes_by_collector_ip(self) -> None:
        port = _free_tcp_port()
        targeted = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )
        other = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.2",
        )

        await targeted.start()
        await other.start()

        writer = None
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"\x00")
            await writer.drain()
            self.assertTrue(await targeted.wait_until_connected(1.0))
            self.assertFalse(await other.wait_until_connected(0.2))
            self.assertTrue(targeted.connected)
            self.assertFalse(other.connected)
        finally:
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            await targeted.stop()
            await other.stop()

    async def test_targeted_transport_stop_closes_only_own_shared_connection(self) -> None:
        port = _free_tcp_port()
        removed = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )
        remaining = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.2",
        )

        await removed.start()
        await remaining.start()
        listener = removed._listener
        self.assertIsNotNone(listener)
        assert listener is not None

        removed_connection = _CollectorConnection(
            remote_ip_hint="127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        removed_writer = _FakeWriter()
        removed_connection._writer = removed_writer  # type: ignore[assignment]
        remaining_connection = _CollectorConnection(
            remote_ip_hint="127.0.0.2",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        remaining_writer = _FakeWriter()
        remaining_connection._writer = remaining_writer  # type: ignore[assignment]
        listener._connections["127.0.0.1"] = removed_connection
        listener._connections["127.0.0.2"] = remaining_connection

        try:
            await removed.stop()

            self.assertTrue(removed_writer.closed)
            self.assertFalse(remaining_writer.closed)
            self.assertNotIn("127.0.0.1", listener._connections)
            self.assertIn("127.0.0.2", listener._connections)
            self.assertIn(("127.0.0.1", port), _LISTENERS)
        finally:
            await remaining.stop()

    async def test_targeted_transport_stop_closes_own_pending_socket(self) -> None:
        port = _free_tcp_port()
        removed = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )
        remaining = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.2",
        )

        await removed.start()
        await remaining.start()
        listener = removed._listener
        self.assertIsNotNone(listener)
        assert listener is not None

        pending_writer = _FakeWriter()
        listener._pending_sockets["127.0.0.1"] = _PendingCollectorSocket(
            remote_ip="127.0.0.1",
            reader=asyncio.StreamReader(),
            writer=pending_writer,  # type: ignore[arg-type]
        )

        try:
            await removed.stop()

            self.assertTrue(pending_writer.closed)
            self.assertNotIn("127.0.0.1", listener._pending_sockets)
            self.assertIn(("127.0.0.1", port), _LISTENERS)
        finally:
            await remaining.stop()

    async def test_unowned_callback_does_not_create_orphan_connection_on_targeted_listener(self) -> None:
        port = _free_tcp_port()
        remaining = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.2",
        )

        await remaining.start()
        listener = remaining._listener
        self.assertIsNotNone(listener)
        assert listener is not None

        reader = writer = None
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"\x00")
            await writer.drain()

            self.assertEqual(await asyncio.wait_for(reader.read(), timeout=1.0), b"")
            self.assertNotIn("127.0.0.1", listener._connections)
            self.assertFalse(await remaining.wait_until_connected(0.05))
            self.assertTrue(listener._server is not None)
        finally:
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            await remaining.stop()

    async def test_targeted_at_transport_stop_closes_only_own_at_connection(self) -> None:
        port = _free_tcp_port()
        removed = SharedCollectorAtTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            collector_ip="127.0.0.1",
        )
        remaining = SharedCollectorAtTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            collector_ip="127.0.0.2",
        )

        await removed.start()
        await remaining.start()
        listener = removed._listener
        self.assertIsNotNone(listener)
        assert listener is not None

        removed_connection = _CollectorAtConnection(
            remote_ip_hint="127.0.0.1",
            write_timeout=0.5,
        )
        removed_writer = _FakeWriter()
        removed_connection._writer = removed_writer  # type: ignore[assignment]
        remaining_connection = _CollectorAtConnection(
            remote_ip_hint="127.0.0.2",
            write_timeout=0.5,
        )
        remaining_writer = _FakeWriter()
        remaining_connection._writer = remaining_writer  # type: ignore[assignment]
        listener._at_connections["127.0.0.1"] = removed_connection
        listener._at_connections["127.0.0.2"] = remaining_connection

        try:
            await removed.stop()

            self.assertTrue(removed_writer.closed)
            self.assertFalse(remaining_writer.closed)
            self.assertNotIn("127.0.0.1", listener._at_connections)
            self.assertIn("127.0.0.2", listener._at_connections)
            self.assertIn(("127.0.0.1", port), _LISTENERS)
        finally:
            await remaining.stop()

    async def test_targeted_transport_can_disconnect_its_new_shared_connection(self) -> None:
        port = _free_tcp_port()
        owner = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )
        targeted = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )

        connected = asyncio.Event()
        disconnected = asyncio.Event()

        async def _collector_client() -> None:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connected.set()
            try:
                writer.write(b"\x00")
                await writer.drain()
                self.assertEqual(await reader.read(1), b"")
                disconnected.set()
            finally:
                writer.close()
                await writer.wait_closed()

        async def _quiet_heartbeat(self) -> None:
            return None

        client_task: asyncio.Task[None] | None = None
        with patch.object(_CollectorConnection, "_heartbeat_loop", new=_quiet_heartbeat):
            await owner.start()
            snapshot = await targeted.async_snapshot_shared_connection()
            await targeted.start()
            try:
                client_task = asyncio.create_task(_collector_client())
                await asyncio.wait_for(connected.wait(), timeout=1.0)
                self.assertTrue(await targeted.wait_until_connected(0.2))
                self.assertTrue(owner.connected)

                await targeted.async_disconnect_if_new_shared_connection(snapshot)

                await asyncio.wait_for(disconnected.wait(), timeout=0.5)
                self.assertFalse(owner.connected)
                self.assertIsNotNone(owner._listener)
            finally:
                await targeted.stop()
                await owner.stop()
                if client_task is not None:
                    await client_task

    async def test_wait_until_connected_activates_pending_socket(self) -> None:
        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )

        connected = asyncio.Event()

        async def _collector_client() -> None:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connected.set()
            try:
                header = decode_header(await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=1.0))
                payload = await asyncio.wait_for(reader.readexactly(header.payload_len), timeout=1.0)
                self.assertEqual(header.fcode, 2)
                self.assertEqual(payload, b"\x05")
                writer.write(
                    build_collector_request(
                        header.tid,
                        b"OK",
                        devcode=header.devcode,
                        collector_addr=header.devaddr,
                        fcode=header.fcode,
                    )
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async def _quiet_heartbeat(self) -> None:
            return None

        with patch.object(_CollectorConnection, "_heartbeat_loop", new=_quiet_heartbeat):
            await transport.start()
            client_task = asyncio.create_task(_collector_client())
            try:
                await asyncio.wait_for(connected.wait(), timeout=1.0)
                self.assertTrue(await transport.wait_until_connected(0.2))
                self.assertTrue(transport.connected)
                header, payload = await transport.async_send_collector(fcode=2, payload=b"\x05")
                self.assertEqual(header.fcode, 2)
                self.assertEqual(payload, b"OK")
                self.assertTrue(transport.connected)
            finally:
                await client_task
                await transport.stop()

    async def test_sniffed_initial_heartbeat_preserves_frame_order_for_follow_up_queries(self) -> None:
        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )

        connected = asyncio.Event()
        heartbeat_sent = asyncio.Event()

        async def _collector_client() -> None:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connected.set()
            try:
                writer.write(
                    build_collector_request(
                        1,
                        b"E5000099990003",
                        devcode=0x0994,
                        collector_addr=1,
                        fcode=1,
                    )
                )
                await writer.drain()
                heartbeat_sent.set()

                header = decode_header(await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=1.0))
                payload = await asyncio.wait_for(reader.readexactly(header.payload_len), timeout=1.0)
                self.assertEqual(header.fcode, 2)
                self.assertEqual(payload, b"\x05")
                writer.write(
                    build_collector_request(
                        header.tid,
                        b"\x01\x05",
                        devcode=header.devcode,
                        collector_addr=header.devaddr,
                        fcode=header.fcode,
                    )
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async def _quiet_heartbeat(self) -> None:
            return None

        with patch.object(_CollectorConnection, "_heartbeat_loop", new=_quiet_heartbeat):
            await transport.start()
            client_task = asyncio.create_task(_collector_client())
            try:
                await asyncio.wait_for(connected.wait(), timeout=1.0)
                await asyncio.wait_for(heartbeat_sent.wait(), timeout=1.0)
                await asyncio.sleep(0.05)
                self.assertTrue(await transport.wait_until_connected(0.2))
                self.assertTrue(await transport.wait_until_heartbeat(0.2))
                self.assertEqual(transport.collector_info.collector_pn, "E5000099990003")

                header, payload = await transport.async_send_collector(
                    fcode=2,
                    payload=b"\x05",
                    devcode=1,
                    collector_addr=1,
                )
                self.assertEqual(header.fcode, 2)
                self.assertEqual(payload, b"\x01\x05")
            finally:
                await client_task
                await transport.stop()

    async def test_wait_until_heartbeat_activates_pending_socket(self) -> None:
        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )

        connected = asyncio.Event()
        release = asyncio.Event()

        async def _collector_client() -> None:
            _reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connected.set()
            try:
                await asyncio.wait_for(release.wait(), timeout=1.0)
            finally:
                writer.close()
                await writer.wait_closed()

        async def _quiet_heartbeat(self) -> None:
            return None

        with patch.object(_CollectorConnection, "_heartbeat_loop", new=_quiet_heartbeat):
            await transport.start()
            client_task = asyncio.create_task(_collector_client())
            try:
                await asyncio.wait_for(connected.wait(), timeout=1.0)
                self.assertTrue(await transport.wait_until_connected(0.2))
                self.assertTrue(transport.connected)
                self.assertFalse(await transport.wait_until_heartbeat(0.02))
                self.assertTrue(transport.connected)
            finally:
                release.set()
                await client_task
                await transport.stop()

    async def test_listener_aliases_single_public_placeholder_for_hairpin_callback(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        placeholder = listener.ensure_connection(
            "93.184.216.34",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        aliased = listener._resolve_public_placeholder_alias("192.168.1.1")

        self.assertIs(aliased, placeholder)
        self.assertIs(listener._connections["93.184.216.34"], placeholder)
        self.assertIs(listener._connections["192.168.1.1"], placeholder)

    async def test_listener_skips_hairpin_alias_when_public_placeholders_are_ambiguous(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.ensure_connection(
            "93.184.216.34",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        listener.ensure_connection(
            "1.1.1.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        aliased = listener._resolve_public_placeholder_alias("192.168.1.1")

        self.assertIsNone(aliased)
        self.assertNotIn("192.168.1.1", listener._connections)

    async def test_listener_aliases_single_default_broadcast_placeholder_for_callback(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        placeholder = listener.ensure_connection(
            "192.168.1.255",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        aliased = listener._resolve_public_placeholder_alias("192.168.1.55")

        self.assertIs(aliased, placeholder)
        self.assertIs(listener._connections["192.168.1.255"], placeholder)
        self.assertIs(listener._connections["192.168.1.55"], placeholder)

    async def test_listener_does_not_alias_private_gateway_callback_to_collector_ip(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.ensure_connection(
            "192.168.1.55",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        aliased = listener._resolve_public_placeholder_alias("192.168.1.1")

        self.assertIsNone(aliased)
        self.assertNotIn("192.168.1.1", listener._connections)

    async def test_listener_pops_one_pending_socket_for_default_broadcast_placeholder_when_multiple_callbacks_arrive(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.ensure_connection(
            "192.168.1.255",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        first = _PendingCollectorSocket(
            remote_ip="192.168.1.55",
            reader=asyncio.StreamReader(),
            writer=object(),
        )
        second = _PendingCollectorSocket(
            remote_ip="192.168.1.14",
            reader=asyncio.StreamReader(),
            writer=object(),
        )
        listener._pending_sockets[first.remote_ip] = first
        listener._pending_sockets[second.remote_ip] = second
        listener._last_pending_ip = second.remote_ip

        selected = listener.pop_pending_socket("192.168.1.255")

        self.assertIs(selected, second)
        self.assertIn(first.remote_ip, listener._pending_sockets)
        self.assertNotIn(second.remote_ip, listener._pending_sockets)

    async def test_listener_matching_callback_ips_returns_connected_and_pending_broadcast_matches(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.ensure_connection(
            "192.168.1.255",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        connected = listener.ensure_connection(
            "192.168.1.55",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        class _OpenWriter:
            def is_closing(self) -> bool:
                return False

        connected._writer = _OpenWriter()  # type: ignore[assignment]
        listener._pending_sockets["192.168.1.14"] = _PendingCollectorSocket(
            remote_ip="192.168.1.14",
            reader=asyncio.StreamReader(),
            writer=object(),
        )

        self.assertEqual(
            set(listener.matching_callback_ips("192.168.1.255")),
            {"192.168.1.55", "192.168.1.14"},
        )

    async def test_listener_current_connection_returns_none_when_multiple_active_connections_exist(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        first = listener.ensure_connection(
            "127.0.0.1",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )
        second = listener.ensure_connection(
            "127.0.0.2",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        class _OpenWriter:
            def is_closing(self) -> bool:
                return False

        first._writer = _OpenWriter()  # type: ignore[assignment]
        second._writer = _OpenWriter()  # type: ignore[assignment]
        listener._last_connection_ip = "127.0.0.2"

        self.assertIsNone(listener.current_connection(heartbeat_interval=60.0, write_timeout=0.5))

    async def test_listener_current_at_connection_returns_none_when_multiple_active_connections_exist(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        first = listener.ensure_at_connection(
            "127.0.0.1",
            write_timeout=0.5,
        )
        second = listener.ensure_at_connection(
            "127.0.0.2",
            write_timeout=0.5,
        )

        class _OpenWriter:
            def is_closing(self) -> bool:
                return False

        first._writer = _OpenWriter()  # type: ignore[assignment]
        second._writer = _OpenWriter()  # type: ignore[assignment]
        listener._last_at_connection_ip = "127.0.0.2"

        self.assertIsNone(listener.current_at_connection(write_timeout=0.5))

    async def test_listener_pop_pending_socket_without_collector_ip_returns_none_when_multiple_pending_exist(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener._pending_sockets["127.0.0.1"] = _PendingCollectorSocket(
            remote_ip="127.0.0.1",
            reader=asyncio.StreamReader(),
            writer=object(),
        )
        listener._pending_sockets["127.0.0.2"] = _PendingCollectorSocket(
            remote_ip="127.0.0.2",
            reader=asyncio.StreamReader(),
            writer=object(),
        )
        listener._last_pending_ip = "127.0.0.2"

        self.assertIsNone(listener.pop_pending_socket(""))

    async def test_transport_without_collector_ip_rejects_ambiguous_pending_send(self) -> None:
        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="",
        )

        class _IdleWriter:
            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        await transport.start()
        try:
            listener = transport._listener
            assert listener is not None
            listener._pending_sockets["127.0.0.1"] = _PendingCollectorSocket(
                remote_ip="127.0.0.1",
                reader=asyncio.StreamReader(),
                writer=_IdleWriter(),
            )
            listener._pending_sockets["127.0.0.2"] = _PendingCollectorSocket(
                remote_ip="127.0.0.2",
                reader=asyncio.StreamReader(),
                writer=_IdleWriter(),
            )

            with self.assertRaisesRegex(ConnectionError, "collector_not_connected"):
                await transport.async_send_collector(fcode=2, payload=b"\x05")
        finally:
            await transport.stop()

    async def test_at_transport_without_collector_ip_rejects_ambiguous_pending_query(self) -> None:
        port = _free_tcp_port()
        transport = SharedCollectorAtTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            collector_ip="",
        )

        class _IdleWriter:
            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        await transport.start()
        try:
            listener = transport._listener
            assert listener is not None
            listener._pending_sockets["127.0.0.1"] = _PendingCollectorSocket(
                remote_ip="127.0.0.1",
                reader=asyncio.StreamReader(),
                writer=_IdleWriter(),
            )
            listener._pending_sockets["127.0.0.2"] = _PendingCollectorSocket(
                remote_ip="127.0.0.2",
                reader=asyncio.StreamReader(),
                writer=_IdleWriter(),
            )

            with self.assertRaisesRegex(ConnectionError, "collector_not_connected"):
                await transport.async_query("WFSS")
        finally:
            await transport.stop()

    async def test_listener_uses_hairpin_alias_during_connection_handling(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        placeholder = listener.ensure_connection(
            "93.184.216.34",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        class _FakeWriter:
            def get_extra_info(self, name: str):
                if name == "peername":
                    return ("192.168.1.1", 12345)
                return None

            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        reader = asyncio.StreamReader()
        reader.feed_data(b"\x00")

        with patch.object(placeholder, "run", new=AsyncMock()) as run_mock:
            await listener._handle_connection(reader, _FakeWriter())
            await asyncio.sleep(0)

        run_mock.assert_awaited_once()
        self.assertEqual(listener._last_connection_ip, "192.168.1.1")
        self.assertIs(listener._connections["192.168.1.1"], placeholder)

    async def test_listener_uses_default_broadcast_alias_during_connection_handling(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        placeholder = listener.ensure_connection(
            "192.168.1.255",
            heartbeat_interval=60.0,
            write_timeout=0.5,
        )

        class _FakeWriter:
            def get_extra_info(self, name: str):
                if name == "peername":
                    return ("192.168.1.55", 12345)
                return None

            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        reader = asyncio.StreamReader()
        reader.feed_data(b"\x00")

        with patch.object(placeholder, "run", new=AsyncMock()) as run_mock:
            await listener._handle_connection(reader, _FakeWriter())
            await asyncio.sleep(0)

        run_mock.assert_awaited_once()
        self.assertEqual(listener._last_connection_ip, "192.168.1.55")
        self.assertIs(listener._connections["192.168.1.55"], placeholder)

    async def test_proxy_capture_route_passes_matching_pending_socket_to_handler(self) -> None:
        handled_chunks: list[bytes] = []

        class _FakeWriter:
            def __init__(self) -> None:
                self.buffer = bytearray()
                self.closed = False

            def write(self, data: bytes) -> None:
                self.buffer.extend(data)

            async def drain(self) -> None:
                return None

            def close(self) -> None:
                self.closed = True

            async def wait_closed(self) -> None:
                return None

        class _FakeListener:
            def __init__(self, pending: _PendingCollectorSocket) -> None:
                self._pending = pending
                self.calls: list[tuple[str, str, str]] = []

            async def pop_pending_socket_for_route(
                self,
                *,
                collector_ip: str = "",
                collector_pn: str = "",
                session_protocol: str = "",
            ) -> _PendingCollectorSocket | None:
                self.calls.append((collector_ip, collector_pn, session_protocol))
                pending = self._pending
                self._pending = None  # type: ignore[assignment]
                return pending

        reader = asyncio.StreamReader()
        reader.feed_data(b"ping")
        reader.feed_eof()
        writer = _FakeWriter()
        pending = _PendingCollectorSocket(
            remote_ip="127.0.0.1",
            reader=reader,
            writer=writer,  # type: ignore[arg-type]
        )

        async def _handler(
            pending_reader: asyncio.StreamReader,
            pending_writer: asyncio.StreamWriter,
        ) -> None:
            handled_chunks.append(await pending_reader.readexactly(4))
            pending_writer.write(b"pong")
            await pending_writer.drain()
            route._running = False
            pending_writer.close()
            await pending_writer.wait_closed()

        route = SharedProxyCaptureRoute(
            host="127.0.0.1",
            port=8899,
            collector_ip="127.0.0.1",
            handler=_handler,
        )
        route._listener = _FakeListener(pending)  # type: ignore[assignment]
        route._running = True

        await route._route_loop()

        self.assertEqual(handled_chunks, [b"ping"])
        self.assertEqual(bytes(writer.buffer), b"pong")
        self.assertTrue(writer.closed)

    async def test_proxy_capture_route_selects_same_peer_pending_by_collector_pn(self) -> None:
        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener._remember_session(
            session_id="session-one",
            remote_ip="203.0.113.10",
            remote_port=41001,
        )
        listener._remember_session(
            session_id="session-two",
            remote_ip="203.0.113.10",
            remote_port=41002,
        )

        first_reader = asyncio.StreamReader()
        second_reader = asyncio.StreamReader()

        class _ProbeWriter(_FakeWriter):
            def __init__(self, reader: asyncio.StreamReader, pn: str) -> None:
                super().__init__()
                self._reader = reader
                self._pn = pn

            async def drain(self) -> None:
                self._reader.feed_data(f"AT+DTUPN:{self._pn}\r\n".encode("ascii"))
                self._reader.feed_eof()

        first_pending = _PendingCollectorSocket(
            remote_ip="203.0.113.10",
            remote_port=41001,
            session_id="session-one",
            reader=first_reader,
            writer=_ProbeWriter(first_reader, "PN-ONE"),  # type: ignore[arg-type]
        )
        second_pending = _PendingCollectorSocket(
            remote_ip="203.0.113.10",
            remote_port=41002,
            session_id="session-two",
            reader=second_reader,
            writer=_ProbeWriter(second_reader, "PN-TWO"),  # type: ignore[arg-type]
        )
        listener._pending_sockets["session-one"] = first_pending
        listener._pending_sockets["session-two"] = second_pending

        handled = asyncio.Event()
        handled_writer: _FakeWriter | None = None

        async def _handler(
            pending_reader: asyncio.StreamReader,
            pending_writer: asyncio.StreamWriter,
        ) -> None:
            nonlocal handled_writer
            handled_writer = pending_writer  # type: ignore[assignment]
            route._running = False
            handled.set()

        route = SharedProxyCaptureRoute(
            host="127.0.0.1",
            port=8899,
            collector_ip="203.0.113.10",
            collector_pn="PN-TWO",
            collector_session_protocol="at_text",
            handler=_handler,
        )
        route._listener = listener
        route._running = True

        await route._route_loop()

        self.assertTrue(handled.is_set())
        self.assertIs(handled_writer, second_pending.writer)
        self.assertIn("session-one", listener._pending_sockets)
        self.assertNotIn("session-two", listener._pending_sockets)
        self.assertEqual(first_pending.initial_bytes, b"")
        self.assertEqual(second_pending.initial_bytes, b"")
        self.assertEqual(
            listener.session_inventory_diagnostics()["pending_session_count"],
            1,
        )

    async def test_at_transport_wait_until_connected_activates_pending_socket(self) -> None:
        port = _free_tcp_port()
        at_transport = SharedCollectorAtTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            collector_ip="127.0.0.1",
        )

        connected = asyncio.Event()
        release = asyncio.Event()

        async def _collector_client() -> None:
            _reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connected.set()
            try:
                await asyncio.wait_for(release.wait(), timeout=1.0)
            finally:
                writer.close()
                await writer.wait_closed()

        async def _quiet_heartbeat(self) -> None:
            return None

        with patch.object(_CollectorConnection, "_heartbeat_loop", new=_quiet_heartbeat):
            await at_transport.start()
            client_task = asyncio.create_task(_collector_client())
            try:
                await asyncio.wait_for(connected.wait(), timeout=1.0)
                self.assertTrue(await at_transport.wait_until_connected(0.2))
                self.assertTrue(at_transport.connected)
            finally:
                release.set()
                await client_task
                await at_transport.stop()

    async def test_at_transport_queries_server_first_session_on_shared_listener(self) -> None:
        port = _free_tcp_port()
        framed = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )
        at_transport = SharedCollectorAtTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            collector_ip="127.0.0.1",
        )

        connected = asyncio.Event()
        release = asyncio.Event()

        async def _collector_client() -> None:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            connected.set()
            try:
                query = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=1.0)
                self.assertEqual(query, b"AT+WFSS?\r\n")
                writer.write(b"AT+WFSS:-55\r\n")
                await writer.drain()
                await asyncio.wait_for(release.wait(), timeout=1.0)
            finally:
                writer.close()
                await writer.wait_closed()

        async def _quiet_heartbeat(self) -> None:
            return None

        with patch.object(_CollectorConnection, "_heartbeat_loop", new=_quiet_heartbeat):
            await framed.start()
            await at_transport.start()
            client_task = asyncio.create_task(_collector_client())
            try:
                await asyncio.wait_for(connected.wait(), timeout=1.0)
                response = await at_transport.async_query("WFSS")
                self.assertEqual(response.command, "WFSS")
                self.assertEqual(response.value, "-55")
                self.assertTrue(framed.connected)
                self.assertTrue(at_transport.connected)
                self.assertTrue(await at_transport.wait_until_connected(0.2))
            finally:
                release.set()
                await client_task
                await at_transport.stop()
                await framed.stop()


if __name__ == "__main__":
    unittest.main()
