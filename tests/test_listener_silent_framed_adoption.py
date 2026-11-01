"""F8: silent same-IP redial after a framed session must enter framed run."""

from __future__ import annotations

import asyncio
import socket
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE,
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.transport import (
    SharedEybondTransport,
    _LISTENERS,
    _PendingCollectorSocket,
    _SharedEybondListener,
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
        self._wait_closed_gate: asyncio.Event | None = None

    def is_closing(self) -> bool:
        return self.closed

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        gate = self._wait_closed_gate
        if gate is not None:
            await gate.wait()
        self.closed = True

    def get_extra_info(self, name: str):
        if name == "peername":
            return ("203.0.113.10", 41000)
        return None


class SilentFramedAdoptionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        for key in tuple(_LISTENERS):
            listener = _LISTENERS.pop(key, None)
            if listener is None:
                continue
            listener._ref_count = 0
            await listener.release()

    async def test_silent_redial_after_framed_session_writes_heartbeat(self) -> None:
        """Real TCP: framed session ends, silent redial gets EyeBond heartbeat."""

        port = _free_tcp_port()
        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=port,
            request_timeout=1.0,
            heartbeat_interval=60.0,
            collector_ip="127.0.0.1",
        )
        await transport.start()
        first_writer = second_writer = None
        try:
            first_reader, first_writer = await asyncio.open_connection(
                "127.0.0.1", port
            )
            first_writer.write(
                build_collector_request(
                    1,
                    b"",
                    devcode=0x0994,
                    collector_addr=1,
                    fcode=4,
                )
            )
            await first_writer.drain()
            first_bytes = await asyncio.wait_for(first_reader.read(64), timeout=2.0)
            self.assertGreaterEqual(len(first_bytes), HEADER_SIZE)
            self.assertEqual(decode_header(first_bytes[:HEADER_SIZE]).fcode, 1)
            first_writer.close()
            await first_writer.wait_closed()
            first_writer = None

            # Allow the prior read-loop finally / disconnect to begin.
            await asyncio.sleep(0.05)

            second_reader, second_writer = await asyncio.open_connection(
                "127.0.0.1", port
            )
            # Peer stays silent — server must speak first (heartbeat).
            second_bytes = await asyncio.wait_for(second_reader.read(64), timeout=2.0)
            self.assertGreaterEqual(len(second_bytes), HEADER_SIZE)
            self.assertEqual(decode_header(second_bytes[:HEADER_SIZE]).fcode, 1)
            self.assertTrue(transport.connected)
        finally:
            if first_writer is not None:
                first_writer.close()
                await first_writer.wait_closed()
            if second_writer is not None:
                second_writer.close()
                await second_writer.wait_closed()
            await transport.stop()

    async def test_silent_adopt_while_prior_wait_closed_still_running(self) -> None:
        """New silent handshake must reach run without awaiting old wait_closed."""

        listener = _SharedEybondListener(host="127.0.0.1", port=_free_tcp_port())
        listener.register_payload_owner("203.0.113.10")

        slow_writer = _FakeWriter()
        slow_writer._wait_closed_gate = asyncio.Event()
        first_reader = asyncio.StreamReader()
        first_pending = _PendingCollectorSocket(
            session_id="session-old",
            remote_ip="203.0.113.10",
            remote_port=41000,
            reader=first_reader,
            writer=slow_writer,  # type: ignore[arg-type]
        )
        listener._remember_session(
            session_id=first_pending.session_id,
            remote_ip=first_pending.remote_ip,
            remote_port=41000,
        )
        listener._pending_sockets[first_pending.session_id] = first_pending
        first_sniff = asyncio.create_task(
            listener._sniff_pending_socket(first_pending)
        )
        first_pending.sniff_task = first_sniff

        # Seed framed bytes so the first socket enters framed run, then EOF.
        first_reader.feed_data(
            build_collector_request(
                1,
                b"",
                devcode=0x0994,
                collector_addr=1,
                fcode=4,
            )
        )
        await asyncio.sleep(0.15)
        self.assertIn("203.0.113.10", listener._connections)
        first_connection = listener._connections["203.0.113.10"]
        self.assertTrue(first_connection.connected)
        # EOF ends the read loop; disconnect will stall on wait_closed.
        first_reader.feed_eof()
        await asyncio.sleep(0.05)

        second_reader = asyncio.StreamReader()
        second_writer = _FakeWriter()
        second_pending = _PendingCollectorSocket(
            session_id="session-new",
            remote_ip="203.0.113.10",
            remote_port=41001,
            reader=second_reader,
            writer=second_writer,  # type: ignore[arg-type]
        )
        listener._remember_session(
            session_id=second_pending.session_id,
            remote_ip=second_pending.remote_ip,
            remote_port=41001,
        )
        listener._pending_sockets[second_pending.session_id] = second_pending
        second_sniff = asyncio.create_task(
            listener._sniff_pending_socket(second_pending)
        )
        second_pending.sniff_task = second_sniff

        async def _heartbeat_arrived() -> None:
            while True:
                if bytes(second_writer.buffer):
                    return
                await asyncio.sleep(0.02)

        await asyncio.wait_for(_heartbeat_arrived(), timeout=2.0)
        written = bytes(second_writer.buffer)
        self.assertGreaterEqual(len(written), HEADER_SIZE)
        self.assertEqual(decode_header(written[:HEADER_SIZE]).fcode, 1)
        self.assertEqual(
            listener._session_inventory["session-new"].state,
            "routed_framed",
        )

        second_reader.feed_eof()
        # Unblock the stalled first disconnect so cleanup can finish.
        assert slow_writer._wait_closed_gate is not None
        slow_writer._wait_closed_gate.set()
        await asyncio.wait_for(
            asyncio.gather(first_sniff, second_sniff, return_exceptions=True),
            timeout=3.0,
        )

    async def test_silent_socket_not_adopted_when_payload_and_at_owners_share_ip(
        self,
    ) -> None:
        """Shared NAT: both owners present → park/identity, not framed adopt."""

        listener = _SharedEybondListener(host="127.0.0.1", port=8899)
        listener.register_payload_owner("203.0.113.10")
        listener.register_at_owner("203.0.113.10")

        new_reader = asyncio.StreamReader()
        new_writer = _FakeWriter()
        new_pending = _PendingCollectorSocket(
            session_id="shared-nat-silent",
            remote_ip="203.0.113.10",
            remote_port=41002,
            reader=new_reader,
            writer=new_writer,  # type: ignore[arg-type]
        )
        listener._remember_session(
            session_id=new_pending.session_id,
            remote_ip=new_pending.remote_ip,
            remote_port=41002,
        )
        listener._pending_sockets[new_pending.session_id] = new_pending
        sniff = asyncio.create_task(listener._sniff_pending_socket(new_pending))
        new_pending.sniff_task = sniff

        await asyncio.sleep(0.4)
        self.assertEqual(bytes(new_writer.buffer), b"")
        self.assertFalse(sniff.done())
        self.assertNotIn("203.0.113.10", listener._connections)
        self.assertNotEqual(
            listener._session_inventory["shared-nat-silent"].state,
            "routed_framed",
        )

        sniff.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await sniff

    async def test_transparent_route_reservation_still_parks_silent_socket(
        self,
    ) -> None:
        """Payload ownership must not steal a transparent exclusive reservation."""

        listener = _SharedEybondListener(host="127.0.0.1", port=8899)
        listener.register_payload_owner("192.168.1.1")
        old_pending = _PendingCollectorSocket(
            session_id="baseline-session",
            remote_ip="192.168.1.1",
            reader=asyncio.StreamReader(),
            writer=_FakeWriter(),  # type: ignore[arg-type]
        )
        listener._pending_sockets[old_pending.session_id] = old_pending
        listener.register_exclusive_collector_route(
            collector_ip="192.168.1.55",
            collector_pn="E50000200000000001",
            transparent=True,
            expected_session_protocol="at_text",
        )

        new_reader = asyncio.StreamReader()
        new_writer = _FakeWriter()
        new_pending = _PendingCollectorSocket(
            session_id="fresh-session",
            remote_ip="192.168.1.1",
            reader=new_reader,
            writer=new_writer,  # type: ignore[arg-type]
        )
        listener._remember_session(
            session_id=new_pending.session_id,
            remote_ip=new_pending.remote_ip,
            remote_port=41000,
        )
        listener._pending_sockets[new_pending.session_id] = new_pending
        sniff = asyncio.create_task(listener._sniff_pending_socket(new_pending))
        new_pending.sniff_task = sniff

        await asyncio.sleep(0.4)
        self.assertEqual(bytes(new_writer.buffer), b"")
        self.assertFalse(sniff.done())
        self.assertNotIn("192.168.1.1", listener._connections)
        state = listener._session_inventory["fresh-session"].state
        self.assertIn(
            state,
            {"waiting_for_exclusive_route", "exclusive_route_silent"},
        )

        sniff.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await sniff


if __name__ == "__main__":
    unittest.main()
