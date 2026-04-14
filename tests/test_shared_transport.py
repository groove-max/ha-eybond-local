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
    SharedEybondTransport,
    _CollectorConnection,
    _SharedEybondListener,
)


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


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

        with patch.object(placeholder, "run", new=AsyncMock()) as run_mock:
            await listener._handle_connection(asyncio.StreamReader(), _FakeWriter())

        run_mock.assert_awaited_once()
        self.assertEqual(listener._last_connection_ip, "192.168.1.1")
        self.assertIs(listener._connections["192.168.1.1"], placeholder)


if __name__ == "__main__":
    unittest.main()