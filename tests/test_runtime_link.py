from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import CollectorInfo
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager


class _FakeTransport:
    def __init__(
        self,
        *,
        connected: bool = False,
        connect_result: bool = True,
        heartbeat_result: bool = True,
    ) -> None:
        self.connected = connected
        self.collector_info = CollectorInfo(remote_ip="192.168.1.14", collector_pn="PN123")
        self._connect_result = connect_result
        self._heartbeat_result = heartbeat_result
        self.connected_waits: list[float] = []
        self.heartbeat_waits: list[float] = []
        self.disconnect_calls = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def wait_until_connected(self, timeout: float) -> bool:
        self.connected_waits.append(timeout)
        if self._connect_result:
            self.connected = True
        return self._connect_result

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        self.heartbeat_waits.append(timeout)
        return self._heartbeat_result

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False


class _FakeAnnouncer:
    def __init__(self) -> None:
        self.last_reply = "set>server=192.168.1.10:8899;"
        self.last_reply_from = "192.168.1.14:58899"
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1


class RuntimeLinkManagerTests(unittest.TestCase):
    def _build_manager(self) -> EybondRuntimeLinkManager:
        with patch(
            "custom_components.eybond_local.runtime.link.resolve_server_ip",
            return_value="192.168.1.10",
        ):
            return EybondRuntimeLinkManager(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
            )

    def test_collector_info_merges_transport_and_discovery_state(self) -> None:
        manager = self._build_manager()
        manager._transport = _FakeTransport(connected=True)  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        collector = manager.collector_info

        self.assertEqual(collector.remote_ip, "192.168.1.14")
        self.assertEqual(collector.collector_pn, "PN123")
        self.assertEqual(collector.last_udp_reply, "set>server=192.168.1.10:8899;")
        self.assertEqual(collector.last_udp_reply_from, "192.168.1.14:58899")

    def test_async_try_connect_uses_discovery_then_stops_it(self) -> None:
        manager = self._build_manager()
        transport = _FakeTransport(connected=False, connect_result=True)
        announcer = _FakeAnnouncer()
        manager._transport = transport  # type: ignore[assignment]
        manager._announcer = announcer  # type: ignore[assignment]

        connected = asyncio.run(manager.async_try_connect(timeout=5.0, require_heartbeat=True))

        self.assertTrue(connected)
        self.assertEqual(announcer.start_calls, 1)
        self.assertEqual(announcer.stop_calls, 1)
        self.assertEqual(transport.connected_waits, [5.0])
        self.assertEqual(transport.heartbeat_waits, [1.5])

    def test_async_ensure_connected_raises_when_transport_never_connects(self) -> None:
        manager = self._build_manager()
        manager._transport = _FakeTransport(connected=False, connect_result=False)  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        with self.assertRaisesRegex(ConnectionError, "collector_not_connected"):
            asyncio.run(manager.async_ensure_connected(timeout=0.5))

    def test_async_try_connect_returns_false_when_heartbeat_is_missing(self) -> None:
        manager = self._build_manager()
        transport = _FakeTransport(connected=True, heartbeat_result=False)
        announcer = _FakeAnnouncer()
        manager._transport = transport  # type: ignore[assignment]
        manager._announcer = announcer  # type: ignore[assignment]

        connected = asyncio.run(manager.async_try_connect(timeout=5.0, require_heartbeat=True))

        self.assertFalse(connected)
        self.assertEqual(transport.heartbeat_waits, [1.5])
        self.assertEqual(announcer.start_calls, 1)
        self.assertEqual(announcer.stop_calls, 0)

    def test_async_ensure_connected_raises_when_heartbeat_times_out(self) -> None:
        manager = self._build_manager()
        manager._transport = _FakeTransport(connected=True, heartbeat_result=False)  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        with self.assertRaisesRegex(ConnectionError, "collector_heartbeat_timeout"):
            asyncio.run(manager.async_ensure_connected(timeout=5.0, require_heartbeat=True))

    def test_async_reset_connection_disconnects_transport_and_restarts_discovery(self) -> None:
        manager = self._build_manager()
        transport = _FakeTransport(connected=True)
        announcer = _FakeAnnouncer()
        manager._transport = transport  # type: ignore[assignment]
        manager._announcer = announcer  # type: ignore[assignment]

        asyncio.run(manager.async_reset_connection(reason="request_timeout"))

        self.assertEqual(transport.disconnect_calls, 1)
        self.assertEqual(announcer.start_calls, 1)

    def test_runtime_manager_uses_bind_ip_for_advertised_endpoint_when_override_is_empty(self) -> None:
        manager = self._build_manager()

        self.assertEqual(manager._announcer._advertised_server_ip, "192.168.1.10")
        self.assertEqual(manager._announcer._advertised_server_port, 8899)


if __name__ == "__main__":
    unittest.main()
