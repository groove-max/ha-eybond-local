from __future__ import annotations

import asyncio
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch


def _fake_probe():
    """Return an AsyncMock standing in for the one-shot UDP callback trigger."""

    return AsyncMock(return_value=types.SimpleNamespace(reply="", reply_from=""))


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.discovery import DiscoveryProbeResult
from custom_components.eybond_local.connection.session_registry import CallbackSessionRegistry
from custom_components.eybond_local.models import CollectorInfo
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager, resolve_server_ip


class _FakeTransport:
    def __init__(
        self,
        *,
        connected: bool = False,
        connect_result: bool = True,
        heartbeat_result: bool = True,
        remote_ip: str = "192.168.1.14",
        observed_sessions: tuple = (),
    ) -> None:
        self.connected = connected
        self.collector_info = CollectorInfo(remote_ip=remote_ip, collector_pn="PN123")
        self._connect_result = connect_result
        self._heartbeat_result = heartbeat_result
        self._observed_sessions = tuple(dict(s) for s in observed_sessions)
        self.connected_waits: list[float] = []
        self.heartbeat_waits: list[float] = []
        self.disconnect_calls = 0
        self.start_calls = 0
        self.stop_calls = 0

    @property
    def listener_key(self) -> str:
        return "fake:0"

    def observed_collector_sessions(self) -> tuple:
        return self._observed_sessions

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1

    async def wait_until_connected(self, timeout: float) -> bool:
        self.connected_waits.append(timeout)
        if self._connect_result:
            self.connected = True
        return self._connect_result

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        self.heartbeat_waits.append(timeout)
        self.collector_info.heartbeat_fresh = self._heartbeat_result
        return self._heartbeat_result

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    def session_inventory_diagnostics(self) -> dict[str, object]:
        return {
            "pending_session_count": 2,
            "recent_session_count": 3,
            "duplicate_peer_ip_count": 1,
            "duplicate_peer_ips": ["203.0.113.10"],
            "sessions": [
                {
                    "session_id": "listener-8899-1",
                    "peer_ip": "203.0.113.10",
                    "state": "pending",
                    "protocol_shape": "unknown",
                    "first_bytes_len": 0,
                }
            ],
        }


class _FakeAnnouncer:
    def __init__(self, *, running: bool = False) -> None:
        self.last_reply = "set>server=192.168.1.10:8899;"
        self.last_reply_from = "192.168.1.14:58899"
        self.running = running
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.running = True
        self.start_calls += 1

    async def stop(self) -> None:
        self.running = False
        self.stop_calls += 1


class RuntimeLinkManagerTests(unittest.TestCase):
    def test_resolve_server_ip_uses_busybox_ip_o_fallback(self) -> None:
        side_effects = [
            subprocess.CalledProcessError(1, ["ip", "-j", "-4", "addr", "show", "up"]),
            "1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever preferred_lft forever\n"
            "2: end0    inet 192.168.1.104/24 brd 192.168.1.255 scope global dynamic noprefixroute end0\\       valid_lft 41807sec preferred_lft 41807sec\n"
            "3: wlan0    inet 192.168.88.92/24 brd 192.168.88.255 scope global dynamic noprefixroute wlan0\\       valid_lft 5809sec preferred_lft 5809sec\n",
        ]

        with patch(
            "custom_components.eybond_local.runtime.link.subprocess.check_output",
            side_effect=side_effects,
        ), patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="192.168.1.104",
        ):
            resolved = resolve_server_ip(
                "192.168.88.91",
                collector_ip="192.168.88.88",
            )

        self.assertEqual(resolved, "192.168.88.92")

    def test_resolve_server_ip_prefers_active_ip_on_collector_subnet(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link._active_ipv4_interfaces",
            return_value=(("192.168.1.104", 24), ("192.168.88.92", 24)),
        ), patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="192.168.1.104",
        ):
            resolved = resolve_server_ip(
                "192.168.88.91",
                collector_ip="192.168.88.88",
            )

        self.assertEqual(resolved, "192.168.88.92")

    def test_resolve_server_ip_keeps_same_subnet_config_for_ap_mode(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link._active_ipv4_interfaces",
            return_value=(("192.168.1.104", 24),),
        ), patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="192.168.1.104",
        ):
            resolved = resolve_server_ip(
                "192.168.88.92",
                collector_ip="192.168.88.88",
            )

        self.assertEqual(resolved, "192.168.88.92")

    def test_resolve_server_ip_tolerates_blocked_socket_fallback(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link._active_ipv4_interfaces",
            return_value=(),
        ), patch(
            "custom_components.eybond_local.runtime.link.socket.socket",
            side_effect=RuntimeError("socket probe blocked"),
        ):
            resolved = resolve_server_ip(
                "192.168.88.95",
                collector_ip="192.168.88.89",
            )

        self.assertEqual(resolved, "192.168.88.95")

    def _build_manager(self, *, collector_ip: str = "192.168.1.14") -> EybondRuntimeLinkManager:
        with patch(
            "custom_components.eybond_local.runtime.link.resolve_server_ip",
            return_value="192.168.1.10",
        ):
            return EybondRuntimeLinkManager(
                server_ip="192.168.1.10",
                collector_ip=collector_ip,
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
            )

    def test_runtime_manager_binds_tcp_wildcard_and_advertises_resolved_ip(self) -> None:
        manager = self._build_manager()

        self.assertEqual(manager.listener_bind_host, "0.0.0.0")
        self.assertEqual(manager._transport._host, "0.0.0.0")
        self.assertEqual(manager._at_transport._host, "0.0.0.0")
        self.assertEqual(manager.effective_server_ip, "192.168.1.10")
        self.assertEqual(manager.effective_advertised_server_ip, "192.168.1.10")
        self.assertEqual(manager._announcer._bind_ip, "192.168.1.10")
        self.assertEqual(manager._announcer._advertised_server_ip, "192.168.1.10")

        diagnostics = manager.listener_diagnostics()
        self.assertEqual(diagnostics["collector_listener_status"], "stopped")
        self.assertEqual(diagnostics["collector_listener_bind_endpoint"], "0.0.0.0:8899")
        self.assertEqual(
            diagnostics["collector_listener_advertised_endpoint"],
            "192.168.1.10:8899",
        )

    def test_listener_diagnostics_include_callback_session_inventory(self) -> None:
        manager = self._build_manager()
        manager._collector_session_protocol = "at_text"
        manager._collector_identity_strategy = "at_dtupn"
        transport = _FakeTransport(connected=False)
        transport._listener = object()  # type: ignore[attr-defined]
        manager._transport = transport  # type: ignore[assignment]

        diagnostics = manager.listener_diagnostics()

        self.assertEqual(diagnostics["collector_callback_session_protocol"], "at_text")
        self.assertEqual(diagnostics["collector_callback_identity_strategy"], "at_dtupn")
        self.assertEqual(diagnostics["collector_callback_pending_session_count"], 2)
        self.assertEqual(diagnostics["collector_callback_recent_session_count"], 3)
        self.assertEqual(diagnostics["collector_callback_duplicate_peer_ip_count"], 1)
        self.assertEqual(diagnostics["collector_callback_identity_status"], "unresolved")
        self.assertEqual(diagnostics["collector_callback_unresolved_session_count"], 1)
        self.assertIn(
            "Multiple collector callbacks share the same peer IP",
            diagnostics["collector_callback_identity_summary"],
        )
        self.assertEqual(
            diagnostics["collector_callback_duplicate_peer_ips"],
            "203.0.113.10",
        )
        self.assertEqual(
            diagnostics["collector_callback_session_inventory"],
            [
                {
                    "session_id": "listener-8899-1",
                    "peer_ip": "203.0.113.10",
                    "state": "pending",
                    "protocol_shape": "unknown",
                    "first_bytes_len": 0,
                }
            ],
        )

    def test_at_text_connect_uses_at_transport_without_payload_heartbeat(self) -> None:
        async def _run() -> None:
            manager = self._build_manager()
            manager._collector_session_protocol = "at_text"
            payload = _FakeTransport(connected=False, connect_result=False)
            at_transport = _FakeTransport(connected=False, connect_result=True)
            manager._transport = payload  # type: ignore[assignment]
            manager._at_transport = at_transport  # type: ignore[assignment]
            manager._announcer = _FakeAnnouncer()
            manager._started = True
            manager._listener_status = "listening"

            ok = await manager.async_try_connect(timeout=0.5, require_heartbeat=True)

            self.assertTrue(ok)
            self.assertTrue(manager.connected)
            self.assertIs(manager.transport, at_transport)
            self.assertEqual(payload.connected_waits, [])
            self.assertEqual(at_transport.connected_waits, [0.5])
            self.assertEqual(at_transport.heartbeat_waits, [])

        asyncio.run(_run())

    def test_reconcile_collector_session_profile_rebuilds_started_link(self) -> None:
        async def _run() -> None:
            manager = self._build_manager()
            manager._started = True
            manager._listener_status = "listening"
            manager._announcer = _FakeAnnouncer(running=True)  # type: ignore[assignment]
            manager._reverse_discovery_enabled = False

            with patch.object(manager, "_stop_all_transports", new=AsyncMock()) as stop_all, patch.object(
                manager,
                "_start_all_transports",
                new=AsyncMock(),
            ) as start_all:
                changed = await manager.async_reconcile_collector_session_profile(
                    collector_session_protocol="at_text",
                    collector_identity_strategy="at_dtupn",
                    reason="test",
                )

            self.assertTrue(changed)
            stop_all.assert_awaited_once()
            start_all.assert_awaited_once()
            self.assertEqual(manager.listener_diagnostics()["collector_callback_session_protocol"], "at_text")
            self.assertEqual(manager.listener_diagnostics()["collector_callback_identity_strategy"], "at_dtupn")
            self.assertTrue(manager._started)
            self.assertEqual(manager.listener_status, "listening")

        asyncio.run(_run())

    def test_collector_info_merges_transport_and_discovery_state(self) -> None:
        manager = self._build_manager()
        manager._transport = _FakeTransport(connected=True)  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        collector = manager.collector_info

        self.assertEqual(collector.remote_ip, "192.168.1.14")
        self.assertEqual(collector.collector_pn, "PN123")
        self.assertEqual(collector.last_udp_reply, "set>server=192.168.1.10:8899;")
        self.assertEqual(collector.last_udp_reply_from, "192.168.1.14:58899")

    def test_collector_info_prefers_more_complete_at_pn(self) -> None:
        manager = self._build_manager()
        transport = _FakeTransport(connected=True)
        transport.collector_info = CollectorInfo(
            remote_ip="192.168.1.14",
            collector_pn="E5000020000000",
            collector_pn_prefix="E",
            collector_pn_digits="5000020000000",
        )
        at_transport = _FakeTransport(connected=True)
        at_transport.collector_info = CollectorInfo(
            remote_ip="192.168.1.14",
            collector_pn="E50000200000000001",
        )
        manager._transport = transport  # type: ignore[assignment]
        manager._at_transport = at_transport  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        collector = manager.collector_info

        self.assertEqual(collector.collector_pn, "E50000200000000001")
        self.assertEqual(collector.collector_pn_prefix, "E")
        self.assertEqual(collector.collector_pn_digits, "50000200000000001")

    def test_collector_info_merges_raw_passthrough_diagnostics_from_at_side(self) -> None:
        # For at_text collectors all raw inverter traffic happens on the AT
        # connection; a support bundle built from the framed-side info alone
        # reports zero raw requests while probes are timing out on the wire.
        manager = self._build_manager()
        transport = _FakeTransport(connected=True)
        at_transport = _FakeTransport(connected=True)
        at_transport.collector_info = CollectorInfo(
            remote_ip="192.168.1.14",
            collector_pn="PN123",
            raw_request_count=7,
            raw_response_count=2,
            raw_timeout_count=5,
            raw_unhandled_line_count=1,
            raw_last_request_ascii="QPI..",
            raw_last_request_hex="515049beac0d",
            raw_last_response_ascii="(PI30..",
            raw_last_response_hex="285049333012340d",
            raw_last_timeout_request_ascii="QPIGS..",
            raw_last_parser="raw_prefix_ascii",
            raw_last_frame_format="transparent",
            raw_last_spacing_wait_ms=10,
            raw_last_response_duration_ms=450,
            raw_last_total_duration_ms=470,
        )
        manager._transport = transport  # type: ignore[assignment]
        manager._at_transport = at_transport  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        collector = manager.collector_info

        self.assertEqual(collector.raw_request_count, 7)
        self.assertEqual(collector.raw_response_count, 2)
        self.assertEqual(collector.raw_timeout_count, 5)
        self.assertEqual(collector.raw_unhandled_line_count, 1)
        self.assertEqual(collector.raw_last_request_ascii, "QPI..")
        self.assertEqual(collector.raw_last_response_ascii, "(PI30..")
        self.assertEqual(collector.raw_last_timeout_request_ascii, "QPIGS..")
        self.assertEqual(collector.raw_last_parser, "raw_prefix_ascii")
        self.assertEqual(collector.raw_last_frame_format, "transparent")
        self.assertEqual(collector.raw_last_response_duration_ms, 450)

    def test_collector_info_uses_pn_binding_without_remote_ip_ambiguity(self) -> None:
        manager = self._build_manager(collector_ip="")
        manager._collector_pn = "PN-TWO"

        transport = _FakeTransport(connected=True, remote_ip="203.0.113.10")
        transport.collector_info = CollectorInfo(
            remote_ip="203.0.113.10",
            collector_pn="PN-TWO",
        )
        at_transport = _FakeTransport(connected=True, remote_ip="203.0.113.10")
        at_transport.collector_info = CollectorInfo(
            remote_ip="203.0.113.10",
            collector_pn="PN-TWO",
        )
        manager._transport = transport  # type: ignore[assignment]
        manager._at_transport = at_transport  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        collector = manager.collector_info

        self.assertEqual(collector.remote_ip, "203.0.113.10")
        self.assertEqual(collector.collector_pn, "PN-TWO")

    def test_callback_on_demand_sends_exactly_one_udp_trigger_per_attempt(self) -> None:
        # Phase 3: one-shot trigger, not a continuous announcer loop.
        manager = self._build_manager()
        transport = _FakeTransport(connected=False, connect_result=True)
        announcer = _FakeAnnouncer()
        manager._transport = transport  # type: ignore[assignment]
        manager._announcer = announcer  # type: ignore[assignment]
        manager._started = True
        manager._listener_status = "listening"
        probe = _fake_probe()

        with patch(
            "custom_components.eybond_local.runtime.link.async_probe_target", probe
        ):
            connected = asyncio.run(
                manager.async_try_connect(timeout=5.0, require_heartbeat=True)
            )

        self.assertTrue(connected)
        self.assertEqual(probe.await_count, 1)  # exactly one UDP trigger
        self.assertEqual(manager._callback_trigger_count, 1)
        self.assertEqual(announcer.start_calls, 0)  # no continuous announcer
        self.assertEqual(transport.connected_waits, [5.0])
        self.assertEqual(transport.heartbeat_waits, [1.5])
        self.assertEqual(
            manager.callback_trigger_diagnostics()["collector_callback_state"],
            "callback_connected",
        )

    def test_async_try_connect_can_wait_without_reverse_discovery(self) -> None:
        manager = self._build_manager()
        transport = _FakeTransport(connected=False, connect_result=True)
        announcer = _FakeAnnouncer()
        manager._transport = transport  # type: ignore[assignment]
        manager._announcer = announcer  # type: ignore[assignment]
        manager.set_reverse_discovery_enabled(False)

        connected = asyncio.run(manager.async_try_connect(timeout=5.0, require_heartbeat=True))

        self.assertTrue(connected)
        self.assertEqual(announcer.start_calls, 0)
        self.assertEqual(announcer.stop_calls, 1)
        self.assertEqual(transport.connected_waits, [5.0])
        self.assertEqual(transport.heartbeat_waits, [1.5])

    def test_disabling_reverse_discovery_stops_running_announcer(self) -> None:
        async def _run() -> _FakeAnnouncer:
            manager = self._build_manager()
            announcer = _FakeAnnouncer(running=True)
            manager._announcer = announcer  # type: ignore[assignment]

            manager.set_reverse_discovery_enabled(False)
            await asyncio.sleep(0)

            return announcer

        announcer = asyncio.run(_run())

        self.assertFalse(announcer.running)
        self.assertEqual(announcer.stop_calls, 1)
        self.assertEqual(announcer.last_reply, "")
        self.assertEqual(announcer.last_reply_from, "")

    def test_transport_prefers_connected_auxiliary_listener(self) -> None:
        manager = self._build_manager()
        primary_transport = _FakeTransport(connected=False)
        auxiliary_transport = _FakeTransport(connected=True)
        manager._transport = primary_transport  # type: ignore[assignment]
        manager._at_transport = _FakeTransport(connected=False)  # type: ignore[assignment]
        manager._auxiliary_listener_ports = {502}
        manager._auxiliary_transports = {502: auxiliary_transport}  # type: ignore[assignment]
        manager._auxiliary_at_transports = {}  # type: ignore[assignment]

        self.assertIs(manager.transport, auxiliary_transport)
        self.assertTrue(manager.connected)

    def test_runtime_link_without_collector_ip_accepts_same_collector_across_listener_ports(self) -> None:
        manager = self._build_manager(collector_ip="")
        primary_transport = _FakeTransport(connected=True, remote_ip="192.168.1.14")
        auxiliary_transport = _FakeTransport(connected=True, remote_ip="192.168.1.14")
        primary_transport.collector_info.heartbeat_fresh = False
        auxiliary_transport.collector_info.heartbeat_fresh = True
        primary_at_transport = _FakeTransport(connected=True, remote_ip="192.168.1.14")
        auxiliary_at_transport = _FakeTransport(connected=True, remote_ip="192.168.1.14")
        manager._transport = primary_transport  # type: ignore[assignment]
        manager._at_transport = primary_at_transport  # type: ignore[assignment]
        manager._auxiliary_listener_ports = {502}
        manager._auxiliary_transports = {502: auxiliary_transport}  # type: ignore[assignment]
        manager._auxiliary_at_transports = {502: auxiliary_at_transport}  # type: ignore[assignment]

        self.assertTrue(manager.connected)
        self.assertIs(manager.active_transport, auxiliary_transport)
        self.assertIs(manager.active_collector_at_transport, primary_at_transport)
        self.assertEqual(manager.collector_info.remote_ip, "192.168.1.14")

    def test_runtime_link_without_collector_ip_fails_closed_when_listener_ports_disagree(self) -> None:
        manager = self._build_manager(collector_ip="")
        manager._transport = _FakeTransport(connected=True, remote_ip="192.168.1.14")  # type: ignore[assignment]
        manager._at_transport = _FakeTransport(connected=True, remote_ip="192.168.1.14")  # type: ignore[assignment]
        manager._auxiliary_listener_ports = {502}
        manager._auxiliary_transports = {502: _FakeTransport(connected=True, remote_ip="192.168.1.55")}  # type: ignore[assignment]
        manager._auxiliary_at_transports = {502: _FakeTransport(connected=True, remote_ip="192.168.1.55")}  # type: ignore[assignment]

        self.assertFalse(manager.connected)
        self.assertIsNone(manager.active_transport)
        self.assertIsNone(manager.active_collector_at_transport)
        self.assertEqual(manager.collector_info.remote_ip, "")

    def test_async_try_connect_uses_connected_auxiliary_listener(self) -> None:
        manager = self._build_manager()
        primary_transport = _FakeTransport(connected=False, connect_result=False)
        auxiliary_transport = _FakeTransport(connected=False, connect_result=True)
        manager._transport = primary_transport  # type: ignore[assignment]
        manager._at_transport = _FakeTransport(connected=False)  # type: ignore[assignment]
        manager._auxiliary_listener_ports = {502}
        manager._auxiliary_transports = {502: auxiliary_transport}  # type: ignore[assignment]
        manager._auxiliary_at_transports = {}  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]
        manager._started = True
        manager._listener_status = "listening"

        connected = asyncio.run(manager.async_try_connect(timeout=5.0, require_heartbeat=True))

        self.assertTrue(connected)
        self.assertFalse(primary_transport.connected)
        self.assertTrue(auxiliary_transport.connected)
        self.assertTrue(auxiliary_transport.connected_waits)
        self.assertEqual(auxiliary_transport.heartbeat_waits, [1.5])

    def test_async_try_connect_accepts_heartbeat_from_auxiliary_listener(self) -> None:
        manager = self._build_manager()
        primary_transport = _FakeTransport(connected=True, heartbeat_result=False)
        auxiliary_transport = _FakeTransport(connected=True, heartbeat_result=True)
        manager._transport = primary_transport  # type: ignore[assignment]
        manager._at_transport = _FakeTransport(connected=False)  # type: ignore[assignment]
        manager._auxiliary_listener_ports = {502}
        manager._auxiliary_transports = {502: auxiliary_transport}  # type: ignore[assignment]
        manager._auxiliary_at_transports = {}  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        connected = asyncio.run(manager.async_try_connect(timeout=5.0, require_heartbeat=True))

        self.assertTrue(connected)
        self.assertTrue(primary_transport.heartbeat_waits)
        self.assertTrue(auxiliary_transport.heartbeat_waits)
        self.assertIs(manager.transport, auxiliary_transport)

    def test_async_ensure_callback_listener_starts_auxiliary_listener_pair(self) -> None:
        manager = self._build_manager()
        payload_transport = _FakeTransport()
        at_transport = _FakeTransport()
        build_calls: list[tuple[str, int]] = []

        def _build_pair(bind_host: str, port: int):
            build_calls.append((bind_host, port))
            return payload_transport, at_transport

        manager._build_transport_pair = _build_pair  # type: ignore[method-assign]

        asyncio.run(manager.async_ensure_callback_listener(502))

        self.assertEqual(manager._auxiliary_listener_ports, {502})
        self.assertEqual(build_calls, [("0.0.0.0", 502)])
        self.assertEqual(payload_transport.start_calls, 1)
        self.assertEqual(at_transport.start_calls, 1)

    def test_async_reconcile_network_rebuilds_advertised_host_without_specific_tcp_bind(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link.resolve_server_ip",
            side_effect=["192.168.1.10", "192.168.1.20"],
        ):
            manager = EybondRuntimeLinkManager(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
            )
            manager._transport = _FakeTransport()  # type: ignore[assignment]
            manager._at_transport = _FakeTransport()  # type: ignore[assignment]
            manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]
            manager._started = True
            manager.set_reverse_discovery_enabled(False)
            builds: list[tuple[str, int, _FakeTransport, _FakeTransport]] = []

            def _build_pair(bind_host: str, port: int):
                payload_transport = _FakeTransport()
                at_transport = _FakeTransport()
                builds.append((bind_host, port, payload_transport, at_transport))
                return payload_transport, at_transport

            manager._build_transport_pair = _build_pair  # type: ignore[method-assign]

            changed = asyncio.run(manager.async_reconcile_network(reason="network_test"))

        self.assertTrue(changed)
        self.assertEqual(builds[-1][0], "0.0.0.0")
        self.assertEqual(builds[-1][1], 8899)
        self.assertEqual(manager.effective_server_ip, "192.168.1.20")
        self.assertEqual(manager._announcer._bind_ip, "192.168.1.20")
        self.assertEqual(manager._announcer._advertised_server_ip, "192.168.1.20")
        self.assertEqual(manager.listener_diagnostics()["collector_listener_rebind_count"], 1)
        self.assertEqual(builds[-1][2].start_calls, 1)
        self.assertEqual(builds[-1][3].start_calls, 1)

    def test_async_trigger_reverse_discovery_uses_bootstrap_listener_defaults(self) -> None:
        manager = self._build_manager()
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        with patch(
            "custom_components.eybond_local.runtime.link.async_probe_target",
            new=AsyncMock(
                return_value=DiscoveryProbeResult(
                    target_ip="192.168.1.14",
                    message="set>server=192.168.1.10:8899;",
                    local_port=43123,
                    reply="rsp>server=1;",
                    reply_from="192.168.1.14:58899",
                )
            ),
        ) as probe_target:
            result = asyncio.run(manager.async_trigger_reverse_discovery())

        probe_target.assert_awaited_once_with(
            bind_ip="192.168.1.10",
            advertised_server_ip="192.168.1.10",
            advertised_server_port=8899,
            target_ip="192.168.1.14",
            udp_port=58899,
            timeout=0.75,
        )
        self.assertEqual(manager._announcer.last_reply, "rsp>server=1;")
        self.assertEqual(manager._announcer.last_reply_from, "192.168.1.14:58899")
        self.assertEqual(result["advertised_endpoint"], "192.168.1.10:8899")

    def test_proxy_capture_route_lifecycle_uses_shared_listener(self) -> None:
        manager = self._build_manager()
        events: list[tuple[str, object]] = []

        class _Handler:
            running = False

            def __init__(self, **kwargs) -> None:
                events.append(("handler_init", kwargs))

            async def start(self) -> None:
                self.running = True
                events.append(("handler_start", None))

            async def stop(self) -> None:
                self.running = False
                events.append(("handler_stop", None))

            async def handle_client(self, reader, writer) -> None:
                pass

        class _Route:
            def __init__(self, **kwargs) -> None:
                events.append(("route_init", kwargs))

            async def start(self) -> None:
                events.append(("route_start", None))

            async def stop(self) -> None:
                events.append(("route_stop", None))

        async def _run() -> None:
            with patch("custom_components.eybond_local.runtime.link.InProcessProxyCaptureHandler", _Handler), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_proxy_capture_route(
                    collector_ip="192.168.1.14",
                    listen_port=502,
                    upstream_host="47.91.67.66",
                    upstream_port=18899,
                    output_path=Path("/tmp/proxy.jsonl"),
                    masked_endpoint="ess.eybond.com",
                    restore_trigger_path=Path("/tmp/proxy.restore"),
                )
                self.assertTrue(manager.proxy_capture_route_running())
                await manager.async_stop_proxy_capture_route()
                self.assertFalse(manager.proxy_capture_route_running())

        asyncio.run(_run())

        self.assertEqual([event for event, _ in events], [
            "handler_init",
            "handler_start",
            "route_init",
            "route_start",
            "route_stop",
            "handler_stop",
        ])
        route_kwargs = dict(events[2][1])
        self.assertEqual(route_kwargs["host"], "0.0.0.0")
        self.assertEqual(route_kwargs["port"], 502)
        self.assertEqual(route_kwargs["collector_ip"], "192.168.1.14")

    def test_route_lease_blocks_shadow_while_proxy_start_is_in_progress(self) -> None:
        manager = self._build_manager()
        proxy_start_entered = asyncio.Event()
        allow_proxy_start = asyncio.Event()

        class _Handler:
            running = False
            ready = False

            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                self.running = True

            async def stop(self) -> None:
                self.running = False

            async def handle_client(self, reader, writer) -> None:
                pass

            def status(self) -> dict[str, object]:
                return {"running": self.running, "ready": self.ready}

        class _Route:
            def __init__(self, **kwargs) -> None:
                self.port = int(kwargs["port"])

            async def start(self) -> None:
                if self.port == 502:
                    proxy_start_entered.set()
                    await allow_proxy_start.wait()

            async def stop(self) -> None:
                pass

        async def _run() -> None:
            with patch(
                "custom_components.eybond_local.runtime.link.InProcessProxyCaptureHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.InProcessFailClosedShadowProxyHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                proxy_start = asyncio.create_task(
                    manager.async_start_proxy_capture_route(
                        owner_id="proxy-owner",
                        entry_id="entry-1",
                        collector_ip="192.168.1.14",
                        listen_port=502,
                        upstream_host="cloud.example",
                        upstream_port=18899,
                        output_path=Path("/tmp/proxy.jsonl"),
                    )
                )
                await proxy_start_entered.wait()

                with self.assertRaisesRegex(RuntimeError, "proxy_capture_route_running"):
                    await manager.async_start_shadow_learning_route(
                        owner_id="shadow-owner",
                        entry_id="entry-1",
                        collector_ip="192.168.1.14",
                        listen_port=503,
                        upstream_host="cloud.example",
                        upstream_port=18899,
                        output_path=Path("/tmp/shadow.jsonl"),
                        seed=object(),
                    )

                self.assertEqual(manager.route_lease.owner_id, "proxy-owner")
                self.assertEqual(manager.route_lease.state, "starting")
                allow_proxy_start.set()
                await proxy_start
                self.assertTrue(manager.proxy_capture_route_running())
                self.assertFalse(manager.shadow_learning_route_running())
                await manager.async_stop_proxy_capture_route(owner_id="proxy-owner")
                self.assertIsNone(manager.route_lease)

        asyncio.run(_run())

    def test_route_lease_is_released_after_start_failure(self) -> None:
        manager = self._build_manager()

        class _Handler:
            running = False
            ready = False

            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                self.running = True

            async def stop(self) -> None:
                self.running = False

            async def handle_client(self, reader, writer) -> None:
                pass

            def status(self) -> dict[str, object]:
                return {"running": self.running, "ready": self.ready}

        class _Route:
            fail_next = True

            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                if self.fail_next:
                    type(self).fail_next = False
                    raise RuntimeError("bind_failed")

            async def stop(self) -> None:
                pass

        async def _run() -> None:
            with patch(
                "custom_components.eybond_local.runtime.link.InProcessProxyCaptureHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.InProcessFailClosedShadowProxyHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                with self.assertRaisesRegex(RuntimeError, "bind_failed"):
                    await manager.async_start_proxy_capture_route(
                        owner_id="proxy-owner",
                        entry_id="entry-1",
                        collector_ip="192.168.1.14",
                        listen_port=502,
                        upstream_host="cloud.example",
                        upstream_port=18899,
                        output_path=Path("/tmp/proxy.jsonl"),
                    )
                self.assertIsNone(manager.route_lease)

                await manager.async_start_shadow_learning_route(
                    owner_id="shadow-owner",
                    entry_id="entry-1",
                    collector_ip="192.168.1.14",
                    listen_port=503,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/shadow.jsonl"),
                    seed=object(),
                )
                self.assertEqual(manager.route_lease.owner_id, "shadow-owner")
                await manager.async_stop_shadow_learning_route(owner_id="shadow-owner")

        asyncio.run(_run())

    def test_route_stop_rejects_mismatched_owner(self) -> None:
        manager = self._build_manager()

        class _Handler:
            running = False

            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                self.running = True

            async def stop(self) -> None:
                self.running = False

            async def handle_client(self, reader, writer) -> None:
                pass

        class _Route:
            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                pass

        async def _run() -> None:
            with patch(
                "custom_components.eybond_local.runtime.link.InProcessProxyCaptureHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_proxy_capture_route(
                    owner_id="proxy-owner",
                    entry_id="entry-1",
                    collector_ip="192.168.1.14",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/proxy.jsonl"),
                )
                with self.assertRaisesRegex(RuntimeError, "route_lease_owner_mismatch"):
                    await manager.async_stop_proxy_capture_route(owner_id="other-owner")
                self.assertTrue(manager.proxy_capture_route_running())
                self.assertEqual(manager.route_lease.owner_id, "proxy-owner")
                await manager.async_stop_proxy_capture_route(owner_id="proxy-owner")

        asyncio.run(_run())

    def test_route_lease_blocks_proxy_while_shadow_is_running(self) -> None:
        manager = self._build_manager()

        class _Handler:
            running = False
            ready = False

            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                self.running = True

            async def stop(self) -> None:
                self.running = False

            async def handle_client(self, reader, writer) -> None:
                pass

            def status(self) -> dict[str, object]:
                return {"running": self.running, "ready": self.ready}

        class _Route:
            def __init__(self, **_kwargs) -> None:
                pass

            async def start(self) -> None:
                pass

            async def stop(self) -> None:
                pass

        async def _run() -> None:
            with patch(
                "custom_components.eybond_local.runtime.link.InProcessFailClosedShadowProxyHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_shadow_learning_route(
                    owner_id="shadow-owner",
                    entry_id="entry-1",
                    collector_ip="192.168.1.14",
                    listen_port=503,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/shadow.jsonl"),
                    seed=object(),
                )
                with self.assertRaisesRegex(RuntimeError, "shadow_learning_route_running"):
                    await manager.async_start_proxy_capture_route(
                        owner_id="proxy-owner",
                        entry_id="entry-1",
                        collector_ip="192.168.1.14",
                        listen_port=502,
                        upstream_host="cloud.example",
                        upstream_port=18899,
                        output_path=Path("/tmp/proxy.jsonl"),
                    )
                self.assertTrue(manager.shadow_learning_route_running())
                self.assertFalse(manager.proxy_capture_route_running())
                await manager.async_stop_shadow_learning_route(owner_id="shadow-owner")

        asyncio.run(_run())

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
        manager._started = True
        manager._listener_status = "listening"
        probe = _fake_probe()

        with patch(
            "custom_components.eybond_local.runtime.link.async_probe_target", probe
        ):
            connected = asyncio.run(
                manager.async_try_connect(timeout=5.0, require_heartbeat=True)
            )

        self.assertFalse(connected)
        self.assertEqual(transport.heartbeat_waits, [1.5])
        # Already connected -> no callback trigger; no continuous announcer.
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(announcer.start_calls, 0)
        self.assertEqual(announcer.stop_calls, 0)

    def test_async_ensure_connected_raises_when_heartbeat_times_out(self) -> None:
        manager = self._build_manager()
        manager._transport = _FakeTransport(connected=True, heartbeat_result=False)  # type: ignore[assignment]
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]

        with self.assertRaisesRegex(ConnectionError, "collector_heartbeat_timeout"):
            asyncio.run(manager.async_ensure_connected(timeout=5.0, require_heartbeat=True))

    def test_async_reset_connection_disconnects_without_restarting_discovery(self) -> None:
        manager = self._build_manager()
        transport = _FakeTransport(connected=True)
        announcer = _FakeAnnouncer()
        manager._transport = transport  # type: ignore[assignment]
        manager._announcer = announcer  # type: ignore[assignment]

        asyncio.run(manager.async_reset_connection(reason="request_timeout"))

        self.assertEqual(transport.disconnect_calls, 1)
        # Phase 3: reset no longer restarts a continuous announcer; the next
        # connect attempt sends a single one-shot callback trigger instead.
        self.assertEqual(announcer.start_calls, 0)

    def test_runtime_manager_uses_bind_ip_for_advertised_endpoint_when_override_is_empty(self) -> None:
        manager = self._build_manager()

        self.assertEqual(manager._transport._host, "0.0.0.0")
        self.assertEqual(manager._announcer._advertised_server_ip, "192.168.1.10")
        self.assertEqual(manager._announcer._advertised_server_port, 8899)

    def test_clear_discovery_reply_clears_the_announcer_source(self) -> None:
        manager = self._build_manager()
        manager._announcer.last_reply = "rsp>server=1;"
        manager._announcer.last_reply_from = "192.168.1.14:58899"

        # collector_info rebuilds from the announcer: the stale values are
        # visible before the clear and gone after it.
        self.assertEqual(manager.collector_info.last_udp_reply, "rsp>server=1;")
        manager.clear_discovery_reply()
        self.assertEqual(manager.collector_info.last_udp_reply, "")
        self.assertEqual(manager.collector_info.last_udp_reply_from, "")

    def test_shared_listener_connection_watchers_filter_by_collector_ip(self) -> None:
        from custom_components.eybond_local.collector.transport import _SharedEybondListener

        listener = _SharedEybondListener(host="0.0.0.0", port=18899)
        scoped_hits: list[str] = []
        any_hits: list[str] = []
        scoped_token = listener.add_connection_watcher("192.168.1.14", scoped_hits.append)
        listener.add_connection_watcher("", any_hits.append)

        listener._notify_connection_watchers("192.168.1.99")
        self.assertEqual(scoped_hits, [])
        self.assertEqual(any_hits, ["192.168.1.99"])

        listener._notify_connection_watchers("192.168.1.14")
        self.assertEqual(scoped_hits, ["192.168.1.14"])
        self.assertEqual(any_hits, ["192.168.1.99", "192.168.1.14"])

        listener.remove_connection_watcher(scoped_token)
        listener._notify_connection_watchers("192.168.1.14")
        self.assertEqual(scoped_hits, ["192.168.1.14"])

    def test_runtime_manager_applies_connection_watcher_across_rebuilds(self) -> None:
        manager = self._build_manager()
        hits: list[str] = []

        manager.set_collector_connection_watcher(hits.append)

        watcher = manager._transport._connection_watcher_callback
        self.assertIsNotNone(watcher)
        watcher("192.168.1.14")
        self.assertEqual(hits, ["192.168.1.14"])

        manager._rebuild_link("192.168.1.10")
        self.assertIsNotNone(manager._transport._connection_watcher_callback)


class CallbackOnDemandPhase3Tests(unittest.TestCase):
    """Phase 3: bounded once-per-attempt callback trigger + typed outcomes."""

    _PN = "V00AAA1111111111"
    _FOREIGN_PN = "V00BBB2222222222"

    def _manager(self, *, callback_on_demand: bool, collector_pn: str = "") -> EybondRuntimeLinkManager:
        with patch(
            "custom_components.eybond_local.runtime.link.resolve_server_ip",
            return_value="192.168.1.10",
        ):
            manager = EybondRuntimeLinkManager(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
            )
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]
        manager.set_reverse_discovery_enabled(callback_on_demand)
        if collector_pn:
            manager._collector_pn = collector_pn
        manager._started = True
        manager._listener_status = "listening"
        return manager

    @staticmethod
    def _session(pn, *, state="routed_framed", shape="eybond_framed_or_binary"):
        return {
            "session_id": "listener-8899-9",
            "peer_ip": "203.0.113.10",
            "listener_port": 8899,
            "collector_pn": pn,
            "state": state,
            "protocol_shape": shape,
            "collector_identity_source": "at_dtupn",
        }

    def _run_connect(self, manager, *, timeout=0.2):
        probe = _fake_probe()
        with patch(
            "custom_components.eybond_local.runtime.link.async_probe_target", probe
        ):
            ok = asyncio.run(manager.async_try_connect(timeout=timeout))
        return ok, probe

    def test_inbound_sends_zero_udp_triggers(self) -> None:
        manager = self._manager(callback_on_demand=False)
        manager._transport = _FakeTransport(connected=False, connect_result=True)  # type: ignore[assignment]
        # Start from a clean announcer: an inbound connect must not produce a
        # "Collector UDP Reply From" as a side effect (it never sends UDP).
        manager._announcer.last_reply = ""
        manager._announcer.last_reply_from = ""
        ok, probe = self._run_connect(manager)
        self.assertTrue(ok)
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(manager._callback_trigger_count, 0)
        # No UDP was sent, so no UDP reply is captured or surfaced.
        self.assertEqual(manager._announcer.last_reply_from, "")
        self.assertEqual(manager._announcer.last_reply, "")

    def test_callback_on_demand_sends_one_trigger_then_times_out(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        manager._transport = _FakeTransport(connected=False, connect_result=False)  # type: ignore[assignment]
        ok, probe = self._run_connect(manager)
        self.assertFalse(ok)
        self.assertEqual(probe.await_count, 1)  # exactly one UDP trigger
        diag = manager.callback_trigger_diagnostics()
        self.assertEqual(diag["collector_callback_state"], "callback_timeout")
        # Phase 4: an actionable, user-facing message accompanies the typed state.
        self.assertIn("did not call back", diag["collector_callback_state_message"])
        self.assertIn("firewall", diag["collector_callback_state_message"])

    def test_callback_on_demand_listener_unavailable(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        manager._started = False
        manager._listener_status = "stopped"
        manager._transport = _FakeTransport(connected=False, connect_result=False)  # type: ignore[assignment]
        ok, probe = self._run_connect(manager)
        self.assertFalse(ok)
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(
            manager.callback_trigger_diagnostics()["collector_callback_state"],
            "callback_listener_unavailable",
        )

    def test_callback_on_demand_identity_mismatch_does_not_claim(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        # A session arrives, but its PN is a DIFFERENT collector.
        manager._transport = _FakeTransport(
            connected=False,
            connect_result=False,
            observed_sessions=(self._session(self._FOREIGN_PN),),
        )  # type: ignore[assignment]
        ok, _probe = self._run_connect(manager)
        self.assertFalse(ok)
        self.assertEqual(
            manager.callback_trigger_diagnostics()["collector_callback_state"],
            "callback_identity_mismatch",
        )
        # The foreign session is not owned by this entry.
        self.assertEqual(manager._session_registry.owner_for_pn(self._FOREIGN_PN), "")

    def test_foreign_identified_session_is_not_identity_ok_for_expected_pn(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        transport = _FakeTransport(
            connected=False,
            connect_result=False,
            observed_sessions=(self._session(self._FOREIGN_PN),),
        )
        transport.session_inventory_diagnostics = lambda: {  # type: ignore[method-assign]
            "pending_session_count": 1,
            "recent_session_count": 1,
            "duplicate_peer_ip_count": 0,
            "duplicate_peer_ips": [],
            "sessions": [
                {
                    "session_id": "listener-8899-9",
                    "peer_ip": "203.0.113.10",
                    "state": "parked_no_payload_owner",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_masked": "V00…2222",
                    "collector_identity_source": "framed_heartbeat",
                }
            ],
        }
        manager._transport = transport  # type: ignore[assignment]

        diagnostics = manager.listener_diagnostics()

        self.assertEqual(diagnostics["collector_callback_identity_status"], "conflict")
        self.assertEqual(diagnostics["collector_callback_identity_mismatch_count"], 1)
        self.assertIn(
            "does not match",
            diagnostics["collector_callback_identity_summary"],
        )

    def test_callback_on_demand_session_claimed_by_other_entry(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        # Our own session is observed (matching PN) but we can't connect...
        manager._transport = _FakeTransport(
            connected=False,
            connect_result=False,
            observed_sessions=(self._session(self._PN),),
        )  # type: ignore[assignment]
        # ...because a DIFFERENT entry owns this identity in the domain registry.
        domain = CallbackSessionRegistry()
        domain.claim("other-entry", collector_pn=self._PN)
        manager.set_callback_ownership(domain, "our-entry")
        ok, _probe = self._run_connect(manager)
        self.assertFalse(ok)
        self.assertEqual(
            manager.callback_trigger_diagnostics()["collector_callback_state"],
            "callback_session_claimed_by_other_entry",
        )

    def test_two_collectors_same_ip_resolved_by_pn(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        manager._transport = _FakeTransport(
            connected=False,
            connect_result=False,
            observed_sessions=(
                self._session(self._PN, state="routed_framed", shape="eybond_framed_or_binary"),
                self._session(self._FOREIGN_PN, state="routed_at_text", shape="at_text"),
            ),
        )  # type: ignore[assignment]
        # The manager negotiates ITS OWN framed wire, not the other collector's
        # AT wire, even though both share the peer IP.
        handle = manager.session_handle
        self.assertEqual(handle.collector_pn, self._PN)
        self.assertEqual(handle.wire, "eybond_framed")

    def test_inbound_never_sends_trigger_even_when_disconnected(self) -> None:
        manager = self._manager(callback_on_demand=False, collector_pn=self._PN)
        manager._transport = _FakeTransport(connected=False, connect_result=False)  # type: ignore[assignment]
        ok, probe = self._run_connect(manager)
        self.assertFalse(ok)
        self.assertEqual(probe.await_count, 0)  # inbound: zero UDP triggers
        # No callback state recorded for inbound.
        self.assertEqual(
            manager.callback_trigger_diagnostics()["collector_callback_state"], ""
        )


if __name__ == "__main__":
    unittest.main()
