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


_OBSERVED_PN = "PN123"


def _observed_framed_session(pn: str = _OBSERVED_PN, session_id: str = "obs-framed"):
    """A trusted, routed framed live session (claimed + observed SessionHandle).

    Payload forwarding now fail-closes until a wire is observed/confirmed, so a
    connected transport must expose a realistic observed session to be usable.
    """

    return {
        "session_id": session_id,
        "collector_pn": pn,
        "peer_ip": "192.168.1.14",
        "listener_port": 8899,
        "state": "routed_framed",
        "protocol_shape": "eybond_framed_or_binary",
        "collector_identity_source": "framed_heartbeat",
    }


def _observed_at_session(pn: str = _OBSERVED_PN, session_id: str = "obs-at"):
    """A trusted, routed AT-text live session."""

    return {
        "session_id": session_id,
        "collector_pn": pn,
        "peer_ip": "192.168.1.14",
        "listener_port": 8899,
        "state": "routed_at_text",
        "protocol_shape": "at_text",
        "collector_identity_source": "at_dtupn",
    }


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
        listener_key: str = "fake:0",
    ) -> None:
        self.connected = connected
        self.collector_info = CollectorInfo(remote_ip=remote_ip, collector_pn="PN123")
        self._connect_result = connect_result
        self._heartbeat_result = heartbeat_result
        self._observed_sessions = tuple(dict(s) for s in observed_sessions)
        # Distinct per transport so _iter_observed_sessions does not dedupe two
        # different fake listeners as one (real listeners have distinct keys).
        self._listener_key = listener_key
        self.connected_waits: list[float] = []
        self.heartbeat_waits: list[float] = []
        self.disconnect_calls = 0
        self.start_calls = 0
        self.stop_calls = 0
        self.preserved_stop_sessions: list[str] = []

    @property
    def listener_key(self) -> str:
        return self._listener_key

    def observed_collector_sessions(self) -> tuple:
        return self._observed_sessions

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self, *, preserve_session_id: str = "") -> None:
        self.stop_calls += 1
        self.preserved_stop_sessions.append(preserve_session_id)

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


class _HeartbeatReplacementTransport(_FakeTransport):
    """Drop the first socket while heartbeat is awaited, then expose its replacement."""

    def __init__(self) -> None:
        super().__init__(connected=True, heartbeat_result=False)
        self._heartbeat_attempt = 0

    async def wait_until_heartbeat(self, timeout: float) -> bool:
        self.heartbeat_waits.append(timeout)
        self._heartbeat_attempt += 1
        if self._heartbeat_attempt == 1:
            self.connected = False
            asyncio.get_running_loop().call_later(
                0.03,
                setattr,
                self,
                "connected",
                True,
            )
            return False
        self.collector_info.heartbeat_fresh = True
        return True


class _CorrelatedLivenessTransport(_FakeTransport):
    """Framed transport with live request traffic but no fresh FC=1 sample."""

    def __init__(self, *, liveness_result: bool) -> None:
        super().__init__(connected=True, heartbeat_result=False)
        self._liveness_result = liveness_result
        self.liveness_waits: list[float] = []

    async def wait_until_liveness(self, timeout: float) -> bool:
        self.liveness_waits.append(timeout)
        return self._liveness_result


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
            "custom_components.eybond_local.runtime.link.common.subprocess.check_output",
            side_effect=side_effects,
        ), patch(
            "custom_components.eybond_local.runtime.link.common._default_local_ip",
            return_value="192.168.1.104",
        ):
            resolved = resolve_server_ip(
                "192.168.88.91",
                collector_ip="192.168.88.88",
            )

        self.assertEqual(resolved, "192.168.88.92")

    def test_resolve_server_ip_prefers_active_ip_on_collector_subnet(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link.common._active_ipv4_interfaces",
            return_value=(("192.168.1.104", 24), ("192.168.88.92", 24)),
        ), patch(
            "custom_components.eybond_local.runtime.link.common._default_local_ip",
            return_value="192.168.1.104",
        ):
            resolved = resolve_server_ip(
                "192.168.88.91",
                collector_ip="192.168.88.88",
            )

        self.assertEqual(resolved, "192.168.88.92")

    def test_resolve_server_ip_keeps_same_subnet_config_for_ap_mode(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link.common._active_ipv4_interfaces",
            return_value=(("192.168.1.104", 24),),
        ), patch(
            "custom_components.eybond_local.runtime.link.common._default_local_ip",
            return_value="192.168.1.104",
        ):
            resolved = resolve_server_ip(
                "192.168.88.92",
                collector_ip="192.168.88.88",
            )

        self.assertEqual(resolved, "192.168.88.92")

    def test_resolve_server_ip_tolerates_blocked_socket_fallback(self) -> None:
        with patch(
            "custom_components.eybond_local.runtime.link.common._active_ipv4_interfaces",
            return_value=(),
        ), patch(
            "custom_components.eybond_local.runtime.link.common.socket.socket",
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
        manager._configured_collector_session_protocol = "at_text"
        manager._collector_identity_strategy = "at_dtupn"
        transport = _FakeTransport(connected=False)
        transport._listener = object()  # type: ignore[attr-defined]
        manager._transport = transport  # type: ignore[assignment]

        diagnostics = manager.listener_diagnostics()

        self.assertEqual(diagnostics["collector_configured_session_protocol"], "at_text")
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
            manager._collector_pn = _OBSERVED_PN
            # Expected (inferred) hint no longer drives the adapter; a routed AT
            # live session provides the confirmed raw/AT wire.
            manager._configured_collector_session_protocol = "at_text"
            payload = _FakeTransport(
                connected=False,
                connect_result=False,
                observed_sessions=(_observed_at_session(),),
            )
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

    def test_liveness_wait_follows_replacement_socket_through_short_gap(self) -> None:
        async def _run() -> None:
            manager = self._build_manager()
            transport = _HeartbeatReplacementTransport()
            manager._transport = transport  # type: ignore[assignment]

            ok = await manager._async_wait_for_payload_liveness(timeout=0.2)

            self.assertTrue(ok)
            self.assertGreaterEqual(len(transport.heartbeat_waits), 2)
            self.assertTrue(transport.connected)

        asyncio.run(_run())

    def test_runtime_accepts_correlated_liveness_without_claiming_heartbeat(self) -> None:
        async def _run() -> None:
            manager = self._build_manager()
            transport = _CorrelatedLivenessTransport(liveness_result=True)
            manager._transport = transport  # type: ignore[assignment]

            ok = await manager.async_try_connect(
                timeout=0.5,
                require_heartbeat=True,
            )

            self.assertTrue(ok)
            self.assertEqual(transport.liveness_waits, [0.5])
            self.assertEqual(transport.heartbeat_waits, [])
            self.assertFalse(transport.collector_info.heartbeat_fresh)

        asyncio.run(_run())

    def test_runtime_rejects_stale_correlated_liveness(self) -> None:
        async def _run() -> None:
            manager = self._build_manager()
            transport = _CorrelatedLivenessTransport(liveness_result=False)
            manager._transport = transport  # type: ignore[assignment]

            ok = await manager.async_try_connect(
                timeout=0.5,
                require_heartbeat=True,
            )

            self.assertFalse(ok)
            self.assertEqual(transport.liveness_waits, [0.5])
            self.assertTrue(transport.connected)

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
            self.assertEqual(
                manager.listener_diagnostics()["collector_configured_session_protocol"],
                "at_text",
            )
            self.assertEqual(manager.listener_diagnostics()["collector_callback_identity_strategy"], "at_dtupn")
            self.assertTrue(manager._started)
            self.assertEqual(manager.listener_status, "listening")

        asyncio.run(_run())

    def test_transport_stop_preserves_the_exact_owned_session(self) -> None:
        async def _run() -> None:
            manager = self._build_manager()
            payload = _FakeTransport(listener_key="payload")
            at_transport = _FakeTransport(listener_key="at")
            manager._transport = payload  # type: ignore[assignment]
            manager._at_transport = at_transport  # type: ignore[assignment]

            with patch.object(
                manager,
                "_claimed_session_id",
                return_value="listener-8899-owned",
            ):
                await manager._stop_all_transports()

            self.assertEqual(
                payload.preserved_stop_sessions,
                ["listener-8899-owned"],
            )
            self.assertEqual(
                at_transport.preserved_stop_sessions,
                ["listener-8899-owned"],
            )

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
            "custom_components.eybond_local.runtime.link.callback.async_send_callback_trigger", probe
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
        manager._collector_pn = _OBSERVED_PN
        primary_transport = _FakeTransport(connected=False)
        auxiliary_transport = _FakeTransport(
            connected=True,
            observed_sessions=(_observed_framed_session(),),
            listener_key="fake:502",
        )
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
        manager._collector_pn = _OBSERVED_PN
        primary_transport = _FakeTransport(connected=True, heartbeat_result=False)
        auxiliary_transport = _FakeTransport(
            connected=True,
            heartbeat_result=True,
            observed_sessions=(_observed_framed_session(),),
            listener_key="fake:502",
        )
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
            return_value="192.168.1.10",
        ), patch(
            "custom_components.eybond_local.runtime.link.transport_lifecycle."
            "resolve_server_ip",
            return_value="192.168.1.20",
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
            "custom_components.eybond_local.runtime.link.callback.async_send_callback_trigger",
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
            source="runtime_manual_trigger",
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
            with patch("custom_components.eybond_local.runtime.link.cloud_routes.InProcessProxyCaptureHandler", _Handler), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_proxy_capture_route(
                    collector_ip="192.168.1.14",
                    expected_session_protocol="at_text",
                    listen_port=502,
                    upstream_host="47.91.67.66",
                    upstream_port=18899,
                    output_path=Path("/tmp/proxy.jsonl"),
                    masked_endpoint="ess.eybond.com",
                    restore_trigger_path=Path("/tmp/proxy.restore"),
                )
                self.assertTrue(manager.proxy_capture_route_running())
                with patch.object(
                    manager,
                    "_send_callback_trigger",
                    new=AsyncMock(),
                ) as trigger:
                    self.assertFalse(
                        await manager.async_try_connect(timeout=0.1)
                    )
                    trigger.assert_not_awaited()
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
                "custom_components.eybond_local.runtime.link.cloud_routes.InProcessProxyCaptureHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.InProcessFailClosedShadowProxyHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.SharedProxyCaptureRoute",
                _Route,
            ):
                proxy_start = asyncio.create_task(
                    manager.async_start_proxy_capture_route(
                        owner_id="proxy-owner",
                        entry_id="entry-1",
                        collector_ip="192.168.1.14",
                        expected_session_protocol="at_text",
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
                        expected_session_protocol="at_text",
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
                "custom_components.eybond_local.runtime.link.cloud_routes.InProcessProxyCaptureHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.InProcessFailClosedShadowProxyHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.SharedProxyCaptureRoute",
                _Route,
            ):
                with self.assertRaisesRegex(RuntimeError, "bind_failed"):
                    await manager.async_start_proxy_capture_route(
                        owner_id="proxy-owner",
                        entry_id="entry-1",
                        collector_ip="192.168.1.14",
                        expected_session_protocol="at_text",
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
                    expected_session_protocol="at_text",
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
                "custom_components.eybond_local.runtime.link.cloud_routes.InProcessProxyCaptureHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_proxy_capture_route(
                    owner_id="proxy-owner",
                    entry_id="entry-1",
                    collector_ip="192.168.1.14",
                    expected_session_protocol="at_text",
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
                "custom_components.eybond_local.runtime.link.cloud_routes.InProcessFailClosedShadowProxyHandler",
                _Handler,
            ), patch(
                "custom_components.eybond_local.runtime.link.cloud_routes.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_shadow_learning_route(
                    owner_id="shadow-owner",
                    entry_id="entry-1",
                    collector_ip="192.168.1.14",
                    expected_session_protocol="at_text",
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
                        expected_session_protocol="at_text",
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
            "custom_components.eybond_local.runtime.link.callback.async_send_callback_trigger", probe
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
            "custom_components.eybond_local.runtime.link.callback.async_send_callback_trigger", probe
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

        # A foreign identified session on a shared listener that this entry does
        # not own is unresolved/unowned -- NOT a conflict (conflict requires
        # positive evidence: a route_identity_mismatch, not mere presence).
        self.assertEqual(diagnostics["collector_callback_identity_status"], "unresolved")
        self.assertEqual(diagnostics["collector_callback_identity_mismatch_count"], 0)
        self.assertEqual(
            diagnostics["collector_callback_foreign_identified_session_count"], 1
        )

    def test_recent_foreign_identified_session_is_not_conflict(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        transport = _FakeTransport(
            connected=False,
            connect_result=False,
            observed_sessions=(self._session(self._FOREIGN_PN),),
        )
        transport.session_inventory_diagnostics = lambda: {  # type: ignore[method-assign]
            "pending_session_count": 0,
            "recent_session_count": 1,
            "duplicate_peer_ip_count": 0,
            "duplicate_peer_ips": [],
            "sessions": [
                {
                    "session_id": "listener-8899-9",
                    "peer_ip": "203.0.113.10",
                    "state": "routed_framed",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_masked": "V00…2222",
                    "collector_identity_source": "at_dtupn",
                }
            ],
        }
        manager._transport = transport  # type: ignore[assignment]

        diagnostics = manager.listener_diagnostics()

        # A recent foreign identified session (not owned, nothing pending) is
        # never a conflict for this entry.
        self.assertNotEqual(
            diagnostics["collector_callback_identity_status"], "conflict"
        )
        self.assertEqual(diagnostics["collector_callback_pending_session_count"], 0)
        self.assertEqual(diagnostics["collector_callback_recent_session_count"], 1)
        self.assertEqual(diagnostics["collector_callback_identity_mismatch_count"], 0)

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
        self.assertEqual(handle.wire_framing, "eybond_framed")

    def test_callback_on_demand_activates_exact_owned_session_without_trigger(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        target = self._session(self._PN)
        target["session_id"] = "listener-8899-target"
        foreign = self._session(self._FOREIGN_PN)
        foreign["session_id"] = "listener-8899-foreign"
        inventory = [target, foreign]
        domain = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        domain.claim(
            "entry-target",
            collector_pn=self._PN,
            session_id="listener-8899-target",
        )
        manager.set_callback_ownership(domain, "entry-target")
        transport = _ClaimRecordingTransport(
            connected=False,
            connect_result=True,
            observed_sessions=(target, foreign),
        )
        manager._transport = transport  # type: ignore[assignment]

        ok, probe = self._run_connect(manager)

        self.assertTrue(ok)
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(manager._callback_trigger_count, 0)
        self.assertIsNotNone(transport.claim_provider)
        self.assertEqual(transport.claim_provider(), "listener-8899-target")
        self.assertEqual(domain.owner_for_pn(self._FOREIGN_PN), "")

    def test_proven_session_activation_is_exact_and_never_triggers(self) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        target = self._session(self._PN)
        target["session_id"] = "listener-8899-certified"
        domain = CallbackSessionRegistry(sessions_source=lambda: (target,))
        domain.claim(
            "entry-target",
            collector_pn=self._PN,
            session_id="listener-8899-certified",
        )
        manager.set_callback_ownership(domain, "entry-target")
        transport = _ClaimRecordingTransport(
            connected=False,
            connect_result=True,
            observed_sessions=(target,),
        )
        manager._transport = transport  # type: ignore[assignment]
        probe = _fake_probe()

        with patch(
            "custom_components.eybond_local.runtime.link.callback.async_send_callback_trigger",
            probe,
        ):
            activated = asyncio.run(
                manager.async_activate_claimed_session(
                    expected_session_id="listener-8899-certified",
                    timeout=2.0,
                )
            )

        self.assertTrue(activated)
        self.assertEqual(transport.connected_waits, [2.0])
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(manager._callback_trigger_count, 0)

        # A stale/foreign certification target fails before transport I/O and
        # must not degrade into the ordinary callback-on-demand path.
        transport.connected = False
        transport.connected_waits.clear()
        refused = asyncio.run(
            manager.async_activate_claimed_session(
                expected_session_id="listener-8899-foreign",
                timeout=2.0,
            )
        )
        self.assertFalse(refused)
        self.assertEqual(transport.connected_waits, [])
        self.assertEqual(probe.await_count, 0)

    def test_callback_on_demand_does_not_retrigger_while_exact_session_is_unusable(
        self,
    ) -> None:
        manager = self._manager(callback_on_demand=True, collector_pn=self._PN)
        target = self._session(self._PN)
        target["session_id"] = "listener-8899-target"
        foreign = self._session(self._FOREIGN_PN)
        foreign["session_id"] = "listener-8899-foreign"
        domain = CallbackSessionRegistry(
            sessions_source=lambda: (target, foreign)
        )
        domain.claim(
            "entry-target",
            collector_pn=self._PN,
            session_id="listener-8899-target",
        )
        manager.set_callback_ownership(domain, "entry-target")
        transport = _ClaimRecordingTransport(
            connected=False,
            connect_result=False,
            observed_sessions=(target, foreign),
        )
        manager._transport = transport  # type: ignore[assignment]

        ok, probe = self._run_connect(manager)

        self.assertFalse(ok)
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(manager._callback_trigger_count, 0)
        self.assertEqual(transport.connected_waits, [0.2])
        self.assertEqual(
            manager.callback_trigger_diagnostics()["collector_callback_state"],
            "callback_timeout",
        )
        self.assertIsNotNone(transport.claim_provider)
        self.assertEqual(transport.claim_provider(), "listener-8899-target")
        self.assertEqual(domain.owner_for_pn(self._FOREIGN_PN), "")

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


class _ClaimRecordingTransport(_FakeTransport):
    """Payload-transport fake that records the registry claim provider."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.claim_provider = None

    def set_claimed_session_provider(self, provider) -> None:
        self.claim_provider = provider


class _FakeAuxAtTransport:
    def __init__(self) -> None:
        self.connected = False
        self.start_calls = 0
        self.stop_calls = 0
        self.collector_info = CollectorInfo()
        self.claim_provider = None
        self.negotiated_wire = None

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self, *, preserve_session_id: str = "") -> None:
        self.stop_calls += 1

    async def wait_until_connected(self, timeout: float) -> bool:
        return False

    def set_claimed_session_provider(self, provider) -> None:
        self.claim_provider = provider

    def set_negotiated_wire(self, wire) -> None:
        self.negotiated_wire = wire


class DomainTransportOwnershipTests(unittest.TestCase):
    """End-to-end inbound transport ownership through the DOMAIN registry."""

    FULL_PN = "V001020SYN62344022"
    SHORT_PN = "V001020SYN6234"
    OTHER_PN = "V000405SYN94677058"
    ENTRY = "entry-1"
    OTHER_ENTRY = "entry-2"

    def _manager(self, *, collector_pn: str) -> EybondRuntimeLinkManager:
        with patch(
            "custom_components.eybond_local.runtime.link.resolve_server_ip",
            return_value="192.168.1.10",
        ):
            manager = EybondRuntimeLinkManager(
                server_ip="192.168.1.10",
                collector_ip="",
                collector_pn=collector_pn,
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
            )
        manager._announcer = _FakeAnnouncer()  # type: ignore[assignment]
        manager.set_reverse_discovery_enabled(False)  # inbound entry
        manager._started = True
        manager._listener_status = "listening"
        # Primary transport (port 8899) sees no session in these scenarios.
        manager._transport = _FakeTransport(connected=False, connect_result=False)  # type: ignore[assignment]
        return manager

    @staticmethod
    def _domain_session(
        session_id: str,
        pn: str,
        *,
        listener_port: int,
        state: str = "parked_waiting_for_identity",
        shape: str = "eybond_framed_or_binary",
        source: str = "framed_heartbeat",
        peer_ip: str = "203.0.113.10",
    ) -> dict[str, object]:
        return {
            "session_id": session_id,
            "peer_ip": peer_ip,
            "listener_port": listener_port,
            "collector_pn": pn,
            "state": state,
            "protocol_shape": shape,
            "collector_identity_source": source,
        }

    def _wire_domain(self, manager, inventory: list[dict[str, object]], *, entry_id: str = ""):
        domain = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        entry = entry_id or self.ENTRY
        domain.claim(entry, collector_pn=manager._collector_pn)
        manager.set_callback_ownership(domain, entry)
        return domain

    def _install_aux_fakes(self, manager) -> dict[int, _ClaimRecordingTransport]:
        built: dict[int, _ClaimRecordingTransport] = {}

        def _pair(_host: str, port: int):
            payload = _ClaimRecordingTransport(connected=False, connect_result=True)
            built[port] = payload
            return payload, _FakeAuxAtTransport()

        manager._build_transport_pair = _pair  # type: ignore[assignment]
        return built

    # 1. Entry primary 8899; owned session of the same PN arrives on listener
    # 18899: runtime follows via the domain registry, claims the exact session
    # id, becomes connected, and sends ZERO UDP triggers.
    def test_owned_session_on_other_listener_connects_without_udp(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        built = self._install_aux_fakes(manager)
        inventory = [
            self._domain_session("listener-18899-7", self.SHORT_PN, listener_port=18899)
        ]
        self._wire_domain(manager, inventory)
        manager._announcer.last_reply = ""
        manager._announcer.last_reply_from = ""

        ok, probe = CallbackOnDemandPhase3Tests._run_connect(self, manager)

        self.assertTrue(ok)
        self.assertIn(18899, manager._auxiliary_listener_ports)
        aux = built[18899]
        self.assertTrue(aux.connected)
        # Exact session id claim through the registry-mediated provider.
        self.assertIsNotNone(aux.claim_provider)
        self.assertEqual(aux.claim_provider(), "listener-18899-7")
        # Zero UDP: inbound never triggers, and no UDP side effects appear.
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(manager._callback_trigger_count, 0)
        self.assertEqual(manager._announcer.last_reply_from, "")
        # Ownership diagnostics expose the full chain.
        diagnostics = manager.listener_diagnostics()
        self.assertEqual(
            diagnostics["collector_session_ownership_authority"], "domain_registry"
        )
        self.assertEqual(diagnostics["collector_session_claim_entry_id"], self.ENTRY)
        self.assertEqual(diagnostics["collector_claimed_session_id"], "listener-18899-7")
        self.assertEqual(diagnostics["collector_claimed_listener_port"], 18899)
        self.assertEqual(diagnostics["collector_primary_tcp_port"], 8899)
        self.assertEqual(diagnostics["collector_active_listener_port"], 18899)

    # 2. Reconnect: same PN first on 8899, then a NEW session id on 18899 --
    # runtime follows the new handle without reload and without UDP.
    def test_reconnect_follows_new_session_on_other_listener(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        built = self._install_aux_fakes(manager)
        inventory = [
            self._domain_session(
                "listener-8899-1", self.FULL_PN, listener_port=8899, state="routed_framed"
            )
        ]
        self._wire_domain(manager, inventory)

        # Phase 1: session on the primary port -- claim id follows it.
        self.assertEqual(manager._claimed_session_id(), "listener-8899-1")

        # Phase 2: old socket closes; the collector dials the 18899 listener.
        inventory.clear()
        inventory.append(
            self._domain_session("listener-18899-2", self.FULL_PN, listener_port=18899)
        )

        ok, probe = CallbackOnDemandPhase3Tests._run_connect(self, manager)

        self.assertTrue(ok)
        self.assertEqual(probe.await_count, 0)
        self.assertIn(18899, manager._auxiliary_listener_ports)
        self.assertEqual(manager._claimed_session_id(), "listener-18899-2")
        self.assertEqual(built[18899].claim_provider(), "listener-18899-2")

    # 3. Full entry PN + short heartbeat PN are ONE identity; the durable full
    # PN never degrades to the short observation.
    def test_short_heartbeat_pn_matches_full_entry_pn(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        self._install_aux_fakes(manager)
        inventory = [
            self._domain_session("listener-18899-3", self.SHORT_PN, listener_port=18899)
        ]
        domain = self._wire_domain(manager, inventory)

        session = manager._owned_domain_session()
        self.assertIsNotNone(session)
        self.assertEqual(session.session_id, "listener-18899-3")
        # Durable identity stays the FULL PN.
        self.assertEqual(domain.claimed_identity(self.ENTRY), self.FULL_PN)
        self.assertEqual(manager._collector_pn, self.FULL_PN)

    # 4. Two different full PNs behind one peer IP: each entry only ever sees
    # its own session -- no socket stealing.
    def test_two_collectors_one_peer_ip_no_socket_stealing(self) -> None:
        inventory = [
            self._domain_session(
                "listener-18899-a", self.FULL_PN, listener_port=18899, peer_ip="203.0.113.10"
            ),
            self._domain_session(
                "listener-18899-b", self.OTHER_PN, listener_port=18899, peer_ip="203.0.113.10"
            ),
        ]
        domain = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        domain.claim(self.ENTRY, collector_pn=self.FULL_PN)
        domain.claim(self.OTHER_ENTRY, collector_pn=self.OTHER_PN)

        manager_a = self._manager(collector_pn=self.FULL_PN)
        manager_a.set_callback_ownership(domain, self.ENTRY)
        manager_b = self._manager(collector_pn=self.OTHER_PN)
        manager_b.set_callback_ownership(domain, self.OTHER_ENTRY)

        self.assertEqual(manager_a._claimed_session_id(), "listener-18899-a")
        self.assertEqual(manager_b._claimed_session_id(), "listener-18899-b")

    # 5. Session identity owned by ANOTHER entry: no socket, no follow; the
    # typed ownership diagnostic stays honest.
    def test_session_claimed_by_other_entry_is_not_taken(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        built = self._install_aux_fakes(manager)
        inventory = [
            self._domain_session("listener-18899-9", self.FULL_PN, listener_port=18899)
        ]
        domain = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        domain.claim(self.OTHER_ENTRY, collector_pn=self.FULL_PN)
        manager.set_callback_ownership(domain, self.ENTRY)

        self.assertIsNone(manager._owned_domain_session())
        self.assertEqual(manager._claimed_session_id(), "")

        ok, probe = CallbackOnDemandPhase3Tests._run_connect(self, manager)
        self.assertFalse(ok)
        self.assertEqual(probe.await_count, 0)
        self.assertEqual(built, {})  # no facade was even created

    # 6. Live framed session on a non-primary listener selects framed_fc4 even
    # against a persisted at_text hint.
    def test_live_framed_on_other_listener_overrides_persisted_at_text(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        manager._configured_collector_session_protocol = "at_text"
        self._install_aux_fakes(manager)
        inventory = [
            self._domain_session(
                "listener-18899-4",
                self.FULL_PN,
                listener_port=18899,
                state="routed_framed",
                shape="eybond_framed_or_binary",
            )
        ]
        self._wire_domain(manager, inventory)

        handle = manager.session_handle
        self.assertTrue(handle.observed)
        self.assertEqual(handle.wire_framing, "eybond_framed")
        self.assertEqual(
            manager._inverter_forward_adapter(), "framed_fc4"
        )

    # 7. Live AT-primary session uses the exact-session data-plane negotiator.
    def test_live_at_text_on_other_listener_uses_mixed_data_plane(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        manager._configured_collector_session_protocol = "eybond_framed"
        self._install_aux_fakes(manager)
        inventory = [
            self._domain_session(
                "listener-18899-5",
                self.FULL_PN,
                listener_port=18899,
                state="routed_at_text",
                shape="at_text",
                source="at_dtupn",
            )
        ]
        self._wire_domain(manager, inventory)

        handle = manager.session_handle
        self.assertTrue(handle.observed)
        self.assertEqual(handle.wire_framing, "at_text")
        self.assertEqual(
            manager._inverter_forward_adapter(), "at_mixed_forward"
        )

    def test_owned_session_monitor_reports_cross_listener_replacement(self) -> None:
        async def _run() -> None:
            manager = self._manager(collector_pn=self.FULL_PN)
            inventory = [
                self._domain_session(
                    "listener-18899-old",
                    self.FULL_PN,
                    listener_port=18899,
                    state="routed_framed",
                )
            ]
            self._wire_domain(manager, inventory)
            notifications: list[str] = []
            manager.set_collector_connection_watcher(notifications.append)
            manager._start_owned_session_monitor()
            baseline = manager.owned_session_generation

            inventory.clear()
            inventory.append(
                self._domain_session(
                    "listener-8899-new",
                    self.SHORT_PN,
                    listener_port=8899,
                    state="routed_framed",
                    peer_ip="192.0.2.44",
                )
            )

            await asyncio.wait_for(
                manager.async_wait_for_owned_session_change(baseline),
                timeout=1.0,
            )
            self.assertGreater(manager.owned_session_generation, baseline)
            self.assertEqual(manager._claimed_session_id(), "listener-8899-new")
            self.assertEqual(notifications, ["192.0.2.44"])
            await manager._stop_owned_session_monitor()

        asyncio.run(_run())

    # 9. Stop releases the auxiliary facades; the domain claim release itself is
    # owned by the entry unload hook (integration __init__).
    def test_stop_cleans_auxiliary_facades(self) -> None:
        manager = self._manager(collector_pn=self.FULL_PN)
        built = self._install_aux_fakes(manager)
        inventory = [
            self._domain_session("listener-18899-6", self.FULL_PN, listener_port=18899)
        ]
        self._wire_domain(manager, inventory)
        CallbackOnDemandPhase3Tests._run_connect(self, manager)
        self.assertIn(18899, built)

        asyncio.run(manager.async_stop())

        self.assertGreaterEqual(built[18899].stop_calls, 1)


if __name__ == "__main__":
    unittest.main()
