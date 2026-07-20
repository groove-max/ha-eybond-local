from __future__ import annotations

import asyncio
import types
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.passive_discovery import (
    PassiveCallbackDiscovery,
    active_callback_probe_scope,
)


class _FakeFlowManager:
    def __init__(self) -> None:
        self.flows: list[tuple[str, dict, dict]] = []
        self.aborted: list[str] = []

    async def async_init(self, domain, *, context, data):
        flow_id = f"flow-{len(self.flows) + 1}"
        context = dict(context or {})
        context.setdefault("flow_id", flow_id)
        self.flows.append((domain, context, data))

    def async_progress(self, include_uninitialized=False):
        return [
            {
                "flow_id": context.get("flow_id"),
                "handler": domain,
                "context": context,
                "data": data,
            }
            for domain, context, data in self.flows
        ]

    async def async_abort(self, flow_id):
        self.aborted.append(flow_id)
        self.flows = [
            flow
            for flow in self.flows
            if flow[1].get("flow_id") != flow_id
        ]


class _FakeConfigEntries:
    def __init__(self, entries=()) -> None:
        self.flow = _FakeFlowManager()
        self._entries = list(entries)
        self.updates: list[tuple[object, dict]] = []

    def async_entries(self, domain):
        return list(self._entries) if domain == DOMAIN else []

    def async_update_entry(self, entry, **kwargs):
        self.updates.append((entry, kwargs))
        if "data" in kwargs:
            entry.data = kwargs["data"]
        if "unique_id" in kwargs:
            entry.unique_id = kwargs["unique_id"]
        if "title" in kwargs:
            entry.title = kwargs["title"]
        return True


class _FakeHass:
    def __init__(self, entries=()) -> None:
        self.config_entries = _FakeConfigEntries(entries)
        self.background_task_names: list[str] = []

    def async_create_task(self, coro):
        return asyncio.create_task(coro)

    def async_create_background_task(self, coro, name):
        self.background_task_names.append(name)
        return asyncio.create_task(coro)


class _FakeListener:
    def __init__(self, sessions) -> None:
        self._sessions = tuple(sessions)

    def discovered_collector_sessions(self):
        return self._sessions


_ACQUIRE = "custom_components.eybond_local.passive_discovery._acquire_shared_listener"
_RELEASE = "custom_components.eybond_local.passive_discovery._release_shared_listener"


class EnsureObservedListenerTests(unittest.IsolatedAsyncioTestCase):
    """Batch 8B.2A: the public scoped custom-listener registration (direction B)."""

    def _discovery(self):
        hass = _FakeHass()
        hass.data = {}
        discovery = PassiveCallbackDiscovery(hass)
        hass.data[DOMAIN] = {"passive_callback_discovery": discovery}
        return discovery

    async def test_ensured_custom_listener_reaches_domain_registry(self) -> None:
        discovery = self._discovery()
        session = {
            "session_id": "s1",
            "collector_pn": "V001020SYN62344022",
            "peer_ip": "203.0.113.9",
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }
        listener = _FakeListener((session,))
        released: list = []
        with patch(_ACQUIRE, AsyncMock(return_value=listener)), patch(
            _RELEASE, AsyncMock(side_effect=lambda l: released.append(l))
        ):
            # A random custom port is invisible to the domain registry until ensured.
            self.assertEqual(discovery.iter_observed_sessions(), ())
            token = await discovery.async_ensure_observed_listener("127.0.0.1", 40001)
            # The SAME domain registry now observes the custom-port session.
            rows = discovery.registry.observed_sessions_per_socket()
            self.assertTrue(
                any(r.session_id == "s1" and r.listener_port == 40001 for r in rows)
            )
            # Scoped release removes it (and drops the listener ref exactly once).
            await discovery.async_release_observed_listener(token)
            self.assertEqual(discovery.iter_observed_sessions(), ())
            self.assertEqual(released, [listener])
            # Idempotent second release.
            await discovery.async_release_observed_listener(token)
            self.assertEqual(released, [listener])

    async def test_session_deduped_by_port_and_session_id(self) -> None:
        # A listener visible from BOTH a standard source and an ensure on the same
        # port yields each physical session exactly once.
        discovery = self._discovery()
        session = {
            "session_id": "dup",
            "collector_pn": "V001020SYN62344022",
            "peer_ip": "203.0.113.9",
            "state": "routed_framed",
        }
        listener = _FakeListener((session,))
        discovery._listeners[40002] = listener
        with patch(_ACQUIRE, AsyncMock(return_value=listener)), patch(
            _RELEASE, AsyncMock()
        ):
            await discovery.async_ensure_observed_listener("127.0.0.1", 40002)
            rows = [
                s for s in discovery.iter_observed_sessions() if s["session_id"] == "dup"
            ]
            self.assertEqual(len(rows), 1)

    async def test_two_tokens_same_port_are_refcounted(self) -> None:
        # Batch 8B.2A corrective: two concurrent ensures on ONE (host, port) each
        # hold a ref; the listener stays observed until BOTH release, release is
        # idempotent, and stop never double-releases.
        discovery = self._discovery()
        session = {
            "session_id": "sess",
            "collector_pn": "V001020SYN62344022",
            "peer_ip": "203.0.113.9",
            "state": "routed_framed",
        }
        listener = _FakeListener((session,))
        released: list = []
        with patch(_ACQUIRE, AsyncMock(return_value=listener)), patch(
            _RELEASE, AsyncMock(side_effect=lambda l, **k: released.append(l))
        ):
            t1 = await discovery.async_ensure_observed_listener("127.0.0.1", 40100)
            t2 = await discovery.async_ensure_observed_listener("127.0.0.1", 40100)

            def _observed() -> bool:
                return any(
                    s["session_id"] == "sess"
                    for s in discovery.iter_observed_sessions()
                )

            # Deduped: the physical session appears once despite two tokens.
            self.assertEqual(
                sum(1 for s in discovery.iter_observed_sessions() if s["session_id"] == "sess"),
                1,
            )
            # First release: the second token still holds it observed.
            await discovery.async_release_observed_listener(t1)
            self.assertTrue(_observed())
            self.assertEqual(len(released), 1)
            # Second release: gone / closed.
            await discovery.async_release_observed_listener(t2)
            self.assertFalse(_observed())
            self.assertEqual(len(released), 2)
            # Idempotent: re-releasing either token drops no more refs.
            await discovery.async_release_observed_listener(t1)
            await discovery.async_release_observed_listener(t2)
            self.assertEqual(len(released), 2)
            # Stop does not double-release already-released leases.
            await discovery.async_stop()
            self.assertEqual(len(released), 2)

    async def test_stop_releases_ensured_leases(self) -> None:
        discovery = self._discovery()
        listener = _FakeListener(())
        released: list = []
        with patch(_ACQUIRE, AsyncMock(return_value=listener)), patch(
            _RELEASE, AsyncMock(side_effect=lambda l, **k: released.append(l))
        ):
            await discovery.async_ensure_observed_listener("127.0.0.1", 40003)
            await discovery.async_stop()
            self.assertIn(listener, released)
            self.assertEqual(discovery._ensured_listeners, {})


class PassiveCallbackDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_active_callback_probe_scope_always_closes_service_scope(self) -> None:
        hass = _FakeHass()
        hass.data = {}
        discovery = PassiveCallbackDiscovery(hass)
        hass.data[DOMAIN] = {"passive_callback_discovery": discovery}

        with self.assertRaisesRegex(RuntimeError, "probe failed"):
            with active_callback_probe_scope(hass, "manual-1"):
                self.assertIn("manual-1", discovery._active_probe_scopes)
                raise RuntimeError("probe failed")

        self.assertNotIn("manual-1", discovery._active_probe_scopes)

    async def test_callback_scope_retains_only_matcher_selected_session(self) -> None:
        hass = _FakeHass()
        hass.data = {}
        discovery = PassiveCallbackDiscovery(hass)
        listener = _FakeListener(())
        discovery._listeners[18899] = listener
        hass.data[DOMAIN] = {"passive_callback_discovery": discovery}

        with active_callback_probe_scope(hass, "manual-1") as retained:
            listener._sessions = (
                {
                    "session_id": "matched-session",
                    "peer_ip": "192.168.1.55",
                    "collector_pn": "E500SYN253884199645",
                    "state": "routed_framed",
                    "collector_identity_source": "fc2_parameter_2",
                },
                {
                    "session_id": "unrelated-session",
                    "peer_ip": "192.168.1.56",
                    "collector_pn": "I300SYN25063387",
                    "state": "routed_framed",
                    "collector_identity_source": "fc2_parameter_2",
                },
            )
            await discovery._async_poll_once()
            self.assertEqual(hass.config_entries.flow.flows, [])
            retained.add("matched-session")

        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        _domain, _context, data = hass.config_entries.flow.flows[0]
        self.assertEqual(data["collector_pn"], "I300SYN25063387")

    async def test_start_listens_on_legacy_binary_smartess_and_runtime_ports(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        acquired_ports: list[int] = []

        async def _fake_acquire(_host, port):
            acquired_ports.append(port)
            return _FakeListener(())

        with patch(
            "custom_components.eybond_local.passive_discovery._acquire_shared_listener",
            side_effect=_fake_acquire,
        ), patch(
            "custom_components.eybond_local.passive_discovery._release_shared_listener",
            new=AsyncMock(),
        ):
            await discovery.async_start()
            await discovery.async_stop()

        self.assertEqual(acquired_ports, [502, 8899, 18899])
        self.assertEqual(hass.background_task_names, ["EyeBond passive callback discovery"])

    async def test_stop_does_not_wait_forever_for_slow_listener_release(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(())
        release_started = asyncio.Event()
        release_finished = asyncio.Event()

        async def _slow_release(_listener):
            release_started.set()
            await asyncio.sleep(0.05)
            release_finished.set()

        with patch(
            "custom_components.eybond_local.passive_discovery._STOP_LISTENER_RELEASE_TIMEOUT_SECONDS",
            0.001,
        ), patch(
            "custom_components.eybond_local.passive_discovery._release_shared_listener",
            side_effect=_slow_release,
        ):
            await discovery.async_stop()

        self.assertTrue(release_started.is_set())
        self.assertEqual(discovery._listeners, {})
        await asyncio.wait_for(release_finished.wait(), timeout=0.2)

    async def test_poll_creates_integration_discovery_flow_for_unclaimed_collector_pn(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V000405SYN94677058",
                    "protocol_shape": "at_text",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )

        await discovery._async_poll_once()
        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        domain, context, data = hass.config_entries.flow.flows[0]
        self.assertEqual(domain, DOMAIN)
        self.assertEqual(context["source"], "integration_discovery")
        self.assertEqual(
            context["title_placeholders"],
            {"name": "Collector PN V000405SYN94677058"},
        )
        self.assertEqual(data["collector_pn"], "V000405SYN94677058")
        self.assertEqual(data["peer_ip"], "195.138.86.175")
        self.assertEqual(data["tcp_port"], 18899)
        self.assertEqual(
            context["eybond_discovery"]["collector_pn"],
            "V000405SYN94677058",
        )

    async def test_active_scan_scope_suppresses_only_new_callback_session(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        listener = _FakeListener(())
        discovery._listeners[18899] = listener

        discovery.begin_active_probe_scope("scan-1")
        listener._sessions = (
            {
                "session_id": "listener-18899-scan-result",
                "peer_ip": "192.168.1.55",
                "collector_pn": "E500SYN253884199645",
                "state": "routed_framed",
                "protocol_shape": "eybond_framed",
                "collector_identity_source": "fc2_parameter_2",
            },
        )

        await discovery._async_poll_once()
        discovery.end_active_probe_scope("scan-1")
        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])

    async def test_scan_suppression_ends_with_the_triggered_tcp_session(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        listener = _FakeListener(())
        discovery._listeners[18899] = listener

        discovery.begin_active_probe_scope("scan-1")
        listener._sessions = (
            {
                "session_id": "listener-18899-triggered",
                "peer_ip": "192.168.1.55",
                "collector_pn": "E500SYN253884199645",
                "state": "routed_framed",
                "protocol_shape": "eybond_framed",
                "collector_identity_source": "fc2_parameter_2",
            },
        )
        discovery.end_active_probe_scope("scan-1")
        await discovery._async_poll_once()
        self.assertEqual(hass.config_entries.flow.flows, [])

        # Closing the scan-triggered socket clears only its transient marker.
        listener._sessions = ()
        await discovery._async_poll_once()
        listener._sessions = (
            {
                "session_id": "listener-18899-later-inbound",
                "peer_ip": "192.168.1.55",
                "collector_pn": "E500SYN253884199645",
                "state": "routed_framed",
                "protocol_shape": "eybond_framed",
                "collector_identity_source": "fc2_parameter_2",
            },
        )
        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_removed_entry_session_is_not_republished_until_reconnect(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        session = {
            "session_id": "listener-8899-configured",
            "peer_ip": "192.168.1.55",
            "collector_pn": "E500SYN253884199645",
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }
        listener = _FakeListener((session,))
        discovery._listeners[8899] = listener
        discovery._registry.claim(
            "entry-e500",
            collector_pn="E500SYN253884199645",
        )

        discovery.retire_entry_sessions("entry-e500")
        discovery._registry.release("entry-e500")
        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])

        listener._sessions = ()
        await discovery._async_poll_once()
        listener._sessions = ({**session, "session_id": "listener-8899-reconnected"},)
        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_interactive_snapshot_includes_retired_live_session(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        session = {
            "session_id": "listener-8899-retired",
            "peer_ip": "203.0.113.17",
            "collector_pn": "V001107SYN282291016",
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }
        discovery._listeners[8899] = _FakeListener((session,))
        discovery._registry.claim(
            "entry-v0011",
            collector_pn="V001107SYN282291016",
        )
        discovery.retire_entry_sessions("entry-v0011")
        discovery._registry.release("entry-v0011")

        await discovery._async_poll_once()
        observations = discovery.snapshot_unclaimed_collector_sessions()

        # Passive publication stays retired, while an explicit interactive scan
        # sees the same live, unclaimed exact session from the shared inventory.
        self.assertEqual(hass.config_entries.flow.flows, [])
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].collector_pn, "V001107SYN282291016")
        self.assertEqual(observations[0].session_id, "listener-8899-retired")
        self.assertEqual(observations[0].listener_port, 8899)

    async def test_retired_socket_survives_coalesced_winner_change(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        retired = {
            "session_id": "listener-8899-retired",
            "peer_ip": "203.0.113.17",
            "collector_pn": "V001107SYN2822",
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }
        listener = _FakeListener((retired,))
        discovery._listeners[8899] = listener
        discovery._registry.claim(
            "entry-v0011",
            collector_pn="V001107SYN2822",
        )
        discovery.retire_entry_sessions("entry-v0011")
        discovery._registry.release("entry-v0011")

        # A temporary same-PN socket becomes the coalesced registry winner. It is
        # owned by an active scan, while the retired physical socket remains raw.
        replacement = {
            **retired,
            "session_id": "listener-8899-scan-overlap",
            "collector_pn": "V001107SYN282291016",
        }
        listener._sessions = (retired, replacement)
        discovery._probe_suppressed_sessions.add(
            "8899:listener-8899-scan-overlap"
        )
        await discovery._async_poll_once()

        # When the overlap disappears, the original exact socket must still be
        # retired; pruning through the old coalesced view lost this marker.
        listener._sessions = (retired,)
        await discovery._async_poll_once()
        self.assertEqual(hass.config_entries.flow.flows, [])

    async def test_user_refresh_republishes_retired_live_session_without_reconnect(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        session = {
            "session_id": "listener-8899-configured",
            "peer_ip": "192.168.1.55",
            "collector_pn": "E500SYN253884199645",
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }
        discovery._listeners[8899] = _FakeListener((session,))
        discovery._registry.claim(
            "entry-e500",
            collector_pn="E500SYN253884199645",
        )
        discovery.retire_entry_sessions("entry-e500")
        discovery._registry.release("entry-e500")
        await discovery._async_poll_once()
        self.assertEqual(hass.config_entries.flow.flows, [])

        result = await discovery.async_show_discovered_devices_again()

        self.assertEqual(result.connected_unclaimed_count, 1)
        self.assertEqual(result.suppressed_candidate_count, 1)
        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_user_refresh_does_not_publish_registry_owned_session(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            (
                {
                    "session_id": "listener-18899-owned",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "state": "routed_framed",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "fc2_parameter_2",
                },
            )
        )
        discovery._registry.claim(
            "entry-pi30",
            collector_pn="V001020SYN62344022",
        )

        result = await discovery.async_show_discovered_devices_again()

        self.assertEqual(result.connected_unclaimed_count, 0)
        self.assertEqual(result.suppressed_candidate_count, 0)
        self.assertEqual(hass.config_entries.flow.flows, [])

    async def test_user_refresh_does_not_duplicate_active_discovery_flow(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            (
                {
                    "session_id": "listener-18899-active-flow",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "state": "routed_framed",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "fc2_parameter_2",
                },
            )
        )
        await discovery._async_poll_once()
        self.assertEqual(len(hass.config_entries.flow.flows), 1)

        result = await discovery.async_show_discovered_devices_again()

        self.assertEqual(result.connected_unclaimed_count, 1)
        self.assertEqual(result.suppressed_candidate_count, 1)
        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_active_scan_scope_does_not_hide_preexisting_inbound_session(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            (
                {
                    "session_id": "listener-18899-preexisting",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V000405SYN94677058",
                    "state": "routed_framed",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "fc2_parameter_2",
                },
            )
        )

        discovery.begin_active_probe_scope("scan-1")
        await discovery._async_poll_once()
        discovery.end_active_probe_scope("scan-1")

        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_poll_does_not_publish_route_identity_mismatch(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-mismatch",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN6234",
                    "state": "route_identity_mismatch",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "framed_heartbeat",
                }
            ]
        )

        with patch(
            "custom_components.eybond_local.passive_discovery._WEAK_IDENTITY_SETTLE_SECONDS",
            0,
        ):
            await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])

    async def test_poll_publishes_strong_route_mismatch_as_distinct_collector(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-strong-mismatch",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "state": "route_identity_mismatch",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "fc2_parameter_2",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN62344022",
        )

    async def test_real_ha_progress_without_data_deduplicates_by_context_unique_id(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        hass.config_entries.flow.flows.append(
            (
                DOMAIN,
                {
                    "source": "integration_discovery",
                    "flow_id": "flow-full",
                    "unique_id": "collector:V001020SYN62344022",
                    "title_placeholders": {
                        "name": "Collector PN V001020SYN62344022"
                    },
                },
                {},
            )
        )
        # Match Home Assistant's real async_progress() shape: it exposes
        # context/unique_id but not the original discovery data.
        hass.config_entries.flow.async_progress = lambda include_uninitialized=False: [
            {
                "flow_id": "flow-full",
                "handler": DOMAIN,
                "context": hass.config_entries.flow.flows[0][1],
                "step_id": "manual",
            }
        ]
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-new",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN6234",
                    "state": "routed_framed",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "framed_heartbeat",
                }
            ]
        )

        with patch(
            "custom_components.eybond_local.passive_discovery._WEAK_IDENTITY_SETTLE_SECONDS",
            0,
        ):
            await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_poll_does_not_republish_session_while_verification_claim_active(self) -> None:
        # An active temporary strategy-verification claim owns the session in
        # the registry: passive discovery must not publish a second candidate
        # for it, while an unrelated collector (other PN, same peer IP) still
        # gets its own flow.
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V000405SYN94677058",
                    "protocol_shape": "at_text",
                    "collector_identity_source": "at_dtupn",
                },
                {
                    "session_id": "listener-18899-2",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "protocol_shape": "at_text",
                    "collector_identity_source": "at_dtupn",
                },
            ]
        )
        discovery._registry.claim(
            "strategy_verification:test",
            collector_pn="V000405SYN94677058",
            session_id="listener-18899-1",
        )

        await discovery._async_poll_once()

        published = {
            data["collector_pn"] for _domain, _context, data in hass.config_entries.flow.flows
        }
        # The claimed collector is suppressed; the independent one is published.
        self.assertEqual(published, {"V001020SYN62344022"})

        discovery._registry.release("strategy_verification:test")
        await discovery._async_poll_once()
        published = {
            data["collector_pn"] for _domain, _context, data in hass.config_entries.flow.flows
        }
        self.assertEqual(
            published, {"V001020SYN62344022", "V000405SYN94677058"}
        )

    async def test_poll_does_not_republish_same_live_session_after_user_cancelled_flow(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        sessions = [
            {
                "session_id": "listener-18899-1",
                "peer_ip": "195.138.86.175",
                "collector_pn": "V001020SYN62344022",
                "protocol_shape": "at_text",
                "collector_identity_source": "at_dtupn",
            }
        ]
        discovery._listeners[18899] = _FakeListener(sessions)

        await discovery._async_poll_once()
        self.assertEqual(len(hass.config_entries.flow.flows), 1)

        # Simulate the frontend/user cancelling the discovery flow while the
        # collector remains connected.  Passive discovery is edge-triggered:
        # do not recreate the same flow every poll for the same live socket,
        # because HA may surface that as "already_in_progress" duplicates.
        hass.config_entries.flow.flows.clear()

        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 0)

        # Once the old socket disappears, a later callback is a new edge and
        # can be published again.
        discovery._listeners[18899] = _FakeListener(())
        await discovery._async_poll_once()
        sessions[0] = {**sessions[0], "session_id": "listener-18899-2"}
        discovery._listeners[18899] = _FakeListener(sessions)
        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        _domain, context, data = hass.config_entries.flow.flows[0]
        self.assertEqual(
            context["title_placeholders"],
            {"name": "Collector PN V001020SYN62344022"},
        )
        self.assertEqual(data["collector_pn"], "V001020SYN62344022")

    async def test_poll_publishes_unclaimed_live_session_only_once(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[8899] = _FakeListener(
            [
                {
                    "session_id": "listener-8899-2",
                    "peer_ip": "192.168.1.55",
                    "collector_pn": "E500002SYN84199645",
                    "protocol_shape": "eybond_framed",
                    "collector_identity_source": "at_dtupn",
                    "state": "routed_framed",
                }
            ]
        )

        await discovery._async_poll_once()
        await discovery._async_poll_once()
        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)

    async def test_poll_ignores_weak_short_pn_without_strong_identity(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN6234",
                    "collector_identity_source": "framed_heartbeat",
                },
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])

    async def test_poll_coalesces_weak_short_pn_into_strong_full_pn(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN6234",
                    "collector_identity_source": "framed_heartbeat",
                },
                {
                    "session_id": "listener-18899-2",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "at_dtupn",
                },
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        _domain, context, data = hass.config_entries.flow.flows[0]
        self.assertEqual(data["collector_pn"], "V001020SYN62344022")
        self.assertEqual(
            context["title_placeholders"],
            {"name": "Collector PN V001020SYN62344022"},
        )

    async def test_poll_preserves_weak_short_pn_flow_when_full_pn_arrives(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        hass.config_entries.flow.flows.append(
            (
                DOMAIN,
                {
                    "source": "integration_discovery",
                    "flow_id": "flow-1",
                },
                {
                    "collector_pn": "V001020SYN6234",
                    "peer_ip": "195.138.86.175",
                    "tcp_port": 18899,
                    "collector_identity_source": "framed_heartbeat",
                },
            )
        )
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-2",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )
        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.aborted, [])
        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN6234",
        )

    async def test_poll_preserves_same_session_flow_during_pn_enrichment(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        hass.config_entries.flow.flows.append(
            (
                DOMAIN,
                {
                    "source": "integration_discovery",
                    "flow_id": "flow-1",
                },
                {
                    "collector_pn": "V001020SYN6234",
                    "peer_ip": "195.138.86.175",
                    "tcp_port": 18899,
                    "session_id": "listener-18899-1",
                    "collector_identity_source": "framed_heartbeat",
                },
            )
        )
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "fc2_parameter_2",
                }
            ]
        )

        with patch(
            "custom_components.eybond_local.passive_discovery._WEAK_IDENTITY_SETTLE_SECONDS",
            0,
        ):
            await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.aborted, [])
        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN6234",
        )

    async def test_poll_preserves_short_pn_flow_across_enriched_reconnect(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        hass.config_entries.flow.flows.append(
            (
                DOMAIN,
                {
                    "source": "integration_discovery",
                    "flow_id": "flow-1",
                },
                {
                    "collector_pn": "V001020SYN6234",
                    "peer_ip": "192.168.1.1",
                    "tcp_port": 18899,
                    "session_id": "listener-18899-1",
                    "collector_identity_source": "framed_heartbeat",
                },
            )
        )
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-2",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.aborted, [])
        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        _domain, context, data = hass.config_entries.flow.flows[0]
        self.assertNotIn("title_placeholders", context)
        self.assertEqual(data["collector_pn"], "V001020SYN6234")
        self.assertEqual(data["peer_ip"], "192.168.1.1")

    async def test_poll_suppresses_late_short_pn_when_full_pn_flow_exists(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        hass.config_entries.flow.flows.append(
            (
                DOMAIN,
                {
                    "source": "integration_discovery",
                    "flow_id": "flow-1",
                },
                {
                    "collector_pn": "V001020SYN62344022",
                    "peer_ip": "195.138.86.175",
                    "tcp_port": 18899,
                    "session_id": "listener-18899-2",
                    "collector_identity_source": "at_dtupn",
                },
            )
        )
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-3",
                    "peer_ip": "192.168.1.1",
                    "collector_pn": "V001020SYN6234",
                    "collector_identity_source": "framed_heartbeat",
                }
            ]
        )

        with patch(
            "custom_components.eybond_local.passive_discovery._WEAK_IDENTITY_SETTLE_SECONDS",
            0,
        ):
            await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.aborted, [])
        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN62344022",
        )

    async def test_poll_allows_multiple_sessions_behind_same_peer_ip(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "at_dtupn",
                },
                {
                    "session_id": "listener-18899-2",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V000405SYN94677058",
                    "collector_identity_source": "at_dtupn",
                },
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 2)
        self.assertEqual(
            {data["collector_pn"] for _domain, _context, data in hass.config_entries.flow.flows},
            {"V001020SYN62344022", "V000405SYN94677058"},
        )

    async def test_poll_skips_weak_short_pn_when_strong_full_pn_flow_is_active(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        hass.config_entries.flow.flows.append(
            (
                DOMAIN,
                {
                    "source": "integration_discovery",
                    "flow_id": "flow-existing",
                },
                {
                    "collector_pn": "V001020SYN62344022",
                    "peer_ip": "195.138.86.175",
                    "tcp_port": 18899,
                    "collector_identity_source": "at_dtupn",
                },
            )
        )
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN6234",
                    "collector_identity_source": "framed_heartbeat",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.aborted, [])
        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN62344022",
        )

    async def test_poll_waits_for_strong_identity_before_notifying_short_pn(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN6234",
                    "collector_identity_source": "framed_heartbeat",
                }
            ]
        )

        await discovery._async_poll_once()
        self.assertEqual(hass.config_entries.flow.flows, [])

        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )
        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN62344022",
        )

    async def test_poll_skips_existing_collector_pn(self) -> None:
        entry = types.SimpleNamespace(
            data={"collector_pn": "V000405SYN94677058"},
            unique_id="collector:V000405SYN94677058",
            title="Collector PN V000405SYN94677058",
        )
        hass = _FakeHass(entries=[entry])
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V000405SYN94677058",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])

    async def test_poll_aborts_discovery_flow_after_collector_gets_configured(self) -> None:
        hass = _FakeHass()
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[8899] = _FakeListener(
            [
                {
                    "session_id": "listener-8899-1",
                    "peer_ip": "192.168.1.55",
                    "collector_pn": "E500002SYN84199645",
                    "collector_identity_source": "at_dtupn",
                    "protocol_shape": "eybond_framed",
                    "state": "routed_framed",
                }
            ]
        )

        await discovery._async_poll_once()
        self.assertEqual(len(hass.config_entries.flow.flows), 1)

        entry = types.SimpleNamespace(
            data={"collector_pn": "E500002SYN84199645"},
            unique_id="collector:E500002SYN84199645",
            title="Collector PN E500002SYN84199645",
        )
        hass.config_entries._entries.append(entry)

        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])
        self.assertEqual(hass.config_entries.flow.aborted, ["flow-1"])

    async def test_poll_upgrades_existing_callback_entry_from_short_to_full_pn(self) -> None:
        entry = types.SimpleNamespace(
            data={
                "collector_pn": "V001107SYN8229",
                "connection_mode": "callback_listener",
                "collector_operation_mode": "home_assistant_only",
                "tcp_port": 18899,
                "collector_session_protocol": "eybond_framed",
            },
            unique_id="collector:V001107SYN8229",
            title="Collector PN V001107SYN8229",
        )
        hass = _FakeHass(entries=[entry])
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "192.168.1.1",
                    "collector_pn": "V001107SYN82291016",
                    "protocol_shape": "at_text",
                    "state": "routed_at_text",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(hass.config_entries.flow.flows, [])
        self.assertEqual(entry.data["collector_pn"], "V001107SYN82291016")
        self.assertEqual(entry.data["collector_session_protocol"], "at_text")
        self.assertEqual(entry.unique_id, "collector:V001107SYN82291016")
        self.assertEqual(entry.title, "Collector PN V001107SYN82291016")

    async def test_live_session_port_does_not_rewrite_stable_entry_port(self) -> None:
        entry = types.SimpleNamespace(
            data={
                "collector_pn": "V001107SYN82291016",
                "connection_mode": "callback_listener",
                "collector_operation_mode": "home_assistant_only",
                "tcp_port": 18899,
                "collector_session_protocol": "eybond_framed",
            },
            unique_id="collector:V001107SYN82291016",
            title="Collector PN V001107SYN82291016",
        )
        hass = _FakeHass(entries=[entry])
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[8899] = _FakeListener(
            [
                {
                    "session_id": "listener-8899-replacement",
                    "peer_ip": "192.168.1.51",
                    "collector_pn": "V001107SYN82291016",
                    "protocol_shape": "eybond_framed",
                    "state": "routed_framed",
                    "collector_identity_source": "fc2_parameter_2",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(entry.data["tcp_port"], 18899)

    async def test_poll_does_not_treat_existing_short_pn_entry_as_full_pn_match(self) -> None:
        entry = types.SimpleNamespace(
            data={
                "collector_pn": "V001020SYN6234",
                "connection_mode": "known_ip",
                "collector_operation_mode": "home_assistant_only",
            },
            unique_id="collector:V001020SYN6234",
            title="Collector PN V001020SYN6234",
        )
        hass = _FakeHass(entries=[entry])
        discovery = PassiveCallbackDiscovery(hass)
        discovery._listeners[18899] = _FakeListener(
            [
                {
                    "session_id": "listener-18899-1",
                    "peer_ip": "195.138.86.175",
                    "collector_pn": "V001020SYN62344022",
                    "collector_identity_source": "at_dtupn",
                }
            ]
        )

        await discovery._async_poll_once()

        self.assertEqual(len(hass.config_entries.flow.flows), 1)
        self.assertEqual(
            hass.config_entries.flow.flows[0][2]["collector_pn"],
            "V001020SYN62344022",
        )
        self.assertEqual(entry.data["collector_pn"], "V001020SYN6234")
        self.assertEqual(entry.data["connection_mode"], "known_ip")
        self.assertEqual(entry.unique_id, "collector:V001020SYN6234")


if __name__ == "__main__":
    unittest.main()
