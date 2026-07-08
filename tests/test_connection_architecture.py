"""Architectural invariant tests for the collector connection axes.

These lock in the contract from the local passive callback architecture plan:
transport ownership (``connection_strategy``), endpoint control
(``endpoint_control_policy``), and proxy state (``proxy_enabled``) are explicit,
opaque, hostname-free entry axes, and the callback session registry owns durable
PN identity and short/full PN reconciliation. Peer IP is never durable identity.
"""

from __future__ import annotations

import asyncio
import types
import unittest

from custom_components.eybond_local import const as C
from custom_components.eybond_local.connection import connection_policy as cp
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
    pn_is_same_identity,
)


def _session(session_id, pn, *, peer_ip="203.0.113.10", source="at_dtupn", state="identified"):
    return {
        "session_id": session_id,
        "peer_ip": peer_ip,
        "peer_port": 5000,
        "state": state,
        "protocol_shape": "eybond_framed",
        "collector_pn": pn,
        "collector_identity_source": source,
    }


class ConnectionPolicyInvariantTests(unittest.TestCase):
    def test_empty_entry_defaults_to_inbound_external_proxy_off(self) -> None:
        self.assertEqual(cp.resolve_connection_strategy({}, {}), C.CONNECTION_STRATEGY_INBOUND)
        self.assertEqual(
            cp.resolve_endpoint_control_policy({}, {}), C.ENDPOINT_CONTROL_EXTERNAL
        )
        self.assertFalse(cp.resolve_proxy_enabled({}, {}))

    def test_smartess_and_ha_legacy_maps_to_callback_on_demand(self) -> None:
        data = {C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA}
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_ha_only_legacy_maps_to_inbound(self) -> None:
        data = {C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY}
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}), C.CONNECTION_STRATEGY_INBOUND
        )

    def test_callback_listener_connection_mode_maps_to_inbound(self) -> None:
        # Passive-callback entries (the collector already dials Home Assistant)
        # are inbound even without an operation mode.
        data = {C.CONF_CONNECTION_MODE: "callback_listener"}
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}), C.CONNECTION_STRATEGY_INBOUND
        )

    def test_explicit_axis_overrides_legacy_derivation(self) -> None:
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_integration_write_provenance_implies_integration_managed(self) -> None:
        data = {C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: "config_flow_pre_bind"}
        self.assertEqual(
            cp.resolve_endpoint_control_policy(data, {}),
            C.ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

    def test_merely_observed_endpoint_source_stays_external(self) -> None:
        data = {C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: "collector_registry"}
        self.assertEqual(
            cp.resolve_endpoint_control_policy(data, {}), C.ENDPOINT_CONTROL_EXTERNAL
        )

    def test_written_value_implies_integration_managed(self) -> None:
        data = {C.CONF_ENDPOINT_WRITTEN_VALUE: "192.168.1.5,8899,TCP"}
        self.assertEqual(
            cp.resolve_endpoint_control_policy(data, {}),
            C.ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

    def test_predicates_gate_callback_and_endpoint(self) -> None:
        # inbound entry never sends a callback trigger / runs reverse discovery.
        self.assertFalse(cp.may_send_callback_trigger(C.CONNECTION_STRATEGY_INBOUND))
        self.assertFalse(
            cp.may_run_steady_reverse_discovery(C.CONNECTION_STRATEGY_INBOUND)
        )
        # callback_on_demand is the only strategy allowed to send the trigger.
        self.assertTrue(
            cp.may_send_callback_trigger(C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND)
        )
        # external endpoint policy never auto-writes/restores; managed may.
        self.assertFalse(cp.may_auto_manage_endpoint(C.ENDPOINT_CONTROL_EXTERNAL))
        self.assertTrue(
            cp.may_auto_manage_endpoint(C.ENDPOINT_CONTROL_INTEGRATION_MANAGED)
        )

    def test_strategy_is_hostname_free(self) -> None:
        # A cloud-looking endpoint hostname must not change the resolved strategy.
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
            C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: "dtu_ess.eybond.com,18899,TCP",
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}), C.CONNECTION_STRATEGY_INBOUND
        )
        # And the reverse: an explicit callback strategy with a LAN-looking
        # endpoint still resolves to callback_on_demand.
        data2 = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: "192.168.1.50,18899,TCP",
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data2, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_original_endpoint_preserved_opaquely(self) -> None:
        # The previous endpoint is preserved verbatim (opaque) and not classified.
        endpoint = "some.vendor.example,18899,TCP"
        diagnostics = cp.entry_axis_diagnostics(
            {C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: endpoint}, {}
        )
        self.assertEqual(diagnostics["original_endpoint"], endpoint)

    def test_migrate_entry_axes_fills_all_three(self) -> None:
        axes = cp.migrate_entry_axes(
            {C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA}, {}
        )
        self.assertEqual(
            axes[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        )
        self.assertEqual(
            axes[C.CONF_ENDPOINT_CONTROL_POLICY], C.ENDPOINT_CONTROL_EXTERNAL
        )
        self.assertIn(C.CONF_PROXY_ENABLED, axes)

    def test_accepting_passive_candidate_is_inbound_external(self) -> None:
        # Simulate the data a passive-discovery candidate carries into entry
        # creation: connection_mode=callback_listener. It must become an inbound,
        # external entry.
        candidate_data = {
            C.CONF_CONNECTION_MODE: "callback_listener",
            C.CONF_COLLECTOR_PN: "V00ABC1234567890",
        }
        axes = cp.migrate_entry_axes(candidate_data, {})
        self.assertEqual(
            axes[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        self.assertEqual(
            axes[C.CONF_ENDPOINT_CONTROL_POLICY], C.ENDPOINT_CONTROL_EXTERNAL
        )


class CallbackSessionRegistryInvariantTests(unittest.TestCase):
    def test_two_collectors_one_peer_ip_distinct_when_pn_differs(self) -> None:
        sessions = [
            _session("s1", "V00AAA1111111111", peer_ip="203.0.113.10"),
            _session("s2", "V00BBB2222222222", peer_ip="203.0.113.10"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        observed = registry.observed_sessions()
        self.assertEqual(len(observed), 2)
        self.assertEqual(
            sorted(s.collector_pn for s in observed),
            ["V00AAA1111111111", "V00BBB2222222222"],
        )

    def test_short_pn_enriched_by_full_pn_not_duplicated(self) -> None:
        sessions = [
            _session("s1", "V00CCC3333", source="heartbeat"),
            _session("s2", "V00CCC3333444455", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        observed = registry.observed_sessions()
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0].collector_pn, "V00CCC3333444455")

    def test_session_claimed_by_exactly_one_entry(self) -> None:
        sessions = [_session("s1", "V00AAA1111111111")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entryA", collector_pn="V00AAA1111111111")
        self.assertEqual(registry.owner_for_pn("V00AAA1111111111"), "entryA")
        with self.assertRaises(ValueError):
            registry.claim("entryB", collector_pn="V00AAA1111111111")
        self.assertEqual(registry.list_unclaimed_sessions(), ())

    def test_claim_binds_by_session_id(self) -> None:
        sessions = [_session("listener-8899-7", "V00AAA1111111111")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        matched = registry.claim("entryA", session_id="listener-8899-7")
        self.assertIsNotNone(matched)
        self.assertEqual(matched.owner_entry_id, "entryA")

    def test_release_frees_session(self) -> None:
        sessions = [_session("s1", "V00AAA1111111111")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entryA", collector_pn="V00AAA1111111111")
        self.assertTrue(registry.release("entryA"))
        self.assertEqual(len(registry.list_unclaimed_sessions()), 1)

    def test_peer_ip_is_not_ownership_identity(self) -> None:
        # Ownership is by PN; the peer IP the collector dials from can change
        # (NAT rebind) without changing ownership, and cannot itself be claimed.
        pn = "V00AAA1111111111"
        state = {"peer_ip": "203.0.113.10"}
        registry = CallbackSessionRegistry(
            sessions_source=lambda: [_session("s1", pn, peer_ip=state["peer_ip"])]
        )
        registry.claim("entryA", collector_pn=pn)
        self.assertEqual(registry.owner_for_pn(pn), "entryA")
        state["peer_ip"] = "198.51.100.20"
        owned = [s for s in registry.observed_sessions() if s.owner_entry_id]
        self.assertEqual(len(owned), 1)
        self.assertEqual(owned[0].owner_entry_id, "entryA")

    def test_reconcile_identity_promotes_short_to_full(self) -> None:
        registry = CallbackSessionRegistry(sessions_source=lambda: [])
        registry.claim("entryA", collector_pn="V00CCC3333", session_id="s9")
        self.assertTrue(
            registry.reconcile_identity(session_id="s9", full_pn="V00CCC3333444455")
        )
        self.assertEqual(registry.claimed_identity("entryA"), "V00CCC3333444455")

    def test_list_unclaimed_excludes_owned(self) -> None:
        sessions = [
            _session("s1", "V00AAA1111111111"),
            _session("s2", "V00BBB2222222222", peer_ip="198.51.100.5"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entryA", collector_pn="V00AAA1111111111")
        unclaimed = registry.list_unclaimed_sessions()
        self.assertEqual([s.collector_pn for s in unclaimed], ["V00BBB2222222222"])

    def test_pn_reconciliation_rejects_short_ambiguous_prefixes(self) -> None:
        # Below the minimum prefix length a shared prefix is not the same identity.
        self.assertFalse(pn_is_same_identity("V00", "V00XYZ"))
        self.assertTrue(pn_is_same_identity("V00CCC3333", "V00CCC3333444455"))
        self.assertFalse(pn_is_same_identity("V00AAA1111111111", "V00BBB2222222222"))


class MigrationInvariantTests(unittest.TestCase):
    def _run_migrate(self, entry) -> bool:
        from custom_components.eybond_local import async_migrate_entry

        updates: dict = {}

        def _async_update_entry(target, **kwargs):
            updates.update(kwargs)
            if "version" in kwargs:
                target.version = kwargs["version"]
            if "data" in kwargs:
                target.data = dict(kwargs["data"])

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
        )
        result = asyncio.run(async_migrate_entry(hass, entry))
        entry._last_updates = updates
        return result

    def test_v1_entry_migrates_to_v2_and_persists_axes(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e1",
            version=1,
            data={C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA},
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(entry.version, 2)
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertEqual(
            entry.data[C.CONF_ENDPOINT_CONTROL_POLICY], C.ENDPOINT_CONTROL_EXTERNAL
        )

    def test_ha_only_bind_entry_migrates_to_inbound_integration_managed(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e2",
            version=1,
            data={C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY},
            options={
                C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: "config_flow_pre_bind",
            },
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        self.assertEqual(
            entry.data[C.CONF_ENDPOINT_CONTROL_POLICY],
            C.ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

    def test_v2_entry_is_not_downgraded(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e3", version=2, data={}, options={}
        )
        self.assertTrue(self._run_migrate(entry))
        # Nothing to migrate; version stays 2, no data rewrite forced.
        self.assertEqual(entry.version, 2)

    def test_future_version_is_refused(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e4", version=3, data={}, options={}
        )
        self.assertFalse(self._run_migrate(entry))


if __name__ == "__main__":
    unittest.main()
