"""Architectural invariant tests for the collector connection axes.

These lock in the contract from the local passive callback architecture plan:
transport ownership (``connection_strategy``), endpoint control
(``endpoint_control_policy``), and proxy state (``proxy_enabled``) are explicit,
opaque, hostname-free entry axes, and the callback session registry owns durable
PN identity and short/full PN reconciliation. Peer IP is never durable identity.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import types
import unittest

from custom_components.eybond_local import _ENTRY_SCHEMA_VERSION
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

    def test_axis_diagnostics_expose_strategy_evidence(self) -> None:
        # The behavioral-verification provenance is part of support diagnostics.
        diagnostics = cp.entry_axis_diagnostics(
            {
                C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
                C.CONF_CONNECTION_STRATEGY_EVIDENCE: (
                    C.CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT
                ),
            },
            {},
        )
        self.assertEqual(
            diagnostics["connection_strategy_evidence"],
            C.CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT,
        )

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
    def test_owned_session_location_excludes_parked_peer_closed_socket(self) -> None:
        sessions = [
            _session(
                "s-closed",
                "V00AAA1111111111",
                state="parked_peer_closed",
            ),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim("entry-1", collector_pn="V00AAA1111111111")

        self.assertIsNone(registry.owned_session_location("entry-1"))

    def test_current_session_for_pn_rebinds_to_latest_replacement_socket(self) -> None:
        sessions = [
            _session("s-old", "V00AAA1111111111", state="routed_framed"),
            _session("s-new", "V00AAA1111111111", state="routed_framed"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))

        current = registry.current_session_for_pn("V00AAA1111111111")

        self.assertIsNotNone(current)
        self.assertEqual(current.session_id, "s-new")

    def test_weak_current_session_lookup_does_not_follow_longer_prefix(self) -> None:
        sessions = [
            _session("s-full", "V00AAA1111111111", state="routed_framed"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))

        self.assertIsNone(
            registry.current_session_for_pn("V00AAA1111", require_exact=True)
        )

    def test_transient_session_claim_owns_only_that_session(self) -> None:
        # Two WEAK sessions sharing a PN prefix: a transient claim of one must
        # not start owning the other (no weak PN is copied into durable
        # ownership, no prefix matching).
        sessions = [
            _session("s-weak-1", "V00AAA1111111", source="heartbeat", state="identified"),
            _session("s-weak-2", "V00AAA1111111", source="heartbeat", state="identified"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))

        registry.claim_session("verify-1", session_id="s-weak-1")

        per_socket = {
            session.session_id: session
            for session in registry.observed_sessions_per_socket()
        }
        self.assertEqual(per_socket["s-weak-1"].owner_entry_id, "verify-1")
        # The prefix twin stays unowned and claimable by someone else.
        self.assertEqual(per_socket["s-weak-2"].owner_entry_id, "")
        registry.claim_session("verify-2", session_id="s-weak-2")
        # The same session cannot be transiently claimed twice.
        with self.assertRaises(ValueError):
            registry.claim_session("verify-3", session_id="s-weak-1")

    def test_transient_claim_promotion_enforces_single_owner(self) -> None:
        sessions = [
            _session("s-1", "V00AAA1111111111"),
            _session("s-2", "V00AAA1111111111"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim_session("verify-1", session_id="s-1")
        registry.claim_session("verify-2", session_id="s-2")

        self.assertTrue(
            registry.promote_claim_to_full_pn("verify-1", "V00AAA1111111111")
        )
        self.assertEqual(registry.claimed_identity("verify-1"), "V00AAA1111111111")
        self.assertEqual(registry.claimed_session_id("verify-1"), "s-1")
        # Promotion of the second owner to the SAME durable identity conflicts.
        with self.assertRaises(ValueError):
            registry.promote_claim_to_full_pn("verify-2", "V00AAA1111111111")
        # A session whose identity is durably owned cannot be transiently claimed.
        with self.assertRaises(ValueError):
            registry.claim_session("verify-4", session_id="s-2")

    def test_verification_claim_retargets_only_after_old_session_closes(self) -> None:
        sessions = [
            _session("s-old", "V00AAA1111111111", state="routed_framed"),
            _session(
                "s-new",
                "V00AAA1111",
                source="heartbeat",
                state="parked_no_payload_owner",
            ),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim_session("verify-1", session_id="s-old")
        registry.promote_claim_to_full_pn("verify-1", "V00AAA1111111111")

        with self.assertRaises(ValueError):
            registry.retarget_claim_to_reconnected_session("verify-1", "s-new")

        sessions[0]["state"] = "closed_disconnected"
        self.assertTrue(
            registry.retarget_claim_to_reconnected_session("verify-1", "s-new")
        )
        self.assertEqual(registry.claimed_session_id("verify-1"), "s-new")

    def test_verification_claim_never_retargets_to_different_identity(self) -> None:
        sessions = [
            _session("s-old", "V00AAA1111111111", state="closed_disconnected"),
            _session(
                "s-other",
                "V00BBB2222",
                source="heartbeat",
                state="parked_no_payload_owner",
            ),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim_session("verify-1", session_id="s-old")
        registry.promote_claim_to_full_pn("verify-1", "V00AAA1111111111")

        self.assertFalse(
            registry.retarget_claim_to_reconnected_session("verify-1", "s-other")
        )
        self.assertEqual(registry.claimed_session_id("verify-1"), "s-old")

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
            if "options" in kwargs:
                target.options = dict(kwargs["options"])

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
        )
        result = asyncio.run(async_migrate_entry(hass, entry))
        entry._last_updates = updates
        return result

    def test_v1_entry_migrates_to_current_schema_and_persists_axes(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e1",
            version=1,
            data={C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA},
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(entry.version, _ENTRY_SCHEMA_VERSION)
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

    def test_v2_entry_migrates_up_to_current_schema(self) -> None:
        # A v2 entry is carried forward through every later step.
        entry = types.SimpleNamespace(
            entry_id="e3", version=2, data={}, options={}
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(entry.version, _ENTRY_SCHEMA_VERSION)

    def test_current_schema_entry_is_not_re_migrated(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e3b", version=_ENTRY_SCHEMA_VERSION, data={}, options={}
        )
        self.assertTrue(self._run_migrate(entry))

    def test_legacy_callback_evidence_survives_migration_untouched(self) -> None:
        # Behavioral test D: entries created before recovery evidence was
        # separated from identity may carry
        # connection_strategy_evidence=callback_trigger. NO migration removes or
        # rewrites it -- dropping the false evidence from NEW writes must never
        # retroactively mutate existing entries, whose strategy resolution still
        # legitimately reads that legacy value.
        legacy_pn = "V001020SYN62344022"
        entry = types.SimpleNamespace(
            entry_id="e-legacy-evidence",
            version=1,
            data={
                C.CONF_CONNECTION_MODE: "known_ip",
                C.CONF_COLLECTOR_PN: legacy_pn,
                C.CONF_CONNECTION_STRATEGY_EVIDENCE: (
                    C.CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER
                ),
            },
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(entry.version, _ENTRY_SCHEMA_VERSION)
        # The legacy evidence is byte-for-byte intact...
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY_EVIDENCE],
            C.CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
        )
        # ...and keeps doing its legacy job: the frozen canonical strategy came
        # out of it, so the entry behaves exactly as it did before the upgrade.
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertEqual(entry.version, _ENTRY_SCHEMA_VERSION)

    def test_future_version_is_refused(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e4", version=_ENTRY_SCHEMA_VERSION + 1, data={}, options={}
        )
        self.assertFalse(self._run_migrate(entry))


class CanonicalConnectionStrategyOwnerTests(unittest.TestCase):
    """entry.data is the single canonical owner of connection_strategy (v4).

    Before v4 there were two writers: the options form wrote the strategy into
    entry.options while the explicit endpoint actions wrote it into entry.data,
    and the resolver read options FIRST -- so a stale options copy silently
    shadowed a successful HA-only / Cloud+HA action.
    """

    def test_data_is_authoritative_over_stale_options(self) -> None:
        # The exact old bug: a Cloud+HA action wrote callback_on_demand into data
        # while a stale options copy still said inbound. data must win.
        data = {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND}
        options = {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND}
        self.assertEqual(
            cp.resolve_connection_strategy(data, options),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_options_are_only_a_pre_migration_fallback(self) -> None:
        # data has no value yet (pre-migration entry) -> options is honored.
        self.assertEqual(
            cp.resolve_connection_strategy(
                {}, {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND}
            ),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_legacy_resolver_reproduces_old_options_first_semantics(self) -> None:
        # Migration input: the value the entry ACTUALLY behaved with pre-upgrade.
        data = {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND}
        options = {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND}
        self.assertEqual(
            cp.legacy_effective_connection_strategy(data, options),
            C.CONNECTION_STRATEGY_INBOUND,
        )

    def test_strategy_provenance_names_canonical_vs_legacy_source(self) -> None:
        canonical = cp.migration_diagnostics(
            {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND}, {}
        )
        self.assertEqual(canonical["connection_strategy_source"], "explicit_data")

        legacy = cp.migration_diagnostics(
            {}, {C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND}
        )
        self.assertEqual(
            legacy["connection_strategy_source"], "legacy_options_pre_migration"
        )
        # A pre-migration entry is not reported as fully explicit.
        self.assertNotEqual(legacy["migration_axes_source"], "explicit")

    def test_runtime_diagnostics_never_override_canonical_strategy(self) -> None:
        # Legacy migration once corrected this cloud-primary shape, but after v4
        # an explicit data value is authoritative even when compatibility fields
        # still describe the old mode.
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
            C.CONF_COLLECTOR_PN: "PNCANONICAL0001",
        }

        diagnostics = cp.entry_axis_diagnostics(data, {})

        self.assertEqual(
            diagnostics[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_INBOUND,
        )
        self.assertEqual(diagnostics["migration_status"], "ok")


class CanonicalStrategyMigrationTests(unittest.TestCase):
    """v3 -> v4 freezes the real pre-upgrade behavior into data."""

    def _run_migrate(self, entry) -> bool:
        from custom_components.eybond_local import async_migrate_entry

        def _async_update_entry(target, **kwargs):
            if "version" in kwargs:
                target.version = kwargs["version"]
            if "data" in kwargs:
                target.data = dict(kwargs["data"])
            if "options" in kwargs:
                target.options = dict(kwargs["options"])

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
        )
        return asyncio.run(async_migrate_entry(hass, entry))

    def test_conflicting_data_options_keeps_old_effective_behavior(self) -> None:
        # Pre-v4 this entry BEHAVED as inbound (options shadowed data). The
        # migration must preserve that, not "heal" it to the data value.
        entry = types.SimpleNamespace(
            entry_id="conflict",
            version=3,
            data={
                C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                C.CONF_COLLECTOR_PN: "A1234567890123",
            },
            options={C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND},
        )
        self.assertTrue(self._run_migrate(entry))

        self.assertEqual(entry.version, _ENTRY_SCHEMA_VERSION)
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        # The options copy is gone: it can never shadow data again.
        self.assertNotIn(C.CONF_CONNECTION_STRATEGY, entry.options)
        # And the post-migration resolver agrees with the pre-migration behavior.
        self.assertEqual(
            cp.resolve_connection_strategy(entry.data, entry.options),
            C.CONNECTION_STRATEGY_INBOUND,
        )

    def test_options_only_strategy_moves_into_data(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="opts-only",
            version=3,
            data={C.CONF_COLLECTOR_PN: "A1234567890123"},
            options={C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND},
        )
        self.assertTrue(self._run_migrate(entry))

        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        self.assertNotIn(C.CONF_CONNECTION_STRATEGY, entry.options)

    def test_migration_does_not_reinvent_strategy_from_address_or_cloud(self) -> None:
        # A cloud-looking endpoint / peer address must not influence the frozen
        # value: the entry behaved as inbound, so inbound is what is preserved.
        entry = types.SimpleNamespace(
            entry_id="opaque",
            version=3,
            data={
                C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: "some.vendor.example,18899,TCP",
                C.CONF_COLLECTOR_IP: "192.0.2.55",
                C.CONF_COLLECTOR_PN: "A1234567890123",
            },
            options={C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )

    def test_migration_is_idempotent_on_already_canonical_entry(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="canonical",
            version=_ENTRY_SCHEMA_VERSION,
            data={C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND},
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        self.assertNotIn(C.CONF_CONNECTION_STRATEGY, entry.options)


class MigrationMatrixTests(unittest.TestCase):
    """Deterministic migration matrix for legacy entry families A-G."""

    def _run_migrate(self, entry) -> bool:
        from custom_components.eybond_local import async_migrate_entry

        def _async_update_entry(target, **kwargs):
            if "version" in kwargs:
                target.version = kwargs["version"]
            if "data" in kwargs:
                target.data = dict(kwargs["data"])

        hass = types.SimpleNamespace(
            config_entries=types.SimpleNamespace(async_update_entry=_async_update_entry)
        )
        return asyncio.run(async_migrate_entry(hass, entry))

    # A. Factory EyeBond, cloud endpoint -> callback_on_demand + external.
    def test_A_factory_cloud_ha_maps_to_callback_on_demand_external(self) -> None:
        result = cp.simulate_migration(
            {
                C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
                # A stale callback_listener onboarding artifact must NOT force inbound.
                C.CONF_CONNECTION_MODE: "callback_listener",
            },
            {},
        )
        self.assertEqual(
            result[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        )
        self.assertEqual(
            result[C.CONF_ENDPOINT_CONTROL_POLICY], C.ENDPOINT_CONTROL_EXTERNAL
        )
        self.assertTrue(result["may_send_callback_trigger"])
        self.assertFalse(result["may_auto_manage_endpoint"])

    # B. Factory HA-only with a real integration write -> inbound + integration_managed.
    def test_B_ha_only_written_endpoint_maps_to_inbound_integration_managed(self) -> None:
        result = cp.simulate_migration(
            {
                C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
                C.CONF_ENDPOINT_WRITTEN_VALUE: "192.168.1.50,18899,TCP",
            },
            {},
        )
        self.assertEqual(
            result[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        self.assertEqual(
            result[C.CONF_ENDPOINT_CONTROL_POLICY],
            C.ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )
        self.assertFalse(result["may_send_callback_trigger"])

    # B'. Factory HA-only without write provenance -> inbound + external.
    def test_B_ha_only_without_write_provenance_stays_external(self) -> None:
        result = cp.simulate_migration(
            {C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY}, {}
        )
        self.assertEqual(
            result[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        self.assertEqual(
            result[C.CONF_ENDPOINT_CONTROL_POLICY], C.ENDPOINT_CONTROL_EXTERNAL
        )

    # C. ESP/BK community collector with HA endpoint -> inbound + external, no trigger.
    def test_C_esp_collector_ha_endpoint_maps_to_inbound_external_no_trigger(self) -> None:
        result = cp.simulate_migration(
            {
                # ESP/ha_only_required collectors persist operation_mode=HA_ONLY.
                C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
                "collector_virtual_bridge": True,
                "collector_bridge_kind": "esp-collector",
            },
            {},
        )
        self.assertEqual(
            result[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )
        self.assertEqual(
            result[C.CONF_ENDPOINT_CONTROL_POLICY], C.ENDPOINT_CONTROL_EXTERNAL
        )
        self.assertFalse(result["may_send_callback_trigger"])

    # E. ValueCloud/G-ASCII: cloud/provider metadata is preserved, not turned into
    # an axis or a forced payload route.
    def test_E_valuecloud_metadata_preserved_and_not_forced_route(self) -> None:
        data = {
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
            C.CONF_COLLECTOR_CLOUD_FAMILY: "valuecloud",
            "collector_session_protocol": "at_text",
        }
        axes = cp.migrate_entry_axes(data, {})
        # Migration only writes the three axes; provider/route fields are untouched.
        self.assertNotIn(C.CONF_COLLECTOR_CLOUD_FAMILY, axes)
        self.assertNotIn("collector_session_protocol", axes)
        # cloud_family must not decide the strategy (opaque, provider-free).
        without_family = dict(data)
        without_family.pop(C.CONF_COLLECTOR_CLOUD_FAMILY)
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}),
            cp.resolve_connection_strategy(without_family, {}),
        )

    # F. Legacy operation_mode-only entry derives axes ONCE, then axes are authoritative.
    def test_F_operation_mode_only_derives_once_then_axes_authoritative(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="legacy-1",
            version=1,
            data={C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA},
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        # After migration the explicit axis is authoritative: flipping the legacy
        # operation mode does NOT change the resolved strategy.
        entry.data[C.CONF_COLLECTOR_OPERATION_MODE] = C.COLLECTOR_OPERATION_HA_ONLY
        self.assertEqual(
            cp.resolve_connection_strategy(entry.data, entry.options),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    # Observed endpoint provenance does NOT imply integration_managed.
    def test_observed_endpoint_source_does_not_set_integration_managed(self) -> None:
        for observed_source in ("runtime_observed", "collector_registry", "smartess_cloud_diagnostics"):
            result = cp.simulate_migration(
                {
                    C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: "some.cloud.example,18899,TCP",
                    C.CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: observed_source,
                },
                {},
            )
            self.assertEqual(
                result[C.CONF_ENDPOINT_CONTROL_POLICY],
                C.ENDPOINT_CONTROL_EXTERNAL,
                observed_source,
            )

    # G. Migration must not touch persisted inverter identity fields.
    def test_G_migration_preserves_detected_identity_fields(self) -> None:
        data = {
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
            C.CONF_DETECTED_MODEL: "SMG 6200",
            C.CONF_DETECTED_SERIAL: "92632500000001",
        }
        axes = cp.migrate_entry_axes(data, {})
        self.assertNotIn(C.CONF_DETECTED_MODEL, axes)
        self.assertNotIn(C.CONF_DETECTED_SERIAL, axes)

    # E500/SMG: an already-migrated inbound cloud-primary entry is CORRECTED to
    # callback_on_demand -- it does not stay inbound merely because of stale fields.
    def test_e500_smg_migrated_inbound_is_corrected_to_callback_on_demand(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="e500",
            version=2,
            data={
                C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
                C.CONF_ENDPOINT_CONTROL_POLICY: C.ENDPOINT_CONTROL_EXTERNAL,
                C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
                C.CONF_COLLECTOR_PN: "V00ABC1234567890",
            },
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(entry.version, _ENTRY_SCHEMA_VERSION)
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )
        # No endpoint was written during migration.
        self.assertNotIn(C.CONF_ENDPOINT_WRITTEN_VALUE, entry.data)
        # Diagnostics flag the correction.
        diag = cp.migration_diagnostics(entry.data, entry.options)
        self.assertIn(diag["migration_status"], ("ok", "corrected"))

    # A genuinely integration-managed inbound cloud-primary entry is NOT corrected.
    def test_integration_managed_inbound_is_not_corrected(self) -> None:
        entry = types.SimpleNamespace(
            entry_id="bound",
            version=2,
            data={
                C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
                C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
                C.CONF_ENDPOINT_WRITTEN_VALUE: "192.168.1.50,18899,TCP",
            },
            options={},
        )
        self.assertTrue(self._run_migrate(entry))
        self.assertEqual(
            entry.data[C.CONF_CONNECTION_STRATEGY], C.CONNECTION_STRATEGY_INBOUND
        )

    def test_verified_inbound_evidence_blocks_migration_correction(self) -> None:
        # A behaviorally-verified inbound entry (restart -> genuine reconnect,
        # no UDP) keeps inbound even with the legacy cloud-primary operation
        # mode still present: the reboot_reconnect provenance exempts it from
        # the unreachable-inbound corrective re-migration.
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
            C.CONF_CONNECTION_STRATEGY_EVIDENCE: "reboot_reconnect",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
        }
        self.assertIsNone(cp.correct_migrated_connection_strategy(data, {}))
        # Without the evidence the same shape is still corrected.
        data.pop(C.CONF_CONNECTION_STRATEGY_EVIDENCE)
        self.assertEqual(
            cp.correct_migrated_connection_strategy(data, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_migration_diagnostics_expose_sources_and_status(self) -> None:
        diag = cp.migration_diagnostics(
            {C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA}, {}
        )
        for key in (
            "migration_status",
            "migration_warning",
            "migration_axes_source",
            "connection_strategy_source",
            "endpoint_control_policy_source",
        ):
            self.assertIn(key, diag)
        self.assertEqual(diag["connection_strategy_source"], "derived_operation_mode_cloud")
        self.assertEqual(diag["migration_axes_source"], "derived")

    def test_entry_axis_diagnostics_reports_canonical_strategy(self) -> None:
        diag = cp.entry_axis_diagnostics(
            {
                C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
                C.CONF_ENDPOINT_CONTROL_POLICY: C.ENDPOINT_CONTROL_EXTERNAL,
                C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
                C.CONF_COLLECTOR_PN: "PNCANONICAL0002",
            },
            {},
        )

        self.assertEqual(
            diag["connection_strategy"],
            C.CONNECTION_STRATEGY_INBOUND,
        )
        self.assertFalse(diag["may_send_callback_trigger"])
        self.assertEqual(diag["migration_status"], "ok")


class OfflinePnLessCallbackMigrationTests(unittest.TestCase):
    """The manual/known-IP offline PN-less callback entry migration + repair state."""

    def test_legacy_known_ip_ha_only_maps_to_callback_on_demand(self) -> None:
        # A user-triggered known-IP callback collector is NOT inbound even when a
        # legacy HA-only operation mode was recorded: it needs a UDP trigger.
        data = {
            C.CONF_CONNECTION_MODE: "known_ip",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_legacy_manual_ha_only_maps_to_callback_on_demand(self) -> None:
        data = {
            C.CONF_CONNECTION_MODE: "manual",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_ha_only_alone_still_inbound(self) -> None:
        # Regression: a plain HA-only entry (no user-triggered connection_mode)
        # stays inbound -- the endpoint points at HA and it dials in.
        data = {C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY}
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}), C.CONNECTION_STRATEGY_INBOUND
        )

    def test_explicit_inbound_is_not_changed(self) -> None:
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
            C.CONF_CONNECTION_MODE: "known_ip",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}), C.CONNECTION_STRATEGY_INBOUND
        )

    def test_verified_reboot_reconnect_evidence_stays_inbound(self) -> None:
        # A behaviorally verified inbound entry is authoritative over legacy fields.
        data = {
            C.CONF_CONNECTION_STRATEGY_EVIDENCE: C.CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT,
            C.CONF_CONNECTION_MODE: "known_ip",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}), C.CONNECTION_STRATEGY_INBOUND
        )

    def test_verified_callback_trigger_evidence_is_callback_on_demand(self) -> None:
        data = {
            C.CONF_CONNECTION_STRATEGY_EVIDENCE: C.CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertEqual(
            cp.resolve_connection_strategy(data, {}),
            C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        )

    def test_explicit_inbound_known_ip_is_not_corrected_by_migration(self) -> None:
        # Item 6: a genuinely-explicit inbound value on a manual/known-IP entry is
        # NOT flipped to callback_on_demand by the corrective re-migration just
        # because the legacy connection_mode looks user-triggered. The correction
        # is reserved for the cloud-primary SmartESS+HA mis-migration shape.
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
            C.CONF_CONNECTION_MODE: "known_ip",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
        }
        self.assertIsNone(cp.correct_migrated_connection_strategy(data, {}))
        # The explicit strategy is preserved end-to-end.
        self.assertEqual(
            cp.simulate_migration(data, {})[C.CONF_CONNECTION_STRATEGY],
            C.CONNECTION_STRATEGY_INBOUND,
        )

    def test_pn_less_manual_entry_reports_identity_binding_required(self) -> None:
        # The exact offline support-package shape: manual + HA_ONLY + no PN.
        data = {
            C.CONF_CONNECTION_MODE: "manual",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
            C.CONF_COLLECTOR_PN: "",
        }
        self.assertTrue(cp.collector_identity_binding_required(data, {}))
        diag = cp.migration_diagnostics(data, {})
        self.assertEqual(diag["migration_status"], "identity_binding_required")
        axes = cp.entry_axis_diagnostics(data, {})
        self.assertFalse(axes["collector_pn_bound"])
        self.assertTrue(axes["collector_identity_binding_required"])

    def test_callback_entry_with_pn_is_not_binding_required(self) -> None:
        data = {
            C.CONF_CONNECTION_MODE: "known_ip",
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
            C.CONF_COLLECTOR_PN: "V001020SYN62344022",
        }
        self.assertFalse(cp.collector_identity_binding_required(data, {}))
        self.assertEqual(cp.migration_diagnostics(data, {})["migration_status"], "ok")

    def test_pn_less_inbound_entry_also_requires_identity_binding(self) -> None:
        # Item 7: the unified invariant covers inbound too. An inbound collector
        # entry claims its dial-in session by durable PN through the callback
        # registry (peer IP is never ownership), so a PN-less inbound entry can
        # never own a session and must fail closed as identity_binding_required.
        data = {
            C.CONF_CONNECTION_STRATEGY: C.CONNECTION_STRATEGY_INBOUND,
            C.CONF_COLLECTOR_OPERATION_MODE: C.COLLECTOR_OPERATION_HA_ONLY,
            C.CONF_COLLECTOR_PN: "",
        }
        self.assertTrue(cp.collector_identity_binding_required(data, {}))
        # The integration listener/bootstrap entry is the sole exception.
        listener = {C.CONF_ENTRY_ROLE: C.ENTRY_ROLE_LISTENER}
        self.assertFalse(cp.collector_identity_binding_required(listener, {}))


class OperationModeIsNotStrategyAuthorityGuardTests(unittest.TestCase):
    def test_operation_mode_does_not_override_user_triggered_connection_mode(self) -> None:
        # Guard: whatever the legacy operation mode, a manual/known-IP entry is
        # callback_on_demand. Operation mode is not the strategy authority.
        for op_mode in (
            C.COLLECTOR_OPERATION_HA_ONLY,
            C.COLLECTOR_OPERATION_SMARTESS_AND_HA,
            "",
        ):
            for conn_mode in ("manual", "known_ip"):
                data = {
                    C.CONF_CONNECTION_MODE: conn_mode,
                    C.CONF_COLLECTOR_OPERATION_MODE: op_mode,
                }
                self.assertEqual(
                    cp.resolve_connection_strategy(data, {}),
                    C.CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                    msg=f"{conn_mode}+{op_mode!r}",
                )

    def test_connection_policy_never_reads_hostname_endpoint_or_peer_ip(self) -> None:
        # Guard: the module contains NO code that parses peer IP / hostnames for
        # the strategy decision. (The endpoint-provenance axis is allowed; parsing
        # an address or a hostname is not.) Checks code tokens, not prose.
        import ast

        tree = ast.parse(Path(cp.__file__).read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    names.add((alias.asname or alias.name).split(".")[0])
        for token in (
            "peer_ip",
            "gethostbyname",
            "ip_address",
            "socket",
            "CONF_COLLECTOR_IP",
        ):
            self.assertNotIn(token, names, msg=token)


if __name__ == "__main__":
    unittest.main()
