"""Batch 1 CP1a -- the neutral strategy-transition endpoint model + resolver.

Pure, no Home Assistant, no config_flow: strict typed construction, the closed
provenance vocabularies, and the default-endpoint resolution priority (explicit >
validated callback proof > caller-role-proven HA endpoint > effective runtime
route > none). The resolver derives NOTHING from peer IP / L2 / hostname / cloud
family -- it has no such input.
"""

from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.strategy_transition_context import (
    CLOUD_PROVENANCE_EXPLICIT_USER,
    CLOUD_PROVENANCE_NONE,
    CLOUD_PROVENANCE_OBSERVED_CURRENT,
    CLOUD_PROVENANCE_ORIGINAL,
    CLOUD_PROVENANCE_REGISTRY,
    CloudRollbackEndpoint,
    PROVENANCE_CALLBACK_PROOF,
    PROVENANCE_CONFIRMED_HA_ENDPOINT,
    PROVENANCE_EFFECTIVE_RUNTIME_ROUTE,
    PROVENANCE_EXPLICIT_ADVERTISED,
    PROVENANCE_NONE,
    PROVENANCE_OBSERVED_CURRENT_ENDPOINT,
    StrategyTransitionContext,
    TransitionEndpointCandidate,
    earned_advertised_route,
    resolve_cloud_rollback_endpoint,
    resolve_confirmed_ha_endpoint,
    resolve_default_ha_endpoint,
)

CB_ON = "callback_on_demand"
INBOUND = "inbound"

# Structure-preserving synthetic PNs (E500 family + V00 family).
_PN_FULL = "E5000025SYN0000000001"
_PN_SHORT = "E5000025SY"
_PN_FOREIGN = "V0011SYNFOREIGN000009"
_TS = "2026-07-21T10:00:00+00:00"


def _rollback(**over):
    base = dict(
        explicit_user_endpoint="",
        durable_original_endpoint="",
        registry_endpoint="",
        registry_pn="",
        entry_pn="",
        observed_current_endpoint="",
        confirmed_ha_endpoint=None,
    )
    base.update(over)
    return resolve_cloud_rollback_endpoint(**base)


def _confirmed(host: str = "192.168.1.50", port: int = 8899):
    return TransitionEndpointCandidate(
        host=host,
        port=port,
        provenance=PROVENANCE_CONFIRMED_HA_ENDPOINT,
    )


def _recovery_contract(*, strategy: str, advertised: str = "195.191.72.37:18899"):
    from custom_components.eybond_local.connection.recovery_contract import (
        CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        CallbackRecoveryProof,
        InboundRecoveryProof,
        RecoveryContract,
    )

    contract = RecoveryContract.empty_for_pn(
        _PN_FULL, identity_source="fc2_parameter_2", updated_at=_TS
    )
    if strategy == CB_ON:
        return contract.with_callback_proof(
            CallbackRecoveryProof(
                method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
                collector_pn=_PN_FULL,
                identity_source="fc2_parameter_2",
                verified_at=_TS,
                trigger_target="203.0.113.10:58899",
                advertised_ha_endpoint=advertised,
                listener_port=8899,
            ),
            updated_at=_TS,
        )
    return contract.with_inbound_proof(
        InboundRecoveryProof(
            method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
            collector_pn=_PN_FULL,
            identity_source="fc2_parameter_2",
            verified_at=_TS,
            session_protocol="eybond_framed",
        ),
        updated_at=_TS,
    )


def _resolve(**over):
    base = dict(
        explicit_advertised_host="",
        explicit_advertised_port=0,
        callback_proof_endpoint="",
        confirmed_ha_endpoint=None,
        observed_current_endpoint="",
        current_strategy=CB_ON,
        server_ip="192.168.1.50",
        tcp_port=8899,
    )
    base.update(over)
    return resolve_default_ha_endpoint(**base)


class ResolverPriority(unittest.TestCase):
    def test_A_local_e500_defaults_to_effective_runtime_route(self) -> None:
        # No explicit, no proof, callback_on_demand -> the editable hint is the
        # effective route 192.168.1.50:8899 (NOT a synthetic port 1).
        c = _resolve()
        self.assertEqual((c.host, c.port), ("192.168.1.50", 8899))
        self.assertEqual(c.provenance, PROVENANCE_EFFECTIVE_RUNTIME_ROUTE)
        self.assertTrue(c.has_candidate)

    def test_B_nat_callback_proof_beats_local_bind(self) -> None:
        c = _resolve(
            callback_proof_endpoint="195.191.72.37:18899",
            server_ip="10.0.0.2",
            tcp_port=502,
        )
        self.assertEqual((c.host, c.port), ("195.191.72.37", 18899))
        self.assertEqual(c.provenance, PROVENANCE_CALLBACK_PROOF)

    def test_C_full_precedence_order(self) -> None:
        confirmed = TransitionEndpointCandidate(
            host="198.51.100.7", port=6000, provenance=PROVENANCE_CONFIRMED_HA_ENDPOINT
        )
        # explicit > proof > confirmed > observed current > effective
        self.assertEqual(
            _resolve(
                explicit_advertised_host="203.0.113.9",
                explicit_advertised_port=7000,
                callback_proof_endpoint="195.191.72.37:18899",
                confirmed_ha_endpoint=confirmed,
            ).provenance,
            PROVENANCE_EXPLICIT_ADVERTISED,
        )
        self.assertEqual(
            _resolve(
                callback_proof_endpoint="195.191.72.37:18899",
                confirmed_ha_endpoint=confirmed,
            ).provenance,
            PROVENANCE_CALLBACK_PROOF,
        )
        self.assertEqual(
            _resolve(confirmed_ha_endpoint=confirmed).provenance,
            PROVENANCE_CONFIRMED_HA_ENDPOINT,
        )
        self.assertEqual(
            _resolve(
                observed_current_endpoint="198.51.100.8,18899,TCP"
            ).provenance,
            PROVENANCE_OBSERVED_CURRENT_ENDPOINT,
        )
        self.assertEqual(_resolve().provenance, PROVENANCE_EFFECTIVE_RUNTIME_ROUTE)

    def test_observed_current_endpoint_prefills_inbound_without_becoming_proof(self) -> None:
        candidate = _resolve(
            current_strategy=INBOUND,
            observed_current_endpoint="195.191.72.37,18899,TCP",
        )
        self.assertEqual(
            (candidate.host, candidate.port, candidate.provenance),
            (
                "195.191.72.37",
                18899,
                PROVENANCE_OBSERVED_CURRENT_ENDPOINT,
            ),
        )
        self.assertNotEqual(
            candidate.provenance, PROVENANCE_CONFIRMED_HA_ENDPOINT
        )

    def test_malformed_observed_current_endpoint_fails_closed(self) -> None:
        for value in (
            object(),
            None,
            " host,18899,TCP ",
            "host",
            "host,18899,UDP",
            "0.0.0.0,18899,TCP",
            "host,0,TCP",
        ):
            candidate = _resolve(observed_current_endpoint=value)
            self.assertEqual(
                candidate.provenance, PROVENANCE_NONE, msg=repr(value)
            )

    def test_effective_route_only_when_currently_callback(self) -> None:
        # An inbound entry with nothing known -> none (honest, no synthetic port).
        c = _resolve(current_strategy=INBOUND)
        self.assertFalse(c.has_candidate)
        self.assertEqual(c.provenance, PROVENANCE_NONE)
        self.assertEqual((c.host, c.port), ("", 0))
        # ...but a proof still wins even for an inbound entry.
        self.assertEqual(
            _resolve(
                current_strategy=INBOUND,
                callback_proof_endpoint="195.191.72.37:18899",
            ).provenance,
            PROVENANCE_CALLBACK_PROOF,
        )

    def test_malformed_present_callback_proof_fails_closed(self) -> None:
        # A PRESENT but malformed proof returns none -- it must NOT fall through
        # to the runtime fallback (a bad address can't be silently replaced).
        for bad in (
            "no-colon",
            "host:0",
            "host:99999",
            ":18899",
            "host:port",
            "host :18899",
            "192.168.1.50: 18899",
            "0.0.0.0:18899",
            None,
            object(),
            "  192.168.1.50:8899  ",  # padded whole string
        ):
            c = _resolve(callback_proof_endpoint=bad)
            self.assertEqual(c.provenance, PROVENANCE_NONE, msg=f"accepted {bad!r}")

    def test_strict_empty_proof_allows_next_source(self) -> None:
        # ONLY the strict empty string means "no proof" -> the next source runs.
        self.assertEqual(
            _resolve(callback_proof_endpoint="").provenance,
            PROVENANCE_EFFECTIVE_RUNTIME_ROUTE,
        )

    def test_confirmed_ha_slot_rejects_wrong_provenance(self) -> None:
        # A candidate carrying the effective-route provenance is not a confirmed
        # HA endpoint -- it must not be accepted at the confirmed slot.
        eff = TransitionEndpointCandidate(
            host="198.51.100.7", port=6000, provenance=PROVENANCE_EFFECTIVE_RUNTIME_ROUTE
        )
        self.assertEqual(
            _resolve(current_strategy=INBOUND, confirmed_ha_endpoint=eff).provenance,
            PROVENANCE_NONE,
        )

    def test_partial_explicit_fails_closed(self) -> None:
        # A partial explicit route (host xor port) must NOT silently fall through
        # to a lower-priority source -- it fails closed so the form asks honestly.
        self.assertEqual(
            _resolve(
                explicit_advertised_host="203.0.113.9",
                explicit_advertised_port=0,
                callback_proof_endpoint="195.191.72.37:18899",
            ).provenance,
            PROVENANCE_NONE,
        )
        self.assertEqual(
            _resolve(
                explicit_advertised_host="",
                explicit_advertised_port=7000,
                callback_proof_endpoint="195.191.72.37:18899",
            ).provenance,
            PROVENANCE_NONE,
        )
        # A wildcard explicit host is not advertisable -> fail closed.
        self.assertEqual(
            _resolve(
                explicit_advertised_host="0.0.0.0", explicit_advertised_port=7000
            ).provenance,
            PROVENANCE_NONE,
        )

    def test_effective_route_skips_wildcard_bind(self) -> None:
        # 0.0.0.0 bind is not an advertisable hint -> none, not "0.0.0.0:8899".
        for wild in ("0.0.0.0", "::", "0:0:0:0:0:0:0:0", "*"):
            self.assertEqual(
                _resolve(server_ip=wild, tcp_port=8899).provenance, PROVENANCE_NONE
            )

    def test_malformed_explicit_variants_fail_closed(self) -> None:
        # Every PRESENT-but-invalid explicit form fails closed to none and never
        # reaches the callback proof or the runtime fallback.
        for host, port in (
            (object(), 0),
            (None, 0),
            ("   ", 0),
            ("", True),
            ("203.0.113.9", True),
            ("203.0.113.9", 70000),
            ("203.0.113.9", 0),  # partial (host only)
            ("", 7000),  # partial (port only)
            ("0.0.0.0", 8899),  # wildcard
            (" 203.0.113.9 ", 7000),  # padded
        ):
            c = _resolve(
                explicit_advertised_host=host,
                explicit_advertised_port=port,
                callback_proof_endpoint="195.191.72.37:18899",  # would win if reached
            )
            self.assertEqual(
                c.provenance, PROVENANCE_NONE, msg=f"reached fallback for {host!r}/{port!r}"
            )

    def test_wildcard_not_accepted_via_confirmed_slot(self) -> None:
        # A confirmed-HA candidate is unconstructible with a wildcard host, so the
        # slot can never carry one.
        for wild in ("0.0.0.0", "::", "0:0:0:0:0:0:0:0", "*"):
            with self.assertRaises(ValueError):
                TransitionEndpointCandidate(
                    host=wild, port=6000, provenance=PROVENANCE_CONFIRMED_HA_ENDPOINT
                )


class EndpointCandidateConstruction(unittest.TestCase):
    def test_none_is_empty(self) -> None:
        n = TransitionEndpointCandidate.none()
        self.assertEqual((n.host, n.port, n.provenance), ("", 0, PROVENANCE_NONE))
        self.assertFalse(n.has_candidate)

    def test_unknown_provenance_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TransitionEndpointCandidate(host="h", port=8899, provenance="nope")

    def test_port_range_and_type(self) -> None:
        for bad in (0, -1, 65536, 70000):
            with self.assertRaises(ValueError):
                TransitionEndpointCandidate(
                    host="h", port=bad, provenance=PROVENANCE_EXPLICIT_ADVERTISED
                )
        with self.assertRaises(TypeError):
            TransitionEndpointCandidate(
                host="h", port=True, provenance=PROVENANCE_EXPLICIT_ADVERTISED
            )

    def test_none_provenance_must_be_empty(self) -> None:
        with self.assertRaises(ValueError):
            TransitionEndpointCandidate(host="h", port=0, provenance=PROVENANCE_NONE)

    def test_non_normalized_host_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TransitionEndpointCandidate(
                host=" h ", port=8899, provenance=PROVENANCE_EXPLICIT_ADVERTISED
            )

    def test_wildcard_host_unconstructible_for_every_provenance(self) -> None:
        provenances = (
            PROVENANCE_EXPLICIT_ADVERTISED,
            PROVENANCE_CALLBACK_PROOF,
            PROVENANCE_CONFIRMED_HA_ENDPOINT,
            PROVENANCE_OBSERVED_CURRENT_ENDPOINT,
            PROVENANCE_EFFECTIVE_RUNTIME_ROUTE,
        )
        for wild in ("0.0.0.0", "::", "0:0:0:0:0:0:0:0", "*"):
            for prov in provenances:
                with self.assertRaises(ValueError):
                    TransitionEndpointCandidate(host=wild, port=8899, provenance=prov)

    def test_none_stays_exactly_empty(self) -> None:
        # none() is EXACTLY ("", 0, "none") -- a non-str host is unconstructible.
        with self.assertRaises(ValueError):
            TransitionEndpointCandidate(host=object(), port=0, provenance=PROVENANCE_NONE)


class CloudRollbackConstruction(unittest.TestCase):
    def test_known_and_none(self) -> None:
        k = CloudRollbackEndpoint(
            endpoint="dtu_ess.eybond.com,18899,TCP", provenance=CLOUD_PROVENANCE_ORIGINAL
        )
        self.assertTrue(k.known)
        n = CloudRollbackEndpoint.none()
        self.assertFalse(n.known)
        self.assertEqual(n.provenance, CLOUD_PROVENANCE_NONE)

    def test_rejections(self) -> None:
        with self.assertRaises(ValueError):
            CloudRollbackEndpoint(endpoint="x", provenance="cloud")  # unknown provenance
        with self.assertRaises(ValueError):
            CloudRollbackEndpoint(endpoint="", provenance=CLOUD_PROVENANCE_ORIGINAL)  # missing
        with self.assertRaises(ValueError):
            CloudRollbackEndpoint(endpoint="x", provenance=CLOUD_PROVENANCE_NONE)  # none w/ value

    def test_known_endpoint_is_syntactically_validated(self) -> None:
        # All existing compact shapes are supported; malformed values are not.
        for bad in ("host,port,TCP", "host,99999,TCP", ",18899,TCP"):
            with self.assertRaises(ValueError):
                CloudRollbackEndpoint(endpoint=bad, provenance=CLOUD_PROVENANCE_ORIGINAL)
        for endpoint in ("ess.eybond.com", "203.0.113.9,18899", "203.0.113.9,18899,TCP"):
            self.assertTrue(
                CloudRollbackEndpoint(
                    endpoint=endpoint, provenance=CLOUD_PROVENANCE_ORIGINAL
                ).known
            )
        with self.assertRaises(ValueError):
            CloudRollbackEndpoint(
                endpoint="host, 18899,tcp", provenance=CLOUD_PROVENANCE_ORIGINAL
            )

    def test_wildcard_rollback_target_rejected(self) -> None:
        # A wildcard bind is syntactically parseable but is never a safe rollback
        # target to write to a collector.
        for wild in ("0.0.0.0,18899,TCP", "0.0.0.0,18899"):
            with self.assertRaises(ValueError):
                CloudRollbackEndpoint(endpoint=wild, provenance=CLOUD_PROVENANCE_ORIGINAL)


class ConfirmedHaEndpointResolution(unittest.TestCase):
    def test_callback_uses_nat_visible_proof_not_local_runtime_address(self) -> None:
        candidate = resolve_confirmed_ha_endpoint(
            current_strategy=CB_ON,
            entry_pn=_PN_FULL,
            advertised_host="195.191.72.37",
            advertised_port=18899,
            recovery_contract=_recovery_contract(strategy=CB_ON),
        )
        self.assertEqual(candidate.provenance, PROVENANCE_CONFIRMED_HA_ENDPOINT)
        self.assertEqual((candidate.host, candidate.port), ("195.191.72.37", 18899))

    def test_callback_persisted_pair_must_match_proof(self) -> None:
        candidate = resolve_confirmed_ha_endpoint(
            current_strategy=CB_ON,
            entry_pn=_PN_FULL,
            advertised_host="192.168.1.50",
            advertised_port=8899,
            recovery_contract=_recovery_contract(strategy=CB_ON),
        )
        self.assertEqual(candidate.provenance, PROVENANCE_NONE)

    def test_inbound_requires_atomic_pair_and_inbound_proof(self) -> None:
        valid = resolve_confirmed_ha_endpoint(
            current_strategy=INBOUND,
            entry_pn=_PN_FULL,
            advertised_host="198.51.100.20",
            advertised_port=18899,
            recovery_contract=_recovery_contract(strategy=INBOUND),
        )
        self.assertEqual((valid.host, valid.port), ("198.51.100.20", 18899))
        missing_pair = resolve_confirmed_ha_endpoint(
            current_strategy=INBOUND,
            entry_pn=_PN_FULL,
            advertised_host="",
            advertised_port=0,
            recovery_contract=_recovery_contract(strategy=INBOUND),
        )
        self.assertEqual(missing_pair.provenance, PROVENANCE_NONE)

    def test_foreign_pn_and_duck_contract_fail_closed(self) -> None:
        for contract, pn in (
            (_recovery_contract(strategy=CB_ON), _PN_FOREIGN),
            (object(), _PN_FULL),
        ):
            candidate = resolve_confirmed_ha_endpoint(
                current_strategy=CB_ON,
                entry_pn=pn,
                advertised_host="195.191.72.37",
                advertised_port=18899,
                recovery_contract=contract,
            )
            self.assertEqual(candidate.provenance, PROVENANCE_NONE)


class ResolveCloudRollbackA_DurableOriginal(unittest.TestCase):
    def test_valid_whole_record_is_original(self) -> None:
        r = _rollback(durable_original_endpoint="ess.eybond.com,18899,TCP")
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_ORIGINAL)
        self.assertEqual(r.endpoint, "ess.eybond.com,18899,TCP")

    def test_valid_original_beats_registry_and_observed(self) -> None:
        r = _rollback(
            durable_original_endpoint="ess.eybond.com,18899,TCP",
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
            observed_current_endpoint="cloud.other,18899,TCP",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_ORIGINAL)

    def test_malformed_original_fails_closed_no_fall_through(self) -> None:
        # A PRESENT-but-malformed durable record must NOT silently drop to the
        # (valid) registry/observed source -- it fails closed to none. No options
        # field mixing: only the chosen record's endpoint reached the resolver.
        r = _rollback(
            durable_original_endpoint="not-an-endpoint###",
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
            observed_current_endpoint="cloud.other,18899,TCP",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_partial_record_missing_endpoint_fails_closed(self) -> None:
        # The boundary passes ``None`` for a present-but-partial record (metadata
        # present, endpoint key absent): present-but-invalid -> fail closed.
        r = _rollback(
            durable_original_endpoint=None,
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_wildcard_original_is_none(self) -> None:
        self.assertEqual(
            _rollback(durable_original_endpoint="0.0.0.0,18899,TCP").provenance,
            CLOUD_PROVENANCE_NONE,
        )

    def test_host_only_original_is_preserved(self) -> None:
        result = _rollback(durable_original_endpoint="ess.eybond.com")
        self.assertEqual(result.provenance, CLOUD_PROVENANCE_ORIGINAL)
        self.assertEqual(result.endpoint, "ess.eybond.com")


class ResolveCloudRollbackB_Registry(unittest.TestCase):
    def test_same_pn_registry_endpoint(self) -> None:
        r = _rollback(
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_REGISTRY)
        self.assertEqual(r.endpoint, "reg.example,18899,TCP")

    def test_short_and_full_same_identity_accepted(self) -> None:
        for entry_pn, reg_pn in ((_PN_SHORT, _PN_FULL), (_PN_FULL, _PN_SHORT)):
            r = _rollback(
                registry_endpoint="reg.example,18899,TCP",
                registry_pn=reg_pn,
                entry_pn=entry_pn,
            )
            self.assertEqual(r.provenance, CLOUD_PROVENANCE_REGISTRY, (entry_pn, reg_pn))

    def test_foreign_pn_rejected(self) -> None:
        r = _rollback(
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FOREIGN,
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_malformed_registry_endpoint_rejected(self) -> None:
        r = _rollback(
            registry_endpoint="bad###endpoint",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_registry_does_not_override_valid_durable(self) -> None:
        r = _rollback(
            durable_original_endpoint="ess.eybond.com,18899,TCP",
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_ORIGINAL)

    def test_host_only_registry_endpoint_is_preserved(self) -> None:
        result = _rollback(
            registry_endpoint="ess.eybond.com",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
        )
        self.assertEqual(result.provenance, CLOUD_PROVENANCE_REGISTRY)
        self.assertEqual(result.endpoint, "ess.eybond.com")


class ResolveCloudRollbackC_ObservedCurrent(unittest.TestCase):
    def test_confirmed_current_not_equivalent_is_candidate(self) -> None:
        r = _rollback(
            observed_current_endpoint="cloud.example,18899,TCP",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_OBSERVED_CURRENT)
        self.assertEqual(r.endpoint, "cloud.example,18899,TCP")

    def test_equivalent_endpoint_different_spelling_is_none(self) -> None:
        # Equivalent host+port+protocol spellings are not rollback candidates.
        for spelling in ("192.168.1.50,8899,TCP", "192.168.1.50,8899"):
            r = _rollback(
                observed_current_endpoint=spelling,
                confirmed_ha_endpoint=_confirmed(),
            )
            self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE, spelling)

    def test_same_host_different_explicit_port_or_protocol_is_distinct(self) -> None:
        for endpoint in ("192.168.1.50,18899,TCP", "192.168.1.50,8899,UDP"):
            r = _rollback(
                observed_current_endpoint=endpoint,
                confirmed_ha_endpoint=_confirmed(),
            )
            self.assertEqual(r.provenance, CLOUD_PROVENANCE_OBSERVED_CURRENT, endpoint)

    def test_same_host_compact_endpoint_is_ambiguous(self) -> None:
        r = _rollback(
            observed_current_endpoint="192.168.1.50",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_missing_ha_endpoint_is_none(self) -> None:
        r = _rollback(
            observed_current_endpoint="cloud.example,18899,TCP",
            confirmed_ha_endpoint=None,
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_malformed_observation_is_none(self) -> None:
        r = _rollback(
            observed_current_endpoint="junk###",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_host_only_different_host_is_candidate(self) -> None:
        result = _rollback(
            observed_current_endpoint="ess.eybond.com",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(result.provenance, CLOUD_PROVENANCE_OBSERVED_CURRENT)
        self.assertEqual(result.endpoint, "ess.eybond.com")

    def test_non_string_observation_is_never_coerced(self) -> None:
        class EndpointDuck:
            def __str__(self) -> str:
                return "cloud.example,18899,TCP"

        result = _rollback(
            observed_current_endpoint=EndpointDuck(),
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(result.provenance, CLOUD_PROVENANCE_NONE)

    def test_observation_does_not_override_durable_or_registry(self) -> None:
        durable = _rollback(
            durable_original_endpoint="ess.eybond.com,18899,TCP",
            observed_current_endpoint="cloud.example,18899,TCP",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(durable.provenance, CLOUD_PROVENANCE_ORIGINAL)
        registry = _rollback(
            registry_endpoint="reg.example,18899,TCP",
            registry_pn=_PN_FULL,
            entry_pn=_PN_FULL,
            observed_current_endpoint="cloud.example,18899,TCP",
            confirmed_ha_endpoint=_confirmed(),
        )
        self.assertEqual(registry.provenance, CLOUD_PROVENANCE_REGISTRY)


class ResolveCloudRollbackD_NoInference(unittest.TestCase):
    def test_cloud_looking_host_without_confirmed_source_is_none(self) -> None:
        # A cloud-looking observed hostname is NOT promoted without a known HA
        # endpoint to prove it is external, and there is no durable/registry fact.
        r = _rollback(observed_current_endpoint="dtu.smartesscloud.com,18899,TCP")
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_NONE)

    def test_lan_looking_endpoint_with_valid_durable_stays_valid(self) -> None:
        # Hostname shape is not the authority: a LAN-looking durable original is
        # still a valid rollback endpoint.
        r = _rollback(durable_original_endpoint="192.168.9.9,18899,TCP")
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_ORIGINAL)
        self.assertEqual(r.endpoint, "192.168.9.9,18899,TCP")

    def test_resolver_signature_has_no_family_provider_kind_or_peer_ip(self) -> None:
        import inspect

        params = set(
            inspect.signature(resolve_cloud_rollback_endpoint).parameters
        )
        for banned in (
            "cloud_family",
            "provider",
            "collector_kind",
            "peer_ip",
            "hostname",
        ):
            self.assertNotIn(banned, params)

    def test_explicit_user_reserved_slot_wins_when_present(self) -> None:
        # CP2B.2 forward-compat: an explicit user endpoint is the top priority.
        r = _rollback(
            explicit_user_endpoint="chosen.example,18899,TCP",
            durable_original_endpoint="ess.eybond.com,18899,TCP",
        )
        self.assertEqual(r.provenance, CLOUD_PROVENANCE_EXPLICIT_USER)
        self.assertEqual(r.endpoint, "chosen.example,18899,TCP")

    def test_all_absent_is_none(self) -> None:
        self.assertEqual(_rollback().provenance, CLOUD_PROVENANCE_NONE)


class CloudRollbackSelectionModel(unittest.TestCase):
    """CP2B.2 Test A: the typed selection model invariants."""

    def _import(self):
        from custom_components.eybond_local.connection.strategy_transition_context import (
            CloudRollbackSelection,
            ROLLBACK_SELECTION_CATALOG,
            ROLLBACK_SELECTION_CONFIRMED_CANDIDATE,
            ROLLBACK_SELECTION_MANUAL,
            ROLLBACK_SOURCE_USER_CONFIRMED_EXISTING,
            ROLLBACK_SOURCE_USER_ENTERED_MANUAL,
            ROLLBACK_SOURCE_USER_SELECTED_CATALOG,
        )

        return (
            CloudRollbackSelection,
            ROLLBACK_SELECTION_CONFIRMED_CANDIDATE,
            ROLLBACK_SELECTION_CATALOG,
            ROLLBACK_SELECTION_MANUAL,
            ROLLBACK_SOURCE_USER_CONFIRMED_EXISTING,
            ROLLBACK_SOURCE_USER_SELECTED_CATALOG,
            ROLLBACK_SOURCE_USER_ENTERED_MANUAL,
        )

    def test_confirmed_candidate(self) -> None:
        S, CONF, _CAT, _MAN, SRC_C, _s2, _s3 = self._import()
        s = S(
            endpoint=CloudRollbackEndpoint("ess.eybond.com,18899,TCP", CLOUD_PROVENANCE_ORIGINAL),
            selection_kind=CONF,
            candidate_provenance=CLOUD_PROVENANCE_ORIGINAL,
            user_confirmed=True,
        )
        self.assertEqual(s.endpoint_value, "ess.eybond.com,18899,TCP")
        self.assertEqual(s.persistence_source, SRC_C)

    def test_catalog(self) -> None:
        S, _c, CAT, _m, _s1, SRC_CAT, _s3 = self._import()
        s = S(
            endpoint=CloudRollbackEndpoint("dtu.example,18899,TCP", CLOUD_PROVENANCE_EXPLICIT_USER),
            selection_kind=CAT,
            catalog_profile_key="smartess_at",
            user_confirmed=True,
        )
        self.assertEqual(s.persistence_source, SRC_CAT)
        self.assertEqual(s.catalog_profile_key, "smartess_at")

    def test_manual_host_only_preserved(self) -> None:
        S, _c, _cat, MAN, _s1, _s2, SRC_MAN = self._import()
        s = S(
            endpoint=CloudRollbackEndpoint("cloud.example", CLOUD_PROVENANCE_EXPLICIT_USER),
            selection_kind=MAN,
            user_confirmed=True,
        )
        self.assertEqual(s.endpoint_value, "cloud.example")
        self.assertEqual(s.persistence_source, SRC_MAN)

    def test_all_endpoint_shapes_supported(self) -> None:
        S, _c, _cat, MAN, *_ = self._import()
        for shape in ("cloud.example", "cloud.example,18899", "cloud.example,18899,TCP"):
            s = S(
                endpoint=CloudRollbackEndpoint(shape, CLOUD_PROVENANCE_EXPLICIT_USER),
                selection_kind=MAN,
                user_confirmed=True,
            )
            self.assertEqual(s.endpoint_value, shape)

    def test_rejections(self) -> None:
        S, CONF, CAT, MAN, *_ = self._import()
        ep_eu = CloudRollbackEndpoint("a.b,1,TCP", CLOUD_PROVENANCE_EXPLICIT_USER)
        ep_orig = CloudRollbackEndpoint("a.b,1,TCP", CLOUD_PROVENANCE_ORIGINAL)
        ep_obs = CloudRollbackEndpoint("a.b,1,TCP", CLOUD_PROVENANCE_OBSERVED_CURRENT)
        cases = [
            ("duck endpoint", lambda: S(endpoint="a.b,1,TCP", selection_kind=MAN, user_confirmed=True)),
            ("unconfirmed", lambda: S(endpoint=ep_eu, selection_kind=MAN, user_confirmed=False)),
            ("confirmed int 1", lambda: S(endpoint=ep_eu, selection_kind=MAN, user_confirmed=1)),
            ("confirmed str", lambda: S(endpoint=ep_eu, selection_kind=MAN, user_confirmed="true")),
            ("manual poses as observed", lambda: S(endpoint=ep_obs, selection_kind=MAN, user_confirmed=True)),
            ("catalog no key", lambda: S(endpoint=ep_eu, selection_kind=CAT, user_confirmed=True)),
            ("catalog wrong provenance", lambda: S(endpoint=ep_orig, selection_kind=CAT, catalog_profile_key="x", user_confirmed=True)),
            ("confirmed provenance mismatch", lambda: S(endpoint=ep_orig, selection_kind=CONF, candidate_provenance=CLOUD_PROVENANCE_OBSERVED_CURRENT, user_confirmed=True)),
            ("confirmed with catalog key", lambda: S(endpoint=ep_orig, selection_kind=CONF, candidate_provenance=CLOUD_PROVENANCE_ORIGINAL, catalog_profile_key="x", user_confirmed=True)),
            ("bad kind", lambda: S(endpoint=ep_eu, selection_kind="whatever", user_confirmed=True)),
            ("wildcard endpoint", lambda: S(endpoint=CloudRollbackEndpoint("0.0.0.0,18899,TCP", CLOUD_PROVENANCE_EXPLICIT_USER), selection_kind=MAN, user_confirmed=True)),
        ]
        for desc, fn in cases:
            with self.subTest(desc=desc):
                with self.assertRaises((ValueError, TypeError)):
                    fn()

    def test_selection_subclass_rejected_by_facade_type_check(self) -> None:
        # A subclass is NOT an exact CloudRollbackSelection: the coordinator's
        # exact-type gate (type(x) is CloudRollbackSelection) rejects it. Proven
        # here at the type level so the authority stays fail-closed.
        S, _c, _cat, MAN, *_ = self._import()

        class _Sneaky(S):
            pass

        obj = _Sneaky(
            endpoint=CloudRollbackEndpoint("a.b,1,TCP", CLOUD_PROVENANCE_EXPLICIT_USER),
            selection_kind=MAN,
            user_confirmed=True,
        )
        self.assertIsNot(type(obj), S)


class ContextConstruction(unittest.TestCase):
    def _candidate(self):
        return TransitionEndpointCandidate(
            host="192.168.1.50", port=8899, provenance=PROVENANCE_EFFECTIVE_RUNTIME_ROUTE
        )

    def test_valid(self) -> None:
        ctx = StrategyTransitionContext(
            current_strategy=CB_ON,
            target_strategy=INBOUND,
            ha_endpoint=self._candidate(),
            collector_trigger_target="",
            cloud_rollback=CloudRollbackEndpoint.none(),
        )
        self.assertTrue(ctx.to_inbound)

    def test_strategy_vocab_and_types(self) -> None:
        with self.assertRaises(ValueError):
            StrategyTransitionContext(
                current_strategy="bogus",
                target_strategy=INBOUND,
                ha_endpoint=self._candidate(),
                collector_trigger_target="",
                cloud_rollback=CloudRollbackEndpoint.none(),
            )
        with self.assertRaises(TypeError):
            StrategyTransitionContext(
                current_strategy=CB_ON,
                target_strategy=INBOUND,
                ha_endpoint={"host": "x"},  # not a TransitionEndpointCandidate
                collector_trigger_target="",
                cloud_rollback=CloudRollbackEndpoint.none(),
            )

    def test_forbidden_shapes(self) -> None:
        # current == target
        with self.assertRaises(ValueError):
            StrategyTransitionContext(
                current_strategy=INBOUND,
                target_strategy=INBOUND,
                ha_endpoint=self._candidate(),
                collector_trigger_target="",
                cloud_rollback=CloudRollbackEndpoint.none(),
            )
        # inbound target must NOT carry a trigger target
        with self.assertRaises(ValueError):
            StrategyTransitionContext(
                current_strategy=CB_ON,
                target_strategy=INBOUND,
                ha_endpoint=self._candidate(),
                collector_trigger_target="192.168.1.77",
                cloud_rollback=CloudRollbackEndpoint.none(),
            )
        # callback target REQUIRES a trigger target
        with self.assertRaises(ValueError):
            StrategyTransitionContext(
                current_strategy=INBOUND,
                target_strategy=CB_ON,
                ha_endpoint=self._candidate(),
                collector_trigger_target="",
                cloud_rollback=CloudRollbackEndpoint.none(),
            )
        # callback trigger target must not be a wildcard
        for wild in ("0.0.0.0", "::", "*"):
            with self.assertRaises(ValueError):
                StrategyTransitionContext(
                    current_strategy=INBOUND,
                    target_strategy=CB_ON,
                    ha_endpoint=self._candidate(),
                    collector_trigger_target=wild,
                    cloud_rollback=CloudRollbackEndpoint.none(),
                )

    def test_callback_target_accepts_normalized_hostname(self) -> None:
        # A normalized hostname is fine -- no IP-only heuristic.
        ctx = StrategyTransitionContext(
            current_strategy=INBOUND,
            target_strategy=CB_ON,
            ha_endpoint=self._candidate(),
            collector_trigger_target="collector.example.com",
            cloud_rollback=CloudRollbackEndpoint.none(),
        )
        self.assertEqual(ctx.collector_trigger_target, "collector.example.com")

    def test_valid_callback_target(self) -> None:
        ctx = StrategyTransitionContext(
            current_strategy=INBOUND,
            target_strategy=CB_ON,
            ha_endpoint=self._candidate(),
            collector_trigger_target="192.168.1.77",
            cloud_rollback=CloudRollbackEndpoint.none(),
        )
        self.assertFalse(ctx.to_inbound)


PN = "V001020SYN62344022"
TS_ROUTE = "2026-07-16T10:00:00+00:00"


def _callback_terminal(advertised: str = "195.191.72.37:18899", *, pn: str = PN):
    from custom_components.eybond_local.connection.recovery.terminal import (
        RecoveryTerminalInput,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        CallbackRecoveryProof,
    )

    return RecoveryTerminalInput(
        collector_pn=pn,
        callback_proof=CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=pn,
            identity_source="fc2_parameter_2",
            verified_at=TS_ROUTE,
            trigger_target="203.0.113.10:58899",
            advertised_ha_endpoint=advertised,
            listener_port=18899,
        ),
    )


def _inbound_terminal(*, pn: str = PN):
    from custom_components.eybond_local.connection.recovery.terminal import (
        RecoveryTerminalInput,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        InboundRecoveryProof,
    )

    return RecoveryTerminalInput(
        collector_pn=pn,
        inbound_proof=InboundRecoveryProof(
            method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
            collector_pn=pn,
            identity_source="fc2_parameter_2",
            verified_at=TS_ROUTE,
            session_protocol="eybond_framed",
        ),
    )


class EarnedAdvertisedRoute(unittest.TestCase):
    """The route PERSISTED on a verified strategy commit -- typed, fail-closed."""

    def test_callback_persists_exactly_the_proof_route(self) -> None:
        self.assertEqual(
            earned_advertised_route(
                committed_strategy="callback_on_demand",
                terminal=_callback_terminal("195.191.72.37:18899"),
                attempted_host="195.191.72.37",
                attempted_port=18899,
            ),
            ("195.191.72.37", 18899, ""),
        )

    def test_callback_proof_route_mismatch_fails_closed(self) -> None:
        host, port, refusal = earned_advertised_route(
            committed_strategy="callback_on_demand",
            terminal=_callback_terminal("1.2.3.4:9000"),  # proof != attempted
            attempted_host="195.191.72.37",
            attempted_port=18899,
        )
        self.assertEqual((host, port), ("", 0))
        self.assertEqual(refusal, "transition_callback_route_mismatch")

    def test_callback_with_wrong_proof_type_is_unproven(self) -> None:
        # An inbound terminal on a callback commit carries no callback proof.
        refusal = earned_advertised_route(
            committed_strategy="callback_on_demand",
            terminal=_inbound_terminal(),
            attempted_host="195.191.72.37",
            attempted_port=18899,
        )[2]
        self.assertEqual(refusal, "transition_callback_route_unproven")

    def test_inbound_persists_confirmed_endpoint(self) -> None:
        self.assertEqual(
            earned_advertised_route(
                committed_strategy="inbound",
                terminal=_inbound_terminal(),
                attempted_host="192.168.1.50",
                attempted_port=8899,
            ),
            ("192.168.1.50", 8899, ""),
        )

    def test_inbound_without_inbound_proof_is_unproven(self) -> None:
        refusal = earned_advertised_route(
            committed_strategy="inbound",
            terminal=_callback_terminal(),
            attempted_host="192.168.1.50",
            attempted_port=8899,
        )[2]
        self.assertEqual(refusal, "transition_inbound_route_unproven")

    def test_non_strategy_commit_persists_nothing(self) -> None:
        self.assertEqual(
            earned_advertised_route(
                committed_strategy=None,
                terminal=_inbound_terminal(),
                attempted_host="192.168.1.50",
                attempted_port=8899,
            ),
            ("", 0, ""),
        )

    def test_malformed_attempted_route_fails_closed(self) -> None:
        # No coercion: empty/wildcard/duck host, str/bool/out-of-range port all
        # yield a typed refusal -- never an empty "success" that lets a commit
        # proceed with no route.
        for host, port in (
            ("", 8899),
            ("0.0.0.0", 8899),
            (object(), 8899),
            (" 192.168.1.50 ", 8899),
            ("192.168.1.50", "8899"),
            ("192.168.1.50", True),
            ("192.168.1.50", 0),
            ("192.168.1.50", 70000),
        ):
            refusal = earned_advertised_route(
                committed_strategy="inbound",
                terminal=_inbound_terminal(),
                attempted_host=host,
                attempted_port=port,
            )[2]
            self.assertEqual(
                refusal, "transition_advertised_route_invalid", msg=f"{host!r}/{port!r}"
            )

    def test_duck_terminal_is_refused(self) -> None:
        import types

        refusal = earned_advertised_route(
            committed_strategy="callback_on_demand",
            terminal=types.SimpleNamespace(callback_proof=None),
            attempted_host="195.191.72.37",
            attempted_port=18899,
        )[2]
        self.assertEqual(refusal, "transition_terminal_proof_required")

    def test_invalid_committed_strategy_is_refused_not_merged(self) -> None:
        # A bogus / non-string / duck strategy is a typed refusal, NEVER a
        # harmless non-strategy merge -- ``None`` is the only true merge.
        class EqualToInbound:
            def __eq__(self, other):
                return other == "inbound"

        class InboundStr(str):
            pass

        for bad in (
            "bogus",
            123,
            object(),
            True,
            EqualToInbound(),
            InboundStr("inbound"),
        ):
            refusal = earned_advertised_route(
                committed_strategy=bad,
                terminal=_inbound_terminal(),
                attempted_host="192.168.1.50",
                attempted_port=8899,
            )[2]
            self.assertEqual(
                refusal, "transition_committed_strategy_invalid", msg=f"{bad!r}"
            )
        self.assertEqual(
            earned_advertised_route(
                committed_strategy=None,
                terminal=_inbound_terminal(),
                attempted_host="192.168.1.50",
                attempted_port=8899,
            ),
            ("", 0, ""),
        )


class AdvertisedInputParsers(unittest.TestCase):
    """Blocker 3: safe submit parsers -- malformed input never raises."""

    def test_parse_advertised_port(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition_context import (
            parse_advertised_port as P,
        )

        self.assertEqual(P(18899), 18899)
        self.assertEqual(P("18899"), 18899)  # decimal string (text field)
        self.assertEqual(P(18899.0), 18899)  # integer-valued float (NumberSelector)
        for bad in (True, "abc", "18899 ", " 18899", "18899.5", 18899.5, 0, 70000, None, object()):
            self.assertIsNone(P(bad), msg=f"accepted {bad!r}")

    def test_normalized_advertised_host(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition_context import (
            normalized_advertised_host as H,
        )

        self.assertEqual(H("192.168.1.50"), "192.168.1.50")
        for bad in ("0.0.0.0", "::", "*", " x ", "", object(), None):
            self.assertIsNone(H(bad), msg=f"accepted {bad!r}")


class Cp1bArchitectureGuards(unittest.TestCase):
    """§8: prefill via the resolver, contract only via from_entry_data, the ONE
    durable route write in the authority commit (never the config flow)."""

    PKG = REPO_ROOT / "custom_components" / "eybond_local"
    CONFIG_FLOW = PKG / "config_flow.py"
    OPTIONS_RUNTIME = PKG / "options_runtime.py"
    OPTIONS_STRATEGY = PKG / "options_strategy.py"
    COORDINATOR = PKG / "runtime" / "coordinator.py"

    def _method(self, path, cls_name, method_name):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        cls = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.ClassDef) and n.name == cls_name
        )
        return next(
            m for m in cls.body
            if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))
            and m.name == method_name
        )

    def _calls(self, node):
        return {
            s.func.id
            for s in ast.walk(node)
            if isinstance(s, ast.Call) and isinstance(s.func, ast.Name)
        }

    def _attrs(self, node):
        return {s.attr for s in ast.walk(node) if isinstance(s, ast.Attribute)}

    def test_prefill_uses_resolver_and_contract_from_entry_data(self) -> None:
        prefill = self._method(
            self.OPTIONS_STRATEGY,
            "StrategyTransitionOptionsMixin",
            "_transition_prefill",
        )
        self.assertIn("resolve_default_ha_endpoint", self._calls(prefill))
        # The callback proof is read ONLY through RecoveryContract.from_entry_data.
        self.assertIn("from_entry_data", self._attrs(prefill))
        self.assertIn("advertised_ha_endpoint", self._attrs(prefill))

    def test_result_step_does_no_route_persistence(self) -> None:
        result = self._method(
            self.OPTIONS_STRATEGY,
            "StrategyTransitionOptionsMixin",
            "async_step_strategy_transition_result",
        )
        names = {
            s.id for s in ast.walk(result) if isinstance(s, ast.Name)
        } | self._attrs(result)
        self.assertNotIn("CONF_ADVERTISED_SERVER_IP", names)
        self.assertNotIn("CONF_ADVERTISED_TCP_PORT", names)
        # No entry write from the terminal step (the authority already committed).
        self.assertNotIn("async_update_entry", self._attrs(result))

    def test_staged_option_payload_excludes_advertised_keys(self) -> None:
        commit = self._method(
            self.OPTIONS_RUNTIME,
            "RuntimeOptionsMixin",
            "_async_commit_runtime_options",
        )
        # The generic option payload staged for the transition is poll/control
        # only -- topology/advertised keys are never carried through it.
        names = {s.id for s in ast.walk(commit) if isinstance(s, ast.Name)}
        # (advertised keys may be read elsewhere, but not staged here)
        payload_src = ast.get_source_segment(
            self.OPTIONS_RUNTIME.read_text(encoding="utf-8"), commit
        )
        self.assertNotIn("CONF_ADVERTISED_SERVER_IP", payload_src or "")
        self.assertNotIn("CONF_ADVERTISED_TCP_PORT", payload_src or "")

    def test_durable_route_write_is_in_authority_commits_only(self) -> None:
        coord_src = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((self.PKG / "runtime").glob("coordinator*.py"))
        )
        non_authority_flow_src = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                self.CONFIG_FLOW,
                self.OPTIONS_RUNTIME,
            )
        )
        repair_authority_src = self.OPTIONS_STRATEGY.read_text(encoding="utf-8")
        # Normal transitions persist through the coordinator commit. Cold repair
        # has no running coordinator, so its single durable commit lives in the
        # repair orchestrator. Both use the same neutral earned-route boundary.
        self.assertIn("earned_advertised_route", coord_src)
        self.assertIn("data[CONF_ADVERTISED_SERVER_IP] = route_host", coord_src)
        self.assertIn("options.pop(CONF_ADVERTISED_SERVER_IP", coord_src)
        self.assertEqual(
            repair_authority_src.count(
                "data[CONF_ADVERTISED_SERVER_IP] = route_host"
            ),
            1,
        )
        self.assertIn("earned_advertised_route", repair_authority_src)
        # Presentation and generic runtime-options code perform no durable route
        # write outside those two atomic authority commits.
        self.assertNotIn(
            "data[CONF_ADVERTISED_SERVER_IP]", non_authority_flow_src
        )
        self.assertNotIn(
            "options[CONF_ADVERTISED_SERVER_IP]", non_authority_flow_src
        )

    def test_resolver_takes_no_peer_l2_hostname_cloudfamily_input(self) -> None:
        # The resolver signature admits ONLY strictly-separated route sources.
        import inspect

        from custom_components.eybond_local.connection import (
            strategy_transition_context as m,
        )

        params = set(inspect.signature(m.resolve_default_ha_endpoint).parameters)
        for banned in ("peer_ip", "l2", "hostname", "cloud_family", "destination"):
            self.assertNotIn(banned, params)


if __name__ == "__main__":
    unittest.main()
