"""Batch 1 CP1a -- the neutral strategy-transition endpoint model + resolver.

Pure, no Home Assistant, no config_flow: strict typed construction, the closed
provenance vocabularies, and the default-endpoint resolution priority (explicit >
validated callback proof > caller-role-proven HA endpoint > effective runtime
route > none). The resolver derives NOTHING from peer IP / L2 / hostname / cloud
family -- it has no such input.
"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.strategy_transition_context import (
    CLOUD_PROVENANCE_NONE,
    CLOUD_PROVENANCE_ORIGINAL,
    CloudRollbackEndpoint,
    PROVENANCE_CALLBACK_PROOF,
    PROVENANCE_CONFIRMED_HA_ENDPOINT,
    PROVENANCE_EFFECTIVE_RUNTIME_ROUTE,
    PROVENANCE_EXPLICIT_ADVERTISED,
    PROVENANCE_NONE,
    StrategyTransitionContext,
    TransitionEndpointCandidate,
    resolve_default_ha_endpoint,
)

CB_ON = "callback_on_demand"
INBOUND = "inbound"


def _resolve(**over):
    base = dict(
        explicit_advertised_host="",
        explicit_advertised_port=0,
        callback_proof_endpoint="",
        confirmed_ha_endpoint=None,
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
        # explicit > proof > confirmed > effective
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
        self.assertEqual(_resolve().provenance, PROVENANCE_EFFECTIVE_RUNTIME_ROUTE)

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
        # A known rollback endpoint that could be WRITTEN must be a valid
        # host,port[,proto]; garbage like "x" is rejected (not merely non-empty).
        for bad in ("x", "host,port,TCP", "host,99999,TCP", ",18899,TCP"):
            with self.assertRaises(ValueError):
                CloudRollbackEndpoint(endpoint=bad, provenance=CLOUD_PROVENANCE_ORIGINAL)
        # host,port (proto defaults, no cloud-family involvement) is valid.
        self.assertTrue(
            CloudRollbackEndpoint(
                endpoint="203.0.113.9,18899", provenance=CLOUD_PROVENANCE_ORIGINAL
            ).known
        )

    def test_wildcard_rollback_target_rejected(self) -> None:
        # A wildcard bind is syntactically parseable but is never a safe rollback
        # target to write to a collector.
        for wild in ("0.0.0.0,18899,TCP", "0.0.0.0,18899"):
            with self.assertRaises(ValueError):
                CloudRollbackEndpoint(endpoint=wild, provenance=CLOUD_PROVENANCE_ORIGINAL)


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


if __name__ == "__main__":
    unittest.main()
