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
    earned_advertised_route,
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
            self.CONFIG_FLOW, "EybondLocalOptionsFlow", "_transition_prefill"
        )
        self.assertIn("resolve_default_ha_endpoint", self._calls(prefill))
        # The callback proof is read ONLY through RecoveryContract.from_entry_data.
        self.assertIn("from_entry_data", self._attrs(prefill))
        self.assertIn("advertised_ha_endpoint", self._attrs(prefill))

    def test_result_step_does_no_route_persistence(self) -> None:
        result = self._method(
            self.CONFIG_FLOW,
            "EybondLocalOptionsFlow",
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
            self.CONFIG_FLOW, "EybondLocalOptionsFlow", "_async_commit_runtime_options"
        )
        # The generic option payload staged for the transition is poll/control
        # only -- topology/advertised keys are never carried through it.
        names = {s.id for s in ast.walk(commit) if isinstance(s, ast.Name)}
        # (advertised keys may be read elsewhere, but not staged here)
        payload_src = ast.get_source_segment(
            self.CONFIG_FLOW.read_text(encoding="utf-8"), commit
        )
        self.assertNotIn("CONF_ADVERTISED_SERVER_IP", payload_src or "")
        self.assertNotIn("CONF_ADVERTISED_TCP_PORT", payload_src or "")

    def test_durable_route_write_is_in_coordinator_commit_only(self) -> None:
        coord_src = self.COORDINATOR.read_text(encoding="utf-8")
        flow_src = self.CONFIG_FLOW.read_text(encoding="utf-8")
        # The ONE durable advertised-route write lives in the coordinator commit,
        # via the neutral earned_advertised_route helper.
        self.assertIn("earned_advertised_route", coord_src)
        self.assertIn("data[CONF_ADVERTISED_SERVER_IP] = route_host", coord_src)
        self.assertIn("options.pop(CONF_ADVERTISED_SERVER_IP", coord_src)
        # The config flow performs NO durable advertised-route write.
        self.assertNotIn("data[CONF_ADVERTISED_SERVER_IP]", flow_src)
        self.assertNotIn("options[CONF_ADVERTISED_SERVER_IP]", flow_src)

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
