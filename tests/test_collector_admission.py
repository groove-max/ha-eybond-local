"""Typed collector-admission foundation (Batch 1).

An observed callback session is only the STARTING point of a controlled recovery
experiment -- never proof of a permanent inbound configuration. These tests pin
the typed trust boundary that both admission source adapters (integration
discovery and the passive phase of a user-started scan) share:

* :class:`ObservedCollectorSession` -- a neutral, immutable record of observed
  facts that never decides a strategy, never promotes a weak heartbeat PN to a
  durable identity, and never treats a peer IP as identity or a callback route;
* :class:`CollectorAdmissionRequest` -- the ONE typed input to the single
  config-flow admission entrypoint;
* architecture guards proving there is exactly ONE admission entrypoint / verifier
  and no source-specific admission algorithm (no second verifier, no branching on
  ``connection_mode`` / ``detection`` reason / collector kind / cloud / peer IP).

Synthetic identities only (``SYN`` / allow-listed ``E500`` fixtures).
"""

from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.admission import (
    CollectorAdmissionRequest,
    ObservedCollectorSession,
)
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
)

FULL_PN = "V001020SYN62344022"
SESSION_ID = "listener-8899-1"
PEER_IP = "203.0.113.10"


def _observed(**overrides) -> ObservedCollectorSession:
    kwargs = {
        "collector_pn": FULL_PN,
        "identity_source": "fc2_parameter_2",
        "session_id": SESSION_ID,
        "listener_port": 8899,
        "protocol_shape": "eybond_framed",
        "peer_hint": PEER_IP,
    }
    kwargs.update(overrides)
    return ObservedCollectorSession(**kwargs)


class ObservedCollectorSessionModelTests(unittest.TestCase):
    def test_valid_strong_observation_is_recorded_and_strong(self) -> None:
        for strong in ("fc2_parameter_2", "at_dtupn"):
            obs = _observed(identity_source=strong)
            self.assertEqual(obs.collector_pn, FULL_PN)
            self.assertEqual(obs.session_id, SESSION_ID)
            self.assertTrue(obs.has_strong_identity)

    def test_weak_observation_is_valid_but_never_strong(self) -> None:
        # A short framed heartbeat PN is an HONEST weak observation; the model
        # records it but must NOT promote it to a durable/strong identity.
        obs = _observed(collector_pn=FULL_PN[:14], identity_source="framed_heartbeat")
        self.assertEqual(obs.collector_pn, FULL_PN[:14])
        self.assertFalse(obs.has_strong_identity)
        # Empty / unknown source is likewise honest-but-weak.
        self.assertFalse(_observed(identity_source="").has_strong_identity)

    def test_strong_weak_decided_only_by_centralized_rule(self) -> None:
        from custom_components.eybond_local.collector_identity import (
            identity_source_is_strong,
        )

        for source in ("fc2_parameter_2", "at_dtupn", "framed_heartbeat", "", "xx"):
            self.assertEqual(
                _observed(identity_source=source).has_strong_identity,
                identity_source_is_strong(source),
            )

    def test_non_string_fields_are_rejected(self) -> None:
        for field_name in ("collector_pn", "identity_source", "session_id"):
            with self.assertRaises(TypeError):
                _observed(**{field_name: 12345})

    def test_padded_strings_are_rejected(self) -> None:
        for field_name in (
            "collector_pn",
            "identity_source",
            "session_id",
            "protocol_shape",
            "peer_hint",
        ):
            with self.assertRaises(ValueError):
                _observed(**{field_name: " padded "})

    def test_required_fields_must_be_present(self) -> None:
        for field_name in ("collector_pn", "session_id"):
            with self.assertRaises(ValueError):
                _observed(**{field_name: ""})

    def test_invalid_port_is_rejected(self) -> None:
        for bad in (0, -1, 70000):
            with self.assertRaises(ValueError):
                _observed(listener_port=bad)
        for bad_type in ("8899", 88.99, True, None):
            with self.assertRaises(TypeError):
                _observed(listener_port=bad_type)

    def test_peer_hint_is_never_identity(self) -> None:
        # The peer is display/diagnostic only: it lives in its own field, is not
        # part of the identity, and does not influence strong/weak.
        obs = _observed(peer_hint="198.51.100.7")
        self.assertEqual(obs.peer_hint, "198.51.100.7")
        self.assertNotEqual(obs.peer_hint, obs.collector_pn)
        self.assertNotEqual(obs.peer_hint, obs.session_id)
        self.assertTrue(_observed(peer_hint="198.51.100.7").has_strong_identity)
        self.assertFalse(
            _observed(peer_hint="198.51.100.7", identity_source="").has_strong_identity
        )

    def test_model_carries_no_connection_strategy(self) -> None:
        names = {f.name for f in fields(ObservedCollectorSession)}
        self.assertEqual(
            names,
            {
                "collector_pn",
                "identity_source",
                "session_id",
                "listener_port",
                "protocol_shape",
                "peer_hint",
            },
        )
        for banned in ("connection_strategy", "strategy", "connection_mode"):
            self.assertNotIn(banned, names)
            self.assertFalse(hasattr(_observed(), banned))

    def test_model_is_frozen_immutable(self) -> None:
        obs = _observed()
        with self.assertRaises(Exception):
            obs.collector_pn = "mutated"  # type: ignore[misc]


class CollectorAdmissionRequestModelTests(unittest.TestCase):
    def test_wraps_observed_session_with_diagnostic_origin(self) -> None:
        obs = _observed()
        request = CollectorAdmissionRequest(observed_session=obs, origin="passive_scan")
        self.assertIs(request.observed_session, obs)
        self.assertEqual(request.origin, "passive_scan")

    def test_duck_typed_observation_is_rejected(self) -> None:
        duck = SimpleNamespace(
            collector_pn=FULL_PN,
            identity_source="fc2_parameter_2",
            session_id=SESSION_ID,
            listener_port=8899,
        )
        with self.assertRaises(TypeError):
            CollectorAdmissionRequest(observed_session=duck)  # type: ignore[arg-type]

    def test_subclass_observation_is_rejected(self) -> None:
        # Exact type identity on the trust boundary: even a real subclass is not
        # an ObservedCollectorSession for admission purposes.
        class _Sneaky(ObservedCollectorSession):
            pass

        sub = _Sneaky(
            collector_pn=FULL_PN,
            identity_source="fc2_parameter_2",
            session_id=SESSION_ID,
            listener_port=8899,
        )
        with self.assertRaises(TypeError):
            CollectorAdmissionRequest(observed_session=sub)

    def test_non_string_origin_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            CollectorAdmissionRequest(observed_session=_observed(), origin=123)  # type: ignore[arg-type]

    def test_padded_origin_is_rejected_empty_is_allowed(self) -> None:
        # ``origin`` is a strict normalized diagnostic label: padding is rejected,
        # an empty label is fine.
        for padded in (" passive_scan", "passive_scan ", "  ", "\tpassive_scan"):
            with self.assertRaises(ValueError):
                CollectorAdmissionRequest(observed_session=_observed(), origin=padded)
        self.assertEqual(
            CollectorAdmissionRequest(observed_session=_observed(), origin="").origin,
            "",
        )
        # The default (no origin) is the empty label.
        self.assertEqual(
            CollectorAdmissionRequest(observed_session=_observed()).origin, ""
        )

    def test_request_carries_only_observation_route_and_diagnostic_origin(self) -> None:
        names = {f.name for f in fields(CollectorAdmissionRequest)}
        # The route is now a real consumer-backed capability: only an ACTIVE
        # scan that exercised it supplies one. It is still not a proof/strategy.
        self.assertEqual(names, {"observed_session", "origin", "callback_route"})
        for banned in (
            "inbound_verified",
            "recovery_contract",
            "proof",
            "strategy",
        ):
            self.assertNotIn(banned, names)

    def test_callback_route_is_exact_typed_and_structurally_valid(self) -> None:
        route = CallbackRecoveryRoute(
            bind_ip="192.0.2.10",
            trigger_target_ip="192.0.2.20",
            trigger_udp_port=58899,
            advertised_ha_host="198.51.100.10",
            advertised_ha_port=18899,
            listener_port=8899,
        )
        request = CollectorAdmissionRequest(
            observed_session=_observed(), callback_route=route
        )
        self.assertIs(request.callback_route, route)
        with self.assertRaises(TypeError):
            CollectorAdmissionRequest(
                observed_session=_observed(),
                callback_route=SimpleNamespace(invalid_reason=lambda: ""),
            )
        with self.assertRaises(ValueError):
            CollectorAdmissionRequest(
                observed_session=_observed(),
                callback_route=CallbackRecoveryRoute(
                    bind_ip="",
                    trigger_target_ip="192.0.2.20",
                    trigger_udp_port=58899,
                    advertised_ha_host="198.51.100.10",
                    advertised_ha_port=18899,
                    listener_port=8899,
                ),
            )


class CollectorAdmissionArchitectureGuards(unittest.TestCase):
    """Load-bearing guards: ONE admission entrypoint/verifier, no source-specific
    admission algorithm. They read the REAL production source from disk (no import,
    so they need none of the HA runtime stubs)."""

    @classmethod
    def setUpClass(cls) -> None:
        package = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
        )
        paths = (
            package / "config_flow.py",
            package / "config_entry.py",
            *sorted((package / "flows" / "config").glob("*.py")),
        )
        sources = {path: path.read_text(encoding="utf-8") for path in paths}
        cls.module_source = "\n".join(sources.values())
        cls.method_source: dict[str, str] = {}
        cls.method_names: set = set()
        for source in sources.values():
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        cls.method_names.add(item.name)
                        segment = ast.get_source_segment(source, item)
                        cls.method_source[item.name] = segment or ""

    def _method_source(self, name: str) -> str:
        return self.method_source[name]

    def _attr_names(self, source: str) -> set:
        import textwrap

        tree = ast.parse(textwrap.dedent(source))
        return {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }

    def _code_identifiers(self, source: str) -> set:
        # Real code identifiers only -- Name/Attribute/kwarg/arg -- so a DOCSTRING
        # or comment that DESCRIBES what is not read ("never on connection_mode")
        # is not a false match.
        import textwrap

        tree = ast.parse(textwrap.dedent(source))
        ids: set = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                ids.add(node.id)
            elif isinstance(node, ast.Attribute):
                ids.add(node.attr)
            elif isinstance(node, ast.keyword) and node.arg:
                ids.add(node.arg)
            elif isinstance(node, ast.arg):
                ids.add(node.arg)
        return ids

    def test_tactical_767253d_branches_are_gone(self) -> None:
        # The source-specific tactical helpers were absorbed into the typed
        # boundary; none may come back.
        for gone in (
            "_async_begin_observed_session_verification",
            "_async_verify_selected_passive_scan_result",
            "_fresh_passive_scan_entry_is_unverified",
        ):
            self.assertNotIn(
                gone,
                self.method_names,
                msg=f"tactical branch {gone!r} must not be reintroduced",
            )
        self.assertNotIn("_strategy_verification_context = {", self.module_source)

    def test_exactly_one_admission_entrypoint_owns_the_transaction(self) -> None:
        # Only the ONE entrypoint mints the in-flight admission TRANSACTION; every
        # other write clears it. That is what makes it the single verifier gate.
        import textwrap

        def _constructor_count(source: str) -> int:
            tree = ast.parse(textwrap.dedent(source))
            return sum(
                1
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "CollectorAdmissionTransaction"
            )

        self.assertIn("_async_begin_collector_admission", self.method_names)
        entry = self._method_source("_async_begin_collector_admission")
        self.assertEqual(
            _constructor_count(self.module_source),
            1,
            "more than one admission entrypoint mints the transaction",
        )
        self.assertEqual(_constructor_count(entry), 1)
        # The constructor result is deliberately assigned to both source-boundary
        # roles: the admission verifier and the callback continuation must be the
        # exact same transaction object.
        self.assertIn("self._admission_transaction = transaction", entry)
        self.assertIn("self._callback_continuation = transaction", entry)

    def test_both_source_adapters_use_the_one_entrypoint(self) -> None:
        discovery = self._method_source("async_step_integration_discovery")
        passive = self._method_source("_async_admit_selected_scan_result")
        for adapter in (discovery, passive):
            self.assertIn("CollectorAdmissionRequest(", adapter)
            self.assertIn("_async_begin_collector_admission", adapter)

    def test_passive_adapter_branches_only_on_typed_observed_session(self) -> None:
        # The adapter must decide "needs admission" from the typed session carrier
        # alone -- never from connection_mode / detection / details / peer IP.
        ids = self._code_identifiers(
            self._method_source("_async_admit_selected_scan_result")
        )
        self.assertIn("observed_session", ids)
        for banned in (
            "connection_mode",
            "detection",
            "details",
            "peer_ip",
            "remote_ip",
        ):
            self.assertNotIn(
                banned, ids, msg=f"passive adapter must not read {banned!r}"
            )

    def test_admission_entrypoint_ignores_diagnostic_origin(self) -> None:
        # ``origin`` is diagnostics only: the algorithm must not branch on it.
        attrs = self._attr_names(self._method_source("_async_begin_collector_admission"))
        self.assertNotIn("origin", attrs)

    def test_fail_closed_terminal_is_source_neutral_on_the_admission_request(
        self,
    ) -> None:
        # SOURCE-NEUTRAL: the terminal guard keys on the typed in-flight
        # CollectorAdmissionTransaction (set by BOTH adapters through the ONE
        # entrypoint), NOT on the selected-result projection -- integration
        # discovery's selected result carries no observed_session, so keying on
        # it would silently miss the discovery source.
        ids = self._code_identifiers(
            self._method_source("_fresh_observed_session_entry_is_unverified")
        )
        # Keyed on the typed in-flight transaction + the real recovery proof ...
        self.assertIn("_admission_transaction", ids)
        self.assertIn("CollectorAdmissionTransaction", ids)
        self.assertIn("inbound_verified", ids)
        # ... never on the selected-result projection, origin, or kind markers.
        for banned in (
            "_selected_result",
            "observed_session",
            "origin",
            "connection_mode",
            "collector_cloud_family",
            "peer_ip",
            "reason",
        ):
            self.assertNotIn(
                banned, ids, msg=f"fail-closed terminal must not read {banned!r}"
            )

    def _string_literals(self, source: str) -> set:
        # String CONSTANTS actually used by the code (dict keys, arguments) --
        # excludes comments and prose, so a guard can forbid a real
        # ``.get("eybond_discovery")`` without tripping over documentation.
        import textwrap

        tree = ast.parse(textwrap.dedent(source))
        return {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }

    def test_run_verification_never_reads_discovery_identity_source(self) -> None:
        # blocker 2 (now Batch 2B): the admission run lives in the NEUTRAL
        # transaction. Identity authority comes ONLY from the typed observation +
        # typed verifier outcomes -- it structurally cannot sniff the flow's
        # eybond_discovery context for a wire identity / require_exact / ownership.
        import ast

        txn_source = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "connection"
            / "admission_transaction.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(txn_source)
        run = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            and n.name == "async_run"
        )
        literals = {
            node.value
            for node in ast.walk(run)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        self.assertNotIn("collector_identity_source", literals)
        self.assertNotIn("eybond_discovery", literals)
        # It DOES take identity from the typed observation + the enriched
        # expected PN.
        attrs = {n.attr for n in ast.walk(run) if isinstance(n, ast.Attribute)}
        self.assertIn("identity_source", attrs)
        self.assertIn("_expected_pn", attrs)


class AdmissionDependencyGraphGuards(unittest.TestCase):
    """The neutral admission model must NOT re-introduce a back-dependency on the
    onboarding layer -- neither statically nor at runtime import."""

    def test_admission_module_imports_no_onboarding_layer(self) -> None:
        # Static: the neutral module has no import statement into onboarding.
        import ast

        source = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "connection"
            / "admission.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported: set = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
            elif isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
        for module in imported:
            self.assertNotIn(
                "onboarding",
                module,
                msg=f"admission model must not import onboarding ({module})",
            )
            self.assertNotIn(
                "strategy_verification",
                module,
                msg=f"admission model must not import strategy_verification ({module})",
            )

    def test_importing_admission_model_does_not_import_strategy_verification(
        self,
    ) -> None:
        # Runtime: a fresh interpreter importing ONLY the neutral admission model
        # must not pull in onboarding.strategy_verification (the removed back-edge).
        import subprocess

        code = (
            "import sys\n"
            f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
            "import custom_components.eybond_local.connection.admission\n"
            "bad = [m for m in sys.modules if 'strategy_verification' in m]\n"
            "print('BAD' if bad else 'CLEAN')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr[-500:])
        self.assertIn("CLEAN", result.stdout)
        self.assertNotIn("BAD", result.stdout)


if __name__ == "__main__":
    unittest.main()
