"""Batch 2D.1 -- the typed callback-continuation ownership seam.

Two kinds of evidence:

* architecture guards (AST / source / clean-interpreter import): the shared
  config-flow callback and recovery orchestration routes through the ONE typed
  seam; the two authorities are invoked only inside the flow-backed adapter;
  exactly one callback identity and one callback recovery authority exist; the
  neutral contract imports no upward layer; no admission-specific branch was
  introduced in the shared callback methods; and CollectorAdmissionTransaction
  is byte-for-byte unchanged;

* behavior parity (functional): the legacy adapter reproduces the exact pre-seam
  field effects and owner transitions, and the shared methods really call the
  seam rather than a bypass.

The A-I flow-level behaviours (ordinary manual, silent bootstrap, reconfigure,
pending, terminal, flow removal, callback/inbound distinction) are additionally
pinned end-to-end by the existing suites, which now run through the seam
unchanged (test_config_flow, test_callback_recovery, test_manual_silent_callback_e2e,
test_pending_collector, test_recovery_terminalization).
"""

from __future__ import annotations

import ast
from contextlib import contextmanager
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
TESTS_DIR = REPO_ROOT / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

PKG = REPO_ROOT / "custom_components" / "eybond_local"
CONFIG_FLOW = PKG / "config_flow.py"
SEAM = PKG / "connection" / "callback_continuation.py"
ADMISSION_TXN = PKG / "connection" / "admission_transaction.py"

# Installs the stub homeassistant modules BEFORE the integration imports.
import test_config_flow as flow_scaffold  # noqa: E402,F401
from test_config_flow import _FakeHass, _wire_session  # noqa: E402

import custom_components.eybond_local.config_flow as config_flow_module
from custom_components.eybond_local.config_flow import EybondLocalConfigFlow
from custom_components.eybond_local.connection.callback_continuation import (
    CallbackContinuation,
    CallbackIdentityContext,
    TerminalDecision,
)
from custom_components.eybond_local.connection.callback_identity import (
    CallbackIdentityOutcome,
    CallbackIdentityRequest,
    IDENTITY_OK,
    SilentSessionBootstrapOffer,
)
from custom_components.eybond_local.connection.recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    CallbackRecoveryProof,
    InboundRecoveryProof,
)
from custom_components.eybond_local.connection.recovery.terminal import (
    RecoveryTerminalInput,
)
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
    RecoveryVerificationOutcome,
    STATE_CALLBACK_VERIFIED,
    STATE_INBOUND_RECOVERED,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.const import (
    CONF_COLLECTOR_IP,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
)

SHARED_METHODS = (
    "_async_run_manual_callback_attempt",
    "_async_run_manual_recovery_transaction",
    "_async_finalize_recovery_entry",
)
AUTHORITIES = (
    "async_run_callback_identity_transaction",
    "async_run_callback_recovery_transaction",
)
ADAPTER_CLASS = "_LegacyCallbackContinuation"
FLOW_CLASS = "EybondLocalConfigFlow"
PN = "V001020SYN62344022"


def _config_flow_tree() -> ast.Module:
    return ast.parse(CONFIG_FLOW.read_text(encoding="utf-8"))


def _class_def(tree: ast.Module, name: str) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"class {name} not found")


def _method_def(cls: ast.ClassDef, name: str):
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"method {name} not found on {cls.name}")


def _called_bare_names(node: ast.AST) -> set[str]:
    names: set[str] = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name):
            names.add(sub.func.id)
    return names


def _attr_names(node: ast.AST) -> set[str]:
    return {
        sub.attr for sub in ast.walk(node) if isinstance(sub, ast.Attribute)
    }


# --------------------------------------------------------------------------- #
# Functional harness (real flow + real registry + patched authorities)         #
# --------------------------------------------------------------------------- #


def _make_flow() -> EybondLocalConfigFlow:
    flow = EybondLocalConfigFlow()
    flow.hass = _FakeHass()
    flow.context = {}
    return flow


class _RecordingRegistry(CallbackSessionRegistry):
    """The real registry, recording every ``release(owner)`` (slots-safe)."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.released_owners: list[str] = []

    def release(self, owner):  # type: ignore[override]
        self.released_owners.append(owner)
        return super().release(owner)


def _install_registry(flow, inventory=()):
    registry = _RecordingRegistry(
        sessions_source=lambda: tuple(dict(s) for s in inventory)
    )
    flow.hass.data.setdefault("eybond_local", {})[
        "callback_session_registry"
    ] = registry
    return registry


def _request() -> CallbackIdentityRequest:
    return CallbackIdentityRequest(
        server_ip="192.168.1.50",
        tcp_port=502,
        udp_port=58899,
        target_ip="192.168.1.77",
        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        owner_prefix="callback_verification",
    )


def _route() -> CallbackRecoveryRoute:
    return CallbackRecoveryRoute(
        bind_ip="192.168.1.50",
        trigger_target_ip="192.168.1.77",
        trigger_udp_port=58899,
        advertised_ha_host="192.168.1.50",
        advertised_ha_port=502,
        listener_port=502,
    )


def _settings() -> dict:
    return {
        CONF_SERVER_IP: "192.168.1.50",
        CONF_TCP_PORT: 502,
        CONF_UDP_PORT: 58899,
        CONF_COLLECTOR_IP: "192.168.1.77",
    }


@contextmanager
def _identity_returns(outcome):
    async def _fake(_hass, request, **_kwargs):
        return outcome

    with patch.object(
        config_flow_module, "async_run_callback_identity_transaction", new=_fake
    ):
        yield


@contextmanager
def _recovery_returns(outcome, recorder=None):
    async def _fake(**kwargs):
        if recorder is not None:
            recorder.append(kwargs)
        return outcome

    with patch.object(
        config_flow_module, "async_run_callback_recovery_transaction", new=_fake
    ):
        yield


def _certified(owner: str, *, session: str = "sess-A") -> CallbackIdentityOutcome:
    return CallbackIdentityOutcome(
        result=IDENTITY_OK,
        collector_pn=PN,
        session_id=session,
        handoff_owner=owner,
    )


SESSION = "sess-A"
TS = "2026-07-16T10:00:00+00:00"


def _prepared_callback_outcome(
    flow, *, owner: str = "callback_recovery:owner-R", session: str = SESSION
) -> tuple[CallbackSessionRegistry, RecoveryVerificationOutcome]:
    """A REAL prepared callback-verified outcome over a real registry.

    The registry claims + promotes + prepares the exact owner (as the recovery
    transaction would), so ``prepared_handoff_identity`` certifies it and the
    seam can adopt/verify it for real -- no proof is ever minted by hand.
    """

    registry = _install_registry(flow, [_wire_session(session, PN)])
    registry.claim_session(owner, session_id=session)
    registry.promote_claim_to_full_pn(owner, PN)
    assert registry.prepare_handoff(owner, PN)
    outcome = RecoveryVerificationOutcome(
        status=STATE_CALLBACK_VERIFIED,
        collector_pn=PN,
        new_session_id=session,
        callback_proof=CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.60:58899",
            advertised_ha_endpoint="198.51.100.7:48899",
            listener_port=18899,
        ),
        handoff_owner=owner,
    )
    return registry, outcome


def _valid_callback_outcome(
    owner: str = "callback_recovery:R", session: str = SESSION
) -> RecoveryVerificationOutcome:
    """A production-valid callback-verified outcome (no registry prep needed)."""

    return RecoveryVerificationOutcome(
        status=STATE_CALLBACK_VERIFIED,
        collector_pn=PN,
        new_session_id=session,
        callback_proof=CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.60:58899",
            advertised_ha_endpoint="198.51.100.7:48899",
            listener_port=18899,
        ),
        handoff_owner=owner,
    )


def _prepared_inbound_outcome(
    flow, *, owner: str = "callback_recovery:inb-R", session: str = SESSION
) -> tuple[CallbackSessionRegistry, RecoveryVerificationOutcome]:
    """A REAL prepared inbound-recovered outcome (autonomous reconnection)."""

    registry = _install_registry(flow, [_wire_session(session, PN)])
    registry.claim_session(owner, session_id=session)
    registry.promote_claim_to_full_pn(owner, PN)
    assert registry.prepare_handoff(owner, PN)
    outcome = RecoveryVerificationOutcome(
        status=STATE_INBOUND_RECOVERED,
        collector_pn=PN,
        new_session_id=session,
        inbound_proof=InboundRecoveryProof(
            method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            session_protocol="eybond_framed",
        ),
        handoff_owner=owner,
    )
    return registry, outcome


class CallbackContinuationArchitectureGuards(unittest.TestCase):
    def test_seam_contract_imports_no_upward_layer(self) -> None:
        tree = ast.parse(SEAM.read_text(encoding="utf-8"))
        segments: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                segments.update(node.module.split("."))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    segments.update(alias.name.split("."))
        offenders = segments & {"config_flow", "onboarding", "runtime"}
        self.assertEqual(offenders, set(), msg=str(offenders))

    def test_clean_interpreter_seam_import_loads_no_upward_module(self) -> None:
        code = (
            "import sys\n"
            f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
            "import custom_components.eybond_local.connection.callback_continuation\n"
            "up = {'onboarding', 'config_flow', 'runtime'}\n"
            "bad = sorted(m for m in sys.modules if "
            "m.startswith('custom_components.eybond_local.') and (up & set(m.split('.'))))\n"
            "print('BAD:' + ','.join(bad) if bad else 'CLEAN')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, cwd=str(REPO_ROOT)
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr[-800:])
        self.assertIn("CLEAN", result.stdout, msg=result.stdout)

    def test_authorities_called_only_inside_the_legacy_adapter(self) -> None:
        tree = _config_flow_tree()
        offenders: dict[str, set[str]] = {}
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            hit = _called_bare_names(node) & set(AUTHORITIES)
            if hit and node.name != ADAPTER_CLASS:
                offenders[node.name] = hit
        # No authority call at module scope either.
        module_calls = {
            sub.func.id
            for sub in tree.body
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
        } & set(AUTHORITIES)
        self.assertEqual(offenders, {}, msg=f"authorities called outside adapter: {offenders}")
        self.assertEqual(module_calls, set())

    def test_shared_methods_route_through_the_seam(self) -> None:
        flow_cls = _class_def(_config_flow_tree(), FLOW_CLASS)
        for name in SHARED_METHODS:
            method = _method_def(flow_cls, name)
            self.assertIn(
                "_callback_continuation",
                _attr_names(method),
                msg=f"{name} must route through self._callback_continuation",
            )
            direct = _called_bare_names(method) & set(AUTHORITIES)
            self.assertEqual(
                direct, set(), msg=f"{name} must not call an authority directly: {direct}"
            )

    def test_no_admission_branch_in_shared_callback_methods(self) -> None:
        flow_cls = _class_def(_config_flow_tree(), FLOW_CLASS)
        for name in SHARED_METHODS:
            method = _method_def(flow_cls, name)
            self.assertNotIn(
                "_admission_transaction",
                _attr_names(method),
                msg=f"2D.1 introduces no admission branch; {name} must not touch it",
            )

    def test_exactly_one_identity_and_one_recovery_authority(self) -> None:
        counts = {name: [] for name in AUTHORITIES}
        for path in sorted(PKG.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in counts:
                    counts[node.name].append(str(path.relative_to(PKG)))
        for name, where in counts.items():
            self.assertEqual(len(where), 1, msg=f"{name} defined in {where}")

    def test_legacy_private_cleanup_is_never_called_as_orchestration(self) -> None:
        # Every orchestration cleanup/adopt goes through the seam; these two
        # private helpers have zero direct `self.` call sites now.
        source = CONFIG_FLOW.read_text(encoding="utf-8")
        self.assertNotIn("self._release_unadopted_recovery_outcome()", source)
        self.assertNotIn("self._adopt_callback_recovery_outcome(", source)

    def test_admission_transaction_is_a_callback_continuation(self) -> None:
        # 2D.2: the transaction now IS a full CallbackContinuation (the neutral
        # transaction-backed implementation chosen for admission-origin flows).
        from custom_components.eybond_local.connection.admission_transaction import (
            CollectorAdmissionTransaction,
        )

        self.assertTrue(
            issubclass(CollectorAdmissionTransaction, CallbackContinuation)
        )
        self.assertEqual(CollectorAdmissionTransaction.__abstractmethods__, frozenset())

    def test_adapter_satisfies_the_neutral_contract(self) -> None:
        flow = _make_flow()
        self.assertIsInstance(flow._callback_continuation, CallbackContinuation)
        self.assertIs(type(flow._callback_continuation), config_flow_module._LegacyCallbackContinuation)

    def test_guarded_shared_methods_read_no_lifecycle_field_directly(self) -> None:
        # The corrective's core invariant: none of the shared identity/recovery/
        # result/finalize/terminal/route methods reads or mutates the eight
        # callback-continuation lifecycle fields directly -- all through the seam.
        fields = {
            "_verification_registry",
            "_verification_claim_owner",
            "_manual_verified_full_pn",
            "_manual_verified_session_id",
            "_manual_silent_offer",
            "_manual_recovery_outcome",
            "_recovery_terminal",
            "_callback_ownership_handed_off",
        }
        guarded = {
            "_async_run_manual_callback_attempt",
            "_async_run_manual_recovery_transaction",
            "async_step_manual_recovery_result",
            "async_step_manual_recovery_inbound_confirm",
            "async_step_manual_recovery_confirm",
            "async_step_manual_recovery_verify",
            "async_step_manual_recovery_failed",
            "async_step_manual_confirm",
            "_async_finalize_recovery_entry",
            "_create_entry_with_handoff",
            "_prepare_ownership_handoff",
            "_rollback_committed_handoff",
            "_async_route_after_manual_callback_success",
            "_async_route_after_manual_callback_failure",
            "_async_manual_bootstrap_retry",
        }
        flow_cls = _class_def(_config_flow_tree(), FLOW_CLASS)
        offenders = []
        for method in flow_cls.body:
            if (
                isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                and method.name in guarded
            ):
                for sub in ast.walk(method):
                    if (
                        isinstance(sub, ast.Attribute)
                        and sub.attr in fields
                        and isinstance(sub.value, ast.Name)
                        and sub.value.id == "self"
                    ):
                        offenders.append((method.name, sub.attr, sub.lineno))
        self.assertEqual(offenders, [], msg=f"direct lifecycle-field access: {offenders}")

    def test_terminal_coordinator_routes_ownership_through_the_seam(self) -> None:
        flow_cls = _class_def(_config_flow_tree(), FLOW_CLASS)
        coordinator = _method_def(flow_cls, "_create_entry_with_handoff")
        attrs = _attr_names(coordinator)
        for op in ("prepare_terminal", "commit_terminal", "rollback_terminal"):
            self.assertIn(
                op, attrs, msg=f"_create_entry_with_handoff must route through {op}"
            )

    def test_every_seam_member_has_a_production_caller(self) -> None:
        source = CONFIG_FLOW.read_text(encoding="utf-8")
        for member in sorted(CallbackContinuation.__abstractmethods__):
            self.assertIn(
                f"_callback_continuation.{member}",
                source,
                msg=f"seam member {member} has no production caller",
            )

    def test_shared_attempt_builds_request_from_identity_context(self) -> None:
        flow_cls = _class_def(_config_flow_tree(), FLOW_CLASS)
        method = _method_def(flow_cls, "_async_run_manual_callback_attempt")
        # The request is built from the seam's typed identity context...
        self.assertIn("identity_context", _attr_names(method))
        # ...and the attempt READS neither legacy identity-context field (the
        # declared-PN restore is a Store, which is allowed).
        ctx_fields = {"_verification_expected_pn", "_verification_old_session_id"}
        loads = [
            (sub.attr, sub.lineno)
            for sub in ast.walk(method)
            if isinstance(sub, ast.Attribute)
            and sub.attr in ctx_fields
            and isinstance(sub.value, ast.Name)
            and sub.value.id == "self"
            and isinstance(sub.ctx, ast.Load)
        ]
        self.assertEqual(loads, [], msg=f"attempt reads legacy identity-context: {loads}")

    def test_callback_reconfigure_cleanup_never_bypasses_the_seam(self) -> None:
        flow_cls = _class_def(_config_flow_tree(), FLOW_CLASS)
        methods = (
            "_async_create_manual_entry",
            "_async_create_entry_from_result",
            "_async_apply_reconfigure",
            "_async_run_manual_callback_attempt",
            "_async_run_manual_recovery_transaction",
        )
        offenders = []
        for name in methods:
            method = _method_def(flow_cls, name)
            for sub in ast.walk(method):
                if (
                    isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr == "_release_verification_claim"
                    and isinstance(sub.func.value, ast.Name)
                    and sub.func.value.id == "self"
                ):
                    offenders.append((name, sub.lineno))
        self.assertEqual(
            offenders,
            [],
            msg=f"callback/reconfigure cleanup must route through the seam: {offenders}",
        )


class CallbackContinuationBehaviorParity(unittest.IsolatedAsyncioTestCase):
    async def test_identity_certified_adopts_owner_pn_and_session(self) -> None:
        # (A success) a certified identity leaves the transaction's prepared owner
        # held as the flow claim, plus the certified full PN and session id.
        flow = _make_flow()
        registry = _install_registry(flow)
        with _identity_returns(_certified("callback_verification:owner-A")):
            outcome = await flow._callback_continuation.async_run_identity(_request())
        self.assertTrue(outcome.identity_certified)
        self.assertIs(flow._verification_registry, registry)
        self.assertEqual(flow._verification_claim_owner, "callback_verification:owner-A")
        self.assertEqual(flow._manual_verified_full_pn, PN)
        self.assertEqual(flow._manual_verified_session_id, "sess-A")
        self.assertIsNone(flow._manual_silent_offer)

    async def test_identity_non_certified_captures_offer_and_holds_nothing(self) -> None:
        # (A failure + B) no owner, no proof; only the typed silent bootstrap
        # offer is retained for the explicit continuation.
        flow = _make_flow()
        _install_registry(flow)
        offer = SilentSessionBootstrapOffer(session_id="sess-silent")
        non_certified = CallbackIdentityOutcome(
            result="callback_timeout", silent_bootstrap_offer=offer
        )
        with _identity_returns(non_certified):
            outcome = await flow._callback_continuation.async_run_identity(_request())
        self.assertFalse(outcome.identity_certified)
        self.assertIs(flow._manual_silent_offer, offer)
        self.assertEqual(flow._verification_claim_owner, "")
        self.assertIsNone(flow._verification_registry)
        self.assertEqual(flow._manual_verified_full_pn, "")

    async def test_identity_owner_to_recovery_owner_transition(self) -> None:
        # (E) identity owner OUT (released), recovery outcome IN (held).
        flow = _make_flow()
        registry = _install_registry(flow)
        with _identity_returns(_certified("callback_verification:owner-A")):
            await flow._callback_continuation.async_run_identity(_request())
        self.assertEqual(flow._verification_claim_owner, "callback_verification:owner-A")

        recovery = _valid_callback_outcome("callback_recovery:owner-R")
        seen: list = []
        with _recovery_returns(recovery, recorder=seen):
            result = await flow._callback_continuation.async_run_recovery(_route())
        self.assertIs(result, recovery)
        self.assertIs(flow._manual_recovery_outcome, recovery)
        # identity owner was released as part of the hand-over
        self.assertEqual(flow._verification_claim_owner, "")
        self.assertIsNone(flow._verification_registry)
        self.assertIn("callback_verification:owner-A", registry.released_owners)
        # the authority received THIS attempt's exact certified pn + session
        self.assertEqual(seen[0]["collector_pn"], PN)
        self.assertEqual(seen[0]["session_id"], "sess-A")

    async def test_release_unadopted_recovery_releases_exact_owner(self) -> None:
        # (F) a held-but-unadopted recovery outcome's exact owner is released and
        # the outcome is cleared.
        flow = _make_flow()
        registry = _install_registry(flow)
        flow._manual_recovery_outcome = _valid_callback_outcome("callback_recovery:held")
        flow._callback_continuation.release_unadopted_recovery()
        self.assertIsNone(flow._manual_recovery_outcome)
        self.assertEqual(registry.released_owners, ["callback_recovery:held"])

    async def test_adopt_recovery_delegates_to_legacy_impl(self) -> None:
        # (G) adoption delegates to the byte-for-byte legacy terminal adopter.
        flow = _make_flow()
        outcome = _valid_callback_outcome("callback_recovery:R")
        calls: list = []

        def _spy(o):
            calls.append(o)
            return True

        with patch.object(flow, "_adopt_callback_recovery_outcome", side_effect=_spy):
            adopted = flow._callback_continuation.adopt_recovery(outcome)
        self.assertTrue(adopted)
        self.assertEqual(calls, [outcome])

    async def test_release_exact_recovery_owner_delegates(self) -> None:
        flow = _make_flow()
        outcome = _valid_callback_outcome("callback_recovery:R")
        calls: list = []
        with patch.object(
            flow, "_release_exact_recovery_owner", side_effect=calls.append
        ):
            flow._callback_continuation.release_exact_recovery_owner(outcome)
        self.assertEqual(calls, [outcome])

    async def test_manual_callback_attempt_routes_identity_through_seam(self) -> None:
        # Routing: the shared submit path invokes the seam exactly once and never
        # the authority directly.
        flow = _make_flow()
        _install_registry(flow)
        seen: list = []

        async def _spy_identity(request):
            seen.append(request)
            return CallbackIdentityOutcome(result="callback_timeout")

        with patch.object(
            flow._callback_continuation, "async_run_identity", new=_spy_identity
        ):
            error = await flow._async_run_manual_callback_attempt(_settings())
        self.assertEqual(len(seen), 1)
        self.assertTrue(error)  # non-certified -> a typed error string
        self.assertEqual(seen[0].target_ip, "192.168.1.77")

    async def test_manual_recovery_run_routes_recovery_through_seam(self) -> None:
        flow = _make_flow()
        _install_registry(flow)
        flow._manual_config = _settings()
        flow._manual_verified_full_pn = PN
        flow._manual_verified_session_id = "sess-A"
        seen: list = []

        async def _spy_recovery(route):
            seen.append(route)
            flow._manual_recovery_outcome = RecoveryVerificationOutcome()
            return flow._manual_recovery_outcome

        with patch.object(
            flow._callback_continuation, "async_run_recovery", new=_spy_recovery
        ):
            await flow._async_run_manual_recovery_transaction()
        self.assertEqual(len(seen), 1)
        self.assertIsInstance(seen[0], CallbackRecoveryRoute)
        self.assertEqual(flow._manual_recovery_error, "")

    async def test_recovery_run_session_unavailable_returns_none(self) -> None:
        # No certified PN/session: the seam returns None and never touches the wire.
        flow = _make_flow()
        _install_registry(flow)
        flow._manual_config = _settings()
        flow._manual_verified_full_pn = ""
        flow._manual_verified_session_id = ""
        called: list = []

        async def _authority(**kwargs):
            called.append(kwargs)
            return _valid_callback_outcome()

        with patch.object(
            config_flow_module,
            "async_run_callback_recovery_transaction",
            new=_authority,
        ):
            result = await flow._callback_continuation.async_run_recovery(_route())
        self.assertIsNone(result)
        self.assertEqual(called, [])  # authority never invoked


class CallbackContinuationFailClosed(unittest.IsolatedAsyncioTestCase):
    """Real proofs only: no duck-typed/foreign outcome may adopt or mutate owners."""

    async def test_malformed_outcome_cannot_be_adopted_and_mutates_no_owner(
        self,
    ) -> None:
        import types

        flow = _make_flow()
        _install_registry(flow)
        # A foreign identity claim is present; a malformed adopt must not touch it.
        flow._verification_claim_owner = "callback_verification:pre"
        flow._verification_registry = flow._callback_session_registry()
        for bad in (
            object(),
            types.SimpleNamespace(handoff_owner="callback_recovery:x"),
            {"handoff_owner": "callback_recovery:x"},
            "not-an-outcome",
        ):
            with self.assertRaises(Exception):
                flow._callback_continuation.adopt_recovery(bad)
            # Zero owner mutation: the pre-existing claim stands, nothing committed.
            self.assertEqual(
                flow._verification_claim_owner, "callback_verification:pre"
            )
            self.assertFalse(flow._callback_ownership_handed_off)

    async def test_uncertifiable_real_outcome_is_not_adopted(self) -> None:
        # A production-valid outcome whose owner the registry cannot certify
        # (never prepared) must not adopt -- and must keep the previous claim.
        flow = _make_flow()
        _install_registry(flow)  # empty: the owner was never prepared here
        flow._verification_claim_owner = "callback_verification:pre"
        outcome = _valid_callback_outcome("callback_recovery:never-prepared")
        self.assertFalse(flow._callback_continuation.adopt_recovery(outcome))
        self.assertEqual(flow._verification_claim_owner, "callback_verification:pre")

    async def test_real_callback_proof_is_adopted(self) -> None:
        flow = _make_flow()
        _registry, outcome = _prepared_callback_outcome(flow)
        self.assertTrue(flow._callback_continuation.adopt_recovery(outcome))
        self.assertEqual(
            flow._verification_claim_owner, outcome.handoff_owner
        )
        ti = flow._callback_continuation.terminal_input
        self.assertEqual(ti.prepared_handoff_owner, outcome.handoff_owner)
        self.assertIsNotNone(ti.callback_proof)

    async def test_real_inbound_proof_is_adopted(self) -> None:
        flow = _make_flow()
        _registry, outcome = _prepared_inbound_outcome(flow)
        self.assertTrue(flow._callback_continuation.adopt_recovery(outcome))
        self.assertEqual(
            flow._verification_claim_owner, outcome.handoff_owner
        )
        self.assertIsNotNone(
            flow._callback_continuation.terminal_input.inbound_proof
        )

    async def test_prepare_terminal_foreign_prepared_owner_aborts_untouched(
        self,
    ) -> None:
        # A prepared recovery owner this flow never ADOPTED is refused before any
        # mutation, and the foreign owner's prepared handoff is not released.
        flow = _make_flow()
        registry, outcome = _prepared_callback_outcome(
            flow, owner="callback_recovery:foreign"
        )
        terminal_input = RecoveryTerminalInput.from_callback_transaction(outcome)
        # The flow adopted NOTHING (no _verification_claim_owner).
        decision = flow._callback_continuation.prepare_terminal(PN, terminal_input)
        self.assertEqual(decision.abort_reason, "recovery_ownership_unavailable")
        self.assertFalse(decision.owns)
        # Untouched: the foreign owner still certifies in the registry.
        self.assertTrue(
            registry.prepared_handoff_identity("callback_recovery:foreign", PN)
        )
        self.assertNotIn("callback_recovery:foreign", registry.released_owners)

    async def test_prepare_commit_terminal_with_adopted_recovery_owner(self) -> None:
        flow = _make_flow()
        _registry, outcome = _prepared_callback_outcome(flow)
        self.assertTrue(flow._callback_continuation.adopt_recovery(outcome))
        ti = flow._callback_continuation.terminal_input
        decision = flow._callback_continuation.prepare_terminal(PN, ti)
        self.assertIsInstance(decision, TerminalDecision)
        self.assertTrue(decision.owns)
        self.assertEqual(decision.abort_reason, "")
        # An adopted recovery owner commits AFTER the terminal returns.
        self.assertFalse(flow._callback_ownership_handed_off)
        flow._callback_continuation.commit_terminal()
        self.assertTrue(flow._callback_ownership_handed_off)

    async def test_rollback_terminal_releases_exactly_the_adopted_owner(self) -> None:
        flow = _make_flow()
        registry, outcome = _prepared_callback_outcome(flow)
        owner = outcome.handoff_owner
        self.assertTrue(flow._callback_continuation.adopt_recovery(outcome))
        ti = flow._callback_continuation.terminal_input
        self.assertTrue(flow._callback_continuation.prepare_terminal(PN, ti).owns)
        flow._callback_continuation.rollback_terminal()
        self.assertEqual(flow._verification_claim_owner, "")
        self.assertIn(owner, registry.released_owners)
        self.assertFalse(flow._callback_ownership_handed_off)


class CallbackContinuationExactTypeBoundary(unittest.IsolatedAsyncioTestCase):
    """Every public seam op enforces exact input type BEFORE any mutation."""

    async def test_identity_context_rejects_coercible_legacy_values_before_run(
        self,
    ) -> None:
        class _PnDuck:
            def __str__(self) -> str:
                return PN

        for field, bad in (
            ("_verification_expected_pn", _PnDuck()),
            ("_verification_expected_pn", PN.encode()),
            ("_verification_expected_pn", 123),
            ("_verification_expected_pn", None),
            ("_verification_expected_pn", f" {PN} "),
            ("_verification_old_session_id", _PnDuck()),
            ("_verification_old_session_id", b"sess-A"),
            ("_verification_old_session_id", 123),
            ("_verification_old_session_id", None),
            ("_verification_old_session_id", " sess-A "),
        ):
            with self.subTest(field=field, bad=bad):
                flow = _make_flow()
                _install_registry(flow)
                flow._manual_verified_full_pn = "PRE-PN"
                flow._verification_claim_owner = "callback_verification:pre"
                setattr(flow, field, bad)
                authority_calls: list = []

                async def _authority(_hass, _request):
                    authority_calls.append(_request)
                    return CallbackIdentityOutcome(result="callback_timeout")

                with patch.object(
                    config_flow_module,
                    "async_run_callback_identity_transaction",
                    new=_authority,
                ):
                    with self.assertRaises((TypeError, ValueError)):
                        await flow._async_run_manual_callback_attempt(_settings())

                self.assertEqual(authority_calls, [])
                self.assertEqual(flow._manual_verified_full_pn, "PRE-PN")
                self.assertEqual(
                    flow._verification_claim_owner, "callback_verification:pre"
                )

    async def test_release_exact_owner_rejects_foreign_types_untouched(self) -> None:
        # The corrective's required test: a foreign object carrying a REAL prepared
        # owner token must be rejected by exact type, releasing nothing.
        import types

        flow = _make_flow()
        owner = "callback_recovery:foreign"
        registry, outcome = _prepared_callback_outcome(flow, owner=owner)
        for bad in (
            object(),
            {"handoff_owner": owner},
            types.SimpleNamespace(handoff_owner=owner),
        ):
            with self.assertRaises(TypeError):
                flow._callback_continuation.release_exact_recovery_owner(bad)
        # The foreign owner is still prepared/owned; nothing was released.
        self.assertTrue(registry.prepared_handoff_identity(owner, PN))
        self.assertEqual(registry.released_owners, [])
        # A REAL outcome still releases exactly its own owner.
        flow._callback_continuation.release_exact_recovery_owner(outcome)
        self.assertEqual(registry.released_owners, [owner])

    async def test_async_run_identity_rejects_wrong_type_without_reset(self) -> None:
        flow = _make_flow()
        _install_registry(flow)
        flow._manual_verified_full_pn = "PRE-PN"
        flow._verification_claim_owner = "callback_verification:pre"
        for bad in (object(), {"target_ip": "x"}, "req"):
            with self.assertRaises(TypeError):
                await flow._callback_continuation.async_run_identity(bad)
            # Zero reset: the pre-existing state survives untouched.
            self.assertEqual(flow._manual_verified_full_pn, "PRE-PN")
            self.assertEqual(
                flow._verification_claim_owner, "callback_verification:pre"
            )

    async def test_async_run_recovery_rejects_wrong_type_without_release(self) -> None:
        flow = _make_flow()
        registry = _install_registry(flow)
        flow._manual_verified_full_pn = PN
        flow._manual_verified_session_id = SESSION
        flow._verification_claim_owner = "callback_verification:pre"
        with self.assertRaises(TypeError):
            await flow._callback_continuation.async_run_recovery(object())
        self.assertEqual(flow._verification_claim_owner, "callback_verification:pre")
        self.assertEqual(registry.released_owners, [])

    async def test_adopt_recovery_rejects_wrong_type_without_mutation(self) -> None:
        flow = _make_flow()
        _install_registry(flow)
        flow._verification_claim_owner = "callback_verification:pre"
        with self.assertRaises(TypeError):
            flow._callback_continuation.adopt_recovery(object())
        self.assertEqual(flow._verification_claim_owner, "callback_verification:pre")

    async def test_prepare_terminal_rejects_wrong_type(self) -> None:
        flow = _make_flow()
        _install_registry(flow)
        with self.assertRaises(TypeError):
            flow._callback_continuation.prepare_terminal(PN, object())


class TerminalDecisionConstruction(unittest.TestCase):
    def test_valid_decisions(self) -> None:
        self.assertEqual(TerminalDecision().abort_reason, "")
        self.assertFalse(TerminalDecision().owns)
        self.assertTrue(TerminalDecision(owns=True).owns)
        for reason in ("already_configured", "recovery_ownership_unavailable"):
            self.assertEqual(TerminalDecision(abort_reason=reason).abort_reason, reason)

    def test_unknown_abort_reason_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TerminalDecision(abort_reason="nope")

    def test_non_normalized_abort_reason_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TerminalDecision(abort_reason=" already_configured ")

    def test_non_str_abort_reason_rejected(self) -> None:
        with self.assertRaises(ValueError):
            TerminalDecision(abort_reason=None)

    def test_abort_and_owns_mutually_exclusive(self) -> None:
        with self.assertRaises(ValueError):
            TerminalDecision(abort_reason="already_configured", owns=True)

    def test_owns_must_be_exact_bool(self) -> None:
        with self.assertRaises(ValueError):
            TerminalDecision(owns=1)


class CallbackIdentityContextConstruction(unittest.TestCase):
    def test_empty_and_normalized_are_valid(self) -> None:
        ctx = CallbackIdentityContext(expected_pn="", old_session_id="")
        self.assertEqual(ctx.expected_pn, "")
        CallbackIdentityContext(expected_pn=PN, old_session_id="sess-1")

    def test_non_normalized_rejected(self) -> None:
        with self.assertRaises(ValueError):
            CallbackIdentityContext(expected_pn=" x ", old_session_id="")
        with self.assertRaises(ValueError):
            CallbackIdentityContext(expected_pn="", old_session_id="s ")

    def test_non_str_rejected(self) -> None:
        with self.assertRaises(TypeError):
            CallbackIdentityContext(expected_pn=None, old_session_id="")


if __name__ == "__main__":
    unittest.main()
