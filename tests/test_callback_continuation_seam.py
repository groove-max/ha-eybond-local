"""Guards and behavior tests for the neutral callback continuation boundary."""

from __future__ import annotations

import ast
from contextlib import contextmanager
from pathlib import Path
import sys
import types
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
TRANSACTION = PKG / "connection" / "admission_transaction.py"

# Install Home Assistant stubs before importing config_flow.
import test_config_flow as flow_scaffold  # noqa: E402,F401
from test_config_flow import _FakeHass, _wire_session  # noqa: E402

import custom_components.eybond_local.connection.admission_transaction as txn_module
from custom_components.eybond_local.config_flow import EybondLocalConfigFlow
from custom_components.eybond_local.connection.admission_transaction import (
    ManualCallbackContinuationTransaction,
)
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
    CallbackRecoveryProof,
)
from custom_components.eybond_local.connection.recovery.terminal import (
    RecoveryTerminalInput,
)
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
    RecoveryVerificationOutcome,
    STATE_CALLBACK_VERIFIED,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.const import (
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
)

PN = "V001020SYN62344022"
SESSION = "listener-18899-9"
TS = "2026-07-16T10:00:00+00:00"


class _RecordingRegistry(CallbackSessionRegistry):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.released_owners: list[str] = []

    def release(self, owner):  # type: ignore[override]
        self.released_owners.append(owner)
        return super().release(owner)


def _registry(inventory=()) -> _RecordingRegistry:
    return _RecordingRegistry(
        sessions_source=lambda: tuple(dict(item) for item in inventory)
    )


def _make_flow() -> EybondLocalConfigFlow:
    flow = EybondLocalConfigFlow()
    flow.hass = _FakeHass()
    flow.context = {}
    return flow


def _install_registry(flow, inventory=()) -> _RecordingRegistry:
    registry = _registry(inventory)
    flow.hass.data.setdefault("eybond_local", {})[
        "callback_session_registry"
    ] = registry
    return registry


def _request(*, expected_pn="", old_session_id="") -> CallbackIdentityRequest:
    return CallbackIdentityRequest(
        server_ip="192.168.1.50",
        tcp_port=18899,
        udp_port=58899,
        target_ip="192.168.1.77",
        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        expected_pn=expected_pn,
        old_session_id=old_session_id,
        owner_prefix="callback_verification",
    )


def _route() -> CallbackRecoveryRoute:
    return CallbackRecoveryRoute(
        bind_ip="192.168.1.50",
        trigger_target_ip="192.168.1.77",
        trigger_udp_port=58899,
        advertised_ha_host="192.168.1.50",
        advertised_ha_port=18899,
        listener_port=18899,
    )


def _certified(owner: str) -> CallbackIdentityOutcome:
    return CallbackIdentityOutcome(
        result=IDENTITY_OK,
        collector_pn=PN,
        session_id=SESSION,
        handoff_owner=owner,
        identity_source="fc2_parameter_2",
        session_protocol="eybond_framed",
    )


def _prepared_identity(registry, owner: str) -> None:
    registry.claim_session(owner, session_id=SESSION)
    registry.promote_claim_to_full_pn(owner, PN)
    assert registry.prepare_handoff(owner, PN)


def _recovery_outcome(owner="callback_recovery:R"):
    return RecoveryVerificationOutcome(
        status=STATE_CALLBACK_VERIFIED,
        collector_pn=PN,
        new_session_id=SESSION,
        callback_proof=CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.77:58899",
            advertised_ha_endpoint="192.168.1.50:18899",
            listener_port=18899,
        ),
        handoff_owner=owner,
    )


@contextmanager
def _identity_returns(outcome):
    async def _fake(_hass, _request, **_kwargs):
        return outcome

    with patch.object(
        txn_module, "async_run_callback_identity_transaction", new=_fake
    ):
        yield


@contextmanager
def _recovery_returns(outcome, recorder=None, registry=None):
    async def _fake(**kwargs):
        if recorder is not None:
            recorder.append(kwargs)
        if registry is not None and outcome.handoff_owner:
            registry.claim_session(outcome.handoff_owner, session_id=SESSION)
            registry.promote_claim_to_full_pn(outcome.handoff_owner, PN)
            assert registry.prepare_handoff(outcome.handoff_owner, PN)
        return outcome

    with patch.object(
        txn_module, "async_run_callback_recovery_transaction", new=_fake
    ):
        yield


class CallbackContinuationArchitectureGuards(unittest.TestCase):
    LEGACY_NAMES = {
        "_LegacyCallbackContinuation",
        "_verification_expected_pn",
        "_verification_old_session_id",
        "_verification_registry",
        "_verification_claim_owner",
        "_manual_verified_full_pn",
        "_manual_verified_session_id",
        "_manual_silent_offer",
        "_manual_recovery_outcome",
        "_recovery_terminal",
        "_callback_ownership_handed_off",
        "_release_verification_claim",
        "_adopt_callback_recovery_outcome",
    }

    def test_config_flow_contains_no_legacy_callback_state_or_adapter(self) -> None:
        source = CONFIG_FLOW.read_text(encoding="utf-8")
        for name in self.LEGACY_NAMES:
            self.assertNotIn(name, source, msg=f"legacy callback state remains: {name}")

    def test_default_flow_uses_neutral_manual_transaction(self) -> None:
        flow = _make_flow()
        self.assertIs(
            type(flow._callback_continuation),
            ManualCallbackContinuationTransaction,
        )
        self.assertIsInstance(flow._callback_continuation, CallbackContinuation)

    def test_both_sources_share_one_authority_implementation(self) -> None:
        tree = ast.parse(TRANSACTION.read_text(encoding="utf-8"))
        manual = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "ManualCallbackContinuationTransaction"
        )
        methods = {
            node.name
            for node in manual.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertFalse(
            methods
            & {
                "async_run_identity",
                "async_run_recovery",
                "adopt_recovery",
                "prepare_terminal",
                "commit_terminal",
                "rollback_terminal",
            }
        )

    def test_neutral_transaction_imports_no_upward_layer(self) -> None:
        tree = ast.parse(TRANSACTION.read_text(encoding="utf-8"))
        segments: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                segments.update(node.module.split("."))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    segments.update(alias.name.split("."))
        self.assertEqual(segments & {"config_flow", "onboarding", "runtime"}, set())

    def test_obsolete_pending_lifecycle_is_absent(self) -> None:
        self.assertFalse((PKG / "pending_collector.py").exists())
        self.assertFalse((PKG / "onboarding" / "pending_attempt.py").exists())
        source = CONFIG_FLOW.read_text(encoding="utf-8")
        self.assertNotIn("async_step_manual_create_pending", source)
        self.assertNotIn("PendingCollectorOptionsFlow", source)
        production = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(PKG.rglob("*.py"))
        )
        for removed in (
            "CONF_PENDING_ID",
            "CONF_PENDING_ADDRESS_HINT",
            "CONF_PENDING_LAST_ATTEMPT_RESULT",
            "PENDING_UNIQUE_ID_PREFIX",
            "PENDING_ATTEMPT_CALLBACK_TIMEOUT",
        ):
            self.assertNotIn(removed, production)

    def test_every_contract_member_has_a_production_caller(self) -> None:
        source = CONFIG_FLOW.read_text(encoding="utf-8")
        for member in CallbackContinuation.__abstractmethods__:
            self.assertIn(f"_callback_continuation.{member}", source)


class ManualCallbackTransactionBehavior(unittest.IsolatedAsyncioTestCase):
    async def test_identity_success_is_private_transaction_state(self) -> None:
        flow = _make_flow()
        registry = _install_registry(flow, [_wire_session(SESSION, PN)])
        owner = "callback_verification:A"
        _prepared_identity(registry, owner)
        with _identity_returns(_certified(owner)):
            outcome = await flow._callback_continuation.async_run_identity(_request())
        self.assertTrue(outcome.identity_certified)
        txn = flow._callback_continuation
        self.assertEqual(txn.certified_pn, PN)
        self.assertEqual(txn.certified_session_id, SESSION)
        self.assertEqual(txn.owner, owner)
        self.assertFalse(
            CallbackContinuationArchitectureGuards.LEGACY_NAMES & set(vars(flow))
        )

    async def test_non_certified_identity_keeps_only_typed_silent_offer(self) -> None:
        flow = _make_flow()
        _install_registry(flow)
        offer = SilentSessionBootstrapOffer(session_id="silent-A")
        with _identity_returns(
            CallbackIdentityOutcome(
                result="callback_timeout", silent_bootstrap_offer=offer
            )
        ):
            await flow._callback_continuation.async_run_identity(_request())
        self.assertIs(flow._callback_continuation.silent_bootstrap_offer, offer)
        self.assertEqual(flow._callback_continuation.certified_pn, "")
        self.assertFalse(flow._callback_continuation.holds_claim)

    async def test_identity_owner_transitions_to_exact_recovery_owner(self) -> None:
        flow = _make_flow()
        registry = _install_registry(flow, [_wire_session(SESSION, PN)])
        identity_owner = "callback_verification:A"
        _prepared_identity(registry, identity_owner)
        with _identity_returns(_certified(identity_owner)):
            await flow._callback_continuation.async_run_identity(_request())
        recovery = _recovery_outcome()
        seen: list[dict] = []
        with _recovery_returns(recovery, seen, registry):
            returned = await flow._callback_continuation.async_run_recovery(_route())
        self.assertIs(returned, recovery)
        self.assertIn(identity_owner, registry.released_owners)
        self.assertEqual(seen[0]["collector_pn"], PN)
        self.assertEqual(seen[0]["session_id"], SESSION)

    async def test_recovery_adoption_requires_exact_produced_object(self) -> None:
        flow = _make_flow()
        registry = _install_registry(flow, [_wire_session(SESSION, PN)])
        identity_owner = "callback_verification:A"
        _prepared_identity(registry, identity_owner)
        with _identity_returns(_certified(identity_owner)):
            await flow._callback_continuation.async_run_identity(_request())
        recovery = _recovery_outcome()
        with _recovery_returns(recovery, registry=registry):
            await flow._callback_continuation.async_run_recovery(_route())
        consumed = flow._callback_continuation.consume_recovery_outcome()
        self.assertIs(consumed, recovery)
        foreign = _recovery_outcome(owner="callback_recovery:foreign")
        with self.assertRaises(ValueError):
            flow._callback_continuation.adopt_recovery(foreign)
        self.assertTrue(flow._callback_continuation.adopt_recovery(recovery))
        decision = flow._callback_continuation.prepare_terminal(
            PN, flow._callback_continuation.terminal_input
        )
        self.assertEqual(decision, TerminalDecision(owns=True))

    def test_passive_inbound_claim_is_exact_and_terminal_safe(self) -> None:
        flow = _make_flow()
        registry = _install_registry(flow, [_wire_session(SESSION, PN)])
        flow._replace_manual_callback_continuation(expected_pn=PN)
        txn = flow._callback_continuation
        self.assertTrue(txn.adopt_passive_inbound_identity(PN, SESSION))
        self.assertEqual(txn.adopt_certified_pn(PN), PN)
        self.assertEqual(
            txn.prepare_terminal(PN, RecoveryTerminalInput.none()),
            TerminalDecision(owns=True),
        )
        txn.commit_terminal()
        txn.release_terminal_owner()
        self.assertTrue(registry.prepared_handoff_identity(txn.owner, PN))

    async def test_wrong_types_fail_before_owner_or_wire_mutation(self) -> None:
        flow = _make_flow()
        registry = _install_registry(flow)
        txn = flow._callback_continuation
        for bad in (object(), {}, types.SimpleNamespace()):
            with self.assertRaises(TypeError):
                await txn.async_run_identity(bad)
        self.assertFalse(txn.holds_claim)
        self.assertEqual(registry.released_owners, [])


class StrictModelConstruction(unittest.TestCase):
    def test_callback_identity_context_is_strict(self) -> None:
        self.assertEqual(CallbackIdentityContext("", "").expected_pn, "")
        for bad in (None, 1, b"x", object()):
            with self.assertRaises(TypeError):
                CallbackIdentityContext(bad, "")
        with self.assertRaises(ValueError):
            CallbackIdentityContext(f" {PN} ", "")

    def test_terminal_decision_is_closed_and_mutually_exclusive(self) -> None:
        self.assertEqual(TerminalDecision(), TerminalDecision(owns=False))
        self.assertTrue(TerminalDecision(owns=True).owns)
        for reason in ("already_configured", "recovery_ownership_unavailable"):
            self.assertEqual(TerminalDecision(abort_reason=reason).abort_reason, reason)
        for bad in ("unknown", " already_configured ", None):
            with self.assertRaises((TypeError, ValueError)):
                TerminalDecision(abort_reason=bad)
        with self.assertRaises(ValueError):
            TerminalDecision(abort_reason="already_configured", owns=True)


if __name__ == "__main__":
    unittest.main()
