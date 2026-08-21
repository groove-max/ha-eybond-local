"""Batch 2D.2 checkpoint 1 -- the transaction-backed callback continuation.

The ONE ``CollectorAdmissionTransaction`` owns the whole admission-origin attempt
(inbound -> callback identity -> callback recovery -> terminal) by implementing the
neutral ``CallbackContinuation`` contract over a SINGLE owner slot. These tests
drive the transaction directly against a real ``CallbackSessionRegistry`` with the
two authorities patched -- NO config_flow, NO Home Assistant stubs, NO network.
Every await is deadline-bounded so a hang fails fast instead of blocking.
"""

from __future__ import annotations

import ast
import asyncio
from contextlib import contextmanager
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PKG = REPO_ROOT / "custom_components" / "eybond_local"
CONFIG_FLOW = PKG / "config_flow.py"
ADMISSION_TXN = PKG / "connection" / "admission_transaction.py"

import custom_components.eybond_local.connection.admission_transaction as txn_module
from custom_components.eybond_local.connection.admission import (
    CollectorAdmissionRequest,
    ObservedCollectorSession,
)
from custom_components.eybond_local.connection.admission_transaction import (
    CollectorAdmissionTransaction,
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
    CONNECTION_STRATEGY_INBOUND,
)

PN = "V001020SYN62344022"
OLD_SESSION = "old-sess-1"
NEW_SESSION = "new-sess-1"
TS = "2026-07-16T10:00:00+00:00"
DEADLINE = 5.0


def _wire_session(session_id: str, pn: str, *, state: str = "identified") -> dict:
    return {
        "session_id": session_id,
        "peer_ip": "203.0.113.10",
        "listener_port": 18899,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": "eybond_framed",
        "collector_identity_source": "fc2_parameter_2",
    }


class _RecordingRegistry(CallbackSessionRegistry):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.released_owners: list[str] = []

    def release(self, owner):  # type: ignore[override]
        self.released_owners.append(owner)
        return super().release(owner)


def _registry(inventory=()):
    return _RecordingRegistry(
        sessions_source=lambda: tuple(dict(s) for s in inventory)
    )


def _observed() -> ObservedCollectorSession:
    return ObservedCollectorSession(
        collector_pn=PN,
        identity_source="fc2_parameter_2",
        session_id=OLD_SESSION,
        listener_port=18899,
        protocol_shape="eybond_framed",
        peer_hint="203.0.113.10",
    )


def _request(*, callback_route=None) -> CollectorAdmissionRequest:
    return CollectorAdmissionRequest(
        observed_session=_observed(),
        origin="passive_scan",
        callback_route=callback_route,
    )


def _identity_request(
    *, expected_pn=PN, old_session_id=OLD_SESSION, strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
) -> CallbackIdentityRequest:
    return CallbackIdentityRequest(
        server_ip="192.168.1.50",
        tcp_port=502,
        udp_port=58899,
        target_ip="192.168.1.77",
        strategy=strategy,
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
        advertised_ha_port=502,
        listener_port=18899,
    )


def _certified_identity(owner: str, *, session=NEW_SESSION, pn=PN) -> CallbackIdentityOutcome:
    return CallbackIdentityOutcome(
        result=IDENTITY_OK, collector_pn=pn, session_id=session, handoff_owner=owner
    )


def _prepared_recovery_outcome(registry, *, owner, session=NEW_SESSION, pn=PN):
    """A REAL callback-verified outcome with an owner prepared in ``registry``."""

    registry.claim_session(owner, session_id=session)
    registry.promote_claim_to_full_pn(owner, pn)
    assert registry.prepare_handoff(owner, pn)
    return RecoveryVerificationOutcome(
        status=STATE_CALLBACK_VERIFIED,
        collector_pn=pn,
        new_session_id=session,
        callback_proof=CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=pn,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.77:58899",
            advertised_ha_endpoint="192.168.1.50:502",
            listener_port=18899,
        ),
        handoff_owner=owner,
    )


@contextmanager
def _identity_returns(outcome):
    async def _fake(_hass, request, **_kw):
        return outcome

    with patch.object(
        txn_module, "async_run_callback_identity_transaction", new=_fake
    ):
        yield


@contextmanager
def _recovery_returns(outcome, recorder=None):
    async def _fake(**kwargs):
        if recorder is not None:
            recorder.append(kwargs)
        return outcome

    with patch.object(
        txn_module, "async_run_callback_recovery_transaction", new=_fake
    ):
        yield


async def _make_callback_ready(registry) -> CollectorAdmissionTransaction:
    """Reach CALLBACK_READY cleanly: inbound run fails (no registry -> no channels,
    no owner), then the explicit callback transition."""

    provider_result = [None]  # async_run's first lookup fails fast (no network)
    txn = CollectorAdmissionTransaction(
        _request(),
        registry_provider=lambda: provider_result[0],
        listener_host="0.0.0.0",
        hass_provider=lambda: types.SimpleNamespace(data={}),
    )
    await asyncio.wait_for(txn.async_run(), timeout=DEADLINE)
    assert txn.state == "failed"
    assert not txn.holds_claim
    provider_result[0] = registry
    txn.begin_callback_continuation()
    assert txn.state == "callback_ready"
    return txn


class TransactionCallbackContinuationBasics(unittest.IsolatedAsyncioTestCase):
    def test_is_callback_continuation_with_no_abstract_left(self) -> None:
        self.assertTrue(issubclass(CollectorAdmissionTransaction, CallbackContinuation))
        self.assertEqual(CollectorAdmissionTransaction.__abstractmethods__, frozenset())

    async def test_begin_callback_only_from_failed(self) -> None:
        txn = CollectorAdmissionTransaction(
            _request(), registry_provider=lambda: None, listener_host="0.0.0.0"
        )
        # READY -> fail-closed
        with self.assertRaises(RuntimeError):
            txn.begin_callback_continuation()
        self.assertEqual(txn.state, "ready")

    async def test_identity_context_is_from_transaction(self) -> None:
        txn = await _make_callback_ready(_registry())
        ctx = txn.identity_context
        self.assertEqual(ctx.expected_pn, PN)
        self.assertEqual(ctx.old_session_id, OLD_SESSION)

    async def test_active_scan_bootstraps_exact_observed_session_once(self) -> None:
        txn = CollectorAdmissionTransaction(
            _request(callback_route=_route()),
            registry_provider=lambda: _registry(),
            listener_host="0.0.0.0",
        )
        txn.begin_observed_callback_continuation()
        self.assertEqual(txn.state, "callback_ready")
        self.assertEqual(txn.identity_context.expected_pn, PN)
        self.assertEqual(txn.identity_context.old_session_id, "")
        intent = txn.observed_wire_probe_intent()
        self.assertEqual(intent.session_id, OLD_SESSION)
        self.assertEqual(intent.collector_pn, PN)

    async def test_active_scan_callback_requires_route_but_accepts_weak_exact_session(
        self,
    ) -> None:
        missing = CollectorAdmissionTransaction(
            _request(), registry_provider=lambda: _registry(), listener_host="0.0.0.0"
        )
        with self.assertRaises(RuntimeError):
            missing.begin_observed_callback_continuation()
        self.assertEqual(missing.state, "ready")

        weak = ObservedCollectorSession(
            collector_pn=PN[:14],
            identity_source="framed_heartbeat",
            session_id=OLD_SESSION,
            listener_port=18899,
            protocol_shape="eybond_framed",
        )
        transaction = CollectorAdmissionTransaction(
            CollectorAdmissionRequest(
                observed_session=weak, callback_route=_route()
            ),
            registry_provider=lambda: _registry(),
            listener_host="0.0.0.0",
        )
        transaction.begin_observed_callback_continuation()
        self.assertEqual(transaction.state, "callback_ready")
        intent = transaction.observed_wire_probe_intent()
        self.assertEqual(intent.session_id, OLD_SESSION)
        self.assertEqual(intent.collector_pn, PN[:14])
        self.assertEqual(intent.identity_source, "framed_heartbeat")


class TransactionIdentityPhase(unittest.IsolatedAsyncioTestCase):
    async def test_identity_exact_type_gate(self) -> None:
        txn = await _make_callback_ready(_registry())
        for bad in (object(), {"strategy": "x"}, "req"):
            with self.assertRaises(TypeError):
                await asyncio.wait_for(txn.async_run_identity(bad), timeout=DEADLINE)
            self.assertEqual(txn.state, "callback_ready")

    async def test_identity_context_match_gate_no_mutation(self) -> None:
        txn = await _make_callback_ready(_registry())
        bad_requests = [
            _identity_request(expected_pn="V000405SYN94677058"),  # foreign PN
            _identity_request(old_session_id="other-session"),  # wrong session
            _identity_request(strategy=CONNECTION_STRATEGY_INBOUND),  # wrong strategy
        ]
        for req in bad_requests:
            with self.assertRaises(ValueError):
                await asyncio.wait_for(txn.async_run_identity(req), timeout=DEADLINE)
            # Zero mutation: still callback-ready, no owner, no certified PN.
            self.assertEqual(txn.state, "callback_ready")
            self.assertFalse(txn.holds_claim)
            self.assertEqual(txn.certified_pn, "")

    async def test_identity_context_rejects_coercible_request_fields(self) -> None:
        class _Duck:
            def __init__(self, value):
                self._value = value

            def __str__(self):
                return self._value

        txn = await _make_callback_ready(_registry())
        calls = []

        async def _authority(_hass, request, **_kw):
            calls.append(request)
            return CallbackIdentityOutcome(result="callback_timeout")

        bad_requests = (
            _identity_request(expected_pn=_Duck(PN)),
            _identity_request(expected_pn=PN.encode()),
            _identity_request(expected_pn=f" {PN} "),
            _identity_request(old_session_id=_Duck(OLD_SESSION)),
            _identity_request(old_session_id=OLD_SESSION.encode()),
            _identity_request(old_session_id=f" {OLD_SESSION} "),
            _identity_request(strategy=_Duck(CONNECTION_STRATEGY_CALLBACK_ON_DEMAND)),
        )
        with patch.object(
            txn_module, "async_run_callback_identity_transaction", new=_authority
        ):
            for request in bad_requests:
                with self.subTest(request=request):
                    with self.assertRaises((TypeError, ValueError)):
                        await txn.async_run_identity(request)
        self.assertEqual(calls, [])
        self.assertEqual(txn.state, "callback_ready")

    async def test_identity_certified_adopts_single_owner_and_enriches(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await _make_callback_ready(registry)
        # A weak observation PN certifies to a full PN; enrichment stays inside.
        outcome = _certified_identity("callback_verification:id1")
        with _identity_returns(outcome):
            got = await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        self.assertTrue(got.identity_certified)
        self.assertEqual(txn.state, "identity_certified")
        self.assertEqual(txn.certified_pn, PN)
        self.assertTrue(txn.holds_claim)  # the ONE owner is the identity owner
        self.assertEqual(txn.identity_context.expected_pn, PN)  # enriched inside

    async def test_identity_non_certified_holds_no_owner(self) -> None:
        txn = await _make_callback_ready(_registry())
        offer = SilentSessionBootstrapOffer(session_id=NEW_SESSION)
        outcome = CallbackIdentityOutcome(
            result="callback_timeout", silent_bootstrap_offer=offer
        )
        with _identity_returns(outcome):
            got = await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        self.assertFalse(got.identity_certified)
        self.assertIs(txn.silent_bootstrap_offer, offer)
        self.assertFalse(txn.holds_claim)
        self.assertEqual(txn.state, "callback_ready")

    async def test_identity_cancel_cleans_up_before_raising(self) -> None:
        txn = await _make_callback_ready(_registry())

        async def _cancel(_hass, _req, **_kw):
            raise asyncio.CancelledError

        with patch.object(
            txn_module, "async_run_callback_identity_transaction", new=_cancel
        ):
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(
                    txn.async_run_identity(_identity_request()), timeout=DEADLINE
                )
        # No stranded RUNNING; no owner; retryable.
        self.assertEqual(txn.state, "callback_ready")
        self.assertFalse(txn.holds_claim)

    async def test_release_during_identity_cannot_resurrect_transaction(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await _make_callback_ready(registry)
        entered = asyncio.Event()
        resume = asyncio.Event()
        owner = "callback_verification:delayed"

        async def _authority(_hass, _request, **_kw):
            registry.claim_session(owner, session_id=NEW_SESSION)
            registry.promote_claim_to_full_pn(owner, PN)
            self.assertTrue(registry.prepare_handoff(owner, PN))
            entered.set()
            await resume.wait()
            return _certified_identity(owner)

        with patch.object(
            txn_module, "async_run_callback_identity_transaction", new=_authority
        ):
            task = asyncio.create_task(txn.async_run_identity(_identity_request()))
            await asyncio.wait_for(entered.wait(), timeout=DEADLINE)
            txn.release()
            resume.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=DEADLINE)

        self.assertEqual(txn.state, "closed")
        self.assertFalse(txn.holds_claim)
        self.assertIn(owner, registry.released_owners)
        self.assertEqual(registry.owner_for_pn(PN), "")


class TransactionRecoveryPhase(unittest.IsolatedAsyncioTestCase):
    async def _certified_txn(self, registry):
        txn = await _make_callback_ready(registry)
        with _identity_returns(_certified_identity("callback_verification:id1")):
            await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        return txn

    async def test_identity_owner_to_recovery_owner_transition(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await self._certified_txn(registry)
        outcome = _prepared_recovery_outcome(registry, owner="callback_recovery:rec1")
        seen: list = []
        with _recovery_returns(outcome, recorder=seen):
            got = await asyncio.wait_for(
                txn.async_run_recovery(_route()), timeout=DEADLINE
            )
        self.assertIs(got, outcome)
        self.assertIs(txn.recovery_outcome, outcome)
        self.assertEqual(txn.state, "recovery_held")
        # identity owner released as part of the hand-over
        self.assertIn("callback_verification:id1", registry.released_owners)
        # the authority got THIS attempt's exact certified pn + session
        self.assertEqual(seen[0]["collector_pn"], PN)
        self.assertEqual(seen[0]["session_id"], NEW_SESSION)

    async def test_recovery_session_unavailable_returns_none_without_wire(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await self._certified_txn(registry)
        txn._certified_session_id = ""  # simulate missing certified session
        called: list = []

        async def _authority(**kwargs):
            called.append(kwargs)
            return _prepared_recovery_outcome(registry, owner="x")

        with patch.object(
            txn_module, "async_run_callback_recovery_transaction", new=_authority
        ):
            got = await asyncio.wait_for(txn.async_run_recovery(_route()), timeout=DEADLINE)
        self.assertIsNone(got)
        self.assertEqual(called, [])

    async def test_recovery_not_runnable_before_identity(self) -> None:
        txn = await _make_callback_ready(_registry())
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(txn.async_run_recovery(_route()), timeout=DEADLINE)

    async def test_release_during_recovery_cleans_delayed_owner_and_stays_closed(
        self,
    ) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await self._certified_txn(registry)
        entered = asyncio.Event()
        resume = asyncio.Event()
        owner = "callback_recovery:delayed"
        outcome = _prepared_recovery_outcome(registry, owner=owner)

        async def _authority(**_kwargs):
            entered.set()
            await resume.wait()
            return outcome

        with patch.object(
            txn_module, "async_run_callback_recovery_transaction", new=_authority
        ):
            task = asyncio.create_task(txn.async_run_recovery(_route()))
            await asyncio.wait_for(entered.wait(), timeout=DEADLINE)
            txn.release()
            resume.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=DEADLINE)

        self.assertEqual(txn.state, "closed")
        self.assertFalse(txn.holds_claim)
        self.assertIn(owner, registry.released_owners)
        self.assertEqual(registry.owner_for_pn(PN), "")


class TransactionAdoptAndTerminal(unittest.IsolatedAsyncioTestCase):
    async def _held_recovery(self, registry, *, owner="callback_recovery:rec1"):
        txn = await _make_callback_ready(registry)
        with _identity_returns(_certified_identity("callback_verification:id1")):
            await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        outcome = _prepared_recovery_outcome(registry, owner=owner)
        with _recovery_returns(outcome):
            await asyncio.wait_for(txn.async_run_recovery(_route()), timeout=DEADLINE)
        return txn, outcome

    async def test_consume_then_adopt_only_the_produced_outcome(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        consumed = txn.consume_recovery_outcome()
        self.assertIs(consumed, outcome)
        self.assertEqual(txn.state, "recovery_consumed")
        # A foreign / separately-built valid outcome cannot be adopted.
        foreign = _prepared_recovery_outcome(
            _registry([_wire_session("z", PN)]), owner="callback_recovery:foreign"
        )
        with self.assertRaises(ValueError):
            txn.adopt_recovery(foreign)
        for bad in (object(), {"handoff_owner": "x"}):
            with self.assertRaises(TypeError):
                txn.adopt_recovery(bad)
        # The exact produced outcome adopts as THE single owner.
        self.assertTrue(txn.adopt_recovery(consumed))
        self.assertEqual(txn.state, "recovery_adopted")
        self.assertEqual(
            txn.terminal_input.prepared_handoff_owner, "callback_recovery:rec1"
        )

    async def test_release_exact_rejects_foreign_and_releases_own(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        foreign = _prepared_recovery_outcome(
            _registry([_wire_session("z", PN)]), owner="callback_recovery:foreign"
        )
        for bad in (object(), {"handoff_owner": "x"}, foreign):
            with self.assertRaises((TypeError, ValueError)):
                txn.release_exact_recovery_owner(bad)
        # foreign owner never released
        self.assertNotIn("callback_recovery:foreign", registry.released_owners)
        # the produced outcome releases exactly its own owner
        txn.consume_recovery_outcome()
        txn.release_exact_recovery_owner(outcome)
        self.assertIn("callback_recovery:rec1", registry.released_owners)

    async def test_terminal_prepare_commit_recovery_owner(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        txn.consume_recovery_outcome()
        self.assertTrue(txn.adopt_recovery(outcome))
        decision = txn.prepare_terminal(PN, txn.terminal_input)
        self.assertIsInstance(decision, TerminalDecision)
        self.assertTrue(decision.owns)
        self.assertFalse(txn.handed_off)  # commit is AFTER the terminal
        txn.commit_terminal()
        self.assertTrue(txn.handed_off)
        self.assertEqual(txn.state, "handed_off")

    async def test_terminal_rollback_releases_exactly_recovery_owner(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        txn.consume_recovery_outcome()
        self.assertTrue(txn.adopt_recovery(outcome))
        self.assertTrue(txn.prepare_terminal(PN, txn.terminal_input).owns)
        txn.rollback_terminal()
        self.assertIn("callback_recovery:rec1", registry.released_owners)
        self.assertFalse(txn.handed_off)
        self.assertEqual(txn.state, "closed")

    async def test_terminal_foreign_prepared_owner_aborts_untouched(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        # A prepared owner this transaction never ADOPTED (still RECOVERY_HELD).
        forged = RecoveryTerminalInput.from_callback_transaction(outcome)
        decision = txn.prepare_terminal(PN, forged)
        self.assertEqual(decision.abort_reason, "recovery_ownership_unavailable")
        self.assertFalse(decision.owns)

    async def test_single_owner_invariant_across_phases(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await _make_callback_ready(registry)
        # CALLBACK_READY: no owner
        self.assertFalse(txn.holds_claim)
        with _identity_returns(_certified_identity("callback_verification:id1")):
            await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        # IDENTITY_CERTIFIED: exactly the identity owner
        self.assertEqual(txn._owner, "callback_verification:id1")
        outcome = _prepared_recovery_outcome(registry, owner="callback_recovery:rec1")
        with _recovery_returns(outcome):
            await asyncio.wait_for(txn.async_run_recovery(_route()), timeout=DEADLINE)
        # RECOVERY_HELD: no owner slot held (recovery owner still in the outcome)
        self.assertEqual(txn._owner, "")
        txn.consume_recovery_outcome()
        self.assertTrue(txn.adopt_recovery(outcome))
        # RECOVERY_ADOPTED: exactly the recovery owner
        self.assertEqual(txn._owner, "callback_recovery:rec1")

    async def test_retry_identity_releases_previous_owner(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = await _make_callback_ready(registry)
        with _identity_returns(_certified_identity("callback_verification:id1")):
            await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        # Probe-again: a FULL new attempt releases the previous identity owner.
        with _identity_returns(_certified_identity("callback_verification:id2")):
            await asyncio.wait_for(
                txn.async_run_identity(_identity_request()), timeout=DEADLINE
            )
        self.assertIn("callback_verification:id1", registry.released_owners)
        self.assertEqual(txn._owner, "callback_verification:id2")

    async def test_retry_after_consuming_recovery_releases_produced_owner(
        self,
    ) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        self.assertIs(txn.consume_recovery_outcome(), outcome)
        with _identity_returns(CallbackIdentityOutcome(result="callback_timeout")):
            await txn.async_run_identity(_identity_request())
        self.assertIn(outcome.handoff_owner, registry.released_owners)
        self.assertEqual(registry.owner_for_pn(PN), "")
        self.assertEqual(txn.state, "callback_ready")

    async def test_close_after_consuming_recovery_releases_produced_owner(
        self,
    ) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn, outcome = await self._held_recovery(registry)
        self.assertIs(txn.consume_recovery_outcome(), outcome)
        txn.release_terminal_owner()
        txn.release_unadopted_recovery()
        self.assertIn(outcome.handoff_owner, registry.released_owners)
        self.assertEqual(registry.owner_for_pn(PN), "")
        self.assertEqual(txn.state, "closed")

    async def test_failed_inbound_transaction_refuses_passive_inbound_shortcut(
        self,
    ) -> None:
        txn = await _make_callback_ready(_registry())
        self.assertFalse(txn.adopt_passive_inbound_identity(PN, NEW_SESSION))
        self.assertEqual(txn.state, "callback_ready")
        self.assertFalse(txn.holds_claim)


class ManualCallbackContinuationBasics(unittest.IsolatedAsyncioTestCase):
    """Manual/Pending/reconfigure uses the same neutral callback lifecycle."""

    def _make(self, registry, *, expected_pn="", old_session_id=""):
        return ManualCallbackContinuationTransaction(
            CallbackIdentityContext(
                expected_pn=expected_pn,
                old_session_id=old_session_id,
            ),
            registry_provider=lambda: registry,
            listener_host="0.0.0.0",
            hass_provider=lambda: types.SimpleNamespace(data={}),
        )

    def test_is_concrete_callback_continuation_without_observed_request(self) -> None:
        txn = self._make(_registry())
        self.assertIsInstance(txn, CallbackContinuation)
        self.assertEqual(txn.state, "callback_ready")
        self.assertEqual(txn.identity_context.expected_pn, "")
        self.assertEqual(txn.identity_context.old_session_id, "")
        with self.assertRaises(RuntimeError):
            _ = txn.request

    async def test_pn_less_manual_identity_adopts_exact_transaction_owner(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        owner = "callback_verification:manual-A"
        registry.claim_session(owner, session_id=NEW_SESSION)
        registry.promote_claim_to_full_pn(owner, PN)
        self.assertTrue(registry.prepare_handoff(owner, PN))
        txn = self._make(registry)
        request = _identity_request(expected_pn="", old_session_id="")
        with _identity_returns(_certified_identity(owner)):
            outcome = await txn.async_run_identity(request)
        self.assertTrue(outcome.identity_certified)
        self.assertEqual(txn.certified_pn, PN)
        self.assertEqual(txn.certified_session_id, NEW_SESSION)
        self.assertEqual(txn.owner, owner)

    def test_manual_retry_restores_durable_declared_identity(self) -> None:
        txn = self._make(_registry(), expected_pn=PN[:14])
        txn._expected_pn = PN  # simulate same-attempt short->full enrichment
        context = txn.identity_context_for_attempt(PN[:14])
        self.assertEqual(context.expected_pn, PN[:14])

    def test_passive_inbound_claim_prepares_exact_terminal_handoff(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = self._make(registry, expected_pn=PN)
        self.assertTrue(txn.adopt_passive_inbound_identity(PN, NEW_SESSION))
        self.assertEqual(txn.adopt_certified_pn(PN), PN)
        decision = txn.prepare_terminal(PN, RecoveryTerminalInput.none())
        self.assertEqual(decision, TerminalDecision(owns=True))
        self.assertTrue(txn.handed_off)
        self.assertTrue(registry.prepared_handoff_identity(txn.owner, PN))
        txn.commit_terminal()
        txn.release_terminal_owner()
        self.assertTrue(registry.prepared_handoff_identity(txn.owner, PN))

    def test_passive_inbound_foreign_identity_is_zero_mutation(self) -> None:
        registry = _registry([_wire_session(NEW_SESSION, PN)])
        txn = self._make(registry, expected_pn=PN)
        foreign = "V000405SYN94677058"
        self.assertFalse(txn.adopt_passive_inbound_identity(foreign, NEW_SESSION))
        self.assertFalse(txn.holds_claim)
        self.assertEqual(registry.released_owners, [])


class AdmissionConvergenceArchitectureGuards(unittest.TestCase):
    """§J: one owner authority; no shared branching; no hand-across; neutral."""

    LEGACY_FIELDS = {
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
    }
    # The shared callback/recovery/terminal orchestration -- must be continuation-
    # driven, never branch on the admission transaction.
    SHARED_METHODS = (
        "_async_run_manual_callback_attempt",
        "_async_run_manual_recovery_transaction",
        "_async_finalize_recovery_entry",
        "async_step_manual_confirm",
        "async_step_manual_recovery_confirm",
        "async_step_manual_recovery_verify",
        "async_step_manual_recovery_result",
        "async_step_manual_recovery_inbound_confirm",
        "async_step_manual_recovery_failed",
        "_create_entry_with_handoff",
        "_async_route_after_manual_callback_success",
        "_async_route_after_manual_callback_failure",
        "_async_manual_inbound_observe",
        "_async_create_manual_entry",
    )
    BRIDGE = "async_step_verify_connection_manual_callback"

    def _flow_methods(self):
        methods = {}
        paths = (
            PKG / "config_flow.py",
            PKG / "config_entry.py",
            *sorted((PKG / "flows" / "config").glob("*.py")),
        )
        for path in paths:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for flow in (node for node in tree.body if isinstance(node, ast.ClassDef)):
                methods.update(
                    {
                        method.name: method
                        for method in flow.body
                        if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                    }
                )
        return methods

    def test_shared_methods_never_branch_on_admission_transaction(self) -> None:
        methods = self._flow_methods()
        offenders = []
        for name in self.SHARED_METHODS:
            method = methods[name]
            for sub in ast.walk(method):
                if (
                    isinstance(sub, ast.Attribute)
                    and sub.attr == "_admission_transaction"
                ):
                    offenders.append((name, sub.lineno))
        self.assertEqual(
            offenders, [], msg=f"shared method branches on admission txn: {offenders}"
        )

    def test_shared_methods_never_touch_legacy_lifecycle_fields(self) -> None:
        methods = self._flow_methods()
        offenders = []
        for name in self.SHARED_METHODS:
            for sub in ast.walk(methods[name]):
                if isinstance(sub, ast.Attribute) and sub.attr in self.LEGACY_FIELDS:
                    offenders.append((name, sub.attr, sub.lineno))
        self.assertEqual(
            offenders,
            [],
            msg=f"shared method touches legacy callback lifecycle: {offenders}",
        )

    def test_bridge_writes_no_legacy_lifecycle_field(self) -> None:
        bridge = self._flow_methods()[self.BRIDGE]
        writes = [
            (sub.attr, sub.lineno)
            for sub in ast.walk(bridge)
            if isinstance(sub, ast.Attribute)
            and sub.attr in self.LEGACY_FIELDS
            and isinstance(sub.ctx, ast.Store)
        ]
        self.assertEqual(writes, [], msg=f"bridge hand-across remains: {writes}")

    def test_bridge_does_not_close_the_transaction(self) -> None:
        bridge = self._flow_methods()[self.BRIDGE]
        calls = {
            sub.func.attr
            for sub in ast.walk(bridge)
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
        }
        self.assertNotIn("async_close", calls)
        self.assertIn("begin_callback_continuation", calls)

    def test_source_boundary_selects_the_transaction_continuation(self) -> None:
        methods = self._flow_methods()
        begin = methods["_async_begin_collector_admission"]
        # `self._callback_continuation = transaction/<txn>` assignment present.
        assigns = [
            sub
            for sub in ast.walk(begin)
            if isinstance(sub, ast.Assign)
            for t in sub.targets
            if isinstance(t, ast.Attribute) and t.attr == "_callback_continuation"
        ]
        self.assertTrue(assigns, msg="source boundary must select the continuation")

    def test_exactly_one_identity_and_one_recovery_authority(self) -> None:
        names = {
            "async_run_callback_identity_transaction": [],
            "async_run_callback_recovery_transaction": [],
        }
        for path in sorted(PKG.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                if (
                    isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name in names
                ):
                    names[node.name].append(str(path.relative_to(PKG)))
        for name, where in names.items():
            self.assertEqual(len(where), 1, msg=f"{name} defined in {where}")

    def test_admission_transaction_defines_no_duplicate_authority(self) -> None:
        # It REUSES the authorities/verifier/proofs by import -- it never defines a
        # second matcher / verifier / recovery engine / handoff algorithm.
        tree = ast.parse(ADMISSION_TXN.read_text(encoding="utf-8"))
        classes = [n.name for n in tree.body if isinstance(n, ast.ClassDef)]
        self.assertEqual(
            classes,
            [
                "CollectorAdmissionTransaction",
                "ManualCallbackContinuationTransaction",
            ],
        )
        manual = next(
            n
            for n in tree.body
            if isinstance(n, ast.ClassDef)
            and n.name == "ManualCallbackContinuationTransaction"
        )
        # The manual specialization may define only source-policy adapters.  The
        # identity/recovery/terminal authorities stay inherited from the ONE
        # transaction implementation above.
        manual_methods = {
            n.name
            for n in manual.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertFalse(
            manual_methods
            & {
                "async_run_identity",
                "async_run_recovery",
                "adopt_recovery",
                "prepare_terminal",
                "commit_terminal",
                "rollback_terminal",
            }
        )
        funcs = [
            n.name
            for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        for banned in (
            "async_run_callback_identity_transaction",
            "async_run_callback_recovery_transaction",
        ):
            self.assertNotIn(banned, funcs)

    def test_admission_transaction_imports_no_upward_layer(self) -> None:
        tree = ast.parse(ADMISSION_TXN.read_text(encoding="utf-8"))
        segments: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                segments.update(node.module.split("."))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    segments.update(alias.name.split("."))
        self.assertEqual(
            segments & {"config_flow", "onboarding", "runtime"}, set()
        )


if __name__ == "__main__":
    unittest.main()
