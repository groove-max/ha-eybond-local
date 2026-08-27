"""Unit tests for the neutral CollectorAdmissionTransaction (Batch 2B).

Short and HA-free: the verifier, restart channel and silent probe are patched, so
these exercise the transaction's OWN orchestration -- strict input, working-PN
enrichment, exact session resolution, claim/owner lifecycle, retry, cancel and
the prepare/rollback/release handoff -- with a fake registry. Synthetic PNs only.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.admission import (
    CollectorAdmissionRequest,
    ObservedCollectorSession,
)
from custom_components.eybond_local.connection import admission_transaction as atm
from custom_components.eybond_local.connection.admission_transaction import (
    CollectorAdmissionTransaction,
    HANDOFF_ALREADY_CONFIGURED,
    HANDOFF_NOT_PREPARED,
    HANDOFF_OK,
)
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
    STATE_INBOUND_VERIFIED,
    InboundRecoveryOutcome,
)
from custom_components.eybond_local.connection.recovery_contract import (
    InboundRecoveryProof,
)

FULL_PN = "V001020SYN62344022"
SHORT_PN = FULL_PN[:14]
OTHER_PN = "V000405SYN94677058"
S1 = "listener-8899-1"
S2 = "listener-8899-2"


def _observed(*, pn=SHORT_PN, source="framed_heartbeat", session_id=S1, port=8899):
    return ObservedCollectorSession(
        collector_pn=pn,
        identity_source=source,
        session_id=session_id,
        listener_port=port,
        peer_hint="203.0.113.10",
    )


def _request(**kw):
    return CollectorAdmissionRequest(observed_session=_observed(**kw), origin="passive_scan")


def _route_request(**kw):
    observed = _observed(**kw)
    return CollectorAdmissionRequest(
        observed_session=observed,
        origin="scan_selected_route",
        callback_route=CallbackRecoveryRoute(
            bind_ip="192.0.2.10",
            trigger_target_ip="203.0.113.10",
            trigger_udp_port=58899,
            advertised_ha_host="192.0.2.10",
            advertised_ha_port=observed.listener_port,
            listener_port=observed.listener_port,
        ),
    )


def _verified_outcome(pn=FULL_PN):
    proof = InboundRecoveryProof(
        method="reboot_reconnect_no_trigger",
        collector_pn=pn,
        identity_source="at_dtupn",
        verified_at="2026-07-20T00:00:00+00:00",
        session_protocol="eybond_framed",
    )
    return InboundRecoveryOutcome(
        status=STATE_INBOUND_VERIFIED, collector_pn=pn, new_session_id=S2, proof=proof
    )


class _FakeRegistry:
    def __init__(self, *, current_session=None, claim_raises=False):
        self.claims: dict[str, str] = {}
        self.released: list[str] = []
        self.prepared: dict[str, str] = {}
        self._current = current_session
        self._claim_raises = claim_raises
        self.resolve_calls: list[tuple[str, bool]] = []
        self.prepare_calls = 0

    def current_session_for_pn(self, pn, *, require_exact):
        self.resolve_calls.append((pn, require_exact))
        return self._current

    def claim_session(self, owner, *, session_id):
        if self._claim_raises:
            raise ValueError("already owned")
        self.claims[owner] = session_id

    def claimed_session_id(self, owner):
        return self.claims.get(owner, "")

    def promote_claim_to_full_pn(self, owner, pn):
        if owner in self.claims:
            self.claims[owner] = self.claims[owner]

    def retarget_claim_to_reconnected_session(self, owner, session_id):
        self.claims[owner] = session_id
        return True

    def session_handle_for_claimed_session(self, owner):
        return None

    def observed_sessions_per_socket(self):
        return ()

    def release(self, owner):
        self.released.append(owner)
        self.claims.pop(owner, None)
        self.prepared.pop(owner, None)

    def prepare_handoff(self, owner, pn):
        self.prepare_calls += 1
        if owner in self.claims:
            self.prepared[owner] = pn
            return True
        return False

    def prepared_handoff_identity(self, owner, pn):
        return pn if owner in self.prepared else ""


class _FakeChannel:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = 0

    async def async_close(self):
        self.closed += 1

    async def async_probe_identity(self):
        return ""


class _FakeProbe:
    opened = 0
    closed = 0

    def __init__(self, **kwargs):
        pass

    async def async_open(self):
        type(self).opened += 1

    async def async_close(self):
        type(self).closed += 1


class _FakeVerifier:
    """Class-level configuration so each test sets the outcome/behavior."""

    outcome: object = None
    behavior = "return"  # "return" | "hang" | "cancel"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def async_verify(self):
        if type(self).behavior == "hang":
            await asyncio.Event().wait()
        if type(self).behavior == "cancel":
            raise asyncio.CancelledError()
        return type(self).outcome


class _Ledger:
    def snapshot_generation(self):
        return 0


def _install_patches():
    """Patch the transaction's verifier/channel/probe/ledger/projection."""

    _FakeProbe.opened = 0
    _FakeProbe.closed = 0
    return [
        patch.object(atm, "InboundRecoveryVerifier", _FakeVerifier),
        patch.object(atm, "ObservedSessionRestartChannel", _FakeChannel),
        patch.object(atm, "get_callback_trigger_ledger", lambda: _Ledger()),
        patch.object(atm, "registry_sessions_projection", lambda registry: (lambda: ())),
        patch(
            "custom_components.eybond_local.collector.silent_session_probe."
            "SilentSessionIdentityProbeChannel",
            _FakeProbe,
        ),
    ]


def _make(request, *, registry, policy=None):
    return CollectorAdmissionTransaction(
        request,
        registry_provider=lambda: registry,
        listener_host="0.0.0.0",
        policy_provider=policy,
    )


class TransactionInputAndProperties(unittest.TestCase):
    def test_strict_input_rejects_duck_request(self):
        duck = SimpleNamespace(observed_session=_observed(), origin="x")
        with self.assertRaises(TypeError):
            _make(duck, registry=_FakeRegistry())

    def test_read_only_surface(self):
        req = _request(pn=FULL_PN, source="at_dtupn")
        txn = _make(req, registry=_FakeRegistry())
        self.assertIs(txn.request, req)
        self.assertEqual(txn.expected_pn, FULL_PN)
        self.assertEqual(txn.peer_hint, "203.0.113.10")
        self.assertIsNone(txn.outcome)
        self.assertFalse(txn.verified)
        self.assertFalse(txn.holds_claim)
        self.assertFalse(txn.handed_off)
        self.assertEqual(txn.failure_reason, "")
        # No proof yet -> the terminal input is the explicit none.
        self.assertFalse(txn.terminal_input.has_proof)


class TransactionRun(unittest.IsolatedAsyncioTestCase):
    async def _run(self, txn):
        for p in _install_patches():
            p.start()
        try:
            await txn.async_run()
        finally:
            patch.stopall()

    async def test_success_holds_claim_and_exposes_typed_proof(self):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        await self._run(txn)
        self.assertTrue(txn.verified)
        self.assertTrue(txn.holds_claim)  # SUCCESS holds the claim
        self.assertEqual(reg.released, [])  # not released on success
        self.assertTrue(txn.terminal_input.has_proof)
        self.assertEqual(txn.terminal_input.collector_pn, FULL_PN)
        self.assertEqual(_FakeProbe.opened, 1)
        self.assertEqual(_FakeProbe.closed, 1)  # probe closed in finally

    async def test_failure_releases_owner_and_closes_channels(self):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = InboundRecoveryOutcome(
            failure_reason="inbound_reconnect_timeout", collector_pn=FULL_PN
        )
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        await self._run(txn)
        self.assertFalse(txn.verified)
        self.assertEqual(txn.failure_reason, "inbound_reconnect_timeout")
        self.assertFalse(txn.holds_claim)
        self.assertEqual(len(reg.released), 1)  # owner released on failure
        self.assertEqual(_FakeProbe.closed, 1)

    async def test_claim_conflict_is_typed_failure_no_owner(self):
        _FakeVerifier.behavior = "return"
        reg = _FakeRegistry(
            current_session=SimpleNamespace(session_id=S1), claim_raises=True
        )
        txn = _make(_request(), registry=reg)
        await self._run(txn)
        self.assertFalse(txn.verified)
        self.assertEqual(txn.failure_reason, atm.FAILURE_SESSION_CLAIMED)
        self.assertFalse(txn.holds_claim)

    async def test_enrichment_adopts_full_pn_and_retry_resolves_it_exactly(self):
        # First run: weak short-PN observation, verification FAILS but the outcome
        # carries the strong FULL PN -> transaction enriches its working PN.
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = InboundRecoveryOutcome(
            failure_reason="inbound_reconnect_timeout", collector_pn=FULL_PN
        )
        reg1 = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(pn=SHORT_PN, source="framed_heartbeat"), registry=reg1)
        await self._run(txn)
        self.assertEqual(txn.expected_pn, FULL_PN)  # enriched
        self.assertEqual(
            txn.request.observed_session.collector_pn, SHORT_PN
        )  # observation immutable

        # Retry: the replacement full-PN S2 exists; the transaction re-resolves by
        # the ENRICHED full PN under the weak (exact) rule.
        txn.reset_for_retry()
        self.assertFalse(txn.holds_claim)  # old claim released before retry
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        reg2 = _FakeRegistry(current_session=SimpleNamespace(session_id=S2))
        txn._registry_provider = lambda: reg2
        await self._run(txn)
        self.assertTrue(txn.verified)
        # Resolved by the FULL PN (enriched), require_exact True (weak source).
        self.assertEqual(reg2.resolve_calls, [(FULL_PN, True)])
        self.assertEqual(reg2.claims and next(iter(reg2.claims.values())), S2)

    async def test_cancel_closes_channels_and_releases_owner(self):
        _FakeVerifier.behavior = "hang"
        _FakeVerifier.outcome = None
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            task = asyncio.ensure_future(txn.async_run())
            await asyncio.sleep(0.02)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            patch.stopall()
        # Channels closed and owner released before the claim went away.
        self.assertEqual(_FakeProbe.closed, 1)
        self.assertFalse(txn.holds_claim)
        self.assertEqual(len(reg.released), 1)


class TransactionHandoff(unittest.IsolatedAsyncioTestCase):
    async def _run_success(self, reg):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            await txn.async_run()
        finally:
            patch.stopall()
        return txn

    async def test_prepare_handoff_success_holds_claim_for_setup(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = await self._run_success(reg)
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_OK)
        self.assertTrue(txn.handed_off)
        # release() is a no-op once handed off (setup completes it).
        txn.release()
        self.assertNotIn(txn._owner, reg.released)

    async def test_prepare_handoff_not_prepared_when_no_claim(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        # Never ran -> no claim.
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_NOT_PREPARED)

    async def test_prepare_handoff_uncertifiable_releases_and_refuses(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = await self._run_success(reg)
        # Make certification return empty -> refuse + release.
        with patch.object(reg, "prepared_handoff_identity", lambda o, p: ""):
            self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_ALREADY_CONFIGURED)
        self.assertFalse(txn.handed_off)
        self.assertFalse(txn.holds_claim)

    async def test_rollback_handoff_releases_committed_owner(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = await self._run_success(reg)
        owner = txn._owner
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_OK)
        txn.rollback_handoff()
        self.assertFalse(txn.handed_off)
        self.assertIn(owner, reg.released)


class _RaisingCtorChannel:
    def __init__(self, **kwargs):
        raise RuntimeError("channel ctor boom")


class _CloseTrackingChannel:
    instances: list = []

    def __init__(self, **kwargs):
        self.closed = 0
        type(self).instances.append(self)

    async def async_close(self):
        self.closed += 1

    async def async_probe_identity(self):
        return ""


class _RaisingCtorProbe:
    def __init__(self, **kwargs):
        raise RuntimeError("probe ctor boom")


class _RaisingOpenProbe:
    instances: list = []

    def __init__(self, **kwargs):
        self.closed = 0
        type(self).instances.append(self)

    async def async_open(self):
        raise RuntimeError("async_open boom")

    async def async_close(self):
        self.closed += 1


class _BlockingCloseProbe:
    entered: asyncio.Event
    release: asyncio.Event
    instances: list = []

    def __init__(self, **kwargs):
        self.closed = 0
        type(self).instances.append(self)

    async def async_open(self):
        pass

    async def async_close(self):
        type(self).entered.set()
        await type(self).release.wait()
        self.closed += 1


class _LookupRaisingRegistry(_FakeRegistry):
    def current_session_for_pn(self, pn, *, require_exact):
        raise RuntimeError("lookup boom")


class TransactionLifecycleHardening(unittest.IsolatedAsyncioTestCase):
    """Blocker 1/2/3: handoff needs a proof, cleanup covers post-claim failures,
    lifecycle methods are state-safe."""

    # A -------------------------------------------------------------------
    async def test_prepare_during_running_is_refused_with_zero_registry_mutation(self):
        _FakeVerifier.behavior = "hang"
        _FakeVerifier.outcome = None
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            task = asyncio.ensure_future(txn.async_run())
            # Wait until the claim exists (RUNNING).
            for _ in range(50):
                if txn.holds_claim:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(txn.holds_claim)
            self.assertEqual(txn.state, "running")
            # prepare_handoff while RUNNING -> NOT_PREPARED, no registry call.
            self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_NOT_PREPARED)
            self.assertEqual(reg.prepare_calls, 0)
            self.assertFalse(txn.handed_off)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            patch.stopall()
        self.assertFalse(txn.holds_claim)
        self.assertEqual(len(reg.released), 1)
        self.assertEqual(reg.prepared, {})

    # B -------------------------------------------------------------------
    async def test_channel_ctor_failure_after_claim_releases_owner(self):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        _FakeProbe.opened = 0
        with patch.object(atm, "ObservedSessionRestartChannel", _RaisingCtorChannel), \
             patch(
                 "custom_components.eybond_local.collector.silent_session_probe."
                 "SilentSessionIdentityProbeChannel", _FakeProbe):
            await txn.async_run()
        self.assertEqual(txn.failure_reason, "verification_error")
        self.assertFalse(txn.verified)
        self.assertFalse(txn.holds_claim)
        self.assertEqual(len(reg.released), 1)
        self.assertEqual(txn.state, "failed")

    # C -------------------------------------------------------------------
    async def test_probe_ctor_failure_closes_channel_and_releases_owner(self):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        _CloseTrackingChannel.instances = []
        with patch.object(atm, "ObservedSessionRestartChannel", _CloseTrackingChannel), \
             patch(
                 "custom_components.eybond_local.collector.silent_session_probe."
                 "SilentSessionIdentityProbeChannel", _RaisingCtorProbe):
            await txn.async_run()
        self.assertEqual(txn.failure_reason, "verification_error")
        self.assertEqual(_CloseTrackingChannel.instances[0].closed, 1)  # channel closed
        self.assertFalse(txn.holds_claim)
        self.assertEqual(len(reg.released), 1)

    # D -------------------------------------------------------------------
    async def test_async_open_failure_closes_probe_and_channel(self):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        _CloseTrackingChannel.instances = []
        _RaisingOpenProbe.instances = []
        with patch.object(atm, "ObservedSessionRestartChannel", _CloseTrackingChannel), \
             patch(
                 "custom_components.eybond_local.collector.silent_session_probe."
                 "SilentSessionIdentityProbeChannel", _RaisingOpenProbe):
            await txn.async_run()
        self.assertEqual(txn.failure_reason, "verification_error")
        self.assertEqual(_CloseTrackingChannel.instances[0].closed, 1)  # channel closed
        self.assertEqual(_RaisingOpenProbe.instances[0].closed, 1)  # probe closed
        self.assertFalse(txn.holds_claim)
        self.assertEqual(len(reg.released), 1)

    # E -------------------------------------------------------------------
    async def test_prepare_before_run_and_after_failed_makes_no_mutation(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        # Before run.
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_NOT_PREPARED)
        # After a FAILED run.
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = InboundRecoveryOutcome(
            failure_reason="restart_not_confirmed", collector_pn=FULL_PN
        )
        for p in _install_patches():
            p.start()
        try:
            await txn.async_run()
        finally:
            patch.stopall()
        self.assertEqual(txn.state, "failed")
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_NOT_PREPARED)
        self.assertEqual(reg.prepare_calls, 0)

    # F -------------------------------------------------------------------
    async def test_prepare_succeeds_once_and_is_idempotent_same_pn(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            await txn.async_run()
        finally:
            patch.stopall()
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_OK)
        self.assertEqual(reg.prepare_calls, 1)
        # Repeated same-identity prepare -> idempotent, no second registry call.
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_OK)
        self.assertEqual(reg.prepare_calls, 1)

    # G -------------------------------------------------------------------
    async def test_foreign_pn_prepare_refused_before_mutation(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            await txn.async_run()
        finally:
            patch.stopall()
        self.assertEqual(txn.prepare_handoff(OTHER_PN), HANDOFF_NOT_PREPARED)
        self.assertEqual(reg.prepare_calls, 0)  # no registry mutation
        self.assertTrue(txn.holds_claim)  # original claim not rebound/released
        self.assertEqual(txn.state, "verified")

    # H -------------------------------------------------------------------
    async def test_release_after_handoff_is_noop_then_rollback_still_releases(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            await txn.async_run()
        finally:
            patch.stopall()
        owner = txn._owner
        self.assertEqual(txn.prepare_handoff(FULL_PN), HANDOFF_OK)
        # release() after HANDED_OFF is a TRUE no-op: keeps the owner for rollback.
        txn.release()
        self.assertNotIn(owner, reg.released)
        self.assertEqual(txn.state, "handed_off")
        # Terminal threw -> rollback still releases exactly the prepared owner.
        txn.rollback_handoff()
        self.assertIn(owner, reg.released)
        self.assertEqual(txn.state, "closed")

    # I -------------------------------------------------------------------
    async def test_reset_for_retry_refused_while_running_and_after_handoff(self):
        # While RUNNING.
        _FakeVerifier.behavior = "hang"
        _FakeVerifier.outcome = None
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        for p in _install_patches():
            p.start()
        try:
            task = asyncio.ensure_future(txn.async_run())
            for _ in range(50):
                if txn.state == "running":
                    break
                await asyncio.sleep(0.01)
            with self.assertRaises(RuntimeError):
                txn.reset_for_retry()
            self.assertTrue(txn.holds_claim)  # no mutation
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            patch.stopall()

        # After HANDED_OFF.
        reg2 = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        txn2 = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg2)
        for p in _install_patches():
            p.start()
        try:
            await txn2.async_run()
        finally:
            patch.stopall()
        self.assertEqual(txn2.prepare_handoff(FULL_PN), HANDOFF_OK)
        with self.assertRaises(RuntimeError):
            txn2.reset_for_retry()
        self.assertEqual(txn2.state, "handed_off")

    async def test_registry_provider_failure_is_typed_and_retryable_without_io(self):
        def _raise_provider():
            raise RuntimeError("registry boom")

        txn = CollectorAdmissionTransaction(
            _request(),
            registry_provider=_raise_provider,
            listener_host="0.0.0.0",
        )
        with patch.object(atm, "ObservedSessionRestartChannel") as channel:
            await txn.async_run()
        channel.assert_not_called()
        self.assertEqual(txn.failure_reason, "verification_error")
        self.assertEqual(txn.state, "failed")
        self.assertFalse(txn.holds_claim)
        txn.reset_for_retry()
        self.assertEqual(txn.state, "ready")

    async def test_missing_registry_fails_before_channel_or_restart(self):
        txn = CollectorAdmissionTransaction(
            _request(),
            registry_provider=lambda: None,
            listener_host="0.0.0.0",
        )
        with patch.object(atm, "ObservedSessionRestartChannel") as channel:
            await txn.async_run()
        channel.assert_not_called()
        self.assertEqual(txn.failure_reason, atm.FAILURE_OWNERSHIP_UNAVAILABLE)
        self.assertEqual(txn.state, "failed")
        self.assertFalse(txn.holds_claim)

    async def test_manual_callback_bridge_normalizes_both_failure_origins(self):
        """The failure-menu action always opens one fresh callback lifecycle."""

        # A passive-inbound failure follows the explicit FAILED transition.
        inbound = _make(_request(), registry=_FakeRegistry())
        inbound._outcome = InboundRecoveryOutcome(
            failure_reason="restart_not_confirmed", collector_pn=SHORT_PN
        )
        inbound._state = "failed"
        inbound.begin_manual_callback_continuation()
        self.assertEqual(inbound.state, "callback_ready")
        self.assertIsNone(inbound.outcome)

        # A selected route can fail before the first callback authority mutates
        # READY.  It is still a valid explicit manual continuation and must not
        # produce admission_transaction_identity_not_runnable on form submit.
        selected = _make(_route_request(), registry=_FakeRegistry())
        selected.begin_manual_callback_continuation()
        self.assertEqual(selected.state, "callback_ready")

        # A held, unadopted callback capability is discarded before the manual
        # attempt.  Exact owner cleanup remains inside the transaction.
        held_registry = _FakeRegistry()
        held = _make(_route_request(), registry=held_registry)
        held._state = "identity_certified"
        held._registry = held_registry
        held._owner = "callback_verification:old"
        held_registry.claims[held._owner] = S1
        held.begin_manual_callback_continuation()
        self.assertEqual(held.state, "callback_ready")
        self.assertFalse(held.holds_claim)
        self.assertEqual(held_registry.released, ["callback_verification:old"])

    async def test_manual_callback_bridge_refuses_live_or_committed_authority(self):
        for state in ("running", "identity_running", "recovery_running", "handed_off", "closed"):
            with self.subTest(state=state):
                txn = _make(_route_request(), registry=_FakeRegistry())
                txn._state = state
                with self.assertRaises(RuntimeError):
                    txn.begin_manual_callback_continuation()
                self.assertEqual(txn.state, state)

    async def test_registry_lookup_failure_is_typed_without_wire_io(self):
        reg = _LookupRaisingRegistry()
        txn = _make(_request(), registry=reg)
        with patch.object(atm, "ObservedSessionRestartChannel") as channel:
            await txn.async_run()
        channel.assert_not_called()
        self.assertEqual(txn.failure_reason, "verification_error")
        self.assertEqual(txn.state, "failed")
        self.assertFalse(txn.holds_claim)

    async def test_repeated_cancel_during_cleanup_closes_all_then_releases(self):
        _FakeVerifier.behavior = "hang"
        _FakeVerifier.outcome = None
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        _BlockingCloseProbe.entered = asyncio.Event()
        _BlockingCloseProbe.release = asyncio.Event()
        _BlockingCloseProbe.instances = []
        _CloseTrackingChannel.instances = []
        patches = [
            patch.object(atm, "InboundRecoveryVerifier", _FakeVerifier),
            patch.object(atm, "ObservedSessionRestartChannel", _CloseTrackingChannel),
            patch.object(atm, "get_callback_trigger_ledger", lambda: _Ledger()),
            patch.object(
                atm, "registry_sessions_projection", lambda registry: (lambda: ())
            ),
            patch(
                "custom_components.eybond_local.collector.silent_session_probe."
                "SilentSessionIdentityProbeChannel",
                _BlockingCloseProbe,
            ),
        ]
        for item in patches:
            item.start()
        try:
            task = asyncio.create_task(txn.async_run())
            for _ in range(50):
                if txn.holds_claim:
                    break
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.wait_for(_BlockingCloseProbe.entered.wait(), timeout=1)
            task.cancel()  # re-delivered while the shielded cleanup is blocked
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            _BlockingCloseProbe.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            _BlockingCloseProbe.release.set()
            for item in reversed(patches):
                item.stop()
        self.assertTrue(task.cancelled())
        self.assertEqual(_BlockingCloseProbe.instances[0].closed, 1)
        self.assertEqual(_CloseTrackingChannel.instances[0].closed, 1)
        self.assertFalse(txn.holds_claim)
        self.assertFalse(txn.verified)
        self.assertEqual(txn.state, "failed")
        self.assertEqual(len(reg.released), 1)

    async def test_cancel_after_verifier_success_during_cleanup_discards_proof(self):
        _FakeVerifier.behavior = "return"
        _FakeVerifier.outcome = _verified_outcome(FULL_PN)
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(pn=FULL_PN, source="at_dtupn"), registry=reg)
        _BlockingCloseProbe.entered = asyncio.Event()
        _BlockingCloseProbe.release = asyncio.Event()
        _BlockingCloseProbe.instances = []
        _CloseTrackingChannel.instances = []
        patches = [
            patch.object(atm, "InboundRecoveryVerifier", _FakeVerifier),
            patch.object(atm, "ObservedSessionRestartChannel", _CloseTrackingChannel),
            patch.object(atm, "get_callback_trigger_ledger", lambda: _Ledger()),
            patch.object(
                atm, "registry_sessions_projection", lambda registry: (lambda: ())
            ),
            patch(
                "custom_components.eybond_local.collector.silent_session_probe."
                "SilentSessionIdentityProbeChannel",
                _BlockingCloseProbe,
            ),
        ]
        for item in patches:
            item.start()
        try:
            task = asyncio.create_task(txn.async_run())
            await asyncio.wait_for(_BlockingCloseProbe.entered.wait(), timeout=1)
            task.cancel()
            _BlockingCloseProbe.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            _BlockingCloseProbe.release.set()
            for item in reversed(patches):
                item.stop()
        self.assertTrue(task.cancelled())
        self.assertFalse(txn.verified)
        self.assertFalse(txn.terminal_input.has_proof)
        self.assertFalse(txn.holds_claim)
        self.assertEqual(txn.state, "failed")
        self.assertEqual(len(reg.released), 1)

    async def test_async_close_finishes_cleanup_before_propagating_cancel(self):
        reg = _FakeRegistry(current_session=SimpleNamespace(session_id=S1))
        txn = _make(_request(), registry=reg)
        owner = "strategy_verification:close"
        reg.claims[owner] = S1
        txn._registry = reg
        txn._owner = owner
        txn._state = "verified"
        _BlockingCloseProbe.entered = asyncio.Event()
        _BlockingCloseProbe.release = asyncio.Event()
        _BlockingCloseProbe.instances = []
        channel = _CloseTrackingChannel()
        probe = _BlockingCloseProbe()
        txn._channel = channel
        txn._silent_probe = probe

        task = asyncio.create_task(txn.async_close())
        await asyncio.wait_for(_BlockingCloseProbe.entered.wait(), timeout=1)
        task.cancel()
        task.cancel()
        _BlockingCloseProbe.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertEqual(probe.closed, 1)
        self.assertEqual(channel.closed, 1)
        self.assertFalse(txn.holds_claim)
        self.assertEqual(txn.state, "closed")
        self.assertEqual(reg.released, [owner])


if __name__ == "__main__":
    unittest.main()
