"""Cold-start degraded-repair Phase-A bootstrap transaction (Batch 8B.1).

Unit-proves the ONE public, causally-isolated bootstrap transaction in
``connection/strategy_transition_repair.py`` against the REAL registry, the REAL
causality ledger, the REAL shared matcher and the REAL typed recovery-state
model. Only Phase B's controlled-reset engine (live sockets) and the listener/
wire I/O (the public ``CallbackBootstrapChannel``) are injected, so causality,
ownership, matching and concurrency are exercised in isolation.

Groups: A causality, B matcher, D ownership, E transition concurrency, F
existing-live shortcut. The wire authority (group C) is proven at the boundary
level in ``test_callback_bootstrap_channel.py``.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.callback_bootstrap import (  # noqa: E402
    ExactSessionRead,
)
from custom_components.eybond_local.connection import (  # noqa: E402
    strategy_transition_repair as repair_mod,
)
from custom_components.eybond_local.connection.callback_ledger import (  # noqa: E402
    CallbackTriggerLedger,
)
from custom_components.eybond_local.connection.recovery_contract import (  # noqa: E402
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    CallbackRecoveryProof,
)
from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    CallbackSessionRegistry,
    PermanentOwnedSessionCertification,
)
from custom_components.eybond_local.connection.strategy_transition import (  # noqa: E402
    STRATEGY_TRANSITION_LEASES,
    TRANSITION_ALREADY_RUNNING,
)
from custom_components.eybond_local.connection.strategy_transition_recovery import (  # noqa: E402
    StrategyTransitionRecoveryState,
)
from custom_components.eybond_local.onboarding.strategy_verification import (  # noqa: E402
    RecoveryVerificationOutcome,
    STATE_CALLBACK_VERIFIED,
)
from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)

FULL_PN = "V001020SYN62344022"
OTHER_PN = "V000405SYN94677058"
ENTRY_ID = "entry-under-repair"
OTHER_ENTRY = "other-entry"
BOOT_SESSION = "boot-sock"
TS = "2026-07-17T10:00:00+00:00"

FAST_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    callback_recovery_session_wait=0.3,
    callback_causality_lease_wait=0.3,
    discovery_timeout=0.05,
)


def _session(sid, pn, *, strong=True, state="identified", port=8899):
    return {
        "session_id": sid,
        "peer_ip": "203.0.113.10",
        "listener_port": port,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": "eybond_framed",
        "session_protocol": "eybond_framed",
        "collector_identity_source": "fc2_parameter_2" if strong else "framed_heartbeat",
    }


def _state(**overrides):
    base = dict(
        collector_pn=FULL_PN,
        now=TS,
        trigger_target_host="192.168.88.72",
        trigger_udp_port=58899,
        advertised_host="public.example",
        advertised_port=18899,
        trigger_bind_host="127.0.0.1",
        listener_bind_host="127.0.0.1",
        local_listener_port=8899,
    )
    base.update(overrides)
    return StrategyTransitionRecoveryState.create(**base)


class _FakeChannel:
    """A fake of the public CallbackBootstrapChannel (I/O only, no wire logic).

    ``sessions()`` is the REAL registry projection; only the listener lifecycle,
    the ledger-recorded trigger, and the exact-session identity read are scripted
    so causality/ownership/matching run against real primitives.
    """

    def __init__(
        self,
        harness,
        *,
        on_send=None,
        reads=None,
        silent=(),
        listener_ok=True,
        foreign_on_send=False,
    ):
        self._h = harness
        self._on_send = on_send
        self._reads = dict(reads or {})
        harness.silent.update(silent)  # live PN-less sockets live on the harness
        self._listener_ok = listener_ok
        self._foreign_on_send = foreign_on_send
        self.opened = 0
        self.closed = 0
        self.sends = 0
        # ONE ledger authority: the same object the sender records into (mirrors
        # the real channel, whose sender and lease share the process ledger).
        self.ledger = harness.ledger
        self.trigger_sender = object()  # reused by Phase B (which is patched)

    async def async_open(self):
        self.opened += 1

    async def async_close(self):
        # Idempotent: safe to call any number of times.
        self.closed += 1

    @property
    def listener_available(self):
        return self._listener_ok

    def sessions(self):
        observed = tuple(
            {
                "session_id": s.session_id,
                "collector_pn": s.collector_pn,
                "state": s.state,
                "has_strong_identity": s.has_strong_identity,
                "collector_identity_source": s.identity_source,
                "listener_port": s.listener_port,
                "raw": dict(s.raw),
            }
            for s in self._h.registry.observed_sessions_per_socket()
        )
        seen = {row["session_id"] for row in observed}
        # Merge live PN-less silent sockets (registry observation wins on dedup).
        silent = tuple(
            {
                "session_id": sid,
                "collector_pn": "",
                "state": "",
                "has_strong_identity": False,
                "collector_identity_source": "",
                "listener_port": 8899,
                "raw": {"session_id": sid, "listener_port": 8899},
            }
            for sid in self._h.silent
            if sid and sid not in seen
        )
        return observed + silent

    async def async_send_trigger(self, route):
        self.sends += 1
        # Attribute the OWN trigger to the held attempt (bumps own_sends).
        self.ledger.record(source="degraded_repair_bootstrap")
        if self._foreign_on_send:
            # A concurrent/external trigger inside our window.
            self.ledger.record(attempt_id="intruder-attempt", source="external")
        if self._on_send is not None:
            self._on_send()

    async def async_read_exact_session_identity(self, session):
        sid = str(session.get("session_id") or "")
        read = self._reads.get(sid)
        if read is None:
            return ExactSessionRead(wire_available=False)
        if read.wire_available and read.collector_pn:
            # A successful probe records the strong PN in the listener/registry.
            self._h.promote(sid, read.collector_pn)
            self._h.silent.discard(sid)
        return read


class _Harness:
    def __init__(self):
        self.inventory: list[dict] = []
        self.silent: set[str] = set()  # live PN-less sockets on the listener
        self.registry = CallbackSessionRegistry(
            sessions_source=lambda: tuple(self.inventory)
        )
        self.ledger = CallbackTriggerLedger()
        self.committed = None
        self.commit_refusal = ""

    def promote(self, sid, pn):
        """A successful exact-session read records a strong PN: enrich an
        existing socket, or materialise a previously-silent one."""

        for i, s in enumerate(self.inventory):
            if s["session_id"] == sid:
                self.inventory[i] = _session(
                    sid, pn, strong=True, port=int(s.get("listener_port") or 8899)
                )
                return
        self.inventory.append(_session(sid, pn, strong=True))

    def add(self, session):
        self.inventory.append(session)

    async def commit(self, updates, terminal):
        if self.commit_refusal:
            return self.commit_refusal
        self.committed = dict(updates)
        return ""

    async def run(self, state, *, channel, proof="success", owner_id=ENTRY_ID, **kwargs):
        base = dict(
            registry=self.registry,
            owner_id=owner_id,
            state=state,
            channel=channel,
            commit=self.commit,
            clock=lambda: TS,
            policy=FAST_POLICY,
            poll_interval=0.01,
        )
        base.update(kwargs)

        async def _fake_phase_b(**tk):
            if proof == "timeout":
                return RecoveryVerificationOutcome(
                    status="inbound_not_verified",
                    failure_reason="callback_recovery_timeout",
                    collector_pn=FULL_PN,
                )
            sid = str(self.registry.claimed_session_id(owner_id) or "")
            cert = self.registry.certify_permanent_owned_session(owner_id, sid)
            return RecoveryVerificationOutcome(
                status=STATE_CALLBACK_VERIFIED,
                collector_pn=FULL_PN,
                new_session_id=sid,
                callback_proof=CallbackRecoveryProof(
                    method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
                    collector_pn=FULL_PN,
                    identity_source="fc2_parameter_2",
                    verified_at=TS,
                    trigger_target="192.168.88.72:58899",
                    advertised_ha_endpoint="public.example:18899",
                    listener_port=8899,
                ),
                owner_certification=cert,
            )

        with patch.object(
            repair_mod, "async_run_callback_recovery_transaction", _fake_phase_b
        ):
            return await repair_mod.async_run_degraded_recovery_repair(**base)

    async def run_phase_a(self, state, *, channel, owner_id=ENTRY_ID, **kwargs):
        base = dict(
            registry=self.registry,
            owner_id=owner_id,
            state=state,
            route=state.callback_route(),
            channel=channel,
            policy=FAST_POLICY,
            poll_interval=0.01,
        )
        base.update(kwargs)
        return await repair_mod.async_run_callback_bootstrap_transaction(**base)


_UNSET = object()


def _dial(h, *, sid=BOOT_SESSION, pn=FULL_PN, strong=True, read_pn=_UNSET, **kw):
    """A channel whose ONE dial-in adds ``sid`` and whose exact read yields it.

    The authoritative read is ALWAYS performed by the transaction, so every
    "collector dials in" scenario supplies both the socket AND its read result.
    """

    resolved_read = pn if read_pn is _UNSET else read_pn
    return _FakeChannel(
        h,
        on_send=lambda: h.add(_session(sid, pn, strong=strong)),
        reads={sid: ExactSessionRead(True, "eybond_framed", resolved_read)},
        **kw,
    )


class PreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_state_object_is_fail_closed(self) -> None:
        h = _Harness()
        ch = _FakeChannel(h)
        result = await h.run({"kind": "garbage"}, channel=ch)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, repair_mod.REPAIR_STATE_INVALID)
        self.assertEqual(ch.sends, 0)

    async def test_valid_state_has_a_complete_route(self) -> None:
        # A validated recovery state always yields a complete route, so the
        # route-incomplete branch is defensive-only; assert the sanity invariant.
        self.assertIsNotNone(_state().callback_route())


class OwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_cold_start_no_pre_existing_claim_succeeds(self) -> None:
        # D: a cold repair creates its own PN-only permanent claim; no
        # coordinator pre-created it.
        h = _Harness()
        self.assertEqual(h.registry.claimed_identity(ENTRY_ID), "")
        ch = _dial(h, strong=False)  # weak dial-in enriched by the read
        result = await h.run(_state(), channel=ch)
        self.assertTrue(result.success, result.failure_reason)
        self.assertEqual(ch.sends, 1)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), BOOT_SESSION)
        assert h.committed is not None
        self.assertEqual(h.committed["connection_strategy"], "callback_on_demand")

    async def test_foreign_existing_owner_fails_before_udp(self) -> None:
        # D: the PN belongs to another owner -> claimed_by_other BEFORE any UDP.
        h = _Harness()
        h.add(_session("other-sock", FULL_PN))
        h.registry.claim(OTHER_ENTRY, collector_pn=FULL_PN)
        ch = _FakeChannel(h)
        result = await h.run(_state(), channel=ch)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, repair_mod.BOOTSTRAP_CLAIMED_BY_OTHER)
        self.assertEqual(ch.sends, 0)
        self.assertEqual(ch.opened, 0)  # never even borrowed the listener

    async def test_existing_same_owner_pn_only_claim_enriches(self) -> None:
        # D: an existing PN-only claim of the SAME owner is idempotently reused.
        h = _Harness()
        h.registry.claim(ENTRY_ID, collector_pn=FULL_PN)
        ch = _dial(h)
        result = await h.run(_state(), channel=ch)
        self.assertTrue(result.success, result.failure_reason)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), BOOT_SESSION)

    async def test_failed_identity_read_leaves_claim_unpinned(self) -> None:
        # D: a weak candidate whose read yields no PN is never pinned.
        h = _Harness()
        ch = _dial(h, strong=False, read_pn="")  # read RAN, produced no PN
        result = await h.run(_state(), channel=ch)
        self.assertFalse(result.success)
        self.assertEqual(result.phase, "bootstrap")
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")  # unpinned
        self.assertIsNone(h.committed)

    async def test_success_returns_reverifiable_certification(self) -> None:
        h = _Harness()
        ch = _dial(h)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CERTIFIED)
        self.assertIsInstance(
            phase_a.certification, PermanentOwnedSessionCertification
        )
        self.assertTrue(
            h.registry.reverify_permanent_owned_session(phase_a.certification)
        )


class CausalityTests(unittest.IsolatedAsyncioTestCase):
    async def test_exactly_one_own_send_on_success(self) -> None:
        h = _Harness()
        ch = _dial(h)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CERTIFIED)
        self.assertEqual(ch.sends, 1)

    async def test_foreign_trigger_is_interference_no_ownership(self) -> None:
        # A: a foreign trigger inside the window -> interference, nothing pinned.
        h = _Harness()
        ch = _dial(h, foreign_on_send=True)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_INTERFERENCE)
        self.assertIsNone(phase_a.certification)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")

    async def test_cancellation_releases_both_leases(self) -> None:
        # A: cancel mid-wait -> the causality lease AND the strategy lease free.
        h = _Harness()
        ch = _FakeChannel(h)  # nobody dials in -> the transaction sits waiting
        task = asyncio.get_running_loop().create_task(h.run(_state(), channel=ch))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(h.ledger._causality_owner, "")  # causality lease free
        self.assertTrue(STRATEGY_TRANSITION_LEASES.acquire(ENTRY_ID))  # strategy free
        STRATEGY_TRANSITION_LEASES.release(ENTRY_ID)

    async def test_two_attempts_serialize_on_causality(self) -> None:
        # A: while one attempt holds the shared ledger's causality lease, a
        # second correct attempt cannot confirm on the same window -> busy.
        h = _Harness()
        ch = _dial(h)
        async with h.ledger.causality_lease("external-holder", timeout=1.0):
            phase_a = await h.run_phase_a(
                _state(),
                channel=ch,
                owner_id="second-owner",
                policy=replace(FAST_POLICY, callback_causality_lease_wait=0.1),
            )
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CAUSALITY_BUSY)
        self.assertEqual(ch.sends, 0)  # never got the window


class MatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_target_plus_foreign_is_ambiguous(self) -> None:
        # B: a target AND a foreign strong session -> ambiguity, bind nothing.
        h = _Harness()

        def _add():
            h.add(_session("a", FULL_PN))
            h.add(_session("b", OTHER_PN))

        ch = _FakeChannel(
            h,
            on_send=_add,
            reads={
                "a": ExactSessionRead(True, "eybond_framed", FULL_PN),
                "b": ExactSessionRead(True, "eybond_framed", OTHER_PN),
            },
        )
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_AMBIGUOUS)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")

    async def test_foreign_only_is_identity_mismatch(self) -> None:
        h = _Harness()
        ch = _dial(h, sid="foreign", pn=OTHER_PN)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_IDENTITY_MISMATCH)

    async def test_baseline_session_is_excluded(self) -> None:
        # B: a same-PN session that existed BEFORE the trigger is not the answer.
        # It is WEAK, so the existing-owner shortcut does not fire; the cold path
        # runs and the baseline exclusion leaves nothing fresh to bind.
        h = _Harness()
        h.add(_session("pre-existing", FULL_PN, strong=False))
        ch = _FakeChannel(h)  # trigger brings nothing new
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_NO_SESSION)

    async def test_read_matcher_disagreement_is_fail_closed(self) -> None:
        # B: we authoritatively read socket A, but a DIFFERENT strong same-PN
        # socket B sits first in the projection, so the matcher binds B != A.
        h = _Harness()

        def _add():
            h.add(_session("B", FULL_PN, strong=True))  # first -> matcher picks it
            h.add(_session("A", FULL_PN, strong=False))  # the one WE read

        ch = _FakeChannel(
            h,
            on_send=_add,
            reads={"A": ExactSessionRead(True, "eybond_framed", FULL_PN)},
        )
        phase_a = await h.run_phase_a(_state(), channel=ch)
        # Our read named A; the shared matcher bound B -> fail closed rather than
        # pin a socket we did not authoritatively verify.
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_IDENTITY_MISMATCH)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")


class ExistingLiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_trusted_live_session_skips_bootstrap_trigger(self) -> None:
        # F: an already-live strong trusted owned session re-certifies with NO
        # trigger.
        h = _Harness()
        h.add(_session("live-sock", FULL_PN))
        h.registry.claim(ENTRY_ID, collector_pn=FULL_PN)
        h.registry.pin_owner_claim_to_session(ENTRY_ID, "live-sock")
        ch = _FakeChannel(h)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_EXISTING_OWNER_CERTIFIED)
        self.assertEqual(phase_a.session_id, "live-sock")
        self.assertEqual(ch.sends, 0)
        self.assertEqual(ch.opened, 0)  # never even borrowed the listener

    async def test_closed_live_session_does_not_skip(self) -> None:
        # F: a claimed-but-closed session is not a shortcut; the cold path runs.
        h = _Harness()
        h.add(_session("dead-sock", FULL_PN, state="closed_disconnected"))
        h.registry.claim(ENTRY_ID, collector_pn=FULL_PN)
        ch = _dial(h)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CERTIFIED)
        self.assertEqual(ch.sends, 1)  # DID run the cold bootstrap


class TransitionConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self) -> None:
        STRATEGY_TRANSITION_LEASES.release(ENTRY_ID)

    async def test_repair_refused_while_transition_lease_held(self) -> None:
        # E: a normal transition holds the entry's strategy lease -> repair of
        # the same entry is typed-refused.
        h = _Harness()
        self.assertTrue(STRATEGY_TRANSITION_LEASES.acquire(ENTRY_ID))
        ch = _dial(h)
        result = await h.run(_state(), channel=ch)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, TRANSITION_ALREADY_RUNNING)
        self.assertEqual(ch.sends, 0)

    async def test_different_entries_are_independent(self) -> None:
        # E: a lease on ENTRY_ID does not block a repair of a different entry.
        h = _Harness()
        self.assertTrue(STRATEGY_TRANSITION_LEASES.acquire(ENTRY_ID))
        ch = _dial(h)
        result = await h.run(_state(), channel=ch, owner_id=OTHER_ENTRY)
        self.assertTrue(result.success, result.failure_reason)


class PhaseBTests(unittest.IsolatedAsyncioTestCase):
    async def test_listener_unavailable_is_typed_before_trigger(self) -> None:
        h = _Harness()
        ch = _FakeChannel(h, listener_ok=False)
        result = await h.run(_state(), channel=ch)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, repair_mod.BOOTSTRAP_LISTENER_UNAVAILABLE)
        self.assertEqual(ch.sends, 0)

    async def test_proof_timeout_after_bootstrap_leaves_no_commit(self) -> None:
        h = _Harness()
        ch = _dial(h)
        result = await h.run(_state(), channel=ch, proof="timeout")
        self.assertFalse(result.success)
        self.assertEqual(result.phase, "proof")
        self.assertIsNone(h.committed)

    async def test_commit_refusal_after_proof_is_terminal(self) -> None:
        h = _Harness()
        h.commit_refusal = "recovery_contract_conflict"
        ch = _dial(h)
        result = await h.run(_state(), channel=ch)
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, "recovery_contract_conflict")


class SilentSocketTests(unittest.IsolatedAsyncioTestCase):
    """BLOCKER 1: a fully-silent PN-less socket reaches the transaction."""

    async def test_silent_socket_with_evidence_is_read_and_certified(self) -> None:
        # THE regression test: on the OLD registry-only sessions() this socket
        # was invisible (registry hides PN-less sockets) -> NO_SESSION. With the
        # merged projection it is read via confirmed evidence, promoted to a
        # strong identity, and the shared matcher certifies it.
        SILENT = "silent-sock"
        h = _Harness()
        ch = _FakeChannel(
            h,
            on_send=lambda: h.silent.add(SILENT),  # collector dials in silently
            reads={SILENT: ExactSessionRead(True, "eybond_framed", FULL_PN)},
        )
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CERTIFIED)
        self.assertEqual(phase_a.session_id, SILENT)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), SILENT)
        # Deduped: after enrichment the id appears exactly once in the projection.
        ids = [s["session_id"] for s in ch.sessions()]
        self.assertEqual(ids.count(SILENT), 1)

    async def test_silent_socket_without_evidence_is_wire_unavailable(self) -> None:
        # A silent socket the channel cannot read (no wire authority) -> the
        # honest typed reason and NO ownership binding.
        SILENT = "silent-sock"
        h = _Harness()
        ch = _FakeChannel(h, on_send=lambda: h.silent.add(SILENT))  # no reads
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_WIRE_UNAVAILABLE)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")  # unbound

    async def test_pre_existing_silent_socket_is_excluded_from_baseline(self) -> None:
        # A silent socket present BEFORE the trigger is baseline: even if we
        # could read it, it is never the answer to THIS trigger.
        PRE = "pre-silent"
        h = _Harness()
        h.silent.add(PRE)  # already there at baseline
        ch = _FakeChannel(
            h, reads={PRE: ExactSessionRead(True, "eybond_framed", FULL_PN)}
        )
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_NO_SESSION)
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")


class IdentityOnlyOwnershipTests(unittest.IsolatedAsyncioTestCase):
    """BLOCKER 2: identity-only intent never auto-binds a socket pre-proof."""

    async def test_weak_baseline_socket_never_bound_and_retarget_still_works(
        self,
    ) -> None:
        # A live WEAK same-PN socket exists BEFORE the trigger. The identity-only
        # claim must NOT bind it (the old claim() did, which then blocked
        # retarget with previous_session_still_live). A new STRONG same-PN socket
        # arrives on the trigger and IS certified despite the live weak baseline.
        h = _Harness()
        h.add(_session("weak-live", FULL_PN, strong=False))  # live weak baseline
        ch = _dial(h)  # BOOT_SESSION dials in strong
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CERTIFIED)
        self.assertEqual(phase_a.session_id, BOOT_SESSION)  # not the weak socket

    async def test_failed_attempt_keeps_pn_intent_but_no_session_binding(
        self,
    ) -> None:
        # A failed cold attempt leaves durable PN ownership but session_id="",
        # and a retry is NOT blocked by previous_session_still_live.
        h = _Harness()
        ch = _FakeChannel(h)  # nobody dials in
        first = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(first.kind, repair_mod.BOOTSTRAP_NO_SESSION)
        self.assertEqual(h.registry.claimed_identity(ENTRY_ID), FULL_PN)  # PN kept
        self.assertEqual(h.registry.claimed_session_id(ENTRY_ID), "")  # unbound
        # Retry succeeds (previous session id is empty -> retarget is unblocked).
        ch2 = _dial(h)
        second = await h.run_phase_a(_state(), channel=ch2)
        self.assertEqual(second.kind, repair_mod.BOOTSTRAP_CERTIFIED)

    async def test_explicitly_bound_strong_owned_session_keeps_shortcut(self) -> None:
        # An ALREADY session-bound strong owned session keeps the no-trigger
        # shortcut (identity-only claim only enriches, preserving the binding).
        h = _Harness()
        h.add(_session("bound-sock", FULL_PN))
        h.registry.claim(ENTRY_ID, collector_pn=FULL_PN)
        h.registry.pin_owner_claim_to_session(ENTRY_ID, "bound-sock")
        ch = _dial(h)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_EXISTING_OWNER_CERTIFIED)
        self.assertEqual(ch.sends, 0)


class LedgerAuthorityTests(unittest.IsolatedAsyncioTestCase):
    """BLOCKER 3: one causality-ledger authority (lease == send ledger)."""

    async def test_transaction_uses_the_channel_ledger_for_the_lease(self) -> None:
        # The own send is counted in the SAME ledger the transaction leased on:
        # own_sends becomes 1 in the channel's ledger, never a separate one.
        h = _Harness()
        ch = _dial(h)
        self.assertEqual(h.ledger.snapshot_generation(), 0)
        phase_a = await h.run_phase_a(_state(), channel=ch)
        self.assertEqual(phase_a.kind, repair_mod.BOOTSTRAP_CERTIFIED)
        # The send landed in the channel ledger (generation advanced there).
        self.assertEqual(ch.ledger, h.ledger)
        self.assertEqual(h.ledger.snapshot_generation(), 1)


class ListenerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    """BLOCKER 4: exactly one open/close for every cold-path outcome."""

    def tearDown(self) -> None:
        STRATEGY_TRANSITION_LEASES.release(ENTRY_ID)

    async def test_open_close_balanced_on_success(self) -> None:
        h = _Harness()
        ch = _dial(h)
        await h.run_phase_a(_state(), channel=ch)
        self.assertEqual((ch.opened, ch.closed), (1, 1))

    async def test_open_close_balanced_on_listener_unavailable(self) -> None:
        h = _Harness()
        ch = _FakeChannel(h, listener_ok=False)
        await h.run_phase_a(_state(), channel=ch)
        self.assertEqual((ch.opened, ch.closed), (1, 1))

    async def test_open_close_balanced_on_causality_busy(self) -> None:
        h = _Harness()
        ch = _dial(h)
        async with h.ledger.causality_lease("holder", timeout=1.0):
            await h.run_phase_a(
                _state(),
                channel=ch,
                owner_id="second",
                policy=replace(FAST_POLICY, callback_causality_lease_wait=0.1),
            )
        self.assertEqual((ch.opened, ch.closed), (1, 1))

    async def test_open_close_balanced_on_matcher_failure(self) -> None:
        h = _Harness()
        ch = _dial(h, sid="foreign", pn=OTHER_PN)  # identity mismatch
        await h.run_phase_a(_state(), channel=ch)
        self.assertEqual((ch.opened, ch.closed), (1, 1))

    async def test_open_close_balanced_on_cancellation(self) -> None:
        h = _Harness()
        ch = _FakeChannel(h)  # nobody dials in -> sits in the wait loop
        task = asyncio.get_running_loop().create_task(h.run(_state(), channel=ch))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual((ch.opened, ch.closed), (1, 1))


if __name__ == "__main__":
    unittest.main()
