"""Batch 8B.2B.1 -- adversarial same-IP / foreign-PN matrix.

Proves that callback identity, bootstrap certification and ownership select the
collector EXCLUSIVELY by strong PN + exact session evidence. The same peer IP,
the order sockets appear in, and unrelated co-located sessions must never
influence identity or ownership. RecoveryContract persistence belongs to the
real-HA flow-manager acceptance layer and is intentionally not claimed here.

The matrix runs at the two layers where the decision is actually made:

* the ONE matcher ``match_callback_answer`` (pure) -- it never reads ``peer_ip``,
  distinguishes "before/after the trigger" only via the baseline set, and fails
  CLOSED (typed timeout / mismatch / ambiguity / interference);
* the ``CallbackSessionRegistry`` ownership authority and the ONE bootstrap
  transaction -- both refuse a foreign identity with a typed reason and never
  adopt a co-located collector by IP or socket order.

Both strong identity forms are exercised: framed ``fc2_parameter_2`` and AT
``at_dtupn`` (the wire-level proof for each lives in
``test_callback_bootstrap_production_wire.py``). Every async case has a hard
local deadline via ``asyncio.wait_for`` so a hang fails in seconds, and the
causality lease / listener channel are released on every exit including cancel.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.callback_ledger import (
    CallbackTriggerLedger,
)
from custom_components.eybond_local.collector_identity import pn_is_same_identity
from custom_components.eybond_local.connection.session_registry import CallbackSessionRegistry
from custom_components.eybond_local.connection.strategy_transition_recovery import (
    StrategyTransitionRecoveryState,
)
from custom_components.eybond_local.connection.strategy_transition_repair import (
    BOOTSTRAP_AMBIGUOUS,
    BOOTSTRAP_CERTIFIED,
    BOOTSTRAP_IDENTITY_MISMATCH,
    BOOTSTRAP_NO_SESSION,
    async_run_callback_bootstrap_transaction,
)
from custom_components.eybond_local.connection.callback_matching import (
    MATCH_IDENTITY_AMBIGUOUS,
    MATCH_IDENTITY_MISMATCH,
    MATCH_OK,
    MATCH_TIMEOUT,
    MATCH_TRIGGER_INTERFERENCE,
    match_callback_answer,
)
from custom_components.eybond_local.onboarding.timeouts import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)

# Synthetic identities only. A/B are unrelated full PNs; S is a shared 14-char
# prefix; A_DIV/B_DIV diverge after S (so S matches BOTH but A_DIV != B_DIV).
TARGET_PN = "V001020SYN62344022"
FOREIGN_PN = "V000405SYN94677058"
SHORT_PN = "V001020SYN6234"          # 14 >= CALLBACK_PN_PREFIX_MATCH_MIN_LEN (10)
DIVERGENT_A = "V001020SYN62344022"   # == TARGET_PN; starts with SHORT_PN
DIVERGENT_B = "V001020SYN62349999"   # starts with SHORT_PN; diverges from DIVERGENT_A
SAME_IP = "203.0.113.9"              # the SINGLE peer IP shared by every socket
TS = "2026-07-18T10:00:00+00:00"

# framed FC=2 param 2 and AT DTUPN -- the two strong identity forms.
STRONG_SOURCES = ("fc2_parameter_2", "at_dtupn")

# Every async case bounded so a hang fails in seconds, not stalls.
_FAST_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    callback_recovery_session_wait=0.2,
    callback_causality_lease_wait=1.0,
)
_HARD_DEADLINE = 5.0


def _matcher_session(sid, pn, *, strong=True, state="identified_strong"):
    """A session in the matcher's shape. ``peer_ip`` is present but the matcher
    never reads it -- its presence is the point (it must not tip any decision)."""

    return {
        "session_id": sid,
        "state": state,
        "has_strong_identity": strong,
        "collector_pn": pn,
        "peer_ip": SAME_IP,
    }


def _obs(sid, pn, *, source="fc2_parameter_2", state="routed_framed", peer_ip=SAME_IP):
    """A raw registry observation (all sockets share ``peer_ip`` by default)."""

    return {
        "session_id": sid,
        "peer_ip": peer_ip,
        "listener_port": 18899,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": "",
        "collector_identity_source": source,
    }


# ---------------------------------------------------------------------------
# 1. The ONE matcher: identity selection never consults peer IP or socket order.
# ---------------------------------------------------------------------------
class SameIpMatcherMatrix(unittest.TestCase):
    def test_matcher_never_reads_peer_ip(self) -> None:
        # The matcher's session shape carries peer_ip, yet stripping it changes
        # NOTHING -- proof by construction that peer IP is not an input.
        with_ip = [_matcher_session("sA", TARGET_PN)]
        without_ip = [{k: v for k, v in with_ip[0].items() if k != "peer_ip"}]
        a = match_callback_answer(
            with_ip, baseline_session_ids=set(), result_pn=TARGET_PN,
            expected_pn=TARGET_PN,
        )
        b = match_callback_answer(
            without_ip, baseline_session_ids=set(), result_pn=TARGET_PN,
            expected_pn=TARGET_PN,
        )
        self.assertEqual((a.result, a.session_id, a.collector_pn), (MATCH_OK, "sA", TARGET_PN))
        self.assertEqual((a.result, a.session_id, a.collector_pn),
                         (b.result, b.session_id, b.collector_pn))

    def test_s2_foreign_before_target_is_not_mistaken_for_it(self) -> None:
        # Scenario 2: foreign B is present BEFORE the trigger (baseline), target A
        # arrives fresh. B (not fresh) can never be the answer; A is selected. The
        # RESULT is identical whether B or A is listed first (no first/last bias).
        for order in ([_matcher_session("sB", FOREIGN_PN), _matcher_session("sA", TARGET_PN)],
                      [_matcher_session("sA", TARGET_PN), _matcher_session("sB", FOREIGN_PN)]):
            with self.subTest(order=[s["session_id"] for s in order]):
                m = match_callback_answer(
                    order, baseline_session_ids={"sB"}, result_pn=TARGET_PN,
                    expected_pn=TARGET_PN,
                )
                self.assertEqual(m.result, MATCH_OK)
                self.assertEqual(m.session_id, "sA")
                self.assertEqual(m.collector_pn, TARGET_PN)

    def test_s4_only_strong_foreign_is_a_typed_mismatch(self) -> None:
        # Scenario 4: only a strong foreign B in the window (read yields B) -> a
        # typed identity mismatch, NOTHING bound.
        m = match_callback_answer(
            [_matcher_session("sB", FOREIGN_PN)], baseline_session_ids=set(),
            result_pn=FOREIGN_PN, expected_pn=TARGET_PN,
        )
        self.assertEqual(m.result, MATCH_IDENTITY_MISMATCH)
        self.assertFalse(m.confirmed)
        self.assertEqual(m.session_id, "")

    def test_s5_two_distinct_strong_fresh_is_typed_ambiguity_order_free(self) -> None:
        # Scenario 5: two distinct strong sockets fresh in the window -> typed
        # ambiguity, never a first/last/peer-IP tiebreak. Order-independent.
        for order in ([_matcher_session("sA", TARGET_PN), _matcher_session("sB", FOREIGN_PN)],
                      [_matcher_session("sB", FOREIGN_PN), _matcher_session("sA", TARGET_PN)]):
            with self.subTest(order=[s["session_id"] for s in order]):
                m = match_callback_answer(
                    order, baseline_session_ids=set(), result_pn=TARGET_PN,
                    expected_pn=TARGET_PN,
                )
                self.assertEqual(m.result, MATCH_IDENTITY_AMBIGUOUS)
                self.assertEqual(m.session_id, "")

    def test_two_silent_unidentified_never_bind_by_ip_or_order(self) -> None:
        # Scenario 5 (silent form): two unidentified sockets (no strong identity)
        # are never in the fresh set -> typed timeout, never bound by IP/order.
        m = match_callback_answer(
            [_matcher_session("s1", "", strong=False, state="accepted"),
             _matcher_session("s2", "", strong=False, state="accepted")],
            baseline_session_ids=set(), result_pn="", expected_pn=TARGET_PN,
        )
        self.assertEqual(m.result, MATCH_TIMEOUT)
        self.assertEqual(m.session_id, "")

    def test_foreign_trigger_in_window_is_typed_interference(self) -> None:
        # A foreign set>server inside our causal window makes any appearing socket
        # unattributable -> typed interference, never a silent adoption.
        m = match_callback_answer(
            [_matcher_session("sA", TARGET_PN)], baseline_session_ids=set(),
            result_pn=TARGET_PN, expected_pn=TARGET_PN,
            trigger_generation_before=5, trigger_generation_after=7,
            expected_own_triggers=1,
        )
        self.assertEqual(m.result, MATCH_TRIGGER_INTERFERENCE)


# ---------------------------------------------------------------------------
# 2. Ownership authority: foreign PN never owns, enriches or retargets by IP.
# ---------------------------------------------------------------------------
class ForeignPnOwnershipMatrix(unittest.TestCase):
    def _registry(self, sessions):
        return CallbackSessionRegistry(sessions_source=lambda: sessions)

    def test_s1_two_collectors_one_ip_are_independent_owners(self) -> None:
        # Scenario 1: A and B live at the SAME peer IP. Each is owned by its own
        # entry by PN; the other stays with its owner; peer IP is never consulted.
        for source in STRONG_SOURCES:
            with self.subTest(identity_source=source):
                sessions = [
                    _obs("sA", TARGET_PN, source=source),
                    _obs("sB", FOREIGN_PN, source=source),
                ]
                reg = self._registry(sessions)
                reg.claim("entry-A", collector_pn=TARGET_PN)
                reg.claim("entry-B", collector_pn=FOREIGN_PN)
                self.assertEqual(reg.owner_for_pn(TARGET_PN), "entry-A")
                self.assertEqual(reg.owner_for_pn(FOREIGN_PN), "entry-B")
                self.assertEqual(reg.owned_session_location("entry-A").session_id, "sA")
                self.assertEqual(reg.owned_session_location("entry-B").session_id, "sB")

    def test_s1_pn_less_claim_never_adopts_a_co_located_collector(self) -> None:
        # A PN-less entry claim can never adopt A just because A is live at the
        # shared IP -- ownership is by PN, not by address.
        reg = self._registry([_obs("sA", TARGET_PN)])
        reg.claim("pn-less")
        self.assertEqual(reg.claimed_identity("pn-less"), "")
        self.assertIsNone(reg.owned_session_location("pn-less"))
        self.assertEqual(reg.owner_for_pn(TARGET_PN), "")  # stays unowned

    def test_s3_late_foreign_never_resets_a_proven_target(self) -> None:
        # Scenario 3: target A is proven (owned + handoff prepared). A late foreign
        # B (even on A's own socket) cannot reset or replace A -- the proof PN
        # stays A. Both the socket-reclaim and the promote paths refuse typed.
        for source in STRONG_SOURCES:
            with self.subTest(identity_source=source):
                sessions = [_obs("sA", TARGET_PN, source=source)]
                reg = self._registry(sessions)
                owner = "callback_verification:aaaa"
                reg.claim_session(owner, session_id="sA")
                reg.promote_claim_to_full_pn(owner, TARGET_PN)
                self.assertTrue(reg.prepare_handoff(owner, TARGET_PN))

                # A's own socket now reports foreign B: a re-claim is refused.
                sessions[:] = [_obs("sA", FOREIGN_PN, source=source)]
                with self.assertRaises(ValueError) as ctx:
                    reg.claim_session(owner, session_id="sA")
                self.assertIn("claim_session_identity_mismatch", str(ctx.exception))

                # And promoting the claim to B is refused too.
                with self.assertRaises(ValueError):
                    reg.promote_claim_to_full_pn(owner, FOREIGN_PN)

                # The proven identity/socket/handoff all still stand for A.
                self.assertEqual(reg.claimed_identity(owner), TARGET_PN)
                self.assertEqual(reg.claimed_session_id(owner), "sA")
                self.assertEqual(reg.prepared_handoff_identity(owner, TARGET_PN), TARGET_PN)
                self.assertEqual(reg.owner_for_pn(FOREIGN_PN), "")

    def test_s6_short_pn_prefix_match_is_not_transitive(self) -> None:
        # Scenario 6: a SHORT PN S is a prefix of two DIVERGENT full PNs. S matches
        # each, but the two full PNs are NOT the same collector. A claim enriched
        # to one full PN can NEVER be re-enriched to the divergent one.
        self.assertTrue(pn_is_same_identity(SHORT_PN, DIVERGENT_A))
        self.assertTrue(pn_is_same_identity(SHORT_PN, DIVERGENT_B))
        self.assertFalse(pn_is_same_identity(DIVERGENT_A, DIVERGENT_B))  # non-transitive

        reg = self._registry([_obs("sA", DIVERGENT_A)])
        reg.claim("entry", collector_pn=SHORT_PN)
        self.assertTrue(pn_is_same_identity(reg.claimed_identity("entry"), DIVERGENT_A))
        self.assertTrue(reg.promote_claim_to_full_pn("entry", DIVERGENT_A))
        # The divergent full PN cannot enrich the claim -- it is a different device.
        with self.assertRaises(ValueError) as ctx:
            reg.promote_claim_to_full_pn("entry", DIVERGENT_B)
        self.assertIn("promote_identity_mismatch", str(ctx.exception))
        self.assertEqual(reg.claimed_identity("entry"), DIVERGENT_A)
        self.assertEqual(reg.owner_for_pn(DIVERGENT_B), "")

    def test_s6_divergent_full_pns_are_distinct_owners(self) -> None:
        # The same short prefix routed to two entries must stay TWO owners: S
        # matching both must not collapse A_full and B_full into one collector.
        sessions = [_obs("sA", DIVERGENT_A), _obs("sB", DIVERGENT_B)]
        reg = self._registry(sessions)
        reg.claim("entry-A", collector_pn=SHORT_PN)
        reg.promote_claim_to_full_pn("entry-A", DIVERGENT_A)
        reg.claim("entry-B", collector_pn=DIVERGENT_B)
        self.assertEqual(reg.owner_for_pn(DIVERGENT_A), "entry-A")
        self.assertEqual(reg.owner_for_pn(DIVERGENT_B), "entry-B")

    def test_s7_same_pn_reconnect_retargets_only_a_and_ignores_b(self) -> None:
        # Scenario 7: A reconnects on a new socket while foreign B is co-present.
        # The claim retargets ONLY to A's new socket; B can never take it over, and
        # the exact-session handle (the ConfirmedWireBinding source) stays A's.
        for source in STRONG_SOURCES:
            with self.subTest(identity_source=source):
                sessions = [_obs("sA1", TARGET_PN, source=source),
                            _obs("sB", FOREIGN_PN, source=source)]
                reg = self._registry(sessions)
                owner = "callback_recovery:owner"
                reg.claim_session(owner, session_id="sA1")
                reg.promote_claim_to_full_pn(owner, TARGET_PN)

                # sA1 closes; A reconnects as sA2 (different port); B still present.
                sessions[:] = [
                    _obs("sA1", TARGET_PN, source=source, state="closed_disconnected"),
                    _obs("sA2", TARGET_PN, source=source, peer_ip="198.51.100.9"),
                    _obs("sB", FOREIGN_PN, source=source),
                ]
                self.assertTrue(
                    reg.retarget_claim_to_reconnected_session(owner, "sA2")
                )
                self.assertEqual(reg.claimed_session_id(owner), "sA2")

                # A retarget to the foreign socket is refused without mutation;
                # the owned exact binding is A on sA2, untouched by B. Identity
                # mismatch is a normal fail-closed candidate refusal here, while
                # a conflicting owner remains the exceptional case.
                self.assertFalse(
                    reg.retarget_claim_to_reconnected_session(owner, "sB")
                )
                handle = reg.session_handle_for_owned_session(owner, "sA2")
                self.assertIsNotNone(handle)
                self.assertTrue(pn_is_same_identity(handle.collector_pn, TARGET_PN))
                self.assertEqual(handle.session_id, "sA2")


# ---------------------------------------------------------------------------
# 3. The ONE bootstrap transaction: typed outcomes + clean lease/channel release.
# ---------------------------------------------------------------------------
class _FakeBootstrapChannel:
    """A scripted bootstrap channel over the ONE ledger -- no sockets.

    ``sessions()`` reads a mutable list (a scripted task mutates it after the
    trigger to model "fresh" arrivals). ``async_send_trigger`` records exactly one
    own send through the ledger; ``async_read_exact_session_identity`` returns the
    scripted authoritative read for a socket. ``block`` optionally hangs the read
    to exercise cancellation.
    """

    def __init__(self, ledger, sessions, reads, *, block: asyncio.Event | None = None):
        self.ledger = ledger
        self._sessions = sessions
        self._reads = reads
        self._block = block
        self.listener_available = True
        self.trigger_calls = 0
        self.open_calls = 0
        self.close_calls = 0

    async def async_open(self) -> None:
        self.open_calls += 1

    async def async_close(self) -> None:
        self.close_calls += 1

    def sessions(self):
        # Mimic the real channel: project the raw observations into the shape the
        # matcher reads, with has_strong_identity derived from the identity source.
        out = []
        for session in self._sessions:
            enriched = dict(session)
            enriched.setdefault(
                "has_strong_identity",
                bool(enriched.get("collector_pn"))
                and str(enriched.get("collector_identity_source") or "")
                in ("fc2_parameter_2", "at_dtupn"),
            )
            out.append(enriched)
        return tuple(out)

    async def async_send_trigger(self, route) -> None:
        self.trigger_calls += 1
        self.ledger.record(target="t", source="matrix")  # attributed to our attempt

    async def async_read_exact_session_identity(self, session):
        if self._block is not None:
            await self._block.wait()
        wire_available, pn = self._reads.get(
            session["session_id"], (True, session.get("collector_pn", ""))
        )
        return SimpleNamespace(wire_available=wire_available, collector_pn=pn)


class AdversarialBootstrapTransactionMatrix(unittest.IsolatedAsyncioTestCase):
    def _state(self):
        return StrategyTransitionRecoveryState.create(
            collector_pn=TARGET_PN, now=TS, trigger_target_host=SAME_IP,
            trigger_udp_port=58899, advertised_host=SAME_IP, advertised_port=18899,
            trigger_bind_host=SAME_IP, listener_bind_host=SAME_IP,
            local_listener_port=18899,
        )

    async def _run(self, channel, *, owner_id="entry", deadline=_HARD_DEADLINE):
        state = self._state()
        return await asyncio.wait_for(
            async_run_callback_bootstrap_transaction(
                registry=channel._registry, owner_id=owner_id, state=state,
                route=state.callback_route(), channel=channel, policy=_FAST_POLICY,
                poll_interval=0.02,
            ),
            timeout=deadline,
        )

    def _channel(self, sessions, reads, *, block=None):
        ledger = CallbackTriggerLedger()
        reg = CallbackSessionRegistry(sessions_source=lambda: sessions)
        ch = _FakeBootstrapChannel(ledger, sessions, reads, block=block)
        ch._registry = reg
        return ch

    async def _appear_after(self, sessions, items, delay=0.03):
        await asyncio.sleep(delay)
        sessions.extend(items)

    async def test_s2_foreign_baseline_then_target_certifies_target(self) -> None:
        # Scenario 2 at the transaction: B present at trigger (baseline), A arrives
        # fresh -> A is certified, B never owned, exactly one trigger.
        for source in STRONG_SOURCES:
            with self.subTest(identity_source=source):
                sessions = [_obs("sB", FOREIGN_PN, source=source)]  # baseline
                ch = self._channel(sessions, {"sA": (True, TARGET_PN)})
                task = asyncio.create_task(
                    self._appear_after(sessions, [_obs("sA", TARGET_PN, source=source)])
                )
                try:
                    out = await self._run(ch)
                finally:
                    await task
                self.assertEqual(out.kind, BOOTSTRAP_CERTIFIED, out.kind)
                self.assertEqual(out.certification.collector_pn, TARGET_PN)
                self.assertEqual(out.session_id, "sA")
                self.assertEqual(ch._registry.owner_for_pn(FOREIGN_PN), "")  # B unowned
                self.assertEqual(ch.trigger_calls, 1)
                self.assertEqual(ch.ledger.causality_owner(), "")  # lease freed

    async def test_s4_only_strong_foreign_fails_typed_and_owns_nothing(self) -> None:
        # Scenario 4: only a strong foreign B in the window -> typed mismatch. No
        # foreign ownership, no certified session, no proof; lease freed.
        sessions: list = []
        ch = self._channel(sessions, {"sB": (True, FOREIGN_PN)})
        task = asyncio.create_task(self._appear_after(sessions, [_obs("sB", FOREIGN_PN)]))
        try:
            out = await self._run(ch)
        finally:
            await task
        self.assertEqual(out.kind, BOOTSTRAP_IDENTITY_MISMATCH, out.kind)
        self.assertIsNone(out.certification)
        self.assertEqual(out.session_id, "")
        self.assertEqual(ch._registry.owner_for_pn(FOREIGN_PN), "")  # B never adopted
        self.assertEqual(ch._registry.claimed_session_id("entry"), "")  # no socket bound
        self.assertEqual(ch.trigger_calls, 1)
        self.assertEqual(ch.ledger.causality_owner(), "")

    async def test_s5_two_distinct_strong_is_typed_ambiguity(self) -> None:
        # Scenario 5: two distinct strong sockets in the window -> typed ambiguity,
        # nothing certified, no first/last/IP tiebreak.
        sessions: list = []
        ch = self._channel(sessions, {"sA": (True, TARGET_PN), "sB": (True, FOREIGN_PN)})
        task = asyncio.create_task(
            self._appear_after(sessions, [_obs("sA", TARGET_PN), _obs("sB", FOREIGN_PN)])
        )
        try:
            out = await self._run(ch)
        finally:
            await task
        self.assertEqual(out.kind, BOOTSTRAP_AMBIGUOUS, out.kind)
        self.assertIsNone(out.certification)
        self.assertEqual(ch._registry.claimed_session_id("entry"), "")
        self.assertEqual(ch.ledger.causality_owner(), "")

    async def test_s8_timeout_frees_the_lease_and_binds_no_socket(self) -> None:
        # Scenario 8 (timeout window): no fresh session -> typed no_session. The
        # causality lease is freed, the channel closed, no socket bound, no proof.
        sessions: list = []
        ch = self._channel(sessions, {})
        out = await self._run(ch)
        self.assertEqual(out.kind, BOOTSTRAP_NO_SESSION, out.kind)
        self.assertEqual(ch.ledger.causality_owner(), "")  # lease freed
        self.assertEqual(ch.close_calls, ch.open_calls)     # channel closed
        self.assertEqual(ch._registry.claimed_session_id("entry"), "")

    async def test_s8_cancellation_mid_read_frees_lease_and_channel(self) -> None:
        # Scenario 8 (cancellation window): cancel while the read blocks. The lease
        # is released and the channel closed before the cancel propagates; no
        # foreign session is adopted and the retry lease is free again.
        block = asyncio.Event()
        sessions = [_obs("sB", FOREIGN_PN)]  # a foreign socket sits in the window
        ch = self._channel(sessions, {"sB": (True, FOREIGN_PN)}, block=block)
        with self.assertRaises(asyncio.TimeoutError):
            await self._run(ch, deadline=0.4)  # hard local deadline -> cancels the read
        block.set()  # let any pending read unwind
        await asyncio.sleep(0)
        # Everything released: lease free, channel closed, nothing adopted.
        self.assertEqual(ch.ledger.causality_owner(), "")
        self.assertEqual(ch.close_calls, ch.open_calls)
        self.assertEqual(ch._registry.owner_for_pn(FOREIGN_PN), "")
        self.assertEqual(ch._registry.claimed_session_id("entry"), "")
        # The lease is genuinely reclaimable: a fresh attempt can acquire it.
        async with ch.ledger.causality_lease("retry", timeout=0.5):
            self.assertEqual(ch.ledger.causality_owner(), "retry")

    async def test_s8_release_frees_the_identity_intent_after_failure(self) -> None:
        # After a failed attempt the durable identity INTENT stays (by design), but
        # the owner can release it so the next attempt starts clean -- no leaked
        # ownership, and nothing foreign was ever adopted.
        sessions: list = []
        ch = self._channel(sessions, {})
        await self._run(ch)
        self.assertEqual(ch._registry.owner_for_pn(TARGET_PN), "entry")  # intent only
        self.assertEqual(ch._registry.claimed_session_id("entry"), "")   # no socket
        self.assertTrue(ch._registry.release("entry"))
        self.assertEqual(ch._registry.owner_for_pn(TARGET_PN), "")


if __name__ == "__main__":
    unittest.main()
