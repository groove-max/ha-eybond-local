"""Ownership handoff + fail-closed guards for the callback session registry.

Covers the config-flow -> permanent-entry ownership transfer (no gap, no double
owner, no leaked claim, durable PN survives a closed socket), and the invariants
that keep peer IP out of ownership: a PN-less claim can never own a session by
IP, two collectors behind one NAT IP stay independent, and short->full PN
enrichment never spawns a second owner.
"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector_identity import pn_is_same_identity
from custom_components.eybond_local.connection.session_registry import CallbackSessionRegistry

FULL_PN = "PNALPHA-FULL-0001"
SHORT_PN = "PNALPHA-FU"  # 10-char prefix of FULL_PN
OTHER_FULL_PN = "PNBETA-FULL-0002"


def _claim_protocol(registry, owner):
    """Read one claim's session_protocol through the registry's PUBLIC view.

    There is no claimed_session_protocol() accessor and this batch is not the
    place to add public API, so use the diagnostics view the registry already
    publishes rather than reaching into _claims.
    """

    for claim in registry.diagnostics()["claims"]:
        if claim["entry_id"] == owner:
            return claim["session_protocol"]
    return None


def _observed(session_id, pn, *, peer_ip="203.0.113.9", state="", source=""):
    return {
        "session_id": session_id,
        "peer_ip": peer_ip,
        "listener_port": 18899,
        "collector_pn": pn,
        "state": state,
        "protocol_shape": "",
        "collector_identity_source": source,
    }


class ClaimIdentityTests(unittest.TestCase):
    """Batch 8B.1: PN-only ownership intent that never scans/binds a socket."""

    def _registry(self, sessions=()):
        return CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))

    def test_creates_pn_only_claim_without_scanning_sessions(self) -> None:
        # A matching strong observed session exists, yet claim_identity binds
        # NO session id and does not enrich from the socket.
        reg = self._registry(
            [_observed("s1", FULL_PN, state="routed_framed", source="fc2_parameter_2")]
        )
        self.assertIsNone(reg.claim_identity("entry", SHORT_PN))
        self.assertEqual(reg.claimed_identity("entry"), SHORT_PN)  # NOT enriched
        self.assertEqual(reg.claimed_session_id("entry"), "")  # NOT bound

    def test_same_identity_enriches_short_to_full_only(self) -> None:
        reg = self._registry()
        reg.claim_identity("entry", SHORT_PN)
        reg.claim_identity("entry", FULL_PN)  # same identity, fuller spelling
        self.assertEqual(reg.claimed_identity("entry"), FULL_PN)
        self.assertEqual(reg.claimed_session_id("entry"), "")

    def test_session_bound_pn_less_claim_refuses_foreign_identity(self) -> None:
        # THE trust-boundary hole: a transient claim already bound to socket B
        # (which reports PN B) must NOT gain durable identity A -- one claim can
        # never own identity A AND physical socket B. Refuse before any mutation.
        reg = self._registry(
            [_observed("socket-B", OTHER_FULL_PN, state="routed_at_text", source="at_dtupn")]
        )
        reg.claim_session("entry", session_id="socket-B")
        self.assertEqual(reg.claimed_identity("entry"), "")  # PN-less transient
        self.assertEqual(reg.claimed_session_id("entry"), "socket-B")
        with self.assertRaises(ValueError):
            reg.claim_identity("entry", FULL_PN)
        # No mutation whatsoever.
        self.assertEqual(reg.claimed_identity("entry"), "")
        self.assertEqual(reg.claimed_session_id("entry"), "socket-B")
        self.assertEqual(reg.owner_for_pn(FULL_PN), "")

    def test_session_bound_pn_less_claim_refuses_even_matching_identity(self) -> None:
        # Even when the bound socket reports the SAME PN, identity-only intent is
        # not a second promotion path: that is promote_claim_to_full_pn's job.
        reg = self._registry(
            [_observed("socket-A", FULL_PN, state="routed_framed", source="fc2_parameter_2")]
        )
        reg.claim_session("entry", session_id="socket-A")
        with self.assertRaises(ValueError):
            reg.claim_identity("entry", FULL_PN)
        self.assertEqual(reg.claimed_identity("entry"), "")  # still PN-less
        self.assertEqual(reg.claimed_session_id("entry"), "socket-A")

    def test_protocol_only_pn_less_claim_is_refused(self) -> None:
        reg = self._registry()
        reg.claim("entry", session_protocol="eybond_framed")  # protocol-only, PN-less
        self.assertEqual(_claim_protocol(reg, "entry"), "eybond_framed")
        with self.assertRaises(ValueError):
            reg.claim_identity("entry", FULL_PN)
        self.assertEqual(reg.claimed_identity("entry"), "")  # unchanged
        self.assertEqual(_claim_protocol(reg, "entry"), "eybond_framed")  # unchanged

    def test_completely_empty_claim_accepts_pn(self) -> None:
        reg = self._registry()
        reg.claim("entry")  # a completely empty, unbound claim
        self.assertEqual(reg.claimed_identity("entry"), "")
        reg.claim_identity("entry", FULL_PN)  # allowed: nothing to contradict
        self.assertEqual(reg.claimed_identity("entry"), FULL_PN)
        self.assertEqual(reg.claimed_session_id("entry"), "")

    def test_preserves_existing_session_protocol_and_handoff(self) -> None:
        reg = self._registry(
            [_observed("s1", FULL_PN, state="routed_at_text", source="at_dtupn")]
        )
        reg.claim("verify:x", collector_pn=FULL_PN, session_id="s1")
        self.assertTrue(reg.prepare_handoff("verify:x", FULL_PN))
        # An identity-only re-claim with the short PN enriches nothing here (PN is
        # already full) and, crucially, resets neither the session nor the handoff.
        reg.claim_identity("verify:x", SHORT_PN)
        self.assertEqual(reg.claimed_session_id("verify:x"), "s1")
        self.assertEqual(
            reg.prepared_handoff_identity("verify:x", FULL_PN), FULL_PN
        )

    def test_foreign_identity_of_same_owner_raises_before_mutation(self) -> None:
        reg = self._registry()
        reg.claim_identity("entry", FULL_PN)
        with self.assertRaises(ValueError):
            reg.claim_identity("entry", OTHER_FULL_PN)
        self.assertEqual(reg.claimed_identity("entry"), FULL_PN)  # unchanged

    def test_pn_owned_by_another_owner_raises(self) -> None:
        reg = self._registry()
        reg.claim_identity("entry-A", FULL_PN)
        with self.assertRaises(ValueError):
            reg.claim_identity("entry-B", FULL_PN)
        self.assertEqual(reg.owner_for_pn(FULL_PN), "entry-A")

    def test_empty_pn_is_refused(self) -> None:
        reg = self._registry()
        with self.assertRaises(ValueError):
            reg.claim_identity("entry", "")


class HandoffLifecycleTests(unittest.TestCase):
    def test_prepare_then_complete_moves_claim_atomically_no_gap(self) -> None:
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        # A config-flow verification holds a transient full-PN claim under a
        # UNIQUE per-attempt owner (not PN-derived).
        registry.claim_session("callback_verification:aaaa", session_id="s1")
        registry.promote_claim_to_full_pn("callback_verification:aaaa", FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "callback_verification:aaaa")

        # prepare_handoff marks it committed; complete_handoff (setup) transfers
        # exactly the committed handoff to the durable entry_id -- no unowned gap.
        self.assertTrue(registry.prepare_handoff("callback_verification:aaaa", FULL_PN))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "callback_verification:aaaa")
        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-1"))

        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")
        self.assertEqual(registry.claimed_identity("callback_verification:aaaa"), "")
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.claimed_session_id("entry-1"), "s1")

    def test_setup_cannot_complete_an_uncommitted_verification_claim(self) -> None:
        # An ACTIVE (uncommitted) verification claim must never be stealable by
        # entry setup: complete_handoff returns False and leaves the claim.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session("callback_verification:aaaa", session_id="s1")
        registry.promote_claim_to_full_pn("callback_verification:aaaa", FULL_PN)
        # No prepare_handoff -> not committed.
        self.assertFalse(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "callback_verification:aaaa")
        self.assertEqual(registry.claimed_identity("entry-1"), "")

    def test_complete_handoff_preserves_durable_pn_when_socket_closed(self) -> None:
        sessions = [_observed("s1", FULL_PN, state="closed_disconnected", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session("callback_verification:aaaa", session_id="s1")
        registry.promote_claim_to_full_pn("callback_verification:aaaa", FULL_PN)
        registry.prepare_handoff("callback_verification:aaaa", FULL_PN)

        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        # A NEW same-PN session on a different socket is now owned by the entry.
        sessions[:] = [_observed("s2", FULL_PN, state="routed_framed", source="at_dtupn")]
        located = registry.owned_session_location("entry-1")
        self.assertIsNotNone(located)
        self.assertEqual(located.session_id, "s2")

    def test_prepare_handoff_rejects_pn_owned_by_another_owner(self) -> None:
        sessions = [
            _observed("s2", OTHER_FULL_PN, source="at_dtupn"),
            _observed("s3", "", source="framed_heartbeat"),  # weak, unowned
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        # entry-1 permanently owns OTHER_FULL_PN; a verification attempt holding a
        # different (weak) socket must not be able to prepare a handoff for it.
        registry.claim("entry-1", collector_pn=OTHER_FULL_PN)
        registry.claim_session("callback_verification:bbbb", session_id="s3")
        with self.assertRaises(ValueError):
            registry.prepare_handoff("callback_verification:bbbb", OTHER_FULL_PN)
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "entry-1")

    def test_complete_handoff_without_committed_claim_is_false(self) -> None:
        registry = CallbackSessionRegistry(sessions_source=lambda: [])
        self.assertFalse(registry.complete_handoff(FULL_PN, "entry-1"))

    def test_two_flows_same_pn_are_distinct_owners_second_conflicts(self) -> None:
        # Item 1: two verification attempts for one PN are DIFFERENT owners. The
        # second cannot promote/prepare the same identity, cannot change the
        # first's session, and cannot release the first's claim.
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            # Flow B's socket is still weak/unidentified, so it can be claimed
            # transiently; the conflict must surface when it tries to OWN the PN.
            _observed("s2", "", peer_ip="198.51.100.9", state="framed", source="framed_heartbeat"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session("callback_verification:aaaa", session_id="s1")
        registry.promote_claim_to_full_pn("callback_verification:aaaa", FULL_PN)

        # Flow B claims its own (weak) socket, then tries to promote/prepare the
        # same durable PN -> single-owner conflict, both paths.
        registry.claim_session("callback_verification:bbbb", session_id="s2")
        with self.assertRaises(ValueError):
            registry.promote_claim_to_full_pn("callback_verification:bbbb", FULL_PN)
        with self.assertRaises(ValueError):
            registry.prepare_handoff("callback_verification:bbbb", FULL_PN)

        # Flow A is untouched: still owns the PN on its original socket.
        self.assertEqual(registry.owner_for_pn(FULL_PN), "callback_verification:aaaa")
        self.assertEqual(registry.claimed_session_id("callback_verification:aaaa"), "s1")

        # Flow B releasing only frees ITS own claim, never flow A's.
        registry.release("callback_verification:bbbb")
        self.assertEqual(registry.owner_for_pn(FULL_PN), "callback_verification:aaaa")

    def test_release_frees_only_the_named_owner(self) -> None:
        sessions = [_observed("s1", FULL_PN, source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=FULL_PN)
        self.assertFalse(registry.release("callback_verification:ghost"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")


class FailClosedOwnershipTests(unittest.TestCase):
    def test_pn_less_claim_never_owns_a_session_by_ip(self) -> None:
        # A different collector is live at the same peer IP. A PN-less entry claim
        # must never adopt it -- peer IP is not ownership.
        sessions = [
            _observed("s1", FULL_PN, peer_ip="198.51.100.7", state="routed_framed", source="at_dtupn")
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("pn-less-entry")  # empty PN + empty session id
        self.assertEqual(registry.claimed_identity("pn-less-entry"), "")
        self.assertIsNone(registry.owned_session_location("pn-less-entry"))
        self.assertIsNone(registry.session_handle_for_entry("pn-less-entry"))
        # The live session stays unowned (its real owner would be by PN, not IP).
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")

    def test_two_collectors_one_peer_ip_stay_independent(self) -> None:
        sessions = [
            _observed("s1", FULL_PN, peer_ip="198.51.100.7", state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, peer_ip="198.51.100.7", state="routed_at_text", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-A", collector_pn=FULL_PN)
        registry.claim("entry-B", collector_pn=OTHER_FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-A")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "entry-B")
        self.assertEqual(registry.owned_session_location("entry-A").session_id, "s1")
        self.assertEqual(registry.owned_session_location("entry-B").session_id, "s2")

    def test_short_to_full_enrichment_keeps_a_single_owner(self) -> None:
        # A claim taken on the weak short PN enriches to the full PN when the
        # strong session is observed -- it must NOT create a second owner.
        sessions = [
            _observed("s1", SHORT_PN, source="framed_heartbeat"),
            _observed("s1b", FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=SHORT_PN)
        # The full PN is the same durable identity; a second claim by the full PN
        # would be the SAME owner, not a new device.
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")
        self.assertEqual(registry.owner_for_pn(SHORT_PN), "entry-1")
        # Enrich to the full PN in place.
        self.assertTrue(registry.promote_claim_to_full_pn("entry-1", FULL_PN))
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)


class ReconnectAndMismatchTests(unittest.TestCase):
    def test_full_pn_arriving_on_a_reconnected_socket_retargets_the_claim(self) -> None:
        # Scenario 2: the strong full PN is observed on a NEW/reconnected socket,
        # not the one first claimed. The claim retargets to the new socket for the
        # SAME durable identity (peer IP irrelevant).
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session("verify-owner", session_id="s1")
        registry.promote_claim_to_full_pn("verify-owner", FULL_PN)
        # s1 closes; a new same-PN socket s2 appears on a different peer port.
        sessions[:] = [
            _observed("s1", FULL_PN, state="closed_disconnected", source="at_dtupn"),
            _observed("s2", FULL_PN, peer_ip="198.51.100.9", state="routed_framed", source="at_dtupn"),
        ]
        self.assertTrue(registry.retarget_claim_to_reconnected_session("verify-owner", "s2"))
        self.assertEqual(registry.claimed_session_id("verify-owner"), "s2")
        # And it can then be handed off to the permanent entry.
        self.assertTrue(registry.prepare_handoff("verify-owner", FULL_PN))
        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")

    def test_overlapping_same_pn_socket_can_retarget_while_previous_is_live(self) -> None:
        """The registry owns an identity, not a one-socket EOF assumption."""

        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim(
            "verify-owner", collector_pn=FULL_PN, session_id="s1"
        )

        self.assertTrue(
            registry.retarget_claim_to_reconnected_session(
                "verify-owner", "s2"
            )
        )
        self.assertEqual(registry.claimed_session_id("verify-owner"), "s2")
        self.assertEqual(sessions[0]["state"], "routed_framed")

    def test_identity_mismatch_does_not_claim_another_collector(self) -> None:
        # Scenario 6: promoting a transient claim to a PN already owned by another
        # entry fails closed -- it never carves a different collector out from its
        # owner.
        sessions = [
            _observed("s1", FULL_PN, source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-owner", collector_pn=OTHER_FULL_PN)
        registry.claim_session("verify-owner", session_id="s1")
        with self.assertRaises(ValueError):
            registry.promote_claim_to_full_pn("verify-owner", OTHER_FULL_PN)
        # entry-owner still owns OTHER_FULL_PN; verify-owner never adopted it.
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "entry-owner")
        self.assertEqual(registry.claimed_identity("verify-owner"), "")

    def test_release_frees_the_temporary_claim(self) -> None:
        # Scenario 5: cancel/error/timeout releases the transient claim so the
        # session is free for the next attempt (or the runtime).
        sessions = [_observed("s1", FULL_PN, source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim_session("verify-owner", session_id="s1")
        self.assertTrue(registry.release("verify-owner"))
        self.assertEqual(registry.claimed_session_id("verify-owner"), "")
        self.assertFalse(registry.release("verify-owner"))  # idempotent

    def test_claiming_a_pn_removes_its_session_from_discovery(self) -> None:
        # Scenario 13: after an entry owns a PN, that collector's session is no
        # longer an unclaimed discovery candidate -- it disappears by PN claim,
        # not by peer IP. A different collector at the same IP stays discoverable.
        sessions = [
            _observed("s1", FULL_PN, peer_ip="198.51.100.7", state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, peer_ip="198.51.100.7", state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        before = {s.collector_pn for s in registry.list_unclaimed_sessions()}
        self.assertIn(FULL_PN, before)
        self.assertIn(OTHER_FULL_PN, before)

        registry.claim("entry-1", collector_pn=FULL_PN)

        after = {s.collector_pn for s in registry.list_unclaimed_sessions()}
        self.assertNotIn(FULL_PN, after)          # claimed -> not a candidate
        self.assertIn(OTHER_FULL_PN, after)       # the co-located collector remains


class HandoffIdentityGuardTests(unittest.TestCase):
    """prepare_handoff must never re-point a live claim at another collector.

    prefer_full_pn exists to enrich a TRANSIENT short PN into the durable full
    one of the SAME collector. Applied across identities it would silently turn a
    claim on A into a claim on B -- exactly how a config flow whose second
    attempt reached B could hand the entry A's claim (or vice versa). The claim
    is derived from a real session, so it is the registry's job to refuse.
    """

    def test_prepare_handoff_refuses_a_different_identity(self) -> None:
        # D. A session-derived claim on A; prepare_handoff(owner, B) must raise
        # and leave the claim untouched and unprepared.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:aaaa"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)

        with self.assertRaises(ValueError) as ctx:
            registry.prepare_handoff(owner, OTHER_FULL_PN)
        self.assertIn("handoff_identity_mismatch", str(ctx.exception))

        # The claim still stands for A, on A's session ...
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)
        self.assertEqual(registry.claimed_session_id(owner), "s1")
        # ... B was never adopted ...
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")
        # ... and handoff_pending stayed False, so nothing may be completed:
        # neither identity is certified, and setup can transfer nothing.
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), "")
        self.assertEqual(registry.prepared_handoff_identity(owner, OTHER_FULL_PN), "")
        self.assertFalse(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)

    def test_prepare_handoff_enriches_short_to_full_of_same_identity(self) -> None:
        # E. The legitimate use of prefer_full_pn: the claim was taken on a
        # transient SHORT PN and the attempt proved the full one. Same collector,
        # so the handoff is allowed and certifies the FULL PN.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:bbbb"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, SHORT_PN)
        self.assertEqual(registry.claimed_identity(owner), SHORT_PN)

        self.assertTrue(registry.prepare_handoff(owner, FULL_PN))

        # The certified identity is the FULL PN, and it is the same single owner.
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")

    def test_prepare_handoff_without_a_claim_reports_nothing_prepared(self) -> None:
        # Item 5: no claim under this owner -> False (not an exception, not a
        # silent success). The flow must not then believe it handed anything off.
        registry = CallbackSessionRegistry(sessions_source=lambda: [])
        self.assertFalse(registry.prepare_handoff("callback_verification:cccc", FULL_PN))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")
        self.assertEqual(
            registry.prepared_handoff_identity("callback_verification:cccc", FULL_PN), ""
        )


class PromoteIdentityGuardTests(unittest.TestCase):
    """promote_claim_to_full_pn enriches ONE identity -- it never switches.

    The bypass this closes: a claim owning A promoted to an unowned B replaced A
    outright, and prepare_handoff(owner, B) then passed its own identity check
    because the claim already said B. The registry must refuse at the point of
    mutation; the handoff check is only defense in depth.
    """

    def test_promote_to_a_different_unowned_identity_is_refused(self) -> None:
        # 1. Claim owns A. B is owned by NOBODY, so the single-owner guard cannot
        # fire -- this is exactly the hole. It must be refused on identity.
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:aaaa"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")  # B is unowned

        with self.assertRaises(ValueError) as ctx:
            registry.promote_claim_to_full_pn(owner, OTHER_FULL_PN)
        self.assertIn("promote_identity_mismatch", str(ctx.exception))

        # Claim untouched: still A, still A's session, still unprepared.
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)
        self.assertEqual(registry.claimed_session_id(owner), "s1")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), "")

        # And the handoff for B is refused too -- the smuggling route is closed at
        # both ends (this guard is what the flow-level batch relies on).
        with self.assertRaises(ValueError) as handoff_ctx:
            registry.prepare_handoff(owner, OTHER_FULL_PN)
        self.assertIn("handoff_identity_mismatch", str(handoff_ctx.exception))
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.prepared_handoff_identity(owner, OTHER_FULL_PN), "")

    def test_promote_short_to_full_enriches(self) -> None:
        # 2. The legitimate promotion: same identity, more complete spelling.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:bbbb"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, SHORT_PN)
        self.assertEqual(registry.claimed_identity(owner), SHORT_PN)

        self.assertTrue(registry.promote_claim_to_full_pn(owner, FULL_PN))
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)
        self.assertEqual(registry.claimed_session_id(owner), "s1")

    def test_promote_full_to_short_never_downgrades(self) -> None:
        # 3. A later WEAK observation (e.g. a heartbeat prefix) of the same
        # collector must not cost the claim its durable full identity.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:cccc"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)

        # Same identity -> no error, and the full PN survives.
        self.assertTrue(registry.promote_claim_to_full_pn(owner, SHORT_PN))
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)
        self.assertTrue(registry.prepare_handoff(owner, FULL_PN))
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), FULL_PN)

    def test_promote_to_the_exact_same_identity_is_idempotent(self) -> None:
        # 4. Re-promoting the same full PN is a safe no-op, and still reports the
        # postcondition (the claim stands on this identity) rather than "changed".
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:dddd"
        registry.claim_session(owner, session_id="s1")

        self.assertTrue(registry.promote_claim_to_full_pn(owner, FULL_PN))
        self.assertTrue(registry.promote_claim_to_full_pn(owner, FULL_PN))
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.claimed_session_id(owner), "s1")
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)

    def test_promote_preserves_the_single_owner_guard(self) -> None:
        # 5. B belongs to another owner: the pre-existing single-owner guard still
        # fires (and still names the conflict), and BOTH claims are untouched.
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-owner", collector_pn=OTHER_FULL_PN)
        owner = "callback_verification:eeee"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)

        with self.assertRaises(ValueError) as ctx:
            registry.promote_claim_to_full_pn(owner, OTHER_FULL_PN)
        self.assertIn("session_already_claimed", str(ctx.exception))

        # Both claims exactly as they were.
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "entry-owner")
        self.assertEqual(registry.claimed_identity("entry-owner"), OTHER_FULL_PN)
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.claimed_session_id(owner), "s1")

    def test_promote_reports_nothing_to_promote_without_raising(self) -> None:
        # Return-value contract: False is "nothing to promote", never "refused".
        registry = CallbackSessionRegistry(sessions_source=lambda: [])
        self.assertFalse(registry.promote_claim_to_full_pn("no-such-owner", FULL_PN))
        registry.claim_session("callback_verification:ffff", session_id="s1")
        self.assertFalse(registry.promote_claim_to_full_pn("callback_verification:ffff", ""))
        self.assertEqual(registry.claimed_identity("callback_verification:ffff"), "")


class ClaimRebindGuardTests(unittest.TestCase):
    """claim()/claim_session() may ENRICH an owner's claim -- never re-point it.

    Both used to overwrite the whole record, so a second call silently switched
    identity (or socket), dropped the durable PN and reset handoff_pending. Every
    legal transition has its own API: promote_claim_to_full_pn/reconcile_identity
    (identity enrichment), retarget_claim_to_reconnected_session (same collector,
    new socket), prepare_handoff/complete_handoff (ownership transfer), and
    release + claim (a new attempt or a different identity).
    """

    def test_claim_to_a_different_unowned_identity_is_refused(self) -> None:
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=FULL_PN)
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")  # B unowned

        with self.assertRaises(ValueError) as ctx:
            registry.claim("entry-1", collector_pn=OTHER_FULL_PN)
        self.assertIn("claim_identity_mismatch", str(ctx.exception))

        # A and every other field untouched; B never adopted.
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")
        self.assertEqual(registry.claimed_session_id("entry-1"), "s1")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")

    def test_claim_short_then_full_enriches(self) -> None:
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        # Claim by the short PN with NO session observed yet, so nothing enriches
        # it behind our back -- the second claim is what must do the enrichment.
        empty: list = []
        registry_no_sessions = CallbackSessionRegistry(sessions_source=lambda: empty)
        registry_no_sessions.claim("entry-1", collector_pn=SHORT_PN)
        self.assertEqual(registry_no_sessions.claimed_identity("entry-1"), SHORT_PN)
        self.assertTrue(registry_no_sessions.claim("entry-1", collector_pn=FULL_PN) is None)
        self.assertEqual(registry_no_sessions.claimed_identity("entry-1"), FULL_PN)

        # And with a live session, the same call still lands on the full PN.
        registry.claim("entry-2", collector_pn=SHORT_PN)
        registry.claim("entry-2", collector_pn=FULL_PN)
        self.assertEqual(registry.claimed_identity("entry-2"), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-2")

    def test_claim_full_then_short_never_downgrades(self) -> None:
        empty: list = []
        registry = CallbackSessionRegistry(sessions_source=lambda: empty)
        registry.claim("entry-1", collector_pn=FULL_PN)

        registry.claim("entry-1", collector_pn=SHORT_PN)  # same identity, weaker

        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")

    def test_repeat_exact_claim_preserves_session_protocol_and_handoff(self) -> None:
        # The wholesale replace also reset these three. A prepared handoff being
        # silently un-prepared would leave setup unable to complete it.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:aaaa"
        registry.claim(owner, collector_pn=FULL_PN, session_protocol="framed")
        self.assertTrue(registry.prepare_handoff(owner, FULL_PN))
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), FULL_PN)

        registry.claim(owner, collector_pn=FULL_PN)  # exact repeat

        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.claimed_session_id(owner), "s1")
        self.assertEqual(_claim_protocol(registry, owner), "framed")
        # handoff_pending survived: the handoff is still completable.
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), FULL_PN)
        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")

    def test_claim_with_a_different_explicit_session_is_refused(self) -> None:
        # An owner is never silently moved to another socket -- that is what
        # retarget_claim_to_reconnected_session is for (and it proves the old
        # socket is closed first).
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", FULL_PN, peer_ip="198.51.100.9", state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=FULL_PN, session_id="s1")

        with self.assertRaises(ValueError) as ctx:
            registry.claim("entry-1", collector_pn=FULL_PN, session_id="s2")
        self.assertIn("claim_session_mismatch", str(ctx.exception))
        self.assertEqual(registry.claimed_session_id("entry-1"), "s1")

    def test_claim_session_to_a_different_session_is_refused(self) -> None:
        sessions = [
            _observed("s1", "", state="routed_framed"),
            _observed("s2", "", state="routed_framed"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:bbbb"
        registry.claim_session(owner, session_id="s1")

        with self.assertRaises(ValueError) as ctx:
            registry.claim_session(owner, session_id="s2")
        self.assertIn("claim_session_mismatch", str(ctx.exception))
        self.assertEqual(registry.claimed_session_id(owner), "s1")

    def test_repeat_claim_session_is_idempotent(self) -> None:
        sessions = [_observed("s1", "", state="routed_framed")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:cccc"
        registry.claim_session(owner, session_id="s1")
        registry.claim_session(owner, session_id="s1")
        self.assertEqual(registry.claimed_session_id(owner), "s1")
        self.assertEqual(registry.claimed_identity(owner), "")

    def test_claim_session_never_demotes_a_durable_claim(self) -> None:
        # A durable-PN claim (possibly with a prepared handoff) must not be turned
        # back into a PN-less transient claim by a re-claim of its own session.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:dddd"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)
        self.assertTrue(registry.prepare_handoff(owner, FULL_PN))

        registry.claim_session(owner, session_id="s1")

        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), owner)
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), FULL_PN)

    def test_explicit_release_then_claim_b_is_allowed(self) -> None:
        # The sanctioned way to re-point an owner: release, then claim.
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=FULL_PN)

        self.assertTrue(registry.release("entry-1"))
        registry.claim("entry-1", collector_pn=OTHER_FULL_PN)

        self.assertEqual(registry.claimed_identity("entry-1"), OTHER_FULL_PN)
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "entry-1")

    def test_release_then_claim_session_b_is_allowed(self) -> None:
        sessions = [
            _observed("s1", "", state="routed_framed"),
            _observed("s2", "", state="routed_framed"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:eeee"
        registry.claim_session(owner, session_id="s1")
        self.assertTrue(registry.release(owner))
        registry.claim_session(owner, session_id="s2")
        self.assertEqual(registry.claimed_session_id(owner), "s2")

    def test_retarget_for_a_same_pn_reconnect_still_works(self) -> None:
        # The legal reconnect path must remain fully functional -- the guards
        # above must not make a reconnected collector unreachable.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:ffff"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)

        sessions[:] = [
            _observed("s1", FULL_PN, state="closed_disconnected", source="at_dtupn"),
            _observed("s2", FULL_PN, peer_ip="198.51.100.9", state="routed_framed", source="at_dtupn"),
        ]
        self.assertTrue(registry.retarget_claim_to_reconnected_session(owner, "s2"))
        self.assertEqual(registry.claimed_session_id(owner), "s2")
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertTrue(registry.prepare_handoff(owner, FULL_PN))
        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-1"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")


class ClaimEvidenceConflictTests(unittest.TestCase):
    """A declared identity and the socket it is bound to must be ONE collector.

    claim(owner, collector_pn=A, session_id=<B's socket>) is two contradicting
    pieces of evidence. It used to be accepted: the claim recorded identity A on
    collector B's physical session, prefer_full_pn was handed two DIFFERENT
    identities (its contract is "the fuller spelling of one identity", so it just
    returned the longer string), and the method handed back B's session as if it
    were A's.
    """

    def test_new_owner_declaring_a_with_bs_session_is_refused(self) -> None:
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)

        with self.assertRaises(ValueError) as ctx:
            registry.claim("entry-1", collector_pn=FULL_PN, session_id="s2")
        self.assertIn("claim_session_identity_mismatch", str(ctx.exception))

        # No claim was created at all -- not even a partial one.
        self.assertEqual(registry.claimed_identity("entry-1"), "")
        self.assertEqual(registry.claimed_session_id("entry-1"), "")
        self.assertEqual(registry.owner_for_pn(FULL_PN), "")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    def test_existing_owner_a_rebound_to_bs_session_is_refused(self) -> None:
        # The existing claim owns A but has NO socket bound yet (the documented
        # "recorded so a later-arriving session binds to it" shape). That is the
        # existing-owner path into the trust boundary: the earlier rebind guard
        # only fires once a session is already bound, so without this check B's
        # socket would bind straight onto A's claim.
        sessions: list = []
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=FULL_PN, session_protocol="framed")
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.claimed_session_id("entry-1"), "")

        # B dials in. No PN is declared now: the socket alone must not drag the
        # claim to B.
        sessions[:] = [
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn")
        ]
        with self.assertRaises(ValueError) as ctx:
            registry.claim("entry-1", session_id="s2")
        self.assertIn("claim_session_identity_mismatch", str(ctx.exception))

        # The previous claim is intact and B was never adopted or bound.
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.claimed_session_id("entry-1"), "")
        self.assertEqual(_claim_protocol(registry, "entry-1"), "framed")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")

    def test_existing_owner_with_a_bound_socket_still_refuses_another_socket(self) -> None:
        # The same intent one step later in the lifecycle: a socket IS bound, so
        # the explicit rebind guard answers first. Different reason, same refusal
        # and same untouched claim -- both gates must hold.
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        registry.claim("entry-1", collector_pn=FULL_PN, session_protocol="framed")
        self.assertEqual(registry.claimed_session_id("entry-1"), "s1")

        with self.assertRaises(ValueError) as ctx:
            registry.claim("entry-1", session_id="s2")
        self.assertIn("claim_session_mismatch", str(ctx.exception))

        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.claimed_session_id("entry-1"), "s1")
        self.assertEqual(_claim_protocol(registry, "entry-1"), "framed")
        self.assertEqual(registry.owner_for_pn(OTHER_FULL_PN), "")

    def test_declared_short_pn_with_that_collectors_session_enriches(self) -> None:
        # The legitimate shape: the evidence AGREES (same identity), so the socket
        # enriches the short PN to the full one.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)

        matched = registry.claim("entry-1", collector_pn=SHORT_PN, session_id="s1")

        self.assertIsNotNone(matched)
        self.assertEqual(matched.collector_pn, FULL_PN)
        self.assertEqual(registry.claimed_identity("entry-1"), FULL_PN)
        self.assertEqual(registry.claimed_session_id("entry-1"), "s1")
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-1")

    def test_session_only_transient_claim_still_works(self) -> None:
        # Unchanged behaviour: no declared PN, so there is nothing to contradict.
        sessions = [
            _observed("s1", "", state="routed_framed"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)

        registry.claim_session("callback_verification:aaaa", session_id="s1")
        self.assertEqual(registry.claimed_session_id("callback_verification:aaaa"), "s1")
        self.assertEqual(registry.claimed_identity("callback_verification:aaaa"), "")

        # And an identified-but-unowned socket may still be transiently claimed.
        registry.claim_session("callback_verification:bbbb", session_id="s2")
        self.assertEqual(registry.claimed_session_id("callback_verification:bbbb"), "s2")

    def test_reclaiming_a_session_now_reporting_another_collector_is_refused(self) -> None:
        # The owner proved A on s1 and prepared its handoff. s1 now reports B.
        # A repeat claim_session(s1) is NOT idempotent -- the evidence changed.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:cccc"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)
        self.assertTrue(registry.prepare_handoff(owner, FULL_PN))

        sessions[:] = [
            _observed("s1", OTHER_FULL_PN, state="routed_framed", source="at_dtupn")
        ]
        with self.assertRaises(ValueError) as ctx:
            registry.claim_session(owner, session_id="s1")
        self.assertIn("claim_session_identity_mismatch", str(ctx.exception))

        # Nothing mutated: identity, socket and the prepared handoff all stand.
        self.assertEqual(registry.claimed_identity(owner), FULL_PN)
        self.assertEqual(registry.claimed_session_id(owner), "s1")
        self.assertEqual(registry.prepared_handoff_identity(owner, FULL_PN), FULL_PN)

    def test_reclaiming_a_session_reporting_the_same_short_identity_is_idempotent(self) -> None:
        # The weak/short spelling of the SAME collector is not a contradiction.
        sessions = [_observed("s1", FULL_PN, state="routed_framed", source="at_dtupn")]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)
        owner = "callback_verification:dddd"
        registry.claim_session(owner, session_id="s1")
        registry.promote_claim_to_full_pn(owner, FULL_PN)

        sessions[:] = [_observed("s1", SHORT_PN, state="routed_framed")]
        registry.claim_session(owner, session_id="s1")  # must not raise

        self.assertEqual(registry.claimed_identity(owner), FULL_PN)  # no downgrade
        self.assertEqual(registry.claimed_session_id(owner), "s1")

    def test_claim_never_returns_a_session_of_another_identity(self) -> None:
        # Whatever claim() hands back must be the identity it claimed -- callers
        # read .collector_pn / .session_id off it.
        sessions = [
            _observed("s1", FULL_PN, state="routed_framed", source="at_dtupn"),
            _observed("s2", OTHER_FULL_PN, state="routed_framed", source="at_dtupn"),
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: sessions)

        returned = registry.claim("entry-1", collector_pn=FULL_PN)
        self.assertIsNotNone(returned)
        self.assertTrue(pn_is_same_identity(returned.collector_pn, FULL_PN))
        self.assertEqual(returned.session_id, "s1")

        # And the contradicting shape raises instead of returning B's session.
        with self.assertRaises(ValueError):
            registry.claim("entry-2", collector_pn=FULL_PN, session_id="s2")
        self.assertEqual(registry.claimed_identity("entry-2"), "")


if __name__ == "__main__":
    unittest.main()
