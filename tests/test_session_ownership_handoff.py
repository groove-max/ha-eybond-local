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

from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)

FULL_PN = "PNALPHA-FULL-0001"
SHORT_PN = "PNALPHA-FU"  # 10-char prefix of FULL_PN
OTHER_FULL_PN = "PNBETA-FULL-0002"


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


if __name__ == "__main__":
    unittest.main()
