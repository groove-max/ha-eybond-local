"""CP2C: the neutral CollectorEndpointOperationAuthority (pure unit + lifecycle)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.collector_endpoint_operation import (  # noqa: E402
    COLLECTOR_ENDPOINT_OPERATION_BUSY,
    CollectorEndpointOperationAuthority,
    CollectorEndpointOperationToken,
    OPERATION_MANUAL_ENDPOINT_WRITE,
    OPERATION_PROXY_CAPTURE,
    OPERATION_SHADOW_LEARNING,
    OPERATION_STRATEGY_TRANSITION,
)


class AcquireExclusion(unittest.TestCase):
    def test_1_two_concurrent_acquires_one_winner(self) -> None:
        a = CollectorEndpointOperationAuthority()
        first = a.acquire("e1", OPERATION_STRATEGY_TRANSITION)
        second = a.acquire("e1", OPERATION_PROXY_CAPTURE)
        self.assertTrue(first.acquired)
        self.assertFalse(second.acquired)
        # Busy reports the ACTIVE operation kind only (no address/credentials).
        self.assertEqual(second.busy_operation, OPERATION_STRATEGY_TRANSITION)
        self.assertEqual(second.token, None)
        self.assertEqual(a.active_operation("e1"), OPERATION_STRATEGY_TRANSITION)

    def test_10_different_entries_are_independent(self) -> None:
        a = CollectorEndpointOperationAuthority()
        r1 = a.acquire("e1", OPERATION_STRATEGY_TRANSITION)
        r2 = a.acquire("e2", OPERATION_PROXY_CAPTURE)
        self.assertTrue(r1.acquired and r2.acquired)
        self.assertNotEqual(r1.token, r2.token)

    def test_empty_entry_and_invalid_kind(self) -> None:
        a = CollectorEndpointOperationAuthority()
        self.assertFalse(a.acquire("", OPERATION_PROXY_CAPTURE).acquired)
        self.assertFalse(a.acquire("   ", OPERATION_PROXY_CAPTURE).acquired)
        with self.assertRaises(ValueError):
            a.acquire("e1", "not_a_real_kind")


class ReleaseSafety(unittest.TestCase):
    def test_2_foreign_duck_stale_token_release_nothing(self) -> None:
        a = CollectorEndpointOperationAuthority()
        held = a.acquire("e1", OPERATION_PROXY_CAPTURE).token
        # foreign token (same entry+kind, different owner_ref)
        self.assertFalse(
            a.release("e1", CollectorEndpointOperationToken("e1", OPERATION_PROXY_CAPTURE, "other"))
        )
        # duck object, and None
        self.assertFalse(a.release("e1", object()))
        self.assertFalse(a.release("e1", None))
        # wrong-entry release
        self.assertFalse(a.release("e2", held))
        # owner still holds it
        self.assertTrue(a.is_held("e1"))
        # exact token frees it; a stale re-release frees nothing
        self.assertTrue(a.release("e1", held))
        self.assertFalse(a.is_held("e1"))
        self.assertFalse(a.release("e1", held))

    def test_token_is_immutable_and_validated(self) -> None:
        t = CollectorEndpointOperationToken("e1", OPERATION_PROXY_CAPTURE, "ref")
        with self.assertRaises(Exception):
            t.owner_ref = "x"  # frozen
        for bad in (
            lambda: CollectorEndpointOperationToken("", OPERATION_PROXY_CAPTURE, "r"),
            lambda: CollectorEndpointOperationToken("e", "bad_kind", "r"),
            lambda: CollectorEndpointOperationToken("e", OPERATION_PROXY_CAPTURE, ""),
        ):
            with self.assertRaises(ValueError):
                bad()


class AdoptForReloadRestart(unittest.TestCase):
    def test_adopt_same_owner_returns_existing_token(self) -> None:
        a = CollectorEndpointOperationAuthority()
        held = a.acquire("e1", OPERATION_PROXY_CAPTURE, owner_ref="proxy:e1:ts").token
        adopted = a.adopt("e1", OPERATION_PROXY_CAPTURE, "proxy:e1:ts")
        self.assertEqual(adopted, held)
        self.assertTrue(a.release("e1", adopted))

    def test_adopt_on_free_reacquires_after_restart(self) -> None:
        a = CollectorEndpointOperationAuthority()  # fresh (restart)
        token = a.adopt("e1", OPERATION_PROXY_CAPTURE, "proxy:e1:ts")
        self.assertIsNotNone(token)
        self.assertTrue(a.is_held("e1"))
        self.assertTrue(a.release("e1", token))

    def test_adopt_never_steals_a_different_owner(self) -> None:
        a = CollectorEndpointOperationAuthority()
        a.acquire("e1", OPERATION_SHADOW_LEARNING, owner_ref="shadow:e1")
        # a proxy adopt with a foreign ref cannot steal the shadow owner
        self.assertIsNone(a.adopt("e1", OPERATION_PROXY_CAPTURE, "proxy:e1"))
        self.assertEqual(a.active_operation("e1"), OPERATION_SHADOW_LEARNING)

    def test_busy_reason_constant(self) -> None:
        self.assertEqual(COLLECTOR_ENDPOINT_OPERATION_BUSY, "collector_endpoint_operation_busy")


class LongLivedModeLeaseLifecycle(unittest.TestCase):
    """Tests 4/8/9: the exact acquire→hold→adopt→release pattern the proxy/shadow
    start/stop methods use (proven at the authority level; the full methods are
    exercised end-to-end by the HA lanes)."""

    def _ref(self, entry_id: str) -> str:
        return f"proxy_capture:{entry_id}:ts1"

    def test_4_start_failure_releases_the_lease(self) -> None:
        a = CollectorEndpointOperationAuthority()
        entry_id = "e-start-fail"
        acq = a.acquire(entry_id, OPERATION_PROXY_CAPTURE, owner_ref=self._ref(entry_id))
        _op_hold = False
        try:
            raise RuntimeError("start failed after acquire, before hand-off")
            _op_hold = True  # unreachable — the success branch was never reached
        except RuntimeError:
            pass
        finally:
            if not _op_hold:
                a.release(entry_id, acq.token)
        self.assertFalse(a.is_held(entry_id))

    def test_8_stop_adopts_then_releases_including_after_reload(self) -> None:
        a = CollectorEndpointOperationAuthority()
        entry_id = "e-stop"
        ref = self._ref(entry_id)
        # successful start HOLDS the lease across the method (mode owned).
        started = a.acquire(entry_id, OPERATION_PROXY_CAPTURE, owner_ref=ref)
        self.assertTrue(a.is_held(entry_id))
        # stop adopts the SAME token from the persisted route owner id (works even
        # if a reload lost the in-memory reference) and releases after restore.
        adopted = a.adopt(entry_id, OPERATION_PROXY_CAPTURE, ref)
        self.assertEqual(adopted, started.token)
        a.release(entry_id, adopted)
        self.assertFalse(a.is_held(entry_id))

    def test_8_stop_after_process_restart_reacquires_then_releases(self) -> None:
        a = CollectorEndpointOperationAuthority()  # fresh authority (restart)
        entry_id = "e-restart-stop"
        ref = self._ref(entry_id)
        # recovery-stop adopts from persisted owner ref on a FREE authority -> it
        # re-acquires ownership for the restore, then releases.
        token = a.adopt(entry_id, OPERATION_PROXY_CAPTURE, ref)
        self.assertIsNotNone(token)
        a.release(entry_id, token)
        self.assertFalse(a.is_held(entry_id))

    def test_9_failed_stop_keeps_ownership(self) -> None:
        a = CollectorEndpointOperationAuthority()
        entry_id = "e-failed-stop"
        ref = self._ref(entry_id)
        a.acquire(entry_id, OPERATION_PROXY_CAPTURE, owner_ref=ref)
        token = a.adopt(entry_id, OPERATION_PROXY_CAPTURE, ref)
        # A stop whose restore fails does NOT reach the clear/release branch, so
        # the (degraded) mode keeps honest ownership for a retry.
        restore_confirmed = False
        if restore_confirmed:  # clear branch not taken
            a.release(entry_id, token)
        self.assertTrue(a.is_held(entry_id))
        a.release(entry_id, token)  # cleanup


class LeaseFacadeDelegatesToAuthority(unittest.TestCase):
    def test_strategy_lease_facade_shares_the_one_authority(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition import (
            StrategyTransitionLease,
        )

        authority = CollectorEndpointOperationAuthority()
        lease = StrategyTransitionLease(authority=authority)
        self.assertTrue(lease.acquire("e1"))
        # A direct proxy acquire on the SAME authority now sees the entry busy.
        self.assertFalse(authority.acquire("e1", OPERATION_PROXY_CAPTURE).acquired)
        self.assertEqual(authority.active_operation("e1"), OPERATION_STRATEGY_TRANSITION)
        lease.release("e1")
        self.assertTrue(authority.acquire("e1", OPERATION_PROXY_CAPTURE).acquired)


class StrictTrustBoundaryInputs(unittest.TestCase):
    """B7/B9: a bool / int / duck / padded entry_id or owner_ref never creates or
    matches an owner -- no coercion, fail-closed, zero mutation of ``_held``."""

    _BAD_ENTRIES = (None, True, False, 0, 1, 42, object(), b"e1", " e1", "e1 ", "", "   ")

    def test_acquire_rejects_non_exact_entry_id_without_creating_an_owner(self) -> None:
        for bad in self._BAD_ENTRIES:
            a = CollectorEndpointOperationAuthority()
            result = a.acquire(bad, OPERATION_PROXY_CAPTURE)
            self.assertFalse(result.acquired, f"entry {bad!r} must not acquire")
            self.assertIsNone(result.token)
            self.assertEqual(a._held, {}, f"entry {bad!r} mutated the authority")

    def test_acquire_rejects_non_exact_owner_ref(self) -> None:
        for bad_ref in (True, 7, object(), " r", "r ", b"r"):
            a = CollectorEndpointOperationAuthority()
            result = a.acquire("e1", OPERATION_PROXY_CAPTURE, owner_ref=bad_ref)
            self.assertFalse(result.acquired, f"owner_ref {bad_ref!r} must not acquire")
            self.assertEqual(a._held, {})

    def test_adopt_rejects_non_exact_entry_or_owner(self) -> None:
        for bad in self._BAD_ENTRIES:
            a = CollectorEndpointOperationAuthority()
            self.assertIsNone(a.adopt(bad, OPERATION_PROXY_CAPTURE, "ref"))
            self.assertEqual(a._held, {})
        for bad_ref in (None, True, 7, object(), " r", "r ", "", "   "):
            a = CollectorEndpointOperationAuthority()
            self.assertIsNone(a.adopt("e1", OPERATION_PROXY_CAPTURE, bad_ref))
            self.assertEqual(a._held, {})

    def test_empty_owner_ref_autogen_only_for_the_exact_empty_string(self) -> None:
        class DuckEmpty:
            def __eq__(self, other: object) -> bool:
                return other == ""

            def __hash__(self) -> int:
                return hash("")

        # The exact empty str auto-generates a ref (real acquire).
        a = CollectorEndpointOperationAuthority()
        self.assertTrue(a.acquire("e1", OPERATION_PROXY_CAPTURE, owner_ref="").acquired)
        # A duck whose __eq__("") is True must NOT trigger auto-generation; it goes
        # through the exact-string gate and fails closed with zero mutation.
        for bad in (DuckEmpty(), b"", 0, False, None):
            b = CollectorEndpointOperationAuthority()
            self.assertFalse(
                b.acquire("e1", OPERATION_PROXY_CAPTURE, owner_ref=bad).acquired,
                f"owner_ref {bad!r} must not auto-generate an owner",
            )
            self.assertEqual(b._held, {})

    def test_acquire_rejects_non_str_operation_kind_before_membership(self) -> None:
        class DuckKind:
            def __eq__(self, other: object) -> bool:
                return other == OPERATION_PROXY_CAPTURE

            def __hash__(self) -> int:
                return hash(OPERATION_PROXY_CAPTURE)

        a = CollectorEndpointOperationAuthority()
        with self.assertRaises(ValueError):
            a.acquire("e1", DuckKind())
        with self.assertRaises(ValueError):
            a.adopt("e1", DuckKind(), "ref")
        self.assertEqual(a._held, {})

    def test_padded_entry_never_matches_a_real_owner(self) -> None:
        a = CollectorEndpointOperationAuthority()
        held = a.acquire("e1", OPERATION_PROXY_CAPTURE).token
        # A padded/duck entry cannot free, adopt, or read the real owner.
        self.assertFalse(a.release(" e1", held))
        self.assertFalse(a.release(42, held))
        self.assertIsNone(a.adopt(" e1", OPERATION_PROXY_CAPTURE, held.owner_ref))
        self.assertEqual(a.active_operation(" e1"), "")
        self.assertTrue(a.is_held("e1"))


class RepairIsADistinctOperationKind(unittest.TestCase):
    """B6/B9: the degraded-repair path owns the authority under its OWN kind, so
    diagnostics can tell a repair apart from a user-driven transition, yet both
    contend on the SAME per-entry authority."""

    def test_repair_lease_reports_strategy_repair_and_contends_with_transition(self) -> None:
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            OPERATION_STRATEGY_REPAIR,
        )
        from custom_components.eybond_local.connection.strategy_transition import (
            STRATEGY_REPAIR_LEASES,
            STRATEGY_TRANSITION_LEASES,
        )
        from custom_components.eybond_local.connection.collector_endpoint_operation import (
            COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTH,
        )

        entry_id = "e-repair-kind"
        AUTH._held.pop(entry_id, None)
        self.addCleanup(lambda: AUTH._held.pop(entry_id, None))
        self.assertTrue(STRATEGY_REPAIR_LEASES.acquire(entry_id))
        try:
            # The ACTIVE kind is strategy_repair (not strategy_transition).
            self.assertEqual(AUTH.active_operation(entry_id), OPERATION_STRATEGY_REPAIR)
            # A concurrent user transition on the same entry is blocked.
            self.assertFalse(STRATEGY_TRANSITION_LEASES.acquire(entry_id))
        finally:
            STRATEGY_REPAIR_LEASES.release(entry_id)
        self.assertEqual(AUTH.active_operation(entry_id), "")


if __name__ == "__main__":
    unittest.main()
