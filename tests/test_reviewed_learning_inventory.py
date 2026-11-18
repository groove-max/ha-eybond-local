"""Read-only support inspection shares the CLI contract, never its approval path."""

from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.shadow_learning import reviewed_recovery as review
from custom_components.eybond_local.support.shadow_learning import session
from tests.test_reviewed_learning_recovery import PN, history_fixture, plan_for


def files(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


class ReviewedInventoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def inspect(self, entry_id="entry", pn=PN):
        return review.inspect_reviewed_learning_history(self.root, entry_id=entry_id, collector_pn=pn)

    def test_absent_hold_and_missing_identity_create_no_files(self):
        self.assertEqual(self.inspect(), {"status": "no_hold"})
        for pn in ("", "../secret", "short"):
            self.assertEqual(self.inspect(pn=pn), {"status": "unavailable", "reason": "identity_unavailable"})
        self.assertEqual(list(self.root.iterdir()), [])

    def test_valid_history_matches_cli_without_changing_any_file(self):
        store, hold, states, _ = history_fixture(self.root, retries=2)
        before = files(self.root)
        expected = store.inspect(PN)
        with patch.object(store.__class__, "approve", side_effect=AssertionError("must not approve")), \
             patch.object(store.__class__, "apply", side_effect=AssertionError("must not recover")):
            result = self.inspect()
        self.assertEqual(result, expected)
        self.assertEqual(result["run_count"], 3)
        self.assertEqual([run["original"] for run in result["runs"]], [True, False, False])
        for secret in (PN, hold.entry_id, str(self.root), states[0].original_endpoint, states[-1].route_owner_id):
            self.assertNotIn(secret, json.dumps(result))
        self.assertEqual(files(self.root), before)

    def test_foreign_entry_is_refused_before_history_is_read(self):
        store, _, _, _ = history_fixture(self.root)
        before = files(self.root)
        with patch.object(store.__class__, "_history", side_effect=AssertionError("foreign history read")):
            self.assertEqual(self.inspect("other-entry"), {"status": "unavailable", "reason": "review_scope_mismatch"})
        self.assertEqual(files(self.root), before)

    def test_total_budget_is_bounded_without_changing_review_approval_policy(self):
        store, hold, _, evidence = history_fixture(self.root)
        before = files(self.root)
        exact = sum(path.stat().st_size for path in evidence)
        with patch.object(review, "_MAX_INSPECTION_TRACE_BYTES", exact):
            self.assertEqual(self.inspect()["status"], "review_required")
        with patch.object(review, "_MAX_INSPECTION_TRACE_BYTES", exact - 1):
            self.assertEqual(self.inspect(), {"status": "unavailable", "reason": "review_record_too_large"})
            self.assertEqual(len(plan_for(store, hold, evidence)["history"]), 2)
        self.assertEqual(files(self.root), before)

    def test_active_or_corrupt_history_is_not_reported_as_absent(self):
        _, _, states, _ = history_fixture(self.root)
        path = session.active_shadow_learning_state_path(self.root, entry_id="entry")
        for contents, reason in (
            (json.dumps(asdict(replace(states[-1], status="learning"))), "review_state_not_stopped"),
            ("{private broken contents", "history_invalid_or_unreadable"),
        ):
            with self.subTest(reason=reason):
                path.write_text(contents)
                before = files(self.root)
                self.assertEqual(self.inspect(), {"status": "unavailable", "reason": reason})
                self.assertEqual(files(self.root), before)

    def test_missing_original_remains_visible_as_an_inspection_error(self):
        history_fixture(self.root)
        next((session.shadow_learning_session_root(self.root) / "retained_runs").glob("*.json")).unlink()
        before = files(self.root)
        self.assertEqual(self.inspect(), {
            "status": "unavailable", "reason": "review_original_run_missing_or_duplicate",
        })
        self.assertEqual(files(self.root), before)

    def test_exception_text_is_not_exported(self):
        for exc in (OSError("/private/password"), ValueError("review_private_secret"), RuntimeError("token=secret")):
            with self.subTest(exc=type(exc)), patch.object(review.ReviewedLearningRecoveryStore, "inspect", side_effect=exc):
                self.assertEqual(self.inspect(), {"status": "unavailable", "reason": "history_invalid_or_unreadable"})

    def test_changed_scope_during_inspection_never_produces_an_inventory(self):
        store, hold, _, _ = history_fixture(self.root)
        original = store._history
        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            store._path(PN).write_text(json.dumps(replace(hold, study_id="changed").to_record()))
            return result
        with patch.object(review.ReviewedLearningRecoveryStore, "_history", side_effect=changed):
            self.assertEqual(self.inspect(), {"status": "unavailable", "reason": "cloud_learning_hold_changed"})


if __name__ == "__main__":
    unittest.main()
