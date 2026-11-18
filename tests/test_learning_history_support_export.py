"""The support ZIP includes complete retained history without granting recovery."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from test_reviewed_learning_recovery import PN, history_fixture
from custom_components.eybond_local.support.package import export_support_package
from custom_components.eybond_local.support.shadow_learning import reviewed_recovery as review


class HistoryExportTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.store, self.hold, self.states, self.traces = history_fixture(self.root, retries=4)
        self.bundle = {"entry": {"entry_id": "entry", "data": {"collector_pn": PN}},
                       "runtime": {"collector": {"collector_pn": PN}}}

    def export(self):
        return export_support_package(config_dir=self.root, entry_id="entry", entry_title="Synthetic",
            support_bundle=self.bundle, raw_capture=None, fixture=None, anonymized_fixture=None).path

    def test_all_five_runs_export_with_hash_binding_and_redaction_without_mutation(self):
        for trace in self.traces:
            events = [json.loads(line) for line in trace.read_text().splitlines()]
            events[1]["payload"] = {"password": "private-password-sentinel", "token": "private-token-sentinel",
                                     "account": "private-account-sentinel", "collector_pn": PN}
            trace.write_text("\n".join(json.dumps(event) for event in events) + "\n")
        before = {p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        inventory, snapshots = self.store.export_evidence(PN, entry_id="entry")
        self.assertEqual(inventory, self.store.inspect(PN, entry_id="entry"))
        self.assertEqual(snapshots, tuple(p.read_bytes() for p in self.traces))
        with zipfile.ZipFile(self.export()) as archive:
            summary = json.loads(archive.read("manifest.json"))["archive_members"]["learning_history"]
            self.assertEqual(summary["status"], "exported")
            self.assertEqual(summary["run_count"], 5)
            index = json.loads(archive.read(summary["index"]))
            self.assertEqual(index["history_sha256"], inventory["history_sha256"])
            for run, trace in zip(index["runs"], self.traces, strict=True):
                self.assertEqual(run["trace_sha256"], hashlib.sha256(trace.read_bytes()).hexdigest())
                events = [json.loads(line) for line in archive.read(run["trace_member"]).splitlines()]
                self.assertEqual(events[0]["kind"], "shadow_session_manifest")
                self.assertEqual(events[-1]["kind"], "shadow_completion_summary")
                self.assertEqual(events[1]["run"], run["index"])
            shared = b"\n".join(archive.read(name) for name in archive.namelist()).decode()
            for private in (PN, "private-password-sentinel", "private-token-sentinel", "private-account-sentinel",
                            str(self.root)):
                self.assertNotIn(private, shared)
        for path, data in before.items():
            self.assertEqual(path.read_bytes(), data)
        self.assertEqual(self.store.load(PN), self.hold)
        self.assertFalse(self.store.receipt_present(self.states[-1]))

    def test_missing_trace_or_exceeded_total_budget_never_export_partial_history(self):
        for budget in (1, 100):
            with self.subTest(budget=budget), patch.object(review, "_MAX_INSPECTION_TRACE_BYTES", budget):
                with zipfile.ZipFile(self.export()) as archive:
                    self.assertFalse(any(name.startswith("evidence/learning_history/") for name in archive.namelist()))
                    summary = json.loads(archive.read("manifest.json"))["archive_members"]["learning_history"]
                    self.assertEqual(summary["status"], "unavailable")
        self.traces[1].unlink()
        with zipfile.ZipFile(self.export()) as archive:
            self.assertIn("support_bundle.json", archive.namelist())
            self.assertFalse(any(name.startswith("evidence/learning_history/") for name in archive.namelist()))

    def test_mismatched_identity_and_private_read_error_stay_out_of_archive(self):
        # Existing allowlisted synthetic PN, distinct from history_fixture's PN.
        self.bundle["runtime"]["collector"]["collector_pn"] = "E5000099990001"
        with patch.object(review.ReviewedLearningRecoveryStore, "export_evidence") as read:
            with zipfile.ZipFile(self.export()) as archive:
                summary = json.loads(archive.read("manifest.json"))["archive_members"]["learning_history"]
                self.assertEqual(summary["reason"], "identity_mismatch")
            read.assert_not_called()
        self.bundle["runtime"]["collector"]["collector_pn"] = PN
        with patch.object(review.ReviewedLearningRecoveryStore, "export_evidence", side_effect=OSError("private-path-secret")):
            with zipfile.ZipFile(self.export()) as archive:
                self.assertNotIn(b"private-path-secret", archive.read("manifest.json"))
                self.assertFalse(any(name.startswith("evidence/learning_history/") for name in archive.namelist()))

    def test_trace_change_after_snapshot_does_not_change_exported_hashed_bytes(self):
        original_check = self.store.require_exact_hold

        def change_trace(hold):
            self.traces[0].write_text("later content; not the inspected snapshot")
            return original_check(hold)

        with patch.object(self.store, "require_exact_hold", side_effect=change_trace):
            inventory, snapshots = self.store.export_evidence(PN, entry_id="entry")
        self.assertNotEqual(snapshots[0], self.traces[0].read_bytes())
        self.assertEqual(hashlib.sha256(snapshots[0]).hexdigest(), inventory["runs"][0]["trace_sha256"])


if __name__ == "__main__":
    unittest.main()
