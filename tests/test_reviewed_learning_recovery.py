"""Explicit historical review is scope-bound, read-only until approved, resumable."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from custom_components.eybond_local.connection.cloud_learning_protection import CloudLearningHold
from custom_components.eybond_local.support.shadow_learning import session
from custom_components.eybond_local.support.shadow_learning.reviewed_recovery import ReviewedLearningRecoveryStore

PN = "E5000020000000"


def history_fixture(root, *, entry_id="entry", pn=PN, retries=1):
    store = ReviewedLearningRecoveryStore(root)
    trace_root = session.shadow_learning_session_root(root)
    trace_root.mkdir(parents=True, exist_ok=True)
    hold = CloudLearningHold(entry_id, "original", pn, "smartess", "2026-11-09T06:01:20Z")
    store.create(hold=hold)
    states, evidence = [], []
    for index in range(retries + 1):
        owner = "original" if not index else f"retry_{index}"
        trace = trace_root / (owner + ".jsonl")
        trace.write_text("\n".join(json.dumps(event) for event in (
            {"kind": "shadow_session_manifest", "entry_id": entry_id, "collector_pn": pn, "session_id": owner},
            {"kind": "test-only-reviewed-evidence", "run": index},
            {"kind": "shadow_session_stopped"},
            {"kind": "shadow_completion_summary", "payload": {"complete": False, "pending": False}},
        )) + "\n")
        evidence.append(trace)
        state = session.build_shadow_learning_session_state(
            entry_id=entry_id, collector_pn=pn, route_owner_id="shadow_learning:" + owner,
            trace_path=str(trace), original_endpoint="cloud.invalid,18899,TCP" if not index else "192.0.2.1,8899,TCP",
            proxy_endpoint="192.0.2.1,8899,TCP", upstream_endpoint="cloud.invalid,18899,TCP",
            restore_required=not index, started_at=f"2026-11-{9+index:02}T06:01:00Z",
            updated_at=f"2026-11-{9+index:02}T06:02:00Z", status="restore_failed",
            protection_version=1, cloud_source="smartess", cloud_commands_possible=True)
        if states:
            session.replace_held_shadow_learning_session(config_dir=root, previous=states[-1],
                state=replace(state, status="preflight"), hold=hold)
        session.save_shadow_learning_session_state(config_dir=root, state=state, entry_id=entry_id)
        states.append(state)
    return store, hold, states, evidence


def plan_for(store, hold, evidence):
    return store.prepare(collector_pn=hold.collector_pn, entry_id=hold.entry_id,
                         study_id=hold.study_id, evidence_files=evidence, command_counts=[90]*len(evidence))


def approve(store, plan):
    store.approve(plan=plan, expected_digest=store.review_digest(plan), confirmed=True)


class ReviewedHistoryTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.store, self.hold, self.states, self.evidence = history_fixture(self.root)

    def state(self):
        return session.load_shadow_learning_session_state(self.root, entry_id="entry")

    def test_preview_no_write_approval_does_not_retire_and_apply_restores_original(self):
        before = {str(p): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        plan = plan_for(self.store, self.hold, self.evidence)
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.root.rglob("*") if p.is_file()})
        with self.assertRaisesRegex(ValueError, "explicit_terminal"):
            self.store.approve(plan=plan, expected_digest=self.store.review_digest(plan), confirmed=False)
        with self.assertRaisesRegex(ValueError, "explicit_terminal"):
            self.store.approve(plan=plan, expected_digest="0"*64, confirmed=True)
        approve(self.store, plan)
        approve(self.store, plan)  # idempotent exact approval
        self.assertEqual(self.state(), self.states[-1])
        self.assertEqual(self.store.load(PN), self.hold)
        corrected, receipt = self.store.apply(expected_state=self.states[-1])
        self.assertEqual(corrected.route_owner_id, self.states[-1].route_owner_id)
        self.assertEqual(corrected.original_endpoint, self.states[0].original_endpoint)
        self.assertTrue(corrected.restore_required)
        self.assertFalse(corrected.cloud_commands_possible)
        self.assertIsNone(self.store.load(PN))
        self.assertEqual(self.store.require_retired_review(corrected), receipt)
        self.assertFalse((self.store._root / "completed_receipts").exists())
        self.assertFalse((self.store._root / "no_request_receipts").exists())

    def test_changed_trace_or_extra_retry_invalidates_approval_without_retirement(self):
        plan = plan_for(self.store, self.hold, self.evidence)
        approve(self.store, plan)
        self.evidence[0].write_text("changed")
        with self.assertRaises(ValueError):
            self.store.apply(expected_state=self.states[-1])
        self.assertEqual(self.state(), self.states[-1])
        self.assertEqual(self.store.load(PN), self.hold)

    def test_new_retry_after_approval_has_no_receipt_and_old_current_is_rejected(self):
        plan = plan_for(self.store, self.hold, self.evidence); approve(self.store, plan)
        previous = self.states[-1]
        newer = replace(previous, route_owner_id="shadow_learning:new_retry", status="preflight",
                        started_at="2026-11-12T06:01:00Z")
        session.replace_held_shadow_learning_session(config_dir=self.root, previous=previous, state=newer, hold=self.hold)
        self.assertFalse(self.store.receipt_present(newer))
        with self.assertRaisesRegex(ValueError, "scope_changed|state_not_stopped"):
            self.store.apply(expected_state=previous)
        self.assertEqual(self.store.load(PN), self.hold)

    def test_missing_original_and_wrong_expected_count_cannot_be_approved(self):
        with self.assertRaisesRegex(ValueError, "every_run"):
            plan_for(self.store, self.hold, self.evidence[:1])
        root = session.shadow_learning_session_root(self.root) / "retained_runs"
        next(root.glob("*.json")).unlink()
        with self.assertRaisesRegex(ValueError, "original_run"):
            plan_for(self.store, self.hold, self.evidence)

    def test_malformed_foreign_missing_and_external_trace_are_rejected(self):
        for fault in ("entry", "pn", "type", "active", "outside", "symlink", "missing", "duplicate"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as td:
                root = Path(td); store, hold, states, evidence = history_fixture(root)
                path = session.active_shadow_learning_state_path(root, entry_id="entry")
                raw = json.loads(path.read_text())
                if fault == "entry": raw["entry_id"] = "foreign"
                if fault == "pn": raw["collector_pn"] = "E5000099990002"
                if fault == "type": raw["cloud_commands_possible"] = "true"
                if fault == "active": raw["status"] = "learning"
                if fault == "outside": raw["trace_path"] = str(Path(td) / "outside.jsonl")
                if fault == "symlink":
                    evidence[-1].unlink(); evidence[-1].symlink_to(evidence[0])
                if fault == "missing": evidence[-1].unlink()
                if fault == "duplicate": raw["route_owner_id"] = states[0].route_owner_id
                path.write_text(json.dumps(raw))
                with self.assertRaises((ValueError, OSError)):
                    plan_for(store, hold, evidence)
                self.assertEqual(store.load(PN), hold)

    def test_corrupt_or_replaced_hold_and_receipt_never_restore(self):
        plan = plan_for(self.store, self.hold, self.evidence); approve(self.store, plan)
        path = self.store._path(PN)
        path.write_text(json.dumps(replace(self.hold, study_id="another").to_record()))
        with self.assertRaisesRegex(ValueError, "hold_changed"):
            self.store.apply(expected_state=self.states[-1])
        self.assertTrue(self.state().cloud_commands_possible)
        path.write_text(json.dumps(self.hold.to_record()))
        self.store._review_path(self.states[-1]).write_text('{"schema_version":1,"schema_version":2}')
        with self.assertRaises(ValueError):
            self.store.apply(expected_state=self.states[-1])
        self.assertEqual(self.store.load(PN), self.hold)

    def test_journal_write_failure_keeps_hold_then_retry_succeeds(self):
        approve(self.store, plan_for(self.store, self.hold, self.evidence))
        with patch.object(session, "atomic_write_record", side_effect=OSError("disk")):
            with self.assertRaises(OSError): self.store.apply(expected_state=self.states[-1])
        self.assertEqual(self.state(), self.states[-1])
        self.assertEqual(self.store.load(PN), self.hold)
        corrected, _ = self.store.apply(expected_state=self.state())
        self.assertTrue(corrected.restore_required)

    def test_receipt_history_types_are_not_coerced_by_dict_equality(self):
        plan = plan_for(self.store, self.hold, self.evidence)
        approve(self.store, plan)
        receipt_path = self.store._review_path(self.states[-1])
        for key, value in (("restore_required", 0), ("cloud_commands_possible", 1), ("protection_version", True)):
            with self.subTest(key=key):
                corrupted = json.loads(json.dumps(plan))
                corrupted["history"][-1]["state"][key] = value
                receipt_path.write_text(json.dumps(corrupted))
                with self.assertRaisesRegex(ValueError, "review_state_type"):
                    self.store.apply(expected_state=self.states[-1])
                self.assertEqual(self.state(), self.states[-1])
                self.assertEqual(self.store.load(PN), self.hold)

    def test_crash_after_journal_or_hold_rename_is_idempotent(self):
        for after_archive in (False, True):
            with self.subTest(after_archive=after_archive), tempfile.TemporaryDirectory() as td:
                store, hold, states, evidence = history_fixture(Path(td))
                approve(store, plan_for(store, hold, evidence))
                archive = store._archive_hold
                def fail(hold):
                    if after_archive: archive(hold)
                    raise OSError("simulated crash boundary")
                with patch.object(store, "_archive_hold", side_effect=fail):
                    with self.assertRaises(OSError): store.apply(expected_state=states[-1])
                current = store._current(hold.entry_id)
                self.assertFalse(current.cloud_commands_possible)
                self.assertEqual(current.original_endpoint, states[0].original_endpoint)
                corrected, _ = store.apply(expected_state=current)
                store.require_retired_review(corrected)

    def test_single_original_and_multiple_retries_share_the_same_contract(self):
        for retries in (0, 2):
            with self.subTest(retries=retries), tempfile.TemporaryDirectory() as td:
                store, hold, states, evidence = history_fixture(Path(td), retries=retries)
                approve(store, plan_for(store, hold, evidence))
                corrected, _ = store.apply(expected_state=states[-1])
                self.assertEqual(corrected.original_endpoint, states[0].original_endpoint)

    def test_missing_intermediate_run_invalidates_recorded_review(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); store, hold, states, evidence = history_fixture(root, retries=2)
            approve(store, plan_for(store, hold, evidence))
            key = hashlib.sha256(states[1].route_owner_id.encode()).hexdigest()
            (session.shadow_learning_session_root(root) / "retained_runs" / (key + ".json")).unlink()
            with self.assertRaisesRegex(ValueError, "history_changed"):
                store.apply(expected_state=states[-1])
            self.assertEqual(store.load(PN), hold)

    def test_share_trace_or_unfinished_lifecycle_is_not_on_host_authority(self):
        trace = self.evidence[0]
        original = trace.read_text()
        for fault in ("masked_pn", "other_run", "not_stopped", "pending", "after_stop", "duplicate_manifest", "duplicate_stop"):
            with self.subTest(fault=fault):
                events = [json.loads(line) for line in original.splitlines()]
                if fault == "masked_pn": events[0]["collector_pn"] = "E5000*********2461"
                if fault == "other_run": events[0]["session_id"] = "another"
                if fault == "not_stopped": events.pop(-2)
                if fault == "pending": events[-1]["payload"]["pending"] = True
                if fault == "after_stop": events.append({"kind": "new command"})
                if fault == "duplicate_manifest": events.insert(1, events[0])
                if fault == "duplicate_stop": events.insert(-1, events[-2])
                trace.write_text("\n".join(map(json.dumps, events)))
                with self.assertRaises(ValueError): plan_for(self.store, self.hold, self.evidence)
                self.assertEqual(self.store.load(PN), self.hold)

    def test_inspect_exports_only_scope_free_facts_without_mutation(self):
        before = {str(p): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        report = self.store.inspect(PN)
        self.assertEqual(report["run_count"], 2)
        self.assertTrue(report["runs"][0]["original"])
        self.assertFalse(report["runs"][1]["original"])
        for secret in (PN, str(self.root), self.states[0].original_endpoint, self.states[-1].route_owner_id):
            self.assertNotIn(secret, json.dumps(report))
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.root.rglob("*") if p.is_file()})


if __name__ == "__main__":
    unittest.main()
