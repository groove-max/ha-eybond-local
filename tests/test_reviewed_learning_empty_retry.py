"""Stopped-before-dispatch is a reviewed retry outcome, never an automatic release."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from tests.test_reviewed_learning_recovery import PN, approve, history_fixture


def five_run_fixture(root, **scope):
    store, hold, states, files = history_fixture(root, retries=4, **scope)
    proofs = list(files)
    for i in (1, 2, 3):
        rows = [json.loads(s) for s in files[i].read_text().splitlines()]
        rows[1] = {"kind": "shadow_at_write", "payload": {"command": "UART"}}
        rows[-1]["payload"].update(confirmed_commands=0, blocked_reason="")
        files[i].write_text("\n".join(map(json.dumps, rows)))
        proofs[i] = root / f"runner-{i}.log"
        proofs[i].write_text(f"Synthetic runner evidence for retry {i}: stopped at preflight before dispatch")
    counts = [90, "not-dispatched", "not-dispatched", "not-dispatched", 90]
    return store, hold, states, files, proofs, counts


class ReviewedEmptyRetryTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store, self.hold, self.states, self.files, self.proofs, self.counts = five_run_fixture(Path(tmp.name))

    def preview(self):
        return self.store.prepare(collector_pn=PN, entry_id=self.hold.entry_id, study_id=self.hold.study_id,
                                  evidence_files=self.proofs, command_counts=self.counts)

    def test_five_runs_do_not_require_dummy_commands_or_wait(self):
        plan = self.preview()
        self.assertEqual([p["reviewed_commands"] for p in plan["evidence"]], [90, 0, 0, 0, 90])
        self.assertEqual(self.store.load(PN), self.hold)
        approve(self.store, plan)
        corrected, _ = self.store.apply(expected_state=self.states[-1])
        self.assertEqual(corrected.original_endpoint, self.states[0].original_endpoint)
        self.assertIsNone(self.store.load(PN))

    def test_numeric_zero_original_or_only_wire_trace_are_not_no_dispatch_evidence(self):
        for index, value in ((1, 0), (1, False), (0, "not-dispatched")):
            with self.subTest(index=index, value=value):
                old = self.counts[index]
                self.counts[index] = value
                with self.assertRaisesRegex(ValueError, "every_run"):
                    self.preview()
                self.counts[index] = old
        self.proofs[1] = self.files[1]
        with self.assertRaisesRegex(ValueError, "independent_evidence"):
            self.preview()

    def test_share_copy_and_support_zip_are_not_sender_evidence(self):
        for contents in (self.files[1].read_bytes().replace(PN.encode(), b"masked"),
                         b"PK\x03\x04test", b" \n", b"\xff\x80", b"SQLite format 3\x00",
                         b'{\n  "kind": "shadow_session_manifest"\n}',
                         b'[{"kind": "shadow_session_manifest"}]',
                         b'{\n "bundle_version": 1, "entry": {}, "evidence": {}\n}',
                         b'{"archive_version": 1, "archive_members": {}}',
                         b'{"history_sha256": "example", "runs": []}'):
            with self.subTest(contents=contents[:4]):
                self.proofs[1].write_bytes(contents)
                with self.assertRaisesRegex(ValueError, "independent_evidence"):
                    self.preview()
        self.assertEqual(self.store.load(PN), self.hold)

    def test_review_can_bind_a_multiline_sender_report(self):
        self.proofs[1].write_text(json.dumps({
            "runner_result": "stopped_before_dispatch", "attempt": 1,
        }, indent=2))
        plan = self.preview()
        self.assertEqual(plan["evidence"][1]["basis"], "not-dispatched")
        self.assertEqual(self.store.load(PN), self.hold)

    def test_incompatible_trace_rejected_even_when_approving_or_applying(self):
        path = self.files[2]
        rows = [json.loads(s) for s in path.read_text().splitlines()]
        plan = self.preview()
        approve(self.store, plan)
        for kind, payload in (("shadow_at_write", {"command": "OTHER"}),
                              ("shadow_modbus_write_observation", {}), ("shadow_pi30_write_response", {}),
                              ("shadow_proxy_intercept_cloud_unclassified", {}),
                              ("shadow_proxy_parser_error", {}), ("shadow_future_command", {})):
            with self.subTest(kind=kind):
                path.write_text("\n".join(map(json.dumps, [rows[0], {"kind": kind, "payload": payload}, *rows[1:]])))
                with self.assertRaisesRegex(ValueError, "conflicts_with_trace"):
                    self.preview()
                with self.assertRaisesRegex(ValueError, "conflicts_with_trace"):
                    self.store.apply(expected_state=self.states[-1])
        for change in ({"pending": True}, {"complete": True}, {"confirmed_commands": 1},
                       {"confirmed_commands": False}, {"blocked_reason": "completion_terminal_confirmation_missing"}):
            with self.subTest(change=change):
                changed = deepcopy(rows)
                changed[-1]["payload"].update(change)
                path.write_text("\n".join(map(json.dumps, changed)))
                with self.assertRaises(ValueError):
                    approve(self.store, plan)
        self.assertEqual(self.store.load(PN), self.hold)
