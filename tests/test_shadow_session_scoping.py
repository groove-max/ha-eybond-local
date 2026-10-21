"""Multiple collectors keep independent recovery obligations, including legacy."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from custom_components.eybond_local.support.shadow_learning import session


class SessionScopingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.first = session.build_shadow_learning_session_state(
            entry_id="one", collector_pn="SYNTHETIC-ONE", route_owner_id="shadow_learning:first",
            trace_path="", original_endpoint="cloud.invalid,502,TCP",
            proxy_endpoint="192.0.2.1,8899,TCP", upstream_endpoint="cloud.invalid,502,TCP",
            restore_required=True, started_at="2026-10-20T00:00:00Z",
            updated_at="2026-10-20T00:00:00Z", status="restore_failed",
        )
        self.second = replace(self.first, entry_id="two", collector_pn="SYNTHETIC-TWO",
                              route_owner_id="shadow_learning:second", status="preflight")

    def save(self, state):
        return session.save_shadow_learning_session_state(
            config_dir=self.root, state=state, entry_id=state.entry_id,
        )

    def load(self, state):
        return session.load_shadow_learning_session_state(self.root, entry_id=state.entry_id)

    def test_two_new_entries_survive_reload_and_independent_finalization(self):
        first = self.save(self.first)
        before = first.read_bytes()
        second = self.save(self.second)
        self.assertNotEqual(first, second)
        self.assertEqual(self.load(self.first), self.first)
        self.assertEqual(self.load(self.second), self.second)
        session.clear_shadow_learning_session_state(self.root, expected_state=self.second, outcome="start_not_mutated")
        self.assertIsNone(self.load(self.second))
        self.assertEqual(first.read_bytes(), before)

    def test_foreign_legacy_hold_is_retained_byte_for_byte(self):
        legacy = session.save_shadow_learning_session_state(config_dir=self.root, state=self.first)
        before = legacy.read_bytes()
        new = self.save(self.second)
        self.assertNotEqual(legacy, new)
        self.assertEqual(legacy.read_bytes(), before)
        self.assertEqual(self.load(self.first), self.first)
        self.assertEqual(self.load(self.second), self.second)
        self.save(replace(self.second, status="ready"))
        self.assertEqual(legacy.read_bytes(), before)

    def test_matching_legacy_owner_continues_same_record(self):
        legacy = session.save_shadow_learning_session_state(config_dir=self.root, state=self.first)
        updated = replace(self.first, status="restoring")
        self.assertEqual(self.save(updated), legacy)
        self.assertEqual(self.load(updated), updated)
        self.assertFalse(session.active_shadow_learning_state_path(self.root, entry_id="one").exists())

    def test_unknown_legacy_owner_and_duplicate_identity_fail_closed(self):
        path = session.save_shadow_learning_session_state(config_dir=self.root, state=self.first)
        with self.assertRaisesRegex(RuntimeError, "record_owned"):
            self.save(replace(self.second, collector_pn=self.first.collector_pn))
        path.write_text("{broken")
        with self.assertRaisesRegex(RuntimeError, "record_owned"):
            self.save(self.second)
        self.assertEqual(path.read_text(), "{broken")

    def test_scoped_corruption_and_wrong_owner_are_not_overwritten(self):
        path = self.save(self.first)
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "record_owned"):
            self.save(replace(self.first, route_owner_id="shadow_learning:another"))
        self.assertEqual(path.read_bytes(), before)
        path.write_text("{broken")
        with self.assertRaisesRegex(RuntimeError, "record_owned"):
            self.save(self.first)
        self.assertEqual(path.read_text(), "{broken")

    def test_stale_finalizer_cannot_clear_another_collectors_record(self):
        one, two = self.save(self.first), self.save(self.second)
        before = (one.read_bytes(), two.read_bytes())
        with self.assertRaisesRegex(RuntimeError, "record_changed"):
            session.clear_shadow_learning_session_state(
                self.root, expected_state=replace(self.first, entry_id=self.second.entry_id),
                outcome="explicit_discard",
            )
        self.assertEqual((one.read_bytes(), two.read_bytes()), before)
