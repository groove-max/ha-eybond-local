from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.command_support import (  # noqa: E402
    clear_unsupported_commands,
    command_skipped_as_unsupported,
    commit_cycle_failures,
    forget_command_failures,
    record_command_failure,
    record_command_success,
    seed_unsupported_commands,
    unsupported_command_diagnostics,
    unsupported_commands,
)


class CommandSupportCacheTests(unittest.TestCase):
    def test_forgetting_required_skip_does_not_manufacture_success(self) -> None:
        state = {}
        seed_unsupported_commands(state, ["block:live", "optional"])
        record_command_failure(state, "block:live")
        record_command_failure(state, "another")
        forget_command_failures(state, "block:live")
        commit_cycle_failures(state)
        self.assertEqual(unsupported_commands(state), ("optional",))
        self.assertNotIn("another", state["driver_unsupported_commands"])

    def test_forgetting_removes_pending_failures_but_keeps_real_success(self) -> None:
        state = {}
        record_command_success(state, "working")
        record_command_failure(state, "block:live")
        record_command_failure(state, "optional")
        forget_command_failures(state, "block:live")
        commit_cycle_failures(state)
        self.assertEqual(state["driver_unsupported_commands"], {"optional": 1})
        forget_command_failures(None, "block:live")
        empty = {}
        forget_command_failures(empty, "block:live")
        self.assertEqual(empty, {})

    def test_failures_commit_only_when_cycle_had_a_success(self) -> None:
        state: dict[str, object] = {}

        # Cycle 1: everything failed — a link problem, nothing committed.
        record_command_failure(state, "QPIWS")
        record_command_failure(state, "Q1")
        commit_cycle_failures(state)
        self.assertEqual(unsupported_commands(state), ())

        # Four mixed cycles: the device answers something, so the failing
        # commands are genuinely unsupported.
        for _ in range(4):
            record_command_success(state, "QPIGS")
            record_command_failure(state, "QPIWS")
            record_command_failure(state, "Q1")
            commit_cycle_failures(state)

        self.assertEqual(unsupported_commands(state), ("Q1", "QPIWS"))
        self.assertTrue(command_skipped_as_unsupported(state, "QPIWS"))
        self.assertFalse(command_skipped_as_unsupported(state, "QPIGS"))

    def test_success_clears_a_previously_marked_command(self) -> None:
        state: dict[str, object] = {}
        seed_unsupported_commands(state, ("QPIWS",))
        self.assertTrue(command_skipped_as_unsupported(state, "QPIWS"))

        record_command_success(state, "QPIWS")
        self.assertFalse(command_skipped_as_unsupported(state, "QPIWS"))

    def test_seed_clear_and_diagnostics(self) -> None:
        state: dict[str, object] = {}
        seed_unsupported_commands(state, ("QET", "Q1", ""))
        self.assertEqual(unsupported_commands(state), ("Q1", "QET"))

        diagnostics = unsupported_command_diagnostics(state)
        self.assertEqual(diagnostics["driver_unsupported_commands"], "Q1, QET")

        clear_unsupported_commands(state)
        self.assertEqual(unsupported_commands(state), ())
        diagnostics = unsupported_command_diagnostics(state)
        self.assertNotIn("driver_unsupported_commands", diagnostics)

    def test_none_state_is_a_no_op(self) -> None:
        record_command_failure(None, "QPIWS")
        record_command_success(None, "QPIWS")
        commit_cycle_failures(None)
        seed_unsupported_commands(None, ("QPIWS",))
        clear_unsupported_commands(None)
        self.assertFalse(command_skipped_as_unsupported(None, "QPIWS"))
        self.assertEqual(unsupported_commands(None), ())


if __name__ == "__main__":
    unittest.main()
