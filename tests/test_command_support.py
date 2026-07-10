from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.command_support import (  # noqa: E402
    UNSUPPORTED_COMMAND_STRIKES,
    apply_unsupported_diagnostics,
    clear_unsupported_commands,
    command_skipped_as_unsupported,
    commit_cycle_failures,
    record_command_failure,
    record_command_success,
    seed_unsupported_commands,
    unsupported_commands,
)


class CommandSupportCacheTests(unittest.TestCase):
    def test_failures_commit_only_when_cycle_had_a_success(self) -> None:
        state: dict[str, object] = {}

        # Cycle 1: everything failed — a link problem, nothing committed.
        record_command_failure(state, "QPIWS")
        record_command_failure(state, "Q1")
        commit_cycle_failures(state)
        self.assertEqual(unsupported_commands(state), ())

        # Mixed cycles: the device answers something, so the failing
        # commands accumulate strikes toward genuinely-unsupported.
        for _ in range(UNSUPPORTED_COMMAND_STRIKES - 1):
            record_command_success(state, "QPIGS")
            record_command_failure(state, "QPIWS")
            record_command_failure(state, "Q1")
            commit_cycle_failures(state)

        # One strike short of the threshold: still probed, not yet skipped.
        self.assertEqual(unsupported_commands(state), ())
        self.assertFalse(command_skipped_as_unsupported(state, "QPIWS"))

        # The final mixed cycle crosses the strike threshold.
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

        values: dict[str, object] = {}
        apply_unsupported_diagnostics(values, state)
        self.assertEqual(values["driver_unsupported_commands"], "Q1, QET")

        clear_unsupported_commands(state)
        self.assertEqual(unsupported_commands(state), ())
        values = {}
        apply_unsupported_diagnostics(values, state)
        self.assertNotIn("driver_unsupported_commands", values)

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
