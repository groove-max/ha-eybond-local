"""Memory admission and safe diagnostic parsing; no host/device mutations."""
import io
import unittest
from unittest.mock import patch

from custom_components.eybond_local.support import memory_guard as memory


class LearningMemoryGuardTests(unittest.TestCase):
    def test_existing_admission_boundary_and_unknown_measurement(self):
        self.assertEqual(memory.shadow_learning_memory_blocker(399), "insufficient_memory:399MiB")
        for available in (400, 401, None):
            self.assertEqual(memory.shadow_learning_memory_blocker(available), "")

    def test_reads_available_not_free_or_total(self):
        with patch("builtins.open", return_value=io.StringIO(
            "MemTotal: 2000000 kB\nMemFree: 1234 kB\nMemAvailable: 188416 kB\n"
        )):
            self.assertEqual(memory.read_available_memory_mib(), 184)

    def test_failure_details_are_numeric_only(self):
        reason = "shadow_learning_preflight_blocked:insufficient_memory:184MiB,other_blocker"
        self.assertEqual(memory.shadow_learning_memory_failure_details(reason), {
            "available_mib": 184, "required_mib": 400,
        })

    def test_untrusted_or_inconsistent_text_is_not_a_memory_diagnostic(self):
        for reason in (None, 184, "insufficient_memory:184MiB", "private-secret",
                       "shadow_learning_preflight_blocked:insufficient_memory:-1MiB",
                       "shadow_learning_preflight_blocked:insufficient_memory:400MiB",
                       "shadow_learning_preflight_blocked:insufficient_memory:184MiB private-secret",
                       "shadow_learning_preflight_blocked:insufficient_memory:" + "9" * 5000 + "MiB"):
            with self.subTest(reason=str(reason)[:100]):
                self.assertEqual(memory.shadow_learning_memory_failure_details(reason), {})
