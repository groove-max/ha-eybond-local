from __future__ import annotations

from time import monotonic
import unittest

from custom_components.eybond_local.drivers.catalog_probe import ProbeDeadline


class ProbeDeadlineTests(unittest.TestCase):
    def test_remaining_budget_clamps_required_action_timeout(self) -> None:
        deadline = ProbeDeadline(10.0)
        deadline._started = monotonic() - 8.5

        self.assertLessEqual(deadline.action_timeout(4.0), 1.6)

    def test_optional_action_requires_full_configured_timeout_budget(self) -> None:
        deadline = ProbeDeadline(10.0)
        deadline._started = monotonic() - 8.5

        self.assertFalse(deadline.has_optional_budget(2.0))
        self.assertTrue(deadline.has_optional_budget(1.0))

