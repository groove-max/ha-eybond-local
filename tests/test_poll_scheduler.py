"""Tests for adaptive poll scheduling."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.const import POLL_MODE_AUTO, POLL_MODE_MANUAL
from custom_components.eybond_local.drivers.registry import poll_policy_for_driver_key
# PI30_POLL_POLICY now lives in its driver module (concrete driver policy); the
# neutral contract only exports PollPolicy + DEFAULT.
from custom_components.eybond_local.drivers.pi30 import PI30_POLL_POLICY
from custom_components.eybond_local.poll_policy import DEFAULT_POLL_POLICY, PollPolicy
from custom_components.eybond_local.runtime.poll_scheduler import PollScheduler

# Local scheduler-shape fixtures (these tests exercise the scheduler math for a
# given policy envelope, not the driver mapping).
ASCII_POLL_POLICY = PollPolicy(min_auto_interval=10.0, max_auto_interval=120.0)
FAST_MODBUS_POLL_POLICY = PollPolicy(min_auto_interval=5.0, max_auto_interval=90.0)
SMG_MODBUS_POLL_POLICY = PollPolicy(min_auto_interval=3.0, max_auto_interval=60.0)


class PollSchedulerTests(unittest.TestCase):
    def test_manual_keeps_configured_interval_and_reports_recommendation(self) -> None:
        scheduler = PollScheduler(
            policy=ASCII_POLL_POLICY,
            mode=POLL_MODE_MANUAL,
            manual_interval=10,
        )

        decision = scheduler.observe(12.0)

        self.assertEqual(decision.mode, POLL_MODE_MANUAL)
        self.assertEqual(decision.effective_interval, 10)
        self.assertEqual(decision.recommended_interval, 16)
        self.assertEqual(decision.utilization_percent, 120)

    def test_auto_respects_ascii_minimum_interval(self) -> None:
        scheduler = PollScheduler(
            policy=ASCII_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=3,
        )

        decision = scheduler.observe(2.0)

        self.assertEqual(decision.effective_interval, 10)
        self.assertEqual(decision.recommended_interval, 10)

    def test_pi30_has_its_own_two_second_floor(self) -> None:
        # The driver declares its own policy; the registry resolves it.
        self.assertIs(poll_policy_for_driver_key("pi30"), PI30_POLL_POLICY)
        self.assertEqual(PI30_POLL_POLICY.min_auto_interval, 2)
        self.assertEqual(PI30_POLL_POLICY.min_manual_interval, 2)

    def test_pi30_manual_recommendation_uses_observed_cycle_not_ascii_floor(self) -> None:
        scheduler = PollScheduler(
            policy=PI30_POLL_POLICY,
            mode=POLL_MODE_MANUAL,
            manual_interval=3,
        )

        decision = scheduler.observe(1.5)

        self.assertEqual(decision.effective_interval, 3)
        self.assertEqual(decision.recommended_interval, 2)
        self.assertEqual(decision.utilization_percent, 50)

    def test_pi30_auto_can_settle_below_old_ascii_floor(self) -> None:
        scheduler = PollScheduler(
            policy=PI30_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=3,
        )

        decision = scheduler.observe(1.5)

        self.assertEqual(decision.effective_interval, 2)
        self.assertEqual(decision.recommended_interval, 2)

    def test_auto_allows_smg_three_second_interval(self) -> None:
        scheduler = PollScheduler(
            policy=SMG_MODBUS_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=3,
        )

        decision = scheduler.observe(0.7)

        self.assertEqual(decision.effective_interval, 3)
        self.assertEqual(decision.recommended_interval, 3)

    def test_auto_smg_shrinks_to_three_second_floor(self) -> None:
        scheduler = PollScheduler(
            policy=SMG_MODBUS_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=10,
        )

        decisions = [scheduler.observe(0.7) for _ in range(10)]

        self.assertEqual(
            [decision.effective_interval for decision in decisions[:7]],
            [9, 8, 7, 6, 5, 4, 3],
        )
        self.assertEqual(decisions[-1].effective_interval, 3)

    def test_auto_fast_modbus_shrinks_to_five_second_floor(self) -> None:
        scheduler = PollScheduler(
            policy=FAST_MODBUS_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=10,
        )

        decisions = [scheduler.observe(0.5) for _ in range(10)]

        self.assertEqual(decisions[-1].effective_interval, 5)

    def test_auto_grows_after_repeated_slow_cycles(self) -> None:
        scheduler = PollScheduler(
            policy=ASCII_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=10,
        )

        decisions = [scheduler.observe(12.0) for _ in range(3)]

        self.assertGreater(decisions[-1].effective_interval, 10)
        self.assertLessEqual(decisions[-1].effective_interval, 16)
        self.assertEqual(decisions[-1].recommended_interval, 16)
        self.assertEqual(decisions[0].utilization_percent, 120)

    def test_auto_does_not_adapt_or_record_samples_on_unsuccessful_cycle(self) -> None:
        scheduler = PollScheduler(
            policy=ASCII_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=10,
        )

        decision = scheduler.observe(80.0, success=False)

        self.assertEqual(decision.effective_interval, 10)
        self.assertEqual(decision.sample_count, 0)
        self.assertEqual(decision.observed_duration, 0.0)
        self.assertEqual(decision.utilization_percent, 800)

    def test_auto_decreases_slowly_after_device_becomes_faster(self) -> None:
        scheduler = PollScheduler(
            policy=ASCII_POLL_POLICY,
            mode=POLL_MODE_AUTO,
            manual_interval=30,
        )
        for _ in range(10):
            scheduler.observe(20.0)
        high = scheduler.effective_interval

        decision = scheduler.observe(4.0)

        self.assertLessEqual(decision.effective_interval, high)
        self.assertGreater(decision.effective_interval, 10)


if __name__ == "__main__":
    unittest.main()
