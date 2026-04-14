"""Tests for estimated energy helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from custom_components.eybond_local.energy import CyclingEnergyAccumulator, EnergyAccumulator


class EnergyAccumulatorTests(unittest.TestCase):
    """Cover trapezoidal energy accumulation semantics."""

    def test_accumulates_kwh_from_power_samples(self) -> None:
        accumulator = EnergyAccumulator()
        start = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)

        self.assertEqual(accumulator.accumulate(1000.0, start), 0.0)
        total = accumulator.accumulate(1000.0, start + timedelta(hours=1))

        self.assertEqual(total, 1.0)

    def test_uses_trapezoidal_average_between_samples(self) -> None:
        accumulator = EnergyAccumulator()
        start = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)

        accumulator.accumulate(500.0, start)
        total = accumulator.accumulate(1500.0, start + timedelta(minutes=30))

        self.assertEqual(total, 0.5)

    def test_ignores_negative_power_samples(self) -> None:
        accumulator = EnergyAccumulator()
        start = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)

        accumulator.accumulate(-200.0, start)
        total = accumulator.accumulate(1000.0, start + timedelta(hours=1))

        self.assertEqual(total, 0.5)

    def test_ignores_large_gaps_until_a_new_baseline_is_established(self) -> None:
        accumulator = EnergyAccumulator()
        start = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)

        accumulator.accumulate(1000.0, start)
        total = accumulator.accumulate(1000.0, start + timedelta(hours=2))

        self.assertEqual(total, 0.0)

    def test_restored_total_is_reused(self) -> None:
        accumulator = EnergyAccumulator(initial_kwh=2.5)

        self.assertEqual(accumulator.total_kwh, 2.5)
        accumulator.total_kwh = 3.25

        self.assertEqual(accumulator.total_kwh, 3.25)

    def test_daily_cycle_accumulator_resets_at_day_boundary(self) -> None:
        accumulator = CyclingEnergyAccumulator(cycle="daily")
        start = datetime(2026, 4, 8, 23, 50, tzinfo=timezone.utc)

        accumulator.accumulate(1000.0, start)
        before_midnight = accumulator.accumulate(1000.0, start + timedelta(minutes=5))
        after_midnight = accumulator.accumulate(1000.0, start + timedelta(minutes=15))

        self.assertAlmostEqual(before_midnight, 0.0833, places=4)
        self.assertEqual(after_midnight, 0.0)
        self.assertEqual(accumulator.period_key, "2026-04-09")

    def test_monthly_cycle_accumulator_resets_at_month_boundary(self) -> None:
        accumulator = CyclingEnergyAccumulator(cycle="monthly")
        start = datetime(2026, 4, 30, 23, 50, tzinfo=timezone.utc)

        accumulator.accumulate(1000.0, start)
        before_month_end = accumulator.accumulate(1000.0, start + timedelta(minutes=5))
        after_month_end = accumulator.accumulate(1000.0, start + timedelta(minutes=15))

        self.assertAlmostEqual(before_month_end, 0.0833, places=4)
        self.assertEqual(after_month_end, 0.0)
        self.assertEqual(accumulator.period_key, "2026-05")

    def test_cycle_accumulator_restores_only_current_period(self) -> None:
        accumulator = CyclingEnergyAccumulator(cycle="daily")
        now = datetime(2026, 4, 9, 12, 0, tzinfo=timezone.utc)

        accumulator.restore(total_kwh=1.75, period_key="2026-04-09", now=now)
        self.assertEqual(accumulator.total_kwh, 1.75)

        accumulator.restore(total_kwh=4.0, period_key="2026-04-08", now=now)
        self.assertEqual(accumulator.total_kwh, 0.0)


if __name__ == "__main__":
    unittest.main()