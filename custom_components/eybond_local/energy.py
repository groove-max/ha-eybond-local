"""Helpers for estimated energy sensors."""

from __future__ import annotations

from datetime import datetime


class EnergyAccumulator:
    """Accumulate energy in kWh from instantaneous power samples in W."""

    def __init__(self, initial_kwh: float = 0.0) -> None:
        self._total_kwh = initial_kwh
        self._prev_power_w: float | None = None
        self._prev_time: datetime | None = None

    @property
    def total_kwh(self) -> float:
        """Return the rounded accumulated total."""

        return round(self._total_kwh, 4)

    @total_kwh.setter
    def total_kwh(self, value: float) -> None:
        """Restore the total from a previous state."""

        self._total_kwh = value

    def accumulate(self, power_w: float, now: datetime) -> float:
        """Add one new power sample and return the updated total."""

        power_w = max(0.0, power_w)
        if self._prev_power_w is not None and self._prev_time is not None:
            dt_seconds = (now - self._prev_time).total_seconds()
            if 0 < dt_seconds <= 3600:
                dt_hours = dt_seconds / 3600.0
                average_power = (self._prev_power_w + power_w) / 2.0
                self._total_kwh += average_power * dt_hours / 1000.0

        self._prev_power_w = power_w
        self._prev_time = now
        return self.total_kwh

    def reset_sample(self) -> None:
        """Forget the previous sample after a disconnect or long gap."""

        self._prev_power_w = None
        self._prev_time = None


class CyclingEnergyAccumulator(EnergyAccumulator):
    """Energy accumulator that resets itself at day/month boundaries."""

    def __init__(self, *, cycle: str, initial_kwh: float = 0.0, period_key: str = "") -> None:
        super().__init__(initial_kwh=initial_kwh)
        if cycle not in {"daily", "monthly"}:
            raise ValueError(f"unsupported_cycle:{cycle}")
        self._cycle = cycle
        self._period_key = period_key

    @property
    def period_key(self) -> str:
        """Return the currently tracked daily/monthly period key."""

        return self._period_key

    def period_key_for(self, now: datetime) -> str:
        """Return the period key that contains one timestamp."""

        if self._cycle == "daily":
            return now.date().isoformat()
        return f"{now.year:04d}-{now.month:02d}"

    def restore(self, *, total_kwh: float, period_key: str, now: datetime) -> None:
        """Restore one previous total only if it belongs to the current cycle."""

        current_period_key = self.period_key_for(now)
        self._period_key = current_period_key
        self._total_kwh = total_kwh if period_key == current_period_key else 0.0
        self.reset_sample()

    def accumulate(self, power_w: float, now: datetime) -> float:
        """Accumulate one sample, resetting automatically when the cycle changes."""

        current_period_key = self.period_key_for(now)
        if current_period_key != self._period_key:
            self._period_key = current_period_key
            self._total_kwh = 0.0
            self.reset_sample()
        return super().accumulate(power_w, now)