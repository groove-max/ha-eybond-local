"""Adaptive poll interval scheduler."""

from __future__ import annotations

from dataclasses import dataclass
import math

from ..const import (
    DEFAULT_POLL_INTERVAL,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from .poll_policy import DEFAULT_POLL_POLICY, PollPolicy


@dataclass(frozen=True)
class PollDecision:
    """One scheduler decision for the next runtime refresh."""

    mode: str
    effective_interval: float
    manual_interval: float
    recommended_interval: float
    utilization_percent: int
    policy_min_interval: float
    policy_max_interval: float
    observed_duration: float
    sample_count: int


def normalize_poll_mode(value: object) -> str:
    """Normalize a stored poll mode."""

    mode = str(value or "").strip().lower()
    if mode == POLL_MODE_AUTO:
        return POLL_MODE_AUTO
    return POLL_MODE_MANUAL


def clamp_interval(value: object, *, minimum: float = 2.0, maximum: float = 3600.0) -> float:
    """Coerce one interval value into a safe floating-point interval."""

    try:
        interval = float(value)
    except (TypeError, ValueError, OverflowError):
        interval = float(DEFAULT_POLL_INTERVAL)
    if not math.isfinite(interval):
        interval = float(DEFAULT_POLL_INTERVAL)
    return min(float(maximum), max(float(minimum), interval))


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    p = min(1.0, max(0.0, float(percentile)))
    index = int(math.ceil(p * len(ordered))) - 1
    return ordered[min(len(ordered) - 1, max(0, index))]


def _recommended_interval(
    *,
    policy: PollPolicy,
    observed_duration: float,
    fallback_interval: float,
) -> float:
    if observed_duration <= 0.0:
        target = fallback_interval
    else:
        target = observed_duration * max(1.0, float(policy.safety_factor))
    return clamp_interval(
        math.ceil(target),
        minimum=policy.min_auto_interval,
        maximum=policy.max_auto_interval,
    )


class PollScheduler:
    """Choose the effective start-to-start poll interval.

    The scheduler never mutates user options.  In manual mode it reports
    diagnostics around the configured interval.  In auto mode it adapts the
    effective interval from recent successful cycle durations.
    """

    def __init__(
        self,
        *,
        policy: PollPolicy = DEFAULT_POLL_POLICY,
        mode: str = POLL_MODE_MANUAL,
        manual_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._policy = policy
        self._mode = normalize_poll_mode(mode)
        self._manual_interval = clamp_interval(manual_interval)
        self._effective_interval = self._initial_effective_interval()
        self._durations: list[float] = []
        self._last_decision = self._build_decision(observed_duration=0.0)

    @property
    def policy(self) -> PollPolicy:
        return self._policy

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def manual_interval(self) -> float:
        return self._manual_interval

    @property
    def effective_interval(self) -> float:
        return self._effective_interval

    @property
    def last_decision(self) -> PollDecision:
        return self._last_decision

    def configure(
        self,
        *,
        policy: PollPolicy | None = None,
        mode: str | None = None,
        manual_interval: float | None = None,
    ) -> None:
        """Update scheduler configuration while preserving observations."""

        if policy is not None:
            self._policy = policy
        if mode is not None:
            self._mode = normalize_poll_mode(mode)
        if manual_interval is not None:
            self._manual_interval = clamp_interval(manual_interval)
        self._effective_interval = self._clamp_effective(self._effective_interval)
        if self._mode == POLL_MODE_MANUAL:
            self._effective_interval = self._manual_interval
        elif self._effective_interval <= 0.0:
            self._effective_interval = self._initial_effective_interval()
        self._last_decision = self._build_decision(
            observed_duration=self._observed_duration()
        )

    def current_interval(self) -> float:
        """Return the interval that should be passed to the next refresh."""

        return self._effective_interval

    def observe(self, duration_seconds: object, *, success: bool = True) -> PollDecision:
        """Record one completed poll cycle and return the next decision."""

        cycle_interval = self._effective_interval
        try:
            duration = max(0.0, float(duration_seconds))
        except (TypeError, ValueError, OverflowError):
            duration = 0.0
        if not math.isfinite(duration):
            duration = 0.0
        if success and duration > 0.0:
            self._durations.append(duration)
            sample_window = max(1, int(self._policy.sample_window))
            if len(self._durations) > sample_window:
                self._durations = self._durations[-sample_window:]
        observed = self._observed_duration()
        recommended = _recommended_interval(
            policy=self._policy,
            observed_duration=observed,
            fallback_interval=self._effective_interval,
        )
        if self._mode == POLL_MODE_AUTO:
            if success:
                self._effective_interval = self._smooth_auto_interval(recommended)
            else:
                self._effective_interval = self._clamp_effective(self._effective_interval)
        else:
            self._effective_interval = self._manual_interval
        self._last_decision = self._build_decision(
            observed_duration=observed,
            last_duration=duration,
            recommended_interval=recommended,
            utilization_interval=cycle_interval,
        )
        return self._last_decision

    def _initial_effective_interval(self) -> float:
        if self._mode == POLL_MODE_AUTO:
            return clamp_interval(
                self._manual_interval,
                minimum=self._policy.min_auto_interval,
                maximum=self._policy.max_auto_interval,
            )
        return self._manual_interval

    def _observed_duration(self) -> float:
        return _percentile(self._durations, self._policy.percentile)

    def _clamp_effective(self, value: float) -> float:
        if self._mode == POLL_MODE_AUTO:
            return clamp_interval(
                value,
                minimum=self._policy.min_auto_interval,
                maximum=self._policy.max_auto_interval,
            )
        return clamp_interval(value)

    def _smooth_auto_interval(self, target: float) -> float:
        current = self._clamp_effective(self._effective_interval)
        if current <= 0.0:
            return self._clamp_effective(target)
        if target > current:
            next_interval = min(target, current * max(1.0, self._policy.grow_step_limit))
        else:
            shrink_floor = current * min(1.0, max(0.0, self._policy.shrink_step_limit))
            next_interval = max(target, shrink_floor)
        if self._mode == POLL_MODE_AUTO:
            rounded = (
                math.floor(next_interval)
                if next_interval < current
                else math.ceil(next_interval)
            )
            return self._clamp_effective(rounded)
        return self._clamp_effective(next_interval)

    def _build_decision(
        self,
        *,
        observed_duration: float,
        last_duration: float | None = None,
        recommended_interval: float | None = None,
        utilization_interval: float | None = None,
    ) -> PollDecision:
        recommended = (
            recommended_interval
            if recommended_interval is not None
            else _recommended_interval(
                policy=self._policy,
                observed_duration=observed_duration,
                fallback_interval=self._effective_interval,
            )
        )
        duration = observed_duration if last_duration is None else last_duration
        interval = (
            self._effective_interval
            if utilization_interval is None
            else clamp_interval(utilization_interval)
        )
        utilization = (
            int(round((duration / interval) * 100.0))
            if interval > 0.0
            else 0
        )
        return PollDecision(
            mode=self._mode,
            effective_interval=self._effective_interval,
            manual_interval=self._manual_interval,
            recommended_interval=recommended,
            utilization_percent=utilization,
            policy_min_interval=self._policy.min_auto_interval,
            policy_max_interval=self._policy.max_auto_interval,
            observed_duration=observed_duration,
            sample_count=len(self._durations),
        )
