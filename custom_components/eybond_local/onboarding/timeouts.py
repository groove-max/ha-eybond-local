"""Shared onboarding timeout policy and deadline helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Awaitable, TypeVar


AwaitableT = TypeVar("AwaitableT")


class OnboardingDeadlineExceeded(TimeoutError):
    """Raised when a shared onboarding deadline has no budget left."""


@dataclass(frozen=True, slots=True)
class OnboardingTimeoutPolicy:
    """Central timeout policy for onboarding flow wrappers and probe phases."""

    discovery_timeout: float = 1.5
    connect_timeout: float = 5.0
    connect_timeout_without_udp_reply: float = 0.75
    heartbeat_timeout: float = 2.0
    auto_attempts: int = 3
    auto_attempt_delay: float = 0.75
    driver_detection_attempts: int = 3
    driver_retry_delay: float = 0.35
    pi30_qpi_probe_timeout: float = 1.0
    smartess_probe_timeout: float = 3.0
    smartess_query_timeout: float = 1.5
    runtime_enrichment_timeout: float = 4.0
    collector_query_timeout: float = 1.0
    driver_onboarding_read_timeout: float = 2.0
    manual_total_timeout: float = 45.0
    auto_total_timeout: float = 45.0
    auto_scan_estimated_seconds: float = 12.5
    deep_scan_followup_estimated_seconds: float = 75.0
    deep_scan_batch_timeout: float = 0.35
    deep_scan_concurrency: int = 32
    deep_scan_timeout_buffer: float = 20.0
    unicast_fallback_probe_timeout: float = 0.35
    unicast_fallback_concurrency: int = 32
    # Absolute runaway guard for one deep scan. The working deadline grows as
    # connected collectors are admitted for identification; this is the wall
    # it can never grow past.
    deep_scan_hard_ceiling_seconds: float = 900.0


DEFAULT_ONBOARDING_TIMEOUT_POLICY = OnboardingTimeoutPolicy()


@dataclass(frozen=True, slots=True)
class OnboardingDeadline:
    """Absolute deadline shared across all nested onboarding phases."""

    deadline_monotonic: float | None = None

    @classmethod
    def from_timeout(cls, timeout_seconds: float | None) -> OnboardingDeadline:
        """Build one deadline from a relative timeout budget."""

        if timeout_seconds is None:
            return cls()
        return cls(deadline_monotonic=monotonic() + max(0.0, float(timeout_seconds)))

    def remaining_seconds(self) -> float | None:
        """Return the remaining deadline budget, or None when unbounded."""

        if self.deadline_monotonic is None:
            return None
        return max(0.0, self.deadline_monotonic - monotonic())

    def bounded_timeout(self, timeout_seconds: float | None = None) -> float | None:
        """Clamp one phase timeout by the remaining deadline budget."""

        candidates: list[float] = []
        if timeout_seconds is not None:
            candidates.append(max(0.0, float(timeout_seconds)))

        remaining = self.remaining_seconds()
        if remaining is not None:
            candidates.append(remaining)

        if not candidates:
            return None
        return min(candidates)

    def nested(self, timeout_seconds: float | None = None) -> OnboardingDeadline:
        """Return one child deadline capped by both parent and local phase budget."""

        bounded = self.bounded_timeout(timeout_seconds)
        if bounded is None:
            return OnboardingDeadline()
        return OnboardingDeadline.from_timeout(bounded)

    async def wait_for(
        self,
        awaitable: Awaitable[AwaitableT],
        *,
        timeout_seconds: float | None = None,
    ) -> AwaitableT:
        """Await one operation without exceeding the shared deadline."""

        bounded = self.bounded_timeout(timeout_seconds)
        if bounded is None:
            return await awaitable
        if bounded <= 0:
            raise OnboardingDeadlineExceeded("onboarding_deadline_exceeded")
        return await asyncio.wait_for(awaitable, timeout=bounded)

    async def sleep(self, delay_seconds: float) -> None:
        """Sleep without overrunning the shared deadline."""

        if delay_seconds <= 0:
            return
        bounded = self.bounded_timeout(delay_seconds)
        if bounded is None:
            await asyncio.sleep(delay_seconds)
            return
        if bounded <= 0:
            raise OnboardingDeadlineExceeded("onboarding_deadline_exceeded")
        await asyncio.sleep(bounded)


def auto_scan_timeout_seconds(
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
) -> float:
    """Return the default end-to-end timeout budget for auto scan."""

    return float(policy.auto_total_timeout)


def manual_probe_timeout_seconds(
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
) -> float:
    """Return the default end-to-end timeout budget for manual onboarding."""

    return float(policy.manual_total_timeout)


def estimate_deep_scan_seconds(
    target_count: int,
    *,
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    driver_sweep_seconds: float | None = None,
) -> float:
    """Estimate deep scan duration from the shared onboarding timeout policy."""

    if target_count <= 0:
        return float(policy.auto_scan_estimated_seconds)

    if driver_sweep_seconds is None:
        driver_sweep_seconds = default_deep_driver_sweep_seconds()
    followup_seconds = max(
        float(policy.deep_scan_followup_estimated_seconds),
        float(driver_sweep_seconds),
    )
    batch_size = max(1, int(policy.deep_scan_concurrency))
    batches = (target_count + batch_size - 1) // batch_size
    return (
        float(policy.auto_scan_estimated_seconds)
        + (batches * float(policy.deep_scan_batch_timeout))
        + followup_seconds
    )


def default_deep_driver_sweep_seconds() -> float:
    """Return the worst-case single-target driver sweep duration.

    Derived from the registered drivers' own signature and probe budgets, so
    the deep-scan time budget follows the driver registry instead of a
    hand-maintained constant that silently goes stale when drivers are added
    or their probe budgets change.
    """

    from ..drivers.registry import iter_drivers  # local import: keeps module load light

    total = 0.0
    for driver in iter_drivers("auto"):
        for attribute in ("signature_timeout", "probe_timeout"):
            try:
                value = float(getattr(driver, attribute, 0.0) or 0.0)
            except (TypeError, ValueError):
                value = 0.0
            total += max(0.0, value)
    return total


class ExtendableOnboardingDeadline:
    """Deadline that grows as newly discovered work is admitted.

    Deep scan cannot know upfront how many collectors will answer: a fixed
    budget sized for one or two identification sweeps starves the tenth
    collector on a large site. This deadline starts from the ordinary
    discovery budget and, every time one connected collector is admitted for
    a driver sweep, guarantees at least that sweep's budget remains — bounded
    by a hard ceiling so a runaway scan still terminates.

    Duck-type compatible with :class:`OnboardingDeadline`.
    """

    def __init__(
        self,
        *,
        base_timeout_seconds: float | None,
        hard_ceiling_seconds: float,
    ) -> None:
        now = monotonic()
        self._ceiling_monotonic = now + max(0.0, float(hard_ceiling_seconds))
        if base_timeout_seconds is None:
            self._deadline_monotonic: float | None = None
        else:
            self._deadline_monotonic = min(
                now + max(0.0, float(base_timeout_seconds)),
                self._ceiling_monotonic,
            )

    def ensure_remaining(self, seconds: float) -> None:
        """Guarantee at least ``seconds`` of budget, up to the hard ceiling."""

        if self._deadline_monotonic is None:
            return
        target = min(monotonic() + max(0.0, float(seconds)), self._ceiling_monotonic)
        if target > self._deadline_monotonic:
            self._deadline_monotonic = target

    def remaining_seconds(self) -> float | None:
        if self._deadline_monotonic is None:
            return None
        return max(0.0, self._deadline_monotonic - monotonic())

    def bounded_timeout(self, timeout_seconds: float | None = None) -> float | None:
        candidates: list[float] = []
        if timeout_seconds is not None:
            candidates.append(max(0.0, float(timeout_seconds)))
        remaining = self.remaining_seconds()
        if remaining is not None:
            candidates.append(remaining)
        if not candidates:
            return None
        return min(candidates)

    def nested(self, timeout_seconds: float | None = None) -> OnboardingDeadline:
        bounded = self.bounded_timeout(timeout_seconds)
        if bounded is None:
            return OnboardingDeadline()
        return OnboardingDeadline.from_timeout(bounded)

    async def wait_for(
        self,
        awaitable: Awaitable[AwaitableT],
        *,
        timeout_seconds: float | None = None,
    ) -> AwaitableT:
        """Await one operation, honoring extensions granted while waiting.

        A plain ``asyncio.wait_for`` snapshots the timeout once; this loop
        re-checks the deadline after each expiry so budget added by
        ``ensure_remaining`` mid-wait keeps the awaited work alive.
        """

        phase_deadline: float | None = None
        if timeout_seconds is not None:
            phase_deadline = monotonic() + max(0.0, float(timeout_seconds))
        task = asyncio.ensure_future(awaitable)
        try:
            while True:
                candidates: list[float] = []
                remaining = self.remaining_seconds()
                if remaining is not None:
                    candidates.append(remaining)
                if phase_deadline is not None:
                    candidates.append(max(0.0, phase_deadline - monotonic()))
                if not candidates:
                    return await task
                bounded = min(candidates)
                if bounded <= 0:
                    raise OnboardingDeadlineExceeded("onboarding_deadline_exceeded")
                done, _ = await asyncio.wait({task}, timeout=bounded)
                if done:
                    return task.result()
        except BaseException:
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            raise

    async def sleep(self, delay_seconds: float) -> None:
        if delay_seconds <= 0:
            return
        bounded = self.bounded_timeout(delay_seconds)
        if bounded is None:
            await asyncio.sleep(delay_seconds)
            return
        if bounded <= 0:
            raise OnboardingDeadlineExceeded("onboarding_deadline_exceeded")
        await asyncio.sleep(bounded)


def deep_scan_timeout_seconds(
    target_count: int,
    *,
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    driver_sweep_seconds: float | None = None,
) -> float:
    """Return the default end-to-end timeout budget for deep scan."""

    estimated = estimate_deep_scan_seconds(
        target_count,
        policy=policy,
        driver_sweep_seconds=driver_sweep_seconds,
    )
    return max(
        auto_scan_timeout_seconds(policy),
        estimated + float(policy.deep_scan_timeout_buffer),
    )
