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
    deep_scan_followup_estimated_seconds: float = 25.0
    deep_scan_batch_timeout: float = 0.35
    deep_scan_concurrency: int = 32
    deep_scan_timeout_buffer: float = 10.0
    unicast_fallback_probe_timeout: float = 0.35
    unicast_fallback_concurrency: int = 32


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
) -> float:
    """Estimate deep scan duration from the shared onboarding timeout policy."""

    if target_count <= 0:
        return float(policy.auto_scan_estimated_seconds)

    batch_size = max(1, int(policy.deep_scan_concurrency))
    batches = (target_count + batch_size - 1) // batch_size
    return (
        float(policy.auto_scan_estimated_seconds)
        + (batches * float(policy.deep_scan_batch_timeout))
        + float(policy.deep_scan_followup_estimated_seconds)
    )


def deep_scan_timeout_seconds(
    target_count: int,
    *,
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
) -> float:
    """Return the default end-to-end timeout budget for deep scan."""

    estimated = estimate_deep_scan_seconds(target_count, policy=policy)
    return max(
        auto_scan_timeout_seconds(policy),
        estimated + float(policy.deep_scan_timeout_buffer),
    )
