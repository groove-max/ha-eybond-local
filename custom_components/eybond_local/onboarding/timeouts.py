"""Onboarding deadline and scan-budget helpers.

The timeout POLICY type and its single default object live in the neutral
top-level :mod:`..timeout_policy` module so the recovery execution layer and
onboarding share ONE policy without any connection/runtime -> onboarding
dependency. They are imported here (and thus remain importable from this module)
purely so onboarding-side callers and this module's own default arguments keep a
single, unchanged seam.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Awaitable, TypeVar

from ..timeout_policy import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    OnboardingTimeoutPolicy,
)


AwaitableT = TypeVar("AwaitableT")


class OnboardingDeadlineExceeded(TimeoutError):
    """Raised when a shared onboarding deadline has no budget left."""


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

        if timeout_seconds is None:
            return OnboardingDeadline(
                deadline_monotonic=self.deadline_monotonic
            )
        local_deadline = monotonic() + max(0.0, float(timeout_seconds))
        if self.deadline_monotonic is None:
            return OnboardingDeadline(deadline_monotonic=local_deadline)
        return OnboardingDeadline(
            deadline_monotonic=min(self.deadline_monotonic, local_deadline)
        )

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


def manual_probe_watchdog_timeout_seconds(
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
) -> float:
    """Return the outer runaway guard for one manual onboarding call.

    The detector's own deadline remains authoritative for all actual work.  The
    extra finalization grace prevents an outer wrapper from cancelling the
    detector while it is converting deadline expiry into an honest partial
    collector result.
    """

    return manual_probe_timeout_seconds(policy) + max(
        0.0, float(policy.result_finalization_grace)
    )
