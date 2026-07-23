from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    OnboardingDeadline,
    auto_scan_timeout_seconds,
    manual_probe_timeout_seconds,
    manual_probe_watchdog_timeout_seconds,
)


class OnboardingTimeoutPolicyTests(unittest.TestCase):
    def test_default_policy_exposes_current_scan_and_manual_budgets(self) -> None:
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY

        self.assertEqual(auto_scan_timeout_seconds(), policy.auto_total_timeout)
        self.assertEqual(manual_probe_timeout_seconds(), policy.manual_total_timeout)
        self.assertEqual(
            manual_probe_watchdog_timeout_seconds(),
            policy.manual_total_timeout + policy.result_finalization_grace,
        )
        self.assertFalse(hasattr(policy, "driver_detection_timeout"))
        for retired in (
            "driver_detection_attempts",
            "driver_retry_delay",
            "deep_scan_concurrency",
            "deep_scan_batch_timeout",
            "deep_scan_identity_settle_seconds",
            "deep_scan_timeout_buffer",
            "deep_scan_hard_ceiling_seconds",
        ):
            self.assertFalse(hasattr(policy, retired), msg=retired)


class OnboardingDeadlineTests(unittest.TestCase):
    def test_deadline_is_fixed_and_never_extended_by_nested_work(self) -> None:
        deadline = OnboardingDeadline.from_timeout(5.0)
        child = deadline.nested(60.0)

        self.assertLessEqual(
            child.deadline_monotonic,
            deadline.deadline_monotonic,
        )

    def test_wait_for_raises_without_extension(self) -> None:
        async def _run() -> None:
            deadline = OnboardingDeadline.from_timeout(0.05)

            async def _too_slow() -> None:
                await asyncio.sleep(1.0)

            with self.assertRaises(TimeoutError):
                await deadline.wait_for(_too_slow())

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
