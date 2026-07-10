from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    ExtendableOnboardingDeadline,
    OnboardingDeadlineExceeded,
    auto_scan_timeout_seconds,
    deep_scan_timeout_seconds,
    default_deep_driver_sweep_seconds,
    estimate_deep_scan_seconds,
    manual_probe_timeout_seconds,
)


class OnboardingTimeoutPolicyTests(unittest.TestCase):
    def test_default_policy_exposes_current_scan_and_manual_budgets(self) -> None:
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY

        self.assertEqual(auto_scan_timeout_seconds(), policy.auto_total_timeout)
        self.assertEqual(manual_probe_timeout_seconds(), policy.manual_total_timeout)
        self.assertFalse(hasattr(policy, "driver_detection_timeout"))
        self.assertGreaterEqual(policy.driver_detection_attempts, 1)
        self.assertGreaterEqual(policy.driver_retry_delay, 0)

    def test_default_policy_derives_slash24_deep_scan_budget_from_estimate_and_buffer(self) -> None:
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY

        self.assertAlmostEqual(
            deep_scan_timeout_seconds(253),
            estimate_deep_scan_seconds(253) + policy.deep_scan_timeout_buffer,
        )
        self.assertGreaterEqual(
            deep_scan_timeout_seconds(253),
            auto_scan_timeout_seconds() + 60.0,
        )

    def test_deep_scan_budget_derives_from_registered_driver_sweep(self) -> None:
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
        sweep = default_deep_driver_sweep_seconds()

        # The registry currently carries 7 drivers with real probe budgets;
        # the derived sweep must cover them all, not a hand-picked constant.
        self.assertGreaterEqual(sweep, 60.0)
        self.assertGreaterEqual(
            estimate_deep_scan_seconds(253),
            estimate_deep_scan_seconds(253, driver_sweep_seconds=0.0),
        )
        self.assertAlmostEqual(
            estimate_deep_scan_seconds(253, driver_sweep_seconds=sweep + 100.0)
            - estimate_deep_scan_seconds(253, driver_sweep_seconds=0.0),
            sweep + 100.0 - policy.deep_scan_followup_estimated_seconds,
        )


class ExtendableDeadlineTests(unittest.TestCase):
    def test_ensure_remaining_extends_up_to_hard_ceiling(self) -> None:
        deadline = ExtendableOnboardingDeadline(
            base_timeout_seconds=5.0,
            hard_ceiling_seconds=120.0,
        )
        base_remaining = deadline.remaining_seconds()
        self.assertIsNotNone(base_remaining)
        self.assertLessEqual(base_remaining, 5.0)

        deadline.ensure_remaining(60.0)
        extended = deadline.remaining_seconds()
        self.assertGreater(extended, 55.0)
        self.assertLessEqual(extended, 60.0)

        # Never shrinks.
        deadline.ensure_remaining(1.0)
        self.assertGreater(deadline.remaining_seconds(), 55.0)

        # Capped by the hard ceiling.
        deadline.ensure_remaining(10_000.0)
        self.assertLessEqual(deadline.remaining_seconds(), 120.0)

    def test_wait_for_honors_extension_granted_mid_wait(self) -> None:
        import asyncio

        async def _run() -> str:
            deadline = ExtendableOnboardingDeadline(
                base_timeout_seconds=0.05,
                hard_ceiling_seconds=30.0,
            )

            async def _slow_success() -> str:
                await asyncio.sleep(0.15)
                return "done"

            async def _extend_soon() -> None:
                await asyncio.sleep(0.02)
                deadline.ensure_remaining(5.0)

            extender = asyncio.create_task(_extend_soon())
            try:
                return await deadline.wait_for(_slow_success())
            finally:
                await extender

        self.assertEqual(asyncio.run(_run()), "done")

    def test_wait_for_raises_without_extension(self) -> None:
        import asyncio

        async def _run() -> None:
            deadline = ExtendableOnboardingDeadline(
                base_timeout_seconds=0.05,
                hard_ceiling_seconds=30.0,
            )

            async def _too_slow() -> None:
                await asyncio.sleep(1.0)

            with self.assertRaises(OnboardingDeadlineExceeded):
                await deadline.wait_for(_too_slow())

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
