from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    auto_scan_timeout_seconds,
    deep_scan_timeout_seconds,
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


if __name__ == "__main__":
    unittest.main()
