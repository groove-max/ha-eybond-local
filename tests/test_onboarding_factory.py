from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.onboarding.eybond import OnboardingDetector
from custom_components.eybond_local.onboarding.factory import create_onboarding_manager


class OnboardingFactoryTests(unittest.TestCase):
    def test_create_onboarding_manager_returns_eybond_detector_branch(self) -> None:
        manager = create_onboarding_manager(
            EybondConnectionSpec(
                server_ip="192.168.1.50",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
            driver_hint="auto",
        )

        self.assertIsInstance(manager, OnboardingDetector)

    def test_create_onboarding_manager_delegates_to_connection_branch_registry(self) -> None:
        connection = EybondConnectionSpec(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.14",
            tcp_port=8899,
            udp_port=58899,
            discovery_target="192.168.1.255",
            discovery_interval=30,
            heartbeat_interval=60,
            request_timeout=5.0,
        )
        branch = Mock()
        branch.create_onboarding_manager.return_value = object()

        with patch(
            "custom_components.eybond_local.onboarding.factory.get_connection_branch_for_spec",
            return_value=branch,
        ) as get_branch:
            manager = create_onboarding_manager(
                connection,
                driver_hint="auto",
            )

        self.assertIs(manager, branch.create_onboarding_manager.return_value)
        get_branch.assert_called_once_with(connection)
        branch.create_onboarding_manager.assert_called_once_with(
            connection,
            driver_hint="auto",
        )


if __name__ == "__main__":
    unittest.main()
