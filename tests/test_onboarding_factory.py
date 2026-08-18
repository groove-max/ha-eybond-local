from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


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
        )

        self.assertIsInstance(manager, OnboardingDetector)

    def test_onboarding_factory_owns_concrete_detector_construction(self) -> None:
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
        manager = object()

        with patch(
            "custom_components.eybond_local.onboarding.factory.OnboardingDetector",
            return_value=manager,
        ) as create_detector:
            result = create_onboarding_manager(connection)

        self.assertIs(result, manager)
        create_detector.assert_called_once_with(connection=connection)


if __name__ == "__main__":
    unittest.main()
