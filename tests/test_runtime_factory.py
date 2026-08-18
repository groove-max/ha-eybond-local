from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.runtime.factory import create_runtime_manager


class RuntimeFactoryTests(unittest.TestCase):
    def test_create_runtime_manager_returns_eybond_runtime_branch(self) -> None:
        runtime = create_runtime_manager(
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
            connection_mode="known_ip",
        )

        self.assertIsInstance(runtime, EybondHub)

    def test_runtime_factory_owns_concrete_runtime_construction(self) -> None:
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
        runtime = object()

        with patch(
            "custom_components.eybond_local.runtime.factory.EybondHub",
            return_value=runtime,
        ) as create_hub:
            result = create_runtime_manager(
                connection,
                driver_hint="auto",
                connection_mode="known_ip",
            )

        self.assertIs(result, runtime)
        create_hub.assert_called_once_with(
            connection=connection,
            driver_hint="auto",
            driver_detection_strategy="first_match",
            connection_mode="known_ip",
        )


if __name__ == "__main__":
    unittest.main()
