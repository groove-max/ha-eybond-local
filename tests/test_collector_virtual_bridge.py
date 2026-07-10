"""Tests for ESP EyeBond Collector token-based bridge detection."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.at import parse_at_response  # noqa: E402
from custom_components.eybond_local.collector.at_runtime import (  # noqa: E402
    query_runtime_collector_at_values,
)
from custom_components.eybond_local.collector.capabilities import (  # noqa: E402
    EspCollectorHardwareToken,
    parse_esp_collector_hardware_token,
)
from custom_components.eybond_local.connection.models import EybondConnectionSpec  # noqa: E402
from custom_components.eybond_local.models import CollectorInfo  # noqa: E402
from custom_components.eybond_local.runtime.hub import EybondHub  # noqa: E402


class _FakeLinkManager:
    """Minimal link manager that surfaces one fixed collector identity."""

    def __init__(self) -> None:
        self.connected = True
        self.collector_info = CollectorInfo(remote_ip="192.0.2.14")
        self.transport = object()
        self.collector_at_transport = None


def _make_hub() -> EybondHub:
    hub = EybondHub(
        connection=EybondConnectionSpec(
            server_ip="192.0.2.10",
            collector_ip="192.0.2.14",
            tcp_port=8899,
            udp_port=58899,
            discovery_target="192.0.2.255",
            discovery_interval=30,
            heartbeat_interval=60,
            request_timeout=5.0,
        ),
    )
    hub._link_manager = _FakeLinkManager()
    return hub


class EspCollectorHardwareTokenTests(unittest.TestCase):
    def test_parses_bridge_version_and_platform(self) -> None:
        token = parse_esp_collector_hardware_token("esp-collector/0.1.5/ESP32")

        self.assertEqual(
            token,
            EspCollectorHardwareToken(
                is_bridge=True,
                version="0.1.5",
                platform="ESP32",
            ),
        )

    def test_ignores_extra_future_segments(self) -> None:
        token = parse_esp_collector_hardware_token(
            "esp-collector/0.2.0/ESP32/cloud/local-feature"
        )

        self.assertTrue(token.is_bridge)
        self.assertEqual(token.version, "0.2.0")
        self.assertEqual(token.platform, "ESP32/cloud/local-feature")

    def test_non_token_hardware_is_not_a_bridge(self) -> None:
        self.assertEqual(
            parse_esp_collector_hardware_token("BK72xx/RTL87xx"),
            EspCollectorHardwareToken(),
        )
        self.assertEqual(
            parse_esp_collector_hardware_token("1.0"),
            EspCollectorHardwareToken(),
        )


class RuntimeCollectorAtQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_query_never_sends_vdtu(self) -> None:
        class _Transport:
            def __init__(self) -> None:
                self.commands: list[str] = []

            async def async_query(self, command: str):
                self.commands.append(command)
                return parse_at_response(f"AT+{command}:")

        transport = _Transport()
        await query_runtime_collector_at_values(transport)

        self.assertNotIn("VDTU", transport.commands)


class HubBridgeTokenTests(unittest.TestCase):
    def test_hardware_token_sets_virtual_bridge_snapshot_fields(self) -> None:
        hub = _make_hub()

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_hardware_version": "esp-collector/0.1.5/ESP32",
            }
        )

        self.assertTrue(snapshot.collector.collector_virtual_bridge)
        self.assertEqual(snapshot.collector.collector_bridge_kind, "esp-collector")
        self.assertEqual(snapshot.collector.collector_bridge_version, "0.1.5")
        self.assertTrue(snapshot.values["collector_virtual_bridge"])
        self.assertEqual(snapshot.values["collector_bridge_kind"], "esp-collector")
        self.assertEqual(snapshot.values["collector_bridge_version"], "0.1.5")

    def test_non_token_hardware_leaves_factory_identity(self) -> None:
        hub = _make_hub()

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_hardware_version": "1.0",
            }
        )

        self.assertFalse(snapshot.collector.collector_virtual_bridge)
        self.assertNotIn("collector_virtual_bridge", snapshot.values)


if __name__ == "__main__":
    unittest.main()
