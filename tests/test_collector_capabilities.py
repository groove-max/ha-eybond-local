from __future__ import annotations

from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.capabilities import (  # noqa: E402
    COLLECTOR_KIND_ESP_EYBOND_BRIDGE,
    COLLECTOR_KIND_FACTORY_EYBOND,
    collector_capability_profile,
    collector_capability_profile_from_runtime,
)
from custom_components.eybond_local.const import (  # noqa: E402
    COLLECTOR_OPERATION_HA_ONLY,
    COLLECTOR_OPERATION_SMARTESS_AND_HA,
)


class CollectorCapabilityProfileTests(unittest.TestCase):
    def test_factory_collector_capabilities_keep_cloud_paths(self) -> None:
        profile = collector_capability_profile()

        self.assertEqual(profile.collector_kind, COLLECTOR_KIND_FACTORY_EYBOND)
        self.assertFalse(profile.virtual_bridge)
        self.assertEqual(
            profile.allowed_operation_modes,
            (COLLECTOR_OPERATION_SMARTESS_AND_HA, COLLECTOR_OPERATION_HA_ONLY),
        )
        self.assertTrue(profile.cloud_evidence)
        self.assertTrue(profile.proxy_capture)
        self.assertTrue(profile.shadow_learning)
        self.assertTrue(profile.wifi_management)
        self.assertFalse(profile.uart_management)

    def test_esp_bridge_capabilities_are_local_only(self) -> None:
        profile = collector_capability_profile(virtual_bridge=True)

        self.assertEqual(profile.collector_kind, COLLECTOR_KIND_ESP_EYBOND_BRIDGE)
        self.assertTrue(profile.virtual_bridge)
        self.assertEqual(profile.allowed_operation_modes, (COLLECTOR_OPERATION_HA_ONLY,))
        self.assertTrue(profile.ha_only_required)
        self.assertFalse(profile.cloud_evidence)
        self.assertFalse(profile.proxy_capture)
        self.assertFalse(profile.shadow_learning)
        self.assertTrue(profile.wifi_management)
        self.assertTrue(profile.uart_management)
        self.assertTrue(profile.uart_runtime_speed_change)
        self.assertEqual(profile.identity_probe, "AT+VDTU")

    def test_esp_bridge_bk72xx_disables_runtime_uart_speed_change(self) -> None:
        profile = collector_capability_profile(
            virtual_bridge=True,
            hardware_version="BK72xx/RTL87xx",
        )

        self.assertTrue(profile.uart_management)
        self.assertFalse(profile.uart_runtime_speed_change)

    def test_runtime_evidence_detects_bridge_from_values(self) -> None:
        profile = collector_capability_profile_from_runtime(
            collector=types.SimpleNamespace(collector_virtual_bridge=False),
            values={
                "collector_virtual_bridge": True,
                "collector_hardware_version": "ESP32",
            },
            data={},
            options={},
        )

        self.assertTrue(profile.virtual_bridge)
        self.assertTrue(profile.uart_runtime_speed_change)

    def test_collector_pn_alone_keeps_factory_capabilities(self) -> None:
        profile = collector_capability_profile_from_runtime(
            collector=types.SimpleNamespace(
                collector_virtual_bridge=False,
                collector_pn="E5000020000000",
            ),
            values={},
            data={},
            options={},
        )

        self.assertFalse(profile.virtual_bridge)
        self.assertEqual(profile.collector_kind, COLLECTOR_KIND_FACTORY_EYBOND)


if __name__ == "__main__":
    unittest.main()
