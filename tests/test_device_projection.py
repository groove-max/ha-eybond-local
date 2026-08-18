from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import CollectorInfo
from custom_components.eybond_local.runtime.device_projection import (
    build_collector_device_info_payload,
    build_inverter_device_info_payload,
)


class DeviceInfoProjectionTests(unittest.TestCase):
    def test_runtime_inverter_identity_wins_over_persisted_fallback(self) -> None:
        payload = build_inverter_device_info_payload(
            entry_id="entry-1",
            entry_title="Persisted title",
            detected_model="Persisted model",
            detected_serial="persisted-serial",
            inverter=SimpleNamespace(
                model_name="Live model",
                serial_number="live-serial",
            ),
        )

        self.assertEqual(payload["name"], "Live model")
        self.assertEqual(payload["model"], "Live model")
        self.assertEqual(payload["serial_number"], "live-serial")
        self.assertEqual(payload["via_device"], ("eybond_local", "entry-1:collector"))

    def test_persisted_inverter_identity_keeps_startup_device_stable(self) -> None:
        payload = build_inverter_device_info_payload(
            entry_id="entry-1",
            entry_title="Collector entry",
            detected_model="PI30 3500",
            detected_serial="55355535553555",
            inverter=None,
        )

        self.assertEqual(payload["name"], "PI30 3500")
        self.assertEqual(payload["model"], "PI30 3500")
        self.assertEqual(payload["serial_number"], "55355535553555")

    def test_factory_collector_projection_has_no_speculative_manufacturer(self) -> None:
        collector = CollectorInfo(
            collector_pn="E50000200000000001",
            profile_name="EyeBond ASCII PN v1",
            smartess_collector_version="8.50.12.3",
        )

        payload = build_collector_device_info_payload(
            entry_id="entry-1",
            collector_ip="192.168.1.55",
            collector_pn=collector.collector_pn,
            collector=collector,
            values={},
            entry_data={"collector_kind": "factory_eybond"},
            entry_options={},
        )

        self.assertEqual(payload["name"], "Collector PN E50000200000000001")
        self.assertEqual(payload["model"], "EyeBond ASCII PN v1")
        self.assertEqual(payload["sw_version"], "8.50.12.3")
        self.assertNotIn("manufacturer", payload)

    def test_virtual_bridge_projection_uses_community_identity(self) -> None:
        values = {
            "collector_hardware_version": "esp-collector/0.4.0/ESP8266",
            "collector_bridge_version": "0.4.0",
        }

        payload = build_collector_device_info_payload(
            entry_id="entry-bridge",
            collector_ip="192.0.2.55",
            collector_pn="V000405SYN94677058",
            collector=None,
            values=values,
            entry_data={"collector_kind": "esp_eybond_bridge"},
            entry_options={},
        )

        self.assertEqual(payload["model"], "ESP EyeBond Collector")
        self.assertEqual(
            payload["manufacturer"],
            "ESP EyeBond Collector (community)",
        )
        self.assertEqual(payload["sw_version"], "0.4.0")
        self.assertEqual(payload["hw_version"], values["collector_hardware_version"])
        self.assertEqual(
            payload["configuration_url"],
            "https://github.com/groove-max/esp-eybond-collector",
        )


class DeviceProjectionArchitectureTests(unittest.TestCase):
    def test_projector_has_no_home_assistant_or_coordinator_dependency(self) -> None:
        source = (
            REPO_ROOT
            / "custom_components/eybond_local/runtime/device_projection.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("homeassistant", source)
        self.assertNotIn("runtime.coordinator", source)
        self.assertNotIn("config_flow", source)

    def test_coordinator_retains_registry_authority_but_not_payload_algorithms(self) -> None:
        source = (
            REPO_ROOT / "custom_components/eybond_local/runtime/coordinator.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        coordinator = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "EybondLocalCoordinator"
        )
        methods = {
            node.name for node in coordinator.body if isinstance(node, ast.FunctionDef)
        }

        self.assertNotIn("_build_inverter_device_info", methods)
        self.assertNotIn("_build_collector_device_info", methods)
        self.assertIn("_async_sync_inverter_device_registry", methods)
        self.assertIn("_async_sync_collector_device_registry", methods)
        self.assertIn("dr.async_get", source)
        self.assertIn("build_inverter_device_info_payload", source)
        self.assertIn("build_collector_device_info_payload", source)


if __name__ == "__main__":
    unittest.main()
