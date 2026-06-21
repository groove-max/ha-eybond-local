from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.entity_descriptions import BASE_SENSOR_DESCRIPTIONS


class EntityDescriptionsTests(unittest.TestCase):
    def test_collector_local_ip_is_not_enabled_by_default(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_local_ip_address"
        )

        self.assertFalse(description.enabled_default)

    def test_collector_signal_strength_is_enabled_by_default(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_signal_strength"
        )

        self.assertTrue(description.enabled_default)
        self.assertEqual(description.unit, "dBm")
        self.assertEqual(description.device_class, "signal_strength")

    def test_collector_operation_mode_is_enabled_by_default_enum_sensor(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_operation_mode"
        )

        self.assertTrue(description.enabled_default)
        self.assertEqual(description.device_class, "enum")
        self.assertEqual(description.translation_key, "collector_operation_mode")

    def test_only_collector_mode_sensor_is_exposed_in_base_sensor_descriptions(self) -> None:
        keys = {item.key for item in BASE_SENSOR_DESCRIPTIONS}

        self.assertIn("collector_operation_mode", keys)
        self.assertNotIn("control_mode", keys)
        self.assertNotIn("collector_operation_endpoint_sync_status", keys)

    def test_collector_callback_sync_error_is_diagnostic_and_disabled_by_default(self) -> None:
        description = next(
            item
            for item in BASE_SENSOR_DESCRIPTIONS
            if item.key == "collector_operation_endpoint_sync_error"
        )

        self.assertTrue(description.diagnostic)
        self.assertFalse(description.enabled_default)

    def test_collector_serial_baudrate_is_enabled_by_default(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_serial_baudrate"
        )

        self.assertTrue(description.enabled_default)

    def test_collector_signal_quality_is_enabled_by_default(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_signal_quality"
        )

        self.assertTrue(description.enabled_default)
        self.assertEqual(description.device_class, "enum")
        self.assertEqual(description.translation_key, "collector_signal_quality")
        self.assertEqual(
            description.options,
            ("unknown", "excellent", "good", "fair", "weak"),
        )

    def test_collector_onboarding_status_is_enabled_by_default(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_onboarding_status"
        )

        self.assertTrue(description.enabled_default)

    def test_collector_ssid_is_visible_as_non_diagnostic_status(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_ssid"
        )

        self.assertFalse(description.diagnostic)
        self.assertTrue(description.enabled_default)

    def test_collector_listener_status_is_enabled_diagnostic_sensor(self) -> None:
        description = next(
            item for item in BASE_SENSOR_DESCRIPTIONS if item.key == "collector_listener_status"
        )

        self.assertTrue(description.diagnostic)
        self.assertTrue(description.enabled_default)

    def test_collector_callback_identity_status_is_enabled_diagnostic_sensor(self) -> None:
        description = next(
            item
            for item in BASE_SENSOR_DESCRIPTIONS
            if item.key == "collector_callback_identity_status"
        )

        self.assertTrue(description.diagnostic)
        self.assertTrue(description.enabled_default)

        summary = next(
            item
            for item in BASE_SENSOR_DESCRIPTIONS
            if item.key == "collector_callback_identity_summary"
        )
        self.assertTrue(summary.diagnostic)
        self.assertFalse(summary.enabled_default)

    def test_collector_listener_details_are_hidden_diagnostic_sensors(self) -> None:
        hidden_keys = {
            "collector_listener_bind_host",
            "collector_listener_bind_endpoint",
            "collector_listener_effective_host",
            "collector_listener_advertised_endpoint",
            "collector_listener_rebind_count",
            "collector_listener_last_error",
        }
        descriptions = {
            item.key: item for item in BASE_SENSOR_DESCRIPTIONS if item.key in hidden_keys
        }
        self.assertEqual(set(descriptions), hidden_keys)
        for item in descriptions.values():
            self.assertTrue(item.diagnostic)
            self.assertFalse(item.enabled_default)

    def test_at_only_collector_entities_are_diagnostic_and_disabled_by_default(self) -> None:
        for key in (
            "collector_type",
            "collector_upload_mode",
            "collector_system_time",
            "collector_cloud_heartbeat_value",
            "collector_link_status",
            "collector_wifi_scan_list",
        ):
            description = next(item for item in BASE_SENSOR_DESCRIPTIONS if item.key == key)
            self.assertTrue(description.diagnostic)
            self.assertFalse(description.enabled_default)


if __name__ == "__main__":
    unittest.main()
