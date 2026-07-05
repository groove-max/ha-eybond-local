from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.registry import (
    binary_sensors_for_driver,
    binary_sensors_for_runtime,
    get_driver,
    measurements_for_driver,
    measurements_for_runtime,
)
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile


class RegistryTests(unittest.TestCase):
    def test_common_primary_core_is_consistent_across_smg_and_pi30(self) -> None:
        expected_primary = {
            "operating_mode": ("Operating Mode", "mdi:state-machine"),
            "output_power": ("Load Power", "mdi:flash"),
            "load_percent": ("Load Percent", "mdi:gauge"),
            "battery_percent": ("Battery Percent", "mdi:battery"),
            "battery_power": ("Battery Power", "mdi:battery-medium"),
            "pv_power": ("PV Power", "mdi:solar-power"),
        }

        for driver_key in ("modbus_smg", "pi30"):
            descriptions = {
                description.key: description
                for description in measurements_for_driver(driver_key)
            }
            for key, (expected_name, expected_icon) in expected_primary.items():
                self.assertIn(key, descriptions)
                self.assertFalse(
                    descriptions[key].diagnostic,
                    msg=f"{driver_key}:{key} should stay in the primary/common sensor set",
                )
                self.assertEqual(descriptions[key].name, expected_name)
                self.assertEqual(descriptions[key].icon, expected_icon)

    def test_measurements_for_smg_do_not_include_pi30_only_keys(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_driver("modbus_smg")
        }
        keys = set(descriptions)

        self.assertIn("collector_remote_ip", keys)
        self.assertIn("warning_code", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("inverter_date", keys)
        self.assertIn("inverter_time", keys)
        self.assertNotIn("protocol_id", keys)
        self.assertNotIn("pv_generation_sum", keys)
        self.assertTrue(descriptions["inverter_date"].diagnostic)
        self.assertFalse(descriptions["inverter_date"].enabled_default)
        self.assertTrue(descriptions["inverter_time"].diagnostic)
        self.assertFalse(descriptions["inverter_time"].enabled_default)

    def test_measurements_for_anenji_runtime_include_variant_specific_keys(self) -> None:
        profile = load_driver_profile("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json")
        descriptions = {
            description.key: description
            for description in measurements_for_runtime(
                driver_key="modbus_smg",
                register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
                write_capabilities=profile.capabilities,
            )
        }

        self.assertIn("pv1_voltage", descriptions)
        self.assertIn("pv1_current", descriptions)
        self.assertIn("pv1_power", descriptions)
        self.assertIn("pv2_voltage", descriptions)
        self.assertIn("pv2_current", descriptions)
        self.assertIn("pv2_power", descriptions)
        self.assertIn("pv_generation_sum", descriptions)
        self.assertTrue(descriptions["input_mode"].enabled_default)
        self.assertTrue(descriptions["battery_type"].enabled_default)

    def test_measurements_for_unknown_runtime_can_stay_base_only(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_runtime(
                include_all_drivers_when_unknown=False,
            )
        }

        self.assertIn("collector_pn", descriptions)
        self.assertIn("detection_confidence", descriptions)
        self.assertNotIn("output_power", descriptions)
        self.assertNotIn("warning_code", descriptions)
        self.assertNotIn("protocol_id", descriptions)

    def test_measurements_for_unknown_runtime_can_be_limited_to_collector_surface(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_runtime(
                include_all_drivers_when_unknown=False,
                collector_only_mode=True,
            )
        }

        self.assertIn("collector_pn", descriptions)
        self.assertIn("collector_operation_mode", descriptions)
        self.assertIn("last_error", descriptions)
        self.assertNotIn("driver_key", descriptions)
        self.assertNotIn("model_name", descriptions)
        self.assertNotIn("serial_number", descriptions)
        self.assertNotIn("detection_confidence", descriptions)
        self.assertNotIn("support_package_path", descriptions)
        self.assertNotIn("local_metadata_status", descriptions)

    def test_collector_only_mode_suppresses_driver_measurements(self) -> None:
        # Field report: a manual "modbus_catalog" driver hint on a collector
        # with no inverter attached materialized the whole Aohai schema as
        # unavailable sensors on the collector device.
        descriptions = {
            description.key
            for description in measurements_for_runtime(
                driver_key="modbus_catalog",
                include_all_drivers_when_unknown=False,
                collector_only_mode=True,
            )
        }

        self.assertIn("collector_pn", descriptions)
        self.assertNotIn("battery_percent", descriptions)
        self.assertNotIn("output_power", descriptions)
        self.assertNotIn("inverter_operation_mode", descriptions)

    def test_collector_only_mode_suppresses_schema_binary_sensors(self) -> None:
        descriptions = binary_sensors_for_runtime(
            driver_key="modbus_smg",
            register_schema_name="modbus_smg/base.json",
            include_all_drivers_when_unknown=False,
            collector_only_mode=True,
        )

        schema_keys = {description.key for description in descriptions}
        self.assertNotIn("power_flow_pv_connection_state", schema_keys)

    def test_binary_sensors_for_unknown_runtime_can_stay_base_only(self) -> None:
        descriptions = binary_sensors_for_runtime(
            include_all_drivers_when_unknown=False,
        )

        self.assertEqual(descriptions, ())

    def test_measurements_for_pi30_do_not_include_smg_only_keys(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_driver("pi30")
        }
        keys = set(descriptions)

        self.assertIn("collector_remote_ip", keys)
        self.assertIn("variant_key", keys)
        self.assertIn("profile_name", keys)
        self.assertIn("register_schema_name", keys)
        self.assertIn("protocol_id", keys)
        self.assertIn("pv_generation_sum", keys)
        self.assertIn("grid_voltage", keys)
        self.assertIn("grid_frequency", keys)
        self.assertIn("output_power", keys)
        self.assertIn("pv_voltage", keys)
        self.assertIn("pv_current", keys)
        self.assertIn("pv_power", keys)
        self.assertIn("battery_power", keys)
        self.assertNotIn("warning_code", keys)
        self.assertTrue(descriptions["input_voltage"].diagnostic)
        self.assertTrue(descriptions["input_frequency"].diagnostic)
        self.assertTrue(descriptions["output_voltage"].diagnostic)
        self.assertTrue(descriptions["output_active_power"].diagnostic)
        self.assertTrue(descriptions["battery_voltage"].diagnostic)
        self.assertTrue(descriptions["battery_charge_current"].diagnostic)
        self.assertTrue(descriptions["battery_discharge_current"].diagnostic)
        self.assertTrue(descriptions["pv_input_voltage"].diagnostic)
        self.assertTrue(descriptions["pv_input_current"].diagnostic)
        self.assertTrue(descriptions["pv_input_power"].diagnostic)
        self.assertTrue(descriptions["grid_voltage"].diagnostic)
        self.assertTrue(descriptions["grid_frequency"].diagnostic)
        self.assertTrue(descriptions["pv_voltage"].diagnostic)
        self.assertTrue(descriptions["pv_current"].diagnostic)

    def test_binary_sensors_for_smg_do_not_include_pi30_only_flags(self) -> None:
        keys = {description.key for description in binary_sensors_for_driver("modbus_smg")}

        self.assertNotIn("scc_flag", keys)
        self.assertNotIn("buzzer_enabled", keys)

    def test_measurements_for_smg_preserve_schema_display_precision(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_driver("modbus_smg")
        }

        self.assertEqual(descriptions["battery_voltage"].suggested_display_precision, 1)
        self.assertEqual(descriptions["grid_frequency"].suggested_display_precision, 2)

    def test_smg_writable_controls_have_default_enabled_readback(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_driver("modbus_smg")
        }
        driver = get_driver("modbus_smg")

        mismatched = sorted(
            capability.key
            for capability in driver.write_capabilities
            if capability.value_kind != "action"
            if capability.value_key in descriptions
            if not descriptions[capability.value_key].enabled_default
        )

        self.assertEqual(mismatched, [])
        self.assertTrue(descriptions["output_mode"].enabled_default)
        self.assertTrue(descriptions["output_source_priority"].enabled_default)
        self.assertTrue(descriptions["buzzer_mode"].enabled_default)
        self.assertTrue(descriptions["charge_source_priority"].enabled_default)
        self.assertTrue(descriptions["battery_float_voltage"].enabled_default)
        self.assertIn("low_dc_protection_soc_grid_mode", descriptions)
        self.assertTrue(descriptions["low_dc_protection_soc_grid_mode"].enabled_default)
        self.assertTrue(descriptions["solar_battery_utility_return_soc_threshold"].enabled_default)
        self.assertTrue(descriptions["low_dc_cutoff_soc"].enabled_default)
        self.assertTrue(descriptions["max_ac_charge_current"].enabled_default)

    def test_pi30_writable_controls_have_readback_entities(self) -> None:
        measurement_keys = {
            description.key
            for description in measurements_for_driver("pi30")
        }
        binary_keys = {
            description.key
            for description in binary_sensors_for_driver("pi30")
        }
        driver = get_driver("pi30")

        missing = sorted(
            capability.key
            for capability in driver.write_capabilities
            if capability.value_kind != "action"
            if capability.value_key not in measurement_keys | binary_keys
        )

        self.assertEqual(missing, [])
        self.assertIn("battery_redischarge_voltage", measurement_keys)
        self.assertIn("buzzer_enabled", binary_keys)

    def test_pi30_fractional_voltage_readbacks_inherit_precision_from_capabilities(self) -> None:
        descriptions = {
            description.key: description
            for description in measurements_for_driver("pi30")
        }

        self.assertEqual(descriptions["battery_recharge_voltage"].suggested_display_precision, 1)
        self.assertEqual(descriptions["battery_redischarge_voltage"].suggested_display_precision, 1)
        self.assertEqual(descriptions["battery_under_voltage"].suggested_display_precision, 1)
        self.assertEqual(descriptions["battery_bulk_voltage"].suggested_display_precision, 1)
        self.assertEqual(descriptions["battery_float_voltage"].suggested_display_precision, 1)


if __name__ == "__main__":
    unittest.main()
