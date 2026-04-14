from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.register_schema_loader import (
    load_register_schema,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.drivers.smg import SmgModbusDriver


class RegisterSchemaLoaderTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_external_register_schema_roots(())
        load_register_schema.cache_clear()

    def test_loads_smg_base_register_schema(self) -> None:
        schema = load_register_schema("modbus_smg/base.json")

        self.assertEqual(schema.key, "modbus_smg_base")
        self.assertEqual(schema.driver_key, "modbus_smg")
        self.assertEqual(schema.protocol_family, "modbus_smg")
        self.assertEqual(schema.source_name, "modbus_smg/base.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/modbus_smg/base.json"))
        self.assertEqual(schema.block("status").start, 100)
        self.assertEqual(schema.block("live").count, 34)
        self.assertEqual(schema.scalar_register("rated_power_register"), 643)
        self.assertEqual(schema.enum_map_for("mode_names")[3], "Off-Grid")
        self.assertEqual(schema.bit_labels_for("warning_code_names")[9], "Battery Not Connected")
        self.assertEqual(len(schema.spec_set("config")), 29)
        self.assertEqual(len(schema.measurement_descriptions), 86)
        self.assertEqual(len(schema.binary_sensor_descriptions), 18)

    def test_infers_measurement_display_precision_from_register_specs(self) -> None:
        schema = load_register_schema("modbus_smg/base.json")

        self.assertEqual(
            schema.measurement_description("battery_voltage").suggested_display_precision,
            1,
        )
        self.assertEqual(
            schema.measurement_description("grid_frequency").suggested_display_precision,
            2,
        )
        self.assertEqual(
            schema.measurement_description("output_rating_frequency").suggested_display_precision,
            2,
        )
        self.assertIsNone(
            schema.measurement_description("grid_power").suggested_display_precision,
        )

    def test_loads_smg_model_overlay_schema(self) -> None:
        schema = load_register_schema("modbus_smg/models/smg_6200.json")

        self.assertEqual(schema.key, "modbus_smg_6200")
        self.assertEqual(schema.title, "SMG 6200 Register Schema")
        self.assertEqual(schema.driver_key, "modbus_smg")
        self.assertEqual(schema.protocol_family, "modbus_smg")
        self.assertEqual(schema.source_name, "modbus_smg/models/smg_6200.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertEqual(schema.block("status").start, 100)
        self.assertEqual(schema.block("live").count, 34)
        self.assertEqual(schema.scalar_register("rated_power_register"), 643)
        self.assertEqual(schema.enum_map_for("mode_names")[3], "Off-Grid")
        self.assertEqual(
            schema.measurement_description("operational_state").name,
            "System Status",
        )
        self.assertEqual(
            schema.binary_sensor_description("battery_connected").name,
            "Battery Connected",
        )

    def test_loads_pi30_base_register_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/base.json")

        self.assertEqual(schema.key, "pi30_ascii_base")
        self.assertEqual(schema.title, "PI30 / ASCII Register Schema")
        self.assertEqual(schema.driver_key, "pi30")
        self.assertEqual(schema.protocol_family, "pi30")
        self.assertEqual(schema.source_name, "pi30_ascii/base.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi30_ascii/base.json"))
        self.assertEqual(schema.measurement_description("protocol_id").name, "Protocol ID")
        self.assertEqual(schema.measurement_description("pv_generation_sum").state_class, "total_increasing")
        self.assertEqual(schema.binary_sensor_description("lcd_backlight_enabled").name, "LCD Backlight Enabled")
        self.assertFalse(schema.binary_sensor_description("lcd_backlight_enabled").live)
        self.assertEqual(schema.enum_map_for("battery_type_names")[3], "Pylon")
        self.assertEqual(schema.enum_map_for("operating_mode_names")["L"], "Line")
        self.assertEqual(schema.enum_map_for("inverter_charge_state_names")[10], "No charging")
        self.assertEqual(schema.enum_map_for("machine_type_names")[10], "Hybrid")
        self.assertEqual(schema.bit_labels_for("alarm_status_names")[5], "Line fail warning")
        self.assertEqual(schema.block("status").count, 0)
        self.assertEqual(schema.block("serial").count, 0)
        self.assertEqual(schema.block("live").count, 0)
        self.assertEqual(schema.block("config").count, 0)
        self.assertEqual(len(schema.spec_sets), 0)

    def test_loads_pi18_base_register_schema(self) -> None:
        schema = load_register_schema("pi18_ascii/base.json")

        self.assertEqual(schema.key, "pi18_ascii_base")
        self.assertEqual(schema.title, "PI18 / Experimental Register Schema")
        self.assertEqual(schema.driver_key, "pi18")
        self.assertEqual(schema.protocol_family, "pi18")
        self.assertEqual(schema.source_name, "pi18_ascii/base.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi18_ascii/base.json"))
        self.assertEqual(schema.measurement_description("protocol_id").name, "Protocol ID")
        self.assertEqual(schema.measurement_description("operating_mode").name, "Operating Mode")
        self.assertEqual(schema.binary_sensor_description("warning_active").name, "Warning Active")
        self.assertEqual(schema.enum_map_for("battery_type_names")[2], "User")
        self.assertEqual(schema.enum_map_for("operating_mode_names")[5], "Hybrid")
        self.assertEqual(schema.block("status").count, 0)
        self.assertEqual(schema.block("serial").count, 0)
        self.assertEqual(schema.block("live").count, 0)
        self.assertEqual(schema.block("config").count, 0)
        self.assertEqual(len(schema.spec_sets), 0)

    def test_loads_pi30_default_model_overlay_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/default.json")

        self.assertEqual(schema.key, "pi30_ascii_default")
        self.assertEqual(schema.title, "PI30 / ASCII Default Register Schema")
        self.assertEqual(schema.driver_key, "pi30")
        self.assertEqual(schema.protocol_family, "pi30")
        self.assertEqual(schema.source_name, "pi30_ascii/models/default.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi30_ascii/models/default.json"))
        self.assertEqual(schema.measurement_description("protocol_id").name, "Protocol ID")
        self.assertEqual(schema.binary_sensor_description("lcd_backlight_enabled").name, "LCD Backlight Enabled")
        self.assertEqual(schema.enum_map_for("operating_mode_names")["L"], "Line")
        self.assertEqual(schema.bit_labels_for("alarm_status_names")[5], "Line fail warning")

    def test_loads_pi30_vmii_model_overlay_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/vmii_nxpw5kw.json")

        self.assertEqual(schema.key, "pi30_ascii_vmii_nxpw5kw")
        self.assertEqual(schema.title, "PI30 / ASCII VMII-NXPW5KW Register Schema")
        self.assertEqual(schema.driver_key, "pi30")
        self.assertEqual(schema.protocol_family, "pi30")
        self.assertEqual(schema.source_name, "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi30_ascii/models/vmii_nxpw5kw.json"))
        self.assertEqual(schema.measurement_description("protocol_id").name, "Protocol ID")
        self.assertEqual(schema.enum_map_for("operating_mode_names")["L"], "Line")

    def test_loads_pi30_pi41_model_overlay_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/pi41.json")

        self.assertEqual(schema.key, "pi30_ascii_pi41")
        self.assertEqual(schema.title, "PI41 / ASCII Register Schema")
        self.assertEqual(schema.source_name, "pi30_ascii/models/pi41.json")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi30_ascii/models/pi41.json"))
        self.assertEqual(schema.enum_map_for("operating_mode_names")["L"], "Line")

    def test_loads_pi30_max_model_overlay_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/pi30_max.json")

        self.assertEqual(schema.key, "pi30_ascii_pi30_max")
        self.assertEqual(schema.title, "PI30 MAX / ASCII Register Schema")
        self.assertEqual(schema.source_name, "pi30_ascii/models/pi30_max.json")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi30_ascii/models/pi30_max.json"))
        self.assertEqual(schema.enum_map_for("operating_mode_names")["L"], "Line")

    def test_loads_pi30_pip_gk_model_overlay_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/pi30_pip_gk.json")

        self.assertEqual(schema.key, "pi30_ascii_pi30_pip_gk")
        self.assertEqual(schema.title, "PI30 PIP-GK / ASCII Register Schema")
        self.assertEqual(schema.source_name, "pi30_ascii/models/pi30_pip_gk.json")
        self.assertTrue(schema.source_path.endswith("register_schemas/pi30_ascii/models/pi30_pip_gk.json"))
        self.assertEqual(schema.enum_map_for("operating_mode_names")["L"], "Line")

    def test_smg_driver_uses_loaded_register_schema(self) -> None:
        schema = load_register_schema("modbus_smg/models/smg_6200.json")
        driver = SmgModbusDriver()

        self.assertEqual(driver.register_schema_metadata.key, schema.key)
        self.assertEqual(driver.register_schema_metadata.block("status").start, schema.block("status").start)
        self.assertEqual(driver.register_schema_metadata.block("status").count, schema.block("status").count)
        self.assertEqual(driver.register_schema_metadata.block("serial").start, schema.block("serial").start)
        self.assertEqual(driver.register_schema_metadata.block("serial").count, schema.block("serial").count)
        self.assertEqual(driver.register_schema_metadata.block("live").start, schema.block("live").start)
        self.assertEqual(driver.register_schema_metadata.block("live").count, schema.block("live").count)
        self.assertEqual(driver.register_schema_metadata.block("config").start, schema.block("config").start)
        self.assertEqual(driver.register_schema_metadata.block("config").count, schema.block("config").count)
        self.assertEqual(driver.register_schema_metadata.scalar_register("rated_power_register"), schema.scalar_register("rated_power_register"))
        self.assertEqual(driver.register_schema_metadata.enum_map_for("mode_names"), schema.enum_map_for("mode_names"))
        self.assertEqual(driver.register_schema_metadata.bit_labels_for("fault_code_names"), schema.bit_labels_for("fault_code_names"))
        self.assertEqual(driver.register_schema_metadata.bit_labels_for("warning_code_names"), schema.bit_labels_for("warning_code_names"))
        self.assertEqual(driver.register_schema_metadata.spec_set("status"), schema.spec_set("status"))
        self.assertEqual(driver.register_schema_metadata.spec_set("live"), schema.spec_set("live"))
        self.assertEqual(driver.register_schema_metadata.spec_set("config"), schema.spec_set("config"))
        self.assertEqual(driver.register_schema_metadata.spec_set("aux_config"), schema.spec_set("aux_config"))
        self.assertEqual(driver.measurements, schema.measurement_descriptions)
        self.assertEqual(
            driver.binary_sensors,
            schema.binary_sensor_descriptions,
        )

    def test_pi30_driver_uses_loaded_register_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/default.json")
        driver = Pi30Driver()

        self.assertEqual(driver.register_schema_metadata.key, schema.key)
        self.assertEqual(driver.measurements, schema.measurement_descriptions)
        self.assertEqual(driver.binary_sensors, schema.binary_sensor_descriptions)

    def test_smg_driver_resolves_external_schema_override_dynamically(self) -> None:
        raw = {
            "extends": "builtin:modbus_smg/models/smg_6200.json",
            "schema_key": "dynamic_external_smg_6200",
            "title": "Dynamic External SMG 6200 Register Schema",
            "measurement_descriptions": [
                {
                    "key": "operational_state",
                    "name": "Dynamic External System Status",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "modbus_smg" / "models" / "smg_6200.json"
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(json.dumps(raw), encoding="utf-8")
            set_external_register_schema_roots((Path(temp_dir),))
            driver = SmgModbusDriver()

            self.assertEqual(
                driver.register_schema_metadata.key,
                "dynamic_external_smg_6200",
            )
            self.assertEqual(
                driver.register_schema_metadata.measurement_description("operational_state").name,
                "Dynamic External System Status",
            )

    def test_prefers_external_schema_root_and_can_extend_builtin_schema(self) -> None:
        raw = {
            "extends": "modbus_smg/base.json",
            "schema_key": "external_smg_6200",
            "title": "External SMG 6200 Register Schema",
            "driver_key": "modbus_smg",
            "protocol_family": "modbus_smg",
            "measurement_descriptions": [
                {
                    "key": "operational_state",
                    "name": "External System Status",
                    "icon": "mdi:rocket-launch",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = (
                Path(temp_dir)
                / "modbus_smg"
                / "models"
                / "smg_6200.json"
            )
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(json.dumps(raw), encoding="utf-8")
            set_external_register_schema_roots((Path(temp_dir),))

            schema = load_register_schema("modbus_smg/models/smg_6200.json")

        self.assertEqual(schema.title, "External SMG 6200 Register Schema")
        self.assertEqual(schema.key, "external_smg_6200")
        self.assertEqual(schema.source_scope, "external")
        self.assertEqual(schema.source_path, str(schema_path.resolve()))
        self.assertEqual(
            schema.measurement_description("operational_state").name,
            "External System Status",
        )
        self.assertEqual(schema.block("live").count, 34)

    def test_explicit_measurement_precision_overrides_inferred_precision(self) -> None:
        raw = {
            "extends": "modbus_smg/base.json",
            "schema_key": "external_smg_precision_override",
            "title": "External SMG Precision Override",
            "measurement_descriptions": [
                {
                    "key": "battery_voltage",
                    "suggested_display_precision": 3,
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "modbus_smg" / "models" / "smg_6200.json"
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(json.dumps(raw), encoding="utf-8")
            set_external_register_schema_roots((Path(temp_dir),))

            schema = load_register_schema("modbus_smg/models/smg_6200.json")

        self.assertEqual(
            schema.measurement_description("battery_voltage").suggested_display_precision,
            3,
        )


if __name__ == "__main__":
    unittest.main()
