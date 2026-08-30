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
    _optional_bitmask,
    load_register_schema,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.drivers.smg import SmgModbusDriver


class RegisterSchemaLoaderTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_external_register_schema_roots(())
        load_register_schema.cache_clear()

    def test_register_bitmask_requires_a_strict_contiguous_field(self) -> None:
        self.assertEqual(_optional_bitmask("0x0030", spec_key="x"), 0x0030)
        self.assertEqual(_optional_bitmask(0x0001, spec_key="x"), 0x0001)
        with self.assertRaises(ValueError):
            _optional_bitmask(True, spec_key="x")
        with self.assertRaises(ValueError):
            _optional_bitmask(" 0x0001", spec_key="x")
        with self.assertRaises(ValueError):
            _optional_bitmask("0x0005", spec_key="x")

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
        self.assertEqual(schema.enum_map_for("battery_type_base")[8], "LiB")
        self.assertEqual(schema.bit_labels_for("warning_code_names")[9], "Battery Not Connected")
        self.assertEqual(
            schema.bit_labels_for("warning_code_names")[19],
            "Lithium Battery Communication Abnormal",
        )
        self.assertEqual(
            schema.bit_labels_for("warning_code_names")[20],
            "Battery Discharge Current Exceeds Set Value",
        )
        self.assertEqual(len(schema.spec_set("config")), 30)
        self.assertEqual(len(schema.measurement_descriptions), 100)
        self.assertEqual(schema.measurement_description("inverter_date").name, "Inverter Date")
        self.assertEqual(schema.measurement_description("inverter_time").name, "Inverter Time")
        self.assertEqual(schema.measurement_description("device_name").name, "Device Name")
        self.assertEqual(schema.measurement_description("power_flow_status").name, "Power Flow Status")
        self.assertEqual(
            schema.measurement_description("power_flow_charge_source_state").name,
            "Power Flow Charge Source",
        )
        self.assertEqual(schema.measurement_description("program_version").name, "Program Version")
        with self.assertRaises(KeyError):
            schema.measurement_description("low_dc_cutoff_soc")
        with self.assertRaises(KeyError):
            schema.measurement_description("max_discharge_current_protection")
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
        config_specs = {spec.key: spec.register for spec in schema.spec_set("config")}
        self.assertEqual(config_specs["beeps_while_primary_source_interrupted"], 304)
        self.assertEqual(config_specs["constant_voltage_to_float_time"], 330)
        self.assertEqual(config_specs["low_dc_cutoff_soc"], 343)
        aux_specs = {spec.key: spec.register for spec in schema.spec_set("aux_config")}
        self.assertEqual(aux_specs["forced_equalization_charging"], 425)
        self.assertEqual(
            schema.measurement_description("max_discharge_current_protection").name,
            "Max Discharge Current Protection",
        )
        self.assertEqual(
            schema.measurement_description("low_dc_cutoff_soc").name,
            "Low DC Cut-Off SOC",
        )
        self.assertEqual(schema.measurement_description("operational_state").name, "System Status")
        self.assertEqual(
            schema.binary_sensor_description("battery_connected").name,
            "Battery Connected",
        )

    def test_loads_anenji_op2_model_overlay_schema(self) -> None:
        # Dual-output (OP2) model: the overlay widens the polled live block to
        # cover output-2 telemetry (239-244) and the config block to cover the
        # OP2 settings (344, 353). The donor unit answered every widened block
        # read (raw_capture range_failures was empty).
        schema = load_register_schema("modbus_smg/models/anenji_op2_6200.json")

        self.assertEqual(schema.key, "modbus_smg_anenji_op2_6200")
        self.assertEqual(schema.driver_key, "modbus_smg")
        # Widened blocks (base: live 201+34, config 300+44).
        self.assertEqual(schema.block("live").start, 201)
        self.assertEqual(schema.block("live").count, 44)
        self.assertEqual(schema.block("config").start, 300)
        self.assertEqual(schema.block("config").count, 54)
        # Base blocks survive the merge untouched.
        self.assertEqual(schema.block("status").start, 100)
        self.assertEqual(schema.block("serial").start, 186)
        # OP2 specs merged on top of the base sets.
        live_registers = {spec.register: spec.key for spec in schema.spec_set("live")}
        self.assertEqual(live_registers[239], "output2_va")
        self.assertEqual(live_registers[240], "output2_power")
        self.assertEqual(live_registers[243], "output2_load_percent")
        self.assertEqual(live_registers[244], "output2_voltage")
        config_registers = {spec.register: spec.key for spec in schema.spec_set("config")}
        self.assertEqual(config_registers[344], "output2_cutoff_soc")
        self.assertEqual(config_registers[353], "output2_overload_warning_percent")
        charge_source_spec = next(
            spec for spec in schema.spec_set("config") if spec.key == "charge_source_priority"
        )
        self.assertEqual(
            charge_source_spec.enum_map,
            {
                1: "PV Priority",
                2: "PV and Utility",
                3: "PV Only",
                4: "PV Priority With Load Reserve",
            },
        )
        # Base live telemetry still present alongside the OP2 additions.
        self.assertEqual(live_registers[202], "grid_voltage")
        self.assertEqual(
            schema.measurement_description("output2_power").name, "Output 2 Power"
        )
        self.assertEqual(schema.measurement_description("output2_voltage").unit, "V")

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
        self.assertEqual(
            {
                key: schema.measurement_description(key).name
                for key in (
                    "tracker_temperature",
                    "inverter_temperature",
                    "battery_temperature",
                    "transformer_temperature",
                )
            },
            {
                "tracker_temperature": "Additional Temperature 1",
                "inverter_temperature": "Additional Temperature 2",
                "battery_temperature": "Additional Temperature 3",
                "transformer_temperature": "Additional Temperature 4",
            },
        )
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

    def test_loads_pi30_smartess_0925_compat_model_overlay_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/smartess_0925_compat.json")

        self.assertEqual(schema.key, "pi30_ascii_smartess_0925_compat")
        self.assertEqual(schema.title, "SmartESS 0925 Compatibility Register Schema")
        self.assertEqual(schema.driver_key, "pi30")
        self.assertEqual(schema.protocol_family, "pi30")
        self.assertEqual(schema.source_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(
            schema.source_path.endswith(
                "register_schemas/pi30_ascii/models/smartess_0925_compat.json"
            )
        )
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

    def test_loads_smartess_0925_model_overlay_schema(self) -> None:
        schema = load_register_schema("smartess_local/models/0925.json")

        self.assertEqual(schema.key, "smartess_0925")
        self.assertEqual(schema.title, "SmartESS 0925 Register Schema")
        self.assertEqual(schema.driver_key, "smartess_local")
        self.assertEqual(schema.protocol_family, "smartess_local")
        self.assertEqual(schema.source_name, "smartess_local/models/0925.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/smartess_local/models/0925.json"))
        self.assertEqual(schema.block("live").start, 4501)
        self.assertEqual(schema.block("live").count, 14)
        self.assertEqual(schema.block("config").start, 5001)
        self.assertEqual(schema.block("config").count, 33)
        self.assertEqual(len(schema.spec_set("live")), 14)
        self.assertEqual(len(schema.spec_set("config_state")), 18)
        self.assertEqual(len(schema.spec_set("energy")), 4)
        self.assertEqual(schema.measurement_description("output_active_power").name, "Output Active Power")
        self.assertEqual(schema.measurement_description("all_energy").state_class, "total_increasing")
        self.assertEqual(schema.measurement_description("today_energy").suggested_display_precision, 2)

    def test_loads_smartess_0921_model_overlay_schema(self) -> None:
        schema = load_register_schema("smartess_local/models/0921.json")

        self.assertEqual(schema.key, "smartess_0921")
        self.assertEqual(schema.title, "SmartESS 0921 Register Schema")
        self.assertEqual(schema.driver_key, "smartess_local")
        self.assertEqual(schema.protocol_family, "smartess_local")
        self.assertEqual(schema.source_name, "smartess_local/models/0921.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/smartess_local/models/0921.json"))
        self.assertEqual(schema.block("live").start, 32)
        self.assertEqual(schema.block("live").count, 54)
        self.assertEqual(schema.block("config").start, 256)
        self.assertEqual(schema.block("config").count, 69)
        self.assertEqual(len(schema.spec_set("live")), 29)
        self.assertEqual(len(schema.spec_set("config")), 37)
        self.assertEqual(schema.measurement_description("grid_power_limit").name, "Grid Power Limit")
        self.assertEqual(schema.measurement_description("cumulative_output_energy").state_class, "total_increasing")
        self.assertEqual(schema.measurement_description("grid_frequency").suggested_display_precision, 2)

    def test_loads_smartess_0912_model_overlay_schema(self) -> None:
        schema = load_register_schema("smartess_local/models/0912.json")

        self.assertEqual(schema.key, "smartess_0912")
        self.assertEqual(schema.title, "SmartESS 0912 Register Schema")
        self.assertEqual(schema.driver_key, "smartess_local")
        self.assertEqual(schema.protocol_family, "smartess_local")
        self.assertEqual(schema.source_name, "smartess_local/models/0912.json")
        self.assertEqual(schema.source_scope, "builtin")
        self.assertTrue(schema.source_path.endswith("register_schemas/smartess_local/models/0912.json"))
        self.assertEqual(schema.block("live").start, 0)
        self.assertEqual(schema.block("live").count, 92)
        self.assertEqual(schema.block("status").start, 95)
        self.assertEqual(schema.block("status").count, 35)
        self.assertEqual(schema.block("config").start, 1000)
        self.assertEqual(schema.block("config").count, 116)
        self.assertEqual(schema.block("update_flags").count, 3)
        self.assertEqual(schema.block("dc_pv_live").count, 24)
        self.assertEqual(len(schema.spec_set("live")), 68)
        self.assertEqual(len(schema.spec_set("status")), 30)
        self.assertEqual(len(schema.spec_set("config")), 93)
        self.assertEqual(len(schema.spec_set("update_flags")), 3)
        self.assertEqual(len(schema.spec_set("dc_pv_live")), 24)
        self.assertEqual(len(schema.measurement_descriptions), 952)
        self.assertIsNotNone(
            next(
                spec
                for spec in schema.spec_set("status")
                if spec.key == "serial_number"
            )
        )
        with self.assertRaises(KeyError):
            schema.measurement_description("serial_number")
        self.assertEqual(schema.measurement_description("battery_power").name, "battery power")
        self.assertEqual(
            schema.measurement_description("highest_temperature_of_battery").unit,
            "°C",
        )
        self.assertEqual(
            schema.measurement_description("charger_to_start_the_update_logo").live,
            False,
        )
        self.assertEqual(
            schema.measurement_description("charger_to_start_the_update_logo").diagnostic,
            True,
        )
        self.assertEqual(schema.measurement_description("battery_soc").suggested_display_precision, 2)

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

    def test_anenji_11kw_schema_exposes_confirmed_shadow_learned_control_values(self) -> None:
        schema = load_register_schema("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json")

        config_specs = {spec.key: spec for spec in schema.spec_set("config")}
        self.assertEqual(config_specs["secondary_output_priority"].register, 602)
        self.assertEqual(
            config_specs["secondary_output_priority"].enum_map,
            {
                0: "OFF",
                1: "SUB",
                2: "SBU",
                3: "SUF",
            },
        )
        self.assertEqual(config_specs["op2_overload_alarm_setting"].register, 608)
        self.assertEqual(config_specs["secondary_charging_priority"].register, 633)
        self.assertEqual(
            config_specs["secondary_charging_priority"].enum_map,
            {
                0: "OFF",
                1: "SOF",
                2: "SNU",
                3: "OSO",
                4: "SOR",
            },
        )
        self.assertEqual(config_specs["constant_voltage_to_float_wait_time"].register, 639)
        self.assertEqual(config_specs["max_discharge_current_protection"].register, 642)
        self.assertEqual(config_specs["op1_offgrid_soc_protection_value"].register, 649)
        pv_generation_day = schema.measurement_description("pv_generation_day")
        self.assertFalse(pv_generation_day.diagnostic)
        self.assertTrue(pv_generation_day.enabled_default)
        self.assertTrue(pv_generation_day.live)

    def test_protocol_3_10_projects_documented_registers_by_protocol_number(self) -> None:
        generic = load_register_schema("modbus_smg/base.json")
        common = load_register_schema(
            "modbus_smg/protocols/communication_protocol_3_10.json"
        )
        protocol_3 = load_register_schema(
            "modbus_smg/protocols/communication_protocol_3.json"
        )
        protocol_4 = load_register_schema(
            "modbus_smg/protocols/communication_protocol_4.json"
        )
        protocol_5 = load_register_schema(
            "modbus_smg/protocols/communication_protocol_5.json"
        )
        protocol_6 = load_register_schema(
            "modbus_smg/protocols/communication_protocol_6.json"
        )
        anj = load_register_schema("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json")
        hhs = load_register_schema("modbus_smg/models/anenji_hhs_11kw_wifi_no_parallel.json")
        sandisolar = load_register_schema(
            "modbus_smg/models/sandisolar_sd_11kp48v_wifi.json"
        )

        common_sources = {
            spec.key: spec.register
            for specs in common.spec_sets.values()
            for spec in specs
        }
        common_registers = {
            spec.register for specs in common.spec_sets.values() for spec in specs
        }
        self.assertEqual(
            {
                key: common_sources[key]
                for key in ("grid_power", "inverter_current", "inverter_power")
            },
            {"grid_power": 340, "inverter_current": 343, "inverter_power": 344},
        )
        for unsupported_key in ("grid_current", "grid_va", "inverter_va"):
            self.assertNotIn(unsupported_key, common_sources)
            with self.assertRaises(KeyError):
                common.measurement_description(unsupported_key)
        self.assertTrue({202, 204, 205, 226, 228, 229}.isdisjoint(common_registers))

        protocol_3_sources = {
            spec.key: spec.register
            for specs in protocol_3.spec_sets.values()
            for spec in specs
        }
        protocol_4_sources = {
            spec.key: spec.register
            for specs in protocol_4.spec_sets.values()
            for spec in specs
        }
        protocol_5_sources = {
            spec.key: spec.register
            for specs in protocol_5.spec_sets.values()
            for spec in specs
        }
        protocol_6_sources = {
            spec.key: spec.register
            for specs in protocol_6.spec_sets.values()
            for spec in specs
        }
        self.assertEqual(
            {
                key: protocol_3_sources[key]
                for key in ("output_current", "output_power", "output_va", "load_percent")
            },
            {
                "output_current": 347,
                "output_power": 348,
                "output_va": 349,
                "load_percent": 350,
            },
        )
        self.assertEqual(
            {
                key: protocol_4_sources[key]
                for key in ("output_current", "output_power", "output_va", "load_percent")
            },
            {
                "output_current": 252,
                "output_power": 254,
                "output_va": 255,
                "load_percent": 256,
            },
        )
        self.assertEqual(
            {
                key: protocol_5_sources[key]
                for key in ("output_current", "output_power", "output_va", "load_percent")
            },
            {
                "output_current": 347,
                "output_power": 348,
                "output_va": 349,
                "load_percent": 350,
            },
        )
        self.assertEqual(
            {
                key: protocol_6_sources[key]
                for key in ("output_current", "output_power", "output_va", "load_percent")
            },
            {
                "output_current": 252,
                "output_power": 254,
                "output_va": 255,
                "load_percent": 256,
            },
        )
        protocol_3_4_only = {
            "pv_power",
            "pv_charging_power",
            "pv_charging_current",
            "pv2_voltage",
            "pv2_current",
            "pv2_power",
        }
        self.assertTrue(protocol_3_4_only.issubset(protocol_3_sources))
        self.assertTrue(protocol_3_4_only.issubset(protocol_4_sources))
        self.assertTrue(protocol_3_4_only.isdisjoint(protocol_5_sources))
        self.assertTrue(protocol_3_4_only.isdisjoint(protocol_6_sources))

        self.assertEqual(anj.spec_sets, protocol_4.spec_sets)
        self.assertEqual(sandisolar.spec_sets, protocol_4.spec_sets)
        self.assertEqual(hhs.spec_sets, protocol_3.spec_sets)
        for schema in (protocol_3, protocol_4, protocol_5, protocol_6, anj, hhs, sandisolar):
            keys = [spec.key for specs in schema.spec_sets.values() for spec in specs]
            self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(
            {spec.key: spec.register for spec in generic.spec_set("live")}["grid_power"],
            204,
        )

    def test_schema_overlay_can_remove_inherited_spec_and_description(self) -> None:
        raw = {
            "extends": "builtin:modbus_smg/base.json",
            "schema_key": "external_smg_without_grid_power",
            "title": "External SMG Without Grid Power",
            "spec_set_removals": {"live": ["grid_power"]},
            "measurement_description_removals": ["grid_power"],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "without_grid_power.json"
            schema_path.write_text(json.dumps(raw), encoding="utf-8")
            set_external_register_schema_roots((Path(temp_dir),))
            schema = load_register_schema("without_grid_power.json")

        self.assertNotIn("grid_power", {spec.key for spec in schema.spec_set("live")})
        with self.assertRaises(KeyError):
            schema.measurement_description("grid_power")

    def test_pi30_driver_uses_loaded_register_schema(self) -> None:
        schema = load_register_schema("pi30_ascii/models/smartess_0925_compat.json")
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

    def test_external_schema_relative_extends_can_fall_back_to_builtin_parent(self) -> None:
        raw = {
            "extends": "../base.json",
            "schema_key": "external_smg_6200_relative",
            "title": "External SMG 6200 Relative Register Schema",
            "driver_key": "modbus_smg",
            "protocol_family": "modbus_smg",
            "measurement_descriptions": [
                {
                    "key": "operational_state",
                    "name": "External Relative System Status",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "modbus_smg" / "models" / "smg_6200.json"
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(json.dumps(raw), encoding="utf-8")
            set_external_register_schema_roots((Path(temp_dir),))

            schema = load_register_schema("modbus_smg/models/smg_6200.json")

        self.assertEqual(schema.title, "External SMG 6200 Relative Register Schema")
        self.assertEqual(schema.key, "external_smg_6200_relative")
        self.assertEqual(schema.source_scope, "external")
        self.assertEqual(schema.source_path, str(schema_path.resolve()))
        self.assertEqual(
            schema.measurement_description("operational_state").name,
            "External Relative System Status",
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
