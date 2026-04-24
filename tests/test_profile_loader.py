from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata import profile_loader


class ProfileLoaderTests(unittest.TestCase):
    def tearDown(self) -> None:
        profile_loader.set_external_profile_roots(())
        profile_loader.load_driver_profile.cache_clear()

    def test_loads_modbus_smg_base_profile_metadata(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("modbus_smg/base.json")

        self.assertEqual(profile.key, "modbus_smg_base")
        self.assertEqual(profile.title, "SMG / Modbus Base Profile")
        self.assertEqual(profile.driver_key, "modbus_smg")
        self.assertEqual(profile.protocol_family, "modbus_smg")
        self.assertEqual(profile.source_name, "modbus_smg/base.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/modbus_smg/base.json"))
        self.assertEqual(len(profile.groups), 4)
        self.assertEqual(len(profile.capabilities), 30)
        self.assertEqual(len(profile.presets), 2)
        self.assertEqual(sum(capability.tested for capability in profile.capabilities), 0)
        self.assertEqual(profile.get_capability("charge_source_priority").register, 331)
        self.assertEqual(
            profile.get_capability("power_saving_mode").resolved_support_tier,
            "standard",
        )
        self.assertEqual(profile.get_capability("power_saving_mode").support_notes, "")
        with self.assertRaises(KeyError):
            profile.get_capability("low_dc_cutoff_soc")

    def test_loads_smg_profile_metadata(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")

        self.assertEqual(profile.key, "smg_modbus")
        self.assertEqual(profile.title, "SMG / Modbus")
        self.assertEqual(profile.driver_key, "modbus_smg")
        self.assertEqual(profile.protocol_family, "modbus_smg")
        self.assertEqual(profile.source_name, "smg_modbus.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/smg_modbus.json"))
        self.assertGreaterEqual(len(profile.groups), 4)
        self.assertEqual(len(profile.capabilities), 33)
        self.assertEqual(len(profile.presets), 2)
        self.assertEqual(
            profile.get_capability("charge_source_priority").enum_value_map[3],
            "PV Only",
        )
        self.assertTrue(profile.get_capability("buzzer_mode").enabled_default)
        self.assertEqual(profile.get_capability("low_dc_protection_soc_grid_mode").register, 341)
        self.assertEqual(profile.get_capability("solar_battery_utility_return_soc_threshold").register, 342)
        self.assertEqual(profile.get_capability("low_dc_cutoff_soc").register, 343)
        self.assertEqual(
            profile.get_capability("power_saving_mode").resolved_support_tier,
            "blocked",
        )
        self.assertIn(
            "exception_code:7",
            profile.get_capability("power_saving_mode").support_notes,
        )

    def test_loads_anenji_4200_protocol_1_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("modbus_smg/models/anenji_4200_protocol_1.json")

        self.assertEqual(profile.key, "modbus_smg_anenji_4200_protocol_1")
        self.assertEqual(profile.title, "Anenji 4200 Protocol 1")
        self.assertEqual(profile.driver_key, "modbus_smg")
        self.assertEqual(profile.protocol_family, "modbus_smg")
        self.assertEqual(profile.source_name, "modbus_smg/models/anenji_4200_protocol_1.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(
            profile.source_path.endswith(
                "profiles/modbus_smg/models/anenji_4200_protocol_1.json"
            )
        )
        self.assertEqual(len(profile.groups), 4)
        self.assertEqual(len(profile.capabilities), 30)
        self.assertEqual(len(profile.presets), 2)
        self.assertEqual(sum(capability.tested for capability in profile.capabilities), 0)
        self.assertFalse(profile.get_capability("charge_source_priority").tested)
        self.assertEqual(profile.get_capability("charge_source_priority").register, 331)
        self.assertEqual(
            profile.get_capability("power_saving_mode").resolved_support_tier,
            "standard",
        )
        self.assertEqual(profile.get_capability("power_saving_mode").support_notes, "")
        with self.assertRaises(KeyError):
            profile.get_capability("low_dc_cutoff_soc")

    def test_loads_pi30_profile_metadata(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii.json")

        self.assertEqual(profile.key, "pi30_ascii")
        self.assertEqual(profile.title, "PI30 / ASCII")
        self.assertEqual(profile.driver_key, "pi30")
        self.assertEqual(profile.protocol_family, "pi30")
        self.assertEqual(profile.source_name, "pi30_ascii.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/pi30_ascii.json"))
        self.assertEqual(len(profile.groups), 3)
        self.assertEqual(len(profile.capabilities), 18)
        self.assertEqual(len(profile.presets), 0)
        self.assertEqual(
            profile.get_capability("output_source_priority").enum_value_map[2],
            "SBU first",
        )
        self.assertEqual(
            profile.get_capability("battery_float_voltage").native_step,
            0.1,
        )

    def test_smg_max_ac_charge_current_remains_editable_under_pv_only_policy(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")
        capability = profile.get_capability("max_ac_charge_current")
        runtime_state = capability.runtime_state(
            {
                "battery_connected": True,
                "charging_inactive": True,
                "utility_charging_allowed": False,
                "charge_source_priority": "PV Only",
            }
        )

        self.assertTrue(runtime_state.editable)
        self.assertNotIn(
            "Utility charging is currently disabled by Charge Source Priority.",
            runtime_state.reasons,
        )

    def test_smg_equalization_controls_are_not_soft_blocked_by_charge_state(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")

        equalization_mode = profile.get_capability("battery_equalization_mode")
        equalization_voltage = profile.get_capability("battery_equalization_voltage")

        runtime_values = {
            "battery_connected": True,
            "battery_equalization_enabled": False,
            "charging_active": True,
            "charging_inactive": False,
        }

        self.assertTrue(equalization_mode.runtime_state(runtime_values).editable)

        voltage_state = equalization_voltage.runtime_state(runtime_values)
        self.assertTrue(voltage_state.visible)
        self.assertTrue(voltage_state.editable)

    def test_smg_charge_current_gate_becomes_warning_while_charging(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")
        capability = profile.get_capability("max_ac_charge_current")
        runtime_state = capability.runtime_state(
            {
                "battery_connected": True,
                "charging_active": True,
                "charging_inactive": False,
            }
        )

        self.assertTrue(runtime_state.visible)
        self.assertTrue(runtime_state.editable)
        self.assertEqual(runtime_state.reasons, ())
        self.assertIn(
            "Stop active battery charging before changing this setting.",
            runtime_state.warnings,
        )

    def test_smg_safe_configuration_gate_becomes_warning_for_output_mode(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")
        runtime_state = profile.get_capability("output_mode").runtime_state(
            {
                "configuration_safe_mode": False,
            }
        )

        self.assertTrue(runtime_state.visible)
        self.assertTrue(runtime_state.editable)
        self.assertEqual(runtime_state.reasons, ())
        self.assertIn(
            "This setting can only be changed while the inverter is in Power On, Standby, or Fault mode.",
            runtime_state.warnings,
        )

    def test_smg_remote_control_gate_becomes_warning_for_remote_actions(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")
        runtime_state = profile.get_capability("remote_turn_on").runtime_state(
            {
                "remote_control_enabled": False,
            }
        )

        self.assertTrue(runtime_state.visible)
        self.assertTrue(runtime_state.editable)
        self.assertEqual(runtime_state.reasons, ())
        self.assertIn(
            "Remote control is disabled by the current Turn On Mode setting.",
            runtime_state.warnings,
        )

    def test_anenji_reset_user_parameters_gate_becomes_warning_in_off_grid_mode(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json")
        runtime_state = profile.get_capability("reset_user_parameters").runtime_state(
            {
                "operating_mode": "Off-Grid",
            }
        )

        self.assertTrue(runtime_state.visible)
        self.assertTrue(runtime_state.editable)
        self.assertEqual(runtime_state.reasons, ())
        self.assertEqual(
            runtime_state.warnings,
            ("This action is only valid outside Off-Grid mode.",),
        )

    def test_smg_off_grid_preset_condition_becomes_warning_not_block(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")
        preset = next(
            preset
            for preset in profile.presets
            if preset.key == "off_grid_self_consumption"
        )
        runtime_state = preset.runtime_state(
            profile,
            {
                "operating_mode": "Line",
                "battery_connected": True,
                "charging_inactive": True,
            },
        )

        self.assertTrue(runtime_state.visible)
        self.assertTrue(runtime_state.applicable)
        self.assertEqual(runtime_state.reasons, ())
        self.assertIn(
            "This recommendation is intended for Off-Grid operation.",
            runtime_state.warnings,
        )

    def test_verified_default_smg_equalization_controls_are_enabled_by_default(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")

        self.assertTrue(profile.get_capability("battery_equalization_mode").enabled_default)
        self.assertTrue(profile.get_capability("battery_equalization_voltage").enabled_default)
        self.assertTrue(profile.get_capability("battery_equalization_time").enabled_default)
        self.assertTrue(profile.get_capability("battery_equalization_timeout").enabled_default)
        self.assertTrue(profile.get_capability("battery_equalization_interval").enabled_default)

    def test_verified_default_smg_tested_controls_are_enabled_by_default(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")

        self.assertTrue(profile.get_capability("turn_on_mode").enabled_default)
        self.assertTrue(profile.get_capability("battery_bulk_voltage").enabled_default)
        self.assertTrue(profile.get_capability("battery_overvoltage_protection_voltage").enabled_default)
        self.assertTrue(profile.get_capability("battery_float_voltage").enabled_default)
        self.assertTrue(profile.get_capability("battery_redischarge_voltage").enabled_default)
        self.assertTrue(profile.get_capability("battery_under_voltage").enabled_default)
        self.assertTrue(profile.get_capability("battery_under_voltage_off_grid").enabled_default)

    def test_verified_default_smg_does_not_expose_anenji_clock_write_capabilities(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smg_modbus.json")

        with self.assertRaises(KeyError):
            profile.get_capability("inverter_date_write")
        with self.assertRaises(KeyError):
            profile.get_capability("inverter_time_write")

    def test_loads_pi30_default_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii/models/default.json")

        self.assertEqual(profile.key, "pi30_ascii_default")
        self.assertEqual(profile.title, "PI30 / ASCII Default Profile")
        self.assertEqual(profile.driver_key, "pi30")
        self.assertEqual(profile.protocol_family, "pi30")
        self.assertEqual(profile.source_name, "pi30_ascii/models/default.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/pi30_ascii/models/default.json"))
        self.assertEqual(len(profile.groups), 3)
        self.assertEqual(len(profile.capabilities), 18)

    def test_loads_pi30_smartess_0925_compat_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii/models/smartess_0925_compat.json")

        self.assertEqual(profile.key, "pi30_ascii_smartess_0925_compat")
        self.assertEqual(profile.title, "SmartESS 0925 Compatibility Profile")
        self.assertEqual(profile.driver_key, "pi30")
        self.assertEqual(profile.protocol_family, "pi30")
        self.assertEqual(profile.source_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(
            profile.source_path.endswith("profiles/pi30_ascii/models/smartess_0925_compat.json")
        )
        self.assertEqual(len(profile.groups), 3)
        self.assertEqual(len(profile.capabilities), 18)

    def test_loads_pi30_vmii_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii/models/vmii_nxpw5kw.json")

        self.assertEqual(profile.key, "pi30_ascii_vmii_nxpw5kw")
        self.assertEqual(profile.title, "PI30 / ASCII VMII-NXPW5KW Profile")
        self.assertEqual(profile.driver_key, "pi30")
        self.assertEqual(profile.protocol_family, "pi30")
        self.assertEqual(profile.source_name, "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/pi30_ascii/models/vmii_nxpw5kw.json"))
        self.assertEqual(len(profile.groups), 3)
        self.assertEqual(len(profile.capabilities), 18)

    def test_loads_pi30_pi41_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii/models/pi41.json")

        self.assertEqual(profile.key, "pi30_ascii_pi41")
        self.assertEqual(profile.title, "PI41 / ASCII Profile")
        self.assertEqual(profile.driver_key, "pi30")
        self.assertEqual(profile.protocol_family, "pi30")
        self.assertEqual(profile.source_name, "pi30_ascii/models/pi41.json")
        self.assertTrue(profile.source_path.endswith("profiles/pi30_ascii/models/pi41.json"))
        self.assertEqual(len(profile.capabilities), 18)

    def test_loads_pi30_max_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii/models/pi30_max.json")

        self.assertEqual(profile.key, "pi30_ascii_pi30_max")
        self.assertEqual(profile.title, "PI30 MAX / ASCII Profile")
        self.assertEqual(profile.source_name, "pi30_ascii/models/pi30_max.json")
        self.assertTrue(profile.source_path.endswith("profiles/pi30_ascii/models/pi30_max.json"))
        self.assertEqual(len(profile.capabilities), 18)

    def test_loads_pi30_pip_gk_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("pi30_ascii/models/pi30_pip_gk.json")

        self.assertEqual(profile.key, "pi30_ascii_pi30_pip_gk")
        self.assertEqual(profile.title, "PI30 PIP-GK / ASCII Profile")
        self.assertEqual(profile.source_name, "pi30_ascii/models/pi30_pip_gk.json")
        self.assertTrue(profile.source_path.endswith("profiles/pi30_ascii/models/pi30_pip_gk.json"))
        self.assertEqual(len(profile.capabilities), 18)

    def test_loads_smartess_0925_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smartess_local/models/0925.json")

        self.assertEqual(profile.key, "smartess_0925")
        self.assertEqual(profile.title, "SmartESS 0925 Profile")
        self.assertEqual(profile.driver_key, "smartess_local")
        self.assertEqual(profile.protocol_family, "smartess_local")
        self.assertEqual(profile.source_name, "smartess_local/models/0925.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/smartess_local/models/0925.json"))
        self.assertEqual(len(profile.groups), 5)
        self.assertEqual(len(profile.capabilities), 30)
        self.assertEqual(len(profile.presets), 0)
        self.assertEqual(profile.get_capability("output_source_priority").enum_value_map[4], "SUF")
        self.assertEqual(profile.get_capability("input_voltage_range").enum_value_map[2], "Generator")
        self.assertEqual(profile.get_capability("battery_type").enum_value_map[6], "Li4")
        self.assertEqual(profile.get_capability("max_total_charge_current").register, 4541)
        self.assertEqual(profile.get_capability("power_saving_enabled").register, 5003)
        self.assertEqual(profile.get_capability("battery_equalization_mode").register, 5011)
        self.assertEqual(profile.get_capability("force_battery_equalization").register, 5012)
        self.assertEqual(profile.get_capability("restore_defaults").register, 5016)

    def test_loads_smartess_0921_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smartess_local/models/0921.json")

        self.assertEqual(profile.key, "smartess_0921")
        self.assertEqual(profile.title, "SmartESS 0921 Profile")
        self.assertEqual(profile.driver_key, "smartess_local")
        self.assertEqual(profile.protocol_family, "smartess_local")
        self.assertEqual(profile.source_name, "smartess_local/models/0921.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/smartess_local/models/0921.json"))
        self.assertEqual(len(profile.groups), 0)
        self.assertEqual(len(profile.capabilities), 0)
        self.assertEqual(len(profile.presets), 0)

    def test_loads_smartess_0912_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("smartess_local/models/0912.json")

        self.assertEqual(profile.key, "smartess_0912")
        self.assertEqual(profile.title, "SmartESS 0912 Profile")
        self.assertEqual(profile.driver_key, "smartess_local")
        self.assertEqual(profile.protocol_family, "smartess_local")
        self.assertEqual(profile.source_name, "smartess_local/models/0912.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(profile.source_path.endswith("profiles/smartess_local/models/0912.json"))
        self.assertEqual(len(profile.groups), 0)
        self.assertEqual(len(profile.capabilities), 0)
        self.assertEqual(len(profile.presets), 0)

    def test_loads_anenji_profile_overlay(self) -> None:
        profile_loader.load_driver_profile.cache_clear()

        profile = profile_loader.load_driver_profile("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json")

        self.assertEqual(profile.key, "modbus_smg_anenji_anj_11kw_48v_wifi_p")
        self.assertEqual(profile.title, "Anenji ANJ-11KW-48V-WIFI-P")
        self.assertEqual(profile.driver_key, "modbus_smg")
        self.assertEqual(profile.protocol_family, "modbus_smg")
        self.assertEqual(profile.source_name, "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json")
        self.assertEqual(profile.source_scope, "builtin")
        self.assertTrue(
            profile.source_path.endswith(
                "profiles/modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"
            )
        )
        self.assertEqual(len(profile.groups), 4)
        self.assertEqual(len(profile.capabilities), 47)
        self.assertEqual(len(profile.presets), 0)

        self.assertEqual(profile.get_capability("output_mode").register, 600)
        self.assertEqual(profile.get_capability("output_mode").enum_value_map[6], "Split-Phase-P2")
        self.assertEqual(profile.get_capability("force_eq_charge").register, 656)
        self.assertEqual(profile.get_capability("input_mode").register, 677)
        self.assertEqual(profile.get_capability("input_mode").enum_value_map[2], "GNT")
        self.assertEqual(profile.get_capability("warning_mask_i").word_count, 2)
        self.assertEqual(profile.get_capability("warning_mask_i").combine, "u32_high_first")
        self.assertEqual(profile.get_capability("output_source_priority").register, 601)
        self.assertEqual(
            profile.get_capability("output_source_priority").enum_value_map[3],
            "PV-Utility-Battery (Grid-Tied PV)",
        )
        self.assertEqual(profile.get_capability("charge_source_priority").register, 632)
        self.assertEqual(
            profile.get_capability("charge_source_priority").enum_value_map[4],
            "PV Priority With Load Reserve",
        )
        self.assertEqual(profile.get_capability("battery_type").register, 630)
        self.assertEqual(profile.get_capability("battery_type").enum_value_map[8], "LiB")
        self.assertEqual(profile.get_capability("turn_on_mode").register, 693)
        self.assertEqual(profile.get_capability("remote_turn_on").register, 694)
        self.assertEqual(profile.get_capability("exit_fault_mode").register, 695)
        self.assertEqual(profile.get_capability("inverter_date_write").register, 696)
        self.assertEqual(profile.get_capability("inverter_date_write").word_count, 3)
        self.assertEqual(profile.get_capability("inverter_time_write").register, 699)
        self.assertEqual(profile.get_capability("inverter_time_write").word_count, 3)
        self.assertTrue(all(capability.tested for capability in profile.capabilities))
        self.assertEqual(profile.get_capability("clear_generation_data").register, 705)
        self.assertEqual(profile.get_capability("reset_user_parameters").register, 706)
        self.assertEqual(profile.get_capability("ground_relay_enabled").register, 707)
        self.assertEqual(profile.get_capability("lithium_battery_activation_time").maximum, 300)
        self.assertEqual(profile.get_capability("battery_equalization_mode").register, 651)
        with self.assertRaises(KeyError):
            profile.get_capability("remote_switch")

    def test_rejects_duplicate_capability_keys(self) -> None:
        raw = {
            "profile_key": "bad_profile",
            "title": "Bad Profile",
            "groups": [{"key": "system", "title": "System"}],
            "capabilities": [
                {
                    "key": "duplicate_capability",
                    "register": 100,
                    "value_kind": "bool",
                    "note": "first",
                    "group": "system",
                },
                {
                    "key": "duplicate_capability",
                    "register": 101,
                    "value_kind": "bool",
                    "note": "second",
                    "group": "system",
                },
            ],
            "presets": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "bad_profile.json"
            profile_path.write_text(json.dumps(raw), encoding="utf-8")
            with mock.patch.object(profile_loader, "PROFILES_DIR", Path(temp_dir)):
                profile_loader.load_driver_profile.cache_clear()
                with self.assertRaisesRegex(
                    ValueError,
                    r"profile:bad_profile:duplicate_capability:duplicate_capability",
                ):
                    profile_loader.load_driver_profile("bad_profile.json")

    def test_rejects_unknown_support_tier(self) -> None:
        raw = {
            "profile_key": "bad_support_profile",
            "title": "Bad Support Profile",
            "groups": [{"key": "system", "title": "System"}],
            "capabilities": [
                {
                    "key": "invalid_support",
                    "register": 100,
                    "value_kind": "bool",
                    "note": "invalid support tier",
                    "group": "system",
                    "support_tier": "mystery",
                }
            ],
            "presets": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "bad_support_profile.json"
            profile_path.write_text(json.dumps(raw), encoding="utf-8")
            with mock.patch.object(profile_loader, "PROFILES_DIR", Path(temp_dir)):
                profile_loader.load_driver_profile.cache_clear()
                with self.assertRaisesRegex(
                    ValueError,
                    r"profile:bad_support_profile:unsupported_support_tier:invalid_support:mystery",
                ):
                    profile_loader.load_driver_profile("bad_support_profile.json")

    def test_prefers_external_profile_root_when_configured(self) -> None:
        raw = {
            "profile_key": "smg_modbus",
            "title": "External SMG / Modbus",
            "driver_key": "modbus_smg",
            "protocol_family": "modbus_smg",
            "groups": [],
            "capabilities": [],
            "presets": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "smg_modbus.json"
            profile_path.write_text(json.dumps(raw), encoding="utf-8")
            profile_loader.set_external_profile_roots((Path(temp_dir),))

            profile = profile_loader.load_driver_profile("smg_modbus.json")

        self.assertEqual(profile.title, "External SMG / Modbus")
        self.assertEqual(profile.source_scope, "external")
        self.assertEqual(profile.source_path, str(profile_path.resolve()))

    def test_external_profile_relative_extends_falls_back_to_builtin_parent(self) -> None:
        builtin_parent = {
            "profile_key": "pi30_ascii",
            "title": "Builtin PI30 / ASCII",
            "driver_key": "pi30",
            "protocol_family": "pi30",
            "groups": [{"key": "system", "title": "System"}],
            "capabilities": [
                {
                    "key": "output_source_priority",
                    "register": 1,
                    "value_kind": "enum",
                    "note": "builtin",
                    "group": "system",
                    "enum_map": {"1": "Utility first"},
                }
            ],
            "presets": [],
        }
        external_child = {
            "extends": "../../pi30_ascii.json",
            "profile_key": "pi30_ascii_smartess_0925_compat",
            "title": "External SmartESS 0925 Compat",
        }

        with tempfile.TemporaryDirectory() as builtin_dir, tempfile.TemporaryDirectory() as external_dir:
            builtin_root = Path(builtin_dir)
            external_root = Path(external_dir)
            (builtin_root / "pi30_ascii.json").write_text(json.dumps(builtin_parent), encoding="utf-8")
            child_path = external_root / "pi30_ascii" / "models" / "smartess_0925_compat.json"
            child_path.parent.mkdir(parents=True, exist_ok=True)
            child_path.write_text(json.dumps(external_child), encoding="utf-8")

            with mock.patch.object(profile_loader, "PROFILES_DIR", builtin_root):
                profile_loader.set_external_profile_roots((external_root,))
                profile_loader.load_driver_profile.cache_clear()

                profile = profile_loader.load_driver_profile(
                    "pi30_ascii/models/smartess_0925_compat.json"
                )

        self.assertEqual(profile.title, "External SmartESS 0925 Compat")
        self.assertEqual(profile.source_scope, "external")
        self.assertEqual(profile.source_path, str(child_path.resolve()))
        self.assertEqual(profile.driver_key, "pi30")
        self.assertEqual(profile.get_capability("output_source_priority").register, 1)

    def test_capability_templates_materialize_variant_capabilities(self) -> None:
        raw = {
            "profile_key": "templated_profile",
            "title": "Templated Profile",
            "driver_key": "modbus_smg",
            "protocol_family": "modbus_smg",
            "groups": [{"key": "system", "title": "System"}],
            "capability_templates": {
                "turn_on_mode": {
                    "value_kind": "enum",
                    "title": "Turn On Mode",
                    "group": "system",
                    "order": 100,
                    "requires_confirm": True,
                    "choices": [
                        {"value": 0, "label": "Local and Remote"},
                        {"value": 1, "label": "Local Only"},
                    ],
                }
            },
            "capabilities": [
                {
                    "key": "turn_on_mode",
                    "template": "turn_on_mode",
                    "register": 693,
                    "note": "Variant-specific register placement.",
                }
            ],
            "presets": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "templated_profile.json"
            profile_path.write_text(json.dumps(raw), encoding="utf-8")
            with mock.patch.object(profile_loader, "PROFILES_DIR", Path(temp_dir)):
                profile_loader.load_driver_profile.cache_clear()
                profile = profile_loader.load_driver_profile("templated_profile.json")

        capability = profile.get_capability("turn_on_mode")

        self.assertEqual(capability.register, 693)
        self.assertEqual(capability.display_name, "Turn On Mode")
        self.assertTrue(capability.requires_confirm)
        self.assertEqual(capability.enum_value_map[0], "Local and Remote")
        self.assertEqual(capability.note, "Variant-specific register placement.")

    def test_capability_defaults_apply_to_template_and_non_template_capabilities(self) -> None:
        raw = {
            "profile_key": "defaulted_profile",
            "title": "Defaulted Profile",
            "driver_key": "modbus_smg",
            "protocol_family": "modbus_smg",
            "groups": [{"key": "system", "title": "System"}],
            "capability_defaults": {"tested": True},
            "capability_templates": {
                "templated_switch": {
                    "value_kind": "bool",
                    "title": "Templated Switch",
                    "group": "system",
                    "choices": [
                        {"value": 0, "label": "Off"},
                        {"value": 1, "label": "On"},
                    ],
                }
            },
            "capabilities": [
                {
                    "key": "templated_switch",
                    "template": "templated_switch",
                    "register": 10,
                },
                {
                    "key": "direct_action",
                    "register": 11,
                    "value_kind": "action",
                    "action_value": 1,
                    "title": "Direct Action",
                    "group": "system",
                },
            ],
            "presets": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "defaulted_profile.json"
            profile_path.write_text(json.dumps(raw), encoding="utf-8")
            with mock.patch.object(profile_loader, "PROFILES_DIR", Path(temp_dir)):
                profile_loader.load_driver_profile.cache_clear()
                profile = profile_loader.load_driver_profile("defaulted_profile.json")

        self.assertTrue(profile.get_capability("templated_switch").tested)
        self.assertTrue(profile.get_capability("direct_action").tested)


if __name__ == "__main__":
    unittest.main()
