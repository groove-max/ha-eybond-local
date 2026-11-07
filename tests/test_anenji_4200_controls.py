"""Issue #88 model overlay: synthetic regression tests, not hardware validation."""

import unittest

from custom_components.eybond_local.drivers.capability_codec import encode_capability_words
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget


PROFILE = "modbus_smg/models/anenji_4200_protocol_1.json"


class Anenji4200ControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.profile = load_driver_profile(PROFILE)

    def test_model_only_controls_do_not_expand_other_4200_or_family_maps(self):
        registers = {
            "constant_voltage_to_float_time": 330,
            "automatic_mains_output_enabled": 338,
            "lithium_auto_activation": 339,
            "low_dc_protection_soc_grid_mode": 341,
            "solar_battery_utility_return_soc_threshold": 342,
            "low_dc_cutoff_soc": 343,
            "max_discharge_current_protection": 351,
        }
        self.assertEqual(len(self.profile.capabilities), 37)
        for key, register in registers.items():
            cap = self.profile.get_capability(key)
            self.assertEqual(cap.register, register)
            self.assertFalse(cap.tested)
            self.assertFalse(cap.enabled_default)
            self.assertTrue(cap.requires_confirm)
        self.assertEqual(self.profile.presets, ())
        for name in (
            "modbus_smg/protocols/classic_smg_rs232_v1.json",
            "modbus_smg/models/smg_variant_4200.json",
        ):
            other = load_driver_profile(name)
            self.assertEqual(other.get_capability("output_rating_voltage").value_kind, "scaled_u16")
            self.assertNotIn(4, other.get_capability("output_source_priority").enum_value_map)
        self.assertEqual(
            len(load_driver_profile("modbus_smg/protocols/classic_smg_rs232_v1.json").presets), 2,
        )
        for key in ("battery_type", "equalization_activate_now", "lithium_manual_activation"):
            with self.assertRaises(KeyError):
                self.profile.get_capability(key)

    def test_discrete_controls_override_templates_without_duplicate_keys(self):
        for key, register, values in (
            ("output_rating_voltage", 320, {"220 V": 2200, "230 V": 2300, "240 V": 2400}),
            ("output_rating_frequency", 321, {"50 Hz": 5000, "60 Hz": 6000}),
        ):
            with self.subTest(key=key):
                cap = self.profile.get_capability(key)
                self.assertEqual(cap.value_kind, "enum")
                self.assertIsNone(cap.divisor)
                self.assertEqual(cap.value_key, key + "_setting")
                self.assertEqual(sum(c.register == register for c in self.profile.capabilities), 1)
                for label, raw in values.items():
                    self.assertEqual(encode_capability_words(cap, label), [raw])
                for invalid in ("221 V", "55 Hz", 2210, 5500):
                    with self.assertRaises(ValueError):
                        encode_capability_words(cap, invalid)
                for mode in ("Power On", "Fault", "Line", "Off-Grid", None):
                    self.assertFalse(cap.runtime_state({
                        "operating_mode": mode, "configuration_safe_mode": True,
                    }).editable)
                self.assertTrue(cap.runtime_state({"operating_mode": "Standby"}).editable)

    def test_ranges_and_conditions_do_not_invent_voltage_or_discharge_limits(self):
        discharge = self.profile.get_capability("max_discharge_current_protection")
        self.assertIsNone(discharge.maximum)
        self.assertEqual(encode_capability_words(discharge, 0), [0])
        self.assertEqual(encode_capability_words(discharge, 501), [501])
        for key in ("battery_bulk_voltage", "battery_float_voltage", "battery_equalization_voltage"):
            self.assertIsNone(self.profile.get_capability(key).maximum)
        for suffix in ("mode", "voltage", "time", "timeout", "interval"):
            cap = self.profile.get_capability("battery_equalization_" + suffix)
            for battery in ("AGM", "Li4", "LiB", None):
                self.assertFalse(cap.runtime_state({"battery_type": battery}).visible)
            self.assertTrue(cap.runtime_state({
                "battery_type": "Flooded", "battery_connected": True,
            }).editable)
        time = self.profile.get_capability("constant_voltage_to_float_time")
        self.assertEqual(time.native_step, 5)
        self.assertEqual(encode_capability_words(time, 0), [0])
        self.assertTrue(time.runtime_state({"battery_type": "User"}).editable)
        self.assertFalse(time.runtime_state({"battery_type": "Li4"}).editable)
        with self.assertRaises(ValueError):
            encode_capability_words(self.profile.get_capability("charge_source_priority"), 0)

    async def test_poll_and_write_use_the_same_discrete_readback_without_extra_poll_block(self):
        target = ProbeTarget(1, 255, 1)
        registers = dict.fromkeys(range(800), 0)
        registers.update({
            171: 0x3501, 184: 1, 301: 4, 320: 2300, 321: 5000, 322: 6,
            330: 0, 331: 2, 338: 1, 339: 0, 341: 50, 342: 95, 343: 20, 351: 0,
        })

        class TrackingTransport(FixtureTransport):
            def __init__(self):
                super().__init__(registers=registers, command_responses=None, probe_target=target)
                self.reads = []
                self.writes = []

            def _handle_read_holding(self, payload):
                self.reads.append((int.from_bytes(payload[2:4], "big"), int.from_bytes(payload[4:6], "big")))
                return super()._handle_read_holding(payload)

            def _handle_write_multiple(self, payload):
                self.writes.append(payload)
                return super()._handle_write_multiple(payload)

        transport = TrackingTransport()
        inverter = DetectedInverter(
            driver_key="modbus_smg", protocol_family="modbus_smg",
            model_name="Synthetic Anenji 4200", serial_number="SYNTHETIC88",
            probe_target=target, variant_key="anenji_4200_protocol_1",
            profile_name=PROFILE, register_schema_name=PROFILE,
            capabilities=self.profile.capabilities,
        )
        driver = SmgModbusDriver()
        result = await driver.async_read_values(transport, inverter)
        self.assertEqual(result.values["output_source_priority"], "SUF")
        self.assertEqual(result.values["output_rating_voltage"], 230)
        self.assertEqual(result.values["output_rating_voltage_setting"], "230 V")
        self.assertEqual(result.values["output_rating_frequency"], 50)
        self.assertEqual(result.values["output_rating_frequency_setting"], "50 Hz")
        self.assertEqual(result.values["automatic_mains_output_enabled"], "Auto")
        self.assertEqual(result.values["lithium_auto_activation"], "Disabled")
        self.assertEqual(result.values["max_discharge_current_protection"], 0)
        self.assertEqual(result.values["low_dc_cutoff_soc"], 20)
        self.assertEqual(transport.reads.count((351, 1)), 1)
        self.assertIn((300, 44), transport.reads)
        self.assertNotIn((300, 54), transport.reads)
        self.assertEqual(transport.writes, [])
        await driver.async_write_capability(transport, inverter, "output_rating_voltage", "240 V")
        self.assertEqual(len(transport.writes), 1)
        result = await driver.async_read_values(transport, inverter)
        self.assertEqual(result.values["output_rating_voltage"], 240)
        self.assertEqual(result.values["output_rating_voltage_setting"], "240 V")
