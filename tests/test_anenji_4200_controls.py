"""Issue #88 model overlay: synthetic regression tests, not hardware validation."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from custom_components.eybond_local.drivers.capability_codec import encode_capability_words
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin
from custom_components.eybond_local.schema import capability_write_exposure_allowed


PROFILE = "modbus_smg/models/anenji_4200_protocol_1.json"


class Anenji4200ControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.profile = load_driver_profile(PROFILE)

    def inverter(self):
        return DetectedInverter(
            driver_key="modbus_smg", protocol_family="modbus_smg",
            model_name="Synthetic Anenji 4200", serial_number="SYNTHETIC88",
            probe_target=ProbeTarget(1, 255, 1), variant_key="anenji_4200_protocol_1",
            profile_name=PROFILE, register_schema_name=PROFILE,
            capabilities=self.profile.capabilities,
        )

    def test_model_only_controls_do_not_expand_other_4200_or_family_maps(self):
        registers = {
            "battery_type": 322,
            "equalization_activate_now": 425,
            "constant_voltage_to_float_time": 330,
            "automatic_mains_output_enabled": 338,
            "lithium_auto_activation": 339,
            "low_dc_protection_soc_grid_mode": 341,
            "solar_battery_utility_return_soc_threshold": 342,
            "low_dc_cutoff_soc": 343,
            "max_discharge_current_protection": 351,
        }
        self.assertEqual(len(self.profile.capabilities), 39)
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
        for key in ("lithium_manual_activation", "modbus_address"):
            with self.assertRaises(KeyError):
                self.profile.get_capability(key)
        classic = load_driver_profile("modbus_smg/protocols/classic_smg_rs232_v1.json")
        for key in ("battery_type", "equalization_activate_now"):
            with self.assertRaises(KeyError):
                classic.get_capability(key)

    def test_followup_controls_are_opt_in_not_promoted_to_auto_or_read_only(self):
        for key in ("battery_type", "equalization_activate_now"):
            cap = self.profile.get_capability(key)
            self.assertEqual(cap.write_function, 6)
            self.assertFalse(cap.enabled_default)
            self.assertFalse(cap.tested)
            self.assertTrue(cap.advanced)
            self.assertTrue(cap.requires_confirm)
            for mode in ("read_only", "auto", "full"):
                self.assertEqual(capability_write_exposure_allowed(
                    cap, control_mode=mode, detection_confidence="high",
                    variant_key="anenji_4200_protocol_1", profile_source_scope="builtin",
                    schema_source_scope="builtin", profile_name=PROFILE,
                ), mode == "full")
        self.assertTrue(self.profile.get_capability("battery_type").unsafe_while_running)
        self.assertIn("may reset", self.profile.get_capability("battery_type").change_summary)

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

    def test_lcd_discharge_range_does_not_invent_voltage_or_dynamic_limits(self):
        discharge = self.profile.get_capability("max_discharge_current_protection")
        self.assertEqual((discharge.minimum, discharge.maximum), (0, 500))
        self.assertEqual(encode_capability_words(discharge, 0), [0])
        self.assertEqual(encode_capability_words(discharge, 500), [500])
        for invalid in (-1, 501):
            with self.assertRaises(ValueError):
                encode_capability_words(discharge, invalid)
        self.assertEqual(load_driver_profile(
            "modbus_smg/models/smg_variant_4200.json",
        ).get_capability("max_discharge_current_protection").maximum, 1000)
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

    async def test_six_battery_types_use_fc06_and_match_normal_readback(self):
        inverter = self.inverter()
        registers = dict.fromkeys(range(800), 0)
        registers.update({171: 0x3501, 184: 1, 322: 6})
        transport = FixtureTransport(registers=registers, command_responses=None,
                                     probe_target=inverter.probe_target)
        wire = []
        send = transport.async_send_forward

        async def tracked(payload, **kwargs):
            wire.append(payload)
            return await send(payload, **kwargs)

        transport.async_send_forward = tracked
        driver = SmgModbusDriver()
        cap = self.profile.get_capability("battery_type")
        choices = {0: "AGM", 1: "Flooded", 2: "User", 4: "Li2", 6: "Li4", 8: "LiB"}
        self.assertEqual(cap.enum_value_map, choices)
        for raw, label in choices.items():
            wire.clear()
            await driver.async_write_capability(transport, inverter, cap.key, label)
            writes = [p for p in wire if p[1] in (6, 16)]
            self.assertEqual(len(writes), 1)
            self.assertEqual(writes[0][:6], bytes.fromhex("01060142") + raw.to_bytes(2, "big"))
            self.assertTrue(any(p[1:6] == bytes.fromhex("0301420001") for p in wire))
            result = await driver.async_read_values(transport, inverter)
            self.assertEqual(result.values["battery_type"], label)
        wire.clear()
        for invalid in (3, 5, 7, "Li1", "Li3"):
            with self.assertRaises(ValueError):
                await driver.async_write_capability(transport, inverter, cap.key, invalid)
        self.assertEqual(wire, [])

    async def test_eq_action_is_single_fc06_without_persistent_readback_or_extra_poll(self):
        inverter = self.inverter()
        registers = dict.fromkeys(range(800), 0)
        registers.update({171: 0x3501, 184: 1, 322: 1, 313: 1})
        transport = FixtureTransport(registers=registers, command_responses=None,
                                     probe_target=inverter.probe_target)
        wire = []
        send = transport.async_send_forward

        async def tracked(payload, **kwargs):
            wire.append(payload)
            return await send(payload, **kwargs)

        transport.async_send_forward = tracked
        driver = SmgModbusDriver()
        await driver.async_write_capability(transport, inverter, "equalization_activate_now", None)
        self.assertEqual(len(wire), 1)
        self.assertEqual(wire[0][:6], bytes.fromhex("010601a90001"))
        wire.clear()
        await driver.async_read_values(transport, inverter)
        for payload in wire:
            self.assertNotIn(payload[1], (6, 16))
            if payload[1] == 3:
                start, count = int.from_bytes(payload[2:4], "big"), int.from_bytes(payload[4:6], "big")
                self.assertFalse(start <= 425 < start + count)

    async def test_eq_prewrite_guard_uses_fresh_type_and_enable_state(self):
        inverter = self.inverter()
        hub = HubManagementMixin()
        hub._inverter = inverter
        hub._driver = SmgModbusDriver()
        hub._driver.async_write_capability = AsyncMock(return_value=1)
        hub._async_ensure_connected = AsyncMock()
        hub._runtime_read_state, hub._write_blockers = {}, {}
        hub._link_manager = SimpleNamespace(transport=object())
        for battery in ("AGM", "Li2", "Li4", "LiB", None, "Flooded", "User"):
            for enabled in (False, True, None):
                with self.subTest(battery=battery, enabled=enabled):
                    snapshot = RuntimeSnapshot(connected=True, inverter=inverter, values={
                        "battery_type": battery, "battery_equalization_enabled": enabled,
                    })
                    hub.async_refresh = AsyncMock(return_value=snapshot)
                    hub._driver.async_write_capability.reset_mock()
                    allowed = battery in ("Flooded", "User") and enabled is True
                    if allowed:
                        self.assertEqual(await hub.async_write_capability("equalization_activate_now", None), 1)
                        hub._driver.async_write_capability.assert_awaited_once()
                    else:
                        with self.assertRaisesRegex(ValueError, "capability_not_editable"):
                            await hub.async_write_capability("equalization_activate_now", None)
                        hub._driver.async_write_capability.assert_not_awaited()
                    self.assertEqual(hub._write_blockers, {})

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
