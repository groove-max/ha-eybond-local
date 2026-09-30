"""Protocol 0237 telemetry and opt-in controls; no customer identity or IO."""
from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.schema import capability_write_exposure_allowed
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import ModbusError


def hopewind_registers():
    # Preserve protocol-shaped measurements, not the customer's serial/firmware.
    return {r: 0 for r in range(40500, 40651)} | {
        40002: 2, 40003: 10000, 40004: 0, 40005: 0,
        40011: 1, 40012: 1650, 40013: 11000,
        40500: 4300, 40501: 5800, 40508: 16, 40509: 82,
        40524: 42, 40525: 36, 40532: 4130, 40533: 4145, 40534: 4158,
        40535: 13, 40536: 13, 40537: 13, 40538: 4999,
        40539: 73, 40540: 0xFFFE, 40541: 76, 40542: 9486,
        40543: 1000, 40544: 372, 40545: 8694, 40546: 1, 40547: 8,
        40548: 1930, 40550: 1849, 40551: 62, 40554: 1079,
        40586: 6510, 40587: 3230, 40588: 123, 40591: 456,
        40594: 72, 40645: 0, 40646: 15, 40647: 400,
    }


class ReadOnlyHopewindTransport(FixtureTransport):
    async def async_send_payload(self, payload, *, route):
        if payload[1] not in (3, 4):
            raise AssertionError("Read-only pack must never send a write")
        return await super().async_send_payload(payload, route=route)


def transport(registers=None):
    return ReadOnlyHopewindTransport(registers=hopewind_registers() if registers is None else registers,
        input_registers={}, command_responses=None, probe_target=ProbeTarget(1, 255, 1))


class HopewindDriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_detect_read_and_capture_share_one_read_only_pack(self):
        driver, link = ModbusCatalogDriver(), transport()
        inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.variant_key, "hopewind_0237")
        self.assertEqual(inverter.register_schema_name, "hopewind_0237/base.json")
        self.assertEqual(inverter.model_name, "Hopewind String (Protocol 0237)")
        self.assertEqual(len(inverter.capabilities), 3)
        self.assertTrue(all(not cap.tested for cap in inverter.capabilities))
        self.assertEqual(inverter.profile_name, "modbus_catalog/hopewind_0237.json")
        values = (await driver.async_read_values(link, inverter)).values
        expected = {"pv1_input_voltage": 430, "pv2_input_voltage": 580,
            "pv_string_1_current": 0.16, "pv_string_17_current": 1.23,
            "pv_string_20_current": 4.56, "pv1_input_power": 420,
            "grid_voltage_ab": 413, "grid_current_a": 1.3, "grid_frequency": 49.99,
            "inverter_ac_power": 730, "ac_reactive_power": -20, "pv_power": 760,
            "inverter_efficiency": 94.86, "power_factor": 1,
            "inverter_temperature": 37.2, "inverter_operation_mode": "On-grid",
            "pv_energy_today": 19.3, "pv_energy_total": 40650.81,
            "rated_power": 15000, "rated_voltage": 400}
        for key, value in expected.items():
            self.assertEqual(values[key], value, key)
        # Grid-tied generation is NOT household load or a site import/export meter.
        for key in ("output_power", "grid_power", "battery_voltage", "battery_power"):
            self.assertNotIn(key, values)
        with self.assertRaises(ValueError):
            await driver.async_write_capability(link, inverter, "inverter_power", True)
        capture = await driver.async_capture_support_evidence(link, inverter)
        self.assertEqual(capture["range_failures"], [])
        self.assertEqual([(x["start"], x["count"]) for x in capture["captured_ranges"]],
                         [(40002, 4), (40011, 3), (40500, 71), (40571, 29), (40600, 51)])

    async def test_rejects_missing_all_zero_and_contradictory_identity(self):
        banks = [{}, {r: 0 for r in range(40500, 40651)}]
        for register, bad in ((40646, 0), (40646, 81), (40647, 0), (40647, 65535),
                              (40546, 2), (40547, 0), (40547, 65535)):
            banks.append(hopewind_registers() | {register: bad})
        for register in (40646, 40647, 40546, 40547):
            bank = hopewind_registers()
            del bank[register]
            banks.append(bank)
        for bank in banks:
            with self.subTest(bank_size=len(bank)):
                self.assertIsNone(await ModbusCatalogDriver().async_probe(transport(bank), ProbeTarget(1, 255, 1)))

    async def test_rejected_settings_reads_do_not_break_telemetry(self):
        for rejected in (False, True):
            with self.subTest(rejected=rejected):
                driver, link = ModbusCatalogDriver(), transport()
                inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))

                async def read(start, count):
                    if start == 40002:
                        if rejected:
                            raise ModbusError("exception_code:2")
                        return [0, 10000, 0, 0]
                    if start == 40011:
                        return [1, 1500, 10000]
                    return [hopewind_registers()[address] for address in range(start, start + count)]

                session = type("Session", (), {"read_holding": AsyncMock(side_effect=read)})()
                async def read_registers(start, count, **kwargs):
                    return await read(start, count)
                session.read_registers = AsyncMock(side_effect=read_registers)
                with patch.object(driver, "_session", return_value=session):
                    evidence = await driver.async_capture_support_evidence(link, inverter)
                    self.assertNotIn("support_read_diagnostics", evidence)
                    self.assertEqual(len(evidence["range_failures"]), int(rejected))
                    self.assertEqual([block["start"] for block in evidence["fixture_ranges"]],
                                     ([40011] if rejected else [40002, 40011]) + [40500, 40571, 40600])
                    session.read_holding.reset_mock()
                    values = (await driver.async_read_values(link, inverter)).values
                    session.read_holding.assert_not_awaited()
                self.assertEqual(values["rated_power"], 15000)
                self.assertEqual(values["active_power_regulation_ratio"], 100)
                self.assertEqual("reactive_power_regulation_mode" in values, not rejected)

    async def test_untested_controls_gating_and_single_register_write_readback(self):
        driver = ModbusCatalogDriver()
        link = FixtureTransport(registers=hopewind_registers(), input_registers={},
            command_responses=None, probe_target=ProbeTarget(1, 255, 1))
        inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
        profile = load_driver_profile(inverter.profile_name)
        for cap in profile.capabilities:
            for mode in ("read_only", "auto", "full"):
                self.assertEqual(capability_write_exposure_allowed(cap,
                    control_mode=mode, detection_confidence="medium",
                    variant_key=inverter.variant_key, profile_source_scope="builtin",
                    schema_source_scope="builtin", profile_name=inverter.profile_name), mode == "full")
            samples = [c.label for c in cap.choices] or [0, 12.34, 100]
            for value in samples:
                before = dict(link._registers)
                with patch.object(link, "async_send_payload", wraps=link.async_send_payload) as wire:
                    result = await driver.async_write_capability(link, inverter, cap.key, value)
                self.assertEqual(result, value)
                self.assertEqual([c.args[0][1] for c in wire.call_args_list], [6, 3])
                self.assertEqual({r:v for r,v in link._registers.items() if r != cap.register},
                                 {r:v for r,v in before.items() if r != cap.register})
                self.assertEqual((await driver.async_read_values(link, inverter)).values[cap.key], value)
        ratio = profile.get_capability("active_power_regulation_ratio")
        self.assertEqual((ratio.native_minimum, ratio.native_maximum, ratio.native_step), (0, 100, 0.01))
        for value in (-0.01, 100.01, 110):
            with self.subTest(value=value), patch.object(link, "async_send_payload", new_callable=AsyncMock) as wire:
                with self.assertRaises(ValueError):
                    await driver.async_write_capability(link, inverter, ratio.key, value)
                wire.assert_not_awaited()

    async def test_settings_scaling_signed_read_and_no_clamping_existing_110_percent(self):
        driver, link = ModbusCatalogDriver(), transport(hopewind_registers() | {40005: 0xFF9C})
        inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
        values = (await driver.async_read_values(link, inverter)).values
        self.assertEqual(values["active_power_regulation_ratio"], 110)
        self.assertEqual(values["active_power_regulation_setpoint"], 16.5)
        self.assertEqual(values["power_factor_regulation_setpoint"], 1)
        self.assertEqual(values["reactive_power_regulation_ratio"], -1)

    async def test_write_rejection_or_missing_readback_never_reports_success(self):
        driver, link = ModbusCatalogDriver(), transport()
        inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
        for error in (ModbusError("exception_code:3"), None):
            session = type("Session", (), {
                "write_single_holding": AsyncMock(side_effect=error),
                "read_holding": AsyncMock(return_value=[0]),
            })()
            with patch.object(driver, "_session", return_value=session):
                with self.assertRaises((ModbusError, RuntimeError)):
                    await driver.async_write_capability(link, inverter,
                        "active_power_regulation_ratio", 50)
            session.write_single_holding.assert_awaited_once_with(40013, 5000)
            if error:
                session.read_holding.assert_not_awaited()
            else:
                session.read_holding.assert_awaited_once_with(40013, 1)

    async def test_settings_poll_reads_one_bounded_group_per_cycle(self):
        driver, link = ModbusCatalogDriver(), transport()
        inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
        state = {}
        for expected in (40002, 40011, 40002):
            with patch.object(link, "async_send_payload", wraps=link.async_send_payload) as wire:
                await driver.async_read_values(link, inverter, runtime_state=state)
            requests = [c.args[0] for c in wire.call_args_list]
            self.assertTrue(all(r[1] == 3 for r in requests))
            settings = [int.from_bytes(r[2:4], "big") for r in requests
                        if int.from_bytes(r[2:4], "big") < 40500]
            self.assertEqual(settings, [expected])

    async def test_standby_zero_telemetry_and_family_power_range_can_identify(self):
        for power in (3, 15, 80):
            bank = {r: 0 for r in range(40500, 40651)} | {
                40646: power, 40647: 400, 40546: 0, 40547: 1,
            }
            self.assertIsNotNone(await ModbusCatalogDriver().async_probe(transport(bank), ProbeTarget(1, 255, 1)))

    async def test_failed_block_does_not_invent_zero_or_retain_old_readings(self):
        driver, link = ModbusCatalogDriver(), transport()
        inverter = await driver.async_probe(link, ProbeTarget(1, 255, 1))
        del link._registers[40538]
        values = (await driver.async_read_values(link, inverter)).values
        self.assertNotIn("grid_frequency", values)
        self.assertNotIn("pv_energy_total", values)
        self.assertEqual(values["rated_power"], 15000)

    def test_schema_coverage_units_and_disabled_extra_channels(self):
        schema = load_register_schema("hopewind_0237/base.json")
        self.assertEqual(len(schema.spec_set("runtime")), 90)
        descriptions = {d.key: d for d in schema.measurement_descriptions}
        for spec in schema.spec_set("runtime"):
            self.assertIn(spec.key, descriptions)
            self.assertTrue(any(b.start <= spec.register and
                spec.register + spec.word_count <= b.start + b.count for b in schema.blocks), spec.key)
        self.assertEqual(descriptions["pv_energy_total"].unit, "kWh")
        self.assertEqual(descriptions["pv_energy_total"].state_class, "total_increasing")
        self.assertFalse(descriptions["pv8_input_voltage"].enabled_default)
        self.assertFalse(descriptions["fault_word_1"].enabled_default)


if __name__ == "__main__":
    unittest.main()
