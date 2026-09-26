"""Protocol 0237 read-only pack: synthetic register replay, no customer identity."""
from __future__ import annotations

import unittest

from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget


def hopewind_registers():
    # Preserve protocol-shaped measurements, not the customer's serial/firmware.
    return {r: 0 for r in range(40500, 40651)} | {
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
        self.assertFalse(inverter.capabilities)
        self.assertFalse(inverter.profile_name)
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
                         [(40500, 71), (40571, 29), (40600, 51)])

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
