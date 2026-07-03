from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402


def _aohai_input_registers() -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in (
        (0, 14), (32, 2), (42, 15), (64, 2), (96, 13),
        (127, 2), (141, 12), (284, 6), (320, 12), (379, 12),
    ):
        for offset in range(count):
            registers[start + offset] = 0

    registers.update(
        {
            0: 2,        # Off-Grid
            2: 2301,     # 230.1 V output
            5: 43,       # 4.3 A
            10: 412,     # 41.2 C
            32: 0,       # no fault
            33: 302,     # Low battery warning
            42: 2298,    # grid 229.8 V
            51: 5001,    # 50.01 Hz
            64: 3550,    # PV 355.0 V
            65: 61,      # PV 6.1 A
            127: 264,    # battery 26.4 V (24 V-class unit)
            128: 78,     # SOC 78 %
            141: 0xFF9C, # -1.00 A discharge
            142: 251,    # 25.1 C
            152: 99,     # SOH
            284: 0, 285: 12000,
            286: 0xFFFF, 287: 0xFFFF - 11999,  # signed -12000 raw -> +1200.0 W
            379: 0, 380: 123,  # 12.3 kWh today
        }
    )
    return registers


def _aohai_holding_registers() -> dict[int, int]:
    return {63: 0, 64: 42000}  # rated power raw 42000 -> 4200.0 W


def _target() -> ProbeTarget:
    return ProbeTarget(devcode=1, collector_addr=255, device_addr=1)


def _transport(
    *,
    input_registers: dict[int, int] | None = None,
    holding_registers: dict[int, int] | None = None,
) -> FixtureTransport:
    return FixtureTransport(
        registers=_aohai_holding_registers() if holding_registers is None else holding_registers,
        input_registers=_aohai_input_registers() if input_registers is None else input_registers,
        command_responses=None,
        probe_target=_target(),
    )


class ModbusCatalogDriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_matches_aohai_plausibility_anchors(self) -> None:
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(_transport(), _target())

        assert inverter is not None
        self.assertEqual(inverter.driver_key, "modbus_catalog")
        self.assertEqual(inverter.model_name, "Sandi Aohai FSA (Modbus)")
        self.assertEqual(inverter.register_schema_name, "aohai_fsa/base.json")
        detection = inverter.details["catalog_detection"]
        self.assertEqual(detection["surface_key"], "aohai_fsa_read_only")
        self.assertIn("identity.aohai_battery_percent_raw", detection["evidence"])

    async def test_probe_matches_48v_variant_with_same_map(self) -> None:
        # Family envelope anchors must accept every electrical variant that
        # shares the register map (24 V vs 48 V units).
        registers = _aohai_input_registers()
        registers[127] = 512  # 51.2 V battery
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(
            _transport(input_registers=registers), _target()
        )
        self.assertIsNotNone(inverter)

    async def test_probe_rejects_out_of_envelope_registers(self) -> None:
        registers = _aohai_input_registers()
        registers[0] = 9999      # not a known status code
        registers[128] = 250     # SOC out of range
        registers[127] = 12000   # 1200 V battery — outside any variant
        driver = ModbusCatalogDriver()
        inverter = await driver.async_probe(
            _transport(input_registers=registers, holding_registers={63: 0, 64: 0}),
            _target(),
        )
        self.assertIsNone(inverter)

    async def test_probe_rejects_silent_device(self) -> None:
        driver = ModbusCatalogDriver()
        transport = FixtureTransport(
            registers={},
            input_registers={},
            command_responses=None,
            probe_target=_target(),
        )
        inverter = await driver.async_probe(transport, _target())
        self.assertIsNone(inverter)

    async def test_read_values_decodes_input_and_holding_spaces(self) -> None:
        driver = ModbusCatalogDriver()
        transport = _transport()
        inverter = await driver.async_probe(transport, _target())
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["inverter_operation_mode"], "Off-Grid")
        self.assertEqual(values["output_voltage"], 230.1)
        self.assertEqual(values["battery_voltage"], 26.4)
        self.assertEqual(values["battery_percent"], 78)
        self.assertEqual(values["battery_current"], -1.0)
        self.assertEqual(values["warning_code"], "Low battery")
        self.assertEqual(values["grid_frequency"], 50.01)
        # Holding-space rated power must not leak into the input-space
        # pv_input_voltage (register 64 exists in both address spaces).
        self.assertEqual(values["rated_power"], 4200.0)
        self.assertEqual(values["pv_input_voltage"], 355.0)
        # Signed 32-bit with negative multiplier (raw -12000 -> +1200.0 W).
        self.assertEqual(values["output_power"], 1200.0)
        self.assertEqual(values["pv_generation_day"], 12.3)

    async def test_registry_exposes_driver_and_measurements(self) -> None:
        from custom_components.eybond_local.drivers.registry import (
            driver_options,
            iter_drivers,
        )

        self.assertIn("modbus_catalog", driver_options())
        drivers = iter_drivers("modbus_catalog")
        self.assertEqual(len(drivers), 1)
        keys = {m.key for m in drivers[0].measurements}
        self.assertIn("battery_percent", keys)
        self.assertIn("output_power", keys)


if __name__ == "__main__":
    unittest.main()
