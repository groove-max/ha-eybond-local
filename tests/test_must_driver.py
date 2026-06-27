from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.must import MustPvPh18Driver  # noqa: E402
from custom_components.eybond_local.drivers.must import _support_capture_ranges  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402


def _must_registers() -> dict[int, int]:
    registers = {
        10103: 540,
        10110: 2,
        15201: 2,
        15202: 1,
        15203: 2,
        15205: 3760,
        15207: 54,
        15208: 850,
        15209: 33,
        15212: 1,
        15217: 12,
        15218: 345,
        15219: 7,
        20000: int.from_bytes(b"PV", "big"),
        20001: 18,
        25201: 2,
        25205: 256,
        25206: 2301,
        25207: 2298,
        25208: 3800,
        25209: 0,
        25210: 12,
        25211: 13,
        25212: 14,
        25213: 450,
        25214: 0xFFCE,
        25215: 440,
        25216: 2534,
        25225: 5001,
        25226: 4998,
        25233: 41,
        25234: 42,
        25274: 0xFF9C,
    }
    return registers


class MustPvPh18DriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_detects_must_pv18_on_slave_four(self) -> None:
        driver = MustPvPh18Driver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)
        transport = FixtureTransport(
            registers=_must_registers(),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.driver_key, "must_pv_ph18")
        self.assertEqual(inverter.protocol_family, "must_pv_ph18")
        self.assertEqual(inverter.model_name, "MUST PV18")
        self.assertEqual(inverter.variant_key, "pv_ph18")
        self.assertEqual(inverter.profile_name, "")
        self.assertEqual(inverter.register_schema_name, "must_pv_ph18/base.json")
        self.assertEqual(inverter.probe_target.device_addr, 4)

    async def test_probe_detects_numeric_pv1800_model_register(self) -> None:
        driver = MustPvPh18Driver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)
        registers = _must_registers()
        registers.pop(20000)
        registers[20001] = 1800
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.driver_key, "must_pv_ph18")
        self.assertEqual(inverter.protocol_family, "must_pv_ph18")
        self.assertEqual(inverter.model_name, "MUST PV1800")
        self.assertEqual(inverter.variant_key, "pv_ph18")
        self.assertEqual(inverter.register_schema_name, "must_pv_ph18/base.json")

    async def test_read_values_decodes_third_party_register_map(self) -> None:
        driver = MustPvPh18Driver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)
        transport = FixtureTransport(
            registers=_must_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["model_number"], "PV18")
        self.assertEqual(values["battery_type"], "Lithium")
        self.assertEqual(values["pv_charger_workstate"], "Work")
        self.assertEqual(values["pv_charger_mppt_state"], "MPPT")
        self.assertEqual(values["pv_charger_charge_state"], "Float")
        self.assertEqual(values["inverter_operation_mode"], "Off-Grid")
        self.assertEqual(values["battery_float_voltage"], 54.0)
        self.assertEqual(values["pv_input_voltage"], 376.0)
        self.assertEqual(values["pv_input_current"], 5.4)
        self.assertEqual(values["pv_charging_power"], 850)
        self.assertEqual(values["pv_generation_sum"], 12345)
        self.assertEqual(values["pv_generation_day"], 7)
        self.assertEqual(values["battery_voltage"], 25.6)
        self.assertEqual(values["output_voltage"], 230.1)
        self.assertEqual(values["grid_voltage"], 229.8)
        self.assertEqual(values["output_current"], 1.2)
        self.assertEqual(values["load_percent"], 25.34)
        self.assertEqual(values["output_frequency"], 50.01)
        self.assertEqual(values["grid_frequency"], 49.98)
        self.assertEqual(values["grid_power"], -50)
        self.assertEqual(values["battery_load"], -100)

    async def test_support_evidence_captures_planned_ranges(self) -> None:
        driver = MustPvPh18Driver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)
        registers = _must_registers()
        for start, count in _support_capture_ranges("must_pv_ph18/base.json"):
            for register in range(start, start + count):
                registers.setdefault(register, 0)
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        evidence = await driver.async_capture_support_evidence(transport, inverter)

        self.assertEqual(evidence["capture_kind"], "must_pv_ph18_modbus_register_dump")
        self.assertEqual(evidence["range_failures"], [])
        planned = [(item["start"], item["count"]) for item in evidence["planned_ranges"]]
        self.assertIn((20000, 17), planned)
        self.assertIn((25201, 74), planned)
        self.assertEqual(len(evidence["fixture_ranges"]), len(planned))

    def test_support_capture_ranges_include_cloud_observed_diagnostic_windows(self) -> None:
        ranges = _support_capture_ranges("must_pv_ph18/base.json")

        self.assertIn((20000, 17), ranges)
        self.assertIn((20101, 32), ranges)
        self.assertIn((20213, 2), ranges)
        self.assertIn((25201, 74), ranges)


if __name__ == "__main__":
    unittest.main()
