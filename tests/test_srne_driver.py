from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.srne import SrneModbusDriver  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402


def _low_byte_ascii_words(text: str, count: int) -> list[int]:
    padded = text[:count].ljust(count)
    return [ord(char) for char in padded]


def _srne_registers(*, include_phase: bool = True) -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in ((53, 20), (256, 18), (516, 4), (528, 18), (554, 14)):
        if start == 554 and not include_phase:
            continue
        for offset in range(count):
            registers[start + offset] = 0

    for offset, word in enumerate(_low_byte_ascii_words("SR-EOV24", 20)):
        registers[53 + offset] = word

    registers.update(
        {
            256: 85,
            257: 512,
            258: 123,
            263: 3561,
            264: 42,
            265: 680,
            267: 2,
            270: 900,
            271: 3482,
            272: 38,
            273: 620,
            516: 7,
            528: 5,
            531: 2301,
            533: 5002,
            534: 2298,
            536: 4998,
            537: 56,
            539: 1200,
            540: 1300,
            542: 101,
            543: 64,
            544: 425,
            545: 392,
        }
    )

    if include_phase:
        registers.update(
            {
                554: 2310,
                555: 2320,
                556: 2280,
                557: 2270,
                560: 25,
                561: 26,
                562: 510,
                563: 520,
                564: 610,
                565: 620,
                566: 31,
                567: 32,
            }
        )
    return registers


class SrneModbusDriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_probe_detects_srne_product_info_on_slave_one(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNotNone(inverter)
        assert inverter is not None
        self.assertEqual(inverter.driver_key, "srne_modbus")
        self.assertEqual(inverter.protocol_family, "srne_modbus")
        self.assertEqual(inverter.model_name, "SRNE SR-EOV24")
        self.assertEqual(inverter.variant_key, "srne_family")
        self.assertEqual(inverter.profile_name, "")
        self.assertEqual(inverter.register_schema_name, "srne_modbus/base.json")
        self.assertEqual(inverter.probe_target.device_addr, 1)
        self.assertEqual(inverter.details["product_info"], "SR-EOV24")

    async def test_probe_rejects_non_srne_product_info(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        registers = _srne_registers()
        for offset, word in enumerate(_low_byte_ascii_words("INV-0001", 20)):
            registers[53 + offset] = word
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNone(inverter)

    async def test_read_values_decodes_srne_register_map(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["product_info"], "SR-EOV24")
        self.assertEqual(values["battery_percent"], 85)
        self.assertEqual(values["battery_voltage"], 51.2)
        self.assertEqual(values["battery_current"], 12.3)
        self.assertEqual(values["pv1_input_voltage"], 356.1)
        self.assertEqual(values["pv1_input_current"], 4.2)
        self.assertEqual(values["pv1_input_power"], 680)
        self.assertEqual(values["inverter_charge_state"], 2)
        self.assertEqual(values["charge_power"], 900)
        self.assertEqual(values["pv2_input_voltage"], 348.2)
        self.assertEqual(values["pv2_input_current"], 3.8)
        self.assertEqual(values["pv2_input_power"], 620)
        self.assertEqual(values["fault_code"], 7)
        self.assertEqual(values["inverter_operation_mode"], "Inverter")
        self.assertEqual(values["grid_voltage"], 230.1)
        self.assertEqual(values["grid_frequency"], 50.02)
        self.assertEqual(values["output_voltage"], 229.8)
        self.assertEqual(values["output_frequency"], 49.98)
        self.assertEqual(values["output_current"], 5.6)
        self.assertEqual(values["output_power"], 1200)
        self.assertEqual(values["output_va"], 1300)
        self.assertEqual(values["battery_charge_current"], 10.1)
        self.assertEqual(values["load_percent"], 64)
        self.assertEqual(values["dcdc_temperature"], 42.5)
        self.assertEqual(values["inverter_temperature"], 39.2)
        self.assertEqual(values["grid_voltage_l2"], 231.0)
        self.assertEqual(values["output_power_l3"], 520)
        self.assertEqual(values["load_percent_l3"], 32)

    async def test_read_values_tolerates_missing_optional_phase_block(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(include_phase=False),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["product_info"], "SR-EOV24")
        self.assertEqual(values["output_power"], 1200)
        self.assertNotIn("grid_voltage_l2", values)

    async def test_support_evidence_captures_planned_ranges(self) -> None:
        driver = SrneModbusDriver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers=_srne_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        evidence = await driver.async_capture_support_evidence(transport, inverter)

        self.assertEqual(evidence["capture_kind"], "srne_modbus_register_dump")
        self.assertEqual(evidence["range_failures"], [])
        planned = [(item["start"], item["count"]) for item in evidence["planned_ranges"]]
        self.assertIn((53, 20), planned)
        self.assertIn((528, 18), planned)
        self.assertIn((554, 14), planned)
        self.assertEqual(len(evidence["fixture_ranges"]), len(planned))


if __name__ == "__main__":
    unittest.main()
