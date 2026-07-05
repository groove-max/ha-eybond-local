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
        25273: 16,
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
        self.assertEqual(inverter.profile_name, "must_pv_ph18/base.json")
        self.assertEqual(inverter.register_schema_name, "must_pv_ph18/base.json")
        self.assertEqual(inverter.probe_target.device_addr, 4)
        # The profile's untested controls ride along with the probe result.
        self.assertTrue(inverter.capabilities)

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

    async def test_every_control_reads_back_a_current_value(self) -> None:
        # The control registers are sparse; the read blocks must be gap-free so
        # the device does not reject them and every control shows its current
        # value (not just after a write). Regression guard for the batch where
        # the wide 20101-20106 / 20125-20132 blocks spanned absent registers and
        # left all controls blank.
        from custom_components.eybond_local.control_policy import can_expose_capability
        from custom_components.eybond_local.const import CONTROL_MODE_FULL

        driver = MustPvPh18Driver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)
        registers = _must_registers()
        registers.update(
            {
                20101: 1, 20102: 2300, 20103: 5000, 20104: 0, 20106: 1,
                20108: 1, 20109: 1, 20111: 0, 20112: 1, 20113: 200,
                20118: 480, 20119: 560, 20125: 450, 20127: 440, 20128: 590,
                20132: 600, 20143: 3,
                10103: 540, 10104: 560, 10108: 600, 10110: 2, 10111: 200,
                10118: 1, 10119: 600, 10121: 60, 10122: 120, 10123: 30,
            }
        )
        transport = FixtureTransport(
            registers=registers, command_responses=None, probe_target=target
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        values = await driver.async_read_values(transport, inverter)

        # Every writable control resolves a current value from its own register.
        for capability in inverter.capabilities:
            self.assertIn(capability.value_key, values, capability.key)
        # The four that used to sit inside the gap-spanning blocks.
        self.assertEqual(values["inverter_output_voltage"], 230.0)
        self.assertEqual(values["inverter_output_frequency"], "50 Hz")
        self.assertEqual(values["battery_low_voltage"], 44.0)
        self.assertEqual(values["battery_high_voltage"], 59.0)
        # Enum read-back decodes to the exact select option label.
        self.assertEqual(values["energy_use_mode"], "SBU (Solar, Battery, Utility)")
        self.assertEqual(values["grid_protect_standard"], "VDE4105")
        # And those decoded labels are valid options on the exposed selects.
        by_key = {c.key: c for c in inverter.capabilities}
        for key in ("energy_use_mode", "grid_protect_standard", "solar_use_aim"):
            capability = by_key[key]
            self.assertTrue(
                can_expose_capability(
                    capability,
                    control_mode=CONTROL_MODE_FULL,
                )
            )
            labels = {choice.label for choice in capability.choices}
            self.assertIn(values[key], labels, key)

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
        self.assertEqual(values["battery_current"], -100)
        self.assertEqual(values["battery_power"], 16)

    async def test_write_capability_uses_single_register_writes(self) -> None:
        driver = MustPvPh18Driver()
        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)
        transport = FixtureTransport(
            registers=_must_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None

        result = await driver.async_write_capability(
            transport, inverter, "charge_source_priority", "Only Solar"
        )
        self.assertEqual(result, "Only Solar")
        self.assertEqual(transport._registers[20143], 3)

        result = await driver.async_write_capability(
            transport, inverter, "grid_max_charge_current", 45.0
        )
        self.assertEqual(result, 45.0)
        self.assertEqual(transport._registers[20125], 450)

    async def test_cloud_confirmed_controls_are_tested_and_exposed_in_auto(self) -> None:
        from custom_components.eybond_local.control_policy import can_expose_capability
        from custom_components.eybond_local.const import CONTROL_MODE_AUTO

        # These map to SmartESS cloud device_settings fields, so they ship
        # tested and are exposed in the default (auto) control mode.
        expected_tested = {
            "offgrid_output_enable",
            "power_save_mode",
            "charge_source_priority",
            "grid_max_charge_current",
            "max_combined_charge_current",
            "inverter_output_voltage",
            "inverter_output_frequency",
            "pv_max_charge_current",
            "float_voltage",
            "absorption_voltage",
            "battery_type",
            "battery_stop_charge_voltage",
            "battery_stop_discharge_voltage",
            "battery_low_voltage",
            "battery_high_voltage",
            "max_discharge_current",
            "energy_use_mode",
            "grid_protect_standard",
            "solar_use_aim",
            "discharge_to_grid_enable",
        }
        driver = MustPvPh18Driver()
        by_key = {c.key: c for c in driver.write_capabilities}
        self.assertTrue(expected_tested.issubset(by_key))
        for key in expected_tested:
            capability = by_key[key]
            self.assertTrue(capability.tested, key)
            self.assertTrue(
                can_expose_capability(
                    capability,
                    control_mode=CONTROL_MODE_AUTO,
                    detection_confidence="high",
                ),
                key,
            )

    async def test_datasheet_only_controls_stay_untested_and_full_control_only(self) -> None:
        from custom_components.eybond_local.control_policy import can_expose_capability
        from custom_components.eybond_local.const import (
            CONTROL_MODE_AUTO,
            CONTROL_MODE_FULL,
        )

        # Datasheet-only controls (in the 1.4.15 xlsx but not exposed by the
        # SmartESS cloud) ship untested: hidden in auto, shown in full control.
        expected_untested = {
            "grid_charge_enable",
            "battery_equalization_enable",
            "battery_equalization_voltage",
            "battery_ah",
            "battery_equalized_time",
            "battery_equalized_timeout",
            "battery_equalization_interval",
        }
        driver = MustPvPh18Driver()
        by_key = {c.key: c for c in driver.write_capabilities}
        self.assertTrue(expected_untested.issubset(by_key))
        for key in expected_untested:
            capability = by_key[key]
            self.assertFalse(capability.tested, key)
            self.assertFalse(
                can_expose_capability(capability, control_mode=CONTROL_MODE_AUTO),
                key,
            )
            self.assertTrue(
                can_expose_capability(capability, control_mode=CONTROL_MODE_FULL),
                key,
            )

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
