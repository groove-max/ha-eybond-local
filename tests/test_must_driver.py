from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.must import MustPvPh18Driver  # noqa: E402
from custom_components.eybond_local.drivers.must import _support_capture_ranges  # noqa: E402
from custom_components.eybond_local.drivers.read_result import (  # noqa: E402
    DriverReadMode,
    DriverReadResult,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.payload.modbus import ModbusError


def _full_values(result: DriverReadResult) -> dict[str, object]:
    if type(result) is not DriverReadResult or result.mode is not DriverReadMode.FULL:
        raise AssertionError("MUST runtime read must be an exact FULL result")
    return result.values


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
    async def test_pv3300_identity_selects_override_without_changing_other_must_models(self) -> None:
        for prefix, suffix, schema, percent in (
            ("PV", 3300, "pv3300", 14),
            ("PV", 18, "base", 0.14),
            ("PV", 1800, "base", 0.14),
            ("PH", 3300, "base", 0.14),
            ("PV", 3301, "base", 0.14),
        ):
            with self.subTest(prefix=prefix, suffix=suffix):
                driver = MustPvPh18Driver()
                target = ProbeTarget(1, 255, 4)
                transport = FixtureTransport(registers=_must_registers() | {
                    20000: int.from_bytes(prefix.encode(), "big"), 20001: suffix, 25216: 14,
                }, command_responses=None, probe_target=target)
                inverter = await driver.async_probe(transport, target)
                self.assertEqual(inverter.register_schema_name, f"must_pv_ph18/{schema}.json")
                values = _full_values(await driver.async_read_values(transport, inverter))
                self.assertEqual(values["load_percent"], percent)
                self.assertEqual(inverter.capabilities, driver.write_capabilities)

    async def test_pv3300_charge_discharge_grid_and_flow_directions(self) -> None:
        from custom_components.eybond_local.canonical_telemetry import project_canonical_telemetry
        from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry

        # Synthetic two-state replay of #46. AC converter power keeps its native
        # sign; battery/grid powers use the integration's charge/import convention.
        for current, battery, grid, load, voltage, converter in (
            (16, 844, 0, 750, 0, 844),
            (-30, -1613, -1753, 0, 2050, -1559),
            (0, 0, 0, 0, 0, 0),
            (0, 0, 500, 0, 2300, 500),
        ):
            with self.subTest(battery=battery, grid=grid):
                driver = MustPvPh18Driver()
                target = ProbeTarget(1, 255, 4)
                registers = _must_registers() | {
                    20001: 3300, 15208: 0, 25207: voltage, 25213: converter & 0xFFFF,
                    25214: grid & 0xFFFF, 25215: load, 25216: 14,
                    25273: battery & 0xFFFF, 25274: current & 0xFFFF,
                }
                transport = FixtureTransport(registers=registers, command_responses=None, probe_target=target)
                inverter = await driver.async_probe(transport, target)
                values = _full_values(await driver.async_read_values(transport, inverter))
                self.assertEqual(values["battery_current"], -current)
                self.assertEqual(values["battery_power"], -battery)
                self.assertEqual(values["grid_power"], -grid)
                self.assertEqual(values["inverter_power"], converter)
                self.assertEqual(values["load_percent"], 14)
                frame = project_canonical_telemetry(fold_driver_telemetry(
                    TypedTelemetryFrame.empty(), driver_key=driver.key, values=values, replace=True,
                )).values()
                self.assertEqual(frame["battery_to_home_power"], load if battery > 0 else 0)
                self.assertEqual(frame["grid_to_battery_power"], -battery if battery < 0 else 0)

    async def test_missing_pv3300_battery_block_remains_missing(self) -> None:
        driver = MustPvPh18Driver()
        target = ProbeTarget(1, 255, 4)
        registers = _must_registers() | {20001: 3300}
        del registers[25273]
        transport = FixtureTransport(registers=registers, command_responses=None, probe_target=target)
        inverter = await driver.async_probe(transport, target)
        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertNotIn("battery_power", values)
        self.assertNotIn("battery_current", values)

    async def test_converter_power_and_load_are_distinct_signed_words(self) -> None:
        from custom_components.eybond_local.canonical_telemetry import project_canonical_telemetry
        from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry

        for converter_raw, load_raw, converter, load in (
            (65049, 0, -487, 0),  # #46: charging converter, unloaded AC output.
            (450, 440, 450, 440),
            (0x8000, 0xFFFF, -32768, -1),
            (0x7FFF, 1200, 32767, 1200),
        ):
            with self.subTest(converter_raw=converter_raw, load_raw=load_raw):
                registers = _must_registers() | {25213: converter_raw, 25215: load_raw}
                driver = MustPvPh18Driver()
                target = ProbeTarget(1, 255, 4)
                transport = FixtureTransport(registers=registers, command_responses=None, probe_target=target)
                inverter = await driver.async_probe(transport, target)
                assert inverter is not None
                values = _full_values(await driver.async_read_values(transport, inverter))
                self.assertEqual(values["inverter_power"], converter)
                self.assertEqual(values["ac_output_power"], load)
                self.assertNotIn("output_power", values)
                frame = project_canonical_telemetry(fold_driver_telemetry(
                    TypedTelemetryFrame.empty(), driver_key=driver.key, values=values, replace=True,
                ))
                self.assertEqual(frame.values()["output_power"], load)

    async def test_pv_energy_uses_documented_high_low_units_and_fresh_identity(self) -> None:
        from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema

        schema = load_register_schema("must_pv_ph18/base.json")
        descriptions = {item.key: item for item in schema.measurement_descriptions}
        self.assertNotIn("pv_generation_sum", descriptions)
        self.assertNotIn("pv_generation_day", descriptions)
        self.assertEqual(descriptions["pv_energy_total"].unit, "kWh")
        self.assertEqual(descriptions["pv_energy_total"].state_class, "total_increasing")
        days = descriptions["pv_operating_days"]
        self.assertEqual(days.unit, "d")
        self.assertNotEqual(days.device_class, "energy")
        self.assertTrue(days.diagnostic)
        self.assertFalse(days.enabled_default)
        for high, low, expected in ((0, 0, 0), (0, 1, 0.1), (12, 345, 12034.5), (0, 9999, 999.9), (1, 0, 1000)):
            with self.subTest(high=high, low=low):
                driver = MustPvPh18Driver()
                target = ProbeTarget(1, 255, 4)
                registers = _must_registers() | {15217: high, 15218: low, 15219: 39}
                transport = FixtureTransport(registers=registers, command_responses=None, probe_target=target)
                inverter = await driver.async_probe(transport, target)
                assert inverter is not None
                values = _full_values(await driver.async_read_values(transport, inverter))
                self.assertEqual(values["pv_energy_total"], expected)
                self.assertEqual(values["pv_operating_days"], 39)
                self.assertNotIn("pv_generation_sum", values)
                self.assertNotIn("pv_generation_day", values)

    async def test_missing_pv_counter_block_does_not_publish_zero_energy(self) -> None:
        registers = _must_registers()
        del registers[15218]
        driver = MustPvPh18Driver()
        target = ProbeTarget(1, 255, 4)
        transport = FixtureTransport(registers=registers, command_responses=None, probe_target=target)
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None
        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertNotIn("pv_energy_total", values)
        self.assertNotIn("pv_operating_days", values)
        self.assertEqual(values["inverter_power"], 450)
        self.assertEqual(values["ac_output_power"], 440)

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

        values = _full_values(await driver.async_read_values(transport, inverter))

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

        values = _full_values(await driver.async_read_values(transport, inverter))

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
        self.assertEqual(values["pv_energy_total"], 12034.5)
        self.assertEqual(values["pv_operating_days"], 7)
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

    def test_current_labels_match_documented_nodes_without_rekeying_entities(self):
        for name in ("base", "pv3300"):
            schema = load_register_schema(f"must_pv_ph18/{name}.json")
            for key, label, address in (
                ("output_current", "Inverter Current", 25210),
                ("ac_output_current", "Grid Current", 25211),
                ("inverter_load_current", "Load Current", 25212),
            ):
                self.assertEqual(schema.measurement_description(key).name, label)
                spec = next(s for specs in schema.spec_sets.values() for s in specs if s.key == key)
                self.assertEqual(spec.register, address)
                self.assertEqual(spec.divisor, 10)

    async def test_pv3300_zero_current_comparison_is_support_only(self):
        for suffix, bulk, missing, expected in (
            (3300, 0, False, True), (3300, 12, False, False),
            (3300, 0, True, False), (18, 0, False, False),
        ):
            with self.subTest(suffix=suffix, bulk=bulk, missing=missing):
                driver = MustPvPh18Driver()
                target = ProbeTarget(1, 255, 4)
                link = FixtureTransport(registers=_must_registers() | {20001: suffix},
                                        command_responses=None, probe_target=target)
                inverter = await driver.async_probe(link, target)

                async def read(start, count):
                    if (start, count) == (25201, 74):
                        if missing:
                            raise ModbusError("exception_code:2")
                        return [bulk if address in (25210, 25211, 25212) else 0
                                for address in range(start, start + count)]
                    if count == 1 and start in (25210, 25211, 25212):
                        return [13]
                    return [0] * count

                session = type("Session", (), {"read_holding": AsyncMock(side_effect=read)})()
                with patch.object(driver, "_session", return_value=session):
                    evidence = await driver.async_capture_support_evidence(link, inverter)
                self.assertEqual("current_read_diagnostics" in evidence, expected)
                if expected:
                    extra = evidence["current_read_diagnostics"]
                    self.assertEqual(extra["status"], "completed")
                    self.assertEqual([block["words"] for block in extra["captured_ranges"]], [[13]] * 3)
                    original = next(b for b in evidence["fixture_ranges"] if b["start"] == 25201)
                    self.assertEqual(original["values"][9:12], [0, 0, 0])
                singles = [call for call in session.read_holding.await_args_list if call.args[1] == 1]
                self.assertEqual(len(singles), 3 if expected else 0)


if __name__ == "__main__":
    unittest.main()
