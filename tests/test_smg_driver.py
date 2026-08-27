from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.smg import (  # noqa: E402
    SmgModbusDriver,
    _apply_capability_read_back,
    _decode_block,
    _support_capture_ranges,
)
from custom_components.eybond_local.drivers.read_result import (  # noqa: E402
    DriverReadMode,
    DriverReadResult,
)
from custom_components.eybond_local.drivers.write_confirmation import (  # noqa: E402
    WRITE_CONFIRMATION_DIAGNOSTIC_KEY,
    write_confirmation_diagnostics,
)
from custom_components.eybond_local.models import RegisterValueSpec  # noqa: E402
from custom_components.eybond_local.control_policy import can_expose_capability  # noqa: E402
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    load_register_schema,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget  # noqa: E402
from custom_components.eybond_local.payload.modbus import crc16_modbus  # noqa: E402
from custom_components.eybond_local.telemetry import (  # noqa: E402
    TelemetryFreshness,
    TypedTelemetryFrame,
    fold_driver_telemetry,
)


def _full_values(result: DriverReadResult) -> dict[str, object]:
    if type(result) is not DriverReadResult or result.mode is not DriverReadMode.FULL:
        raise AssertionError("SMG runtime read must be an exact FULL result")
    return result.values


def _register_map_for_ranges(ranges: tuple[tuple[int, int], ...]) -> dict[int, int]:
    registers: dict[int, int] = {}
    for start, count in ranges:
        for register in range(start, start + count):
            registers[register] = register & 0xFFFF
    return registers


class SmgSupportCaptureRangeTests(unittest.TestCase):
    def test_support_capture_ranges_include_future_11k_windows(self) -> None:
        self.assertEqual(
            _support_capture_ranges(),
            (
                (100, 10),
                (171, 1),
                (184, 1),
                (186, 12),
                (201, 34),
                (277, 5),
                (300, 54),
                (389, 3),
                (406, 1),
                (420, 1),
                (425, 1),
                (607, 1),
                (626, 8),
                (643, 2),
                (696, 49),
            ),
        )

    def test_support_capture_ranges_include_protocol_1_fault_log_window_for_anenji_4200(self) -> None:
        self.assertEqual(
            _support_capture_ranges("modbus_smg/models/anenji_4200_protocol_1.json"),
            (
                (100, 10),
                (171, 1),
                (184, 1),
                (186, 12),
                (201, 34),
                (277, 5),
                (300, 54),
                (389, 3),
                (406, 1),
                (420, 1),
                (607, 1),
                (626, 8),
                (643, 2),
                (696, 49),
            ),
        )

    def test_support_capture_ranges_use_requested_schema_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "modbus_smg" / "models" / "future_smg.json"
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(
                json.dumps(
                    {
                        "extends": "builtin:modbus_smg/models/smg_6200.json",
                        "schema_key": "future_smg",
                        "title": "Future SMG",
                        "driver_key": "modbus_smg",
                        "protocol_family": "modbus_smg",
                        "scalar_registers": {
                            "future_probe_register": 900,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            set_external_register_schema_roots((Path(temp_dir),))
            try:
                self.assertIn((900, 1), _support_capture_ranges("modbus_smg/models/future_smg.json"))
            finally:
                set_external_register_schema_roots(())

    def test_support_capture_ranges_include_anenji_protocol_3_10_windows(self) -> None:
        self.assertEqual(
            _support_capture_ranges("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"),
            (
                (100, 10),
                (171, 1),
                (184, 1),
                (186, 46),
                (252, 5),
                (277, 5),
                (302, 4),
                (326, 2),
                (338, 18),
                (376, 18),
                (414, 18),
                (600, 57),
                (677, 18),
                (696, 9),
                (707, 1),
                (709, 1),
                (858, 2),
            ),
        )


class SmgSupportCaptureEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_capture_support_evidence_includes_future_11k_windows(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 11000",
            serial_number="92632500000001",
            probe_target=target,
            register_schema_name="modbus_smg/models/smg_6200.json",
        )
        expected_ranges = _support_capture_ranges(inverter.register_schema_name)
        transport = FixtureTransport(
            registers=_register_map_for_ranges(expected_ranges),
            command_responses=None,
            probe_target=target,
        )

        evidence = await driver.async_capture_support_evidence(transport, inverter)

        self.assertEqual(
            evidence["capture_notes"],
            [
                "Includes supplemental SMG identity points and family discovery ranges: 171, 184, 277-281, 338-353, 389-391, 607, 626-633, 643-644, 696-704.",
                "Protocol-1 SMG layouts also include documented fault/log windows: 700-744.",
            ],
        )
        self.assertEqual(
            [(item["start"], item["count"]) for item in evidence["planned_ranges"]],
            list(expected_ranges),
        )
        self.assertEqual(evidence["range_failures"], [])

        captured_by_start = {item["start"]: item for item in evidence["captured_ranges"]}
        self.assertEqual(captured_by_start[171]["count"], 1)
        self.assertEqual(captured_by_start[184]["count"], 1)
        self.assertEqual(captured_by_start[300]["count"], 54)
        self.assertEqual(captured_by_start[607]["words"], [607])
        self.assertEqual(captured_by_start[626]["count"], 8)
        self.assertEqual(captured_by_start[643]["count"], 2)
        self.assertEqual(captured_by_start[696]["count"], 49)
        self.assertEqual(len(evidence["fixture_ranges"]), len(expected_ranges))

    async def test_capture_support_evidence_includes_anenji_protocol_3_10_windows(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji ANJ-11KW-48V-WIFI-P",
            serial_number="ANJ11KW240001",
            probe_target=target,
            variant_key="anenji_anj_11kw_48v_wifi_p",
            register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        expected_ranges = _support_capture_ranges(inverter.register_schema_name)
        transport = FixtureTransport(
            registers=_register_map_for_ranges(expected_ranges),
            command_responses=None,
            probe_target=target,
        )

        evidence = await driver.async_capture_support_evidence(transport, inverter)

        self.assertEqual(
            [(item["start"], item["count"]) for item in evidence["planned_ranges"]],
            list(expected_ranges),
        )
        self.assertEqual(evidence["range_failures"], [])

        captured_by_start = {item["start"]: item for item in evidence["captured_ranges"]}
        self.assertEqual(captured_by_start[326]["count"], 2)
        self.assertEqual(captured_by_start[338]["count"], 18)
        self.assertEqual(captured_by_start[376]["count"], 18)
        self.assertEqual(captured_by_start[414]["count"], 18)
        self.assertEqual(captured_by_start[677]["count"], 18)
        self.assertEqual(captured_by_start[707]["words"], [707])
        self.assertEqual(captured_by_start[709]["words"], [709])
        self.assertEqual(captured_by_start[858]["count"], 2)


def _ascii_words(text: str, *, word_count: int) -> dict[int, int]:
    payload = text.encode("ascii")[: word_count * 2].ljust(word_count * 2, b"\x00")
    return {
        offset: int.from_bytes(payload[offset * 2 : offset * 2 + 2], "big")
        for offset in range(word_count)
    }


class SmgAnenjiVariantTests(unittest.IsolatedAsyncioTestCase):
    def _anenji_registers(self) -> dict[int, int]:
        registers: dict[int, int] = {
            register: 0
            for start, stop in ((100, 110), (171, 185), (198, 232), (600, 657), (696, 705))
            for register in range(start, stop)
        }
        for offset, value in _ascii_words("ANJ11KW240001", word_count=12).items():
            registers[186 + offset] = value

        registers.update(
            {
                100: 0,
                101: 0,
                104: 0,
                105: 0,
                171: 32768,
                184: 4,
                198: 1,
                201: 3,
                202: 123,
                203: 5000,
                204: 420,
                205: 480,
                206: 250,
                207: 0,
                226: 456,
                227: 5000,
                228: 3800,
                229: 4200,
                230: 125,
                231: 31,
                252: 210,
                253: 5000,
                254: 4200,
                255: 4600,
                256: 65,
                277: 512,
                278: 80,
                279: 4100,
                280: 78,
                281: 29,
                302: 2400,
                303: 1800,
                304: 75,
                305: 33,
                338: 2300,
                342: 2305,
                346: 2298,
                351: 649,
                352: 1,
                353: 7,
                389: 667,
                390: 0,
                391: 0,
                600: 5,
                601: 2,
                606: 2300,
                607: 5000,
                630: 4,
                631: 620,
                632: 2,
                637: 560,
                638: 540,
                640: 1000,
                641: 300,
                643: 520,
                644: 480,
                646: 470,
                647: 25,
                648: 45,
                650: 15,
                651: 1,
                652: 580,
                653: 60,
                654: 120,
                655: 30,
                677: 1,
                678: 0,
                679: 0,
                680: 0,
                681: 0,
                682: 1,
                683: 1,
                684: 1,
                685: 0,
                686: 0,
                687: 65535,
                688: 60927,
                689: 0,
                690: 0,
                691: 11000,
                692: 0,
                693: 0,
                694: 1,
                696: 2026,
                697: 4,
                698: 17,
                699: 7,
                700: 22,
                701: 1,
                702: 314,
                703: 0,
                704: 12345,
                707: 0,
                709: 6,
            }
        )
        return registers

    async def test_probe_selects_anenji_variant_and_tested_capability_profile(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)

        class PointIdentityOnlyTransport(FixtureTransport):
            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                if address in {171, 184} and count != 1:
                    raise RuntimeError("illegal_data_address")
                return super()._handle_read_holding(payload)

        transport = PointIdentityOnlyTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "anenji_anj_11kw_48v_wifi_p")
        self.assertEqual(inverter.model_name, "Anenji ANJ-11KW-48V-WIFI-P")
        self.assertEqual(
            inverter.profile_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(
            inverter.register_schema_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(len(inverter.capability_groups), 4)
        # Pinned control count for this profile. Document-backed controls may
        # be present as explicit Full-Control-only candidates, but must not be
        # silently exposed by the automatic policy before a hardware retest.
        self.assertEqual(len(inverter.capabilities), 53)
        self.assertEqual(inverter.get_capability("output_mode").register, 600)
        secondary_output = inverter.get_capability("secondary_output_priority")
        self.assertEqual(secondary_output.register, 602)
        self.assertFalse(secondary_output.tested)
        self.assertEqual(secondary_output.provenance, "doc_backed")
        self.assertEqual(secondary_output.resolved_support_tier, "conditional")
        self.assertFalse(
            can_expose_capability(
                secondary_output,
                control_mode="auto",
                detection_confidence="high",
            )
        )
        self.assertTrue(
            can_expose_capability(
                secondary_output,
                control_mode="full",
                detection_confidence="high",
            )
        )
        self.assertEqual(inverter.get_capability("charge_source_priority").register, 632)
        self.assertEqual(inverter.get_capability("force_eq_charge").register, 656)
        self.assertEqual(inverter.get_capability("input_mode").register, 677)
        self.assertEqual(inverter.get_capability("warning_mask_i").register, 687)
        self.assertEqual(inverter.get_capability("turn_on_mode").register, 693)
        self.assertEqual(inverter.get_capability("remote_turn_on").register, 694)
        self.assertEqual(inverter.get_capability("exit_fault_mode").register, 695)
        self.assertEqual(inverter.get_capability("inverter_date_write").register, 696)
        self.assertEqual(inverter.get_capability("inverter_time_write").register, 699)
        with self.assertRaises(KeyError):
            inverter.get_capability("remote_switch")
        self.assertTrue(
            all(
                capability.tested
                for capability in inverter.capabilities
                if capability.key != "secondary_output_priority"
            )
        )
        self.assertTrue(
            all(
                can_expose_capability(
                    capability,
                    control_mode="auto",
                    detection_confidence="high",
                )
                for capability in inverter.capabilities
                if capability.key != "secondary_output_priority"
            )
        )
        self.assertEqual(inverter.details["device_type"], 32768)
        self.assertEqual(inverter.details["device_catalog"]["model_code"], 32768)
        self.assertEqual(inverter.details["protocol_number"], 4)
        self.assertNotIn("device_name", inverter.details)
        self.assertNotIn("program_version", inverter.details)
        self.assertNotIn("rated_cell_count", inverter.details)
        # max_discharge_current_protection (reg 642) is now part of the anenji
        # 11kw model catalog (a tested control decoded into details), so assert
        # its decoded value instead of its absence.
        self.assertEqual(inverter.details["max_discharge_current_protection"], 0)
        self.assertEqual(inverter.details["output_mode"], "Split-Phase-P1")

    async def test_issue_13_sandisolar_fingerprint_uses_protocol_4_schema(self) -> None:
        fixture = json.loads(
            (
                REPO_ROOT
                / "tests"
                / "fixtures"
                / "issue_13_sandisolar_sd_11kp48v_beta4.json"
            ).read_text(encoding="utf-8")
        )
        registers = self._anenji_registers()
        for captured_range in fixture["ranges"]:
            start = int(captured_range["start"])
            for offset, value in enumerate(captured_range["values"]):
                registers[start + offset] = int(value)

        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.model_name, "Sandisolar SD 11KP48V WIFI")
        self.assertEqual(inverter.variant_key, "sandisolar_sd_11kp48v_wifi")
        self.assertEqual(
            inverter.profile_name,
            "modbus_smg/models/sandisolar_sd_11kp48v_wifi.json",
        )
        self.assertEqual(
            inverter.register_schema_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(inverter.details["device_type"], 0x8003)
        self.assertEqual(inverter.details["protocol_number"], 4)
        self.assertTrue(all(not capability.tested for capability in inverter.capabilities))
        self.assertFalse(
            can_expose_capability(
                inverter.get_capability("output_source_priority"),
                control_mode="auto",
                detection_confidence="high",
            )
        )
        self.assertTrue(
            can_expose_capability(
                inverter.get_capability("output_source_priority"),
                control_mode="full",
                detection_confidence="high",
            )
        )

        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertEqual(values["inverter_frequency"], 50.01)
        self.assertEqual(values["inverter_temperature"], 35)
        self.assertNotEqual(values["inverter_temperature"], 5001)

    async def test_probe_selects_hhs_11kw_telemetry_without_anj_controls(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        registers = self._anenji_registers()
        registers[171] = 29440
        registers[184] = 3
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(
            inverter.variant_key,
            "anenji_hhs_11kw_wifi_no_parallel",
        )
        self.assertEqual(
            inverter.model_name,
            "Anenji HHS-11kW-WIFI (without parallel)",
        )
        self.assertEqual(inverter.profile_name, "")
        self.assertEqual(
            inverter.register_schema_name,
            "modbus_smg/models/anenji_hhs_11kw_wifi_no_parallel.json",
        )
        self.assertEqual(inverter.capabilities, ())
        self.assertEqual(inverter.capability_groups, ())
        self.assertEqual(inverter.details["device_catalog"]["tier"], "partial")
        self.assertEqual(inverter.details["device_type"], 29440)
        self.assertEqual(inverter.details["protocol_number"], 3)

    async def test_hhs_grid_power_uses_cloud_correlated_register_340(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        registers = self._anenji_registers()
        registers[171] = 29440
        registers[184] = 3
        registers[204] = 0
        registers[340] = 1729
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(
            inverter.register_schema_name,
            "modbus_smg/models/anenji_hhs_11kw_wifi_no_parallel.json",
        )
        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertEqual(values["grid_power"], 1729)

    async def test_probe_rejects_anenji_variant_when_variant_anchor_fields_are_invalid(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        registers = self._anenji_registers()
        registers[691] = 0
        registers[693] = 99
        registers[694] = 99
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        if inverter is not None:
            self.assertNotEqual(inverter.variant_key, "anenji_anj_11kw_48v_wifi_p")

    async def test_read_values_uses_variant_schema_mapping(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji ANJ-11KW-48V-WIFI-P",
            serial_number="ANJ11KW240001",
            probe_target=target,
            variant_key="anenji_anj_11kw_48v_wifi_p",
            profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            capabilities=(),
        )
        transport = FixtureTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["operating_mode"], "Off-Grid")
        self.assertEqual(values["grid_voltage"], 230.0)
        self.assertEqual(values["grid_frequency"], 50.0)
        self.assertEqual(values["output_voltage"], 229.8)
        self.assertEqual(values["output_power"], 4200)
        self.assertEqual(values["battery_voltage"], 51.2)
        self.assertEqual(values["battery_percent"], 78)
        self.assertEqual(values["pv_power"], 2400)
        self.assertEqual(values["pv_voltage"], 66.7)
        self.assertEqual(values["pv_current"], 0.1)
        self.assertEqual(values["pv1_voltage"], 64.9)
        self.assertEqual(values["pv1_current"], 0.1)
        self.assertEqual(values["pv1_power"], 7)
        self.assertEqual(values["pv2_voltage"], 66.7)
        self.assertEqual(values["pv2_current"], 0.0)
        self.assertEqual(values["pv2_power"], 0)
        self.assertEqual(values["input_mode"], "UPS")
        self.assertEqual(values["secondary_output_priority"], "OFF")
        self.assertEqual(values["parallel_pv_detection_mode"], 0)
        self.assertEqual(values["external_ct_enabled"], "Disabled")
        self.assertEqual(values["warning_mask_i"], 4294962687)
        self.assertEqual(values["dry_contact_mode"], "Normal Mode")
        self.assertEqual(values["automatic_mains_output_enabled"], "Disabled")
        self.assertEqual(values["pv_grid_connected_max_power"], 11000)
        self.assertEqual(values["island_detection_enabled"], "Disabled")
        self.assertEqual(values["turn_on_mode"], "Local and Remote")
        self.assertEqual(values["remote_switch"], "Remote Turn-On")
        self.assertEqual(values["inverter_date"], "2026-04-17")
        self.assertEqual(values["inverter_time"], "07:22:01")
        self.assertEqual(values["pv_generation_day"], 3.14)
        self.assertEqual(values["pv_generation_sum"], 123.45)
        self.assertEqual(values["ground_relay_enabled"], "Disabled")
        self.assertEqual(values["lithium_battery_activation_time"], 6)

    async def test_read_values_batch_optional_clock_registers_when_single_reads_are_zero(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji ANJ-11KW-48V-WIFI-P",
            serial_number="ANJ11KW240001",
            probe_target=target,
            variant_key="anenji_anj_11kw_48v_wifi_p",
            profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            capabilities=(),
        )

        class ClockBlockOnlyTransport(FixtureTransport):
            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                if 696 <= address <= 701 and count == 1:
                    response = bytearray([self._probe_target.device_addr, 0x03, 0x02])
                    response.extend((0).to_bytes(2, "big"))
                    response_crc = crc16_modbus(response)
                    response.extend(response_crc.to_bytes(2, "little"))
                    return bytes(response)
                return super()._handle_read_holding(payload)

        transport = ClockBlockOnlyTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["inverter_date"], "2026-04-17")
        self.assertEqual(values["inverter_time"], "07:22:01")

    async def test_write_capability_uses_inverter_capabilities(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji ANJ-11KW-48V-WIFI-P",
            serial_number="ANJ11KW240001",
            probe_target=target,
            variant_key="anenji_anj_11kw_48v_wifi_p",
            profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            register_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            capabilities=(),
        )
        transport = FixtureTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )

        with self.assertRaises(ValueError):
            await driver.async_write_capability(transport, inverter, "remote_switch", 1)

    async def test_write_u32_capability_updates_two_register_words(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        written = await driver.async_write_capability(transport, inverter, "warning_mask_i", 0x12345678)

        self.assertEqual(written, 0x12345678)
        self.assertEqual(transport._registers[687], 0x1234)
        self.assertEqual(transport._registers[688], 0x5678)

        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertEqual(values["warning_mask_i"], 0x12345678)

    async def test_documented_secondary_output_write_targets_register_602(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        capability = inverter.get_capability("secondary_output_priority")
        self.assertFalse(capability.tested)

        written = await driver.async_write_capability(
            transport,
            inverter,
            "secondary_output_priority",
            "SBU",
        )

        self.assertEqual(written, "SBU")
        self.assertEqual(transport._registers[602], 2)

    async def test_secondary_output_write_records_immediate_and_delayed_poll_readback(self) -> None:
        """Regression for issue #13.3: old -> requested may converge later."""

        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)

        class DeferredApplyTransport(FixtureTransport):
            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self._pending_write: dict[int, int] = {}
                self.full_poll_count = 0

            def _handle_write_multiple(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                old_values = {
                    address + offset: self._registers[address + offset]
                    for offset in range(count)
                }
                response = super()._handle_write_multiple(payload)
                self._pending_write = {
                    address + offset: self._registers[address + offset]
                    for offset in range(count)
                }
                self._registers.update(old_values)
                return response

            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                response = super()._handle_read_holding(payload)
                if (
                    self._pending_write
                    and count > 1
                    and address <= 602 < address + count
                ):
                    self.full_poll_count += 1
                    # The first full poll honestly returns the old value. Apply
                    # the pending setting only after that response so the next
                    # normal poll observes convergence.
                    self._registers.update(self._pending_write)
                return response

        transport = DeferredApplyTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None
        runtime_state: dict[str, object] = {}

        written = await driver.async_write_capability(
            transport,
            inverter,
            "secondary_output_priority",
            "SBU",
            runtime_state=runtime_state,
        )

        self.assertEqual(written, "SBU")
        immediate = write_confirmation_diagnostics(runtime_state)[
            WRITE_CONFIRMATION_DIAGNOSTIC_KEY
        ]
        self.assertEqual(immediate["requested_words"], [2])
        self.assertEqual(immediate["immediate_status"], "mismatched")
        self.assertEqual(immediate["immediate_value"], "OFF")
        # Detection evidence keeps the pre-write value; the requested value is
        # never projected optimistically through DetectedInverter.details.
        self.assertEqual(inverter.details["secondary_output_priority"], "OFF")

        first_poll = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
        )
        self.assertEqual(first_poll.values["secondary_output_priority"], "OFF")
        first_diagnostic = first_poll.diagnostics[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]
        self.assertEqual(first_diagnostic["first_full_poll_status"], "mismatched")
        self.assertEqual(
            first_diagnostic["convergence"],
            "requested_value_not_observed",
        )

        second_poll = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
        )
        self.assertEqual(second_poll.values["secondary_output_priority"], "SBU")
        second_diagnostic = second_poll.diagnostics[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]
        self.assertEqual(second_diagnostic["latest_full_poll_status"], "matched")
        self.assertEqual(second_diagnostic["latest_full_poll_value"], "SBU")
        self.assertEqual(second_diagnostic["full_poll_observation_count"], 2)
        self.assertEqual(
            second_diagnostic["convergence"],
            "requested_value_observed_after_mismatch",
        )

    async def test_immediate_read_failure_is_recorded_but_full_poll_can_confirm(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)

        class ImmediateReadFailsTransport(FixtureTransport):
            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self._write_completed = False
                self._failed_once = False

            def _handle_write_multiple(self, payload: bytes) -> bytes:
                response = super()._handle_write_multiple(payload)
                self._write_completed = True
                return response

            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                if (
                    self._write_completed
                    and not self._failed_once
                    and address == 602
                    and count == 1
                ):
                    self._failed_once = True
                    raise RuntimeError("simulated_exact_read_failure")
                return super()._handle_read_holding(payload)

        transport = ImmediateReadFailsTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)
        assert inverter is not None
        runtime_state: dict[str, object] = {}

        written = await driver.async_write_capability(
            transport,
            inverter,
            "secondary_output_priority",
            "SBU",
            runtime_state=runtime_state,
        )
        self.assertEqual(written, "SBU")
        immediate = write_confirmation_diagnostics(runtime_state)[
            WRITE_CONFIRMATION_DIAGNOSTIC_KEY
        ]
        self.assertEqual(immediate["immediate_status"], "unavailable")
        self.assertEqual(
            immediate["immediate_error"],
            "RuntimeError:simulated_exact_read_failure",
        )

        result = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=runtime_state,
        )
        diagnostic = result.diagnostics[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]
        self.assertEqual(diagnostic["latest_full_poll_status"], "matched")
        self.assertEqual(diagnostic["convergence"], "requested_value_observed")

    def _op2_inverter(self, target: ProbeTarget) -> DetectedInverter:
        from custom_components.eybond_local.metadata.profile_loader import load_driver_profile

        profile = load_driver_profile("modbus_smg/models/anenji_op2_6200.json")
        return DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Aninerel 6200 (dual output)",
            serial_number="99632600000002",
            probe_target=target,
            variant_key="default",
            profile_name="modbus_smg/models/anenji_op2_6200.json",
            register_schema_name="modbus_smg/models/anenji_op2_6200.json",
            capabilities=profile.capabilities,
        )

    async def test_write_bitmask_capability_preserves_other_register_bits(self) -> None:
        # OP2 enable is bit 0 of register 354; the other 15 bits belong to
        # unknown settings and MUST survive a write (read-modify-write).
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = self._op2_inverter(target)
        transport = FixtureTransport(
            registers={354: 0xABCE},
            command_responses=None,
            probe_target=target,
        )

        runtime_state: dict[str, object] = {}
        written = await driver.async_write_capability(
            transport,
            inverter,
            "output2_enable",
            True,
            runtime_state=runtime_state,
        )
        self.assertEqual(written, "On")
        self.assertEqual(transport._registers[354], 0xABCF)
        diagnostic = write_confirmation_diagnostics(runtime_state)[
            WRITE_CONFIRMATION_DIAGNOSTIC_KEY
        ]
        self.assertEqual(diagnostic["immediate_words"], [1])
        self.assertEqual(diagnostic["immediate_status"], "matched")

        written = await driver.async_write_capability(
            transport,
            inverter,
            "output2_enable",
            False,
            runtime_state=runtime_state,
        )
        self.assertEqual(written, "Off")
        self.assertEqual(transport._registers[354], 0xABCE)

    async def test_bitmask_pre_write_read_modbus_error_is_not_a_write_rejection(self) -> None:
        # A Modbus exception on the read-modify-write PRE-READ (reg 354) must
        # surface as CapabilityPreWriteReadError, not a Modbus write rejection —
        # otherwise the hub records a persistent 'unsupported_or_locked' blocker
        # for a control nothing was ever written to. No write is attempted.
        from custom_components.eybond_local.drivers.smg import (
            CapabilityPreWriteReadError,
        )
        from custom_components.eybond_local.payload.modbus import ModbusError

        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = self._op2_inverter(target)

        class _ReadFailSession:
            def __init__(self) -> None:
                self.writes: list[tuple[int, list[int]]] = []

            async def read_holding(self, register: int, count: int) -> list[int]:
                raise ModbusError("exception_code:7")

            async def write_holding(self, register: int, values: list[int]) -> None:
                self.writes.append((register, list(values)))

        session = _ReadFailSession()
        with patch.object(
            SmgModbusDriver, "_session", staticmethod(lambda *a, **k: session)
        ):
            with self.assertRaises(CapabilityPreWriteReadError):
                await driver.async_write_capability(
                    object(), inverter, "output2_enable", True
                )
        self.assertEqual(session.writes, [])

    def test_capability_read_back_extracts_bitmask_field(self) -> None:
        from custom_components.eybond_local.drivers.smg import _apply_capability_read_back
        from custom_components.eybond_local.metadata.profile_loader import load_driver_profile

        capability = load_driver_profile(
            "modbus_smg/models/anenji_op2_6200.json"
        ).get_capability("output2_enable")

        values: dict[str, object] = {}
        _apply_capability_read_back(values, (capability,), ((354, [0xABCF]),))
        self.assertEqual(values["output2_enable"], 1)

        values = {}
        _apply_capability_read_back(values, (capability,), ((354, [0xABCE]),))
        self.assertEqual(values["output2_enable"], 0)

    async def test_write_inverter_clock_capabilities_updates_date_and_time_words(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        written_date = await driver.async_write_capability(
            transport,
            inverter,
            "inverter_date_write",
            "2026-04-18",
        )
        written_time = await driver.async_write_capability(
            transport,
            inverter,
            "inverter_time_write",
            "08:09:10",
        )

        self.assertEqual(written_date, "2026-04-18")
        self.assertEqual(written_time, "08:09:10")
        self.assertEqual(transport._registers[696], 2026)
        self.assertEqual(transport._registers[697], 4)
        self.assertEqual(transport._registers[698], 18)
        self.assertEqual(transport._registers[699], 8)
        self.assertEqual(transport._registers[700], 9)
        self.assertEqual(transport._registers[701], 10)

        values = _full_values(await driver.async_read_values(transport, inverter))
        self.assertEqual(values["inverter_date"], "2026-04-18")
        self.assertEqual(values["inverter_time"], "08:09:10")

    async def test_force_eq_charge_action_writes_register_656(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._anenji_registers(),
            command_responses=None,
            probe_target=target,
        )
        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        runtime_state: dict[str, object] = {}
        written = await driver.async_write_capability(
            transport,
            inverter,
            "force_eq_charge",
            None,
            runtime_state=runtime_state,
        )

        self.assertEqual(written, 1)
        self.assertEqual(transport._registers[656], 1)
        self.assertEqual(write_confirmation_diagnostics(runtime_state), {})


class SmgFamilyFallbackTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)

    def _smg_family_registers(
        self,
        *,
        rated_power: int,
        device_type: int = 0x1E00,
        device_name_text: str | None = "SMG II 6200",
        program_version_text: str | None = "U1.00",
        rated_cell_count: int = 16,
    ) -> dict[int, int]:
        registers: dict[int, int] = {
            register: 0
            for start, stop in (
                (100, 110),
                (171, 185),
                (186, 198),
                (201, 235),
                (300, 344),
                (351, 352),
                (406, 407),
                (420, 421),
                (626, 645),
            )
            for register in range(start, stop)
        }
        if device_name_text is not None:
            for offset, value in _ascii_words(device_name_text, word_count=12).items():
                registers[172 + offset] = value
        for offset, value in _ascii_words("SMG11K240001", word_count=12).items():
            registers[186 + offset] = value
        if program_version_text is not None:
            for offset, value in _ascii_words(program_version_text, word_count=8).items():
                registers[626 + offset] = value

        registers.update(
            {
                171: device_type,
                184: 1,
                201: 3,
                202: 2300,
                203: 5000,
                204: 120,
                210: 2295,
                211: 12,
                212: 5000,
                213: 2500,
                215: 512,
                219: 650,
                220: 10,
                223: 800,
                225: 40,
                231: 97,
                300: 0,
                301: 1,
                302: 0,
                303: 3,
                305: 1,
                306: 1,
                307: 0,
                308: 1,
                309: 1,
                310: 0,
                313: 1,
                314: 0x1234,
                315: 0x5678,
                316: 1,
                320: 2300,
                321: 5000,
                322: 2,
                323: 620,
                324: 560,
                325: 540,
                326: 520,
                327: 480,
                329: 470,
                331: 1,
                332: 600,
                333: 200,
                334: 580,
                335: 60,
                336: 120,
                337: 30,
                338: 1,
                341: 25,
                342: 45,
                343: 15,
                351: 80,
                406: 0,
                420: 1,
                643: rated_power,
                644: rated_cell_count,
            }
        )
        return registers

    async def test_learned_overlay_scalar_has_typed_telemetry_parity(self) -> None:
        """An activated learned schema uses the normal driver telemetry path."""

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_name = "learned/shadow_learning/device/typed_overlay.json"
            schema_path = Path(temp_dir) / schema_name
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(
                json.dumps(
                    {
                        "extends": "builtin:modbus_smg/models/smg_6200.json",
                        "schema_key": "typed_learned_overlay",
                        "title": "Typed learned overlay",
                        "driver_key": "modbus_smg",
                        "protocol_family": "modbus_smg",
                        "spec_sets": {
                            "aux_config": [
                                {
                                    "key": "learned_read_404",
                                    "register": 404,
                                    "divisor": 10,
                                    "decimals": 1,
                                }
                            ]
                        },
                        "measurement_descriptions": [
                            {
                                "key": "learned_read_404",
                                "name": "Learned voltage",
                                "unit": "V",
                                "device_class": "voltage",
                                "state_class": "measurement",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            set_external_register_schema_roots((Path(temp_dir),))
            self.addCleanup(set_external_register_schema_roots, ())

            driver = SmgModbusDriver()
            target = ProbeTarget(
                devcode=0x0001,
                collector_addr=0xFF,
                device_addr=0x01,
            )
            registers = self._smg_family_registers(rated_power=6200)
            registers[404] = 567
            transport = FixtureTransport(
                registers=registers,
                command_responses=None,
                probe_target=target,
            )
            inverter = await driver.async_probe(transport, target)
            assert inverter is not None
            inverter.register_schema_name = schema_name

            result = await driver.async_read_values(transport, inverter)
            self.assertIs(type(result), DriverReadResult)
            self.assertIs(result.mode, DriverReadMode.FULL)
            self.assertEqual(result.values["learned_read_404"], 56.7)

            frame = fold_driver_telemetry(
                TypedTelemetryFrame.empty(),
                driver_key=inverter.driver_key,
                values=result.values,
                replace=True,
            )
            point = frame.point("learned_read_404")
            assert point is not None
            self.assertEqual(point.value, 56.7)
            self.assertIs(point.freshness, TelemetryFreshness.FRESH)

    async def test_probe_keeps_supported_6200_layout_on_default_variant(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._smg_family_registers(rated_power=6200),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "default")
        self.assertEqual(inverter.model_name, "SMG 6200")
        self.assertEqual(inverter.profile_name, "modbus_smg/models/smg_6200.json")
        self.assertEqual(inverter.register_schema_name, "modbus_smg/models/smg_6200.json")
        self.assertGreater(len(inverter.capabilities), 0)
        self.assertEqual(inverter.details["protocol_number"], 1)
        self.assertEqual(inverter.details["device_type"], 0x1E00)
        self.assertEqual(inverter.details["device_name"], "SMG II 6200")
        self.assertEqual(inverter.details["program_version"], "U1.00")
        self.assertEqual(inverter.details["rated_cell_count"], 16)
        self.assertEqual(inverter.details["max_discharge_current_protection"], 80)
        self.assertEqual(inverter.details["rated_power"], 6200)

    async def test_probe_detects_6200_despite_out_of_enum_settings_and_state(self) -> None:
        # Regression for the real incident: a leaked shadow-learning write pushed
        # output_source_priority out of its enum, and detection then rejected an obvious SMG
        # 6200 (-> no_supported_driver_matched, all entities unavailable). Identity must rest on
        # immutable anchors (rated_power), NEVER on user settings (output_source_priority,
        # output_mode) or runtime state (operating_mode). All three are forced out of range here.
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        registers = self._smg_family_registers(rated_power=6200)
        registers[301] = 3   # output_source_priority: enum only defines 0..2
        registers[300] = 7   # output_mode: enum only defines 0..4
        registers[201] = 99  # operating_mode: mode_names only defines 0..6
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "default")
        self.assertEqual(inverter.model_name, "SMG 6200")
        self.assertEqual(inverter.details["rated_power"], 6200)

    def test_capability_read_back_fills_learned_register_state(self) -> None:
        # Learned-overlay controls have a register but no decode spec, so without read-back the
        # switch toggles yet shows no state. The driver reads the raw register from the polled
        # blocks into values[value_key], WITHOUT touching register_schema_name (which would
        # break write-exposure for every control). Already-decoded keys are never overwritten,
        # and registers outside the polled blocks are skipped.
        values: dict[str, object] = {"buzzer_mode": "Beeps ON"}
        config = [0] * 44  # config block 300..343
        config[304 - 300] = 1  # Beeps -> on
        config[338 - 300] = 0  # Auto AC Output -> off
        caps = (
            SimpleNamespace(value_key="learned_beeps_304", key="learned_beeps_304", register=304),
            SimpleNamespace(value_key="learned_auto_338", key="learned_auto_338", register=338),
            SimpleNamespace(value_key="buzzer_mode", key="buzzer_mode", register=303),  # already decoded
            SimpleNamespace(value_key="learned_eq_999", key="learned_eq_999", register=999),  # not polled
        )

        _apply_capability_read_back(values, caps, ((300, config),))

        self.assertEqual(values["learned_beeps_304"], 1)
        self.assertEqual(values["learned_auto_338"], 0)
        self.assertEqual(values["buzzer_mode"], "Beeps ON")  # spec value not clobbered
        self.assertNotIn("learned_eq_999", values)  # outside polled blocks -> skipped

    def test_capability_read_back_scales_by_divisor(self) -> None:
        # A learned scaled number (divisor 10): raw register 560 -> native 56.0, so the number
        # entity shows the displayed value, consistent with its native_min/max and write encode.
        values: dict[str, object] = {}
        config = [0] * 44
        config[320 - 300] = 560
        caps = (
            SimpleNamespace(value_key="learned_v_320", key="learned_v_320", register=320, divisor=10),
        )
        _apply_capability_read_back(values, caps, ((300, config),))
        self.assertEqual(values["learned_v_320"], 56.0)

    async def test_read_out_of_block_capability_registers(self) -> None:
        # Out-of-block controls (Boot method 406, Output control 420) are read directly; in-block
        # controls and momentary actions are skipped.
        from custom_components.eybond_local.drivers.smg import (
            _read_out_of_block_capability_registers,
        )

        reads: list[int] = []

        class _Session:
            async def read_holding(self, register: int, count: int):
                reads.append(register)
                return [{406: 1, 420: 0}.get(register, 0)]

        caps = (
            SimpleNamespace(value_key="boot_406", key="boot_406", register=406, value_kind="enum"),
            SimpleNamespace(value_key="output_420", key="output_420", register=420, value_kind="bool"),
            SimpleNamespace(value_key="in_block_304", key="in_block_304", register=304, value_kind="bool"),
            SimpleNamespace(value_key="action_460", key="action_460", register=460, value_kind="action"),
        )
        extra = await _read_out_of_block_capability_registers(
            _Session(), caps, ((300, [0] * 44),)
        )

        self.assertEqual(sorted(reads), [406, 420])  # 304 in-block, 460 action -> not read
        self.assertIn((406, [1]), extra)
        self.assertIn((420, [0]), extra)

    async def test_probe_uses_bounded_identity_reads_before_selected_full_probe(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)

        class ProbeReadTrackingTransport(FixtureTransport):
            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self.read_requests: list[tuple[int, int]] = []

            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                self.read_requests.append((address, count))
                return super()._handle_read_holding(payload)

        transport = ProbeReadTrackingTransport(
            registers=self._smg_family_registers(rated_power=6200),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "default")
        # Required identity points are independent reads before the selected
        # full-schema probe; sparse 171..184 reads are not wire-compatible with
        # every confirmed SMG-family device.
        self.assertEqual(transport.read_requests[:2], [(171, 1), (184, 1)])
        self.assertIn((186, 12), transport.read_requests[:4])
        self.assertIn((643, 1), transport.read_requests)
        # Full-schema probing happens once, for the catalog-selected binding.
        self.assertIn((406, 1), transport.read_requests)
        self.assertIn((420, 1), transport.read_requests)
        self.assertNotIn((186, 46), transport.read_requests)
        self.assertNotIn((600, 57), transport.read_requests)
        self.assertNotIn((677, 18), transport.read_requests)

    async def test_probe_abstains_when_identity_window_is_unreadable(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)

        class IdentityFailureOnceTransport(FixtureTransport):
            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self.failed_identity_read = False
                self.read_requests: list[tuple[int, int]] = []

            def _handle_read_holding(self, payload: bytes) -> bytes:
                address = int.from_bytes(payload[2:4], "big")
                count = int.from_bytes(payload[4:6], "big")
                self.read_requests.append((address, count))
                if not self.failed_identity_read and address == 171 and count == 1:
                    self.failed_identity_read = True
                    raise RuntimeError("identity read failed")
                return super()._handle_read_holding(payload)

        transport = IdentityFailureOnceTransport(
            registers=self._smg_family_registers(rated_power=6200),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        self.assertIsNone(inverter)
        self.assertTrue(transport.failed_identity_read)
        self.assertNotIn((600, 57), transport.read_requests)
        self.assertNotIn((677, 18), transport.read_requests)

    async def test_probe_omits_placeholder_default_device_name(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        registers = self._smg_family_registers(rated_power=6200)
        registers[172] = 0x3030
        registers[173] = 0x3030
        for register in range(174, 184):
            registers[register] = 0
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.details["protocol_number"], 1)
        self.assertEqual(inverter.details["device_type"], 0x1E00)
        self.assertNotIn("device_name", inverter.details)
        self.assertEqual(inverter.details["program_version"], "U1.00")

    async def test_probe_selects_explicit_anenji_4200_protocol_1_variant(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._smg_family_registers(
                rated_power=4200,
                device_type=0x3501,
                device_name_text=None,
                program_version_text=None,
                rated_cell_count=2,
            ),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "anenji_4200_protocol_1")
        self.assertEqual(inverter.model_name, "Anenji 4200 (Protocol 1)")
        self.assertEqual(inverter.profile_name, "modbus_smg/models/anenji_4200_protocol_1.json")
        self.assertEqual(inverter.register_schema_name, "modbus_smg/models/anenji_4200_protocol_1.json")
        self.assertEqual(inverter.details["protocol_number"], 1)
        self.assertEqual(inverter.details["device_type"], 0x3501)
        self.assertEqual(inverter.details["rated_power"], 4200)
        self.assertNotIn("max_discharge_current_protection", inverter.details)
        self.assertEqual(len(inverter.capabilities), 30)
        self.assertEqual(len(inverter.capability_presets), 2)
        charge_source_priority = next(
            capability
            for capability in inverter.capabilities
            if capability.key == "charge_source_priority"
        )
        self.assertFalse(charge_source_priority.tested)
        self.assertNotIn(
            "low_dc_cutoff_soc",
            {capability.key for capability in inverter.capabilities},
        )

    async def test_read_values_exposes_documented_base_layout_config_diagnostics(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="SMG11K240001",
            probe_target=target,
            variant_key="default",
            profile_name="modbus_smg/models/smg_6200.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            details={
                "device_type": 0x1E00,
                "protocol_number": 1,
                "device_name": "SMG II 6200",
                "program_version": "U1.00",
                "max_discharge_current_protection": 80,
                "rated_cell_count": 16,
                "rated_power": 6200,
            },
            capabilities=(),
        )
        transport = FixtureTransport(
            registers=self._smg_family_registers(rated_power=6200),
            command_responses=None,
            probe_target=target,
        )

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["battery_type"], "User")
        self.assertEqual(values["power_flow_status"], 97)
        self.assertEqual(values["power_flow_pv_connection_state"], "Connected")
        self.assertEqual(values["power_flow_utility_connection_state"], "Disconnected")
        self.assertEqual(values["power_flow_battery_state"], "Discharging")
        self.assertEqual(values["power_flow_load_state"], "Active")
        self.assertEqual(values["power_flow_charge_source_state"], "Idle")
        self.assertEqual(values["warning_mask_i"], 0x12345678)
        self.assertEqual(values["dry_contact_mode"], "Grounding Box Mode")
        self.assertEqual(values["automatic_mains_output_enabled"], "Enabled")

    async def test_read_values_clamps_signed_idle_load_noise_to_zero(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="SMG11K240001",
            probe_target=target,
            variant_key="default",
            profile_name="modbus_smg/models/smg_6200.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            capabilities=(),
        )
        registers = self._smg_family_registers(rated_power=6200)
        registers[213] = 0xFFFD
        transport = FixtureTransport(
            registers=registers,
            command_responses=None,
            probe_target=target,
        )

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["output_power"], 0)

    async def test_read_values_backfills_missing_default_probe_details(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="SMG11K240001",
            probe_target=target,
            variant_key="default",
            profile_name="modbus_smg/models/smg_6200.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            details={
                "rated_power": 6200,
            },
            capabilities=(),
        )
        transport = FixtureTransport(
            registers=self._smg_family_registers(rated_power=6200),
            command_responses=None,
            probe_target=target,
        )

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["protocol_number"], 1)
        self.assertEqual(values["device_type"], 0x1E00)
        self.assertEqual(values["device_name"], "SMG II 6200")
        self.assertEqual(values["program_version"], "U1.00")
        self.assertEqual(values["rated_cell_count"], 16)
        self.assertEqual(values["max_discharge_current_protection"], 80)
        self.assertEqual(inverter.details["protocol_number"], 1)
        self.assertEqual(inverter.details["device_type"], 0x1E00)
        self.assertEqual(inverter.details["device_name"], "SMG II 6200")
        self.assertEqual(inverter.details["program_version"], "U1.00")
        self.assertEqual(inverter.details["rated_cell_count"], 16)
        self.assertEqual(inverter.details["max_discharge_current_protection"], 80)

    async def test_read_values_use_common_protocol_1_layout_for_anenji_4200_variant(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji 4200 (Protocol 1)",
            serial_number="99432409105281",
            probe_target=target,
            variant_key="anenji_4200_protocol_1",
            profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            details={
                "device_type": 0x3501,
                "protocol_number": 1,
                "rated_power": 4200,
            },
            capabilities=(),
        )
        transport = FixtureTransport(
            registers=self._smg_family_registers(
                rated_power=4200,
                device_type=0x3501,
                device_name_text=None,
                program_version_text=None,
                rated_cell_count=2,
            ),
            command_responses=None,
            probe_target=target,
        )

        values = _full_values(await driver.async_read_values(transport, inverter))

        self.assertEqual(values["protocol_number"], 1)
        self.assertEqual(values["device_type"], 0x3501)
        self.assertEqual(values["power_flow_status"], 97)
        self.assertEqual(values["power_flow_battery_state"], "Discharging")
        self.assertEqual(values["power_flow_load_state"], "Active")
        self.assertEqual(values["turn_on_mode"], "Local and Remote")
        self.assertEqual(values["remote_switch"], "Remote Turn-On")
        self.assertNotIn("max_discharge_current_protection", values)
        self.assertNotIn("max_discharge_current_protection", inverter.details)

    async def test_probe_falls_back_to_read_only_family_variant_for_unknown_smg_power_class(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
            registers=self._smg_family_registers(rated_power=11000),
            command_responses=None,
            probe_target=target,
        )

        inverter = await driver.async_probe(transport, target)

        assert inverter is not None
        self.assertEqual(inverter.variant_key, "family_fallback")
        self.assertEqual(inverter.model_name, "SMG Family (Unverified Variant)")
        # Family tier is structurally read-only: NO profile attaches at all.
        self.assertEqual(inverter.profile_name, "")
        self.assertEqual(inverter.register_schema_name, "modbus_smg/base.json")
        self.assertEqual(len(inverter.capabilities), 0)
        self.assertEqual(len(inverter.capability_presets), 0)
        self.assertEqual(len(inverter.capability_groups), 0)
        self.assertEqual(inverter.details["rated_power"], 11000)
        catalog_details = inverter.details["device_catalog"]
        self.assertEqual(catalog_details["kind"], "family")
        self.assertEqual(catalog_details["tier"], "partial")
        descriptor_decision = catalog_details["descriptor_decision"]
        self.assertEqual(descriptor_decision["kind"], "descriptor_decision_shadow")
        self.assertEqual(descriptor_decision["agreement"], "match")
        self.assertEqual(
            descriptor_decision["evaluation"]["resolved_key"],
            "modbus_smg.family_fallback",
        )
        self.assertEqual(
            inverter.details["descriptor_decision_shadow"],
            descriptor_decision,
        )
        self.assertEqual(
            descriptor_decision["selection"]["source"],
            "compiled_catalog_runtime_fallback",
        )
        self.assertTrue(descriptor_decision["selection"]["safe_switch_active"])


class DecodeUnavailableSentinelTests(unittest.TestCase):
    """An all-ones UNSIGNED register decodes as unavailable, not 65535."""

    def test_unsigned_all_ones_is_unavailable(self) -> None:
        specs = (RegisterValueSpec(key="output_power", register=10),)
        decoded = _decode_block(10, [0xFFFF], specs)
        self.assertIsNone(decoded["output_power"])

    def test_unsigned_all_ones_with_divisor_is_unavailable(self) -> None:
        specs = (RegisterValueSpec(key="grid_voltage", register=10, divisor=10),)
        decoded = _decode_block(10, [0xFFFF], specs)
        self.assertIsNone(decoded["grid_voltage"])

    def test_u32_all_ones_is_unavailable(self) -> None:
        specs = (
            RegisterValueSpec(
                key="energy", register=10, word_count=2, combine="u32_high_first"
            ),
        )
        decoded = _decode_block(10, [0xFFFF, 0xFFFF], specs)
        self.assertIsNone(decoded["energy"])

    def test_signed_minus_one_is_kept(self) -> None:
        # On a signed register 0xFFFF == -1 is a legitimate reading (e.g. a
        # small reverse current) and must NOT be treated as unavailable.
        specs = (RegisterValueSpec(key="battery_current", register=10, signed=True),)
        decoded = _decode_block(10, [0xFFFF], specs)
        self.assertEqual(decoded["battery_current"], -1)

    def test_smg_output_power_schema_decodes_negative_idle_noise_as_signed(self) -> None:
        schema = load_register_schema("modbus_smg/base.json")
        spec = next(item for item in schema.spec_set("live") if item.key == "output_power")

        decoded = _decode_block(213, [0xFFFD], (spec,))

        self.assertTrue(spec.signed)
        self.assertEqual(decoded["output_power"], -3)

    def test_normal_unsigned_value_is_unchanged(self) -> None:
        specs = (RegisterValueSpec(key="output_power", register=10),)
        decoded = _decode_block(10, [4200], specs)
        self.assertEqual(decoded["output_power"], 4200)


if __name__ == "__main__":
    unittest.main()
