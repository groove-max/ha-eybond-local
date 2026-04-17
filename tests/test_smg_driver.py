from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.smg import (  # noqa: E402
    SmgModbusDriver,
    _support_capture_ranges,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.metadata.register_schema_loader import (  # noqa: E402
    set_external_register_schema_roots,
)
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget  # noqa: E402


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
                (186, 12),
                (201, 34),
                (277, 5),
                (300, 54),
                (389, 3),
                (406, 1),
                (420, 1),
                (607, 1),
                (643, 1),
                (696, 8),
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
                (696, 8),
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
            serial_number="92632511100118",
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
                "Includes supplemental SMG family discovery ranges for 11K-like variants: 277-281, 338-353, 389-391, 607, 696-703.",
            ],
        )
        self.assertEqual(
            [(item["start"], item["count"]) for item in evidence["planned_ranges"]],
            list(expected_ranges),
        )
        self.assertEqual(evidence["range_failures"], [])

        captured_by_start = {item["start"]: item for item in evidence["captured_ranges"]}
        self.assertEqual(captured_by_start[300]["count"], 54)
        self.assertEqual(captured_by_start[607]["words"], [607])
        self.assertEqual(captured_by_start[696]["count"], 8)
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
            for start, stop in ((100, 110), (198, 232), (600, 657))
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
                171: 1,
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
                351: 3600,
                352: 67,
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
                693: 0,
                694: 1,
            }
        )
        return registers

    async def test_probe_selects_anenji_variant_and_read_only_profile(self) -> None:
        driver = SmgModbusDriver()
        target = ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01)
        transport = FixtureTransport(
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
        self.assertEqual(inverter.capabilities, ())
        self.assertEqual(inverter.details["protocol_number"], 4)
        self.assertEqual(inverter.details["output_mode"], "Split-Phase-P1")

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

        values = await driver.async_read_values(transport, inverter)

        self.assertEqual(values["operating_mode"], "Off-Grid")
        self.assertEqual(values["grid_voltage"], 230.0)
        self.assertEqual(values["grid_frequency"], 50.0)
        self.assertEqual(values["output_voltage"], 229.8)
        self.assertEqual(values["output_power"], 4200)
        self.assertEqual(values["battery_voltage"], 51.2)
        self.assertEqual(values["battery_percent"], 78)
        self.assertEqual(values["pv_power"], 2400)
        self.assertEqual(values["turn_on_mode"], "Local and Remote")
        self.assertEqual(values["remote_switch"], "Remote Turn-On")

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


if __name__ == "__main__":
    unittest.main()