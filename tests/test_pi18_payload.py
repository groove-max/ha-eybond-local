from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.payload.pi18 import (
    Pi18Error,
    build_request,
    parse_current_time,
    parse_energy_counter,
    parse_firmware_versions,
    parse_protocol_id,
    parse_qflag,
    parse_qfws,
    parse_qmod,
    parse_qpigs,
    parse_qpiri,
    parse_response,
    parse_serial_number,
)
from custom_components.eybond_local.payload.pi30 import crc16_xmodem


def _frame(payload: str) -> bytes:
    body = f"^D{len(payload) + 3:03d}{payload}".encode("ascii")
    crc = crc16_xmodem(body)
    return body + bytes(((crc >> 8) & 0xFF, crc & 0xFF)) + b"\r"


class Pi18PayloadTests(unittest.TestCase):
    def test_build_request_appends_crc_and_cr(self) -> None:
        request = build_request("^P005PI")
        self.assertTrue(request.endswith(b"\r"))
        self.assertEqual(request[:-3], b"^P005PI")

    def test_parse_response_decodes_pi18_payload(self) -> None:
        self.assertEqual(parse_response(_frame("18")), "18")

    def test_parse_response_rejects_bad_crc(self) -> None:
        with self.assertRaises(Pi18Error):
            parse_response(b"^D00518\x00\x00\r")

    def test_parse_probe_fields(self) -> None:
        self.assertEqual(parse_protocol_id("18")["protocol_id"], "PI18")
        self.assertEqual(
            parse_serial_number("1401234567890123456789"),
            {"serial_number": "01234567890123"},
        )
        versions = parse_firmware_versions("00001,00002,00003")
        self.assertEqual(versions["main_cpu_firmware_version"], "00001")

    def test_parse_qpiri_decodes_rated_fields(self) -> None:
        values = parse_qpiri("2200,190,2200,500,190,5000,5000,480,540,500,420,564,540,2,30,80,0,1,2,1,0,1,0,1,2")
        self.assertEqual(values["output_rating_active_power"], 5000)
        self.assertEqual(values["battery_type_code"], 2)
        self.assertEqual(values["solar_power_priority_code"], 1)
        self.assertEqual(values["qpiri_field_count"], 25)

    def test_parse_qpigs_decodes_live_fields(self) -> None:
        values = parse_qpigs("2301,500,2300,500,4500,4200,80,520,515,514,20,30,90,35,33,34,1200,800,3200,3100,1,2,1,1,2,2,1,0")
        self.assertEqual(values["output_active_power"], 4200)
        self.assertEqual(values["battery_voltage"], 52.0)
        self.assertEqual(values["pv_input_power"], 2000)
        self.assertEqual(values["qpigs_field_count"], 28)

    def test_parse_qmod_qflag_qfws(self) -> None:
        self.assertEqual(parse_qmod("05"), {"operating_mode_code": 5})
        flags = parse_qflag("1,1,0,0,0,1,1,1,0")
        self.assertTrue(flags["buzzer_enabled"])
        warnings = parse_qfws("00,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
        self.assertEqual(warnings["fault_code"], 0)
        self.assertTrue(warnings["line_fail_warning"])

    def test_parse_energy_and_time(self) -> None:
        self.assertEqual(parse_energy_counter("00001234", key="pv_generation_sum"), {"pv_generation_sum": 1234})
        self.assertEqual(parse_current_time("20160214201314"), {"clock_token": "20160214"})


if __name__ == "__main__":
    unittest.main()