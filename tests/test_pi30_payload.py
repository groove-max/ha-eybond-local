from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.payload.pi30 import (
    Pi30Error,
    build_request,
    crc16_xmodem,
    parse_energy_counter,
    parse_firmware_version,
    parse_protocol_id,
    parse_q1,
    parse_qflag,
    parse_qpigs,
    parse_qpiri,
    parse_qpiws,
    parse_qt_clock,
    parse_qmod,
    parse_response,
    parse_serial_number,
)


def _frame(payload: str) -> bytes:
    body = f"({payload}".encode("ascii")
    crc = crc16_xmodem(body)
    high = (crc >> 8) & 0xFF
    low = crc & 0xFF
    if high in {0x28, 0x0D, 0x0A}:
        high += 1
    if low in {0x28, 0x0D, 0x0A}:
        low += 1
    return body + bytes((high, low)) + b"\r"


class Pi30PayloadTests(unittest.TestCase):
    def test_build_request_appends_crc_and_cr(self) -> None:
        self.assertEqual(build_request("QPI"), b"QPI\xbe\xac\r")

    def test_parse_response_decodes_ascii_payload(self) -> None:
        self.assertEqual(parse_response(_frame("PI30")), "PI30")

    def test_parse_response_rejects_bad_crc(self) -> None:
        with self.assertRaises(Pi30Error):
            parse_response(b"(PI30\x00\x00\r")

    def test_parse_qpiri_decodes_base_layout(self) -> None:
        values = parse_qpiri(
            "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1"
        )

        self.assertEqual(values["output_rating_active_power"], 4200)
        self.assertEqual(values["battery_type_code"], 2)
        self.assertEqual(values["output_source_priority_code"], 2)
        self.assertEqual(values["battery_redischarge_voltage"], 27.0)

    def test_parse_qpiri_accepts_decimal_encoded_integer_fields(self) -> None:
        values = parse_qpiri(
            "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 23.0 22.0 27.8 27.7 2 030 060 1 1 2 1 01 0 0 27.0 0 1 23.0 10 22.0"
        )

        self.assertEqual(values["battery_type_code"], 2)
        self.assertEqual(values["max_ac_charging_current"], 30)
        self.assertEqual(values["max_charging_time_cv_stage"], 23)
        self.assertEqual(values["operation_logic_code"], 10)
        self.assertEqual(values["max_discharging_current"], 22)

    def test_parse_qpigs_decodes_live_layout_and_derives_pv_power(self) -> None:
        values = parse_qpigs(
            "239.5 49.9 239.5 49.9 0927 0924 015 396 26.60 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 000"
        )

        self.assertEqual(values["output_active_power"], 924)
        self.assertEqual(values["battery_voltage"], 26.6)
        self.assertEqual(values["pv_input_current"], 2.2)
        self.assertEqual(values["pv_input_power"], 695.0)

    def test_parse_protocol_and_identity_fields(self) -> None:
        self.assertEqual(parse_protocol_id("PI30"), {"protocol_id": "PI30"})
        self.assertEqual(
            parse_serial_number("553555355535552"),
            {"serial_number": "553555355535552"},
        )

    def test_parse_firmware_version(self) -> None:
        self.assertEqual(
            parse_firmware_version("00012.09", key="main_cpu_firmware_version"),
            {"main_cpu_firmware_version": "00012.09"},
        )

    def test_parse_qflag_decodes_enabled_and_disabled_capabilities(self) -> None:
        values = parse_qflag("EazDbjkuvxy")

        self.assertTrue(values["buzzer_enabled"])
        self.assertTrue(values["record_fault_code_enabled"])
        self.assertFalse(values["overload_bypass_enabled"])
        self.assertEqual(values["capability_flags_enabled"], "az")
        self.assertEqual(values["capability_flags_disabled"], "bjkuvxy")

    def test_parse_qpiws_decodes_alarm_bits(self) -> None:
        values = parse_qpiws("00000100000000000000000000000000")

        self.assertTrue(values["alarm_active"])
        self.assertEqual(values["alarm_bits_raw"], "00000100000000000000000000000000")

    def test_parse_q1_decodes_extended_layout(self) -> None:
        values = parse_q1(
            "00001 16971 01 00 00 026 033 022 029 02 00 000 0036 0000 0000 49.95 10 0 060 030 100 030 58.40 000 120 0 0000"
        )

        self.assertTrue(values["scc_flag"])
        self.assertFalse(values["allow_scc_on_flag"])
        self.assertEqual(values["tracker_temperature"], 26)
        self.assertEqual(values["fan_speed"], 36)
        self.assertEqual(values["sync_frequency"], 49.95)
        self.assertEqual(values["inverter_charge_state_code"], 10)

    def test_parse_energy_counter_and_qt_clock(self) -> None:
        self.assertEqual(parse_energy_counter("12345", key="pv_generation_sum"), {"pv_generation_sum": 12345})
        self.assertEqual(parse_qt_clock("20260407113059"), {"clock_token": "20260407"})

    def test_parse_qmod_returns_mode_code(self) -> None:
        self.assertEqual(
            parse_qmod("L"),
            {"operating_mode_code": "L"},
        )


if __name__ == "__main__":
    unittest.main()