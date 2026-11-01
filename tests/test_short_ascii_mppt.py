"""MPPT semantic ownership without live session admission or wire guessing."""

from dataclasses import FrozenInstanceError, asdict, replace
import io
import json
from pathlib import Path
from types import SimpleNamespace
import struct
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.protocol import decode_header
from custom_components.eybond_local.collector.transport.binary_framing import (
    BinaryFrame, BinaryFrameDecoder, BinaryFramingError, BinaryGrammar,
)
from custom_components.eybond_local.payload.short_ascii_mppt import (
    MpptFault, MpptWorkMode, parse_mppt_runtime,
)
from tools.decode_short_ascii_mppt import decode_assumed_runtime, main


def runtime_frame(*, subtype=0x0200, voltage=1200, power=37, battery=512,
                  temperature=278, load=31, mode=1, daily=23, total=420, fault=0):
    body = struct.pack(">HHHHHHBHHB", subtype, voltage, power, battery,
                       temperature, load, mode, daily, total, fault)
    return BinaryFrame(BinaryGrammar.AABB, b"\xaa\xbb" + body + bytes([sum(body) & 255]))


class MpptRuntimeTests(unittest.TestCase):
    def test_documented_offsets_scales_and_separate_owners(self):
        value = parse_mppt_runtime(runtime_frame())
        self.assertEqual(asdict(value), {
            "pv_voltage_v": 120.0, "pv_power_w": 370,
            "mppt_battery_voltage_v": 51.2, "mppt_temperature_c": 27.8,
            "dc_load_current_a": 3.1, "work_mode_code": 1,
            "daily_energy_kwh": 2.3, "total_energy_kwh": 42.0, "fault_code": 0,
        })
        self.assertEqual(value.work_mode, MpptWorkMode.TRACKING)
        self.assertIsNone(value.fault)
        self.assertTrue(set(asdict(value)).isdisjoint({
            "battery_voltage", "bms_total_voltage", "battery_reference_voltage",
            "temperature", "load_active_power", "pv_current", "output_current",
        }))

    def test_sample_is_immutable(self):
        sample = parse_mppt_runtime(runtime_frame())
        with self.assertRaises(FrozenInstanceError):
            sample.pv_power_w = 0

    def test_every_documented_mode_and_fault(self):
        for mode in MpptWorkMode:
            self.assertEqual(parse_mppt_runtime(runtime_frame(mode=mode)).work_mode, mode)
        for fault in MpptFault:
            self.assertEqual(parse_mppt_runtime(runtime_frame(fault=fault)).fault, fault)

    def test_unknown_enums_preserve_codes_without_guessing_or_bitfield_decode(self):
        for code in (0, 4, 128, 255):
            value = parse_mppt_runtime(runtime_frame(mode=code, fault=code))
            self.assertEqual((value.work_mode_code, value.fault_code), (code, code))
            self.assertIsNone(value.work_mode)
            self.assertIsNone(value.fault)

    def test_zero_and_max_unsigned_values_are_wire_values_not_availability_claims(self):
        for raw in (0, 65535):
            sample = parse_mppt_runtime(runtime_frame(
                voltage=raw, power=raw, battery=raw, temperature=raw,
                load=raw, daily=raw, total=raw,
            ))
            self.assertEqual(sample.pv_power_w, raw * 10)
            self.assertEqual(sample.mppt_temperature_c, raw / 10)
            self.assertEqual(sample.mppt_battery_voltage_v, raw / 10)
            self.assertEqual(sample.total_energy_kwh, raw / 10)
            self.assertFalse(hasattr(sample, "available"))

    def test_settings_and_unknown_subtypes_are_not_runtime(self):
        for subtype in (0x0202, 0x0201, 0xFFFF):
            with self.subTest(subtype=subtype), self.assertRaises(ValueError):
                parse_mppt_runtime(runtime_frame(subtype=subtype))

    def test_no_prefix_suffix_truncation_or_chunk_concatenation_accepted(self):
        frame = runtime_frame()
        for wire in [frame.wire[:n] for n in range(21)] + [
            b"\x00" + frame.wire, frame.wire + b"\r", frame.wire * 2,
        ]:
            with self.subTest(length=len(wire)), self.assertRaises(ValueError):
                parse_mppt_runtime(replace(frame, wire=wire))

    def test_every_single_byte_corruption_is_rejected(self):
        frame = runtime_frame()
        for index in range(21):
            wire = bytearray(frame.wire)
            wire[index] ^= 1
            with self.subTest(index=index), self.assertRaises(ValueError):
                parse_mppt_runtime(replace(frame, wire=bytes(wire)))

    def test_explicit_frame_contract_required_even_for_valid_bytes(self):
        frame = runtime_frame()
        for value in (frame.wire, None, replace(frame, wire=bytearray(frame.wire)),
                      replace(frame, grammar=BinaryGrammar.MIXED),
                      replace(frame, grammar=BinaryGrammar.EYBOND),
                      replace(frame, header=decode_header(frame.wire[:8]))):
            with self.subTest(value=type(value).__name__), self.assertRaisesRegex(
                ValueError, "mppt_frame_contract_invalid",
            ):
                parse_mppt_runtime(value)

    def test_semantics_do_not_relax_ambiguous_transport_grammar(self):
        # PV ~113.1 V / 20 W: legal EyeBond header AND AABB/0200.
        frame = runtime_frame(voltage=1131, power=2)
        self.assertEqual(parse_mppt_runtime(frame).pv_voltage_v, 113.1)
        self.assertEqual(parse_mppt_runtime(frame).pv_power_w, 20)
        # Offline explicit AABB assumption can inspect the sample. Without a
        # claim, MIXED must still fail-close — checksum is not ownership.
        decoder = BinaryFrameDecoder(BinaryGrammar.MIXED, started_at=0, timeout=1)
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            decoder.feed(frame.wire, now=0)
        # Matching outstanding claim selects AABB; bad checksum still rejects.
        owned = BinaryFrameDecoder(
            BinaryGrammar.MIXED, started_at=0, timeout=1,
            auxiliary_claim=SimpleNamespace(subtype=b"\x02\x00"),
        )
        self.assertEqual(owned.feed(frame.wire, now=0), 21)
        self.assertEqual(
            parse_mppt_runtime(owned.finish(now=0)).pv_power_w, 20,
        )
        bad = frame.wire[:-1] + bytes([frame.wire[-1] ^ 1])
        bad_decoder = BinaryFrameDecoder(
            BinaryGrammar.MIXED, started_at=0, timeout=1,
            auxiliary_claim=SimpleNamespace(subtype=b"\x02\x00"),
        )
        with self.assertRaisesRegex(BinaryFramingError, "aabb_checksum_invalid"):
            bad_decoder.feed(bad, now=0)
        # 0202 claim must not accept a 0200 colliding reply as its answer.
        mismatched = BinaryFrameDecoder(
            BinaryGrammar.MIXED, started_at=0, timeout=1,
            auxiliary_claim=SimpleNamespace(subtype=b"\x02\x02"),
        )
        with self.assertRaisesRegex(BinaryFramingError, "binary_frame_ambiguous"):
            mismatched.feed(frame.wire, now=0)
        with self.assertRaises(ValueError):
            parse_mppt_runtime(runtime_frame(subtype=0x0202))

    def test_all_tcp_splits_of_explicit_aabb_frame_keep_semantics(self):
        frame = runtime_frame()
        for split in range(22):
            decoder = BinaryFrameDecoder(BinaryGrammar.AABB, started_at=0, timeout=1)
            decoder.feed(frame.wire[:split], now=0)
            decoder.feed(frame.wire[split:], now=0)
            self.assertEqual(parse_mppt_runtime(decoder.finish(now=0)), parse_mppt_runtime(frame))


class MpptOfflineToolTests(unittest.TestCase):
    def test_report_labels_assumption_and_overlap_without_live_admission(self):
        for power in (4, 37):
            report = decode_assumed_runtime(runtime_frame(voltage=1085, power=power).wire.hex())
            self.assertFalse(report["live_session_admitted"])
            self.assertEqual(report["wire_format_assumption"], "aabb_runtime_0200")
            self.assertEqual(report["semantic_schema"], "19b4_segment4")
            self.assertEqual(report["eybond_header_overlap"], power == 4)
            self.assertEqual(report["eybond_claimed_total_length"], 1091 if power == 4 else None)
            self.assertEqual(report["sample"]["pv_power_w"], power * 10)

    def test_cli_decodes_offline_without_network(self):
        with patch("sys.stdout", new_callable=io.StringIO) as output, patch(
            "socket.socket", side_effect=AssertionError("unexpected network access"),
        ):
            self.assertEqual(main(["--wire-format", "aabb-runtime", "--frame-hex", runtime_frame().wire.hex()]), 0)
        self.assertEqual(json.loads(output.getvalue())["sample"]["pv_power_w"], 370)

    def test_cli_errors_are_safe_and_do_not_echo_input(self):
        for value in ("sensitive-not-hex", "ab" * 100, runtime_frame(subtype=0x0202).wire.hex()):
            with patch("sys.stdout", new_callable=io.StringIO) as output:
                self.assertEqual(main(["--wire-format", "aabb-runtime", "--frame-hex", value]), 1)
            self.assertEqual(set(json.loads(output.getvalue())), {"error"})
            self.assertNotIn(value, output.getvalue())

    def test_cli_requires_explicit_format_assumption(self):
        with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit) as error:
            main(["--frame-hex", runtime_frame().wire.hex()])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
