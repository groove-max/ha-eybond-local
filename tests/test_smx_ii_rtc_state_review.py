"""PR #91: exact SMX-II state enum and raw RTC regression evidence.

The two samples were captured through read-only FC03 on physical SMX-II APP 6.67.
Neither establishes a valid calendar date (raw month byte is 22).
"""
from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema  # noqa: E402
from custom_components.eybond_local.models import RegisterValueSpec  # noqa: E402
from custom_components.eybond_local.payload.register_decode import decode_block  # noqa: E402

SCHEMA = "srne_modbus/smx_ii_24v.json"
HARDWARE_SAMPLES = (
    (0x5216, 0x1016, 0x3913),
    (0x5216, 0x1017, 0x0C0C),
)
RAW_WORD_KEYS = (
    "system_datetime_raw_020c",
    "system_datetime_raw_020d",
    "system_datetime_raw_020e",
)
BYTE_KEYS = (
    "system_datetime_year_byte",
    "system_datetime_month_byte",
    "system_datetime_day_byte",
    "system_datetime_hour_byte",
    "system_datetime_minute_byte",
    "system_datetime_second_byte",
)

class SmxIIReviewRegressions(unittest.TestCase):
    def test_machine_state_documented_values_and_unknown_numeric_code(self) -> None:
        schema = load_register_schema(SCHEMA)
        spec = next(s for s in schema.spec_set("runtime") if s.key == "inverter_operation_mode")
        self.assertEqual(spec.register, 0x0210)
        self.assertEqual(
            {k: spec.enum_map[k] for k in (8, 9, 10)},
            {8: "Battery activation", 9: "Manual shutdown", 10: "Fault"},
        )
        self.assertNotIn(11, spec.enum_map)
        for code, expected in (
            (5, "Running on inverter"),
            (8, "Battery activation"),
            (9, "Manual shutdown"),
            (10, "Fault"),
            (11, "Unknown (11)"),
            (42, "Unknown (42)"),
        ):
            with self.subTest(code=code):
                self.assertEqual(decode_block(528, [code], (spec,))[spec.key], expected)

    def test_real_clock_captures_remain_raw_and_datetime_unavailable(self) -> None:
        schema = load_register_schema(SCHEMA)
        specs = tuple(s for s in schema.spec_set("runtime") if 524 <= s.register <= 526)
        self.assertEqual(len(specs), 10)
        for words, raw_fields in zip(
            HARDWARE_SAMPLES,
            ((0x52, 0x16, 0x10, 0x16, 0x39, 0x13),
             (0x52, 0x16, 0x10, 0x17, 0x0C, 0x0C)),
        ):
            with self.subTest(words=words):
                values = decode_block(524, list(words), specs)
                self.assertIsNone(values["system_datetime"])
                self.assertEqual(tuple(values[k] for k in RAW_WORD_KEYS), words)
                self.assertEqual(tuple(values[k] for k in BYTE_KEYS), raw_fields)
                self.assertEqual(values["system_datetime_month_byte"], 22)

    def test_synthetic_calendar_remains_unavailable_for_exact_model(self) -> None:
        schema = load_register_schema(SCHEMA)
        spec = next(s for s in schema.spec_set("runtime") if s.key == "system_datetime")
        self.assertIsNone(decode_block(524, [0x380A, 0x0312, 0x0C22], (spec,))["system_datetime"])

    def test_legacy_generic_datetime_decoder_unchanged(self) -> None:
        synthetic = RegisterValueSpec(key="generic", register=524, word_count=3, combine="srne_datetime")
        self.assertEqual(
            decode_block(524, [0x3808, 0x160C, 0x2238], (synthetic,))["generic"],
            "2026-09-22 12:34:56",
        )

    def test_raw_fields_are_disabled_diagnostics(self) -> None:
        schema = load_register_schema(SCHEMA)
        descriptions = {s.key: s for s in schema.measurement_descriptions}
        for key in RAW_WORD_KEYS + BYTE_KEYS:
            with self.subTest(key=key):
                self.assertTrue(descriptions[key].diagnostic)
                self.assertFalse(descriptions[key].enabled_default)
        self.assertIn("system_datetime", descriptions)

if __name__ == "__main__":
    unittest.main()
