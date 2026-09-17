"""Quiet Q1/MPPT fault diagnostics (entity publish, no WARNING spam)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_mppt import runtime_frame
from test_short_ascii_optional import _rb
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_mppt_optional import values_from_reply
from custom_components.eybond_local.metadata.register_schema_loader import (
    clear_register_schema_loader_cache, load_register_schema,
)
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.short_ascii import (
    Q1_ERROR_LABELS, parse_q1, q1_error_label,
)
from custom_components.eybond_local.payload.short_ascii_mppt import (
    MPPT_ERROR_LABELS, mppt_error_label,
)


FAULT_MEASUREMENT_KEYS = ("q1_error_code", "q1_error", "mppt_error_code", "mppt_error")
HEX_CAPTURE_KEYS = ("q1_raw_hex", "rb_raw_hex", "rb_tail_hex", "rb_body_len")


class FaultLabelTests(unittest.TestCase):
    def test_q1_labels_cover_19b4_table_including_excess_temperature(self):
        self.assertEqual(q1_error_label(0), "normal")
        self.assertEqual(q1_error_label(8), "excess temperature")
        self.assertEqual(Q1_ERROR_LABELS[8], "excess temperature")
        self.assertEqual(q1_error_label(42), "unknown(42)")

    def test_mppt_labels_cover_19b4_table(self):
        self.assertEqual(mppt_error_label(0), "normal")
        self.assertEqual(
            mppt_error_label(1), "MPPT internal temperature is too high",
        )
        self.assertEqual(MPPT_ERROR_LABELS[1], "MPPT internal temperature is too high")
        self.assertEqual(mppt_error_label(4), "unknown(4)")

    def test_mppt_optional_publishes_quiet_label(self):
        values = values_from_reply(runtime_frame(fault=0).wire)
        self.assertEqual(values["mppt_error_code"], 0)
        self.assertEqual(values["mppt_error"], "normal")
        values = values_from_reply(runtime_frame(fault=1).wire)
        self.assertEqual(values["mppt_error_code"], 1)
        self.assertEqual(
            values["mppt_error"], "MPPT internal temperature is too high",
        )


class FaultSchemaTests(unittest.TestCase):
    def setUp(self):
        clear_register_schema_loader_cache()
        self.schema = load_register_schema(EybondShortAsciiDriver().register_schema_name)

    def test_quiet_fault_diagnostics_default_on(self):
        for key in FAULT_MEASUREMENT_KEYS:
            with self.subTest(key=key):
                description = self.schema.measurement_description(key)
                self.assertTrue(description.diagnostic)
                self.assertTrue(description.enabled_default)
        ups = self.schema.binary_sensor_description("ups_fault")
        self.assertTrue(ups.diagnostic)
        self.assertTrue(ups.enabled_default)
        self.assertEqual(ups.device_class, "problem")
        # Other diagnostics stay opt-in unless separately enabled.
        flags = self.schema.measurement_description("short_ascii_status_flags")
        self.assertTrue(flags.diagnostic)
        self.assertFalse(flags.enabled_default)
        keys = {item.key for item in self.schema.measurement_descriptions}
        binary = {item.key for item in self.schema.binary_sensor_descriptions}
        self.assertNotIn("short_ascii_fault_code", keys)
        self.assertNotIn("inverter_fault", binary)


class FaultPublishDriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.driver = EybondShortAsciiDriver()
        self.target = ProbeTarget(0x02FF, 255, 1)

    async def test_successful_q1_publishes_faults_without_warning_or_hex(self):
        transport = _Transport()
        inverter = await self.driver.async_probe(transport, self.target)
        transport.requests.clear()
        log_name = "custom_components.eybond_local.drivers"
        with self.assertNoLogs(log_name, level="WARNING"):
            result = await self.driver.async_read_values(
                transport, inverter, runtime_state={},
            )
        self.assertEqual(result.values["q1_error_code"], 0)
        self.assertEqual(result.values["q1_error"], "normal")
        self.assertIs(result.values["ups_fault"], False)
        for key in HEX_CAPTURE_KEYS:
            self.assertNotIn(key, result.values)

    async def test_rb_ok_keeps_hex_out_of_values(self):
        responses = _responses()
        responses["RB"] = _rb()
        transport = _Transport(responses)
        inverter = await self.driver.async_probe(transport, self.target)
        log_name = "custom_components.eybond_local.drivers"
        with self.assertNoLogs(log_name, level="WARNING"):
            result = await self.driver.async_read_values(
                transport, inverter, runtime_state={},
            )
        for key in HEX_CAPTURE_KEYS:
            self.assertNotIn(key, result.values)
        self.assertNotIn("aabb_raw_hex", result.values)

    async def test_failed_q1_does_not_emit_fault_capture(self):
        transport = _Transport()
        inverter = await self.driver.async_probe(transport, self.target)
        transport.responses["Q1"] = b"NAK\r"
        log_name = "custom_components.eybond_local.drivers"
        with self.assertNoLogs(log_name, level="WARNING"):
            with self.assertRaises(Exception):
                await self.driver.async_read_values(transport, inverter, runtime_state={})


if __name__ == "__main__":
    unittest.main()
