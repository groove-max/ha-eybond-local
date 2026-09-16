"""Optional MPPT aux solicitation: documented 0200 only, OptionalSample TTL."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_mppt import runtime_frame
from test_short_ascii_optional import _rb, _rh
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_mppt_optional import (
    RUNTIME_QUERY_0200, assert_runtime_query_only, values_from_reply,
)
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.models import ProbeTarget


def _settings_query() -> bytes:
    return b"\x5a\xa5\x02\x02" + bytes(16) + b"\x04"


class MpptOptionalHelpersTests(unittest.TestCase):
    def test_only_documented_0200_query_is_accepted(self):
        assert_runtime_query_only(RUNTIME_QUERY_0200)
        self.assertEqual(len(RUNTIME_QUERY_0200), 21)
        with self.assertRaises(ValueError):
            assert_runtime_query_only(_settings_query())
        with self.assertRaises(ValueError):
            assert_runtime_query_only(b"")

    def test_decode_maps_runtime_fields_for_later_schema_keys(self):
        values = values_from_reply(runtime_frame().wire)
        self.assertEqual(values["pv_voltage"], 120.0)
        self.assertEqual(values["pv_power"], 370)
        self.assertEqual(values["mppt_battery_voltage"], 51.2)
        self.assertEqual(values["mppt_temperature"], 27.8)
        self.assertEqual(values["dc_load_current"], 3.1)
        self.assertEqual(values["mppt_work_mode_code"], 1)
        self.assertEqual(values["mppt_daily_energy"], 2.3)
        self.assertEqual(values["mppt_total_energy"], 42.0)
        self.assertEqual(values["mppt_error_code"], 0)
        for key in ("battery_voltage", "temperature", "load_power", "aabb_last_wire"):
            self.assertNotIn(key, values)

    def test_settings_0202_reply_is_not_live_telemetry(self):
        with self.assertRaises(ValueError):
            values_from_reply(runtime_frame(subtype=0x0202).wire)


class MpptOptionalReadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {
            "RB": _rb(), "F": b"#115.0 105 48.00 60.0\r", "RH": _rh(accuracy=1),
        }
        self.transport = _Transport(
            responses, aux_responses={RUNTIME_QUERY_0200: runtime_frame().wire},
        )
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(767, 255, 1))
        self.state = {}
        self.transport.requests.clear()
        self.transport.aux_requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def _prime_through_rh(self):
        await self.read(0)
        await self.read(1)
        await self.read(2)
        self.transport.aux_requests.clear()

    async def test_solicits_documented_0200_only_after_rb_f_rh_slot(self):
        await self._prime_through_rh()
        result = await self.read(3)
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertNotIn(_settings_query(), self.transport.aux_requests)
        self.assertEqual(result.values["pv_voltage"], 120.0)
        self.assertEqual(result.values["pv_power"], 370)
        self.assertIn("MPPT=ok", result.diagnostics["short_ascii_optional_status"])

    async def test_ttl_expiry_clears_mppt_without_tip_harvest_fields(self):
        await self._prime_through_rh()
        await self.read(3)
        expired = await self.read(64)
        self.assertNotIn("pv_voltage", expired.values)
        self.assertNotIn("pv_power", expired.values)
        self.assertNotIn("short_ascii_mppt_age_seconds", expired.diagnostics)
        self.assertIn("MPPT=expired", expired.diagnostics["short_ascii_optional_status"])
        for key in expired.values:
            self.assertFalse(key.startswith("aabb_last_"))

    async def test_never_sends_settings_0202_even_when_aux_configured(self):
        self.transport.aux_responses[_settings_query()] = runtime_frame(subtype=0x0202).wire
        await self._prime_through_rh()
        await self.read(3)
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])

    async def test_no_tip_harvest_or_waiter_state(self):
        await self._prime_through_rh()
        result = await self.read(3)
        reads = self.state[STATE_KEY]
        self.assertFalse(hasattr(reads, "aabb_last_wire"))
        self.assertFalse(hasattr(reads, "aabb_waiter"))
        for key in result.values:
            self.assertFalse(key.startswith("aabb_last_"))
        self.assertFalse(any("harvest" in key for key in result.diagnostics))

    async def test_mandatory_failure_clears_mppt_sample(self):
        await self._prime_through_rh()
        await self.read(3)
        self.assertIn("pv_voltage", (await self.read(4)).values)
        self.transport.responses["Q1"] = b"NAK\r"
        with self.assertRaises(Exception):
            await self.read(5)
        self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))

    async def test_binding_change_drops_mppt_without_reuse(self):
        await self._prime_through_rh()
        await self.read(3)
        self.transport.requests.clear()
        self.transport.aux_requests.clear()
        self.inverter = replace(self.inverter)
        result = await self.read(4)
        self.assertNotIn("pv_voltage", result.values)
        # New binding restarts optional schedule at FC4 RB; no MPPT aux yet.
        self.assertIn(b"RB\x01\r", self.transport.requests)
        self.assertEqual(self.transport.aux_requests, [])

    async def test_disconnect_during_mppt_is_link_failure_and_clears(self):
        await self._prime_through_rh()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        self.transport.connected = False
        with self.assertRaises(ConnectionError):
            await self.read(3)
        self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))


if __name__ == "__main__":
    unittest.main()
