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
    ADMIT_OPTION_KEY, RUNTIME_QUERY_0200, assert_runtime_query_only, values_from_reply,
)
from custom_components.eybond_local.drivers.command_support import unsupported_commands
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.metadata.register_schema_loader import (
    clear_register_schema_loader_cache,
    load_register_schema,
)
from custom_components.eybond_local.models import ProbeTarget


MPPT_SCHEMA_KEYS = (
    "pv_voltage",
    "pv_power",
    "mppt_battery_voltage",
    "mppt_temperature",
    "dc_load_current",
    "mppt_work_mode_code",
    "mppt_daily_energy",
    "mppt_total_energy",
    "mppt_error_code",
    "mppt_error",
)


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
        self.assertEqual(values["mppt_error"], "normal")
        for key in ("battery_voltage", "temperature", "load_power", "aabb_last_wire"):
            self.assertNotIn(key, values)
        for key in MPPT_SCHEMA_KEYS:
            self.assertIn(key, values)

    def test_settings_0202_reply_is_not_live_telemetry(self):
        with self.assertRaises(ValueError):
            values_from_reply(runtime_frame(subtype=0x0202).wire)


class MpptSchemaAdmissionTests(unittest.TestCase):
    def setUp(self):
        clear_register_schema_loader_cache()
        self.driver = EybondShortAsciiDriver()
        self.schema = load_register_schema(self.driver.register_schema_name)

    def test_mppt_keys_present_and_opt_in(self):
        keys = {item.key for item in self.schema.measurement_descriptions}
        quiet_faults = {"mppt_error_code", "mppt_error"}
        for key in MPPT_SCHEMA_KEYS:
            with self.subTest(key=key):
                self.assertIn(key, keys)
                description = self.schema.measurement_description(key)
                if key in quiet_faults:
                    self.assertTrue(description.enabled_default)
                else:
                    self.assertFalse(description.enabled_default)

    def test_mppt_units_device_classes_and_quiet_error(self):
        expected = {
            "pv_voltage": ("V", "voltage", "measurement", False, False),
            "pv_power": ("W", "power", "measurement", False, False),
            "mppt_battery_voltage": ("V", "voltage", "measurement", False, False),
            "mppt_temperature": ("°C", "temperature", "measurement", False, False),
            "dc_load_current": ("A", "current", "measurement", False, False),
            "mppt_work_mode_code": (None, None, None, True, False),
            "mppt_daily_energy": ("kWh", "energy", "total_increasing", False, False),
            "mppt_total_energy": ("kWh", "energy", "total_increasing", False, False),
            "mppt_error_code": (None, None, None, True, True),
            "mppt_error": (None, None, None, True, True),
        }
        for key, (unit, device_class, state_class, diagnostic, enabled) in expected.items():
            with self.subTest(key=key):
                description = self.schema.measurement_description(key)
                self.assertEqual(description.unit, unit)
                self.assertEqual(description.device_class, device_class)
                self.assertEqual(description.state_class, state_class)
                self.assertEqual(description.diagnostic, diagnostic)
                self.assertEqual(description.enabled_default, enabled)

        # Distinct owners: names must not collide with BMS / AC / inverter labels.
        self.assertEqual(
            self.schema.measurement_description("mppt_battery_voltage").name,
            "MPPT Battery Voltage",
        )
        self.assertEqual(
            self.schema.measurement_description("dc_load_current").name,
            "MPPT DC Load Current",
        )
        self.assertEqual(
            self.schema.measurement_description("mppt_temperature").name,
            "MPPT Temperature",
        )
        self.assertNotEqual(
            self.schema.measurement_description("mppt_battery_voltage").name,
            self.schema.measurement_description("bms_total_voltage").name,
        )
        self.assertNotEqual(
            self.schema.measurement_description("mppt_temperature").name,
            self.schema.measurement_description("temperature").name,
        )


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
        self.state = {ADMIT_OPTION_KEY: True}
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
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        mppt.next_due = 0
        self.transport.aux_requests.clear()

    async def test_stock_install_does_not_poll_0200_without_admission(self):
        self.state.pop(ADMIT_OPTION_KEY, None)
        for now in (0, 1, 2, 3, 4):
            result = await self.read(now)
        self.assertEqual(self.transport.aux_requests, [])
        self.assertNotIn("pv_power", result.values)
        self.assertIn("MPPT=not_admitted", result.diagnostics["short_ascii_optional_status"])
        self.assertEqual(result.diagnostics.get("short_ascii_mppt_admission"), ADMIT_OPTION_KEY)

    async def test_solicits_documented_0200_on_stale_rb_collision(self):
        """Admitted MPPT: first stale RB collision solicits documented 0200."""
        result = await self.read(0)
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertNotIn(_settings_query(), self.transport.aux_requests)
        self.assertEqual(result.values["pv_voltage"], 120.0)
        self.assertEqual(result.values["pv_power"], 370)
        self.assertIn("MPPT=ok", result.diagnostics["short_ascii_optional_status"])

    async def test_ttl_expiry_clears_mppt_without_tip_harvest_fields(self):
        await self._prime_through_rh()
        # Pin schedule so expiry is observable without anti-starve re-polling.
        for sample in self.state[STATE_KEY].samples:
            sample.next_due = 10_000
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        if mppt.sampled_at is None:
            mppt.values = {"pv_voltage": 120.0, "pv_power": 370}
            mppt.sampled_at = 0.0
            mppt.outcome = "ok"
        expired = await self.read(64)
        self.assertNotIn("pv_voltage", expired.values)
        self.assertNotIn("pv_power", expired.values)
        self.assertNotIn("short_ascii_mppt_age_seconds", expired.diagnostics)
        self.assertIn("MPPT=expired", expired.diagnostics["short_ascii_optional_status"])
        for key in expired.values:
            self.assertFalse(key.startswith("aabb_last_"))

    async def test_never_sends_settings_0202_even_when_aux_configured(self):
        self.transport.aux_responses[_settings_query()] = runtime_frame(subtype=0x0202).wire
        result = await self.read(0)
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertNotIn(_settings_query(), self.transport.aux_requests)
        self.assertEqual(result.values["pv_power"], 370)

    async def test_no_tip_harvest_or_waiter_state(self):
        result = await self.read(0)
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
        # New binding rebuilds optional state; stale MPPT wins first RB collision.
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertIn("pv_voltage", result.values)

    async def test_disconnect_during_mppt_does_not_wipe_q1(self):
        """MPPT fence must not raise from refresh_one / wipe Q1 extras."""
        await self._prime_through_rh()
        # Keep prior FC4 samples so we can assert they survive the MPPT miss.
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        self.transport.connected = False
        result = await self.read(3)
        self.assertEqual(result.values["grid_voltage"], 230.0)
        self.assertNotIn("pv_power", result.values)
        self.assertIn("MPPT=timeout", result.diagnostics["short_ascii_optional_status"])
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        self.assertAlmostEqual(mppt.next_due, 3.0, delta=1.0)
        self.assertEqual(unsupported_commands(self.state), ())
        self.assertEqual(result.diagnostics.get("driver_unsupported_commands"), "")
        # One attempt only — disconnected skips the in-cycle retry.
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])

    async def test_mppt_connection_error_does_not_wipe_fc4_or_q1(self):
        """Aux ConnectionError (fence) soft-fails like TimeoutError — keep Q1/FC4."""
        await self._prime_through_rh()
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000
        # Timeout then fence on retry — both soft; prior FC4/Q1 must survive.
        self.transport.aux_responses[RUNTIME_QUERY_0200] = [
            asyncio.TimeoutError(),
            ConnectionError("collector_session_changed"),
        ]
        result = await self.read(3)
        self.assertEqual(result.values["grid_voltage"], 230.0)
        self.assertEqual(result.values["battery_soc"], 80)
        self.assertEqual(result.values["short_ascii_rated_voltage"], 115)
        self.assertNotIn("pv_power", result.values)
        self.assertIn("MPPT=timeout", result.diagnostics["short_ascii_optional_status"])
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        self.assertAlmostEqual(mppt.next_due, 3.0, delta=1.0)
        self.assertEqual(unsupported_commands(self.state), ())
        self.assertEqual(result.diagnostics.get("driver_unsupported_commands"), "")
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200, RUNTIME_QUERY_0200])
        # Direct fence while down: still soft, one attempt, no wipe.
        self.transport.aux_requests.clear()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = ConnectionError(
            "collector_not_connected",
        )
        self.transport.connected = False
        fenced = await self.read(4)
        self.assertEqual(fenced.values["grid_voltage"], 230.0)
        self.assertEqual(fenced.values["battery_soc"], 80)
        self.assertEqual(fenced.values["short_ascii_rated_voltage"], 115)
        self.assertNotIn("pv_power", fenced.values)
        self.assertIn("MPPT=timeout", fenced.diagnostics["short_ascii_optional_status"])
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertEqual(unsupported_commands(self.state), ())

    async def test_mppt_timeout_retries_once_then_succeeds(self):
        await self._prime_through_rh()
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000
        self.transport.aux_responses[RUNTIME_QUERY_0200] = [
            asyncio.TimeoutError(),
            runtime_frame().wire,
        ]
        result = await self.read(3)
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200, RUNTIME_QUERY_0200])
        self.assertEqual(result.values["pv_power"], 370)
        self.assertIn("MPPT=ok", result.diagnostics["short_ascii_optional_status"])
        self.assertEqual(unsupported_commands(self.state), ())
        self.assertEqual(result.diagnostics.get("driver_unsupported_commands"), "")

    async def test_mppt_double_timeout_due_immediately_without_penalty(self):
        await self._prime_through_rh()
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        result = await self.read(3)
        self.assertNotIn("pv_power", result.values)
        self.assertIn("MPPT=timeout", result.diagnostics["short_ascii_optional_status"])
        # First failure + one in-cycle retry — both timeout.
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200, RUNTIME_QUERY_0200])
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        self.assertAlmostEqual(mppt.next_due, 3.0, delta=1.0)
        self.assertLess(mppt.next_due, 3.0 + 15.0)
        self.assertEqual(unsupported_commands(self.state), ())
        # Next poll can succeed immediately (no +30 s backoff).
        self.transport.aux_requests.clear()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = runtime_frame().wire
        recovered = await self.read(4)
        self.assertEqual(recovered.values["pv_power"], 370)
        self.assertIn("MPPT=ok", recovered.diagnostics["short_ascii_optional_status"])
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])

    async def test_mppt_timeouts_never_blacklist_as_unsupported(self):
        """Aux 0200 contention must not persist short_ascii:MPPT forever."""
        await self._prime_through_rh()
        # Hold FC4 samples fresh so the ≤1 optional/cycle slot stays on MPPT.
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        for now in (3, 4, 5, 6, 7):
            result = await self.read(now)
            self.assertNotIn("pv_power", result.values)
            self.assertIn("MPPT=timeout", result.diagnostics["short_ascii_optional_status"])
        self.assertEqual(unsupported_commands(self.state), ())
        self.assertEqual(result.diagnostics.get("driver_unsupported_commands"), "")
        self.transport.aux_responses[RUNTIME_QUERY_0200] = runtime_frame().wire
        recovered = await self.read(8)
        self.assertEqual(recovered.values["pv_power"], 370)
        self.assertIn("MPPT=ok", recovered.diagnostics["short_ascii_optional_status"])


    async def test_prefer_fc4_when_mppt_also_due(self):
        await self._prime_through_rh()
        # Fresh within TTL → prefer FC4 (anti-starve idle).
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        mppt.sampled_at = 2.0
        mppt.values = {"pv_power": 370}
        mppt.outcome = "ok"
        for sample in self.state[STATE_KEY].samples:
            sample.next_due = 3
        self.transport.requests.clear()
        self.transport.aux_requests.clear()
        result = await self.read(3)
        self.assertIn(b"RB\x01\r", self.transport.requests)
        self.assertEqual(self.transport.aux_requests, [])
        self.assertIn("battery_soc", result.values)

    async def test_stale_rb_collision_takes_mppt(self):
        """First RB+MPPT collision while MPPT is stale solicits 0200."""
        await self._prime_through_rh()
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        mppt.clear()
        mppt.outcome = "not_checked"
        for sample in self.state[STATE_KEY].samples:
            sample.next_due = 3
        self.transport.requests.clear()
        self.transport.aux_requests.clear()
        result = await self.read(3)
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertNotIn(b"RB\x01\r", self.transport.requests)
        self.assertEqual(result.values["pv_power"], 370)

    async def test_structural_typeerror_backs_off_without_retry(self):
        await self._prime_through_rh()
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000
        self.transport.aux_responses[RUNTIME_QUERY_0200] = TypeError(
            "unsupported_auxiliary_transport:SharedCollectorAtTransport",
        )
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        mppt.next_due = 0
        self.transport.aux_requests.clear()
        result = await self.read(3)
        self.assertNotIn("pv_power", result.values)
        self.assertIn("MPPT=invalid_response", result.diagnostics["short_ascii_optional_status"])
        # One wire attempt only — no in-cycle retry for structural TypeError.
        self.assertEqual(self.transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertAlmostEqual(mppt.next_due, 3.0 + 120.0, delta=1.0)


class MpptSupportCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_capture_includes_correlated_0200_request_reply(self):
        driver = EybondShortAsciiDriver()
        transport = _Transport(
            _responses(),
            aux_responses={RUNTIME_QUERY_0200: runtime_frame().wire},
        )
        inverter = await driver.async_probe(transport, ProbeTarget(767, 255, 1))
        transport.aux_requests.clear()
        stock = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(transport.aux_requests, [])
        self.assertNotIn("0200", stock["responses_hex"])
        evidence = await driver.async_capture_support_evidence(
            transport,
            inverter,
            runtime_state={ADMIT_OPTION_KEY: True},
        )
        self.assertEqual(evidence["capture_kind"], "short_ascii_read_only")
        self.assertEqual(transport.aux_requests, [RUNTIME_QUERY_0200])
        self.assertEqual(evidence["responses_hex"]["0200_request"], RUNTIME_QUERY_0200.hex())
        self.assertEqual(evidence["responses_hex"]["0200"], runtime_frame().wire.hex())


if __name__ == "__main__":
    unittest.main()
