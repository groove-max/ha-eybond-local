"""Optional read qualification and freshness, with synthetic device data."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from custom_components.eybond_local.drivers.command_support import (
    clear_unsupported_commands, seed_unsupported_commands, unsupported_commands,
)
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.short_ascii_optional import OptionalReads, OptionalSample, STATE_KEY
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.short_ascii import ShortAsciiError, parse_f, parse_rb, parse_rh


def _rb(*, voltage=520, soc=80, charge_raw=30, discharge_raw=0):
    body = bytearray(37)
    for offset, value in ((0, voltage), (3, charge_raw), (5, discharge_raw), (7, 250), (9, 13),
                          (11, 300), (13, 270), (15, 335), (17, 330), (19, 448), (21, 560)):
        body[offset:offset + 2] = value.to_bytes(2, "big")
    body[2], body[23], body[24] = soc, 1, 1
    return _rb_body(body)


def _rb_body(body):
    return b"\x01" + bytes(body) + bytes([sum(body) & 255]) + b"\r"


def _rh(*, accuracy=1):
    """30-byte RH settings frame (live proxy + 19B4 segment-5); accuracy at body[8]."""
    body = bytearray(27)
    body[8] = accuracy
    return b"\x01" + bytes(body) + bytes([sum(body) & 255]) + b"\r"


class OptionalPayloadTests(unittest.TestCase):
    def test_f_has_ratings_not_instantaneous_power(self):
        self.assertEqual(parse_f(b"#115.0 105 48.00 60.0\r"), {
            "short_ascii_rated_voltage": 115, "short_ascii_rated_current": 105,
            "short_ascii_rated_battery_voltage": 48, "short_ascii_rated_frequency": 60,
        })
        for wire in (b"115.0 105 48.00 60.0\r", b"#115.0 105 48.00 60.0\r\n",
                     b"#115.0_105 48.00 60.0\r", b"#115.0 105   nan 60.0\r"):
            with self.assertRaises(ShortAsciiError):
                parse_f(wire)

    def test_rb_documented_current_scale_without_inventing_battery_power_in_parser(self):
        values = parse_rb(_rb())
        self.assertEqual(values["bms_total_voltage"], 52)
        self.assertEqual(values["battery_soc"], 80)
        # 19B4 multiply=0.1 — raw 30 / 10 = 3.0 A charge, 0 discharge.
        self.assertEqual(values["bms_charging_current"], 3.0)
        self.assertEqual(values["bms_discharging_current"], 0.0)
        self.assertEqual(values["bms_cell_temperature"], 25)
        self.assertEqual(values["bms_cycle_count"], 13)
        self.assertEqual(values["bms_internal_temperature"], 30)
        self.assertEqual(values["bms_mos_temperature"], 27)
        self.assertEqual(values["bms_max_cell_voltage"], 3.35)
        self.assertEqual(values["bms_min_cell_voltage"], 3.30)
        self.assertEqual(values["bms_low_protection_voltage"], 44.8)
        self.assertEqual(values["bms_charge_cutoff_voltage"], 56)
        # Measured DC watts are derived outside parse_rb (additive module).
        for key in ("battery_voltage", "battery_current", "battery_power", "load_power"):
            self.assertNotIn(key, values)
        self.assertIs(values["short_ascii_bms_charge_path_enabled"], True)

    def test_rh_parses_display_accuracy_and_rejects_corrupt_frames(self):
        self.assertEqual(parse_rh(_rh(accuracy=1)), {
            "short_ascii_bms_current_display_accuracy": 1,
        })
        self.assertEqual(parse_rh(_rh(accuracy=0)), {
            "short_ascii_bms_current_display_accuracy": 0,
        })
        # Live proxy capture (RH=1, remaining settings zero).
        live = bytes.fromhex(
            "01000000000000000001000000000000000000000000000000000000010d"
        )
        self.assertEqual(parse_rh(live)["short_ascii_bms_current_display_accuracy"], 1)
        for wire in (_rh()[:-1], _rh() + b"\r", b"", b"NAK\r", bytearray(_rh())):
            with self.assertRaises(ShortAsciiError):
                parse_rh(wire)
        bad_enum = bytearray(_rh())
        bad_enum[9] = 2
        bad_enum[-2] = sum(bad_enum[1:-2]) & 255
        with self.assertRaises(ShortAsciiError):
            parse_rh(bytes(bad_enum))
        wire = bytearray(_rh())
        wire[-2] ^= 1
        with self.assertRaises(ShortAsciiError):
            parse_rh(bytes(wire))

    def test_rb_current_scale_matches_documented_decimal_fields(self):
        values = parse_rb(_rb(charge_raw=294, discharge_raw=1510))
        self.assertEqual(values["bms_charging_current"], 29.4)
        self.assertEqual(values["bms_discharging_current"], 151.0)

    def test_rb_missing_primary_measurements_withdraws_even_nonzero_trailing_fields(self):
        self.assertEqual(parse_rb(_rb(voltage=0, soc=0)), {"short_ascii_bms_data_available": False})
        self.assertEqual(parse_rb(_rb(voltage=520, soc=0))["battery_soc"], 0)
        with self.assertRaises(ShortAsciiError):
            parse_rb(_rb(voltage=0, soc=10))

    def test_rb_checksum_is_eight_bit_and_protects_each_byte(self):
        wire = _rb()
        self.assertGreater(sum(wire[1:-2]), 255)
        for index in range(len(wire)):
            altered = bytearray(wire)
            altered[index] ^= 1
            with self.subTest(index=index), self.assertRaises(ShortAsciiError):
                parse_rb(bytes(altered))

    def test_rb_invalid_shape_padding_soc_or_flags_never_publishes_partial_values(self):
        for wire in (_rb()[:-1], _rb() + b"\r", b"", b"NAK\r", bytearray(_rb())):
            with self.assertRaises(ShortAsciiError):
                parse_rb(wire)
        for offset, value in ((2, 101), (23, 2), (24, 255), (25, 1), (36, 1)):
            body = bytearray(_rb()[1:-2]); body[offset] = value
            with self.assertRaises(ShortAsciiError):
                parse_rb(_rb_body(body))

    def test_rb_internal_cr_is_not_a_packet_boundary(self):
        self.assertIn(b"\r", _rb()[:-1])
        self.assertEqual(parse_rb(_rb())["bms_cycle_count"], 13)

    def test_sample_ttl_is_exclusive_and_never_extends_on_read(self):
        for now, expected in ((10, {"value": 5}), (69.999, {"value": 5}), (70, {}), (9, {})):
            sample = OptionalSample("RB", 30, 60, parse_rb, sampled_at=10, values={"value": 5})
            self.assertEqual(sample.fresh_values(now), expected)
        sample = OptionalSample("RB", 30, 60, parse_rb, sampled_at=10, values={"value": 5})
        values = sample.fresh_values(11); values["value"] = 6
        self.assertEqual(sample.fresh_values(69), {"value": 5})
        self.assertEqual(sample.fresh_values(70), {})


class OptionalReadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {
            "RB": _rb(), "F": b"#115.0 105 48.00 60.0\r", "RH": _rh(accuracy=1),
        }
        self.transport = _Transport(responses)
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(767, 255, 1))
        self.state = {}
        self.transport.requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def test_one_optional_query_per_cycle_fair_scheduling_and_full_snapshot(self):
        first = await self.read(0)
        self.assertEqual(first.mode, DriverReadMode.FULL)
        self.assertEqual(first.values["bms_total_voltage"], 52)
        self.assertEqual(first.values["battery_reference_voltage"], 13.2)
        self.assertNotIn("short_ascii_rated_voltage", first.values)
        # Currents withheld until RH=1 and F (F2 / T5.S3.W).
        self.assertNotIn("bms_charging_current", first.values)
        second = await self.read(1)
        self.assertEqual(second.values["short_ascii_rated_voltage"], 115)
        self.assertEqual(second.values["battery_soc"], 80)
        third = await self.read(2)
        self.assertEqual(third.values["battery_soc"], 80)
        # RH is gate-only: accuracy must not appear as a sensor key.
        self.assertNotIn("short_ascii_bms_current_display_accuracy", third.values)
        fourth = await self.read(3)
        self.assertEqual(fourth.values["battery_soc"], 80)
        self.assertEqual(
            self.transport.requests,
            [
                b"Q1\x01\r", b"RB\x01\r",
                b"Q1\x01\r", b"F\x01\r",
                b"Q1\x01\r", b"RH\x01\r",
                b"Q1\x01\r",
            ],
        )
        self.assertGreater(fourth.diagnostics["short_ascii_rb_age_seconds"], 2.9)
        self.assertLess(fourth.diagnostics["short_ascii_rb_age_seconds"], 3.1)

    async def test_rb_timeout_invalidates_immediately_but_retains_current_q1_and_f(self):
        await self.read(0); await self.read(1); await self.read(2)
        self.transport.responses["RB"] = asyncio.TimeoutError()
        result = await self.read(31)
        self.assertNotIn("battery_soc", result.values)
        self.assertNotIn("bms_total_voltage", result.values)
        self.assertNotIn("short_ascii_bms_data_available", result.values)
        self.assertEqual(result.values["grid_voltage"], 230)
        self.assertEqual(result.values["short_ascii_rated_voltage"], 115)
        self.assertIn("RB=timeout", result.diagnostics["short_ascii_optional_status"])
        self.assertNotIn("short_ascii_rb_age_seconds", result.diagnostics)
        self.assertNotIn("battery_soc", (await self.read(32)).values)

    async def test_link_loss_holds_last_good_then_recovers_to_zero_soc(self):
        await self.read(0); await self.read(1); await self.read(2)
        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        held = await self.read(31)
        self.assertEqual(held.values["battery_soc"], 80)
        self.assertEqual(held.values["bms_total_voltage"], 52)
        self.assertTrue(held.values["short_ascii_bms_data_available"])
        self.assertIn("RB=link_loss_hold", held.diagnostics["short_ascii_optional_status"])
        self.assertGreater(held.diagnostics["short_ascii_rb_age_seconds"], 30)
        self.transport.responses["RB"] = _rb(soc=0)
        recovered = await self.read(62)
        self.assertEqual(recovered.values["battery_soc"], 0)
        self.assertTrue(recovered.values["short_ascii_bms_data_available"])

    async def test_missing_rb_does_not_block_f_and_repeated_failures_use_existing_recheck(self):
        self.transport.responses["RB"] = b"NAK\r"
        for now in (0, 1, 31, 62, 93, 124):
            result = await self.read(now)
            self.assertEqual(result.values["grid_voltage"], 230)
        self.assertEqual(self.transport.requests.count(b"RB\x01\r"), 4)
        self.assertEqual(self.transport.requests.count(b"F\x01\r"), 1)
        self.assertEqual(self.transport.requests.count(b"RH\x01\r"), 1)
        self.assertEqual(unsupported_commands(self.state), ("short_ascii:RB",))
        self.assertIn("short_ascii:RB", result.diagnostics["driver_unsupported_commands"])
        # One skipped cycle so outcome becomes unsupported; clear then re-enable.
        await self.read(125)
        clear_unsupported_commands(self.state)
        self.transport.responses["RB"] = _rb()
        self.assertEqual((await self.read(126)).values["battery_soc"], 80)
        self.assertEqual(unsupported_commands(self.state), ())

    async def test_mandatory_failure_or_cancellation_clears_samples_without_negative_strikes(self):
        for failure in (b"NAK\r", asyncio.TimeoutError(), asyncio.CancelledError()):
            await self.asyncSetUp()
            await self.read(0); await self.read(1)
            self.transport.responses["Q1"] = failure
            with self.assertRaises((ShortAsciiError, asyncio.TimeoutError, asyncio.CancelledError)):
                await self.read(2)
            self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))
            self.assertEqual(unsupported_commands(self.state), ())
            self.transport.responses["Q1"] = _responses()["Q1"]
            self.transport.responses["RB"] = b"NAK\r"
            self.assertNotIn("battery_soc", (await self.read(3)).values)

    async def test_optional_cancellation_or_connection_loss_does_not_charge_an_unsupported_strike(self):
        for failure in (asyncio.CancelledError(), ConnectionError()):
            await self.asyncSetUp()
            await self.read(0); await self.read(1); await self.read(2)
            self.transport.responses["RB"] = failure
            with self.assertRaises(type(failure)):
                await self.read(31)
            self.assertEqual(unsupported_commands(self.state), ())
            self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))

    async def test_runtime_state_rebinding_and_clock_rollback_never_reuse_samples(self):
        for change in ("transport", "inverter", "clock"):
            await self.asyncSetUp()
            await self.read(10); await self.read(11)
            if change == "transport":
                self.transport = _Transport(_responses() | {"RB": b"NAK\r"})
            elif change == "inverter":
                self.inverter = replace(self.inverter)
            self.transport.responses["RB"] = b"NAK\r"
            result = await self.read(0 if change == "clock" else 12)
            self.assertNotIn("battery_soc", result.values)
            self.assertNotIn("short_ascii_rated_voltage", result.values)

    async def test_persisted_unsupported_group_is_not_queried_or_projected(self):
        seed_unsupported_commands(self.state, ("short_ascii:RB", "short_ascii:F", "short_ascii:RH"))
        result = await self.read(0)
        self.assertEqual(self.transport.requests, [b"Q1\x01\r"])
        self.assertNotIn("battery_soc", result.values)
        self.assertEqual(
            result.diagnostics["short_ascii_optional_status"],
            "RB=unsupported; F=unsupported; RH=unsupported",
        )

    async def test_failed_optional_request_on_disconnected_transport_is_a_link_failure(self):
        await self.read(0)
        self.transport.responses["F"] = asyncio.TimeoutError()
        self.transport.connected = False
        with self.assertRaises(ConnectionError):
            await self.read(1)
        self.assertEqual(unsupported_commands(self.state), ())
        self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))

    async def test_corrupt_optional_response_removes_previous_sample_without_partial_values(self):
        await self.read(0); await self.read(1); await self.read(2)
        wire = bytearray(_rb()); wire[-2] ^= 1
        self.transport.responses["RB"] = bytes(wire)
        result = await self.read(31)
        self.assertEqual(result.values["grid_voltage"], 230)
        self.assertNotIn("battery_soc", result.values)
        self.assertFalse(any(key.startswith("bms_") for key in result.values))
        self.assertIn("RB=invalid_response", result.diagnostics["short_ascii_optional_status"])

    async def test_sample_can_expire_while_another_group_is_awaited(self):
        clock = [59.0]
        reads = OptionalReads(self.transport, self.inverter, 59)
        rb, rated, _rh_sample = reads.samples
        rb.values, rb.sampled_at, rb.next_due = {"battery_soc": 80}, 0, 100

        class Session:
            async def request(self, command):
                self_outer.assertEqual(command, "F")
                clock[0] = 62
                return b"#115.0 105 48.00 60.0\r"

        self_outer = self
        values, diagnostics = await reads.refresh_one(Session(), {}, lambda: clock[0])
        self.assertNotIn("battery_soc", values)
        self.assertNotIn("short_ascii_rb_age_seconds", diagnostics)
        self.assertEqual(values["short_ascii_rated_voltage"], 115)
        self.assertEqual(rated.sampled_at, 62)
        self.assertIn("RB=expired", diagnostics["short_ascii_optional_status"])


if __name__ == "__main__":
    unittest.main()
