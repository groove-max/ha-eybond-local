"""09C1 synthetic and owner-capture contracts, separate from Short-ASCII/PI30."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from dataclasses import replace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers.eybond_09c1 import Eybond09C1Driver
from custom_components.eybond_local.drivers.eybond_09c1_pv import PvSample, STATE_KEY
from custom_components.eybond_local.drivers.command_support import (
    clear_unsupported_commands, seed_unsupported_commands, unsupported_commands,
)
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.registry import get_driver, iter_drivers, serial_is_stable
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.urtu09c1 import (
    READ_COMMANDS, Urtu09C1Error, Urtu09C1Session, build_request,
    parse_f, parse_pv, parse_pv_channel, parse_q1, parse_qf,
)
from custom_components.eybond_local.payload.short_ascii import ShortAsciiError, parse_q1 as parse_addressed_q1


def responses():
    return {
        "Q1": b"(232.0 241.0 229.0 025 49.9 52.4 31.0 00100001\r",
        "QF": b"(50.1\r", "PV?": b"(3215 123 0 002180\r",
        "F": b"(230.0 12K 48.00 50.0\r", "G?": b"(Normal 04  \r",
        "PV1": b"(3105 101 0 003470\r", "PV2": b"(2780 086 0 001230\r",
    }


class Transport:
    connected = True

    def __init__(self):
        self.responses = responses()
        self.requests = []

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        assert type(route) is EybondLinkRoute
        assert (route.devcode, route.collector_addr) == (1, 255)
        assert payload in [command.encode() + b"\r" for command in READ_COMMANDS]
        self.requests.append(payload)
        value = self.responses[payload[:-1].decode()]
        if isinstance(value, BaseException):
            raise value
        return value

    def select_payload_route(self, *args, **kwargs):
        raise AssertionError("09C1 must not change to a raw UART/AT route")


class PayloadTests(unittest.TestCase):
    def test_exact_allowlisted_commands_no_address_or_checksum(self):
        for command in READ_COMMANDS:
            self.assertEqual(build_request(command), command.encode() + b"\r")
        for command in (None, "", "Q1\r", "Q1\x01", "SON", "RB", "MP", "QPI", "SET"):
            with self.subTest(command=command), self.assertRaises(Urtu09C1Error):
                build_request(command)

    def test_measurements_and_distinct_frequency_owners(self):
        q1 = parse_q1(responses()["Q1"])
        self.assertEqual({key: q1[key] for key in (
            "grid_voltage", "output_voltage", "load_percent", "grid_frequency",
            "battery_voltage", "temperature",
        )}, {"grid_voltage": 232, "output_voltage": 229, "load_percent": 25,
             "grid_frequency": 49.9, "battery_voltage": 52.4, "temperature": 31})
        self.assertNotIn("output_frequency", q1)
        self.assertEqual(parse_qf(responses()["QF"])["output_frequency"], 50.1)
        pv = parse_pv(responses()["PV?"])
        self.assertEqual((pv["pv_voltage"], pv["pv_current"]), (321.5, 12.3))
        for key in ("battery_soc", "output_power", "battery_power", "pv_power", "energy_total", "serial_number"):
            self.assertNotIn(key, q1 | pv | parse_f(responses()["F"]))

    def test_zero_measurements_and_negative_temperature(self):
        values = parse_q1(b"(000.0 000.0 000.0 000 00.0 00.0 -5.0 00000000\r")
        self.assertEqual(values["grid_voltage"], 0)
        self.assertEqual(values["battery_voltage"], 0)
        self.assertEqual(values["temperature"], -5)
        self.assertEqual(parse_pv(b"(0000 000 0 000000\r")["pv_current"], 0)

    def test_channel_ownership_scaling_and_valid_zero_not_missing(self):
        for command, volts, amps in (("PV1", 310.5, 10.1), ("PV2", 278, 8.6)):
            self.assertEqual(parse_pv_channel(responses()[command], command=command), {
                f"{command.lower()}_voltage": volts, f"{command.lower()}_current": amps,
            })
            self.assertEqual(parse_pv_channel(b"(0000 000 0 000000\r", command=command), {
                f"{command.lower()}_voltage": 0, f"{command.lower()}_current": 0,
            })
        for command in (None, True, 1, "PV?", "pv1", "PV3", "PV1\r"):
            with self.subTest(command=command), self.assertRaises(Urtu09C1Error):
                parse_pv_channel(responses()["PV1"], command=command)

    def test_channels_reject_partial_nak_echo_wrong_dialect_and_bad_fields(self):
        for command in ("PV1", "PV2"):
            frame = responses()[command]
            bad_frames = [None, bytearray(frame), b"", b"NAK\r", b"(NAK\r", b"ACK\r",
                          command.encode() + b"\r", frame[:-1], frame + b"\n", frame * 2,
                          b"\x01" + frame, b"#" + frame[1:], b"(0590 059 ",
                          b"(310. 101 0 003470\r", b"(3105 -01 0 003470\r",
                          b"(3105 101 X 003470\r", b"(3105 101 0 00347x\r"]
            bad_frames.extend(frame[:i] + b"\xff" + frame[i + 1:] for i in range(len(frame)))
            bad_frames.extend(frame[:i] + b"_" + frame[i + 1:] for i in (5, 9, 11))
            for bad in bad_frames:
                with self.subTest(command=command, bad=bad), self.assertRaises(Urtu09C1Error):
                    parse_pv_channel(bad, command=command)

    def test_owner_offgrid_flag_shape_is_not_a_cloud_fault_enum(self):
        values = parse_q1(b"(000.0 000.0 238.0 012 00.0 51.8 34.0 10001001\r")
        self.assertFalse(values["grid_available"])
        self.assertFalse(values["inverter_fault"])
        self.assertEqual(values["operating_mode"], "Battery (energy saving)")
        self.assertNotIn("inverter_status", values)

    def test_fault_voltage_stays_separate_from_live_input_and_output(self):
        for fault in (b"000.0", b"241.0", b"248.0"):
            raw = responses()["Q1"]
            raw = raw[:7] + fault + raw[12:]
            values = parse_q1(raw)
            self.assertEqual(values["grid_voltage"], 232)
            self.assertEqual(values["output_voltage"], 229)
            self.assertEqual(values["urtu09c1_fault_voltage"], float(fault))

    def test_status_bits_follow_09c1_not_short_ascii(self):
        for position, key, true_bit in ((0, "grid_available", 48), (1, "battery_low", 49),
                                       (2, "urtu09c1_ac_charger_enabled", 49),
                                       (3, "inverter_fault", 49), (7, "urtu09c1_buzzer_enabled", 49)):
            for bit in (48, 49):
                raw = bytearray(responses()["Q1"])
                raw[38 + position] = bit
                self.assertIs(parse_q1(bytes(raw))[key], bit == true_bit)
        for mode, name in ((b"00", "Bypass"), (b"01", "AVR"),
                           (b"10", "Battery (energy saving)"), (b"11", "Inverter failure")):
            raw = bytearray(responses()["Q1"])
            raw[42:44] = mode
            self.assertEqual(parse_q1(bytes(raw))["operating_mode"], name)

    def test_rating_units_not_current_or_measured_power(self):
        for field, watts in ((b"12K", 12000), (b"8.0", 8000), (b"750", 750)):
            result = parse_f(b"(230.0 " + field + b" 48.00 50.0\r")
            self.assertEqual(result["urtu09c1_rated_power"], watts)
            self.assertNotIn("output_power", result)

    def test_malformed_and_other_dialects_are_rejected_without_stripping(self):
        for command, parser in (("Q1", parse_q1), ("QF", parse_qf), ("PV?", parse_pv), ("F", parse_f)):
            frame = responses()[command]
            for bad in (None, b"", frame[:-1], frame + b"\n", frame + frame,
                        b"#" + frame[1:], b"\x01" + frame, bytearray(frame)):
                with self.subTest(command=command, bad=bad), self.assertRaises(Urtu09C1Error):
                    parser(bad)
            for index in range(1, len(frame) - 1):
                raw = frame[:index] + b"\xff" + frame[index + 1:]
                with self.subTest(command=command, index=index), self.assertRaises(Urtu09C1Error):
                    parser(raw)
        with self.assertRaises(ShortAsciiError):
            parse_addressed_q1(responses()["Q1"])
        # PI30 QPIGS and the binary-status/checksum URTU1920 reply cannot bind.
        for frame in (b"(230.0 50.0 230.0 50.0 1000 0800 050 400 52.0 010 080 030\r",
                      b"\x01" + bytes(49) + b"\r"):
            with self.assertRaises(Urtu09C1Error):
                parse_q1(frame)


class DriverTests(unittest.IsolatedAsyncioTestCase):
    def test_new_driver_is_appended_without_changing_existing_scan_priority(self):
        self.assertEqual([driver.key for driver in iter_drivers("auto")], [
            "modbus_smg", "srne_modbus", "must_pv_ph18", "modbus_catalog", "pi30",
            "eybond_g_ascii", "smartess_local", "pi18", "eybond_short_ascii", "eybond_09c1",
        ])

    async def asyncSetUp(self):
        self.driver = Eybond09C1Driver()
        self.transport = Transport()
        self.target = ProbeTarget(1, 255, 1)

    async def test_catalog_requires_all_four_shapes_and_persists_only_identity(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        self.assertIsNotNone(inverter)
        self.assertEqual(inverter.model_name, "EyeBond 09C1 family")
        self.assertEqual(inverter.serial_number, "")
        self.assertFalse(serial_is_stable(self.driver.key, inverter))
        self.assertEqual(set(inverter.details), {"protocol_id", "catalog_detection"})
        self.assertEqual(inverter.details["catalog_detection"]["surface_key"], "eybond_09c1_read_only")
        self.assertCountEqual(self.transport.requests, [b"Q1\r", b"QF\r", b"PV?\r", b"F\r"])
        self.assertIsInstance(get_driver(self.driver.key), Eybond09C1Driver)
        for command in ("Q1", "QF", "PV?", "F"):
            self.transport.responses = responses() | {command: b"NAK\r"}
            self.assertIsNone(await self.driver.async_probe(self.transport, self.target), command)

    async def test_full_read_never_revives_missing_groups(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        before = await self.driver.async_read_values(self.transport, inverter)
        self.assertEqual(before.mode, DriverReadMode.FULL)
        self.assertEqual(before.values["output_frequency"], 50.1)
        for command in ("QF", "PV?", "F"):
            self.transport.responses[command] = TimeoutError()
        after = await self.driver.async_read_values(self.transport, inverter)
        self.assertEqual(after.mode, DriverReadMode.FULL)
        self.assertEqual(after.values["grid_frequency"], 49.9)
        for key in ("output_frequency", "pv_current", "pv_voltage", "urtu09c1_rated_power"):
            self.assertNotIn(key, after.values)
        self.transport.responses["Q1"] = TimeoutError()
        with self.assertRaises(TimeoutError):
            await self.driver.async_read_values(self.transport, inverter)

    async def test_cancel_propagates_and_support_capture_is_bounded_read_only(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        evidence = await self.driver.async_capture_support_evidence(self.transport, inverter)
        self.assertEqual(set(evidence["responses_hex"]), set(READ_COMMANDS))
        self.assertEqual(evidence["failures"], {})
        self.transport.responses["QF"] = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.driver.async_read_values(self.transport, inverter)

    async def test_no_write_controls_or_raw_fallback(self):
        self.assertTrue(self.driver.support_marker().read_only)
        self.assertFalse(self.driver.write_capabilities)
        with self.assertRaisesRegex(ValueError, "unsupported_capability"):
            await self.driver.async_write_capability(self.transport, None, "power", 1)
        with self.assertRaisesRegex(Urtu09C1Error, "requires_fc4"):
            await Urtu09C1Session(self.transport, RawSerialLinkRoute()).request("Q1")
        self.assertEqual(self.transport.requests, [])


class PvRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver, self.transport = Eybond09C1Driver(), Transport()
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(1, 255, 1))
        self.state = {}
        self.transport.requests.clear()

    async def read(self, now, *, poll_interval=None):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state,
            now_monotonic=now, poll_interval=poll_interval,
        )

    async def test_issue_50_owner_captures_keep_protocol_numbering_and_legacy_values(self):
        # The owner's two experiments used different physical inputs/times.
        # Command numbers are crossed on that unit, not evidence for a global
        # port swap. PV? from each nested support archive is independent too.
        cases = (
            ("physical_PV1", {
                "Q1": b"(000.0 000.0 240.0 015 00.0 53.2 36.0 10001001\r",
                "PV?": b"(0000 000 0 002180\r",
                "PV1": b"(0000 000 0 002180\r",
                "PV2": b"(0610 055 0 002180\r",
            }, (0, 0, 61, 5.5, 0, 0)),
            ("physical_PV2", {
                "Q1": b"(000.0 000.0 242.0 005 00.0 53.2 33.0 10001001\r",
                "PV?": b"(0590 059 0 002180\r",
                "PV1": b"(0600 058 0 002180\r",
                "PV2": b"(0000 000 0 002180\r",
            }, (60, 5.8, 0, 0, 59, 5.9)),
        )
        for name, raw, expected in cases:
            with self.subTest(experiment=name):
                self.state.clear()
                self.transport.responses = responses() | raw | {
                    "QF": b"(60.0\r", "F": b"(230.0 12K 48.00 60.0\r",
                }
                await self.read(0)
                result = await self.read(1)
                self.assertTrue(result.diagnostics["urtu09c1_pv_qualified"])
                self.assertEqual(tuple(result.values[key] for key in (
                    "pv1_voltage", "pv1_current", "pv2_voltage", "pv2_current",
                    "pv_voltage", "pv_current",
                )), expected)
                self.assertIs(result.values["grid_available"], False)
                self.assertIs(result.values["inverter_fault"], False)
                for key in ("pv_power", "pv_total_current", "energy_total"):
                    self.assertNotIn(key, result.values)

    async def test_one_extra_query_fair_cadence_and_separate_freshness(self):
        first = await self.read(0)
        self.assertEqual(first.mode, DriverReadMode.FULL)
        self.assertNotIn("pv1_voltage", first.values)
        self.assertFalse(first.diagnostics["urtu09c1_pv_qualified"])
        self.assertEqual(first.values["pv_voltage"], 321.5)
        self.assertNotIn("pv2_voltage", first.values)
        second = await self.read(1)
        self.assertEqual(second.values["pv2_voltage"], 278)
        self.assertEqual(second.values["pv1_voltage"], 310.5)
        self.assertTrue(second.diagnostics["urtu09c1_pv_qualified"])
        self.assertEqual(second.diagnostics["urtu09c1_pv_status"], "PV1=cached; PV2=ok")
        third = await self.read(2)
        self.assertEqual(third.diagnostics["urtu09c1_pv_status"], "PV1=cached; PV2=cached")
        base = [b"Q1\r", b"QF\r", b"PV?\r", b"F\r"]
        self.assertEqual(self.transport.requests, base + [b"PV1\r"] + base + [b"PV2\r"] + base)
        self.assertAlmostEqual(third.diagnostics["urtu09c1_pv1_age_seconds"], 2, delta=0.1)
        self.assertAlmostEqual(third.diagnostics["urtu09c1_pv2_age_seconds"], 1, delta=0.1)
        for key in ("pv_power", "pv_total_power", "pv_total_current", "pv_energy", "energy_total"):
            self.assertNotIn(key, third.values)

    async def test_state_is_required_for_new_reads_but_not_legacy_reads(self):
        for _ in range(3):
            result = await self.driver.async_read_values(self.transport, self.inverter)
            self.assertEqual(result.values["pv_current"], 12.3)
            self.assertNotIn("pv1_voltage", result.values)
        self.assertEqual(self.transport.requests, [b"Q1\r", b"QF\r", b"PV?\r", b"F\r"] * 3)

    async def test_channel_failure_withdraws_only_its_values_never_falls_back(self):
        for error in (TimeoutError(), b"NAK\r", b"(3105 101 0 00347x\r"):
            with self.subTest(error=error):
                self.state = {}
                self.transport.responses = responses()
                await self.read(0)
                await self.read(1)
                self.transport.responses["PV1"] = error
                result = await self.read(31)
                self.assertNotIn("pv1_voltage", result.values)
                self.assertNotIn("pv1_current", result.values)
                self.assertNotIn("urtu09c1_pv1_age_seconds", result.diagnostics)
                self.assertEqual(result.values["pv2_voltage"], 278)
                self.assertEqual(result.values["pv_voltage"], 321.5)
                self.assertEqual(result.values["output_frequency"], 50.1)
                self.assertNotIn("pv_power", result.values)

    async def test_new_negative_cache_never_changes_legacy_optional_behavior(self):
        self.transport.responses.update({command: TimeoutError() for command in ("QF", "PV?", "F", "PV1")})
        for now in range(0, 310, 31):
            result = await self.read(now)
        self.assertEqual(unsupported_commands(self.state), ("09c1:PV1",))
        self.assertEqual(result.diagnostics["driver_unsupported_commands"], "09c1:PV1")
        self.assertIn("PV1=unsupported", result.diagnostics["urtu09c1_pv_status"])
        self.assertEqual(self.transport.requests.count(b"PV1\r"), 4)
        for command in (b"QF\r", b"PV?\r", b"F\r"):
            self.assertEqual(self.transport.requests.count(command), 10)
        self.assertNotIn("pv_voltage", result.values)
        self.assertNotIn("pv2_voltage", result.values)
        self.assertFalse(result.diagnostics["urtu09c1_pv_qualified"])
        self.transport.responses = responses()
        clear_unsupported_commands(self.state)
        await self.read(341)
        recovered = await self.read(372)
        self.assertEqual(recovered.values["pv1_voltage"], 310.5)
        self.assertEqual(recovered.values["pv_voltage"], 321.5)
        self.assertEqual(unsupported_commands(self.state), ())

    async def test_both_missing_channels_stop_without_affecting_q1_or_pv_question(self):
        self.transport.responses.update({"PV1": b"NAK\r", "PV2": TimeoutError()})
        for now in range(0, 341, 31):
            result = await self.read(now)
        self.assertEqual(unsupported_commands(self.state), ("09c1:PV1", "09c1:PV2"))
        self.assertEqual(self.transport.requests.count(b"PV1\r"), 4)
        self.assertEqual(self.transport.requests.count(b"PV2\r"), 4)
        self.assertEqual(result.values["grid_voltage"], 232)
        self.assertEqual(result.values["pv_voltage"], 321.5)
        for key in ("pv1_voltage", "pv2_voltage", "pv_power"):
            self.assertNotIn(key, result.values)

    async def test_persisted_negative_facts_skip_queries_and_recheck_resumes(self):
        seed_unsupported_commands(self.state, ["09c1:PV1", "09c1:PV2"])
        result = await self.read(0)
        self.assertEqual(self.transport.requests, [b"Q1\r", b"QF\r", b"PV?\r", b"F\r"])
        self.assertNotIn("pv1_voltage", result.values)
        clear_unsupported_commands(self.state)
        self.assertNotIn("pv1_voltage", (await self.read(1)).values)
        self.assertEqual((await self.read(2)).values["pv2_voltage"], 278)

    async def test_success_resets_strikes_and_zero_channel_is_supported(self):
        self.transport.responses["PV1"] = b"NAK\r"
        await self.read(0)
        await self.read(1)
        await self.read(31)
        await self.read(32)
        self.transport.responses["PV1"] = b"(0000 000 0 000000\r"
        result = await self.read(62)
        self.assertEqual(result.values["pv1_voltage"], 0)
        self.assertEqual(result.values["pv1_current"], 0)
        self.transport.responses["PV1"] = b"NAK\r"
        for now in range(93, 249, 31):
            await self.read(now)
        self.assertEqual(unsupported_commands(self.state), ())

    async def test_long_poll_interval_keeps_both_channels_with_explicit_age(self):
        await self.read(0, poll_interval=300)
        result = await self.read(300, poll_interval=300)
        self.assertEqual(result.values["pv1_voltage"], 310.5)
        self.assertEqual(result.values["pv2_voltage"], 278)
        self.assertAlmostEqual(result.diagnostics["urtu09c1_pv1_age_seconds"], 300, delta=0.1)
        expired = await self.read(1500, poll_interval=300)
        self.assertEqual(expired.values["pv1_voltage"], 310.5)
        self.assertNotIn("pv2_voltage", expired.values)
        self.assertIn("PV2=expired", expired.diagnostics["urtu09c1_pv_status"])

    def test_ttl_is_exclusive_and_does_not_refresh_when_republished(self):
        sample = PvSample("PV1", sampled_at=10, values={"pv1_voltage": 300})
        values = sample.fresh_values(11)
        values["pv1_voltage"] = 999
        self.assertEqual(sample.fresh_values(69.999), {"pv1_voltage": 300})
        self.assertEqual(sample.fresh_values(70), {})
        self.assertEqual(sample.fresh_values(71), {})

    async def test_increasing_poll_interval_never_extends_existing_sample_expiry(self):
        await self.read(0, poll_interval=30)
        await self.read(1, poll_interval=30)
        result = await self.read(100, poll_interval=300)
        self.assertEqual(result.values["pv1_voltage"], 310.5)
        self.assertNotIn("pv2_voltage", result.values)

    async def test_first_channel_must_not_qualify_using_pv_question_or_expired_peer(self):
        first = await self.read(0)
        self.assertEqual(first.values["pv_voltage"], 321.5)
        second = await self.read(100)
        self.assertFalse(second.diagnostics["urtu09c1_pv_qualified"])
        self.assertNotIn("pv1_voltage", second.values)
        self.assertNotIn("pv2_voltage", second.values)
        qualified = await self.read(101)
        self.assertTrue(qualified.diagnostics["urtu09c1_pv_qualified"])
        self.assertEqual(qualified.values["pv1_voltage"], 310.5)
        self.assertEqual(qualified.values["pv2_voltage"], 278)

    async def test_one_channel_can_be_unsupported_after_pair_qualification(self):
        await self.read(0)
        await self.read(1)
        self.transport.responses["PV2"] = b"NAK\r"
        for now in range(31, 342, 31):
            result = await self.read(now)
        self.assertTrue(result.diagnostics["urtu09c1_pv_qualified"])
        self.assertEqual(result.values["pv1_voltage"], 310.5)
        self.assertNotIn("pv2_voltage", result.values)
        self.assertEqual(unsupported_commands(self.state), ("09c1:PV2",))
        self.assertEqual(result.values["pv_voltage"], 321.5)

    async def test_same_object_disconnect_reconnect_requires_new_pair(self):
        await self.read(0)
        await self.read(1)
        self.transport.responses["QF"] = ConnectionError()
        before = len(self.transport.requests)
        lost = await self.read(2)
        self.assertEqual(self.transport.requests[before:], [b"Q1\r", b"QF\r", b"PV?\r", b"F\r"])
        self.assertNotIn("pv1_voltage", lost.values)
        self.assertNotIn("pv2_voltage", lost.values)
        self.assertFalse(lost.diagnostics["urtu09c1_pv_qualified"])
        self.transport.responses = responses()
        first = await self.read(3)
        self.assertNotIn("pv1_voltage", first.values)
        self.assertTrue((await self.read(4)).diagnostics["urtu09c1_pv_qualified"])

    async def test_same_transport_hub_reset_and_replaced_runtime_never_reuse_samples(self):
        from types import SimpleNamespace
        from custom_components.eybond_local.runtime.hub.refresh import HubRefreshMixin

        for reset in ("hub", "new_state"):
            with self.subTest(reset=reset):
                self.state = {}
                await self.read(0)
                await self.read(1)
                if reset == "hub":
                    hub = SimpleNamespace(_runtime_read_state=self.state,
                                          _persistent_unsupported_commands=("09c1:PV2",))
                    HubRefreshMixin._reset_runtime_read_state(hub)
                    self.assertEqual(unsupported_commands(self.state), ("09c1:PV2",))
                else:
                    self.state = {}
                first = await self.read(2)
                self.assertFalse(first.diagnostics["urtu09c1_pv_qualified"])
                self.assertNotIn("pv1_voltage", first.values)
                self.assertNotIn("pv2_voltage", first.values)

    async def test_timeout_with_disconnected_transport_clears_pair_without_strike(self):
        await self.read(0)
        await self.read(1)
        original = self.transport.async_send_payload

        async def lose_link(payload, *, route, request_timeout=None):
            if payload == b"PV1\r":
                self.transport.connected = False
                raise TimeoutError()
            return await original(payload, route=route, request_timeout=request_timeout)

        with patch.object(self.transport, "async_send_payload", new=lose_link):
            lost = await self.read(31)
        self.assertFalse(lost.diagnostics["urtu09c1_pv_qualified"])
        self.assertNotIn("pv1_voltage", lost.values)
        self.assertNotIn("pv2_voltage", lost.values)
        self.assertEqual(self.state.get("driver_unsupported_commands"), {})
        self.assertEqual(lost.values["grid_voltage"], 232)

    async def test_failed_mandatory_read_or_cancellation_clears_samples_and_no_strikes(self):
        for command, exc in (("Q1", TimeoutError()), ("QF", asyncio.CancelledError()),
                             ("PV1", asyncio.CancelledError()), ("PV2", asyncio.CancelledError())):
            with self.subTest(command=command):
                self.state = {}
                self.transport.responses = responses()
                await self.read(0)
                await self.read(1)
                self.transport.responses[command] = exc
                if command == "PV2":
                    await self.read(31)
                before = len(self.transport.requests)
                with self.assertRaises(type(exc)):
                    await self.read(32)
                if command == "Q1":
                    self.assertEqual(self.transport.requests[before:], [b"Q1\r"])
                self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))
                self.assertEqual(unsupported_commands(self.state), ())
                self.assertNotIn("driver_unsupported_pending_failures", self.state)
                self.transport.responses = responses()
                result = await self.read(33)
                self.assertNotIn("pv2_voltage", result.values)

    async def test_connection_loss_does_not_poison_support_or_raise_optional_failure(self):
        await self.read(0)
        await self.read(1)
        self.transport.responses["PV1"] = ConnectionError()
        for now in range(31, 341, 31):
            result = await self.read(now)
        self.assertEqual(unsupported_commands(self.state), ())
        self.assertEqual(result.values["grid_voltage"], 232)
        self.transport.connected = False
        before = len(self.transport.requests)
        result = await self.read(400)
        self.assertEqual(self.transport.requests[before:], [b"Q1\r", b"QF\r", b"PV?\r", b"F\r"])
        self.assertNotIn("pv1_voltage", result.values)
        self.assertNotIn("pv2_voltage", result.values)

    async def test_samples_do_not_cross_transports_inverters_or_backward_clock(self):
        for change in ("transport", "inverter", "clock"):
            with self.subTest(change=change):
                self.state = {}
                await self.read(100)
                await self.read(101)
                if change == "transport":
                    self.transport = Transport()
                elif change == "inverter":
                    self.inverter = replace(self.inverter)
                result = await self.read(0 if change == "clock" else 102)
                self.assertNotIn("pv1_voltage", result.values)
                self.assertNotIn("pv2_voltage", result.values)
                self.assertFalse(result.diagnostics["urtu09c1_pv_qualified"])

    async def test_actual_wait_bound_and_request_budget_reach_transport(self):
        started = asyncio.Event()

        async def stalled(payload, *, route, request_timeout):
            self.assertEqual(payload, b"PV1\r")
            self.assertEqual(request_timeout, 0.01)
            started.set()
            await asyncio.Event().wait()

        with patch.object(self.transport, "async_send_payload", new=stalled):
            with self.assertRaises(TimeoutError):
                await Urtu09C1Session(self.transport, self.inverter.probe_target.link_route, timeout=0.01).request("PV1")
        self.assertTrue(started.is_set())

    async def test_capture_reads_both_channels_even_if_runtime_rejected_them(self):
        seed_unsupported_commands(self.state, ["09c1:PV1", "09c1:PV2"])
        self.transport.responses["PV1"] = b"NAK\r"
        self.transport.responses["PV2"] = TimeoutError()
        evidence = await self.driver.async_capture_support_evidence(self.transport, self.inverter)
        self.assertEqual(evidence["responses_hex"]["PV1"], b"NAK\r".hex())
        self.assertEqual(evidence["failures"], {"PV2": "TimeoutError"})
        self.assertEqual(self.transport.requests, [command.encode() + b"\r" for command in READ_COMMANDS])
