"""Synthetic 09C1 wire contracts, separate from addressed Short-ASCII/PI30."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers.eybond_09c1 import Eybond09C1Driver
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.registry import get_driver, iter_drivers, serial_is_stable
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.urtu09c1 import (
    READ_COMMANDS, Urtu09C1Error, Urtu09C1Session, build_request,
    parse_f, parse_pv, parse_q1, parse_qf,
)
from custom_components.eybond_local.payload.short_ascii import ShortAsciiError, parse_q1 as parse_addressed_q1


def responses():
    return {
        "Q1": b"(232.0 241.0 229.0 025 49.9 52.4 31.0 00100001\r",
        "QF": b"(50.1\r", "PV?": b"(3215 123 0 002180\r",
        "F": b"(230.0 12K 48.00 50.0\r", "G?": b"(Normal 04  \r",
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
