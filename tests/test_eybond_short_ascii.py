"""Synthetic wire and real catalog/driver contracts; no owner identifiers."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.registry import get_driver, iter_drivers, serial_is_stable
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.metadata.compiled_detection_catalog import load_compiled_detection_catalog
from custom_components.eybond_local.metadata.effective_metadata import resolve_effective_metadata_selection
from custom_components.eybond_local.metadata.effective_metadata_snapshot import (
    build_effective_metadata_snapshot_from_runtime, effective_metadata_snapshot_from_dict,
)
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget
from custom_components.eybond_local.payload.short_ascii import (
    ShortAsciiError, ShortAsciiSession, build_short_ascii_request, parse_md, parse_mp, parse_q1,
)


def _q1(body=None):
    if body is None:
        body = b"230.0 04 03 115.0 013 60.0 13.2 35.0 1000010" + b"\x00\x03\x99"
    return b"\x01" + body + (sum(body) & 65535).to_bytes(2, "big") + b"\r"


def _responses(firmware=b"S2-8127-260101-V7.00"):
    return {"MD": firmware + b"  \x00\r", "MP": b"\x01" + bytes(range(36)) + b"\r", "Q1": _q1()}


class _Transport:
    connected = True
    collector_info = CollectorInfo(collector_pn="I30000200000000001")

    def __init__(self, responses=None):
        self.responses = _responses() if responses is None else responses
        self.requests = []

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        assert type(route) is EybondLinkRoute
        assert route.devcode == 0x02FF and route.collector_addr == 255
        assert payload[-2:] == b"\x01\r"
        self.requests.append(payload)
        result = self.responses.get(payload[:-2].decode("ascii"), b"NAK\r")
        if isinstance(result, BaseException):
            raise result
        return result

    def select_payload_route(self, *args, **kwargs):
        raise AssertionError("A qualified FC4 command must not fall back to raw UART")


class ShortAsciiPayloadTests(unittest.TestCase):
    def test_only_documented_read_queries_and_exact_address(self):
        for command in ("MP", "Q1", "MD", "F", "RH", "RB"):
            for address in (0, 1, 255):
                self.assertEqual(build_short_ascii_request(command, address),
                                 command.encode() + bytes([address]) + b"\r")
        for command in ("", "QPI", "SON", "SOFF", "W", "Q1\r", "Q1\x01", None):
            with self.assertRaises(ShortAsciiError):
                build_short_ascii_request(command, 1)
        for address in (-1, 256, "1", 1.0, True, None):
            with self.assertRaises(ShortAsciiError):
                build_short_ascii_request("Q1", address)

    def test_exact_q1_fields_no_grid_frequency_or_pack_voltage_or_invented_power(self):
        values = parse_q1(_q1())
        expected = {
            "grid_voltage": 230.0, "output_voltage": 115.0, "load_percent": 13.0,
            "output_frequency": 60.0, "battery_reference_voltage": 13.2, "temperature": 35.0,
        }
        for key, value in expected.items():
            self.assertEqual(values[key], value)
        for key in ("grid_frequency", "battery_voltage", "battery_current", "battery_soc",
                    "output_power", "output_active_power", "pv_power", "serial_number"):
            self.assertNotIn(key, values)

    def test_zero_values_are_measurements_not_missing_data(self):
        values = parse_q1(_q1(b"000.0 04 03 000.0 000 00.0 00.0 00.0 0000000" + bytes(3)))
        for key in ("grid_voltage", "output_voltage", "load_percent", "output_frequency",
                    "battery_reference_voltage", "temperature"):
            self.assertEqual(values[key], 0)

    def test_negative_temperature_keeps_its_sign(self):
        body = bytearray(_q1()[1:-3])
        body[32:36] = b"-5.0"
        self.assertEqual(parse_q1(_q1(bytes(body)))["temperature"], -5)

    def test_all_status_bits_have_independent_documented_meanings(self):
        for position, key, true_byte in (
            (1, "inverter_fault", 49), (2, "grid_available", 48),
            (3, "short_ascii_mains_input_connected", 48), (4, "battery_low", 49),
            (5, "pv_controller_present", 49),
        ):
            for bit in (48, 49):
                body = bytearray(_q1()[1:-3])
                body[37 + position] = bit
                self.assertIs(parse_q1(_q1(bytes(body)))[key], bit == true_byte)

    def test_every_single_byte_corruption_is_rejected(self):
        frame = _q1()
        for index in range(len(frame)):
            with self.subTest(index=index):
                altered = bytearray(frame)
                altered[index] ^= 1
                with self.assertRaises(ShortAsciiError):
                    parse_q1(bytes(altered))

    def test_unsigned_checksum_and_internal_cr_are_not_truncated(self):
        body = bytearray(_q1()[1:-3])
        body[-1] = 13
        self.assertEqual(parse_q1(_q1(bytes(body)))["load_percent"], 13)
        for last in range(256):
            body[-1] = last
            frame = _q1(bytes(body))
            if frame[-2] == 13:
                self.assertEqual(parse_q1(frame)["output_frequency"], 60)
                break
        else:
            self.fail("Missing checksum fixture")
        self.assertGreater(sum(_q1()[1:-3]), 255)

    def test_no_missing_grid_prefix_or_shifted_columns_or_trailing_junk(self):
        for frame in (_q1()[1:], _q1()[6:], _q1()[:-1], _q1() + b"\n",
                      _q1() + _q1(), bytes(51), b"", bytearray(_q1())):
            with self.assertRaises(ShortAsciiError):
                parse_q1(frame)
        for offset, bad in ((0, b" NaN "), (22, b" nan"), (37, b"2000000"), (5, b"_")):
            body = bytearray(_q1()[1:-3])
            body[offset:offset + len(bad)] = bad
            with self.assertRaises(ShortAsciiError):
                parse_q1(_q1(bytes(body)))

    def test_mp_qualifies_shape_only_without_interpreting_settings(self):
        frame = _responses()["MP"]
        self.assertIn(b"\r", frame[:-1])
        self.assertEqual(parse_mp(frame), {"short_ascii_mp_length": 38})
        for bad in (frame[:-1], frame + b"\r", b"\x02" + frame[1:]):
            with self.assertRaises(ShortAsciiError):
                parse_mp(bad)

    def test_md_is_firmware_not_serial_and_padding_is_not_free_form(self):
        for firmware in (b"S2-8127-260101-V7.00", b"S2-8127-260202-V7.3B"):
            parsed = parse_md(_responses(firmware)["MD"])
            self.assertEqual(parsed["short_ascii_firmware"], firmware.decode())
            self.assertNotIn("serial_number", parsed)
            self.assertNotIn("model_name", parsed)
        for bad in (b" " * 20 + b"  \x00\r", _responses()["MD"] + b"\n",
                    b"\x01" + _responses()["MD"], b"QPI30" + bytes(18) + b"\r"):
            with self.assertRaises(ShortAsciiError):
                parse_md(bad)


class ShortAsciiDriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.driver = EybondShortAsciiDriver()
        self.target = ProbeTarget(0x02FF, 255, 1)
        self.transport = _Transport()

    async def test_catalog_detects_family_without_inventing_retail_model_or_serial(self):
        for firmware in (b"S2-8127-260101-V7.00", b"S2-8127-260202-V7.3B"):
            transport = _Transport(_responses(firmware))
            inverter = await self.driver.async_probe(transport, self.target)
            self.assertIsNotNone(inverter)
            self.assertEqual(inverter.model_name, "EyeBond Short-ASCII family")
            self.assertEqual(inverter.serial_number, "")
            self.assertFalse(serial_is_stable(self.driver.key, inverter))
            self.assertEqual(inverter.variant_key, "urtu1920_checksum")
            self.assertEqual(inverter.capabilities, ())
            self.assertEqual(set(transport.requests), {b"Q1\x01\r", b"MP\x01\r", b"MD\x01\r"})
            self.assertEqual(len(transport.requests), 3)
            self.assertNotIn("temperature", inverter.details)
            self.assertNotIn("grid_voltage", inverter.details)
            self.assertEqual(inverter.details["catalog_detection"]["resolution"], "family")

    async def test_all_three_independent_responses_are_required(self):
        for command in ("Q1", "MP", "MD"):
            for failure in (b"NAK\r", asyncio.TimeoutError(), b""):
                with self.subTest(command=command, failure=type(failure).__name__):
                    responses = _responses()
                    responses[command] = failure
                    inverter = await self.driver.async_probe(_Transport(responses), self.target)
                    self.assertIsNone(inverter)

    async def test_signature_is_a_checked_q1_not_just_any_ascii_response(self):
        self.assertTrue(await self.driver.async_probe_signature(self.transport, self.target))
        self.assertEqual(self.transport.requests, [b"Q1\x01\r"])
        self.transport.responses["Q1"] = b"230 04 03 115 013 60 13 35 1000010\r"
        self.assertFalse(await self.driver.async_probe_signature(self.transport, self.target))

    async def test_poll_keeps_current_q1_when_optional_rb_is_unsupported(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        self.transport.requests.clear()
        state = {"previous": {"battery_soc": 80, "battery_voltage": 53.2, "pv_power": 4000}}
        result = await self.driver.async_read_values(self.transport, inverter, runtime_state=state)
        self.assertEqual(result.mode, DriverReadMode.FULL)
        self.assertEqual(self.transport.requests, [b"Q1\x01\r", b"RB\x01\r"])
        self.assertNotIn("short_ascii_q1_length", result.values)
        self.assertNotIn("battery_soc", result.values)
        self.assertNotIn("battery_voltage", result.values)
        self.assertNotIn("pv_power", result.values)
        self.assertEqual(result.values["temperature"], 35)
        self.assertEqual(state["previous"]["pv_power"], 4000)

    async def test_failed_runtime_read_never_returns_an_empty_success(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        for failure in (b"NAK\r", ConnectionError(), asyncio.TimeoutError()):
            self.transport.responses["Q1"] = failure
            with self.assertRaises((ShortAsciiError, ConnectionError, asyncio.TimeoutError)):
                await self.driver.async_read_values(self.transport, inverter)

    async def test_write_entry_point_always_rejects_before_io(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        self.transport.requests.clear()
        for key in ("output_source_priority", "SON", "W", "unknown"):
            with self.assertRaisesRegex(ValueError, "unsupported_capability"):
                await self.driver.async_write_capability(self.transport, inverter, key, 1)
        self.assertEqual(self.transport.requests, [])
        self.assertEqual(self.driver.write_capabilities, ())
        self.assertTrue(self.driver.support_marker().read_only)
        surface = load_compiled_detection_catalog().surfaces["eybond_short_ascii_read_only"]
        self.assertTrue(surface.read_only)

    async def test_read_only_catalog_snapshot_persists_without_a_fake_controls_profile(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        snapshot = build_effective_metadata_snapshot_from_runtime(inverter=inverter, confidence="medium")
        self.assertTrue(snapshot.is_valid)
        self.assertEqual(snapshot.profile_name, "")
        restored = effective_metadata_snapshot_from_dict(snapshot.as_dict())
        self.assertTrue(restored.is_valid)
        self.assertEqual(restored, snapshot)
        selection = resolve_effective_metadata_selection(persisted_snapshot=restored)
        self.assertEqual(selection.effective_owner_key, self.driver.key)
        self.assertEqual(selection.register_schema_name, inverter.register_schema_name)
        self.assertIsNone(selection.profile_metadata)
        self.assertEqual(selection.profile_name, "")

    async def test_schema_only_snapshot_requires_current_matching_catalog_evidence(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        snapshot = build_effective_metadata_snapshot_from_runtime(inverter=inverter, confidence="medium")
        for change in (
            {"catalog_version": ""}, {"catalog_version": "old"}, {"candidate_keys": ()},
            {"surface_key": ""}, {"surface_key": "smg_base"}, {"evidence_fingerprint": ""},
            {"descriptor_revisions": ()}, {"descriptor_revisions": ("wrong:1",)},
            {"effective_owner_key": "pi30"}, {"variant_key": "default"},
            {"register_schema_name": "modbus_smg/base.json"},
            {"profile_name": "modbus_smg/protocols/communication_protocol_4.json"},
            {"candidate_keys": ("smg_family_fallback",)}, {"resolution_level": ""},
            {"confidence": "none"},
        ):
            with self.subTest(change=change):
                self.assertFalse(replace(snapshot, **change).is_valid)

    async def test_read_only_snapshot_does_not_fall_back_to_driver_default_controls(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        snapshot = build_effective_metadata_snapshot_from_runtime(inverter=inverter, confidence="medium")
        selection = resolve_effective_metadata_selection(
            persisted_snapshot=snapshot, driver=get_driver("pi30"),
        )
        self.assertEqual(selection.effective_owner_key, self.driver.key)
        self.assertEqual(selection.profile_name, "")
        self.assertIsNone(selection.profile_metadata)
        self.assertEqual(selection.register_schema_name, inverter.register_schema_name)

    async def test_support_capture_keeps_binary_trailers_and_only_bounded_read_queries(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        self.transport.requests.clear()
        capture = await self.driver.async_capture_support_evidence(self.transport, inverter)
        self.assertEqual(capture["responses_hex"]["MP"], _responses()["MP"].hex())
        self.assertEqual(capture["responses_hex"]["Q1"], _q1().hex())
        self.assertEqual(capture["failures"], {})
        self.assertEqual(self.transport.requests, [b"MP\x01\r", b"Q1\x01\r", b"MD\x01\r", b"F\x01\r", b"RH\x01\r", b"RB\x01\r"])
        # Generic support sweeps select raw routes on some AT devices; this
        # driver deliberately doesn't advertise a query through that API.
        self.assertEqual(self.driver.support_probe_plan(), ())

    async def test_cancellation_propagates_from_probe_and_poll(self):
        inverter = await self.driver.async_probe(self.transport, self.target)
        self.transport.responses["Q1"] = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.driver.async_probe_signature(self.transport, self.target)
        with self.assertRaises(asyncio.CancelledError):
            await self.driver.async_probe(self.transport, self.target)
        with self.assertRaises(asyncio.CancelledError):
            await self.driver.async_read_values(self.transport, inverter)

    async def test_raw_uart_route_and_unqualified_query_are_not_selected(self):
        raw = ShortAsciiSession(self.transport, RawSerialLinkRoute(), 1)
        with self.assertRaisesRegex(ShortAsciiError, "requires_fc4"):
            await raw.request("Q1")
        self.assertEqual(self.transport.requests, [])
        session = ShortAsciiSession(self.transport, self.target.link_route, 1)
        with self.assertRaises(ShortAsciiError):
            await session.request("SON")
        self.assertEqual(self.transport.requests, [])

    def test_registration_preserves_existing_driver_order(self):
        keys = [driver.key for driver in iter_drivers("auto")]
        self.assertEqual(keys[-1], self.driver.key)
        self.assertEqual(keys[:-1], [
            "modbus_smg", "srne_modbus", "must_pv_ph18", "modbus_catalog",
            "pi30", "eybond_g_ascii", "smartess_local", "pi18",
        ])
        self.assertIsInstance(get_driver(self.driver.key), EybondShortAsciiDriver)


if __name__ == "__main__":
    unittest.main()
