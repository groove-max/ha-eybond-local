"""Narrow MarsRock read-map qualification, without claiming a live callback."""

import asyncio
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import tempfile
import zipfile

from custom_components.eybond_local.drivers.marsrock import (
    MarsRockDriver, decode_marsrock_identity, decode_marsrock_live,
)
from custom_components.eybond_local.drivers.registry import driver_options, iter_drivers
from custom_components.eybond_local.canonical_telemetry import apply_canonical_measurements
from custom_components.eybond_local.derived_energy import derived_energy_descriptions_for_keys
from custom_components.eybond_local.link_models import RawSerialLinkRoute
from custom_components.eybond_local.payload.modbus import ModbusError, parse_read_holding_response
from custom_components.eybond_local.payload.modbus import crc16_modbus
from custom_components.eybond_local.support.package import export_support_package
from custom_components.eybond_local.support.runtime_projection import build_support_fixture
from custom_components.eybond_local.fixtures.utils import anonymize_fixture_json

FIXTURE = json.loads((Path(__file__).parent / "fixtures/marsrock_read_observation.json").read_text())


def words_for(start, cycle=0):
    row = next(r for r in FIXTURE["records"] if r["start"] == start and r["cycle"] == cycle)
    return parse_read_holding_response(bytes.fromhex(row["response_hex"]), slave_id=1, count=row["count"])


class Replay:
    connected = True

    def __init__(self, cycle=0, raw=False):
        self.responses = {bytes.fromhex(r["request_hex"]): bytes.fromhex(r["response_hex"])
                          for r in FIXTURE["records"] if r["cycle"] == cycle}
        self.calls = []
        self.raw = raw
        self.fail = False
        self.point_words = {}
        self.errors = {}

    def select_payload_route(self, route, *, payload_family):
        return RawSerialLinkRoute(protocol="modbus_rtu") if self.raw else route

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        expected_route = RawSerialLinkRoute(protocol="modbus_rtu") if self.raw else MarsRockDriver.probe_targets[0].link_route
        assert route == expected_route
        start, count = int.from_bytes(payload[2:4], "big"), int.from_bytes(payload[4:6], "big")
        assert payload[1] == 3 and (start, count) in (
            (0, 20), (59, 66), (60, 1), (94, 1), (99, 1), (107, 1), (110, 1),
            (151, 10), (271, 7), (305, 2), (331, 2), (451, 5),
        )
        self.calls.append(payload)
        if self.fail:
            raise asyncio.TimeoutError("synthetic read failure")
        if (start, count) in self.errors:
            raise self.errors[start, count]
        if payload in self.responses:
            return self.responses[payload]
        if count == 1:
            word = self.point_words.get(start, words_for(59)[start - 59])
            body = bytes([1, 3, 2]) + word.to_bytes(2, "big")
        else:
            body = bytes([1, 0x83, 2])  # Unimplemented synthetic history window.
        return body + crc16_modbus(body).to_bytes(2, "little")


class MarsRockDecoderTests(unittest.TestCase):
    def test_three_observed_cycles_and_only_qualified_measurements(self):
        for cycle, grid, dc, power in ((0, 235, 20.5, 0.6), (1, 234.7, 20.6, 0), (2, 233.9, 20.5, 0.6)):
            with self.subTest(cycle=cycle):
                self.assertEqual(decode_marsrock_identity(words_for(0, cycle)), "9900000043")
                values = decode_marsrock_live(words_for(59, cycle))
                self.assertEqual(values, {"grid_voltage": grid, "dc_input_voltage": dc,
                    "generation_power": power, "generation_energy_total": 1.2})
                apply_canonical_measurements("marsrock_grid_tie", values)
                self.assertEqual(len(values), 4)
                self.assertEqual(derived_energy_descriptions_for_keys(set(values)), ())

    def test_low_word_first_and_big_endian_words_are_independent(self):
        words = words_for(59)
        words[63 - 59], words[64 - 59] = 0x1234, 0x0002
        words[86 - 59], words[87 - 59] = 0x5678, 0x0001
        values = decode_marsrock_live(words)
        self.assertEqual(values["generation_energy_total"], 0x00021234 / 10)
        self.assertEqual(values["generation_power"], 0x00015678 / 10)

    def test_zero_power_is_valid_but_unqualified_padding_is_not_exposed(self):
        self.assertEqual(decode_marsrock_live([0] * 66), {
            "grid_voltage": 0, "dc_input_voltage": 0, "generation_power": 0, "generation_energy_total": 0,
        })
        self.assertEqual(len(MarsRockDriver().measurements), 4)

    def test_malformed_identity_and_words_fail_closed(self):
        for offset, value in ((0, 0), (1, 1), (3, 0), (3, 65535), (3, True)):
            words = words_for(0)
            words[offset] = value
            with self.assertRaises(ModbusError):
                decode_marsrock_identity(words)
        for words in ([], [0] * 65, [0] * 67, [True] * 66, [-1] * 66, [65536] * 66):
            with self.assertRaises(ModbusError):
                decode_marsrock_live(words)

    def test_explicit_only_and_no_borrowed_controls(self):
        driver = MarsRockDriver()
        self.assertIn(driver.key, driver_options())
        self.assertEqual([d.key for d in iter_drivers(driver.key)], [driver.key])
        self.assertNotIn(driver.key, {d.key for d in iter_drivers("auto")})
        self.assertEqual(driver.write_capabilities, ())
        self.assertEqual(driver.binary_sensors, ())
        self.assertFalse(driver.cloud_learning_supported)
        self.assertTrue(driver.support_marker().read_only)


class MarsRockDriverTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch(
            "custom_components.eybond_local.drivers.marsrock.SUPPORT_READ_SPACING_SECONDS", 0,
        ))

    async def test_explicit_selection_uses_real_detection_pipeline(self):
        from custom_components.eybond_local.runtime.driver_detection import async_detect_inverter
        context = await async_detect_inverter(Replay(), driver_hint="marsrock_grid_tie")
        self.assertEqual(context.inverter.driver_key, "marsrock_grid_tie")

    async def test_probe_poll_and_capture_use_bounded_reads(self):
        for raw in (False, True):
            driver, transport = MarsRockDriver(), Replay(raw=raw)
            inverter = await driver.async_probe(transport, driver.probe_targets[0])
            self.assertIsNotNone(inverter)
            self.assertEqual(inverter.serial_number, "9900000043")
            self.assertEqual(inverter.capabilities, ())
            read = await driver.async_read_values(transport, inverter)
            self.assertEqual(read.values, decode_marsrock_live(words_for(59)))
            evidence = await driver.async_capture_support_evidence(transport, inverter)
            self.assertEqual([(b["start"], b["count"]) for b in evidence["fixture_ranges"]], [(0, 20), (59, 66)])
            self.assertEqual(evidence["status"], "completed")
            self.assertEqual(len(evidence["planned_ranges"]), 12)
            self.assertEqual(len(evidence["captured_ranges"]), 7)
            self.assertEqual(len(evidence["range_failures"]), 5)
            self.assertTrue(all(row["comparison"] == "same_raw_value"
                                for row in evidence["candidate_comparisons"]))
            self.assertTrue(all(row["qualification"] == "unverified"
                                for row in evidence["candidate_comparisons"]))
            before = len(transport.calls)
            with self.assertRaises(NotImplementedError):
                await driver.async_write_capability(transport, inverter, "on_off", 1)
            self.assertEqual(len(transport.calls), before)
            transport.fail = True
            with self.assertRaises(ModbusError):
                await driver.async_read_values(transport, inverter)
            self.assertIsNone(await driver.async_probe(transport, driver.probe_targets[0]))

    async def test_candidate_changes_do_not_rewrite_baseline_fixture_or_runtime(self):
        driver, transport = MarsRockDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        transport.point_words = {60: 7, 94: 213, 99: 123, 107: 4500, 110: 21}
        evidence = await driver.async_capture_support_evidence(transport, inverter)
        self.assertTrue(all(row["comparison"] == "different_raw_value"
                            for row in evidence["candidate_comparisons"]))
        self.assertTrue(all(row["block_word"] == 0 for row in evidence["candidate_comparisons"]))
        self.assertEqual(evidence["fixture_ranges"][1]["values"], words_for(59))
        read = await driver.async_read_values(transport, inverter)
        self.assertEqual(read.values, decode_marsrock_live(words_for(59)))
        self.assertFalse(driver.cloud_learning_supported)
        fixture = build_support_fixture(evidence, inverter=None, collector_payload=None)
        anonymized = anonymize_fixture_json(fixture)
        anonymous_identity = anonymized["ranges"][0]["values"]
        self.assertNotEqual(decode_marsrock_identity(anonymous_identity), "9900000043")
        self.assertEqual(len(decode_marsrock_identity(anonymous_identity)), 10)
        self.assertEqual(anonymized["ranges"][1], fixture["ranges"][1])
        with tempfile.TemporaryDirectory() as directory:
            package = export_support_package(
                config_dir=Path(directory), entry_id="marsrock_synthetic",
                entry_title="Synthetic MarsRock", support_bundle={}, raw_capture=evidence,
                fixture=fixture, anonymized_fixture=anonymized,
            )
            with zipfile.ZipFile(package.path) as archive:
                self.assertIsNone(archive.testzip())
                self.assertEqual(json.loads(archive.read("raw_capture.json")), evidence)
                self.assertEqual(json.loads(archive.read("fixture/raw_fixture.json"))["ranges"],
                                 evidence["fixture_ranges"])
                saved = json.loads(archive.read("fixture/anonymized_fixture.json"))
                self.assertEqual(saved, anonymized)

    async def test_transport_failure_stops_optional_reads_but_preserves_evidence(self):
        driver, transport = MarsRockDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        transport.calls.clear()
        transport.errors[94, 1] = ConnectionError("synthetic_offline")
        evidence = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(evidence["status"], "stopped_on_error")
        self.assertEqual(len(transport.calls), 4)
        self.assertEqual(len(evidence["captured_ranges"]), 3)
        self.assertIsNone(evidence["candidate_comparisons"][1]["point_word"])
        self.assertEqual(evidence["candidate_comparisons"][1]["comparison"], "not_available")
        self.assertEqual(evidence["range_failures"][0]["error"], "synthetic_offline")

    async def test_missing_baseline_is_not_replaced_by_point_reads(self):
        driver, transport = MarsRockDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        transport.errors[59, 66] = ModbusError("exception_code:2")
        evidence = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(evidence["status"], "completed")
        self.assertEqual(len(evidence["fixture_ranges"]), 1)
        self.assertTrue(all(row["block_word"] is None and row["point_word"] == 0
                            for row in evidence["candidate_comparisons"]))
        self.assertTrue(all(row["comparison"] == "not_available"
                            for row in evidence["candidate_comparisons"]))

    async def test_deadline_and_unload_cancel_do_not_continue_history_reads(self):
        for cancel in (False, True):
            driver, transport = MarsRockDriver(), Replay()
            inverter = await driver.async_probe(transport, driver.probe_targets[0])
            entered, exited = asyncio.Event(), asyncio.Event()
            original = transport.async_send_payload

            async def stalled(payload, **kwargs):
                if int.from_bytes(payload[2:4], "big") == 94:
                    entered.set()
                    try:
                        await asyncio.Future()
                    finally:
                        exited.set()
                return await original(payload, **kwargs)

            transport.async_send_payload = stalled
            with patch("custom_components.eybond_local.drivers.marsrock.SUPPORT_READ_BUDGET_SECONDS",
                       1 if cancel else 0.02):
                task = asyncio.create_task(driver.async_capture_support_evidence(transport, inverter))
                await asyncio.wait_for(entered.wait(), 1)
                if cancel:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                else:
                    evidence = await asyncio.wait_for(task, 1)
                    self.assertEqual(evidence["status"], "budget_exhausted")
                    self.assertEqual(len(evidence["captured_ranges"]), 3)
                    self.assertEqual(evidence["range_failures"][0]["error"], "diagnostic_budget_exhausted")
            self.assertTrue(exited.is_set())
            self.assertTrue(all(int.from_bytes(p[2:4], "big") < 151 for p in transport.calls))

    async def test_crc_length_and_header_failures_cannot_bind(self):
        for corruption in ("crc", "length", "identity"):
            driver, transport = MarsRockDriver(), Replay()
            request = next(iter(transport.responses))
            response = transport.responses[request]
            if corruption == "crc":
                transport.responses[request] = response[:-1] + bytes([response[-1] ^ 1])
            elif corruption == "length":
                transport.responses[request] = response[:-2]
            else:
                from custom_components.eybond_local.payload.modbus import crc16_modbus
                body = response[:3] + b"\0\0" + response[5:-2]
                transport.responses[request] = body + crc16_modbus(body).to_bytes(2, "little")
            self.assertIsNone(await driver.async_probe(transport, driver.probe_targets[0]))
