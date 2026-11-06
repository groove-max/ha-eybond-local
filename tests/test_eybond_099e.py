"""Experimental VPHB decoder, manual admission and read-only route contracts."""

import asyncio
import json
from pathlib import Path
import unittest

from custom_components.eybond_local.drivers.eybond_099e import Eybond099EDriver
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.registry import (
    driver_options, iter_drivers, measurements_for_runtime, serial_is_stable,
)
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.payload.vphb import (
    PROTOCOL_ID, STATUS_REQUEST, VphbError, VphbSession, VphbStatus,
)
from custom_components.eybond_local.canonical_telemetry import apply_canonical_measurements
from custom_components.eybond_local.derived_energy import derived_energy_descriptions_for_keys

FIXTURES = Path(__file__).parent / "fixtures/protocol_candidates"
OWNER = bytes.fromhex(json.loads((FIXTURES / "issue82_099e_status.json").read_text())["response_hex"])
REFERENCE = bytes.fromhex(json.loads((FIXTURES / "fivestar_vphb_status.json").read_text())["response_hex"])


class Replay:
    connected = True

    def __init__(self, response=OWNER):
        self.response = response
        self.calls = []

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        assert payload == STATUS_REQUEST
        assert route == EybondLinkRoute(devcode=1, collector_addr=255)
        self.calls.append((payload, route, request_timeout))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response

    def select_payload_route(self, *args, **kwargs):
        raise AssertionError("No implicit raw/AT route change")


class PayloadTests(unittest.TestCase):
    def test_public_sample(self):
        self.assertEqual(len(REFERENCE), 78)
        self.assertEqual(VphbStatus(REFERENCE).values(), {
            "protocol_id": PROTOCOL_ID,
            "grid_voltage": 235.4, "grid_frequency": 49.9,
            "output_voltage": 230.3, "output_frequency": 50.0,
            "battery_voltage": 26.3, "battery_soc": 100,
            "load_percent": 18, "temperature": 36,
            "pv_voltage": 75, "solar_charger_temperature": 31,
        })

    def test_owner_sample_is_offline_arithmetic_not_hardware_qualification(self):
        self.assertEqual(len(OWNER), 84)
        self.assertEqual(VphbStatus(OWNER).values(), {
            "protocol_id": PROTOCOL_ID,
            "grid_voltage": 0, "grid_frequency": 0,
            "output_voltage": 230.2, "output_frequency": 50,
            "battery_voltage": 53.4, "battery_soc": 82,
            "load_percent": 4, "temperature": 22,
            "pv_voltage": 0, "solar_charger_temperature": 16,
        })

    def test_complete_raw_evidence_includes_both_extension_variants(self):
        for raw, extra_size in ((REFERENCE, 6), (OWNER, 12)):
            evidence = VphbStatus(raw).evidence()
            self.assertEqual(bytes.fromhex(evidence["response_hex"]), raw)
            self.assertEqual(bytes.fromhex(evidence["vendor_window"]["payload_hex"]), raw[5:71])
            self.assertEqual(len(bytes.fromhex(evidence["unparsed_extension"]["payload_hex"])), extra_size)
            self.assertEqual(evidence["field_map_verification"], "experimental_replay_only")

    def test_reject_wrong_types_lengths_echo_nak_and_other_protocols(self):
        for raw in (None, OWNER.hex(), bytearray(OWNER), b"", OWNER[:-1], OWNER + b"\r",
                    OWNER * 2, OWNER[:72], REFERENCE[:72], STATUS_REQUEST, b"NAK\r",
                    b"(000.0 000.0 230.2 004 50.0 2.22 30.0 10000000\r"):
            with self.subTest(raw=raw), self.assertRaises(VphbError):
                VphbStatus(raw)

    def test_every_single_byte_corruption_rejected_including_unknown_tail(self):
        for original in (REFERENCE, OWNER):
            for offset in range(len(original)):
                raw = bytearray(original)
                raw[offset] ^= 1
                with self.subTest(length=len(raw), offset=offset), self.assertRaises(VphbError):
                    VphbStatus(bytes(raw))

    def test_no_battery_factor_or_derived_power_from_capacity_load_or_pv(self):
        for raw in (REFERENCE, OWNER):
            values = VphbStatus(raw).values()
            self.assertEqual(values["battery_voltage"], int.from_bytes(raw[25:27], "little") / 10)
            self.assertNotIn("pv_current", values)
            self.assertNotIn("solar_charge_current", values)
            self.assertNotIn("operating_mode", values)
            self.assertNotIn("serial_number", values)
            canonical = apply_canonical_measurements("eybond_099e", values)
            for key in ("pv_power", "output_power", "battery_power", "grid_power"):
                self.assertNotIn(key, canonical)
            self.assertFalse(derived_energy_descriptions_for_keys(tuple(values)))

    def test_schema_and_registration_are_manual_without_controls(self):
        driver = Eybond099EDriver()
        self.assertIn(driver.key, driver_options())
        self.assertEqual([d.key for d in iter_drivers(driver.key)], [driver.key])
        self.assertNotIn(driver.key, {d.key for d in iter_drivers("auto")})
        self.assertFalse(serial_is_stable(driver.key))
        self.assertEqual(driver.write_capabilities, ())
        self.assertEqual(driver.binary_sensors, ())
        self.assertFalse(driver.cloud_learning_supported)
        self.assertEqual(driver.support_marker().verification, "experimental_replay_only")
        keys = {d.key for d in driver.measurements}
        self.assertEqual(keys, set(VphbStatus(OWNER).values()))
        runtime_keys = {d.key for d in measurements_for_runtime(driver_key=driver.key)}
        self.assertTrue(keys <= runtime_keys)


class DriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_manual_detection_pipeline_never_uses_cloud_code_as_route(self):
        from custom_components.eybond_local.runtime.driver_detection import async_detect_inverter
        transport = Replay()
        context = await async_detect_inverter(transport, driver_hint="eybond_099e")
        self.assertEqual(context.inverter.driver_key, "eybond_099e")
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(context.inverter.probe_target.devcode, 1)
        self.assertEqual(context.inverter.probe_target.collector_addr, 255)
        self.assertEqual(context.inverter.serial_number, "")
        for key in (*VphbStatus(OWNER).values(), "response_hex"):
            if key != "protocol_id":
                self.assertNotIn(key, context.inverter.details)

    async def test_one_status_request_per_probe_poll_capture_and_no_writes(self):
        driver, transport = Eybond099EDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(inverter.capabilities, ())
        result = await driver.async_read_values(transport, inverter)
        self.assertEqual(result.mode, DriverReadMode.FULL)
        self.assertEqual(result.values, VphbStatus(OWNER).values())
        self.assertEqual(result.diagnostics["vphb_status_evidence"]["response_hex"], OWNER.hex())
        self.assertEqual(len(transport.calls), 2)
        capture = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(capture["responses_hex"], {"status": OWNER.hex()})
        self.assertEqual(capture["failures"], {})
        self.assertEqual(len(transport.calls), 3)
        with self.assertRaises(NotImplementedError):
            await driver.async_write_capability(transport, inverter, "anything", 1)
        self.assertEqual(len(transport.calls), 3)

    async def test_invalid_and_failed_reads_never_fallback_or_replay_old_values(self):
        driver, transport = Eybond099EDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        state = {}
        await driver.async_read_values(transport, inverter, runtime_state=state)
        for response, error in ((b"", VphbError), (OWNER[:-1], VphbError),
                                (ConnectionError(), ConnectionError), (TimeoutError(), TimeoutError)):
            transport.response = response
            before = len(transport.calls)
            with self.subTest(response=response), self.assertRaises(error):
                await driver.async_read_values(transport, inverter, runtime_state=state)
            self.assertEqual(len(transport.calls), before + 1)
            self.assertIsNone(await driver.async_probe(transport, driver.probe_targets[0]))
        self.assertEqual(state, {})
        transport.response = REFERENCE
        self.assertEqual((await driver.async_read_values(transport, inverter)).values["battery_voltage"], 26.3)

    async def test_capture_preserves_rejected_packet_and_failure(self):
        driver, transport = Eybond099EDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        transport.response = OWNER[:-1]
        capture = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(capture["responses_hex"]["status"], OWNER[:-1].hex())
        self.assertEqual(capture["failures"], {"status": "VphbError"})
        self.assertNotIn("vphb_status_evidence", capture)

    async def test_cancel_propagates_from_probe_poll_and_capture(self):
        driver, transport = Eybond099EDriver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        transport.response = asyncio.CancelledError()
        for call in (lambda: driver.async_probe(transport, driver.probe_targets[0]),
                     lambda: driver.async_read_values(transport, inverter),
                     lambda: driver.async_capture_support_evidence(transport, inverter)):
            with self.assertRaises(asyncio.CancelledError):
                await call()

    async def test_raw_route_is_rejected_before_any_transmission(self):
        transport = Replay()
        with self.assertRaisesRegex(VphbError, "requires_fc4"):
            await VphbSession(transport, RawSerialLinkRoute()).read_status()
        self.assertEqual(transport.calls, [])

    async def test_hung_sender_is_cancelled_without_retry(self):
        cancelled = asyncio.Event()
        calls = []

        class Hung:
            async def async_send_payload(self, payload, *, route, request_timeout=None):
                calls.append(payload)
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        with self.assertRaises(TimeoutError):
            await VphbSession(Hung(), Eybond099EDriver.probe_targets[0].link_route, timeout=0.01).read_status()
        self.assertTrue(cancelled.is_set())
        self.assertEqual(calls, [STATUS_REQUEST])
