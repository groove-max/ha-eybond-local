"""Q1-only manual admission, fixed FC4 route, and conservative telemetry."""

import asyncio
from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.canonical_telemetry import apply_canonical_measurements
from custom_components.eybond_local.derived_energy import derived_energy_descriptions_for_keys
from custom_components.eybond_local.drivers.eybond_q1 import EybondQ1Driver, PROTOCOL_ID
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.drivers.registry import driver_options, iter_drivers, serial_is_stable
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.payload.q1 import Q1Error, Q1Session, STATUS_REQUEST, decode_q1

# Public owner capture in #97; no collector/account identifiers.
OWNER = b"(249.6 000.0 234.5 000 60.0 2.34 30.0 10000000\r"
EXPECTED = {"protocol_id": PROTOCOL_ID, "grid_voltage": 249.6, "output_voltage": 234.5,
            "load_percent": 0.0, "q1_frequency": 60.0, "temperature": 30.0}


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
        raise AssertionError("No implicit raw/AT route selection")


class PayloadTests(unittest.TestCase):
    def test_owner_fields_and_negative_temperature(self):
        sample = decode_q1(OWNER)
        self.assertEqual(sample.battery_text, "2.34")
        self.assertEqual(sample.battery_value, 2.34)
        self.assertEqual(sample.flags, "10000000")
        self.assertEqual(sample.fault_voltage, 0)
        self.assertEqual(decode_q1(OWNER.replace(b"30.0", b"-1.2")).temperature, -1.2)
        self.assertEqual(decode_q1(OWNER.replace(b"2.34", b"56.2")).battery_value, 56.2)

    def test_reject_lengths_types_delimiters_echo_nak_fields_and_flags(self):
        for raw in (None, OWNER.hex(), bytearray(OWNER), b"", OWNER[:-1], OWNER + b"\n",
                    OWNER * 2, STATUS_REQUEST, b"NAK\r", OWNER.replace(b"(", b"#"),
                    OWNER.replace(b"\r", b"\n"), OWNER.replace(b"249.6", b"249,6"),
                    OWNER.replace(b"2.34", b"nan "), OWNER.replace(b"10000000", b"10000002")):
            with self.subTest(raw=raw), self.assertRaises(Q1Error):
                decode_q1(raw)
        for position in (6, 12, 18, 22, 27, 32, 37):
            with self.subTest(position=position), self.assertRaisesRegex(Q1Error, "separator"):
                decode_q1(OWNER[:position] + b"\t" + OWNER[position + 1:])

    def test_registry_is_explicit_and_read_only(self):
        driver = EybondQ1Driver()
        self.assertIn(driver.key, driver_options())
        self.assertEqual([d.key for d in iter_drivers(driver.key)], [driver.key])
        self.assertNotIn(driver.key, {d.key for d in iter_drivers("auto")})
        self.assertFalse(serial_is_stable(driver.key))
        self.assertEqual(driver.write_capabilities, ())
        self.assertEqual(driver.binary_sensors, ())
        self.assertFalse(driver.cloud_learning_supported)
        self.assertTrue(driver.support_marker().read_only)
        self.assertEqual({d.key for d in driver.measurements}, set(EXPECTED))
        canonical = apply_canonical_measurements(driver.key, EXPECTED)
        for key in ("battery_voltage", "battery_soc", "output_power", "pv_power", "operating_mode",
                    "grid_frequency", "output_frequency", "inverter_fault", "grid_available"):
            self.assertNotIn(key, canonical)
        self.assertFalse(derived_energy_descriptions_for_keys(tuple(EXPECTED)))


class DriverTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_detection_and_no_live_identity_fields(self):
        from custom_components.eybond_local.runtime.driver_detection import async_detect_inverter
        transport = Replay()
        context = await async_detect_inverter(transport, driver_hint="eybond_q1")
        self.assertEqual(context.inverter.driver_key, "eybond_q1")
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(context.inverter.serial_number, "")
        expected_details = {
            "protocol_id": PROTOCOL_ID, "selection": "manual_read_map",
            "field_map_verification": "experimental_replay_only",
        }
        self.assertEqual({key: context.inverter.details[key] for key in expected_details}, expected_details)
        for key in (*EXPECTED, "response_hex", "battery_voltage_unscaled", "status_flags"):
            if key != "protocol_id":
                self.assertNotIn(key, context.inverter.details)

    async def test_one_q1_per_operation_no_scaling_writes_or_other_commands(self):
        driver, transport = EybondQ1Driver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        self.assertEqual(inverter.capabilities, ())
        result = await driver.async_read_values(transport, inverter)
        self.assertEqual(result.mode, DriverReadMode.FULL)
        self.assertEqual(result.values, EXPECTED)
        evidence = result.diagnostics["q1_status_evidence"]
        self.assertEqual(evidence["response_hex"], OWNER.hex())
        self.assertEqual(evidence["battery_voltage_unscaled"], 2.34)
        self.assertEqual(evidence["status_flags"], "10000000")
        capture = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(capture["q1_status_evidence"], evidence)
        self.assertEqual(capture["responses_hex"], {"Q1": OWNER.hex()})
        self.assertEqual(capture["failures"], {})
        self.assertEqual(len(transport.calls), 3)
        with self.assertRaisesRegex(NotImplementedError, "q1_read_only"):
            await driver.async_write_capability(transport, inverter, "anything", 1)
        self.assertEqual(len(transport.calls), 3)

    async def test_failed_read_has_no_stale_cache_and_capture_keeps_rejected_bytes(self):
        driver, transport = EybondQ1Driver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        state = {}
        await driver.async_read_values(transport, inverter, runtime_state=state)
        for response, error in ((b"", Q1Error), (OWNER[:-1], Q1Error),
                                (ConnectionError(), ConnectionError), (TimeoutError(), TimeoutError)):
            transport.response = response
            before = len(transport.calls)
            with self.subTest(response=response), self.assertRaises(error):
                await driver.async_read_values(transport, inverter, runtime_state=state)
            self.assertEqual(len(transport.calls), before + 1)
            self.assertIsNone(await driver.async_probe(transport, driver.probe_targets[0]))
        self.assertEqual(state, {})
        transport.response = OWNER[:-1]
        capture = await driver.async_capture_support_evidence(transport, inverter)
        self.assertEqual(capture["responses_hex"], {"Q1": OWNER[:-1].hex()})
        self.assertEqual(capture["failures"], {"Q1": "Q1Error"})
        self.assertNotIn("q1_status_evidence", capture)
        transport.response = OWNER.replace(b"2.34", b"56.2")
        self.assertNotIn("battery_voltage", (await driver.async_read_values(transport, inverter)).values)

    async def test_cancellation_propagates_and_raw_route_is_not_sent(self):
        driver, transport = EybondQ1Driver(), Replay()
        inverter = await driver.async_probe(transport, driver.probe_targets[0])
        transport.response = asyncio.CancelledError()
        for call in (lambda: driver.async_probe(transport, driver.probe_targets[0]),
                     lambda: driver.async_read_values(transport, inverter),
                     lambda: driver.async_capture_support_evidence(transport, inverter)):
            with self.assertRaises(asyncio.CancelledError):
                await call()
        transport = Replay()
        with self.assertRaisesRegex(Q1Error, "requires_fc4"):
            await Q1Session(transport, RawSerialLinkRoute()).read_status()
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
            await Q1Session(Hung(), EybondQ1Driver.probe_targets[0].link_route, timeout=0.01).read_status()
        self.assertTrue(cancelled.is_set())
        self.assertEqual(calls, [STATUS_REQUEST])


if __name__ == "__main__":
    unittest.main()
