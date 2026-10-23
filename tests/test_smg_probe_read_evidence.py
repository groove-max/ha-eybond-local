"""Preserve failure location without changing SMG admission or polling policy."""

import asyncio
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.drivers.probe_evidence import ProbeReadSession
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import crc16_modbus, decode_read_request
from custom_components.eybond_local.runtime.hub.refresh import _sanitize_probe_diagnostic


FIXTURE = json.loads((Path(__file__).parent / "fixtures/anenji_6200_issue66_registers.json").read_text())
TARGET = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)


class Replay:
    """Real supplied words plus explicitly synthetic missing-register behavior."""

    def __init__(self, *, fail_at=None, error=None):
        self.words = {int(k): v for k, v in FIXTURE["identity"].items()}
        for key in ("live", "config"):
            self.words.update(enumerate(FIXTURE[key], FIXTURE[f"{key}_start"]))
        # The public file deliberately excludes the serial. Blank is accepted
        # after a successful read, but is NOT the owner's observed serial.
        self.words.update({i: 0 for i in range(186, 198)})
        self.fail_at, self.error = fail_at, error
        self.requests = []

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        assert route == TARGET.link_route
        request = decode_read_request(payload)
        assert request is not None and request.function_code == 3
        self.requests.append((request.address, request.count))
        if request.address == self.fail_at:
            raise self.error
        addresses = range(request.address, request.address + request.count)
        if any(i not in self.words for i in addresses):
            # Synthetic illegal-address reply, not a field observation.
            body = bytes((1, 0x83, 2))
        else:
            body = bytes((1, 3, request.count * 2)) + b"".join(
                self.words[i].to_bytes(2, "big") for i in addresses
            )
        return body + crc16_modbus(body).to_bytes(2, "little")


class SmgProbeReadEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_anenji_catalog_binds_supplied_baseline(self):
        replay = Replay()
        attempt = await SmgModbusDriver().async_probe_with_evidence(replay, TARGET)
        self.assertEqual(attempt.inverter.model_name, "Anenji 6200")
        self.assertEqual(attempt.inverter.details["rated_power"], 6200)
        self.assertEqual(attempt.inverter.details["output_rating_voltage"], 230)
        self.assertEqual(attempt.inverter.details["device_catalog"]["kind"], "device")
        self.assertIsNone(attempt.no_match_evidence)
        # Tolerated optional exceptions do not become admission failures.
        self.assertEqual(replay.requests[:5], [(171, 1), (184, 1), (186, 12), (201, 34), (300, 44)])

    async def test_core_and_auxiliary_timeouts_identify_only_failed_read(self):
        for address, count in ((186, 12), (201, 34), (300, 44), (406, 1)):
            with self.subTest(address=address):
                replay = Replay(fail_at=address, error=TimeoutError("private endpoint/account"))
                attempt = await SmgModbusDriver().async_probe_with_evidence(replay, TARGET)
                self.assertIsNone(attempt.inverter)
                evidence = attempt.no_match_evidence.as_public_dict()
                action = f"modbus_smg.runtime.read_holding.{address}.{count}"
                self.assertEqual(evidence["status"], "runtime_validation_failed")
                self.assertEqual(evidence["candidate_keys"], ["anenji_6200"])
                self.assertEqual(evidence["action_failures"], [{"action": action, "reason": "timeout"}])
                self.assertEqual(evidence["failed_actions"], [action])
                self.assertNotIn("private", json.dumps(evidence))
                # Existing exact+family attempts, with the existing two Modbus
                # tries each. The observer must not add its own retries.
                self.assertEqual(replay.requests.count((address, count)), 4)
                self.assertEqual(_sanitize_probe_diagnostic(evidence), evidence)

    async def test_disconnect_reason_survives_hub_filter_without_exception_text(self):
        attempt = await SmgModbusDriver().async_probe_with_evidence(
            Replay(fail_at=420, error=ConnectionError("private 192.0.2.66 password=secret")), TARGET,
        )
        evidence = attempt.no_match_evidence.as_public_dict()
        self.assertEqual(evidence["action_failures"], [{
            "action": "modbus_smg.runtime.read_holding.420.1", "reason": "disconnected",
        }])
        self.assertEqual(_sanitize_probe_diagnostic(evidence), evidence)
        self.assertNotIn("private", json.dumps(evidence))

    async def test_late_metadata_failure_does_not_add_a_family_retry(self):
        replay = Replay(fail_at=172, error=ConnectionError("private"))
        attempt = await SmgModbusDriver().async_probe_with_evidence(replay, TARGET)
        self.assertIsNone(attempt.inverter)
        self.assertEqual(attempt.no_match_evidence.action_failures[0].action_key,
                         "modbus_smg.runtime.read_holding.172.12")
        self.assertEqual(replay.requests.count((186, 12)), 1)
        self.assertEqual(replay.requests.count((172, 12)), 1)

    async def test_failed_validation_is_not_misreported_as_a_read_failure(self):
        replay = Replay()
        replay.words[320] = 0
        attempt = await SmgModbusDriver().async_probe_with_evidence(replay, TARGET)
        self.assertIsNone(attempt.inverter)
        self.assertEqual(attempt.no_match_evidence.status, "runtime_validation_failed")
        self.assertEqual(attempt.no_match_evidence.action_failures, ())

    async def test_unrelated_error_after_optional_read_keeps_original_exception(self):
        driver = SmgModbusDriver()

        async def unrelated_error(session, target, catalog_probe):
            try:
                await session.read_holding(406, 1)
            except Exception:
                pass  # Simulate a tolerated optional read before unrelated code.
            raise ValueError("synthetic decoder failure")

        with patch.object(driver, "_async_probe_with_catalog", side_effect=unrelated_error):
            with self.assertRaisesRegex(ValueError, "synthetic decoder failure"):
                await driver.async_probe_with_evidence(Replay(), TARGET)

    async def test_cancellation_and_parallel_attempts_do_not_leak_probe_state(self):
        driver = SmgModbusDriver()
        with self.assertRaises(asyncio.CancelledError):
            await driver.async_probe_with_evidence(
                Replay(fail_at=201, error=asyncio.CancelledError()), TARGET,
            )
        failed, matched = await asyncio.gather(
            driver.async_probe_with_evidence(Replay(fail_at=201, error=ConnectionError()), TARGET),
            driver.async_probe_with_evidence(Replay(), TARGET),
        )
        self.assertIsNotNone(failed.no_match_evidence)
        self.assertIsNotNone(matched.inverter)
        self.assertIsNone(matched.no_match_evidence)

    async def test_trace_retains_only_explicitly_reported_failure_and_bounds_size(self):
        class Session:
            async def read_holding(self, address, count):
                raise ValueError("raw payload/secret must not survive")
        trace = ProbeReadSession(Session(), prefix="synthetic.runtime")
        for address in range(35):
            with self.assertRaises(ValueError):
                await trace.read_holding(address, 1)
            if address:
                trace.retain_failure()
        self.assertEqual(len(trace.failures), 32)
        self.assertNotIn("synthetic.runtime.read_holding.0.1", trace.failures)
        self.assertEqual(trace.last_failure.reason, "read_error")
