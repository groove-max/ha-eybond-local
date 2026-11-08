"""Real fixed-query inspector/session with synthetic radio and private payloads."""

import asyncio
import json
import unittest
from unittest.mock import patch

from custom_components.eybond_local.collector.ble_diagnostics import BleExchangeDiagnostics
from custom_components.eybond_local.collector.smartess_ble import SmartEssBleSession, VENDOR_LAYOUT
from custom_components.eybond_local.support.ble_inspection import BleInspection
from custom_components.eybond_local.support.diagnostic_export import build_shareable_payload
from test_smartess_ble import _FakeBleLink


PN = "E50000200000000001"
REPLIES = {
    "AT+DTUPN?": f"AT+DTUPN:{PN}\r\n",
    "AT+FWVER?": "AT+FWVER:8.50.12.3\r\n",
    "AT+ATVER?": "AT+ATVER:1.11\r\n",
    "AT+CLDSRVHOST1?": "AT+CLDSRVHOST1:example.invalid,502,TCP\r\n",
}


class InspectLink(_FakeBleLink):
    def __init__(self, mode):
        super().__init__([VENDOR_LAYOUT.service_uuid])
        self.mode = mode
        self.reply = b""

    async def write(self, uuid, data, *, response=False):
        await super().write(uuid, data, response=response)
        self.reply = REPLIES[data.decode()].encode()
        if self.mode == "write_timeout":
            await asyncio.Future()
        if self.mode == "write_error":
            raise RuntimeError("private-password-in-backend-error")
        if self.mode == "complete":
            self.push_notification(self.reply)
        elif self.mode == "split":
            self.push_notification(self.reply[:4])
            self.push_notification(self.reply[4:])
        elif self.mode == "partial":
            self.push_notification(self.reply[:4])
        elif self.mode == "unrelated":
            self.push_notification(b"AT+PASS:private-password-from-notify\r\n")
        elif self.mode.startswith("pn_"):
            is_pn = data == b"AT+DTUPN?"
            first = len(self.write_calls) == 1
            if is_pn and (first or self.mode in {"pn_missing", "pn_cached"}):
                # Synthetic unmatched traffic, not the owner's unknown bytes.
                self.push_notification(b"--------")
                self.push_notification(data)
                self.push_notification(b"11111111\x01\x03\x02\0\1")
                self.push_notification(b"AT+PASS:private-password-from-notify\r\n")
            elif is_pn and self.mode == "pn_mismatch":
                self.push_notification(b"AT+DTUPN:Q00000000000010001\r\n")
            elif is_pn and self.mode == "pn_cancel":
                raise asyncio.CancelledError()
            else:
                self.push_notification(self.reply)

    async def read(self, uuid):
        self.read_calls.append(uuid)
        if self.mode == "read_error":
            raise RuntimeError("private-password-from-read-error")
        if self.mode == "read_timeout":
            await asyncio.Future()
        if self.mode == "pn_cached" and len(self.write_calls) == 5:
            return self.reply  # A recheck must not accept this cached PN.
        return self.reply if self.mode == "read_reply" else b""


class BleInspectionTransportTests(unittest.IsolatedAsyncioTestCase):
    async def inspect(self, mode):
        link = InspectLink(mode)
        session = SmartEssBleSession(link)
        attempt = BleInspection(PN, append_crlf=False)
        with patch("custom_components.eybond_local.support.ble_inspection.QUERY_TIMEOUT", .15), \
             patch("custom_components.eybond_local.collector.smartess_ble._VENDOR_NOTIFY_POLL_TIMEOUT", .02), \
             patch("custom_components.eybond_local.collector.smartess_ble._VENDOR_READ_POLL_TIMEOUT", .02):
            try:
                await attempt.run(session)
            finally:
                await session.disconnect()
        artifact = build_shareable_payload(attempt.result(address="AA:BB:CC:DD:EE:FF"))
        encoded = json.dumps(artifact)
        self.assertNotIn("private-password", encoded)
        self.assertNotIn(PN, encoded)
        self.assertNotIn("AA:BB:CC:DD:EE:FF", encoded)
        self.assertIsNone(session._exchange_diagnostics)
        self.assertFalse(link.connected)
        self.assertEqual(artifact["context"]["connection"]["status"], "ready")
        self.assertFalse(artifact["context"]["settings_changed"])
        self.assertLessEqual(len(link.write_calls), 5)
        expected = [*REPLIES, "AT+DTUPN?"] if mode.startswith("pn_") else list(REPLIES)
        self.assertEqual([item[1].decode() for item in link.write_calls], expected[:len(link.write_calls)])
        self.assertTrue(all(item[2] for item in link.write_calls))
        return artifact, link

    async def test_initial_pn_timeout_rechecked_once_after_successful_metadata(self):
        result, link = await self.inspect("pn_recovered")
        self.assertTrue(result["success"])
        self.assertEqual(len(link.write_calls), 5)
        self.assertEqual(result["context"]["identity_status"], "matched")
        first, *_, retry = result["results"]
        self.assertEqual(first["status"], "timeout")
        self.assertEqual(first["attempt"], 1)
        self.assertEqual(retry["status"], "received")
        self.assertEqual(retry["attempt"], 2)
        self.assertEqual(retry["reason"], "identity_after_metadata")
        self.assertEqual(retry["transport"]["reply_source"], "notify")
        self.assertEqual(retry["transport"]["read_attempts"], 0)
        self.assertEqual(first["transport"]["ignored_notify"], {
            "placeholder": 1, "command_echo": 1, "binary_or_control": 1,
            "unmatched_text": 1,
        })

    async def test_pn_recheck_does_not_override_mismatch_or_keep_retrying(self):
        for mode, error, identity in (("pn_missing", "partial_or_unbound", "unavailable"),
                                      ("pn_cached", "partial_or_unbound", "unavailable"),
                                      ("pn_mismatch", "collector_identity_mismatch", "mismatch")):
            with self.subTest(mode=mode):
                result, link = await self.inspect(mode)
                self.assertFalse(result["success"])
                self.assertEqual(result["error"], error)
                self.assertEqual(result["context"]["identity_status"], identity)
                self.assertEqual(len(link.write_calls), 5)
                self.assertEqual(result["results"][-1]["transport"]["read_attempts"], 0)

    async def test_retry_cancellation_preserves_first_attempt_and_cleans_up(self):
        link = InspectLink("pn_cancel")
        session = SmartEssBleSession(link)
        attempt = BleInspection(PN)
        with patch("custom_components.eybond_local.support.ble_inspection.QUERY_TIMEOUT", .03):
            try:
                with self.assertRaises(asyncio.CancelledError):
                    await attempt.run(session)
            finally:
                await session.disconnect()
        self.assertEqual(len(link.write_calls), 5)
        self.assertEqual(attempt.error, "cancelled")
        self.assertEqual(attempt.observations[0]["status"], "timeout")
        self.assertEqual(attempt.observations[-1]["status"], "cancelled")
        self.assertIsNone(session._exchange_diagnostics)
        self.assertFalse(link.connected)

    async def test_complete_and_split_notifications_succeed_without_snapshot_reads(self):
        for mode, count in (("complete", 1), ("split", 2)):
            with self.subTest(mode=mode):
                result, link = await self.inspect(mode)
                self.assertTrue(result["success"])
                self.assertEqual(link.read_calls, [])
                self.assertEqual(len(result["results"]), 4)
                for row in result["results"]:
                    trace = row["transport"]
                    self.assertEqual(row["status"], "received")
                    self.assertEqual(trace["phase"], "reply_complete")
                    self.assertTrue(trace["write_completed"])
                    self.assertEqual(trace["notifications_received"], count)
                    self.assertEqual(trace["notifications_consumed"], count)
                    self.assertGreater(trace["notification_bytes"], 4)
                    self.assertEqual(trace["matched_notify_bytes"], trace["notification_bytes"])
                    self.assertEqual(trace["read_attempts"], 0)
                    self.assertEqual(trace["reply_source"], "notify")

    async def test_read_fallback_is_explicit_not_notification_evidence(self):
        result, _ = await self.inspect("read_reply")
        self.assertTrue(result["success"])
        for row in result["results"]:
            trace = row["transport"]
            self.assertEqual(trace["notifications_received"], 0)
            self.assertEqual(trace["read_completed"], 1)
            self.assertGreater(trace["matched_read_bytes"], 0)
            self.assertEqual(trace["reply_source"], "read")

    async def test_silence_partial_and_unrelated_bytes_are_distinguishable(self):
        for mode in ("silent", "partial", "unrelated"):
            with self.subTest(mode=mode):
                result, _ = await self.inspect(mode)
                self.assertEqual(result["error"], "partial_or_unbound")
                for row in result["results"]:
                    trace = row["transport"]
                    self.assertEqual(row["status"], "timeout")
                    self.assertEqual(trace["phase"], "receiving")
                    self.assertTrue(trace["write_completed"])
                    self.assertEqual(trace["notifications_received"], 0 if mode == "silent" else 1)
                    self.assertEqual(trace["matched_notify_bytes"], 4 if mode == "partial" else 0)
                    self.assertEqual(trace["reply_source"], "none")
                    self.assertGreater(trace["elapsed_ms"], 0)
                    if mode == "partial":
                        self.assertEqual(trace["read_attempts"], 0)
                    else:
                        self.assertGreater(trace["read_empty"], 0)

    async def test_write_timeout_is_not_misreported_as_silence_after_completed_write(self):
        result, _ = await self.inspect("write_timeout")
        for row in result["results"]:
            self.assertEqual(row["status"], "timeout")
            self.assertEqual(row["transport"]["phase"], "writing")
            self.assertFalse(row["transport"]["write_completed"])
            self.assertEqual(row["transport"]["read_attempts"], 0)

    async def test_write_failure_stops_without_exporting_exception_text(self):
        result, link = await self.inspect("write_error")
        self.assertEqual(result["error"], "ble_transport_error")
        self.assertEqual(len(link.write_calls), 1)
        self.assertEqual(result["results"][0]["transport"]["phase"], "writing")

    async def test_read_errors_and_timeouts_remain_separate_and_payload_free(self):
        for mode, counter in (("read_error", "read_errors"), ("read_timeout", "read_timeouts")):
            with self.subTest(mode=mode):
                result, _ = await self.inspect(mode)
                for row in result["results"]:
                    trace = row["transport"]
                    self.assertEqual(row["status"], "timeout")
                    self.assertGreater(trace[counter], 0)
                    self.assertEqual(trace["read_completed"], 0)
                    self.assertEqual(trace["notifications_received"], 0)

    async def test_cancel_finalizes_query_trace_and_does_not_leak_into_next_exchange(self):
        link = InspectLink("write_timeout")
        session = SmartEssBleSession(link)
        attempt = BleInspection(PN)
        task = asyncio.create_task(attempt.run(session))
        try:
            for _ in range(30):
                if link.write_calls:
                    break
                await asyncio.sleep(0)
            self.assertEqual(len(link.write_calls), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            trace = attempt.observations[0]["transport"]
            self.assertEqual(attempt.observations[0]["status"], "cancelled")
            self.assertEqual(trace["phase"], "writing")
            self.assertFalse(trace["write_completed"])
            link.mode = "complete"
            next_trace = BleExchangeDiagnostics()
            await session.exchange_text("AT+FWVER?", append_crlf=False, diagnostics=next_trace)
            self.assertEqual(next_trace.notifications_received, 1)
            self.assertEqual(trace["notifications_received"], 0)
        finally:
            await session.disconnect()

    def test_trace_is_fixed_field_and_detached(self):
        trace = BleExchangeDiagnostics()
        trace.ignored_notify["placeholder"] = 1
        self.assertFalse(hasattr(trace, "__dict__"))
        exported = trace.snapshot()
        trace.notifications_received = 1
        trace.ignored_notify["placeholder"] = 2
        self.assertEqual(exported["notifications_received"], 0)
        self.assertEqual(exported["ignored_notify"], {"placeholder": 1})
        self.assertEqual({key for key, value in exported.items() if isinstance(value, str)},
                         {"phase", "reply_source"})
