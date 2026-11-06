"""Fixed BLE metadata queries, including negative/partial identity evidence."""

import asyncio
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock

from custom_components.eybond_local.collector.smartess_ble import (
    PROXY_LAYOUT, SmartEssBleSession, VENDOR_LAYOUT,
)
from custom_components.eybond_local.support.ble_inspection import BleInspection
from custom_components.eybond_local.support.diagnostic_export import build_shareable_payload
from test_smartess_ble import _FakeBleLink


PN = "E50000200000000001"
REPLIES = [f"AT+DTUPN:{PN}", "AT+FWVER:1.2.3", "AT+ATVER:2.1",
           "AT+CLDSRVHOST1:example.invalid,18899,TCP"]


class BleInspectionTests(unittest.IsolatedAsyncioTestCase):
    def test_bluetooth_is_declared_optional_not_required_for_classic_setup(self):
        path = Path(__file__).resolve().parents[1] / "custom_components/eybond_local/manifest.json"
        manifest = json.loads(path.read_text())
        self.assertIn("bluetooth", manifest["after_dependencies"])
        self.assertNotIn("bluetooth", manifest["dependencies"])

    async def test_fixed_queries_only_over_existing_session(self):
        for crlf in (False, True):
            with self.subTest(crlf=crlf):
                link = _FakeBleLink([VENDOR_LAYOUT.service_uuid],
                                    read_results=[value.encode() for value in REPLIES])
                session = SmartEssBleSession(link)
                attempt = BleInspection(PN, append_crlf=crlf)
                try:
                    await attempt.run(session)
                finally:
                    await session.disconnect()
                self.assertTrue(attempt.result(address="AA:BB:CC:DD:EE:FF").success)
                self.assertEqual([item[1] for item in link.write_calls], [
                    command.encode() + (b"\r\n" if crlf else b"") for command in (
                        "AT+DTUPN?", "AT+FWVER?", "AT+ATVER?", "AT+CLDSRVHOST1?",
                    )])
                self.assertTrue(all(item[2] for item in link.write_calls))
                self.assertFalse(link.connected)

    async def run_replies(self, replies, pn=PN):
        session = AsyncMock()
        session.connect.return_value = VENDOR_LAYOUT
        session.exchange_text.side_effect = replies
        attempt = BleInspection(pn)
        await attempt.run(session)
        return attempt, session

    async def test_mismatch_stops_after_identity(self):
        attempt, session = await self.run_replies(REPLIES, pn="Q00000000000010001")
        self.assertEqual(session.exchange_text.await_count, 1)
        self.assertEqual(attempt.error, "collector_identity_mismatch")
        self.assertFalse(attempt.result(address="test").success)

    async def test_missing_identity_retains_partial_metadata_not_success(self):
        attempt, _ = await self.run_replies([TimeoutError(), *REPLIES[1:]])
        self.assertEqual(attempt.identity_status, "unavailable")
        self.assertEqual(attempt.observations[1]["value"], "1.2.3")
        self.assertFalse(attempt.result(address="test").success)

    async def test_host_only_does_not_invent_port_or_protocol(self):
        attempt, _ = await self.run_replies([*REPLIES[:3], "AT+CLDSRVHOST1:example.invalid"])
        endpoint = attempt.observations[3]["value"]
        self.assertIsNone(endpoint["port"])
        self.assertIsNone(endpoint["protocol"])
        self.assertFalse(endpoint["port_reported"])

    async def test_unparsed_bytes_not_retained_and_echo_is_not_version(self):
        for reply in ("AT+FWVER?", "AT+FWVER:R001", "AT+FWVER:" + "x" * 513,
                      "AT+PASS:private-secret", "AT+FWVER:1.0\r\nAT+PASS:secret"):
            with self.subTest(reply=reply[:20]):
                attempt, _ = await self.run_replies([REPLIES[0], reply, *REPLIES[2:]])
                self.assertEqual(attempt.observations[1]["status"], "unrecognized_reply")
                artifact = build_shareable_payload(attempt.result(address="AA:BB:CC:DD:EE:FF"))
                encoded = json.dumps(artifact)
                self.assertNotIn("secret", encoded)
                self.assertNotIn(PN, encoded)
                self.assertNotIn("AA:BB:CC:DD:EE:FF", encoded)

    async def test_typed_read_status_and_no_missing_firmware_default(self):
        attempt, _ = await self.run_replies([REPLIES[0], "AT+FWVER?:R001", *REPLIES[2:]])
        self.assertEqual(attempt.observations[1]["status"], "query_status")
        self.assertNotIn("value", attempt.observations[1])

    async def test_unsupported_layout_sends_nothing(self):
        session = AsyncMock()
        session.connect.return_value = PROXY_LAYOUT
        attempt = BleInspection(PN)
        await attempt.run(session)
        session.exchange_text.assert_not_called()
        self.assertEqual(attempt.error, "unsupported_ble_layout")

    async def test_connect_failure_is_not_success(self):
        for failure, expected in ((TimeoutError(), "ble_connect_timeout"),
                                  (RuntimeError("untrusted details"), "ble_connection_failed")):
            session = AsyncMock()
            session.connect.side_effect = failure
            attempt = BleInspection(PN)
            await attempt.run(session)
            self.assertEqual(attempt.error, expected)
            session.exchange_text.assert_not_called()

    async def test_cancel_is_preserved_with_partial_evidence(self):
        session = AsyncMock()
        session.connect.return_value = VENDOR_LAYOUT
        session.exchange_text.side_effect = asyncio.CancelledError()
        attempt = BleInspection(PN)
        with self.assertRaises(asyncio.CancelledError):
            await attempt.run(session)
        self.assertEqual(attempt.error, "cancelled")
        self.assertEqual(attempt.observations[0]["status"], "cancelled")

    async def test_unexpected_gatt_error_finalizes_pending_query(self):
        attempt, session = await self.run_replies([REPLIES[0], RuntimeError("GATT error")])
        self.assertEqual(attempt.error, "ble_transport_error")
        self.assertEqual(attempt.observations[-1]["status"], "transport_error")
        self.assertEqual(session.exchange_text.await_count, 2)
