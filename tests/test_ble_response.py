"""Regressions for real BLE notify/ReadValue races, without physical hardware."""

import asyncio
import unittest
from unittest.mock import patch

from custom_components.eybond_local.collector.ble_response import MAX_REPLY_BYTES, VendorAtReply
from custom_components.eybond_local.collector.smartess_ble import (
    SmartEssBleError, SmartEssBleSession, VENDOR_LAYOUT,
)
from test_smartess_ble import _FakeBleLink


class VendorAtReplyTests(unittest.TestCase):
    def feed(self, reply, raw, source="notify", now=0.0):
        reply.feed(raw, source=source, now=now)

    def result(self, reply, now=1.0):
        return reply.result(now=now, settle=0.5)

    def test_marker_and_missing_value_are_not_replies(self):
        for raw in (b"AT+FWVER", b"AT+FWVER:", b"AT+FWVER:\r\n", b"AT+FWVER?"):
            reply = VendorAtReply("AT+FWVER?")
            self.feed(reply, raw)
            self.assertIsNone(self.result(reply), raw)

    def test_split_marker_value_and_terminator(self):
        reply = VendorAtReply("AT+FWVER?")
        for raw in (b"AT+FWVER", b":8.50", b".12.3\r", b"\n"):
            self.feed(reply, raw)
        self.assertEqual(self.result(reply, now=0), "AT+FWVER:8.50.12.3")

    def test_fragment_sources_are_not_concatenated(self):
        reply = VendorAtReply("AT+FWVER?")
        self.feed(reply, b"AT+FWVER", "notify")
        self.feed(reply, b":old\r\n", "read")
        self.assertIsNone(self.result(reply))
        self.feed(reply, b":8.50.12.3\r\n", "notify")
        self.assertEqual(self.result(reply), "AT+FWVER:8.50.12.3")

    def test_partial_notification_is_not_overridden_by_read_snapshot(self):
        reply = VendorAtReply("AT+ATVER?")
        self.feed(reply, b"AT+ATVER:1.", "notify")
        self.feed(reply, b"AT+ATVER:9.99\r\n", "read")
        self.assertIsNone(self.result(reply, now=0.1))
        self.feed(reply, b"11\r\n", "notify")
        self.assertEqual(self.result(reply), "AT+ATVER:1.11")

    def test_snapshot_stale_tail_rejected_without_clean_notify(self):
        reply = VendorAtReply("AT+ATVER?")
        self.feed(reply, b"AT+ATVER:1.11\r\n2.3\r\n", "read")
        self.assertIsNone(self.result(reply))

    def test_cumulative_read_replaces_previous_snapshot(self):
        reply = VendorAtReply("AT+FWVER?")
        for raw in (b"AT+FWVER", b"AT+FWVER:8.50", b"AT+FWVER:8.50.12.3\r\n"):
            self.feed(reply, raw, "read")
        self.assertEqual(self.result(reply), "AT+FWVER:8.50.12.3")

    def test_legacy_split_header_reads_still_work(self):
        reply = VendorAtReply("AT+WFLKAP=Network,AES,WPA2_PSK,private")
        self.feed(reply, b"AT+WFLKA", "read")
        self.feed(reply, b"P:W000\r\n", "read")
        self.assertEqual(self.result(reply), "AT+WFLKAP:W000")

    def test_bare_or_partial_write_status_is_not_success(self):
        for raw in (b"AT+WFLKAP:", b"AT+WFLKAP:W0", b"AT+WFLKAP:W00\r\n"):
            reply = VendorAtReply("AT+WFLKAP=Network,AES,WPA2_PSK,private")
            self.feed(reply, raw)
            self.assertIsNone(self.result(reply), raw)

    def test_unterminated_ssid_settles_without_losing_split_utf8(self):
        reply = VendorAtReply("AT+INTPARA41?")
        raw = "AT+INTPARA:41,Дім-----WiFi".encode()
        self.feed(reply, raw[:14])
        self.feed(reply, raw[14:])
        self.assertIsNone(self.result(reply, now=0.1))
        self.assertEqual(self.result(reply), raw.decode())

    def test_other_parameter_echo_and_binary_are_not_values(self):
        reply = VendorAtReply("AT+INTPARA4?")
        for raw in (b"AT+INTPARA:41,wrong", b"AT+INTPARA4?", b"11111111\x01\x03\x02\0\1"):
            self.feed(reply, raw)
        self.assertIsNone(self.result(reply))
        self.feed(reply, b"AT+INTPARA:4,correct\r\n")
        self.assertEqual(self.result(reply), "AT+INTPARA:4,correct")

    def test_scan_snapshots_do_not_duplicate_networks(self):
        reply = VendorAtReply("AT+INTPARA49?")
        for raw in (b"49,[Home,-48]", b"49,[Home,-48]", b"49,[Home,-48][Office,-70]", b"------"):
            self.feed(reply, raw, "read")
        self.assertEqual(self.result(reply), "AT+INTPARA:49,[Home,-48][Office,-70]")

    def test_scan_partial_record_is_not_complete(self):
        reply = VendorAtReply("AT+INTPARA49?")
        self.feed(reply, b"AT+INTPARA:49,[Home,-48")
        self.feed(reply, b"------")
        self.assertIsNone(reply.result(now=10, settle=0.5, deadline=True))

    def test_fragment_and_single_packet_size_are_bounded(self):
        reply = VendorAtReply("AT+INTPARA41?")
        self.feed(reply, reply.prefix + b"a" * (MAX_REPLY_BYTES-len(reply.prefix)))
        with self.assertRaisesRegex(ValueError, "ble_response_too_large"):
            self.feed(reply, b"a")
        with self.assertRaisesRegex(ValueError, "ble_response_too_large"):
            self.feed(VendorAtReply("AT+FWVER?"), b"a" * (MAX_REPLY_BYTES+1))

    def test_empty_heartbeat_is_allowed_but_not_empty_identity(self):
        for name in ("HTBT", "DTUPN"):
            reply = VendorAtReply(f"AT+{name}?")
            self.feed(reply, f"AT+{name}:\r\n".encode())
            self.assertEqual(self.result(reply), "AT+HTBT:" if name == "HTBT" else None)

    def test_firmware_query_status_remains_classifiable(self):
        reply = VendorAtReply("AT+FWVER?")
        self.feed(reply, b"AT+FWVER?:R001\r\n")
        self.assertEqual(self.result(reply), "AT+FWVER:R001")


class VendorExchangeTests(unittest.IsolatedAsyncioTestCase):
    async def test_notification_only_does_not_accept_matching_cached_value(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid], read_results=[b'AT+CLDSRVHOST1:old.example,502,TCP\r\n'])
        session = await self.session(link)
        with self.assertRaisesRegex(SmartEssBleError, 'notification_timeout'):
            await session.exchange_text('AT+CLDSRVHOST1?', timeout=.03, allow_read_fallback=False)
        self.assertEqual(link.read_calls, [])

    async def test_notification_only_handles_a_complete_split_reply(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
        session = await self.session(link)
        task = asyncio.create_task(session.exchange_text('AT+DTUPN?', timeout=.5, allow_read_fallback=False))
        await asyncio.sleep(0)
        link.push_notification(b'AT+DTU')
        link.push_notification(b'PN:E5000099990001\r\n')
        self.assertEqual(await task, 'AT+DTUPN:E5000099990001')
        self.assertEqual(link.read_calls, [])

    async def asyncSetUp(self):
        self.poll = patch("custom_components.eybond_local.collector.smartess_ble._VENDOR_NOTIFY_POLL_TIMEOUT", 0.005)
        self.poll.start()
        self.addCleanup(self.poll.stop)

    async def session(self, link):
        session = SmartEssBleSession(link)
        await session.connect()
        self.addAsyncCleanup(session.disconnect)
        return session

    async def test_notify_arriving_during_snapshot_read_wins(self):
        class RacingLink(_FakeBleLink):
            async def read(self, uuid):
                self.push_notification(b"AT+ATVER:1.11\r\n")
                return b"AT+ATVER:1.11\r\n2.3\r\n"
        session = await self.session(RacingLink([VENDOR_LAYOUT.service_uuid]))
        self.assertEqual(await session.exchange_text("AT+ATVER?", timeout=0.1), "AT+ATVER:1.11")

    async def test_notify_is_used_even_if_inflight_read_fails(self):
        class RacingLink(_FakeBleLink):
            async def read(self, uuid):
                self.push_notification(b"AT+ATVER:1.11\r\n")
                raise RuntimeError("read unavailable")
        session = await self.session(RacingLink([VENDOR_LAYOUT.service_uuid]))
        self.assertEqual(await session.exchange_text("AT+ATVER?", timeout=0.1), "AT+ATVER:1.11")

    async def test_unrelated_notify_does_not_discard_valid_snapshot(self):
        class RacingLink(_FakeBleLink):
            async def read(self, uuid):
                self.push_notification(b"AT+LINK:W301\r\n")
                return b"AT+ATVER:1.11\r\n"
        session = await self.session(RacingLink([VENDOR_LAYOUT.service_uuid]))
        self.assertEqual(await session.exchange_text("AT+ATVER?", timeout=0.1), "AT+ATVER:1.11")

    async def test_split_bare_marker_does_not_return_early(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid], read_results=[b"AT+FWVER", b":8.50.12.3\r\n"])
        session = await self.session(link)
        self.assertEqual(await session.exchange_text("AT+FWVER?", timeout=0.1), "AT+FWVER:8.50.12.3")
        self.assertEqual(len(link.read_calls), 2)

    async def test_started_notification_stream_does_not_trigger_snapshot_reads(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
        session = await self.session(link)
        task = asyncio.create_task(session.exchange_text("AT+FWVER?", timeout=1))
        await asyncio.sleep(0)
        link.push_notification(b"AT+FWVER:8.")
        await asyncio.sleep(0.02)
        self.assertFalse(task.done())
        self.assertFalse(link.read_calls)
        link.push_notification(b"50.12.3\r\n")
        self.assertEqual(await task, "AT+FWVER:8.50.12.3")

    async def test_concurrent_exchanges_have_one_command_owner(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
        session = await self.session(link)
        first = asyncio.create_task(session.exchange_text("AT+FWVER?", timeout=1))
        await asyncio.sleep(0)
        second = asyncio.create_task(session.exchange_text("AT+ATVER?", timeout=1))
        await asyncio.sleep(0)
        self.assertEqual(len(link.write_calls), 1)
        link.push_notification(b"AT+FWVER:8.50.12.3\r\n")
        self.assertEqual(await first, "AT+FWVER:8.50.12.3")
        await asyncio.sleep(0)
        self.assertEqual(len(link.write_calls), 2)
        link.push_notification(b"AT+ATVER:1.11\r\n")
        self.assertEqual(await second, "AT+ATVER:1.11")

    async def test_cancel_releases_owner_without_repeating_write(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
        session = await self.session(link)
        task = asyncio.create_task(session.exchange_text("AT+INTPARA=29,1", timeout=1))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        link._read_results = [b"AT+FWVER:8.50.12.3\r\n"]
        self.assertEqual(await session.exchange_text("AT+FWVER?", timeout=0.1), "AT+FWVER:8.50.12.3")
        self.assertEqual(len(link.write_calls), 2)

    async def test_disconnect_reconnect_cannot_finish_old_exchange(self):
        link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
        session = await self.session(link)
        task = asyncio.create_task(session.exchange_text("AT+FWVER?", timeout=1))
        await asyncio.sleep(0)
        await session.disconnect()
        await session.connect()
        link.push_notification(b"AT+FWVER:8.50.12.3\r\n")
        with self.assertRaisesRegex(SmartEssBleError, "ble_not_connected"):
            await task


if __name__ == "__main__":
    unittest.main()
