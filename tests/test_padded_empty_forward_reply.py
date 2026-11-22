"""#89 PCAP-derived zero-result envelopes; synthetic routes, no private trace."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
import test_empty_forward_reply as helpers
from custom_components.eybond_local.collector.protocol import decode_header
from custom_components.eybond_local.collector.transport.common import _PrefixedAsyncReader
from custom_components.eybond_local.collector.transport.empty_forward_reply import EmptyForwardReplyWindow

frame = helpers.frame


class PaddedEmptyReplyTests(unittest.IsolatedAsyncioTestCase):
    task = helpers.EmptyForwardReaderTests.task
    open = helpers.EmptyForwardReaderTests.open
    send = helpers.EmptyForwardReaderTests.send

    async def test_late_padded_reply_preserves_next_transaction_at_every_split(self):
        # Ten occurrences in #89's PCAP: an aligned length=0 FC4 envelope
        # followed by exactly29 zero bytes. Seven match a request on that
        # socket; three arrive after reconnect and MUST remain uncorrelated.
        for kind in ("framed", "at"):
            for split in range(38):
                with self.subTest(kind=kind, split=split):
                    c, r, w, _ = await self.open(kind)
                    old, old_tid = await self.send(kind, c, w, timeout=.01)
                    with self.assertRaises(TimeoutError):
                        await old
                    current, tid = await self.send(kind, c, w)
                    wire = frame(old_tid, empty=True) + bytes(29)
                    r.feed_data(wire[:split])
                    await asyncio.sleep(0)
                    r.feed_data(wire[split:] + frame(tid, payload=b"next-response"))
                    self.assertEqual(await current, b"next-response")
                    self.assertTrue(c.connected)
                    await c.disconnect()

    async def test_padding_is_not_a_successful_query_or_write_ack(self):
        for kind in ("framed", "at"):
            for payload in (bytes.fromhex("515049beac0d"), b"synthetic-write"):
                c, r, w, _ = await self.open(kind)
                pending, tid = await self.send(kind, c, w, payload=payload)
                r.feed_data(frame(tid, empty=True) + bytes(29))
                with self.assertRaisesRegex(TimeoutError, "collector_empty_forward_response"):
                    await pending
                self.assertIsNone(c.last_framed_exchange_monotonic)
                current, tid = await self.send(kind, c, w)
                r.feed_data(frame(tid, payload=b"verified-next-response"))
                self.assertEqual(await current, b"verified-next-response")
                await c.disconnect()

    async def test_wrong_length_nonzero_or_intervening_frame_stay_fatal(self):
        bad_tails = [bytes(n) for n in (16, 28, 30, 37, 44, 46, 53, 58)]
        bad_tails += [bytes(20) + b"\x01" + bytes(8)]
        bad_tails += [bytes(29) + b"abcdefgh", bytes(45) + b"abcdefgh"]
        bad_tails += [frame(0, fc=1) + bytes(29)]
        bad_tails += [b"AT+WSSQ=75\r\n" + bytes(29)]
        for kind in ("framed", "at"):
            for tail in bad_tails:
                with self.subTest(kind=kind, tail=tail.hex()):
                    c, r, w, run = await self.open(kind)
                    old, old_tid = await self.send(kind, c, w, timeout=.01)
                    with self.assertRaises(TimeoutError):
                        await old
                    current, tid = await self.send(kind, c, w)
                    r.feed_data(frame(old_tid, empty=True) + tail + frame(tid, payload=b"untrusted"))
                    await asyncio.wait_for(run, .5)
                    with self.assertRaises(ConnectionError):
                        await current
                    self.assertTrue(w.closed)

    async def test_late_previous_socket_padded_reply_is_not_authorized(self):
        for kind in ("framed", "at"):
            c, r, w, run = await self.open(kind)
            pending, old_tid = await self.send(kind, c, w, timeout=.01)
            with self.assertRaises(TimeoutError):
                await pending
            await c.disconnect()
            await asyncio.gather(run, return_exceptions=True)
            c, r, w, run = await self.open(kind, connection=c)
            r.feed_data(frame(old_tid, empty=True) + bytes(29))
            await asyncio.wait_for(run, .5)
            self.assertTrue(w.closed)

    async def test_truncated_padding_timeout_eof_and_cancel_close_session(self):
        for kind in ("framed", "at"):
            for end in ("timeout", "eof", "cancel"):
                with self.subTest(kind=kind, end=end), patch(
                    "custom_components.eybond_local.collector.transport.connections._FRAMED_PAYLOAD_COMPLETION_TIMEOUT",
                    .05,
                ):
                    c, r, w, run = await self.open(kind)
                    old, old_tid = await self.send(kind, c, w, timeout=.01)
                    with self.assertRaises(TimeoutError):
                        await old
                    current, tid = await self.send(kind, c, w)
                    r.feed_data(frame(old_tid, empty=True) + bytes(8))
                    if end == "eof":
                        r.feed_eof()
                    elif end == "cancel":
                        await helpers._wait(lambda: r._waiter is not None and not r._buffer)
                        await c.disconnect()
                    await asyncio.wait_for(asyncio.gather(run, return_exceptions=True), .5)
                    with self.assertRaises(ConnectionError):
                        await current
                    self.assertTrue(w.closed)
                    self.assertIsNone(c.last_framed_exchange_monotonic)

    async def test_invalid_suffix_logs_offset_not_payload_or_inferred_length(self):
        for consumed, width, offset in ((0, 29, 18), (29, 16, 10)):
            window = EmptyForwardReplyWindow()
            window.record(decode_header(frame(15)))
            self.assertTrue(window.consume(decode_header(frame(15, empty=True)), {}))
            if consumed:
                await window.async_consume_padding(
                    bytes(8), window.take_padding_boundary(), AsyncMock(return_value=bytes(21)),
                    timeout=1,
                )
            segment = bytes(offset) + b"x" * (width - offset)
            with self.assertLogs(
                "custom_components.eybond_local.collector.transport.empty_forward_reply", level="DEBUG",
            ) as logs, self.assertRaisesRegex(ValueError, "^collector_empty_forward_padding_invalid$"):
                await window.async_consume_padding(
                    segment[:8], window.take_padding_boundary(), AsyncMock(return_value=segment[8:]),
                    timeout=1,
                )
            self.assertEqual(logs.records[-1].getMessage(),
                             f"Invalid suffix after empty FC4 tid=15 consumed_padding_bytes={consumed} "
                             f"expected_zero_boundary={consumed + width} first_nonzero_offset={consumed + offset}")

    async def test_padding_permission_is_single_use_and_keeps_original_deadline(self):
        window = EmptyForwardReplyWindow()
        path = "custom_components.eybond_local.collector.transport.empty_forward_reply.monotonic"
        with patch(path, return_value=100):
            window.record(decode_header(frame(15)))
            self.assertTrue(window.consume(decode_header(frame(15, empty=True)), {}))
            boundary = window.take_padding_boundary()
            self.assertEqual(boundary.deadline, 110)
            self.assertIsNone(window.take_padding_boundary())
        read = AsyncMock(return_value=bytes(21))
        with patch(path, return_value=111):
            with self.assertRaisesRegex(ValueError, "collector_empty_forward_padding_timeout"):
                await window.async_consume_padding(bytes(8), boundary, read, timeout=.1)
        read.assert_not_awaited()

    async def test_excess_padding_never_resyncs_to_a_following_reply(self):
        # 37 used to pass in the framed reader when coalesced with a whole reply:
        # fixed29 consumed, then generic resync discarded the extra eight bytes.
        # Lengths are synthetic regression cases, NOT a claim about #89's wire.
        for kind in ("framed", "at"):
            for count in (37, 53, 58):
                for split in (8, 36, 37, 38, 44, 45, 46):
                    with self.subTest(kind=kind, count=count, split=split):
                        c, r, w, run = await self.open(kind)
                        old, old_tid = await self.send(kind, c, w, timeout=.01)
                        with self.assertRaises(TimeoutError):
                            await old
                        current, tid = await self.send(kind, c, w, payload=b"synthetic-write")
                        wire = frame(old_tid, empty=True) + bytes(count)
                        r.feed_data(wire[:split])
                        await asyncio.sleep(0)
                        r.feed_data(wire[split:] + frame(tid, payload=b"ACK"))
                        await asyncio.wait_for(run, .5)
                        with self.assertRaises(ConnectionError):
                            await current
                        self.assertTrue(w.closed)
                        self.assertIsNone(c.last_framed_exchange_monotonic)
                        self.assertEqual(c.collector_info.last_disconnect_reason,
                                         "collector_empty_forward_padding_invalid" if count == 37
                                         else "collector_empty_forward_padding_excess")

    async def test_excess_evidence_is_one_boundary_bounded_and_never_logs_content(self):
        window = EmptyForwardReplyWindow()
        window.record(decode_header(frame(15)))
        self.assertTrue(window.consume(decode_header(frame(15, empty=True)), {}))
        boundary = window.take_padding_boundary()
        read = AsyncMock(return_value=bytes(21))
        await window.async_consume_padding(bytes(8), boundary, read, timeout=.1)
        read.return_value = bytes(8)
        await window.async_consume_padding(
            bytes(8), window.take_padding_boundary(), read, timeout=.1,
        )
        excess_boundary = window.take_padding_boundary()
        self.assertEqual(excess_boundary.consumed_bytes, 45)
        self.assertEqual(excess_boundary.deadline, boundary.deadline)
        self.assertIsNone(window.take_padding_boundary())

        r = asyncio.StreamReader()
        secret = b"private-wire-content-not-for-logs"
        r.feed_data(bytes(400) + secret)
        reader = _PrefixedAsyncReader(r)
        read.reset_mock()
        with self.assertLogs(
            "custom_components.eybond_local.collector.transport.empty_forward_reply",
            level="WARNING",
        ) as logs:
            with self.assertRaisesRegex(ValueError, "collector_empty_forward_padding_excess"):
                await window.async_consume_padding(
                    bytes(8), excess_boundary, read, timeout=.1,
                    peek_buffered=reader.peek_buffered,
                )
        message = " ".join(logs.output)
        self.assertIn("additional_zero_prefix_bytes=264", message)
        self.assertIn("buffered_peek_bytes=256", message)
        self.assertIn("exact_padding_length=unknown", message)
        self.assertNotIn(secret.decode(), message)
        read.assert_not_awaited()
        self.assertEqual(await reader.readexactly(400 + len(secret)), bytes(400) + secret)

    async def test_intervening_frame_consumes_excess_boundary_marker(self):
        window = EmptyForwardReplyWindow()
        window.record(decode_header(frame(15)))
        window.consume(decode_header(frame(15, empty=True)), {})
        await window.async_consume_padding(
            bytes(8), window.take_padding_boundary(), AsyncMock(return_value=bytes(21)), timeout=.1,
        )
        marker = window.take_padding_boundary()
        read = AsyncMock()
        self.assertFalse(await window.async_consume_padding(frame(0, fc=1), marker, read, timeout=.1))
        self.assertIsNone(window.take_padding_boundary())
        read.assert_not_awaited()

    async def test_buffer_peek_respects_prefix_and_does_not_consume(self):
        r = asyncio.StreamReader()
        r.feed_data(b"inner-buffer")
        reader = _PrefixedAsyncReader(r, b"prefix-")
        self.assertEqual(reader.peek_buffered(-1), b"")
        self.assertEqual(reader.peek_buffered(0), b"")
        self.assertEqual(reader.peek_buffered(3), b"pre")
        self.assertEqual(reader.peek_buffered(9), b"prefix-in")
        self.assertEqual(await reader.readexactly(19), b"prefix-inner-buffer")


if __name__ == "__main__":
    unittest.main()
