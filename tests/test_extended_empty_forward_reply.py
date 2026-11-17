"""#89 second PCAP: exact 29/45-zero suffixes, never arbitrary zero skipping."""

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
from custom_components.eybond_local.collector.transport.empty_forward_reply import EmptyForwardReplyWindow

frame = helpers.frame


class ExtendedEmptyReplyTests(unittest.IsolatedAsyncioTestCase):
    task = helpers.EmptyForwardReaderTests.task
    open = helpers.EmptyForwardReaderTests.open
    send = helpers.EmptyForwardReaderTests.send

    async def test_exact_45_tail_preserves_next_reply_at_every_tcp_split(self):
        # Eleven 45-byte tails in the second PCAP; six match requests on their
        # physical socket, five do not. Only the former authorize this grammar.
        for kind in ("framed", "at"):
            for split in range(54):
                with self.subTest(kind=kind, split=split):
                    c, r, w, _ = await self.open(kind)
                    old, old_tid = await self.send(kind, c, w, timeout=.01)
                    with self.assertRaises(TimeoutError):
                        await old
                    current, tid = await self.send(kind, c, w)
                    wire = frame(old_tid, empty=True) + bytes(45)
                    r.feed_data(wire[:split])
                    await asyncio.sleep(0)
                    r.feed_data(wire[split:] + frame(tid, payload=b"verified-next-response"))
                    self.assertEqual(await current, b"verified-next-response")
                    self.assertTrue(c.connected)
                    await c.disconnect()

    async def test_tail_length_can_change_without_becoming_a_write_ack(self):
        for kind in ("framed", "at"):
            c, r, w, _ = await self.open(kind)
            for size in (29, 45, 29, 45):
                for request in (bytes.fromhex("515049beac0d"), b"synthetic-write"):
                    pending, tid = await self.send(kind, c, w, payload=request)
                    r.feed_data(frame(tid, empty=True) + bytes(size))
                    with self.assertRaisesRegex(TimeoutError, "collector_empty_forward_response"):
                        await pending
                    self.assertIsNone(c.last_framed_exchange_monotonic)
                    self.assertTrue(c.connected)
            pending, tid = await self.send(kind, c, w)
            r.feed_data(frame(tid, payload=b"normal-response"))
            self.assertEqual(await pending, b"normal-response")
            await c.disconnect()

    async def test_45_tail_cannot_claim_a_request_from_a_previous_socket(self):
        for kind in ("framed", "at"):
            c, r, w, run = await self.open(kind)
            old, tid = await self.send(kind, c, w, timeout=.01)
            with self.assertRaises(TimeoutError):
                await old
            await c.disconnect()
            await asyncio.gather(run, return_exceptions=True)
            c, r, w, run = await self.open(kind, connection=c)
            r.feed_data(frame(tid, empty=True) + bytes(45))
            await asyncio.wait_for(run, .5)
            self.assertTrue(w.closed)
            self.assertEqual(c.collector_info.last_disconnect_reason, "collector_frame_length_invalid")

    async def test_truncated_extension_timeout_eof_and_cancel_are_fatal(self):
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
                    current, _ = await self.send(kind, c, w)
                    r.feed_data(frame(old_tid, empty=True) + bytes(37))
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

    async def test_extension_does_not_renew_deadline(self):
        window = EmptyForwardReplyWindow()
        path = "custom_components.eybond_local.collector.transport.empty_forward_reply.monotonic"
        with patch(path, return_value=100):
            window.record(decode_header(frame(15)))
            self.assertTrue(window.consume(decode_header(frame(15, empty=True)), {}))
        with patch(path, return_value=109):
            await window.async_consume_padding(
                bytes(8), window.take_padding_boundary(), AsyncMock(return_value=bytes(21)), timeout=2,
            )
        boundary = window.take_padding_boundary()
        self.assertEqual(boundary.deadline, 110)
        self.assertEqual(boundary.consumed_bytes, 29)
        read = AsyncMock(return_value=bytes(8))
        with patch(path, return_value=111):
            with self.assertRaisesRegex(ValueError, "collector_empty_forward_padding_timeout"):
                await window.async_consume_padding(bytes(8), boundary, read, timeout=2)
        read.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
