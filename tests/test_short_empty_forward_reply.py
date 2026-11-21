"""Issue #89: one input-register read returns a negative FC4 and seven zeros."""

import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import test_empty_forward_reply as helpers
from test_collector_auxiliary_session import _wait
from custom_components.eybond_local.collector.protocol import decode_header
from custom_components.eybond_local.collector.transport.empty_forward_reply import EmptyForwardReplyWindow


INPUT_READ = bytes.fromhex("010488b800019a4f")
frame = helpers.frame


class ShortEmptyReplyTests(IsolatedAsyncioTestCase):
    task = helpers.EmptyForwardReaderTests.task
    open = helpers.EmptyForwardReaderTests.open
    send = helpers.EmptyForwardReaderTests.send

    async def test_captured_exchange_survives_idle_before_next_poll(self):
        for kind in ("framed", "at"):
            with self.subTest(kind=kind), patch(
                "custom_components.eybond_local.collector.transport.connections._FRAMED_HEADER_COMPLETION_TIMEOUT",
                .05,
            ):
                c, r, writer, _ = await self.open(kind)
                if kind == "framed":
                    await _wait(lambda: any(decode_header(x).fcode == 1 for x in writer.writes))
                c._tid._value = 0x02bb
                method = c.async_send_forward if kind == "framed" else c.async_send_framed_inverter_payload

                async def issue(payload, expected):
                    pending = self.task(method(payload, devcode=1, collector_addr=255, request_timeout=1))
                    await _wait(lambda: bytes.fromhex(expected) in writer.writes)
                    return pending

                first = await issue(bytes.fromhex("0103c768000138a2"), "02bc0001000aff040103c768000138a2")
                r.feed_data(bytes.fromhex("02bc00010009ff040103020000b844"))
                self.assertEqual(await first, bytes.fromhex("0103020000b844"))
                second = await issue(INPUT_READ, "02bd0001000aff04010488b800019a4f")
                r.feed_data(bytes.fromhex("02bd00010000ff0400000000000000"))
                with self.assertRaisesRegex(TimeoutError, "collector_empty_forward_response"):
                    await second
                await asyncio.sleep(.1)
                self.assertTrue(c.connected)
                third, tid = await self.send(kind, c, writer)
                r.feed_data(frame(tid, payload=b"later"))
                self.assertEqual(await third, b"later")
                await c.disconnect()

    async def test_fragmentation_and_existing_padded_and_unpadded_responses(self):
        for kind in ("framed", "at"):
            c, r, w, _ = await self.open(kind)
            for size in (0, 7, 29, 45):
                for split in (range(16) if size == 7 else (3,)):
                    with self.subTest(kind=kind, size=size, split=split):
                        old, tid = await self.send(kind, c, w, payload=INPUT_READ, timeout=.01)
                        with self.assertRaises(TimeoutError):
                            await old
                        current, next_tid = await self.send(kind, c, w)
                        wire = frame(tid, empty=True) + bytes(size) + frame(next_tid, payload=b"next")
                        r.feed_data(wire[:split])
                        await asyncio.sleep(0)
                        r.feed_data(wire[split:])
                        self.assertEqual(await current, b"next")
            await c.disconnect()

    async def test_restriction_to_valid_single_input_reads(self):
        for payload in (INPUT_READ, INPUT_READ[:-1] + b"\x00", bytes.fromhex("0103c768000138a2"),
                        bytes.fromhex("010688b800011b8d"), b"QPIGS", b""):
            window = EmptyForwardReplyWindow()
            window.record(decode_header(frame(12)), payload)
            self.assertTrue(window.consume(decode_header(frame(12, empty=True)), {}))
            self.assertEqual(window.take_padding_boundary().allow_short_suffix, payload == INPUT_READ)


    async def test_zero_route_tid_wrap_preserves_another_negative_reply(self):
        for kind in ("framed", "at"):
            c, r, w, _ = await self.open(kind)
            old, tid = await self.send(kind, c, w, payload=INPUT_READ, timeout=.01)
            with self.assertRaises(TimeoutError):
                await old
            c._tid._value = 0xffff
            method = c.async_send_forward if kind == "framed" else c.async_send_framed_inverter_payload
            current = self.task(method(b"query", devcode=0, collector_addr=0, request_timeout=1))
            await _wait(lambda: frame(0, devcode=0, address=0, payload=b"query") in w.writes)
            r.feed_data(frame(tid, empty=True) + frame(0, devcode=0, address=0, empty=True))
            with self.assertRaisesRegex(TimeoutError, "collector_empty_forward_response"):
                await current
            self.assertTrue(c.connected)
            await c.disconnect()

    async def test_truncated_short_suffix_is_not_another_reply(self):
        for kind in ("framed", "at"):
            with self.subTest(kind=kind), patch(
                "custom_components.eybond_local.collector.transport.connections._FRAMED_HEADER_COMPLETION_TIMEOUT",
                .05,
            ):
                c, r, w, run = await self.open(kind)
                request, tid = await self.send(kind, c, w, payload=INPUT_READ)
                r.feed_data(frame(tid, empty=True) + bytes(6))
                with self.assertRaisesRegex(TimeoutError, "collector_empty_forward_response"):
                    await request
                await asyncio.wait_for(run, .5)
                self.assertEqual(c.collector_info.last_disconnect_reason, "collector_empty_forward_padding_timeout")
                self.assertTrue(w.closed)

    async def test_unpadded_real_header_with_zero_prefix_stays_intact(self):
        for kind in ("framed", "at"):
            c, r, w, _ = await self.open(kind)
            pending, tid = await self.send(kind, c, w, payload=INPUT_READ)
            r.feed_data(frame(tid, empty=True))
            with self.assertRaises(TimeoutError):
                await pending
            c._tid._value = 0xffff
            current, tid = await self.send(kind, c, w)
            wire = frame(tid, devcode=1, payload=b"normal")
            self.assertEqual(wire[:3], bytes(3))
            r.feed_data(wire)
            self.assertEqual(await current, b"normal")
            await c.disconnect()
