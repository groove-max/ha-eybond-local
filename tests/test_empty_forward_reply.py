"""#89: correlated zero-length FC4 is a negative result, never a write ACK."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from test_collector_auxiliary_session import _Writer, _wait
from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
from custom_components.eybond_local.collector.transport.connections import _CollectorAtConnection, _CollectorConnection
from custom_components.eybond_local.collector.transport.empty_forward_reply import EmptyForwardReplyWindow


def frame(tid, *, fc=4, devcode=0x0994, address=255, payload=b"", empty=False):
    wire = build_collector_request(tid, payload, devcode=devcode, collector_addr=address, fcode=fc)
    return wire[:4] + b"\x00\x00" + wire[6:8] if empty else wire


class EmptyForwardWindowTests(unittest.TestCase):
    def test_exact_route_function_length_expiry_and_single_use(self):
        window = EmptyForwardReplyWindow()
        with patch("custom_components.eybond_local.collector.transport.empty_forward_reply.monotonic", return_value=100):
            window.record(decode_header(frame(15)))
            for kwargs in ({"tid": 16}, {"tid": 15, "fc": 2},
                           {"tid": 15, "address": 1}, {"tid": 15, "devcode": 1}):
                self.assertFalse(window.consume(decode_header(frame(**kwargs, empty=True)), {}))
            self.assertFalse(window.consume(decode_header(frame(15)), {}))  # standard length2
            self.assertTrue(window.consume(decode_header(frame(15, empty=True)), {}))
            self.assertFalse(window.consume(decode_header(frame(15, empty=True)), {}))
            window.record(decode_header(frame(15)))
        with patch("custom_components.eybond_local.collector.transport.empty_forward_reply.monotonic", return_value=111):
            self.assertFalse(window.consume(decode_header(frame(15, empty=True)), {}))

    def test_memory_bound_and_tid_reuse_by_non_forward_request(self):
        window = EmptyForwardReplyWindow()
        for tid in range(100):
            window.record(decode_header(frame(tid)))
        self.assertLessEqual(len(window._requests), 16)
        self.assertFalse(window.consume(decode_header(frame(0, empty=True)), {}))
        window.record(decode_header(frame(99, fc=2)))
        self.assertFalse(window.consume(decode_header(frame(99, empty=True)), {}))


class EmptyForwardReaderTests(unittest.IsolatedAsyncioTestCase):
    def task(self, coroutine):
        task = asyncio.create_task(coroutine)

        async def stop():
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.addAsyncCleanup(stop)
        return task

    async def open(self, kind, connection=None):
        if connection is None:
            connection = (_CollectorConnection(heartbeat_interval=60, write_timeout=.2)
                          if kind == "framed" else _CollectorAtConnection(write_timeout=.2))
        reader = asyncio.StreamReader()
        writer = _Writer(reader)
        run = self.task(connection.run(reader, writer))
        await _wait(lambda: connection._writer is writer and connection.connected)
        self.addAsyncCleanup(connection.disconnect)
        return connection, reader, writer, run

    async def send(self, kind, c, w, *, timeout=.5, payload=bytes.fromhex("515049beac0d")):
        count = len(w.writes)
        method = c.async_send_forward if kind == "framed" else c.async_send_framed_inverter_payload
        task = self.task(method(payload, devcode=0x0994, collector_addr=255, request_timeout=timeout))
        await _wait(lambda: any(decode_header(b).fcode == 4 for b in w.writes[count:]))
        request = next(b for b in w.writes[count:] if decode_header(b).fcode == 4)
        return task, decode_header(request).tid

    async def test_active_empty_fails_query_or_write_without_closing_socket(self):
        for kind in ("framed", "at"):
            for payload in (bytes.fromhex("515049beac0d"), b"synthetic-write"):
                for split in range(1, 9):
                    with self.subTest(kind=kind, split=split, payload=payload):
                        c, r, w, _ = await self.open(kind)
                        task, tid = await self.send(kind, c, w, payload=payload)
                        wire = frame(tid, empty=True)
                        r.feed_data(wire[:split])
                        await asyncio.sleep(0)
                        r.feed_data(wire[split:])
                        with self.assertRaisesRegex(TimeoutError, "collector_empty_forward_response"):
                            await task
                        self.assertTrue(c.connected)
                        self.assertFalse(w.closed)
                        self.assertIsNone(c.last_framed_exchange_monotonic)
                        task, tid = await self.send(kind, c, w)
                        r.feed_data(frame(tid, payload=b"next-response"))
                        self.assertEqual(await task, b"next-response")
                        await c.disconnect()

    async def test_late_empty_does_not_complete_next_request(self):
        for kind in ("framed", "at"):
            c, r, w, _ = await self.open(kind)
            old, old_tid = await self.send(kind, c, w, timeout=.01)
            with self.assertRaises(TimeoutError):
                await old
            current, tid = await self.send(kind, c, w)
            r.feed_data(frame(old_tid, empty=True))
            await asyncio.sleep(.01)
            self.assertFalse(current.done())
            r.feed_data(frame(tid, payload=b"current-response"))
            self.assertEqual(await current, b"current-response")
            self.assertTrue(c.connected)
            await c.disconnect()

    async def test_unknown_or_wrong_route_and_old_socket_stay_fatal(self):
        for kind in ("framed", "at"):
            for stale_socket in (False, True):
                c, r, w, run = await self.open(kind)
                request, tid = await self.send(kind, c, w, timeout=.01)
                with self.assertRaises(TimeoutError):
                    await request
                if stale_socket:
                    await c.disconnect()
                    await asyncio.gather(run, return_exceptions=True)
                    c, r, w, run = await self.open(kind, connection=c)
                    bad = frame(tid, empty=True)
                else:
                    bad = frame(tid, devcode=1, empty=True)
                r.feed_data(bad)
                await asyncio.wait_for(run, .5)
                self.assertTrue(w.closed)
                self.assertEqual(c.collector_info.last_disconnect_reason, "collector_frame_length_invalid")

    async def test_failed_local_write_cannot_authorize_empty_reply(self):
        for kind in ("framed", "at"):
            c, _, _, _ = await self.open(kind)
            with patch.object(c, "_async_write", side_effect=OSError("synthetic failure")):
                method = c.async_send_forward if kind == "framed" else c.async_send_framed_inverter_payload
                with self.assertRaises(OSError):
                    await method(b"query", devcode=0x0994, collector_addr=255, request_timeout=.1)
            self.assertFalse(c._empty_forward_replies._requests)
            await c.disconnect()


if __name__ == "__main__":
    unittest.main()
