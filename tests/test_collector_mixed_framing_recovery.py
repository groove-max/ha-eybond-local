"""#89: retire a desynchronized AT/binary stream, never search for replies.

The invalid eight-byte prefixes and one CRC-valid FC4 response were logged in
the supplied debug trace. Their combinations, pending requests, fragmentation
and reconnects here are synthetic, not a reconstructed user TCP capture.
"""

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
from custom_components.eybond_local.collector.transport.connections import _CollectorAtConnection

CONNECTIONS = "custom_components.eybond_local.collector.transport.connections"
INVALID_LENGTH = bytes.fromhex("12e309940000ff04")
RAW_PREFIX = bytes.fromhex("0103140000000000")
OBSERVED_FRAME = bytes.fromhex("12e900010009ff040103027a021ae5")


def frame(tid=0x12E9, *, fc=4, payload=OBSERVED_FRAME[8:]):
    return build_collector_request(tid, payload, devcode=1, collector_addr=255, fcode=fc)


class MixedFramingRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def task(self, coroutine):
        task = asyncio.create_task(coroutine)

        async def stop():
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.addAsyncCleanup(stop)
        return task

    async def open(self, mode="", connection=None, **callbacks):
        if connection is None:
            connection = _CollectorAtConnection(
                write_timeout=.5, raw_passthrough_bootstrap="none",
                raw_passthrough_frame_format=mode,
            )
        reader = asyncio.StreamReader()
        writer = _Writer(reader)
        run = self.task(connection.run(reader, writer, **callbacks))
        await _wait(lambda: connection._writer is writer and connection.connected)
        self.addAsyncCleanup(connection.disconnect)
        return connection, reader, writer, run

    def pending(self, connection, tid=0x12E9, fc=4):
        future = asyncio.get_running_loop().create_future()
        connection._pending_framed_response[tid] = future
        connection._pending_framed_fcode[tid] = fc
        return future

    async def send(self, connection, writer):
        count = len(writer.writes)
        task = self.task(connection.async_send_framed_inverter_payload(
            b"synthetic-read", devcode=1, collector_addr=255, request_timeout=1,
        ))
        await _wait(lambda: len(writer.writes) > count)
        return task, decode_header(writer.writes[-1]).tid

    async def test_logged_invalid_headers_retire_before_a_following_valid_reply(self):
        for mode in ("", "plain_line"):
            for bad, expected in (
                (INVALID_LENGTH, "collector_frame_length_invalid"),
                (RAW_PREFIX, "collector_frame_length_invalid"),
                (bytes.fromhex("123400010002ff7f"), "collector_frame_function_invalid"),
                (bytes.fromhex("123400011003ff04"), "collector_frame_payload_too_large"),
            ):
                wire = bad + OBSERVED_FRAME
                deliveries = [[wire], [bytes([b]) for b in wire]]
                deliveries += [[wire[:i], wire[i:]] for i in range(1, len(wire))]
                for chunks in deliveries:
                    with self.subTest(mode=mode, bad=bad.hex(), sizes=list(map(len, chunks))):
                        c, reader, writer, run = await self.open(mode)
                        pending = self.pending(c)
                        for chunk in chunks:
                            if writer.closed:
                                break
                            reader.feed_data(chunk)
                            await asyncio.sleep(0)
                        await asyncio.wait_for(run, .5)
                        with self.assertRaises(ConnectionError):
                            await pending
                        self.assertTrue(writer.closed)
                        self.assertFalse(c.connected)
                        self.assertEqual(c.collector_info.raw_unhandled_line_count, 1)
                        self.assertEqual(c.collector_info.last_disconnect_reason, expected)
                        self.assertFalse(c._pending_framed_response)

    async def test_valid_frames_preserve_tcp_fragmentation_and_correlation(self):
        # An unmatched frame must not complete the pending transaction. The
        # payload contains delimiter-like bytes that must remain binary data.
        payload = b"\r\nAT+FWVER:payload\r\n\xaa\xbb"
        wire = frame(17) + frame(fc=2) + frame(payload=payload)
        deliveries = [[wire], [bytes([b]) for b in wire]]
        deliveries += [[wire[:i], wire[i:]] for i in range(1, len(wire))]
        for mode in ("", "plain_line"):
            for chunks in deliveries:
                c, reader, writer, _ = await self.open(mode)
                pending = self.pending(c)
                for chunk in chunks:
                    reader.feed_data(chunk)
                    await asyncio.sleep(0)
                header, result = await asyncio.wait_for(pending, .5)
                self.assertEqual((header.tid, header.fcode, result), (0x12E9, 4, payload))
                self.assertFalse(writer.closed)
                self.assertEqual(c.collector_info.raw_unhandled_line_count, 0)
                await c.disconnect()

    async def test_started_prefix_header_and_payload_are_bounded_in_both_modes(self):
        for mode in ("", "plain_line"):
            for partial in (b"A", b"AB", OBSERVED_FRAME[:1], OBSERVED_FRAME[:2],
                            OBSERVED_FRAME[:3], OBSERVED_FRAME[:7], OBSERVED_FRAME[:9]):
                with self.subTest(mode=mode, partial=partial.hex()):
                    with patch(f"{CONNECTIONS}._FRAMED_HEADER_COMPLETION_TIMEOUT", .02), \
                         patch(f"{CONNECTIONS}._FRAMED_PAYLOAD_COMPLETION_TIMEOUT", .02):
                        c, reader, writer, run = await self.open(mode)
                        pending = self.pending(c)
                        reader.feed_data(partial)
                        await asyncio.wait_for(run, .5)
                        with self.assertRaises(ConnectionError):
                            await pending
                        expected = ("collector_frame_payload_timeout" if len(partial) > 8
                                    else "collector_frame_header_timeout")
                        self.assertEqual(c.collector_info.last_disconnect_reason, expected)
                        self.assertTrue(writer.closed)

    async def test_idle_silence_does_not_disconnect(self):
        with patch(f"{CONNECTIONS}._FRAMED_HEADER_COMPLETION_TIMEOUT", .01):
            for mode in ("", "plain_line"):
                c, reader, writer, _ = await self.open(mode)
                await asyncio.sleep(.04)
                self.assertTrue(c.connected)
                pending = self.pending(c)
                reader.feed_data(OBSERVED_FRAME)
                self.assertEqual((await pending)[1], OBSERVED_FRAME[8:])
                await c.disconnect()

    async def test_plain_line_mode_binary_fragment_can_exceed_text_idle_gap(self):
        c, reader, writer, _ = await self.open("plain_line")
        pending = self.pending(c)
        reader.feed_data(OBSERVED_FRAME[:4])
        await asyncio.sleep(.08)  # Old text fallback discarded after 50 ms.
        reader.feed_data(OBSERVED_FRAME[4:9])
        await asyncio.sleep(.08)
        reader.feed_data(OBSERVED_FRAME[9:])
        self.assertEqual((await asyncio.wait_for(pending, .5))[1], OBSERVED_FRAME[8:])
        self.assertTrue(c.connected)

    async def test_reconnect_does_not_migrate_old_requests_or_reuse_old_tid(self):
        c, reader, writer, old_run = await self.open()
        first, old_tid = await self.send(c, writer)
        queued = self.task(c.async_query("FWVER", request_timeout=1))
        await asyncio.sleep(0)  # Capture the old owner before acquiring request_lock.
        reader.feed_data(INVALID_LENGTH + frame(old_tid))
        await asyncio.wait_for(old_run, .5)
        for pending in (first, queued):
            with self.assertRaises(ConnectionError):
                await pending
        self.assertEqual(len(writer.writes), 1)
        self.assertFalse(c.connected)

        _, new_reader, new_writer, _ = await self.open(connection=c)
        fresh, new_tid = await self.send(c, new_writer)
        self.assertNotEqual(new_tid, old_tid)
        new_reader.feed_data(frame(old_tid) + frame(new_tid, fc=2))
        await asyncio.sleep(.01)
        self.assertFalse(fresh.done())
        new_reader.feed_data(frame(new_tid))
        self.assertEqual(await fresh, OBSERVED_FRAME[8:])
        self.assertTrue(c.connected)
        self.assertFalse(new_writer.closed)

    async def test_slow_old_cleanup_cannot_close_replacement_after_framing_failure(self):
        closed = []
        c, reader, writer, old_run = await self.open(
            session_id="old", session_closed_callback=lambda *args: closed.append(args[0]),
        )
        writer.close_gate = asyncio.Event()
        request, tid = await self.send(c, writer)
        reader.feed_data(INVALID_LENGTH)
        await _wait(lambda: writer.closed)
        self.assertEqual(closed, ["old"])
        with self.assertRaises(ConnectionError):
            await request
        _, new_reader, new_writer, _ = await self.open(connection=c, session_id="new")
        fresh, new_tid = await self.send(c, new_writer)
        writer.close_gate.set()
        await asyncio.wait_for(old_run, .5)
        new_reader.feed_data(frame(tid) + frame(new_tid))
        self.assertEqual(await fresh, OBSERVED_FRAME[8:])
        self.assertEqual(c._session_id, "new")
        self.assertTrue(c.connected)

    async def test_unfinished_unowned_plain_text_is_not_a_new_frame_boundary(self):
        c, reader, writer, run = await self.open("plain_line")
        reader.feed_data(b"229.9 49.98")  # No terminator, no raw request owner.
        await asyncio.wait_for(run, .5)
        self.assertTrue(writer.closed)
        self.assertEqual(c.collector_info.last_disconnect_reason, "raw_plain_line_stale_timeout")

    async def test_real_tcp_session_closes_and_fresh_callback_can_exchange(self):
        c = _CollectorAtConnection(write_timeout=.5, raw_passthrough_bootstrap="none")
        runs = []

        def accept(reader, writer):
            runs.append(self.task(c.run(reader, writer)))

        server = await asyncio.start_server(accept, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        clients = []

        async def request(client_reader):
            operation = self.task(c.async_send_framed_inverter_payload(
                b"synthetic-read", devcode=1, collector_addr=255, request_timeout=1,
            ))
            header = decode_header(await asyncio.wait_for(client_reader.readexactly(8), .5))
            await asyncio.wait_for(client_reader.readexactly(header.payload_len), .5)
            return operation, header.tid

        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            clients.append(writer)
            await _wait(lambda: c.connected)
            old, tid = await request(reader)
            writer.write(INVALID_LENGTH + frame(tid))
            await writer.drain()
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(old, .5)
            self.assertEqual(await asyncio.wait_for(reader.read(), .5), b"")
            await asyncio.wait_for(runs[0], .5)
            self.assertFalse(c.connected)

            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            clients.append(writer)
            await _wait(lambda: c.connected)
            fresh, new_tid = await request(reader)
            writer.write(frame(tid) + frame(new_tid))
            await writer.drain()
            self.assertEqual(await asyncio.wait_for(fresh, .5), OBSERVED_FRAME[8:])
            self.assertTrue(c.connected)
        finally:
            await c.disconnect()
            for writer in clients:
                writer.close()
                await writer.wait_closed()
            server.close()
            await server.wait_closed()
            await asyncio.gather(*runs, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
