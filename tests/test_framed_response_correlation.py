"""Exercise reply correlation through the real framed sender and stream reader."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.management import (
    FramedCollectorManagementAdapter,
)
from custom_components.eybond_local.collector.protocol import (
    build_collector_request,
    decode_header,
)
from custom_components.eybond_local.collector.transport.connections import (
    _CollectorConnection,
)


class _Writer:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.closed = False

    def is_closing(self):
        return self.closed

    def write(self, frame):
        self.frames.put_nowait(frame)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class _Transport:
    def __init__(self, connection):
        self.connection = connection

    async def async_send_collector(self, **kwargs):
        return await self.connection.async_send_collector(
            **kwargs, request_timeout=0.1,
        )


def _reply(tid, function, payload=b"", *, devcode=0x0102, address=0xFF):
    # Real collectors can reply with a different devcode/address than requested.
    # Correlation must not introduce an address equality requirement.
    return build_collector_request(
        tid, payload, devcode=devcode, collector_addr=address, fcode=function,
    )


@asynccontextmanager
async def _session():
    connection = _CollectorConnection(heartbeat_interval=60, write_timeout=0.1)
    writer = _Writer()
    reader = asyncio.StreamReader()
    connection._writer = writer
    connection._reader = reader
    connection._reader_task = asyncio.create_task(connection._read_loop(reader))
    try:
        yield connection, writer, reader
    finally:
        await connection.disconnect()


class FramedResponseCorrelationTests(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_read_ignores_same_tid_heartbeat_and_foreign_function(self):
        async with _session() as (connection, writer, reader):
            adapter = FramedCollectorManagementAdapter(lambda: _Transport(connection))

            async def collector():
                for parameter, value in ((21, b"cloud.example.test,18899,TCP"), (30, b"0")):
                    request = await writer.frames.get()
                    header = decode_header(request)
                    self.assertEqual((header.fcode, request[8:]), (2, bytes((parameter,))))
                    self.assertEqual((header.devcode, header.devaddr), (1, 1))
                    reader.feed_data(
                        _reply(header.tid, 1, b"E5000020000000")
                        + _reply(header.tid, 3, bytes((0, parameter)))
                        + _reply(header.tid + 100, 2, b"\x00\x15foreign")
                        + _reply(header.tid, 2, bytes((0, parameter)) + value)
                    )

            peer = asyncio.create_task(collector())
            try:
                state = await adapter.async_read_endpoint_state()
                self.assertEqual(state.current_endpoint, "cloud.example.test,18899,TCP")
                self.assertEqual(state.reboot_required, "0")
                await peer
                self.assertTrue(writer.frames.empty())
                self.assertFalse(connection._pending)
                self.assertFalse(connection._pending_fcode)
            finally:
                peer.cancel()
                await asyncio.gather(peer, return_exceptions=True)

    async def test_wrong_function_cannot_confirm_set_or_inverter_request(self):
        for function in (3, 4):
            with self.subTest(function=function):
                async with _session() as (connection, writer, reader):
                    request_payload = b"\x15cloud.example.test,18899,TCP" if function == 3 else b"test"
                    correct_reply = b"\x00\x15" if function == 3 else b"correct"
                    task = asyncio.create_task(connection.async_send_collector(
                        fcode=function, payload=request_payload, request_timeout=0.1,
                    ))
                    header = decode_header(await writer.frames.get())
                    reader.feed_data(
                        _reply(header.tid, 2, b"unrelated")
                        + _reply(header.tid, function, correct_reply)
                    )
                    response_header, payload = await task
                    self.assertEqual(response_header.fcode, function)
                    self.assertEqual(payload, correct_reply)
                    self.assertFalse(connection._pending)
                    self.assertFalse(connection._pending_fcode)

    async def test_wrong_function_only_times_out_without_proving_liveness(self):
        async with _session() as (connection, writer, reader):
            task = asyncio.create_task(connection.async_send_collector(
                fcode=2, payload=b"\x15", request_timeout=0.02,
            ))
            header = decode_header(await writer.frames.get())
            reader.feed_data(_reply(header.tid, 3, b"\x00\x15"))
            with self.assertRaises(TimeoutError):
                await task
            self.assertIsNone(connection._last_liveness_monotonic)
            self.assertFalse(connection._pending)
            self.assertFalse(connection._pending_fcode)
            self.assertTrue(connection.connected)

    async def test_explicit_heartbeat_request_can_still_receive_heartbeat(self):
        async with _session() as (connection, writer, reader):
            task = asyncio.create_task(connection.async_send_collector(
                fcode=1, request_timeout=0.1,
            ))
            header = decode_header(await writer.frames.get())
            reader.feed_data(_reply(header.tid, 1, b"E5000020000000"))
            self.assertEqual((await task)[0].fcode, 1)
            self.assertTrue(connection.collector_info.heartbeat_fresh)

    async def test_cancelled_request_does_not_claim_late_reply(self):
        async with _session() as (connection, writer, reader):
            task = asyncio.create_task(connection.async_send_collector(
                fcode=2, payload=b"\x15", request_timeout=0.1,
            ))
            old_header = decode_header(await writer.frames.get())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(connection._pending)
            self.assertFalse(connection._pending_fcode)
            successor = asyncio.create_task(connection.async_send_collector(
                fcode=2, payload=b"\x1e", request_timeout=0.1,
            ))
            header = decode_header(await writer.frames.get())
            reader.feed_data(
                _reply(old_header.tid, 2, b"\x00\x15late")
                + _reply(header.tid, 2, b"\x00\x1e0")
            )
            self.assertEqual((await successor)[1], b"\x00\x1e0")

    async def test_disconnect_fails_request_without_accepting_foreign_reply(self):
        async with _session() as (connection, writer, reader):
            task = asyncio.create_task(connection.async_send_collector(
                fcode=2, payload=b"\x15", request_timeout=0.1,
            ))
            header = decode_header(await writer.frames.get())
            reader.feed_data(_reply(header.tid, 3, b"\x00\x15"))
            # The reader must process the foreign frame before cleanup, not simply
            # have it discarded by cancelling the stream task.
            reader.feed_eof()
            await connection._reader_task
            await connection.disconnect()
            with self.assertRaisesRegex(ConnectionError, "collector_disconnected"):
                await task
            self.assertFalse(connection._pending)
            self.assertFalse(connection._pending_fcode)
