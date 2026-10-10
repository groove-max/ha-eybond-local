"""Both real socket readers: auxiliary framing/ownership, no model semantics."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.collector.transport.connections import (
    _CollectorAtConnection, _CollectorConnection,
)
from custom_components.eybond_local.collector.transport.auxiliary_session import (
    AuxiliaryReadSession,
)


def _query(subtype: bytes = b"\x02\x00") -> bytes:
    return b"\x5a\xa5" + subtype + bytes(16) + bytes([sum(subtype) & 255])


def _reply(subtype: bytes = b"\x02\x00", *, collision: bool = False) -> bytes:
    body = b"\xaa\xbb" + subtype + b"\x04\x3d\x00" + bytes([4 if collision else 70])
    body += bytes(12)
    return body + bytes([sum(body[2:]) & 255])


def _framed(*, tid: int = 0xAABB, size: int = 20, devcode: int = 0x0200) -> bytes:
    return build_collector_request(
        tid, bytes(size), devcode=devcode, collector_addr=255, fcode=4,
    )


class _Writer:
    def __init__(self, reader):
        self.reader = reader
        self.closed = False
        self.writes = []
        self.drain_gate = None
        self.close_gate = None

    def is_closing(self):
        return self.closed

    def get_extra_info(self, key, default=None):
        return ("192.0.2.1", 1234) if key == "peername" else default

    def write(self, payload):
        self.writes.append(payload)

    async def drain(self):
        if self.drain_gate is not None:
            await self.drain_gate.wait()

    def close(self):
        self.closed = True
        self.reader.feed_eof()

    async def wait_closed(self):
        if self.close_gate is not None:
            await self.close_gate.wait()


async def _wait(predicate):
    async def check():
        while not predicate():
            await asyncio.sleep(0)
    await asyncio.wait_for(check(), 1)


class AuxiliaryConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def _stop(self, task):
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def _task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.addAsyncCleanup(self._stop, task)
        return task

    async def _open(self, kind, connection=None):
        if connection is None:
            if kind == "framed":
                connection = _CollectorConnection(heartbeat_interval=300, write_timeout=1)
                connection._heartbeat_loop = AsyncMock()
            else:
                connection = _CollectorAtConnection(
                    write_timeout=1, raw_passthrough_bootstrap="uart",
                    raw_passthrough_frame_format="plain_line",
                )
        reader = asyncio.StreamReader()
        writer = _Writer(reader)
        run = self._task(connection.run(reader, writer))
        await _wait(lambda: connection._writer is writer and connection.connected)
        self.addAsyncCleanup(connection.disconnect)
        return connection, reader, writer, run

    async def _start_read(self, connection, writer, *, timeout=1, subtype=b"\x02\x00"):
        count = len(writer.writes)
        task = self._task(connection.async_send_auxiliary_read(
            _query(subtype), request_timeout=timeout,
        ))
        await _wait(lambda: len(writer.writes) > count)
        self.assertEqual(writer.writes[-1], _query(subtype))
        return task

    async def test_framed_crlf_at_metadata_does_not_consume_auxiliary_reply(self):
        connection, reader, writer, _ = await self._open("framed")
        pending = await self._start_read(connection, writer)
        wire = _reply()
        reader.feed_data(b"\r\nAT+FWVER:8.50.12.3\r\n" + wire)
        self.assertEqual(await asyncio.wait_for(pending, 1), wire)
        self.assertEqual(connection.collector_info.smartess_collector_version, "8.50.12.3")
        self.assertTrue(connection.connected)
        # No UART command or protocol switch was sent by receiving AT metadata.
        self.assertEqual(writer.writes, [_query()])

    async def test_both_readers_every_split_and_no_uart_bootstrap(self):
        for kind in ("framed", "at"):
            connection, reader, writer, _ = await self._open(kind)
            for subtype in (b"\x02\x00", b"\x02\x02"):
                wire = _reply(subtype)
                for split in range(len(wire) + 1):
                    with self.subTest(kind=kind, subtype=subtype, split=split):
                        pending = await self._start_read(connection, writer, subtype=subtype)
                        reader.feed_data(wire[:split])
                        await asyncio.sleep(0)
                        reader.feed_data(wire[split:])
                        self.assertEqual(await pending, wire)
                        self.assertTrue(connection.connected)
                        self.assertTrue(connection._auxiliary_session.enabled)
            self.assertTrue(all(wire in (_query(), _query(b"\x02\x02")) for wire in writer.writes))
            # Transport must not turn auxiliary bytes into inverter/collector metadata.
            self.assertEqual(connection.collector_info.raw_response_count, 0)
            self.assertEqual(connection.collector_info.collector_pn, "")

    async def test_auxiliary_and_coalesced_at_and_framed_tail_are_independent(self):
        for kind in ("framed", "at"):
            connection, reader, writer, _ = await self._open(kind)
            pending = await self._start_read(connection, writer)
            reader.feed_data(
                _reply() + b"AT+FWVER:8.50.12.3\r\n"
                + _framed(tid=12, devcode=0x02FF)
            )
            self.assertEqual(await pending, _reply())
            await _wait(lambda: connection._auxiliary_session.at_boundary)
            self.assertTrue(connection.connected)
            self.assertEqual(connection.collector_info.smartess_collector_version, "8.50.12.3")
            # A new read still sees its own reply, not any suffix of the first.
            second = await self._start_read(connection, writer, subtype=b"\x02\x02")
            reader.feed_data(_reply(b"\x02\x02"))
            self.assertEqual(await second, _reply(b"\x02\x02"))

    async def test_unsupported_query_never_writes_or_enables_grammar(self):
        for kind in ("framed", "at"):
            connection, _, writer, _ = await self._open(kind)
            for wire in (b"", _query()[:-1], _query() + b"x", _query()[:-1] + b"\x03",
                         b"\x5a\xa5\x02\x01" + bytes(17)):
                with self.assertRaisesRegex(ValueError, "auxiliary_read_query_unsupported"):
                    await connection.async_send_auxiliary_read(wire, request_timeout=1)
            self.assertFalse(connection._auxiliary_session.enabled)
            self.assertEqual(writer.writes, [])

    async def test_timeout_and_cancellation_fence_physical_socket(self):
        for kind in ("framed", "at"):
            for cancel in (False, True):
                with self.subTest(kind=kind, cancel=cancel):
                    connection, reader, writer, _ = await self._open(kind)
                    pending = await self._start_read(connection, writer, timeout=0.03 if not cancel else 1)
                    reader.feed_data(_reply()[:9])
                    await _wait(lambda: not connection._auxiliary_session.at_boundary)
                    if cancel:
                        pending.cancel()
                    with self.assertRaises(asyncio.CancelledError if cancel else asyncio.TimeoutError):
                        await pending
                    self.assertTrue(writer.closed)
                    self.assertFalse(connection.connected)
                    self.assertTrue(connection._auxiliary_session.closed)
                    self.assertIsNone(connection._auxiliary_session.claim)
                    self.assertTrue(connection._reader_task is None or connection._reader_task.done())
                    with self.assertRaises(ConnectionError):
                        await connection.async_send_auxiliary_read(_query(), request_timeout=1)
                    self.assertEqual(writer.writes, [_query()])

    async def test_cancel_before_request_lock_does_not_change_session(self):
        for kind in ("framed", "at"):
            connection, _, writer, _ = await self._open(kind)
            await connection._request_lock.acquire()
            task = self._task(connection.async_send_auxiliary_read(_query(), request_timeout=1))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            connection._request_lock.release()
            self.assertFalse(connection._auxiliary_session.enabled)
            self.assertFalse(writer.closed)
            self.assertEqual(writer.writes, [])

    async def test_cancel_before_write_keeps_stream_but_never_publishes_a_claim(self):
        for kind in ("framed", "at"):
            connection, _, writer, _ = await self._open(kind)
            await connection._write_lock.acquire()
            task = self._task(connection.async_send_auxiliary_read(_query(), request_timeout=1))
            await _wait(lambda: connection._auxiliary_session.enabled)
            self.assertIsNone(connection._auxiliary_session.claim)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            connection._write_lock.release()
            self.assertFalse(writer.closed)
            self.assertEqual(writer.writes, [])
            # Cancellation cannot switch MIXED back into the old grammar.
            self.assertTrue(connection._auxiliary_session.enabled)

    async def test_cancel_during_drain_and_write_timeout_fence(self):
        for kind in ("framed", "at"):
            for cancel in (False, True):
                connection, _, writer, _ = await self._open(kind)
                writer.drain_gate = asyncio.Event()
                connection._write_timeout = 0.03 if not cancel else 1
                task = await self._start_read(connection, writer)
                if cancel:
                    task.cancel()
                with self.assertRaises(asyncio.CancelledError if cancel else asyncio.TimeoutError):
                    await task
                self.assertTrue(writer.closed)
                self.assertIsNone(connection._auxiliary_session.claim)

    async def test_invalid_or_unowned_frame_closes_without_returning_data(self):
        for kind in ("framed", "at"):
            for bad, reason in (
                (_reply()[:-1] + bytes([_reply()[-1] ^ 1]), "aabb_checksum_invalid"),
                (_reply(b"\x02\x02"), "aabb_response_subtype_mismatch"),
                (_reply(b"\x02\x03"), "aabb_subtype_unsupported"),
                (_reply(collision=True), "binary_frame_ambiguous"),
            ):
                with self.subTest(kind=kind, reason=reason):
                    connection, reader, writer, run = await self._open(kind)
                    task = await self._start_read(connection, writer)
                    reader.feed_data(bad)
                    with self.assertRaises(ConnectionError):
                        await task
                    await asyncio.gather(run, return_exceptions=True)
                    self.assertTrue(writer.closed)
                    self.assertEqual(connection.collector_info.last_disconnect_reason, reason)

    async def test_eof_or_stall_never_becomes_a_short_success(self):
        for kind in ("framed", "at"):
            for eof in (False, True):
                for prefix_size in (1, 3, 9, 20):
                    with self.subTest(kind=kind, eof=eof, prefix_size=prefix_size):
                        connection, reader, writer, _ = await self._open(kind)
                        with patch(
                            "custom_components.eybond_local.collector.transport.connections."
                            "_FRAMED_PAYLOAD_COMPLETION_TIMEOUT", 0.03,
                        ), patch(
                            "custom_components.eybond_local.collector.transport.connections."
                            "_FRAMED_HEADER_COMPLETION_TIMEOUT", 0.03,
                        ):
                            task = await self._start_read(connection, writer)
                            reader.feed_data(_reply()[:prefix_size])
                            if eof:
                                reader.feed_eof()
                            with self.assertRaises(ConnectionError):
                                await task
                        self.assertTrue(writer.closed)
                        self.assertEqual(connection.collector_info.raw_response_count, 0)

    async def test_late_checksum_colliding_framed_reply_is_not_auxiliary(self):
        # A valid21-byte AABB prefix of a28-byte EyeBond frame, AND a frame
        # whose entire21bytes satisfy both grammars. Future liveness is irrelevant.
        for kind in ("framed", "at"):
            for size in (13, 20):
                connection, reader, writer, _ = await self._open(kind)
                wire = bytearray(_framed(size=size))
                wire[20] = sum(wire[2:20]) & 255
                task = await self._start_read(connection, writer)
                reader.feed_data(wire)
                with self.assertRaises(ConnectionError):
                    await task
                self.assertEqual(connection.collector_info.last_disconnect_reason, "binary_frame_ambiguous")
                self.assertEqual(connection.collector_info.raw_response_count, 0)

    async def test_default_session_does_not_reinterpret_aabb_tid(self):
        for kind in ("framed", "at"):
            for done in (False, True):
                connection, reader, _, _ = await self._open(kind)
                future = asyncio.get_running_loop().create_future()
                if done:
                    future.cancel()
                if kind == "framed":
                    connection._pending[0xAABB] = future
                    connection._pending_fcode[0xAABB] = 4
                else:
                    connection._pending_framed_response[0xAABB] = future
                    connection._pending_framed_fcode[0xAABB] = 4
                reader.feed_data(_framed())
                await _wait(lambda: connection._auxiliary_session.at_boundary)
                if not done:
                    self.assertEqual((await asyncio.wait_for(future, 1))[1], bytes(20))
                self.assertTrue(connection.connected)
                self.assertFalse(connection._auxiliary_session.enabled)

    async def test_no_new_request_can_claim_a_frame_already_being_assembled(self):
        for kind in ("framed", "at"):
            connection, reader, writer, run = await self._open(kind)
            first = await self._start_read(connection, writer)
            reader.feed_data(_reply())
            await first
            reader.feed_data(_reply()[:8])
            await _wait(lambda: not connection._auxiliary_session.at_boundary)
            with self.assertRaisesRegex(ConnectionError, "auxiliary_frame_in_progress"):
                await connection.async_send_auxiliary_read(_query(), request_timeout=1)
            reader.feed_data(_reply()[8:])
            await asyncio.gather(run, return_exceptions=True)
            self.assertEqual(connection.collector_info.last_disconnect_reason, "aabb_response_unowned")
            self.assertEqual(writer.writes, [_query()])

    async def test_duplicate_auxiliary_cannot_satisfy_next_read(self):
        for kind in ("framed", "at"):
            connection, reader, writer, run = await self._open(kind)
            task = await self._start_read(connection, writer)
            reader.feed_data(_reply() + _reply())
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.gather(run, return_exceptions=True)
            self.assertTrue(writer.closed)
            self.assertEqual(connection.collector_info.last_disconnect_reason, "aabb_response_unowned")
            with self.assertRaises(ConnectionError):
                await connection.async_send_auxiliary_read(_query(), request_timeout=1)

    async def test_queued_auxiliary_request_cannot_migrate_to_replacement(self):
        for kind in ("framed", "at"):
            connection, _, writer, _ = await self._open(kind)
            old_state = connection._auxiliary_session
            await connection._request_lock.acquire()
            task = self._task(connection.async_send_auxiliary_read(_query(), request_timeout=1))
            await asyncio.sleep(0)
            _, _, new_writer, _ = await self._open(kind, connection)
            connection._request_lock.release()
            with self.assertRaises(ConnectionError):
                await task
            self.assertTrue(old_state.closed)
            self.assertTrue(writer.closed)
            self.assertFalse(new_writer.closed)
            self.assertEqual(new_writer.writes, [])
            self.assertFalse(connection._auxiliary_session.enabled)

    async def test_cancelled_old_cleanup_cannot_close_replacement(self):
        for kind in ("framed", "at"):
            connection, _, writer, _ = await self._open(kind)
            writer.drain_gate = asyncio.Event()
            task = await self._start_read(connection, writer)
            writer.close_gate = asyncio.Event()
            task.cancel()
            await _wait(lambda: writer.closed)
            _, reader, new_writer, _ = await self._open(kind, connection)
            writer.close_gate.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(new_writer.closed)
            next_task = await self._start_read(connection, new_writer)
            reader.feed_data(_reply())
            self.assertEqual(await next_task, _reply())

    async def test_auxiliary_session_does_not_allow_a_modbus_uart_mode_switch(self):
        connection, reader, writer, _ = await self._open("at")
        first = await self._start_read(connection, writer)
        reader.feed_data(_reply())
        await first
        with self.assertRaisesRegex(ConnectionError, "auxiliary_session_raw_route_conflict"):
            await connection.async_send_raw_payload(
                b"\x01\x03", payload_protocol="modbus_rtu", request_timeout=1,
            )
        self.assertEqual(writer.writes, [_query()])

    async def test_old_disconnect_cannot_fail_new_sessions_at_waiter(self):
        for kind in ("framed", "at"):
            with self.subTest(kind=kind):
                connection, old_reader, old_writer, old_run = await self._open(kind)
                old_writer.close_gate = asyncio.Event()
                old_reader.feed_eof()
                await _wait(lambda: connection._writer is None)
                _, reader, writer, _ = await self._open(kind, connection)
                task = self._task(connection.async_query("FWVER", request_timeout=1))
                await _wait(lambda: bool(writer.writes))
                old_writer.close_gate.set()
                await asyncio.gather(old_run, return_exceptions=True)
                self.assertFalse(task.done(), "Old cleanup failed the replacement's waiter")
                reader.feed_data(b"AT+FWVER:8.50.12.3\r\n")
                response = await task
                self.assertEqual(response.value, "8.50.12.3")

    async def test_enabled_binary_reader_does_not_treat_ascii_tid_as_plain_line(self):
        for kind in ("framed", "at"):
            connection, reader, writer, _ = await self._open(kind)
            task = await self._start_read(connection, writer)
            reader.feed_data(_framed(tid=0x2800) + _reply())
            self.assertEqual(await task, _reply())
            self.assertTrue(connection.connected)

    async def test_cancelled_framed_finalizer_cannot_remove_reused_tid_owner(self):
        for kind in ("framed", "at"):
            connection, _, writer, _ = await self._open(kind)
            writer.drain_gate = asyncio.Event()
            connection._tid._value = 0xFFFF  # next TID wraps to zero
            sender = (connection.async_send_collector if kind == "framed"
                      else connection.async_send_bridge_identity_probe)
            task = self._task(sender(fcode=4, payload=b"Q1\x01\r", request_timeout=1))
            await _wait(lambda: bool(writer.writes))
            self.assertEqual(writer.writes[0][:2], b"\x00\x00")
            pending = (connection._pending if kind == "framed"
                       else connection._pending_framed_response)
            old_future = pending[0]
            successor = asyncio.get_running_loop().create_future()
            # Simulate registry replacement before the old caller's finally.
            pending[0] = successor
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertIs(pending[0], successor)
            if kind == "at":
                self.assertEqual(connection._pending_framed_fcode[0], 4)
            old_future.cancel()
            successor.cancel()

    def test_new_session_has_no_inherited_grammar_or_future(self):
        old = AuxiliaryReadSession()
        old.enabled = True
        old.close()
        new = AuxiliaryReadSession()
        self.assertFalse(new.enabled)
        self.assertFalse(new.closed)
        self.assertIsNone(new.claim)


if __name__ == "__main__":
    unittest.main()
