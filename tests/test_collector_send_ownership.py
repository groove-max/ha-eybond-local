"""Commands, parser results and cleanup belong to one physical session."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_collector_auxiliary_session import _Writer, _wait, _query, _reply
from custom_components.eybond_local.collector.transport.common import _PrefixedAsyncReader
from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.payload.modbus import build_read_request
from custom_components.eybond_local.collector.transport.connections import (
    _CollectorAtConnection, _CollectorConnection,
)


class _Gate:
    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def __aenter__(self):
        self.entered.set()
        await self.release.wait()

    async def __aexit__(self, *args):
        return False


class CollectorSendOwnershipTests(unittest.IsolatedAsyncioTestCase):
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
            if kind == "framed":
                connection = _CollectorConnection(heartbeat_interval=300, write_timeout=1)
                connection._heartbeat_loop = AsyncMock()
            else:
                connection = _CollectorAtConnection(write_timeout=1, raw_passthrough_bootstrap="none")
        reader = asyncio.StreamReader()
        writer = _Writer(reader)
        run = self.task(connection.run(reader, writer))
        await _wait(lambda: connection._writer is writer and connection.connected)
        self.addAsyncCleanup(connection.disconnect)
        return connection, reader, writer, run

    def send(self, connection, kind, operation):
        if operation == "query":
            return connection.async_query("FWVER", request_timeout=0.05)
        if operation == "write":
            return connection.async_write("SYST", "1", request_timeout=0.05)
        if operation == "raw":
            return connection.async_send_raw_payload(b"QPI\r", request_timeout=0.05)
        if operation == "direct":
            return connection._async_write(b"synthetic")
        if kind == "framed":
            return connection.async_send_collector(
                fcode=4, payload=b"Q1\x01\r", devcode=767,
                collector_addr=255, request_timeout=0.05,
            )
        return connection.async_send_framed_inverter_payload(
            b"Q1\x01\r", devcode=767, collector_addr=255, request_timeout=0.05,
        )

    async def replace(self, kind, connection, reader, run):
        reader.feed_eof()
        await run
        return await self.open(kind, connection)

    async def assert_new_session_works(self, connection, reader, writer):
        count = len(writer.writes)
        task = self.task(connection.async_query("FWVER", request_timeout=1))
        await _wait(lambda: len(writer.writes) > count)
        reader.feed_data(b"AT+FWVER:8.50.12.3\r\n")
        self.assertEqual((await task).value, "8.50.12.3")

    async def test_queued_request_cannot_write_to_replacement(self):
        for kind in ("framed", "at"):
            for operation in ("frame", "query", "write", *(("raw",) if kind == "at" else ())):
                with self.subTest(kind=kind, operation=operation):
                    connection, reader, old_writer, run = await self.open(kind)
                    gate = _Gate()
                    connection._request_lock = gate
                    task = self.task(self.send(connection, kind, operation))
                    await gate.entered.wait()
                    _, new_reader, writer, _ = await self.replace(kind, connection, reader, run)
                    gate.release.set()
                    result = (await asyncio.gather(task, return_exceptions=True))[0]
                    self.assertEqual(writer.writes, [], "old command migrated to new socket")
                    self.assertIsInstance(result, ConnectionError)
                    self.assertTrue(connection.connected)
                    self.assertEqual(old_writer.writes, [])
                    await self.assert_new_session_works(connection, new_reader, writer)

    async def test_queued_write_cannot_write_to_replacement(self):
        for kind in ("framed", "at"):
            for operation in ("frame", "query", "write", "direct", *(("raw",) if kind == "at" else ())):
                with self.subTest(kind=kind, operation=operation):
                    connection, reader, old_writer, run = await self.open(kind)
                    gate = _Gate()
                    connection._write_lock = gate
                    task = self.task(self.send(connection, kind, operation))
                    await gate.entered.wait()
                    pending = list(getattr(connection, "_pending", {}).values())
                    pending += list(getattr(connection, "_pending_framed_response", {}).values())
                    pending += [getattr(connection, key, None) for key in (
                        "_pending_at_response", "_pending_response", "_pending_raw_response",
                    )]
                    _, new_reader, writer, _ = await self.replace(kind, connection, reader, run)
                    gate.release.set()
                    result = (await asyncio.gather(task, return_exceptions=True))[0]
                    self.assertEqual(writer.writes, [], "queued write acquired replacement writer")
                    self.assertIsInstance(result, ConnectionError)
                    self.assertTrue(connection.connected)
                    self.assertEqual(old_writer.writes, [])
                    for future in pending:
                        if future is not None:
                            self.assertFalse(future._log_traceback, "unretrieved disconnect failure")
                    await self.assert_new_session_works(connection, new_reader, writer)

    async def test_same_session_queued_query_still_completes(self):
        for kind in ("framed", "at"):
            for lock_name in ("_request_lock", "_write_lock"):
                connection, reader, writer, _ = await self.open(kind)
                gate = _Gate()
                setattr(connection, lock_name, gate)
                task = self.task(connection.async_query("FWVER", request_timeout=1))
                await gate.entered.wait()
                gate.release.set()
                await _wait(lambda: bool(writer.writes))
                reader.feed_data(b"AT+FWVER:8.50.12.3\r\n")
                self.assertEqual((await task).value, "8.50.12.3")
                self.assertTrue(connection.connected)

    async def test_cancellation_while_queued_never_sends_or_closes_session(self):
        for kind in ("framed", "at"):
            for lock_name in ("_request_lock", "_write_lock"):
                connection, reader, writer, _ = await self.open(kind)
                gate = _Gate()
                setattr(connection, lock_name, gate)
                task = self.task(connection.async_query("FWVER", request_timeout=1))
                await gate.entered.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                gate.release.set()
                self.assertEqual(writer.writes, [])
                self.assertTrue(connection.connected)
                await self.assert_new_session_works(connection, reader, writer)

    async def test_delayed_drain_cannot_publish_old_response_to_new_session(self):
        for kind in ("framed", "at"):
            connection, reader, old_writer, run = await self.open(kind)
            old_writer.drain_gate = asyncio.Event()
            task = self.task(connection.async_query("FWVER", request_timeout=1))
            await _wait(lambda: bool(old_writer.writes))
            future = connection._pending_at_response if kind == "framed" else connection._pending_response
            reader.feed_data(b"AT+FWVER:8.50.12.3\r\n")
            await _wait(future.done)
            _, new_reader, writer, _ = await self.replace(kind, connection, reader, run)
            old_writer.drain_gate.set()
            with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                await task
            self.assertEqual(connection.collector_info.smartess_collector_version, "")
            self.assertEqual(writer.writes, [])
            await self.assert_new_session_works(connection, new_reader, writer)

    async def test_auxiliary_completed_reply_keeps_its_socket_owner(self):
        for kind in ("framed", "at"):
            for replace in (False, True):
                with self.subTest(kind=kind, replace=replace):
                    connection, reader, old_writer, run = await self.open(kind)
                    old_writer.drain_gate = asyncio.Event()
                    task = self.task(connection.async_send_auxiliary_read(_query(), request_timeout=1))
                    await _wait(lambda: bool(old_writer.writes))
                    claim = connection._auxiliary_session.claim
                    reader.feed_data(_reply())
                    await _wait(claim.future.done)
                    reader.feed_eof()
                    await run
                    if replace:
                        _, new_reader, writer, _ = await self.open(kind, connection)
                    old_writer.drain_gate.set()
                    if replace:
                        with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                            await task
                        self.assertEqual(writer.writes, [])
                        self.assertFalse(connection._auxiliary_session.enabled)
                        await self.assert_new_session_works(connection, new_reader, writer)
                    else:
                        # A completed reply followed by ordinary EOF is valid;
                        # closed=True alone cannot distinguish replacement.
                        self.assertEqual(await task, _reply())

    async def test_old_auxiliary_failure_is_not_a_successor_recovery_signal(self):
        for kind in ("framed", "at"):
            for drain_timeout in (False, True):
                with self.subTest(kind=kind, drain_timeout=drain_timeout):
                    connection, reader, old_writer, run = await self.open(kind)
                    connection._write_timeout = 0.1 if drain_timeout else 1
                    old_writer.drain_gate = asyncio.Event()
                    task = self.task(connection.async_send_auxiliary_read(_query(), request_timeout=1))
                    await _wait(lambda: bool(old_writer.writes))
                    future = connection._auxiliary_session.claim.future
                    _, new_reader, writer, _ = await self.replace(kind, connection, reader, run)
                    if not drain_timeout:
                        old_writer.drain_gate.set()
                    with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                        await task
                    self.assertFalse(future._log_traceback)
                    self.assertFalse(writer.closed)
                    self.assertFalse(connection._auxiliary_session.enabled)
                    await self.assert_new_session_works(connection, new_reader, writer)

    async def test_detached_reader_cannot_deliver_during_old_writer_cleanup(self):
        for kind in ("framed", "at"):
            for ignore_first_cancel in (False, True):
                for pending in (False, True):
                    with self.subTest(kind=kind, ignore_first_cancel=ignore_first_cancel, pending=pending):
                        connection, old_reader, old_writer, _ = await self.open(kind)
                        reader_task = connection._reader_task
                        entered = asyncio.Event()
                        release = asyncio.Event()
                        self.addCleanup(release.set)
                        read_at_response = _PrefixedAsyncReader.read_at_response

                        async def delayed_line(reader):
                            line = await read_at_response(reader)
                            if reader._reader is old_reader:
                                entered.set()
                                try:
                                    await release.wait()
                                except asyncio.CancelledError:
                                    if not ignore_first_cancel:
                                        raise
                                    # Model wait_for's completion/cancel race:
                                    # cancellation is not an ownership fence.
                                    await release.wait()
                            return line

                        with patch.object(_PrefixedAsyncReader, "read_at_response", delayed_line):
                            old_reader.feed_data(b"AT+FWVER:old-session\r\n")
                            await asyncio.wait_for(entered.wait(), 1)
                            old_writer.close_gate = asyncio.Event()
                            self.addCleanup(old_writer.close_gate.set)
                            cleanup = self.task(connection.disconnect())
                            await _wait(lambda: old_writer.closed)
                            _, new_reader, writer, _ = await self.open(kind, connection)
                            if pending:
                                query = self.task(connection.async_query("FWVER", request_timeout=1))
                                await _wait(lambda: bool(writer.writes))
                                future = (connection._pending_at_response if kind == "framed"
                                          else connection._pending_response)
                            release.set()
                            await asyncio.wait_for(asyncio.gather(reader_task, return_exceptions=True), 1)
                            self.assertEqual(connection.collector_info.smartess_collector_version, "")
                            self.assertEqual(connection.collector_info.last_disconnect_reason, "")
                            if pending:
                                self.assertFalse(future.done(), "retired reader completed successor request")
                                new_reader.feed_data(b"AT+FWVER:8.50.12.3\r\n")
                                self.assertEqual((await query).value, "8.50.12.3")
                            else:
                                await self.assert_new_session_works(connection, new_reader, writer)
                            old_writer.close_gate.set()
                            await cleanup
                            self.assertTrue(connection.connected)

    async def test_old_drain_timeout_is_not_a_reconnect_error_for_successor(self):
        for kind in ("framed", "at"):
            connection, reader, old_writer, run = await self.open(kind)
            connection._write_timeout = 0.1
            old_writer.drain_gate = asyncio.Event()
            task = self.task(connection.async_query("FWVER", request_timeout=1))
            await _wait(lambda: bool(old_writer.writes))
            _, new_reader, writer, _ = await self.replace(kind, connection, reader, run)
            # Timeout occurs on the OLD drain after the successor is active.
            # collector_write_timeout would tell the hub to reconnect that successor.
            with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                await task
            self.assertTrue(connection.connected)
            self.assertEqual(writer.writes, [])
            await self.assert_new_session_works(connection, new_reader, writer)

    async def test_retired_binary_read_cannot_complete_reused_tid_or_record_identity(self):
        for kind in ("framed", "at"):
            for heartbeat in (False, True):
                with self.subTest(kind=kind, heartbeat=heartbeat):
                    connection, old_reader, old_writer, _ = await self.open(kind)
                    reader_task = connection._reader_task
                    entered, release = asyncio.Event(), asyncio.Event()
                    self.addCleanup(release.set)
                    payload = b"E50000200000000001" if heartbeat else bytes(20)
                    readexactly = _PrefixedAsyncReader.readexactly

                    async def delayed_payload(reader, size):
                        value = await readexactly(reader, size)
                        if reader._reader is old_reader and size == len(payload):
                            entered.set()
                            try:
                                await release.wait()
                            except asyncio.CancelledError:
                                await release.wait()
                        return value

                    with patch.object(_PrefixedAsyncReader, "readexactly", delayed_payload):
                        old_reader.feed_data(build_collector_request(
                            1, payload, devcode=767, collector_addr=255,
                            fcode=1 if heartbeat else 4,
                        ))
                        await asyncio.wait_for(entered.wait(), 1)
                        old_writer.close_gate = asyncio.Event()
                        self.addCleanup(old_writer.close_gate.set)
                        cleanup = self.task(connection.disconnect())
                        await _wait(lambda: old_writer.closed)
                        _, new_reader, writer, _ = await self.open(kind, connection)
                        connection._tid._value = 0
                        if kind == "framed":
                            query = self.task(connection.async_send_collector(
                                fcode=4, payload=b"Q1\x01\r", devcode=767,
                                collector_addr=255, request_timeout=1,
                            ))
                        else:
                            query = self.task(connection.async_send_framed_inverter_payload(
                                b"Q1\x01\r", devcode=767, collector_addr=255, request_timeout=1,
                            ))
                        await _wait(lambda: bool(writer.writes))
                        self.assertEqual(writer.writes[0][:2], b"\x00\x01")
                        pending = (connection._pending if kind == "framed"
                                   else connection._pending_framed_response)
                        future = pending[1]
                        release.set()
                        await asyncio.wait_for(asyncio.gather(reader_task, return_exceptions=True), 1)
                        self.assertFalse(future.done())
                        self.assertEqual(connection.collector_info.collector_pn, "")
                        self.assertEqual(connection.collector_info.last_disconnect_reason, "")
                        new_reader.feed_data(build_collector_request(
                            1, b"new-response", devcode=767, collector_addr=255, fcode=4,
                        ))
                        result = await query
                        self.assertEqual(result[1] if kind == "framed" else result, b"new-response")
                        old_writer.close_gate.set()
                        await cleanup
                        await self.assert_new_session_works(connection, new_reader, writer)

    async def test_retired_reader_errors_do_not_change_successor_diagnostics(self):
        for kind in ("framed", "at"):
            for error in (asyncio.TimeoutError(), asyncio.IncompleteReadError(b"", 1), ConnectionResetError()):
                with self.subTest(kind=kind, error=type(error).__name__):
                    connection, old_reader, old_writer, _ = await self.open(kind)
                    reader_task = connection._reader_task
                    entered, release = asyncio.Event(), asyncio.Event()
                    self.addCleanup(release.set)
                    read_at_response = _PrefixedAsyncReader.read_at_response

                    async def delayed_error(reader):
                        value = await read_at_response(reader)
                        if reader._reader is old_reader:
                            entered.set()
                            try:
                                await release.wait()
                            except asyncio.CancelledError:
                                await release.wait()
                            raise error
                        return value

                    with patch.object(_PrefixedAsyncReader, "read_at_response", delayed_error):
                        old_reader.feed_data(b"AT+FWVER:old-session\r\n")
                        await asyncio.wait_for(entered.wait(), 1)
                        old_writer.close_gate = asyncio.Event()
                        self.addCleanup(old_writer.close_gate.set)
                        cleanup = self.task(connection.disconnect())
                        await _wait(lambda: old_writer.closed)
                        _, new_reader, writer, _ = await self.open(kind, connection)
                        release.set()
                        await asyncio.wait_for(asyncio.gather(reader_task, return_exceptions=True), 1)
                        self.assertEqual(connection.collector_info.last_disconnect_reason, "")
                        self.assertEqual(connection.collector_info.raw_unhandled_line_count, 0)
                        old_writer.close_gate.set()
                        await cleanup
                        await self.assert_new_session_works(connection, new_reader, writer)

    async def test_retired_raw_reader_cannot_complete_successor_raw_request(self):
        for protocol in ("", "modbus_rtu"):
            with self.subTest(protocol=protocol):
                connection, old_reader, old_writer, _ = await self.open("at")
                reader_task = connection._reader_task
                entered, release = asyncio.Event(), asyncio.Event()
                self.addCleanup(release.set)
                method = "readexactly" if protocol else "readuntil"
                original = getattr(_PrefixedAsyncReader, method)
                request = build_read_request(1, 0, 1) if protocol else b"QPI\r"
                old_reply = b"\x01\x03\x02\x00\x01\x79\x84" if protocol else b"(PI30\r"
                new_reply = b"\x01\x03\x02\x00\x02\x39\x85" if protocol else b"(PI18\r"

                async def delayed_raw(reader, size_or_separator):
                    value = await original(reader, size_or_separator)
                    if reader._reader is old_reader and (not protocol or size_or_separator == 4):
                        entered.set()
                        try:
                            await release.wait()
                        except asyncio.CancelledError:
                            await release.wait()
                    return value

                def send():
                    return connection.async_send_raw_payload(
                        request, request_timeout=1, payload_protocol=protocol,
                    )

                with patch.object(_PrefixedAsyncReader, method, delayed_raw):
                    old_query = self.task(send())
                    await _wait(lambda: bool(old_writer.writes))
                    old_reader.feed_data(old_reply)
                    await asyncio.wait_for(entered.wait(), 1)
                    old_writer.close_gate = asyncio.Event()
                    self.addCleanup(old_writer.close_gate.set)
                    cleanup = self.task(connection.disconnect())
                    await _wait(lambda: old_writer.closed)
                    with self.assertRaises(ConnectionError):
                        await old_query
                    _, new_reader, writer, _ = await self.open("at", connection)
                    query = self.task(send())
                    await _wait(lambda: bool(writer.writes))
                    future = (
                        connection._runtime_modbus.claim.future
                        if protocol else connection._pending_raw_response
                    )
                    release.set()
                    await asyncio.wait_for(asyncio.gather(reader_task, return_exceptions=True), 1)
                    self.assertFalse(future.done())
                    self.assertEqual(connection.collector_info.raw_response_count, 0)
                    self.assertEqual(connection.collector_info.raw_last_parser, "")
                    self.assertEqual(connection.collector_info.last_disconnect_reason, "")
                    new_reader.feed_data(new_reply)
                    self.assertEqual(await query, new_reply)
                    old_writer.close_gate.set()
                    await cleanup
                    await self.assert_new_session_works(connection, new_reader, writer)

    async def test_raw_spacing_wait_cannot_send_on_successor(self):
        connection, reader, _, run = await self.open("at")
        gate = _Gate()
        async def spacing():
            async with gate:
                return 0
        connection._async_wait_raw_passthrough_spacing_locked = spacing
        task = self.task(self.send(connection, "at", "raw"))
        await gate.entered.wait()
        _, new_reader, writer, _ = await self.replace("at", connection, reader, run)
        gate.release.set()
        with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
            await task
        self.assertEqual(writer.writes, [])
        self.assertFalse(connection._raw_passthrough_bootstrapped)
        await self.assert_new_session_works(connection, new_reader, writer)

    async def test_confirmed_reply_then_eof_is_not_a_failed_operation(self):
        for kind in ("framed", "at"):
            for operation, command, value in (("query", "FWVER", "8.50.12.3"), ("write", "SYST", "W000")):
                connection, reader, writer, run = await self.open(kind)
                writer.drain_gate = asyncio.Event()
                task = self.task(self.send(connection, kind, operation))
                await _wait(lambda: bool(writer.writes))
                future = connection._pending_at_response if kind == "framed" else connection._pending_response
                reader.feed_data(f"AT+{command}:{value}\r\n".encode())
                await _wait(future.done)
                reader.feed_eof()
                await run
                writer.drain_gate.set()
                self.assertEqual((await task).value, value)
                self.assertFalse(connection.connected)

    async def test_ownership_guard_distinguishes_epoch_even_if_writer_reference_reused(self):
        from custom_components.eybond_local.collector.transport.send_ownership import SocketSendOwner
        for kind in ("framed", "at"):
            connection, _, _, _ = await self.open(kind)
            owner = SocketSendOwner.capture(connection)
            original_epoch = connection._run_epoch
            connection._run_epoch += 1
            try:
                for check in (owner.check, owner.check_reply):
                    with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                        check(connection)
            finally:
                connection._run_epoch = original_epoch

    async def test_raw_bootstrap_cannot_write_uart_or_mark_successor_bootstrapped(self):
        connection, reader, old_writer, run = await self.open("at")
        connection._raw_passthrough_bootstrap = "uart"
        old_writer.drain_gate = asyncio.Event()
        task = self.task(self.send(connection, "at", "raw"))
        await _wait(lambda: bool(old_writer.writes))
        self.assertEqual(old_writer.writes, [b"AT+UART?\r\n"])
        reader.feed_data(b"AT+UART:2400,8,1,NONE\r\n")
        await _wait(connection._pending_response.done)
        _, new_reader, writer, _ = await self.replace("at", connection, reader, run)
        old_writer.drain_gate.set()
        with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
            await task
        self.assertFalse(connection._raw_passthrough_bootstrapped)
        self.assertEqual(writer.writes, [])
        self.assertEqual(old_writer.writes, [b"AT+UART?\r\n"])
        await self.assert_new_session_works(connection, new_reader, writer)

    async def test_bootstrap_cancel_is_not_saved_as_a_completed_attempt(self):
        connection, reader, writer, _ = await self.open("at")
        connection._raw_passthrough_bootstrap = "uart"
        writer.drain_gate = asyncio.Event()
        task = self.task(self.send(connection, "at", "raw"))
        await _wait(lambda: bool(writer.writes))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(connection._raw_passthrough_bootstrapped)
        self.assertEqual(writer.writes, [b"AT+UART?\r\n"])
        writer.drain_gate.set()
        await self.assert_new_session_works(connection, reader, writer)


if __name__ == "__main__":
    unittest.main()
