"""Issue #70: query echo/status must not become a binary frame or metadata.

Synthetic wire shapes based on the log prefix and old firmware's %s:%c001
unknown-command response, not a claim of a complete user packet capture.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from unittest.mock import AsyncMock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.at import (
    CollectorAtQueryStatus, parse_at_response,
)
from custom_components.eybond_local.collector.at_runtime import read_runtime_collector_at_values
from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
from custom_components.eybond_local.collector.transport.binary_framing import BinaryFramingError
from custom_components.eybond_local.collector.transport.common import _PrefixedAsyncReader
from custom_components.eybond_local.collector.transport.connections import (
    _CollectorAtConnection, _CollectorConnection,
)

_COMMON = "custom_components.eybond_local.collector.transport.common"


def _frame(tid=0x4142, payload=b"next-binary-reply", *, devcode=1):
    return build_collector_request(
        tid, payload, devcode=devcode, collector_addr=1, fcode=4,
    )


class QueryStatusParsingTests(unittest.TestCase):
    def test_query_status_is_not_a_setting_value(self):
        for separator in (b"", b"\r\n"):
            with self.subTest(separator=separator):
                with self.assertRaises(CollectorAtQueryStatus) as caught:
                    parse_at_response(b"AT+HTBT?" + separator + b":R001\r\n")
                self.assertEqual(caught.exception.command, "HTBT")
                self.assertEqual(caught.exception.status, "R001")
                self.assertEqual(str(caught.exception), "collector_at_query_status_R001")

    def test_real_version_and_other_named_values_are_not_status(self):
        for command in ("ATVER", "FWVER", "INTPARA41"):
            response = parse_at_response(f"AT+{command}:R001\r\n")
            self.assertEqual(response.value, "R001")


class QueryStatusStreamTests(unittest.IsolatedAsyncioTestCase):
    def make_connection(self, at=False, auxiliary=False, command="HTBT", tid=0x4142):
        connection = (_CollectorAtConnection(write_timeout=.5) if at else
                      _CollectorConnection(heartbeat_interval=60, write_timeout=.5))
        connection._auxiliary_session.enabled = auxiliary
        query = asyncio.get_running_loop().create_future()
        reply = asyncio.get_running_loop().create_future()
        connection._pending_at_command = command
        if at:
            connection._pending_response = query
            connection._pending_framed_response[tid] = reply
            connection._pending_framed_fcode[tid] = 4
        else:
            connection._pending_at_response = query
            connection._pending[tid] = reply
            connection._pending_fcode[tid] = 4
        return connection, query, reply

    async def deliver(self, connection, chunks):
        reader = asyncio.StreamReader()
        task = asyncio.create_task(connection._read_loop(reader))
        try:
            for chunk in chunks:
                reader.feed_data(chunk)
                await asyncio.sleep(0)
            reader.feed_eof()
            await asyncio.wait_for(task, 1)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_query_status_preserves_framed_reply_for_all_tcp_splits(self):
        for at in (False, True):
            for auxiliary in (False, True):
                wire = b"AT+HTBT?\r\n:R001\r\n" + _frame()
                deliveries = [[wire], [bytes([b]) for b in wire]]
                deliveries += [[wire[:i], wire[i:]] for i in range(1, len(wire))]
                for chunks in deliveries:
                    with self.subTest(at=at, auxiliary=auxiliary, lengths=list(map(len, chunks))):
                        connection, query, reply = self.make_connection(at, auxiliary)
                        await self.deliver(connection, chunks)
                        self.assertTrue(query.done())
                        self.assertIsInstance(query.exception(), CollectorAtQueryStatus)
                        self.assertTrue(reply.done())
                        self.assertEqual(reply.result()[1], b"next-binary-reply")
                        self.assertEqual(connection.collector_info.last_disconnect_reason, "collector_eof")

    async def test_framed_leading_crlf_and_query_echo_without_internal_crlf(self):
        for wire in (b"\r\nAT+HTBT?\r\n:R001\r\n", b"AT+HTBT?:R001\r\n"):
            connection, query, reply = self.make_connection()
            await self.deliver(connection, [wire + _frame()])
            self.assertIsInstance(query.exception(), CollectorAtQueryStatus)
            self.assertEqual(reply.result()[1], b"next-binary-reply")

    async def test_unmatched_status_does_not_complete_another_command(self):
        for at in (False, True):
            connection, query, reply = self.make_connection(at, command="FWVER")
            await self.deliver(connection, [b"AT+HTBT?\r\n:R001\r\n" + _frame()])
            self.assertFalse(query.done())
            self.assertEqual(reply.result()[1], b"next-binary-reply")
            self.assertFalse(connection.collector_info.smartess_collector_version)

    async def test_query_echo_alone_does_not_consume_a_following_valid_reply(self):
        for at in (False, True):
            connection, query, reply = self.make_connection(at)
            await self.deliver(connection, [b"AT+HTBT?\r\nAT+HTBT:60\r\n" + _frame()])
            self.assertEqual(query.result().value, "60")
            self.assertEqual(reply.result()[1], b"next-binary-reply")

    async def test_valid_binary_colon_prefix_after_echo_is_preserved(self):
        # A legal TID ':R' must not be consumed as the start of a text status.
        # Also exercise mismatches after one or two matching status digits.
        for tid, devcode in ((0x3A52, 0x3132), (0x3A52, 0x3030), (0x3A00, 1)):
            for at in (False, True):
                connection, query, reply = self.make_connection(at, tid=tid)
                payload = b"AT+HTBT?\r\n:R001\r\ninside-binary"
                await self.deliver(connection, [
                    b"AT+HTBT?\r\n" + _frame(tid, payload, devcode=devcode),
                ])
                self.assertFalse(query.done())
                self.assertEqual(reply.result()[1], payload)

    async def test_error_response_does_not_become_firmware_or_identity(self):
        for at in (False, True):
            for command in ("FWVER", "DTUPN", "CLDSRVHOST1"):
                connection, query, reply = self.make_connection(at, command=command)
                await self.deliver(connection, [
                    f"AT+{command}?\r\n:R001\r\n".encode() + _frame(),
                ])
                self.assertIsInstance(query.exception(), CollectorAtQueryStatus)
                self.assertEqual(reply.result()[1], b"next-binary-reply")
                info = connection.collector_info
                self.assertFalse(info.collector_pn)
                self.assertFalse(info.smartess_collector_version)
                self.assertFalse(info.collector_server_endpoint)

    async def test_runtime_metadata_continues_after_rejected_query(self):
        class Transport:
            async def async_query(self, command):
                if command == "ENUPMODE":
                    return parse_at_response(b"AT+ENUPMODE?\r\n:R001\r\n")
                return parse_at_response(f"AT+{command}:60\r\n")

        result = await read_runtime_collector_at_values(Transport())
        self.assertEqual(result.failed_commands, 1)
        self.assertFalse(result.timed_out)
        self.assertNotIn("collector_upload_mode", result.values)
        self.assertEqual(result.values["collector_cloud_heartbeat_value"], "60")

    async def prime_reader(self, tail=b""):
        raw = asyncio.StreamReader()
        reader = _PrefixedAsyncReader(raw)
        raw.feed_data(b"HTBT?\r\n" + tail)
        self.assertEqual(await reader.read_at_response(), b"HTBT?\r\n")
        return reader, raw

    async def test_status_boundary_survives_idle_gap_after_echo(self):
        reader, raw = await self.prime_reader()
        with patch(f"{_COMMON}._AT_TEXT_RESPONSE_IDLE_TIMEOUT", .001):
            await asyncio.sleep(.01)
            raw.feed_data(b"R001\r\n")
            self.assertEqual(await reader.read_at_query_status(b":"), b"AT+HTBT?\r\n:R001\r\n")

    async def test_no_generic_status_skip_without_immediately_preceding_echo(self):
        reader, raw = await self.prime_reader(b"R001\r\n")
        self.assertIsNone(await reader.read_at_query_status(b"A"))
        self.assertIsNone(await reader.read_at_query_status(b":"))
        self.assertEqual(await reader.readexactly(6), b"R001\r\n")
        other = _PrefixedAsyncReader(asyncio.StreamReader())
        self.assertIsNone(await other.read_at_query_status(b":"))

    async def test_write_echo_and_malformed_query_do_not_enable_status_skip(self):
        for tail in (b"HTBT=60\r\n", b"HTBT ?\r\n", b"HTBT?\n", b"HTBT:R001\r\n"):
            raw = asyncio.StreamReader()
            reader = _PrefixedAsyncReader(raw)
            raw.feed_data(tail)
            with patch(f"{_COMMON}._AT_TEXT_RESPONSE_IDLE_TIMEOUT", .005):
                self.assertEqual(await reader.read_at_response(), tail)
            self.assertIsNone(await reader.read_at_query_status(b":"))

    async def test_unknown_tail_is_not_skipped_to_reach_next_valid_frame(self):
        for tail in (b":R001junk\r\n", b":R01\r\n", b":W001\r\n", b"garbage"):
            connection, query, reply = self.make_connection()
            await self.deliver(connection, [b"AT+HTBT?\r\n" + tail + _frame()])
            self.assertFalse(query.done())
            self.assertFalse(reply.done())
            self.assertTrue(connection.collector_info.last_disconnect_reason.startswith("collector_frame_"))

    async def test_status_tail_timeout_eof_and_cancellation_are_bounded(self):
        for terminal in ("timeout", "eof", "cancel"):
            with self.subTest(terminal=terminal):
                reader, raw = await self.prime_reader(b"R00")
                if terminal == "eof":
                    raw.feed_eof()
                with patch(f"{_COMMON}._AT_QUERY_STATUS_COMPLETION_TIMEOUT", .02):
                    task = asyncio.create_task(reader.read_at_query_status(b":"))
                    if terminal == "cancel":
                        await asyncio.sleep(.005)
                        task.cancel()
                    error = {"timeout": BinaryFramingError, "eof": asyncio.IncompleteReadError,
                             "cancel": asyncio.CancelledError}[terminal]
                    with self.assertRaises(error):
                        await asyncio.wait_for(task, .5)
                self.assertEqual(await reader.readexactly(3), b"R00")

    async def test_eof_at_every_truncated_status_boundary(self):
        for tail in (b"R001\r\n"[:i] for i in range(6)):
            reader, raw = await self.prime_reader(tail)
            raw.feed_eof()
            with self.assertRaises(asyncio.IncompleteReadError):
                await asyncio.wait_for(reader.read_at_query_status(b":"), .5)
            self.assertEqual(await reader.readexactly(len(tail)), tail)

    async def test_real_tcp_query_error_releases_lock_and_polling_continues(self):
        for at in (False, True):
            with self.subTest(at=at):
                connection = (_CollectorAtConnection(write_timeout=.5) if at else
                              _CollectorConnection(heartbeat_interval=60, write_timeout=.5))
                if not at:
                    connection._heartbeat_loop = AsyncMock()
                sessions = []

                async def accepted(reader, writer):
                    sessions.append(asyncio.current_task())
                    await connection.run(reader, writer, session_id="same-socket")

                server = await asyncio.start_server(accepted, "127.0.0.1", 0)
                peer_reader, peer_writer = await asyncio.open_connection(
                    "127.0.0.1", server.sockets[0].getsockname()[1],
                )
                query_task = None
                try:
                    self.assertTrue(await connection.wait_until_connected(.5))
                    query_task = asyncio.create_task(connection.async_query("HTBT", request_timeout=.5))
                    self.assertEqual(await asyncio.wait_for(peer_reader.readuntil(b"\n"), .5), b"AT+HTBT?\r\n")
                    peer_writer.write(b"AT+HTBT?\r\n")
                    await peer_writer.drain()
                    await asyncio.sleep(.01)
                    peer_writer.write(b":R001\r\n")
                    await peer_writer.drain()
                    with self.assertRaises(CollectorAtQueryStatus):
                        await query_task
                    self.assertTrue(connection.connected)
                    self.assertFalse(connection.collector_info.heartbeat_fresh)

                    # A real value R001 is a legitimate AT interpreter version.
                    query_task = asyncio.create_task(connection.async_query("ATVER", request_timeout=.5))
                    self.assertEqual(await asyncio.wait_for(peer_reader.readuntil(b"\n"), .5), b"AT+ATVER?\r\n")
                    peer_writer.write(b"AT+ATVER:R001\r\n")
                    await peer_writer.drain()
                    self.assertEqual((await query_task).value, "R001")

                    if at:
                        request = connection.async_send_framed_inverter_payload(
                            b"READ", devcode=1, collector_addr=1, request_timeout=.5,
                        )
                    else:
                        request = connection.async_send_collector(
                            fcode=4, payload=b"READ", devcode=1, collector_addr=1,
                            request_timeout=.5,
                        )
                    query_task = asyncio.create_task(request)
                    header = decode_header(await asyncio.wait_for(peer_reader.readexactly(8), .5))
                    self.assertEqual(await peer_reader.readexactly(header.payload_len), b"READ")
                    peer_writer.write(_frame(header.tid, b"fresh-inverter-data"))
                    await peer_writer.drain()
                    response = await query_task
                    self.assertEqual(response if at else response[1], b"fresh-inverter-data")
                    self.assertEqual(connection._session_id, "same-socket")
                    self.assertEqual(connection.collector_info.disconnect_count, 0)
                finally:
                    if query_task is not None:
                        if not query_task.done():
                            query_task.cancel()
                        await asyncio.gather(query_task, return_exceptions=True)
                    peer_writer.close()
                    with contextlib.suppress(ConnectionError):
                        await peer_writer.wait_closed()
                    await connection.disconnect()
                    server.close()
                    await server.wait_closed()
                    await asyncio.gather(*sessions, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
