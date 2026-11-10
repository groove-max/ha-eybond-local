"""Bounded AT/framed boundary repair, not a reconstruction of #6's TCP capture.

Only the reported eight-byte header and heartbeat shape are known. These are
synthetic complete streams, including adversarial valid binary TIDs.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.protocol import (
    build_collector_request, decode_header,
)
from custom_components.eybond_local.collector.transport.binary_framing import (
    runtime_eybond_header_error,
)
from custom_components.eybond_local.collector.transport.common import _PrefixedAsyncReader
from custom_components.eybond_local.collector.transport.connections import _CollectorConnection

COMMON = "custom_components.eybond_local.collector.transport.common"
CONNECTIONS = "custom_components.eybond_local.collector.transport.connections"
AT = b"AT+FWVER:8.50.7.18"


def frame(tid=0x01DA, *, fcode=1, payload=b"I30000200000000001", devcode=0x0102, address=1):
    return build_collector_request(
        tid, payload, devcode=devcode, collector_addr=address, fcode=fcode,
    )


class LateAtTerminatorTests(unittest.IsolatedAsyncioTestCase):
    async def prime(self, text=AT, *, initial=False):
        raw = asyncio.StreamReader()
        reader = _PrefixedAsyncReader(raw, text if initial else b"")
        if not initial:
            raw.feed_data(text)
        prefix = await reader.read_framed_prefix(await reader.readexactly(1))
        with patch(f"{COMMON}._AT_TEXT_RESPONSE_IDLE_TIMEOUT", .005):
            response = prefix + await reader.read_at_response(prefix=prefix)
        self.assertEqual(response, text)
        return reader, raw

    async def read_frame(self, reader):
        prefix = await reader.read_framed_prefix(await reader.readexactly(1))
        header = prefix + await reader.readexactly(8 - len(prefix))
        decoded = decode_header(header)
        self.assertEqual(runtime_eybond_header_error(decoded), "")
        return header + await reader.readexactly(decoded.payload_len)

    async def test_reported_header_repaired_only_after_idle_at_response(self):
        wire = frame()
        self.assertEqual((b"\r\n" + wire)[:8].hex(), "0d0a01da01020014")
        for text, tail in ((AT, b"\r\n"), (AT + b"\r", b"\n")):
            for fcode in (1, 2, 4):
                with self.subTest(text=text, fcode=fcode):
                    reader, raw = await self.prime(text)
                    # With a one-byte separator the unshifted FC byte is the
                    # actual address. Use an unambiguous invalid FC here; a
                    # valid original header must still win (separate test).
                    expected = frame(fcode=fcode, address=255 if tail == b"\n" else 1)
                    raw.feed_data(tail + expected + frame(0x1234))
                    raw.feed_eof()
                    self.assertEqual(await self.read_frame(reader), expected)
                    self.assertEqual(await self.read_frame(reader), frame(0x1234))

    async def test_all_tcp_splits_and_bytewise_delivery_preserve_next_frame(self):
        wire = b"\r\n" + frame()
        deliveries = [[wire], [bytes([b]) for b in wire]]
        deliveries += [[wire[:i], wire[i:]] for i in range(1, len(wire))]
        for chunks in deliveries:
            with self.subTest(lengths=list(map(len, chunks))):
                reader, raw = await self.prime()
                task = asyncio.create_task(self.read_frame(reader))
                try:
                    for chunk in chunks:
                        raw.feed_data(chunk)
                        await asyncio.sleep(0)
                    raw.feed_eof()
                    self.assertEqual(await asyncio.wait_for(task, .5), frame())
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

    async def test_valid_binary_crlf_tid_wins_even_if_shifted_header_is_valid(self):
        # Both offsets have legal headers. The original frame wins, without
        # waiting for the much longer hypothetical shifted payload.
        wire = frame(0x0D0A, fcode=4, payload=b"\x00\x04")
        self.assertEqual(runtime_eybond_header_error(decode_header(wire[2:10])), "")
        for text in (AT, AT + b"\r"):
            reader, raw = await self.prime(text)
            raw.feed_data(wire)
            self.assertEqual(await asyncio.wait_for(self.read_frame(reader), .2), wire)

    async def test_binary_ids_and_payload_text_are_not_stripped_after_at(self):
        for text, tid in ((AT, 0x0D0A), (AT + b"\r", 0x0A41)):
            for devcode in (1, 0x4154, 0x0D0A):
                with self.subTest(text=text, tid=tid, devcode=devcode):
                    reader, raw = await self.prime(text)
                    wire = frame(tid, devcode=devcode, fcode=4, payload=b"\r\nAT+SYST:inside\r\n")
                    raw.feed_data(wire)
                    raw.feed_eof()
                    self.assertEqual(await self.read_frame(reader), wire)

    async def test_ambiguous_lf_is_not_stripped_even_after_a_partial_terminator(self):
        reader, raw = await self.prime(AT + b"\r")
        wire = b"\n" + frame()
        self.assertEqual(runtime_eybond_header_error(decode_header(wire[:8])), "")
        # Do not speculate past this valid header or wait for its payload here.
        await self.assert_unshifted(reader, raw, wire)

    async def test_partial_terminator_before_another_at_response(self):
        reader, raw = await self.prime(AT + b"\r")
        raw.feed_data(b"\nAT+WFSS:-55\r\n" + frame())
        raw.feed_eof()
        prefix = await reader.read_framed_prefix(await reader.readexactly(1))
        self.assertEqual(prefix + await reader.read_at_response(prefix=prefix), b"AT+WFSS:-55\r\n")
        self.assertEqual(await self.read_frame(reader), frame())

    async def assert_unshifted(self, reader, raw, wire):
        raw.feed_data(wire)
        raw.feed_eof()
        prefix = await reader.read_framed_prefix(await reader.readexactly(1))
        self.assertEqual(prefix + await reader.readexactly(len(wire) - len(prefix)), wire)

    async def test_no_resync_without_pending_terminator_or_past_extra_junk(self):
        for text in (None, AT + b"\r\n", b"AT+FWVER?", b"AT+FWVER:\x01broken"):
            with self.subTest(text=text):
                if text is None:
                    raw = asyncio.StreamReader()
                    reader = _PrefixedAsyncReader(raw)
                else:
                    reader, raw = await self.prime(text)
                await self.assert_unshifted(reader, raw, b"\r\n" + frame())
        for tail in (b"\r\n\r\n" + frame(), b"\r\nNOT-A-FRAME", b" \r\n" + frame()):
            reader, raw = await self.prime()
            await self.assert_unshifted(reader, raw, tail)

    async def test_pending_terminator_cannot_cross_an_intervening_read(self):
        for direct in (False, True):
            reader, raw = await self.prime()
            before = frame(0x1234, fcode=4)
            raw.feed_data(before)
            if direct:
                self.assertEqual(await reader.readexactly(len(before)), before)
            else:
                self.assertEqual(await self.read_frame(reader), before)
            await self.assert_unshifted(reader, raw, b"\r\n" + frame())

    async def test_pending_terminator_is_not_shared_between_readers(self):
        await self.prime()
        raw = asyncio.StreamReader()
        reader = _PrefixedAsyncReader(raw)
        await self.assert_unshifted(reader, raw, b"\r\n" + frame())

    async def test_invalid_candidate_restores_lookahead_without_huge_payload_read(self):
        for header in (
            b"\x01\xda\x01\x02\xff\xff\x01\x04",  # oversized
            b"\x01\xda\x01\x02\x00\x01\x01\x04",  # negative length
            b"\x01\xda\x01\x02\x00\x14\x01\x20",  # invalid FC
        ):
            reader, raw = await self.prime()
            await self.assert_unshifted(reader, raw, b"\r\n" + header + b"suffix")

    async def test_empty_and_maximum_payload_are_bounded_and_preserved(self):
        for payload in (b"", bytes(range(256)) * 16):
            reader, raw = await self.prime()
            # Force an invalid original header even when the shifted payload
            # length's low byte coincides with a supported original FC.
            wire = frame(fcode=4, payload=payload, devcode=0xFFFF)
            raw.feed_data(b"\r\n" + wire + frame(0x1234))
            raw.feed_eof()
            self.assertEqual(await self.read_frame(reader), wire)
            self.assertEqual(await self.read_frame(reader), frame(0x1234))

    async def test_aabb_candidate_does_not_authorize_eybond_repair(self):
        reader, raw = await self.prime()
        await self.assert_unshifted(reader, raw, b"\r\n" + frame(0xAABB))

    async def test_prefix_origin_not_lost_by_complete_frame_lookahead(self):
        reader, raw = await self.prime(initial=True)
        self.assertTrue(reader.line_from_prefix_only())
        reader.begin_line()
        raw.feed_data(b"\r\n" + frame())
        self.assertEqual(await self.read_frame(reader), frame())
        self.assertFalse(reader.line_from_prefix_only())


class LateAtTerminatorConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_query_and_poll_keep_the_same_physical_session(self):
        # Real TCP on loopback only, no collector/HA/device network access.
        connection = _CollectorConnection(heartbeat_interval=60, write_timeout=1)
        connection._heartbeat_loop = AsyncMock()
        sessions = []

        async def accepted(reader, writer):
            sessions.append(asyncio.current_task())
            await connection.run(reader, writer, session_id="synthetic-late-crlf")

        server = await asyncio.start_server(accepted, "127.0.0.1", 0)
        peer_reader, peer_writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1],
        )
        pending = None
        try:
            self.assertTrue(await connection.wait_until_connected(1))
            pending = asyncio.create_task(connection.async_query("FWVER", request_timeout=1))
            self.assertEqual(await asyncio.wait_for(peer_reader.readuntil(b"\n"), 1),
                             b"AT+FWVER?\r\n")
            peer_writer.write(AT)
            await peer_writer.drain()
            self.assertEqual((await pending).value, "8.50.7.18")
            pending = asyncio.create_task(connection.async_send_collector(
                fcode=4, payload=b"READ", devcode=0x0994, collector_addr=255, request_timeout=1,
            ))
            header = decode_header(await asyncio.wait_for(peer_reader.readexactly(8), 1))
            self.assertEqual(await peer_reader.readexactly(header.payload_len), b"READ")
            wire = b"\r\n" + frame() + frame(header.tid, fcode=4, payload=b"fresh-values")
            for chunk in (wire[:1], wire[1:9], wire[9:]):
                peer_writer.write(chunk)
                await peer_writer.drain()
                await asyncio.sleep(.001)
            self.assertEqual((await pending)[1], b"fresh-values")
            self.assertTrue(connection.connected)
            self.assertTrue(await connection.wait_until_heartbeat(.02))
            self.assertEqual(connection._session_id, "synthetic-late-crlf")
            self.assertEqual(connection.collector_info.disconnect_count, 0)
        finally:
            if pending is not None:
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            peer_writer.close()
            await peer_writer.wait_closed()
            await connection.disconnect()
            server.close()
            await server.wait_closed()
            await asyncio.gather(*sessions, return_exceptions=True)

    async def start_connection(self, *, auxiliary=False, text=AT):
        connection = _CollectorConnection(heartbeat_interval=60, write_timeout=.5)
        connection._auxiliary_session.enabled = auxiliary
        at_reply = asyncio.get_running_loop().create_future()
        connection._pending_at_response = at_reply
        connection._pending_at_command = "FWVER"
        raw = asyncio.StreamReader()
        task = asyncio.create_task(connection._read_loop(raw))
        self.addAsyncCleanup(self.stop_task, task)
        raw.feed_data(text)
        self.assertEqual((await asyncio.wait_for(at_reply, .5)).value, "8.50.7.18")
        return connection, raw, task

    async def stop_task(self, task):
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_runtime_heartbeat_and_correlated_reply_survive_late_terminator(self):
        with patch(f"{COMMON}._AT_TEXT_RESPONSE_IDLE_TIMEOUT", .005):
            for auxiliary in (False, True):
                for text, tail in ((AT, b"\r\n"), (AT + b"\r", b"\n")):
                    with self.subTest(auxiliary=auxiliary, text=text):
                        connection, raw, task = await self.start_connection(auxiliary=auxiliary, text=text)
                        reply = asyncio.get_running_loop().create_future()
                        connection._pending[0x1234] = reply
                        connection._pending_fcode[0x1234] = 4
                        heartbeat = frame(address=255 if tail == b"\n" else 1)
                        raw.feed_data(tail + heartbeat + frame(0x1234, fcode=4, payload=b"telemetry"))
                        raw.feed_eof()
                        await asyncio.wait_for(task, .5)
                        self.assertTrue(reply.done(), connection.collector_info.last_disconnect_reason)
                        self.assertEqual(reply.result()[1], b"telemetry")
                        self.assertTrue(await connection.wait_until_heartbeat(.02))
                        self.assertEqual(connection.collector_info.last_disconnect_reason, "collector_eof")

    async def test_incomplete_frame_cannot_resolve_a_pending_request(self):
        with patch(f"{COMMON}._AT_TEXT_RESPONSE_IDLE_TIMEOUT", .005):
            for auxiliary in (False, True):
                connection, raw, task = await self.start_connection(auxiliary=auxiliary)
                reply = asyncio.get_running_loop().create_future()
                connection._pending[0x01DA] = reply
                connection._pending_fcode[0x01DA] = 4
                wire = b"\r\n" + frame(fcode=4)
                raw.feed_data(wire[:-1])
                await asyncio.sleep(.01)
                self.assertFalse(reply.done())
                raw.feed_data(wire[-1:])
                raw.feed_eof()
                await asyncio.wait_for(task, .5)
                self.assertEqual(reply.result()[1], b"I30000200000000001")

    async def test_unknown_header_without_at_context_stays_fatal(self):
        for auxiliary in (False, True):
            connection = _CollectorConnection(heartbeat_interval=60, write_timeout=.5)
            connection._auxiliary_session.enabled = auxiliary
            raw = asyncio.StreamReader()
            raw.feed_data(b"\r\n" + frame())
            raw.feed_eof()
            await asyncio.wait_for(connection._read_loop(raw), .5)
            self.assertEqual(connection.collector_info.last_disconnect_reason,
                             "collector_frame_function_invalid")
            self.assertIsNone(connection._last_heartbeat_monotonic)

    async def test_truncated_repair_keeps_deadline_eof_and_cancellation(self):
        with patch(f"{COMMON}._AT_TEXT_RESPONSE_IDLE_TIMEOUT", .005), patch(
            f"{CONNECTIONS}._FRAMED_HEADER_COMPLETION_TIMEOUT", .03,
        ):
            for auxiliary in (False, True):
                for end in ("timeout", "eof", "cancel"):
                    # At each prefix/header/payload boundary, never publish an
                    # incomplete recovered heartbeat or wait for a new deadline.
                    for size in (1, 2, 6, 8, 9, 10, 14, len(frame()) + 1):
                        with self.subTest(auxiliary=auxiliary, end=end, size=size):
                            connection, raw, task = await self.start_connection(auxiliary=auxiliary)
                            raw.feed_data((b"\r\n" + frame())[:size])
                            if end == "eof":
                                raw.feed_eof()
                            elif end == "cancel":
                                await asyncio.sleep(0)
                                task.cancel()
                            if end == "cancel":
                                with self.assertRaises(asyncio.CancelledError):
                                    await task
                            else:
                                await asyncio.wait_for(task, .5)
                                self.assertEqual(connection.collector_info.last_disconnect_reason,
                                                 "collector_eof" if end == "eof" else "collector_frame_header_timeout")
                            self.assertIsNone(connection._last_heartbeat_monotonic)


if __name__ == "__main__":
    unittest.main()
