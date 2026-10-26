"""Exact-socket diagnostics survive closure without becoming identity authority."""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.identity_probe import build_identity_probe_request
from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.collector.transport import (
    _PendingCollectorSocket, _SharedEybondListener,
)


PN = "E50000200000009777"


class ProbeWriter:
    def __init__(self, reader, *, response=b"", eof=False, fail=""):
        self.reader = reader
        self.response = response
        self.eof = eof
        self.fail = fail
        self.writes = []
        self.closed = False
        self.draining = asyncio.Event()

    def write(self, data):
        if self.fail == "write":
            raise OSError("private-error-marker")
        self.writes.append(data)

    async def drain(self):
        self.draining.set()
        if self.fail == "drain":
            raise ConnectionResetError("private-error-marker")
        if self.fail == "block_drain":
            await asyncio.Event().wait()
        if self.response:
            self.reader.feed_data(self.response)
        if self.eof:
            self.reader.feed_eof()

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class IdentityProbeProgressTests(unittest.IsolatedAsyncioTestCase):
    def setup_probe(self, **kwargs):
        listener = _SharedEybondListener(host="127.0.0.1", port=8899)
        reader = asyncio.StreamReader()
        writer = ProbeWriter(reader, **kwargs)
        pending = _PendingCollectorSocket(
            remote_ip="192.0.2.56", reader=reader, writer=writer,
            session_id="probe-socket", remote_port=40000,
        )
        listener._remember_session(
            session_id=pending.session_id, remote_ip=pending.remote_ip,
            remote_port=pending.remote_port,
        )
        listener._pending_sockets[pending.session_id] = pending
        return listener, pending, writer

    def progress(self, listener):
        return listener.session_inventory_diagnostics()["sessions"][0]["identity_probes"][-1]

    async def exchange(self, listener, pending, protocol="at_text", **kwargs):
        request = build_identity_probe_request(protocol)
        return await listener._exchange_identity_probe(pending, request, **kwargs)

    async def test_at_identity_echo_and_reply_have_separate_payload_free_evidence(self):
        response = b"\r\nAT+DTUPN?\r\n" + f"AT+DTUPN:{PN}\r\n".encode()
        listener, pending, writer = self.setup_probe(response=response)
        with self.assertLogs(
            "custom_components.eybond_local.collector.transport.listener", level="DEBUG",
        ) as logs:
            result = await self.exchange(listener, pending)
        self.assertEqual(result[1], PN)
        progress = self.progress(listener)
        self.assertEqual(progress["response_bytes"], len(response))
        self.assertEqual(progress["outcome"], "identity_received")
        self.assertTrue(progress["drain_completed"])
        self.assertEqual(writer.writes, [build_identity_probe_request("at_text").payload])
        self.assertNotIn(PN, json.dumps(progress) + str(logs.output))
        # Diagnostics alone never bind an identity or produce a discoverable device.
        self.assertEqual(listener.discovered_collector_sessions(), ())

    async def test_unrecognized_at_bytes_are_retained_after_peer_close(self):
        listener, pending, _ = self.setup_probe(response=b"ERROR\r\n", eof=True)
        self.assertEqual((await self.exchange(listener, pending))[1], "")
        listener._mark_session_state(pending.session_id, "parked_peer_closed")
        progress = self.progress(listener)
        self.assertEqual(progress["response_bytes"], 7)
        self.assertEqual(progress["outcome"], "peer_closed")
        self.assertTrue(progress["peer_eof"])
        self.assertTrue(progress["write_returned"])

    async def test_partial_at_timeout_is_not_reported_as_zero_bytes(self):
        listener, pending, _ = self.setup_probe(response=b"AT+DTUPN:E500002")
        with self.assertRaises(asyncio.TimeoutError):
            await self.exchange(listener, pending)
        progress = self.progress(listener)
        self.assertEqual(progress["outcome"], "timeout")
        self.assertEqual(progress["phase"], "waiting_response")
        self.assertEqual(progress["response_bytes"], len(b"AT+DTUPN:E500002"))
        self.assertEqual(listener.discovered_collector_sessions(), ())

    async def test_silent_timeout_records_completed_write_and_no_read_bytes(self):
        listener, pending, _ = self.setup_probe()
        with self.assertRaises(asyncio.TimeoutError):
            await self.exchange(listener, pending)
        self.assertTrue(self.progress(listener)["drain_completed"])
        self.assertEqual(self.progress(listener)["response_bytes"], 0)
        self.assertFalse(self.progress(listener)["peer_eof"])

    async def test_eof_before_query_never_writes_to_dead_socket(self):
        listener, pending, writer = self.setup_probe()
        pending.reader.feed_eof()
        self.assertEqual(await listener._identify_pending_socket_for_route(
            pending, session_protocol="at_text",
        ), "")
        self.assertEqual(writer.writes, [])
        progress = self.progress(listener)
        self.assertEqual(progress["outcome"], "closed_before_write")
        self.assertTrue(progress["peer_eof"])
        self.assertFalse(progress["write_started"])

    async def test_closing_writer_is_distinct_from_peer_eof(self):
        listener, pending, writer = self.setup_probe()
        writer.closed = True
        await self.exchange(listener, pending)
        self.assertEqual(self.progress(listener)["outcome"], "closed_before_write")
        self.assertFalse(self.progress(listener)["peer_eof"])
        self.assertEqual(writer.writes, [])

    async def test_write_error_does_not_claim_write_returned_or_include_exception(self):
        listener, pending, _ = self.setup_probe(fail="write")
        with self.assertRaises(OSError):
            await self.exchange(listener, pending)
        progress = self.progress(listener)
        self.assertEqual(progress["outcome"], "io_error")
        self.assertEqual(progress["phase"], "writing")
        self.assertTrue(progress["write_started"])
        self.assertFalse(progress["write_returned"])
        self.assertNotIn("private-error-marker", json.dumps(progress))

    async def test_drain_error_does_not_claim_drain_completed(self):
        listener, pending, _ = self.setup_probe(fail="drain")
        with self.assertRaises(ConnectionResetError):
            await self.exchange(listener, pending)
        progress = self.progress(listener)
        self.assertTrue(progress["write_returned"])
        self.assertFalse(progress["drain_completed"])
        self.assertEqual(progress["phase"], "draining")

    async def test_cancellation_during_drain_is_not_reported_as_timeout(self):
        listener, pending, writer = self.setup_probe(fail="block_drain")
        task = asyncio.create_task(self.exchange(listener, pending))
        await asyncio.wait_for(writer.draining.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.progress(listener)["outcome"], "cancelled")
        self.assertFalse(self.progress(listener)["drain_completed"])

    async def test_framed_response_records_header_and_payload(self):
        response = build_collector_request(
            1, b"\x00\x02" + PN.encode(), devcode=1, collector_addr=1, fcode=2,
        )
        listener, pending, _ = self.setup_probe(response=response)
        self.assertEqual((await self.exchange(listener, pending, "eybond_framed"))[1], PN)
        progress = self.progress(listener)
        self.assertEqual(progress["response_bytes"], len(response))
        self.assertEqual(progress["probe_kind"], "framed_fc2")

    async def test_partial_framed_eof_records_bytes_without_promoting_identity(self):
        listener, pending, _ = self.setup_probe(response=b"\x01\x00\x01", eof=True)
        with self.assertRaises(asyncio.IncompleteReadError):
            await self.exchange(listener, pending, "eybond_framed")
        self.assertEqual(self.progress(listener)["response_bytes"], 3)
        self.assertEqual(self.progress(listener)["outcome"], "peer_closed")
        self.assertEqual(listener.discovered_collector_sessions(), ())

    async def test_initial_known_wire_read_uses_same_diagnostics(self):
        listener, pending, _ = self.setup_probe(response=b"ERROR\r\n")
        result = await self.exchange(listener, pending, initial_read=True)
        self.assertEqual(result, (b"ERROR\r\n", "", ""))
        self.assertEqual(self.progress(listener)["outcome"], "response_received")
        self.assertEqual(self.progress(listener)["response_bytes"], 7)

    async def test_history_is_bounded_copied_and_separate_for_replacement_socket(self):
        listener, pending, _ = self.setup_probe(response=b"AT+DTUPN:" + PN.encode() + b"\r\n")
        for _ in range(6):
            await self.exchange(listener, pending)
        rows = listener.session_inventory_diagnostics()["sessions"]
        self.assertEqual(len(rows[0]["identity_probes"]), 4)
        rows[0]["identity_probes"][-1]["outcome"] = "changed"
        self.assertEqual(self.progress(listener)["outcome"], "identity_received")
        listener._remember_session(
            session_id="replacement", remote_ip=pending.remote_ip, remote_port=40001,
        )
        self.assertNotIn("identity_probes", listener.session_inventory_diagnostics()["sessions"][1])

    async def test_public_probe_rearms_close_watcher_and_keeps_query_result(self):
        listener, pending, writer = self.setup_probe(response=b"ERROR\r\n", eof=True)
        self.assertEqual(await listener.async_identify_pending_session(
            pending.session_id, session_protocol="at_text",
        ), "")
        await asyncio.wait_for(pending.sniff_task, 2)
        self.assertTrue(writer.closed)
        self.assertEqual(self.progress(listener)["response_bytes"], 7)
        self.assertEqual(self.progress(listener)["outcome"], "peer_closed")
        self.assertEqual(listener.discovered_collector_sessions(), ())

    async def test_public_probe_rechecks_reservation_after_pausing_reader(self):
        listener, pending, writer = self.setup_probe()
        # Model a route being reserved while the initial reader task is joined.
        with (
            patch.object(listener, "_reserved_for_transparent_route", side_effect=[False, True]) as reserved,
            patch.object(listener, "_pause_pending_sniff", new_callable=AsyncMock) as pause,
            patch.object(listener, "_resume_pending_watch") as resume,
        ):
            self.assertEqual(await listener.async_identify_pending_session(
                pending.session_id, session_protocol="at_text",
            ), "")
        self.assertEqual(reserved.call_count, 2)
        pause.assert_awaited_once_with(pending)
        resume.assert_called_once_with(pending)
        self.assertEqual(writer.writes, [])
        self.assertNotIn("identity_probes", listener.session_inventory_diagnostics()["sessions"][0])
