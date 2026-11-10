"""Raw PI30 diagnostics on both production socket readers, no hardware IO."""

import asyncio
import unittest

from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
from custom_components.eybond_local.collector.transport.diagnostic_pi30 import (
    DiagnosticPi30Session, MAX_PI30_DIAGNOSTIC_FRAME,
)
from custom_components.eybond_local.link_models import RawSerialLinkRoute
from custom_components.eybond_local.payload.pi30 import build_request
from custom_components.eybond_local.support.diagnostic_export import build_shareable_payload
from custom_components.eybond_local.support.diagnostic_runner import run_scenario
from test_diagnostic_wire import WirePeer, context, stream


SCRIPT = "driver pi30\nwire raw_pi30\noperation_timeout 0.15\nascii QPI\nascii QMOD"


class Pi30Peer(WirePeer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response = None

    def write(self, wire):
        raw = wire.startswith((b"Q", b"P"))
        header = None if raw else decode_header(wire[:8])
        if header is not None and header.fcode == 1:
            return
        payload = wire if raw else wire[8:]
        self.requests.append(("raw_pi30" if raw else "fc4", payload))
        if not self.reply or self.requests[-1][0] != self.mode:
            return
        if self.response is not None:
            response = self.response
        else:
            # QPI matches the saved cloud response, independent of its wrapper.
            response = (bytes.fromhex("28504933309a0b0d") if payload == build_request("QPI")
                        else build_request("(B"))
        if self.corrupt:
            response = response[:-2] + bytes((response[-2] ^ 1,)) + response[-1:]
        if header is not None:
            response = build_collector_request(header.tid, response, devcode=header.devcode,
                                               collector_addr=header.devaddr, fcode=header.fcode)
        if self.before_response:
            self.reader.feed_data(self.before_response)
        if self.truncate:
            response = response[:3]
        for byte in response:
            asyncio.get_running_loop().call_soon(self.reader.feed_data, bytes((byte,)))


def pi30_stream(**kwargs):
    return stream(mode="raw_pi30", peer_factory=Pi30Peer, **kwargs)


class Pi30WireTests(unittest.IsolatedAsyncioTestCase):
    async def test_raw_and_fc4_read_without_route_or_profile_change(self):
        for at in (False, True):
            async with pi30_stream(at=at) as (transport, connection, peer):
                reader_task = connection._reader_task
                result = await run_scenario(SCRIPT, context(transport, confirm=True))
                self.assertTrue(result.success, result.output)
                self.assertEqual([r["response"]["payload"] for r in result.results], ["PI30", "B"])
                self.assertEqual([wire for wire, _ in peer.requests], ["raw_pi30"] * 2)
                self.assertIs(connection._reader_task, reader_task)
                self.assertEqual(connection.collector_info.inverter_forward_mode, "")
                self.assertIsNone(connection._diagnostic_pi30.claim)
                self.assertTrue(connection.connected)
                shared = build_shareable_payload(result)
                self.assertEqual(shared["context"]["requested_wire"], "raw_pi30")
                self.assertEqual(shared["context"]["wire_events"][0]["response_hex"],
                                 "28504933309a0b0d")
                self.assertTrue(shared["context"]["wire_events"][0]["transport_progress"]["response_received"])
                peer.mode = "fc4"
                result = await run_scenario(SCRIPT.replace("raw_pi30", "fc4").replace(
                    "driver pi30", "driver pi30\ndevcode 258\ncollector_addr 255\ndevice_addr 0"),
                    context(transport, confirm=True))
                self.assertTrue(result.success, result.output)
                self.assertEqual([wire for wire, _ in peer.requests][-2:], ["fc4"] * 2)

    async def test_heartbeat_at_and_binary_header_starting_with_parenthesis(self):
        for at in (False, True):
            async with pi30_stream(at=at) as (transport, _, peer):
                peer.before_response = (b"AT+FWVER:synthetic\r\n" +
                    build_collector_request(0x2801, b"", devcode=0x0102,
                                            collector_addr=255, fcode=1) +
                    build_collector_request(0x280d, b"", devcode=0x0102,
                                            collector_addr=255, fcode=1))
                result = await run_scenario(SCRIPT, context(transport, confirm=True))
                self.assertTrue(result.success, result.output)

    async def test_negative_reply_is_evidence_not_a_transport_timeout(self):
        for at in (False, True):
            async with pi30_stream(at=at) as (transport, connection, peer):
                peer.response = build_request("(NAK")
                result = await run_scenario(SCRIPT, context(transport, confirm=True))
                self.assertTrue(result.success, result.output)
                self.assertEqual(result.results[0]["response"]["payload"], "NAK")
                self.assertTrue(connection.connected)

    async def test_failed_or_unbounded_reply_closes_once_and_preserves_fragment(self):
        for at in (False, True):
            for failure in ("silence", "crc", "truncated", "oversize", "binary_body"):
                async with pi30_stream(at=at, reply=failure != "silence") as (transport, connection, peer):
                    peer.corrupt = failure == "crc"
                    peer.truncate = failure == "truncated"
                    if failure == "oversize":
                        peer.response = b"(" + b"x" * MAX_PI30_DIAGNOSTIC_FRAME
                    if failure == "binary_body":
                        # Valid CRC is not enough: PI30 content must be ASCII.
                        from custom_components.eybond_local.payload.pi30 import _encode_crc
                        body = b"(\xff"
                        peer.response = body + _encode_crc(body) + b"\r"
                    result = await run_scenario(SCRIPT.replace(
                        "driver pi30", "driver pi30\nstop_on_error false"), context(transport, confirm=True))
                    self.assertFalse(result.success, result.output)
                    self.assertEqual(len(peer.requests), 1)
                    self.assertTrue(peer.closed)
                    self.assertIsNone(connection._diagnostic_pi30.claim)
                    event = result.context["wire_events"][0]
                    if failure != "silence":
                        self.assertTrue(event.get("received_fragment_hex"), event)
                    if failure == "crc":
                        self.assertIn("crc_mismatch", event["reason"])

    async def test_confirmation_and_codec_mismatch_reject_before_io(self):
        async with pi30_stream() as (transport, _, peer):
            result = await run_scenario(SCRIPT, context(transport))
            self.assertFalse(result.success)
            self.assertIn("confirm_write", result.error)
            for driver in ("pi18", "eybond_g_ascii"):
                result = await run_scenario(SCRIPT.replace("driver pi30", f"driver {driver}"),
                                            context(transport, confirm=True))
                self.assertFalse(result.success)
                self.assertIn("raw_pi30 requires driver pi30", result.error)
            self.assertEqual(peer.requests, [])

    async def test_all_commands_validated_before_the_first_send(self):
        for command in ("Q\tPI", "Q" * MAX_PI30_DIAGNOSTIC_FRAME):
            async with pi30_stream() as (transport, _, peer):
                result = await run_scenario(SCRIPT + "\nascii " + command,
                                            context(transport, confirm=True))
                self.assertFalse(result.success)
                self.assertEqual(peer.requests, [])

    async def test_unsent_cancel_and_busy_reader_leave_socket_intact(self):
        for at in (False, True):
            async with pi30_stream(at=at) as (transport, connection, peer):
                await connection._request_lock.acquire()
                try:
                    pending = asyncio.create_task(run_scenario(SCRIPT, context(transport, confirm=True)))
                    await asyncio.sleep(.01)
                    pending.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await pending
                finally:
                    connection._request_lock.release()
                self.assertFalse(peer.closed)
                self.assertEqual(peer.requests, [])
                connection._auxiliary_session.at_boundary = False
                result = await run_scenario(SCRIPT, context(transport, confirm=True))
                self.assertFalse(result.success)
                self.assertFalse(peer.closed)
                self.assertEqual(peer.requests, [])

    async def test_serial_response_is_redacted_without_altering_local_evidence(self):
        async with pi30_stream() as (transport, _, peer):
            serial = "1234567890123456"
            peer.response = build_request("(" + serial)
            result = await run_scenario(SCRIPT.replace("QPI", "QID"), context(transport, confirm=True))
            self.assertTrue(result.success, result.output)
            import json
            share = json.dumps(build_shareable_payload(result))
            self.assertNotIn(serial, share)
            self.assertNotIn(serial.encode().hex(), share)
            self.assertEqual(result.results[0]["response"]["payload"], serial)

    async def test_cancelled_exchange_does_not_send_remaining_commands(self):
        for at in (False, True):
            async with pi30_stream(at=at, reply=False) as (transport, connection, peer):
                pending = asyncio.create_task(run_scenario(SCRIPT, context(transport, confirm=True)))
                async with asyncio.timeout(1):
                    while not peer.requests:
                        await asyncio.sleep(0)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                self.assertTrue(peer.closed)
                self.assertIsNone(connection._diagnostic_pi30.claim)
                self.assertEqual(len(peer.requests), 1)

    async def test_old_transport_cannot_use_or_close_a_successor(self):
        for at in (False, True):
            async with pi30_stream(at=at, reply=False) as (transport, connection, peer):
                pinned = transport.create_diagnostic_transport("raw_pi30", timeout=.2)
                pending = asyncio.create_task(run_scenario(SCRIPT, context(transport, confirm=True)))
                async with asyncio.timeout(1):
                    while not peer.requests:
                        await asyncio.sleep(0)
                reader = asyncio.StreamReader()
                successor = Pi30Peer(reader, mode="raw_pi30")
                new_run = asyncio.create_task(connection.run(reader, successor))
                try:
                    self.assertFalse((await pending).success)
                    async with asyncio.timeout(1):
                        while connection._writer is not successor:
                            await asyncio.sleep(0)
                    with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                        await pinned.async_send_payload(build_request("QPI"),
                            route=RawSerialLinkRoute(protocol="pi30_ascii"))
                    self.assertFalse(successor.closed)
                    self.assertEqual(successor.requests, [])
                    result = await run_scenario(SCRIPT, context(transport, confirm=True))
                    self.assertTrue(result.success, result.output)
                finally:
                    await connection.disconnect()
                    new_run.cancel()
                    await asyncio.gather(new_run, return_exceptions=True)

    async def test_request_validation_is_strict_before_writing(self):
        session = DiagnosticPi30Session()
        for payload in (b"", b"QPI\r", b"AT+RESET\r\n", build_request("QPI\nPOP00"),
                        build_request("Q" * MAX_PI30_DIAGNOSTIC_FRAME)):
            with self.assertRaises(ValueError):
                session.validate_request(payload)
        session.validate_request(build_request("QPI"))
