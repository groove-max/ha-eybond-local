"""General diagnostic wire overrides; simulated streams, never real devices."""

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
from custom_components.eybond_local.collector.transport import (
    SharedCollectorAtTransport, SharedEybondTransport, _CollectorAtConnection, _CollectorConnection,
)
from custom_components.eybond_local.payload.modbus import crc16_modbus, decode_read_request, decode_write_request
from custom_components.eybond_local.collector.transport.binary_framing import runtime_eybond_header_error
from custom_components.eybond_local.support.diagnostic_export import build_shareable_payload
from custom_components.eybond_local.support.diagnostic_commands import parse_scenario, ScenarioError
from custom_components.eybond_local.support.diagnostic_runner import DiagnosticRuntimeContext, run_scenario


FIXTURE = json.loads((Path(__file__).parent / "fixtures/marsrock_read_observation.json").read_text())


def crc(body):
    return body + crc16_modbus(body).to_bytes(2, "little")


class WirePeer:
    def __init__(self, reader, *, mode="raw_modbus", reply=True):
        self.reader, self.mode, self.reply = reader, mode, reply
        self.closed = False
        self.requests = []
        self.words = {354: 1}
        self.corrupt = False
        self.exception = None
        self.before_response = b""
        self.truncate = False
        self.mismatch_echo = False

    def write(self, wire):
        raw_read, raw_write = decode_read_request(wire), decode_write_request(wire)
        header = None if raw_read or raw_write else decode_header(wire[:8])
        if header is not None and header.fcode == 1:
            return
        payload = wire if header is None else wire[8:]
        read, write = decode_read_request(payload), decode_write_request(payload)
        if header is not None and header.fcode == 2:
            response = b"\x00\x229600\r\n"
        else:
            assert read or write, payload.hex()
            self.requests.append(("raw_modbus" if header is None else "fc4", payload))
            if not self.reply or self.requests[-1][0] != self.mode:
                return
            if self.exception is not None:
                response = crc(bytes((payload[0], payload[1] | 0x80, self.exception)))
            elif read:
                if (read.address, read.count) == (0, 20):
                    row = next(r for r in FIXTURE["records"] if r["request_hex"] == payload.hex())
                    response = bytes.fromhex(row["response_hex"])
                else:
                    response = crc(bytes((payload[0], payload[1], read.count * 2)) + b"".join(
                        self.words.get(i, 1).to_bytes(2, "big") for i in range(read.address, read.address + read.count)))
            else:
                self.words.update(enumerate(write.values, write.address))
                response = crc(payload[:6])
                if self.mismatch_echo:
                    response = crc(response[:3] + bytes([response[3] ^ 1]) + response[4:6])
        if self.corrupt:
            response = response[:-1] + bytes((response[-1] ^ 1,))
        if header is not None:
            response = build_collector_request(header.tid, response, devcode=header.devcode,
                                               collector_addr=header.devaddr, fcode=header.fcode)
        if self.before_response:
            self.reader.feed_data(self.before_response)
        if self.truncate:
            self.reader.feed_data(response[:4])
            return
        # Fragment EVERY byte. No assumption about TCP read boundaries.
        for byte in response:
            asyncio.get_running_loop().call_soon(self.reader.feed_data, bytes((byte,)))

    async def drain(self): pass
    def is_closing(self): return self.closed
    def close(self): self.closed = True
    async def wait_closed(self): self.closed = True
    def get_extra_info(self, name, default=None):
        return ("192.0.2.43", 40000) if name == "peername" else default


@asynccontextmanager
async def stream(*, at=False, mode="raw_modbus", reply=True):
    reader = asyncio.StreamReader()
    peer = WirePeer(reader, mode=mode, reply=reply)
    if at:
        connection = _CollectorAtConnection(remote_ip_hint="192.0.2.43", write_timeout=.1,
                                             raw_passthrough_bootstrap="uart_write_same_value")
        transport = SharedCollectorAtTransport(host="127.0.0.1", port=18899,
                                               collector_ip="192.0.2.43", request_timeout=.1)
        transport.set_negotiated_wire("at_text")
        method = "_at_connection"
    else:
        connection = _CollectorConnection(remote_ip_hint="192.0.2.43", heartbeat_interval=3600, write_timeout=.1)
        transport = SharedEybondTransport(host="127.0.0.1", port=8899, collector_ip="192.0.2.43",
                                          request_timeout=.1, heartbeat_interval=3600)
        method = "_connection"
    task = asyncio.create_task(connection.run(reader, peer))
    await connection.wait_until_connected(1)
    try:
        with patch.object(transport, method, return_value=connection):
            yield transport, connection, peer
    finally:
        await connection.disconnect()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def scenario(commands="read 0 20", *, wire="raw_modbus"):
    return (f"driver modbus\nwire {wire}\ndevcode 1\ncollector_addr 255\n"
            f"device_addr 1\noperation_timeout 0.15\n{commands}")


def context(transport, *, confirm=False):
    return DiagnosticRuntimeContext(transport=transport, configured_driver_hint="marsrock_grid_tie",
                                    is_connected=lambda: transport.connected, confirm_write=confirm)


class DiagnosticWireTests(unittest.IsolatedAsyncioTestCase):
    async def test_marsrock_read_on_framed_and_at_without_model_or_bootstrap_changes(self):
        for at in (False, True):
            async with stream(at=at) as (transport, connection, peer):
                result = await run_scenario(scenario(), context(transport))
                self.assertTrue(result.success, result.output)
                self.assertEqual(len(result.results[0]["response"]["decimal"]), 20)
                self.assertEqual([m for m, _ in peer.requests], ["raw_modbus"])
                self.assertEqual(result.context["requested_wire"], "raw_modbus")
                self.assertEqual(result.context["wire_events"][0]["request_hex"], "01030000001445c5")
                self.assertEqual(connection.collector_info.inverter_forward_mode, "")
                if at:
                    self.assertFalse(connection.mixed_frame_observed)
                self.assertTrue(connection.connected)

    async def test_explicit_fc4_on_both_owners_does_not_learn_wire(self):
        for at in (False, True):
            async with stream(at=at, mode="fc4") as (transport, connection, peer):
                result = await run_scenario(scenario(wire="fc4"), context(transport))
                self.assertTrue(result.success, result.output)
                self.assertEqual([m for m, _ in peer.requests], ["fc4"])
                self.assertEqual(connection.collector_info.inverter_forward_mode, "")
                if at:
                    self.assertFalse(connection.mixed_frame_observed)

    async def test_timeout_trace_distinguishes_unsent_request_from_silent_peer(self):
        for at in (False, True):
            for wire in ("raw_modbus", "fc4"):
                for lock_name in ("_request_lock", "_write_lock", None):
                    with self.subTest(at=at, wire=wire, lock=lock_name):
                        async with stream(at=at, mode=wire, reply=False) as (transport, connection, peer):
                            lock = getattr(connection, lock_name) if lock_name else None
                            if lock is not None:
                                await lock.acquire()
                            try:
                                result = await run_scenario(scenario(wire=wire), context(transport))
                            finally:
                                if lock is not None:
                                    lock.release()
                            self.assertFalse(result.success)
                            event = result.context["wire_events"][0]
                            progress = event["transport_progress"]
                            self.assertEqual(progress["phase"], {
                                "_request_lock": "waiting_request_lock",
                                "_write_lock": "waiting_write_lock",
                                None: "waiting_response",
                            }[lock_name])
                            self.assertEqual(progress["write_returned"], lock is None)
                            self.assertEqual(progress["drain_completed"], lock is None)
                            self.assertFalse(progress["response_received"])
                            self.assertEqual(len(peer.requests), int(lock is None))
                            self.assertEqual("tid" in progress, wire == "fc4" and lock_name != "_request_lock")
                            # An unsent cancellation must not retire a healthy socket.
                            self.assertEqual(peer.closed, wire == "raw_modbus" and lock is None)

    async def test_success_trace_is_per_exchange_and_shareable(self):
        for at in (False, True):
            for wire in ("raw_modbus", "fc4"):
                async with stream(at=at, mode=wire) as (transport, _, peer):
                    result = await run_scenario(scenario("read 354\nread 354", wire=wire), context(transport))
                    self.assertTrue(result.success, result.output)
                    events = result.context["wire_events"]
                    for event in events:
                        progress = event["transport_progress"]
                        self.assertEqual(progress["phase"], "response_received")
                        self.assertTrue(progress["write_returned"])
                        self.assertTrue(progress["drain_completed"])
                        self.assertTrue(progress["response_received"])
                    if wire == "fc4":
                        self.assertNotEqual(events[0]["transport_progress"]["tid"], events[1]["transport_progress"]["tid"])
                    share = build_shareable_payload(result)
                    self.assertEqual(share["context"]["wire_events"], events)
                    self.assertEqual(len(peer.requests), 2)

    async def test_trace_does_not_claim_a_drained_write_after_write_failure(self):
        for at in (False, True):
            for wire in ("raw_modbus", "fc4"):
                for failure in ("write", "drain"):
                    with self.subTest(at=at, wire=wire, failure=failure):
                        async with stream(at=at, mode=wire, reply=False) as (transport, _, peer):
                            # Heartbeat is outside this run; finish its initial drain first.
                            await asyncio.sleep(0)
                            await asyncio.sleep(0)
                            with patch.object(peer, failure, side_effect=OSError("synthetic stream failure")):
                                result = await run_scenario(scenario(wire=wire), context(transport))
                            self.assertFalse(result.success)
                            progress = result.context["wire_events"][0]["transport_progress"]
                            self.assertTrue(progress["write_started"])
                            self.assertEqual(progress["write_returned"], failure == "drain")
                            self.assertFalse(progress["drain_completed"])
                            self.assertFalse(progress["response_received"])

    async def test_unsent_timeout_does_not_export_a_previous_raw_reply(self):
        for at in (False, True):
            async with stream(at=at) as (transport, connection, peer):
                first = await run_scenario(scenario("read 354"), context(transport))
                self.assertTrue(first.success)
                self.assertTrue(connection._diagnostic_modbus.received)
                async with connection._request_lock:
                    blocked = await run_scenario(scenario("read 355"), context(transport))
                self.assertFalse(blocked.success)
                event = blocked.context["wire_events"][0]
                self.assertNotIn("received_fragment_hex", event)
                self.assertEqual(event["transport_progress"]["phase"], "waiting_request_lock")
                self.assertEqual(len(peer.requests), 1)
                self.assertFalse(peer.closed)

    async def test_empty_fc4_reply_is_transport_evidence_not_valid_inverter_data(self):
        for at in (False, True):
            async with stream(at=at, mode="fc4", reply=False) as (transport, connection, peer):
                original_write = peer.write

                def empty_reply(wire):
                    original_write(wire)
                    header = decode_header(wire[:8])
                    if header.fcode == 4:
                        peer.reader.feed_data(build_collector_request(
                            header.tid, b"", devcode=header.devcode,
                            collector_addr=header.devaddr, fcode=4,
                        ))

                with patch.object(peer, "write", side_effect=empty_reply):
                    result = await run_scenario(scenario(wire="fc4"), context(transport))
                self.assertFalse(result.success)
                self.assertEqual(result.results[0]["error"], "response_too_short")
                event = result.context["wire_events"][0]
                self.assertEqual(event["response_size"], 0)
                self.assertTrue(event["transport_progress"]["response_received"])
                self.assertEqual(len(peer.requests), 1)
                self.assertTrue(connection.connected)

    async def test_old_empty_fc4_reply_does_not_complete_a_current_request(self):
        for at in (False, True):
            async with stream(at=at, mode="fc4", reply=False) as (transport, connection, peer):
                original_write = peer.write

                def stale_reply(wire):
                    original_write(wire)
                    header = decode_header(wire[:8])
                    if header.fcode == 4:
                        peer.reader.feed_data(build_collector_request(
                            (header.tid - 1) & 0xFFFF, b"", devcode=header.devcode,
                            collector_addr=header.devaddr, fcode=4,
                        ))

                with patch.object(peer, "write", side_effect=stale_reply):
                    result = await run_scenario(scenario(wire="fc4"), context(transport))
                self.assertFalse(result.success)
                progress = result.context["wire_events"][0]["transport_progress"]
                self.assertEqual(progress["phase"], "waiting_response")
                self.assertTrue(progress["drain_completed"])
                self.assertFalse(progress["response_received"])
                self.assertTrue(connection.connected)
                peer.reply = True
                recovered = await run_scenario(scenario(wire="fc4"), context(transport))
                self.assertTrue(recovered.success, recovered.output)
                self.assertTrue(recovered.context["wire_events"][0]["transport_progress"]["response_received"])

    async def test_at_facade_pins_its_negotiated_framed_owner(self):
        async with stream(mode="fc4") as (_, connection, peer):
            facade = SharedCollectorAtTransport(host="127.0.0.1", port=8899,
                                                collector_ip="192.0.2.43", request_timeout=.1)
            facade.set_negotiated_wire("eybond_framed")
            with patch.object(facade, "_framed_connection", return_value=connection):
                result = await run_scenario(scenario(wire="fc4"), context(facade))
            self.assertTrue(result.success, result.output)
            self.assertEqual(len(peer.requests), 1)

    async def test_write_commands_require_confirmation_before_any_read(self):
        for command in ("write 354 1", "write_single 354 1", "write_bit 354 0 0"):
            async with stream() as (transport, _, peer):
                result = await run_scenario(scenario("read 354\n" + command), context(transport))
                self.assertFalse(result.success)
                self.assertIn("confirm_write", result.error)
                self.assertEqual(peer.requests, [])

    async def test_general_read_input_and_fc06_fc16_bit_writes(self):
        for wire in ("raw_modbus", "fc4"):
            async with stream(mode=wire) as (transport, _, peer):
                result = await run_scenario(scenario(
                    "read_input 354\nwrite_single 354 1\nwrite 354 1\nwrite_bit 354 0 0\nread 354",
                    wire=wire), context(transport, confirm=True))
                self.assertTrue(result.success, result.output)
                self.assertEqual([p[1] for _, p in peer.requests], [4, 6, 16, 3, 16, 3])
                self.assertEqual(result.results[-1]["response"]["decimal"], [0])

    async def test_ordinary_management_frames_and_at_lines_can_interleave(self):
        for at in (False, True):
            async with stream(at=at) as (transport, connection, peer):
                peer.before_response = (b"AT+FWVER:synthetic\r\n" +
                    build_collector_request(77, b"", devcode=0, collector_addr=1, fcode=1))
                result = await run_scenario(scenario(), context(transport))
                self.assertTrue(result.success, result.output)
                self.assertTrue(connection.connected)

    async def test_heartbeat_can_share_raw_slave_and_function_prefix(self):
        async with stream() as (transport, connection, peer):
            peer.before_response = build_collector_request(
                0x0103, b"", devcode=0, collector_addr=1, fcode=1,
            )
            result = await run_scenario(scenario(), context(transport))
            self.assertTrue(result.success, result.output)
            self.assertTrue(connection.connected)

    async def test_fc4_crc_error_is_not_automatically_retried(self):
        async with stream(mode="fc4") as (transport, connection, peer):
            peer.corrupt = True
            result = await run_scenario(scenario(wire="fc4"), context(transport))
            self.assertFalse(result.success)
            self.assertEqual(len(peer.requests), 1)
            self.assertEqual(result.results[0]["error"], "crc_mismatch")
            self.assertTrue(connection.connected)

    async def test_actual_ambiguous_header_does_not_certify_a_write(self):
        # A synthetic FC06 echo whose CRC is also a valid EyeBond function.
        value = next(v for v in range(2, 4099) if not runtime_eybond_header_error(
            decode_header(crc(b"\x01\x06\x01\x62" + v.to_bytes(2, "big")))))
        async with stream() as (transport, _, peer):
            result = await run_scenario(scenario(f"write_single 354 {value}"), context(transport, confirm=True))
            self.assertFalse(result.success)
            self.assertTrue(peer.closed)
            self.assertEqual(result.context["wire_events"][0]["reason"], "diagnostic_raw_eybond_ambiguous")

    async def test_wire_override_ends_without_changing_normal_routing(self):
        for at in (False, True):
            async with stream(at=at) as (transport, connection, peer):
                first = await run_scenario(scenario(), context(transport))
                self.assertTrue(first.success, first.output)
                self.assertIsNone(connection._diagnostic_modbus.claim)
                # A separate explicit FC4 run on the same socket works normally.
                peer.mode = "fc4"
                second = await run_scenario(scenario(wire="fc4"), context(transport))
                self.assertTrue(second.success, second.output)
                self.assertEqual([m for m, _ in peer.requests], ["raw_modbus", "fc4"])
                self.assertEqual(connection.collector_info.inverter_forward_mode, "")

    async def test_raw_does_not_need_envelope_addresses_or_a_model_map(self):
        async with stream() as (transport, _, peer):
            result = await run_scenario(
                "driver modbus\nwire raw_modbus\ndevice_addr 1\nread 0 20", context(transport),
            )
            self.assertTrue(result.success, result.output)
            self.assertEqual(len(peer.requests), 1)

    async def test_unsupported_override_fails_without_sending(self):
        class Unsupported:
            connected = True

            async def async_send_payload(self, *args, **kwargs):
                raise AssertionError("must not send")

        result = await run_scenario(scenario(), context(Unsupported()))
        self.assertFalse(result.success)
        self.assertIn("diagnostic_wire_override_not_supported", result.error)

    async def test_raw_failure_cannot_continue_on_a_late_reply(self):
        async with stream(reply=False) as (transport, _, peer):
            result = await run_scenario(scenario("write 354 1\nread 354").replace(
                "driver modbus", "driver modbus\nstop_on_error false"), context(transport, confirm=True))
            self.assertFalse(result.success)
            self.assertEqual(len(result.results), 2)
            self.assertEqual(len(peer.requests), 1)
            self.assertTrue(peer.closed)

    async def test_replaced_run_survives_old_raw_request_cleanup(self):
        for at in (False, True):
            async with stream(at=at, reply=False) as (transport, connection, peer):
                pending = asyncio.create_task(run_scenario(scenario(), context(transport)))
                async with asyncio.timeout(1):
                    while not peer.requests:
                        await asyncio.sleep(0)
                reader = asyncio.StreamReader()
                successor = WirePeer(reader)
                new_run = asyncio.create_task(connection.run(reader, successor))
                try:
                    result = await pending
                    self.assertFalse(result.success)
                    async with asyncio.timeout(1):
                        while connection._writer is not successor:
                            await asyncio.sleep(0)
                    self.assertFalse(successor.closed)
                    self.assertEqual(successor.requests, [])
                    result = await run_scenario(scenario(), context(transport))
                    self.assertTrue(result.success, result.output)
                    self.assertFalse(successor.closed)
                finally:
                    await connection.disconnect()
                    await asyncio.gather(new_run, return_exceptions=True)

    async def test_replacement_while_waiting_write_lock_never_sends(self):
        async with stream() as (transport, connection, peer):
            await connection._write_lock.acquire()
            pending = asyncio.create_task(run_scenario(scenario(), context(transport)))
            try:
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                connection._run_epoch += 1
            finally:
                connection._write_lock.release()
            result = await pending
            self.assertFalse(result.success)
            self.assertEqual(peer.requests, [])
            self.assertFalse(peer.closed)

    async def test_trace_is_bounded_and_reports_command_lines(self):
        async with stream(mode="fc4") as (transport, _, peer):
            result = await run_scenario(scenario("\n".join(["read 354"] * 130), wire="fc4"), context(transport))
            self.assertTrue(result.success, result.output)
            self.assertEqual(len(peer.requests), 130)
            self.assertEqual(len(result.context["wire_events"]), 128)
            self.assertEqual(result.context["wire_events_dropped"], 2)
            self.assertEqual(result.context["wire_events"][0]["line"], 7)

    async def test_shareable_trace_does_not_reintroduce_known_identity_words(self):
        async with stream() as (transport, _, peer):
            result = await run_scenario(scenario("read 186 2"), context(transport))
            self.assertTrue(result.success, result.output)
            share = build_shareable_payload(result)
            self.assertEqual(share["context"]["wire_events"][0]["response_hex"], "REDACTED_IDENTITY_RANGE")
            self.assertNotEqual(result.context["wire_events"][0]["response_hex"], "REDACTED_IDENTITY_RANGE")
            self.assertEqual(share["results"][0]["response"]["decimal"], [None, None])

    async def test_failed_raw_response_fences_socket_and_does_not_retry(self):
        for failure in ("silence", "crc", "truncated", "echo"):
            async with stream(reply=failure != "silence") as (transport, connection, peer):
                peer.corrupt = failure == "crc"
                peer.truncate = failure == "truncated"
                peer.mismatch_echo = failure == "echo"
                result = await run_scenario(scenario("write_single 354 1"), context(transport, confirm=True))
                self.assertFalse(result.success, result.output)
                self.assertEqual(len(peer.requests), 1)
                self.assertTrue(peer.closed)
                self.assertIsNone(connection._diagnostic_modbus.claim)

    async def test_exception_reply_is_rejected_not_a_timeout(self):
        async with stream() as (transport, connection, peer):
            peer.exception = 2
            result = await run_scenario(scenario(), context(transport))
            self.assertFalse(result.success)
            self.assertEqual(result.results[0]["error"], "exception_code:2")
            self.assertTrue(connection.connected)
            self.assertEqual(len(peer.requests), 1)

    async def test_cancel_raw_write_closes_only_its_owned_stream(self):
        async with stream(reply=False) as (transport, connection, peer):
            task = asyncio.create_task(run_scenario(scenario("write_single 354 1"), context(transport, confirm=True)))
            while not peer.requests:
                await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(peer.closed)
            self.assertIsNone(connection._diagnostic_modbus.claim)

    async def test_pinned_override_does_not_follow_replacement(self):
        async with stream() as (transport, connection, peer):
            pinned = transport.create_diagnostic_transport("raw_modbus", timeout=.1)
            connection._run_epoch += 1
            with self.assertRaisesRegex(ConnectionError, "collector_session_changed"):
                pinned.select_payload_route(None, payload_family="modbus_rtu")
            self.assertEqual(peer.requests, [])
            self.assertFalse(peer.closed)

    async def test_invalid_scenario_never_touches_wire(self):
        async with stream() as (transport, _, peer):
            for text in (scenario().replace("driver modbus", "driver pi30"),
                         scenario().replace("device_addr 1", "device_addr 0"),
                         scenario().replace("0.15", "121"), scenario("read 0 126"),
                         scenario().replace("0.15", "nan")):
                result = await run_scenario(text, context(transport))
                self.assertFalse(result.success, text)
                self.assertEqual(peer.requests, [])

    def test_parser_extensions_and_nonfinite_timeout(self):
        parsed = parse_scenario("driver modbus\nwire raw_modbus\nread_input 1 2\nwrite_single 2 1")
        self.assertEqual(parsed.directives.wire, "raw_modbus")
        self.assertEqual(parsed.commands[0].function, 4)
        self.assertTrue(parsed.commands[1].single)
        for text in ("wire guessed\nread 1", "operation_timeout inf\nread 1", "write_single 1 2 3"):
            with self.assertRaises(ScenarioError):
                parse_scenario(text)
