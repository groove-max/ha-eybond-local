"""Explicit binary payloads: exact bytes, no inferred codec, no physical I/O."""

import asyncio
from dataclasses import replace
import unittest
from unittest.mock import Mock, patch

from custom_components.eybond_local.collector.protocol import build_collector_request, decode_header
from custom_components.eybond_local.link_models import EybondLinkRoute
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import crc16_modbus
from custom_components.eybond_local.support.diagnostic_commands import parse_scenario, ScenarioError
from custom_components.eybond_local.support.diagnostic_export import build_shareable_payload
from custom_components.eybond_local.support.diagnostic_runner import DiagnosticRuntimeContext, run_scenario
from test_diagnostic_wire import stream


PAYLOAD = bytes.fromhex("43 4D 53 47 06 49 4E 56 2D 48 42 0D")
HEADER = "driver raw\nwire fc4\ndevcode 1\ncollector_addr 255\ndevice_addr 1\n"
SCRIPT = HEADER + "payload_hex " + PAYLOAD.hex(" ")


class Device:
    connected = True

    def __init__(self, reply=b"\x80\xff\x00\x0d", error=None):
        self.reply, self.error = reply, error
        self.factories, self.calls = [], []

    def create_diagnostic_transport(self, wire, *, timeout):
        self.factories.append((wire, timeout))
        return self

    def select_payload_route(self, *args, **kwargs):
        raise AssertionError("Opaque diagnostics must not negotiate a route")

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        self.calls.append((payload, route, request_timeout))
        if self.error:
            raise self.error
        return self.reply


def context(device, **kwargs):
    return DiagnosticRuntimeContext(transport=device, confirm_write=True, **kwargs)


class PayloadParserTests(unittest.TestCase):
    def test_exact_octets_spaces_tabs_and_contiguous_hex(self):
        for text in (PAYLOAD.hex(), PAYLOAD.hex(" ").upper(), PAYLOAD.hex("\t")):
            with self.subTest(text=text):
                command = parse_scenario("payload_hex " + text).commands[0]
                self.assertEqual(command.payload, PAYLOAD)
                self.assertEqual(command.kind, "payload")
        self.assertEqual(len(parse_scenario("payload_hex " + "aa" * 512).commands[0].payload), 512)

    def test_invalid_hex_and_oversized_payload_are_rejected(self):
        for text in ("", "f", "0x43", "43 4", "4 3", "43,4d", "zz", "43\\x06", "43\x06", "aa" * 513):
            with self.subTest(text=text), self.assertRaises(ScenarioError):
                parse_scenario("payload_hex " + text)


class PayloadRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_bytes_route_and_export_no_crc_or_terminator_added(self):
        device = Device()
        default = Mock(side_effect=AssertionError("must not borrow a driver target"))
        result = await run_scenario(SCRIPT, context(
            device, active_driver_key="pi30", active_probe_target=ProbeTarget(2462, 1, 2),
            driver_default_probe_target=default,
        ))
        self.assertTrue(result.success, result.output)
        default.assert_not_called()
        self.assertEqual(device.factories, [("fc4", 10)])
        self.assertEqual(device.calls, [(PAYLOAD, EybondLinkRoute(devcode=1, collector_addr=255), 10)])
        response = result.results[0]["response"]
        self.assertEqual(response, {"raw_hex": device.reply.hex(), "response_size": 4,
                                    "reply_status": "payload_received", "decode_status": "not_attempted"})
        exported = build_shareable_payload(result)
        self.assertEqual(exported["results"][0]["response"], response)
        self.assertEqual(exported["context"]["wire_events"][0]["request_hex"], PAYLOAD.hex())
        self.assertIn("decode_status: not_attempted", exported["output"])

    async def test_confirmation_required_before_transport_factory(self):
        device = Device()
        result = await run_scenario(SCRIPT, replace(context(device), confirm_write=False))
        self.assertFalse(result.success)
        self.assertIn("confirm_write", result.output)
        self.assertEqual(device.factories, [])
        self.assertEqual(device.calls, [])

    async def test_explicit_complete_route_required_and_entire_script_validated(self):
        scripts = [SCRIPT.replace(line, "") for line in (
            "driver raw\n", "wire fc4\n", "devcode 1\n", "collector_addr 255\n", "device_addr 1\n",
        )]
        scripts += [SCRIPT.replace("wire fc4", "wire " + wire) for wire in ("auto", "raw_modbus")]
        scripts += [SCRIPT.replace("driver raw", "driver " + driver) for driver in ("pi30", "modbus")]
        scripts += [SCRIPT + "\n" + command for command in ("read 0", "ascii Q1", "payload_hex 4", "write 1 1")]
        scripts += [SCRIPT.replace("devcode 1", "devcode 65536"),
                    SCRIPT.replace("collector_addr 255", "collector_addr 256")]
        for script in scripts:
            with self.subTest(script=script):
                device = Device()
                result = await run_scenario(script, context(
                    device, active_driver_key="raw", active_probe_target=ProbeTarget(1, 255, 1),
                ))
                self.assertFalse(result.success, result.output)
                self.assertEqual(device.factories, [])
                self.assertEqual(device.calls, [])

    async def test_empty_reply_is_not_a_decoded_acknowledgement(self):
        device = Device(reply=b"")
        result = await run_scenario(SCRIPT, context(device))
        # Success describes transport completion, never decoded inverter data.
        self.assertTrue(result.success)
        self.assertEqual(result.results[0]["response"], {
            "raw_hex": "", "response_size": 0, "reply_status": "empty_reply", "decode_status": "not_attempted",
        })

    async def test_error_or_timeout_is_not_retried_and_stops_next_command(self):
        for error in (ConnectionError("disconnected"), asyncio.TimeoutError()):
            device = Device(error=error)
            result = await run_scenario(SCRIPT + "\npayload_hex 01", context(device))
            self.assertFalse(result.success)
            self.assertEqual(len(device.calls), 1)
            self.assertEqual(result.context["wire_events"][0]["status"], "error")

    async def test_cancellation_propagates(self):
        device = Device(error=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await run_scenario(SCRIPT, context(device))
        self.assertEqual(len(device.calls), 1)

    async def test_known_identity_read_is_redacted_in_step_text_and_wire_only_in_share(self):
        for address, count, sensitive in ((185, 2, True), (197, 1, True), (185, 1, False), (198, 1, False)):
            with self.subTest(address=address, count=count):
                body = b"\x01\x03" + address.to_bytes(2, "big") + count.to_bytes(2, "big")
                request = body + crc16_modbus(body).to_bytes(2, "little")
                device = Device(reply=b"\xab\xcd\xef\xaa")
                result = await run_scenario(HEADER + "payload_hex " + request.hex(), context(device))
                self.assertTrue(result.success)
                exported = build_shareable_payload(result)
                expected = "REDACTED_IDENTITY_RANGE" if sensitive else device.reply.hex()
                self.assertEqual(exported["results"][0]["response"]["raw_hex"], expected)
                self.assertEqual(exported["context"]["wire_events"][0]["response_hex"], expected)
                self.assertIn("raw_hex: " + expected, exported["output"])
                self.assertEqual(result.results[0]["response"]["raw_hex"], device.reply.hex())
                self.assertEqual(result.context["wire_events"][0]["response_hex"], device.reply.hex())
                self.assertIn(device.reply.hex(), result.output)

    async def test_actual_framing_on_both_session_owners_with_fragmented_binary_response(self):
        # Synthetic reply tests byte fidelity only, NOT an inferred 099E map.
        reply = bytes((i * 7) % 256 for i in range(72))
        for at in (False, True):
            async with stream(at=at, mode="fc4") as (transport, connection, peer):
                requests = []

                def respond(wire):
                    header = decode_header(wire[:8])
                    if header.fcode == 1:
                        return
                    requests.append((header, wire[8:]))
                    response = build_collector_request(header.tid, reply, devcode=header.devcode,
                                                        collector_addr=header.devaddr, fcode=header.fcode)
                    for byte in response:
                        asyncio.get_running_loop().call_soon(peer.reader.feed_data, bytes((byte,)))

                with patch.object(peer, "write", side_effect=respond):
                    result = await run_scenario(SCRIPT, context(transport))
                self.assertTrue(result.success, result.output)
                self.assertEqual(len(requests), 1)
                header, payload = requests[0]
                self.assertEqual((header.fcode, header.devcode, header.devaddr), (4, 1, 255))
                self.assertEqual(payload, PAYLOAD)
                self.assertEqual(result.results[0]["response"]["raw_hex"], reply.hex())
                self.assertTrue(result.context["wire_events"][0]["transport_progress"]["response_received"])
                self.assertEqual(connection.collector_info.inverter_forward_mode, "")
                if at:
                    self.assertFalse(connection.mixed_frame_observed)
                self.assertTrue(connection.connected)
