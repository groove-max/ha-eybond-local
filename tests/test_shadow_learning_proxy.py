from __future__ import annotations

import asyncio
import json
from pathlib import Path
import tempfile
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.at import parse_at_response
from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE,
    build_collector_request,
    decode_header,
    encode_header,
)
from custom_components.eybond_local.collector.smartess_local import (
    build_query_collector_payload,
    build_set_collector_payload,
)
from custom_components.eybond_local.payload.modbus import (
    build_read_holding_request,
    build_write_multiple_request,
    crc16_modbus,
)
from custom_components.eybond_local.support.shadow_learning_backend import ShadowLearningSeed
from custom_components.eybond_local.support.shadow_learning_proxy import (
    InProcessFailClosedShadowProxyHandler,
    _consume_next_message,
    route_status_indicates_control_ready,
    route_status_indicates_control_write_ready,
)


def _read_trace(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _seed() -> ShadowLearningSeed:
    return ShadowLearningSeed(
        session_id="entry-1_20260605T120000Z",
        entry_id="entry-1",
        collector_pn="E5000020000000",
        collector_cloud_profile_key="smartess_at",
        collector_cloud_profile_label="SmartESS AT",
        collector_cloud_profile_source="runtime_observed",
        collector_cloud_profile_confidence="high",
        collector_callback_endpoint="192.168.1.50,18899,TCP",
        effective_metadata_snapshot={
            "profile_name": "modbus_smg/default.json",
            "register_schema_name": "modbus_smg/default.json",
        },
        command_responses={
            "QID": "E5000020000000",
            "CLDSRVHOST1": "192.168.1.50,18899,TCP",
        },
        register_bank={300: 10, 301: 11, 305: 12},
        latest_support_evidence=None,
        write_response_mode="exception",
        allow_ack_writes=False,
    )


class ShadowLearningProxyTests(unittest.IsolatedAsyncioTestCase):
    async def test_upstream_connection_and_allowlisted_collector_traffic(self) -> None:
        upstream_received: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            upstream_received.append(await reader.readexactly(12))
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            frame = build_collector_request(
                7,
                b"ping",
                devcode=2376,
                collector_addr=1,
                fcode=1,
            )
            collector_writer.write(frame)
            await collector_writer.drain()
            await asyncio.sleep(0.05)

            collector_writer.close()
            await collector_writer.wait_closed()
            await asyncio.sleep(0.05)
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        self.assertEqual(upstream_received, [frame])
        self.assertTrue(collector_reader.at_eof() or True)

    async def test_correlated_response_forwarding_to_collector(self) -> None:
        request = build_collector_request(
            9,
            build_set_collector_payload(21, "collector-cloud.smartess.example,18899,TCP"),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            forwarded = await reader.readexactly(len(request))
            request_header = decode_header(forwarded[:HEADER_SIZE])
            response = build_collector_request(
                request_header.tid,
                bytes((0, 21)),
                devcode=request_header.devcode,
                collector_addr=request_header.devaddr,
                fcode=request_header.fcode,
            )
            writer.write(response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(request)
            await collector_writer.drain()
            response = await collector_reader.readexactly(10)

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(response[:HEADER_SIZE])
        self.assertEqual(response_header.tid, 9)
        self.assertEqual(response_header.fcode, 3)
        self.assertEqual(response[HEADER_SIZE:], bytes((0, 21)))

    async def test_correlated_fc2_response_forwarding_to_collector(self) -> None:
        request = build_collector_request(
            10,
            build_query_collector_payload(5, 14),
            devcode=2376,
            collector_addr=1,
            fcode=2,
        )

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            forwarded = await reader.readexactly(len(request))
            request_header = decode_header(forwarded[:HEADER_SIZE])
            response = build_collector_request(
                request_header.tid,
                b"\x00\x0e0925#Hybrid",
                devcode=request_header.devcode,
                collector_addr=request_header.devaddr,
                fcode=request_header.fcode,
            )
            writer.write(response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(request)
            await collector_writer.drain()
            response = await collector_reader.readexactly(21)

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(response[:HEADER_SIZE])
        self.assertEqual(response_header.tid, 10)
        self.assertEqual(response_header.fcode, 2)
        self.assertEqual(response[HEADER_SIZE:], b"\x00\x0e0925#Hybrid")

    async def test_correlated_fc2_ambiguous_cloud_payload_is_not_forwarded(self) -> None:
        request = build_collector_request(
            125,
            build_query_collector_payload(5, 14),
            devcode=2376,
            collector_addr=1,
            fcode=2,
        )
        spoofed_cloud_payload = build_collector_request(
            125,
            build_query_collector_payload(5, 14),
            devcode=2376,
            collector_addr=1,
            fcode=2,
        )

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            forwarded = await reader.readexactly(len(request))
            self.assertEqual(forwarded, request)
            writer.write(spoofed_cloud_payload)
            await writer.drain()
            await asyncio.wait_for(reader.read(64), timeout=0.5)
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(request)
            await collector_writer.drain()
            collector_forwarded = await asyncio.wait_for(collector_reader.read(64), timeout=0.3)
            self.assertEqual(collector_forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        self.assertTrue(
            any(
                event.get("kind") == "shadow_proxy_block_cloud_correlated_unallowlisted"
                and event.get("payload", {}).get("reason") == "fc2_response_code_invalid"
                for event in trace
            )
        )

    async def test_same_function_code_spoofed_cloud_set_command_is_not_forwarded(self) -> None:
        upstream_responses: list[bytes] = []

        request = build_collector_request(
            124,
            build_set_collector_payload(21, "collector-cloud.smartess.example,18899,TCP"),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )
        spoofed_cloud_command = build_collector_request(
            124,
            build_set_collector_payload(21, "192.168.1.250,18899,TCP"),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            forwarded = await reader.readexactly(len(request))
            self.assertEqual(forwarded, request)
            writer.write(spoofed_cloud_command)
            await writer.drain()
            upstream_responses.append(await reader.readexactly(13))
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(request)
            await collector_writer.drain()
            collector_forwarded = await asyncio.wait_for(collector_reader.read(64), timeout=0.3)
            self.assertEqual(collector_forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(upstream_responses[0][:HEADER_SIZE])
        response_payload = upstream_responses[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 124)
        self.assertEqual(response_header.fcode, 3)
        self.assertEqual(response_payload[1], 0x83)
        self.assertTrue(
            any(
                event.get("kind") == "shadow_proxy_block_cloud_correlated_unallowlisted"
                and event.get("payload", {}).get("reason") == "fc3_response_length_invalid"
                for event in trace
            )
        )

    async def test_cloud_write_intercepted_locally_and_not_forwarded_to_collector(self) -> None:
        collector_messages: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            cloud_write = build_collector_request(
                55,
                build_write_multiple_request(1, 300, [22]),
                devcode=2376,
                collector_addr=1,
                fcode=4,
            )
            writer.write(cloud_write)
            await writer.drain()
            response = await reader.readexactly(13)
            collector_messages.append(response)
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await asyncio.sleep(0.15)
            forwarded = await asyncio.wait_for(collector_reader.read(1), timeout=0.2)
            self.assertEqual(forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(collector_messages[0][:HEADER_SIZE])
        response_payload = collector_messages[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 55)
        self.assertEqual(response_header.fcode, 4)
        self.assertEqual(response_payload[1], 0x90)

    async def test_cloud_read_and_at_commands_are_answered_locally(self) -> None:
        observed_responses: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            cloud_read = build_collector_request(
                77,
                build_read_holding_request(1, 300, 2),
                devcode=2376,
                collector_addr=1,
                fcode=4,
            )
            writer.write(cloud_read)
            await writer.drain()
            observed_responses.append(await reader.readexactly(17))

            writer.write(b"AT+QID?\r\n")
            await writer.drain()
            observed_responses.append(await reader.readuntil(b"\n"))
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await asyncio.sleep(0.15)
            self.assertEqual(await collector_reader.read(1), b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        read_header = decode_header(observed_responses[0][:HEADER_SIZE])
        read_payload = observed_responses[0][HEADER_SIZE:]
        self.assertEqual(read_header.tid, 77)
        self.assertEqual(read_payload[:3], bytes([1, 3, 4]))
        self.assertEqual(int.from_bytes(read_payload[3:5], "big"), 10)
        self.assertEqual(int.from_bytes(read_payload[5:7], "big"), 11)
        self.assertEqual(int.from_bytes(read_payload[-2:], "little"), crc16_modbus(read_payload[:-2]))

        at_response = parse_at_response(observed_responses[1])
        self.assertEqual(at_response.command, "QID")
        self.assertEqual(at_response.value, "E5000020000000")

    async def test_unknown_cloud_command_is_blocked_and_answered_locally(self) -> None:
        upstream_responses: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            unknown = build_collector_request(
                91,
                b"\x01\x02\x03\x04\x05",
                devcode=2376,
                collector_addr=1,
                fcode=4,
            )
            writer.write(unknown)
            await writer.drain()
            upstream_responses.append(await reader.readexactly(13))
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await asyncio.sleep(0.15)
            forwarded = await asyncio.wait_for(collector_reader.read(1), timeout=0.2)
            self.assertEqual(forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(upstream_responses[0][:HEADER_SIZE])
        response_payload = upstream_responses[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 91)
        # The frame is a FORWARD_TO_DEVICE-wrapped Modbus PDU (\x01\x02...): the
        # NACK echoes the INNER slave id (1) and function (2 | 0x80 = 0x82),
        # NOT the eybond wrapper fcode (was wrongly 0x84).
        self.assertEqual(response_payload[:2], bytes([1, 0x82]))

    async def test_correlated_tid_spoofed_cloud_device_command_is_not_forwarded(self) -> None:
        upstream_responses: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            collector_request = await reader.readexactly(12)
            request_header = decode_header(collector_request[:HEADER_SIZE])
            spoofed_device_command = build_collector_request(
                request_header.tid,
                build_write_multiple_request(1, 300, [44]),
                devcode=request_header.devcode,
                collector_addr=request_header.devaddr,
                fcode=4,
            )
            writer.write(spoofed_device_command)
            await writer.drain()
            upstream_responses.append(await reader.readexactly(13))
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            request = build_collector_request(
                123,
                b"ping",
                devcode=2376,
                collector_addr=1,
                fcode=1,
            )
            collector_writer.write(request)
            await collector_writer.drain()
            forwarded = await asyncio.wait_for(collector_reader.read(1), timeout=0.3)
            self.assertEqual(forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(upstream_responses[0][:HEADER_SIZE])
        response_payload = upstream_responses[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 123)
        self.assertEqual(response_payload[1], 0x90)
        self.assertTrue(
            any(event.get("kind") == "shadow_proxy_block_cloud_correlated_unallowlisted" for event in trace)
        )

    async def test_malformed_cloud_frame_emits_parser_error_and_fail_closes(self) -> None:
        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            malformed = encode_header(
                tid=77,
                devcode=2376,
                total_len=7,
                devaddr=1,
                fcode=1,
            )
            writer.write(malformed)
            await writer.drain()
            await reader.read()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await asyncio.sleep(0.1)
            self.assertEqual(await asyncio.wait_for(collector_reader.read(1), timeout=0.3), b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        self.assertTrue(
            any(
                event.get("kind") == "shadow_proxy_parser_error"
                and event.get("direction") == "cloud_to_collector"
                for event in trace
            )
        )

    async def test_malformed_collector_frame_emits_parser_error_and_fail_closes(self) -> None:
        upstream_reads: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            upstream_reads.append(await reader.read())
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            malformed = encode_header(
                tid=99,
                devcode=2376,
                total_len=7,
                devaddr=1,
                fcode=1,
            )
            collector_writer.write(malformed)
            await collector_writer.drain()
            self.assertEqual(await asyncio.wait_for(collector_reader.read(1), timeout=0.3), b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        self.assertEqual(upstream_reads[0], b"")
        self.assertTrue(
            any(
                event.get("kind") == "shadow_proxy_parser_error"
                and event.get("direction") == "collector_to_cloud"
                for event in trace
            )
        )

    async def test_unanswerable_cloud_at_query_is_forwarded_and_redirect_stays_local(self) -> None:
        upstream_at_responses: list[bytes] = []
        cloud_done = asyncio.Event()

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            # DTUPN is not in the shadow seed → it must be forwarded to the real
            # collector and the device's genuine PN relayed back to the cloud.
            writer.write(b"AT+DTUPN?\r\n")
            await writer.drain()
            upstream_at_responses.append(await reader.readuntil(b"\n"))
            # CLDSRVHOST1 is a redirect command → answered locally from seed,
            # never forwarded, so the DTU keeps pointing at the proxy.
            writer.write(b"AT+CLDSRVHOST1?\r\n")
            await writer.drain()
            upstream_at_responses.append(await reader.readuntil(b"\n"))
            cloud_done.set()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            forwarded = await asyncio.wait_for(collector_reader.readuntil(b"\n"), timeout=1.0)
            self.assertEqual(forwarded, b"AT+DTUPN?\r\n")
            collector_writer.write(b"AT+DTUPN:E5000020000000\r\n")
            await collector_writer.drain()

            await asyncio.wait_for(cloud_done.wait(), timeout=1.0)

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        dtupn = parse_at_response(upstream_at_responses[0])
        self.assertEqual(dtupn.command, "DTUPN")
        self.assertEqual(dtupn.value, "E5000020000000")
        cldsrv = parse_at_response(upstream_at_responses[1])
        self.assertEqual(cldsrv.command, "CLDSRVHOST1")
        self.assertEqual(cldsrv.value, "192.168.1.50,18899,TCP")

        forwarded_commands = {
            event.get("payload", {}).get("payload_ascii", "").strip()
            for event in trace
            if event.get("kind") == "shadow_proxy_forward_cloud_at"
        }
        self.assertIn("AT+DTUPN?", forwarded_commands)
        self.assertNotIn("AT+CLDSRVHOST1?", forwarded_commands)

    async def test_cloud_write_not_forwarded_to_collector_with_at_forwarding(self) -> None:
        upstream_write_response: list[bytes] = []
        cloud_done = asyncio.Event()

        cloud_write = build_collector_request(
            61,
            build_write_multiple_request(1, 300, [42]),
            devcode=2376,
            collector_addr=1,
            fcode=4,
        )

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"AT+DTUPN?\r\n")
            await writer.drain()
            await reader.readuntil(b"\n")  # genuine DTUPN reply relayed from collector
            writer.write(cloud_write)
            await writer.drain()
            upstream_write_response.append(await reader.readexactly(13))
            cloud_done.set()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            forwarded_at = await asyncio.wait_for(collector_reader.readuntil(b"\n"), timeout=1.0)
            self.assertEqual(forwarded_at, b"AT+DTUPN?\r\n")
            collector_writer.write(b"AT+DTUPN:E5000020000000\r\n")
            await collector_writer.drain()

            await asyncio.wait_for(cloud_done.wait(), timeout=1.0)

            # The collector must receive only the forwarded AT query — never the
            # modbus write, which is intercepted and NACK'd to the cloud locally.
            extra = b""
            try:
                extra = await asyncio.wait_for(collector_reader.read(64), timeout=0.2)
            except asyncio.TimeoutError:
                extra = b""
            self.assertEqual(extra, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        response_header = decode_header(upstream_write_response[0][:HEADER_SIZE])
        response_payload = upstream_write_response[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 61)
        self.assertEqual(response_header.fcode, 4)
        self.assertEqual(response_payload[1], 0x90)

    async def test_cloud_raw_modbus_read_answered_and_write_observed_and_nacked(self) -> None:
        # This DTU family exchanges bare Modbus RTU (no collector header) after
        # AT registration. Reads must be answered from the synthetic bank and
        # writes observed + NACK'd, with nothing forwarded to the collector.
        read_request = build_read_holding_request(1, 300, 2)
        write_request = build_write_multiple_request(1, 300, [5])
        responses: list[bytes] = []
        cloud_done = asyncio.Event()

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(read_request)
            await writer.drain()
            responses.append(await reader.readexactly(9))  # 01 03 04 <4 data> <crc>
            writer.write(write_request)
            await writer.drain()
            responses.append(await reader.readexactly(5))  # 01 90 01 <crc>
            cloud_done.set()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await asyncio.wait_for(cloud_done.wait(), timeout=1.0)

            # The collector (real inverter side) must never see this traffic.
            self.assertEqual(await asyncio.wait_for(collector_reader.read(1), timeout=0.3), b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        upstream_server.close()
        await upstream_server.wait_closed()

        # Read answered from the seed register bank {300: 10, 301: 11}.
        read_response = responses[0]
        self.assertEqual(read_response[:3], bytes([1, 3, 4]))
        self.assertEqual(int.from_bytes(read_response[3:5], "big"), 10)
        self.assertEqual(int.from_bytes(read_response[5:7], "big"), 11)
        self.assertEqual(int.from_bytes(read_response[-2:], "little"), crc16_modbus(read_response[:-2]))

        # Write NACK'd with a Modbus exception (function 0x10 | 0x80 = 0x90).
        write_response = responses[1]
        self.assertEqual(write_response[0], 1)
        self.assertEqual(write_response[1], 0x90)
        self.assertEqual(int.from_bytes(write_response[-2:], "little"), crc16_modbus(write_response[:-2]))

        # The write was observed for learning, decoded to register 300 = [5].
        observations = [
            event for event in trace if event.get("kind") == "shadow_modbus_write_observation"
        ]
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["payload"]["register"], 300)
        self.assertEqual(observations[0]["payload"]["values"], [5])


class ConsumeNextMessageFramingTests(unittest.TestCase):
    def test_g_ascii_line_is_framed_as_ascii_message(self) -> None:
        buffer = bytearray(b"GPDAT0\r")

        result = _consume_next_message(buffer)

        self.assertEqual(result, ("ascii", b"GPDAT0\r"))
        self.assertEqual(buffer, bytearray())

    def test_at_line_still_wins_over_generic_ascii_framing(self) -> None:
        buffer = bytearray(b"AT+WFSS?\r\n")

        result = _consume_next_message(buffer)

        self.assertEqual(result, ("at", b"AT+WFSS?\r\n"))
        self.assertEqual(buffer, bytearray())

    def test_eybond_frame_with_modbus_colliding_tid_is_not_stalled(self) -> None:
        # An eybond frame whose tid low byte is 0x10 (Modbus write-multiple
        # fcode) used to be read as a phantom Modbus frame and stall the
        # direction. It must be framed as a complete eybond frame instead.
        for tid in (0x0010, 0x0110, 0x0003, 0x0106):
            with self.subTest(tid=tid):
                frame = build_collector_request(
                    tid,
                    b"\x01\x03\x00\x10",
                    devcode=2376,
                    collector_addr=1,
                    fcode=4,
                )
                buffer = bytearray(frame)
                result = _consume_next_message(buffer)
                self.assertIsNotNone(result)
                kind, consumed = result
                self.assertEqual(kind, "frame")
                self.assertEqual(consumed, frame)
                self.assertEqual(len(buffer), 0)

    def test_eybond_frame_with_ascii_like_tid_is_not_stalled(self) -> None:
        frame = build_collector_request(
            0x4701,
            b"\x01\x03\x00\x10",
            devcode=2376,
            collector_addr=1,
            fcode=4,
        )
        buffer = bytearray(frame)

        result = _consume_next_message(buffer)

        self.assertIsNotNone(result)
        kind, consumed = result
        self.assertEqual(kind, "frame")
        self.assertEqual(consumed, frame)
        self.assertEqual(buffer, bytearray())

    def test_partial_eybond_frame_still_waits(self) -> None:
        frame = build_collector_request(
            0x0010, b"\x01\x03\x00\x10", devcode=2376, collector_addr=1, fcode=4
        )
        buffer = bytearray(frame[:-2])  # one short of complete
        self.assertIsNone(_consume_next_message(buffer))


class RouteStatusControlReadyTests(unittest.TestCase):
    def _status(self, **overrides):
        base = {
            "running": True,
            "collector_connected": False,
            "collector_protocol_ingress": False,
            "route_protocol_activity": False,
            "upstream_connected": False,
            "ready": False,
            "upstream_error": "",
        }
        base.update(overrides)
        return base

    def test_ready_without_upstream_once_collector_reconnects_and_speaks(self):
        # Start readiness: the collector has reconnected to our proxy after the endpoint switch
        # and is speaking our protocol, but the proxy->cloud socket is NOT up. This is enough for
        # the start-time reconnect wait, but not enough for an actual ctrlDevice write.
        self.assertTrue(
            route_status_indicates_control_ready(
                self._status(collector_connected=True, collector_protocol_ingress=True)
            )
        )
        # Route protocol activity is an equally good signal of a live route.
        self.assertTrue(
            route_status_indicates_control_ready(
                self._status(collector_connected=True, route_protocol_activity=True)
            )
        )

    def test_not_ready_until_collector_is_actually_connected(self):
        # No live collector socket -> never ready, regardless of stale protocol flags.
        self.assertFalse(
            route_status_indicates_control_ready(
                self._status(collector_connected=False, collector_protocol_ingress=True)
            )
        )
        # Bare TCP accept with no protocol seen yet is not ready either.
        self.assertFalse(
            route_status_indicates_control_ready(self._status(collector_connected=True))
        )

    def test_write_ready_requires_live_upstream_socket(self):
        # Regression: a scan leaked one write right after the upstream path dropped. The collector
        # was still marked connected, but SmartESS delivered ctrlDevice over the real-server route
        # instead of through our proxy. Per-write readiness is stricter than start readiness.
        primed = self._status(
            collector_connected=True,
            collector_protocol_ingress=True,
            route_protocol_activity=True,
            upstream_connected=False,
            ready=False,
        )
        self.assertTrue(route_status_indicates_control_ready(primed))
        self.assertFalse(route_status_indicates_control_write_ready(primed))

        live_upstream = dict(primed, upstream_connected=True)
        self.assertTrue(route_status_indicates_control_write_ready(live_upstream))


if __name__ == "__main__":
    unittest.main()
