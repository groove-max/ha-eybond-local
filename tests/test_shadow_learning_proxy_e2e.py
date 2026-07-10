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


from custom_components.eybond_local.collector.protocol import HEADER_SIZE, build_collector_request, decode_header
from custom_components.eybond_local.collector.smartess_local import build_set_collector_payload
from custom_components.eybond_local.payload.modbus import build_write_multiple_request
from custom_components.eybond_local.support.shadow_learning_backend import ShadowLearningSeed
from custom_components.eybond_local.support.shadow_learning_proxy import InProcessFailClosedShadowProxyHandler


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


def _read_trace(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class ShadowLearningProxyE2ETests(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_topology_forwards_collector_traffic_and_correlated_response(self) -> None:
        collector_to_cloud: list[bytes] = []
        request = build_collector_request(
            701,
            build_set_collector_payload(21, "collector-cloud.smartess.example,18899,TCP"),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )
        correlated_response = build_collector_request(
            701,
            bytes((0, 21)),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )

        async def _cloud_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            collector_to_cloud.append(await reader.readexactly(len(request)))
            writer.write(correlated_response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud_handler, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=cloud_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy_e2e.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(request)
            await collector_writer.drain()
            collector_received = await collector_reader.readexactly(len(correlated_response))

            collector_writer.close()
            await collector_writer.wait_closed()
            await collector_reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        cloud_server.close()
        await cloud_server.wait_closed()

        self.assertEqual(collector_to_cloud, [request])
        self.assertEqual(collector_received, correlated_response)

    async def test_loopback_topology_blocks_same_function_code_spoofed_set_command(self) -> None:
        cloud_observed_collector: list[bytes] = []
        cloud_observed_response: list[bytes] = []
        cloud_write_sent = asyncio.Event()

        request = build_collector_request(
            733,
            build_set_collector_payload(21, "collector-cloud.smartess.example,18899,TCP"),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )
        spoofed_cloud_command = build_collector_request(
            733,
            build_set_collector_payload(21, "192.168.1.250,18899,TCP"),
            devcode=2376,
            collector_addr=1,
            fcode=3,
        )

        async def _cloud_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            cloud_observed_collector.append(await reader.readexactly(len(request)))
            writer.write(spoofed_cloud_command)
            await writer.drain()
            cloud_observed_response.append(await reader.readexactly(13))
            cloud_write_sent.set()
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud_handler, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy_e2e.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=cloud_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(request)
            await collector_writer.drain()

            await asyncio.wait_for(cloud_write_sent.wait(), timeout=1.0)
            collector_forwarded = await asyncio.wait_for(collector_reader.read(128), timeout=0.4)
            self.assertEqual(collector_forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            await collector_reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        cloud_server.close()
        await cloud_server.wait_closed()

        response_header = decode_header(cloud_observed_response[0][:HEADER_SIZE])
        response_payload = cloud_observed_response[0][HEADER_SIZE:]
        self.assertEqual(cloud_observed_collector, [request])
        self.assertEqual(response_header.tid, 733)
        self.assertEqual(response_header.fcode, 3)
        self.assertEqual(response_payload[1], 0x83)
        self.assertTrue(
            any(
                event.get("kind") == "shadow_proxy_block_cloud_correlated_unallowlisted"
                and event.get("payload", {}).get("reason") == "fc3_response_length_invalid"
                for event in trace
            )
        )

    async def test_loopback_topology_intercepts_cloud_writes_and_keeps_collector_clean(self) -> None:
        cloud_observed_collector: list[bytes] = []
        cloud_observed_response: list[bytes] = []
        write_sent = asyncio.Event()

        async def _cloud_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            cloud_observed_collector.append(await reader.readexactly(12))
            cloud_write = build_collector_request(
                915,
                build_write_multiple_request(1, 300, [22]),
                devcode=2376,
                collector_addr=1,
                fcode=4,
            )
            writer.write(cloud_write)
            await writer.drain()
            cloud_observed_response.append(await reader.readexactly(13))
            write_sent.set()
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud_handler, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=cloud_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_proxy_e2e.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            heartbeat = build_collector_request(311, b"ping", devcode=2376, collector_addr=1, fcode=1)
            collector_writer.write(heartbeat)
            await collector_writer.drain()

            await asyncio.wait_for(write_sent.wait(), timeout=1.0)
            collector_forwarded = await asyncio.wait_for(collector_reader.read(64), timeout=0.4)
            self.assertEqual(collector_forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            await collector_reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        cloud_server.close()
        await cloud_server.wait_closed()

        response_header = decode_header(cloud_observed_response[0][:HEADER_SIZE])
        response_payload = cloud_observed_response[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 915)
        self.assertEqual(response_payload[:2], bytes([1, 0x90]))
        self.assertEqual(cloud_observed_collector, [heartbeat])

    async def test_loopback_topology_blocks_unknown_cloud_commands(self) -> None:
        cloud_observed_response: list[bytes] = []

        async def _cloud_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            _ = await reader.readexactly(12)
            unknown = build_collector_request(944, b"\x01\x02\x03\x04\x05", devcode=2376, collector_addr=1, fcode=4)
            writer.write(unknown)
            await writer.drain()
            cloud_observed_response.append(await reader.readexactly(13))
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud_handler, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_proxy_e2e.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=cloud_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            collector_reader, collector_writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            collector_writer.write(build_collector_request(401, b"ping", devcode=2376, collector_addr=1, fcode=1))
            await collector_writer.drain()

            collector_forwarded = await asyncio.wait_for(collector_reader.read(64), timeout=0.4)
            self.assertEqual(collector_forwarded, b"")

            collector_writer.close()
            await collector_writer.wait_closed()
            await collector_reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()
            trace = _read_trace(trace_path)

        cloud_server.close()
        await cloud_server.wait_closed()

        response_header = decode_header(cloud_observed_response[0][:HEADER_SIZE])
        response_payload = cloud_observed_response[0][HEADER_SIZE:]
        self.assertEqual(response_header.tid, 944)
        # FORWARD_TO_DEVICE-wrapped Modbus PDU: the NACK echoes the inner slave
        # id (1) and function (2 | 0x80 = 0x82), not the wrapper fcode (0x84).
        self.assertEqual(response_payload[:2], bytes([1, 0x82]))
        self.assertTrue(
            any(event.get("kind") == "shadow_proxy_intercept_cloud_unclassified" for event in trace)
        )


if __name__ == "__main__":
    unittest.main()
