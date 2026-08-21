from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.proxy_capture.session import (  # noqa: E402
    InProcessProxyCaptureHandler,
    build_proxy_capture_command,
    build_proxy_capture_restore_trigger_path,
    build_proxy_capture_trace_path,
    inspect_proxy_capture_start_status,
    inspect_proxy_capture_trace,
    summarize_proxy_capture_trace,
)
from custom_components.eybond_local.support.collector_cloud_proxy import (  # noqa: E402
    JsonLineWriter,
    handle_proxy_client,
)


class ProxySessionTests(unittest.TestCase):
    def test_build_proxy_capture_trace_path_uses_proxy_trace_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = build_proxy_capture_trace_path(
                config_dir=Path(tmpdir),
                entry_id="entry-1",
                collector_pn="E5000020000000",
                timestamp="20260428T120000000000Z",
            )

        self.assertTrue(str(path).endswith("entry_1_20260428T120000000000Z.jsonl"))

    def test_build_proxy_capture_command_targets_cloud_proxy_tool(self) -> None:
        command = build_proxy_capture_command(
            listen_host="0.0.0.0",
            listen_port=18899,
            upstream_host="collector-cloud.smartess.example",
            upstream_port=18899,
            output_path=Path("/tmp/session.jsonl"),
            masked_endpoint="collector-cloud.smartess.example,18899,TCP",
            restore_trigger_path=Path("/tmp/session.restore"),
            python_executable="/usr/bin/python3",
        )

        self.assertEqual(command[0], "/usr/bin/python3")
        self.assertTrue(
            command[2].endswith("custom_components/eybond_local/support/collector_cloud_proxy.py")
        )
        self.assertIn("--output", command)
        self.assertEqual(command[command.index("--output") + 1], "/tmp/session.jsonl")
        self.assertIn("--restore-endpoint", command)
        self.assertEqual(
            command[command.index("--restore-endpoint") + 1],
            "collector-cloud.smartess.example,18899,TCP",
        )
        self.assertIn("--restore-trigger-file", command)
        self.assertEqual(command[command.index("--restore-trigger-file") + 1], "/tmp/session.restore")

    def test_build_proxy_capture_restore_trigger_path_uses_sidecar_suffix(self) -> None:
        self.assertEqual(
            build_proxy_capture_restore_trigger_path(Path("/tmp/session.jsonl")),
            Path("/tmp/session.jsonl.restore"),
        )

    def test_inspect_proxy_capture_start_status_reads_connect_upstream_and_restore_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                '{"kind": "connect", "client": "192.168.1.55:40000"}\n'
                '{"kind": "upstream_connect_error", "error": "ConnectionRefusedError"}\n'
                '{"kind": "proxy_identity_observed", "identity_verified": true}\n'
                '{"kind": "restore_inject_response", "label": "restore_endpoint_at", "response_value": "W000"}\n',
                encoding="utf-8",
            )

            status = inspect_proxy_capture_start_status(trace_path)

        self.assertTrue(status["connected"])
        self.assertEqual(status["upstream_error"], "ConnectionRefusedError")
        self.assertTrue(status["identity_verified"])
        self.assertFalse(status["identity_mismatch"])
        self.assertTrue(status["restore_acknowledged"])
        self.assertNotIn("restore_confirmed", status)

    def test_proxy_start_status_rejects_foreign_cloud_handshake_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                '{"kind": "connect"}\n'
                '{"kind": "proxy_identity_observed", "identity_verified": false}\n',
                encoding="utf-8",
            )

            status = inspect_proxy_capture_start_status(trace_path)

        self.assertTrue(status["connected"])
        self.assertFalse(status["identity_verified"])
        self.assertTrue(status["identity_mismatch"])

    def test_restore_response_is_acknowledgement_not_terminal_proof(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                '{"kind": "restore_inject_response", '
                '"label": "restore_endpoint", '
                '"response_value": "arbitrary-non-empty-response"}\n',
                encoding="utf-8",
            )

            status = inspect_proxy_capture_start_status(trace_path)

        self.assertTrue(status["restore_acknowledged"])
        self.assertNotIn("restore_confirmed", status)

    def test_summarize_proxy_capture_trace_counts_kinds(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                "{\"kind\": \"chunk\"}\n{\"kind\": \"frame\"}\nnot-json\n",
                encoding="utf-8",
            )

            summary = summarize_proxy_capture_trace(trace_path)

        self.assertTrue(summary["exists"])
        self.assertEqual(summary["line_count"], 3)
        self.assertEqual(summary["kind_counts"]["chunk"], 1)
        self.assertEqual(summary["kind_counts"]["frame"], 1)
        self.assertEqual(summary["invalid_lines"], 1)

    def test_summarize_proxy_capture_trace_counts_g_ascii_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                "{\"kind\": \"chunk\", \"chunk_hex\": \"4750444154300d\"}\n"
                "{\"kind\": \"chunk\", \"chunk_hex\": \"283138332e31203032372e360d\"}\n",
                encoding="utf-8",
            )

            summary = summarize_proxy_capture_trace(trace_path)

        self.assertEqual(summary["g_ascii_command_counts"], {"GPDAT0": 1})
        self.assertEqual(summary["g_ascii_response_counts"], {"data": 1})

    def test_inspect_proxy_capture_trace_returns_recent_kinds_and_last_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                "{\"kind\": \"chunk\", \"timestamp\": \"2026-04-28T12:00:01Z\"}\n"
                "{\"kind\": \"frame\", \"timestamp\": \"2026-04-28T12:00:02Z\"}\n"
                "{\"kind\": \"masked_endpoint_response\", \"timestamp\": \"2026-04-28T12:00:03Z\"}\n",
                encoding="utf-8",
            )

            inspection = inspect_proxy_capture_trace(trace_path, recent_limit=2)

        self.assertTrue(inspection["exists"])
        self.assertEqual(inspection["line_count"], 3)
        self.assertEqual(inspection["recent_kinds"], "frame -> masked_endpoint_response")
        self.assertIn("masked AT+CLDSRVHOST1 response", inspection["recent_events"])
        self.assertEqual(inspection["last_timestamp"], "2026-04-28T12:00:03Z")
        self.assertIn("chunk=1", inspection["kind_summary"])

    def test_inspect_proxy_capture_trace_builds_full_live_log_with_transport_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                "{\"kind\": \"chunk\", \"timestamp\": \"2026-04-28T12:00:01Z\", \"direction\": \"collector_to_cloud\", \"chunk_len\": 10, \"chunk_hex\": \"41542b574653533f0d0a\", \"chunk_ascii\": \"AT+WFSS?\\r\\n\"}\n"
                "{\"kind\": \"frame\", \"timestamp\": \"2026-04-28T12:00:02Z\", \"direction\": \"cloud_to_collector\", \"tid\": 1, \"devcode\": 258, \"devaddr\": 5, \"fcode\": 4, \"fcode_name\": \"FC_FORWARD_TO_DEVICE\", \"payload_hex\": \"0103006400034414\", \"payload_ascii\": \"\"}\n"
                "{\"kind\": \"tail\", \"timestamp\": \"2026-04-28T12:00:03Z\", \"direction\": \"cloud_to_collector\", \"remaining_hex\": \"deadbeef\", \"remaining_ascii\": \"\"}\n",
                encoding="utf-8",
            )

            inspection = inspect_proxy_capture_trace(trace_path, recent_limit=2)

        self.assertIn("AT query WFSS?", inspection["live_log"])
        self.assertIn("EyeBond FC_FORWARD_TO_DEVICE", inspection["live_log"])
        self.assertIn("RTU read request slave=1 fc=0x03 addr=0x0064 count=3", inspection["live_log"])
        self.assertIn("unrecognized binary 4 bytes hex=de ad be ef", inspection["live_log"])
        self.assertEqual(inspection["recent_kinds"], "frame -> tail")

    def test_inspect_proxy_capture_trace_formats_proxy_lifecycle_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                '{"kind":"upstream_preconnected","upstream_host":"cloud.example","upstream_port":18899}\n'
                '{"kind":"proxy_identity_observed","collector_pn":"E50000200000000001","identity_verified":true}\n'
                '{"kind":"proxy_operational_activity"}\n'
                '{"kind":"restore_trigger_seen"}\n',
                encoding="utf-8",
            )

            inspection = inspect_proxy_capture_trace(trace_path)

        self.assertIn(
            "cloud connection prepared (cloud.example:18899)",
            inspection["live_log"],
        )
        self.assertIn(
            "collector identity verified E50000200000000001",
            inspection["live_log"],
        )
        self.assertIn("cloud data exchange confirmed", inspection["live_log"])
        self.assertIn(
            "restoring the collector cloud endpoint",
            inspection["live_log"],
        )
        self.assertNotIn("upstream_preconnected", inspection["live_log"])

    def test_inspect_proxy_capture_trace_labels_g_ascii_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                "{\"kind\": \"chunk\", \"timestamp\": \"2026-06-25T12:00:01Z\", \"direction\": \"cloud_to_collector\", \"chunk_hex\": \"4750560d\"}\n"
                "{\"kind\": \"chunk\", \"timestamp\": \"2026-06-25T12:00:02Z\", \"direction\": \"collector_to_cloud\", \"chunk_hex\": \"283138332e31203032372e360d\"}\n",
                encoding="utf-8",
            )

            inspection = inspect_proxy_capture_trace(trace_path, recent_limit=2)

        self.assertIn("G-ASCII command GPV", inspection["live_log"])
        self.assertIn("G-ASCII response", inspection["live_log"])


class TransparentCloudRelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_at_metadata_exchange_is_not_operational_data_plane(self) -> None:
        expected_pn = "E50000200000000001"
        proxy_done = asyncio.Event()

        async def _cloud(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            writer.write(b"AT+DTUPN?\r\n")
            await writer.drain()
            await reader.readuntil(b"\n")
            writer.write(b"AT+ATVER?\r\n")
            await writer.drain()
            await reader.readuntil(b"\n")
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "metadata-only.jsonl"
            with trace_path.open("w", encoding="utf-8") as output:
                frame_writer = JsonLineWriter(output)

                async def _proxy(
                    reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter,
                ) -> None:
                    try:
                        await handle_proxy_client(
                            reader,
                            writer,
                            upstream_host="127.0.0.1",
                            upstream_port=cloud_port,
                            frame_writer=frame_writer,
                            restore_target=None,
                            restore_after=0,
                            restore_at_followup="",
                            restore_trigger_file=None,
                            expected_collector_pn=expected_pn,
                        )
                    finally:
                        proxy_done.set()

                proxy_server = await asyncio.start_server(
                    _proxy,
                    "127.0.0.1",
                    0,
                )
                proxy_port = proxy_server.sockets[0].getsockname()[1]
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1",
                    proxy_port,
                )
                self.assertEqual(await reader.readuntil(b"\n"), b"AT+DTUPN?\r\n")
                writer.write(f"AT+DTUPN:{expected_pn}\r\n".encode("ascii"))
                await writer.drain()
                self.assertEqual(await reader.readuntil(b"\n"), b"AT+ATVER?\r\n")
                writer.write(b"AT+ATVER:1.11\r\n")
                await writer.drain()
                await reader.read()
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(proxy_done.wait(), timeout=2.0)
                proxy_server.close()
                await proxy_server.wait_closed()

            status = inspect_proxy_capture_start_status(trace_path)
        cloud_server.close()
        await cloud_server.wait_closed()

        self.assertTrue(status["identity_verified"])
        self.assertFalse(status["operational_activity"])

    async def test_cloud_handshake_is_forwarded_and_verifies_collector_identity(
        self,
    ) -> None:
        expected_pn = "E50000200000000001"
        data_request = bytes.fromhex("0103006400034414")
        data_response = bytes.fromhex("0103060000000000002175")
        cloud_received: list[bytes] = []
        proxy_done = asyncio.Event()
        proxy_errors: list[BaseException] = []

        async def _cloud(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            writer.write(b"AT+DTUPN?\r\n")
            await writer.drain()
            cloud_received.append(await reader.readuntil(b"\n"))
            writer.write(data_request)
            await writer.drain()
            cloud_received.append(await reader.readexactly(len(data_response)))
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            with trace_path.open("w", encoding="utf-8") as output:
                frame_writer = JsonLineWriter(output)

                async def _proxy(
                    reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter,
                ) -> None:
                    try:
                        await handle_proxy_client(
                            reader,
                            writer,
                            upstream_host="127.0.0.1",
                            upstream_port=cloud_port,
                            frame_writer=frame_writer,
                            restore_target=None,
                            restore_after=0,
                            restore_at_followup="",
                            restore_trigger_file=None,
                            expected_collector_pn=expected_pn,
                        )
                    except BaseException as exc:
                        proxy_errors.append(exc)
                    finally:
                        proxy_done.set()

                proxy_server = await asyncio.start_server(_proxy, "127.0.0.1", 0)
                proxy_port = proxy_server.sockets[0].getsockname()[1]
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1",
                    proxy_port,
                )
                self.assertEqual(await reader.readuntil(b"\n"), b"AT+DTUPN?\r\n")
                response = f"AT+DTUPN:{expected_pn}\r\n".encode("ascii")
                writer.write(response)
                await writer.drain()
                self.assertEqual(
                    await reader.readexactly(len(data_request)),
                    data_request,
                )
                writer.write(data_response)
                await writer.drain()
                await reader.read()
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(proxy_done.wait(), timeout=2.0)
                proxy_server.close()
                await proxy_server.wait_closed()

            cloud_server.close()
            await cloud_server.wait_closed()
            status = inspect_proxy_capture_start_status(trace_path)

        self.assertEqual(
            cloud_received,
            [
                f"AT+DTUPN:{expected_pn}\r\n".encode("ascii"),
                data_response,
            ],
        )
        self.assertEqual(proxy_errors, [])
        self.assertTrue(status["connected"])
        self.assertTrue(status["identity_verified"])
        self.assertFalse(status["identity_mismatch"])
        self.assertTrue(status["operational_activity"])

    async def test_handler_preconnects_real_upstream_before_collector_arrives(
        self,
    ) -> None:
        expected_pn = "E50000200000000001"
        data_request = bytes.fromhex("0103006400034414")
        data_response = bytes.fromhex("0103060000000000002175")
        cloud_connected = asyncio.Event()
        cloud_done = asyncio.Event()

        async def _cloud(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            cloud_connected.set()
            writer.write(b"AT+DTUPN?\r\n")
            await writer.drain()
            self.assertEqual(
                await reader.readuntil(b"\n"),
                f"AT+DTUPN:{expected_pn}\r\n".encode("ascii"),
            )
            writer.write(data_request)
            await writer.drain()
            self.assertEqual(
                await reader.readexactly(len(data_response)),
                data_response,
            )
            cloud_done.set()
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(_cloud, "127.0.0.1", 0)
        cloud_port = cloud_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "preconnected.jsonl"
            handler = InProcessProxyCaptureHandler(
                upstream_host="127.0.0.1",
                upstream_port=cloud_port,
                output_path=trace_path,
                expected_collector_pn=expected_pn,
            )
            await handler.start()
            await asyncio.wait_for(cloud_connected.wait(), timeout=1.0)

            proxy_server = await asyncio.start_server(
                handler.handle_client,
                "127.0.0.1",
                0,
            )
            proxy_port = proxy_server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            self.assertEqual(await reader.readuntil(b"\n"), b"AT+DTUPN?\r\n")
            writer.write(f"AT+DTUPN:{expected_pn}\r\n".encode("ascii"))
            await writer.drain()
            self.assertEqual(
                await reader.readexactly(len(data_request)),
                data_request,
            )
            writer.write(data_response)
            await writer.drain()
            await asyncio.wait_for(cloud_done.wait(), timeout=1.0)
            writer.close()
            await writer.wait_closed()
            await handler.stop()
            proxy_server.close()
            await proxy_server.wait_closed()

            status = inspect_proxy_capture_start_status(trace_path)
            trace_text = trace_path.read_text(encoding="utf-8")

        cloud_server.close()
        await cloud_server.wait_closed()
        self.assertTrue(status["identity_verified"])
        self.assertTrue(status["operational_activity"])
        self.assertIn('"kind": "upstream_preconnected"', trace_text)
        self.assertIn('"kind": "pipe_ended"', trace_text)


if __name__ == "__main__":
    unittest.main()
