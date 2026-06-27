from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.proxy_session import (  # noqa: E402
    build_proxy_capture_command,
    build_proxy_capture_restore_trigger_path,
    build_proxy_capture_trace_path,
    inspect_proxy_capture_start_status,
    inspect_proxy_capture_trace,
    summarize_proxy_capture_trace,
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
                '{"kind": "restore_inject_response", "label": "restore_endpoint_at", "response_value": "W000"}\n',
                encoding="utf-8",
            )

            status = inspect_proxy_capture_start_status(trace_path)

        self.assertTrue(status["connected"])
        self.assertEqual(status["upstream_error"], "ConnectionRefusedError")
        self.assertTrue(status["restore_confirmed"])

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


if __name__ == "__main__":
    unittest.main()
