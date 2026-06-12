from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.proxy_trace import (
    anonymize_proxy_trace_text,
    active_proxy_capture_state_path,
    build_proxy_capture_lease_deadline,
    build_proxy_trace_manifest,
    build_proxy_capture_session_state,
    clear_proxy_capture_session_state,
    export_proxy_trace_bundle,
    export_proxy_trace_manifest,
    load_proxy_capture_session_state,
    load_latest_proxy_trace_manifest,
    proxy_capture_restore_guard_reason,
    proxy_capture_session_is_expired,
    proxy_trace_root,
    refresh_proxy_capture_session_lease,
    save_proxy_capture_session_state,
)


class ProxyTraceTests(unittest.TestCase):
    def test_build_proxy_trace_manifest(self) -> None:
        manifest = build_proxy_trace_manifest(
            source="collector_proxy_capture",
            trace_path="/config/eybond_local/proxy_traces/session.jsonl",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            anonymized=True,
            session={"restore_required": True},
            summary={"at_messages": 3},
        )

        self.assertEqual(manifest["source"], "collector_proxy_capture")
        self.assertEqual(manifest["match"]["entry_id"], "entry-1")
        self.assertEqual(manifest["match"]["collector_pn"], "E5000020000000")
        self.assertEqual(manifest["trace"]["path"], "/config/eybond_local/proxy_traces/session.jsonl")
        self.assertTrue(manifest["trace"]["anonymized"])
        self.assertEqual(manifest["session"]["restore_required"], True)
        self.assertEqual(manifest["summary"]["at_messages"], 3)

    def test_export_and_load_latest_proxy_trace_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            manifest = build_proxy_trace_manifest(
                source="collector_proxy_capture",
                trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                entry_id="entry-1",
                collector_pn="E5000020000000",
            )

            exported = export_proxy_trace_manifest(config_dir=config_dir, manifest=manifest)
            loaded = load_latest_proxy_trace_manifest(
                config_dir,
                entry_id="entry-1",
            )

            self.assertTrue(exported.exists())
            self.assertEqual(exported.parent, proxy_trace_root(config_dir))
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.path, exported)
            self.assertEqual(loaded.payload["trace"]["path"], manifest["trace"]["path"])

    def test_export_proxy_trace_bundle_includes_manifest_and_anonymized_trace(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            trace_path = proxy_trace_root(config_dir) / "session.jsonl"
            trace_path.parent.mkdir(parents=True, exist_ok=True)
            trace_path.write_text(
                '{"kind": "chunk", "direction": "collector_to_cloud", "remote": "192.168.1.55:40000", "chunk_hex": "41542b445455504e3d4535303030303230303030303030303030310d0a", "chunk_ascii": "AT+DTUPN=E50000200000000001\\r\\n"}\n',
                encoding="utf-8",
            )
            manifest = build_proxy_trace_manifest(
                source="collector_proxy_capture",
                trace_path=str(trace_path),
                entry_id="entry-1",
                collector_pn="E5000020000000",
            )
            manifest_path = export_proxy_trace_manifest(config_dir=config_dir, manifest=manifest)

            bundle_path = export_proxy_trace_bundle(manifest_path=manifest_path, overwrite=True)

            self.assertTrue(bundle_path.exists())
            with zipfile.ZipFile(bundle_path) as archive:
                names = set(archive.namelist())
                manifest_text = archive.read(manifest_path.name).decode("utf-8")
                anonymized_text = archive.read("session.anonymized.jsonl").decode("utf-8")
                collector_dump = archive.read("session.collector_to_server.raw.hex").decode("utf-8")
                server_dump = archive.read("session.server_to_collector.raw.hex").decode("utf-8")
            self.assertEqual(
                names,
                {
                    manifest_path.name,
                    "session.anonymized.jsonl",
                    "session.collector_to_server.raw.hex",
                    "session.server_to_collector.raw.hex",
                },
            )
            self.assertIn('"remote": "192.168.1.55:40000"', anonymized_text)
            self.assertIn('"chunk_ascii": "AT+DTUPN=E500**********0001\\r\\n"', anonymized_text)
            self.assertNotIn("E50000200000000001", anonymized_text)
            self.assertNotIn("E5000020000000", manifest_text)
            self.assertIn("453530302a2a2a2a2a2a2a2a2a2a30303031", collector_dump)
            self.assertEqual(server_dump, "")

    def test_export_proxy_trace_bundle_can_include_raw_trace(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            trace_path = proxy_trace_root(config_dir) / "session.jsonl"
            trace_path.parent.mkdir(parents=True, exist_ok=True)
            trace_path.write_text(
                '{"kind": "chunk", "direction": "collector_to_cloud", "chunk_hex": "deadbeef"}\n'
                '{"kind": "chunk", "direction": "cloud_to_collector", "chunk_hex": "cafebabe"}\n',
                encoding="utf-8",
            )
            manifest = build_proxy_trace_manifest(
                source="collector_proxy_capture",
                trace_path=str(trace_path),
                entry_id="entry-1",
                collector_pn="E5000020000000",
                anonymized=False,
            )
            manifest_path = export_proxy_trace_manifest(config_dir=config_dir, manifest=manifest)

            bundle_path = export_proxy_trace_bundle(manifest_path=manifest_path, overwrite=True)

            with zipfile.ZipFile(bundle_path) as archive:
                names = set(archive.namelist())
                raw_text = archive.read(trace_path.name).decode("utf-8")
                collector_dump = archive.read("session.collector_to_server.raw.hex").decode("utf-8")
                server_dump = archive.read("session.server_to_collector.raw.hex").decode("utf-8")
            self.assertEqual(
                names,
                {
                    manifest_path.name,
                    trace_path.name,
                    "session.collector_to_server.raw.hex",
                    "session.server_to_collector.raw.hex",
                },
            )
            self.assertIn("deadbeef", raw_text)
            self.assertEqual(collector_dump, "deadbeef\n")
            self.assertEqual(server_dump, "cafebabe\n")

    def test_anonymize_proxy_trace_text_masks_only_collector_serials(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_path = Path(tmpdir) / "session.jsonl"
            trace_path.write_text(
                '{"kind": "restore_inject_response", "client": "192.168.1.55:40000", "response_value": "E50000200000000001", "payload_ascii": "AT+DTUPN:E50000200000000001"}\n',
                encoding="utf-8",
            )

            anonymized = anonymize_proxy_trace_text(trace_path)

        self.assertIn('"client": "192.168.1.55:40000"', anonymized)
        self.assertIn('"response_value": "E500**********0001"', anonymized)
        self.assertIn('"payload_ascii": "AT+DTUPN:E500**********0001"', anonymized)
        self.assertNotIn("E50000200000000001", anonymized)

    def test_save_load_and_clear_proxy_capture_session_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            state = build_proxy_capture_session_state(
                entry_id="entry-1",
                collector_pn="E5000020000000",
                trace_path="/config/eybond_local/proxy_traces/session.jsonl",
                original_endpoint="collector-cloud.smartess.example,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                restore_required=True,
                anonymized=True,
                started_at="2026-04-28T12:00:00Z",
                expires_at="2026-04-28T12:05:00Z",
                status="running",
            )

            saved_path = save_proxy_capture_session_state(config_dir=config_dir, state=state)
            loaded = load_proxy_capture_session_state(config_dir)

            self.assertEqual(saved_path, active_proxy_capture_state_path(config_dir))
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.trace_path, state.trace_path)
            self.assertEqual(loaded.original_endpoint, state.original_endpoint)
            self.assertEqual(loaded.proxy_endpoint, state.proxy_endpoint)
            self.assertTrue(loaded.restore_required)

            clear_proxy_capture_session_state(config_dir)
            self.assertIsNone(load_proxy_capture_session_state(config_dir))

    def test_refresh_proxy_capture_session_lease_updates_deadline(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            trace_path="/config/eybond_local/proxy_traces/session.jsonl",
            original_endpoint="collector-cloud.smartess.example,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            restore_required=True,
            anonymized=True,
            started_at="2026-04-28T12:00:00Z",
            expires_at="2026-04-28T12:05:00Z",
            status="running",
        )

        refreshed = refresh_proxy_capture_session_lease(
            state,
            lease_seconds=600,
            now=datetime(2026, 4, 28, 12, 10, tzinfo=timezone.utc),
        )

        self.assertEqual(refreshed.status, "running")
        self.assertEqual(
            refreshed.expires_at,
            build_proxy_capture_lease_deadline(
                lease_seconds=600,
                now=datetime(2026, 4, 28, 12, 10, tzinfo=timezone.utc),
            ),
        )

    def test_proxy_capture_session_expiry_uses_expires_at_deadline(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            trace_path="",
            original_endpoint="collector-cloud.smartess.example,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            restore_required=True,
            anonymized=True,
            started_at="2026-04-28T12:00:00Z",
            expires_at="2026-04-28T12:05:00+00:00",
            status="running",
        )

        self.assertFalse(
            proxy_capture_session_is_expired(
                state,
                now=datetime(2026, 4, 28, 12, 4, 59, tzinfo=timezone.utc),
            )
        )
        self.assertTrue(
            proxy_capture_session_is_expired(
                state,
                now=datetime(2026, 4, 28, 12, 5, tzinfo=timezone.utc),
            )
        )

    def test_proxy_capture_restore_guard_blocks_manual_endpoint_changes(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            trace_path="",
            original_endpoint="47.91.67.66,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            restore_required=True,
            anonymized=True,
            started_at="2026-04-28T12:00:00Z",
            expires_at="2026-04-28T12:05:00+00:00",
            status="running",
        )

        self.assertEqual(
            proxy_capture_restore_guard_reason(
                state,
                current_endpoint="custom.example,18899,TCP",
            ),
            "current_endpoint_changed",
        )
        self.assertEqual(
            proxy_capture_restore_guard_reason(
                state,
                current_endpoint="192.168.1.50,18899,TCP",
            ),
            "",
        )


if __name__ == "__main__":
    unittest.main()