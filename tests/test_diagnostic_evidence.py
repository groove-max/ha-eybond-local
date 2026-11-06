"""Optional support evidence: identity isolation, bounded storage and privacy."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from custom_components.eybond_local.const import LOCAL_METADATA_DIR
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget
from custom_components.eybond_local.support.runtime_projection import build_inverter_support_payload
from custom_components.eybond_local.payload.modbus import crc16_modbus
from custom_components.eybond_local.support.diagnostic_evidence import (
    DIAGNOSTIC_MEMBER, PROTOCOL_MEMBER, MAX_AGE, MAX_RECORD_BYTES,
    diagnostic_evidence_scope, load_diagnostic_evidence, save_diagnostic_evidence,
    scope_from_bundle,
)
from custom_components.eybond_local.support.diagnostic_export import export_diagnostic_run
from custom_components.eybond_local.support.diagnostic_runner import DiagnosticRunResult
from custom_components.eybond_local.support.package import export_support_package
from test_protocol_candidates import synthetic_exchange


PN = "E5000020000000"
ENTRY = "synthetic-entry"
NOW = datetime.now(timezone.utc)


def bundle():
    return {"entry": {"data": {"collector_pn": PN}}, "runtime": {
        "collector": {"collector_pn": PN}, "inverter": {
            "driver_key": "modbus_smg", "serial_number": "SYNTHETIC-INVERTER",
            "model_name": "Synthetic model", "variant_key": "synthetic",
            "probe_target": {"devcode": 1, "collector_addr": 255, "device_addr": 1},
        },
    }}


def result():
    return DiagnosticRunResult(success=True, output="synthetic capture", results=[],
        context={"entry_id": ENTRY, "collector_pn": PN,
                 "wire_events": [synthetic_exchange()],
                 "session": {"password": "synthetic-private", "token": "synthetic-token"}},
        started_at=NOW.isoformat(), finished_at=NOW.isoformat(), error=None)


class DiagnosticEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bundle = bundle()
        self.scope = scope_from_bundle(ENTRY, self.bundle)

    def save(self, *, finished=None, scope=None):
        return save_diagnostic_evidence(self.root, ENTRY, scope or self.scope,
            {"finished_at": (finished or NOW).isoformat(), "success": False,
             "context": {"password": "synthetic-private"}},
            {"status": "no_structural_match"})

    def load(self, *, scope=None, now=NOW):
        return load_diagnostic_evidence(self.root, ENTRY, scope or self.scope, now=now)

    def cache_path(self):
        return self.root / LOCAL_METADATA_DIR / "diagnostic_evidence" / f"{hashlib.sha256(ENTRY.encode()).hexdigest()}.json"

    def test_same_identity_survives_reload_but_not_replacement(self):
        self.save()
        summary, members = self.load(scope=scope_from_bundle(ENTRY, deepcopy(self.bundle)))
        self.assertEqual(summary["status"], "included")
        self.assertTrue(summary["historical_capture"])
        self.assertFalse(members[DIAGNOSTIC_MEMBER]["success"])
        self.assertNotIn("synthetic-private", json.dumps(members))
        self.assertEqual(self.cache_path().stat().st_mode & 0o777, 0o600)
        changes = [("collector", "collector_pn", "Q0000000000001"),
                   ("inverter", "serial_number", "replacement"),
                   ("inverter", "driver_key", "pi30"),
                   ("inverter", "variant_key", "different")]
        for role, key, value in changes:
            changed = deepcopy(self.bundle)
            changed["runtime"][role][key] = value
            with self.subTest(key=key):
                summary, members = self.load(scope=scope_from_bundle(ENTRY, changed))
                self.assertEqual(summary["status"], "identity_mismatch")
                self.assertEqual(members, {})
        changed = deepcopy(self.bundle)
        changed["runtime"]["inverter"]["probe_target"]["collector_addr"] = 1
        self.assertEqual(self.load(scope=scope_from_bundle(ENTRY, changed))[1], {})
        self.assertEqual(load_diagnostic_evidence(self.root, "other-entry", self.scope)[0]["status"], "not_recorded")

    def test_scope_ignores_ip_but_unknown_inverter_is_not_later_known_inverter(self):
        changed = deepcopy(self.bundle)
        changed["runtime"]["collector"]["remote_ip"] = "192.0.2.123"
        self.assertEqual(scope_from_bundle(ENTRY, changed), self.scope)
        changed["runtime"]["inverter"] = None
        unknown = scope_from_bundle(ENTRY, changed)
        self.assertEqual(unknown.identity_level, "collector_only")
        self.save(scope=unknown)
        self.assertEqual(self.load()[0]["status"], "identity_mismatch")
        self.assertIsNone(diagnostic_evidence_scope(ENTRY))
        self.assertIsNone(diagnostic_evidence_scope(ENTRY, configured_pn=PN, inverter={}))
        self.assertEqual(diagnostic_evidence_scope(ENTRY, configured_pn=PN,
            collector={"collector_pn": PN[:-1]}), unknown)

    def test_typed_runtime_and_archive_projection_have_identical_binding(self):
        inverter = DetectedInverter(driver_key="modbus_smg", protocol_family="modbus_rtu",
            model_name="Synthetic model", serial_number="SYNTHETIC-INVERTER",
            probe_target=ProbeTarget(1, 255, 1), variant_key="synthetic")
        runtime_scope = diagnostic_evidence_scope(ENTRY, configured_pn=PN,
            collector=CollectorInfo(remote_ip="192.0.2.23", collector_pn=PN), inverter=inverter)
        self.bundle["runtime"]["inverter"] = build_inverter_support_payload(inverter)
        self.assertEqual(scope_from_bundle(ENTRY, self.bundle), runtime_scope)
        self.assertEqual(runtime_scope, self.scope)
        for malformed in ({"runtime": []}, {"runtime": "invalid"},
                          {"entry": "invalid"}, {"entry": {"data": "invalid"}}):
            self.assertIsNone(scope_from_bundle(ENTRY, malformed))

    def test_retention_window_and_bad_clock(self):
        self.save()
        self.assertEqual(self.load(now=NOW + MAX_AGE)[0]["status"], "included")
        for now in (NOW - timedelta(seconds=1), NOW + MAX_AGE + timedelta(seconds=1)):
            self.assertEqual(self.load(now=now)[0]["status"], "outside_retention_window")

    def test_invalid_and_oversize_files_do_not_break_support_export(self):
        self.save()
        for content in (b"[]", b"{", b"{}", b"[" * 10000, b"x" * (MAX_RECORD_BYTES + 1)):
            with self.subTest(size=len(content)):
                self.cache_path().write_bytes(content)
                self.assertEqual(self.load()[1], {})
        self.save()
        original = json.loads(self.cache_path().read_text())
        for update in ({"version": 99}, {"finished_at": "yesterday"},
                       {"finished_at": "2026-11-06T12:00:00"}, {"diagnostic": []},
                       {"diagnostic": {"finished_at": "different"}}):
            self.cache_path().write_text(json.dumps(original | update))
            self.assertEqual(self.load()[1], {})

    def test_symlink_and_fifo_are_not_opened_as_evidence(self):
        self.save()
        path = self.cache_path()
        external = self.root / "unrelated.json"
        external.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(external)
        self.assertEqual(self.load()[0]["status"], "invalid_record")
        path.unlink()
        os.mkfifo(path)
        self.assertEqual(self.load()[0]["status"], "invalid_record")
        path.unlink()
        path.parent.rmdir()
        path.parent.symlink_to(self.root, target_is_directory=True)
        self.assertEqual(self.load()[0]["status"], "invalid_record")
        with self.assertRaises(OSError):
            self.save()

    def test_tombstones_replace_previous_success_when_unbound_or_too_large(self):
        self.save()
        status = save_diagnostic_evidence(self.root, ENTRY, self.scope,
            {"finished_at": NOW.isoformat(), "output": "x" * MAX_RECORD_BYTES}, {})
        self.assertEqual(status, "record_too_large")
        self.assertEqual(self.load()[0]["status"], "record_too_large")
        self.assertLess(self.cache_path().stat().st_size, 1024)
        self.save()
        save_diagnostic_evidence(self.root, ENTRY, None, {"finished_at": NOW.isoformat()}, {})
        self.assertEqual(self.load()[0]["status"], "identity_mismatch")

    def test_export_cache_zip_roundtrip_redaction_and_original_build(self):
        diagnostic = result()
        exported = export_diagnostic_run(config_dir=self.root, entry_id=ENTRY,
                                        result=diagnostic, device_scope=self.scope)
        self.assertIn("synthetic-private", exported.result_path.read_text())
        self.assertNotIn("synthetic-private", exported.shareable_path.read_text())
        original_build = json.loads(exported.shareable_path.read_text())["context"]["integration_build"]
        package = export_support_package(config_dir=self.root, entry_id=ENTRY, entry_title="test",
            support_bundle=self.bundle, raw_capture=None, fixture=None, anonymized_fixture=None)
        with ZipFile(package.path) as archive:
            manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["archive_members"]["diagnostic_evidence"]["status"], "included")
            captured = json.loads(archive.read(DIAGNOSTIC_MEMBER))
            report = json.loads(archive.read(PROTOCOL_MEMBER))
            self.assertEqual(report["candidate_keys"], ["eybond_099e_06"])
            self.assertEqual(captured["context"]["integration_build"], original_build)
            self.assertEqual(captured["context"]["wire_events"][0]["response_hex"],
                             synthetic_exchange()["response_hex"])
            self.assertNotIn(PN, archive.read(DIAGNOSTIC_MEMBER).decode())
            self.assertNotIn(ENTRY, archive.read(DIAGNOSTIC_MEMBER).decode())
            self.assertNotIn("synthetic-token", archive.read(DIAGNOSTIC_MEMBER).decode())

    def test_last_failed_run_replaces_old_structural_match(self):
        export_diagnostic_run(config_dir=self.root, entry_id=ENTRY, result=result(), device_scope=self.scope)
        failed = replace(result(), success=False, error="timeout", context={"wire_events": []})
        export_diagnostic_run(config_dir=self.root, entry_id=ENTRY, result=failed, device_scope=self.scope)
        summary, members = self.load()
        self.assertEqual(summary["status"], "included")
        self.assertFalse(members[DIAGNOSTIC_MEMBER]["success"])
        self.assertEqual(members[PROTOCOL_MEMBER]["candidate_keys"], [])

    def test_known_identity_wire_range_is_not_reintroduced_by_cache(self):
        body = bytes.fromhex("010300ba0007")
        request = body + crc16_modbus(body).to_bytes(2, "little")
        diagnostic = result()
        diagnostic.context["wire_events"] = [synthetic_exchange() | {
            "request_hex": request.hex(), "request_size": len(request),
            "response_hex": "abcdefaa", "response_size": 4,
        }]
        export_diagnostic_run(config_dir=self.root, entry_id=ENTRY, result=diagnostic, device_scope=self.scope)
        _, members = self.load()
        event = members[DIAGNOSTIC_MEMBER]["context"]["wire_events"][0]
        self.assertEqual(event["response_hex"], "REDACTED_IDENTITY_RANGE")
        self.assertEqual(diagnostic.context["wire_events"][0]["response_hex"], "abcdefaa")

    def test_optional_cache_failure_preserves_raw_and_shareable_exports(self):
        module = "custom_components.eybond_local.support.diagnostic_export"
        with patch(f"{module}.save_diagnostic_evidence", side_effect=OSError("full")):
            exported = export_diagnostic_run(config_dir=self.root, entry_id=ENTRY, result=result())
        payload = json.loads(exported.shareable_path.read_text())
        self.assertTrue(payload["success"])
        self.assertEqual(payload["context"]["support_archive_cache_status"], "cache_write_failed")
        self.assertTrue(exported.result_path.exists())


if __name__ == "__main__":
    unittest.main()
