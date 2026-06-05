from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.bundle import build_support_bundle_payload
from custom_components.eybond_local.support.package import (
    build_shadow_learning_runtime_values,
    export_support_package,
)


class ShadowLearningSupportPackageTests(unittest.TestCase):
    def test_exports_shadow_learning_artifacts_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            trace_path = self._write_shadow_trace(config_dir)
            profile_path, schema_path = self._write_generated_overlay_pair(config_dir)

            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000025388419"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG 6200",
                    "serial_number": "92632511100118",
                },
                values={
                    "shadow_learning_trace_path": str(trace_path),
                    "local_profile_draft_path": str(profile_path),
                    "local_schema_draft_path": str(schema_path),
                    "shadow_learning_plan": {
                        "items": [{"field_id": "sys_eybond_ctrl_53", "value": "1"}],
                        "signature": {"mode": "manual"},
                    },
                    "shadow_learning_orchestration": {
                        "correlation": {
                            "matched_count": 1,
                            "unmatched_attempt_count": 0,
                            "unmatched_write_count": 0,
                        }
                    },
                },
                data={"server_ip": "192.168.1.50"},
                options={
                    "device_scoped_overlay_activation": {
                        "profile_name": "learned/shadow_learning/device/overlay_profile.json",
                        "register_schema_name": "learned/shadow_learning/device/overlay_schema.json",
                        "scope": "device",
                        "activation_scope": {
                            "collector_pn": "E5000025388419",
                            "secret_token": "must_not_be_archived",
                        },
                    }
                },
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                support_bundle=support_bundle,
                raw_capture={"capture_kind": "modbus_register_dump"},
                fixture={"fixture_version": 1, "ranges": []},
                anonymized_fixture={"fixture_version": 1, "ranges": [], "anonymized": True},
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                activation = json.loads(
                    archive.read("evidence/shadow_learning/activation_manifest.json").decode("utf-8")
                )
                trace_lines = archive.read("evidence/shadow_learning/trace.jsonl").decode("utf-8").strip().splitlines()
                writes_lines = archive.read("evidence/shadow_learning/writes.jsonl").decode("utf-8").strip().splitlines()

            self.assertIn("evidence/shadow_learning/trace.jsonl", names)
            self.assertIn("evidence/shadow_learning/events.jsonl", names)
            self.assertIn("evidence/shadow_learning/writes.jsonl", names)
            self.assertIn("evidence/shadow_learning/session_manifest.json", names)
            self.assertIn("evidence/shadow_learning/learn_plan.json", names)
            self.assertIn("evidence/shadow_learning/orchestration.json", names)
            self.assertIn("evidence/shadow_learning/correlation_report.json", names)
            self.assertIn("evidence/shadow_learning/generated_overlay_profile.json", names)
            self.assertIn("evidence/shadow_learning/generated_overlay_schema.json", names)
            self.assertIn("evidence/shadow_learning/activation_manifest.json", names)

            shadow_members = manifest["archive_members"]["shadow_learning"]
            self.assertEqual(
                shadow_members["activation_manifest"],
                "evidence/shadow_learning/activation_manifest.json",
            )
            self.assertEqual(
                shadow_members["generated_overlay_profile"],
                "evidence/shadow_learning/generated_overlay_profile.json",
            )
            self.assertEqual(
                shadow_members["generated_overlay_schema"],
                "evidence/shadow_learning/generated_overlay_schema.json",
            )

            self.assertNotIn("secret_token", json.dumps(activation))
            parsed_write = json.loads(writes_lines[0])
            self.assertNotIn("session_token", parsed_write)

            parsed_trace = [json.loads(line) for line in trace_lines]
            serialized_trace = json.dumps(parsed_trace)
            self.assertNotIn("authorization", serialized_trace)
            self.assertNotIn("session_token", serialized_trace)

    def test_runtime_artifact_publication_values_are_sanitized(self) -> None:
        values = build_shadow_learning_runtime_values(
            plan={
                "items": [{"field_id": "field-1", "password": "hidden"}],
                "secret_note": "hidden",
            },
            orchestration={
                "results": [{"field_id": "field-1", "session_token": "hidden"}],
                "correlation": {
                    "matched_count": 1,
                    "authorization": "Bearer hidden",
                },
            },
            activation={
                "scope": "device",
                "activation_scope": {
                    "collector_pn": "E5000025388419",
                    "api_secret": "hidden",
                },
            },
            session_id="entry-shadow-session",
            device_scope={
                "collector_pn": "E5000025388419",
                "cloud_sn": "E50000253884199645094801",
            },
        )

        serialized = json.dumps(values)
        self.assertNotIn("hidden", serialized)
        self.assertNotIn("password", serialized)
        self.assertNotIn("session_token", serialized)
        self.assertNotIn("authorization", serialized)
        self.assertEqual(
            values["shadow_learning_device_scope"]["cloud_sn"],
            "E50000253884199645094801",
        )

    def test_exports_runtime_activation_manifest_when_entry_option_is_not_saved(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            runtime_values = build_shadow_learning_runtime_values(
                activation={
                    "status": "draft_generated",
                    "scope": "device",
                    "activation_scope": {
                        "collector_pn": "E5000025388419",
                        "cloud_sn": "E50000253884199645094801",
                    },
                },
                session_id="entry-shadow-session",
            )
            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000025388419"},
                inverter={"driver_key": "modbus_smg"},
                values=runtime_values,
                data={},
                options={},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                support_bundle=support_bundle,
                raw_capture={},
                fixture=None,
                anonymized_fixture=None,
            )

            with zipfile.ZipFile(result.path) as archive:
                activation = json.loads(
                    archive.read(
                        "evidence/shadow_learning/activation_manifest.json"
                    ).decode("utf-8")
                )

            self.assertEqual(activation["status"], "draft_generated")
            self.assertEqual(
                activation["activation_scope"]["cloud_sn"],
                "E50000253884199645094801",
            )

    def test_keeps_support_archive_compatible_when_shadow_artifacts_absent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry-no-shadow",
                entry_title="No Shadow",
                connected=True,
                collector={"collector_pn": "E5000025388419"},
                inverter={"driver_key": "modbus_smg", "model_name": "SMG", "serial_number": "123"},
                values={"operating_mode": "Line"},
                data={"server_ip": "192.168.1.50"},
                options={},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-no-shadow",
                entry_title="No Shadow",
                support_bundle=support_bundle,
                raw_capture={"capture_kind": "modbus_register_dump"},
                fixture={"fixture_version": 1, "ranges": []},
                anonymized_fixture={"fixture_version": 1, "ranges": [], "anonymized": True},
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))

            self.assertIsNone(manifest["archive_members"]["shadow_learning"])
            self.assertFalse(any(name.startswith("evidence/shadow_learning/") for name in names))

    def test_rejects_explicit_shadow_artifact_paths_outside_expected_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            outside_trace = config_dir / "outside_trace.jsonl"
            outside_profile = config_dir / "outside_profile.json"
            outside_schema = config_dir / "outside_schema.json"
            outside_trace.write_text('{"kind":"shadow_session_manifest"}\n', encoding="utf-8")
            outside_profile.write_text(
                '{"shadow_learning_overlay":{"scope":"device"}}',
                encoding="utf-8",
            )
            outside_schema.write_text(
                '{"shadow_learning_overlay":{"scope":"device"}}',
                encoding="utf-8",
            )
            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000025388419"},
                inverter={"driver_key": "modbus_smg"},
                values={
                    "shadow_learning_trace_path": str(outside_trace),
                    "local_profile_draft_path": str(outside_profile),
                    "local_schema_draft_path": str(outside_schema),
                },
                data={},
                options={},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                support_bundle=support_bundle,
                raw_capture={},
                fixture=None,
                anonymized_fixture=None,
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())

            self.assertFalse(any(name.startswith("evidence/shadow_learning/") for name in names))

    def test_support_package_module_has_no_direct_smartess_cloud_dependency(self) -> None:
        module_path = REPO_ROOT / "custom_components" / "eybond_local" / "support" / "package.py"
        source = module_path.read_text(encoding="utf-8")

        self.assertNotIn("smartess_cloud", source)

    def _write_shadow_trace(self, config_dir: Path) -> Path:
        trace_root = config_dir / "eybond_local" / "shadow_learning_traces"
        trace_root.mkdir(parents=True, exist_ok=True)
        trace_path = trace_root / "entry_shadow_20260605T100000000000Z.jsonl"
        lines = [
            {
                "kind": "shadow_session_manifest",
                "timestamp": "2026-06-05T10:00:00+00:00",
                "session_id": "entry-shadow_20260605T100000000000Z",
                "entry_id": "entry-shadow",
                "collector_pn": "E5000025388419",
                "cloud_pn": "E50000253884199645",
                "cloud_sn": "E50000253884199645094801",
            },
            {
                "kind": "shadow_connect",
                "timestamp": "2026-06-05T10:00:01+00:00",
                "direction": "cloud_to_shadow",
                "payload": {"remote": "192.168.1.20:5555", "authorization": "Bearer nope"},
            },
            {
                "kind": "shadow_modbus_write_observation",
                "timestamp": "2026-06-05T10:00:02+00:00",
                "direction": "cloud_to_shadow",
                "payload": {
                    "register": 201,
                    "values": [1],
                    "function_code": 6,
                    "session_token": "must_not_be_archived",
                },
            },
        ]
        trace_path.write_text("".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines), encoding="utf-8")
        return trace_path

    def _write_generated_overlay_pair(self, config_dir: Path) -> tuple[Path, Path]:
        profile_path = (
            config_dir
            / "eybond_local"
            / "profiles"
            / "learned"
            / "shadow_learning"
            / "device"
            / "overlay_profile.json"
        )
        schema_path = (
            config_dir
            / "eybond_local"
            / "register_schemas"
            / "learned"
            / "shadow_learning"
            / "device"
            / "overlay_schema.json"
        )
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        schema_path.parent.mkdir(parents=True, exist_ok=True)

        overlay_manifest = {
            "kind": "shadow_learning_device_overlay",
            "scope": "device",
            "session": {"session_id": "entry-shadow_20260605T100000000000Z"},
            "correlation_summary": {"matched_count": 1, "unmatched_attempt_count": 0, "unmatched_write_count": 0},
            "learned_capabilities": [{"key": "learned_output_mode_201", "register": 201}],
        }

        profile_path.write_text(
            json.dumps(
                {
                    "draft_of": "smg_modbus.json",
                    "shadow_learning_overlay": overlay_manifest,
                    "capabilities": [],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        schema_path.write_text(
            json.dumps(
                {
                    "draft_of": "modbus_smg/models/smg_6200.json",
                    "shadow_learning_overlay": overlay_manifest,
                    "measurement_descriptions": [],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return profile_path, schema_path


if __name__ == "__main__":
    unittest.main()
