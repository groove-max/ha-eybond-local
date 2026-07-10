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
from custom_components.eybond_local.support.shadow_learning_overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)
from custom_components.eybond_local.support.shadow_learning_review_model import (
    build_activation_selection,
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
                collector={"collector_pn": "E5000020000000"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG 6200",
                    "serial_number": "92632500000001",
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
                            "collector_pn": "E5000020000000",
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

    def test_exports_generic_protocol_write_observations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            trace_path = self._write_shadow_trace(
                config_dir,
                write_event_kind="shadow_protocol_write_observation",
            )

            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={"driver_key": "must_pv_ph18"},
                values={"shadow_learning_trace_path": str(trace_path)},
                data={},
                options={},
                profile_name="must_pv_ph18/base.json",
                register_schema_name="must_pv_ph18/base.json",
            )
            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                support_bundle=support_bundle,
                raw_capture={},
                fixture={},
                anonymized_fixture={},
            )

            with zipfile.ZipFile(result.path) as archive:
                writes_lines = (
                    archive.read("evidence/shadow_learning/writes.jsonl")
                    .decode("utf-8")
                    .strip()
                    .splitlines()
                )

            self.assertEqual(len(writes_lines), 1)
            parsed_write = json.loads(writes_lines[0])
            self.assertEqual(parsed_write["register"], 201)

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
                    "collector_pn": "E5000020000000",
                    "api_secret": "hidden",
                },
            },
            session_id="entry-shadow-session",
            device_scope={
                "collector_pn": "E5000020000000",
                "cloud_sn": "E50000200000000001000001",
            },
        )

        serialized = json.dumps(values)
        self.assertNotIn("hidden", serialized)
        self.assertNotIn("password", serialized)
        self.assertNotIn("session_token", serialized)
        self.assertNotIn("authorization", serialized)
        self.assertEqual(
            values["shadow_learning_device_scope"]["cloud_sn"],
            "E50000200000000001000001",
        )

    def test_exports_runtime_activation_manifest_when_entry_option_is_not_saved(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            runtime_values = build_shadow_learning_runtime_values(
                activation={
                    "status": "draft_generated",
                    "scope": "device",
                    "activation_scope": {
                        "collector_pn": "E5000020000000",
                        "cloud_sn": "E50000200000000001000001",
                    },
                },
                session_id="entry-shadow-session",
            )
            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
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
            # Archive members are share-safe: long numeric identifiers are
            # masked everywhere, including the activation scope.
            self.assertEqual(
                activation["activation_scope"]["cloud_sn"],
                "E5000***************0001",
            )

    def test_keeps_support_archive_compatible_when_shadow_artifacts_absent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry-no-shadow",
                entry_title="No Shadow",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
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
                collector={"collector_pn": "E5000020000000"},
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

    def test_support_package_preserves_all_discovered_controls_with_review_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            overlay = generate_shadow_learning_overlay_drafts(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_review_session_manifest(),
                correlation=_review_correlation_payload(),
            )

            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={"driver_key": "modbus_smg"},
                values={
                    "local_profile_draft_path": str(overlay.profile_path),
                    "local_schema_draft_path": str(overlay.schema_path),
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
                profile_payload = json.loads(
                    archive.read(
                        "evidence/shadow_learning/generated_overlay_profile.json"
                    ).decode("utf-8")
                )

            self.assertIn(
                "evidence/shadow_learning/generated_overlay_profile.json", names
            )
            review_model = profile_payload["shadow_learning_overlay"]["review_model"]
            registers = {int(entry["register"]) for entry in review_model["learned_all"]}

            # Every discovered control survives into the support package, including the
            # high-risk / disabled one.
            self.assertEqual(registers, {705, 690})
            by_register = {
                int(entry["register"]): entry for entry in review_model["learned_all"]
            }
            self.assertTrue(by_register[705]["enabled_by_default"])
            self.assertFalse(by_register[690]["enabled_by_default"])
            self.assertEqual(by_register[690]["risk_level"], "high")
            self.assertEqual(review_model["counts"]["learned_all"], 2)
            self.assertEqual(review_model["counts"]["excluded_by_policy"], 1)

    def test_support_package_module_has_no_direct_smartess_cloud_dependency(self) -> None:
        module_path = REPO_ROOT / "custom_components" / "eybond_local" / "support" / "package.py"
        source = module_path.read_text(encoding="utf-8")

        self.assertNotIn("smartess_cloud", source)

    def test_support_package_includes_control_discovery_evidence_with_user_selection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            overlay = generate_shadow_learning_overlay_drafts(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_review_session_manifest(),
                correlation=_review_correlation_payload(),
            )
            profile_payload = json.loads(overlay.profile_path.read_text(encoding="utf-8"))
            review_model = profile_payload["shadow_learning_overlay"]["review_model"]
            by_register = {int(entry["register"]): entry for entry in review_model["learned_all"]}
            backlight_key = by_register[705]["key"]
            reset_key = by_register[690]["key"]

            # The user keeps the normal control (renamed) and leaves the high-risk one off.
            selection = build_activation_selection(
                review_model=review_model,
                selections={
                    "controls": {
                        backlight_key: {"enabled": True, "label": "Display Backlight"},
                        reset_key: {"enabled": False},
                    }
                },
            )
            activation = {
                "status": "activated",
                "scope": "device",
                "activation_scope": {
                    "collector_pn": "E5000020000000",
                    "secret_token": "must_not_be_archived",
                },
                **selection,
            }

            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={"driver_key": "modbus_smg"},
                values={
                    "local_profile_draft_path": str(overlay.profile_path),
                    "local_schema_draft_path": str(overlay.schema_path),
                },
                data={},
                options={"device_scoped_overlay_activation": activation},
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
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                discovery = json.loads(
                    archive.read(
                        "evidence/shadow_learning/control_discovery.json"
                    ).decode("utf-8")
                )

            self.assertIn("evidence/shadow_learning/control_discovery.json", names)
            self.assertEqual(
                manifest["archive_members"]["shadow_learning"]["control_discovery"],
                "evidence/shadow_learning/control_discovery.json",
            )
            self.assertEqual(discovery["kind"], "control_discovery_evidence")
            self.assertEqual(discovery["selection_source"], "user_activation")

            # Every discovered control survives, including the disabled high-risk one.
            discovered_by_register = {
                int(entry["register"]): entry for entry in discovery["discovered_controls"]
            }
            self.assertEqual(set(discovered_by_register), {705, 690})
            self.assertEqual(discovery["counts"]["discovered"], 2)
            self.assertTrue(discovered_by_register[690]["risk_reasons"])
            self.assertEqual(discovered_by_register[690]["risk_level"], "high")
            self.assertFalse(discovered_by_register[690]["enabled_by_default"])
            self.assertTrue(discovered_by_register[705]["enabled_by_default"])

            # Selected subset carries the user label; the excluded one keeps its reasons.
            selected_by_key = {entry["key"]: entry for entry in discovery["selected_controls"]}
            excluded_by_key = {entry["key"]: entry for entry in discovery["excluded_controls"]}
            self.assertIn(backlight_key, selected_by_key)
            self.assertEqual(selected_by_key[backlight_key]["label"], "Display Backlight")
            self.assertNotIn(reset_key, selected_by_key)
            self.assertIn(reset_key, excluded_by_key)
            self.assertTrue(excluded_by_key[reset_key]["reasons"])

            # Activation state summary.
            self.assertTrue(discovery["activation"]["present"])
            self.assertTrue(discovery["activation"]["has_user_selection"])
            self.assertEqual(discovery["activation"]["status"], "activated")
            self.assertEqual(discovery["activation"]["scope"], "device")
            self.assertEqual(
                discovery["activation"]["selected_control_keys"], [backlight_key]
            )

            # Recursive sanitization: no credential survives into the consolidated evidence.
            serialized = json.dumps(discovery)
            self.assertNotIn("secret_token", serialized)
            self.assertNotIn("must_not_be_archived", serialized)

    def test_support_package_includes_review_selection_before_activation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            overlay = generate_shadow_learning_overlay_drafts(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_review_session_manifest(),
                correlation=_review_correlation_payload(),
            )
            profile_payload = json.loads(overlay.profile_path.read_text(encoding="utf-8"))
            review_model = profile_payload["shadow_learning_overlay"]["review_model"]
            by_register = {int(entry["register"]): entry for entry in review_model["learned_all"]}
            backlight_key = by_register[705]["key"]
            reset_key = by_register[690]["key"]

            selection = build_activation_selection(
                review_model=review_model,
                selections={
                    "controls": {
                        backlight_key: {"enabled": True, "label": "Display Backlight"},
                        reset_key: {"enabled": False, "label": "Leave Reset Off"},
                    }
                },
            )
            runtime_values = build_shadow_learning_runtime_values(
                profile_draft_path=str(overlay.profile_path),
                schema_draft_path=str(overlay.schema_path),
                activation={
                    "status": "review_selected",
                    "active": False,
                    "scope": "device",
                    **selection,
                },
            )
            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
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
                discovery = json.loads(
                    archive.read(
                        "evidence/shadow_learning/control_discovery.json"
                    ).decode("utf-8")
                )

            self.assertEqual(discovery["selection_source"], "user_review")
            self.assertTrue(discovery["activation"]["present"])
            self.assertTrue(discovery["activation"]["has_user_selection"])
            self.assertEqual(discovery["activation"]["status"], "review_selected")
            self.assertEqual(
                discovery["activation"]["selected_control_keys"], [backlight_key]
            )
            selected_by_key = {entry["key"]: entry for entry in discovery["selected_controls"]}
            excluded_by_key = {entry["key"]: entry for entry in discovery["excluded_controls"]}
            self.assertEqual(selected_by_key[backlight_key]["label"], "Display Backlight")
            self.assertIn(reset_key, excluded_by_key)

    def test_control_discovery_evidence_defaults_to_policy_split_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            overlay = generate_shadow_learning_overlay_drafts(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_review_session_manifest(),
                correlation=_review_correlation_payload(),
            )

            support_bundle = build_support_bundle_payload(
                entry_id="entry-shadow",
                entry_title="Shadow Device",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={"driver_key": "modbus_smg"},
                values={
                    "local_profile_draft_path": str(overlay.profile_path),
                    "local_schema_draft_path": str(overlay.schema_path),
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
                discovery = json.loads(
                    archive.read(
                        "evidence/shadow_learning/control_discovery.json"
                    ).decode("utf-8")
                )

            # Even with no user activation, the package still records the full discovered
            # set and the default enabled/excluded split so developers can review it.
            self.assertIn("evidence/shadow_learning/control_discovery.json", names)
            self.assertEqual(discovery["selection_source"], "policy_default")
            self.assertEqual(discovery["counts"]["discovered"], 2)
            self.assertEqual(discovery["counts"]["selected"], 1)
            self.assertEqual(discovery["counts"]["excluded"], 1)
            self.assertEqual(
                {int(entry["register"]) for entry in discovery["selected_controls"]},
                {705},
            )
            self.assertEqual(
                {int(entry["register"]) for entry in discovery["excluded_controls"]},
                {690},
            )
            self.assertTrue(discovery["excluded_controls"][0]["reasons"])
            self.assertFalse(discovery["activation"]["present"])
            self.assertFalse(discovery["activation"]["has_user_selection"])

    def _write_shadow_trace(
        self,
        config_dir: Path,
        *,
        write_event_kind: str = "shadow_modbus_write_observation",
    ) -> Path:
        trace_root = config_dir / "eybond_local" / "shadow_learning_traces"
        trace_root.mkdir(parents=True, exist_ok=True)
        trace_path = trace_root / "entry_shadow_20260605T100000000000Z.jsonl"
        lines = [
            {
                "kind": "shadow_session_manifest",
                "timestamp": "2026-06-05T10:00:00+00:00",
                "session_id": "entry-shadow_20260605T100000000000Z",
                "entry_id": "entry-shadow",
                "collector_pn": "E5000020000000",
                "cloud_pn": "E50000200000000001",
                "cloud_sn": "E50000200000000001000001",
            },
            {
                "kind": "shadow_connect",
                "timestamp": "2026-06-05T10:00:01+00:00",
                "direction": "cloud_to_shadow",
                "payload": {"remote": "192.168.1.20:5555", "authorization": "Bearer nope"},
            },
            {
                "kind": write_event_kind,
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


def _review_session_manifest() -> dict[str, object]:
    return {
        "session_id": "smg-shadow-session-review",
        "collector_pn": "E5000020000000",
        "cloud_pn": "E50000200000000001",
        "cloud_sn": "E50000200000000001000001",
        "devcode": 2376,
        "devaddr": 1,
        "write_response_mode": "exception",
    }


def _review_correlation_payload() -> dict[str, object]:
    return {
        "matched_count": 3,
        "unmatched_attempt_count": 0,
        "unmatched_write_count": 0,
        "matched": [
            {
                "sequence_index": 0,
                "field_id": "sys_eybond_ctrl_53",
                "field_name": "Backlight Control",
                "requested_value": "0",
                "requested_at": "2026-06-05T12:00:01+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:01.100000+00:00",
                    "function_code": 16,
                    "register": 705,
                    "values": [0],
                    "devcode": 2376,
                    "devaddr": 1,
                },
            },
            {
                "sequence_index": 1,
                "field_id": "sys_eybond_ctrl_53",
                "field_name": "Backlight Control",
                "requested_value": "1",
                "requested_at": "2026-06-05T12:00:02+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:02.100000+00:00",
                    "function_code": 16,
                    "register": 705,
                    "values": [1],
                    "devcode": 2376,
                    "devaddr": 1,
                },
            },
            {
                "sequence_index": 2,
                "field_id": "sys_eybond_ctrl_500",
                "field_name": "Reset user parameters",
                "requested_value": "1",
                "requested_at": "2026-06-05T12:00:04+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:04.100000+00:00",
                    "function_code": 6,
                    "register": 690,
                    "values": [1],
                    "devcode": 2376,
                    "devaddr": 1,
                },
            },
        ],
    }


if __name__ == "__main__":
    unittest.main()
