from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
import zipfile
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.bundle import build_support_bundle_payload
from custom_components.eybond_local.support.download import (
    async_register_support_package_download_view,
    resolve_support_package_download_path,
    sign_support_package_download_url,
    support_package_authenticated_download_url,
)
from custom_components.eybond_local.support.package import (
    _mask_jsonl_text,
    export_support_package,
)


class SupportPackageTests(unittest.TestCase):
    def test_authenticated_support_package_download_url_uses_entry_id(self) -> None:
        self.assertEqual(
            support_package_authenticated_download_url("entry123"),
            "/api/eybond_local/support_package/entry123",
        )

    def test_signed_support_package_download_url_uses_ha_signed_path(self) -> None:
        auth_module = types.ModuleType("homeassistant.components.http.auth")
        auth_module.async_sign_path = (
            lambda _hass, path, _expiration: f"{path}?authSig=signed"
        )

        with patch.dict(sys.modules, {"homeassistant.components.http.auth": auth_module}):
            self.assertEqual(
                sign_support_package_download_url(object(), "entry123"),
                "/api/eybond_local/support_package/entry123?authSig=signed",
            )

    def test_download_view_registration_noops_for_minimal_hass_stub(self) -> None:
        self.assertFalse(
            async_register_support_package_download_view(types.SimpleNamespace())
        )

    def test_exports_support_package_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry123",
                entry_title="SMG 6200",
                connected=True,
                collector={
                    "collector_pn": "E5000020000000",
                    "collector_cloud_profile_key": "smartess_at",
                    "collector_cloud_profile_label": "SmartESS AT",
                    "collector_cloud_profile_source": "smartess_cloud_diagnostics",
                    "collector_cloud_profile_confidence": "high",
                },
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG 6200",
                    "serial_number": "92632500000001",
                },
                values={"operating_mode": "Off-Grid"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
                cloud_evidence={
                    "evidence_version": 1,
                    "source": "smartess_cloud_probe",
                    "match": {"entry_id": "entry123", "collector_pn": "E5000020000000"},
                    "device_identity": {"pn": "E50000200000000001", "sn": "E500...000001"},
                    "summary": {"actions": ["device_list", "device_detail"]},
                    "payload": {"request": {"command": "device-bundle"}},
                },
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry123",
                entry_title="SMG 6200",
                support_bundle=support_bundle,
                raw_capture={
                    "capture_kind": "modbus_register_dump",
                    "captured_ranges": [{"start": 201, "count": 2, "words": [1, 2]}],
                    "range_failures": [],
                },
                fixture={
                    "fixture_version": 1,
                    "name": "modbus_smg_support_capture",
                    "ranges": [{"start": 201, "count": 2, "values": [1, 2]}],
                },
                anonymized_fixture={
                    "fixture_version": 1,
                    "name": "anon_fixture",
                    "ranges": [{"start": 201, "count": 2, "values": [1, 2]}],
                    "anonymized": True,
                },
                profile_source={
                    "name": "smg_modbus.json",
                    "scope": "builtin",
                    "path": "/config/custom_components/eybond_local/profiles/smg_modbus.json",
                },
                register_schema_source={
                    "name": "modbus_smg/models/smg_6200.json",
                    "scope": "builtin",
                    "path": "/config/custom_components/eybond_local/register_schemas/modbus_smg/models/smg_6200.json",
                },
            )
            path = result.path

            self.assertEqual(path.suffix, ".zip")
            self.assertIsNone(result.download_path)
            self.assertIsNone(result.download_url)
            self.assertFalse((config_dir / "www").exists())
            with zipfile.ZipFile(path) as archive:
                names = set(archive.namelist())
                self.assertIn("manifest.json", names)
                self.assertIn("support_bundle.json", names)
                self.assertIn("raw_capture.json", names)
                self.assertIn("evidence/cloud_evidence.json", names)
                self.assertIn("fixture/raw_fixture.json", names)
                self.assertIn("fixture/anonymized_fixture.json", names)
                self.assertIn("README.txt", names)

                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                bundled = json.loads(archive.read("support_bundle.json").decode("utf-8"))
                cloud_evidence = json.loads(
                    archive.read("evidence/cloud_evidence.json").decode("utf-8")
                )
                raw_capture = json.loads(archive.read("raw_capture.json").decode("utf-8"))
                anonymized_fixture = json.loads(
                    archive.read("fixture/anonymized_fixture.json").decode("utf-8")
                )
                readme = archive.read("README.txt").decode("utf-8")

            self.assertEqual(manifest["entry"]["entry_id"], "entry123")
            self.assertEqual(manifest["archive_version"], 2)
            self.assertEqual(
                manifest["archive_members"]["cloud_evidence"],
                "evidence/cloud_evidence.json",
            )
            self.assertEqual(
                manifest["effective_metadata"]["profile_source"]["scope"],
                "builtin",
            )
            self.assertEqual(bundled["entry"]["entry_id"], "entry123")
            self.assertEqual(
                bundled["runtime"]["collector"]["collector_cloud_profile_key"],
                "smartess_at",
            )
            self.assertEqual(
                bundled["runtime"]["collector"]["collector_cloud_profile_label"],
                "SmartESS AT",
            )
            self.assertEqual(
                bundled["runtime"]["collector"]["collector_cloud_profile_source"],
                "smartess_cloud_diagnostics",
            )
            self.assertEqual(
                bundled["runtime"]["collector"]["collector_cloud_profile_confidence"],
                "high",
            )
            self.assertEqual(
                bundled["evidence"]["cloud"],
                {"archive_member": "evidence/cloud_evidence.json"},
            )
            self.assertEqual(cloud_evidence["source"], "smartess_cloud_probe")
            self.assertIn("payload", cloud_evidence)
            self.assertNotIn("payload", bundled["evidence"]["cloud"])
            self.assertEqual(raw_capture["capture_kind"], "modbus_register_dump")
            self.assertTrue(anonymized_fixture["anonymized"])
            self.assertIn("collector, inverter, and integration role sections", readme)

    def test_archive_members_mask_long_numeric_identifiers(self) -> None:
        # Field report: the PN was starred out in some archive members and
        # printed in full in others. Every member must use one masking rule.
        pn = "E50000200000000001"
        masked_pn = "E5000*********0001"
        serial = "92632500000001"
        frame_hex = ("AT+DTUPN:" + pn).encode("ascii").hex()
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry123",
                entry_title="Bridge",
                connected=True,
                collector={"collector_pn": pn},
                inverter={"driver_key": "modbus_smg", "serial_number": serial},
                values={"collector_pn": pn},
                data={"collector_pn": pn, "server_ip": "192.168.1.50"},
                options={},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )
            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry123",
                entry_title="Bridge",
                support_bundle=support_bundle,
                raw_capture={
                    "capture_kind": "modbus_register_dump",
                    "note": f"observed pn {pn}",
                    "frames": [frame_hex],
                },
                fixture={"fixture_version": 1, "identity": pn},
                anonymized_fixture=None,
            )
            with zipfile.ZipFile(result.path) as archive:
                for name in archive.namelist():
                    text = archive.read(name).decode("utf-8")
                    self.assertNotIn(pn, text, f"unmasked PN in {name}")
                    self.assertNotIn(serial, text, f"unmasked serial in {name}")
                    self.assertNotIn(
                        frame_hex, text, f"unmasked PN-bearing frame in {name}"
                    )
                bundled = json.loads(
                    archive.read("support_bundle.json").decode("utf-8")
                )
                raw_capture = json.loads(
                    archive.read("raw_capture.json").decode("utf-8")
                )

        self.assertEqual(bundled["runtime"]["collector"]["collector_pn"], masked_pn)
        self.assertEqual(bundled["entry"]["data"]["collector_pn"], masked_pn)
        self.assertEqual(raw_capture["note"], f"observed pn {masked_pn}")
        # The hex frame stays hex, with the embedded ASCII identifier masked.
        decoded_frame = bytes.fromhex(raw_capture["frames"][0]).decode("ascii")
        self.assertEqual(decoded_frame, "AT+DTUPN:" + masked_pn)

    def test_manifest_recommended_artifact_survives_masking(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry123",
                entry_title="Bridge",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter=None,
                values={},
                data={},
                options={},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )
            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry123",
                entry_title="Bridge",
                support_bundle=support_bundle,
                raw_capture=None,
                fixture=None,
                anonymized_fixture=None,
            )
            with zipfile.ZipFile(result.path) as archive:
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))

        # The archive filename carries a compact timestamp the identifier
        # mask would star out; the manifest must reference the real file.
        self.assertEqual(
            manifest["sharing_guidance"]["recommended_artifact"], result.path.name
        )

    def test_masking_covers_dict_keys(self) -> None:
        from custom_components.eybond_local.support.masking import (
            mask_numeric_identifiers,
        )

        masked = mask_numeric_identifiers({"E50000200000000001": {"nested": "ok"}})
        self.assertEqual(list(masked), ["E5000*********0001"])
        self.assertEqual(masked["E5000*********0001"], {"nested": "ok"})

    def test_mask_jsonl_text_masks_strings_but_keeps_numbers(self) -> None:
        lines = (
            '{"ts": 1751600000.123, "pn": "E50000200000000001", "register": 12345678901}\n'
            "not-json 92632500000001\n"
        )

        masked = _mask_jsonl_text(lines)

        first, second = masked.splitlines()
        record = json.loads(first)
        # Numeric fields survive untouched — only strings are masked.
        self.assertEqual(record["ts"], 1751600000.123)
        self.assertEqual(record["register"], 12345678901)
        self.assertEqual(record["pn"], "E5000*********0001")
        self.assertEqual(second, "not-json 9263******0001")
        self.assertTrue(masked.endswith("\n"))

    def test_resolves_authenticated_download_path_for_latest_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_root = config_dir / "eybond_local" / "support_packages"
            support_root.mkdir(parents=True)
            archive_path = support_root / "entry123_20260628T000000000000Z.zip"
            archive_path.write_bytes(b"zip")
            coordinator = types.SimpleNamespace(
                data=types.SimpleNamespace(
                    values={"support_package_path": str(archive_path)}
                )
            )

            self.assertEqual(
                resolve_support_package_download_path(
                    config_dir=config_dir,
                    entry_id="entry123",
                    coordinator=coordinator,
                ),
                archive_path.resolve(),
            )

    def test_rejects_unsafe_authenticated_download_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_root = config_dir / "eybond_local" / "support_packages"
            support_root.mkdir(parents=True)
            other_entry = support_root / "other_20260628T000000000000Z.zip"
            other_entry.write_bytes(b"zip")
            outside = config_dir / "outside.zip"
            outside.write_bytes(b"zip")
            not_zip = support_root / "entry123_20260628T000000000000Z.txt"
            not_zip.write_text("not zip", encoding="utf-8")

            for candidate in (other_entry, outside, not_zip):
                coordinator = types.SimpleNamespace(
                    data=types.SimpleNamespace(
                        values={"support_package_path": str(candidate)}
                    )
                )
                self.assertIsNone(
                    resolve_support_package_download_path(
                        config_dir=config_dir,
                        entry_id="entry123",
                        coordinator=coordinator,
                    )
                )

    def test_explicit_publish_download_copy_exposes_support_package(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry-publish",
                entry_title="SMG 6200",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG 6200",
                    "serial_number": "92632500000001",
                },
                values={},
                data={},
                options={},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-publish",
                entry_title="SMG 6200",
                support_bundle=support_bundle,
                raw_capture={"capture_kind": "modbus_register_dump"},
                fixture=None,
                anonymized_fixture=None,
                publish_download_copy=True,
            )

            self.assertIsNotNone(result.download_path)
            self.assertEqual(
                result.download_url,
                f"/local/eybond_local/support_packages/{result.path.name}",
            )
            self.assertTrue(result.download_path.exists())

    def test_exports_command_based_replay_fixture_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry456",
                entry_title="PowMr 4.2kW",
                connected=True,
                collector={"collector_pn": "Q0000000000001"},
                inverter={
                    "driver_key": "pi30",
                    "model_name": "PowMr 4.2kW",
                    "serial_number": "55355535553555",
                },
                values={"operating_mode": "Line"},
                data={"server_ip": "192.168.1.50", "driver_hint": "pi30"},
                options={},
                profile_name="pi30_ascii/models/pi30_max.json",
                register_schema_name="pi30_ascii/models/pi30_max.json",
                variant_key="pi30_max",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry456",
                entry_title="PowMr 4.2kW",
                support_bundle=support_bundle,
                raw_capture={
                    "capture_kind": "pi30_ascii_dump",
                    "responses": {"QPI": "PI30", "QID": "55355535553555", "QET": "NAK"},
                    "failures": {},
                },
                fixture={
                    "fixture_version": 1,
                    "name": "pi30_support_capture",
                    "probe_target": {"devcode": 2452, "collector_addr": 1, "device_addr": 0},
                    "command_responses": {"QPI": "PI30", "QID": "55355535553555", "QET": "NAK"},
                },
                anonymized_fixture={
                    "fixture_version": 1,
                    "name": "anon_fixture",
                    "probe_target": {"devcode": 2452, "collector_addr": 1, "device_addr": 0},
                    "command_responses": {"QPI": "PI30", "QID": "11111111111111", "QET": "NAK"},
                    "anonymized": True,
                },
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())
                raw_fixture = json.loads(archive.read("fixture/raw_fixture.json").decode("utf-8"))
                anonymized_fixture = json.loads(
                    archive.read("fixture/anonymized_fixture.json").decode("utf-8")
                )
                bundled = json.loads(archive.read("support_bundle.json").decode("utf-8"))

            self.assertEqual(raw_fixture["command_responses"]["QPI"], "PI30")
            self.assertEqual(raw_fixture["command_responses"]["QET"], "NAK")
            self.assertNotEqual(
                anonymized_fixture["command_responses"]["QID"],
                raw_fixture["command_responses"]["QID"],
            )
            self.assertTrue(anonymized_fixture["anonymized"])
            self.assertNotIn("evidence/cloud_evidence.json", names)
            self.assertIsNone(bundled["evidence"]["cloud"])

    def test_exports_family_fallback_archive_with_explicit_support_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry-fallback",
                entry_title="SMG Family",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG Family",
                    "serial_number": "92632500000001",
                    "variant_key": "family_fallback",
                },
                values={"operating_mode": "Off-Grid"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="modbus_smg/family_fallback.json",
                register_schema_name="modbus_smg/base.json",
                variant_key="family_fallback",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-fallback",
                entry_title="SMG Family",
                support_bundle=support_bundle,
                raw_capture={
                    "capture_kind": "modbus_register_dump",
                    "captured_ranges": [{"start": 201, "count": 2, "words": [1, 2]}],
                    "range_failures": [],
                },
                fixture={
                    "fixture_version": 1,
                    "name": "smg_family_fallback_capture",
                    "ranges": [{"start": 201, "count": 2, "values": [1, 2]}],
                },
                anonymized_fixture={
                    "fixture_version": 1,
                    "name": "smg_family_fallback_capture_anon",
                    "ranges": [{"start": 201, "count": 2, "values": [1, 2]}],
                    "anonymized": True,
                },
            )

            with zipfile.ZipFile(result.path) as archive:
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                readme = archive.read("README.txt").decode("utf-8")
                bundled = json.loads(archive.read("support_bundle.json").decode("utf-8"))

            self.assertEqual(
                manifest["support_marker"]["key"],
                "read_only_unverified_smg_family",
            )
            self.assertIn("Read-only unverified SMG family", readme)
            self.assertIn("Built-in writes are intentionally disabled", readme)
            self.assertEqual(
                bundled["source_metadata"]["support_marker"]["key"],
                "read_only_unverified_smg_family",
            )

    def test_exports_read_only_profile_archive_with_explicit_support_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry-anenji-4200",
                entry_title="Anenji 4200",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "Anenji 4200 (Protocol 1)",
                    "serial_number": "99432409105281",
                    "variant_key": "anenji_4200_protocol_1",
                },
                values={"operating_mode": "Off-Grid"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
                register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
                variant_key="anenji_4200_protocol_1",
            )
            support_bundle["source_metadata"].pop("support_marker", None)

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-anenji-4200",
                entry_title="Anenji 4200",
                support_bundle=support_bundle,
                raw_capture={
                    "capture_kind": "modbus_register_dump",
                    "captured_ranges": [{"start": 201, "count": 2, "words": [1, 2]}],
                    "range_failures": [],
                },
                fixture={
                    "fixture_version": 1,
                    "name": "anenji_4200_protocol_1_capture",
                    "ranges": [{"start": 201, "count": 2, "values": [1, 2]}],
                },
                anonymized_fixture={
                    "fixture_version": 1,
                    "name": "anenji_4200_protocol_1_capture_anon",
                    "ranges": [{"start": 201, "count": 2, "values": [1, 2]}],
                    "anonymized": True,
                },
            )

            with zipfile.ZipFile(result.path) as archive:
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                readme = archive.read("README.txt").decode("utf-8")

            self.assertIsNone(manifest["support_marker"])
            self.assertNotIn("Read-only unverified SMG family", readme)
            self.assertNotIn("Built-in writes are intentionally disabled", readme)

    def test_archive_manifest_does_not_reference_missing_fixtures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry-eybond-g-ascii",
                entry_title="EyeBond G-ASCII",
                connected=True,
                collector={"collector_pn": "A0000000000001"},
                inverter={
                    "driver_key": "eybond_g_ascii",
                    "model_name": "EyeBond G-ASCII inverter",
                    "serial_number": "A0000000000001",
                    "variant_key": "g_ascii_family",
                },
                values={"protocol_id": "EYBOND_G_ASCII"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="",
                register_schema_name="",
                variant_key="g_ascii_family",
                effective_owner_key="eybond_g_ascii",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-eybond-g-ascii",
                entry_title="EyeBond G-ASCII",
                support_bundle=support_bundle,
                raw_capture={},
                fixture=None,
                anonymized_fixture=None,
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                readme = archive.read("README.txt").decode("utf-8")

            self.assertNotIn("fixture/raw_fixture.json", names)
            self.assertNotIn("fixture/anonymized_fixture.json", names)
            self.assertIsNone(manifest["support_marker"])
            self.assertIsNone(manifest["archive_members"]["raw_fixture"])
            self.assertIsNone(manifest["archive_members"]["anonymized_fixture"])
            self.assertNotIn("Read-only unverified SMG family", readme)
            self.assertNotIn("fixture/raw_fixture.json", readme)
            self.assertNotIn("fixture/anonymized_fixture.json", readme)

    def test_shadow_learning_trace_must_match_current_entry_or_collector(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            trace_root = config_dir / "eybond_local" / "shadow_learning_traces"
            trace_root.mkdir(parents=True)
            stale_trace = trace_root / "other_20260619T201327774513Z.jsonl"
            stale_trace.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "kind": "shadow_session_manifest",
                                "entry_id": "other-entry",
                                "collector_pn": "E5000020000000",
                                "session_id": "other-session",
                            }
                        ),
                        json.dumps(
                            {
                                "kind": "shadow_modbus_write_observation",
                                "payload": {"register": 300, "values": [1]},
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            support_bundle = build_support_bundle_payload(
                entry_id="entry-current",
                entry_title="SmartESS 0925",
                connected=True,
                collector={"collector_pn": "Q0000000000001"},
                inverter={
                    "driver_key": "smartess_local",
                    "model_name": "PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)",
                    "serial_number": "",
                },
                values={"shadow_learning_trace_path": str(stale_trace)},
                data={"collector_pn": "Q0000000000001"},
                options={},
                profile_name="smartess_local/models/0925.json",
                register_schema_name="smartess_local/models/0925.json",
                variant_key="smartess_0925",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-current",
                entry_title="SmartESS 0925",
                support_bundle=support_bundle,
                raw_capture={},
                fixture=None,
                anonymized_fixture=None,
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))

            self.assertNotIn("evidence/shadow_learning/trace.jsonl", names)
            self.assertIsNone(manifest["archive_members"]["shadow_learning"])

    def test_shadow_learning_trace_matching_current_entry_is_included(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            trace_root = config_dir / "eybond_local" / "shadow_learning_traces"
            trace_root.mkdir(parents=True)
            trace = trace_root / "entry_current_20260628T201327774513Z.jsonl"
            trace.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "kind": "shadow_session_manifest",
                                "entry_id": "entry-current",
                                "collector_pn": "Q0000000000001",
                                "session_id": "entry-current-session",
                            }
                        ),
                        json.dumps(
                            {
                                "kind": "shadow_modbus_write_observation",
                                "payload": {"register": 4536, "values": [1]},
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            support_bundle = build_support_bundle_payload(
                entry_id="entry-current",
                entry_title="SmartESS 0925",
                connected=True,
                collector={"collector_pn": "Q0000000000001"},
                inverter={
                    "driver_key": "smartess_local",
                    "model_name": "PowMr 4.2kW / VMII-NXPW5KW (SmartESS 0925)",
                    "serial_number": "",
                },
                values={},
                data={"collector_pn": "Q0000000000001"},
                options={},
                profile_name="smartess_local/models/0925.json",
                register_schema_name="smartess_local/models/0925.json",
                variant_key="smartess_0925",
            )

            result = export_support_package(
                config_dir=config_dir,
                entry_id="entry-current",
                entry_title="SmartESS 0925",
                support_bundle=support_bundle,
                raw_capture={},
                fixture=None,
                anonymized_fixture=None,
            )

            with zipfile.ZipFile(result.path) as archive:
                names = set(archive.namelist())
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))

            self.assertIn("evidence/shadow_learning/trace.jsonl", names)
            self.assertEqual(
                manifest["archive_members"]["shadow_learning"]["trace"],
                "evidence/shadow_learning/trace.jsonl",
            )


if __name__ == "__main__":
    unittest.main()
