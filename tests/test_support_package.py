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
from custom_components.eybond_local.support.package import export_support_package


class SupportPackageTests(unittest.TestCase):
    def test_exports_support_package_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry123",
                entry_title="SMG 6200",
                connected=True,
                collector={"collector_pn": "E5000025388419"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG 6200",
                    "serial_number": "92632511100118",
                },
                values={"operating_mode": "Off-Grid"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
                cloud_evidence={
                    "evidence_version": 1,
                    "source": "smartess_cloud_probe",
                    "match": {"entry_id": "entry123", "collector_pn": "E5000025388419"},
                    "device_identity": {"pn": "E50000253884199645", "sn": "E500...094801"},
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
            self.assertIsNotNone(result.download_path)
            self.assertEqual(
                result.download_url,
                f"/local/eybond_local/support_packages/{path.name}",
            )
            self.assertTrue(result.download_path.exists())
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
                bundled["evidence"]["cloud"],
                {"archive_member": "evidence/cloud_evidence.json"},
            )
            self.assertEqual(cloud_evidence["source"], "smartess_cloud_probe")
            self.assertIn("payload", cloud_evidence)
            self.assertNotIn("payload", bundled["evidence"]["cloud"])
            self.assertEqual(raw_capture["capture_kind"], "modbus_register_dump")
            self.assertTrue(anonymized_fixture["anonymized"])

    def test_exports_command_based_replay_fixture_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            support_bundle = build_support_bundle_payload(
                entry_id="entry456",
                entry_title="PowMr 4.2kW",
                connected=True,
                collector={"collector_pn": "Q0033482254531"},
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
                collector={"collector_pn": "E5000025388419"},
                inverter={
                    "driver_key": "modbus_smg",
                    "model_name": "SMG Family",
                    "serial_number": "92632511100118",
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
                collector={"collector_pn": "E5000025388419"},
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


if __name__ == "__main__":
    unittest.main()
