from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.cloud_evidence import (
    build_cloud_evidence_payload,
    export_cloud_evidence,
    fetch_and_export_smartess_device_bundle_cloud_evidence,
    load_latest_cloud_evidence,
)


class CloudEvidenceTests(unittest.TestCase):
    def test_exports_and_loads_latest_matching_cloud_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            older = build_cloud_evidence_payload(
                source="smartess_cloud_probe",
                payload={"request": {"command": "device-bundle", "older": True}},
                entry_id="entry123",
                collector_pn="E5000025388419",
                pn="E50000253884199645",
                sn="E50000253884199645094801",
                devcode=2376,
                devaddr=1,
            )
            newer = build_cloud_evidence_payload(
                source="smartess_cloud_probe",
                payload={"request": {"command": "device-bundle", "older": False}},
                entry_id="entry123",
                collector_pn="E5000025388419",
                pn="E50000253884199645",
                sn="E50000253884199645094801",
                devcode=2376,
                devaddr=1,
            )

            export_cloud_evidence(config_dir=config_dir, evidence=older)
            latest_path = export_cloud_evidence(config_dir=config_dir, evidence=newer)

            record = load_latest_cloud_evidence(
                config_dir,
                entry_id="entry123",
                collector_pn="E5000025388419",
            )

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.path, latest_path)
            self.assertFalse(record.payload["payload"]["request"]["older"])

    def test_returns_none_when_no_matching_cloud_evidence_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            evidence = build_cloud_evidence_payload(
                source="smartess_cloud_probe",
                payload={"request": {"command": "device-bundle"}},
                entry_id="other-entry",
                collector_pn="Q0033482254531",
                pn="Q0033482254531",
                sn="Q00334822545310001",
                devcode=258,
                devaddr=1,
            )
            export_cloud_evidence(config_dir=config_dir, evidence=evidence)

            record = load_latest_cloud_evidence(
                config_dir,
                entry_id="entry123",
                collector_pn="E5000025388419",
            )

            self.assertIsNone(record)

    def test_skips_non_utf8_cloud_evidence_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            root = config_dir / "eybond_local" / "cloud_evidence"
            root.mkdir(parents=True, exist_ok=True)
            (root / "bad_latest.json").write_bytes(b"\xff\xfe\x00\x00")

            evidence = build_cloud_evidence_payload(
                source="smartess_cloud_probe",
                payload={"request": {"command": "device-bundle"}},
                entry_id="entry123",
                collector_pn="E5000025388419",
                pn="E50000253884199645",
                sn="E50000253884199645094801",
                devcode=2376,
                devaddr=1,
            )
            valid_path = export_cloud_evidence(config_dir=config_dir, evidence=evidence)

            record = load_latest_cloud_evidence(
                config_dir,
                entry_id="entry123",
                collector_pn="E5000025388419",
            )

            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.path, valid_path)

    def test_fetch_and_export_smartess_device_bundle_cloud_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            bundle_payload = {
                "request": {
                    "command": "device-bundle",
                    "params": {
                        "pn": "E50000253884199645",
                        "sn": "E50000253884199645094801",
                        "devcode": 2376,
                        "devaddr": 1,
                    },
                },
                "responses": {
                    "device_list": {},
                    "device_detail": {},
                    "device_settings": {},
                    "energy_flow": {},
                },
                "normalized": {
                    "device_list": {"device_count": 1},
                    "device_detail": {"section_counts": {"bc_": 1, "pv_": 1}},
                    "device_settings": {
                        "field_count": 5,
                        "mapped_field_count": 3,
                        "exact_0925_field_count": 3,
                        "probable_0925_field_count": 1,
                        "cloud_only_field_count": 1,
                        "current_values_included": False,
                        "write_action": "ctrlDevice",
                    },
                },
            }

            with patch(
                "custom_components.eybond_local.support.cloud_evidence.fetch_device_bundle_for_collector",
                return_value=bundle_payload,
            ):
                record = fetch_and_export_smartess_device_bundle_cloud_evidence(
                    config_dir=config_dir,
                    username="groove",
                    password="secret",
                    collector_pn="E5000025388419",
                    source="smartess_cloud_onboarding",
                )

            self.assertTrue(record.path.exists())
            self.assertEqual(record.payload["match"]["collector_pn"], "E5000025388419")
            self.assertEqual(record.payload["summary"]["settings_write_action"], "ctrlDevice")


if __name__ == "__main__":
    unittest.main()