from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.bundle import (
    build_support_bundle_payload,
    export_support_bundle,
)


def _sample_cloud_evidence() -> dict[str, object]:
    return {
        "evidence_version": 1,
        "source": "smartess_cloud_probe",
        "match": {"entry_id": "entry123", "collector_pn": "E5000025388419"},
        "device_identity": {
            "pn": "E50000253884199645",
            "sn": "E50000253884199645094801",
            "devcode": 2376,
            "devaddr": 1,
        },
        "summary": {"actions": ["device_list", "device_detail"]},
        "payload": {"request": {"command": "device-bundle"}},
    }


def _sample_support_bundle_payload() -> dict[str, object]:
    return build_support_bundle_payload(
        entry_id="entry123",
        entry_title="SMG 6200",
        connected=True,
        collector={"collector_pn": "E5000025388419"},
        inverter={
            "driver_key": "modbus_smg",
            "model_name": "SMG 6200",
            "variant_key": "default",
            "serial_number": "92632511100118",
            "profile_name": "smg_modbus.json",
            "register_schema_name": "modbus_smg/models/smg_6200.json",
        },
        values={"operating_mode": "Off-Grid"},
        data={"server_ip": "192.168.1.50"},
        options={"poll_interval": 10},
        profile_name="smg_modbus.json",
        register_schema_name="modbus_smg/models/smg_6200.json",
        variant_key="default",
        cloud_evidence=_sample_cloud_evidence(),
    )


class SupportBundleTests(unittest.TestCase):
    def test_builds_support_bundle_payload(self) -> None:
        raw = _sample_support_bundle_payload()

        self.assertEqual(raw["entry"]["entry_id"], "entry123")
        self.assertEqual(raw["source_metadata"]["profile_name"], "smg_modbus.json")
        self.assertEqual(raw["source_metadata"]["variant_key"], "default")
        self.assertEqual(raw["runtime"]["values"]["operating_mode"], "Off-Grid")
        self.assertEqual(raw["evidence"]["cloud"]["source"], "smartess_cloud_probe")

    def test_builds_support_bundle_payload_with_smartess_raw_effective_split(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-smartess",
            entry_title="SmartESS 0925",
            connected=True,
            collector={"collector_pn": "E5000025388419"},
            inverter={
                "driver_key": "pi30",
                "model_name": "SmartESS 0925",
                "variant_key": "default",
                "serial_number": "92632511100118",
                "profile_name": "pi30_ascii/models/smartess_0925_compat.json",
                "register_schema_name": "pi30_ascii/models/smartess_0925_compat.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="pi30_ascii/models/smartess_0925_compat.json",
            register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
            variant_key="default",
            effective_owner_key="pi30",
            effective_owner_name="PI30-family runtime",
            smartess_family_name="SmartESS 0925",
            raw_profile_name="smartess_local/models/0925.json",
            raw_register_schema_name="smartess_local/models/0925.json",
            smartess_protocol_asset_id="0925",
            smartess_profile_key="smartess_0925",
        )

        self.assertEqual(raw["source_metadata"]["effective_owner_key"], "pi30")
        self.assertEqual(raw["source_metadata"]["effective_owner_name"], "PI30-family runtime")
        self.assertEqual(raw["source_metadata"]["smartess_family_name"], "SmartESS 0925")
        self.assertEqual(
            raw["source_metadata"]["raw_profile_name"],
            "smartess_local/models/0925.json",
        )
        self.assertEqual(raw["source_metadata"]["smartess_protocol_asset_id"], "0925")

    def test_builds_support_bundle_payload_with_family_fallback_marker(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-fallback",
            entry_title="SMG Family",
            connected=True,
            collector={"collector_pn": "E5000025388419"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG Family",
                "variant_key": "family_fallback",
                "serial_number": "92632511100118",
                "profile_name": "modbus_smg/family_fallback.json",
                "register_schema_name": "modbus_smg/base.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="modbus_smg/family_fallback.json",
            register_schema_name="modbus_smg/base.json",
            variant_key="family_fallback",
        )

        marker = raw["source_metadata"]["support_marker"]
        self.assertEqual(marker["key"], "read_only_unverified_smg_family")
        self.assertEqual(marker["label"], "Read-only unverified SMG family")
        self.assertTrue(marker["read_only"])
        self.assertEqual(marker["verification"], "unverified")

    def test_builds_support_bundle_payload_with_non_fallback_read_only_smg_profile_marker(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-doc-backed",
            entry_title="SMG Candidate",
            connected=True,
            collector={"collector_pn": "E5000025388419"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG Candidate",
                "variant_key": "doc_backed_variant",
                "serial_number": "SMG11K240123",
                "profile_name": "modbus_smg/family_fallback.json",
                "register_schema_name": "modbus_smg/base.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="modbus_smg/family_fallback.json",
            register_schema_name="modbus_smg/base.json",
            variant_key="doc_backed_variant",
        )

        marker = raw["source_metadata"]["support_marker"]
        self.assertEqual(marker["key"], "read_only_unverified_smg_family")
        self.assertEqual(marker["label"], "Read-only unverified SMG family")
        self.assertTrue(marker["read_only"])
        self.assertEqual(marker["verification"], "unverified")

    def test_builds_support_bundle_payload_without_read_only_marker_for_untested_anenji_4200_profile(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-anenji-4200",
            entry_title="Anenji 4200",
            connected=True,
            collector={"collector_pn": "E5000025388419"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "Anenji 4200 (Protocol 1)",
                "variant_key": "anenji_4200_protocol_1",
                "serial_number": "99432409105281",
                "profile_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                "register_schema_name": "modbus_smg/models/anenji_4200_protocol_1.json",
            },
            values={"operating_mode": "Off-Grid"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            register_schema_name="modbus_smg/models/anenji_4200_protocol_1.json",
            variant_key="anenji_4200_protocol_1",
        )

        self.assertIsNone(raw["source_metadata"]["support_marker"])

    def test_exports_support_bundle_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            payload = _sample_support_bundle_payload()

            path = export_support_bundle(
                config_dir=config_dir,
                entry_id="entry123",
                entry_title="SMG 6200",
                connected=bool(payload["runtime"]["connected"]),
                collector=payload["runtime"]["collector"],
                inverter=payload["runtime"]["inverter"],
                values=payload["runtime"]["values"],
                data=payload["entry"]["data"],
                options=payload["entry"]["options"],
                profile_name=payload["source_metadata"]["profile_name"],
                register_schema_name=payload["source_metadata"]["register_schema_name"],
                variant_key=payload["source_metadata"]["variant_key"],
                cloud_evidence=payload["evidence"]["cloud"],
            )

            self.assertEqual(path.suffix, ".json")
            self.assertTrue(path.exists())
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(raw["entry"]["entry_id"], "entry123")
            self.assertEqual(raw["source_metadata"]["profile_name"], "smg_modbus.json")
            self.assertEqual(raw["source_metadata"]["variant_key"], "default")
            self.assertEqual(
                raw["source_metadata"]["register_schema_name"],
                "modbus_smg/models/smg_6200.json",
            )
            self.assertEqual(raw["runtime"]["values"]["operating_mode"], "Off-Grid")
            self.assertEqual(
                raw["evidence"]["cloud"]["device_identity"]["devcode"],
                2376,
            )


if __name__ == "__main__":
    unittest.main()
