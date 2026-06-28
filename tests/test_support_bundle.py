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
        "match": {"entry_id": "entry123", "collector_pn": "E5000020000000"},
        "device_identity": {
            "pn": "E50000200000000001",
            "sn": "E50000200000000001000001",
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
        collector={"collector_pn": "E5000020000000"},
        inverter={
            "driver_key": "modbus_smg",
            "model_name": "SMG 6200",
            "variant_key": "default",
            "serial_number": "92632500000001",
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
        self.assertEqual(raw["roles"]["collector"]["identity"]["collector_pn"], "E5000020000000")
        self.assertEqual(raw["roles"]["inverter"]["identity"]["model_name"], "SMG 6200")
        self.assertEqual(raw["roles"]["inverter"]["values"]["operating_mode"], "Off-Grid")
        self.assertEqual(raw["evidence"]["cloud"]["source"], "smartess_cloud_probe")

    def test_builds_support_bundle_payload_with_role_value_split(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="Collector PN E5000020000000",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={"driver_key": "modbus_smg", "model_name": "SMG 6200"},
            values={
                "collector_signal_strength": -67,
                "smartess_protocol_asset_id": "0925",
                "integration_build_git_describe": "v0.2.0-beta.2-75-gabcdef0",
                "runtime_reconnect_count": 1,
                "last_error": "",
                "operating_mode": "Off-Grid",
            },
            data={"collector_ip": "192.168.1.55", "collector_pn": "E5000020000000"},
            options={"collector_operation_mode": "smartess_cloud_home_assistant"},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )

        self.assertIn("collector_signal_strength", raw["roles"]["collector"]["values"])
        self.assertIn("smartess_protocol_asset_id", raw["roles"]["collector"]["values"])
        self.assertIn("integration_build_git_describe", raw["roles"]["integration"]["values"])
        self.assertIn("runtime_reconnect_count", raw["roles"]["integration"]["values"])
        self.assertIn("last_error", raw["roles"]["integration"]["values"])
        self.assertIn("operating_mode", raw["roles"]["inverter"]["values"])

    def test_builds_support_bundle_payload_with_descriptor_decision_shadow_evidence(self) -> None:
        descriptor_decision_shadow = {
            "kind": "descriptor_decision_shadow",
            "agreement": "match",
            "evaluation": {"status": "resolved", "resolved_key": "smg_6200"},
        }

        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG 6200",
                "details": {
                    "device_catalog": {
                        "descriptor_decision": descriptor_decision_shadow,
                    },
                },
            },
            values={"operating_mode": "Off-Grid"},
            data={},
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            variant_key="default",
        )

        self.assertEqual(
            raw["evidence"]["descriptor_decision_shadow"],
            descriptor_decision_shadow,
        )

    def test_builds_support_bundle_with_canonical_catalog_detection(self) -> None:
        catalog_detection = {
            "resolution": "exact",
            "candidate_keys": ["smg_6200"],
            "surface_key": "smg_6200_full",
            "catalog_version": "2026.06.2",
        }

        raw = build_support_bundle_payload(
            entry_id="entry123",
            entry_title="SMG 6200",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG 6200",
                "details": {
                    "device_catalog": {
                        "compiled_resolution": catalog_detection,
                    },
                },
            },
            values={},
            data={},
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            variant_key="default",
        )

        self.assertEqual(raw["evidence"]["catalog_detection"], catalog_detection)

    def test_builds_support_bundle_payload_with_smartess_raw_effective_split(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-smartess",
            entry_title="SmartESS 0925",
            connected=True,
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "pi30",
                "model_name": "SmartESS 0925",
                "variant_key": "default",
                "serial_number": "92632500000001",
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
            collector={"collector_pn": "E5000020000000"},
            inverter={
                "driver_key": "modbus_smg",
                "model_name": "SMG Family",
                "variant_key": "family_fallback",
                "serial_number": "92632500000001",
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
            collector={"collector_pn": "E5000020000000"},
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
            collector={"collector_pn": "E5000020000000"},
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

    def test_builds_support_bundle_payload_without_smg_marker_for_eybond_g_ascii_family(self) -> None:
        raw = build_support_bundle_payload(
            entry_id="entry-eybond-g-ascii",
            entry_title="EyeBond G-ASCII",
            connected=True,
            collector={"collector_pn": "A0000000000001"},
            inverter={
                "driver_key": "eybond_g_ascii",
                "model_name": "EyeBond G-ASCII inverter",
                "variant_key": "g_ascii_family",
                "serial_number": "A0000000000001",
            },
            values={"protocol_id": "EYBOND_G_ASCII"},
            data={"server_ip": "192.168.1.50"},
            options={"poll_interval": 10},
            profile_name="",
            register_schema_name="",
            variant_key="g_ascii_family",
            effective_owner_key="eybond_g_ascii",
        )

        self.assertIsNone(raw["source_metadata"]["support_marker"])

    def test_export_support_bundle_writes_json_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = export_support_bundle(
                config_dir=Path(temp_dir),
                entry_id="entry123",
                entry_title="SMG 6200",
                connected=True,
                collector={"collector_pn": "E5000020000000"},
                inverter={"model_name": "SMG 6200"},
                values={"operating_mode": "Off-Grid"},
                data={"server_ip": "192.168.1.50"},
                options={"poll_interval": 10},
                profile_name="smg_modbus.json",
                register_schema_name="modbus_smg/models/smg_6200.json",
                variant_key="default",
                cloud_evidence=_sample_cloud_evidence(),
            )

            self.assertTrue(path.exists())
            exported = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(exported["entry"]["entry_id"], "entry123")
            self.assertEqual(exported["runtime"]["values"]["operating_mode"], "Off-Grid")
            self.assertEqual(exported["evidence"]["cloud"]["source"], "smartess_cloud_probe")
if __name__ == "__main__":
    unittest.main()
