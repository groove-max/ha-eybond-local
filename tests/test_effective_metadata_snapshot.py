from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
import types
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.effective_metadata_snapshot import (  # noqa: E402
    build_effective_metadata_snapshot,
    build_effective_metadata_snapshot_from_runtime,
    effective_metadata_snapshot_from_dict,
    effective_metadata_snapshot_to_dict,
)
from custom_components.eybond_local.metadata.compiled_detection_catalog import (  # noqa: E402
    load_compiled_detection_catalog,
)


class EffectiveMetadataSnapshotTests(unittest.TestCase):
    def test_build_snapshot_normalizes_live_metadata_like_fields(self) -> None:
        snapshot = build_effective_metadata_snapshot(
            effective_owner_key=" modbus_smg ",
            effective_owner_name=" SMG-family runtime ",
            variant_key=" anenji_anj_11kw_48v_wifi_p ",
            profile_name=" modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json ",
            register_schema_name=" modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json ",
            confidence="HIGH",
            generation=" 7 ",
            generated_at="2026-06-03T18:41:00Z",
        )

        self.assertEqual(snapshot.effective_owner_key, "modbus_smg")
        self.assertEqual(snapshot.effective_owner_name, "SMG-family runtime")
        self.assertEqual(snapshot.variant_key, "anenji_anj_11kw_48v_wifi_p")
        self.assertEqual(
            snapshot.profile_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(
            snapshot.register_schema_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(snapshot.confidence, "high")
        self.assertEqual(snapshot.generation, 7)
        self.assertEqual(snapshot.generated_at, "2026-06-03T18:41:00+00:00")
        self.assertTrue(snapshot.is_valid)

    def test_build_snapshot_from_runtime_objects_prefers_effective_selection_owner(self) -> None:
        inverter = types.SimpleNamespace(
            driver_key="pi30",
            variant_key="default",
            profile_name="pi30_ascii/models/smartess_0925_compat.json",
            register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
        )
        selection = types.SimpleNamespace(
            effective_owner_key="pi30",
            effective_owner_name="PI30-family runtime",
            profile_name="",
            register_schema_name="",
        )

        snapshot = build_effective_metadata_snapshot_from_runtime(
            inverter=inverter,
            selection=selection,
            confidence="medium",
            generation=2,
            generated_at=datetime(2026, 6, 3, 18, 42, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(snapshot.effective_owner_key, "pi30")
        self.assertEqual(snapshot.effective_owner_name, "PI30-family runtime")
        self.assertEqual(snapshot.variant_key, "default")
        self.assertEqual(snapshot.confidence, "medium")
        self.assertEqual(snapshot.generation, 2)
        self.assertEqual(snapshot.generated_at, "2026-06-03T18:42:30+00:00")
        self.assertTrue(snapshot.is_valid)

    def test_deserialize_missing_payload_returns_empty_invalid_snapshot(self) -> None:
        self.assertTrue(effective_metadata_snapshot_from_dict(None).is_empty)
        self.assertFalse(effective_metadata_snapshot_from_dict(None).is_valid)
        self.assertTrue(effective_metadata_snapshot_from_dict({}).is_empty)
        self.assertFalse(effective_metadata_snapshot_from_dict({}).is_valid)

    def test_deserialize_invalid_values_is_safe_and_normalized(self) -> None:
        snapshot = effective_metadata_snapshot_from_dict(
            {
                "effective_owner_key": ["bad"],
                "effective_owner_name": {"bad": "value"},
                "variant_key": True,
                "profile_name": " ",
                "register_schema_name": None,
                "confidence": "definitely",
                "generation": "-5",
                "generated_at": "not-a-time",
            }
        )

        self.assertEqual(snapshot.effective_owner_key, "")
        self.assertEqual(snapshot.effective_owner_name, "")
        self.assertEqual(snapshot.variant_key, "")
        self.assertEqual(snapshot.profile_name, "")
        self.assertEqual(snapshot.register_schema_name, "")
        self.assertEqual(snapshot.confidence, "none")
        self.assertEqual(snapshot.generation, 0)
        self.assertEqual(snapshot.generated_at, "")
        self.assertTrue(snapshot.is_empty)
        self.assertFalse(snapshot.is_valid)

    def test_deserialize_rejects_numeric_scalars_for_text_fields(self) -> None:
        snapshot = effective_metadata_snapshot_from_dict(
            {
                "effective_owner_key": 1,
                "effective_owner_name": 2,
                "variant_key": 3,
                "profile_name": 4,
                "register_schema_name": 5,
                "confidence": "high",
            }
        )

        self.assertEqual(snapshot.effective_owner_key, "")
        self.assertEqual(snapshot.effective_owner_name, "")
        self.assertEqual(snapshot.variant_key, "")
        self.assertEqual(snapshot.profile_name, "")
        self.assertEqual(snapshot.register_schema_name, "")
        self.assertEqual(snapshot.confidence, "high")
        self.assertFalse(snapshot.is_valid)

    def test_deserialize_accepts_legacy_alias_fields(self) -> None:
        snapshot = effective_metadata_snapshot_from_dict(
            {
                "driver_key": "modbus_smg",
                "owner_name": "SMG-family runtime",
                "variant_key": "anenji_4200_protocol_1",
                "profile_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                "schema_name": "modbus_smg/models/anenji_4200_protocol_1.json",
                "detection_confidence": "high",
                "version": "11",
                "timestamp": "2026-06-03T18:50:00Z",
            }
        )

        self.assertEqual(snapshot.effective_owner_key, "modbus_smg")
        self.assertEqual(snapshot.effective_owner_name, "SMG-family runtime")
        self.assertEqual(snapshot.variant_key, "anenji_4200_protocol_1")
        self.assertEqual(
            snapshot.profile_name,
            "modbus_smg/models/anenji_4200_protocol_1.json",
        )
        self.assertEqual(
            snapshot.register_schema_name,
            "modbus_smg/models/anenji_4200_protocol_1.json",
        )
        self.assertEqual(snapshot.confidence, "high")
        self.assertEqual(snapshot.generation, 11)
        self.assertEqual(snapshot.generated_at, "2026-06-03T18:50:00+00:00")
        self.assertTrue(snapshot.is_valid)

    def test_snapshot_round_trips_through_plain_dict(self) -> None:
        original = build_effective_metadata_snapshot(
            effective_owner_key="pi30",
            effective_owner_name="PI30-family runtime",
            variant_key="pi30_max",
            profile_name="pi30_ascii/models/pi30_max.json",
            register_schema_name="pi30_ascii/models/pi30_max.json",
            confidence="high",
            generation=3,
            generated_at="2026-06-03T19:00:00+00:00",
        )

        encoded = effective_metadata_snapshot_to_dict(original)
        restored = effective_metadata_snapshot_from_dict(encoded)

        self.assertEqual(restored, original)
        self.assertEqual(restored.as_dict(), encoded)

    def test_catalog_bound_snapshot_validates_current_surface(self) -> None:
        catalog = load_compiled_detection_catalog()
        snapshot = build_effective_metadata_snapshot(
            effective_owner_key="modbus_smg",
            variant_key="default",
            profile_name="modbus_smg/models/smg_6200.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            confidence="high",
            candidate_keys=("smg_6200",),
            resolution_level="exact",
            surface_key="smg_6200_full",
            evidence_fingerprint="abc",
            catalog_version=catalog.catalog_version,
            descriptor_revisions=(
                f"smg_6200:{catalog.devices['smg_6200'].revision}",
            ),
        )

        self.assertTrue(snapshot.is_valid)
        self.assertEqual(snapshot.candidate_keys, ("smg_6200",))
        self.assertEqual(snapshot.surface_key, "smg_6200_full")

    def test_stale_catalog_bound_snapshot_is_invalid(self) -> None:
        snapshot = build_effective_metadata_snapshot(
            effective_owner_key="modbus_smg",
            variant_key="default",
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
            confidence="high",
            surface_key="smg_6200_full",
            catalog_version="stale",
        )

        self.assertFalse(snapshot.is_valid)


if __name__ == "__main__":
    unittest.main()
