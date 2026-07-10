from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.smartess_semantic_catalog_loader import (  # noqa: E402
    clear_smartess_semantic_catalog_cache,
    load_smartess_semantic_catalog,
    resolve_smartess_cloud_classification,
    resolve_smartess_cloud_entry,
    resolve_smartess_semantic_entry,
)


class SmartEssSemanticCatalogLoaderTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_smartess_semantic_catalog_cache()

    def test_loads_known_semantic_entries(self) -> None:
        catalog = load_smartess_semantic_catalog()

        self.assertEqual(catalog.catalog_version, 1)
        self.assertIn("not local Modbus register ids", catalog.cloud_id_semantics)
        self.assertIn("output_priority", catalog.entries)
        self.assertIn("equalization_activated_immediately", catalog.entries)

        output_priority = catalog.entries["output_priority"]
        self.assertEqual(output_priority.canonical_title, "Output Priority")
        self.assertEqual(output_priority.smartess_cloud.bucket, "exact_0925")
        self.assertEqual(output_priority.smartess_cloud.source, "root_map_0925")
        self.assertIn("Direct 0925 root-map match", output_priority.smartess_cloud.reason)
        self.assertEqual(output_priority.smartess_cloud.asset_registers, (4537,))
        self.assertEqual(output_priority.smartess_cloud.profile_keys, ("output_source_priority",))
        self.assertEqual(output_priority.smg_bridge.measurement_keys, ("output_source_priority",))

    def test_resolves_catalog_backed_cloud_classification_metadata(self) -> None:
        exact = resolve_smartess_cloud_classification("Output Priority")
        probable = resolve_smartess_cloud_classification("Output Mode")
        unknown = resolve_smartess_cloud_classification("Not A Real SmartESS Field")

        self.assertEqual(exact["bucket"], "exact_0925")
        self.assertEqual(exact["source"], "root_map_0925")
        self.assertEqual(exact["asset_register"], 4537)
        self.assertEqual(probable["bucket"], "probable_0925")
        self.assertEqual(probable["source"], "pi30_family_surface")
        self.assertNotIn("asset_register", probable)
        self.assertEqual(unknown["bucket"], "cloud_only")
        self.assertEqual(unknown["source"], "cloud_payload_only")
        self.assertNotIn("asset_register", unknown)

    def test_resolves_by_alias_and_preserves_consumer_specific_bindings(self) -> None:
        entry = resolve_smartess_semantic_entry("Equalization Activated Immediately")

        assert entry is not None
        self.assertEqual(entry.semantic_key, "equalization_activated_immediately")
        self.assertEqual(entry.smartess_cloud.profile_keys, ("force_battery_equalization",))
        self.assertEqual(entry.smg_bridge.profile_keys, ("force_eq_charge",))

    def test_cloud_resolution_uses_cloud_specific_aliases_only(self) -> None:
        cloud_entry = resolve_smartess_cloud_entry("Main Output Priority")
        bridge_only_entry = resolve_smartess_semantic_entry("LCD Backlight")

        assert cloud_entry is not None
        assert bridge_only_entry is not None
        self.assertEqual(cloud_entry.semantic_key, "output_priority")
        self.assertEqual(bridge_only_entry.semantic_key, "backlight_control")
        self.assertIsNone(resolve_smartess_cloud_entry("LCD Backlight"))

        bridge_only_classification = resolve_smartess_cloud_classification("LCD Backlight")
        self.assertEqual(bridge_only_classification["bucket"], "cloud_only")
        self.assertEqual(bridge_only_classification["source"], "cloud_payload_only")

    def test_resolves_normalized_titles_case_insensitively(self) -> None:
        entry = resolve_smartess_semantic_entry("main_output_priority")

        assert entry is not None
        self.assertEqual(entry.semantic_key, "output_priority")
        self.assertEqual(entry.all_titles, ("Output Priority", "Main Output Priority"))

    def test_cloud_id_fields_are_string_typed_and_asset_registers_are_int_typed(self) -> None:
        catalog = load_smartess_semantic_catalog()

        for entry in catalog.entries.values():
            self.assertTrue(all(isinstance(item, str) for item in entry.smartess_cloud.cloud_field_ids))
            self.assertTrue(all(isinstance(item, int) for item in entry.smartess_cloud.asset_registers))
            if entry.smartess_cloud.asset_registers:
                self.assertTrue(entry.smartess_cloud.bucket)
                self.assertTrue(entry.smartess_cloud.source)
                self.assertTrue(entry.smartess_cloud.reason)

    def test_validation_rejects_duplicate_aliases(self) -> None:
        clear_smartess_semantic_catalog_cache()
        invalid_catalog = {
            "catalog_version": 1,
            "description": "duplicate alias",
            "cloud_id_semantics": "Cloud field ids are cloud ids, not local Modbus register ids.",
            "entries": [
                {
                    "semantic_key": "first",
                    "canonical_title": "Shared Title",
                },
                {
                    "semantic_key": "second",
                    "canonical_title": "Other Title",
                    "title_aliases": ["Shared Title"],
                },
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smartess_semantic_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "duplicate_alias"):
                load_smartess_semantic_catalog()

    def test_validation_requires_cloud_id_modbus_distinction(self) -> None:
        clear_smartess_semantic_catalog_cache()
        invalid_catalog = {
            "catalog_version": 1,
            "description": "missing distinction",
            "cloud_id_semantics": "Cloud field ids are identifiers.",
            "entries": [
                {
                    "semantic_key": "output_priority",
                    "canonical_title": "Output Priority",
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smartess_semantic_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "missing_cloud_id_modbus_distinction"):
                load_smartess_semantic_catalog()

    def test_validation_rejects_cloud_asset_register_without_classification(self) -> None:
        clear_smartess_semantic_catalog_cache()
        invalid_catalog = {
            "catalog_version": 1,
            "description": "missing classification",
            "cloud_id_semantics": "Cloud field ids are cloud ids, not local Modbus register ids.",
            "entries": [
                {
                    "semantic_key": "battery_type",
                    "canonical_title": "Battery Type",
                    "smartess_cloud": {"asset_registers": [4539]},
                }
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smartess_semantic_catalog_loader.json.loads",
            return_value=invalid_catalog,
        ):
            with self.assertRaisesRegex(ValueError, "missing_bucket"):
                load_smartess_semantic_catalog()


if __name__ == "__main__":
    unittest.main()
