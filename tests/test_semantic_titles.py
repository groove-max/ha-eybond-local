from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.semantic_titles_loader import (  # noqa: E402
    clear_semantic_title_catalog_cache,
    load_semantic_title_catalog,
    normalize_title,
    resolve_semantic_title,
)


class SemanticTitleCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_semantic_title_catalog_cache()
        self.addCleanup(clear_semantic_title_catalog_cache)

    def test_catalog_loads_with_entries(self) -> None:
        catalog = load_semantic_title_catalog()
        self.assertTrue(catalog.catalog_version)
        self.assertGreaterEqual(len(catalog.by_normalized_title), 20)

    def test_resolves_canonical_title(self) -> None:
        entry = resolve_semantic_title("Battery Voltage")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.semantic_key, "battery_voltage")
        self.assertEqual(entry.device_class, "voltage")
        self.assertEqual(entry.unit, "V")
        self.assertEqual(entry.state_class, "measurement")

    def test_case_and_spacing_insensitive(self) -> None:
        # The cloud renders "Output frequency"; the catalog stores "Output Frequency".
        entry = resolve_semantic_title("output FREQUENCY")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.semantic_key, "output_frequency")
        self.assertEqual(entry.device_class, "frequency")

    def test_cloud_typo_alias_resolves(self) -> None:
        # The live cloud sends "DC Module Termperature" (sic).
        entry = resolve_semantic_title("DC Module Termperature")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.semantic_key, "dc_module_temperature")
        self.assertEqual(entry.device_class, "temperature")

    def test_percent_entry_has_unit_but_no_device_class(self) -> None:
        entry = resolve_semantic_title("Load Percent")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.unit, "%")
        self.assertEqual(entry.device_class, "")

    def test_unknown_title_returns_none(self) -> None:
        self.assertIsNone(resolve_semantic_title("Quantum Flux Reading"))

    def test_cross_vendor_alias_shared(self) -> None:
        # "Main Output Priority" (Anenji) maps to the same key as "Output priority".
        a = resolve_semantic_title("Output priority")
        b = resolve_semantic_title("Main Output Priority")
        self.assertIsNotNone(a)
        self.assertEqual(a.semantic_key, b.semantic_key)

    def test_normalize_title_helper(self) -> None:
        self.assertEqual(normalize_title("  Battery-Voltage  "), "battery voltage")
        self.assertEqual(normalize_title("PV/Current (A)"), "pv current a")


if __name__ == "__main__":
    unittest.main()
