"""Options-flow wiring for the explicit experimental-map choice."""

from __future__ import annotations

import sys
from pathlib import Path
import types
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata import (  # noqa: E402
    compiled_detection_catalog as catalog_module,
)


ENTRY_KEY = "lvyuan_ty_sic_6_5kbe_w1"


class ExperimentalCatalogEntryTests(unittest.TestCase):
    """The menu must find the experimental entries without loading a device."""

    def test_exactly_one_experimental_entry_exists(self) -> None:
        catalog = catalog_module.load_compiled_detection_catalog()
        keys = sorted(
            key
            for key, descriptor in catalog.devices.items()
            if descriptor.explicit_selection_only
        )
        self.assertEqual(keys, [ENTRY_KEY])

    def test_explicit_resolution_returns_the_read_only_surface(self) -> None:
        descriptor, surface = (
            catalog_module.resolve_explicitly_selected_model_surface(ENTRY_KEY)
        )
        self.assertEqual(descriptor.key, ENTRY_KEY)
        self.assertEqual(surface.driver_key, "modbus_catalog")
        self.assertEqual(surface.variant_key, ENTRY_KEY)
        self.assertTrue(surface.read_only)

    def test_empty_and_unknown_entries_fail_closed(self) -> None:
        for key in ("", "   ", "not_a_real_entry"):
            with self.subTest(key=key):
                self.assertIsNone(
                    catalog_module.resolve_explicitly_selected_model_surface(key)
                )

    def test_auto_resolution_never_returns_the_experimental_entry(self) -> None:
        """With no explicit selection the resolver must not offer the map."""

        catalog = catalog_module.load_compiled_detection_catalog()
        descriptor = catalog.devices[f"{ENTRY_KEY}"]
        result = catalog.resolution_for_candidates(
            protocol_key=descriptor.protocol_key,
            candidate_keys=(ENTRY_KEY,),
            evidence={},
        )
        self.assertIsNone(result.surface_key)


if __name__ == "__main__":
    unittest.main()
