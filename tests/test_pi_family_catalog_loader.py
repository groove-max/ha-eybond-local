from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.pi_family_catalog_loader import load_pi_family_catalog


class PiFamilyCatalogLoaderTests(unittest.TestCase):
    def test_loads_protocol_family_codes(self) -> None:
        catalog = load_pi_family_catalog()

        self.assertEqual(catalog.protocol_families["PI16"], "pi16")
        self.assertEqual(catalog.protocol_families["PI18"], "pi18")
        self.assertEqual(catalog.protocol_families["PI30"], "pi30")
        self.assertEqual(catalog.protocol_families["PI41"], "pi41")

    def test_loads_vmii_variant_overlay_mapping(self) -> None:
        catalog = load_pi_family_catalog()

        vmii = next(item for item in catalog.pi30_variants if item.key == "vmii_nxpw5kw")
        self.assertIn("vmii-nxpw5kw", vmii.rules[0].model_candidates)
        self.assertEqual(vmii.profile_name, "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(vmii.register_schema_name, "pi30_ascii/models/vmii_nxpw5kw.json")

    def test_loads_rule_based_variants(self) -> None:
        catalog = load_pi_family_catalog()

        pi30_max = next(item for item in catalog.pi30_variants if item.key == "pi30_max")
        self.assertEqual(len(pi30_max.rules), 2)
        self.assertEqual(pi30_max.rules[0].min_qpiri_fields, 28)
        self.assertIn("d", pi30_max.rules[1].qflag_contains_any)
        self.assertEqual(pi30_max.profile_name, "pi30_ascii/models/pi30_max.json")

        pip_gk = next(item for item in catalog.pi30_variants if item.key == "pi30_pip_gk")
        self.assertEqual(pip_gk.rules[0].qmod_codes, ("E",))
        self.assertEqual(pip_gk.rules[0].min_qpigs_fields, 21)
        self.assertEqual(pip_gk.register_schema_name, "pi30_ascii/models/pi30_pip_gk.json")

        pi41 = next(item for item in catalog.pi30_variants if item.key == "pi41")
        self.assertEqual(pi41.rules[0].protocol_ids, ("PI41",))
        self.assertEqual(pi41.profile_name, "pi30_ascii/models/pi41.json")


if __name__ == "__main__":
    unittest.main()