from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.runtime_inventory import (  # noqa: E402
    build_runtime_profile_inventory,
    runtime_profile_names,
)


class RuntimeInventoryTests(unittest.TestCase):
    def test_profile_names_are_derived_from_compiled_runtime_surfaces(self) -> None:
        names = runtime_profile_names()

        self.assertEqual(len(names), 17)
        self.assertIn("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json", names)
        self.assertIn("modbus_smg/default.json", names)
        self.assertIn("modbus_smg/models/smg_6200.json", names)
        self.assertIn("modbus_smg/models/anenji_4200_protocol_1.json", names)
        self.assertIn("modbus_smg/models/anenji_anj_5kw_48v_wifi.json", names)
        self.assertIn("modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json", names)
        self.assertIn("modbus_smg/models/anenji_op2_6200.json", names)
        self.assertIn("pi30_ascii/models/smartess_0925_compat.json", names)
        self.assertNotIn("modbus_smg/family_fallback.json", names)

    def test_build_runtime_profile_inventory(self) -> None:
        inventory = build_runtime_profile_inventory()
        summary = inventory["summary"]

        self.assertEqual(summary["profiles"], len(inventory["profiles"]))
        self.assertEqual(summary["profiles"], 17)
        self.assertEqual(summary["capabilities"], 469)
        self.assertEqual(summary["validation_state_counts"], {"tested": 359, "untested": 110})
        self.assertEqual(
            summary["support_tier_counts"],
            {"blocked": 6, "conditional": 238, "standard": 225},
        )

        profile_by_key = {item["profile_key"]: item for item in inventory["profiles"]}
        self.assertIn("eybond_g_ascii_lvyuan_ty_sic_3_6kbe_w1", profile_by_key)
        self.assertIn("smg_modbus", profile_by_key)
        self.assertIn("modbus_smg_6200", profile_by_key)
        self.assertIn("modbus_smg_anenji_4200_protocol_1", profile_by_key)
        self.assertIn("modbus_smg_anenji_anj_5kw_48v_wifi", profile_by_key)
        self.assertIn("modbus_smg_anenji_anj_11kw_48v_wifi_p", profile_by_key)
        self.assertIn("modbus_smg_anenji_op2_6200", profile_by_key)
        self.assertIn("pi30_ascii_smartess_0925_compat", profile_by_key)
        self.assertEqual(profile_by_key["smg_modbus"]["capabilities"], 33)
        self.assertEqual(profile_by_key["modbus_smg_6200"]["capabilities"], 38)
        self.assertEqual(
            profile_by_key["modbus_smg_anenji_4200_protocol_1"]["capabilities"],
            30,
        )
        self.assertEqual(
            profile_by_key["modbus_smg_anenji_anj_5kw_48v_wifi"]["capabilities"],
            40,
        )
        self.assertEqual(
            profile_by_key["modbus_smg_anenji_anj_11kw_48v_wifi_p"]["capabilities"],
            52,
        )
        self.assertEqual(profile_by_key["modbus_smg_anenji_op2_6200"]["capabilities"], 37)
        self.assertEqual(
            profile_by_key["eybond_g_ascii_lvyuan_ty_sic_3_6kbe_w1"]["capabilities"],
            34,
        )
        self.assertEqual(profile_by_key["smg_modbus"]["driver_key"], "modbus_smg")
        self.assertEqual(
            profile_by_key["eybond_g_ascii_lvyuan_ty_sic_3_6kbe_w1"]["driver_key"],
            "eybond_g_ascii",
        )
        self.assertEqual(profile_by_key["smg_modbus"]["protocol_family"], "modbus_smg")

    def test_build_runtime_profile_inventory_accepts_explicit_names(self) -> None:
        inventory = build_runtime_profile_inventory(("modbus_smg/default.json",))

        self.assertEqual(inventory["summary"]["profiles"], 1)
        self.assertEqual(inventory["summary"]["capabilities"], 33)
        self.assertEqual(inventory["profiles"][0]["profile_key"], "smg_modbus")


if __name__ == "__main__":
    unittest.main()
