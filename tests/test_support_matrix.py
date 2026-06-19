from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.support.matrix import (
    build_profile_support_matrix,
)


class SupportMatrixTests(unittest.TestCase):
    def test_build_profile_support_matrix(self) -> None:
        matrix = build_profile_support_matrix(load_driver_profile("smg_modbus.json"))
        summary = matrix["summary"]

        self.assertEqual(matrix["profile_key"], "smg_modbus")
        self.assertEqual(summary["capabilities"], len(matrix["capabilities"]))
        self.assertEqual(sum(summary["validation_state_counts"].values()), summary["capabilities"])
        self.assertEqual(sum(summary["support_tier_counts"].values()), summary["capabilities"])
        self.assertGreaterEqual(summary["validation_state_counts"]["tested"], 20)
        self.assertEqual(summary["support_tier_counts"]["blocked"], 2)

        blocked = {
            item["key"]: item
            for item in matrix["capabilities"]
            if item["support_tier"] == "blocked"
        }
        self.assertEqual(set(blocked), {"power_saving_mode", "overload_bypass_mode"})
        self.assertIn("exception_code:7", blocked["power_saving_mode"]["support_notes"])


if __name__ == "__main__":
    unittest.main()
