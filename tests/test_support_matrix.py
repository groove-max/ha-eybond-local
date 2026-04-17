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
    render_support_matrix_markdown,
)


class SupportMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = load_driver_profile("smg_modbus.json")
        cls.generated_markdown_path = REPO_ROOT / "docs" / "generated" / "SMG_SUPPORT_MATRIX.generated.md"

    def test_build_profile_support_matrix(self) -> None:
        matrix = build_profile_support_matrix(self.profile)
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

    def test_render_markdown_contains_key_sections(self) -> None:
        matrix = build_profile_support_matrix(self.profile)
        markdown = render_support_matrix_markdown(matrix)

        self.assertIn("# Support Matrix: SMG-family default runtime profile", markdown)
        self.assertIn("implementation-level runtime report", markdown)
        self.assertIn("| `low_dc_cutoff_soc` | `343` | `battery` | `tested` | `conditional` |", markdown)
        self.assertIn("| `power_saving_mode` | `307` | `system` | `untested` | `blocked` |", markdown)
        self.assertIn("| `charge_source_priority` | `331` | `charging` | `tested` | `conditional` |", markdown)

    def test_generated_markdown_export_is_in_sync(self) -> None:
        matrix = build_profile_support_matrix(self.profile)
        expected = render_support_matrix_markdown(matrix)
        if not expected.endswith("\n"):
            expected += "\n"
        current = self.generated_markdown_path.read_text(encoding="utf-8")

        self.assertEqual(
            current,
            expected,
            msg=(
                "Generated support matrix is out of sync. Re-run:\n"
                f"python3 {REPO_ROOT / 'tools' / 'export_support_matrix.py'} "
                "--profile smg_modbus.json --format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
