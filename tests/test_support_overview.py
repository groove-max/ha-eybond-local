from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.overview import (
    build_support_overview,
    render_support_overview_markdown,
)


class SupportOverviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generated_markdown_path = REPO_ROOT / "docs" / "generated" / "SUPPORT_OVERVIEW.generated.md"

    def test_build_support_overview(self) -> None:
        overview = build_support_overview()
        summary = overview["summary"]

        self.assertEqual(summary["profiles"], len(overview["profiles"]))
        self.assertGreaterEqual(summary["profiles"], 1)
        self.assertGreaterEqual(summary["capabilities"], 33)
        self.assertGreaterEqual(summary["validation_state_counts"]["tested"], 25)
        self.assertEqual(summary["support_tier_counts"]["blocked"], 2)

        profile_by_key = {item["profile_key"]: item for item in overview["profiles"]}
        self.assertIn("smg_modbus", profile_by_key)
        self.assertEqual(profile_by_key["smg_modbus"]["capabilities"], 33)
        self.assertEqual(profile_by_key["smg_modbus"]["driver_key"], "modbus_smg")
        self.assertEqual(profile_by_key["smg_modbus"]["protocol_family"], "modbus_smg")

    def test_render_markdown_contains_key_sections(self) -> None:
        overview = build_support_overview()
        markdown = render_support_overview_markdown(overview)

        self.assertIn("# Project Support Overview", markdown)
        self.assertIn("Generated from declarative profile metadata", markdown)
        self.assertIn("| `SMG / Modbus` | `smg_modbus` | `modbus_smg` | `modbus_smg` | `33` |", markdown)

    def test_generated_markdown_export_is_in_sync(self) -> None:
        overview = build_support_overview()
        expected = render_support_overview_markdown(overview)
        if not expected.endswith("\n"):
            expected += "\n"
        current = self.generated_markdown_path.read_text(encoding="utf-8")

        self.assertEqual(
            current,
            expected,
            msg=(
                "Generated support overview is out of sync. Re-run:\n"
                f"python3 {REPO_ROOT / 'tools' / 'export_support_overview.py'} "
                "--format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
