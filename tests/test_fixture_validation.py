from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.validation import (
    build_fixture_validation_overview,
    render_fixture_validation_markdown,
)
from custom_components.eybond_local.fixtures.catalog import catalog_has_entries


LOCAL_FIXTURE_TESTS_ENABLED = (
    os.environ.get("EYBOND_ENABLE_LOCAL_FIXTURE_TESTS") == "1" and catalog_has_entries()
)


@unittest.skipUnless(
    LOCAL_FIXTURE_TESTS_ENABLED,
    "Local fixture catalog tests are disabled. Set EYBOND_ENABLE_LOCAL_FIXTURE_TESTS=1 and populate .local/fixtures/catalog/.",
)
class FixtureValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generated_markdown_path = REPO_ROOT / ".local" / "generated" / "FIXTURE_VALIDATION.generated.md"

    def test_build_fixture_validation_overview(self) -> None:
        overview = asyncio.run(build_fixture_validation_overview())
        summary = overview["summary"]

        self.assertEqual(summary["fixtures"], len(overview["entries"]))
        self.assertGreaterEqual(summary["fixtures"], 1)
        self.assertIn("ok", summary["status_counts"])
        self.assertIn("modbus_smg", summary["drivers"])
        self.assertIn("pi18", summary["drivers"])

        entry_by_slug = {item["slug"]: item for item in overview["entries"]}
        self.assertIn("smg-6200-live-capture", entry_by_slug)
        self.assertEqual(entry_by_slug["smg-6200-live-capture"]["status"], "ok")
        self.assertEqual(entry_by_slug["smg-6200-live-capture"]["driver_key"], "modbus_smg")
        self.assertIn("pi18-5000-synthetic-capture", entry_by_slug)
        self.assertEqual(entry_by_slug["pi18-5000-synthetic-capture"]["status"], "ok")
        self.assertEqual(entry_by_slug["pi18-5000-synthetic-capture"]["driver_key"], "pi18")
        self.assertIn("pi30-vmii-nxpw5kw-live-capture", entry_by_slug)
        self.assertEqual(entry_by_slug["pi30-vmii-nxpw5kw-live-capture"]["status"], "ok")
        self.assertEqual(entry_by_slug["pi30-vmii-nxpw5kw-live-capture"]["driver_key"], "pi30")

    def test_render_markdown_contains_key_sections(self) -> None:
        overview = asyncio.run(build_fixture_validation_overview())
        markdown = render_fixture_validation_markdown(overview)

        self.assertIn("# Fixture Validation Overview", markdown)
        self.assertIn("Generated from replaying local fixtures", markdown)
        self.assertIn("| `smg-6200-live-capture` | `ok` | `modbus_smg` | `SMG 6200` |", markdown)
        self.assertIn("| `pi18-5000-synthetic-capture` | `ok` | `pi18` | `PI18 5000` | `Hybrid` |", markdown)
        self.assertIn("| `pi30-vmii-nxpw5kw-live-capture` | `ok` | `pi30` | `VMII-NXPW5KW` | `Line` |", markdown)

    def test_generated_markdown_export_is_in_sync(self) -> None:
        overview = asyncio.run(build_fixture_validation_overview())
        expected = render_fixture_validation_markdown(overview)
        if not expected.endswith("\n"):
            expected += "\n"
        current = self.generated_markdown_path.read_text(encoding="utf-8")

        self.assertEqual(
            current,
            expected,
            msg=(
                "Generated fixture validation is out of sync. Re-run:\n"
                f"python3 {REPO_ROOT / 'tools' / 'export_fixture_validation.py'} "
                "--format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
