from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.release import (
    build_release_readiness,
    render_release_readiness_markdown,
)
from custom_components.eybond_local.fixtures.catalog import catalog_has_entries


LOCAL_FIXTURE_TESTS_ENABLED = (
    os.environ.get("EYBOND_ENABLE_LOCAL_FIXTURE_TESTS") == "1" and catalog_has_entries()
)


@unittest.skipUnless(
    LOCAL_FIXTURE_TESTS_ENABLED,
    "Local release-readiness tests are disabled. Set EYBOND_ENABLE_LOCAL_FIXTURE_TESTS=1 and populate .local/fixtures/catalog/.",
)
class ReleaseReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generated_markdown_path = REPO_ROOT / ".local" / "generated" / "RELEASE_READINESS.generated.md"

    def test_build_release_readiness(self) -> None:
        report = asyncio.run(build_release_readiness())

        self.assertEqual(report["integration"]["domain"], "eybond_local")
        manifest = json.loads(
            (REPO_ROOT / "custom_components" / "eybond_local" / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(report["integration"]["version"], manifest["version"])
        self.assertEqual(report["status"], "incomplete")
        self.assertGreaterEqual(report["summary"]["drivers"], 18)
        self.assertGreaterEqual(report["summary"]["profiles"], 18)
        self.assertEqual(report["summary"]["fixtures"], 2)
        self.assertEqual(report["summary"]["validated_ok"], 2)
        self.assertEqual(report["summary"]["validated_mismatch"], 0)
        self.assertEqual(report["summary"]["validated_error"], 0)
        self.assertEqual(report["summary"]["readiness_counts"]["evidence_backed"], 2)
        self.assertGreaterEqual(report["summary"]["readiness_counts"]["profile_only"], 1)
        self.assertEqual(report["blockers"], [])

        release_fixtures = sum(
            int(item["fixture_count"]) for item in report["drivers"]
        )
        self.assertEqual(release_fixtures, report["summary"]["fixtures"])

    def test_render_markdown_contains_key_sections(self) -> None:
        report = asyncio.run(build_release_readiness())
        markdown = render_release_readiness_markdown(report)

        self.assertIn("# Release Readiness", markdown)
        self.assertIn("Generated from manifest metadata and local evidence", markdown)
        self.assertIn("- status: `incomplete`", markdown)
        self.assertIn("| `modbus_smg` | `modbus_smg_6200` | `evidence_backed` | `30` | `8` | `1` | `1` | `0` | `0` |", markdown)
        self.assertIn("| `pi30` | `pi30_ascii_pi30_max` | `evidence_backed` | `20` | `0` | `1` | `1` | `0` | `0` |", markdown)
        self.assertNotIn("| `pi18` |", markdown)

    def test_generated_markdown_export_is_in_sync(self) -> None:
        report = asyncio.run(build_release_readiness())
        expected = render_release_readiness_markdown(report)
        if not expected.endswith("\n"):
            expected += "\n"
        current = self.generated_markdown_path.read_text(encoding="utf-8")

        self.assertEqual(
            current,
            expected,
            msg=(
                "Generated release readiness is out of sync. Re-run:\n"
                f"python3 {REPO_ROOT / '.local' / 'tools' / 'export_release_readiness.py'} "
                "--format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
