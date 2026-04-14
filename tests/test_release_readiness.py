from __future__ import annotations

import asyncio
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
        self.assertEqual(report["integration"]["version"], "0.1.43")
        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["summary"]["drivers"], 2)
        self.assertEqual(report["summary"]["profiles"], 2)
        self.assertEqual(report["summary"]["fixtures"], 2)
        self.assertEqual(report["summary"]["validated_ok"], 2)
        self.assertEqual(report["summary"]["readiness_counts"], {"evidence_backed": 2})
        self.assertEqual(report["blockers"], [])

    def test_render_markdown_contains_key_sections(self) -> None:
        report = asyncio.run(build_release_readiness())
        markdown = render_release_readiness_markdown(report)

        self.assertIn("# Release Readiness", markdown)
        self.assertIn("Generated from manifest metadata and local evidence", markdown)
        self.assertIn("- status: `ready`", markdown)
        self.assertIn("| `modbus_smg` | `smg_modbus` | `evidence_backed` | `25` | `8` | `1` | `1` | `0` | `0` |", markdown)
        self.assertIn("| `pi30` | `pi30_ascii` | `evidence_backed` | `18` | `0` | `1` | `1` | `0` | `0` |", markdown)
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
                f"python3 {REPO_ROOT / 'tools' / 'export_release_readiness.py'} "
                "--format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
