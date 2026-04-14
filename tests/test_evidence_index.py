from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.evidence import (
    build_evidence_index,
    render_evidence_index_markdown,
)
from custom_components.eybond_local.fixtures.catalog import catalog_has_entries


LOCAL_FIXTURE_TESTS_ENABLED = (
    os.environ.get("EYBOND_ENABLE_LOCAL_FIXTURE_TESTS") == "1" and catalog_has_entries()
)


@unittest.skipUnless(
    LOCAL_FIXTURE_TESTS_ENABLED,
    "Local fixture evidence tests are disabled. Set EYBOND_ENABLE_LOCAL_FIXTURE_TESTS=1 and populate .local/fixtures/catalog/.",
)
class EvidenceIndexTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generated_markdown_path = REPO_ROOT / ".local" / "generated" / "EVIDENCE_INDEX.generated.md"

    def test_build_evidence_index(self) -> None:
        index = asyncio.run(build_evidence_index())
        summary = index["summary"]

        self.assertEqual(summary["drivers"], len(index["entries"]))
        self.assertGreaterEqual(summary["drivers"], 1)
        self.assertEqual(summary["profiles"], 2)
        self.assertEqual(summary["fixtures"], 3)
        self.assertIn("evidence_backed", summary["readiness_counts"])
        self.assertIn("experimental", summary["readiness_counts"])
        self.assertNotIn("profile_only", summary["readiness_counts"])

        entry_by_driver = {item["driver_key"]: item for item in index["entries"]}
        self.assertIn("modbus_smg", entry_by_driver)
        self.assertEqual(entry_by_driver["modbus_smg"]["profile_key"], "smg_modbus")
        self.assertEqual(entry_by_driver["modbus_smg"]["readiness"], "evidence_backed")
        self.assertEqual(entry_by_driver["modbus_smg"]["validated_ok"], 1)
        self.assertIn("pi18", entry_by_driver)
        self.assertEqual(entry_by_driver["pi18"]["profile_key"], "")
        self.assertEqual(entry_by_driver["pi18"]["readiness"], "experimental")
        self.assertEqual(entry_by_driver["pi18"]["evidence_scope"], "experimental")
        self.assertIn("pi30", entry_by_driver)
        self.assertEqual(entry_by_driver["pi30"]["profile_key"], "pi30_ascii")
        self.assertEqual(entry_by_driver["pi30"]["readiness"], "evidence_backed")
        self.assertEqual(entry_by_driver["pi30"]["fixture_count"], 1)
        self.assertEqual(entry_by_driver["pi30"]["validated_ok"], 1)

    def test_render_markdown_contains_key_sections(self) -> None:
        index = asyncio.run(build_evidence_index())
        markdown = render_evidence_index_markdown(index)

        self.assertIn("# Driver Evidence Index", markdown)
        self.assertIn("Generated from declarative profiles, local fixture coverage, and replay validation", markdown)
        self.assertIn(
            "| `modbus_smg` | `smg_modbus` | `modbus_smg` | `25` | `8` | `1` | `1` | `0` | `0` | `evidence_backed` |",
            markdown,
        )
        self.assertIn(
            "| `pi18` | `-` | `pi18` | `0` | `0` | `1` | `1` | `0` | `0` | `experimental` | PI18 5000 |",
            markdown,
        )
        self.assertIn(
            "| `pi30` | `pi30_ascii` | `pi30` | `18` | `0` | `1` | `1` | `0` | `0` | `evidence_backed` | VMII-NXPW5KW |",
            markdown,
        )

    def test_generated_markdown_export_is_in_sync(self) -> None:
        index = asyncio.run(build_evidence_index())
        expected = render_evidence_index_markdown(index)
        if not expected.endswith("\n"):
            expected += "\n"
        current = self.generated_markdown_path.read_text(encoding="utf-8")

        self.assertEqual(
            current,
            expected,
            msg=(
                "Generated evidence index is out of sync. Re-run:\n"
                f"python3 {REPO_ROOT / 'tools' / 'export_evidence_index.py'} "
                "--format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
