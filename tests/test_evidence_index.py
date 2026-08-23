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
        self.assertGreaterEqual(summary["profiles"], 18)
        self.assertEqual(summary["fixtures"], 3)
        self.assertEqual(summary["validated_ok"], 3)
        self.assertEqual(summary["validated_mismatch"], 0)
        self.assertEqual(summary["validated_error"], 0)
        self.assertEqual(summary["readiness_counts"]["evidence_backed"], 2)
        self.assertEqual(summary["readiness_counts"]["experimental"], 1)
        self.assertGreaterEqual(summary["readiness_counts"]["profile_only"], 1)

        entry_by_profile = {
            (item["driver_key"], item["profile_key"]): item
            for item in index["entries"]
        }
        smg = entry_by_profile[("modbus_smg", "modbus_smg_6200")]
        self.assertEqual(smg["readiness"], "evidence_backed")
        self.assertEqual(smg["fixture_count"], 1)
        self.assertEqual(smg["validated_ok"], 1)

        pi18 = entry_by_profile[("pi18", "")]
        self.assertEqual(pi18["readiness"], "experimental")
        self.assertEqual(pi18["evidence_scope"], "experimental")
        self.assertEqual(pi18["fixture_count"], 1)
        self.assertEqual(pi18["validated_ok"], 1)

        pi30 = entry_by_profile[("pi30", "pi30_ascii_pi30_max")]
        self.assertEqual(pi30["readiness"], "evidence_backed")
        self.assertEqual(pi30["fixture_count"], 1)
        self.assertEqual(pi30["validated_ok"], 1)

        # One fixture belongs to one exact runtime profile. It must not be
        # multiplied across every profile that happens to use the same driver.
        self.assertEqual(
            sum(int(item["fixture_count"]) for item in index["entries"]),
            summary["fixtures"],
        )
        self.assertEqual(
            sum(int(item["validated_ok"]) for item in index["entries"]),
            summary["validated_ok"],
        )

    def test_render_markdown_contains_key_sections(self) -> None:
        index = asyncio.run(build_evidence_index())
        markdown = render_evidence_index_markdown(index)

        self.assertIn("# Driver Evidence Index", markdown)
        self.assertIn("Generated from declarative profiles, local fixture coverage, and replay validation", markdown)
        self.assertIn(
            "| `modbus_smg` | `modbus_smg_6200` | `modbus_smg` | `30` | `8` | `1` | `1` | `0` | `0` | `evidence_backed` | SMG 6200 |",
            markdown,
        )
        self.assertIn(
            "| `pi18` | `-` | `pi18` | `0` | `0` | `1` | `1` | `0` | `0` | `experimental` | PI18 5000 |",
            markdown,
        )
        self.assertIn(
            "| `pi30` | `pi30_ascii_pi30_max` | `pi30` | `20` | `0` | `1` | `1` | `0` | `0` | `evidence_backed` | PI30 4200 |",
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
                f"python3 {REPO_ROOT / '.local' / 'tools' / 'export_evidence_index.py'} "
                "--format markdown "
                f"--output {self.generated_markdown_path}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
