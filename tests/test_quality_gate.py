from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.quality_gate import (
    DOCS_DIR,
    GENERATED_DOCS_DIR,
    PACKAGE_DIR,
    TOOLS_DIR,
    build_quality_gate_steps,
    generated_exports,
)


class QualityGateTests(unittest.TestCase):
    def test_generated_exports_cover_all_checked_docs(self) -> None:
        exports = generated_exports()

        self.assertEqual(len(exports), 2)
        self.assertEqual(exports[0].profile_name, "smg_modbus.json")
        self.assertEqual(DOCS_DIR.name, "docs")
        self.assertEqual(GENERATED_DOCS_DIR.parent, DOCS_DIR)
        self.assertEqual(exports[0].output_path, GENERATED_DOCS_DIR / "SMG_SUPPORT_MATRIX.generated.md")
        self.assertEqual(exports[-1].output_path, GENERATED_DOCS_DIR / "SUPPORT_OVERVIEW.generated.md")

    def test_build_steps_without_refresh_uses_check_mode(self) -> None:
        steps = build_quality_gate_steps(python_executable="python3", refresh_generated=False)
        step_keys = [step.key for step in steps]

        self.assertEqual(step_keys[:3], ["validate_profiles", "unit_tests", "compileall"])
        self.assertIn("check_support_matrix", step_keys)
        self.assertNotIn("refresh_support_matrix", step_keys)
        self.assertEqual(steps[0].command, ("python3", str(TOOLS_DIR / "validate_profiles.py")))
        self.assertEqual(
            steps[2].command,
            ("python3", "-m", "compileall", str(PACKAGE_DIR), str(TOOLS_DIR)),
        )
        for step in steps[3:]:
            self.assertIn("--check", step.command)

    def test_build_steps_with_refresh_writes_then_checks(self) -> None:
        steps = build_quality_gate_steps(python_executable="python3", refresh_generated=True)
        step_keys = [step.key for step in steps]

        self.assertIn("refresh_support_matrix", step_keys)
        self.assertIn("check_support_matrix", step_keys)
        refresh_index = step_keys.index("refresh_support_matrix")
        check_index = step_keys.index("check_support_matrix")
        self.assertLess(refresh_index, check_index)
        self.assertNotIn("--check", steps[refresh_index].command)
        self.assertIn("--check", steps[check_index].command)


if __name__ == "__main__":
    unittest.main()
