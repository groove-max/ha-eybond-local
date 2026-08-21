from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.validate import affected_test_files


class AffectedValidationMappingTests(unittest.TestCase):
    def _names(self, *paths: str) -> set[str]:
        return {
            path.name
            for path in affected_test_files(tuple(Path(value) for value in paths))
        }

    def test_runtime_coordinator_selects_behavior_and_boundary_tests(self) -> None:
        selected = self._names(
            "custom_components/eybond_local/runtime/coordinator_strategy.py"
        )
        self.assertIn("test_coordinator_device_hierarchy.py", selected)
        self.assertIn("test_coordinator_module_boundaries.py", selected)
        self.assertIn("test_cross_layer_architecture.py", selected)

    def test_future_grouped_paths_use_the_same_family_mapping(self) -> None:
        selected = self._names(
            "custom_components/eybond_local/runtime/coordinator/strategy.py",
            "custom_components/eybond_local/flows/options/proxy.py",
            "custom_components/eybond_local/collector/transport/listener.py",
        )
        self.assertIn("test_coordinator_device_hierarchy.py", selected)
        self.assertIn("test_config_flow.py", selected)
        self.assertIn("test_shared_transport.py", selected)

    def test_changed_unit_test_selects_itself(self) -> None:
        self.assertEqual(
            self._names("tests/test_strategy_transition.py"),
            {"test_strategy_transition.py"},
        )

    def test_docs_only_change_selects_no_tests(self) -> None:
        self.assertEqual(self._names("docs/VALIDATION.md"), set())


if __name__ == "__main__":
    unittest.main()
