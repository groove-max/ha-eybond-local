from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools.validate import affected_test_files


class AffectedValidationSelectionTests(unittest.TestCase):
    def _selected(self, production_path: str) -> set[str]:
        return {
            path.name
            for path in affected_test_files((Path(production_path),))
        }

    def test_typed_telemetry_boundary_selects_behavior_and_projection_tests(self) -> None:
        selected = self._selected("custom_components/eybond_local/telemetry.py")

        self.assertTrue(
            {
                "test_typed_telemetry.py",
                "test_driver_read_contract.py",
                "test_canonical_telemetry.py",
                "test_support_bundle.py",
            }.issubset(selected)
        )

    def test_neutral_wire_selects_every_direct_behavior_family(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/collector/collector_wire.py"
        )

        self.assertTrue(
            {
                "test_collector_management.py",
                "test_smartess_local.py",
                "test_shadow_learning_proxy.py",
                "test_shadow_learning_proxy_e2e.py",
                "test_fake_collector.py",
                "test_config_flow.py",
                "test_collector_metadata_architecture.py",
            }.issubset(selected)
        )

    def test_metadata_reader_selects_structured_outcome_and_boundary_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/collector/at_runtime.py"
        )

        self.assertTrue(
            {
                "test_collector_at.py",
                "test_collector_metadata.py",
                "test_collector_metadata_architecture.py",
                "test_collector_virtual_bridge.py",
            }.issubset(selected)
        )

    def test_dessmonitor_selects_client_runner_engine_and_architecture_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/dessmonitor_cloud.py"
        )

        self.assertTrue(
            {
                "test_dessmonitor_cloud.py",
                "test_dessmonitor_learning.py",
                "test_cloud_learning_engines.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )


if __name__ == "__main__":
    unittest.main()
