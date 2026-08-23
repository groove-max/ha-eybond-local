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

    def test_shadow_read_route_boundary_selects_capture_binding_and_activation(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/shadow_learning/read_evidence.py"
        )

        self.assertTrue(
            {
                "test_read_learning_binder.py",
                "test_shadow_learning_backend.py",
                "test_shadow_learning_overlay_generator.py",
                "test_device_scoped_overlay_activation.py",
                "test_effective_metadata.py",
                "test_cloud_evidence_architecture.py",
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
                "test_dessmonitor_collection.py",
                "test_dessmonitor_history.py",
                "test_dessmonitor_history_resolution.py",
                "test_cloud_local_history_correlation.py",
                "test_dessmonitor_time_basis.py",
                "test_dessmonitor_learning.py",
                "test_dessmonitor_semantics.py",
                "test_cloud_semantic_evidence.py",
                "test_cloud_local_coverage.py",
                "test_cloud_learning_engines.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_dessmonitor_collection_selects_every_boundary_and_consumer(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/dessmonitor_collection.py"
        )

        self.assertTrue(
            {
                "test_dessmonitor_collection.py",
                "test_dessmonitor_cloud.py",
                "test_dessmonitor_history.py",
                "test_dessmonitor_time_basis.py",
                "test_dessmonitor_history_resolution.py",
                "test_dessmonitor_learning.py",
                "test_cloud_learning_engines.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_dessmonitor_history_selects_client_capability_and_guards(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/dessmonitor_history.py"
        )

        self.assertTrue(
            {
                "test_dessmonitor_history.py",
                "test_dessmonitor_collection.py",
                "test_dessmonitor_history_resolution.py",
                "test_cloud_local_history_correlation.py",
                "test_dessmonitor_cloud.py",
                "test_cloud_learning_engines.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_dessmonitor_time_basis_selects_history_client_and_guards(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/dessmonitor_time_basis.py"
        )

        self.assertTrue(
            {
                "test_dessmonitor_time_basis.py",
                "test_dessmonitor_collection.py",
                "test_dessmonitor_history.py",
                "test_dessmonitor_history_resolution.py",
                "test_cloud_local_history_correlation.py",
                "test_dessmonitor_cloud.py",
                "test_cloud_learning_engines.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_dessmonitor_history_resolution_selects_both_input_boundaries(
        self,
    ) -> None:
        selected = self._selected(
            "custom_components/eybond_local/dessmonitor_history_resolution.py"
        )

        self.assertTrue(
            {
                "test_dessmonitor_history_resolution.py",
                "test_dessmonitor_collection.py",
                "test_cloud_local_history_correlation.py",
                "test_dessmonitor_history.py",
                "test_dessmonitor_time_basis.py",
                "test_dessmonitor_cloud.py",
                "test_cloud_learning_engines.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_local_register_series_selects_producer_and_boundary_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/drivers/local_register_series.py"
        )

        self.assertTrue(
            {
                "test_local_register_series.py",
                "test_local_register_collection.py",
                "test_local_register_evidence.py",
                "test_cloud_local_history_correlation.py",
                "test_driver_local_register_evidence.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_local_register_collection_selects_runtime_flow_and_guards(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/local_register_collection.py"
        )

        self.assertTrue(
            {
                "test_local_register_collection.py",
                "test_local_register_series.py",
                "test_coordinator_device_hierarchy.py",
                "test_config_flow.py",
                "test_cloud_learning_engines.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_history_correlator_selects_every_typed_input_boundary(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/cloud_local_history_correlation.py"
        )

        self.assertTrue(
            {
                "test_cloud_local_history_correlation.py",
                "test_dessmonitor_history_resolution.py",
                "test_local_register_series.py",
                "test_cloud_semantic_evidence.py",
                "test_cloud_learning_engines.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_smartess_history_selects_provider_neutral_consumers(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/smartess_history.py"
        )

        self.assertTrue(
            {
                "test_smartess_history.py",
                "test_smartess_read_only.py",
                "test_cloud_history_evidence.py",
                "test_cloud_local_history_correlation.py",
                "test_cloud_learning_engines.py",
                "test_config_flow.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_neutral_cloud_history_selects_both_provider_adapters(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/cloud_history_evidence.py"
        )

        self.assertTrue(
            {
                "test_cloud_history_evidence.py",
                "test_smartess_history.py",
                "test_smartess_read_only.py",
                "test_dessmonitor_learning.py",
                "test_cloud_local_history_correlation.py",
                "test_config_flow.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_history_representability_selects_context_and_archive_boundaries(
        self,
    ) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/"
            "cloud_local_history_representability.py"
        )

        self.assertTrue(
            {
                "test_cloud_local_history_correlation.py",
                "test_local_register_series.py",
                "test_cloud_semantic_evidence.py",
                "test_coordinator_device_hierarchy.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_translations.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_history_draft_selects_model_flow_archive_and_guards(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/"
            "cloud_local_history_draft.py"
        )

        self.assertTrue(
            {
                "test_cloud_local_history_correlation.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_history_draft_writer_selects_artifact_and_architecture_tests(
        self,
    ) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/"
            "cloud_local_history_draft_writer.py"
        )

        self.assertTrue(
            {
                "test_cloud_local_history_correlation.py",
                "test_config_flow.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_history_draft_flow_adapter_selects_writer_and_boundary_tests(
        self,
    ) -> None:
        selected = self._selected(
            "custom_components/eybond_local/flows/options/"
            "shadow_inactive_draft.py"
        )

        self.assertTrue(
            {
                "test_cloud_local_history_correlation.py",
                "test_config_flow.py",
                "test_cloud_evidence_architecture.py",
                "test_flow_module_boundaries.py",
            }.issubset(selected)
        )

    def test_cloud_semantics_selects_adapter_review_and_boundary_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/cloud_semantic_evidence.py"
        )

        self.assertTrue(
            {
                "test_cloud_semantic_evidence.py",
                "test_cloud_local_coverage.py",
                "test_cloud_local_history_correlation.py",
                "test_dessmonitor_semantics.py",
                "test_dessmonitor_learning.py",
                "test_config_flow.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_cloud_local_coverage_selects_telemetry_review_and_archive_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/support/cloud_local_coverage.py"
        )

        self.assertTrue(
            {
                "test_cloud_local_coverage.py",
                "test_typed_telemetry.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_cloud_metadata_review_selects_flow_translation_and_guards(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/flows/options/"
            "shadow_metadata_review.py"
        )

        self.assertTrue(
            {
                "test_config_flow.py",
                "test_cloud_local_history_correlation.py",
                "test_shadow_learning_support_package.py",
                "test_translations.py",
                "test_flow_module_boundaries.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_local_register_evidence_selects_producer_runtime_and_archive_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/drivers/local_register_evidence.py"
        )

        self.assertTrue(
            {
                "test_local_register_evidence.py",
                "test_local_register_series.py",
                "test_cloud_local_history_correlation.py",
                "test_driver_local_register_evidence.py",
                "test_hub.py",
                "test_config_flow.py",
                "test_shadow_learning_support_package.py",
                "test_cloud_evidence_architecture.py",
            }.issubset(selected)
        )

    def test_modbus_driver_change_selects_driver_and_local_evidence_tests(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/drivers/smg.py"
        )

        self.assertTrue(
            {
                "test_smg_driver.py",
                "test_driver_local_register_evidence.py",
            }.issubset(selected)
        )

    def test_flow_translation_change_selects_translation_contracts(self) -> None:
        selected = self._selected(
            "custom_components/eybond_local/flow_translations/uk.json"
        )

        self.assertIn("test_translations.py", selected)


if __name__ == "__main__":
    unittest.main()
