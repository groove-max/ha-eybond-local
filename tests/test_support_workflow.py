from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.workflow import build_support_workflow_state


class SupportWorkflowTests(unittest.TestCase):
    def test_builtin_support_prefers_support_archive_for_extra_evidence(self) -> None:
        workflow = build_support_workflow_state(
            has_inverter=True,
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG / Modbus",
            detection_confidence="high",
            profile_source_scope="builtin",
            schema_source_scope="builtin",
        )

        self.assertEqual(workflow["level"], "builtin")
        self.assertEqual(workflow["level_label"], "Built-in support")
        self.assertEqual(workflow["primary_action"], "create_support_package")
        self.assertIn("Step 1:", workflow["plan"])
        self.assertEqual(workflow["step_1"], "Keep using built-in support normally.")
        self.assertIn("ZIP file", workflow["step_3"])
        self.assertTrue(workflow["advanced_hint"])

    def test_partial_support_recommends_support_archive(self) -> None:
        workflow = build_support_workflow_state(
            has_inverter=True,
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG / Modbus",
            detection_confidence="low",
            profile_source_scope="builtin",
            schema_source_scope="builtin",
        )

        self.assertEqual(workflow["level"], "partial")
        self.assertEqual(workflow["level_label"], "Partial support")
        self.assertIn("Create a support archive", workflow["next_action"])
        self.assertIn("Step 2:", workflow["plan"])
        self.assertEqual(workflow["step_2"], "Send the ZIP file to the developer.")

    def test_experimental_support_recommends_reload(self) -> None:
        workflow = build_support_workflow_state(
            has_inverter=True,
            effective_owner_key="modbus_smg",
            effective_owner_name="SMG / Modbus",
            detection_confidence="high",
            profile_source_scope="external",
            schema_source_scope="builtin",
        )

        self.assertEqual(workflow["level"], "experimental")
        self.assertEqual(workflow["level_label"], "Experimental local metadata")
        self.assertEqual(workflow["primary_action"], "reload_local_metadata")
        self.assertEqual(workflow["step_1"], "Reload local metadata.")

    def test_unknown_support_prefers_support_archive(self) -> None:
        workflow = build_support_workflow_state(
            has_inverter=False,
            effective_owner_key="",
            effective_owner_name="",
            detection_confidence="none",
            profile_source_scope="",
            schema_source_scope="",
        )

        self.assertEqual(workflow["level"], "unknown")
        self.assertEqual(workflow["level_label"], "Unknown support")
        self.assertEqual(workflow["primary_action"], "create_support_package")
        self.assertEqual(workflow["step_1"], "Create a support archive.")
        self.assertEqual(workflow["step_2"], "Send the ZIP file to the developer.")
        self.assertEqual(
            workflow["step_3"],
            "Wait for built-in support before working with local experimental metadata.",
        )

    def test_smartess_collector_evidence_avoids_generic_unknown_support(self) -> None:
        workflow = build_support_workflow_state(
            has_inverter=False,
            effective_owner_key="",
            effective_owner_name="",
            detection_confidence="none",
            profile_source_scope="",
            schema_source_scope="",
            smartess_protocol_asset_id="0000",
            smartess_collector_version="8.50.12.3",
        )

        self.assertEqual(workflow["level"], "smartess_pending")
        self.assertEqual(workflow["level_label"], "SmartESS collector evidence")
        self.assertEqual(workflow["primary_action"], "create_support_package")
        self.assertIn("SmartESS app support", workflow["next_action"])
        self.assertIn("collector evidence", workflow["step_3"])

    def test_known_runtime_owner_without_live_inverter_counts_as_pending(self) -> None:
        workflow = build_support_workflow_state(
            has_inverter=False,
            effective_owner_key="pi30",
            effective_owner_name="PI30 / ASCII",
            smartess_family_name="SmartESS 0925",
            detection_confidence="none",
            profile_source_scope="builtin",
            schema_source_scope="builtin",
        )

        self.assertEqual(workflow["level"], "pending")
        self.assertIn("PI30 / ASCII", workflow["summary"])
        self.assertIn("SmartESS family context: SmartESS 0925.", workflow["advanced_hint"])


if __name__ == "__main__":
    unittest.main()
