from __future__ import annotations

import json
import unittest

from custom_components.eybond_local.drivers.catalog_identity import (
    CatalogIdentityProbe,
)
from custom_components.eybond_local.metadata.detection_decision_tree import (
    build_detection_decision_tree,
    evaluate_detection_decision_tree,
    serialize_detection_decision_evaluation,
)
from custom_components.eybond_local.metadata.detection_evidence import (
    anchor_evidence_from_catalog_identity_details,
    anchor_evidence_from_catalog_identity_probe,
    anchor_evidence_from_identity_mapping,
    build_descriptor_decision_report,
    build_descriptor_decision_report_from_catalog_identity_probe,
    combined_anchor_evidence,
)
from custom_components.eybond_local.metadata.device_catalog_loader import (
    MATCH_DEVICE,
    DeviceCatalogMatch,
    clear_device_catalog_cache,
)
from custom_components.eybond_local.metadata.detection_descriptor_loader import (
    clear_detection_descriptor_catalog_cache,
)


class DetectionEvidenceTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_device_catalog_cache()
        clear_detection_descriptor_catalog_cache()
        super().tearDown()

    def test_catalog_probe_normalizes_to_descriptor_anchor_evidence(self) -> None:
        probe = CatalogIdentityProbe(
            layout_code=1,
            model_code=7680,
            rated_power=6200,
            serial_ascii="92632500000001",
            match=DeviceCatalogMatch(kind=MATCH_DEVICE),
        )

        evidence = anchor_evidence_from_catalog_identity_probe(probe)

        self.assertEqual(evidence["fingerprint.layout_code"], 1)
        self.assertEqual(evidence["fingerprint.model_code"], 7680)
        self.assertEqual(evidence["fingerprint.rated_power"], 6200)
        self.assertIs(evidence["structural.serial_ascii_plausible"], True)

    def test_catalog_probe_evidence_resolves_known_device_in_decision_tree(self) -> None:
        probe = CatalogIdentityProbe(
            layout_code=1,
            model_code=7680,
            rated_power=6200,
            serial_ascii="92632500000001",
            match=DeviceCatalogMatch(kind=MATCH_DEVICE),
        )
        tree = build_detection_decision_tree(protocol_family="modbus_smg")

        result = evaluate_detection_decision_tree(
            tree,
            anchor_evidence_from_catalog_identity_probe(probe),
        )

        self.assertTrue(result.resolved)
        self.assertEqual(result.resolved_key, "smg_6200")
        payload = serialize_detection_decision_evaluation(result)
        json.dumps(payload, sort_keys=True)
        self.assertEqual(payload["status"], "resolved")
        self.assertEqual(payload["resolved_key"], "smg_6200")

    def test_catalog_details_normalize_without_driver_probe_object(self) -> None:
        evidence = anchor_evidence_from_catalog_identity_details(
            {
                "kind": MATCH_DEVICE,
                "layout_code": 1,
                "model_code": 7680,
                "rated_power": 6200,
            }
        )

        self.assertEqual(
            evidence,
            {
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 7680,
                "fingerprint.rated_power": 6200,
            },
        )

    def test_identity_mapping_accepts_raw_and_normalized_keys(self) -> None:
        raw_evidence = anchor_evidence_from_identity_mapping(
            {
                "layout_code": 1,
                "fingerprint.model_code": 7680,
                "rated_power": 6200,
                "serial_ascii": "92632500000001",
            }
        )

        self.assertEqual(raw_evidence["fingerprint.layout_code"], 1)
        self.assertEqual(raw_evidence["fingerprint.model_code"], 7680)
        self.assertEqual(raw_evidence["fingerprint.rated_power"], 6200)
        self.assertIs(raw_evidence["structural.serial_ascii_plausible"], True)

    def test_combined_anchor_evidence_merges_non_empty_parts(self) -> None:
        self.assertEqual(
            combined_anchor_evidence(
                None,
                {"fingerprint.layout_code": 1},
                {},
                {"variant.protocol_number": 3},
            ),
            {
                "fingerprint.layout_code": 1,
                "variant.protocol_number": 3,
            },
        )

    def test_unknown_model_evidence_resolves_to_family_fallback(self) -> None:
        tree = build_detection_decision_tree(protocol_family="modbus_smg")

        result = evaluate_detection_decision_tree(
            tree,
            anchor_evidence_from_identity_mapping(
                {
                    "layout_code": 1,
                    "model_code": 999,
                }
            ),
        )

        self.assertTrue(result.resolved)
        self.assertEqual(result.resolved_key, "modbus_smg.family_fallback")

    def test_conflicting_optional_rated_power_rejects_exact_model_leaf(self) -> None:
        tree = build_detection_decision_tree(protocol_family="modbus_smg")

        result = evaluate_detection_decision_tree(
            tree,
            anchor_evidence_from_identity_mapping(
                {
                    "layout_code": 1,
                    "model_code": 7680,
                    "rated_power": 11000,
                }
            ),
        )

        self.assertTrue(result.resolved)
        self.assertEqual(result.resolved_key, "modbus_smg.family_fallback")

    def test_descriptor_report_records_matching_catalog_decision(self) -> None:
        report = build_descriptor_decision_report(
            protocol_family="modbus_smg",
            evidence=anchor_evidence_from_identity_mapping(
                {
                    "layout_code": 1,
                    "model_code": 7680,
                    "rated_power": 6200,
                }
            ),
            catalog_match_kind="device",
            catalog_entry_key="smg_6200",
        )

        json.dumps(report, sort_keys=True)
        self.assertEqual(report["kind"], "descriptor_decision_shadow")
        self.assertEqual(report["agreement"], "match")
        evaluation = report["evaluation"]
        self.assertIsInstance(evaluation, dict)
        assert isinstance(evaluation, dict)
        self.assertEqual(evaluation["status"], "resolved")
        self.assertEqual(evaluation["resolved_key"], "smg_6200")

    def test_descriptor_report_marks_mismatch_without_affecting_catalog_match(self) -> None:
        report = build_descriptor_decision_report(
            protocol_family="modbus_smg",
            evidence=anchor_evidence_from_identity_mapping(
                {
                    "layout_code": 1,
                    "model_code": 999,
                }
            ),
            catalog_match_kind="device",
            catalog_entry_key="smg_6200",
        )

        self.assertEqual(report["agreement"], "mismatch")
        evaluation = report["evaluation"]
        self.assertIsInstance(evaluation, dict)
        assert isinstance(evaluation, dict)
        self.assertEqual(evaluation["resolved_key"], "modbus_smg.family_fallback")

    def test_descriptor_report_from_probe_omits_serial_value(self) -> None:
        report = build_descriptor_decision_report_from_catalog_identity_probe(
            CatalogIdentityProbe(
                layout_code=1,
                model_code=7680,
                rated_power=6200,
                serial_ascii="92632500000001",
                match=DeviceCatalogMatch(kind=MATCH_DEVICE),
            )
        )

        self.assertIsNotNone(report)
        assert report is not None
        evidence = report["evidence"]
        self.assertIsInstance(evidence, dict)
        assert isinstance(evidence, dict)
        self.assertNotIn("serial_ascii", evidence)
        self.assertIs(evidence["structural.serial_ascii_plausible"], True)

    def test_none_inputs_are_empty_evidence(self) -> None:
        self.assertEqual(anchor_evidence_from_catalog_identity_probe(None), {})
        self.assertEqual(anchor_evidence_from_catalog_identity_details(None), {})
        self.assertEqual(anchor_evidence_from_identity_mapping(None), {})
        self.assertIsNone(build_descriptor_decision_report_from_catalog_identity_probe(None))


if __name__ == "__main__":
    unittest.main()
