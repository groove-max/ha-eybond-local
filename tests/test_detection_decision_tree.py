from __future__ import annotations

import json
import unittest

from custom_components.eybond_local.metadata.detection_decision_tree import (
    DetectionDecisionLeaf,
    DetectionDecisionNode,
    build_detection_decision_tree,
    evaluate_detection_decision_tree,
    serialize_detection_decision_tree,
)
from custom_components.eybond_local.metadata.detection_descriptor_loader import (
    DetectionAnchorCondition,
    DetectionBindingDescriptor,
    DetectionDescriptorCatalog,
    DetectionDeviceDescriptor,
    clear_detection_descriptor_catalog_cache,
    load_detection_descriptor_catalog,
)


class DetectionDecisionTreeTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_detection_descriptor_catalog_cache()
        super().tearDown()

    def test_builds_read_only_tree_from_real_descriptor_catalog(self) -> None:
        catalog = load_detection_descriptor_catalog()

        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=catalog,
        )

        self.assertEqual(
            tree.descriptor_count,
            len(catalog.descriptors_for_protocol("modbus_smg")),
        )
        self.assertIsInstance(tree.root, DetectionDecisionNode)
        assert isinstance(tree.root, DetectionDecisionNode)
        self.assertEqual(tree.root.anchor_key, "fingerprint.layout_code")
        self.assertTrue(tree.root.forced)
        self.assertIn("fingerprint.model_code", tree.anchor_keys)
        self.assertEqual(tree.ambiguous_leaf_count, 0)
        self.assertGreaterEqual(tree.max_depth, 2)

    def test_unknown_protocol_returns_empty_leaf(self) -> None:
        tree = build_detection_decision_tree(protocol_family="unknown")

        self.assertEqual(tree.descriptor_count, 0)
        self.assertIsInstance(tree.root, DetectionDecisionLeaf)
        assert isinstance(tree.root, DetectionDecisionLeaf)
        self.assertEqual(tree.root.candidate_keys, ())
        self.assertIsNone(tree.root.resolved_key)
        self.assertTrue(tree.root.ambiguous)

    def test_tree_adds_branch_specific_rules_after_duplicate_fingerprint(self) -> None:
        catalog = _synthetic_duplicate_fingerprint_catalog()

        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=catalog,
        )

        self.assertEqual(
            tree.anchor_keys,
            (
                "fingerprint.layout_code",
                "fingerprint.model_code",
                "variant.protocol_number",
            ),
        )
        self.assertEqual(tree.ambiguous_leaf_count, 0)
        self.assertEqual(tree.max_depth, 3)

    def test_tree_reports_ambiguous_leaf_when_descriptors_are_not_separable(self) -> None:
        binding = _synthetic_binding()
        base_catalog = load_detection_descriptor_catalog()
        catalog = DetectionDescriptorCatalog(
            protocols=base_catalog.protocols,
            devices=(
                _synthetic_descriptor("synthetic_a", binding=binding),
                _synthetic_descriptor("synthetic_b", binding=binding),
            ),
        )

        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=catalog,
        )

        self.assertEqual(tree.ambiguous_leaf_count, 1)
        self.assertEqual(
            tree.anchor_keys,
            ("fingerprint.layout_code", "fingerprint.model_code"),
        )

    def test_tree_serialization_is_json_safe_diagnostic_payload(self) -> None:
        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=_synthetic_duplicate_fingerprint_catalog(),
        )

        payload = serialize_detection_decision_tree(tree)

        json.dumps(payload, sort_keys=True)
        self.assertEqual(payload["protocol_family"], "modbus_smg")
        self.assertEqual(payload["descriptor_count"], 2)
        self.assertEqual(payload["ambiguous_leaf_count"], 0)
        self.assertEqual(
            payload["anchor_keys"],
            [
                "fingerprint.layout_code",
                "fingerprint.model_code",
                "variant.protocol_number",
            ],
        )
        root = payload["root"]
        self.assertIsInstance(root, dict)
        assert isinstance(root, dict)
        self.assertEqual(root["type"], "node")
        self.assertEqual(root["anchor_key"], "fingerprint.layout_code")
        self.assertTrue(root["forced"])

    def test_evaluation_resolves_descriptor_from_complete_evidence(self) -> None:
        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=_synthetic_duplicate_fingerprint_catalog(),
        )

        result = evaluate_detection_decision_tree(
            tree,
            {
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 35,
                "variant.protocol_number": 3,
            },
        )

        self.assertTrue(result.resolved)
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.resolved_key, "synthetic_b")
        self.assertEqual(
            tuple(step.anchor_key for step in result.path),
            (
                "fingerprint.layout_code",
                "fingerprint.model_code",
                "variant.protocol_number",
            ),
        )

    def test_evaluation_reports_missing_next_anchor_without_guessing(self) -> None:
        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=_synthetic_duplicate_fingerprint_catalog(),
        )

        result = evaluate_detection_decision_tree(
            tree,
            {
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 35,
            },
        )

        self.assertFalse(result.resolved)
        self.assertEqual(result.status, "missing_anchor")
        self.assertEqual(result.missing_anchor_key, "variant.protocol_number")
        self.assertEqual(result.candidate_keys, ("synthetic_a", "synthetic_b"))

    def test_evaluation_reports_no_match_for_invalid_protocol_fingerprint(self) -> None:
        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=_synthetic_duplicate_fingerprint_catalog(),
        )

        result = evaluate_detection_decision_tree(
            tree,
            {
                "fingerprint.layout_code": 99,
                "fingerprint.model_code": 35,
                "variant.protocol_number": 3,
            },
        )

        self.assertEqual(result.status, "no_match")
        self.assertIsNone(result.resolved_key)
        self.assertEqual(result.path, ())

    def test_evaluation_uses_family_fallback_when_known_layout_has_unknown_model(self) -> None:
        catalog = load_detection_descriptor_catalog()
        tree = build_detection_decision_tree(
            protocol_family="modbus_smg",
            catalog=catalog,
        )

        result = evaluate_detection_decision_tree(
            tree,
            {
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 999,
            },
        )

        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.resolved_key, "modbus_smg.family_fallback")
        self.assertEqual(result.path[-1].matched_signature, "one_of:1,2,11")


def _synthetic_duplicate_fingerprint_catalog() -> DetectionDescriptorCatalog:
    binding = _synthetic_binding()
    base_catalog = load_detection_descriptor_catalog()
    return DetectionDescriptorCatalog(
        protocols=base_catalog.protocols,
        devices=(
            _synthetic_descriptor(
                "synthetic_a",
                binding=binding,
                protocol_number=1,
            ),
            _synthetic_descriptor(
                "synthetic_b",
                binding=binding,
                protocol_number=3,
            ),
        ),
    )


def _synthetic_binding() -> DetectionBindingDescriptor:
    return DetectionBindingDescriptor(
        driver_key="modbus_smg",
        variant_key="synthetic",
        profile_name="smg_modbus.json",
        register_schema_name="modbus_smg/models/smg_6200.json",
    )


def _synthetic_descriptor(
    key: str,
    *,
    binding: DetectionBindingDescriptor,
    protocol_number: int | None = None,
) -> DetectionDeviceDescriptor:
    anchors = [
        DetectionAnchorCondition(
            key="fingerprint.layout_code",
            source="test",
            equals=1,
        ),
        DetectionAnchorCondition(
            key="fingerprint.model_code",
            source="test",
            equals=35,
        ),
    ]
    if protocol_number is not None:
        anchors.append(
            DetectionAnchorCondition(
                key="variant.protocol_number",
                source="test",
                equals=protocol_number,
                cost=2,
            )
        )
    return DetectionDeviceDescriptor(
        key=key,
        protocol_family="modbus_smg",
        model_name=key,
        tier="full",
        binding=binding,
        anchors=tuple(anchors),
    )


if __name__ == "__main__":
    unittest.main()
