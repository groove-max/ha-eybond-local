from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.smg_identity_rules import (  # noqa: E402
    SmgIdentityEvidence,
    clear_smg_identity_rule_catalog_cache,
    load_smg_identity_rule_catalog,
    score_smg_identity_candidates,
)


def _default_smg_evidence() -> SmgIdentityEvidence:
    return SmgIdentityEvidence(
        protocol_family="modbus_smg",
        anchors={
            "serial": "SMG11K240001",
            "operating_mode": "Line",
            "protocol_number": 1,
            "device_type": 0x1E00,
            "rated_power": 6200,
            "device_name": "SMG II 6200",
            "program_version": "U1.00",
            "turn_on_mode": "Local and Remote",
            "remote_switch": "Remote Turn-On",
            "output_rating_voltage": 230.0,
            "output_rating_frequency": 50.0,
        },
    )


def _anenji_4200_evidence() -> SmgIdentityEvidence:
    return SmgIdentityEvidence(
        protocol_family="modbus_smg",
        anchors={
            "serial": "99432409105281",
            "protocol_number": 1,
            "device_type": 0x3501,
            "rated_power": 4200,
            "turn_on_mode": "Local and Remote",
            "remote_switch": "Remote Turn-On",
        },
    )


def _anenji_11kw_evidence() -> SmgIdentityEvidence:
    return SmgIdentityEvidence(
        protocol_family="modbus_smg",
        anchors={
            "serial": "ANJ11KW240001",
            "protocol_number": 4,
            "device_type": 1,
            "rated_power": 11000,
            "pv_grid_connected_max_power": 11000,
            "turn_on_mode": "Local and Remote",
            "remote_switch": "Remote Turn-On",
        },
    )


def _anenji_11kw_protocol_6_evidence() -> SmgIdentityEvidence:
    return SmgIdentityEvidence(
        protocol_family="modbus_smg",
        anchors={
            "serial": "ANJ11KW240006",
            "protocol_number": 6,
            "device_type": 1,
            "rated_power": 11000,
            "pv_grid_connected_max_power": 11000,
            "turn_on_mode": "Local and Remote",
            "remote_switch": "Remote Turn-On",
        },
    )


def _family_fallback_evidence() -> SmgIdentityEvidence:
    return SmgIdentityEvidence(
        protocol_family="modbus_smg",
        anchors={
            "serial": "SMG11K240001",
            "protocol_number": 1,
            "device_type": 0x1E00,
            "rated_power": 11000,
            "device_name": "SMG II 6200",
            "program_version": "U1.00",
        },
    )


class SmgIdentityRuleCatalogTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_smg_identity_rule_catalog_cache()

    def test_loads_variant_rules_with_expected_shape(self) -> None:
        catalog = load_smg_identity_rule_catalog()

        self.assertEqual(catalog.protocol_family, "modbus_smg")
        self.assertEqual(
            tuple(rule.variant_key for rule in catalog.rules),
            (
                "default",
                "anenji_4200_protocol_1",
                "anenji_anj_11kw_48v_wifi_p",
                "family_fallback",
            ),
        )

        default_rule = catalog.rules[0]
        # Identity rests on IMMUTABLE anchors only. operating_mode (runtime state) was removed
        # so a transient/unknown mode cannot block identification; output_rating_voltage/
        # frequency remain solely as structural sanity (registers outside any control sweep).
        self.assertEqual(
            tuple(requirement.anchor_key for requirement in default_rule.required),
            (
                "rated_power",
                "output_rating_voltage",
                "output_rating_frequency",
            ),
        )
        self.assertEqual(default_rule.confidence, "high")

        fallback_rule = catalog.rules[-1]
        self.assertTrue(fallback_rule.family_only)
        self.assertTrue(fallback_rule.read_only)
        self.assertTrue(fallback_rule.provisional)


class SmgIdentityRuleScoringTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_smg_identity_rule_catalog_cache()

    def test_scores_default_6200_as_high_confidence_first_candidate(self) -> None:
        candidates = score_smg_identity_candidates(_default_smg_evidence())

        self.assertGreaterEqual(len(candidates), 2)
        self.assertEqual(candidates[0].variant_key, "default")
        self.assertEqual(candidates[0].confidence, "high")
        self.assertIn("confirmed_variant", candidates[0].reasons)
        self.assertIn("required_anchor:rated_power=6200", candidates[0].reasons)
        self.assertEqual(candidates[-1].variant_key, "family_fallback")
        self.assertTrue(candidates[-1].read_only)

    def test_scores_anenji_4200_protocol_1_as_medium_confidence(self) -> None:
        candidates = score_smg_identity_candidates(_anenji_4200_evidence())

        self.assertEqual(candidates[0].variant_key, "anenji_4200_protocol_1")
        self.assertEqual(candidates[0].confidence, "medium")
        self.assertIn("unverified_variant", candidates[0].reasons)
        self.assertIn("required_anchor:device_type=13569", candidates[0].reasons)

    def test_scores_anenji_11kw_as_high_confidence_first_candidate(self) -> None:
        candidates = score_smg_identity_candidates(_anenji_11kw_evidence())

        self.assertEqual(candidates[0].variant_key, "anenji_anj_11kw_48v_wifi_p")
        self.assertEqual(candidates[0].confidence, "high")
        self.assertIn("confirmed_variant", candidates[0].reasons)
        self.assertIn("required_anchor:protocol_number=4", candidates[0].reasons)

    def test_scores_anenji_11kw_protocol_6_within_guard_range(self) -> None:
        candidates = score_smg_identity_candidates(_anenji_11kw_protocol_6_evidence())

        self.assertEqual(candidates[0].variant_key, "anenji_anj_11kw_48v_wifi_p")
        self.assertEqual(candidates[0].confidence, "high")
        self.assertIn("required_anchor:protocol_number=6", candidates[0].reasons)

    def test_scores_unknown_smg_like_evidence_as_read_only_family_fallback(self) -> None:
        candidates = score_smg_identity_candidates(_family_fallback_evidence())

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].variant_key, "family_fallback")
        self.assertEqual(candidates[0].confidence, "medium")
        self.assertTrue(candidates[0].read_only)
        self.assertTrue(candidates[0].provisional)
        self.assertIn("family_fallback_variant", candidates[0].reasons)
        self.assertIn("read_only_variant", candidates[0].reasons)

    def test_unknown_enum_values_do_not_promote_to_specific_variant(self) -> None:
        candidates = score_smg_identity_candidates(
            SmgIdentityEvidence(
                protocol_family="modbus_smg",
                anchors={
                    "serial": "SMG11K240001",
                    "protocol_number": "Unknown (7)",
                    "device_type": "Unknown (4660)",
                    "rated_power": 11000,
                },
            )
        )

        self.assertEqual(candidates[0].variant_key, "family_fallback")
        self.assertEqual(candidates[0].confidence, "medium")
        self.assertNotIn("confirmed_variant", candidates[0].reasons)

    def test_anenji_11kw_unknown_turn_on_mode_falls_back(self) -> None:
        evidence = _anenji_11kw_evidence()
        candidates = score_smg_identity_candidates(
            SmgIdentityEvidence(
                protocol_family=evidence.protocol_family,
                anchors={
                    **evidence.anchors,
                    "turn_on_mode": "Unknown (7)",
                },
            )
        )

        self.assertEqual(candidates[0].variant_key, "family_fallback")
        self.assertNotIn("confirmed_variant", candidates[0].reasons)

    def test_anenji_11kw_missing_pv_grid_connected_max_power_falls_back(self) -> None:
        evidence = _anenji_11kw_evidence()
        anchors = dict(evidence.anchors)
        anchors.pop("pv_grid_connected_max_power", None)
        candidates = score_smg_identity_candidates(
            SmgIdentityEvidence(
                protocol_family=evidence.protocol_family,
                anchors=anchors,
            )
        )

        self.assertEqual(candidates[0].variant_key, "family_fallback")
        self.assertNotIn("confirmed_variant", candidates[0].reasons)

    def test_anenji_4200_unknown_remote_switch_falls_back(self) -> None:
        evidence = _anenji_4200_evidence()
        candidates = score_smg_identity_candidates(
            SmgIdentityEvidence(
                protocol_family=evidence.protocol_family,
                anchors={
                    **evidence.anchors,
                    "remote_switch": "Unknown (9)",
                },
            )
        )

        self.assertEqual(candidates[0].variant_key, "family_fallback")
        self.assertNotIn("unverified_variant", candidates[0].reasons)

    def test_non_smg_family_does_not_receive_fallback(self) -> None:
        candidates = score_smg_identity_candidates(
            SmgIdentityEvidence(
                protocol_family="other_family",
                anchors={
                    "protocol_number": 1,
                    "device_type": 0x1E00,
                    "rated_power": 6200,
                },
            )
        )

        self.assertEqual(candidates, ())

    def test_scores_synthetic_catalog_only_variant_without_scoring_code_changes(self) -> None:
        clear_smg_identity_rule_catalog_cache()
        synthetic_catalog = {
            "protocol_family": "modbus_smg",
            "rules": [
                {
                    "variant_key": "synthetic_catalog_variant",
                    "confidence": "medium",
                    "read_only": True,
                    "provisional": True,
                    "required": [
                        {
                            "anchor_key": "protocol_number",
                            "equals": 9,
                        },
                        {
                            "anchor_key": "pv_grid_connected_max_power",
                            "min": 200,
                            "max": 20000,
                        },
                    ],
                    "preferred": ["serial"],
                },
                {
                    "variant_key": "family_fallback",
                    "confidence": "medium",
                    "read_only": True,
                    "provisional": True,
                    "family_only": True,
                    "required": [],
                    "preferred": ["serial", "protocol_number"],
                },
            ],
        }

        with patch(
            "custom_components.eybond_local.metadata.smg_identity_rules.json.loads",
            return_value=synthetic_catalog,
        ):
            candidates = score_smg_identity_candidates(
                SmgIdentityEvidence(
                    protocol_family="modbus_smg",
                    anchors={
                        "serial": "SYNTH9K240001",
                        "protocol_number": 9,
                        "pv_grid_connected_max_power": 9000,
                    },
                )
            )

        self.assertEqual(candidates[0].variant_key, "synthetic_catalog_variant")
        self.assertEqual(candidates[0].confidence, "medium")
        self.assertTrue(candidates[0].read_only)
        self.assertTrue(candidates[0].provisional)
        self.assertIn("required_anchor:protocol_number=9", candidates[0].reasons)


if __name__ == "__main__":
    unittest.main()