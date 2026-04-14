from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.pi_family import (
    build_pi_model_name,
    classify_pi_protocol,
    resolve_pi_identity,
    resolve_pi30_metadata_names,
)


class PiFamilyTests(unittest.TestCase):
    def test_classify_pi_protocol_maps_known_families(self) -> None:
        self.assertEqual(classify_pi_protocol("PI16"), "pi16")
        self.assertEqual(classify_pi_protocol("pi18"), "pi18")
        self.assertEqual(classify_pi_protocol("PI30"), "pi30")
        self.assertEqual(classify_pi_protocol("PI41"), "pi41")
        self.assertIsNone(classify_pi_protocol("ABC"))

    def test_build_pi_model_name_prefers_model_number(self) -> None:
        self.assertEqual(
            build_pi_model_name("PI30", {"model_number": "VMII-NXPW5KW", "output_rating_active_power": 5000}),
            "VMII-NXPW5KW",
        )

    def test_build_pi_model_name_falls_back_to_protocol_and_power(self) -> None:
        self.assertEqual(
            build_pi_model_name("PI30", {"output_rating_active_power": 4200}),
            "PI30 4200",
        )

    def test_resolve_pi_identity_detects_pi30_vmii_variant(self) -> None:
        identity = resolve_pi_identity(
            "PI30",
            {"protocol_id": "PI30", "model_number": "VMII-NXPW5KW", "output_rating_active_power": 5000},
        )

        assert identity is not None
        self.assertEqual(identity.family_key, "pi30")
        self.assertEqual(identity.model_name, "VMII-NXPW5KW")
        self.assertEqual(identity.variant_key, "vmii_nxpw5kw")

    def test_resolve_pi30_metadata_names_uses_vmii_overlay(self) -> None:
        names = resolve_pi30_metadata_names(
            {"protocol_id": "PI30", "model_number": "VMII-NXPW5KW"},
            "VMII-NXPW5KW",
            default_profile_name="pi30_ascii/models/default.json",
            default_register_schema_name="pi30_ascii/models/default.json",
        )

        self.assertEqual(names.profile_name, "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(names.register_schema_name, "pi30_ascii/models/vmii_nxpw5kw.json")

    def test_resolve_pi30_metadata_names_uses_pi41_overlay(self) -> None:
        names = resolve_pi30_metadata_names(
            {"protocol_id": "PI41", "output_rating_active_power": 5000},
            "PI41 5000",
            default_profile_name="pi30_ascii/models/default.json",
            default_register_schema_name="pi30_ascii/models/default.json",
        )

        self.assertEqual(names.profile_name, "pi30_ascii/models/pi41.json")
        self.assertEqual(names.register_schema_name, "pi30_ascii/models/pi41.json")

    def test_resolve_pi30_metadata_names_uses_pi30_max_overlay(self) -> None:
        names = resolve_pi30_metadata_names(
            {"protocol_id": "PI30", "qpiri_field_count": 28},
            "PI30 5000",
            default_profile_name="pi30_ascii/models/default.json",
            default_register_schema_name="pi30_ascii/models/default.json",
        )

        self.assertEqual(names.profile_name, "pi30_ascii/models/pi30_max.json")
        self.assertEqual(names.register_schema_name, "pi30_ascii/models/pi30_max.json")

    def test_resolve_pi30_metadata_names_uses_pip_gk_overlay(self) -> None:
        names = resolve_pi30_metadata_names(
            {
                "protocol_id": "PI30",
                "operating_mode_code": "E",
                "qpiri_field_count": 25,
                "qpigs_field_count": 21,
                "qpiws_bit_count": 36,
            },
            "PI30 5000",
            default_profile_name="pi30_ascii/models/default.json",
            default_register_schema_name="pi30_ascii/models/default.json",
        )

        self.assertEqual(names.profile_name, "pi30_ascii/models/pi30_pip_gk.json")
        self.assertEqual(names.register_schema_name, "pi30_ascii/models/pi30_pip_gk.json")

    def test_resolve_pi30_metadata_names_falls_back_to_defaults(self) -> None:
        names = resolve_pi30_metadata_names(
            {"protocol_id": "PI30", "model_number": "MKS2-4200"},
            "MKS2-4200",
            default_profile_name="pi30_ascii/models/default.json",
            default_register_schema_name="pi30_ascii/models/default.json",
        )

        self.assertEqual(names.profile_name, "pi30_ascii/models/default.json")
        self.assertEqual(names.register_schema_name, "pi30_ascii/models/default.json")

    def test_resolve_pi_identity_detects_pi30_max_from_qpiri_or_qflag(self) -> None:
        identity = resolve_pi_identity(
            "PI30",
            {
                "protocol_id": "PI30",
                "qpiri_field_count": 28,
                "capability_flags_enabled": "adz",
            },
        )

        assert identity is not None
        self.assertEqual(identity.variant_key, "pi30_max")

    def test_resolve_pi_identity_detects_pip_gk_from_probe_facts(self) -> None:
        identity = resolve_pi_identity(
            "PI30",
            {
                "protocol_id": "PI30",
                "operating_mode_code": "E",
                "qpiri_field_count": 25,
                "qpigs_field_count": 21,
                "qpiws_bit_count": 36,
            },
        )

        assert identity is not None
        self.assertEqual(identity.variant_key, "pi30_pip_gk")

    def test_resolve_pi_identity_detects_pi41_from_protocol_id(self) -> None:
        identity = resolve_pi_identity(
            "PI41",
            {"protocol_id": "PI41", "output_rating_active_power": 5000},
        )

        assert identity is not None
        self.assertEqual(identity.family_key, "pi41")
        self.assertEqual(identity.variant_key, "pi41")


if __name__ == "__main__":
    unittest.main()