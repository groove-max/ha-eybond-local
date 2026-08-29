from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.profile_loader import (  # noqa: E402
    clear_profile_loader_cache,
    load_driver_profile,
)
from custom_components.eybond_local.schema import (  # noqa: E402
    capability_write_exposure_allowed,
)


CLASSIC_SMG_PROFILE = "modbus_smg/protocols/classic_smg_rs232_v1.json"
DOCUMENTED_WRITE_REGISTERS = frozenset(
    {
        300,
        301,
        302,
        303,
        305,
        306,
        307,
        308,
        309,
        310,
        313,
        320,
        321,
        323,
        324,
        325,
        326,
        327,
        329,
        331,
        332,
        333,
        334,
        335,
        336,
        337,
        406,
        420,
        426,
    }
)
DOCUMENTED_BUT_NOT_USER_CONTROLS = frozenset(
    {
        108,  # Warning word: write semantics are not defined by the document.
        304,
        311,
        312,
        314,
        315,
        316,
        317,
        318,
        319,
        322,
        328,
        330,
        338,
        339,
        340,
        425,
    }
)


class ClassicSmgWriteProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        clear_profile_loader_cache()

    def test_document_profile_contains_only_semantically_defined_writes(self) -> None:
        profile = load_driver_profile(CLASSIC_SMG_PROFILE)

        self.assertEqual(profile.key, "modbus_smg_classic_smg_rs232_v1")
        self.assertEqual(len(profile.capabilities), 30)
        self.assertEqual(
            {capability.register for capability in profile.capabilities},
            DOCUMENTED_WRITE_REGISTERS,
        )
        self.assertTrue(
            DOCUMENTED_BUT_NOT_USER_CONTROLS.isdisjoint(
                capability.register for capability in profile.capabilities
            )
        )
        self.assertEqual(
            {
                capability.key
                for capability in profile.capabilities
                if capability.register == 420
            },
            {"remote_turn_on", "remote_shutdown"},
        )
        self.assertTrue(all(not capability.tested for capability in profile.capabilities))
        self.assertEqual(
            {capability.provenance for capability in profile.capabilities},
            {"doc_backed"},
        )
        self.assertEqual(
            {capability.support_tier for capability in profile.capabilities},
            {"conditional"},
        )

    def test_classic_models_share_one_profile_without_a_legacy_base_alias(self) -> None:
        profiles_root = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "protocol_catalogs"
            / "profiles"
            / "modbus_smg"
        )
        self.assertFalse((profiles_root / "base.json").exists())

        expected_parents = {
            profiles_root / "default.json": (
                "modbus_smg/protocols/classic_smg_rs232_v1.json"
            ),
            profiles_root / "models" / "anenji_4200_protocol_1.json": (
                "../protocols/classic_smg_rs232_v1.json"
            ),
            profiles_root / "models" / "aninerel_anl_4200t_24l_w_pro.json": (
                "../protocols/classic_smg_rs232_v1.json"
            ),
            profiles_root / "models" / "smg_variant_4200.json": (
                "../protocols/classic_smg_rs232_v1.json"
            ),
        }
        for path, expected_parent in expected_parents.items():
            with self.subTest(path=path.name):
                raw = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(raw.get("extends"), expected_parent)

    def test_smg_6200_preserves_hardware_evidence_and_policy(self) -> None:
        profile_name = "modbus_smg/models/smg_6200.json"
        profile = load_driver_profile(profile_name)

        self.assertEqual(len(profile.capabilities), 38)
        self.assertEqual(sum(capability.tested for capability in profile.capabilities), 30)
        documented_verified = profile.get_capability("output_source_priority")
        self.assertTrue(documented_verified.tested)
        self.assertEqual(documented_verified.provenance, "doc_backed")

        for key, register in {
            "beeps_while_primary_source_interrupted": 304,
            "battery_type": 322,
            "constant_voltage_to_float_time": 330,
            "automatic_mains_output_enabled": 338,
            "forced_equalization_charging": 425,
        }.items():
            with self.subTest(key=key):
                capability = profile.get_capability(key)
                self.assertEqual(capability.register, register)
                self.assertTrue(capability.tested)
                self.assertEqual(capability.provenance, "verified")

        self.assertTrue(
            self._allowed(
                documented_verified,
                profile_name=profile_name,
                control_mode="auto",
            )
        )
        untested = profile.get_capability("output_mode")
        self.assertFalse(
            self._allowed(untested, profile_name=profile_name, control_mode="auto")
        )
        self.assertTrue(
            self._allowed(untested, profile_name=profile_name, control_mode="full")
        )
        for key in ("power_saving_mode", "overload_bypass_mode"):
            self.assertFalse(
                self._allowed(
                    profile.get_capability(key),
                    profile_name=profile_name,
                    control_mode="full",
                )
            )

    def test_all_classic_model_overlays_preserve_validation_counts(self) -> None:
        expected = {
            "modbus_smg/default.json": (33, 25, 2),
            "modbus_smg/models/smg_6200.json": (38, 30, 2),
            "modbus_smg/models/anenji_4200_protocol_1.json": (30, 0, 0),
            "modbus_smg/models/smg_variant_4200.json": (43, 21, 0),
            "modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json": (30, 22, 0),
            "modbus_smg/models/anenji_op2_6200.json": (37, 25, 2),
            "modbus_smg/models/anenji_anj_5kw_48v_wifi.json": (40, 40, 0),
        }
        for profile_name, counts in expected.items():
            with self.subTest(profile_name=profile_name):
                profile = load_driver_profile(profile_name)
                self.assertEqual(
                    (
                        len(profile.capabilities),
                        sum(item.tested for item in profile.capabilities),
                        sum(
                            item.resolved_support_tier == "blocked"
                            for item in profile.capabilities
                        ),
                    ),
                    counts,
                )

    def test_unvalidated_exact_model_requires_full_control(self) -> None:
        profile_name = "modbus_smg/models/anenji_4200_protocol_1.json"
        profile = load_driver_profile(profile_name)
        capability = profile.get_capability("output_source_priority")

        self.assertEqual(
            {item.register for item in profile.capabilities},
            DOCUMENTED_WRITE_REGISTERS,
        )
        self.assertTrue(all(not item.tested for item in profile.capabilities))
        self.assertEqual(
            {item.provenance for item in profile.capabilities},
            {"doc_backed"},
        )
        self.assertFalse(
            self._allowed(capability, profile_name=profile_name, control_mode="auto")
        )
        self.assertTrue(
            self._allowed(capability, profile_name=profile_name, control_mode="full")
        )

    def test_test_stand_fingerprint_binds_exact_smg_6200_surface(self) -> None:
        fixture = json.loads(
            (
                REPO_ROOT
                / "tests"
                / "fixtures"
                / "smg_6200_maintainer_identity.json"
            ).read_text(encoding="utf-8")
        )
        registers = {int(key): int(value) for key, value in fixture["registers"].items()}
        self.assertEqual(
            (registers[171], registers[184], registers[643]),
            (0x1E00, 1, 6200),
        )

        catalog = json.loads(
            (
                REPO_ROOT
                / "custom_components"
                / "eybond_local"
                / "protocol_catalogs"
                / "inverter_catalog.json"
            ).read_text(encoding="utf-8")
        )
        device = next(
            item
            for item in catalog["devices"]
            if item["entry_key"] == fixture["expected"]["entry_key"]
        )
        self.assertEqual(device["fingerprint"]["layout_code"], 1)
        self.assertEqual(device["fingerprint"]["model_code"], 0x1E00)
        self.assertEqual(device["fingerprint"]["rated_power_one_of"], [6200])
        surface = next(
            item for item in catalog["surfaces"] if item["key"] == device["surface_key"]
        )
        self.assertEqual(surface["key"], fixture["expected"]["surface_key"])
        self.assertEqual(surface["profile_name"], fixture["expected"]["profile_name"])

        fallback = next(
            item for item in catalog["surfaces"] if item["key"] == "smg_family_read_only"
        )
        self.assertTrue(fallback["read_only"])
        self.assertEqual(fallback["profile_name"], "")

    @staticmethod
    def _allowed(capability, *, profile_name: str, control_mode: str) -> bool:
        return capability_write_exposure_allowed(
            capability,
            control_mode=control_mode,
            detection_confidence="high",
            variant_key="classic_smg_test",
            profile_source_scope="builtin",
            schema_source_scope="builtin",
            profile_name=profile_name,
        )


if __name__ == "__main__":
    unittest.main()
