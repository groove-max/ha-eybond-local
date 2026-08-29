from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.profile_loader import (
    clear_profile_loader_cache,
    load_driver_profile,
)
from custom_components.eybond_local.schema import capability_write_exposure_allowed


COMMON_REGISTERS = frozenset(
    {
        600,
        601,
        602,
        603,
        604,
        606,
        607,
        630,
        631,
        632,
        633,
        634,
        635,
        637,
        638,
        639,
        640,
        641,
        642,
        643,
        644,
        646,
        647,
        648,
        650,
        651,
        652,
        653,
        654,
        655,
        656,
        677,
        678,
        679,
        680,
        681,
        682,
        683,
        684,
        687,
        689,
        690,
        693,
        694,
        695,
        696,
        699,
        705,
        706,
        709,
        858,
        859,
    }
)
PROTOCOL_4_6_REGISTERS = frozenset({608, 609, 610, 611, 645, 649, 707})
INVALID_WITHOUT_EXACT_MODEL = frozenset({685, 686, 691, 692})
BLOCKED_CAPABILITIES = frozenset(
    {"clear_generation_data", "reset_user_parameters"}
)


class CommunicationProtocolWriteProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        clear_profile_loader_cache()

    def test_each_protocol_exposes_only_its_documented_register_matrix(self) -> None:
        for version in (3, 4, 5, 6):
            with self.subTest(version=version):
                profile = load_driver_profile(
                    f"modbus_smg/protocols/communication_protocol_{version}.json"
                )
                expected = COMMON_REGISTERS
                if version in (4, 6):
                    expected |= PROTOCOL_4_6_REGISTERS
                self.assertEqual(
                    {capability.register for capability in profile.capabilities},
                    expected,
                )
                self.assertTrue(
                    INVALID_WITHOUT_EXACT_MODEL.isdisjoint(
                        capability.register for capability in profile.capabilities
                    )
                )
                self.assertTrue(
                    all(not capability.tested for capability in profile.capabilities)
                )
                self.assertEqual(
                    {capability.provenance for capability in profile.capabilities},
                    {"doc_backed"},
                )
                self.assertEqual(
                    {
                        capability.key
                        for capability in profile.capabilities
                        if capability.resolved_support_tier == "blocked"
                    },
                    BLOCKED_CAPABILITIES,
                )

    def test_hhs_exact_model_gets_protocol_3_controls_as_untested(self) -> None:
        profile_name = (
            "modbus_smg/models/anenji_hhs_11kw_wifi_no_parallel.json"
        )
        profile = load_driver_profile(profile_name)

        self.assertEqual(
            {capability.register for capability in profile.capabilities},
            COMMON_REGISTERS,
        )
        self.assertTrue(all(not capability.tested for capability in profile.capabilities))
        self.assertFalse(
            profile.get_capability(
                "lithium_battery_automatic_activation_enabled"
            ).poll_readback
        )
        ordinary = profile.get_capability("output_source_priority")
        self.assertFalse(
            capability_write_exposure_allowed(
                ordinary,
                control_mode="auto",
                detection_confidence="high",
                variant_key="anenji_hhs_11kw_wifi_no_parallel",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name=profile_name,
            )
        )
        self.assertTrue(
            capability_write_exposure_allowed(
                ordinary,
                control_mode="full",
                detection_confidence="high",
                variant_key="anenji_hhs_11kw_wifi_no_parallel",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name=profile_name,
            )
        )
        self.assertFalse(
            capability_write_exposure_allowed(
                profile.get_capability("reset_user_parameters"),
                control_mode="full",
                detection_confidence="high",
                variant_key="anenji_hhs_11kw_wifi_no_parallel",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name=profile_name,
            )
        )

    def test_exact_model_overlays_preserve_only_existing_validation_evidence(self) -> None:
        anenji = load_driver_profile(
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"
        )
        self.assertEqual(len(anenji.capabilities), 63)
        self.assertEqual(
            {capability.key for capability in anenji.capabilities if not capability.tested},
            {
                "secondary_output_priority",
                "secondary_output_priority_start_time",
                "secondary_output_priority_end_time",
                "op2_output_enabled",
                "op2_output_start_hour",
                "op2_output_end_hour",
                "lithium_battery_automatic_activation_enabled",
                "lithium_battery_activation_once",
                "clear_generation_data",
                "reset_user_parameters",
                "op1_offgrid_low_voltage_protection",
                "secondary_charging_priority_start_time",
                "secondary_charging_priority_end_time",
            },
        )
        self.assertTrue(anenji.get_capability("op2_overload_alarm_setting").tested)
        self.assertTrue(anenji.get_capability("external_ct_enabled").tested)

        sandisolar = load_driver_profile(
            "modbus_smg/models/sandisolar_sd_11kp48v_wifi.json"
        )
        self.assertEqual(
            {capability.key for capability in sandisolar.capabilities if capability.tested},
            {"secondary_output_priority", "secondary_charging_priority"},
        )


if __name__ == "__main__":
    unittest.main()
