from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import WriteCapability
from custom_components.eybond_local.schema import (
    capability_write_exposure_allowed,
    preset_write_exposure_allowed,
)


class WriteExposurePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.smg_profile = load_driver_profile("smg_modbus.json")
        cls.anenji_profile = load_driver_profile(
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.variant.json"
        )

    def test_full_control_does_not_enable_family_fallback_writes(self) -> None:
        capability = self.smg_profile.get_capability("charge_source_priority")
        capabilities_by_key = {cap.key: cap for cap in self.smg_profile.capabilities}
        preset = next(
            item for item in self.smg_profile.presets if item.key == "off_grid_self_consumption"
        )

        self.assertFalse(
            capability_write_exposure_allowed(
                capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="family_fallback",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name="modbus_smg/family_fallback.json",
            )
        )
        self.assertFalse(
            preset_write_exposure_allowed(
                preset,
                capabilities_by_key=capabilities_by_key,
                control_mode="full",
                detection_confidence="high",
                variant_key="family_fallback",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name="modbus_smg/family_fallback.json",
            )
        )

    def test_confirmed_verified_smg_and_anenji_writes_stay_exposed(self) -> None:
        smg_capability = self.smg_profile.get_capability("charge_source_priority")
        anenji_capability = self.anenji_profile.get_capability("output_mode")

        self.assertTrue(
            capability_write_exposure_allowed(
                smg_capability,
                control_mode="auto",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name="modbus_smg/default.json",
            )
        )
        self.assertTrue(
            capability_write_exposure_allowed(
                anenji_capability,
                control_mode="auto",
                detection_confidence="high",
                variant_key="anenji_anj_11kw_48v_wifi_p",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.variant.json",
            )
        )

    def test_cloud_hint_capability_never_becomes_runtime_writable(self) -> None:
        cloud_hint_capability = WriteCapability(
            key="smartess_cloud_hint_only",
            register=699,
            value_kind="u16",
            note="cloud hint",
            tested=True,
            provenance="cloud_hint",
        )

        self.assertFalse(
            capability_write_exposure_allowed(
                cloud_hint_capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name="modbus_smg/default.json",
            )
        )

    def test_verified_writes_require_confirmed_local_metadata_proof(self) -> None:
        capability = self.smg_profile.get_capability("charge_source_priority")

        self.assertFalse(
            capability_write_exposure_allowed(
                capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="external",
                profile_name="modbus_smg/default.json",
            )
        )

    def test_verified_presets_require_confirmed_local_metadata_proof(self) -> None:
        capabilities_by_key = {cap.key: cap for cap in self.smg_profile.capabilities}
        preset = next(
            item for item in self.smg_profile.presets if item.key == "off_grid_self_consumption"
        )

        self.assertFalse(
            preset_write_exposure_allowed(
                preset,
                capabilities_by_key=capabilities_by_key,
                control_mode="full",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="external",
                profile_name="modbus_smg/default.json",
            )
        )


if __name__ == "__main__":
    unittest.main()
