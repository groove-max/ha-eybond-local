from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.control_policy import (
    can_expose_capability,
    can_expose_preset,
    controls_enabled,
    controls_reason,
)
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile


class ControlPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = load_driver_profile("smg_modbus.json")

    def test_auto_mode_requires_high_confidence_for_controls(self) -> None:
        tested_capability = self.profile.get_capability("charge_source_priority")
        untested_capability = self.profile.get_capability("power_saving_mode")

        self.assertTrue(
            can_expose_capability(
                tested_capability,
                control_mode="auto",
                detection_confidence="high",
            )
        )
        self.assertFalse(
            can_expose_capability(
                untested_capability,
                control_mode="auto",
                detection_confidence="high",
            )
        )
        self.assertFalse(
            can_expose_capability(
                tested_capability,
                control_mode="auto",
                detection_confidence="medium",
            )
        )

    def test_control_mode_full_overrides_confidence(self) -> None:
        self.assertTrue(controls_enabled(control_mode="full", detection_confidence="none"))
        self.assertEqual(
            controls_reason(control_mode="full", detection_confidence="none"),
            "manual_full_override",
        )

    def test_preset_exposure_requires_all_items_tested(self) -> None:
        capabilities_by_key = {cap.key: cap for cap in self.profile.capabilities}
        safe_preset = next(
            preset for preset in self.profile.presets if preset.key == "off_grid_self_consumption"
        )
        mixed_preset = next(
            preset for preset in self.profile.presets if preset.key == "hybrid_pv_with_grid_backup"
        )

        self.assertTrue(
            can_expose_preset(
                safe_preset,
                capabilities_by_key=capabilities_by_key,
                control_mode="auto",
                detection_confidence="high",
            )
        )
        self.assertTrue(
            can_expose_preset(
                mixed_preset,
                capabilities_by_key=capabilities_by_key,
                control_mode="auto",
                detection_confidence="high",
            )
        )


if __name__ == "__main__":
    unittest.main()
