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
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"
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
                profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
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
        # Proof means metadata from a managed root: "builtin" (shipped) or "external"
        # (the integration-managed overlay roots under the HA config dir). Anything
        # else -- an unmanaged absolute path or an unknown/empty scope -- stays
        # fail-closed for verified writes.
        capability = self.smg_profile.get_capability("charge_source_priority")

        self.assertFalse(
            capability_write_exposure_allowed(
                capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="absolute",
                profile_name="modbus_smg/default.json",
            )
        )
        self.assertFalse(
            capability_write_exposure_allowed(
                capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="",
                schema_source_scope="",
                profile_name="modbus_smg/default.json",
            )
        )
        # Managed external metadata (device-scoped overlays extending the proven
        # built-in base) now satisfies the proof.
        self.assertTrue(
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

    def test_verified_builtin_control_stays_exposed_under_active_device_overlay(self) -> None:
        # Regression: activating a device-scoped overlay makes the effective metadata
        # "external", which fails _has_confirmed_local_metadata_proof and would suppress
        # EVERY verified built-in control -- the whole inverter's settings went unavailable
        # the moment one learned control was activated. A device-scoped overlay extends the
        # proven built-in base, so its built-in verified controls remain exposable.
        capability = self.smg_profile.get_capability("charge_source_priority")

        self.assertTrue(
            capability_write_exposure_allowed(
                capability,
                control_mode="auto",
                detection_confidence="high",
                variant_key="default",
                profile_source_scope="external",
                schema_source_scope="external",
                profile_name="learned/shadow_learning/example/profile.json",
                device_scoped_overlay_active=True,
            )
        )
        # Managed "external" metadata is now trusted as proof even without the
        # overlay-activation flag (it lives in the integration-managed roots and
        # extends the proven built-in base).
        self.assertTrue(
            capability_write_exposure_allowed(
                capability,
                control_mode="auto",
                detection_confidence="high",
                variant_key="default",
                profile_source_scope="external",
                schema_source_scope="external",
                profile_name="modbus_smg/default.json",
                device_scoped_overlay_active=False,
            )
        )
        # An unmanaged metadata source without an active overlay still fails the
        # proof and keeps verified writes suppressed.
        self.assertFalse(
            capability_write_exposure_allowed(
                capability,
                control_mode="auto",
                detection_confidence="high",
                variant_key="default",
                profile_source_scope="absolute",
                schema_source_scope="absolute",
                profile_name="modbus_smg/default.json",
                device_scoped_overlay_active=False,
            )
        )

    def test_verified_presets_require_confirmed_local_metadata_proof(self) -> None:
        capabilities_by_key = {cap.key: cap for cap in self.smg_profile.capabilities}
        preset = next(
            item for item in self.smg_profile.presets if item.key == "off_grid_self_consumption"
        )

        # An unmanaged/unknown metadata source still blocks verified presets.
        self.assertFalse(
            preset_write_exposure_allowed(
                preset,
                capabilities_by_key=capabilities_by_key,
                control_mode="full",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="absolute",
                profile_name="modbus_smg/default.json",
            )
        )
        # Managed external metadata now satisfies the proof for presets too.
        self.assertTrue(
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

    def test_device_scoped_learned_capability_can_be_exposed_on_family_fallback_when_activated(self) -> None:
        learned_capability = WriteCapability(
            key="learned_shadow_705",
            register=705,
            value_kind="u16",
            note="learned",
            tested=False,
            provenance="cloud_hint",
            experimental=True,
            metadata_scope="device",
        )

        self.assertTrue(
            capability_write_exposure_allowed(
                learned_capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="family_fallback",
                profile_source_scope="external",
                schema_source_scope="external",
                profile_name="learned/shadow_learning/example/profile.json",
                device_scoped_overlay_active=True,
            )
        )

    def _learned_capability(self, key: str = "learned_shadow_705", register: int = 705) -> WriteCapability:
        return WriteCapability(
            key=key,
            register=register,
            value_kind="u16",
            note="learned",
            tested=False,
            provenance="cloud_hint",
            experimental=True,
            metadata_scope="device",
        )

    def _allowed(self, capability: WriteCapability, **overrides) -> bool:
        kwargs = dict(
            control_mode="full",
            detection_confidence="high",
            variant_key="family_fallback",
            profile_source_scope="external",
            schema_source_scope="external",
            profile_name="learned/shadow_learning/example/profile.json",
            device_scoped_overlay_active=True,
        )
        kwargs.update(overrides)
        return capability_write_exposure_allowed(capability, **kwargs)

    def test_selected_device_scoped_capability_is_exposed(self) -> None:
        capability = self._learned_capability()

        self.assertTrue(
            self._allowed(capability, selected_control_keys=frozenset({"learned_shadow_705"}))
        )

    def test_selected_device_scoped_capability_is_exposed_under_auto_control_mode(self) -> None:
        # Regression: the device runs control_mode="auto", and learned controls are
        # observed from cloud traffic (never write-tested, so ``tested=False``), so the
        # base control-mode gate withheld them under "auto" -- the activated, selected
        # control never appeared as an entity. The per-control activation is the explicit
        # opt-in, so it must be exposed under "auto" (and "full"), not only "full".
        capability = self._learned_capability()

        self.assertTrue(
            self._allowed(
                capability,
                control_mode="auto",
                selected_control_keys=frozenset({"learned_shadow_705"}),
            )
        )

    def test_selected_device_scoped_capability_is_blocked_under_read_only(self) -> None:
        # Read-only is a deliberate "no writes at all" mode and still suppresses even an
        # activated, selected learned control.
        capability = self._learned_capability()

        self.assertFalse(
            self._allowed(
                capability,
                control_mode="read_only",
                selected_control_keys=frozenset({"learned_shadow_705"}),
            )
        )

    def test_unselected_device_scoped_capability_is_blocked(self) -> None:
        capability = self._learned_capability()

        # A selection is declared but does not include this control's key.
        self.assertFalse(
            self._allowed(capability, selected_control_keys=frozenset({"learned_shadow_999"}))
        )

    def test_empty_selection_blocks_all_device_scoped_capabilities(self) -> None:
        capability = self._learned_capability()

        # An explicit empty selection exposes nothing (fail-closed).
        self.assertFalse(self._allowed(capability, selected_control_keys=frozenset()))

    def test_legacy_activation_without_selection_exposes_learned_capability(self) -> None:
        capability = self._learned_capability()

        # ``None`` selection means a legacy activation that predates selected-control
        # activation: every learned control stays exposed for backward compatibility.
        self.assertTrue(self._allowed(capability, selected_control_keys=None))

    def test_selection_does_not_affect_verified_model_capability(self) -> None:
        verified_capability = self.smg_profile.get_capability("charge_source_priority")

        # A verified, locally-proven capability is not a device-scoped learned control, so
        # the selection filter must never gate it — even an empty selection leaves it exposed.
        self.assertTrue(
            capability_write_exposure_allowed(
                verified_capability,
                control_mode="auto",
                detection_confidence="high",
                variant_key="smg_6200",
                profile_source_scope="builtin",
                schema_source_scope="builtin",
                profile_name="modbus_smg/default.json",
                device_scoped_overlay_active=True,
                selected_control_keys=frozenset(),
            )
        )

    def test_non_device_scoped_cloud_hint_capability_stays_blocked_on_family_fallback(self) -> None:
        non_scoped_capability = WriteCapability(
            key="learned_shadow_705",
            register=705,
            value_kind="u16",
            note="learned",
            tested=False,
            provenance="cloud_hint",
            experimental=True,
            metadata_scope="",
        )

        self.assertFalse(
            capability_write_exposure_allowed(
                non_scoped_capability,
                control_mode="full",
                detection_confidence="high",
                variant_key="family_fallback",
                profile_source_scope="external",
                schema_source_scope="external",
                profile_name="learned/shadow_learning/example/profile.json",
                device_scoped_overlay_active=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
