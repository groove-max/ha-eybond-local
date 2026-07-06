from __future__ import annotations

import dataclasses
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata import device_catalog_loader  # noqa: E402
from custom_components.eybond_local.metadata.compiled_detection_catalog import (  # noqa: E402
    RESOLUTION_EXACT,
    RESOLUTION_FAMILY,
    RESOLUTION_UNRESOLVED,
    load_compiled_detection_catalog,
)
from custom_components.eybond_local.metadata.profile_loader import builtin_profile_path  # noqa: E402
from custom_components.eybond_local.metadata.device_catalog_loader import (  # noqa: E402
    FORCE_UNSUPPORTED_SENTINEL_NAME,
    TIER_PARTIAL,
    clear_device_catalog_cache,
    force_unsupported_models,
    load_device_catalog,
    refresh_force_unsupported_override,
    resolve_catalog_surface_binding,
    resolve_runtime_probe_policy,
    resolve_support_capture_policy,
    serial_ascii_plausible,
)


COMPONENT_ROOT = REPO_ROOT / "custom_components" / "eybond_local"



def _tree_resolve(catalog, *, protocol_key, evidence):
    """Resolve a FIXED evidence dict through the decision tree.

    Replaces the retired value-based CompiledDetectionCatalog.resolve():
    these tests guard catalog-data integrity, and the tree is the single
    production resolution engine.
    """

    from custom_components.eybond_local.metadata.detection_decision_tree import (
        evaluate_detection_decision_tree_static,
    )

    evaluation = evaluate_detection_decision_tree_static(
        catalog.decision_trees[protocol_key], evidence
    )
    candidate_keys = (
        evaluation.candidate_keys
        if evaluation.status in {"resolved", "ambiguous"}
        else ()
    )
    return catalog.resolution_for_candidates(
        protocol_key=protocol_key,
        candidate_keys=candidate_keys,
        evidence=evidence,
    )

class CatalogValidationGuardTest(unittest.TestCase):
    """The source catalog rejects ambiguity at load, never resolves it by order."""

    def test_ambiguous_fingerprint_is_rejected(self) -> None:
        # Two fingerprint-based devices sharing (layout_code, model_code) is
        # ambiguous and must raise at validation rather than be resolved by
        # catalog order (Acceptance Criterion: ambiguity is explicit).
        catalog = load_device_catalog()
        base = next(device for device in catalog.devices if not device.anchors)
        duplicate = dataclasses.replace(base, entry_key=f"{base.entry_key}_dup")
        bad = dataclasses.replace(catalog, devices=catalog.devices + (duplicate,))

        with self.assertRaises(ValueError) as ctx:
            device_catalog_loader._validate_device_catalog(bad)
        self.assertIn("ambiguous_fingerprint", str(ctx.exception))

    def test_overlapping_family_defaults_are_rejected(self) -> None:
        # Two family defaults claiming an overlapping layout code must raise.
        catalog = load_device_catalog()
        self.assertTrue(catalog.family_defaults)
        first = catalog.family_defaults[0]
        overlapping = dataclasses.replace(first)  # identical when_layout_codes
        bad = dataclasses.replace(catalog, family_defaults=(first, overlapping))

        with self.assertRaises(ValueError) as ctx:
            device_catalog_loader._validate_device_catalog(bad)
        self.assertIn("overlapping_family_defaults", str(ctx.exception))


class TierValidationTest(unittest.TestCase):
    def test_invalid_tier_string_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            device_catalog_loader._validate_tier("x", "Full", "p.json")

    def test_full_tier_requires_a_controls_profile(self) -> None:
        with self.assertRaises(ValueError):
            device_catalog_loader._validate_tier("x", "full", "")
        self.assertEqual(device_catalog_loader._validate_tier("x", "full", "p.json"), "full")

    def test_partial_tier_must_not_carry_a_profile(self) -> None:
        with self.assertRaises(ValueError):
            device_catalog_loader._validate_tier("x", "partial", "p.json")
        self.assertEqual(device_catalog_loader._validate_tier("x", "partial", ""), "partial")

    def test_shipped_catalog_satisfies_the_tier_invariant(self) -> None:
        clear_device_catalog_cache()
        self.addCleanup(clear_device_catalog_cache)
        catalog = load_device_catalog()
        for entry in catalog.devices:
            self.assertIn(entry.tier, ("full", "partial"))
            if entry.tier == "full":
                self.assertTrue(entry.binding.profile_name)
            else:
                self.assertEqual(entry.binding.profile_name, "")


class DeviceCatalogLoadTest(unittest.TestCase):
    def setUp(self) -> None:
        clear_device_catalog_cache()
        self.addCleanup(clear_device_catalog_cache)

    def test_catalog_loads_with_expected_structure(self) -> None:
        catalog = load_device_catalog()
        self.assertEqual(catalog.schema_version, 2)
        self.assertTrue(catalog.catalog_version)
        self.assertIn("modbus_smg", catalog.protocols)
        self.assertIn("eybond_modbus", catalog.transports)
        self.assertGreaterEqual(len(catalog.layouts), 2)
        self.assertGreaterEqual(len(catalog.surfaces), 4)
        self.assertGreaterEqual(len(catalog.devices), 4)
        self.assertGreaterEqual(len(catalog.family_defaults), 1)

    def test_identity_probe_covers_fingerprint_fields(self) -> None:
        catalog = load_device_catalog()
        protocol = catalog.protocols["modbus_smg"]
        self.assertEqual(
            tuple(action.key for action in protocol.probe_actions),
            (
                "modbus_smg.identity.171",
                "modbus_smg.identity.186",
                "modbus_smg.identity.643",
            ),
        )
        self.assertTrue(all(action.kind == "modbus_read" for action in protocol.probe_actions))

        probe = catalog.transports["eybond_modbus"]
        self.assertEqual(probe.fields["layout_code"].register, 184)
        self.assertEqual(probe.fields["model_code"].register, 171)
        self.assertEqual(probe.fields["rated_power"].register, 643)
        self.assertEqual(probe.fields["serial_ascii"].register, 186)
        covered = set()
        for start, count in probe.read_blocks:
            covered.update(range(start, start + count))
        for field in ("layout_code", "model_code", "rated_power"):
            self.assertIn(probe.fields[field].register, covered)

    def test_every_referenced_payload_file_exists(self) -> None:
        catalog = load_device_catalog()
        bindings = [entry.binding for entry in catalog.devices]
        bindings.extend(default.binding for default in catalog.family_defaults)
        for binding in bindings:
            if binding.register_schema_name:
                path = COMPONENT_ROOT / "protocol_catalogs" / "register_schemas" / binding.register_schema_name
                self.assertTrue(path.is_file(), f"missing schema payload: {path}")
            if binding.profile_name:
                path = builtin_profile_path(binding.profile_name)
                self.assertTrue(path.is_file(), f"missing profile payload: {path}")
        for layout in catalog.layouts:
            if layout.base_schema:
                path = COMPONENT_ROOT / "protocol_catalogs" / "register_schemas" / layout.base_schema
                self.assertTrue(path.is_file(), f"missing layout base schema: {path}")

    def test_fingerprints_are_unique(self) -> None:
        catalog = load_device_catalog()
        seen: set[tuple[int, int, tuple[int, ...]]] = set()
        for entry in catalog.devices:
            if entry.anchors:
                continue
            key = (
                entry.fingerprint.layout_code,
                entry.fingerprint.model_code,
                entry.fingerprint.rated_power_one_of,
            )
            self.assertNotIn(key, seen, f"duplicate fingerprint: {entry.entry_key}")
            seen.add(key)

    def test_writes_locked_outside_device_entries(self) -> None:
        catalog = load_device_catalog()
        for default in catalog.family_defaults:
            self.assertEqual(default.binding.profile_name, "")
            self.assertEqual(default.tier, TIER_PARTIAL)
            self.assertTrue(default.model_name)

    def test_catalog_models_own_runtime_probe_policy(self) -> None:
        catalog = load_device_catalog()
        smg_6200 = next(entry for entry in catalog.devices if entry.entry_key == "smg_6200")
        validation = {rule.key: rule for rule in smg_6200.runtime_probe.validation}
        self.assertEqual(validation["rated_power"].one_of, (6200,))
        optional = {
            item.key: (item.register, item.word_count)
            for item in smg_6200.runtime_probe.optional_registers
        }
        self.assertEqual(optional["max_discharge_current_protection"], (351, 1))
        ascii_ranges = {
            item.key: (item.register, item.word_count)
            for item in smg_6200.runtime_probe.optional_ascii
        }
        self.assertEqual(ascii_ranges["program_version"], (626, 8))

        anenji_11kw = next(entry for entry in catalog.devices if entry.entry_key == "anenji_anj_11kw")
        validation = {rule.key: rule for rule in anenji_11kw.runtime_probe.validation}
        self.assertEqual(validation["protocol_number"].one_of, (3, 4, 5, 6))
        self.assertEqual(validation["pv_grid_connected_max_power"].min_value, 200)
        self.assertEqual(validation["pv_grid_connected_max_power"].max_value, 20000)

    def test_catalog_surfaces_own_runtime_and_support_metadata(self) -> None:
        catalog = load_device_catalog()
        surface = catalog.surfaces["smg_6200_full"]

        self.assertTrue(surface.default_for_driver)
        self.assertFalse(surface.read_only)
        self.assertEqual(surface.binding.profile_name, "modbus_smg/models/smg_6200.json")
        self.assertIn((700, 45), surface.support_capture.ranges)
        self.assertEqual(
            resolve_catalog_surface_binding("modbus_smg"),
            surface.binding,
        )
        self.assertEqual(
            resolve_support_capture_policy(
                driver_key="modbus_smg",
                variant_key=surface.binding.variant_key,
                profile_name=surface.binding.profile_name,
                register_schema_name=surface.binding.register_schema_name,
            ),
            surface.support_capture,
        )

    def test_runtime_probe_policy_resolves_by_binding(self) -> None:
        policy = resolve_runtime_probe_policy(
            driver_key="modbus_smg",
            variant_key="default",
            profile_name="modbus_smg/models/smg_6200.json",
            register_schema_name="modbus_smg/models/smg_6200.json",
        )

        self.assertIn(
            "rated_power",
            {rule.key for rule in policy.validation},
        )
        self.assertIn(
            "device_type",
            {item.key for item in policy.optional_registers},
        )


class CompiledDeviceCatalogCorpusTest(unittest.TestCase):
    def test_every_smg_fingerprint_resolves_to_its_descriptor(self) -> None:
        source = load_device_catalog()
        compiled = load_compiled_detection_catalog()
        for entry in source.devices:
            if entry.anchors:
                continue
            evidence = {
                "fingerprint.layout_code": entry.fingerprint.layout_code,
                "fingerprint.model_code": entry.fingerprint.model_code,
            }
            if entry.fingerprint.rated_power_one_of:
                evidence["fingerprint.rated_power"] = entry.fingerprint.rated_power_one_of[0]
            result = _tree_resolve(compiled, protocol_key="modbus_smg", evidence=evidence)
            self.assertEqual(result.resolution, RESOLUTION_EXACT, entry.entry_key)
            self.assertEqual(result.candidate_keys, (entry.entry_key,))
            self.assertEqual(result.surface_key, entry.surface_key)

    def test_aninerel_anl_4200t_binds_full_surface_with_confirmed_controls(self) -> None:
        # Field history (0.3.0-beta.1): the model resolved exactly but sat on
        # the read-only family surface with no controls; it then shipped the
        # SMG control set as untested, and the tester confirmed writes.
        from custom_components.eybond_local.metadata.profile_loader import (
            load_driver_profile,
        )

        catalog = load_compiled_detection_catalog()
        surface = catalog.surfaces["aninerel_anl_4200t_24l_w_pro_full"]
        self.assertFalse(surface.read_only)
        profile = load_driver_profile(surface.profile_name)
        self.assertTrue(profile.capabilities)
        # The tester confirmed writes on 2026-07-05: the family-proven set is
        # tested (exposed in auto mode); the family's unverified extras stay
        # untested (full-control only).
        tested = {c.key for c in profile.capabilities if c.tested}
        untested = {c.key for c in profile.capabilities if not c.tested}
        self.assertIn("output_source_priority", tested)
        self.assertIn("battery_bulk_voltage", tested)
        self.assertIn("charge_source_priority", tested)
        self.assertEqual(
            untested,
            {
                "exit_fault_mode",
                "output_mode",
                "output_rating_frequency",
                "output_rating_voltage",
                "overload_bypass_mode",
                "power_saving_mode",
                "remote_shutdown",
                "remote_turn_on",
            },
        )

    def test_smg_variant_4200_binds_full_surface_from_shadow_learning(self) -> None:
        # dc6514 SMG-4200 variant: a full shadow-learning run correlated the
        # SmartESS cloud writes to real registers, confirming the field->register
        # map. The entry previously sat on the read-only family surface; it now
        # ships those hardware-correlated controls tested (exposed in auto mode),
        # with the fault-lock action left untested.
        from custom_components.eybond_local.metadata.profile_loader import (
            load_driver_profile,
        )

        catalog = load_compiled_detection_catalog()
        surface = catalog.surfaces["smg_variant_4200_full"]
        self.assertFalse(surface.read_only)
        self.assertEqual(
            surface.profile_name, "modbus_smg/models/smg_variant_4200.json"
        )
        profile = load_driver_profile(surface.profile_name)
        tested = {c.key for c in profile.capabilities if c.tested}
        expected_tested = {
            # First graduation batch (base-referenced controls).
            "input_voltage_range",
            "buzzer_mode",
            "lcd_backlight_mode",
            "lcd_auto_return_mode",
            "turn_on_mode",
            "output_rating_voltage",
            "output_rating_frequency",
            "battery_under_voltage",
            "battery_under_voltage_off_grid",
            # Second batch: a follow-up shadow-learning run correlated these
            # cloud writes to their own registers, so they graduate tested too.
            "battery_type",
            "float_charge_wait_time",
            "automatic_mains_output_enabled",
            "lithium_auto_activation",
            "low_dc_protection_soc_ac_mode",
            "low_dc_recovery_soc_ac_mode",
            "op1_offgrid_soc_protection",
            "battery_low_cutoff_soc",
            "max_discharge_current_protection",
            "op1_offgrid_battery_low_voltage",
            "op2_overload_warning_percent",
            "output2_allow_output",
        }
        self.assertEqual(tested, expected_tested)
        # The fault-lock exit action and the momentary equalization action stay
        # untested (full-control only): the exit is only effective in a fault
        # state, and the equalization action was never confirmed on hardware.
        by_key = {c.key: c for c in profile.capabilities}
        self.assertFalse(by_key["exit_fault_mode"].tested)
        self.assertFalse(by_key["equalization_activate_now"].tested)
        # The battery-type enum for this device uses the vendor's
        # non-contiguous value set (0/1/2/4/6/8).
        self.assertEqual(
            [choice.value for choice in by_key["battery_type"].choices],
            [0, 1, 2, 4, 6, 8],
        )
        # This device's Input Mode has three values (APL/UPS/GNT), overriding
        # the two-value family default.
        input_mode = by_key["input_voltage_range"]
        self.assertEqual(
            [choice.value for choice in input_mode.choices], [0, 1, 2]
        )

    def test_aninerel_anl_4200t_uses_24v_battery_voltage_windows(self) -> None:
        # The unit is 24 V (the "24L" in the model name); the family templates
        # carry 48 V-class windows (40.0-65.0 V) that would reject every valid
        # 24 V setpoint. The exact limits for this model are undocumented,
        # so the windows are deliberately wide and the inverter's own write
        # validation is the authority.
        from custom_components.eybond_local.metadata.profile_loader import (
            load_driver_profile,
        )

        profile = load_driver_profile(
            "modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json"
        )
        caps = {c.key: c for c in profile.capabilities}
        for key in (
            "battery_bulk_voltage",
            "battery_float_voltage",
            "battery_under_voltage",
            "battery_under_voltage_off_grid",
            "battery_redischarge_voltage",
            "battery_overvoltage_protection_voltage",
            "battery_equalization_voltage",
        ):
            self.assertEqual(caps[key].minimum, 0, key)
            self.assertEqual(caps[key].maximum, 700, key)
        # Overrides keep the template identity (scaled 0.1 V writes).
        self.assertEqual(caps["battery_bulk_voltage"].value_kind, "scaled_u16")
        self.assertEqual(caps["battery_bulk_voltage"].divisor, 10)

    def test_aninerel_anl_4200t_uses_five_value_output_priority_enum(self) -> None:
        # The tester's vendor-app field list shows this firmware generation
        # uses a five-value output-priority enum (0 UTI .. 4 SUF) and a
        # charger-priority enum of 1-3; the family's SMG enums mislabeled
        # both (his live register 301 read 3 = SUB showed "Unknown (3)").
        from custom_components.eybond_local.metadata.profile_loader import (
            load_driver_profile,
        )
        from custom_components.eybond_local.metadata.register_schema_loader import (
            load_register_schema,
        )

        profile = load_driver_profile(
            "modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json"
        )
        caps = {c.key: c for c in profile.capabilities}
        self.assertEqual(
            [choice.value for choice in caps["output_source_priority"].choices],
            [0, 1, 2, 3, 4],
        )
        self.assertEqual(
            [choice.value for choice in caps["charge_source_priority"].choices],
            [1, 2, 3],
        )

        schema = load_register_schema(
            "modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json"
        )
        spec = next(
            s for s in schema.spec_set("config") if s.register == 301
        )
        self.assertEqual(spec.enum_map[3], "Solar-Utility-Battery (SUB)")
        # Select current-option matching requires byte-identical labels
        # between the schema enum table and the capability choices.
        for choice in caps["output_source_priority"].choices:
            self.assertEqual(spec.enum_map[choice.value], choice.label)

    def test_model_profile_capabilities_are_exposable_in_full_control(self) -> None:
        # Field regression: capabilities labeled provenance=cloud_hint are
        # write-suppressed in EVERY mode (cloud hints are metadata only), so
        # the tester's Output Priority select vanished entirely. Model-bound
        # untested capabilities must all pass the full-control exposure gate.
        from custom_components.eybond_local.metadata.profile_loader import (
            load_driver_profile,
        )
        from custom_components.eybond_local.schema import (
            capability_write_exposure_allowed,
        )

        for profile_name, variant_key in (
            ("modbus_smg/models/aninerel_anl_4200t_24l_w_pro.json", "aninerel_anl_4200t_24l_w_pro"),
            ("modbus_smg/models/anenji_op2_6200.json", "default"),
            ("modbus_catalog/growatt_spf.json", "growatt_spf"),
            ("modbus_catalog/deye_lv.json", "deye_lv"),
            ("must_pv_ph18/base.json", "pv_ph18"),
        ):
            profile = load_driver_profile(profile_name)
            hidden = [
                capability.key
                for capability in profile.capabilities
                if not capability_write_exposure_allowed(
                    capability,
                    control_mode="full",
                    detection_confidence="high",
                    variant_key=variant_key,
                    profile_source_scope="builtin",
                    schema_source_scope="builtin",
                    profile_name=profile_name,
                )
            ]
            self.assertEqual(hidden, [], profile_name)
            # An exposable capability must also register ENABLED: entities
            # default-disabled in the registry read as "no controls appeared"
            # (0.3.0-beta.1 MUST PV1800 field report).
            registry_disabled = [
                capability.key
                for capability in profile.capabilities
                if not capability.enabled_default
            ]
            if profile_name in (
                "must_pv_ph18/base.json",
                "modbus_catalog/growatt_spf.json",
                "modbus_catalog/deye_lv.json",
            ):
                self.assertEqual(registry_disabled, [], profile_name)

    def test_unknown_known_layout_resolves_family(self) -> None:
        result = _tree_resolve(load_compiled_detection_catalog(), 
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 9999,
            },
        )
        self.assertEqual(result.resolution, RESOLUTION_FAMILY)
        self.assertEqual(result.surface_key, "smg_family_read_only")

    def test_conflicting_optional_rated_power_resolves_family(self) -> None:
        result = _tree_resolve(load_compiled_detection_catalog(), 
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 1,
                "fingerprint.model_code": 7680,
                "fingerprint.rated_power": 5500,
            },
        )
        self.assertEqual(result.resolution, RESOLUTION_FAMILY)

    def test_unknown_layout_remains_unresolved(self) -> None:
        result = _tree_resolve(load_compiled_detection_catalog(), 
            protocol_key="modbus_smg",
            evidence={
                "fingerprint.layout_code": 99,
                "fingerprint.model_code": 1234,
            },
        )
        self.assertEqual(result.resolution, RESOLUTION_UNRESOLVED)

    def test_must_pv1800_numeric_marker_resolves_to_issue_5_descriptor(self) -> None:
        result = _tree_resolve(load_compiled_detection_catalog(), 
            protocol_key="must_pv_ph18",
            evidence={
                "protocol.protocol_id": "MUST_PV_PH18",
                "identity.model_number": "PV1800",
            },
        )

        self.assertEqual(result.resolution, RESOLUTION_EXACT)
        self.assertEqual(result.candidate_keys, ("must_pv18_3024",))
        self.assertEqual(result.surface_key, "must_pv_ph18_full")

    def test_serial_plausibility_helper(self) -> None:
        self.assertTrue(serial_ascii_plausible("92632500000001"))
        self.assertTrue(serial_ascii_plausible("70S10300000005Q"))
        self.assertFalse(serial_ascii_plausible(""))
        self.assertFalse(serial_ascii_plausible("\x00\x00\x00"))
        self.assertFalse(serial_ascii_plausible("ab"))


class ForceUnsupportedSentinelTest(unittest.TestCase):
    """On-device sentinel toggles force-unsupported without an env var or code edit."""

    def setUp(self) -> None:
        # Isolate from any ambient env-derived value and restore the module flag.
        self._const_patch = patch.object(
            device_catalog_loader, "FORCE_UNSUPPORTED_MODELS", False
        )
        self._const_patch.start()
        self.addCleanup(self._const_patch.stop)
        self.addCleanup(refresh_force_unsupported_override, None)

    def test_sentinel_present_enables_and_absent_disables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            refresh_force_unsupported_override(root)
            self.assertFalse(force_unsupported_models())

            (root / FORCE_UNSUPPORTED_SENTINEL_NAME).write_text("", encoding="ascii")
            refresh_force_unsupported_override(root)
            self.assertTrue(force_unsupported_models())

            (root / FORCE_UNSUPPORTED_SENTINEL_NAME).unlink()
            refresh_force_unsupported_override(root)
            self.assertFalse(force_unsupported_models())

    def test_none_root_clears_override(self) -> None:
        refresh_force_unsupported_override(None)
        self.assertFalse(force_unsupported_models())

    def test_env_constant_wins_regardless_of_sentinel(self) -> None:
        with patch.object(device_catalog_loader, "FORCE_UNSUPPORTED_MODELS", True):
            refresh_force_unsupported_override(None)
            self.assertTrue(force_unsupported_models())


if __name__ == "__main__":
    unittest.main()
