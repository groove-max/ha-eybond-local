from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata import device_catalog_loader  # noqa: E402
from custom_components.eybond_local.metadata.device_catalog_loader import (  # noqa: E402
    FORCE_UNSUPPORTED_SENTINEL_NAME,
    MATCH_DEVICE,
    MATCH_FAMILY,
    MATCH_NO_DATA,
    MATCH_UNIDENTIFIED,
    TIER_FULL,
    TIER_PARTIAL,
    clear_device_catalog_cache,
    force_unsupported_models,
    load_device_catalog,
    match_device_identity,
    refresh_force_unsupported_override,
    serial_ascii_plausible,
)


COMPONENT_ROOT = REPO_ROOT / "custom_components" / "eybond_local"


class DeviceCatalogLoadTest(unittest.TestCase):
    def setUp(self) -> None:
        clear_device_catalog_cache()
        self.addCleanup(clear_device_catalog_cache)

    def test_catalog_loads_with_expected_structure(self) -> None:
        catalog = load_device_catalog()
        self.assertEqual(catalog.schema_version, 1)
        self.assertTrue(catalog.catalog_version)
        self.assertIn("eybond_modbus", catalog.transports)
        self.assertGreaterEqual(len(catalog.layouts), 2)
        self.assertGreaterEqual(len(catalog.devices), 4)
        self.assertGreaterEqual(len(catalog.family_defaults), 1)

    def test_identity_probe_covers_fingerprint_fields(self) -> None:
        probe = load_device_catalog().transports["eybond_modbus"]
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
                path = COMPONENT_ROOT / "register_schemas" / binding.register_schema_name
                self.assertTrue(path.is_file(), f"missing schema payload: {path}")
            if binding.profile_name:
                path = COMPONENT_ROOT / "profiles" / binding.profile_name
                self.assertTrue(path.is_file(), f"missing profile payload: {path}")
        for layout in catalog.layouts:
            if layout.base_schema:
                path = COMPONENT_ROOT / "register_schemas" / layout.base_schema
                self.assertTrue(path.is_file(), f"missing layout base schema: {path}")

    def test_fingerprints_are_unique(self) -> None:
        catalog = load_device_catalog()
        seen: set[tuple[int, int, tuple[int, ...]]] = set()
        for entry in catalog.devices:
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


class DeviceCatalogMatchCorpusTest(unittest.TestCase):
    """Replay the literal fingerprints from the user-dump corpus."""
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)


    def test_smg_6200_own_device(self) -> None:
        match = match_device_identity(
            layout_code=1,
            model_code=7680,
            rated_power=6200,
            serial_ascii="92632511100118",
        )
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "smg_6200")
        self.assertEqual(match.tier, TIER_FULL)
        self.assertEqual(
            match.confidence_signals,
            ("layout_code", "model_code", "rated_power", "serial_ascii"),
        )

    def test_smg_variant_4200_user_dump(self) -> None:
        match = match_device_identity(
            layout_code=11, model_code=30721, rated_power=4200,
            serial_ascii="15573418948999",
        )
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "smg_variant_4200")
        self.assertEqual(match.tier, TIER_PARTIAL)
        self.assertEqual(match.entry.binding.profile_name, "")

    def test_anenji_11kw_user_dump_with_foreign_rated_register(self) -> None:
        # @643 reads 505 on this layout (not a rated power); the entry must
        # match without rated narrowing and the layout must mark @643 invalid.
        match = match_device_identity(
            layout_code=4, model_code=32768, rated_power=505,
            serial_ascii="70S10348568005Q",
        )
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "anenji_anj_11kw")
        self.assertFalse(match.layout.rated_power_register_valid)

    def test_anenji_4200_migrated_rule(self) -> None:
        match = match_device_identity(layout_code=1, model_code=13569, rated_power=4200)
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "anenji_4200")

    def test_anenji_op2_6200_user_dump(self) -> None:
        # aninerel.zip 2026-06-12: dual-output (OP2) 6.2 kW unit, fw 7903_A6260126v1
        # (reg626 hex string == model_code), cloud devcode 6514, range_failures [].
        match = match_device_identity(
            layout_code=11, model_code=30979, rated_power=6200,
            serial_ascii="99632601111397",
        )
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "anenji_op2_6200")
        self.assertEqual(match.tier, TIER_PARTIAL)
        self.assertEqual(
            match.entry.binding.register_schema_name,
            "modbus_smg/models/anenji_op2_6200.json",
        )
        # Writes stay locked: OP2 enable (reg 354) is NOT exposed until verified.
        self.assertEqual(match.entry.binding.profile_name, "")

    def test_anenji_6200_2025_user_dump(self) -> None:
        # Anenji2025.zip 2026-06-12: same family, single output, fw 3700_A6250424v1.
        # Same rated power (6200) as smg_6200 but a DIFFERENT model_code — rated
        # power never identifies a model on its own.
        match = match_device_identity(
            layout_code=1, model_code=14080, rated_power=6200,
            serial_ascii="92632507102827",
        )
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "anenji_6200")
        self.assertEqual(match.tier, TIER_PARTIAL)
        self.assertEqual(match.entry.binding.profile_name, "")

    def test_force_unsupported_downgrades_known_model_to_family(self) -> None:
        # Debug toggle: a fully-supported device (SMG 6200) must drop to the
        # family/partial tier so the learning flow can be validated on it.
        with patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "force_unsupported_models",
            return_value=True,
        ):
            match = match_device_identity(
                layout_code=1, model_code=7680, rated_power=6200,
                serial_ascii="92632511100118",
            )
        self.assertEqual(match.kind, MATCH_FAMILY)
        self.assertEqual(match.tier, TIER_PARTIAL)
        self.assertIsNone(match.entry)
        self.assertEqual(match.family_default.binding.profile_name, "")

    def test_force_unsupported_unknown_layout_stays_unidentified(self) -> None:
        with patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "force_unsupported_models",
            return_value=True,
        ):
            match = match_device_identity(layout_code=99, model_code=1234)
        self.assertEqual(match.kind, MATCH_UNIDENTIFIED)

    def test_all_zero_identity_is_no_data_never_unknown(self) -> None:
        # The 2026-06-08 incident: comm-down captures read @171=0 @184=0 and
        # were misclassified as no_supported_driver_matched.
        match = match_device_identity(layout_code=0, model_code=0, rated_power=0)
        self.assertEqual(match.kind, MATCH_NO_DATA)

    def test_absent_identity_is_no_data(self) -> None:
        match = match_device_identity(layout_code=None, model_code=None)
        self.assertEqual(match.kind, MATCH_NO_DATA)


class DeviceCatalogMatchSemanticsTest(unittest.TestCase):
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)

    def test_unknown_model_in_known_layout_falls_to_family_partial(self) -> None:
        match = match_device_identity(layout_code=1, model_code=9999, rated_power=5500)
        self.assertEqual(match.kind, MATCH_FAMILY)
        self.assertEqual(match.tier, TIER_PARTIAL)
        self.assertEqual(match.family_default.binding.register_schema_name, "modbus_smg/base.json")
        self.assertEqual(match.family_default.binding.profile_name, "")

    def test_other_wattage_of_pinned_model_is_family_not_device(self) -> None:
        # SMG 6200 pins rated_power one_of [6200]; a 5500 unit with the same
        # codes must NOT silently get the 6200 schema.
        match = match_device_identity(layout_code=1, model_code=7680, rated_power=5500)
        self.assertEqual(match.kind, MATCH_FAMILY)

    def test_unread_rated_power_still_matches_pinned_entry(self) -> None:
        match = match_device_identity(layout_code=1, model_code=7680, rated_power=None)
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertEqual(match.entry.entry_key, "smg_6200")
        self.assertNotIn("rated_power", match.confidence_signals)

    def test_unknown_layout_is_unidentified(self) -> None:
        match = match_device_identity(layout_code=99, model_code=1234)
        self.assertEqual(match.kind, MATCH_UNIDENTIFIED)
        self.assertIsNone(match.layout)

    def test_scrambled_serial_does_not_reject_only_drops_signal(self) -> None:
        # Anonymized fixtures scramble the serial words; identity must hold.
        match = match_device_identity(
            layout_code=1, model_code=7680, rated_power=6200, serial_ascii="\x00\x00"
        )
        self.assertEqual(match.kind, MATCH_DEVICE)
        self.assertNotIn("serial_ascii", match.confidence_signals)

    def test_serial_plausibility_helper(self) -> None:
        self.assertTrue(serial_ascii_plausible("92632511100118"))
        self.assertTrue(serial_ascii_plausible("70S10348568005Q"))
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
