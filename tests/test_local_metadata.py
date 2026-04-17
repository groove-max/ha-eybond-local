from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.local_metadata import (
    clear_local_metadata_loader_caches,
    create_local_profile_draft,
    create_local_schema_draft,
    draft_activates_automatically,
    ensure_local_metadata_dirs,
    local_profile_override_details,
    local_register_schema_override_details,
    resolve_local_metadata_rollback_paths,
    rollback_local_metadata_overrides,
)
from custom_components.eybond_local.metadata.smartess_draft import (
    create_smartess_known_family_draft,
    resolve_smartess_known_family_draft_plan,
)
from custom_components.eybond_local.metadata.smartess_smg_bridge import (
    create_smartess_smg_bridge_draft,
    resolve_smartess_smg_bridge_plan,
)


def _sample_smartess_cloud_evidence() -> dict[str, object]:
    return {
        "source": "smartess_cloud_probe",
        "match": {"collector_pn": "E5000025388419"},
        "device_identity": {
            "pn": "E50000253884199645",
            "sn": "E50000253884199645094801",
            "devcode": 2376,
            "devaddr": 1,
        },
        "summary": {
            "actions": [
                "device_list",
                "device_detail",
                "device_settings",
                "energy_flow",
            ],
            "detail_sections": ["bc_", "bt_", "gd_", "pv_", "sy_"],
            "device_count": 1,
        },
        "payload": {
            "normalized": {
                "device_detail": {
                    "section_counts": {
                        "bc_": 1,
                        "bt_": 1,
                        "gd_": 1,
                        "pv_": 1,
                        "sy_": 1,
                    }
                }
            }
        },
    }


def _sample_smartess_smg_cloud_evidence() -> dict[str, object]:
    return {
        "source": "smartess_cloud_probe",
        "match": {"collector_pn": "E5000025388419"},
        "device_identity": {
            "pn": "E50000253884199645",
            "sn": "E50000253884199645094801",
            "devcode": 2376,
            "devaddr": 1,
        },
        "summary": {
            "device_count": 1,
            "settings_field_count": 12,
        },
        "payload": {
            "normalized": {
                "device_settings": {
                    "field_count": 12,
                    "fields": [
                        {"title": "Output Mode", "bucket": "probable_0925"},
                        {"title": "Boot method", "bucket": "cloud_only"},
                        {"title": "Output Voltage", "bucket": "exact_0925"},
                        {"title": "Output Frequency", "bucket": "exact_0925"},
                        {"title": "Bulk Charging Voltage (C.V Voltage)", "bucket": "exact_0925"},
                        {"title": "Floating Charging Voltage", "bucket": "exact_0925"},
                        {"title": "EQ Charing Voltage", "bucket": "exact_0925"},
                        {"title": "EQ Interval Time", "bucket": "exact_0925"},
                        {
                            "title": "Off grid mode battery discharge SOC protection value",
                            "bucket": "cloud_only",
                        },
                        {"title": "Exit Fault Mode", "bucket": "cloud_only"},
                        {"title": "Power Saving Mode", "bucket": "exact_0925"},
                        {"title": "Output control", "bucket": "cloud_only"},
                    ],
                }
            }
        },
    }


def _sample_smartess_anenji_cloud_evidence() -> dict[str, object]:
    return {
        "source": "smartess_cloud_probe",
        "match": {"collector_pn": "ANJ11KW240001"},
        "payload": {
            "normalized": {
                "device_settings": {
                    "field_count": 11,
                    "fields": [
                        {"title": "Main Output Priority", "bucket": "cloud_only"},
                        {"title": "Output Voltage Setting", "bucket": "cloud_only"},
                        {
                            "title": "Mains mode battery discharge recovery point",
                            "bucket": "cloud_only",
                        },
                        {"title": "Battery Eq mode enable", "bucket": "cloud_only"},
                        {"title": "Low DC Protection SOC In AC Mode", "bucket": "cloud_only"},
                        {"title": "Low DC Recovery SOC In AC Mode", "bucket": "cloud_only"},
                        {"title": "Battery Low Cut Off SOC", "bucket": "cloud_only"},
                        {"title": "Boot method", "bucket": "cloud_only"},
                        {"title": "Remote switch", "bucket": "cloud_only"},
                        {"title": "Inverter Date", "bucket": "cloud_only"},
                        {"title": "Energy-saving mode switch", "bucket": "cloud_only"},
                        {"title": "Input Mode", "bucket": "cloud_only"},
                        {"title": "bat_eq_time", "bucket": "cloud_only"},
                        {"title": "Clean Generation Power", "bucket": "cloud_only"},
                        {"title": "Equalization activated immediately", "bucket": "cloud_only"},
                    ],
                }
            }
        },
    }


def _find_item(raw_items: list[dict[str, object]], key: str) -> dict[str, object]:
    for item in raw_items:
        if str(item.get("key") or "") == key:
            return item
    raise KeyError(key)


class LocalMetadataTests(unittest.TestCase):
    def test_detects_when_one_draft_name_overrides_builtin_metadata(self) -> None:
        self.assertTrue(draft_activates_automatically("smg_modbus.json", None))
        self.assertTrue(draft_activates_automatically("smg_modbus.json", "smg_modbus.json"))
        self.assertTrue(
            draft_activates_automatically(
                "modbus_smg/models/smg_6200.json",
                "./modbus_smg/models/smg_6200.json",
            )
        )
        self.assertFalse(draft_activates_automatically("smg_modbus.json", "drafts/smg_modbus.json"))
        self.assertFalse(
            draft_activates_automatically(
                "modbus_smg/models/smg_6200.json",
                "modbus_smg/models/local_smg_6200.json",
            )
        )

    def test_creates_local_profile_draft_from_builtin_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            path = create_local_profile_draft(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
            )

            self.assertEqual(path.name, "smg_modbus.json")
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(raw["draft_of"], "smg_modbus.json")
            self.assertTrue(raw["experimental"])
            self.assertIn("(Local Draft)", raw["title"])
            override = local_profile_override_details(config_dir, "smg_modbus.json")
            self.assertTrue(override["exists"])
            self.assertEqual(override["path"], str(path))

    def test_creates_local_schema_draft_that_extends_builtin_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            path = create_local_schema_draft(
                config_dir=config_dir,
                source_schema_name="modbus_smg/models/smg_6200.json",
            )

            self.assertEqual(path.name, "smg_6200.json")
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                raw["extends"],
                "builtin:modbus_smg/models/smg_6200.json",
            )
            self.assertEqual(raw["driver_key"], "modbus_smg")
            self.assertEqual(raw["protocol_family"], "modbus_smg")
            self.assertIn("(Local Draft)", raw["title"])
            override = local_register_schema_override_details(
                config_dir,
                "modbus_smg/models/smg_6200.json",
            )
            self.assertTrue(override["exists"])
            self.assertEqual(override["path"], str(path))

    def test_reports_missing_local_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            profile_override = local_profile_override_details(config_dir, "smg_modbus.json")
            schema_override = local_register_schema_override_details(
                config_dir,
                "modbus_smg/models/smg_6200.json",
            )

            self.assertFalse(profile_override["exists"])
            self.assertIn("No active local override", str(profile_override["status"]))
            self.assertFalse(schema_override["exists"])
            self.assertIn("No active local override", str(schema_override["status"]))

    def test_resolves_active_local_metadata_rollback_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)
            profile_path = create_local_profile_draft(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
            )
            schema_path = create_local_schema_draft(
                config_dir=config_dir,
                source_schema_name="modbus_smg/models/smg_6200.json",
            )

            rollback_paths = resolve_local_metadata_rollback_paths(
                config_dir=config_dir,
                profile_name="smg_modbus.json",
                schema_name="modbus_smg/models/smg_6200.json",
                profile_metadata=type(
                    "ProfileMeta",
                    (),
                    {"source_scope": "external", "source_path": str(profile_path)},
                )(),
                schema_metadata=type(
                    "SchemaMeta",
                    (),
                    {"source_scope": "external", "source_path": str(schema_path)},
                )(),
            )

            self.assertEqual(rollback_paths.profile_path, profile_path)
            self.assertEqual(rollback_paths.schema_path, schema_path)
            self.assertEqual(rollback_paths.paths, (profile_path, schema_path))

    def test_rollback_only_allows_managed_local_override_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)
            profile_path = create_local_profile_draft(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
            )

            rollback_paths = resolve_local_metadata_rollback_paths(
                config_dir=config_dir,
                profile_name="smg_modbus.json",
                profile_metadata=type(
                    "ProfileMeta",
                    (),
                    {"source_scope": "external", "source_path": str(Path(temp_dir) / "outside.json")},
                )(),
            )

            self.assertEqual(rollback_paths.paths, ())
            self.assertTrue(profile_path.exists())

    def test_rollback_removes_active_local_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)
            profile_path = create_local_profile_draft(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
            )
            schema_path = create_local_schema_draft(
                config_dir=config_dir,
                source_schema_name="modbus_smg/models/smg_6200.json",
            )

            removed_paths = rollback_local_metadata_overrides(
                config_dir=config_dir,
                profile_name="smg_modbus.json",
                schema_name="modbus_smg/models/smg_6200.json",
                profile_metadata=type(
                    "ProfileMeta",
                    (),
                    {"source_scope": "external", "source_path": str(profile_path)},
                )(),
                schema_metadata=type(
                    "SchemaMeta",
                    (),
                    {"source_scope": "external", "source_path": str(schema_path)},
                )(),
            )

            self.assertEqual(removed_paths, (profile_path, schema_path))
            self.assertFalse(profile_path.exists())
            self.assertFalse(schema_path.exists())

    def test_resolves_known_smartess_draft_plan_from_pi30_like_cloud_evidence(self) -> None:
        plan = resolve_smartess_known_family_draft_plan(
            cloud_evidence=_sample_smartess_cloud_evidence(),
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.asset_id, "0925")
        self.assertEqual(plan.source_profile_name, "pi30_ascii/models/smartess_0925_compat.json")
        self.assertEqual(plan.driver_label, "SmartESS 0925")
        self.assertEqual(plan.raw_profile_name, "smartess_local/models/0925.json")
        self.assertEqual(plan.raw_schema_name, "smartess_local/models/0925.json")

    def test_creates_smartess_known_family_draft_files(self) -> None:
        plan = resolve_smartess_known_family_draft_plan(
            cloud_evidence=_sample_smartess_cloud_evidence(),
        )
        assert plan is not None

        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            profile_path, schema_path = create_smartess_known_family_draft(
                config_dir=config_dir,
                plan=plan,
                cloud_evidence=_sample_smartess_cloud_evidence(),
            )

            profile_raw = json.loads(profile_path.read_text(encoding="utf-8"))
            schema_raw = json.loads(schema_path.read_text(encoding="utf-8"))

            self.assertEqual(profile_raw["draft_of"], "pi30_ascii/models/smartess_0925_compat.json")
            self.assertTrue(profile_raw["experimental"])
            self.assertEqual(profile_raw["smartess_draft"]["asset_id"], "0925")
            self.assertEqual(
                profile_raw["smartess_draft"]["raw_profile_name"],
                "smartess_local/models/0925.json",
            )
            self.assertIn("(Local SmartESS Draft)", profile_raw["title"])
            self.assertEqual(
                schema_raw["extends"],
                "builtin:pi30_ascii/models/smartess_0925_compat.json",
            )
            self.assertEqual(
                schema_raw["smartess_draft"]["raw_schema_name"],
                "smartess_local/models/0925.json",
            )
            self.assertEqual(schema_raw["smartess_draft"]["cloud_summary"]["device_count"], 1)
            self.assertIn("(Local SmartESS Draft)", schema_raw["title"])

    def test_resolves_smartess_smg_bridge_plan_for_active_smg_metadata(self) -> None:
        plan = resolve_smartess_smg_bridge_plan(
            effective_owner_key="modbus_smg",
            source_profile_name="smg_modbus.json",
            source_schema_name="modbus_smg/models/smg_6200.json",
            cloud_evidence=_sample_smartess_smg_cloud_evidence(),
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.bridge_label, "SmartESS SMG bridge")
        self.assertEqual(plan.source_profile_name, "smg_modbus.json")
        self.assertEqual(plan.source_schema_name, "modbus_smg/models/smg_6200.json")
        self.assertIn("output_mode", plan.profile_enable_keys)
        self.assertIn("turn_on_mode", plan.profile_enable_keys)
        self.assertIn("exit_fault_mode", plan.profile_enable_keys)
        self.assertIn("low_dc_cutoff_soc", plan.measurement_enable_keys)
        self.assertNotIn("low_dc_cutoff_soc", plan.profile_enable_keys)
        self.assertIn("Power Saving Mode", plan.blocked_field_titles)
        self.assertIn("Output control", plan.skipped_field_titles)

    def test_resolves_smartess_smg_bridge_plan_for_active_anenji_metadata(self) -> None:
        plan = resolve_smartess_smg_bridge_plan(
            effective_owner_key="modbus_smg",
            source_profile_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            source_schema_name="modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
            cloud_evidence=_sample_smartess_anenji_cloud_evidence(),
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(
            plan.source_profile_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertEqual(
            plan.source_schema_name,
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json",
        )
        self.assertIn("output_rating_voltage", plan.profile_enable_keys)
        self.assertIn("battery_redischarge_voltage", plan.profile_enable_keys)
        self.assertIn("battery_equalization_mode", plan.profile_enable_keys)
        self.assertIn("power_saving_mode", plan.profile_enable_keys)
        self.assertIn("battery_equalization_time", plan.profile_enable_keys)
        self.assertIn("clear_generation_data", plan.profile_enable_keys)
        self.assertIn("force_eq_charge", plan.profile_enable_keys)
        self.assertIn("turn_on_mode", plan.profile_enable_keys)
        self.assertTrue(
            any(
                match.cloud_title == "Input Mode" and match.profile_key == "input_mode"
                for match in plan.matches
            )
        )
        self.assertNotIn("output_source_priority", plan.profile_enable_keys)
        self.assertIn("output_source_priority", plan.measurement_enable_keys)
        self.assertIn("output_rating_voltage", plan.measurement_enable_keys)
        self.assertIn("battery_redischarge_voltage", plan.measurement_enable_keys)
        self.assertIn("battery_equalization_mode", plan.measurement_enable_keys)
        self.assertIn("input_mode", plan.measurement_enable_keys)
        self.assertIn("battery_equalization_time", plan.measurement_enable_keys)
        self.assertIn("low_dc_protection_soc_grid_mode", plan.measurement_enable_keys)
        self.assertIn(
            "solar_battery_utility_return_soc_threshold",
            plan.measurement_enable_keys,
        )
        self.assertIn("low_dc_cutoff_soc", plan.measurement_enable_keys)
        self.assertIn("turn_on_mode", plan.measurement_enable_keys)
        self.assertIn("remote_switch", plan.measurement_enable_keys)
        self.assertIn("inverter_date", plan.measurement_enable_keys)
        self.assertIn("power_saving_mode", plan.measurement_enable_keys)

    def test_creates_smartess_smg_bridge_files(self) -> None:
        plan = resolve_smartess_smg_bridge_plan(
            effective_owner_key="modbus_smg",
            source_profile_name="smg_modbus.json",
            source_schema_name="modbus_smg/models/smg_6200.json",
            cloud_evidence=_sample_smartess_smg_cloud_evidence(),
        )
        assert plan is not None

        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            ensure_local_metadata_dirs(config_dir)

            profile_path, schema_path = create_smartess_smg_bridge_draft(
                config_dir=config_dir,
                plan=plan,
                cloud_evidence=_sample_smartess_smg_cloud_evidence(),
            )

            profile_raw = json.loads(profile_path.read_text(encoding="utf-8"))
            schema_raw = json.loads(schema_path.read_text(encoding="utf-8"))

            self.assertEqual(profile_raw["draft_of"], "smg_modbus.json")
            self.assertTrue(profile_raw["experimental"])
            self.assertEqual(profile_raw["smartess_bridge"]["kind"], "smg_bridge")
            self.assertIn("(Local SmartESS SMG Bridge)", profile_raw["title"])
            self.assertTrue(
                bool(_find_item(profile_raw["capabilities"], "output_mode").get("enabled_default"))
            )
            self.assertTrue(
                bool(_find_item(profile_raw["capabilities"], "turn_on_mode").get("enabled_default"))
            )
            self.assertFalse(
                bool(_find_item(profile_raw["capabilities"], "power_saving_mode").get("enabled_default"))
            )

            self.assertEqual(schema_raw["draft_of"], "modbus_smg/models/smg_6200.json")
            self.assertTrue(schema_raw["experimental"])
            self.assertEqual(schema_raw["smartess_bridge"]["kind"], "smg_bridge")
            self.assertIn("(Local SmartESS SMG Bridge)", schema_raw["title"])
            self.assertTrue(
                bool(
                    _find_item(
                        schema_raw["measurement_descriptions"],
                        "low_dc_cutoff_soc",
                    ).get("enabled_default")
                )
            )
            self.assertTrue(
                bool(
                    _find_item(
                        schema_raw["measurement_descriptions"],
                        "output_rating_voltage",
                    ).get("enabled_default")
                )
            )
            self.assertNotIn(
                "power_saving_mode",
                {
                    str(item.get("key") or "")
                    for item in schema_raw["measurement_descriptions"]
                    if isinstance(item, dict)
                },
            )

    def test_clear_local_metadata_loader_caches_clears_profile_and_schema_loaders(self) -> None:
        with (
            patch(
                "custom_components.eybond_local.metadata.profile_loader.clear_profile_loader_cache"
            ) as clear_profile_cache,
            patch(
                "custom_components.eybond_local.metadata.register_schema_loader.clear_register_schema_loader_cache"
            ) as clear_schema_cache,
        ):
            clear_local_metadata_loader_caches()

        clear_profile_cache.assert_called_once_with()
        clear_schema_cache.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
