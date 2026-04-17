from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.derived_energy import (
    compute_derived_power,
    default_enabled_derived_energy_keys,
    derived_energy_cycle_descriptions_for_keys,
    derived_energy_descriptions_for_keys,
    derived_energy_entity_descriptions_for_keys,
)


class DerivedEnergyTests(unittest.TestCase):
    def test_smg_available_keys_enable_practical_energy_sensors(self) -> None:
        descriptions = derived_energy_descriptions_for_keys(
            {"output_power", "pv_power", "battery_average_power", "grid_power"}
        )
        keys = {description.key for description in descriptions}

        self.assertIn("estimated_load_energy", keys)
        self.assertIn("estimated_pv_energy", keys)
        self.assertIn("estimated_grid_import_energy", keys)
        self.assertIn("estimated_grid_export_energy", keys)
        self.assertIn("estimated_battery_charge_energy", keys)
        self.assertIn("estimated_battery_discharge_energy", keys)
        self.assertNotIn("estimated_output_energy", keys)

    def test_common_power_keys_enable_split_to_home_energy_sensors(self) -> None:
        descriptions = derived_energy_descriptions_for_keys(
            {"output_power", "pv_power", "battery_power"}
        )
        keys = {description.key for description in descriptions}

        self.assertIn("estimated_pv_to_home_energy", keys)
        self.assertIn("estimated_battery_to_home_energy", keys)
        self.assertIn("estimated_grid_to_home_energy", keys)

    def test_compute_derived_power_uses_signed_battery_average_power_for_smg(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys({"battery_average_power"})
        }

        self.assertEqual(
            compute_derived_power(
                {"battery_average_power": 420.0},
                descriptions["estimated_battery_charge_energy"],
            ),
            420.0,
        )
        self.assertEqual(
            compute_derived_power(
                {"battery_average_power": 420.0},
                descriptions["estimated_battery_discharge_energy"],
            ),
            0.0,
        )
        self.assertEqual(
            compute_derived_power(
                {"battery_average_power": -315.0},
                descriptions["estimated_battery_charge_energy"],
            ),
            0.0,
        )
        self.assertEqual(
            compute_derived_power(
                {"battery_average_power": -315.0},
                descriptions["estimated_battery_discharge_energy"],
            ),
            315.0,
        )

    def test_compute_derived_power_uses_signed_grid_power_for_import_export(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys({"grid_power"})
        }

        self.assertEqual(
            compute_derived_power(
                {"grid_power": 640.0},
                descriptions["estimated_grid_import_energy"],
            ),
            640.0,
        )
        self.assertEqual(
            compute_derived_power(
                {"grid_power": 640.0},
                descriptions["estimated_grid_export_energy"],
            ),
            0.0,
        )
        self.assertEqual(
            compute_derived_power(
                {"grid_power": -275.0},
                descriptions["estimated_grid_import_energy"],
            ),
            0.0,
        )
        self.assertEqual(
            compute_derived_power(
                {"grid_power": -275.0},
                descriptions["estimated_grid_export_energy"],
            ),
            275.0,
        )

    def test_compute_derived_power_uses_direct_flow_keys_for_grid_import_export(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys(
                {
                    "grid_to_home_power",
                    "grid_to_battery_power",
                    "pv_to_grid_power",
                }
            )
        }

        self.assertEqual(
            compute_derived_power(
                {
                    "grid_to_home_power": 140.0,
                    "grid_to_battery_power": 36.0,
                    "pv_to_grid_power": 220.0,
                },
                descriptions["estimated_grid_import_energy"],
            ),
            176.0,
        )
        self.assertEqual(
            compute_derived_power(
                {
                    "grid_to_home_power": 140.0,
                    "grid_to_battery_power": 36.0,
                    "pv_to_grid_power": 220.0,
                },
                descriptions["estimated_grid_export_energy"],
            ),
            220.0,
        )

    def test_compute_derived_power_splits_home_load_by_priority(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys(
                {"output_power", "pv_power", "battery_power", "grid_power"}
            )
        }

        values = {
            "output_power": 5000.0,
            "pv_power": 2000.0,
            "battery_power": -1500.0,
            "grid_power": 2500.0,
        }

        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_pv_to_home_energy"]),
            2000.0,
        )
        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_battery_to_home_energy"]),
            1500.0,
        )
        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_grid_to_home_energy"]),
            1500.0,
        )

    def test_grid_to_home_falls_back_to_residual_without_grid_power(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys(
                {"output_power", "pv_power", "battery_power"}
            )
        }

        values = {
            "output_power": 3500.0,
            "pv_power": 1000.0,
            "battery_power": -500.0,
        }

        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_grid_to_home_energy"]),
            2000.0,
        )

    def test_split_to_home_clamps_when_pv_alone_exceeds_load(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys(
                {"output_power", "pv_power", "battery_power", "grid_power"}
            )
        }

        values = {
            "output_power": 1000.0,
            "pv_power": 1500.0,
            "battery_power": -800.0,
            "grid_power": 500.0,
        }

        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_pv_to_home_energy"]),
            1000.0,
        )
        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_battery_to_home_energy"]),
            0.0,
        )
        self.assertEqual(
            compute_derived_power(values, descriptions["estimated_grid_to_home_energy"]),
            0.0,
        )

    def test_pi_available_keys_keep_existing_multiply_and_output_energy_paths(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_descriptions_for_keys(
                {"output_active_power", "battery_voltage", "battery_charge_current"}
            )
        }

        self.assertIn("estimated_output_energy", descriptions)
        self.assertEqual(
            compute_derived_power(
                {
                    "output_active_power": 1500.0,
                    "battery_voltage": 52.0,
                    "battery_charge_current": 10.0,
                },
                descriptions["estimated_output_energy"],
            ),
            1500.0,
        )
        self.assertEqual(
            compute_derived_power(
                {
                    "output_active_power": 1500.0,
                    "battery_voltage": 52.0,
                    "battery_charge_current": 10.0,
                },
                descriptions["estimated_battery_charge_energy"],
            ),
            520.0,
        )

    def test_canonical_output_power_replaces_legacy_output_energy_entity(self) -> None:
        descriptions = {
            description.key
            for description in derived_energy_descriptions_for_keys(
                {"output_power", "output_active_power", "battery_power", "pv_power"}
            )
        }

        self.assertIn("estimated_load_energy", descriptions)
        self.assertIn("estimated_pv_energy", descriptions)
        self.assertIn("estimated_battery_charge_energy", descriptions)
        self.assertIn("estimated_battery_discharge_energy", descriptions)
        self.assertNotIn("estimated_output_energy", descriptions)

    def test_default_enabled_keys_include_practical_energy_entities(self) -> None:
        keys = default_enabled_derived_energy_keys()

        self.assertIn("estimated_load_energy_daily", keys)
        self.assertIn("estimated_pv_energy_daily", keys)
        self.assertIn("estimated_pv_to_home_energy_daily", keys)
        self.assertIn("estimated_battery_to_home_energy_daily", keys)
        self.assertIn("estimated_grid_to_home_energy_daily", keys)
        self.assertIn("estimated_grid_import_energy_daily", keys)
        self.assertIn("estimated_grid_export_energy_daily", keys)
        self.assertIn("estimated_battery_charge_energy_daily", keys)
        self.assertIn("estimated_battery_discharge_energy_daily", keys)
        self.assertNotIn("estimated_load_energy", keys)
        self.assertNotIn("estimated_pv_energy", keys)
        self.assertNotIn("estimated_pv_energy_monthly", keys)
        self.assertNotIn("estimated_pv_to_home_energy", keys)
        self.assertNotIn("estimated_battery_to_home_energy", keys)
        self.assertNotIn("estimated_grid_to_home_energy", keys)
        self.assertNotIn("estimated_grid_import_energy", keys)
        self.assertNotIn("estimated_grid_export_energy", keys)
        self.assertNotIn("estimated_battery_charge_energy", keys)
        self.assertNotIn("estimated_battery_discharge_energy", keys)

    def test_derived_energy_entities_are_not_exposed_when_only_card_totals_are_needed(self) -> None:
        descriptions = derived_energy_entity_descriptions_for_keys(
            {"output_power", "pv_power", "battery_power", "grid_power"}
        )

        self.assertEqual(descriptions, ())

    def test_cycle_descriptions_include_pv_load_and_split_helpers(self) -> None:
        descriptions = {
            description.key: description
            for description in derived_energy_cycle_descriptions_for_keys(
                {
                    "solar_feed_to_grid_enabled",
                    "estimated_battery_charge_energy",
                    "estimated_battery_discharge_energy",
                    "estimated_grid_import_energy",
                    "estimated_grid_export_energy",
                    "estimated_pv_energy",
                    "estimated_load_energy",
                    "estimated_pv_to_home_energy",
                    "estimated_battery_to_home_energy",
                    "estimated_grid_to_home_energy",
                }
            )
        }

        self.assertIn("estimated_load_energy_daily", descriptions)
        self.assertIn("estimated_pv_energy_daily", descriptions)
        self.assertIn("estimated_battery_charge_energy_daily", descriptions)
        self.assertIn("estimated_battery_discharge_energy_daily", descriptions)
        self.assertIn("estimated_pv_to_home_energy_daily", descriptions)
        self.assertIn("estimated_battery_to_home_energy_daily", descriptions)
        self.assertIn("estimated_grid_to_home_energy_daily", descriptions)
        self.assertIn("estimated_grid_import_energy_daily", descriptions)
        self.assertIn("estimated_grid_export_energy_daily", descriptions)
        self.assertEqual(descriptions["estimated_load_energy_daily"].source_key, "estimated_load_energy")
        self.assertEqual(descriptions["estimated_pv_energy_daily"].source_key, "estimated_pv_energy")
        self.assertEqual(
            descriptions["estimated_battery_charge_energy_daily"].source_key,
            "estimated_battery_charge_energy",
        )
        self.assertEqual(
            descriptions["estimated_battery_discharge_energy_daily"].source_key,
            "estimated_battery_discharge_energy",
        )
        self.assertEqual(
            descriptions["estimated_pv_to_home_energy_daily"].source_key,
            "estimated_pv_to_home_energy",
        )
        self.assertEqual(
            descriptions["estimated_battery_to_home_energy_daily"].source_key,
            "estimated_battery_to_home_energy",
        )
        self.assertEqual(
            descriptions["estimated_grid_to_home_energy_daily"].source_key,
            "estimated_grid_to_home_energy",
        )
        self.assertEqual(
            descriptions["estimated_grid_import_energy_daily"].source_key,
            "estimated_grid_import_energy",
        )
        self.assertEqual(
            descriptions["estimated_grid_export_energy_daily"].source_key,
            "estimated_grid_export_energy",
        )

    def test_grid_export_daily_helper_requires_export_support_key(self) -> None:
        descriptions = {
            description.key
            for description in derived_energy_cycle_descriptions_for_keys(
                {"estimated_grid_export_energy"}
            )
        }

        self.assertNotIn("estimated_grid_export_energy_daily", descriptions)

    def test_grid_export_daily_helper_appears_for_signed_grid_power_fallback(self) -> None:
        descriptions = {
            description.key
            for description in derived_energy_cycle_descriptions_for_keys(
                {"estimated_grid_export_energy", "grid_power"}
            )
        }

        self.assertIn("estimated_grid_export_energy_daily", descriptions)

    def test_grid_export_daily_helper_appears_when_export_support_key_exists(self) -> None:
        descriptions = {
            description.key
            for description in derived_energy_cycle_descriptions_for_keys(
                {"estimated_grid_export_energy", "solar_feed_to_grid_enabled"}
            )
        }

        self.assertIn("estimated_grid_export_energy_daily", descriptions)


if __name__ == "__main__":
    unittest.main()
