from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local import _default_enabled_unique_ids, _prime_metadata_caches


class InitModuleTests(unittest.TestCase):
    def test_prime_metadata_caches_delegates_to_registry(self) -> None:
        with patch("custom_components.eybond_local.drivers.registry.prime_metadata_caches") as prime:
            _prime_metadata_caches()

        prime.assert_called_once_with()

    def test_default_enabled_unique_ids_include_derived_energy_defaults(self) -> None:
        unique_ids = _default_enabled_unique_ids("entry123")

        self.assertIn("entry123_battery_power", unique_ids)
        self.assertIn("entry123_last_error", unique_ids)
        self.assertIn("entry123_estimated_load_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_pv_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_pv_to_home_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_battery_to_home_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_grid_to_home_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_grid_import_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_grid_export_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_battery_charge_energy_daily", unique_ids)
        self.assertIn("entry123_estimated_battery_discharge_energy_daily", unique_ids)
        self.assertNotIn("entry123_estimated_load_energy", unique_ids)
        self.assertNotIn("entry123_estimated_pv_energy", unique_ids)
        self.assertNotIn("entry123_estimated_pv_energy_monthly", unique_ids)
        self.assertNotIn("entry123_estimated_pv_to_home_energy", unique_ids)
        self.assertNotIn("entry123_estimated_battery_to_home_energy", unique_ids)
        self.assertNotIn("entry123_estimated_grid_to_home_energy", unique_ids)
        self.assertNotIn("entry123_estimated_grid_import_energy", unique_ids)
        self.assertNotIn("entry123_estimated_grid_export_energy", unique_ids)
        self.assertNotIn("entry123_estimated_battery_charge_energy", unique_ids)
        self.assertNotIn("entry123_estimated_battery_discharge_energy", unique_ids)
        self.assertIn("entry123_pv_to_home_power", unique_ids)
        self.assertIn("entry123_pv_to_battery_power", unique_ids)
        self.assertIn("entry123_pv_to_grid_power", unique_ids)
        self.assertIn("entry123_battery_to_home_power", unique_ids)
        self.assertIn("entry123_grid_to_home_power", unique_ids)
        self.assertIn("entry123_grid_to_battery_power", unique_ids)
        self.assertIn("entry123_output_source_priority", unique_ids)
        self.assertIn("entry123_charge_source_priority", unique_ids)
        self.assertIn("entry123_battery_float_voltage", unique_ids)
        self.assertIn("entry123_max_ac_charge_current", unique_ids)


if __name__ == "__main__":
    unittest.main()
