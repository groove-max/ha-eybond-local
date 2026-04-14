from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.canonical_telemetry import (
    apply_canonical_measurements,
    canonical_measurements_for_driver,
)


class CanonicalTelemetryTests(unittest.TestCase):
    def test_pi30_canonical_measurements_expose_common_core_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("pi30")
        }

        self.assertIn("grid_voltage", keys)
        self.assertIn("grid_frequency", keys)
        self.assertIn("output_power", keys)
        self.assertIn("pv_voltage", keys)
        self.assertIn("pv_current", keys)
        self.assertIn("pv_power", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("pv_to_battery_power", keys)
        self.assertIn("pv_to_grid_power", keys)
        self.assertIn("battery_to_home_power", keys)
        self.assertIn("grid_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_pi18_canonical_measurements_expose_common_flow_aliases(self) -> None:
        keys = {
            description.key
            for description in canonical_measurements_for_driver("pi18")
        }

        self.assertIn("output_power", keys)
        self.assertIn("pv_voltage", keys)
        self.assertIn("pv_power", keys)
        self.assertIn("battery_power", keys)
        self.assertIn("pv_to_home_power", keys)
        self.assertIn("grid_to_battery_power", keys)

    def test_apply_canonical_measurements_builds_pi30_common_values(self) -> None:
        values = {
            "input_voltage": 230.0,
            "input_frequency": 50.0,
            "output_active_power": 1400,
            "pv_input_voltage": 118.0,
            "pv_input_current": 8.5,
            "pv_input_power": 1003,
            "battery_voltage": 51.2,
            "battery_charge_current": 12.0,
            "battery_discharge_current": 0.0,
        }

        apply_canonical_measurements("pi30", values)

        self.assertEqual(values["grid_voltage"], 230.0)
        self.assertEqual(values["grid_frequency"], 50.0)
        self.assertEqual(values["output_power"], 1400)
        self.assertEqual(values["pv_voltage"], 118.0)
        self.assertEqual(values["pv_current"], 8.5)
        self.assertEqual(values["pv_power"], 1003)
        self.assertEqual(values["battery_power"], 614.4)
        self.assertEqual(values["pv_to_home_power"], 1003.0)
        self.assertEqual(values["pv_to_battery_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_home_power"], 397.0)
        self.assertEqual(values["grid_to_battery_power"], 614.4)

    def test_apply_canonical_measurements_preserves_native_smg_values(self) -> None:
        values = {
            "grid_voltage": 228.0,
            "output_power": 1750,
            "pv_power": 920,
            "battery_average_power": -315.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["grid_voltage"], 228.0)
        self.assertEqual(values["output_power"], 1750)
        self.assertEqual(values["pv_power"], 920)
        self.assertEqual(values["battery_power"], -315.0)

    def test_apply_canonical_measurements_builds_flow_split_from_direct_pv_charge(self) -> None:
        values = {
            "output_power": 109.0,
            "pv_power": 492.0,
            "battery_average_power": 629.0,
            "grid_power": 203.0,
            "pv_charging_power": 666.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["battery_power"], 629.0)
        self.assertEqual(values["pv_to_home_power"], 109.0)
        self.assertEqual(values["pv_to_battery_power"], 383.0)
        self.assertEqual(values["pv_to_grid_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 246.0)
        self.assertEqual(values["grid_to_home_power"], 0.0)

    def test_apply_canonical_measurements_routes_residual_grid_import_to_home(self) -> None:
        values = {
            "output_power": 85.0,
            "pv_power": 0.0,
            "battery_average_power": 36.0,
            "grid_power": 142.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["pv_to_home_power"], 0.0)
        self.assertEqual(values["pv_to_battery_power"], 0.0)
        self.assertEqual(values["battery_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 36.0)
        self.assertEqual(values["grid_to_home_power"], 106.0)

    def test_apply_canonical_measurements_builds_pi18_flow_values(self) -> None:
        values = {
            "output_active_power": 950,
            "pv_input_voltage": 120.0,
            "pv_input_power": 600,
            "battery_voltage": 51.2,
            "battery_charge_current": 0.0,
            "battery_discharge_current": 4.0,
        }

        apply_canonical_measurements("pi18", values)

        self.assertEqual(values["output_power"], 950)
        self.assertEqual(values["pv_power"], 600)
        self.assertEqual(values["battery_power"], -204.8)
        self.assertEqual(values["pv_to_home_power"], 600.0)
        self.assertEqual(values["battery_to_home_power"], 204.8)
        self.assertEqual(values["grid_to_home_power"], 145.2)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_does_not_infer_grid_flow_when_grid_voltage_is_zero(self) -> None:
        values = {
            "grid_voltage": 0.0,
            "output_active_power": 950,
            "pv_input_voltage": 120.0,
            "pv_input_power": 600,
            "battery_voltage": 51.2,
            "battery_charge_current": 0.0,
            "battery_discharge_current": 4.0,
        }

        apply_canonical_measurements("pi18", values)

        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)

    def test_apply_canonical_measurements_ignores_measured_grid_noise_when_grid_is_absent(self) -> None:
        values = {
            "grid_voltage": 0.0,
            "output_power": 85.0,
            "pv_power": 0.0,
            "battery_average_power": 13.0,
            "grid_power": 13.0,
        }

        apply_canonical_measurements("modbus_smg", values)

        self.assertEqual(values["grid_to_home_power"], 0.0)
        self.assertEqual(values["grid_to_battery_power"], 0.0)


if __name__ == "__main__":
    unittest.main()
