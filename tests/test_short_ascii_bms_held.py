"""BMS held estimates: mirror live, hold ≤180 s across link-loss."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from custom_components.eybond_local.drivers.short_ascii_bms_held import (
    BMS_HOLD_KEYS,
    HELD_TTL_S,
    BmsHeldEstimates,
    bms_held_for,
    held_estimate_key,
)
from custom_components.eybond_local.drivers.short_ascii_estimates import (
    best_available_ac_load_estimate_values,
)
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema


def _live(**extra: object) -> dict[str, object]:
    base: dict[str, object] = {
        "short_ascii_bms_data_available": True,
        "bms_total_voltage": 52.6,
        "battery_soc": 55,
        "bms_charging_current": 6.2,
        "bms_discharging_current": 0.0,
        "battery_power": 326.1,
        "short_ascii_bms_charge_path_enabled": True,
        "short_ascii_bms_discharge_path_enabled": True,
    }
    base.update(extra)
    return base


class BmsHeldEstimateUnitTests(unittest.TestCase):
    def test_mirrors_live_with_zero_age(self):
        held = BmsHeldEstimates()
        out = held.apply(_live(), now=10.0)
        self.assertEqual(out["bms_held_estimate_mode"], "live")
        self.assertEqual(out["bms_held_estimate_age_seconds"], 0.0)
        self.assertEqual(out["battery_soc_held_estimate"], 55)
        self.assertEqual(out["battery_power_held_estimate"], 326.1)
        self.assertEqual(out["bms_total_voltage_held_estimate"], 52.6)
        self.assertTrue(out["short_ascii_bms_charge_path_enabled_held_estimate"])

    def test_holds_across_link_loss_within_ttl(self):
        held = BmsHeldEstimates()
        held.apply(_live(), now=0.0)
        gap = {
            "short_ascii_bms_data_available": False,
            "estimated_ac_load_power": 0.0,
        }
        out = held.apply(gap, now=179.0)
        self.assertEqual(out["bms_held_estimate_mode"], "held")
        self.assertEqual(out["bms_held_estimate_age_seconds"], 179.0)
        self.assertEqual(out["battery_soc_held_estimate"], 55)
        self.assertEqual(out["battery_power_held_estimate"], 326.1)
        self.assertNotIn("battery_soc", out)
        self.assertNotIn("battery_power", out)

    def test_expires_at_ttl(self):
        held = BmsHeldEstimates()
        held.apply(_live(), now=0.0)
        self.assertEqual(held.apply({"short_ascii_bms_data_available": False}, now=HELD_TTL_S), {})
        self.assertEqual(held.values, {})
        self.assertEqual(held.key_sampled_at, {})

    def test_partial_live_keeps_last_battery_power(self):
        """RH/F gate omits currents: do not wipe last-good watts from the hold cache."""
        held = BmsHeldEstimates()
        held.apply(_live(), now=0.0)
        partial = {
            "short_ascii_bms_data_available": True,
            "bms_total_voltage": 52.7,
            "battery_soc": 56,
            # no charging/discharging/battery_power this cycle
        }
        out = held.apply(partial, now=30.0)
        self.assertEqual(out["bms_held_estimate_mode"], "live_partial")
        self.assertEqual(out["bms_total_voltage_held_estimate"], 52.7)
        self.assertEqual(out["battery_soc_held_estimate"], 56)
        self.assertEqual(out["battery_power_held_estimate"], 326.1)
        self.assertEqual(out["bms_charging_current_held_estimate"], 6.2)
        self.assertGreater(out["bms_held_estimate_age_seconds"], 0.0)

    def test_partial_then_link_loss_still_holds_power(self):
        held = BmsHeldEstimates()
        held.apply(_live(), now=0.0)
        held.apply({
            "short_ascii_bms_data_available": True,
            "bms_total_voltage": 52.7,
            "battery_soc": 56,
        }, now=20.0)
        out = held.apply({"short_ascii_bms_data_available": False}, now=50.0)
        self.assertEqual(out["bms_held_estimate_mode"], "held")
        self.assertEqual(out["battery_power_held_estimate"], 326.1)

    def test_live_without_voltage_does_not_seed(self):
        held = BmsHeldEstimates()
        out = held.apply({"short_ascii_bms_data_available": True, "battery_soc": 40}, now=1.0)
        self.assertEqual(out, {})

    def test_project_without_observe_keeps_age(self):
        held = BmsHeldEstimates()
        held.observe(_live(), now=10.0)
        out = held.project(30.0)
        self.assertEqual(out["bms_held_estimate_mode"], "held")
        self.assertEqual(out["bms_held_estimate_age_seconds"], 20.0)
        self.assertEqual(out["battery_soc_held_estimate"], 55)

    def test_runtime_state_reuses_cache(self):
        state: dict = {}
        a = bms_held_for(state)
        b = bms_held_for(state)
        self.assertIs(a, b)
        a.apply(_live(), now=0.0)
        self.assertEqual(bms_held_for(state).values["battery_soc"], 55)

    def test_hold_keys_cover_schema_held_entities(self):
        from custom_components.eybond_local.drivers.eybond_short_ascii import (
            EybondShortAsciiDriver,
        )
        schema = load_register_schema(EybondShortAsciiDriver().register_schema_name)
        keys = {item.key for item in schema.measurement_descriptions}
        bin_keys = {item.key for item in schema.binary_sensor_descriptions}
        for source in BMS_HOLD_KEYS:
            held = held_estimate_key(source)
            if source.startswith("short_ascii_bms_") and source.endswith("_enabled"):
                self.assertIn(held, bin_keys, held)
            else:
                self.assertIn(held, keys, held)
        self.assertIn("bms_held_estimate_age_seconds", keys)
        self.assertIn("bms_held_estimate_mode", keys)

    def test_held_schema_rows_are_diagnostic(self):
        from custom_components.eybond_local.drivers.eybond_short_ascii import (
            EybondShortAsciiDriver,
        )
        schema = load_register_schema(EybondShortAsciiDriver().register_schema_name)
        default_on = {
            "bms_total_voltage_held_estimate",
            "battery_soc_held_estimate",
            "bms_charging_current_held_estimate",
            "bms_discharging_current_held_estimate",
            "battery_power_held_estimate",
        }
        for source in BMS_HOLD_KEYS:
            held = held_estimate_key(source)
            if source.startswith("short_ascii_bms_") and source.endswith("_enabled"):
                description = schema.binary_sensor_description(held)
            else:
                description = schema.measurement_description(held)
            self.assertTrue(description.diagnostic, held)
            if held in default_on:
                self.assertTrue(description.enabled_default, held)
                self.assertEqual(description.state_class, "measurement", held)
                self.assertIsNotNone(description.device_class, held)
        for key in ("bms_held_estimate_age_seconds", "bms_held_estimate_mode"):
            description = schema.measurement_description(key)
            self.assertTrue(description.diagnostic, key)


class BestAvailableIgnoresHeldBatteryTests(unittest.TestCase):
    def test_silent_q1_does_not_use_held_discharge(self):
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "battery_power_held_estimate": -400.0,
            "pv_power": 50.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 50.0,
            "best_available_ac_load_estimate_source": "pv_only",
        })

    def test_live_battery_ignores_held(self):
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "battery_power": -100.0,
            "battery_power_held_estimate": -900.0,
        })
        self.assertEqual(out["best_available_ac_load_estimate"], 100.0)
        self.assertEqual(out["best_available_ac_load_estimate_source"], "bms_dc")

    def test_silent_q1_held_charge_does_not_build_residual(self):
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "battery_power_held_estimate": 300.0,
            "pv_power": 500.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 500.0,
            "best_available_ac_load_estimate_source": "pv_only",
        })


if __name__ == "__main__":
    unittest.main()
