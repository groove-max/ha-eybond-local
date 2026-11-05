"""MPPT soft-miss publishes labelled held estimates and omits live PV keys."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from custom_components.eybond_local.drivers.eybond_short_ascii import (
    EybondShortAsciiDriver,
)
from custom_components.eybond_local.drivers.short_ascii_mppt_held import (
    HELD_TTL_S, MpptHeldEstimates,
)
from custom_components.eybond_local.metadata.register_schema_loader import (
    clear_register_schema_loader_cache,
    load_register_schema,
)


class MpptHeldEstimateUnitTests(unittest.TestCase):
    def test_soft_miss_omits_live_and_publishes_held(self):
        held = MpptHeldEstimates()
        held.observe({"pv_power": 500, "pv_voltage": 92.4, "mppt_temperature": 41}, now=10)
        live = held.project(10)
        self.assertEqual(live["pv_power_held_estimate"], 500)
        self.assertEqual(live["mppt_held_estimate_mode"], "live")
        self.assertEqual(live["mppt_held_estimate_age_seconds"], 0.0)
        later = held.project(10 + 30)
        self.assertEqual(later["pv_power_held_estimate"], 500)
        self.assertEqual(later["pv_voltage_held_estimate"], 92.4)
        self.assertEqual(later["mppt_temperature_held_estimate"], 41)
        self.assertEqual(later["mppt_held_estimate_mode"], "held")
        self.assertNotIn("pv_power", later)
        self.assertEqual(held.project(10 + HELD_TTL_S), {})


class MpptHeldSchemaTests(unittest.TestCase):
    def test_mppt_held_rows_are_diagnostic_opt_in_and_not_energy_totals(self):
        clear_register_schema_loader_cache()
        schema = load_register_schema(EybondShortAsciiDriver().register_schema_name)
        rows = [
            item for item in schema.measurement_descriptions
            if (
                item.key.endswith("_held_estimate")
                or item.key in {
                    "mppt_held_estimate_age_seconds",
                    "mppt_held_estimate_mode",
                }
            )
            and item.key.startswith(("pv_", "mppt_", "dc_load_"))
        ]
        self.assertGreaterEqual(len(rows), 12)
        energy = {
            "mppt_daily_energy_held_estimate",
            "mppt_total_energy_held_estimate",
        }
        text_only = {
            "mppt_work_mode_code_held_estimate",
            "mppt_error_code_held_estimate",
            "mppt_error_held_estimate",
            "mppt_held_estimate_mode",
        }
        for item in rows:
            self.assertTrue(item.diagnostic, item.key)
            self.assertFalse(item.enabled_default, item.key)
            if item.key in energy:
                self.assertIsNone(item.device_class, item.key)
                self.assertIsNone(item.state_class, item.key)
                self.assertNotEqual(item.state_class, "total_increasing")
                self.assertEqual(item.unit, "kWh")
            elif item.key in text_only:
                self.assertIsNone(item.state_class, item.key)
            else:
                self.assertEqual(item.state_class, "measurement", item.key)
                if item.key != "mppt_held_estimate_age_seconds":
                    self.assertIsNotNone(item.device_class, item.key)
