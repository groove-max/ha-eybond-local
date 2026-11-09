"""Labelled AC-load estimate: load% × rated VA, never measured active power."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_optional import _rb, _rh
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_estimates import (
    best_available_ac_load_estimate_values,
    estimated_ac_load_values,
)
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.short_ascii import parse_q1


class EstimatedAcLoadUnitTests(unittest.TestCase):
    def test_estimate_is_percent_times_rated_va(self):
        self.assertEqual(
            estimated_ac_load_values({
                "load_percent": 13,
                "short_ascii_rated_voltage": 115,
                "short_ascii_rated_current": 105,
            }),
            {"estimated_ac_load_power": 1569.8},
        )

    def test_estimate_omitted_without_ratings_or_load_percent(self):
        self.assertEqual(estimated_ac_load_values({"load_percent": 13}), {})
        self.assertEqual(
            estimated_ac_load_values({
                "short_ascii_rated_voltage": 115, "short_ascii_rated_current": 105,
            }),
            {},
        )
        self.assertEqual(estimated_ac_load_values({}), {})

    def test_zero_load_percent_is_zero_estimate_not_missing(self):
        self.assertEqual(
            estimated_ac_load_values({
                "load_percent": 0,
                "short_ascii_rated_voltage": 115,
                "short_ascii_rated_current": 105,
            }),
            {"estimated_ac_load_power": 0.0},
        )


class BestAvailableAcLoadEstimateUnitTests(unittest.TestCase):
    _Q1 = {
        "load_percent": 13,
        "short_ascii_rated_voltage": 115,
        "short_ascii_rated_current": 105,
        "estimated_ac_load_power": 1569.8,
    }

    def test_q1_reporting_wins_over_bms_discharge(self):
        """When Q1 publishes real load, do not rewrite it with BMS (± PV)."""
        out = best_available_ac_load_estimate_values({
            **self._Q1, "battery_power": -1300.5, "pv_power": 200.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 1569.8,
            "best_available_ac_load_estimate_source": "q1_percent",
        })
        self.assertNotIn("load_power", out)
        self.assertNotIn("load_power_source", out)

    def test_silent_q1_discharge_uses_bms_dc(self):
        out = best_available_ac_load_estimate_values({
            "load_percent": 0,
            "estimated_ac_load_power": 0.0,
            "battery_power": -1300.5,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 1300.5,
            "best_available_ac_load_estimate_source": "bms_dc",
        })

    def test_discharge_plus_pv_adds_solar_to_bms_dc(self):
        """Shoulder: Q1 silent, pack discharging, PV consumed by the house."""
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "load_percent": 0,
            "battery_power": -403.5,
            "pv_power": 80.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 483.5,
            "best_available_ac_load_estimate_source": "bms_dc_plus_pv",
        })

    def test_discharge_with_zero_pv_stays_bms_dc(self):
        out = best_available_ac_load_estimate_values({
            "load_percent": 0,
            "estimated_ac_load_power": 0.0,
            "battery_power": -403.5,
            "pv_power": 0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 403.5,
            "best_available_ac_load_estimate_source": "bms_dc",
        })

    def test_charging_uses_q1_percent(self):
        out = best_available_ac_load_estimate_values({
            **self._Q1, "battery_power": 182.0, "pv_power": 200.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 1569.8,
            "best_available_ac_load_estimate_source": "q1_percent",
        })

    def test_silent_q1_charge_plus_pv_uses_residual(self):
        """Q1 floor + charging: leftover PV after charge is house AC demand."""
        out = best_available_ac_load_estimate_values({
            "load_percent": 0,
            "estimated_ac_load_power": 0.0,
            "battery_power": 378.0,
            "pv_power": 690.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 312.0,
            "best_available_ac_load_estimate_source": "pv_minus_charge",
        })

    def test_silent_q1_charge_pv_below_charge_clamps_zero(self):
        out = best_available_ac_load_estimate_values({
            "load_percent": 0,
            "estimated_ac_load_power": 0.0,
            "battery_power": 500.0,
            "pv_power": 200.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 0.0,
            "best_available_ac_load_estimate_source": "pv_minus_charge",
        })

    def test_reporting_q1_ignores_charge_pv_residual(self):
        out = best_available_ac_load_estimate_values({
            **self._Q1, "battery_power": 378.0, "pv_power": 690.0,
        })
        self.assertEqual(out["best_available_ac_load_estimate_source"], "q1_percent")
        self.assertEqual(out["best_available_ac_load_estimate"], 1569.8)

    def test_zero_load_percent_with_discharge_uses_bms_dc(self):
        out = best_available_ac_load_estimate_values({
            "load_percent": 0,
            "short_ascii_rated_voltage": 115,
            "short_ascii_rated_current": 105,
            "estimated_ac_load_power": 0.0,
            "battery_power": -850.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 850.0,
            "best_available_ac_load_estimate_source": "bms_dc",
        })

    def test_light_discharge_still_balances_no_deadband(self):
        """Former −25 W floor removed: any discharge contributes to balance."""
        silent = {
            "load_percent": 0,
            "estimated_ac_load_power": 0.0,
        }
        at = best_available_ac_load_estimate_values({
            **silent, "battery_power": -25.0,
        })
        self.assertEqual(at["best_available_ac_load_estimate_source"], "bms_dc")
        self.assertEqual(at["best_available_ac_load_estimate"], 25.0)

        light = best_available_ac_load_estimate_values({
            **silent, "battery_power": -24.9,
        })
        self.assertEqual(light["best_available_ac_load_estimate_source"], "bms_dc")
        self.assertEqual(light["best_available_ac_load_estimate"], 24.9)

        reporting = best_available_ac_load_estimate_values({
            **self._Q1, "battery_power": -25.0,
        })
        self.assertEqual(reporting["best_available_ac_load_estimate_source"], "q1_percent")
        self.assertEqual(reporting["best_available_ac_load_estimate"], 1569.8)

    def test_silent_q1_pv_plus_discharge_is_sum(self):
        """PV 500 + pack discharge 100 → house ≈ 600 (user energy-balance rule)."""
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "battery_power": -100.0,
            "pv_power": 500.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 600.0,
            "best_available_ac_load_estimate_source": "bms_dc_plus_pv",
        })

    def test_silent_q1_idle_pack_uses_full_pv(self):
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "battery_power": 0.0,
            "pv_power": 400.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 400.0,
            "best_available_ac_load_estimate_source": "pv_idle",
        })

    def test_silent_q1_pv_only_when_bms_missing(self):
        out = best_available_ac_load_estimate_values({
            "estimated_ac_load_power": 0.0,
            "pv_power": 400.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 400.0,
            "best_available_ac_load_estimate_source": "pv_only",
        })

    def test_idle_light_discharge_uses_q1(self):
        out = best_available_ac_load_estimate_values({
            **self._Q1, "battery_power": -10.0,
        })
        self.assertEqual(out, {
            "best_available_ac_load_estimate": 1569.8,
            "best_available_ac_load_estimate_source": "q1_percent",
        })

    def test_computes_q1_when_estimated_missing_and_omits_without_inputs(self):
        from_inputs = best_available_ac_load_estimate_values({
            "load_percent": 13,
            "short_ascii_rated_voltage": 115,
            "short_ascii_rated_current": 105,
        })
        self.assertEqual(from_inputs, {
            "best_available_ac_load_estimate": 1569.8,
            "best_available_ac_load_estimate_source": "q1_percent",
        })
        self.assertEqual(best_available_ac_load_estimate_values({}), {})
        # Charge with no PV is a hole, not 0 W of solar. Omit the estimate.
        self.assertEqual(
            best_available_ac_load_estimate_values({"battery_power": 50.0}),
            {},
        )
        # Silent Q1 (0 W) plus a charging or idle pack is still that hole.
        silent_q1 = {
            "load_percent": 0,
            "short_ascii_rated_voltage": 52.0,
            "short_ascii_rated_current": 100.0,
        }
        self.assertEqual(
            best_available_ac_load_estimate_values({**silent_q1, "battery_power": 156.0}),
            {},
        )
        self.assertEqual(
            best_available_ac_load_estimate_values({**silent_q1, "battery_power": 0.0}),
            {},
        )


class EstimatedAcLoadDriverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {
            "RB": _rb(), "F": b"#115.0 105 48.00 60.0\r", "RH": _rh(accuracy=1),
        }
        self.transport = _Transport(responses)
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(767, 255, 1))
        self.state = {}
        self.transport.requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def test_estimate_labelled_load_percent_kept_no_grid_freq_or_measured_load_power(self):
        first = await self.read(0)
        self.assertEqual(first.values["load_percent"], 13)
        self.assertNotIn("estimated_ac_load_power", first.values)
        self.assertNotIn("grid_frequency", first.values)
        self.assertNotIn("load_power", first.values)

        second = await self.read(1)
        self.assertEqual(second.values["load_percent"], 13)
        self.assertEqual(second.values["estimated_ac_load_power"], 1569.8)
        self.assertEqual(second.values["best_available_ac_load_estimate"], 1569.8)
        self.assertEqual(second.values["best_available_ac_load_estimate_source"], "q1_percent")
        self.assertEqual(second.values["output_frequency"], 60)
        self.assertNotIn("grid_frequency", second.values)
        for key in ("load_power", "load_power_from_percent", "load_power_source",
                    "output_power", "output_active_power", "rated_apparent_power"):
            self.assertNotIn(key, second.values)

        schema = load_register_schema(self.driver.register_schema_name)
        description = schema.measurement_description("estimated_ac_load_power")
        self.assertEqual(description.name, "Estimated AC Load Power")
        self.assertTrue(description.enabled_default)
        self.assertEqual(description.unit, "W")
        self.assertEqual(description.device_class, "power")
        best = schema.measurement_description("best_available_ac_load_estimate")
        self.assertEqual(best.name, "Best Available AC Load Estimate")
        self.assertTrue(best.enabled_default)
        self.assertEqual(best.unit, "W")
        self.assertEqual(best.device_class, "power")
        self.assertEqual(best.state_class, "measurement")
        self.assertTrue(best.diagnostic)
        source = schema.measurement_description("best_available_ac_load_estimate_source")
        self.assertTrue(source.diagnostic)
        self.assertFalse(source.enabled_default)
        keys = {item.key for item in schema.measurement_descriptions}
        self.assertNotIn("grid_frequency", keys)
        self.assertNotIn("load_power", keys)
        self.assertNotIn("load_power_source", keys)
        self.assertIn("load_percent", keys)
        self.assertIn("output_frequency", keys)

    async def test_q1_parser_still_does_not_alias_or_invent_power(self):
        values = parse_q1(_responses()["Q1"])
        self.assertEqual(values["load_percent"], 13)
        self.assertEqual(values["output_frequency"], 60)
        for key in ("grid_frequency", "estimated_ac_load_power", "load_power",
                    "best_available_ac_load_estimate"):
            self.assertNotIn(key, values)

    async def test_estimate_withdraws_when_f_ratings_expire(self):
        await self.read(0)
        await self.read(1)
        self.assertEqual((await self.read(2)).values["estimated_ac_load_power"], 1569.8)
        expired = await self.read(902)
        self.assertNotIn("short_ascii_rated_voltage", expired.values)
        self.assertNotIn("estimated_ac_load_power", expired.values)
        self.assertNotIn("best_available_ac_load_estimate", expired.values)
        self.assertEqual(expired.values["load_percent"], 13)


if __name__ == "__main__":
    unittest.main()
