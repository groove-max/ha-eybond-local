"""Measured BMS currents + battery DC watts (T5.S3.W), separate from AC-load estimate."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_optional import _rb, _rh
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_battery_dc import battery_dc_power_values
from custom_components.eybond_local.drivers.short_ascii_estimates import estimated_ac_load_values
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.drivers.short_ascii_rb_filter import hard_reject_reason
from custom_components.eybond_local.metadata.register_schema_loader import (
    clear_register_schema_loader_cache,
    load_register_schema,
)
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.short_ascii import parse_rb


class BatteryDcUnitTests(unittest.TestCase):
    def test_charge_positive_discharge_negative(self):
        self.assertEqual(
            battery_dc_power_values({
                "bms_total_voltage": 52.0,
                "bms_charging_current": 3.5,
                "bms_discharging_current": 0.0,
            }),
            {"battery_power": 182.0},
        )
        self.assertEqual(
            battery_dc_power_values({
                "bms_total_voltage": 52.8,
                "bms_charging_current": 0.0,
                "bms_discharging_current": 29.0,
            }),
            {"battery_power": -1531.2},
        )

    def test_omit_without_voltage_or_either_current(self):
        self.assertEqual(battery_dc_power_values({
            "bms_charging_current": 1.0, "bms_discharging_current": 0.0,
        }), {})
        self.assertEqual(battery_dc_power_values({
            "bms_total_voltage": 52.0, "bms_charging_current": 1.0,
        }), {})
        self.assertEqual(battery_dc_power_values({}), {})

    def test_estimate_and_battery_dc_are_independent_keys(self):
        values = {
            "load_percent": 13,
            "short_ascii_rated_voltage": 115,
            "short_ascii_rated_current": 105,
            "bms_total_voltage": 52.0,
            "bms_charging_current": 0.0,
            "bms_discharging_current": 29.0,
        }
        estimate = estimated_ac_load_values(values)
        measured = battery_dc_power_values(values)
        self.assertEqual(estimate["estimated_ac_load_power"], 1569.8)
        self.assertEqual(measured["battery_power"], -1508.0)
        self.assertNotEqual(
            estimate["estimated_ac_load_power"], measured["battery_power"],
        )


class BatteryDcDriverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        clear_register_schema_loader_cache()
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {
            "RB": _rb(charge_raw=0, discharge_raw=290),
            "F": b"#115.0 105 48.00 60.0\r",
            "RH": _rh(accuracy=1),
        }
        self.transport = _Transport(responses)
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(767, 255, 1))
        self.state = {}
        self.transport.requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def _arm_rh1_and_f(self):
        """RB first (stripped), then F, then RH=1; force a gated RB refresh."""
        await self.read(0)  # RB without I (F/RH not ready)
        await self.read(1)  # F
        await self.read(2)  # RH=1
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 3
        return await self.read(3)

    async def test_currents_and_battery_dc_publish_only_when_rh1_and_f(self):
        gated = await self._arm_rh1_and_f()
        self.assertEqual(gated.values["bms_charging_current"], 0.0)
        self.assertEqual(gated.values["bms_discharging_current"], 29.0)
        self.assertEqual(gated.values["bms_total_voltage"], 52.0)
        # 52.0 V × (0 − 29) = −1508.0 W measured DC
        self.assertEqual(gated.values["battery_power"], -1508.0)
        self.assertEqual(gated.values["estimated_ac_load_power"], 1569.8)
        self.assertNotIn("load_power", gated.values)
        self.assertNotEqual(
            gated.values["battery_power"], gated.values["estimated_ac_load_power"],
        )

        # Keep RB from refreshing so the ~60 s OptionalSample TTL can fire alone.
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 10_000
        expired = await self.read(64)
        self.assertNotIn("battery_power", expired.values)
        self.assertNotIn("bms_charging_current", expired.values)
        self.assertNotIn("bms_discharging_current", expired.values)
        self.assertNotIn("battery_soc", expired.values)
        # F still fresh → estimate remains; MX2 split preserved after BMS expiry.
        self.assertEqual(expired.values["estimated_ac_load_power"], 1569.8)
        self.assertEqual(expired.values["load_percent"], 13)

    async def test_rh0_and_unread_rh_omit_currents(self):
        # Before RH: first RB must omit I/P even with F later.
        first = await self.read(0)
        self.assertIn("bms_total_voltage", first.values)
        self.assertNotIn("bms_charging_current", first.values)
        self.assertNotIn("battery_power", first.values)
        await self.read(1)  # F
        # RH=0: still omit after RB refresh.
        self.transport.responses["RH"] = _rh(accuracy=0)
        await self.read(2)  # RH=0
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 3
        omitted = await self.read(3)
        self.assertEqual(omitted.values["bms_total_voltage"], 52.0)
        self.assertNotIn("bms_charging_current", omitted.values)
        self.assertNotIn("bms_discharging_current", omitted.values)
        self.assertNotIn("battery_power", omitted.values)

    async def test_f2_absurd_currents_without_f_never_poison_last_good(self):
        # First optional is RB before F/RH. Absurd discharge must not publish or
        # stick as keep_previous / last-good once F later arrives.
        self.transport.responses["RB"] = _rb(charge_raw=0, discharge_raw=19968)  # 1996.8 A
        t0 = await self.read(0)
        self.assertEqual(t0.values.get("bms_total_voltage"), 52.0)
        self.assertNotIn("bms_discharging_current", t0.values)
        self.assertNotIn("battery_power", t0.values)
        await self.read(1)  # F
        await self.read(2)  # RH=1
        # Held RB sample still has no I (stripped at parse). Force RB refresh:
        # with F+RH present, absurd current must hard-reject and leave no poison.
        self.transport.responses["RB"] = _rb(charge_raw=0, discharge_raw=19968)
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 3
        rejected = await self.read(3)
        self.assertIn("RB=rejected", rejected.diagnostics["short_ascii_optional_status"])
        self.assertNotIn("bms_discharging_current", rejected.values)
        self.assertNotIn("battery_power", rejected.values)
        # Prior good sample had V/SoC only — still no I/P poison.
        self.assertEqual(rejected.values.get("bms_total_voltage"), 52.0)
        self.assertNotIn("battery_power", self.state[STATE_KEY].rb_filter.last_good)

    async def test_hard_reject_absurd_current_when_keys_and_ratings_present(self):
        await self._arm_rh1_and_f()
        # Sane V, absurd discharge (~384.4 A after /10) above F VA/Vbat ≈ 251.6 A.
        self.transport.responses["RB"] = _rb(charge_raw=0, discharge_raw=3844)
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 33
        rejected = await self.read(33)
        self.assertIn("RB=rejected", rejected.diagnostics["short_ascii_optional_status"])
        self.assertEqual(rejected.diagnostics["rb_hard_reject_count"], 1)
        # Prior good gated sample retained for remaining ~60 s TTL (not 180 s hold).
        self.assertEqual(rejected.values["battery_power"], -1508.0)
        self.assertEqual(rejected.values["bms_discharging_current"], 29.0)
        # Direct gate still names the live key.
        parsed = parse_rb(_rb(charge_raw=0, discharge_raw=3844))
        parsed.update(battery_dc_power_values(parsed))
        self.assertEqual(
            hard_reject_reason(
                parsed,
                rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0,
            ),
            "current",
        )

    async def test_schema_entities_disabled_by_default_and_estimate_untouched(self):
        schema = load_register_schema(self.driver.register_schema_name)
        for key in ("bms_charging_current", "bms_discharging_current", "battery_power"):
            description = schema.measurement_description(key)
            self.assertFalse(description.enabled_default)
            self.assertEqual(description.unit, "A" if "current" in key else "W")
        estimate = schema.measurement_description("estimated_ac_load_power")
        self.assertTrue(estimate.enabled_default)
        self.assertEqual(estimate.name, "Estimated AC Load Power")
        keys = {item.key for item in schema.measurement_descriptions}
        self.assertNotIn("load_power", keys)
        self.assertNotIn("load_power_source", keys)


if __name__ == "__main__":
    unittest.main()
