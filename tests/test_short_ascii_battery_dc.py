"""Measured BMS currents + battery DC watts, gated by RH decimals + F ratings."""

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
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.metadata.register_schema_loader import (
    clear_register_schema_loader_cache,
    load_register_schema,
)
from custom_components.eybond_local.models import ProbeTarget


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

        # Keep RB from refreshing so the ~60 s OptionalSample TTL can fire alone.
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 10_000
        expired = await self.read(64)
        self.assertNotIn("battery_power", expired.values)
        self.assertNotIn("bms_charging_current", expired.values)
        self.assertNotIn("bms_discharging_current", expired.values)
        self.assertNotIn("battery_soc", expired.values)
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

    async def test_f_expiry_strips_held_currents(self):
        await self._arm_rh1_and_f()
        # Hold RB; age F past TTL so re-gate strips I/P while voltage remains.
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        f = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "F")
        rb.next_due = 10_000
        f.next_due = 10_000
        f.sampled_at = 4 - 900  # expired at now=4 while RB (sampled at 3) is still fresh
        stripped = await self.read(4)
        self.assertEqual(stripped.values.get("bms_total_voltage"), 52.0)
        self.assertNotIn("bms_charging_current", stripped.values)
        self.assertNotIn("bms_discharging_current", stripped.values)
        self.assertNotIn("battery_power", stripped.values)

    async def test_incomplete_f_omits_currents_while_rh1(self):
        # F present but incomplete (zero rated current / battery voltage) → no I/P.
        for label, frame in (
            ("rated_current_zero", b"#115.0 000 48.00 60.0\r"),
            ("rated_battery_voltage_zero", b"#115.0 105 00.00 60.0\r"),
        ):
            with self.subTest(label=label):
                self.transport.responses["F"] = frame
                self.transport.responses["RH"] = _rh(accuracy=1)
                self.state.clear()
                self.transport.requests.clear()
                await self.read(0)  # RB without I (F incomplete / RH unread)
                await self.read(1)  # F incomplete
                await self.read(2)  # RH=1
                rb = next(s for s in self.state[STATE_KEY].samples if s.command == "RB")
                rb.next_due = 3
                omitted = await self.read(3)
                self.assertEqual(omitted.values.get("bms_total_voltage"), 52.0)
                self.assertNotIn("bms_charging_current", omitted.values)
                self.assertNotIn("bms_discharging_current", omitted.values)
                self.assertNotIn("battery_power", omitted.values)
                f = next(s for s in self.state[STATE_KEY].samples if s.command == "F")
                self.assertIsNotNone(f.sampled_at)
                rh = next(s for s in self.state[STATE_KEY].samples if s.command == "RH")
                self.assertEqual(
                    rh.values.get("short_ascii_bms_current_display_accuracy"), 1,
                )

    async def test_rh_expiry_strips_held_currents(self):
        await self._arm_rh1_and_f()
        # Hold RB; age RH past TTL so re-gate strips I/P while voltage remains.
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rh = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RH")
        f = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "F")
        rb.next_due = 10_000
        rh.next_due = 10_000
        f.next_due = 10_000
        rh.sampled_at = 4 - 900  # expired at now=4 while RB (sampled at 3) is still fresh
        stripped = await self.read(4)
        self.assertEqual(stripped.values.get("bms_total_voltage"), 52.0)
        self.assertNotIn("bms_charging_current", stripped.values)
        self.assertNotIn("bms_discharging_current", stripped.values)
        self.assertNotIn("battery_power", stripped.values)

    async def test_schema_entities_disabled_by_default(self):
        schema = load_register_schema(self.driver.register_schema_name)
        for key in ("bms_charging_current", "bms_discharging_current", "battery_power"):
            description = schema.measurement_description(key)
            self.assertFalse(description.enabled_default)
            self.assertEqual(description.unit, "A" if "current" in key else "W")
        keys = {item.key for item in schema.measurement_descriptions}
        self.assertNotIn("load_power", keys)
        self.assertNotIn("load_power_source", keys)


if __name__ == "__main__":
    unittest.main()
