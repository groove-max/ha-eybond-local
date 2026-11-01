"""ADR 0003 / G.V2 G.I1 G.L1: F-gated Pack-V, 3× I ceiling, link-loss omit."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_optional import _rb, _rh
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.drivers.short_ascii_rb_filter import (
    POWER_OVERLOAD_FACTOR,
    RbPublishFilter,
    hard_reject_reason,
    is_link_loss_signature,
    pack_voltage_window,
)
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.short_ascii import parse_rb


def _good_rb_values(**overrides):
    values = {
        "short_ascii_bms_data_available": True,
        "bms_total_voltage": 52.0,
        "battery_soc": 80,
    }
    values.update(overrides)
    return values


class HardRejectUnitTests(unittest.TestCase):
    def test_pre_f_does_not_hard_reject_on_voltage_alone(self):
        # G.V2: without F, 16 V poison is not rejected on V band alone.
        self.assertIsNone(hard_reject_reason(_good_rb_values(bms_total_voltage=16.0)))
        self.assertIsNone(hard_reject_reason(_good_rb_values(bms_total_voltage=1230.1)))
        self.assertIsNone(hard_reject_reason(_good_rb_values(bms_total_voltage=25.6)))

    def test_f24_accepts_maksym_256v_case(self):
        # Maksym: 25.6 V with F battery rating 24 V must publish.
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=24.0)
        lo, hi = pack_voltage_window(24.0)
        self.assertEqual((lo, hi), (18.0, 32.0))
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(bms_total_voltage=25.6), **ratings,
        ))

    def test_f_known_rejects_impossible_pack_voltage(self):
        ratings48 = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_total_voltage=16.0), **ratings48),
            "pack_voltage",
        )
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_total_voltage=1230.1), **ratings48),
            "pack_voltage",
        )
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_total_voltage=25.6), **ratings48),
            "pack_voltage",
        )
        ratings24 = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=24.0)
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_total_voltage=16.0), **ratings24),
            "pack_voltage",
        )

    def test_soc_zero_with_sane_voltage_publishes(self):
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(battery_soc=0, bms_total_voltage=52.0), **ratings,
        ))

    def test_soc_out_of_range_rejected(self):
        self.assertEqual(hard_reject_reason(_good_rb_values(battery_soc=101)), "soc")
        self.assertEqual(hard_reject_reason(_good_rb_values(battery_soc=-1)), "soc")

    def test_current_12x_allowed_under_3x_ceiling(self):
        # G.I1: tip's 1× I gate rejected 1.2×; 3× VA/Vbat must allow it.
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        rated_va = 115.0 * 105.0
        one_x = rated_va / 48.0
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(bms_discharging_current=one_x * 1.2), **ratings,
        ))
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(bms_discharging_current=one_x * POWER_OVERLOAD_FACTOR), **ratings,
        ))
        self.assertEqual(
            hard_reject_reason(
                _good_rb_values(bms_discharging_current=one_x * POWER_OVERLOAD_FACTOR + 1.0),
                **ratings,
            ),
            "current",
        )

    def test_power_between_one_and_three_va_publishes_above_three_rejects(self):
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        rated_va = 115.0 * 105.0
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(battery_power=rated_va * 2.0), **ratings,
        ))
        self.assertEqual(
            hard_reject_reason(_good_rb_values(battery_power=rated_va * 3.01), **ratings),
            "power",
        )

    def test_fingerprint_2203_dies_on_voltage_when_f48_known(self):
        values = _good_rb_values(
            battery_soc=0, bms_total_voltage=16.0, battery_power=-28740.8,
            bms_charging_current=2048.0, bms_discharging_current=3844.3,
        )
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        self.assertEqual(hard_reject_reason(values, **ratings), "pack_voltage")


class LinkLossFilterUnitTests(unittest.TestCase):
    def test_parse_rb_link_loss_is_signature(self):
        parsed = parse_rb(_rb(voltage=0, soc=0))
        self.assertTrue(is_link_loss_signature(parsed))

    def test_link_loss_omits_without_last_good_hold(self):
        # G.L1: no tip-style 180 s republish of last-good.
        filt = RbPublishFilter()
        good = parse_rb(_rb())
        self.assertEqual(filt.decide(good, now=10.0).outcome, "ok")

        lost = filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=40.0)
        self.assertEqual(lost.outcome, "no_data")
        self.assertFalse(lost.keep_previous)
        self.assertTrue(lost.refresh_sampled_at)
        self.assertEqual(lost.values, {"short_ascii_bms_data_available": False})
        self.assertNotIn("battery_soc", lost.values or {})
        self.assertNotIn("bms_total_voltage", lost.values or {})
        self.assertEqual(filt.bms_link_loss_count, 1)

        # Continuous link-loss does not re-count or invent held values.
        again = filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=100.0)
        self.assertEqual(again.outcome, "no_data")
        self.assertEqual(again.values, {"short_ascii_bms_data_available": False})
        self.assertEqual(filt.bms_link_loss_count, 1)

    def test_hard_reject_omits_live_bms_keys(self):
        filt = RbPublishFilter()
        filt.decide(parse_rb(_rb()), now=0.0)
        bad = parse_rb(_rb(voltage=160, soc=0))  # 16.0 V
        decision = filt.decide(
            bad, now=30.0,
            rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0,
        )
        self.assertEqual(decision.outcome, "rejected")
        self.assertFalse(decision.keep_previous)
        self.assertTrue(decision.refresh_sampled_at)
        self.assertEqual(decision.values, {"short_ascii_bms_data_available": False})
        self.assertNotIn("battery_soc", decision.values or {})
        self.assertNotIn("bms_total_voltage", decision.values or {})
        self.assertEqual(filt.rb_hard_reject_count, 1)
        self.assertEqual(filt.bms_link_loss_count, 0)
        self.assertEqual(filt.reading_hold_pending_count, 0)


class LinkLossDriverTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_cached_rb_replay_projects_held_age(self):
        first = await self.read(0)
        self.assertEqual(first.diagnostics["bms_held_estimate_mode"], "live")
        self.assertEqual(first.diagnostics["bms_held_estimate_age_seconds"], 0.0)
        replay = await self.read(10)
        self.assertIn("battery_soc", replay.values)
        self.assertEqual(replay.diagnostics["bms_held_estimate_mode"], "held")
        self.assertGreater(replay.diagnostics["bms_held_estimate_age_seconds"], 5)

    async def test_link_loss_full_omits_bms_keys_and_sets_unavailable(self):
        await self.read(0)
        await self.read(1)  # F
        await self.read(2)  # RH
        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        result = await self.read(31)
        self.assertFalse(result.values["short_ascii_bms_data_available"])
        self.assertFalse(any(key.startswith("bms_") for key in result.values))
        self.assertNotIn("battery_soc", result.values)
        self.assertNotIn("battery_power", result.values)
        self.assertEqual(result.diagnostics["bms_link_loss_count"], 1)
        self.assertEqual(result.diagnostics["bms_held_estimate_mode"], "held")
        self.assertIn("battery_soc_held_estimate", result.diagnostics)
        self.assertNotIn("battery_soc_held_estimate", result.values)
        # Hub FULL freshness: only available=False is present — no last-good V/SoC.
        self.assertEqual(
            self.state[STATE_KEY].samples[0].outcome,
            "no_data",
        )

    async def test_hard_reject_of_impossible_v_omits_live_bms_keys(self):
        await self.read(0)
        await self.read(1)  # F known → V window active
        await self.read(2)
        rb = next(sample for sample in self.state[STATE_KEY].samples if sample.command == "RB")
        rb.next_due = 3
        self.transport.responses["RB"] = _rb(voltage=160, soc=0)  # 16.0 V vs F=48
        rejected = await self.read(3)
        self.assertFalse(rejected.values["short_ascii_bms_data_available"])
        self.assertFalse(any(key.startswith("bms_") for key in rejected.values))
        self.assertNotIn("bms_total_voltage", rejected.values)
        self.assertNotIn("battery_soc", rejected.values)
        self.assertNotIn("battery_power", rejected.values)
        self.assertEqual(rejected.diagnostics["rb_hard_reject_count"], 1)
        self.assertEqual(rejected.diagnostics["bms_held_estimate_mode"], "held")


if __name__ == "__main__":
    unittest.main()
