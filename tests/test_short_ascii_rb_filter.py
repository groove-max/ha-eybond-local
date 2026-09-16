"""ADR 0003: RB hard-rejects + dual TTL (60 s optional vs 180 s link-loss hold)."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_optional import _rb
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.drivers.short_ascii_rb_filter import (
    LINK_LOSS_HOLD_S,
    PACK_VOLTAGE_MAX_V,
    PACK_VOLTAGE_MIN_V,
    RbPublishFilter,
    hard_reject_reason,
    is_link_loss_signature,
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
    def test_corrupt_pack_voltage_rejected(self):
        self.assertEqual(hard_reject_reason(_good_rb_values(bms_total_voltage=16.0)), "pack_voltage")
        self.assertEqual(hard_reject_reason(_good_rb_values(bms_total_voltage=1230.1)), "pack_voltage")
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_total_voltage=PACK_VOLTAGE_MIN_V - 0.1)),
            "pack_voltage",
        )
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_total_voltage=PACK_VOLTAGE_MAX_V + 0.1)),
            "pack_voltage",
        )

    def test_soc_zero_with_sane_voltage_publishes(self):
        self.assertIsNone(hard_reject_reason(_good_rb_values(battery_soc=0, bms_total_voltage=52.0)))

    def test_soc_out_of_range_rejected(self):
        self.assertEqual(hard_reject_reason(_good_rb_values(battery_soc=101)), "soc")
        self.assertEqual(hard_reject_reason(_good_rb_values(battery_soc=-1)), "soc")

    def test_power_between_one_and_three_va_publishes_above_three_rejects(self):
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        rated_va = 115.0 * 105.0
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(battery_power=rated_va * 2.0), **ratings,
        ))
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(battery_power=-(rated_va * 2.9)), **ratings,
        ))
        self.assertEqual(
            hard_reject_reason(_good_rb_values(battery_power=rated_va * 3.01), **ratings),
            "power",
        )

    def test_current_above_inverter_limit_rejected_when_ratings_present(self):
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        # F VA / F Vbat ≈ 251.6 A
        self.assertIsNone(hard_reject_reason(
            _good_rb_values(bms_discharge_current=250.0), **ratings,
        ))
        self.assertEqual(
            hard_reject_reason(_good_rb_values(bms_discharge_current=260.0), **ratings),
            "current",
        )

    def test_fingerprint_2203_dies_on_voltage_not_soc_or_power_band(self):
        # SoC 0 + 16 V + |P|≈28.7 kW (< 3×12 kVA). Must die on V.
        values = _good_rb_values(
            battery_soc=0, bms_total_voltage=16.0, battery_power=-28740.8,
            bms_charging_current=2048.0, bms_discharge_current=3844.3,
        )
        ratings = dict(rated_voltage=115.0, rated_current=105.0, rated_battery_voltage=48.0)
        self.assertEqual(hard_reject_reason(values, **ratings), "pack_voltage")


class LinkLossFilterUnitTests(unittest.TestCase):
    def test_parse_rb_link_loss_is_signature(self):
        parsed = parse_rb(_rb(voltage=0, soc=0))
        self.assertTrue(is_link_loss_signature(parsed))

    def test_hold_caps_at_180s_from_last_good_without_refresh(self):
        filt = RbPublishFilter()
        good = parse_rb(_rb())
        first = filt.decide(good, now=10.0)
        self.assertEqual(first.outcome, "ok")
        self.assertTrue(first.refresh_sampled_at)
        self.assertEqual(filt.last_good_at, 10.0)

        held = filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=40.0)
        self.assertEqual(held.outcome, "link_loss_hold")
        self.assertTrue(held.keep_previous)
        self.assertFalse(held.refresh_sampled_at)
        # Cap is from last-good time, not from this link-loss frame.
        self.assertEqual(filt.hold_until, 10.0 + LINK_LOSS_HOLD_S)
        self.assertEqual(held.values["battery_soc"], 80)

        later = filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=100.0)
        self.assertEqual(later.outcome, "link_loss_hold")
        self.assertEqual(filt.hold_until, 10.0 + LINK_LOSS_HOLD_S)

        past = filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=10.0 + LINK_LOSS_HOLD_S)
        self.assertEqual(past.outcome, "no_data")
        self.assertFalse(filt.last_good)
        self.assertIsNone(filt.last_good_at)
        self.assertIsNone(filt.hold_until)

    def test_link_loss_count_increments_on_streak_entry_not_each_poll(self):
        filt = RbPublishFilter()
        filt.decide(parse_rb(_rb()), now=0.0)
        self.assertEqual(filt.bms_link_loss_count, 0)
        self.assertEqual(filt.rb_hard_reject_count, 0)
        self.assertEqual(filt.reading_hold_pending_count, 0)

        first = filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=10.0)
        self.assertEqual(first.outcome, "link_loss_hold")
        self.assertEqual(filt.bms_link_loss_count, 1)
        # Same streak: continuous link-loss frames must not re-count.
        for t in (40.0, 70.0, 100.0):
            self.assertEqual(
                filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=t).outcome,
                "link_loss_hold",
            )
            self.assertEqual(filt.bms_link_loss_count, 1)
        # Hold expired; still link-loss signature — same streak, still 1.
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=0.0 + LINK_LOSS_HOLD_S).outcome,
            "no_data",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)
        # Recover then drop again → second transition.
        filt.decide(parse_rb(_rb()), now=200.0)
        filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=210.0)
        self.assertEqual(filt.bms_link_loss_count, 2)
        self.assertEqual(filt.rb_hard_reject_count, 0)
        self.assertEqual(filt.reading_hold_pending_count, 0)
        self.assertEqual(
            filt.diagnostic_counters(),
            {
                "bms_link_loss_count": 2,
                "rb_hard_reject_count": 0,
                "reading_hold_pending_count": 0,
            },
        )

    def test_link_loss_without_last_good_still_counts_once(self):
        filt = RbPublishFilter()
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=1.0).outcome,
            "no_data",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=2.0).outcome,
            "no_data",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)

    def test_hard_reject_increments_reject_count_not_link_loss_or_hold_pending(self):
        filt = RbPublishFilter()
        filt.decide(parse_rb(_rb()), now=0.0)
        bad = parse_rb(_rb(voltage=160, soc=0))  # 16.0 V
        self.assertEqual(filt.decide(bad, now=30.0).outcome, "rejected")
        self.assertEqual(filt.bms_link_loss_count, 0)
        self.assertEqual(filt.rb_hard_reject_count, 1)
        self.assertEqual(filt.reading_hold_pending_count, 0)
        # Second reject bumps again.
        self.assertEqual(filt.decide(bad, now=31.0).outcome, "rejected")
        self.assertEqual(filt.rb_hard_reject_count, 2)

    def test_hard_reject_mid_link_loss_does_not_re_count_streak(self):
        filt = RbPublishFilter()
        filt.decide(parse_rb(_rb()), now=0.0)
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=10.0).outcome,
            "link_loss_hold",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)
        bad = parse_rb(_rb(voltage=160, soc=0))  # 16.0 V junk mid-dropout
        self.assertEqual(filt.decide(bad, now=20.0).outcome, "rejected")
        self.assertEqual(filt.rb_hard_reject_count, 1)
        # Still same streak after junk — must not bump link-loss count.
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=30.0).outcome,
            "link_loss_hold",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)

    def test_clear_mid_link_loss_streak_does_not_double_count(self):
        # Envelope/checksum path calls clear(); streak flag must survive.
        filt = RbPublishFilter()
        filt.decide(parse_rb(_rb()), now=0.0)
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=10.0).outcome,
            "link_loss_hold",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)
        filt.clear()
        self.assertFalse(filt.last_good)
        self.assertIsNone(filt.hold_until)
        # Continued link-loss after clear is still the same streak.
        self.assertEqual(
            filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=20.0).outcome,
            "no_data",
        )
        self.assertEqual(filt.bms_link_loss_count, 1)
        # True ok recovery ends the streak; next dropout is a new transition.
        filt.decide(parse_rb(_rb()), now=30.0)
        filt.decide(parse_rb(_rb(voltage=0, soc=0)), now=40.0)
        self.assertEqual(filt.bms_link_loss_count, 2)

    def test_hard_reject_does_not_start_hold_or_replace_last_good(self):
        filt = RbPublishFilter()
        filt.decide(parse_rb(_rb()), now=0.0)
        bad = parse_rb(_rb(voltage=160, soc=0))  # 16.0 V
        self.assertEqual(bad["bms_total_voltage"], 16.0)
        decision = filt.decide(bad, now=30.0)
        self.assertEqual(decision.outcome, "rejected")
        self.assertIsNone(filt.hold_until)
        self.assertEqual(filt.last_good["bms_total_voltage"], 52.0)


class RbFilterDriverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {"RB": _rb(), "F": b"#115.0 105 48.00 60.0\r"}
        self.transport = _Transport(responses)
        self.inverter = await self.driver.async_probe(self.transport, ProbeTarget(767, 255, 1))
        self.state = {}
        self.transport.requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def test_impossible_voltage_after_good_envelope_is_not_published(self):
        for raw_voltage, decoded in ((160, 16.0), (12301, 1230.1)):
            with self.subTest(decoded_v=decoded):
                await self.asyncSetUp()
                await self.read(0); await self.read(1)
                self.assertEqual((await self.read(2)).values.get("bms_total_voltage"), 52)
                self.transport.responses["RB"] = _rb(voltage=raw_voltage, soc=0 if raw_voltage == 160 else 1)
                rejected = await self.read(31)
                self.assertNotEqual(rejected.values.get("bms_total_voltage"), decoded)
                self.assertEqual(rejected.values.get("bms_total_voltage"), 52)
                self.assertEqual(rejected.values.get("battery_soc"), 80)
                self.assertIn("RB=rejected", rejected.diagnostics["short_ascii_optional_status"])

    async def test_soc_zero_with_sane_voltage_publishes(self):
        await self.read(0); await self.read(1)
        self.transport.responses["RB"] = _rb(soc=0)
        result = await self.read(31)
        self.assertEqual(result.values["battery_soc"], 0)
        self.assertEqual(result.values["bms_total_voltage"], 52)
        self.assertTrue(result.values["short_ascii_bms_data_available"])

    async def test_link_loss_hold_keeps_last_good_stale_until_180s(self):
        await self.read(0); await self.read(1)
        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        held = await self.read(31)
        self.assertEqual(held.values["battery_soc"], 80)
        self.assertGreaterEqual(held.diagnostics["short_ascii_rb_age_seconds"], 30)
        # Avoid further RB polls; dual-clock only (hold capped from last-good ~0).
        rb = self.state[STATE_KEY].samples[0]
        rb.next_due = 10_000
        # Past optional 60 s TTL but inside 180 s hold: still present, still stale.
        still = await self.read(100)
        self.assertEqual(still.values["battery_soc"], 80)
        self.assertGreater(still.diagnostics["short_ascii_rb_age_seconds"], 60)
        # last_good_at ≈ 0 → hold_until 180; after that values must clear.
        expired = await self.read(181)
        self.assertNotIn("battery_soc", expired.values)
        self.assertNotIn("bms_total_voltage", expired.values)

    async def test_ttl_expiry_clears_last_good_so_link_loss_cannot_resurrect(self):
        # critical regression: TTL must kill last_good; later link-loss must not
        # republish with sampled_at is None.
        await self.read(0); await self.read(1)
        rb = self.state[STATE_KEY].samples[0]
        rb.next_due = 10_000
        expired = await self.read(61)
        self.assertNotIn("battery_soc", expired.values)
        self.assertIsNone(rb.sampled_at)
        self.assertFalse(self.state[STATE_KEY].rb_filter.last_good)

        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        rb.next_due = 62
        resurrected = await self.read(62)
        self.assertNotIn("battery_soc", resurrected.values)
        self.assertNotIn("bms_total_voltage", resurrected.values)
        # Never publish held BMS fields without a freshness clock.
        self.assertNotIn("battery_soc", rb.values)
        self.assertFalse(self.state[STATE_KEY].rb_filter.last_good)
        self.assertNotIn("RB=link_loss_hold", resurrected.diagnostics["short_ascii_optional_status"])

    async def test_continuous_link_loss_stops_after_180s_from_last_good(self):
        # serious regression: every link-loss frame must not sticky-extend hold.
        await self.read(0); await self.read(1)
        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        self.assertEqual((await self.read(31)).values.get("battery_soc"), 80)
        for t in (61, 91, 121, 151):
            still = await self.read(t)
            self.assertEqual(still.values.get("battery_soc"), 80, msg=f"t={t}")
            self.assertIn("RB=link_loss_hold", still.diagnostics["short_ascii_optional_status"])
        gone = await self.read(181)
        self.assertNotIn("battery_soc", gone.values)
        self.assertNotIn("bms_total_voltage", gone.values)

    async def test_checksum_fail_drops_without_starting_hold(self):
        await self.read(0); await self.read(1)
        wire = bytearray(_rb())
        wire[-2] ^= 1
        self.transport.responses["RB"] = bytes(wire)
        result = await self.read(31)
        self.assertNotIn("battery_soc", result.values)
        self.assertIn("RB=invalid_response", result.diagnostics["short_ascii_optional_status"])
        # Still gone well inside a hypothetical 180 s window.
        rb = self.state[STATE_KEY].samples[0]
        rb.next_due = 10_000
        self.assertNotIn("battery_soc", (await self.read(100)).values)
        reads = self.state[STATE_KEY]
        self.assertIsNone(reads.rb_filter.hold_until)
        self.assertFalse(reads.rb_filter.last_good)

    async def test_optional_rb_sixty_second_ttl_without_link_loss(self):
        await self.read(0); await self.read(1)
        # Do not re-query RB; age past 60 s clears without link-loss hold.
        self.state[STATE_KEY].samples[0].next_due = 10_000
        late = await self.read(61)
        self.assertNotIn("battery_soc", late.values)
        self.assertIn("RB=expired", late.diagnostics["short_ascii_optional_status"])

    async def test_diagnostic_counters_publish_transitions_and_hold_pending_stays_zero(self):
        await self.read(0); await self.read(1)
        baseline = await self.read(2)
        self.assertEqual(baseline.diagnostics["bms_link_loss_count"], 0)
        self.assertEqual(baseline.diagnostics["rb_hard_reject_count"], 0)
        self.assertEqual(baseline.diagnostics["reading_hold_pending_count"], 0)

        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        held = await self.read(31)
        self.assertEqual(held.diagnostics["bms_link_loss_count"], 1)
        self.assertEqual(held.diagnostics["rb_hard_reject_count"], 0)
        self.assertEqual(held.diagnostics["reading_hold_pending_count"], 0)
        # No RB re-poll; counters still published (hub replaces diagnostics).
        rb = self.state[STATE_KEY].samples[0]
        rb.next_due = 10_000
        idle = await self.read(32)
        self.assertEqual(idle.diagnostics["bms_link_loss_count"], 1)
        self.assertEqual(idle.diagnostics["rb_hard_reject_count"], 0)
        self.assertEqual(idle.diagnostics["reading_hold_pending_count"], 0)

        for t in (61, 91, 121):
            still = await self.read(t)
            self.assertEqual(still.diagnostics["bms_link_loss_count"], 1)
            self.assertEqual(still.diagnostics["rb_hard_reject_count"], 0)
            self.assertEqual(still.diagnostics["reading_hold_pending_count"], 0)

        # Recover then second dropout.
        self.transport.responses["RB"] = _rb()
        rb.next_due = 150
        recovered = await self.read(150)
        self.assertEqual(recovered.values["battery_soc"], 80)
        self.assertEqual(recovered.diagnostics["bms_link_loss_count"], 1)

        self.transport.responses["RB"] = _rb(voltage=0, soc=0)
        second = await self.read(181)
        self.assertEqual(second.diagnostics["bms_link_loss_count"], 2)
        self.assertEqual(second.diagnostics["rb_hard_reject_count"], 0)
        self.assertEqual(second.diagnostics["reading_hold_pending_count"], 0)

    async def test_hard_reject_publishes_reject_counter(self):
        await self.read(0); await self.read(1)
        await self.read(2)
        self.transport.responses["RB"] = _rb(voltage=160, soc=0)  # 16.0 V
        rejected = await self.read(31)
        self.assertIn("RB=rejected", rejected.diagnostics["short_ascii_optional_status"])
        self.assertEqual(rejected.diagnostics["rb_hard_reject_count"], 1)
        self.assertEqual(rejected.diagnostics["bms_link_loss_count"], 0)
        self.assertEqual(rejected.diagnostics["reading_hold_pending_count"], 0)


if __name__ == "__main__":
    unittest.main()
