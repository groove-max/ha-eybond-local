"""Whole-inverter read-outage expiry, without a collector or wall-clock sleeps."""

from __future__ import annotations

from dataclasses import replace
import unittest
from unittest.mock import patch

from custom_components.eybond_local.drivers.read_result import DriverReadMode, DriverReadResult
from custom_components.eybond_local.payload.modbus import ModbusError
import test_hub


class RuntimeMeasurementExpiryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.hub = test_hub.HubEvidenceAdmissionAndRecoveryTests().make_hub()
        self.now = 100.0
        self.clock = patch(
            "custom_components.eybond_local.runtime.hub.snapshot.monotonic",
            side_effect=lambda: self.now,
        )
        self.clock.start()
        self.addCleanup(self.clock.stop)

    async def read(self, values, *, delta=False, interval=10):
        result = DriverReadResult(
            values, mode=DriverReadMode.DELTA if delta else DriverReadMode.FULL,
        )
        with patch.object(self.hub._driver, "async_read_values", return_value=result):
            return await self.hub.async_refresh(poll_interval=interval)

    async def fail(self, *, interval=10):
        with patch.object(
            self.hub._driver, "async_read_values", side_effect=ModbusError("request_timeout"),
        ):
            return await self.hub.async_refresh(poll_interval=interval)

    async def test_short_miss_retains_then_complete_outage_expires(self):
        await self.read({"battery_voltage": 52.3, "output_power": 1600, "learned_flag": True})
        self.now = 159.9
        short = await self.fail()
        self.assertEqual(short.runtime_value("battery_voltage"), 52.3)
        self.now = 160.0
        expired = await self.fail()
        self.assertTrue(expired.connected)
        for key in ("battery_voltage", "output_power", "learned_flag"):
            self.assertFalse(expired.has_runtime_value(key), key)
        self.assertEqual(expired.values["runtime_measurement_expired_count"], 3)
        self.assertEqual(self.hub._link_manager.reset_calls, 0)

    async def test_five_hour_outage_cannot_reappear_from_last_snapshot(self):
        good = await self.read({"battery_voltage": 52.3, "output_power": 1600})
        self.now += 18000
        expired = await self.fail()
        self.assertFalse(expired.has_runtime_value("output_power"))
        replay = self.hub._build_snapshot(extra_values=good.runtime_values())
        self.assertFalse(replay.has_runtime_value("battery_voltage"))
        self.assertFalse(replay.has_runtime_value("output_power"))
        self.assertEqual(replay.values["runtime_measurement_expired_count"], 2)

    async def test_expiry_uses_three_slow_poll_intervals(self):
        await self.read({"battery_voltage": 52.3}, interval=60)
        self.now = 279.9
        self.assertTrue((await self.fail(interval=60)).has_runtime_value("battery_voltage"))
        self.now = 280
        self.assertFalse((await self.fail(interval=60)).has_runtime_value("battery_voltage"))

    async def test_failed_poll_interval_changes_do_not_extend_the_deadline(self):
        await self.read({"battery_voltage": 52.3})
        self.now = 160
        expired = await self.fail(interval=600)
        self.assertFalse(expired.has_runtime_value("battery_voltage"))

    async def test_collector_metadata_does_not_refresh_inverter(self):
        await self.read({"battery_voltage": 52.3})
        self.now = 160
        snapshot = self.hub._build_snapshot(extra_values={"collector_wifi_ssid": "test"})
        self.assertFalse(snapshot.has_runtime_value("battery_voltage"))
        self.assertEqual(snapshot.runtime_value("collector_wifi_ssid"), "test")
        self.assertTrue(snapshot.connected)

    async def test_empty_delta_does_not_refresh_deadline(self):
        await self.read({"battery_voltage": 52.3})
        self.now = 150
        await self.read({}, delta=True)
        self.now = 160
        snapshot = await self.read({}, delta=True)
        self.assertFalse(snapshot.has_runtime_value("battery_voltage"))

    async def test_healthy_delta_preserves_slow_fields(self):
        await self.read({"battery_voltage": 52.3, "slow_setting": 30})
        for now in (150, 200, 250, 300):
            self.now = now
            snapshot = await self.read({"battery_voltage": 52.4}, delta=True)
            self.assertEqual(snapshot.runtime_value("slow_setting"), 30)
            self.assertEqual(snapshot.values["runtime_measurement_expired_count"], 0)

    async def test_first_delta_after_long_gap_does_not_resurrect_other_values(self):
        await self.read({"battery_voltage": 52.3, "output_power": 1600})
        self.now = 1000
        recovered = await self.read({"battery_voltage": 52.4}, delta=True)
        self.assertEqual(recovered.runtime_value("battery_voltage"), 52.4)
        self.assertFalse(recovered.has_runtime_value("output_power"))
        next_read = await self.read({"output_power": 100}, delta=True)
        self.assertEqual(next_read.runtime_value("output_power"), 100)
        self.assertEqual(next_read.values["runtime_measurement_expired_count"], 0)

    async def test_bootstrap_details_cannot_reseed_expired_measurement(self):
        self.hub._inverter = replace(self.hub._inverter, details={"battery_voltage": 99})
        await self.read({"battery_voltage": 52.3})
        self.now = 160
        self.assertFalse((await self.fail()).has_runtime_value("battery_voltage"))

    async def test_identity_change_clears_expired_key_ownership(self):
        await self.read({"battery_voltage": 52.3})
        self.now = 160
        await self.fail()
        self.hub._inverter = replace(self.hub._inverter, serial_number="REPLACEMENT")
        self.hub._accept_inverter_binding_identity()
        fresh = await self.read({"battery_voltage": 24.5})
        self.assertEqual(fresh.runtime_value("battery_voltage"), 24.5)
        self.assertEqual(fresh.values["runtime_measurement_expired_count"], 0)

    async def test_null_only_result_does_not_renew_other_measurements(self):
        await self.read({"battery_voltage": 52.3})
        self.now = 150
        await self.read({"optional": None}, delta=True)
        self.now = 160
        snapshot = self.hub._build_snapshot()
        self.assertFalse(snapshot.has_runtime_value("battery_voltage"))

    async def test_expired_canonical_power_is_not_reseeded_by_carried_extra(self):
        self.hub._inverter = replace(
            self.hub._inverter, driver_key="pi30", protocol_family="pi30",
        )
        self.hub._accept_inverter_binding_identity()
        good = await self.read({"output_active_power": 1600, "battery_voltage": 52.3})
        self.assertEqual(good.runtime_value("output_power"), 1600)
        self.now = 160
        await self.fail()
        snapshot = self.hub._build_snapshot(extra_values=good.runtime_values())
        for key in ("output_active_power", "output_power", "battery_voltage"):
            self.assertFalse(snapshot.has_runtime_value(key), key)
        self.assertEqual(snapshot.values["runtime_measurement_value_count"], 0)
        self.assertEqual(snapshot.values["runtime_measurement_fresh_count"], 0)

    async def test_non_scalar_diagnostics_do_not_renew_measurements(self):
        await self.read({"battery_voltage": 52.3})
        self.now = 150
        await self.read({"debug_details": {"ok": True}}, delta=True)
        self.now = 160
        self.assertFalse(self.hub._build_snapshot().has_runtime_value("battery_voltage"))

    def test_invalid_intervals_use_finite_default_budget(self):
        for interval in (None, float("nan"), float("inf"), -10, "60", True):
            with self.subTest(interval=interval):
                self.hub._configure_runtime_measurement_freshness(interval)
                self.assertEqual(self.hub._runtime_measurement_max_age_seconds, 60)


if __name__ == "__main__":
    unittest.main()
