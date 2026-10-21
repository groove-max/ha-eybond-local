"""External PI30 setting changes must reach runtime without local writes."""

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
from unittest import IsolatedAsyncioTestCase
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from custom_components.eybond_local.drivers import pi30
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.payload.pi30 import Pi30Error
from test_pi30_capability_acquisition import QPIRI, _saved_binding, _session


class Pi30PeriodicSettingsTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = pi30.Pi30Driver()
        self.inverter = _saved_binding()
        self.inverter.capabilities = load_driver_profile("pi30_ascii.json").capabilities
        self.inverter.details.update(output_source_priority="Utility first")
        self.state = {}
        fields = QPIRI.split()
        fields[16] = "2"
        self.qpiri = " ".join(fields)
        self.session = _session({"QPIRI": self.qpiri, "QFLAG": "EaDx"})
        for patcher in (
            patch.object(pi30.Pi30Driver, "_session", return_value=self.session),
            patch.object(pi30, "async_acquire_charge_current_controls", new=AsyncMock(return_value={})),
            patch.object(pi30, "_async_collect_runtime_values", new=AsyncMock(
                side_effect=lambda *args, **kwargs: (
                    {"operating_mode_code": "L", "battery_voltage": 26.5}, frozenset(), {},
                ),
            )),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    async def read(self, now):
        return await self.driver.async_read_values(
            None, self.inverter, runtime_state=self.state,
            poll_interval=10, now_monotonic=now,
        )

    def commands(self):
        return [call.args[0] for call in self.session.request.await_args_list]

    async def test_external_changes_are_read_periodically_one_command_per_cycle(self):
        await self.read(100)
        await self.read(159)
        self.assertEqual(self.commands(), [])
        first = await self.read(160)
        self.assertEqual(first.values["output_source_priority"], "SBU first")
        self.assertEqual(first.values["output_source_priority_code"], 2)
        self.assertEqual(first.values["battery_bulk_voltage"], 28.7)
        self.assertEqual(self.commands(), ["QPIRI"])
        flags = await self.read(170)
        self.assertIs(flags.values["buzzer_enabled"], True)
        self.assertIs(flags.values["lcd_backlight_enabled"], False)
        self.assertEqual(self.commands(), ["QPIRI", "QFLAG"])
        await self.read(219)
        self.assertEqual(self.commands(), ["QPIRI", "QFLAG"])
        await self.read(221)
        self.assertEqual(self.commands(), ["QPIRI", "QFLAG", "QPIRI"])
        self.assertEqual(self.inverter.details["output_source_priority"], "Utility first")

    async def test_failed_settings_do_not_drop_telemetry_or_disable_future_reads(self):
        for failure in (
            Pi30Error("nak"), Pi30Error("request_timeout"),
            TimeoutError(), ConnectionResetError(), "bad",
        ):
            with self.subTest(failure=repr(failure)):
                self.state = {}
                await self.read(100)
                self.session.request.side_effect = (
                    lambda command: self.qpiri if command == "QPIRI" else "EaDx"
                )
                if isinstance(failure, Exception):
                    self.session.request.side_effect = failure
                else:
                    self.session.request.side_effect = lambda command: failure
                result = await self.read(160)
                self.assertEqual(result.values["battery_voltage"], 26.5)
                self.assertNotIn("output_source_priority", result.values)
                self.assertFalse(result.removed_keys)
                self.assertIn(result.diagnostics["pi30_settings_refresh_outcome"], ("error", "timeout"))
                count = self.session.request.await_count
                await self.read(161)  # Other due group can fail independently.
                await self.read(170)
                self.assertEqual(self.session.request.await_count, count + 1)
                self.session.request.side_effect = (
                    lambda command: self.qpiri if command == "QPIRI" else "EaDx"
                )
                result = await self.read(221)
                self.assertEqual(result.values["output_source_priority"], "SBU first")

    async def test_no_extra_reads_without_fresh_telemetry_or_runtime_state(self):
        await self.read(100)
        with patch.object(pi30, "_async_collect_runtime_values", new=AsyncMock(
            return_value=({}, frozenset(), {}),
        )):
            await self.read(200)
        self.assertEqual(self.commands(), [])
        await self.driver.async_read_values(None, self.inverter, now_monotonic=300)
        self.assertEqual(self.commands(), [])
        result = await self.read(210)
        self.assertEqual(result.values["output_source_priority"], "SBU first")

    async def test_pending_write_confirmation_takes_priority_and_is_not_duplicated(self):
        await self.read(100)
        cap = self.inverter.get_capability("output_source_priority")
        self.state[pi30._SETTING_READBACK_KEY] = pi30._PendingSettingReadback(
            self.inverter, cap, "QPIRI", remaining=2,
        )
        for now in (160, 170):
            result = await self.read(now)
            self.assertEqual(result.values["output_source_priority"], "SBU first")
        self.assertEqual(self.commands(), ["QPIRI", "QPIRI"])
        await self.read(180)
        self.assertEqual(self.commands(), ["QPIRI", "QPIRI", "QFLAG"])
        await self.read(229)
        self.assertEqual(len(self.commands()), 3)

    async def test_reset_rebind_and_cancellation_do_not_publish_late_response(self):
        for mode in ("reset", "rebind", "cancel"):
            with self.subTest(mode=mode):
                self.state = {}
                await self.read(100)

                async def request(command):
                    if mode == "reset":
                        self.state.clear()
                    elif mode == "rebind":
                        other = replace(self.inverter)
                        await self.driver.async_read_values(
                            None, other, runtime_state=self.state, now_monotonic=161,
                        )
                    else:
                        raise asyncio.CancelledError
                    return self.qpiri

                self.session.request.side_effect = request
                if mode == "cancel":
                    with self.assertRaises(asyncio.CancelledError):
                        await self.read(160)
                else:
                    result = await self.read(160)
                    self.assertNotIn("output_source_priority", result.values)

    async def test_periodic_reads_are_in_combined_diagnostics(self):
        await self.read(100)
        result = await self.read(160)
        self.assertEqual(result.diagnostics["pi30_read_commands"], "QPIRI")
        self.assertIn("QPIRI=", result.diagnostics["pi30_read_command_durations_ms"])
        self.assertEqual(result.diagnostics["pi30_settings_refresh_command"], "QPIRI")
        self.assertEqual(result.diagnostics["pi30_settings_refresh_outcome"], "ok")
        self.assertEqual(result.diagnostics["pi30_poll_scope"], "telemetry")

    async def test_slow_settings_read_is_bounded_and_does_not_retry_immediately(self):
        await self.read(100)

        async def request(command):
            await asyncio.sleep(10)
            return self.qpiri

        self.session.request.side_effect = request
        with patch.object(pi30, "_SETTINGS_REFRESH_TIMEOUT", 0.01):
            result = await asyncio.wait_for(self.read(160), timeout=1)
        self.assertEqual(result.diagnostics["pi30_settings_refresh_outcome"], "timeout")
        self.assertEqual(result.values["battery_voltage"], 26.5)
        self.assertEqual(self.commands(), ["QPIRI"])
        self.session.request.side_effect = lambda command: "EaDx"
        await self.read(170)
        await self.read(180)
        self.assertEqual(self.commands(), ["QPIRI", "QFLAG"])


if __name__ == "__main__":
    unittest.main()
