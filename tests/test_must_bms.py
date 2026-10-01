"""PV3300 optional BMS semantics and runtime ownership, without device IO."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.drivers.must import MustPvPh18Driver
from custom_components.eybond_local.drivers.must_bms import STATE_KEY, async_read_bms, decode_bms
from custom_components.eybond_local.drivers.read_result import DriverReadMode
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.telemetry import TypedTelemetryFrame, fold_driver_telemetry
from test_must_driver import _must_registers


class MustBmsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.schema = load_register_schema("must_pv_ph18/pv3300.json")
        self.link = SimpleNamespace(connected=True)
        self.inverter = object()
        self.state = {}
        self.now = 10.0
        self.session = SimpleNamespace(read_holding=AsyncMock(return_value=[537, 97, 31, 0, 83]))

    async def read(self):
        return await async_read_bms(self.session, self.link, self.inverter, self.schema,
                                    self.state, lambda: self.now)

    def test_documented_scaling_soc_bounds_and_native_current_sign(self):
        self.assertEqual(decode_bms([537, 97, 31, 0, 83], self.schema), {
            "bms_battery_voltage": 53.7, "bms_battery_current": 9.7,
            "bms_battery_temperature": 31, "battery_soc": 83,
        })
        for soc in (0, 1, 100, 101, 32767, 32768, 65535):
            with self.subTest(soc=soc):
                values = decode_bms([537, 65526, 0, 0, soc], self.schema)
                self.assertEqual(values["bms_battery_current"], -1.0)
                self.assertEqual(values.get("battery_soc"), soc if soc <= 100 else None)
                self.assertNotIn("battery_current", values)
        for words in ([0] * 5, [0, 0, 0, 65535, 0], [65535] * 5):
            self.assertEqual(decode_bms(words, self.schema), {})
        for words in ([], [1] * 4, [1] * 6, [1, 2, 3, 0, -1], [1, 2, 3, 0, True]):
            with self.assertRaises(ModbusError):
                decode_bms(words, self.schema)
        self.assertEqual(decode_bms([65535, 65535, 65535, 0, 83], self.schema), {
            "bms_battery_current": -0.1, "battery_soc": 83,
        })

    async def test_success_then_error_invalidates_and_recovers_after_backoff(self):
        values, diagnostics = await self.read()
        self.assertEqual(values["battery_soc"], 83)
        self.assertEqual(diagnostics["must_bms_status"], "ok")
        self.session.read_holding.side_effect = TimeoutError()
        self.now = 20
        values, diagnostics = await self.read()
        self.assertEqual(values, {})
        self.assertEqual(diagnostics["must_bms_status"], "timeout")
        self.assertEqual(diagnostics["must_bms_retry_after_seconds"], 60)
        self.now = 79
        self.assertEqual((await self.read())[0], {})
        self.assertEqual(self.session.read_holding.await_count, 2)
        self.now = 80
        self.session.read_holding.side_effect = None
        self.assertEqual((await self.read())[0]["battery_soc"], 83)

    async def test_unsupported_and_no_data_are_temporary_not_global(self):
        for error, delay, status in ((ModbusError("exception_code:2"), 300, "unsupported"),
                                     (ModbusError("exception_code:1"), 300, "unsupported"),
                                     (ModbusError("crc_mismatch"), 60, "invalid_response"),
                                     (None, 60, "no_data")):
            self.state.clear()
            self.session.read_holding.side_effect = error
            self.session.read_holding.return_value = [0] * 5
            values, diagnostics = await self.read()
            self.assertEqual(values, {})
            self.assertEqual(diagnostics["must_bms_status"], status)
            self.assertEqual(diagnostics["must_bms_retry_after_seconds"], delay)
            self.session.read_holding.side_effect = None
            self.session.read_holding.return_value = [537, 0, 31, 0, 0]
            for reset in ("transport", "inverter", "clock", "runtime"):
                if reset == "transport": self.link = SimpleNamespace(connected=True)
                elif reset == "inverter": self.inverter = object()
                elif reset == "clock": self.now -= 1
                else: self.state.clear()
                self.assertEqual((await self.read())[0]["battery_soc"], 0)

    async def test_deadline_cancellation_and_positive_disconnect(self):
        started = asyncio.Event()
        async def hang(*args):
            started.set()
            await asyncio.Event().wait()
        self.session.read_holding.side_effect = hang
        with patch("custom_components.eybond_local.drivers.must_bms.READ_TIMEOUT", 0.01):
            values, diagnostics = await asyncio.wait_for(self.read(), 1)
        self.assertEqual(values, {})
        self.assertEqual(diagnostics["must_bms_status"], "timeout")
        self.state.clear()
        started.clear()
        task = asyncio.create_task(self.read())
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.state[STATE_KEY].retry_at, 0)
        for error in (ConnectionError("offline"), TimeoutError()):
            self.session.read_holding.side_effect = error
            self.link.connected = False
            with self.assertRaises(ConnectionError):
                await self.read()

    async def test_driver_full_snapshots_never_carry_failed_bms_or_change_core(self):
        driver = MustPvPh18Driver()
        target = ProbeTarget(1, 255, 4)
        registers = _must_registers() | {20001: 3300} | dict(zip(range(109, 114), [537,97,31,0,83]))
        link = FixtureTransport(registers=registers, command_responses=None, probe_target=target)
        inverter = await driver.async_probe(link, target)
        result = await driver.async_read_values(link, inverter, runtime_state=self.state, now_monotonic=10)
        self.assertEqual(result.mode, DriverReadMode.FULL)
        self.assertEqual(result.values["battery_soc"], 83)
        self.assertEqual(result.values["battery_voltage"], 25.6)
        frame = fold_driver_telemetry(TypedTelemetryFrame.empty(), driver_key=driver.key,
                                     values=result.values, replace=True)
        del link._registers[109]
        for now in (20, 30):
            result = await driver.async_read_values(link, inverter, runtime_state=self.state, now_monotonic=now)
            frame = fold_driver_telemetry(frame, driver_key=driver.key, values=result.values, replace=True)
            self.assertNotIn("battery_soc", frame.values())
            self.assertEqual(frame.values()["battery_voltage"], 25.6)
        # Rebinding to another MUST model cannot inherit even negative BMS state.
        other = replace(inverter, register_schema_name="must_pv_ph18/base.json")
        with patch.object(driver, "_session", return_value=SimpleNamespace(
            read_registers=AsyncMock(return_value=[0] * 74), read_holding=AsyncMock()
        )) as factory:
            await driver.async_read_values(link, other, runtime_state=self.state)
            factory.return_value.read_holding.assert_not_awaited()
