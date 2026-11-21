"""#88: uncertain ACK and readback are independent; never resend a setting."""

import asyncio
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.drivers.write_confirmation import (
    WRITE_CONFIRMATION_DIAGNOSTIC_KEY, start_write_confirmation, write_confirmation_diagnostics,
)
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.modbus import ModbusError


PROFILE = "modbus_smg/models/anenji_4200_protocol_1.json"


class UnconfirmedWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_ack_keeps_outcome_unknown_even_when_poll_matches(self):
        for apply_write in (False, True):
            with self.subTest(apply_write=apply_write):
                target = ProbeTarget(1, 255, 1)
                profile = load_driver_profile(PROFILE)
                capability = profile.get_capability("battery_type")
                old_raw = 6  # Li4, as reported on the user's LCD
                old_label = capability.enum_value_map[old_raw]
                inverter = DetectedInverter(
                    driver_key="modbus_smg", protocol_family="modbus_smg",
                    model_name="Synthetic 4200", serial_number=None, probe_target=target,
                    profile_name=PROFILE, register_schema_name=PROFILE,
                    capabilities=profile.capabilities, details={"battery_type": old_label},
                )

                class Device(FixtureTransport):
                    writes = 0
                    reads = 0

                    def _handle_write_multiple(self, payload):
                        self.writes += 1
                        self.last_write = payload
                        if apply_write:
                            super()._handle_write_multiple(payload)
                        return b""

                    def _handle_read_holding(self, payload):
                        self.reads += 1
                        return super()._handle_read_holding(payload)

                registers = dict.fromkeys(range(800), 0)
                registers[322] = old_raw
                device = Device(registers=registers, command_responses=None, probe_target=target)
                state = {}
                start_write_confirmation(
                    state, capability_key="old_attempt", value_key="old_value",
                    requested_value=9, expected_value=9, requested_words=(9,),
                )
                driver = SmgModbusDriver()
                with self.assertRaisesRegex(ModbusError, "response_too_short"):
                    await driver.async_write_capability(device, inverter, "battery_type", "Flooded", runtime_state=state)
                self.assertEqual(device.writes, 1)
                self.assertEqual(device.last_write.hex(), "0110014200010200017872")
                self.assertEqual(device.reads, 0)  # no hidden recovery traffic
                trace = write_confirmation_diagnostics(state)[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]
                self.assertEqual(trace["capability_key"], "battery_type")
                self.assertEqual(trace["write_status"], "unconfirmed")
                self.assertIn("response_too_short", trace["write_error"])
                self.assertEqual(trace["immediate_status"], "unavailable")
                self.assertEqual(inverter.details["battery_type"], old_label)

                result = await driver.async_read_values(device, inverter, runtime_state=state)
                expected = "Flooded" if apply_write else old_label
                self.assertEqual(result.values["battery_type"], expected)
                trace = result.diagnostics[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]
                self.assertEqual(trace["first_full_poll_value"], expected)
                self.assertEqual(trace["write_status"], "unconfirmed")
                self.assertEqual(device.writes, 1)

    async def test_failure_and_cancellation_are_retained_without_swallowing(self):
        from unittest.mock import AsyncMock, patch

        profile = load_driver_profile(PROFILE)
        inverter = DetectedInverter(
            driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic",
            serial_number=None, probe_target=ProbeTarget(1, 255, 1), capabilities=profile.capabilities,
        )
        for error, expected in ((ModbusError("exception_code:3"), "rejected"),
                                (ModbusError("exception_code:11"), "unconfirmed"),
                                (TimeoutError("request_timeout"), "unconfirmed"),
                                (asyncio.CancelledError(), "unconfirmed")):
            with self.subTest(error=type(error).__name__):
                state = {}
                session = AsyncMock()
                session.write_holding.side_effect = error
                with patch.object(SmgModbusDriver, "_session", return_value=session):
                    with self.assertRaises(type(error)):
                        await SmgModbusDriver().async_write_capability(
                            object(), inverter, "battery_type", "Flooded", runtime_state=state,
                        )
                session.write_holding.assert_awaited_once_with(322, [1])
                session.write_single_holding.assert_not_awaited()
                session.read_holding.assert_not_awaited()
                trace = write_confirmation_diagnostics(state)[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]
                self.assertEqual(trace["write_status"], expected)


if __name__ == "__main__":
    unittest.main()
