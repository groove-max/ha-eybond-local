"""Malformed write acknowledgements are uncertain, not persistent write locks."""

from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import (
    ModbusError, ModbusSession, ModbusWriteResponseError, crc16_modbus,
)
from custom_components.eybond_local.drivers.modbus_write_error import classify_modbus_write_error
from custom_components.eybond_local.runtime.hub.management import HubManagementMixin


class WriteFailureRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_ack_context_for_both_write_functions(self):
        cap = load_driver_profile("modbus_smg/models/anenji_4200_protocol_1.json").get_capability("equalization_activate_now")
        for function in (6, 16):
            for response in (b"", b"\x01", b"\x01\x06\x01\xa9", bytes(8)):
                with self.subTest(function=function, response=response):
                    transport = SimpleNamespace(async_send_payload=AsyncMock(return_value=response))
                    session = ModbusSession(transport, devcode=1, collector_addr=255, slave_id=1)
                    with self.assertRaises(ModbusWriteResponseError) as caught:
                        if function == 6:
                            await session.write_single_holding(425, 1)
                        else:
                            await session.write_holding(425, [1])
                    error = caught.exception
                    self.assertEqual((error.function, error.address, error.response_length), (function, 425, len(response)))
                    transport.async_send_payload.assert_awaited_once()
                    verdict = classify_modbus_write_error(cap, error, operating_mode="Standby")
                    self.assertIsNone(verdict.blocker)
                    self.assertIn("write_response_unconfirmed", str(verdict.user_error))
                    self.assertIn("register 425", str(verdict.user_error))
                    self.assertIn("was not resent", str(verdict.user_error))

    async def test_valid_exception_keeps_existing_classification_and_is_not_retried(self):
        cap = load_driver_profile("modbus_smg/models/anenji_4200_protocol_1.json").get_capability("equalization_activate_now")
        for function in (6, 16):
            for code in (1, 2, 3, 7):
                reply = bytes((1, function | 0x80, code))
                reply += crc16_modbus(reply).to_bytes(2, "little")
                transport = SimpleNamespace(async_send_payload=AsyncMock(return_value=reply))
                session = ModbusSession(transport, devcode=1, collector_addr=255, slave_id=1)
                with self.assertRaises(ModbusError) as caught:
                    if function == 6:
                        await session.write_single_holding(425, 1)
                    else:
                        await session.write_holding(425, [1])
                self.assertNotIsInstance(caught.exception, ModbusWriteResponseError)
                self.assertEqual(str(caught.exception), f"exception_code:{code}")
                verdict = classify_modbus_write_error(cap, caught.exception, operating_mode="Standby")
                if code == 3:
                    self.assertIn("illegal_data_value", str(verdict.user_error))
                    self.assertIsNone(verdict.blocker)
                else:
                    self.assertIsNotNone(verdict.blocker)
                transport.async_send_payload.assert_awaited_once()

    async def test_failed_eq_does_not_lock_the_next_battery_type_write(self):
        profile_name = "modbus_smg/models/anenji_4200_protocol_1.json"
        profile = load_driver_profile(profile_name)
        target = ProbeTarget(1, 255, 1)
        inverter = DetectedInverter(
            driver_key="modbus_smg", protocol_family="modbus_smg", model_name="Synthetic Anenji",
            serial_number="synthetic", probe_target=target, profile_name=profile_name,
            register_schema_name=profile_name, capabilities=profile.capabilities,
        )

        class ShortEqTransport(FixtureTransport):
            def __init__(self):
                super().__init__(registers={322: 1}, command_responses=None, probe_target=target)
                self.writes = []

            async def async_send_payload(self, payload, *, route):
                if payload[1] in (6, 16):
                    self.writes.append(payload)
                    if int.from_bytes(payload[2:4], "big") == 425:
                        return b""
                return await super().async_send_payload(payload, route=route)

        transport = ShortEqTransport()
        hub = HubManagementMixin()
        hub._inverter, hub._driver = inverter, SmgModbusDriver()
        hub._async_ensure_connected = AsyncMock()
        hub._runtime_read_state, hub._write_blockers = {}, {}
        hub._link_manager = SimpleNamespace(transport=transport)
        hub.async_refresh = AsyncMock(return_value=RuntimeSnapshot(
            connected=True, inverter=inverter, values={
                "operating_mode": "Standby", "battery_type": "Flooded",
                "battery_equalization_enabled": True,
            },
        ))
        with self.assertRaises((ModbusError, ValueError)):
            await hub.async_write_capability("equalization_activate_now", None)
        self.assertEqual(hub._write_blockers, {})
        self.assertEqual(len(transport.writes), 1, "Never resend an uncertain EQ action")
        self.assertEqual(await hub.async_write_capability("battery_type", "Flooded"), "Flooded")
        self.assertEqual(len(transport.writes), 2)
        self.assertEqual(hub._write_blockers, {})
