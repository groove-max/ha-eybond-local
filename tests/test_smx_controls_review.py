"""PR #96 review regressions against current-main write-evidence contract.

All transports are in-memory synthetic fixtures. No hardware writes occur.
SRNE Modbus V2.07 sections 3.4 and P02: 0x05 wrong password,
0x0B insufficient permission; 0x0211=0 no user password entered,
0x0211=1 user password entered. 0xDF00 is a write-only command.
"""

from __future__ import annotations

from types import SimpleNamespace
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.drivers.srne import SrneModbusDriver
from custom_components.eybond_local.drivers.write_confirmation import (
    WRITE_CONFIRMATION_DIAGNOSTIC_KEY, write_confirmation_diagnostics,
)
from custom_components.eybond_local.models import RuntimeSnapshot
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.drivers.registry import measurements_for_runtime

from test_smx_controls import SCHEMA, TARGET, _WriteTraceTransport


def _trace(state):
    return write_confirmation_diagnostics(state)[WRITE_CONFIRMATION_DIAGNOSTIC_KEY]


class SmxWriteEvidenceReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = SrneModbusDriver()
        self.device = _WriteTraceTransport()
        self.inverter = await self.driver.async_probe(self.device, TARGET)
        self.assertIsNotNone(self.inverter)

    async def test_acknowledged_setting_and_observed_raw_value_are_separate(self):
        state = {}
        result = await self.driver.async_write_capability(
            self.device, self.inverter, "max_ac_charge_current", 60,
            runtime_state=state,
        )
        self.assertEqual(result, 60.0)
        self.assertEqual(_trace(state)["write_status"], "acknowledged")
        self.assertEqual(_trace(state)["immediate_status"], "matched")
        self.assertEqual(_trace(state)["immediate_words"], [600])
        self.assertEqual(self.device.requests, [(6, 0xE205, 600)])

    async def test_empty_and_malformed_ack_do_not_resend_or_readback(self):
        for bad_reply in (b"", b"\x01\x06\x00\x00\x00\x01\x00\x00"):
            with self.subTest(reply=bad_reply.hex()):
                class BadAckDevice(_WriteTraceTransport):
                    def _handle_write_single(self, payload):
                        super()._handle_write_single(payload)  # simulate device application
                        return bad_reply

                device = BadAckDevice()
                inverter = await self.driver.async_probe(device, TARGET)
                state = {}
                with self.assertRaises(ModbusError):
                    await self.driver.async_write_capability(
                        device, inverter, "max_ac_charge_current", 60,
                        runtime_state=state,
                    )
                self.assertEqual(device.requests, [(6, 0xE205, 600)])
                self.assertEqual(_trace(state)["write_status"], "unconfirmed")
                self.assertEqual(_trace(state)["immediate_status"], "unavailable")
                # Underlying fixture applies the value, but the missing ACK
                # must remain unresolved even if later polls happen to match.
                await self.driver.async_read_values(
                    device, inverter, runtime_state=state, poll_interval=5.0,
                )
                self.assertEqual(_trace(state)["write_status"], "unconfirmed")
                self.assertEqual(device.requests, [(6, 0xE205, 600)])

    async def test_hub_never_resends_after_collector_write_connection_loss(self):
        """The generic hub retry rule must not replay an ambiguous SMX write."""
        hub = EybondHub(connection=EybondConnectionSpec(
            server_ip="192.0.2.10", collector_ip="192.0.2.11",
            tcp_port=8899, udp_port=58899,
            discovery_target="192.0.2.255", discovery_interval=30,
            heartbeat_interval=60, request_timeout=5.0,
        ))
        hub._driver = self.driver
        hub._inverter = self.inverter
        hub._link_manager = SimpleNamespace(transport=self.device)
        session = AsyncMock()
        session.write_single_holding.side_effect = ConnectionError("collector_write_timeout")

        async def first_refresh():
            return RuntimeSnapshot(values={"max_ac_charge_current": 20})

        with patch.object(hub, "_async_ensure_connected", new_callable=AsyncMock) as ensure, \
             patch.object(hub, "async_refresh", side_effect=first_refresh), \
             patch.object(self.driver, "_session", return_value=session):
            with self.assertRaisesRegex(RuntimeError, "smx_ii_setting_write_outcome_unknown"):
                await hub.async_write_capability("max_ac_charge_current", 60)
        self.assertEqual(session.write_single_holding.await_count, 1)
        self.assertEqual(ensure.await_count, 1)  # no retry reconnect
        self.assertEqual(_trace(hub._runtime_read_state)["write_status"], "unconfirmed")

    async def test_explicit_setting_rejection_retained_without_login(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = ModbusError("exception_code:3")
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaisesRegex(ModbusError, "exception_code:3"):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 1)
        session.read_holding.assert_not_awaited()
        self.assertEqual(_trace(state)["write_status"], "rejected")

    async def test_cancel_during_setting_write_keeps_uncertain_evidence(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = asyncio.CancelledError()
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaises(asyncio.CancelledError):
                await self.driver.async_write_capability(
                    object(), self.inverter, "max_ac_charge_current", 60,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 1)
        session.read_holding.assert_not_awaited()
        self.assertEqual(_trace(state)["write_status"], "unconfirmed")
        self.assertEqual(_trace(state)["immediate_status"], "unavailable")

    async def test_cancel_during_immediate_readback_keeps_ack_evidence(self):
        state = {}
        session = AsyncMock()
        session.read_holding.side_effect = asyncio.CancelledError()
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaises(asyncio.CancelledError):
                await self.driver.async_write_capability(
                    object(), self.inverter, "max_ac_charge_current", 60,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 1)
        self.assertEqual(_trace(state)["write_status"], "acknowledged")
        self.assertEqual(_trace(state)["immediate_status"], "unavailable")

    async def test_explicit_permission_denial_followed_by_wrong_login(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = ModbusError("exception_code:11")
        session.read_holding.return_value = [0]
        session.write_holding.side_effect = ModbusError("exception_code:5")
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaisesRegex(ModbusError, "exception_code:5"):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 1)
        session.write_holding.assert_awaited_once_with(0xE203, [0])
        self.assertEqual(_trace(state)["write_status"], "rejected")
        self.assertEqual(_trace(state)["write_error"], "ModbusError:exception_code")

    async def test_explicit_denial_and_authorized_status_skip_login(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = ModbusError("exception_code:11")
        session.read_holding.return_value = [1]
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaisesRegex(ModbusError, "exception_code:11"):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        session.write_holding.assert_not_awaited()
        self.assertEqual(_trace(state)["write_status"], "rejected")

    async def test_login_success_but_second_setting_rejected(self):
        state = {}
        session = AsyncMock()
        session.read_holding.return_value = [0]
        session.write_single_holding.side_effect = (
            ModbusError("exception_code:11"), ModbusError("exception_code:3")
        )
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaisesRegex(ModbusError, "exception_code:3"):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 2)
        session.write_holding.assert_awaited_once_with(0xE203, [0])
        self.assertEqual(_trace(state)["write_status"], "rejected")
        self.assertEqual(_trace(state)["write_error"], "ModbusError:exception_code")

    async def test_cancel_during_password_login_preserves_setting_rejection(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = ModbusError("exception_code:11")
        session.read_holding.return_value = [0]
        session.write_holding.side_effect = asyncio.CancelledError()
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaises(asyncio.CancelledError):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 1)
        session.write_holding.assert_awaited_once_with(0xE203, [0])
        # Login is not an acknowledgement of the setting write.
        self.assertEqual(_trace(state)["write_status"], "rejected")

    async def test_cancel_during_retried_setting_retains_uncertain_outcome(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = (
            ModbusError("exception_code:11"), asyncio.CancelledError()
        )
        session.read_holding.return_value = [0]
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaises(asyncio.CancelledError):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        self.assertEqual(session.write_single_holding.await_count, 2)
        session.write_holding.assert_awaited_once_with(0xE203, [0])
        self.assertEqual(_trace(state)["write_status"], "unconfirmed")
        self.assertEqual(_trace(state)["immediate_status"], "unavailable")

    async def test_deferred_power_control_absent_in_all_modes_and_hub(self):
        from custom_components.eybond_local.control_policy import can_expose_capability
        keys = {item.key for item in self.inverter.capabilities}
        self.assertEqual(len(keys), 35)
        self.assertNotIn("inverter_power", keys)
        for mode in ("auto", "full", "read_only"):
            self.assertNotIn("inverter_power", {
                item.key for item in self.inverter.capabilities
                if can_expose_capability(item, control_mode=mode, detection_confidence="high")
            })

        class FakeLink:
            transport = self.device

        hub = EybondHub(connection=EybondConnectionSpec(
            server_ip="192.0.2.10", collector_ip="192.0.2.11",
            tcp_port=8899, udp_port=58899,
            discovery_target="192.0.2.255", discovery_interval=30,
            heartbeat_interval=60, request_timeout=5.0,
        ))
        hub._link_manager = FakeLink()
        hub._driver = self.driver
        hub._inverter = self.inverter
        with patch.object(hub, "_async_ensure_connected", new_callable=AsyncMock):
            with self.assertRaises((ValueError, KeyError)):
                await hub.async_write_capability("inverter_power", True)
        self.assertEqual(self.device.requests, [])

    async def test_uncertain_ack_never_runs_permission_login(self):
        state = {}
        session = AsyncMock()
        session.write_single_holding.side_effect = ModbusError("request_timeout")
        with patch.object(self.driver, "_session", return_value=session):
            with self.assertRaises(ModbusError):
                await self.driver.async_write_capability(
                    object(), self.inverter, "output_priority", 1,
                    runtime_state=state,
                )
        session.write_holding.assert_not_awaited()
        self.assertEqual(session.write_single_holding.await_count, 1)
        self.assertEqual(_trace(state)["write_status"], "unconfirmed")

    async def test_hub_confirms_normal_poll_runtime_value_not_optimistic_value(self):
        """Exercise real driver, hub confirmation and observable runtime key."""
        class FakeLink:
            def __init__(self, device):
                self.transport = device
                self.connected = True

        hub = EybondHub(connection=EybondConnectionSpec(
            server_ip="192.0.2.10", collector_ip="192.0.2.11",
            tcp_port=8899, udp_port=58899,
            discovery_target="192.0.2.255", discovery_interval=30,
            heartbeat_interval=60, request_timeout=5.0,
        ))
        hub._link_manager = FakeLink(self.device)
        hub._driver = self.driver
        hub._inverter = self.inverter

        published = []
        async def refresh():
            result = await self.driver.async_read_values(
                self.device, self.inverter,
                runtime_state=hub._runtime_read_state, poll_interval=5.0,
            )
            snapshot = RuntimeSnapshot(values=result.values, last_error=None)
            published.append(snapshot)
            return snapshot

        with patch.object(hub, "_async_ensure_connected", new_callable=AsyncMock), \
             patch.object(hub, "async_refresh", side_effect=refresh):
            outcome = await hub.async_write_capability("max_ac_charge_current", 60)

        self.assertEqual(outcome, 60.0)
        self.assertEqual(self.device.requests, [(6, 0xE205, 600)])
        self.assertGreaterEqual(len(published), 2)
        self.assertEqual(published[-1].runtime_value("max_ac_charge_current"), 60.0)
        self.assertTrue(any(
            item.key == "max_ac_charge_current" for item in measurements_for_runtime(
                driver_key="srne_modbus", register_schema_name=SCHEMA,
            )
        ))
        self.assertEqual(_trace(hub._runtime_read_state)["write_status"], "acknowledged")
        self.assertEqual(_trace(hub._runtime_read_state)["latest_full_poll_status"], "matched")


if __name__ == "__main__":
    unittest.main()
