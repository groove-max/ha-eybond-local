"""Settings cadence, fresh write confirmation and routing timing regressions."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from custom_components.eybond_local.drivers import eybond_g_ascii as driver_module
from custom_components.eybond_local.drivers.eybond_g_ascii import EybondGAsciiDriver
from custom_components.eybond_local.drivers.read_cadence import PeriodicReadCadence
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.ascii_line import AsciiLineSession
from test_g_ascii_poll_recovery import CORE_REPLIES, ScriptedSession, timeout


class GAsciiPeriodicSettingsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")
        self.cap = self.profile.get_capability("max_charging_current_setting")
        self.driver = EybondGAsciiDriver()
        self.inverter = DetectedInverter(
            driver_key=self.driver.key, protocol_family="eybond_g_ascii",
            model_name="test", serial_number="TEST-ONLY",
            probe_target=ProbeTarget(0x0994, 255, 0), capabilities=(self.cap,),
        )
        self.replies = {**CORE_REPLIES, "CHGC???": "030"}
        self.session = ScriptedSession(lambda command, count: self.replies.get(command, ""))
        self.state = {}

    async def read(self, now):
        with patch.object(self.driver, "_session", return_value=self.session):
            return await self.driver.async_read_values(
                object(), self.inverter, runtime_state=self.state, now_monotonic=now, poll_interval=10,
            )

    async def write(self, value):
        command = f"{self.cap.command}{value:03d}"
        self.replies[command] = "ACK"
        with patch.object(self.driver, "_session", return_value=self.session):
            return await self.driver.async_write_capability(
                object(), self.inverter, self.cap.key, value, runtime_state=self.state,
            )

    async def test_external_change_is_periodically_read_without_local_write(self):
        self.assertEqual((await self.read(100)).values[self.cap.value_key], 30)
        self.replies["CHGC???"] = "040"
        self.session.commands.clear()
        second = await self.read(110)
        self.assertNotIn(self.cap.value_key, second.values)
        self.assertNotIn(self.cap.value_key, second.removed_keys)
        self.assertNotIn("CHGC???", self.session.commands)
        self.assertIn("GBAT", self.session.commands)  # mixed live/config always fresh
        self.assertEqual(second.diagnostics["eybond_g_ascii_settings_cadence"]["oldest_success_age_s"], 10)
        self.assertEqual((await self.read(160)).values[self.cap.value_key], 40)

    async def test_ack_never_changes_identity_and_pending_reads_bypass_cadence(self):
        await self.read(100)
        self.inverter.details[self.cap.value_key] = 30
        await self.write(40)
        self.assertEqual(self.inverter.details[self.cap.value_key], 30)
        first = await self.read(101)
        self.assertEqual(first.values[self.cap.value_key], 30)  # ACK did not confirm 40
        self.replies["CHGC???"] = "040"
        self.assertEqual((await self.read(102)).values[self.cap.value_key], 40)
        self.assertNotIn(self.cap.value_key, (await self.read(103)).values)

    async def test_missing_readback_invalidates_old_value_on_both_confirmation_passes(self):
        await self.read(100)
        await self.write(40)
        self.replies["CHGC???"] = timeout()
        for now in (101, 102):
            read = await self.read(now)
            self.assertIn(self.cap.value_key, read.removed_keys)
            self.assertNotIn(self.cap.value_key, read.values)

    async def test_rebinding_does_not_reuse_cadence_or_pending_confirmation(self):
        await self.read(100)
        await self.write(40)
        self.inverter = replace(self.inverter, serial_number="OTHER-TEST")
        self.replies["CHGC???"] = "020"
        result = await self.read(101)
        self.assertEqual(result.values[self.cap.value_key], 20)
        self.assertNotIn(driver_module._PENDING_READBACK_KEY, self.state)

    async def test_invalidated_binding_cannot_commit_old_cadence(self):
        await self.read(100)
        def reply(command, count):
            if command == "CHGC???":
                self.state.clear()
            return self.replies.get(command, "")
        self.session.reply = reply
        with self.assertRaisesRegex(ConnectionError, "binding_changed"):
            await self.read(160)
        self.assertEqual(self.state, {})

    async def test_binding_reset_during_core_cannot_publish_old_session(self):
        await self.read(100)
        def reply(command, count):
            if command == "GMOD":
                self.state.clear()
            return self.replies.get(command, "")
        self.session.reply = reply
        with self.assertRaisesRegex(ConnectionError, "binding_changed"):
            await self.read(110)
        self.assertEqual(self.state, {})

    async def test_missing_selector_cannot_disable_bms_forever(self):
        await self.read(100)
        self.state["eybond_g_ascii_bms_enabled"] = {"enabled": False, "observed": 100}
        self.session.commands.clear()
        await self.read(110)
        self.assertNotIn("GBMS", self.session.commands)
        await self.read(161)
        self.assertIn("GBMS", self.session.commands)

    async def test_cancelled_optional_sweep_does_not_advance_deadlines(self):
        await self.read(100)
        before = deepcopy(self.state)
        self.replies["CHGC???"] = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.read(160)
        self.assertEqual(self.state, before)

    async def test_shared_read_command_is_sent_once(self):
        alias = replace(self.cap, key="alias", read_key="alias_read_key")
        self.inverter.capabilities = (self.cap, alias)
        with patch.dict(driver_module._G_ASCII_SETTINGS_BY_READ_KEY, {
            alias.read_key: driver_module._G_ASCII_SETTINGS_BY_READ_KEY[self.cap.read_key],
        }):
            result = await self.read(100)
        self.assertEqual(self.session.commands.count("CHGC???"), 1)
        self.assertEqual(result.values[alias.value_key], 30)

    async def test_deferred_soc_not_permanently_skipped_when_bms_becomes_available(self):
        self.inverter.capabilities = self.profile.capabilities
        await self.read(100)
        self.replies.update({"GBMS": "1 0 520 100 250 75 100 200 0 0 5000 560", "BSOCU???": "25"})
        seen = False
        for now in range(160, 400, 10):
            self.session.commands.clear()
            await self.read(now)
            seen |= "BSOCU???" in self.session.commands
        self.assertTrue(seen)


class ReadCadenceTests(unittest.TestCase):
    def test_initial_snapshot_then_bounded_fair_rotation_and_new_groups(self):
        state = {}
        groups = tuple(f"group_{i}" for i in range(22))
        cadence = PeriodicReadCadence(state, now=100)
        self.assertEqual(cadence.select(groups, limit=4), set(groups))
        for key in groups:
            cadence.attempted(key, success=True)
        cadence.finish(groups)
        self.assertFalse(PeriodicReadCadence(state, now=159).select(groups, limit=4))
        seen = set()
        for now in range(160, 220, 10):
            cadence = PeriodicReadCadence(state, now=now)
            selected = cadence.select(groups, limit=4)
            self.assertLessEqual(len(selected), 4)
            seen.update(selected)
            for key in selected:
                cadence.attempted(key, success=True)
            cadence.finish(groups)
        self.assertEqual(seen, set(groups))
        groups += ("new_learned_group",)
        self.assertIn("new_learned_group", PeriodicReadCadence(state, now=211).select(groups, limit=4))

    def test_failure_paces_attempt_without_claiming_freshness_and_forced_group_bypasses(self):
        state = {}
        cadence = PeriodicReadCadence(state, now=100)
        cadence.attempted("x", success=True)
        cadence.finish(("x",))
        cadence = PeriodicReadCadence(state, now=160)
        cadence.attempted("x", success=False)
        self.assertEqual(cadence.finish(("x",))["oldest_success_age_s"], 60)
        cadence = PeriodicReadCadence(state, now=170)
        self.assertFalse(cadence.select(("x",), limit=4))
        self.assertEqual(cadence.select(("x",), limit=4, forced=("x",)), {"x"})


class AsciiTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_raw_counters_are_not_used_for_framed_requests(self):
        class Transport:
            collector_info = SimpleNamespace(raw_last_spacing_wait_ms=123, raw_last_response_duration_ms=456)
            async def async_send_payload(self, payload, *, route):
                return b"(B\r"
        for route, expected in ((EybondLinkRoute(devcode=0x0994, collector_addr=255), {}),
                                (RawSerialLinkRoute(), {"spacing_wait_ms": 123, "response_duration_ms": 456})):
            session = AsciiLineSession(Transport(), route=route)
            await session.request("GMOD")
            self.assertEqual(session.last_transport_timing(), expected)
