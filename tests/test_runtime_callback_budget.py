"""The UDP reply window must not consume callback arrival/identity time."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.eybond_local.collector.silent_session_probe import SessionObservation
from custom_components.eybond_local.connection.callback_ledger import get_callback_trigger_ledger
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager


PN = "E50000200000000001"
FOREIGN = "E50000200000009777"


def observation(*, wire="eybond_framed", pn=PN, session_id="synthetic"):
    return SessionObservation(
        session_id=session_id, collector_pn=pn,
        identity_source="at_dtupn" if wire == "at_text" else "fc2_parameter_2",
        protocol_shape=wire, state="waiting_for_route_identity",
    )


class RuntimeCallbackBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        with patch(
            "custom_components.eybond_local.runtime.link.resolve_server_ip",
            return_value="192.0.2.10",
        ):
            self.manager = EybondRuntimeLinkManager(
                server_ip="192.0.2.10", collector_ip="192.0.2.14",
                tcp_port=8899, udp_port=58899, discovery_target="192.0.2.255",
                discovery_interval=30, heartbeat_interval=60,
            )
        self.manager._collector_pn = PN
        self.observations = []
        self.channel = SimpleNamespace(
            available=True, async_open=AsyncMock(), async_close=AsyncMock(),
            snapshot_session_observations=lambda: tuple(self.observations),
            async_identify_exact_session=AsyncMock(),
            async_retire_exact_session=AsyncMock(return_value=True),
        )
        self.activation = AsyncMock(return_value=True)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch(
            "custom_components.eybond_local.collector.silent_session_probe."
            "SilentSessionIdentityProbeChannel", return_value=self.channel,
        ))
        self.stack.enter_context(patch.object(
            self.manager, "_async_follow_owned_session_listener", new=AsyncMock(),
        ))
        self.stack.enter_context(patch.object(
            self.manager, "_apply_live_wire_to_transports", new=Mock(),
        ))
        self.stack.enter_context(patch.object(
            self.manager, "_async_await_callback_session", new=self.activation,
        ))

    def patch_udp(self, callback):
        return self.stack.enter_context(patch(
            "custom_components.eybond_local.runtime.link.callback."
            "async_send_callback_trigger", side_effect=callback,
        ))

    async def connect(self, timeout=0.02):
        return await self.manager._async_callback_connect_within_causality(
            timeout=timeout, require_heartbeat=False,
        )

    def attempt(self):
        return self.manager.callback_trigger_diagnostics()["collector_callback_last_attempt"]

    async def test_slow_udp_preserves_arrived_framed_and_at_sessions(self):
        for wire in ("eybond_framed", "at_text"):
            with self.subTest(wire=wire):
                self.observations.clear()

                async def udp(**kwargs):
                    self.observations.append(observation(wire=wire))
                    await asyncio.sleep(0.04)  # Longer than the post-trigger budget.
                    return SimpleNamespace(reply="rsp>server=1;", reply_from="192.0.2.14:58899")

                probe = self.patch_udp(udp)
                self.assertTrue(await self.connect())
                self.assertEqual(self.attempt()["identity_result"], "identified")
                self.assertEqual(self.attempt()["baseline_sessions"], 0)
                self.assertGreaterEqual(self.attempt()["trigger_elapsed_ms"], 30)
                self.assertEqual(self.attempt()["wait_budget_ms"], 20)
                self.assertEqual(self.attempt()["trigger_result"], "reply_received")
                probe.assert_awaited_once()

    async def test_no_udp_reply_still_accepts_valid_later_tcp(self):
        async def udp(**kwargs):
            await asyncio.sleep(0.04)
            asyncio.get_running_loop().call_later(0.01, self.observations.append, observation())
            return SimpleNamespace(reply="", reply_from="")

        self.patch_udp(udp)
        self.assertTrue(await self.connect(timeout=0.1))
        self.assertEqual(self.attempt()["trigger_result"], "no_reply")
        self.assertEqual(self.attempt()["outcome"], "connected")

    async def test_no_socket_is_bounded_and_does_not_retrigger(self):
        async def udp(**kwargs):
            await asyncio.sleep(0.04)
            return SimpleNamespace(reply="rsp>server=1;", reply_from="192.0.2.14:58899")

        probe = self.patch_udp(udp)
        self.assertFalse(await asyncio.wait_for(self.connect(), timeout=1))
        self.assertEqual(self.attempt()["identity_result"], "no_session")
        self.assertGreaterEqual(self.attempt()["identity_elapsed_ms"], 15)
        self.activation.assert_not_awaited()
        probe.assert_awaited_once()
        before = self.attempt()
        self.manager.clear_discovery_reply()
        self.assertEqual(self.attempt(), before)
        before["outcome"] = "tampered"
        self.assertEqual(self.attempt()["outcome"], "no_session")
        self.assertNotIn(PN, str(self.attempt()))
        self.assertNotIn("192.0.2.", str(self.attempt()))

    async def test_foreign_and_ambiguous_sessions_are_not_activated(self):
        for sessions, expected in (
            ([observation(pn=FOREIGN)], "foreign_identity"),
            ([observation(), observation(session_id="second")], "ambiguous"),
        ):
            with self.subTest(expected=expected):
                self.observations.clear()

                async def udp(**kwargs):
                    self.observations.extend(sessions)
                    await asyncio.sleep(0.04)
                    return SimpleNamespace(reply="", reply_from="")

                self.patch_udp(udp)
                self.assertFalse(await self.connect())
                self.assertEqual(self.attempt()["identity_result"], expected)
        self.activation.assert_not_awaited()

    async def test_pre_send_baseline_is_preserved_across_slow_udp(self):
        self.observations.append(observation(pn=FOREIGN, session_id="existing-foreign"))

        async def udp(**kwargs):
            self.observations.append(observation())
            await asyncio.sleep(0.04)
            return SimpleNamespace(reply="", reply_from="")

        self.patch_udp(udp)
        self.assertTrue(await self.connect())
        self.assertEqual(self.attempt()["baseline_sessions"], 1)

    async def test_cancellation_during_identity_releases_channel_and_lease(self):
        async def udp(**kwargs):
            return SimpleNamespace(reply="", reply_from="")

        entered = asyncio.Event()

        async def negotiate(**kwargs):
            entered.set()
            await asyncio.Event().wait()

        self.patch_udp(udp)
        self.stack.enter_context(patch.object(
            self.manager._session_identity_negotiator, "async_negotiate", side_effect=negotiate,
        ))
        task = asyncio.create_task(self.connect())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.attempt()["outcome"], "cancelled")
        self.assertEqual(self.attempt()["stage"], "identity")
        self.channel.async_close.assert_awaited_once()
        self.activation.assert_not_awaited()
        self.assertEqual(self.manager._active_identity_challenge_protocol, "")
        async with get_callback_trigger_ledger().causality_lease("subsequent-test", timeout=0):
            pass


if __name__ == "__main__":
    unittest.main()
