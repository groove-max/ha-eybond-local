"""Own-lease retriggering after silent baseline closure, not periodic polling."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import test_callback_identity as unit
from custom_components.eybond_local.connection import callback_identity as identity
from custom_components.eybond_local.collector.silent_session_probe import SilentSessionIdentityProbeChannel


class CallbackIdentityRetriggerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.live = unit._Live()
        self.registry = unit.CallbackSessionRegistry(sessions_source=self.live)
        self.hass = unit._FakeHass(self.registry)
        self.silent = {"old"}
        self.closed = set()
        self.listener = SimpleNamespace(
            silent_pending_collector_sessions=lambda: [{"session_id": sid} for sid in self.silent],
            closed_collector_session_ids=lambda: frozenset(self.closed),
        )
        self.enterContext(patch(
            "custom_components.eybond_local.collector.transport._acquire_shared_listener",
            new=AsyncMock(return_value=self.listener),
        ))
        self.release = self.enterContext(patch(
            "custom_components.eybond_local.collector.transport._release_shared_listener",
            new=AsyncMock(),
        ))
        self.enterContext(unit._no_probe_scope())
        self.enterContext(patch.object(identity, "DEFAULT_ONBOARDING_TIMEOUT_POLICY", replace(
            identity.DEFAULT_ONBOARDING_TIMEOUT_POLICY,
            callback_identity_retrigger_spacing=0, callback_identity_silent_settle=0,
        )))
        self.enterContext(patch.object(identity, "_SESSION_POLL_INTERVAL", 0.005))
        self.reader = unit._Reader()

    def close_old(self):
        self.silent.discard("old")
        self.closed.add("old")

    async def run_attempt(self, sender, **kwargs):
        result = await identity.async_run_callback_identity_transaction(
            self.hass, unit._request(session_wait_timeout=0.04, **kwargs),
            sender=sender, reader=self.reader,
        )
        self.release.assert_awaited_once_with(self.listener)
        self.assertEqual(unit.get_callback_trigger_ledger().causality_owner(), "")
        return result

    async def test_repeat_uses_same_owner_target_route_and_shared_matcher(self):
        requests, owners = [], []

        class Sender:
            async def async_send(inner, request):
                requests.append(request)
                ledger = unit.get_callback_trigger_ledger()
                owners.append(ledger.causality_owner())
                self.assertEqual(unit._foreign_send(ledger), "refused")
                ledger.record(target=request.target_ip, source="test")
                if len(requests) == 1:
                    self.close_old()
                else:
                    self.live.sessions.append(unit._observed("new", unit.FULL_PN))

        outcome = await self.run_attempt(
            Sender(), advertised_server_ip="198.51.100.10", advertised_tcp_port=8899,
        )
        self.assertTrue(outcome.identity_certified, outcome.result)
        self.assertEqual(outcome.session_id, "new")
        self.assertEqual(len(requests), 2)
        self.assertIs(requests[0], requests[1])
        self.assertTrue(owners[0])
        self.assertEqual(owners[0], owners[1])
        self.assertEqual(len(self.reader.calls), 1)
        self.registry.release(outcome.handoff_owner)

    async def test_missing_or_extra_own_send_during_retry_fails_closed(self):
        for count in (0, 2):
            with self.subTest(count=count):
                self.silent = {"old"}
                self.closed.clear()
                self.release.reset_mock()
                sender = unit._Sender()

                def on_send():
                    self.close_old()
                    sender.sends = count

                sender.on_send = on_send
                outcome = await self.run_attempt(sender)
                self.assertEqual(outcome.result, identity.IDENTITY_TRIGGER_NOT_SENT)
                self.assertEqual(sender.calls, 2)
                self.assertFalse(self.reader.calls)
                self.assertEqual(self.registry.diagnostics()["claim_count"], 0)

    async def test_two_distinct_baseline_closures_keep_exact_three_send_accounting(self):
        self.silent.add("other-old")
        sender = unit._Sender()

        def on_send():
            if sender.calls == 1:
                self.silent.remove("other-old")
                self.closed.add("other-old")
            elif sender.calls == 2:
                self.close_old()
            else:
                self.live.sessions.append(unit._observed("new", unit.FULL_PN))

        sender.on_send = on_send
        outcome = await self.run_attempt(sender)
        self.assertTrue(outcome.identity_certified, outcome.result)
        self.assertEqual(sender.calls, 3)
        self.assertEqual(outcome.session_id, "new")
        self.registry.release(outcome.handoff_owner)

    async def test_foreign_send_after_closure_prevents_retrigger_or_claim(self):
        def interfere():
            self.close_old()
            unit.get_callback_trigger_ledger().record(target="192.0.2.99", source="foreign", attempt_id="")

        sender = unit._Sender(on_send=interfere)
        outcome = await self.run_attempt(sender)
        self.assertEqual(outcome.result, identity.IDENTITY_TRIGGER_INTERFERENCE)
        self.assertEqual(sender.calls, 1)
        self.assertFalse(self.reader.calls)

    async def test_second_trigger_with_wrong_full_pn_is_not_certified(self):
        sender = unit._Sender()

        def on_send():
            self.close_old()
            if sender.calls == 2:
                self.live.sessions.append(unit._observed("new", unit.FULL_PN))

        sender.on_send = on_send
        outcome = await self.run_attempt(sender, expected_pn=unit.OTHER_FULL_PN)
        self.assertEqual(outcome.result, identity.IDENTITY_MISMATCH)
        self.assertFalse(outcome.identity_certified)
        self.assertEqual(self.registry.diagnostics()["claim_count"], 0)

    async def test_foreign_send_during_retry_blocks_even_a_silent_offer(self):
        sender = unit._Sender()

        def on_send():
            self.close_old()
            if sender.calls == 2:
                self.silent.add("new")
                unit.get_callback_trigger_ledger().record(target="192.0.2.99", source="foreign", attempt_id="")

        sender.on_send = on_send
        outcome = await self.run_attempt(sender)
        self.assertEqual(outcome.result, identity.IDENTITY_TRIGGER_INTERFERENCE)
        self.assertIsNone(outcome.silent_bootstrap_offer)
        self.assertEqual(sender.calls, 2)
        self.assertFalse(self.reader.calls)

    async def test_inbound_never_retriggers_after_baseline_closure(self):
        sender = unit._Sender()

        def closed_view():
            self.close_old()
            return frozenset(self.closed)

        self.listener.closed_collector_session_ids = closed_view
        outcome = await self.run_attempt(sender, strategy="inbound")
        self.assertEqual(outcome.result, identity.IDENTITY_TIMEOUT)
        self.assertEqual(sender.calls, 0)

    async def test_unplanned_own_send_while_waiting_blocks_silent_probe_authority(self):
        sender = unit._Sender()

        def on_send():
            self.close_old()
            if sender.calls == 2:
                self.silent.add("new")

        def silent_view():
            if sender.calls == 2:
                unit.get_callback_trigger_ledger().record(target=unit.NAT_IP, source="unplanned")
            return [{"session_id": sid} for sid in self.silent]

        self.listener.silent_pending_collector_sessions = silent_view
        sender.on_send = on_send
        outcome = await self.run_attempt(sender)
        self.assertEqual(outcome.result, identity.IDENTITY_TRIGGER_NOT_SENT)
        self.assertIsNone(outcome.silent_bootstrap_offer)
        self.assertFalse(self.reader.calls)

    async def test_broadcast_placeholder_is_never_retriggered(self):
        sender = unit._Sender(on_send=self.close_old)
        outcome = await self.run_attempt(sender, target_ip="192.0.2.255")
        self.assertEqual(outcome.result, identity.IDENTITY_TIMEOUT)
        self.assertEqual(sender.calls, 1)

    async def test_cancellation_during_retry_releases_listener_and_causality(self):
        entered = asyncio.Event()
        calls = 0

        class Sender:
            async def async_send(inner, request):
                nonlocal calls
                calls += 1
                if calls == 1:
                    unit.get_callback_trigger_ledger().record(target=request.target_ip, source="test")
                    self.close_old()
                else:
                    entered.set()
                    await asyncio.Event().wait()

        task = asyncio.create_task(identity.async_run_callback_identity_transaction(
            self.hass, unit._request(session_wait_timeout=1), sender=Sender(),
        ))
        try:
            await asyncio.wait_for(entered.wait(), 1)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self.release.assert_awaited_once_with(self.listener)
        self.assertEqual(unit.get_callback_trigger_ledger().causality_owner(), "")
        self.assertEqual(self.registry.diagnostics()["claim_count"], 0)

    async def test_retry_send_cannot_extend_the_overall_wait_budget(self):
        calls = 0

        class Sender:
            async def async_send(inner, request):
                nonlocal calls
                calls += 1
                if calls == 1:
                    unit.get_callback_trigger_ledger().record(target=request.target_ip, source="test")
                    self.close_old()
                else:
                    await asyncio.Event().wait()

        async with asyncio.timeout(1):
            outcome = await self.run_attempt(Sender())
        self.assertEqual(outcome.result, identity.IDENTITY_TRIGGER_NOT_SENT)
        self.assertEqual(calls, 2)

    async def test_unavailable_closure_view_does_not_infer_closed_sockets(self):
        channel = SilentSessionIdentityProbeChannel(host="127.0.0.1", port=8899)
        await channel.async_open()
        try:
            self.listener.closed_collector_session_ids = lambda: (_ for _ in ()).throw(RuntimeError())
            self.assertEqual(channel.snapshot_closed_session_ids(), frozenset())
            del self.listener.closed_collector_session_ids
            self.assertEqual(channel.snapshot_closed_session_ids(), frozenset())
        finally:
            await channel.async_close()

    def test_only_concrete_unicast_ipv4_targets_qualify_for_extra_sends(self):
        for target in ("", "collector.local", "0.0.0.0", "224.0.0.1", "255.255.255.255", "192.0.2.255", "::1"):
            with self.subTest(target=target):
                self.assertFalse(identity._is_addressed_callback_target(target))
        self.assertTrue(identity._is_addressed_callback_target("192.0.2.10"))


if __name__ == "__main__":
    unittest.main()
