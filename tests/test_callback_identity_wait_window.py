"""Deterministic regressions for silent reconnects that outlive a 20s window."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from custom_components.eybond_local.connection import callback_identity as identity
from custom_components.eybond_local.timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY


class _Clock:
    def __init__(self):
        self.now = 0.0

    def time(self):
        return self.now

    async def sleep(self, duration):
        self.now += duration


class CallbackIdentityWaitWindowTests(unittest.IsolatedAsyncioTestCase):
    async def wait_for_sessions(self, schedule, *, readable=None, baseline=(), closed=None, retrigger=None):
        clock = _Clock()
        channel = SimpleNamespace(snapshot_silent_session_ids=lambda: frozenset(
            session_id for session_id, start, end in schedule
            if start <= clock.now < end
        ), snapshot_closed_session_ids=lambda: frozenset(closed(clock.now) if closed else ()))
        fake_asyncio = SimpleNamespace(
            get_running_loop=lambda: clock, sleep=clock.sleep,
            wait_for=asyncio.wait_for, TimeoutError=asyncio.TimeoutError,
        )
        with (
            patch.object(identity, "asyncio", fake_asyncio),
            patch.object(identity, "_session_views", side_effect=lambda _: (
                readable(clock.now) if readable else []
            )),
        ):
            result = await identity._async_wait_for_new_session(
                object(), baseline=frozenset(), old_session_id="",
                timeout=DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_identity_session_wait,
                probe_channel=channel, pending_baseline=frozenset(baseline),
                retrigger=retrigger,
            )
        return result, clock.now

    async def test_reconnect_after_previous_twenty_second_window_is_offered_alive(self):
        # Issue #75: an old socket predates the attempt; its replacement arrives
        # roughly 31s later and lasts ~35s. Never probe/adopt the old socket.
        result, elapsed = await self.wait_for_sessions(
            [("old", -9, 26), ("new", 31, 66)], baseline=["old"],
        )
        self.assertEqual(result, (None, True, "new", ""))
        self.assertGreaterEqual(elapsed, 31)
        self.assertLess(elapsed, 36)

    async def test_early_silent_socket_is_offered_before_it_closes(self):
        result, elapsed = await self.wait_for_sessions([("new", 1, 15)])
        self.assertEqual(result, (None, True, "new", ""))
        self.assertLess(elapsed, 6)

    async def test_closed_socket_never_leaves_a_stale_offer(self):
        result, _ = await self.wait_for_sessions([("closed", 1, 1.5)])
        self.assertEqual(result, (None, True, "", ""))

    async def test_two_silent_sockets_do_not_become_unambiguous_when_one_closes(self):
        result, _ = await self.wait_for_sessions(
            [("first", 1, 2), ("second", 1.5, 100)],
        )
        self.assertEqual(result, (None, True, "", ""))

    async def test_preexisting_socket_is_not_offered(self):
        result, _ = await self.wait_for_sessions(
            [("old", -5, 100)], baseline=["old"],
        )
        self.assertEqual(result, (None, False, "", ""))

    async def test_delayed_initial_identity_wins_over_silent_offer(self):
        session = {"session_id": "new", "state": "routed_framed",
                   "protocol_shape": "eybond_framed"}
        result, elapsed = await self.wait_for_sessions(
            [("new", 1, 2)], readable=lambda now: [session] if now >= 2 else [],
        )
        self.assertIs(result[0], session)
        self.assertLess(elapsed, 3)

    async def test_only_a_new_trigger_after_fin_produces_the_new_socket(self):
        calls = []

        async def retrigger():
            calls.append(True)
            schedule.append(("new", 0, 100))
            return ""

        schedule = [("old", -9, 21)]
        result, elapsed = await self.wait_for_sessions(
            schedule, baseline=["old"], closed=lambda now: ["old"] if now >= 21 else [],
            retrigger=retrigger,
        )
        self.assertEqual(result, (None, True, "new", ""))
        self.assertEqual(len(calls), 1)
        self.assertGreaterEqual(elapsed, 23)
        self.assertLess(elapsed, 24)

    async def test_closure_is_consumed_once_and_deadline_is_not_restarted(self):
        calls = []

        async def retrigger():
            calls.append(True)
            return ""

        result, elapsed = await self.wait_for_sessions(
            [("old", -1, 21)], baseline=["old"],
            closed=lambda now: ["old"] if now >= 21 else [], retrigger=retrigger,
        )
        self.assertEqual(result, (None, False, "", ""))
        self.assertEqual(len(calls), 1)
        self.assertGreaterEqual(elapsed, 60)
        self.assertLess(elapsed, 60.3)

    async def test_many_closures_are_coalesced_and_repeat_count_is_bounded(self):
        calls = []

        async def retrigger():
            calls.append(True)
            return ""

        schedule = [(str(n), -1, n) for n in range(1, 15)]
        await self.wait_for_sessions(
            schedule, baseline=[row[0] for row in schedule],
            closed=lambda now: [sid for sid, _, end in schedule if now >= end],
            retrigger=retrigger,
        )
        self.assertEqual(len(calls), 2)

    async def test_disappearance_foreign_closure_or_new_candidate_never_retriggers(self):
        async def forbidden():
            self.fail("No further trigger is authorized")

        for schedule, closed in (
            ([("old", -1, 1)], lambda now: []),  # vanished, not proved closed
            ([("old", -1, 100)], lambda now: ["not-baseline"]),
            ([("old", -1, 10), ("new", 1, 1.5)], lambda now: ["old"] if now >= 10 else []),
        ):
            with self.subTest(schedule=schedule):
                await self.wait_for_sessions(
                    schedule, baseline=["old"], closed=closed, retrigger=forbidden,
                )

    async def test_a_baseline_silent_socket_becoming_readable_is_still_excluded(self):
        result, elapsed = await self.wait_for_sessions(
            [("old", -1, 1)], baseline=["old"],
            readable=lambda now: [{"session_id": "old", "state": "routed_framed"}] if now >= 1 else [],
        )
        self.assertEqual(result, (None, False, "", ""))
        self.assertGreaterEqual(elapsed, 60)

    async def test_retrigger_failure_is_not_misreported_as_collector_timeout(self):
        async def failed():
            return identity.IDENTITY_TRIGGER_NOT_SENT

        result, elapsed = await self.wait_for_sessions(
            [("old", -1, 1)], baseline=["old"], closed=lambda now: ["old"], retrigger=failed,
        )
        self.assertEqual(result, (None, False, "", identity.IDENTITY_TRIGGER_NOT_SENT))
        self.assertLess(elapsed, 3)


if __name__ == "__main__":
    unittest.main()
