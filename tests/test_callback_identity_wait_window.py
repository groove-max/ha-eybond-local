"""Deterministic regressions for silent reconnects that outlive a 20s window."""

from __future__ import annotations

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
    async def wait_for_sessions(self, schedule, *, readable=None, baseline=()):
        clock = _Clock()
        channel = SimpleNamespace(snapshot_silent_session_ids=lambda: frozenset(
            session_id for session_id, start, end in schedule
            if start <= clock.now < end
        ))
        fake_asyncio = SimpleNamespace(get_running_loop=lambda: clock, sleep=clock.sleep)
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


if __name__ == "__main__":
    unittest.main()
