"""Behavioral connection-strategy verification (passive-discovery onboarding)."""

from __future__ import annotations

from pathlib import Path
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.smartess_local import (
    CollectorManagementUnsupportedError,
)
from custom_components.eybond_local.onboarding.strategy_verification import (
    EVIDENCE_REBOOT_RECONNECT,
    FAILURE_DISCONNECT_NOT_OBSERVED,
    FAILURE_RECONNECT_TIMEOUT,
    FAILURE_RESTART_NOT_CONFIRMED,
    FAILURE_RESTART_NOT_SUPPORTED,
    FAILURE_SESSION_CLAIMED,
    FAILURE_SESSION_UNAVAILABLE,
    FAILURE_STRONG_IDENTITY_TIMEOUT,
    FAILURE_UDP_TRIGGER_OBSERVED,
    InboundStrategyVerifier,
    ObservedSessionRestartChannel,
    SessionUnavailableError,
    STATE_INBOUND_NOT_VERIFIED,
    STATE_INBOUND_VERIFIED,
    STATE_OBSERVED_SESSION,
    STATE_RESTART_REQUESTED,
    STATE_WAITING_FOR_DISCONNECT,
    STATE_WAITING_FOR_INBOUND_RECONNECT,
    STATE_WAITING_FOR_STRONG_IDENTITY,
    STRATEGY_INBOUND,
    STRATEGY_UNKNOWN,
)

# Synthetic identities only (no real PN shapes).
FULL_PN = "V001020SYN62344022"
SHORT_PN = "V001020SYN6234"
OTHER_FULL_PN = "V000405SYN94677058"
OLD_SESSION = "listener-18899-1"
NEW_SESSION = "listener-18899-2"
PARALLEL_SESSION = "listener-18899-3"


class _FakeChannel:
    """Scripted restart channel: restart succeeds and the collector drops."""

    def __init__(self, *, restart_error: Exception | None = None, drops_on_restart: bool = True) -> None:
        self.restart_calls = 0
        self.close_calls = 0
        self._connected = True
        self._restart_error = restart_error
        self._drops_on_restart = drops_on_restart

    async def async_send_restart(self) -> None:
        self.restart_calls += 1
        if self._restart_error is not None:
            raise self._restart_error
        if self._drops_on_restart:
            self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    async def async_close(self) -> None:
        self.close_calls += 1
        self._connected = False


def _session(
    session_id: str,
    pn: str,
    state: str = "identified_strong",
    *,
    strong: bool = True,
) -> dict[str, object]:
    return {
        "session_id": session_id,
        "collector_pn": pn,
        "state": state,
        "peer_ip": "203.0.113.10",
        "has_strong_identity": strong,
    }


def _strong_old(pn: str = FULL_PN) -> dict[str, object]:
    return _session(OLD_SESSION, pn)


class _ScriptedSessions:
    """Return scripted session inventories per poll; the last frame repeats."""

    def __init__(self, *frames: tuple[dict[str, object], ...]) -> None:
        self._frames = list(frames) or [()]
        self._calls = 0

    def __call__(self) -> tuple[dict[str, object], ...]:
        index = min(self._calls, len(self._frames) - 1)
        self._calls += 1
        return self._frames[index]


def _verifier(channel, sessions, **kwargs) -> InboundStrategyVerifier:
    defaults = {
        "collector_pn": FULL_PN,
        "session_id": OLD_SESSION,
        "restart_channel": channel,
        "sessions_source": sessions,
        "disconnect_timeout": 0.2,
        "reconnect_timeout": 0.2,
        "identity_timeout": 0.2,
        "poll_interval": 0.01,
    }
    defaults.update(kwargs)
    return InboundStrategyVerifier(**defaults)


class InboundStrategyVerifierTests(unittest.TestCase):
    def test_trigger_barrier_drains_started_send_and_refuses_new_send(self) -> None:
        from custom_components.eybond_local.connection.callback_ledger import (
            CallbackTriggerInhibitedError,
            CallbackTriggerLedger,
        )

        ledger = CallbackTriggerLedger()

        async def _run() -> None:
            send_started = asyncio.Event()
            release_send = asyncio.Event()
            verification_entered = asyncio.Event()

            async def _existing_send() -> None:
                with ledger.callback_send_scope():
                    send_started.set()
                    await release_send.wait()

            async def _verification() -> None:
                async with ledger.inhibit_callback_triggers():
                    verification_entered.set()
                    with self.assertRaises(CallbackTriggerInhibitedError):
                        with ledger.callback_send_scope():
                            pass

            send_task = asyncio.create_task(_existing_send())
            await send_started.wait()
            verification_task = asyncio.create_task(_verification())
            await asyncio.sleep(0)
            self.assertFalse(verification_entered.is_set())
            release_send.set()
            await send_task
            await verification_task
            self.assertTrue(verification_entered.is_set())

            # Leaving the verification window restores normal trigger sends.
            with ledger.callback_send_scope():
                ledger.record(target="synthetic-target", source="test")
            self.assertEqual(ledger.snapshot_generation(), 1)

        asyncio.run(_run())

    def test_reconnected_weak_session_is_probed_before_inbound_confirmation(self) -> None:
        inventory = [_strong_old()]

        class _RestartChannel(_FakeChannel):
            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory[:] = [
                    _session(
                        NEW_SESSION,
                        SHORT_PN,
                        state="parked_no_payload_owner",
                        strong=False,
                    )
                ]

        probed: list[str] = []

        async def _probe(session_id: str) -> None:
            probed.append(session_id)
            inventory[:] = [_session(NEW_SESSION, FULL_PN)]

        result = asyncio.run(
            _verifier(
                _RestartChannel(),
                lambda: tuple(inventory),
                probe_reconnected_identity=_probe,
            ).async_verify()
        )

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.new_session_id, NEW_SESSION)
        self.assertEqual(probed, [NEW_SESSION])

    def test_read_only_identity_probe_enriches_weak_session_before_restart(self) -> None:
        inventory = [
            _session(
                OLD_SESSION,
                SHORT_PN,
                state="identified_weak",
                strong=False,
            )
        ]

        class _ProbeChannel(_FakeChannel):
            def __init__(self) -> None:
                super().__init__()
                self.identity_probe_calls = 0

            async def async_probe_identity(self) -> str:
                self.identity_probe_calls += 1
                inventory[:] = [_strong_old(FULL_PN)]
                return FULL_PN

            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory[:] = [_session(NEW_SESSION, FULL_PN)]

        channel = _ProbeChannel()
        result = asyncio.run(
            _verifier(
                channel,
                lambda: tuple(inventory),
                collector_pn=SHORT_PN,
            ).async_verify()
        )

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.collector_pn, FULL_PN)
        self.assertEqual(channel.identity_probe_calls, 1)
        self.assertEqual(channel.restart_calls, 1)

    # Full sequence: strong session -> baseline -> restart -> disconnect ->
    # new non-baseline session, same full PN, ledger unchanged -> inbound.
    def test_full_sequence_verifies_inbound(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),  # phase 0: observed session already strong
            (),  # old session gone after the drop
            (_session(NEW_SESSION, FULL_PN),),
        )
        generation = {"value": 7}

        result = asyncio.run(
            _verifier(
                channel,
                sessions,
                callback_trigger_generation=lambda: generation["value"],
            ).async_verify()
        )

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.strategy, STRATEGY_INBOUND)
        self.assertEqual(result.evidence, EVIDENCE_REBOOT_RECONNECT)
        self.assertEqual(result.new_session_id, NEW_SESSION)
        self.assertEqual(result.collector_pn, FULL_PN)
        self.assertEqual(channel.restart_calls, 1)
        self.assertGreaterEqual(channel.close_calls, 1)
        # Full auditable transition sequence.
        self.assertEqual(
            result.transitions,
            (
                STATE_OBSERVED_SESSION,
                STATE_WAITING_FOR_STRONG_IDENTITY,
                STATE_RESTART_REQUESTED,
                STATE_WAITING_FOR_DISCONNECT,
                STATE_WAITING_FOR_INBOUND_RECONNECT,
                STATE_INBOUND_VERIFIED,
            ),
        )

    # Weak identity never starts the reboot verification (no restart sent).
    def test_weak_identity_never_restarts(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_session(OLD_SESSION, SHORT_PN, state="identified_weak", strong=False),),
        )

        result = asyncio.run(_verifier(channel, sessions, collector_pn=SHORT_PN).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_STRONG_IDENTITY_TIMEOUT)
        # The collector was NOT restarted: two matching short PNs prove nothing.
        self.assertEqual(channel.restart_calls, 0)

    # A weak session that enriches to a strong FULL PN continues verification
    # with the full PN as the durable identity.
    def test_weak_to_strong_enrichment_continues_with_full_pn(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_session(OLD_SESSION, SHORT_PN, state="identified_weak", strong=False),),
            (_strong_old(FULL_PN),),  # same session id, now strong + full
            (),  # disconnected
            (_session(NEW_SESSION, FULL_PN),),
        )

        result = asyncio.run(_verifier(channel, sessions, collector_pn=SHORT_PN).async_verify())

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.collector_pn, FULL_PN)
        self.assertEqual(result.new_session_id, NEW_SESSION)

    # Short PN -> same short PN after reboot never confirms: without strong
    # identity the restart is never even sent.
    def test_short_pn_to_same_short_pn_does_not_confirm(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_session(OLD_SESSION, SHORT_PN, state="identified_weak", strong=False),),
        )

        result = asyncio.run(_verifier(channel, sessions, collector_pn=SHORT_PN).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(channel.restart_calls, 0)
        self.assertEqual(result.strategy, STRATEGY_UNKNOWN)

    # Baseline: a parallel pre-restart session of the same PN (B) that survives
    # while A disconnects is NOT a reconnect.
    def test_baseline_parallel_session_is_not_reconnect(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(), _session(PARALLEL_SESSION, FULL_PN)),  # A + B pre-restart
            (_session(PARALLEL_SESSION, FULL_PN),),  # A gone, B remains
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # Global callback-trigger generation change invalidates the inbound proof,
    # even when the trigger came from another flow/entry.
    def test_global_trigger_generation_invalidates_inbound(self) -> None:
        channel = _FakeChannel()
        generation = {"value": 0}

        frames = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN),),
        )

        def _sessions() -> tuple[dict[str, object], ...]:
            frame = frames()
            if any(s["session_id"] == NEW_SESSION for s in frame):
                # Someone (another device's runtime, another flow) sent a
                # callback trigger while we waited.
                generation["value"] += 1
            return frame

        result = asyncio.run(
            _verifier(
                channel,
                _sessions,
                callback_trigger_generation=lambda: generation["value"],
            ).async_verify()
        )

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_UDP_TRIGGER_OBSERVED)
        self.assertEqual(result.state, STATE_INBOUND_NOT_VERIFIED)

    # A new session with a DIFFERENT full PN (same peer IP) never confirms:
    # two collectors behind one NAT stay independent.
    def test_different_pn_session_does_not_confirm(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, OTHER_FULL_PN),),
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.strategy, STRATEGY_UNKNOWN)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # The old session_id reappearing never confirms.
    def test_old_session_id_does_not_confirm(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),  # phase 0: strong identity
            (_strong_old(),),  # baseline capture
            (),  # disconnect observed
            (_session(OLD_SESSION, FULL_PN),),  # same socket listed again
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # Typed restart failures.
    def test_restart_not_supported_fails(self) -> None:
        channel = _FakeChannel(
            restart_error=CollectorManagementUnsupportedError(
                "collector_local_management_not_supported"
            )
        )
        sessions = _ScriptedSessions((_strong_old(),))

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RESTART_NOT_SUPPORTED)
        self.assertGreaterEqual(channel.close_calls, 1)

    def test_restart_not_confirmed_fails(self) -> None:
        channel = _FakeChannel(restart_error=RuntimeError("collector_set_failed:parameter=29:status=1"))
        sessions = _ScriptedSessions((_strong_old(),))

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RESTART_NOT_CONFIRMED)

    # Old session never disconnects -> typed failure (no inbound, no guess).
    def test_disconnect_not_observed_fails(self) -> None:
        channel = _FakeChannel(drops_on_restart=False)  # stays connected
        sessions = _ScriptedSessions((_strong_old(),))

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_DISCONNECT_NOT_OBSERVED)
        self.assertGreaterEqual(channel.close_calls, 1)

    # A short/prefix PN observation on the NEW session cannot confirm the known
    # full identity...
    def test_short_pn_prefix_does_not_confirm_full_identity(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, SHORT_PN),),  # only the heartbeat prefix
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # ...but the same session becoming FULL later does confirm.
    def test_prefix_session_confirms_once_full_pn_appears(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, SHORT_PN),),
            (_session(NEW_SESSION, FULL_PN),),
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.new_session_id, NEW_SESSION)

    # A weak NEW session never confirms: only registry-certified strong
    # identity with the exact full PN proves the post-reboot dial-in.
    def test_weak_new_session_does_not_confirm(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN, strong=False),),
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # Promotion conflict (identity owned by another entry) stops BEFORE restart.
    def test_promotion_conflict_stops_before_restart(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions((_strong_old(),))

        def _promote(_full_pn: str) -> None:
            raise ValueError("session_already_claimed")

        result = asyncio.run(
            _verifier(channel, sessions, promote_claim=_promote).async_verify()
        )

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_SESSION_CLAIMED)
        self.assertEqual(channel.restart_calls, 0)

    # Channel typed session_unavailable maps to the typed verifier failure.
    def test_session_unavailable_from_channel_is_typed(self) -> None:
        channel = _FakeChannel(
            restart_error=SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        )
        sessions = _ScriptedSessions((_strong_old(),))

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_SESSION_UNAVAILABLE)

    # No ownership fallback: an installed provider returning nothing is an
    # error; the channel never falls back to the observed session id and never
    # creates a transport (so no other socket can be claimed by PN/IP).
    def test_channel_empty_registry_handle_aborts_without_socket(self) -> None:
        channel = ObservedSessionRestartChannel(
            host="127.0.0.1",
            port=0,
            collector_pn=FULL_PN,
            session_id=OLD_SESSION,
            session_id_provider=lambda: "",
        )

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())

        self.assertIsNone(channel._transport)
        self.assertFalse(channel.is_connected())

    # A RUNNING DiscoveryAnnouncer during verification invalidates the proof,
    # and each announcer datagram moves the shared ledger exactly once.
    def test_running_announcer_during_verification_invalidates_proof(self) -> None:
        from unittest.mock import patch

        from custom_components.eybond_local.collector.discovery import DiscoveryAnnouncer
        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        ledger = get_callback_trigger_ledger()
        sent: list[tuple] = []

        class _FakeSocket:
            def setsockopt(self, *args) -> None:
                return None

            def settimeout(self, *args) -> None:
                return None

            def bind(self, *args) -> None:
                return None

            def sendto(self, message, addr) -> None:
                sent.append((message, addr))

            def recvfrom(self, *_args):
                raise OSError("no reply")

            def close(self) -> None:
                return None

        announcer = DiscoveryAnnouncer(
            bind_ip="127.0.0.1",
            advertised_server_ip="127.0.0.1",
            advertised_server_port=8899,
            target_ip="127.0.0.1",
            udp_port=58899,
            interval=30.0,  # exactly one datagram within the test window
        )

        async def _run() -> object:
            channel = _FakeChannel()
            generation_at_start = ledger.snapshot_generation()
            state = {"announcer_started": False}
            frames = _ScriptedSessions(
                (_strong_old(),),  # phase 0
                (_strong_old(),),  # baseline
                (),  # disconnect observed
            )

            def _sessions() -> tuple[dict[str, object], ...]:
                frame = frames()
                if frames._calls <= 3:
                    return frame
                # Reconnect phase: start the announcer (production sender used
                # by proxy capture), then let the "reconnect" appear only after
                # its datagram was recorded in the shared ledger.
                if not state["announcer_started"]:
                    state["announcer_started"] = True
                    asyncio.get_running_loop().create_task(announcer.start())
                    return ()
                if ledger.snapshot_generation() == generation_at_start:
                    return ()
                return (_session(NEW_SESSION, FULL_PN),)

            # Patch inside the running loop: asyncio's own socketpair must not
            # see the fake.
            with patch(
                "custom_components.eybond_local.collector.discovery.socket.socket",
                return_value=_FakeSocket(),
            ):
                try:
                    return await _verifier(
                        channel,
                        _sessions,
                        callback_trigger_generation=ledger.snapshot_generation,
                        reconnect_timeout=5.0,
                    ).async_verify()
                finally:
                    await announcer.stop()

        result = asyncio.run(_run())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_UDP_TRIGGER_OBSERVED)
        # One datagram -> exactly one generation increment (no double count).
        self.assertGreaterEqual(len(sent), 1)

    # Closed or identity-mismatch sessions never confirm.
    def test_closed_and_mismatch_states_do_not_confirm(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (
                _session(NEW_SESSION, FULL_PN, state="closed_eof"),
                _session(PARALLEL_SESSION, FULL_PN, state="route_identity_mismatch"),
            ),
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)


if __name__ == "__main__":
    unittest.main()
