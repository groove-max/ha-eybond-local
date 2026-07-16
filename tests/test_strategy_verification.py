"""The typed inbound recovery transaction (reboot -> autonomous reconnect).

The verifier proves RECOVERY, never strategy: its product is a typed
``InboundRecoveryProof`` (only on full success) and it owns its whole causal
window (callback causality lease + trigger inhibitor) itself.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
import types
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.management import (
    CollectorManagementUnsupportedError,
)
from custom_components.eybond_local.connection.callback_ledger import (
    CallbackTriggerLedger,
)
from custom_components.eybond_local.connection.recovery_contract import (
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    RecoveryContract,
)
from custom_components.eybond_local.onboarding.strategy_verification import (
    FAILURE_CAUSALITY_BUSY,
    FAILURE_DISCONNECT_NOT_OBSERVED,
    FAILURE_RECONNECT_TIMEOUT,
    FAILURE_RECONNECTED_SESSION_UNTRUSTED,
    FAILURE_RESTART_NOT_CONFIRMED,
    FAILURE_RESTART_NOT_SUPPORTED,
    FAILURE_SESSION_CLAIMED,
    FAILURE_SESSION_UNAVAILABLE,
    FAILURE_STRONG_IDENTITY_TIMEOUT,
    FAILURE_UDP_TRIGGER_OBSERVED,
    InboundRecoveryVerifier,
    ObservedSessionRestartChannel,
    SessionUnavailableError,
    STATE_INBOUND_NOT_VERIFIED,
    STATE_INBOUND_VERIFIED,
    STATE_OBSERVED_SESSION,
    STATE_RESTART_REQUESTED,
    STATE_WAITING_FOR_DISCONNECT,
    STATE_WAITING_FOR_INBOUND_RECONNECT,
    STATE_WAITING_FOR_STRONG_IDENTITY,
)
from custom_components.eybond_local.onboarding.timeouts import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)

# Synthetic identities only (no real PN shapes).
FULL_PN = "V001020SYN62344022"
SHORT_PN = "V001020SYN6234"
OTHER_FULL_PN = "V000405SYN94677058"
OLD_SESSION = "listener-18899-1"
NEW_SESSION = "listener-18899-2"
PARALLEL_SESSION = "listener-18899-3"
TS = "2026-07-16T10:00:00+00:00"

# The FAST policy: budgets come from OnboardingTimeoutPolicy (no module magic
# constants left to monkeypatch), so tests tune a policy instance.
FAST_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    inbound_strong_identity_timeout=0.2,
    inbound_restart_disconnect_timeout=0.2,
    inbound_reconnect_timeout=0.2,
    callback_causality_lease_wait=1.0,
)


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
    identity_source: str | None = None,
    protocol_shape: str = "eybond_framed",
) -> dict[str, object]:
    source = (
        identity_source
        if identity_source is not None
        else ("fc2_parameter_2" if strong else "framed_heartbeat")
    )
    return {
        "session_id": session_id,
        "collector_pn": pn,
        "state": state,
        "peer_ip": "203.0.113.10",
        "has_strong_identity": strong,
        "collector_identity_source": source,
        # The raw listener observation the wire is negotiated from.
        "raw": {
            "session_id": session_id,
            "state": "routed_framed",
            "protocol_shape": protocol_shape,
        },
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


def _verifier(channel, sessions, **kwargs) -> InboundRecoveryVerifier:
    defaults = {
        "collector_pn": FULL_PN,
        "session_id": OLD_SESSION,
        "restart_channel": channel,
        "sessions_source": sessions,
        "clock": lambda: TS,
        "policy": FAST_POLICY,
        # A fresh ledger per verifier: the transaction owns its own causal
        # window without cross-test interference on the process-global one.
        "ledger": CallbackTriggerLedger(),
        # A retarget capability is MANDATORY for any proof-producing run;
        # tests exercising its absence pass ``retarget_claim=None`` explicitly.
        "retarget_claim": lambda _sid: True,
        "poll_interval": 0.01,
    }
    defaults.update(kwargs)
    return InboundRecoveryVerifier(**defaults)


class InboundRecoveryVerifierTests(unittest.TestCase):
    def test_trigger_barrier_drains_started_send_and_refuses_new_send(self) -> None:
        from custom_components.eybond_local.connection.callback_ledger import (
            CallbackTriggerInhibitedError,
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

    # Full sequence: strong session -> baseline -> reboot -> disconnect ->
    # new non-baseline session, same full PN, silent window -> typed proof.
    def test_full_sequence_yields_typed_inbound_proof(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),  # phase 0: observed session already strong
            (),  # old session gone after the drop
            (_session(NEW_SESSION, FULL_PN),),
        )
        retargets: list[str] = []

        result = asyncio.run(
            _verifier(
                channel,
                sessions,
                retarget_claim=lambda sid: retargets.append(sid) or True,
            ).async_verify()
        )

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.new_session_id, NEW_SESSION)
        self.assertEqual(result.collector_pn, FULL_PN)
        self.assertEqual(channel.restart_calls, 1)
        self.assertGreaterEqual(channel.close_calls, 1)
        # SUCCESS retargeted ownership onto the NEW socket before returning.
        self.assertEqual(retargets, [NEW_SESSION])
        # The typed proof carries everything the recovery contract needs.
        proof = result.proof
        self.assertIsNotNone(proof)
        self.assertEqual(proof.method, INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER)
        self.assertEqual(proof.collector_pn, FULL_PN)
        self.assertEqual(proof.identity_source, "fc2_parameter_2")
        self.assertEqual(proof.verified_at, TS)
        self.assertEqual(proof.session_protocol, "eybond_framed")
        # ... and is accepted verbatim by the strict contract model.
        contract = RecoveryContract.empty_for_pn(
            proof.collector_pn, identity_source=proof.identity_source
        ).with_inbound_proof(proof, updated_at=proof.verified_at)
        self.assertTrue(contract.inbound_verified)
        # No strategy and no legacy evidence anywhere on the outcome.
        self.assertFalse(hasattr(result, "strategy"))
        self.assertFalse(hasattr(result, "evidence"))
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

    def test_reconnected_weak_session_is_probed_exactly_once(self) -> None:
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
        retargets: list[str] = []

        async def _probe(session_id: str) -> None:
            probed.append(session_id)
            inventory[:] = [_session(NEW_SESSION, FULL_PN)]

        result = asyncio.run(
            _verifier(
                _RestartChannel(),
                lambda: tuple(inventory),
                probe_reconnected_identity=_probe,
                retarget_claim=lambda sid: retargets.append(sid) or True,
            ).async_verify()
        )

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.new_session_id, NEW_SESSION)
        # EXACTLY ONE authoritative enrichment per candidate socket: a
        # duplicate invocation is structurally impossible.
        self.assertEqual(probed, [NEW_SESSION])
        # Retarget happened BEFORE the probe (the reader is claim-pinned), and
        # the success-path retarget is idempotent -- same socket, no churn.
        self.assertEqual(retargets[0], NEW_SESSION)
        self.assertTrue(all(sid == NEW_SESSION for sid in retargets))

    def test_probe_skipped_when_retarget_refused(self) -> None:
        # The claim could not be moved onto the candidate: the pinned reader
        # would read the WRONG socket, so the probe must not run at all.
        inventory = [_strong_old()]

        class _RestartChannel(_FakeChannel):
            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory[:] = [
                    _session(NEW_SESSION, SHORT_PN, strong=False)
                ]

        probed: list[str] = []

        async def _probe(session_id: str) -> None:
            probed.append(session_id)

        result = asyncio.run(
            _verifier(
                _RestartChannel(),
                lambda: tuple(inventory),
                probe_reconnected_identity=_probe,
                retarget_claim=lambda _sid: False,
            ).async_verify()
        )

        self.assertFalse(result.inbound_verified)
        self.assertEqual(probed, [])

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

    # Weak identity never starts the reboot verification (no restart sent).
    def test_weak_identity_never_restarts(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_session(OLD_SESSION, SHORT_PN, state="identified_weak", strong=False),),
        )

        result = asyncio.run(_verifier(channel, sessions, collector_pn=SHORT_PN).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
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
        self.assertEqual(result.proof.collector_pn, FULL_PN)
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
        self.assertIsNone(result.proof)
        self.assertEqual(channel.restart_calls, 0)

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
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # Defense in depth behind the lease: a ledger generation change during the
    # window invalidates the proof even though the lease/inhibitor were held.
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
                # Someone (an out-of-process sender, a raw tool) slipped a
                # trigger through while we waited.
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
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_UDP_TRIGGER_OBSERVED)
        self.assertEqual(result.status, STATE_INBOUND_NOT_VERIFIED)

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
        self.assertIsNone(result.proof)
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
                "collector_reboot_unsupported_on_at_wire"
            )
        )
        sessions = _ScriptedSessions((_strong_old(),))

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
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
        self.assertIsNone(result.proof)
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
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_RECONNECT_TIMEOUT)

    # The registry says strong but the certified SOURCE is not authoritative:
    # the proof gate fails closed (identity_source_is_strong is the authority).
    def test_strong_flag_with_weak_source_cannot_carry_a_proof(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (
                _session(
                    NEW_SESSION,
                    FULL_PN,
                    strong=True,
                    identity_source="framed_heartbeat",
                ),
            ),
        )

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
        self.assertEqual(
            result.failure_reason, FAILURE_RECONNECTED_SESSION_UNTRUSTED
        )

    # Success requires the claim to end up on the NEW socket.
    def test_success_requires_retargeted_claim(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN),),
        )

        result = asyncio.run(
            _verifier(
                channel, sessions, retarget_claim=lambda _sid: False
            ).async_verify()
        )

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_SESSION_CLAIMED)

    def test_missing_retarget_capability_refuses_before_any_reset(self) -> None:
        # A proof-producing success is structurally impossible without the
        # ownership retarget hook: the engine refuses upfront -- BEFORE the
        # lease and BEFORE the collector is rebooted -- instead of producing
        # a proof whose claim nobody moved.
        from custom_components.eybond_local.onboarding.strategy_verification import (
            FAILURE_OWNERSHIP_UNAVAILABLE,
        )

        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN),),
        )

        result = asyncio.run(
            _verifier(channel, sessions, retarget_claim=None).async_verify()
        )

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_OWNERSHIP_UNAVAILABLE)
        self.assertEqual(channel.restart_calls, 0)

    # The verifier must never return a success the contract would refuse: the
    # proof is pre-validated through the strict RecoveryContract builder.
    def test_invalid_clock_yields_typed_failure_and_no_final_retarget(self) -> None:
        from custom_components.eybond_local.onboarding.strategy_verification import (
            FAILURE_INBOUND_PROOF_INVALID,
        )

        for label, bad_clock in (
            ("naive", lambda: "2026-07-16T10:00:00"),
            ("date_only", lambda: "2026-07-16"),
            ("empty", lambda: ""),
            ("garbage", lambda: "soon"),
            ("raises", lambda: (_ for _ in ()).throw(RuntimeError("clock broken"))),
        ):
            with self.subTest(clock=label):
                channel = _FakeChannel()
                sessions = _ScriptedSessions(
                    (_strong_old(),),
                    (),
                    (_session(NEW_SESSION, FULL_PN),),
                )
                retargets: list[str] = []

                result = asyncio.run(
                    _verifier(
                        channel,
                        sessions,
                        clock=bad_clock,
                        retarget_claim=lambda sid: retargets.append(sid) or True,
                    ).async_verify()
                )

                self.assertFalse(result.inbound_verified)
                self.assertIsNone(result.proof)
                self.assertEqual(
                    result.failure_reason, FAILURE_INBOUND_PROOF_INVALID
                )
                # The FINAL retarget never ran: the claim was not moved for a
                # success that cannot be persisted.
                self.assertEqual(retargets, [])
                # Channel cleanup still happened.
                self.assertGreaterEqual(channel.close_calls, 1)

    def test_valid_aware_clock_with_offset_stays_green(self) -> None:
        channel = _FakeChannel()
        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN),),
        )

        result = asyncio.run(
            _verifier(
                channel, sessions, clock=lambda: "2026-07-16T13:00:00+03:00"
            ).async_verify()
        )

        self.assertTrue(result.inbound_verified)
        self.assertEqual(result.proof.verified_at, "2026-07-16T13:00:00+03:00")

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


class CausalityOwnershipTests(unittest.IsolatedAsyncioTestCase):
    """The transaction OWNS its causal window -- it cannot run unwrapped."""

    async def test_lease_and_inhibitor_are_held_for_the_whole_window(self) -> None:
        from custom_components.eybond_local.connection.callback_ledger import (
            CallbackTriggerInhibitedError,
        )

        ledger = CallbackTriggerLedger()
        seen: dict[str, object] = {}

        class _ObservingChannel(_FakeChannel):
            async def async_send_restart(self) -> None:
                # DURING the verification window: the lease is held and any
                # callback sender is refused by the inhibitor.
                seen["owner_during"] = ledger.causality_owner()
                try:
                    with ledger.callback_send_scope():
                        seen["send_allowed"] = True
                except CallbackTriggerInhibitedError:
                    seen["send_allowed"] = False
                await super().async_send_restart()

        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN),),
        )
        result = await _verifier(
            _ObservingChannel(), sessions, ledger=ledger
        ).async_verify()

        self.assertTrue(result.inbound_verified)
        self.assertTrue(str(seen["owner_during"]).startswith("recovery_verification:"))
        self.assertFalse(seen["send_allowed"])
        # After the transaction: lease released, sends allowed again.
        self.assertEqual(ledger.causality_owner(), "")
        with ledger.callback_send_scope():
            pass

    async def test_concurrent_callback_attempt_cannot_enter_the_window(self) -> None:
        # A callback identity attempt sharing the ledger queues on the SAME
        # lease: it cannot slip between the verifier's baseline and proof.
        ledger = CallbackTriggerLedger()
        in_restart = asyncio.Event()
        release_restart = asyncio.Event()

        class _PausingChannel(_FakeChannel):
            async def async_send_restart(self) -> None:
                in_restart.set()
                await release_restart.wait()
                await super().async_send_restart()

        sessions = _ScriptedSessions(
            (_strong_old(),),
            (),
            (_session(NEW_SESSION, FULL_PN),),
        )
        verify_task = asyncio.create_task(
            _verifier(_PausingChannel(), sessions, ledger=ledger).async_verify()
        )
        await asyncio.wait_for(in_restart.wait(), timeout=2.0)

        async def _competing_attempt() -> str:
            try:
                async with ledger.causality_lease("callback_identity:x", timeout=0.05):
                    return "entered"
            except Exception:
                return "blocked"

        # While the verifier holds the window, the competing attempt is blocked.
        self.assertEqual(await _competing_attempt(), "blocked")
        release_restart.set()
        result = await asyncio.wait_for(verify_task, timeout=2.0)
        self.assertTrue(result.inbound_verified)
        # Afterwards the same attempt serializes in normally.
        self.assertEqual(await _competing_attempt(), "entered")

    async def test_busy_causality_is_a_typed_refusal(self) -> None:
        ledger = CallbackTriggerLedger()
        channel = _FakeChannel()
        sessions = _ScriptedSessions((_strong_old(),))

        async with ledger.causality_lease("callback_identity:other", timeout=1.0):
            result = await _verifier(
                channel,
                sessions,
                ledger=ledger,
                policy=replace(FAST_POLICY, callback_causality_lease_wait=0.05),
            ).async_verify()

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_CAUSALITY_BUSY)
        # The collector was never touched.
        self.assertEqual(channel.restart_calls, 0)

    async def test_cancellation_at_every_await_phase_cleans_up(self) -> None:
        # Phases: strong-identity wait, disconnect wait, reconnect wait. Cancel
        # in each: no proof escapes, the channel is closed, and the lease +
        # inhibitor are fully released (a send succeeds right after).
        async def _cancel_in_phase(frames, *, stall_restart: bool) -> None:
            ledger = CallbackTriggerLedger()

            channel = _FakeChannel(drops_on_restart=not stall_restart)
            verifier = _verifier(
                channel,
                frames,
                ledger=ledger,
                policy=replace(
                    FAST_POLICY,
                    inbound_strong_identity_timeout=30.0,
                    inbound_restart_disconnect_timeout=30.0,
                    inbound_reconnect_timeout=30.0,
                ),
            )
            task = asyncio.create_task(verifier.async_verify())
            await asyncio.sleep(0.08)  # let it park in the target phase
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertGreaterEqual(channel.close_calls, 1)
            self.assertEqual(ledger.causality_owner(), "")
            with ledger.callback_send_scope():  # inhibitor fully released
                pass

        # 1) parked waiting for strong identity (old session never strong).
        await _cancel_in_phase(
            _ScriptedSessions(
                (_session(OLD_SESSION, SHORT_PN, strong=False),),
            ),
            stall_restart=False,
        )
        # 2) parked waiting for disconnect (old socket never closes).
        await _cancel_in_phase(
            _ScriptedSessions((_strong_old(),)),
            stall_restart=True,
        )
        # 3) parked waiting for reconnect (nothing ever dials back).
        await _cancel_in_phase(
            _ScriptedSessions(
                (_strong_old(),),
                (),
            ),
            stall_restart=False,
        )


def _real_handle(
    wire: str,
    adapter: str,
    *,
    conflict: str = "",
    state: str = "routed_framed",
):
    """A REAL SessionHandle: the channel accepts nothing else."""

    from custom_components.eybond_local.connection.session_handle import SessionHandle

    return SessionHandle(
        session_id=OLD_SESSION,
        collector_pn=FULL_PN,
        wire_framing=wire,
        collector_management_adapter=adapter,
        conflict=conflict,
        state=state,
    )


class ObservedSessionRestartChannelTests(unittest.TestCase):
    """The channel's ownership and management-adapter boundaries.

    Every check fails typed BEFORE any transport exists: no handle provider,
    no session provider, a forged handle, an unobserved or conflicting one --
    none of them may claim a socket or write a byte, and nothing is ever
    "assumed framed".
    """

    def _channel(self, **overrides) -> ObservedSessionRestartChannel:
        kwargs = dict(
            host="127.0.0.1",
            port=0,
            collector_pn=FULL_PN,
            session_id=OLD_SESSION,
            session_id_provider=lambda: OLD_SESSION,
            handle_provider=lambda: _real_handle(
                "eybond_framed", "framed_collector_commands"
            ),
        )
        kwargs.update(overrides)
        return ObservedSessionRestartChannel(**kwargs)

    def _assert_no_transport(self, channel) -> None:
        self.assertIsNone(channel._framed_transport)
        self.assertIsNone(channel._at_transport)
        self.assertFalse(channel.is_connected())

    def test_channel_empty_registry_handle_aborts_without_socket(self) -> None:
        # No ownership fallback: an installed provider returning nothing is an
        # error; the channel never falls back to the observed session id and
        # never creates a transport (so no other socket can be claimed by
        # PN/IP).
        channel = self._channel(session_id_provider=lambda: "")

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())

        self._assert_no_transport(channel)

    def test_no_session_provider_never_uses_static_session_id(self) -> None:
        # The statically observed session_id is context, never ownership: with
        # no registry claim resolver the channel fails typed, transportless.
        channel = self._channel(session_id_provider=None)

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())
        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_probe_identity())

        self._assert_no_transport(channel)

    def test_no_handle_provider_is_never_assumed_framed(self) -> None:
        # A valid claimed session id alone is NOT enough: without a live
        # negotiated handle there is no wire and no adapter -- and no transport
        # is ever created to find out.
        channel = self._channel(handle_provider=None)

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())
        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_probe_identity())

        self._assert_no_transport(channel)

    def test_forged_duck_handle_is_rejected(self) -> None:
        # An attribute look-alike must never pick a wire: only the REAL
        # SessionHandle type is trusted.
        duck = types.SimpleNamespace(
            observed=True,
            conflict="",
            wire_framing="eybond_framed",
            collector_management_adapter="framed_collector_commands",
        )
        channel = self._channel(handle_provider=lambda: duck)

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())
        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_probe_identity())

        self._assert_no_transport(channel)

    def test_unobserved_handle_with_framed_adapter_is_rejected(self) -> None:
        # observed=False (unknown wire) cannot be saved by a plausible-looking
        # adapter id: nothing was negotiated, nothing may be rebooted.
        handle = _real_handle(
            "unknown", "framed_collector_commands", state="parked_waiting_for_identity"
        )
        self.assertFalse(handle.observed)
        channel = self._channel(handle_provider=lambda: handle)

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())

        self._assert_no_transport(channel)

    def test_conflicting_handle_is_rejected(self) -> None:
        handle = _real_handle(
            "eybond_framed",
            "framed_collector_commands",
            conflict="wire_conflict:state=routed_at_text:shape=eybond_framed",
        )
        channel = self._channel(handle_provider=lambda: handle)

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())

        self._assert_no_transport(channel)

    def test_at_adapter_reboot_is_typed_unsupported_without_wire_io(self) -> None:
        # An AT-text live session: the negotiated management adapter honestly
        # reports reboot=False, so the channel raises the typed unsupported
        # error BEFORE claiming any socket or writing any byte -- no simulated
        # reboot, no invented AT command.
        handle = _real_handle("at_text", "at_commands", state="routed_at_text")
        channel = self._channel(handle_provider=lambda: handle)

        with self.assertRaises(CollectorManagementUnsupportedError):
            asyncio.run(channel.async_send_restart())

        self._assert_no_transport(channel)

    def test_unknown_adapter_handle_fails_closed(self) -> None:
        # An unobserved/unknown handle is untrusted BEFORE the adapter switch.
        handle = _real_handle("unknown", "none", state="parked_waiting_for_identity")
        channel = self._channel(handle_provider=lambda: handle)

        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())

        self._assert_no_transport(channel)

    def test_registry_returns_no_handle_for_closed_session(self) -> None:
        # A closed socket's remembered protocol_shape must never resurrect a
        # management adapter: the registry answers None, the channel fails
        # typed, and no transport is created.
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        sessions = [
            {
                "session_id": OLD_SESSION,
                "collector_pn": FULL_PN,
                "state": "closed_disconnected",
                "protocol_shape": "eybond_framed",
                "collector_identity_source": "fc2_parameter_2",
            }
        ]
        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
        registry.claim_session("owner-x", session_id=OLD_SESSION)

        self.assertIsNone(registry.session_handle_for_claimed_session("owner-x"))

        channel = self._channel(
            handle_provider=lambda: registry.session_handle_for_claimed_session(
                "owner-x"
            )
        )
        with self.assertRaises(SessionUnavailableError):
            asyncio.run(channel.async_send_restart())
        self._assert_no_transport(channel)

    def test_at_unsupported_reboot_maps_to_typed_verifier_failure(self) -> None:
        # End to end through the verifier: an AT session fails as
        # restart_not_supported, with no proof and no disconnect wait.
        handle = _real_handle("at_text", "at_commands", state="routed_at_text")
        channel = self._channel(handle_provider=lambda: handle)
        sessions = _ScriptedSessions((_strong_old(),))

        result = asyncio.run(_verifier(channel, sessions).async_verify())

        self.assertFalse(result.inbound_verified)
        self.assertIsNone(result.proof)
        self.assertEqual(result.failure_reason, FAILURE_RESTART_NOT_SUPPORTED)


class VerifierArchitectureGuardTests(unittest.TestCase):
    """The verifier stays inside its transport/management boundary."""

    _MODULE = (
        REPO_ROOT
        / "custom_components"
        / "eybond_local"
        / "onboarding"
        / "strategy_verification.py"
    )

    def _source(self) -> str:
        return self._MODULE.read_text(encoding="utf-8")

    def _names(self) -> set[str]:
        import ast

        source = self._source()
        names: set[str] = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    names.update(alias.name.split("."))
            elif isinstance(node, ast.ImportFrom):
                names.update((node.module or "").split("."))
                names.update(alias.name for alias in node.names)
        return names

    def test_no_provider_specific_or_raw_wire_dependencies(self) -> None:
        names = self._names()
        for banned in (
            # provider-specific session / raw reboot helper / wire details
            "SmartEssLocalSession",
            "smartess_local",
            "async_send_collector_reboot_or_apply",
            "build_set_collector_payload",
            "FC_SET_COLLECTOR",
            # classification sources that must never pick the wire
            "collector_kind",
            "cloud_family",
            "hostname",
            "peer_ip",
            "virtual_bridge",
            "driver_key",
        ):
            self.assertNotIn(banned, names, msg=f"banned dependency: {banned}")

    def test_reboot_goes_through_the_single_adapter_switch(self) -> None:
        names = self._names()
        self.assertIn("select_collector_management_adapter", names)
        self.assertIn("SessionPinnedIdentityReader", names)

    def test_no_module_level_inbound_timeout_constants(self) -> None:
        source = self._source()
        for gone in (
            "INBOUND_RECONNECT_TIMEOUT_SECONDS",
            "RESTART_DISCONNECT_TIMEOUT_SECONDS",
            "STRONG_IDENTITY_TIMEOUT_SECONDS",
        ):
            self.assertNotIn(gone, source, msg=f"magic constant returned: {gone}")

    def test_outcome_defines_no_proof_schema_of_its_own(self) -> None:
        # Only recovery_contract.py defines the proof schema; the verifier
        # imports it and never re-declares a proof-shaped dataclass.
        import ast

        source = self._source()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.ClassDef):
                self.assertNotIn(
                    node.name,
                    ("InboundRecoveryProof", "CallbackRecoveryProof", "RecoveryContract"),
                    msg="proof schema must live only in recovery_contract.py",
                )

    def test_verifier_returns_no_strategy_or_evidence(self) -> None:
        from custom_components.eybond_local.onboarding.strategy_verification import (
            InboundRecoveryOutcome,
        )
        from dataclasses import fields

        field_names = {field.name for field in fields(InboundRecoveryOutcome)}
        self.assertEqual(
            field_names,
            {
                "status",
                "failure_reason",
                "collector_pn",
                "new_session_id",
                "proof",
                "transitions",
            },
        )


if __name__ == "__main__":
    unittest.main()
