"""Production assembly for one callback recovery transaction."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
from typing import Any, Callable
import uuid

from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY, OnboardingTimeoutPolicy
from .verification_channel import ObservedSessionRestartChannel
from .verification_engine import _ControlledResetRecoveryEngine
from .verification_models import (
    CallbackRecoveryRoute,
    CallbackRecoveryTriggerSender,
    FAILURE_SESSION_CLAIMED,
    RecoveryVerificationOutcome,
    STATE_INBOUND_NOT_VERIFIED,
    STATE_OBSERVED_SESSION,
    _DEFAULT_POLL_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)

def registry_sessions_projection(
    registry: Any,
) -> Callable[[], tuple[dict[str, Any], ...]]:
    """Project the registry's OWN per-socket truth into the engine's shape.

    This is the ONE public typed seam for turning registry-owned session facts
    into the recovery engine's session shape. Callers pass a
    ``CallbackSessionRegistry``; they can NOT hand the transaction a forged
    session mapping.

    The one authority: every field the engine trusts -- the strong/weak
    verdict, the certified identity source, the listener port, the raw
    negotiated wire -- comes from the same ``CallbackSessionRegistry`` that
    owns the claim. The strong/weak verdict is the registry's DERIVED
    ``CallbackSession.has_strong_identity`` (a function of the identity
    source), so even a lying ``has_strong_identity`` flag in the underlying
    listener inventory cannot survive this projection. There is deliberately
    no way to hand the public transaction a second, forged version of the
    session truth.
    """

    def _sessions() -> tuple[dict[str, Any], ...]:
        try:
            observed = registry.observed_sessions_per_socket()
        except Exception:
            logger.debug(
                "Callback recovery: registry sessions projection failed",
                exc_info=True,
            )
            return ()
        return tuple(
            {
                "session_id": session.session_id,
                "collector_pn": session.collector_pn,
                "state": session.state,
                "has_strong_identity": session.has_strong_identity,
                "collector_identity_source": session.identity_source,
                "listener_port": session.listener_port,
                "raw": dict(session.raw),
            }
            for session in observed
        )

    return _sessions


async def async_run_callback_recovery_transaction(
    *,
    registry: Any,
    collector_pn: str,
    session_id: str,
    route: CallbackRecoveryRoute,
    clock: Callable[[], str],
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    listener_host: str = "0.0.0.0",
    trigger_sender: CallbackRecoveryTriggerSender | None = None,
    ledger: Any = None,
    poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    permanent_owner_id: str | None = None,
    owner_certifier: Callable[[str], Any] | None = None,
) -> RecoveryVerificationOutcome:
    """THE public callback-recovery transaction: owns the whole claim lifecycle.

    Permanent-owner mode (Batch 8 repair): when ``permanent_owner_id`` +
    ``owner_certifier`` are given, the transaction runs UNDER that existing
    permanent owner (no transient onboarding owner, no prepared handoff), and
    a failure does NOT release the permanent claim. The outcome then carries a
    typed ``owner_certification`` instead of a ``handoff_owner``.

    Safety is structural, not contractual: this wrapper -- not a caller --
    creates the transient registry claim, wires the ownership hooks, the
    registry-only sessions projection and the negotiated-handle channel, runs
    the shared reset machine, and guarantees cleanup. There is NO caller-
    supplied sessions source: the engine observes sessions exclusively
    through the SAME registry that holds the claim, so identity strength,
    identity source, listener port and negotiated wire cannot be forged past
    the registry's own certification.

    On success the claim is already COMMITTED as a prepared handoff on the
    NEW session and the outcome carries the exact ``handoff_owner`` token
    (verify with ``registry.prepared_handoff_identity(owner, pn)``); the next
    batch's terminal flow consumes that capability directly -- persisting the
    proof into an entry is deliberately NOT done here. On any failure or
    cancellation the temporary claim is released (which also destroys a
    prepared-but-unconsumed handoff), the channel and its transports are
    stopped, and the lease/inhibitor are gone.
    """

    permanent_mode = bool(permanent_owner_id) and owner_certifier is not None
    owner = (
        str(permanent_owner_id).strip()
        if permanent_mode
        else f"callback_recovery:{uuid.uuid4().hex}"
    )
    try:
        # In permanent mode the durable claim already exists; this pins it to
        # the (already-bootstrapped) session id idempotently. In onboarding
        # mode it creates the transient claim.
        registry.claim_session(owner, session_id=session_id)
    except ValueError as exc:
        logger.info(
            "Callback recovery: session %s already claimed: %s", session_id, exc
        )
        outcome = RecoveryVerificationOutcome(
            status=STATE_INBOUND_NOT_VERIFIED,
            failure_reason=FAILURE_SESSION_CLAIMED,
            collector_pn=str(collector_pn or "").strip(),
            transitions=(STATE_OBSERVED_SESSION, STATE_INBOUND_NOT_VERIFIED),
        )
        return outcome

    def _claimed_session_id() -> str:
        try:
            return str(registry.claimed_session_id(owner) or "").strip()
        except Exception:
            return ""

    def _promote(full_pn: str) -> None:
        registry.promote_claim_to_full_pn(owner, full_pn)

    def _retarget(new_session_id: str) -> bool:
        try:
            if registry.claimed_session_id(owner) == new_session_id:
                return True
            return bool(
                registry.retarget_claim_to_reconnected_session(owner, new_session_id)
            )
        except ValueError:
            return False

    def _prepare(full_pn: str) -> str:
        # Commit THIS transaction's claim as a prepared handoff; the returned
        # token is the capability the outcome hands to the next batch.
        try:
            return owner if registry.prepare_handoff(owner, full_pn) else ""
        except ValueError as exc:
            logger.info(
                "Callback recovery: handoff for %s not preparable: %s",
                full_pn,
                exc,
            )
            return ""

    channel = ObservedSessionRestartChannel(
        host=listener_host,
        port=route.listener_port,
        collector_pn="",
        session_id=session_id,
        session_id_provider=_claimed_session_id,
        handle_provider=lambda: registry.session_handle_for_claimed_session(owner),
    )
    from ...collector.silent_session_probe import SilentSessionIdentityProbeChannel

    silent_probe = SilentSessionIdentityProbeChannel(
        host=listener_host, port=route.listener_port
    )
    await silent_probe.async_open()

    async def _probe_reconnected_identity(_new_session_id: str) -> str:
        # The engine already retargeted the claim onto the candidate; the
        # channel's providers resolve exactly that socket on its live wire.
        return await channel.async_probe_identity()

    engine = _ControlledResetRecoveryEngine(
        collector_pn=collector_pn,
        session_id=session_id,
        restart_channel=channel,
        sessions_source=registry_sessions_projection(registry),
        clock=clock,
        policy=policy,
        callback_route=route,
        trigger_sender=trigger_sender,
        promote_claim=_promote,
        retarget_claim=_retarget,
        prepare_handoff=None if permanent_mode else _prepare,
        owner_certifier=owner_certifier if permanent_mode else None,
        probe_reconnected_identity=_probe_reconnected_identity,
        silent_session_probe=silent_probe,
        ledger=ledger,
        poll_interval=poll_interval,
    )
    succeeded = False
    try:
        outcome = await engine.async_verify()
        succeeded = outcome.callback_verified or outcome.inbound_recovered
        return outcome
    finally:
        with suppress(Exception):
            await silent_probe.async_close()
        with suppress(Exception):
            await channel.async_close()
        # A permanent owner keeps its durable claim across a failed repair;
        # only a transient onboarding owner is released here.
        if not succeeded and not permanent_mode:
            with suppress(Exception):
                registry.release(owner)
