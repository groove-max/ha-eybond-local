"""Coordinator-independent degraded strategy-transition repair (Batch 8B.1).

After a confirmed endpoint restore whose callback strategy was never proven,
the entry holds a typed :class:`StrategyTransitionRecoveryState`. This module
finishes that transition from a COLD start -- no loaded coordinator, no live
collector session -- through ONE public, causally-isolated Phase-A bootstrap
transaction plus the existing Phase-B recovery proof.

Two deliberately-separate logical ``set>server`` sequences (never merged into
one causality window):

* Phase A -- :func:`async_run_callback_bootstrap_transaction`: establish durable
  ownership intent, then EITHER re-certify an already-live trusted owned session
  (no trigger), OR run ONE causal window (baseline -> exactly one
  ``set>server`` -> authoritative exact-session identity read on the negotiated
  wire -> the SHARED :func:`match_callback_answer` -> a guarded permanent-owner
  pin + certification). A foreign trigger, an ambiguous or foreign session, a
  read/matcher disagreement or a missing wire are refused with a typed outcome
  and ZERO ownership mutation. The permanent claim is pinned ONLY after a strong
  identity is proven -- never speculatively on a weak/foreign candidate.
* Phase B -- the full callback recovery proof
  (:func:`async_run_callback_recovery_transaction`). Only its success commits
  ``connection_strategy=callback_on_demand`` + the RecoveryContract and clears
  the recovery state, in one reload.

The whole repair runs under the SAME per-entry ``STRATEGY_TRANSITION_LEASES`` as
a normal strategy transition, so a repair and a live transition of one entry can
never run at once. All listener/transport/trigger/projection/wire I/O is owned
by the public :class:`CallbackBootstrapChannel`; this module never touches a
listener internal, never picks a wire, and never re-implements the matcher.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from ..const import (
    CONF_CONNECTION_STRATEGY,
    CONF_ENDPOINT_CONTROL_POLICY,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    ENDPOINT_CONTROL_EXTERNAL,
)
from .callback_matching import (
    MATCH_IDENTITY_AMBIGUOUS,
    MATCH_IDENTITY_MISMATCH,
    MATCH_TRIGGER_INTERFERENCE,
    match_callback_answer,
)
from .recovery.terminal import RecoveryTerminalInput
from .recovery.verification import async_run_callback_recovery_transaction
from ..timeout_policy import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    OnboardingTimeoutPolicy,
)
from .callback_ledger import CallbackCausalityBusyError
from ..collector_identity import pn_is_same_identity
from .strategy_transition import STRATEGY_REPAIR_LEASES, TRANSITION_ALREADY_RUNNING
from .strategy_transition_recovery import StrategyTransitionRecoveryState
from .strategy_transition_recovery import (
    RECOVERY_PHASE_PENDING,
    RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
)
from .strategy_transition_context import CloudRollbackEndpoint

logger = logging.getLogger(__name__)

# Orchestration-level preflight reasons (before Phase A even starts).
REPAIR_STATE_INVALID = "repair_state_invalid"
REPAIR_ROUTE_INCOMPLETE = "repair_route_incomplete"
REPAIR_ROLLBACK_ENDPOINT_UNAVAILABLE = "transition_rollback_endpoint_unavailable"
REPAIR_CONFIRMED_PERSIST_UNAVAILABLE = "transition_persist_confirmed_unavailable"

# Typed Phase-A bootstrap outcomes (translation keys; never raw exception text).
BOOTSTRAP_EXISTING_OWNER_CERTIFIED = "existing_owner_certified"
BOOTSTRAP_CERTIFIED = "bootstrap_certified"
BOOTSTRAP_NO_SESSION = "no_session"
BOOTSTRAP_AMBIGUOUS = "ambiguous"
BOOTSTRAP_IDENTITY_MISMATCH = "identity_mismatch"
BOOTSTRAP_INTERFERENCE = "interference"
BOOTSTRAP_TRIGGER_NOT_SENT = "trigger_not_sent"
BOOTSTRAP_WIRE_UNAVAILABLE = "wire_unavailable"
BOOTSTRAP_CLAIMED_BY_OTHER = "claimed_by_other"
BOOTSTRAP_OWNERSHIP_UNAVAILABLE = "ownership_unavailable"
BOOTSTRAP_LISTENER_UNAVAILABLE = "listener_unavailable"
BOOTSTRAP_CAUSALITY_BUSY = "causality_busy"

_CERTIFIED_KINDS = frozenset(
    {BOOTSTRAP_EXISTING_OWNER_CERTIFIED, BOOTSTRAP_CERTIFIED}
)


@dataclass(frozen=True, slots=True)
class PhaseABootstrapOutcome:
    """The strict typed result of ONE Phase-A bootstrap transaction.

    A success carries the literal ``PermanentOwnedSessionCertification`` for the
    exact ``(owner, session, PN)``; a failure never carries a certification.
    """

    kind: str
    certification: Any = None
    session_id: str = ""

    @property
    def certified(self) -> bool:
        return self.kind in _CERTIFIED_KINDS and self.certification is not None


@dataclass(frozen=True, slots=True)
class DegradedRepairResult:
    success: bool
    failure_reason: str = ""
    phase: str = ""  # "bootstrap" | "proof" | ""
    outcome: Any = None
    bootstrap: PhaseABootstrapOutcome | None = None


# ---------------------------------------------------------------------------
# Phase A -- the ONE public, causally-isolated bootstrap transaction.
# ---------------------------------------------------------------------------
def _ownership_failure(exc: Exception) -> PhaseABootstrapOutcome:
    """Map a registry ownership ValueError to a typed outcome (no mutation)."""

    if str(exc).startswith("session_already_claimed"):
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_CLAIMED_BY_OTHER)
    return PhaseABootstrapOutcome(kind=BOOTSTRAP_OWNERSHIP_UNAVAILABLE)


def _existing_live_owner_certification(
    registry: Any, owner_id: str, collector_pn: str
) -> Any:
    """Certify an ALREADY-live, strong, trusted owned session, or ``None``.

    A stale / closed / conflicting / non-strong / foreign session yields
    ``None`` so the cold bootstrap still runs -- an existing session is never
    passed off as the result of a fresh trigger.
    """

    sid = str(registry.claimed_session_id(owner_id) or "").strip()
    if not sid:
        return None
    handle = registry.session_handle_for_owned_session(owner_id, sid)
    if handle is None or not handle.observed or handle.conflict:
        return None
    if not pn_is_same_identity(handle.collector_pn, collector_pn):
        return None
    # certify_permanent_owned_session re-checks strong identity + liveness +
    # ownership; None means "not certifiable", so no shortcut.
    return registry.certify_permanent_owned_session(owner_id, sid)


def _is_new_socket(
    session: dict[str, Any], baseline_ids: frozenset[str], listener_port: int
) -> bool:
    sid = str(session.get("session_id") or "").strip()
    if not sid or sid in baseline_ids:
        return False
    if str(session.get("state") or "").startswith("closed"):
        return False
    if int(session.get("listener_port") or 0) != int(listener_port):
        return False
    return True


async def _wait_and_read(
    *,
    channel: Any,
    state: StrategyTransitionRecoveryState,
    collector_pn: str,
    baseline_ids: frozenset[str],
    poll_interval: float,
    policy: OnboardingTimeoutPolicy,
) -> tuple[str, str, bool]:
    """Poll for the new socket and take ONE authoritative identity read.

    Returns ``(result_pn, read_session_id, saw_unreadable)``. ``result_pn`` is
    the identity the exact-session read produced (the matcher's evidence) and
    ``read_session_id`` the socket it was read from. ``saw_unreadable`` records
    that a fresh candidate existed but no trusted wire authority could read it,
    so the transaction can fail closed with ``wire_unavailable`` rather than a
    generic timeout.
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(1.0, float(policy.callback_recovery_session_wait))
    listener_port = int(state.local_listener_port)
    same_pn: tuple[str, str] = ("", "")  # (pn, session_id) for our identity
    other_pn: tuple[str, str] = ("", "")  # first foreign read (matcher rejects it)
    saw_unreadable = False
    probed: set[str] = set()
    while True:
        for session in channel.sessions():
            sid = str(session.get("session_id") or "").strip()
            if not _is_new_socket(session, baseline_ids, listener_port):
                continue
            if sid in probed:
                continue
            probed.add(sid)
            # The AUTHORITATIVE read is always done -- it is the matcher's
            # ``result_pn`` evidence. A candidate with no trusted wire is
            # recorded so the honest ``wire_unavailable`` reason can surface.
            read = await channel.async_read_exact_session_identity(session)
            if not read.wire_available:
                saw_unreadable = True
                continue
            pn = str(read.collector_pn or "").strip()
            if not pn:
                continue
            if pn_is_same_identity(pn, collector_pn):
                same_pn = (pn, sid)
            elif not other_pn[0]:
                other_pn = (pn, sid)
        if same_pn[0]:
            break
        if loop.time() >= deadline:
            break
        await asyncio.sleep(poll_interval)
    result_pn, read_sid = same_pn if same_pn[0] else other_pn
    return result_pn, read_sid, saw_unreadable


def _map_match_failure(result: str, *, saw_unreadable: bool) -> PhaseABootstrapOutcome:
    if result == MATCH_TRIGGER_INTERFERENCE:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_INTERFERENCE)
    if result == MATCH_IDENTITY_AMBIGUOUS:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_AMBIGUOUS)
    if result == MATCH_IDENTITY_MISMATCH:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_IDENTITY_MISMATCH)
    # MATCH_TIMEOUT: nothing fresh confirmed. If a candidate arrived but no
    # trusted wire could read it, that is the honest, differentiated reason.
    if saw_unreadable:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_WIRE_UNAVAILABLE)
    return PhaseABootstrapOutcome(kind=BOOTSTRAP_NO_SESSION)


async def _cold_bootstrap(
    *,
    registry: Any,
    owner_id: str,
    state: StrategyTransitionRecoveryState,
    route: Any,
    channel: Any,
    collector_pn: str,
    attempt: Any,
    poll_interval: float,
    policy: OnboardingTimeoutPolicy,
) -> PhaseABootstrapOutcome:
    """The body inside the held causality lease. Sends exactly one trigger."""

    baseline_ids = frozenset(
        sid
        for session in channel.sessions()
        if (sid := str(session.get("session_id") or "").strip())
    )
    old_sid = str(registry.claimed_session_id(owner_id) or "").strip()

    try:
        await channel.async_send_trigger(route)  # exactly ONE logical set>server
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # own_sends check below turns this into a typed fail
        logger.info("Degraded repair bootstrap trigger could not be sent: %s", exc)
    if attempt.own_sends != 1:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_TRIGGER_NOT_SENT)

    result_pn, read_sid, saw_unreadable = await _wait_and_read(
        channel=channel,
        state=state,
        collector_pn=collector_pn,
        baseline_ids=baseline_ids,
        poll_interval=poll_interval,
        policy=policy,
    )

    # A foreign trigger inside our window makes any appearing session
    # unattributable -- refuse before touching ownership (the matcher re-checks).
    if attempt.foreign_sends:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_INTERFERENCE)

    match = match_callback_answer(
        channel.sessions(),
        baseline_session_ids=baseline_ids,
        result_pn=result_pn,
        expected_pn=collector_pn,
        old_session_id=old_sid,
        trigger_generation_before=0,
        trigger_generation_after=attempt.observed_sends,
        expected_own_triggers=1,
    )
    if not match.confirmed:
        return _map_match_failure(match.result, saw_unreadable=saw_unreadable)
    # Our authoritative read and the matcher's verdict must name the SAME socket.
    if read_sid and match.session_id != read_sid:
        logger.info(
            "Degraded repair read %s but matcher chose %s", read_sid, match.session_id
        )
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_IDENTITY_MISMATCH)

    bootstrap_sid = match.session_id
    # Permanent-claim mutation ONLY now: strong identity proven + matcher agreed.
    # ``retarget_claim_to_reconnected_session`` is the guarded op -- it refuses a
    # live-session replacement, a foreign identity and another owner, and never
    # changes the durable PN, leaving no mutation on refusal.
    try:
        pinned = registry.retarget_claim_to_reconnected_session(owner_id, bootstrap_sid)
    except ValueError as exc:
        return _ownership_failure(exc)
    if not pinned:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_IDENTITY_MISMATCH)
    cert = registry.certify_permanent_owned_session(owner_id, bootstrap_sid)
    if cert is None:
        return PhaseABootstrapOutcome(kind=BOOTSTRAP_IDENTITY_MISMATCH)
    return PhaseABootstrapOutcome(
        kind=BOOTSTRAP_CERTIFIED, certification=cert, session_id=bootstrap_sid
    )


async def async_run_callback_bootstrap_transaction(
    *,
    registry: Any,
    owner_id: str,
    state: StrategyTransitionRecoveryState,
    route: Any,
    channel: Any,
    policy: OnboardingTimeoutPolicy | None = None,
    poll_interval: float = 0.2,
) -> PhaseABootstrapOutcome:
    """Establish permanent ownership of the callback session, or fail typed.

    An IDENTITY-ONLY ownership intent is recorded BEFORE any listener probe / UDP
    (a cold repair must not assume a coordinator pre-created the claim). That
    intent -- ``registry.claim_identity`` -- is itself an ownership mutation, but
    it binds NO socket: a socket binding/retarget is forbidden until AFTER the
    proof. An already-live owned session that the registry can strong-certify
    short-circuits with a re-certification and NO trigger; a bare same-PN socket
    next to an identity-only claim is NOT silently bound. The cold path runs one
    causal window and pins the permanent claim only after a strong identity is
    proven through the shared matcher.

    The causality authority is ``channel.ledger`` -- the exact ledger the
    channel's sender records through -- so the lease and the send never diverge.
    """

    if policy is None:
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
    collector_pn = state.collector_pn

    # 1. Identity-only durable ownership intent BEFORE any listener probe / UDP.
    #    It records durable PN ownership and NOTHING about a socket, so it can
    #    never auto-bind a live baseline socket. Any conflict resolves HERE.
    try:
        registry.claim_identity(owner_id, collector_pn)
    except ValueError as exc:
        return _ownership_failure(exc)

    # 2. Existing-live-owner shortcut: ONLY an already explicitly session-bound
    #    claim the registry can strong-certify (registry-only, no listener, no
    #    trigger). An identity-only claim has no session id, so this never fires
    #    off a bare nearby socket.
    existing = _existing_live_owner_certification(registry, owner_id, collector_pn)
    if existing is not None:
        return PhaseABootstrapOutcome(
            kind=BOOTSTRAP_EXISTING_OWNER_CERTIFIED,
            certification=existing,
            session_id=getattr(existing, "session_id", ""),
        )

    # 3. Cold path: ONE listener lifecycle covering EVERY outcome (unavailable,
    #    busy, matcher failure, exception, cancellation, success).
    ledger = channel.ledger
    attempt_key = f"degraded_repair:{owner_id}:{uuid.uuid4().hex}"
    try:
        await channel.async_open()
        if not channel.listener_available:
            return PhaseABootstrapOutcome(kind=BOOTSTRAP_LISTENER_UNAVAILABLE)
        try:
            async with ledger.causality_lease(
                attempt_key, timeout=policy.callback_causality_lease_wait
            ) as attempt:
                return await _cold_bootstrap(
                    registry=registry,
                    owner_id=owner_id,
                    state=state,
                    route=route,
                    channel=channel,
                    collector_pn=collector_pn,
                    attempt=attempt,
                    poll_interval=poll_interval,
                    policy=policy,
                )
        except CallbackCausalityBusyError:
            return PhaseABootstrapOutcome(kind=BOOTSTRAP_CAUSALITY_BUSY)
    finally:
        await channel.async_close()


# ---------------------------------------------------------------------------
# The full repair: ONE strategy lease over Phase A (bootstrap) + Phase B (proof).
# ---------------------------------------------------------------------------
async def async_run_degraded_recovery_repair(
    *,
    registry: Any,
    owner_id: str,
    state: StrategyTransitionRecoveryState,
    channel: Any,
    commit: Callable[[dict[str, Any], RecoveryTerminalInput], Awaitable[str]],
    clock: Callable[[], str],
    policy: OnboardingTimeoutPolicy | None = None,
    poll_interval: float = 0.2,
    strategy_leases: Any = None,
    pending_restore_endpoint: CloudRollbackEndpoint | None = None,
    persist_restore_confirmed: (
        Callable[[StrategyTransitionRecoveryState], Any] | None
    ) = None,
) -> DegradedRepairResult:
    """Finish the degraded transition from a cold start. Single-owner path.

    Runs under the SAME per-entry ``STRATEGY_TRANSITION_LEASES`` as a normal
    transition, acquired synchronously and released in an immediate try/finally,
    so a repair and a live transition of one entry are mutually exclusive
    (``transition_already_running``) while different entries stay independent.
    """

    if policy is None:
        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
    if strategy_leases is None:
        strategy_leases = STRATEGY_REPAIR_LEASES
    if type(state) is not StrategyTransitionRecoveryState:
        return DegradedRepairResult(success=False, failure_reason=REPAIR_STATE_INVALID)
    route = state.callback_route()
    if route is None or route.invalid_reason():
        # A marker/state without a usable route is fail-closed: NO trigger.
        return DegradedRepairResult(
            success=False, failure_reason=REPAIR_ROUTE_INCOMPLETE
        )

    # ``transition_pending`` is write-ahead intent, NOT evidence that the cloud
    # endpoint was restored.  It must finish that exact physical action through
    # the certified Phase-A session and persist the confirmed phase at the
    # management receipt boundary.  Treating pending as confirmed makes the
    # subsequent callback proof reboot a collector that still points at HA; it
    # then autonomously reconnects and the callback window can never be causal.
    if state.phase == RECOVERY_PHASE_PENDING:
        if (
            type(pending_restore_endpoint) is not CloudRollbackEndpoint
            or not pending_restore_endpoint.known
        ):
            return DegradedRepairResult(
                success=False,
                failure_reason=REPAIR_ROLLBACK_ENDPOINT_UNAVAILABLE,
            )
        if persist_restore_confirmed is None:
            return DegradedRepairResult(
                success=False,
                failure_reason=REPAIR_CONFIRMED_PERSIST_UNAVAILABLE,
            )
    elif state.phase == RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN:
        if pending_restore_endpoint is not None or persist_restore_confirmed is not None:
            return DegradedRepairResult(
                success=False,
                failure_reason=REPAIR_STATE_INVALID,
            )
    else:  # defensive behind the strict recovery-state constructor
        return DegradedRepairResult(success=False, failure_reason=REPAIR_STATE_INVALID)

    if not strategy_leases.acquire(owner_id):
        return DegradedRepairResult(
            success=False, failure_reason=TRANSITION_ALREADY_RUNNING
        )
    try:
        # ---- Phase A: the public bootstrap transaction -------------------
        bootstrap = await async_run_callback_bootstrap_transaction(
            registry=registry,
            owner_id=owner_id,
            state=state,
            route=route,
            channel=channel,
            policy=policy,
            poll_interval=poll_interval,
        )
        if not bootstrap.certified:
            return DegradedRepairResult(
                success=False,
                failure_reason=bootstrap.kind,
                phase="bootstrap",
                bootstrap=bootstrap,
            )

        # ---- Phase B: the full callback recovery proof -------------------
        def _owner_certifier(_full_pn: str) -> Any:
            current_sid = str(registry.claimed_session_id(owner_id) or "")
            return registry.certify_permanent_owned_session(owner_id, current_sid)

        management_endpoint = ""
        on_management_confirmed = None
        if state.phase == RECOVERY_PHASE_PENDING:
            assert type(pending_restore_endpoint) is CloudRollbackEndpoint
            assert persist_restore_confirmed is not None
            management_endpoint = pending_restore_endpoint.endpoint

            def _persist_confirmed(_receipt: Any) -> None:
                confirmed = state.with_phase(
                    RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
                    now=clock(),
                )
                persisted = persist_restore_confirmed(confirmed)
                if inspect.isawaitable(persisted):
                    raise RuntimeError("repair_confirmed_persist_must_be_sync")
                if persisted:
                    raise RuntimeError(str(persisted))

            on_management_confirmed = _persist_confirmed

        outcome = await async_run_callback_recovery_transaction(
            registry=registry,
            collector_pn=state.collector_pn,
            session_id=bootstrap.session_id,
            route=route,
            clock=clock,
            policy=policy,
            listener_host=state.listener_bind_host,
            trigger_sender=channel.trigger_sender,
            ledger=channel.ledger,
            poll_interval=poll_interval,
            permanent_owner_id=owner_id,
            owner_certifier=_owner_certifier,
            management_endpoint=management_endpoint,
            on_management_confirmed=on_management_confirmed,
        )
        if not outcome.callback_verified:
            return DegradedRepairResult(
                success=False,
                failure_reason=str(outcome.failure_reason or "")
                or "callback_recovery_timeout",
                phase="proof",
                outcome=outcome,
                bootstrap=bootstrap,
            )
        if not registry.reverify_permanent_owned_session(outcome.owner_certification):
            return DegradedRepairResult(
                success=False,
                failure_reason="transition_owner_certification_stale",
                phase="proof",
                outcome=outcome,
                bootstrap=bootstrap,
            )

        terminal = RecoveryTerminalInput.from_permanent_owner_transaction(outcome)
        refusal = await commit(
            {
                CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                CONF_ENDPOINT_CONTROL_POLICY: ENDPOINT_CONTROL_EXTERNAL,
            },
            terminal,
        )
        if refusal:
            return DegradedRepairResult(
                success=False,
                failure_reason=refusal,
                phase="proof",
                outcome=outcome,
                bootstrap=bootstrap,
            )
        return DegradedRepairResult(
            success=True, phase="proof", outcome=outcome, bootstrap=bootstrap
        )
    finally:
        strategy_leases.release(owner_id)


__all__ = [
    "BOOTSTRAP_AMBIGUOUS",
    "BOOTSTRAP_CAUSALITY_BUSY",
    "BOOTSTRAP_CERTIFIED",
    "BOOTSTRAP_CLAIMED_BY_OTHER",
    "BOOTSTRAP_EXISTING_OWNER_CERTIFIED",
    "BOOTSTRAP_IDENTITY_MISMATCH",
    "BOOTSTRAP_INTERFERENCE",
    "BOOTSTRAP_LISTENER_UNAVAILABLE",
    "BOOTSTRAP_NO_SESSION",
    "BOOTSTRAP_OWNERSHIP_UNAVAILABLE",
    "BOOTSTRAP_TRIGGER_NOT_SENT",
    "BOOTSTRAP_WIRE_UNAVAILABLE",
    "DegradedRepairResult",
    "PhaseABootstrapOutcome",
    "REPAIR_ROUTE_INCOMPLETE",
    "REPAIR_ROLLBACK_ENDPOINT_UNAVAILABLE",
    "REPAIR_CONFIRMED_PERSIST_UNAVAILABLE",
    "REPAIR_STATE_INVALID",
    "async_run_callback_bootstrap_transaction",
    "async_run_degraded_recovery_repair",
]
