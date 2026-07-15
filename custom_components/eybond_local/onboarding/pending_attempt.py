"""One bounded callback attempt for a pending collector entry.

This is the runtime-side twin of the manual verification the config flow already
performs, reduced to exactly what a pending entry needs. It reuses the SAME
one-shot onboarding path (``attempts=1`` -> exactly one UDP trigger, recorded in
the integration-wide callback trigger ledger), the SAME shared matcher
(:mod:`onboarding.callback_matching`) for its verdict, and the SAME session
registry handoff -- so a pending entry is not a second, parallel implementation.

Contract (deliberately narrow):

* exactly ONE UDP callback trigger per attempt -- no announcer, no loop;
* the wait is bounded by the existing centralized onboarding timeout policy;
* the verdict (baseline / strength / identity / ambiguity / trigger provenance)
  is decided ONLY by the shared matcher, never re-derived here;
* only a registry-certified strong FULL PN counts as a durable identity, and the
  registry -- not this module -- certifies it at promotion;
* peer IP is never matched, never an identity;
* on anything other than success the caller raises ConfigEntryNotReady and Home
  Assistant owns the retry/backoff. Nothing here schedules a retry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any
import uuid

from ..connection.callback_ledger import get_callback_trigger_ledger
from ..connection.models import build_connection_spec_from_values
from ..connection.session_registry import pn_is_same_identity
from ..const import (
    CONF_COLLECTOR_IP,
    CONF_DRIVER_HINT,
    CONF_PENDING_ADDRESS_HINT,
    CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
    DRIVER_HINT_AUTO,
    PENDING_ATTEMPT_CALLBACK_TIMEOUT,
    PENDING_ATTEMPT_IDENTITY_AMBIGUOUS,
    PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED,
    PENDING_ATTEMPT_PROMOTED,
    PENDING_ATTEMPT_TARGET_UNAVAILABLE,
    PENDING_ATTEMPT_TRIGGER_INTERFERENCE,
)
from .callback_matching import (
    MATCH_IDENTITY_AMBIGUOUS,
    MATCH_IDENTITY_MISMATCH,
    MATCH_TIMEOUT,
    MATCH_TRIGGER_INTERFERENCE,
    match_callback_answer,
)
from .factory import create_onboarding_manager
from .timeouts import DEFAULT_ONBOARDING_TIMEOUT_POLICY

# Map the shared matcher's typed verdicts onto the pending-entry status values
# the options flow shows. No raw exception text ever reaches the user.
_ATTEMPT_RESULT_FOR_MATCH = {
    MATCH_TIMEOUT: PENDING_ATTEMPT_CALLBACK_TIMEOUT,
    MATCH_IDENTITY_MISMATCH: PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED,
    MATCH_IDENTITY_AMBIGUOUS: PENDING_ATTEMPT_IDENTITY_AMBIGUOUS,
    MATCH_TRIGGER_INTERFERENCE: PENDING_ATTEMPT_TRIGGER_INTERFERENCE,
}

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PendingAttemptOutcome:
    """Typed result of ONE pending callback attempt."""

    result: str
    collector_pn: str = ""
    evidence: str = ""
    handoff_owner: str = ""
    detected: dict[str, Any] = field(default_factory=dict)


def _baseline_session_ids(hass: Any) -> frozenset[str]:
    """Snapshot the session ids that exist BEFORE the trigger.

    Anything already in this set is, by definition, not an answer to the trigger
    we are about to send.
    """

    from ..passive_discovery import get_callback_session_registry

    registry = get_callback_session_registry(hass)
    if registry is None:
        return frozenset()
    try:
        return frozenset(
            str(getattr(session, "session_id", "") or "")
            for session in registry.observed_sessions_per_socket()
            if str(getattr(session, "session_id", "") or "")
        )
    except Exception:  # pragma: no cover - diagnostics must not break the attempt
        logger.debug("Pending baseline snapshot failed", exc_info=True)
        return frozenset()


def _session_views(hass: Any) -> tuple[dict[str, Any], ...]:
    """Return the registry's public per-socket view as plain mappings."""

    from ..passive_discovery import get_callback_session_registry

    registry = get_callback_session_registry(hass)
    if registry is None:
        return ()
    try:
        sessions = registry.observed_sessions_per_socket()
    except Exception:  # pragma: no cover - diagnostics must not break the attempt
        logger.debug("Pending session scan failed", exc_info=True)
        return ()
    return tuple(
        {
            "session_id": str(getattr(session, "session_id", "") or ""),
            "collector_pn": str(getattr(session, "collector_pn", "") or ""),
            "state": str(getattr(session, "state", "") or ""),
            "has_strong_identity": bool(getattr(session, "has_strong_identity", False)),
        }
        for session in sessions
    )


def _detected_identity(results: Any) -> str:
    """Return the durable PN THIS attempt's own probe actually reached.

    This is the attempt's identity evidence. Without it the attempt proves
    nothing and must fail closed -- an arbitrary new strong session is NOT
    evidence that our trigger reached that collector.
    """

    for result in results or ():
        collector = getattr(result, "collector", None)
        collector_pn = str(
            getattr(getattr(collector, "collector", None), "collector_pn", "") or ""
        ).strip()
        if collector_pn:
            return collector_pn
    return ""


async def async_run_pending_callback_attempt(
    hass: Any,
    entry: Any,
) -> PendingAttemptOutcome:
    """Run exactly ONE bounded callback attempt for a pending entry."""

    target = str(
        entry.data.get(CONF_PENDING_ADDRESS_HINT)
        or entry.data.get(CONF_COLLECTOR_IP)
        or ""
    ).strip()
    if not target:
        # callback_on_demand needs somewhere to send its single trigger.
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_TARGET_UNAVAILABLE)

    from ..passive_discovery import active_callback_probe_scope

    scope_id = f"pending_callback:{entry.entry_id}:{uuid.uuid4().hex}"
    with active_callback_probe_scope(hass, scope_id) as retained_sessions:
        return await _async_run_pending_callback_attempt_scoped(
            hass,
            entry,
            target,
            retained_sessions=retained_sessions,
        )


async def _async_run_pending_callback_attempt_scoped(
    hass: Any,
    entry: Any,
    target: str,
    *,
    retained_sessions: set[str],
) -> PendingAttemptOutcome:
    """Run one pending callback attempt inside its discovery attribution scope."""

    baseline = _baseline_session_ids(hass)

    settings = dict(entry.data)
    settings[CONF_COLLECTOR_IP] = target
    try:
        detector = create_onboarding_manager(
            build_connection_spec_from_values(
                str(entry.data.get("connection_type") or "eybond"),
                settings,
            ),
            driver_hint=str(entry.data.get(CONF_DRIVER_HINT) or DRIVER_HINT_AUTO),
        )
    except Exception:
        logger.debug("Pending attempt could not build a probe", exc_info=True)
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_TARGET_UNAVAILABLE)

    policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
    ledger = get_callback_trigger_ledger()
    generation_before = ledger.snapshot_generation()
    try:
        # attempts=1 -> EXACTLY ONE UDP callback trigger, recorded in the shared
        # callback trigger ledger. The total budget is the existing centralized
        # onboarding policy; no new scheduler, no tight loop, no magic constant.
        results = await detector.async_auto_detect(
            collector_ip=target,
            discovery_target="",
            attempts=1,
            connect_timeout=policy.connect_timeout,
            heartbeat_timeout=policy.heartbeat_timeout,
            total_timeout=policy.manual_total_timeout,
        )
    except TimeoutError:
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_CALLBACK_TIMEOUT)
    except Exception:
        logger.debug("Pending callback attempt failed", exc_info=True)
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_CALLBACK_TIMEOUT)

    # The identity THIS attempt's own probe reached. It -- not inventory order --
    # is what may bind a session.
    result_pn = _detected_identity(results)
    if not result_pn:
        # The probe confirmed no durable identity: fail closed. A strong session
        # that merely appeared is not proof that OUR trigger reached it.
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED)

    match = match_callback_answer(
        _session_views(hass),
        baseline_session_ids=baseline,
        result_pn=result_pn,
        trigger_generation_before=generation_before,
        trigger_generation_after=ledger.snapshot_generation(),
        expected_own_triggers=1,
    )
    if not match.confirmed:
        return PendingAttemptOutcome(result=_ATTEMPT_RESULT_FOR_MATCH.get(
            match.result, PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED
        ))

    handoff_owner = f"pending_attempt:{uuid.uuid4().hex}"
    from ..passive_discovery import get_callback_session_registry

    registry = get_callback_session_registry(hass)
    if registry is None:  # pragma: no cover - registry exists whenever sessions do
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED)
    try:
        # Own exactly the session that answered THIS attempt, then promote the
        # claim to the durable full PN and commit it for setup's handoff.
        registry.claim_session(handoff_owner, session_id=match.session_id)
        registry.promote_claim_to_full_pn(handoff_owner, match.collector_pn)
        registry.prepare_handoff(handoff_owner, match.collector_pn)
    except ValueError as exc:
        # The identity is owned by another entry/flow: fail closed, claim nothing
        # and never disturb the existing owner.
        logger.info(
            "Pending attempt for %s: identity already owned (%s)",
            match.collector_pn,
            exc,
        )
        try:
            registry.release(handoff_owner)
        except Exception:  # pragma: no cover
            pass
        return PendingAttemptOutcome(result=PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED)

    retained_sessions.add(match.session_id)

    return PendingAttemptOutcome(
        result=PENDING_ATTEMPT_PROMOTED,
        collector_pn=match.collector_pn,
        evidence=CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
        handoff_owner=handoff_owner,
    )


def pending_attempt_matches_identity(expected_pn: str, observed_pn: str) -> bool:
    """Return whether an observed PN is the same durable identity as expected.

    Short/full reconciliation is delegated to the registry's single
    implementation; this never compares addresses.
    """

    return pn_is_same_identity(expected_pn, observed_pn)


__all__ = [
    "PendingAttemptOutcome",
    "async_run_pending_callback_attempt",
    "pending_attempt_matches_identity",
]
