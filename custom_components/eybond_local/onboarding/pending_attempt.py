"""One bounded callback attempt for a pending collector entry.

This is now a THIN compatibility caller of the shared identity transaction
(:mod:`onboarding.callback_identity`): it maps the pending entry's stored
settings onto a transaction request and maps the transaction's typed outcome
onto the pending status values the options flow shows. It owns no part of the
proof itself.

It used to run ``detector.async_auto_detect`` -- the full driver sweep -- purely
to learn which PN answered, before any identity was confirmed. That cost tens of
seconds per attempt, outlived the session it was identifying, and left the entry
looping on callback_timeout. A pending entry needs the collector's identity and
nothing else: it starts no runtime and picks no driver, so no detection belongs
here at all.

Contract (unchanged for callers):

* exactly ONE UDP callback trigger per attempt -- no announcer, no loop;
* the wait is bounded; nothing here schedules a retry;
* only a registry-certified strong FULL PN counts as a durable identity;
* peer IP is never matched, never an identity;
* on anything other than success the caller raises ConfigEntryNotReady and Home
  Assistant owns the retry/backoff.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any

from ..connection.session_registry import pn_is_same_identity
from ..const import (
    CONF_COLLECTOR_IP,
    CONF_PENDING_ADDRESS_HINT,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
    PENDING_ATTEMPT_CALLBACK_TIMEOUT,
    PENDING_ATTEMPT_IDENTITY_AMBIGUOUS,
    PENDING_ATTEMPT_IDENTITY_CLAIMED_BY_OTHER,
    PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED,
    PENDING_ATTEMPT_IDENTITY_UNVERIFIED,
    PENDING_ATTEMPT_PROMOTED,
    PENDING_ATTEMPT_TARGET_UNAVAILABLE,
    PENDING_ATTEMPT_TRIGGER_INTERFERENCE,
    PENDING_ATTEMPT_TRIGGER_NOT_SENT,
)
from .callback_identity import (
    CallbackIdentityRequest,
    IDENTITY_AMBIGUOUS,
    IDENTITY_CONFLICT,
    IDENTITY_MISMATCH,
    IDENTITY_TARGET_REQUIRED,
    IDENTITY_TIMEOUT,
    IDENTITY_TRIGGER_INTERFERENCE,
    IDENTITY_TRIGGER_NOT_SENT,
    IDENTITY_UNVERIFIED,
    async_run_callback_identity_transaction,
)

# Map the transaction's typed outcomes onto the pending-entry status values the
# options flow shows. No raw exception text ever reaches the user.
#
# callback_trigger_not_sent is deliberately NOT folded into interference: the
# pending card must not blame a phantom competing flow for our own failure to
# get a datagram out.
# Six DISTINCT failures, six distinct statuses. Collapsing them was actively
# misleading: "another attempt interfered" sent users hunting a competing flow
# that did not exist, and "not confirmed" hid whether the collector was silent,
# unreadable, or already owned by another entry.
_ATTEMPT_RESULT_FOR_IDENTITY = {
    # our trigger sequence never went out (inhibited window, socket error, or a
    # competing attempt holding the causality lease)
    IDENTITY_TRIGGER_NOT_SENT: PENDING_ATTEMPT_TRIGGER_NOT_SENT,
    # the sequence went out; nothing dialed in
    IDENTITY_TIMEOUT: PENDING_ATTEMPT_CALLBACK_TIMEOUT,
    # a competing trigger made the answer unattributable
    IDENTITY_TRIGGER_INTERFERENCE: PENDING_ATTEMPT_TRIGGER_INTERFERENCE,
    # it connected, but its identity could not be read
    IDENTITY_UNVERIFIED: PENDING_ATTEMPT_IDENTITY_UNVERIFIED,
    # it identified as a different collector than expected
    IDENTITY_MISMATCH: PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED,
    # more than one collector answered
    IDENTITY_AMBIGUOUS: PENDING_ATTEMPT_IDENTITY_AMBIGUOUS,
    # the identity belongs to another entry/flow
    IDENTITY_CONFLICT: PENDING_ATTEMPT_IDENTITY_CLAIMED_BY_OTHER,
    # callback_on_demand with nowhere to send
    IDENTITY_TARGET_REQUIRED: PENDING_ATTEMPT_TARGET_UNAVAILABLE,
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


def _pending_entry_strategy(entry: Any) -> str:
    """The entry's CANONICAL strategy. It decides whether we may dial out at all.

    Imported lazily: pending_collector imports this module's outcome type, so a
    module-level import would be circular.
    """

    from ..pending_collector import pending_entry_strategy

    return pending_entry_strategy(entry)


async def async_run_callback_identity_transaction_for_entry(hass: Any, entry: Any):
    """Build ONE transaction request from a pending entry's stored settings."""

    target = str(
        entry.data.get(CONF_PENDING_ADDRESS_HINT)
        or entry.data.get(CONF_COLLECTOR_IP)
        or ""
    ).strip()
    request = CallbackIdentityRequest(
        server_ip=str(entry.data.get(CONF_SERVER_IP) or ""),
        tcp_port=int(entry.data.get(CONF_TCP_PORT) or 0),
        udp_port=int(entry.data.get(CONF_UDP_PORT) or 0),
        target_ip=target,
        strategy=_pending_entry_strategy(entry),
        owner_prefix="pending_attempt",
    )
    return await async_run_callback_identity_transaction(hass, request)


async def async_run_pending_callback_attempt(
    hass: Any,
    entry: Any,
) -> PendingAttemptOutcome:
    """Run exactly ONE bounded callback attempt for a pending entry.

    Identity only: the shared transaction establishes the link and proves which
    collector answered. No driver detection runs here -- a pending entry has no
    runtime to configure, and detection is what used to burn the session before
    the identity was ever confirmed.
    """

    outcome = await async_run_callback_identity_transaction_for_entry(hass, entry)
    if not outcome.confirmed:
        return PendingAttemptOutcome(
            result=_ATTEMPT_RESULT_FOR_IDENTITY.get(
                outcome.result, PENDING_ATTEMPT_IDENTITY_NOT_CONFIRMED
            )
        )
    return PendingAttemptOutcome(
        result=PENDING_ATTEMPT_PROMOTED,
        collector_pn=outcome.collector_pn,
        evidence=CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER,
        handoff_owner=outcome.handoff_owner,
    )


def pending_attempt_matches_identity(expected_pn: str, observed_pn: str) -> bool:
    """Return whether an observed PN is the same durable identity as expected.

    Short/full reconciliation is delegated to the registry's single
    implementation; this never compares addresses.
    """

    return pn_is_same_identity(expected_pn, observed_pn)


__all__ = [
    "PendingAttemptOutcome",
    "async_run_callback_identity_transaction_for_entry",
    "async_run_pending_callback_attempt",
    "pending_attempt_matches_identity",
]
