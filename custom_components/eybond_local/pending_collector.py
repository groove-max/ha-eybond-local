"""Pending-collector lifecycle: a collector saved before its full PN is known.

A pending entry is a real, restart-surviving config entry that is deliberately
NOT a collector runtime. It starts no coordinator, creates no devices/entities,
never writes an endpoint, and never claims a session by address. It carries only
the user's canonical ``connection_strategy`` choice (``entry.data``) plus, for
``callback_on_demand``, the address to trigger.

Two lifecycles, chosen by the canonical strategy -- never by hostname, endpoint,
collector kind, bridge kind or peer IP:

``inbound``
    Passive. Home Assistant sends NOTHING. There is no scheduler, retry loop or
    timeout: the shared listener already accepts inbound sessions, and an
    unclaimed session with a durable full PN is offered to the user as a
    *candidate* in the pending options flow. Binding is always an explicit user
    confirmation -- an unknown session is never auto-attached to a pending entry
    (with several pending entries that would be a coin toss).

``callback_on_demand``
    Exactly ONE bounded attempt per ``async_setup_entry``, through the same
    one-shot onboarding/callback path used by manual verification: snapshot the
    pre-trigger session baseline, send exactly one UDP trigger, and bounded-wait
    on the existing centralized attempt timeout. No verified durable PN ->
    ``ConfigEntryNotReady``. Retry and backoff belong entirely to Home Assistant;
    "Retry now" is just ``async_reload`` (one more setup attempt), never a loop.

Promotion is atomic and happens in place on the SAME entry (never a second
collector entry): verified full PN -> collision check -> registry handoff ->
single ``async_update_entry`` -> normal runtime. Any failure rolls the handoff
back and leaves the entry pending, unmutated.
"""

from __future__ import annotations

import logging
from typing import Any

from .connection.connection_policy import (
    is_pending_collector_entry,
    resolve_connection_strategy,
)
from .collector_identity import normalize_pn, pn_is_same_identity
from .const import (
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_STRATEGY,
    CONF_CONNECTION_STRATEGY_EVIDENCE,
    CONF_ENTRY_ROLE,
    CONF_PENDING_ADDRESS_HINT,
    CONF_PENDING_ID,
    CONF_PENDING_LAST_ATTEMPT_RESULT,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    DOMAIN,
    ENTRY_ROLE_PENDING_COLLECTOR,
    PENDING_ATTEMPT_CANDIDATE_READY,
    PENDING_ATTEMPT_WAITING_INBOUND,
)

logger = logging.getLogger(__name__)

# entry.data keys that exist ONLY while pending. All are removed at promotion.
PENDING_ONLY_FIELDS: tuple[str, ...] = (
    CONF_PENDING_ID,
    CONF_PENDING_ADDRESS_HINT,
    CONF_PENDING_LAST_ATTEMPT_RESULT,
)


class PendingPromotionError(Exception):
    """A typed pending-promotion failure (never surfaced as raw text)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def pending_entry_strategy(entry: Any) -> str:
    """Return the canonical strategy of a pending entry (data is the owner)."""

    return resolve_connection_strategy(dict(entry.data), dict(entry.options))


def is_pending_entry(entry: Any) -> bool:
    """Return whether a config entry currently holds the pending role."""

    return is_pending_collector_entry(dict(entry.data), dict(entry.options))


# --- inbound: user-confirmed candidate binding --------------------------------


def list_inbound_candidates(hass: Any) -> list[dict[str, str]]:
    """Return unclaimed sessions with a DURABLE FULL PN, for user confirmation.

    Only the public :class:`CallbackSessionRegistry` view is used. A candidate is
    offered ONLY when it has a durable full PN (strong identity): a short PN is
    transient evidence and can never identify a collector on its own. Peer IP is
    included strictly as a display hint -- it is never matched against a pending
    entry's address, so two collectors behind one NAT stay distinct and are told
    apart by PN.
    """

    from .passive_discovery import get_callback_session_registry

    registry = get_callback_session_registry(hass)
    if registry is None:
        return []
    candidates: list[dict[str, str]] = []
    seen: list[str] = []
    try:
        sessions = registry.list_unclaimed_sessions()
    except Exception:  # pragma: no cover - diagnostics must not break the flow
        logger.debug("Callback session registry unavailable", exc_info=True)
        return []
    for session in sessions:
        collector_pn = str(getattr(session, "collector_pn", "") or "").strip()
        if not collector_pn:
            continue
        if not bool(getattr(session, "has_strong_identity", False)):
            # Weak/short identity is transient evidence, never a durable identity.
            continue
        if any(pn_is_same_identity(known, collector_pn) for known in seen):
            continue
        seen.append(collector_pn)
        candidates.append(
            {
                "collector_pn": collector_pn,
                # Diagnostic hint only. NOT used to pick or match a candidate.
                "peer_ip": str(getattr(session, "peer_ip", "") or ""),
                "session_id": str(getattr(session, "session_id", "") or ""),
            }
        )
    return candidates


def pending_attempt_status(entry: Any) -> str:
    """Return the typed status of a pending entry's last attempt."""

    stored = str(entry.data.get(CONF_PENDING_LAST_ATTEMPT_RESULT) or "").strip()
    if stored:
        return stored
    if pending_entry_strategy(entry) == CONNECTION_STRATEGY_INBOUND:
        return PENDING_ATTEMPT_WAITING_INBOUND
    return PENDING_ATTEMPT_WAITING_CALLBACK


# --- promotion ----------------------------------------------------------------


def _collector_unique_id(collector_pn: str) -> str:
    return f"collector:{collector_pn}"


def find_conflicting_collector_entry(hass: Any, collector_pn: str, entry_id: str):
    """Return another config entry that already owns this durable IDENTITY.

    Identity, not string equality: a stored short PN and a freshly-read full PN
    are the SAME collector, so a plain ``unique_id ==`` compare would happily
    create a duplicate. Reconciliation is delegated to the registry's single
    implementation (``pn_is_same_identity``); addresses are never compared.
    """

    target = normalize_pn(collector_pn)
    if not target:
        return None
    for other in hass.config_entries.async_entries(DOMAIN):
        if getattr(other, "entry_id", None) == entry_id:
            continue
        candidates = [str(other.data.get(CONF_COLLECTOR_PN) or "").strip()]
        unique_id = str(getattr(other, "unique_id", "") or "")
        if unique_id.startswith("collector:"):
            candidates.append(unique_id.split(":", 1)[1])
        for candidate in candidates:
            if candidate and pn_is_same_identity(candidate, target):
                return other
    return None


def async_promote_pending_entry(
    hass: Any,
    entry: Any,
    *,
    collector_pn: str,
    evidence: str = "",
    handoff_owner: str = "",
    detected: dict[str, Any] | None = None,
) -> None:
    """Atomically turn a pending entry into a NORMAL collector entry, in place.

    Order matters and every step is fail-closed:

    1. a verified durable FULL PN is required (never a short PN, never an
       address);
    2. ``collector:{pn}`` collision with a DIFFERENT entry -> abort, the pending
       entry is left byte-for-byte unchanged;
    3. the registry ownership handoff (already prepared under the attempt owner)
       is completed by entry setup afterwards;
    4. ONE ``async_update_entry`` swaps unique_id + data together, so there is no
       half-promoted state;
    5. the caller then lets the normal runtime start (the callback path falls
       straight through to normal setup; the inbound path reloads the entry).

    Raises :class:`PendingPromotionError` with a typed reason on any failure; the
    caller rolls back the handoff and keeps the entry pending.

    ``handoff_owner`` is REQUIRED: promotion is only ever allowed on a
    registry-certified identity. The registry must publicly confirm that this
    owner holds a PREPARED handoff for this exact identity -- a caller may not
    promote an entry on a PN it merely believes in, and no private state is read
    to check it.
    """

    collector_pn = str(collector_pn or "").strip()
    if not collector_pn:
        raise PendingPromotionError("identity_not_confirmed")

    # --- trust boundary -----------------------------------------------------
    # The identity must be certified by the registry, under THIS attempt's owner.
    # The registry answers with the identity it can vouch for (session-derived),
    # and THAT is what gets persisted -- never the caller's possibly-short input.
    if not handoff_owner:
        raise PendingPromotionError("identity_not_confirmed")
    from .passive_discovery import get_callback_session_registry

    registry = get_callback_session_registry(hass)
    if registry is None:
        raise PendingPromotionError("identity_not_confirmed")
    certified_pn = registry.prepared_handoff_identity(handoff_owner, collector_pn)
    if not certified_pn:
        # The owner never claimed a concrete session for this identity, or the
        # handoff was never prepared, or the PN is a different collector.
        raise PendingPromotionError("identity_not_confirmed")
    collector_pn = certified_pn

    if find_conflicting_collector_entry(hass, collector_pn, entry.entry_id) is not None:
        # Fail closed: another entry already owns this durable identity. Never
        # create a second collector entry for one PN, and never mutate this one.
        raise PendingPromotionError("already_configured")

    data = dict(entry.data)
    # The canonical strategy the user chose is PRESERVED across promotion.
    data[CONF_COLLECTOR_PN] = collector_pn
    data[CONF_ENTRY_ROLE] = ""
    if evidence:
        data[CONF_CONNECTION_STRATEGY_EVIDENCE] = evidence
    for key in PENDING_ONLY_FIELDS:
        data.pop(key, None)
    # Verified detection metadata only -- never invented values.
    for key, value in (detected or {}).items():
        if value not in (None, ""):
            data[key] = value

    try:
        hass.config_entries.async_update_entry(
            entry,
            unique_id=_collector_unique_id(collector_pn),
            data=data,
        )
    except Exception as exc:  # pragma: no cover - HA raises on unique_id clash
        raise PendingPromotionError("already_configured") from exc

    logger.info(
        "EyeBond pending entry %s promoted to collector %s (strategy %s)",
        entry.entry_id,
        collector_pn,
        data.get(CONF_CONNECTION_STRATEGY),
    )


def release_pending_attempt_claim(hass: Any, handoff_owner: str) -> None:
    """Release ONLY this pending attempt's own transient registry claim.

    Never touches another flow's or entry's claim, and never writes an endpoint.
    """

    if not handoff_owner:
        return
    from .passive_discovery import get_callback_session_registry

    registry = get_callback_session_registry(hass)
    if registry is None:
        return
    try:
        registry.release(handoff_owner)
    except Exception:  # pragma: no cover - cleanup must never raise
        logger.debug("Pending attempt claim release failed", exc_info=True)


__all__ = [
    "PENDING_ONLY_FIELDS",
    "PendingPromotionError",
    "async_promote_pending_entry",
    "find_conflicting_collector_entry",
    "is_pending_entry",
    "list_inbound_candidates",
    "pending_attempt_status",
    "pending_entry_strategy",
    "release_pending_attempt_claim",
]
