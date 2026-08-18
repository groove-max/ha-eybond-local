"""Provider-neutral collector identity syntax and reconciliation boundary.

Collector part numbers are protocol identifiers, not arbitrary display text.
This module owns both their strict wire-safe syntax validation and the pure
short/full comparison rules shared by discovery, recovery, runtime, and the
session ownership registry. It owns no sockets, listener inventory, claims,
handoffs, runtime state, or Home Assistant objects.
"""

from __future__ import annotations

import re

_COLLECTOR_PN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{5,63}\Z")

# One canonical prefix-match length for short/full PN reconciliation. A weak
# short PN (e.g. the heartbeat prefix) can be a prefix of the full AT+DTUPN PN;
# below this length a prefix is too ambiguous to treat as the same collector.
CALLBACK_PN_PREFIX_MATCH_MIN_LEN = 10

_STRONG_IDENTITY_SOURCES = frozenset({"at_dtupn", "fc2_parameter_2"})


def validated_collector_pn(value: object) -> str:
    """Return an exact normalized wire-safe PN, or an empty string.

    The boundary is intentionally non-coercing: binary/control-rich payloads,
    whitespace-padded values, string subclasses, and overlong identifiers are
    not collector identities.
    """

    if type(value) is not str:
        return ""
    if value != value.strip():
        return ""
    if not _COLLECTOR_PN_PATTERN.fullmatch(value):
        return ""
    return value


def identity_source_is_strong(source: object) -> bool:
    """Return whether one observation is authoritative collector identity."""

    return str(source or "").strip() in _STRONG_IDENTITY_SOURCES


def prefer_identity_source(current: object, candidate: object) -> str:
    """Keep the strongest identity evidence observed for one live session."""

    current_value = str(current or "").strip()
    candidate_value = str(candidate or "").strip()
    if identity_source_is_strong(current_value):
        return current_value
    if identity_source_is_strong(candidate_value):
        return candidate_value
    return candidate_value or current_value


def normalize_pn(value: object) -> str:
    """Return a trimmed collector PN string for compatibility parsing."""

    return str(value or "").strip()


def pn_is_same_identity(
    left: object,
    right: object,
    *,
    min_prefix_len: int = CALLBACK_PN_PREFIX_MATCH_MIN_LEN,
) -> bool:
    """Return whether two PNs denote the same durable collector identity."""

    a = normalize_pn(left)
    b = normalize_pn(right)
    if not a or not b:
        return False
    if a == b:
        return True
    if min(len(a), len(b)) < min_prefix_len:
        return False
    return a.startswith(b) or b.startswith(a)


def prefer_full_pn(left: object, right: object) -> str:
    """Return the more complete PN of two same-identity PNs (the longer one)."""

    a = normalize_pn(left)
    b = normalize_pn(right)
    if not a:
        return b
    if not b:
        return a
    return a if len(a) >= len(b) else b


def reconcile_pn(current: object, candidate: object) -> str:
    """Merge two observations into the more complete same-identity PN.

    A genuine conflict keeps ``current`` rather than silently switching
    identity. Callers that need a typed refusal must compare first with
    :func:`pn_is_same_identity`.
    """

    a = normalize_pn(current)
    b = normalize_pn(candidate)
    if not b:
        return a
    if not a:
        return b
    if a == b:
        return a
    if b.startswith(a):
        return b
    if a.startswith(b):
        return a
    return a


def reconcile_durable_pn(durable: object, observed: object) -> tuple[str, bool]:
    """Reconcile a live observation without replacing a durable identity.

    Returns ``(collector_pn, conflict)``. A short/full observation is merged
    only through the canonical minimum-prefix rule; a foreign or too-short
    prefix keeps the durable PN and reports a conflict.
    """

    durable_pn = normalize_pn(durable)
    observed_pn = normalize_pn(observed)
    if not durable_pn:
        return observed_pn, False
    if not observed_pn:
        return durable_pn, False
    if not pn_is_same_identity(durable_pn, observed_pn):
        return durable_pn, True
    return reconcile_pn(durable_pn, observed_pn), False


__all__ = [
    "CALLBACK_PN_PREFIX_MATCH_MIN_LEN",
    "identity_source_is_strong",
    "normalize_pn",
    "pn_is_same_identity",
    "prefer_full_pn",
    "prefer_identity_source",
    "reconcile_durable_pn",
    "reconcile_pn",
    "validated_collector_pn",
]
