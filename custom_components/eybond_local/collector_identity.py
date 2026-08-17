"""Provider-neutral collector identity syntax boundary.

Collector part numbers are protocol identifiers, not arbitrary display text.
This module deliberately validates only their wire-safe syntax; whether an
identity observation is strong or weak remains the session registry's job.
"""

from __future__ import annotations

import re

_COLLECTOR_PN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{5,63}\Z")


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


__all__ = ["validated_collector_pn"]
