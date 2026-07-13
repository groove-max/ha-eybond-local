"""Validated, atomic confirmed-session-protocol evidence.

The confirmed collector wire is persisted as four related fields (protocol,
durable PN, provenance source, observed-at). Passing them around as loose
"trusted" strings after a single check in one place is fragile: a later edit can
easily read three of the four and forget the fourth, or mix a value read from
``options`` with one read from ``data``. This module makes the evidence an
immutable value object that can ONLY be constructed through a validator, so the
type itself is the guarantee that it was validated -- no "caller validated"
comments required downstream.

Validation is fail-closed: a record is accepted only when it is a COMPLETE
record read atomically from ONE mapping (never a mix of ``data`` and ``options``)
with provenance source ``live_session``, a known wire protocol, a durable PN, and
a PN whose identity matches the entry's durable PN. Cloud family / endpoint /
collector kind / driver key / peer IP can never produce it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ..const import (
    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
)
from .session_registry import pn_is_same_identity, reconcile_pn

_CONFIRMED_WIRE_PROTOCOLS = frozenset({"eybond_framed", "at_text"})


@dataclass(frozen=True, slots=True)
class ConfirmedSessionProtocolEvidence:
    """Durable, validated confirmed-live wire evidence for one collector."""

    protocol: str
    collector_pn: str
    source: str
    observed_at: str = ""

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, object] | None,
        *,
        entry_pn: object,
    ) -> "ConfirmedSessionProtocolEvidence | None":
        """Validate one COMPLETE record read atomically from a SINGLE mapping.

        Returns ``None`` unless the source is exactly ``live_session``, the
        protocol is a known confirmed wire, a durable PN is present, the entry
        has a durable PN, and the two PNs are the same short/full identity. The
        stored PN is the preferred (fuller) reconciled identity.
        """

        if not isinstance(record, Mapping):
            return None
        source = str(record.get(CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE) or "").strip().lower()
        if source != COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE:
            return None
        protocol = str(record.get(CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL) or "").strip().lower()
        if protocol not in _CONFIRMED_WIRE_PROTOCOLS:
            return None
        pn = str(record.get(CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN) or "").strip()
        if not pn:
            return None
        entry = str(entry_pn or "").strip()
        if not entry or not pn_is_same_identity(entry, pn):
            return None
        observed_at = str(
            record.get(CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT) or ""
        ).strip()
        return cls(
            protocol=protocol,
            collector_pn=reconcile_pn(entry, pn),
            source=COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
            observed_at=observed_at,
        )

    @classmethod
    def from_entry(
        cls,
        data: Mapping[str, object],
        options: Mapping[str, object],
        *,
        entry_pn: object,
    ) -> "ConfirmedSessionProtocolEvidence | None":
        """Return validated evidence from an entry, whole-record precedence.

        ``options`` is tried as a COMPLETE record first, then ``data``. A record
        is never assembled from a mix of the two mappings.
        """

        for mapping in (options, data):
            evidence = cls.from_record(mapping, entry_pn=entry_pn)
            if evidence is not None:
                return evidence
        return None

    @classmethod
    def coerce(
        cls,
        candidate: object,
        *,
        entry_pn: object,
    ) -> "ConfirmedSessionProtocolEvidence | None":
        """Re-validate an arbitrary object at a trust boundary; None if untrusted.

        This is the fail-closed gate the seed path uses. The type is NOT taken as
        a proof of validity on its own: even a genuine instance can be forged via
        the raw dataclass constructor (``source="cloud_family"``, an unknown
        protocol, an empty PN). So this:

        * rejects anything that is not a real ``ConfirmedSessionProtocolEvidence``
          (a duck-typed ``SimpleNamespace`` never passes), and
        * re-runs the FULL record validator against ``entry_pn`` -- source must be
          ``live_session``, protocol a known confirmed wire, a durable PN present,
          and the PN the same short/full identity as the entry.

        Returns a freshly validated instance (PN reconciled to the fuller
        identity) or ``None``. Never raises: an untrusted object yields no binding
        rather than a startup exception.
        """

        if not isinstance(candidate, cls):
            return None
        return cls.from_record(
            {
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: candidate.source,
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: candidate.protocol,
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: candidate.collector_pn,
                CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT: (
                    candidate.observed_at
                ),
            },
            entry_pn=entry_pn,
        )
