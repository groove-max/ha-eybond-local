"""Provider-neutral write-ahead hold; NOT proof of a live interception route.

A cloud request may survive its HTTP caller. Neither endpoint readback, a
successful HTTP status, nor elapsed time establishes completed delivery. This
record has no automatic expiry. Separate exact no-request or completed-command
receipts can retire an intent through its lifecycle owner, never a bare counter
or HTTP status. A lifecycle owner must enforce it before changing the collector's
route; persisting the record alone does not protect an inverter.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import re

from ..collector_identity import validated_collector_pn

_ID = re.compile(r"[A-Za-z0-9_-]{1,96}\Z")
_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})\Z",
    re.ASCII,
)
_SOURCES = frozenset({"smartess", "dessmonitor", "valuecloud", "legacy"})
_FIELDS = frozenset({
    "schema_version", "entry_id", "study_id", "collector_pn", "source", "armed_at",
})


class CloudLearningProtectionRequired(RuntimeError):
    """A hold or untrustworthy protection record forbids direct-cloud return."""


@dataclass(frozen=True, slots=True)
class CloudLearningHold:
    """Immutable intent, written durably BEFORE the first test-control request.

    PN is the durable device scope; entry_id/study_id identify the original
    owner, not permission for a new entry or process to resume sending commands.
    Callers must supply an authoritative full PN; validation here is syntax only.
    No credentials, command values, signed URLs, endpoints or live-session claims.
    """

    entry_id: str = field(repr=False)
    study_id: str = field(repr=False)
    collector_pn: str = field(repr=False)
    source: str
    armed_at: str

    def __post_init__(self) -> None:
        for value in (self.entry_id, self.study_id):
            if type(value) is not str or not _ID.fullmatch(value):
                raise ValueError("cloud_learning_hold_owner_invalid")
        if not validated_collector_pn(self.collector_pn):
            raise ValueError("cloud_learning_hold_identity_invalid")
        if type(self.source) is not str or self.source not in _SOURCES:
            raise ValueError("cloud_learning_hold_source_invalid")
        if type(self.armed_at) is not str or not _TIMESTAMP.fullmatch(self.armed_at):
            raise ValueError("cloud_learning_hold_timestamp_invalid")
        try:
            stamp = datetime.fromisoformat(self.armed_at)
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                raise ValueError
        except ValueError:
            raise ValueError("cloud_learning_hold_timestamp_invalid") from None

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "entry_id": self.entry_id,
            "study_id": self.study_id,
            "collector_pn": self.collector_pn,
            "source": self.source,
            "armed_at": self.armed_at,
        }

    @classmethod
    def from_record(cls, raw: object) -> CloudLearningHold:
        """Do not interpret corrupt/unknown records as an absent hold."""
        if (
            type(raw) is not dict
            or set(raw) != _FIELDS
            or type(raw["schema_version"]) is not int
            or raw["schema_version"] != 1
        ):
            raise ValueError("cloud_learning_hold_record_invalid")
        return cls(**{key: value for key, value in raw.items() if key != "schema_version"})
