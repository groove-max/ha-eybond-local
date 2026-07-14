"""Normalized outcome of one collector-metadata channel read.

Leaf module (no collector-layer imports) so the wire readers
(``at_runtime`` / ``parameter_registry``) and the higher-level
``collector.metadata`` channels can all share one result type without an import
cycle. A channel read reports not just the decoded values but WHY it looked the
way it did, so the runtime service can decide cache/fresh/strike honestly and
diagnostics can show the real outcome instead of a bare ``{}``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# A read either produced usable metadata, produced none for a benign reason, or
# failed to talk to the collector. These are the ONLY outcomes; the service maps
# them to cache/fresh/strike decisions.
OUTCOME_SUCCESS = "success"          # commands delivered, metadata obtained
OUTCOME_PARTIAL = "partial"          # some metadata obtained, then truncated (still fresh)
OUTCOME_EMPTY = "empty"              # commands delivered, link alive, zero metadata
OUTCOME_TRANSPORT_ERROR = "transport_error"  # delivery failure (timeout/disconnect/OSError)
OUTCOME_COMMAND_ERROR = "command_error"      # malformed response / parse failure

_FRESH_OUTCOMES = frozenset({OUTCOME_SUCCESS, OUTCOME_PARTIAL})
_DELIVERY_FAILURE_OUTCOMES = frozenset({OUTCOME_TRANSPORT_ERROR, OUTCOME_COMMAND_ERROR})


@dataclass(frozen=True)
class CollectorMetadataChannelReadResult:
    """One channel read: its values and an honest, non-sensitive outcome.

    ``safe_error_code`` is a typed code only (an exception class name or a short
    reason slug) -- never a raw response/value, which could leak an endpoint or
    credential.
    """

    values: dict[str, object] = field(default_factory=dict)
    outcome: str = OUTCOME_EMPTY
    safe_error_code: str = ""
    attempted_commands: int = 0
    successful_commands: int = 0
    failed_commands: int = 0
    timed_out: bool = False

    @property
    def has_values(self) -> bool:
        return bool(self.values)

    @property
    def is_fresh(self) -> bool:
        """Whether this read should refresh the cache (success OR partial)."""

        return self.outcome in _FRESH_OUTCOMES

    @property
    def is_strike(self) -> bool:
        """Whether this read is dead-channel evidence.

        Only a channel that DELIVERED commands over a live link yet returned no
        metadata counts as a strike. A transport/command error is not evidence
        the commands are unsupported.
        """

        return self.outcome == OUTCOME_EMPTY

    @property
    def is_delivery_failure(self) -> bool:
        return self.outcome in _DELIVERY_FAILURE_OUTCOMES

    @classmethod
    def transport_error(cls, code: str, *, timed_out: bool = False, attempted: int = 0):
        return cls(
            outcome=OUTCOME_TRANSPORT_ERROR,
            safe_error_code=code,
            timed_out=timed_out,
            attempted_commands=attempted,
        )
