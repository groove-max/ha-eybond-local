"""Typed, non-persisted capability-write confirmation evidence.

The write transport acknowledgement only proves that a Modbus request was
accepted on the wire.  It does not prove that the inverter applied the value.
This module keeps the three observations separate:

* the requested native value and raw register words;
* an exact-register read immediately after the write;
* the value observed by subsequent normal driver polls.

The trace lives only in the hub's per-session ``runtime_state``.  It is exposed
as sanitized driver diagnostics for Support Archives, but it never overrides a
polled setting value and is never persisted as device identity or metadata.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from math import isfinite
from typing import Any


WRITE_CONFIRMATION_DIAGNOSTIC_KEY = "runtime_capability_write_confirmation"

_WRITE_CONFIRMATION_STATE_KEY = "capability_write_confirmation"
_OBSERVATION_PENDING = "pending"
_OBSERVATION_MATCHED = "matched"
_OBSERVATION_MISMATCHED = "mismatched"
_OBSERVATION_UNAVAILABLE = "unavailable"
_OBSERVATION_STATUSES = frozenset(
    {
        _OBSERVATION_PENDING,
        _OBSERVATION_MATCHED,
        _OBSERVATION_MISMATCHED,
        _OBSERVATION_UNAVAILABLE,
    }
)


@dataclass(frozen=True, slots=True)
class CapabilityWriteConfirmation:
    """One bounded write/readback sequence for a single capability."""

    capability_key: str
    value_key: str
    requested_value: object
    expected_value: object
    requested_words: tuple[int, ...]
    immediate_status: str = _OBSERVATION_PENDING
    immediate_value: object | None = None
    immediate_words: tuple[int, ...] = ()
    immediate_error: str = ""
    first_poll_status: str = _OBSERVATION_PENDING
    first_poll_value: object | None = None
    latest_poll_status: str = _OBSERVATION_PENDING
    latest_poll_value: object | None = None
    poll_observation_count: int = 0
    poll_mismatch_seen: bool = False
    poll_match_seen: bool = False

    def __post_init__(self) -> None:
        """Reject malformed internal evidence rather than exporting a lie."""

        _strict_key(self.capability_key, field_name="capability_key")
        _strict_key(self.value_key, field_name="value_key")
        _strict_scalar(self.requested_value, field_name="requested_value")
        _strict_scalar(self.expected_value, field_name="expected_value")
        _strict_words(self.requested_words, field_name="requested_words", allow_empty=False)
        _strict_status(self.immediate_status, field_name="immediate_status")
        _strict_status(self.first_poll_status, field_name="first_poll_status")
        _strict_status(self.latest_poll_status, field_name="latest_poll_status")
        _strict_scalar(self.immediate_value, field_name="immediate_value")
        _strict_scalar(self.first_poll_value, field_name="first_poll_value")
        _strict_scalar(self.latest_poll_value, field_name="latest_poll_value")
        _strict_words(self.immediate_words, field_name="immediate_words", allow_empty=True)
        if type(self.immediate_error) is not str or self.immediate_error != self.immediate_error.strip():
            raise ValueError("write_confirmation_immediate_error_invalid")
        if type(self.poll_observation_count) is not int or self.poll_observation_count < 0:
            raise ValueError("write_confirmation_poll_count_invalid")
        if type(self.poll_mismatch_seen) is not bool or type(self.poll_match_seen) is not bool:
            raise TypeError("write_confirmation_poll_flags_invalid")

    def with_immediate_observation(
        self,
        *,
        value: object,
        words: tuple[int, ...],
        matched: bool,
    ) -> "CapabilityWriteConfirmation":
        """Record the exact-register observation made after the write."""

        if type(matched) is not bool:
            raise TypeError("write_confirmation_immediate_match_not_bool")
        return replace(
            self,
            immediate_status=(
                _OBSERVATION_MATCHED if matched else _OBSERVATION_MISMATCHED
            ),
            immediate_value=value,
            immediate_words=words,
            immediate_error="",
        )

    def with_immediate_unavailable(
        self,
        *,
        error: BaseException,
    ) -> "CapabilityWriteConfirmation":
        """Record a failed post-write read without masking a later full poll."""

        return replace(
            self,
            immediate_status=_OBSERVATION_UNAVAILABLE,
            immediate_value=None,
            immediate_words=(),
            immediate_error=_error_marker(error),
        )

    def with_poll_observation(
        self,
        *,
        value: object | None,
        matched: bool | None,
    ) -> "CapabilityWriteConfirmation":
        """Record one normal full-poll observation until the target appears.

        Once a poll has observed the requested value, the trace freezes.  A
        later user/cloud change is a new event and must not rewrite the outcome
        of this write attempt.
        """

        if self.poll_match_seen:
            return self
        if matched is not None and type(matched) is not bool:
            raise TypeError("write_confirmation_poll_match_invalid")
        status = (
            _OBSERVATION_UNAVAILABLE
            if matched is None
            else (_OBSERVATION_MATCHED if matched else _OBSERVATION_MISMATCHED)
        )
        count = self.poll_observation_count + 1
        first_status = self.first_poll_status
        first_value = self.first_poll_value
        if self.poll_observation_count == 0:
            first_status = status
            first_value = value
        return replace(
            self,
            first_poll_status=first_status,
            first_poll_value=first_value,
            latest_poll_status=status,
            latest_poll_value=value,
            poll_observation_count=count,
            poll_mismatch_seen=(self.poll_mismatch_seen or matched is False),
            poll_match_seen=(matched is True),
        )

    @property
    def convergence(self) -> str:
        """Return a bounded, non-causal summary of observed convergence."""

        if self.poll_match_seen and self.poll_mismatch_seen:
            return "requested_value_observed_after_mismatch"
        if self.poll_match_seen:
            return "requested_value_observed"
        if self.poll_mismatch_seen:
            return "requested_value_not_observed"
        if self.poll_observation_count:
            return "poll_readback_unavailable"
        return "full_poll_pending"

    def diagnostics(self) -> dict[str, Any]:
        """Return the sanitized Support-Archive projection."""

        return {
            "capability_key": self.capability_key,
            "value_key": self.value_key,
            "requested_value": self.requested_value,
            "expected_value": self.expected_value,
            "requested_words": list(self.requested_words),
            "immediate_status": self.immediate_status,
            "immediate_value": self.immediate_value,
            "immediate_words": list(self.immediate_words),
            "immediate_error": self.immediate_error,
            "first_full_poll_status": self.first_poll_status,
            "first_full_poll_value": self.first_poll_value,
            "latest_full_poll_status": self.latest_poll_status,
            "latest_full_poll_value": self.latest_poll_value,
            "full_poll_observation_count": self.poll_observation_count,
            "convergence": self.convergence,
        }


def start_write_confirmation(
    runtime_state: dict[str, Any] | None,
    *,
    capability_key: str,
    value_key: str,
    requested_value: object,
    expected_value: object,
    requested_words: tuple[int, ...],
) -> CapabilityWriteConfirmation:
    """Start and optionally retain one per-session write trace."""

    trace = CapabilityWriteConfirmation(
        capability_key=capability_key,
        value_key=value_key,
        requested_value=requested_value,
        expected_value=expected_value,
        requested_words=requested_words,
    )
    store_write_confirmation(runtime_state, trace)
    return trace


def load_write_confirmation(
    runtime_state: dict[str, Any] | None,
) -> CapabilityWriteConfirmation | None:
    """Return only the exact typed trace from one runtime state."""

    if type(runtime_state) is not dict:
        return None
    trace = runtime_state.get(_WRITE_CONFIRMATION_STATE_KEY)
    return trace if type(trace) is CapabilityWriteConfirmation else None


def store_write_confirmation(
    runtime_state: dict[str, Any] | None,
    trace: CapabilityWriteConfirmation,
) -> None:
    """Store an exact typed trace when a hub runtime state is available."""

    if runtime_state is None:
        return
    if type(runtime_state) is not dict:
        raise TypeError("write_confirmation_runtime_state_not_dict")
    if type(trace) is not CapabilityWriteConfirmation:
        raise TypeError("write_confirmation_trace_invalid")
    runtime_state[_WRITE_CONFIRMATION_STATE_KEY] = trace


def write_confirmation_diagnostics(
    runtime_state: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return zero or one driver diagnostic for the retained trace."""

    trace = load_write_confirmation(runtime_state)
    if trace is None:
        return {}
    return {WRITE_CONFIRMATION_DIAGNOSTIC_KEY: trace.diagnostics()}


def _strict_key(value: object, *, field_name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"write_confirmation_{field_name}_not_string")
    if not value or value != value.strip():
        raise ValueError(f"write_confirmation_{field_name}_invalid")


def _strict_scalar(value: object, *, field_name: str) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and isfinite(value):
        return
    raise TypeError(f"write_confirmation_{field_name}_not_scalar")


def _strict_words(
    words: object,
    *,
    field_name: str,
    allow_empty: bool,
) -> None:
    if type(words) is not tuple:
        raise TypeError(f"write_confirmation_{field_name}_not_tuple")
    if not allow_empty and not words:
        raise ValueError(f"write_confirmation_{field_name}_empty")
    for word in words:
        if type(word) is not int or not 0 <= word <= 0xFFFF:
            raise ValueError(f"write_confirmation_{field_name}_word_invalid")


def _strict_status(value: object, *, field_name: str) -> None:
    if type(value) is not str or value not in _OBSERVATION_STATUSES:
        raise ValueError(f"write_confirmation_{field_name}_invalid")


def _error_marker(error: BaseException) -> str:
    """Return a bounded non-sensitive exception marker."""

    error_type = type(error).__name__
    raw_code = str(error).partition(":")[0].strip()
    if (
        raw_code
        and len(raw_code) <= 64
        and all(character.isalnum() or character == "_" for character in raw_code)
    ):
        return f"{error_type}:{raw_code}"
    return error_type


__all__ = [
    "CapabilityWriteConfirmation",
    "WRITE_CONFIRMATION_DIAGNOSTIC_KEY",
    "load_write_confirmation",
    "start_write_confirmation",
    "store_write_confirmation",
    "write_confirmation_diagnostics",
]
