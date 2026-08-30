"""Typed, non-sensitive evidence for an inverter probe that did not match.

The runtime needs enough information to diagnose an unsupported fingerprint,
but it must not retain raw wire payloads, endpoint addresses, or arbitrary
exception text.  Drivers therefore mint this small closed model and the hub
re-validates its serialized projection before exposing it in diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass


_EVIDENCE_KINDS = frozenset({"catalog_identity"})
_EVIDENCE_STATUSES = frozenset(
    {
        "read_failed",
        "partial_identity",
        "unresolved_identity",
        "runtime_validation_failed",
    }
)
_RESOLUTIONS = frozenset({"", "unresolved", "exact", "family"})
_ACTION_FAILURE_REASONS = frozenset(
    {"timeout", "modbus_exception", "read_error"}
)


def _normalized_identifier(value: object, *, field: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 128
    ):
        raise ValueError(f"invalid_{field}")
    return value


def _normalized_identifiers(
    values: object,
    *,
    field: str,
) -> tuple[str, ...]:
    if type(values) is not tuple:
        raise TypeError(f"invalid_{field}")
    normalized = tuple(
        _normalized_identifier(value, field=field) for value in values
    )
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"duplicate_{field}")
    return normalized


@dataclass(frozen=True, slots=True)
class ProbeActionFailure:
    """One sanitized failure of a catalog identity action."""

    action_key: str
    reason: str
    exception_code: int | None = None

    def __post_init__(self) -> None:
        _normalized_identifier(self.action_key, field="action_key")
        if type(self.reason) is not str or self.reason not in _ACTION_FAILURE_REASONS:
            raise ValueError("invalid_action_failure_reason")
        if self.reason == "modbus_exception":
            if (
                type(self.exception_code) is not int
                or not 1 <= self.exception_code <= 0xFF
            ):
                raise ValueError("invalid_modbus_exception_code")
        elif self.exception_code is not None:
            raise ValueError("unexpected_exception_code")

    def as_public_dict(self) -> dict[str, object]:
        """Return the closed diagnostic projection."""

        result: dict[str, object] = {
            "action": self.action_key,
            "reason": self.reason,
        }
        if self.exception_code is not None:
            result["exception_code"] = self.exception_code
        return result


@dataclass(frozen=True, slots=True)
class DriverProbeEvidence:
    """Safe evidence explaining why one responding driver did not match."""

    kind: str
    status: str
    protocol_key: str
    layout_code: int | None = None
    model_code: int | None = None
    resolution: str = ""
    candidate_keys: tuple[str, ...] = ()
    executed_actions: tuple[str, ...] = ()
    failed_actions: tuple[str, ...] = ()
    action_failures: tuple[ProbeActionFailure, ...] = ()

    def __post_init__(self) -> None:
        if type(self.kind) is not str or self.kind not in _EVIDENCE_KINDS:
            raise ValueError("invalid_probe_evidence_kind")
        if type(self.status) is not str or self.status not in _EVIDENCE_STATUSES:
            raise ValueError("invalid_probe_evidence_status")
        _normalized_identifier(self.protocol_key, field="protocol_key")
        for field_name, value in (
            ("layout_code", self.layout_code),
            ("model_code", self.model_code),
        ):
            if value is not None and (
                type(value) is not int or not 0 <= value <= 0xFFFF
            ):
                raise ValueError(f"invalid_{field_name}")
        if type(self.resolution) is not str or self.resolution not in _RESOLUTIONS:
            raise ValueError("invalid_probe_resolution")
        _normalized_identifiers(self.candidate_keys, field="candidate_keys")
        _normalized_identifiers(self.executed_actions, field="executed_actions")
        _normalized_identifiers(self.failed_actions, field="failed_actions")
        if type(self.action_failures) is not tuple or any(
            type(item) is not ProbeActionFailure for item in self.action_failures
        ):
            raise TypeError("invalid_action_failures")
        failure_keys = tuple(item.action_key for item in self.action_failures)
        if len(set(failure_keys)) != len(failure_keys):
            raise ValueError("duplicate_action_failure")
        if any(key not in self.failed_actions for key in failure_keys):
            raise ValueError("unlisted_action_failure")
        if set(self.executed_actions) & set(self.failed_actions):
            raise ValueError("action_cannot_succeed_and_fail")
        if self.status == "read_failed" and (
            self.layout_code is not None
            or self.model_code is not None
            or self.executed_actions
            or self.resolution
        ):
            raise ValueError("invalid_read_failed_evidence")
        if self.status == "partial_identity" and (
            not self.executed_actions
            or self.resolution
        ):
            raise ValueError("invalid_partial_identity_evidence")
        if self.status == "unresolved_identity" and (
            self.layout_code is None
            or self.model_code is None
            or self.resolution != "unresolved"
        ):
            raise ValueError("invalid_unresolved_identity_evidence")
        if self.status == "runtime_validation_failed" and (
            self.layout_code is None
            or self.model_code is None
            or self.resolution not in {"exact", "family"}
        ):
            raise ValueError("invalid_runtime_validation_evidence")

    @property
    def has_wire_evidence(self) -> bool:
        """Whether a response proved identity data or an explicit rejection."""

        return bool(
            self.executed_actions
            or any(
                failure.reason == "modbus_exception"
                for failure in self.action_failures
            )
        )

    def as_public_dict(self) -> dict[str, object]:
        """Return the closed diagnostic projection."""

        result: dict[str, object] = {
            "kind": self.kind,
            "status": self.status,
            "protocol": self.protocol_key,
        }
        if self.layout_code is not None:
            result["layout_code"] = self.layout_code
        if self.model_code is not None:
            result["model_code"] = self.model_code
        if self.resolution:
            result["resolution"] = self.resolution
        if self.candidate_keys:
            result["candidate_keys"] = list(self.candidate_keys)
        if self.executed_actions:
            result["executed_actions"] = list(self.executed_actions)
        if self.failed_actions:
            result["failed_actions"] = list(self.failed_actions)
        if self.action_failures:
            result["action_failures"] = [
                failure.as_public_dict() for failure in self.action_failures
            ]
        return result


class DriverProbeNoMatchError(RuntimeError):
    """A normal no-match outcome carrying typed, non-sensitive evidence."""

    def __init__(self, evidence: DriverProbeEvidence) -> None:
        if type(evidence) is not DriverProbeEvidence:
            raise TypeError("probe_evidence_required")
        super().__init__("driver_probe_no_match")
        self.evidence = evidence


__all__ = [
    "DriverProbeEvidence",
    "DriverProbeNoMatchError",
    "ProbeActionFailure",
]
