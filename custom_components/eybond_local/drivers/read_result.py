"""Explicit driver runtime-read contract.

A driver's ``async_read_values`` may return either a full runtime snapshot or a
partial update. Historically the runtime hub treated every returned ``dict`` as
a full snapshot, which silently reverted a driver whose cycle can omit some
measurements (PI30 -- optional/energy commands may fail transiently, be skipped
as unsupported, or early-exit) back to its detection-time values whenever a
measurement was absent from a cycle.

This module makes the semantics explicit and unambiguous:

* :class:`DriverReadResult` is the typed contract a driver may return.
* ``mode`` is a typed :class:`DriverReadMode` -- the runtime never guesses
  full-vs-delta from the driver key.
* ``FULL`` means "this is the complete current runtime snapshot".
* ``DELTA`` means "apply only these values"; ``removed_keys`` explicitly
  invalidates keys (not just add/update).
* :func:`coerce_driver_read_result` is the single, centralized, typed backward
  compatibility shim: an exact legacy ``dict`` is interpreted as ``FULL``. An
  unknown, duck-typed, or malformed result is rejected fail-closed rather than
  mis-treated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DriverReadMode(Enum):
    """How the runtime must apply a driver read result."""

    FULL = "full"
    DELTA = "delta"


@dataclass(frozen=True, slots=True)
class DriverReadResult:
    """One typed driver runtime-read result.

    * ``values`` -- the measurement/runtime values produced this cycle.
    * ``mode`` -- FULL (complete snapshot) or DELTA (partial update).
    * ``removed_keys`` -- keys to invalidate/remove (DELTA only; forbidden for
      FULL, where absence already means removal).
    * ``diagnostics`` -- non-measurement diagnostic values to surface in the
      runtime snapshot (durations, group ages, command outcomes, ...).
    """

    values: dict[str, Any]
    mode: DriverReadMode = DriverReadMode.FULL
    removed_keys: frozenset[str] = field(default_factory=frozenset)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the exact driver-to-runtime contract without coercion."""

        if type(self.values) is not dict:
            raise TypeError("driver_read_values_not_dict")
        if type(self.mode) is not DriverReadMode:
            raise TypeError("driver_read_mode_invalid")
        if type(self.removed_keys) is not frozenset:
            raise TypeError("driver_read_removed_keys_not_frozenset")
        if type(self.diagnostics) is not dict:
            raise TypeError("driver_read_diagnostics_not_dict")

        value_keys = _strict_keys(self.values, field_name="values")
        removed_keys = _strict_keys(
            self.removed_keys,
            field_name="removed_keys",
        )
        diagnostic_keys = _strict_keys(
            self.diagnostics,
            field_name="diagnostics",
        )
        if self.mode is DriverReadMode.FULL and removed_keys:
            raise ValueError("driver_read_full_has_removed_keys")
        if value_keys & removed_keys:
            raise ValueError("driver_read_value_also_removed")
        if value_keys & diagnostic_keys:
            raise ValueError("driver_read_value_diagnostic_overlap")
        if removed_keys & diagnostic_keys:
            raise ValueError("driver_read_removed_diagnostic_overlap")


def _strict_keys(values: object, *, field_name: str) -> set[str]:
    """Return exact normalized keys for one already type-gated collection."""

    keys: set[str] = set()
    for key in values:  # type: ignore[union-attr]
        if type(key) is not str:
            raise TypeError(f"driver_read_{field_name}_key_not_string")
        if not key or key != key.strip():
            raise ValueError(f"driver_read_{field_name}_key_not_normalized")
        keys.add(key)
    return keys


def coerce_driver_read_result(raw: object, *, driver_key: str = "") -> DriverReadResult:
    """Return a typed :class:`DriverReadResult` from a driver return value.

    Centralized, typed backward compatibility: a legacy bare ``dict`` is a FULL
    snapshot (the historical contract). A ``DriverReadResult`` is validated and
    returned as-is. Anything else -- including an invalid ``mode`` -- is rejected
    fail-closed so a defect can never be silently mis-applied.
    """

    if type(raw) is DriverReadResult:
        return raw
    if type(raw) is dict:
        return DriverReadResult(values=dict(raw), mode=DriverReadMode.FULL)
    raise TypeError(
        f"invalid_driver_read_result:{driver_key or '?'}:{type(raw).__name__}"
    )
