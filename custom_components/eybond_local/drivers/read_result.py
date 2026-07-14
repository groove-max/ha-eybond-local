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
  compatibility shim: a legacy bare ``dict`` is interpreted as ``FULL``. An
  unknown/invalid result is rejected fail-closed rather than mis-treated.
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
    * ``removed_keys`` -- keys to invalidate/remove (DELTA only; ignored for
      FULL, where absence already means removal).
    * ``diagnostics`` -- non-measurement diagnostic values to surface in the
      runtime snapshot (durations, group ages, command outcomes, ...).
    """

    values: dict[str, Any]
    mode: DriverReadMode = DriverReadMode.FULL
    removed_keys: frozenset[str] = field(default_factory=frozenset)
    diagnostics: dict[str, Any] = field(default_factory=dict)


def coerce_driver_read_result(raw: object, *, driver_key: str = "") -> DriverReadResult:
    """Return a typed :class:`DriverReadResult` from a driver return value.

    Centralized, typed backward compatibility: a legacy bare ``dict`` is a FULL
    snapshot (the historical contract). A ``DriverReadResult`` is validated and
    returned as-is. Anything else -- including an invalid ``mode`` -- is rejected
    fail-closed so a defect can never be silently mis-applied.
    """

    if isinstance(raw, DriverReadResult):
        if not isinstance(raw.mode, DriverReadMode):
            raise TypeError(
                f"invalid_driver_read_mode:{driver_key or '?'}:{raw.mode!r}"
            )
        return raw
    if isinstance(raw, dict):
        return DriverReadResult(values=dict(raw), mode=DriverReadMode.FULL)
    raise TypeError(
        f"invalid_driver_read_result:{driver_key or '?'}:{type(raw).__name__}"
    )
