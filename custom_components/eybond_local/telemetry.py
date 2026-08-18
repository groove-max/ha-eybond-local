"""Strict, immutable runtime telemetry models.

The integration still exposes a broad ``RuntimeSnapshot.values`` compatibility
mapping. That mapping contains measurements, collector metadata, tooling status
and occasionally structured diagnostics. It is therefore not a safe
measurement contract.

This module is the first narrow Typed Telemetry boundary: it projects only
driver-produced scalar values, records whether each value was read in the
current successful driver cycle or carried from an earlier DELTA result, and
keeps FULL/DELTA/removal semantics explicit. It intentionally does not coerce
values or guess that a list/dict diagnostic is telemetry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from typing import Any


TelemetryScalar = bool | int | float | str | None


class TelemetryValueKind(Enum):
    """Exact scalar representation carried by one telemetry point."""

    UNKNOWN = "unknown"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"
    TEXT = "text"


class TelemetryFreshness(Enum):
    """Whether a point was observed in the latest successful driver result."""

    FRESH = "fresh"
    CARRIED = "carried"


def _strict_key(value: object) -> str:
    if type(value) is not str:
        raise TypeError("telemetry_key_not_string")
    if not value or value != value.strip():
        raise ValueError("telemetry_key_not_normalized")
    return value


def _strict_driver_key(value: object, *, allow_empty: bool = False) -> str:
    if type(value) is not str:
        raise TypeError("telemetry_driver_key_not_string")
    if value != value.strip() or (not allow_empty and not value):
        raise ValueError("telemetry_driver_key_not_normalized")
    return value


def telemetry_value_kind(value: TelemetryScalar) -> TelemetryValueKind:
    """Return the exact kind for a supported scalar, rejecting coercions."""

    if value is None:
        return TelemetryValueKind.UNKNOWN
    if type(value) is bool:
        return TelemetryValueKind.BOOLEAN
    if type(value) is int:
        return TelemetryValueKind.INTEGER
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("telemetry_number_not_finite")
        return TelemetryValueKind.NUMBER
    if type(value) is str:
        return TelemetryValueKind.TEXT
    raise TypeError("telemetry_value_not_scalar")


def is_telemetry_scalar(value: object) -> bool:
    """Return whether *value* can enter the typed scalar telemetry layer."""

    if value is None or type(value) in (bool, int, str):
        return True
    return type(value) is float and math.isfinite(value)


@dataclass(frozen=True, slots=True)
class TelemetryPoint:
    """One exact driver-produced scalar and its cycle freshness."""

    key: str
    value: TelemetryScalar
    freshness: TelemetryFreshness

    def __post_init__(self) -> None:
        _strict_key(self.key)
        if type(self.freshness) is not TelemetryFreshness:
            raise TypeError("telemetry_freshness_invalid")
        telemetry_value_kind(self.value)

    @property
    def kind(self) -> TelemetryValueKind:
        """Return the exact scalar kind without storing a second authority."""

        return telemetry_value_kind(self.value)

    def as_carried(self) -> "TelemetryPoint":
        """Return this point marked as reused by the current snapshot."""

        if self.freshness is TelemetryFreshness.CARRIED:
            return self
        return TelemetryPoint(
            key=self.key,
            value=self.value,
            freshness=TelemetryFreshness.CARRIED,
        )


@dataclass(frozen=True, slots=True)
class TypedTelemetryFrame:
    """Immutable, unique-key scalar telemetry for one selected driver."""

    driver_key: str
    points: tuple[TelemetryPoint, ...]

    def __post_init__(self) -> None:
        _strict_driver_key(self.driver_key, allow_empty=not self.points)
        if type(self.points) is not tuple:
            raise TypeError("telemetry_points_not_tuple")
        keys: set[str] = set()
        for point in self.points:
            if type(point) is not TelemetryPoint:
                raise TypeError("telemetry_point_invalid")
            if point.key in keys:
                raise ValueError("telemetry_point_duplicate")
            keys.add(point.key)

    @classmethod
    def empty(cls) -> "TypedTelemetryFrame":
        """Return the neutral frame used before a driver is bound."""

        return cls(driver_key="", points=())

    def point(self, key: object) -> TelemetryPoint | None:
        """Return one point by exact normalized key."""

        normalized = _strict_key(key)
        for point in self.points:
            if point.key == normalized:
                return point
        return None

    def value(self, key: object, default: Any = None) -> TelemetryScalar | Any:
        """Return one scalar while preserving an explicit caller default."""

        point = self.point(key)
        return default if point is None else point.value

    def values(self) -> dict[str, TelemetryScalar]:
        """Return a compatibility mapping of typed scalar values."""

        return {point.key: point.value for point in self.points}

    def as_carried(self) -> "TypedTelemetryFrame":
        """Mark every retained point as carried for an error/offline snapshot."""

        if not self.points or all(
            point.freshness is TelemetryFreshness.CARRIED for point in self.points
        ):
            return self
        return TypedTelemetryFrame(
            driver_key=self.driver_key,
            points=tuple(point.as_carried() for point in self.points),
        )

    @property
    def fresh_count(self) -> int:
        return sum(
            point.freshness is TelemetryFreshness.FRESH for point in self.points
        )

    @property
    def carried_count(self) -> int:
        return len(self.points) - self.fresh_count


def fold_driver_telemetry(
    previous: TypedTelemetryFrame,
    *,
    driver_key: str,
    values: dict[str, Any],
    replace: bool,
    removed_keys: frozenset[str] = frozenset(),
) -> TypedTelemetryFrame:
    """Apply one FULL/DELTA driver result to the typed scalar frame.

    ``replace=True`` is FULL: previous points disappear and every accepted
    scalar is fresh. DELTA carries untouched previous points, removes explicit
    keys, and replaces current keys with fresh points. A current structured
    diagnostic removes a previous scalar point of the same key but remains in
    the legacy values mapping owned by the caller.
    """

    if type(previous) is not TypedTelemetryFrame:
        raise TypeError("telemetry_previous_frame_invalid")
    normalized_driver = _strict_driver_key(driver_key)
    if type(values) is not dict:
        raise TypeError("telemetry_values_not_dict")
    if type(replace) is not bool:
        raise TypeError("telemetry_replace_not_bool")
    if type(removed_keys) is not frozenset:
        raise TypeError("telemetry_removed_keys_not_frozenset")

    removed = {_strict_key(key) for key in removed_keys}
    current_keys = {_strict_key(key) for key in values}
    if replace or previous.driver_key != normalized_driver:
        merged: dict[str, TelemetryPoint] = {}
    else:
        merged = {
            point.key: point.as_carried()
            for point in previous.points
            if point.key not in removed and point.key not in current_keys
        }

    for key, value in values.items():
        if not is_telemetry_scalar(value):
            continue
        merged[key] = TelemetryPoint(
            key=key,
            value=value,
            freshness=TelemetryFreshness.FRESH,
        )

    return TypedTelemetryFrame(
        driver_key=normalized_driver,
        points=tuple(merged[key] for key in sorted(merged)),
    )


__all__ = [
    "TelemetryFreshness",
    "TelemetryPoint",
    "TelemetryScalar",
    "TelemetryValueKind",
    "TypedTelemetryFrame",
    "fold_driver_telemetry",
    "is_telemetry_scalar",
    "telemetry_value_kind",
]
