"""Resolve DESSMonitor device-local history through exact provider evidence.

History parsing and timezone acquisition intentionally live in separate trust
boundaries.  This module is their only composition point: it accepts the exact
typed series and exact typed device-time basis, verifies that both belong to
the same full cloud identity, and derives canonical UTC timestamps.  The
result remains observation-only evidence and cannot claim a local register
mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .dessmonitor_history import (
    DessMonitorHistoryPoint,
    DessMonitorHistorySeries,
)
from .dessmonitor_time_basis import DessMonitorDeviceTimeBasis


DESSMONITOR_RESOLVED_HISTORY_SCHEMA_VERSION = 1
DESSMONITOR_RESOLVED_HISTORY_AUTHORITY = (
    "provider_identity_bound_time_resolution"
)
DESSMONITOR_RESOLVED_HISTORY_MAPPING = "unproven"


def _utc_timestamp(value: object) -> str:
    if type(value) is not str:
        raise TypeError("dessmonitor_resolved_utc_timestamp_invalid")
    if not value or value != value.strip():
        raise ValueError("dessmonitor_resolved_utc_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            "dessmonitor_resolved_utc_timestamp_invalid"
        ) from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.utcoffset().total_seconds() != 0
        or parsed.isoformat() != value
    ):
        raise ValueError("dessmonitor_resolved_utc_timestamp_invalid")
    return value


@dataclass(frozen=True, slots=True)
class DessMonitorResolvedHistoryPoint:
    """One provider value with both original local and derived UTC time."""

    device_local_timestamp: str
    utc_timestamp: str
    value: str

    def __post_init__(self) -> None:
        DessMonitorHistoryPoint(
            device_local_timestamp=self.device_local_timestamp,
            value=self.value,
        )
        _utc_timestamp(self.utc_timestamp)

    def to_record(self) -> dict[str, str]:
        return {
            "device_local_timestamp": self.device_local_timestamp,
            "utc_timestamp": self.utc_timestamp,
            "value": self.value,
        }

    @classmethod
    def from_record(
        cls,
        record: object,
    ) -> "DessMonitorResolvedHistoryPoint | None":
        if type(record) is not dict or set(record) != {
            "device_local_timestamp",
            "utc_timestamp",
            "value",
        }:
            return None
        try:
            return cls(
                device_local_timestamp=record["device_local_timestamp"],
                utc_timestamp=record["utc_timestamp"],
                value=record["value"],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class DessMonitorResolvedHistorySeries:
    """One exact history series resolved by its provider-owned time basis."""

    source_series: DessMonitorHistorySeries
    time_basis: DessMonitorDeviceTimeBasis
    points: tuple[DessMonitorResolvedHistoryPoint, ...]

    def __post_init__(self) -> None:
        if type(self.source_series) is not DessMonitorHistorySeries:
            raise TypeError("dessmonitor_resolved_source_series_invalid")
        if type(self.time_basis) is not DessMonitorDeviceTimeBasis:
            raise TypeError("dessmonitor_resolved_time_basis_invalid")
        if self.source_series.identity != self.time_basis.identity:
            raise ValueError("dessmonitor_resolved_identity_mismatch")
        if type(self.points) is not tuple:
            raise TypeError("dessmonitor_resolved_points_invalid")
        if len(self.points) != len(self.source_series.points):
            raise ValueError("dessmonitor_resolved_point_count_mismatch")

        previous_utc = ""
        for source, resolved in zip(
            self.source_series.points,
            self.points,
            strict=True,
        ):
            if type(resolved) is not DessMonitorResolvedHistoryPoint:
                raise TypeError("dessmonitor_resolved_point_invalid")
            if (
                resolved.device_local_timestamp
                != source.device_local_timestamp
                or resolved.value != source.value
                or resolved.utc_timestamp
                != self.time_basis.to_utc_timestamp(
                    source.device_local_timestamp
                )
            ):
                raise ValueError("dessmonitor_resolved_point_mismatch")
            if previous_utc and resolved.utc_timestamp <= previous_utc:
                raise ValueError("dessmonitor_resolved_points_not_ordered")
            previous_utc = resolved.utc_timestamp

    @property
    def point_count(self) -> int:
        return len(self.points)

    def to_record(self) -> dict[str, Any]:
        """Serialize the derivation without minting mapping authority."""

        return {
            "schema_version": DESSMONITOR_RESOLVED_HISTORY_SCHEMA_VERSION,
            "authority": DESSMONITOR_RESOLVED_HISTORY_AUTHORITY,
            "provider_id": "smartess",
            "source_id": "dessmonitor",
            "local_mapping": DESSMONITOR_RESOLVED_HISTORY_MAPPING,
            "local_mapping_proven": False,
            "source_series": self.source_series.to_record(),
            "time_basis": self.time_basis.to_record(),
            "points": [point.to_record() for point in self.points],
            "point_count": self.point_count,
        }

    @classmethod
    def from_record(
        cls,
        record: object,
    ) -> "DessMonitorResolvedHistorySeries | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "provider_id",
            "source_id",
            "local_mapping",
            "local_mapping_proven",
            "source_series",
            "time_basis",
            "points",
            "point_count",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"]
            != DESSMONITOR_RESOLVED_HISTORY_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != DESSMONITOR_RESOLVED_HISTORY_AUTHORITY
            or type(record["provider_id"]) is not str
            or record["provider_id"] != "smartess"
            or type(record["source_id"]) is not str
            or record["source_id"] != "dessmonitor"
            or type(record["local_mapping"]) is not str
            or record["local_mapping"] != DESSMONITOR_RESOLVED_HISTORY_MAPPING
            or record["local_mapping_proven"] is not False
            or type(record["points"]) is not list
        ):
            return None
        source_series = DessMonitorHistorySeries.from_record(
            record["source_series"]
        )
        time_basis = DessMonitorDeviceTimeBasis.from_record(
            record["time_basis"]
        )
        if source_series is None or time_basis is None:
            return None
        points: list[DessMonitorResolvedHistoryPoint] = []
        for raw_point in record["points"]:
            point = DessMonitorResolvedHistoryPoint.from_record(raw_point)
            if point is None:
                return None
            points.append(point)
        try:
            series = cls(
                source_series=source_series,
                time_basis=time_basis,
                points=tuple(points),
            )
        except (TypeError, ValueError):
            return None
        if (
            type(record["point_count"]) is not int
            or record["point_count"] != series.point_count
        ):
            return None
        return series


def resolve_dessmonitor_history_time_basis(
    source_series: DessMonitorHistorySeries,
    time_basis: DessMonitorDeviceTimeBasis,
) -> DessMonitorResolvedHistorySeries:
    """Resolve one series only through exact same-identity provider evidence."""

    if type(source_series) is not DessMonitorHistorySeries:
        raise TypeError("dessmonitor_resolved_source_series_invalid")
    if type(time_basis) is not DessMonitorDeviceTimeBasis:
        raise TypeError("dessmonitor_resolved_time_basis_invalid")
    if source_series.identity != time_basis.identity:
        raise ValueError("dessmonitor_resolved_identity_mismatch")
    return DessMonitorResolvedHistorySeries(
        source_series=source_series,
        time_basis=time_basis,
        points=tuple(
            DessMonitorResolvedHistoryPoint(
                device_local_timestamp=point.device_local_timestamp,
                utc_timestamp=time_basis.to_utc_timestamp(
                    point.device_local_timestamp
                ),
                value=point.value,
            )
            for point in source_series.points
        ),
    )


__all__ = [
    "DESSMONITOR_RESOLVED_HISTORY_AUTHORITY",
    "DESSMONITOR_RESOLVED_HISTORY_MAPPING",
    "DESSMONITOR_RESOLVED_HISTORY_SCHEMA_VERSION",
    "DessMonitorResolvedHistoryPoint",
    "DessMonitorResolvedHistorySeries",
    "resolve_dessmonitor_history_time_basis",
]
