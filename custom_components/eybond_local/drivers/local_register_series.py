"""Bounded repeated local-register evidence for offline correlation review.

One live snapshot cannot distinguish a real mapping from a static or duplicate
value.  This module collects and validates several exact driver-owned
``LocalRegisterSnapshot`` objects without interpreting their words, consulting
cloud data, or activating a learned mapping.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .local_register_evidence import LocalRegisterSnapshot


LOCAL_REGISTER_SERIES_SCHEMA_VERSION = 1
LOCAL_REGISTER_SERIES_AUTHORITY = "repeated_live_local_wire_observation"
LOCAL_REGISTER_SERIES_TIME_BASIS = "aware_utc_snapshot_timestamps"

# DESSMonitor's documented daily chart precision is five minutes.  Five local
# observations therefore span twenty minutes and provide one sample beyond the
# review correlator's four-sample minimum without turning a foreground flow into
# a long-running operation.
DEFAULT_LOCAL_REGISTER_SERIES_SAMPLE_COUNT = 5
DEFAULT_LOCAL_REGISTER_SERIES_INTERVAL_SECONDS = 300

_MIN_SNAPSHOTS = 3
_MAX_SNAPSHOTS = 64
_MIN_INTERVAL_SECONDS = 1
_MAX_INTERVAL_SECONDS = 300


def _aware_datetime(value: str, reason: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError as exc:
        raise ValueError(reason) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(reason)
    return parsed


def _bounded_int(
    value: object,
    *,
    minimum: int,
    maximum: int,
    reason: str,
) -> int:
    if type(value) is not int:
        raise TypeError(reason)
    if value < minimum or value > maximum:
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True)
class LocalRegisterSeriesPlan:
    """One bounded repeated-snapshot schedule, with no execution authority."""

    sample_count: int
    sample_interval_seconds: int

    def __post_init__(self) -> None:
        _bounded_int(
            self.sample_count,
            minimum=_MIN_SNAPSHOTS,
            maximum=_MAX_SNAPSHOTS,
            reason="local_register_series_snapshot_count_invalid",
        )
        _bounded_int(
            self.sample_interval_seconds,
            minimum=_MIN_INTERVAL_SECONDS,
            maximum=_MAX_INTERVAL_SECONDS,
            reason="local_register_series_interval_invalid",
        )

    @property
    def duration_seconds(self) -> int:
        """Return elapsed schedule time, excluding individual read duration."""

        return (self.sample_count - 1) * self.sample_interval_seconds

    def to_record(self) -> dict[str, int]:
        return {
            "sample_count": self.sample_count,
            "sample_interval_seconds": self.sample_interval_seconds,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def from_record(cls, record: object) -> "LocalRegisterSeriesPlan | None":
        if type(record) is not dict or set(record) != {
            "sample_count",
            "sample_interval_seconds",
            "duration_seconds",
        }:
            return None
        try:
            plan = cls(
                sample_count=record["sample_count"],
                sample_interval_seconds=record["sample_interval_seconds"],
            )
        except (TypeError, ValueError):
            return None
        if type(record["duration_seconds"]) is not int:
            return None
        if record["duration_seconds"] != plan.duration_seconds:
            return None
        return plan


@dataclass(frozen=True, slots=True)
class LocalRegisterSnapshotSeries:
    """Several ordered snapshots from one exact collector and driver."""

    collector_pn: str
    driver_key: str
    sample_interval_seconds: int
    snapshots: tuple[LocalRegisterSnapshot, ...]

    def __post_init__(self) -> None:
        if type(self.collector_pn) is not str:
            raise TypeError("local_register_series_collector_pn_invalid")
        if not self.collector_pn or self.collector_pn != self.collector_pn.strip():
            raise ValueError("local_register_series_collector_pn_invalid")
        if type(self.driver_key) is not str:
            raise TypeError("local_register_series_driver_key_invalid")
        if not self.driver_key or self.driver_key != self.driver_key.strip():
            raise ValueError("local_register_series_driver_key_invalid")
        _bounded_int(
            self.sample_interval_seconds,
            minimum=_MIN_INTERVAL_SECONDS,
            maximum=_MAX_INTERVAL_SECONDS,
            reason="local_register_series_interval_invalid",
        )
        if type(self.snapshots) is not tuple:
            raise TypeError("local_register_series_snapshots_invalid")
        if not _MIN_SNAPSHOTS <= len(self.snapshots) <= _MAX_SNAPSHOTS:
            raise ValueError("local_register_series_snapshot_count_invalid")

        previous_completed: datetime | None = None
        for snapshot in self.snapshots:
            if type(snapshot) is not LocalRegisterSnapshot:
                raise TypeError("local_register_series_snapshot_invalid")
            if snapshot.collector_pn != self.collector_pn:
                raise ValueError("local_register_series_identity_changed")
            if snapshot.driver_key != self.driver_key:
                raise ValueError("local_register_series_driver_changed")
            started = _aware_datetime(
                snapshot.started_at,
                "local_register_series_snapshot_time_invalid",
            )
            completed = _aware_datetime(
                snapshot.completed_at,
                "local_register_series_snapshot_time_invalid",
            )
            if previous_completed is not None and started < previous_completed:
                raise ValueError("local_register_series_snapshots_overlap")
            previous_completed = completed

    @property
    def snapshot_count(self) -> int:
        return len(self.snapshots)

    @property
    def observed_register_count(self) -> int:
        return sum(
            snapshot.observed_register_count for snapshot in self.snapshots
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": LOCAL_REGISTER_SERIES_SCHEMA_VERSION,
            "authority": LOCAL_REGISTER_SERIES_AUTHORITY,
            "source": "driver_modbus_read_series",
            "time_basis": LOCAL_REGISTER_SERIES_TIME_BASIS,
            "cloud_mapping_proven": False,
            "collector_pn": self.collector_pn,
            "driver_key": self.driver_key,
            "sample_interval_seconds": self.sample_interval_seconds,
            "snapshots": [snapshot.to_record() for snapshot in self.snapshots],
            "snapshot_count": self.snapshot_count,
            "observed_register_count": self.observed_register_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "LocalRegisterSnapshotSeries | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "source",
            "time_basis",
            "cloud_mapping_proven",
            "collector_pn",
            "driver_key",
            "sample_interval_seconds",
            "snapshots",
            "snapshot_count",
            "observed_register_count",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"] != LOCAL_REGISTER_SERIES_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != LOCAL_REGISTER_SERIES_AUTHORITY
            or type(record["source"]) is not str
            or record["source"] != "driver_modbus_read_series"
            or type(record["time_basis"]) is not str
            or record["time_basis"] != LOCAL_REGISTER_SERIES_TIME_BASIS
            or record["cloud_mapping_proven"] is not False
            or type(record["snapshots"]) is not list
        ):
            return None
        snapshots: list[LocalRegisterSnapshot] = []
        for raw_snapshot in record["snapshots"]:
            snapshot = LocalRegisterSnapshot.from_record(raw_snapshot)
            if snapshot is None:
                return None
            snapshots.append(snapshot)
        try:
            series = cls(
                collector_pn=record["collector_pn"],
                driver_key=record["driver_key"],
                sample_interval_seconds=record["sample_interval_seconds"],
                snapshots=tuple(snapshots),
            )
        except (TypeError, ValueError):
            return None
        if (
            type(record["snapshot_count"]) is not int
            or record["snapshot_count"] != series.snapshot_count
            or type(record["observed_register_count"]) is not int
            or record["observed_register_count"]
            != series.observed_register_count
        ):
            return None
        return series


SnapshotCapture = Callable[[], Awaitable[LocalRegisterSnapshot]]
Sleep = Callable[[float], Awaitable[None]]


async def async_capture_local_register_series(
    *,
    capture_snapshot: SnapshotCapture,
    sample_count: int,
    sample_interval_seconds: int,
    sleep: Sleep = asyncio.sleep,
) -> LocalRegisterSnapshotSeries:
    """Capture a bounded sequence while preserving exact producer identity."""

    if not callable(capture_snapshot) or not callable(sleep):
        raise TypeError("local_register_series_callback_invalid")
    _bounded_int(
        sample_count,
        minimum=_MIN_SNAPSHOTS,
        maximum=_MAX_SNAPSHOTS,
        reason="local_register_series_snapshot_count_invalid",
    )
    _bounded_int(
        sample_interval_seconds,
        minimum=_MIN_INTERVAL_SECONDS,
        maximum=_MAX_INTERVAL_SECONDS,
        reason="local_register_series_interval_invalid",
    )

    snapshots: list[LocalRegisterSnapshot] = []
    collector_pn = ""
    driver_key = ""
    for index in range(sample_count):
        if index:
            await sleep(float(sample_interval_seconds))
        snapshot = await capture_snapshot()
        if type(snapshot) is not LocalRegisterSnapshot:
            raise TypeError("local_register_series_snapshot_invalid")
        if not snapshots:
            collector_pn = snapshot.collector_pn
            driver_key = snapshot.driver_key
        elif snapshot.collector_pn != collector_pn:
            raise ValueError("local_register_series_identity_changed")
        elif snapshot.driver_key != driver_key:
            raise ValueError("local_register_series_driver_changed")
        snapshots.append(snapshot)

    return LocalRegisterSnapshotSeries(
        collector_pn=collector_pn,
        driver_key=driver_key,
        sample_interval_seconds=sample_interval_seconds,
        snapshots=tuple(snapshots),
    )


__all__ = [
    "LOCAL_REGISTER_SERIES_AUTHORITY",
    "LOCAL_REGISTER_SERIES_SCHEMA_VERSION",
    "LOCAL_REGISTER_SERIES_TIME_BASIS",
    "DEFAULT_LOCAL_REGISTER_SERIES_INTERVAL_SECONDS",
    "DEFAULT_LOCAL_REGISTER_SERIES_SAMPLE_COUNT",
    "LocalRegisterSeriesPlan",
    "LocalRegisterSnapshotSeries",
    "async_capture_local_register_series",
]
