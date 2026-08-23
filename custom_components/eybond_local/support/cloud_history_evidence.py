"""Provider-neutral, observation-only cloud history evidence.

Cloud clients own authentication, request construction and response parsing.
This module is the single composition boundary after a provider has proven the
device identity and resolved its device-local timestamps to UTC.  The records
are deliberately incapable of claiming a local register mapping or activation
authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_CHART,
    CLOUD_FIELD_KIND_KEY_PARAMETER,
)


CLOUD_HISTORY_SCHEMA_VERSION = 1
CLOUD_HISTORY_AUTHORITY = "provider_normalized_history_observation"
CLOUD_HISTORY_MAPPING = "unproven"

CLOUD_HISTORY_STATUS_COMPLETE = "complete"
CLOUD_HISTORY_STATUS_PARTIAL = "partial"
CLOUD_HISTORY_STATUS_UNAVAILABLE = "unavailable"
CLOUD_HISTORY_STATUS_TIME_BASIS_UNAVAILABLE = "time_basis_unavailable"

_MAX_TEXT_LENGTH = 512
_MAX_SERIES = 8
_MAX_POINTS = 4096
_STATUSES = frozenset(
    {
        CLOUD_HISTORY_STATUS_COMPLETE,
        CLOUD_HISTORY_STATUS_PARTIAL,
        CLOUD_HISTORY_STATUS_UNAVAILABLE,
        CLOUD_HISTORY_STATUS_TIME_BASIS_UNAVAILABLE,
    }
)
_FIELD_KINDS = frozenset(
    {
        CLOUD_FIELD_KIND_CHART,
        CLOUD_FIELD_KIND_KEY_PARAMETER,
    }
)


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip() or len(value) > _MAX_TEXT_LENGTH:
        raise ValueError(reason)
    return value


def _optional_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if value != value.strip() or len(value) > _MAX_TEXT_LENGTH:
        raise ValueError(reason)
    return value


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


def _requested_date(value: object, *, allow_empty: bool) -> str:
    if type(value) is not str:
        raise TypeError("cloud_history_date_invalid")
    if not value:
        if allow_empty:
            return value
        raise ValueError("cloud_history_date_invalid")
    if value != value.strip():
        raise ValueError("cloud_history_date_invalid")
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("cloud_history_date_invalid") from exc
    if parsed.isoformat() != value:
        raise ValueError("cloud_history_date_invalid")
    return value


def _device_local_timestamp(value: object, *, requested_date: str) -> str:
    value = _required_token(value, "cloud_history_local_timestamp_invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("cloud_history_local_timestamp_invalid") from exc
    if (
        parsed.strftime("%Y-%m-%d %H:%M:%S") != value
        or parsed.date().isoformat() != requested_date
    ):
        raise ValueError("cloud_history_local_timestamp_invalid")
    return value


def _utc_timestamp(value: object) -> str:
    value = _required_token(value, "cloud_history_utc_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("cloud_history_utc_timestamp_invalid") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.utcoffset().total_seconds() != 0
        or parsed.isoformat() != value
    ):
        raise ValueError("cloud_history_utc_timestamp_invalid")
    return value


def _numeric_text(value: object) -> str:
    value = _required_token(value, "cloud_history_value_invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("cloud_history_value_invalid") from exc
    if not parsed.is_finite():
        raise ValueError("cloud_history_value_invalid")
    return value


@dataclass(frozen=True, slots=True)
class CloudHistoryIdentity:
    """Exact cloud device identity shared by normalized history evidence."""

    pn: str
    sn: str
    devcode: int
    devaddr: int

    def __post_init__(self) -> None:
        _required_token(self.pn, "cloud_history_identity_pn_invalid")
        _required_token(self.sn, "cloud_history_identity_sn_invalid")
        _bounded_int(
            self.devcode,
            minimum=0,
            maximum=0xFFFF,
            reason="cloud_history_identity_devcode_invalid",
        )
        _bounded_int(
            self.devaddr,
            minimum=0,
            maximum=0xFF,
            reason="cloud_history_identity_devaddr_invalid",
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "pn": self.pn,
            "sn": self.sn,
            "devcode": self.devcode,
            "devaddr": self.devaddr,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudHistoryIdentity | None":
        if type(record) is not dict or set(record) != {
            "pn",
            "sn",
            "devcode",
            "devaddr",
        }:
            return None
        try:
            return cls(
                pn=record["pn"],
                sn=record["sn"],
                devcode=record["devcode"],
                devaddr=record["devaddr"],
            )
        except (TypeError, ValueError, KeyError):
            return None


@dataclass(frozen=True, slots=True)
class CloudHistoryPoint:
    """One numeric provider value with original local and derived UTC time."""

    device_local_timestamp: str
    utc_timestamp: str
    value: str

    def __post_init__(self) -> None:
        # Date membership is checked by the owning series.
        _required_token(
            self.device_local_timestamp,
            "cloud_history_local_timestamp_invalid",
        )
        _utc_timestamp(self.utc_timestamp)
        _numeric_text(self.value)

    def to_record(self) -> dict[str, str]:
        return {
            "device_local_timestamp": self.device_local_timestamp,
            "utc_timestamp": self.utc_timestamp,
            "value": self.value,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudHistoryPoint | None":
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
        except (TypeError, ValueError, KeyError):
            return None


@dataclass(frozen=True, slots=True)
class CloudHistorySeries:
    """One identity-bound history series normalized by its source adapter."""

    provider_id: str
    source_id: str
    source_action: str
    field_kind: str
    identity: CloudHistoryIdentity
    series_key: str
    title: str
    unit: str
    requested_date: str
    precision_minutes: int
    timezone_offset_seconds: int
    points: tuple[CloudHistoryPoint, ...]

    def __post_init__(self) -> None:
        _required_token(self.provider_id, "cloud_history_provider_invalid")
        _required_token(self.source_id, "cloud_history_source_invalid")
        _required_token(self.source_action, "cloud_history_action_invalid")
        field_kind = _required_token(
            self.field_kind,
            "cloud_history_field_kind_invalid",
        )
        if field_kind not in _FIELD_KINDS:
            raise ValueError("cloud_history_field_kind_invalid")
        if type(self.identity) is not CloudHistoryIdentity:
            raise TypeError("cloud_history_identity_invalid")
        _required_token(self.series_key, "cloud_history_series_key_invalid")
        _required_token(self.title, "cloud_history_title_invalid")
        _optional_token(self.unit, "cloud_history_unit_invalid")
        requested_date = _requested_date(self.requested_date, allow_empty=False)
        _bounded_int(
            self.precision_minutes,
            minimum=0,
            maximum=1440,
            reason="cloud_history_precision_invalid",
        )
        _bounded_int(
            self.timezone_offset_seconds,
            minimum=-18 * 3600,
            maximum=18 * 3600,
            reason="cloud_history_timezone_invalid",
        )
        if type(self.points) is not tuple:
            raise TypeError("cloud_history_points_invalid")
        if len(self.points) > _MAX_POINTS:
            raise ValueError("cloud_history_points_limit_exceeded")
        previous_local = ""
        previous_utc = ""
        for point in self.points:
            if type(point) is not CloudHistoryPoint:
                raise TypeError("cloud_history_point_invalid")
            local_timestamp = _device_local_timestamp(
                point.device_local_timestamp,
                requested_date=requested_date,
            )
            utc_timestamp = _utc_timestamp(point.utc_timestamp)
            if (
                (previous_local and local_timestamp <= previous_local)
                or (previous_utc and utc_timestamp <= previous_utc)
            ):
                raise ValueError("cloud_history_points_not_ordered")
            previous_local = local_timestamp
            previous_utc = utc_timestamp

    @property
    def point_count(self) -> int:
        return len(self.points)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_HISTORY_SCHEMA_VERSION,
            "authority": CLOUD_HISTORY_AUTHORITY,
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "source_action": self.source_action,
            "field_kind": self.field_kind,
            "identity": self.identity.to_record(),
            "series_key": self.series_key,
            "title": self.title,
            "unit": self.unit,
            "requested_date": self.requested_date,
            "precision_minutes": self.precision_minutes,
            "timezone_offset_seconds": self.timezone_offset_seconds,
            "local_mapping": CLOUD_HISTORY_MAPPING,
            "local_mapping_proven": False,
            "points": [point.to_record() for point in self.points],
            "point_count": self.point_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudHistorySeries | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "provider_id",
            "source_id",
            "source_action",
            "field_kind",
            "identity",
            "series_key",
            "title",
            "unit",
            "requested_date",
            "precision_minutes",
            "timezone_offset_seconds",
            "local_mapping",
            "local_mapping_proven",
            "points",
            "point_count",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"] != CLOUD_HISTORY_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != CLOUD_HISTORY_AUTHORITY
            or type(record["local_mapping"]) is not str
            or record["local_mapping"] != CLOUD_HISTORY_MAPPING
            or record["local_mapping_proven"] is not False
            or type(record["points"]) is not list
        ):
            return None
        identity = CloudHistoryIdentity.from_record(record["identity"])
        if identity is None:
            return None
        points: list[CloudHistoryPoint] = []
        for raw_point in record["points"]:
            point = CloudHistoryPoint.from_record(raw_point)
            if point is None:
                return None
            points.append(point)
        try:
            series = cls(
                provider_id=record["provider_id"],
                source_id=record["source_id"],
                source_action=record["source_action"],
                field_kind=record["field_kind"],
                identity=identity,
                series_key=record["series_key"],
                title=record["title"],
                unit=record["unit"],
                requested_date=record["requested_date"],
                precision_minutes=record["precision_minutes"],
                timezone_offset_seconds=record["timezone_offset_seconds"],
                points=tuple(points),
            )
        except (TypeError, ValueError, KeyError):
            return None
        if (
            type(record["point_count"]) is not int
            or record["point_count"] != series.point_count
        ):
            return None
        return series


@dataclass(frozen=True, slots=True)
class CloudHistoryCollection:
    """One bounded provider-owned history attempt for one exact device."""

    provider_id: str
    source_id: str
    identity: CloudHistoryIdentity
    requested_date: str
    timezone_offset_seconds: int | None
    attempted_series_count: int
    failed_series_count: int
    budget_exhausted: bool
    series: tuple[CloudHistorySeries, ...]

    def __post_init__(self) -> None:
        _required_token(self.provider_id, "cloud_history_provider_invalid")
        _required_token(self.source_id, "cloud_history_source_invalid")
        if type(self.identity) is not CloudHistoryIdentity:
            raise TypeError("cloud_history_identity_invalid")
        if self.timezone_offset_seconds is None:
            _requested_date(self.requested_date, allow_empty=True)
            if self.requested_date:
                raise ValueError("cloud_history_time_basis_shape_invalid")
        else:
            _bounded_int(
                self.timezone_offset_seconds,
                minimum=-18 * 3600,
                maximum=18 * 3600,
                reason="cloud_history_timezone_invalid",
            )
            _requested_date(self.requested_date, allow_empty=False)
        _bounded_int(
            self.attempted_series_count,
            minimum=0,
            maximum=_MAX_SERIES,
            reason="cloud_history_attempted_count_invalid",
        )
        _bounded_int(
            self.failed_series_count,
            minimum=0,
            maximum=_MAX_SERIES,
            reason="cloud_history_failed_count_invalid",
        )
        if type(self.budget_exhausted) is not bool:
            raise TypeError("cloud_history_budget_state_invalid")
        if type(self.series) is not tuple:
            raise TypeError("cloud_history_series_invalid")
        if len(self.series) > _MAX_SERIES:
            raise ValueError("cloud_history_series_limit_exceeded")
        if self.timezone_offset_seconds is None:
            if self.attempted_series_count or self.failed_series_count or self.series:
                raise ValueError("cloud_history_time_basis_shape_invalid")
        elif (
            len(self.series) + self.failed_series_count
            != self.attempted_series_count
        ):
            raise ValueError("cloud_history_count_mismatch")
        keys: set[tuple[str, str]] = set()
        for item in self.series:
            if type(item) is not CloudHistorySeries:
                raise TypeError("cloud_history_series_item_invalid")
            if (
                item.provider_id != self.provider_id
                or item.source_id != self.source_id
                or item.identity != self.identity
                or item.requested_date != self.requested_date
                or item.timezone_offset_seconds != self.timezone_offset_seconds
            ):
                raise ValueError("cloud_history_series_mismatch")
            key = (item.source_action, item.series_key)
            if key in keys:
                raise ValueError("cloud_history_series_duplicate")
            keys.add(key)

    @property
    def status(self) -> str:
        if self.timezone_offset_seconds is None:
            return CLOUD_HISTORY_STATUS_TIME_BASIS_UNAVAILABLE
        if not self.series:
            return CLOUD_HISTORY_STATUS_UNAVAILABLE
        if self.failed_series_count or self.budget_exhausted:
            return CLOUD_HISTORY_STATUS_PARTIAL
        return CLOUD_HISTORY_STATUS_COMPLETE

    @property
    def collected_series_count(self) -> int:
        return len(self.series)

    @property
    def point_count(self) -> int:
        return sum(item.point_count for item in self.series)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_HISTORY_SCHEMA_VERSION,
            "authority": CLOUD_HISTORY_AUTHORITY,
            "provider_id": self.provider_id,
            "source_id": self.source_id,
            "read_only": True,
            "local_mapping_proven": False,
            "activation_allowed": False,
            "identity": self.identity.to_record(),
            "requested_date": self.requested_date,
            "timezone_offset_seconds": self.timezone_offset_seconds,
            "attempted_series_count": self.attempted_series_count,
            "failed_series_count": self.failed_series_count,
            "budget_exhausted": self.budget_exhausted,
            "series": [item.to_record() for item in self.series],
            "status": self.status,
            "collected_series_count": self.collected_series_count,
            "point_count": self.point_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudHistoryCollection | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "provider_id",
            "source_id",
            "read_only",
            "local_mapping_proven",
            "activation_allowed",
            "identity",
            "requested_date",
            "timezone_offset_seconds",
            "attempted_series_count",
            "failed_series_count",
            "budget_exhausted",
            "series",
            "status",
            "collected_series_count",
            "point_count",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"] != CLOUD_HISTORY_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != CLOUD_HISTORY_AUTHORITY
            or record["read_only"] is not True
            or record["local_mapping_proven"] is not False
            or record["activation_allowed"] is not False
            or type(record["budget_exhausted"]) is not bool
            or type(record["series"]) is not list
            or type(record["status"]) is not str
            or record["status"] not in _STATUSES
        ):
            return None
        identity = CloudHistoryIdentity.from_record(record["identity"])
        if identity is None:
            return None
        series: list[CloudHistorySeries] = []
        for raw_series in record["series"]:
            item = CloudHistorySeries.from_record(raw_series)
            if item is None:
                return None
            series.append(item)
        try:
            collection = cls(
                provider_id=record["provider_id"],
                source_id=record["source_id"],
                identity=identity,
                requested_date=record["requested_date"],
                timezone_offset_seconds=record["timezone_offset_seconds"],
                attempted_series_count=record["attempted_series_count"],
                failed_series_count=record["failed_series_count"],
                budget_exhausted=record["budget_exhausted"],
                series=tuple(series),
            )
        except (TypeError, ValueError, KeyError):
            return None
        if (
            record["status"] != collection.status
            or type(record["collected_series_count"]) is not int
            or record["collected_series_count"]
            != collection.collected_series_count
            or type(record["point_count"]) is not int
            or record["point_count"] != collection.point_count
        ):
            return None
        return collection


__all__ = [
    "CLOUD_HISTORY_AUTHORITY",
    "CLOUD_HISTORY_SCHEMA_VERSION",
    "CLOUD_HISTORY_STATUS_COMPLETE",
    "CLOUD_HISTORY_STATUS_PARTIAL",
    "CLOUD_HISTORY_STATUS_TIME_BASIS_UNAVAILABLE",
    "CLOUD_HISTORY_STATUS_UNAVAILABLE",
    "CloudHistoryCollection",
    "CloudHistoryIdentity",
    "CloudHistoryPoint",
    "CloudHistorySeries",
]
