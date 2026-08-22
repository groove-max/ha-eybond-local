"""Typed read-only DESSMonitor history evidence.

The public API timestamps history rows in the device's configured local time,
but it does not include that time zone in either history response. This module
therefore preserves the provider timestamp verbatim and marks its time basis as
unresolved. Nothing here converts those timestamps to UTC, correlates a cloud
field with local wire data, or creates a local mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote

from .dessmonitor_cloud import (
    DEFAULT_BASE_URL,
    DEFAULT_LANGUAGE,
    DEFAULT_MAX_TEXT_LENGTH,
    DEFAULT_TIMEOUT,
    DessMonitorCloudError,
    DessMonitorDeviceIdentity,
    DessMonitorSession,
    fetch_signed_action,
)


DESSMONITOR_HISTORY_SCHEMA_VERSION = 1
DESSMONITOR_HISTORY_AUTHORITY = "cloud_history_observation_only"
DESSMONITOR_HISTORY_TIME_BASIS = "device_local_timezone_unresolved"
DESSMONITOR_HISTORY_MAPPING = "unproven"

DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER = "queryDeviceKeyParameterOneDay"
DESSMONITOR_HISTORY_SOURCE_SOLE_CHART = "queryDeviceSoleChartEs"

_HISTORY_SOURCES = frozenset(
    {
        DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
        DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    }
)
_MAX_HISTORY_POINTS = 4096


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if (
        not value
        or value != value.strip()
        or len(value) > DEFAULT_MAX_TEXT_LENGTH
    ):
        raise ValueError(reason)
    return value


def _optional_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if value != value.strip() or len(value) > DEFAULT_MAX_TEXT_LENGTH:
        raise ValueError(reason)
    return value


def _history_date(value: object) -> str:
    value = _required_token(value, "dessmonitor_history_date_invalid")
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("dessmonitor_history_date_invalid") from exc
    if parsed.isoformat() != value:
        raise ValueError("dessmonitor_history_date_invalid")
    return value


def _parsed_device_local_timestamp(value: object) -> tuple[str, datetime]:
    value = _required_token(value, "dessmonitor_history_timestamp_invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("dessmonitor_history_timestamp_invalid") from exc
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != value:
        raise ValueError("dessmonitor_history_timestamp_invalid")
    return value, parsed


def _device_local_timestamp(value: object, *, expected_date: str) -> str:
    value, parsed = _parsed_device_local_timestamp(value)
    if parsed.date().isoformat() != expected_date:
        raise ValueError("dessmonitor_history_timestamp_outside_date")
    return value


def _numeric_text(value: object) -> str:
    value = _required_token(value, "dessmonitor_history_value_invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("dessmonitor_history_value_invalid") from exc
    if not parsed.is_finite():
        raise ValueError("dessmonitor_history_value_invalid")
    return value


def _precision_minutes(value: object, *, source_action: str) -> int:
    if type(value) is not int:
        raise TypeError("dessmonitor_history_precision_invalid")
    if source_action == DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER:
        if value != 0:
            raise ValueError("dessmonitor_history_precision_invalid")
        return value
    if value < 1 or value > 1440:
        raise ValueError("dessmonitor_history_precision_invalid")
    return value


@dataclass(frozen=True, slots=True)
class DessMonitorHistoryPoint:
    """One numeric value at one unresolved device-local timestamp."""

    device_local_timestamp: str
    value: str

    def __post_init__(self) -> None:
        _parsed_device_local_timestamp(
            self.device_local_timestamp,
        )
        _numeric_text(self.value)

    def to_record(self) -> dict[str, str]:
        return {
            "device_local_timestamp": self.device_local_timestamp,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class DessMonitorHistorySeries:
    """One bounded cloud series that has no trusted UTC conversion."""

    identity: DessMonitorDeviceIdentity
    source_action: str
    series_key: str
    requested_date: str
    points: tuple[DessMonitorHistoryPoint, ...]
    title: str = ""
    unit: str = ""
    precision_minutes: int = 0

    def __post_init__(self) -> None:
        if type(self.identity) is not DessMonitorDeviceIdentity:
            raise TypeError("dessmonitor_history_identity_invalid")
        _required_token(self.series_key, "dessmonitor_history_series_key_invalid")
        _optional_token(self.title, "dessmonitor_history_title_invalid")
        _optional_token(self.unit, "dessmonitor_history_unit_invalid")
        if type(self.source_action) is not str:
            raise TypeError("dessmonitor_history_source_invalid")
        if (
            self.source_action != self.source_action.strip()
            or self.source_action not in _HISTORY_SOURCES
        ):
            raise ValueError("dessmonitor_history_source_invalid")
        requested_date = _history_date(self.requested_date)
        _precision_minutes(
            self.precision_minutes,
            source_action=self.source_action,
        )
        if type(self.points) is not tuple:
            raise TypeError("dessmonitor_history_points_invalid")
        if len(self.points) > _MAX_HISTORY_POINTS:
            raise ValueError("dessmonitor_history_points_limit_exceeded")
        previous = ""
        for point in self.points:
            if type(point) is not DessMonitorHistoryPoint:
                raise TypeError("dessmonitor_history_point_invalid")
            timestamp = _device_local_timestamp(
                point.device_local_timestamp,
                expected_date=requested_date,
            )
            if previous and timestamp <= previous:
                raise ValueError("dessmonitor_history_points_not_strictly_ordered")
            previous = timestamp

    @property
    def point_count(self) -> int:
        return len(self.points)

    def to_record(self) -> dict[str, Any]:
        """Serialize without claiming a UTC timestamp or local mapping."""

        return {
            "schema_version": DESSMONITOR_HISTORY_SCHEMA_VERSION,
            "authority": DESSMONITOR_HISTORY_AUTHORITY,
            "provider_id": "smartess",
            "source_id": "dessmonitor",
            "source_action": self.source_action,
            "identity": self.identity.to_record(),
            "series_key": self.series_key,
            "title": self.title,
            "unit": self.unit,
            "requested_date": self.requested_date,
            "precision_minutes": self.precision_minutes,
            "time_basis": DESSMONITOR_HISTORY_TIME_BASIS,
            "timezone_offset": "",
            "local_mapping": DESSMONITOR_HISTORY_MAPPING,
            "local_mapping_proven": False,
            "points": [point.to_record() for point in self.points],
            "point_count": self.point_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "DessMonitorHistorySeries | None":
        """Parse persisted/transient evidence without raising."""

        expected_keys = {
            "schema_version",
            "authority",
            "provider_id",
            "source_id",
            "source_action",
            "identity",
            "series_key",
            "title",
            "unit",
            "requested_date",
            "precision_minutes",
            "time_basis",
            "timezone_offset",
            "local_mapping",
            "local_mapping_proven",
            "points",
            "point_count",
        }
        if type(record) is not dict or set(record) != expected_keys:
            return None
        identity_record = record.get("identity")
        if type(identity_record) is not dict or set(identity_record) != {
            "pn",
            "sn",
            "devcode",
            "devaddr",
        }:
            return None
        if (
            type(record.get("schema_version")) is not int
            or record.get("schema_version") != DESSMONITOR_HISTORY_SCHEMA_VERSION
            or type(record.get("authority")) is not str
            or record.get("authority") != DESSMONITOR_HISTORY_AUTHORITY
            or type(record.get("provider_id")) is not str
            or record.get("provider_id") != "smartess"
            or type(record.get("source_id")) is not str
            or record.get("source_id") != "dessmonitor"
            or type(record.get("time_basis")) is not str
            or record.get("time_basis") != DESSMONITOR_HISTORY_TIME_BASIS
            or type(record.get("timezone_offset")) is not str
            or record.get("timezone_offset") != ""
            or type(record.get("local_mapping")) is not str
            or record.get("local_mapping") != DESSMONITOR_HISTORY_MAPPING
            or record.get("local_mapping_proven") is not False
            or type(record.get("points")) is not list
        ):
            return None
        points: list[DessMonitorHistoryPoint] = []
        for item in record["points"]:
            if type(item) is not dict or set(item) != {
                "device_local_timestamp",
                "value",
            }:
                return None
            try:
                points.append(
                    DessMonitorHistoryPoint(
                        device_local_timestamp=item["device_local_timestamp"],
                        value=item["value"],
                    )
                )
            except (TypeError, ValueError):
                return None
        try:
            series = cls(
                identity=DessMonitorDeviceIdentity(
                    pn=identity_record["pn"],
                    sn=identity_record["sn"],
                    devcode=identity_record["devcode"],
                    devaddr=identity_record["devaddr"],
                ),
                source_action=record["source_action"],
                series_key=record["series_key"],
                title=record["title"],
                unit=record["unit"],
                requested_date=record["requested_date"],
                precision_minutes=record["precision_minutes"],
                points=tuple(points),
            )
        except (TypeError, ValueError, KeyError):
            return None
        if type(record.get("point_count")) is not int:
            return None
        if record["point_count"] != series.point_count:
            return None
        return series


def _point_from_provider(
    row: object,
    *,
    timestamp_key: str,
    expected_date: str,
) -> DessMonitorHistoryPoint | None:
    if type(row) is not dict:
        return None
    try:
        timestamp = _device_local_timestamp(
            row.get(timestamp_key),
            expected_date=expected_date,
        )
        value = _numeric_text(row.get("val"))
        return DessMonitorHistoryPoint(
            device_local_timestamp=timestamp,
            value=value,
        )
    except (TypeError, ValueError):
        return None


def _normalized_points(
    rows: object,
    *,
    timestamp_key: str,
    requested_date: str,
) -> tuple[DessMonitorHistoryPoint, ...]:
    if type(rows) is not list:
        raise DessMonitorCloudError("history_payload_invalid")
    if len(rows) > _MAX_HISTORY_POINTS:
        raise DessMonitorCloudError("history_payload_limit_exceeded")
    points: dict[str, DessMonitorHistoryPoint] = {}
    for row in rows:
        point = _point_from_provider(
            row,
            timestamp_key=timestamp_key,
            expected_date=requested_date,
        )
        if point is None:
            continue
        if point.device_local_timestamp in points:
            raise DessMonitorCloudError("history_payload_timestamp_ambiguous")
        points[point.device_local_timestamp] = point
    return tuple(points[key] for key in sorted(points))


def parse_key_parameter_history(
    dat: object,
    *,
    identity: DessMonitorDeviceIdentity,
    parameter: str,
    requested_date: str,
) -> DessMonitorHistorySeries:
    """Parse one official ``queryDeviceKeyParameterOneDay`` response body."""

    if type(identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_history_identity_invalid")
    parameter = _required_token(parameter, "dessmonitor_history_parameter_invalid")
    requested_date = _history_date(requested_date)
    if type(dat) is not dict:
        raise DessMonitorCloudError("history_payload_invalid")
    points = _normalized_points(
        dat.get("parameter"),
        timestamp_key="ts",
        requested_date=requested_date,
    )
    return DessMonitorHistorySeries(
        identity=identity,
        source_action=DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
        series_key=parameter,
        title=parameter,
        unit="",
        requested_date=requested_date,
        precision_minutes=0,
        points=points,
    )


def parse_sole_chart_history(
    dat: object,
    *,
    identity: DessMonitorDeviceIdentity,
    requested_date: str,
    precision_minutes: int,
) -> DessMonitorHistorySeries:
    """Parse one official ``queryDeviceSoleChartEs`` response body."""

    if type(identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_history_identity_invalid")
    requested_date = _history_date(requested_date)
    _precision_minutes(
        precision_minutes,
        source_action=DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    )
    if type(dat) is not dict:
        raise DessMonitorCloudError("history_payload_invalid")
    series_key = _required_token(
        dat.get("optional"),
        "dessmonitor_history_series_key_invalid",
    )
    title = _optional_token(dat.get("name", ""), "dessmonitor_history_title_invalid")
    unit = _optional_token(
        dat.get("uint", dat.get("unit", "")),
        "dessmonitor_history_unit_invalid",
    )
    points = _normalized_points(
        dat.get("rets"),
        timestamp_key="key",
        requested_date=requested_date,
    )
    return DessMonitorHistorySeries(
        identity=identity,
        source_action=DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
        series_key=series_key,
        title=title,
        unit=unit,
        requested_date=requested_date,
        precision_minutes=precision_minutes,
        points=points,
    )


def _device_parameters(identity: DessMonitorDeviceIdentity) -> str:
    if type(identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_history_identity_invalid")
    return (
        f"&pn={quote(identity.pn, safe='')}"
        f"&sn={quote(identity.sn, safe='')}"
        f"&devcode={identity.devcode}"
        f"&devaddr={identity.devaddr}"
    )


def fetch_key_parameter_history(
    *,
    session: DessMonitorSession,
    identity: DessMonitorDeviceIdentity,
    parameter: str,
    requested_date: str,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
) -> DessMonitorHistorySeries:
    """Fetch one bounded read-only key-parameter daily series."""

    parameter = _required_token(parameter, "dessmonitor_history_parameter_invalid")
    requested_date = _history_date(requested_date)
    action = (
        f"&action={DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER}"
        + _device_parameters(identity)
        + f"&parameter={quote(parameter, safe='')}"
        + f"&date={quote(requested_date, safe='')}"
    )
    envelope = fetch_signed_action(
        action=action,
        session=session,
        base_url=base_url,
        language=language,
        timeout=timeout,
    )
    return parse_key_parameter_history(
        envelope.dat,
        identity=identity,
        parameter=parameter,
        requested_date=requested_date,
    )


def fetch_sole_chart_history(
    *,
    session: DessMonitorSession,
    identity: DessMonitorDeviceIdentity,
    requested_date: str,
    precision_minutes: int = 5,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
) -> DessMonitorHistorySeries:
    """Fetch one bounded read-only same-day chart series."""

    requested_date = _history_date(requested_date)
    _precision_minutes(
        precision_minutes,
        source_action=DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
    )
    action = (
        f"&action={DESSMONITOR_HISTORY_SOURCE_SOLE_CHART}"
        + _device_parameters(identity)
        + f"&precision={precision_minutes}"
        + f"&sdate={quote(requested_date + ' 00:00:00', safe='')}"
        + f"&edate={quote(requested_date + ' 23:59:59', safe='')}"
    )
    envelope = fetch_signed_action(
        action=action,
        session=session,
        base_url=base_url,
        language=language,
        timeout=timeout,
    )
    return parse_sole_chart_history(
        envelope.dat,
        identity=identity,
        requested_date=requested_date,
        precision_minutes=precision_minutes,
    )


__all__ = [
    "DESSMONITOR_HISTORY_AUTHORITY",
    "DESSMONITOR_HISTORY_MAPPING",
    "DESSMONITOR_HISTORY_SCHEMA_VERSION",
    "DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER",
    "DESSMONITOR_HISTORY_SOURCE_SOLE_CHART",
    "DESSMONITOR_HISTORY_TIME_BASIS",
    "DessMonitorHistoryPoint",
    "DessMonitorHistorySeries",
    "fetch_key_parameter_history",
    "fetch_sole_chart_history",
    "parse_key_parameter_history",
    "parse_sole_chart_history",
]
