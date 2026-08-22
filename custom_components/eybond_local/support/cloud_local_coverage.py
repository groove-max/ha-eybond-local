"""Compare cloud semantic hints with the current typed local telemetry frame.

This boundary answers only whether Home Assistant already exposes a semantic
key locally.  It does not claim that a cloud field was correlated to a local
register, and it deliberately serializes neither cloud nor local values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..telemetry import TelemetryFreshness, TypedTelemetryFrame
from .cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_SETTING,
    CLOUD_SEMANTIC_STATUS_RECOGNIZED,
    CloudSemanticEvidenceReport,
)


CLOUD_LOCAL_COVERAGE_SCHEMA_VERSION = 1
CLOUD_LOCAL_COVERAGE_AUTHORITY = "runtime_semantic_presence_only"

CLOUD_LOCAL_STATUS_AVAILABLE_FRESH = "available_fresh"
CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED = "available_carried"
CLOUD_LOCAL_STATUS_VALUE_UNKNOWN = "value_unknown"
CLOUD_LOCAL_STATUS_NOT_OBSERVED = "not_observed"

_COVERAGE_STATUSES = frozenset(
    {
        CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
        CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
        CLOUD_LOCAL_STATUS_VALUE_UNKNOWN,
        CLOUD_LOCAL_STATUS_NOT_OBSERVED,
    }
)
_LOCAL_FRESHNESS = frozenset({"", "fresh", "carried"})
_LOCAL_ORIGINS = frozenset({"", "driver", "canonical"})
_LOCAL_VALUE_KINDS = frozenset(
    {"", "unknown", "boolean", "integer", "number", "text"}
)
_MAX_ITEMS = 512


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


def _closed_token(
    value: object,
    allowed: frozenset[str],
    reason: str,
) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if value != value.strip() or value not in allowed:
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True)
class CloudLocalCoverageItem:
    """One semantic key and its current local-presence verdict."""

    semantic_key: str
    cloud_field_count: int
    status: str
    local_freshness: str = ""
    local_origin: str = ""
    local_value_kind: str = ""

    def __post_init__(self) -> None:
        _required_token(self.semantic_key, "cloud_local_semantic_key_invalid")
        if type(self.cloud_field_count) is not int:
            raise TypeError("cloud_local_field_count_invalid")
        if self.cloud_field_count <= 0:
            raise ValueError("cloud_local_field_count_invalid")
        _closed_token(
            self.status,
            _COVERAGE_STATUSES,
            "cloud_local_status_invalid",
        )
        _closed_token(
            self.local_freshness,
            _LOCAL_FRESHNESS,
            "cloud_local_freshness_invalid",
        )
        _closed_token(
            self.local_origin,
            _LOCAL_ORIGINS,
            "cloud_local_origin_invalid",
        )
        _closed_token(
            self.local_value_kind,
            _LOCAL_VALUE_KINDS,
            "cloud_local_value_kind_invalid",
        )

        has_local_point = self.status != CLOUD_LOCAL_STATUS_NOT_OBSERVED
        if has_local_point != bool(
            self.local_freshness and self.local_origin and self.local_value_kind
        ):
            raise ValueError("cloud_local_presence_shape_invalid")
        if (
            self.status == CLOUD_LOCAL_STATUS_AVAILABLE_FRESH
            and self.local_freshness != "fresh"
        ):
            raise ValueError("cloud_local_fresh_status_invalid")
        if (
            self.status == CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED
            and self.local_freshness != "carried"
        ):
            raise ValueError("cloud_local_carried_status_invalid")
        if self.status == CLOUD_LOCAL_STATUS_VALUE_UNKNOWN:
            if self.local_value_kind != "unknown":
                raise ValueError("cloud_local_unknown_status_invalid")
        elif has_local_point and self.local_value_kind == "unknown":
            raise ValueError("cloud_local_available_value_unknown")

    def to_record(self) -> dict[str, Any]:
        return {
            "semantic_key": self.semantic_key,
            "cloud_field_count": self.cloud_field_count,
            "status": self.status,
            "local_freshness": self.local_freshness,
            "local_origin": self.local_origin,
            "local_value_kind": self.local_value_kind,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudLocalCoverageItem | None":
        if type(record) is not dict or set(record) != {
            "semantic_key",
            "cloud_field_count",
            "status",
            "local_freshness",
            "local_origin",
            "local_value_kind",
        }:
            return None
        try:
            return cls(
                semantic_key=record["semantic_key"],
                cloud_field_count=record["cloud_field_count"],
                status=record["status"],
                local_freshness=record["local_freshness"],
                local_origin=record["local_origin"],
                local_value_kind=record["local_value_kind"],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class CloudLocalCoverageReport:
    """Bounded semantic-presence report for one typed runtime frame."""

    driver_key: str
    items: tuple[CloudLocalCoverageItem, ...]

    def __post_init__(self) -> None:
        if type(self.driver_key) is not str:
            raise TypeError("cloud_local_driver_key_invalid")
        if self.driver_key != self.driver_key.strip():
            raise ValueError("cloud_local_driver_key_invalid")
        if type(self.items) is not tuple:
            raise TypeError("cloud_local_items_invalid")
        if len(self.items) > _MAX_ITEMS:
            raise ValueError("cloud_local_items_limit_exceeded")
        keys: set[str] = set()
        for item in self.items:
            if type(item) is not CloudLocalCoverageItem:
                raise TypeError("cloud_local_item_invalid")
            if item.semantic_key in keys:
                raise ValueError("cloud_local_semantic_key_duplicate")
            keys.add(item.semantic_key)
        if self.items and not self.driver_key:
            raise ValueError("cloud_local_driver_key_missing")

    @property
    def available_count(self) -> int:
        return sum(
            item.status
            in {
                CLOUD_LOCAL_STATUS_AVAILABLE_FRESH,
                CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED,
            }
            for item in self.items
        )

    @property
    def unknown_value_count(self) -> int:
        return sum(
            item.status == CLOUD_LOCAL_STATUS_VALUE_UNKNOWN for item in self.items
        )

    @property
    def not_observed_count(self) -> int:
        return sum(
            item.status == CLOUD_LOCAL_STATUS_NOT_OBSERVED for item in self.items
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_LOCAL_COVERAGE_SCHEMA_VERSION,
            "authority": CLOUD_LOCAL_COVERAGE_AUTHORITY,
            "local_mapping_proven": False,
            "driver_key": self.driver_key,
            "items": [item.to_record() for item in self.items],
            "available_count": self.available_count,
            "unknown_value_count": self.unknown_value_count,
            "not_observed_count": self.not_observed_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudLocalCoverageReport | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "local_mapping_proven",
            "driver_key",
            "items",
            "available_count",
            "unknown_value_count",
            "not_observed_count",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"] != CLOUD_LOCAL_COVERAGE_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != CLOUD_LOCAL_COVERAGE_AUTHORITY
            or record["local_mapping_proven"] is not False
            or type(record["items"]) is not list
        ):
            return None
        items: list[CloudLocalCoverageItem] = []
        for raw_item in record["items"]:
            item = CloudLocalCoverageItem.from_record(raw_item)
            if item is None:
                return None
            items.append(item)
        try:
            report = cls(driver_key=record["driver_key"], items=tuple(items))
        except (TypeError, ValueError):
            return None
        if (
            type(record["available_count"]) is not int
            or type(record["unknown_value_count"]) is not int
            or type(record["not_observed_count"]) is not int
            or record["available_count"] != report.available_count
            or record["unknown_value_count"] != report.unknown_value_count
            or record["not_observed_count"] != report.not_observed_count
        ):
            return None
        return report


def build_cloud_local_coverage_report(
    semantic_report: CloudSemanticEvidenceReport,
    telemetry: TypedTelemetryFrame,
) -> CloudLocalCoverageReport:
    """Compare recognized read semantics with exact current local points."""

    if type(semantic_report) is not CloudSemanticEvidenceReport:
        raise TypeError("cloud_local_semantic_report_invalid")
    if type(telemetry) is not TypedTelemetryFrame:
        raise TypeError("cloud_local_telemetry_invalid")

    field_counts: dict[str, int] = {}
    for observation in semantic_report.observations:
        if (
            observation.status == CLOUD_SEMANTIC_STATUS_RECOGNIZED
            and observation.field_kind != CLOUD_FIELD_KIND_SETTING
        ):
            field_counts[observation.semantic_key] = (
                field_counts.get(observation.semantic_key, 0) + 1
            )

    items: list[CloudLocalCoverageItem] = []
    for semantic_key, cloud_field_count in field_counts.items():
        point = telemetry.point(semantic_key)
        if point is None:
            items.append(
                CloudLocalCoverageItem(
                    semantic_key=semantic_key,
                    cloud_field_count=cloud_field_count,
                    status=CLOUD_LOCAL_STATUS_NOT_OBSERVED,
                )
            )
            continue
        freshness = point.freshness.value
        value_kind = point.kind.value
        if value_kind == "unknown":
            status = CLOUD_LOCAL_STATUS_VALUE_UNKNOWN
        elif point.freshness is TelemetryFreshness.FRESH:
            status = CLOUD_LOCAL_STATUS_AVAILABLE_FRESH
        else:
            status = CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED
        items.append(
            CloudLocalCoverageItem(
                semantic_key=semantic_key,
                cloud_field_count=cloud_field_count,
                status=status,
                local_freshness=freshness,
                local_origin=point.origin.value,
                local_value_kind=value_kind,
            )
        )
    return CloudLocalCoverageReport(
        driver_key=telemetry.driver_key,
        items=tuple(items),
    )


__all__ = [
    "CLOUD_LOCAL_COVERAGE_AUTHORITY",
    "CLOUD_LOCAL_COVERAGE_SCHEMA_VERSION",
    "CLOUD_LOCAL_STATUS_AVAILABLE_CARRIED",
    "CLOUD_LOCAL_STATUS_AVAILABLE_FRESH",
    "CLOUD_LOCAL_STATUS_NOT_OBSERVED",
    "CLOUD_LOCAL_STATUS_VALUE_UNKNOWN",
    "CloudLocalCoverageItem",
    "CloudLocalCoverageReport",
    "build_cloud_local_coverage_report",
]
