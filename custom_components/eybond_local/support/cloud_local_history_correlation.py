"""Review-only correlation of cloud history and local register series.

This module may identify exact temporal candidates, but it never promotes a
candidate to a local mapping.  It requires multiple changing observations,
uses only explicit scale/sign transforms, and retains both typed evidence
inputs so every derived verdict can be revalidated after serialization.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from ..collector_identity import pn_is_same_identity
from ..drivers.local_register_series import LocalRegisterSnapshotSeries
from .cloud_history_evidence import (
    CloudHistoryCollection,
    CloudHistorySeries,
)
from .cloud_semantic_evidence import (
    CLOUD_SEMANTIC_STATUS_RECOGNIZED,
    CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT,
    CloudSemanticObservation,
    classify_cloud_semantic_observation,
)


CLOUD_LOCAL_HISTORY_CORRELATION_SCHEMA_VERSION = 1
CLOUD_LOCAL_HISTORY_CORRELATION_AUTHORITY = "review_candidate_only"
CLOUD_LOCAL_HISTORY_MAPPING = "candidate_not_proven"

CLOUD_LOCAL_HISTORY_REVIEW_SCHEMA_VERSION = 1
CLOUD_LOCAL_HISTORY_REVIEW_AUTHORITY = "review_composition_only"

CLOUD_LOCAL_HISTORY_REVIEW_STATUS_NO_ELIGIBLE = "no_eligible_series"
CLOUD_LOCAL_HISTORY_REVIEW_STATUS_REVIEWED = "reviewed_no_candidate"
CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES = "review_candidates_available"

CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES = "insufficient_samples"
CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION = "insufficient_variation"
CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE = "no_exact_candidate"
CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS = "ambiguous_exact_candidates"
CLOUD_LOCAL_HISTORY_STATUS_UNIQUE = "unique_exact_candidate"

_STATUSES = frozenset(
    {
        CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES,
        CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION,
        CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE,
        CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS,
        CLOUD_LOCAL_HISTORY_STATUS_UNIQUE,
    }
)
_DIVISORS = (1, 10, 100, 1000)
_MIN_ALIGNED_SAMPLES = 4
_MIN_DISTINCT_VALUES = 3
_MAX_ALIGNMENT_TOLERANCE_SECONDS = 3600


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


def _aware_datetime(value: str, reason: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(reason) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(reason)
    return parsed


@dataclass(frozen=True, slots=True, order=True)
class LocalRegisterLocation:
    """One exact Modbus word address including tunnel/read provenance."""

    devcode: int
    collector_addr: int
    device_addr: int
    function: int
    register: int

    def __post_init__(self) -> None:
        _bounded_int(
            self.devcode,
            minimum=0,
            maximum=0xFFFF,
            reason="cloud_local_history_devcode_invalid",
        )
        for value in (self.collector_addr, self.device_addr):
            _bounded_int(
                value,
                minimum=0,
                maximum=0xFF,
                reason="cloud_local_history_address_invalid",
            )
        if type(self.function) is not int:
            raise TypeError("cloud_local_history_function_invalid")
        if self.function not in {3, 4}:
            raise ValueError("cloud_local_history_function_invalid")
        _bounded_int(
            self.register,
            minimum=0,
            maximum=0xFFFF,
            reason="cloud_local_history_register_invalid",
        )

    def to_record(self) -> dict[str, int]:
        return {
            "devcode": self.devcode,
            "collector_addr": self.collector_addr,
            "device_addr": self.device_addr,
            "function": self.function,
            "register": self.register,
        }

    @classmethod
    def from_record(cls, record: object) -> "LocalRegisterLocation | None":
        if type(record) is not dict or set(record) != {
            "devcode",
            "collector_addr",
            "device_addr",
            "function",
            "register",
        }:
            return None
        try:
            return cls(
                devcode=record["devcode"],
                collector_addr=record["collector_addr"],
                device_addr=record["device_addr"],
                function=record["function"],
                register=record["register"],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class CloudLocalHistoryCandidate:
    """One exact transform that matched every eligible aligned sample."""

    location: LocalRegisterLocation
    divisor: int
    signed: bool
    aligned_sample_count: int
    distinct_cloud_value_count: int

    def __post_init__(self) -> None:
        if type(self.location) is not LocalRegisterLocation:
            raise TypeError("cloud_local_history_candidate_location_invalid")
        if type(self.divisor) is not int:
            raise TypeError("cloud_local_history_candidate_divisor_invalid")
        if self.divisor not in _DIVISORS:
            raise ValueError("cloud_local_history_candidate_divisor_invalid")
        if type(self.signed) is not bool:
            raise TypeError("cloud_local_history_candidate_signed_invalid")
        _bounded_int(
            self.aligned_sample_count,
            minimum=_MIN_ALIGNED_SAMPLES,
            maximum=64,
            reason="cloud_local_history_candidate_sample_count_invalid",
        )
        _bounded_int(
            self.distinct_cloud_value_count,
            minimum=_MIN_DISTINCT_VALUES,
            maximum=self.aligned_sample_count,
            reason="cloud_local_history_candidate_variation_invalid",
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "location": self.location.to_record(),
            "divisor": self.divisor,
            "signed": self.signed,
            "aligned_sample_count": self.aligned_sample_count,
            "distinct_cloud_value_count": self.distinct_cloud_value_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudLocalHistoryCandidate | None":
        if type(record) is not dict or set(record) != {
            "location",
            "divisor",
            "signed",
            "aligned_sample_count",
            "distinct_cloud_value_count",
        }:
            return None
        location = LocalRegisterLocation.from_record(record["location"])
        if location is None:
            return None
        try:
            return cls(
                location=location,
                divisor=record["divisor"],
                signed=record["signed"],
                aligned_sample_count=record["aligned_sample_count"],
                distinct_cloud_value_count=record[
                    "distinct_cloud_value_count"
                ],
            )
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True, slots=True)
class CloudLocalHistoryCorrelationReport:
    """Revalidatable candidate report with no activation authority."""

    cloud_history: CloudHistorySeries
    local_series: LocalRegisterSnapshotSeries
    semantic: CloudSemanticObservation
    alignment_tolerance_seconds: int
    status: str
    candidates: tuple[CloudLocalHistoryCandidate, ...]

    def __post_init__(self) -> None:
        if type(self.cloud_history) is not CloudHistorySeries:
            raise TypeError("cloud_local_history_cloud_series_invalid")
        if type(self.local_series) is not LocalRegisterSnapshotSeries:
            raise TypeError("cloud_local_history_local_series_invalid")
        if type(self.semantic) is not CloudSemanticObservation:
            raise TypeError("cloud_local_history_semantic_invalid")
        _validate_identity_and_semantic(
            self.cloud_history,
            self.local_series,
            self.semantic,
        )
        _bounded_int(
            self.alignment_tolerance_seconds,
            minimum=1,
            maximum=_MAX_ALIGNMENT_TOLERANCE_SECONDS,
            reason="cloud_local_history_tolerance_invalid",
        )
        if type(self.status) is not str:
            raise TypeError("cloud_local_history_status_invalid")
        if self.status not in _STATUSES:
            raise ValueError("cloud_local_history_status_invalid")
        if type(self.candidates) is not tuple:
            raise TypeError("cloud_local_history_candidates_invalid")
        if any(
            type(candidate) is not CloudLocalHistoryCandidate
            for candidate in self.candidates
        ):
            raise TypeError("cloud_local_history_candidate_invalid")

        expected_status, expected_candidates = _correlate(
            self.cloud_history,
            self.local_series,
            alignment_tolerance_seconds=self.alignment_tolerance_seconds,
        )
        if self.status != expected_status or self.candidates != expected_candidates:
            raise ValueError("cloud_local_history_derived_verdict_mismatch")

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_LOCAL_HISTORY_CORRELATION_SCHEMA_VERSION,
            "authority": CLOUD_LOCAL_HISTORY_CORRELATION_AUTHORITY,
            "local_mapping": CLOUD_LOCAL_HISTORY_MAPPING,
            "local_mapping_proven": False,
            "minimum_aligned_samples": _MIN_ALIGNED_SAMPLES,
            "minimum_distinct_values": _MIN_DISTINCT_VALUES,
            "alignment_tolerance_seconds": self.alignment_tolerance_seconds,
            "cloud_history": self.cloud_history.to_record(),
            "local_series": self.local_series.to_record(),
            "semantic": self.semantic.to_record(),
            "status": self.status,
            "candidates": [item.to_record() for item in self.candidates],
            "candidate_count": self.candidate_count,
        }

    @classmethod
    def from_record(
        cls,
        record: object,
    ) -> "CloudLocalHistoryCorrelationReport | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "local_mapping",
            "local_mapping_proven",
            "minimum_aligned_samples",
            "minimum_distinct_values",
            "alignment_tolerance_seconds",
            "cloud_history",
            "local_series",
            "semantic",
            "status",
            "candidates",
            "candidate_count",
        }:
            return None
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"]
            != CLOUD_LOCAL_HISTORY_CORRELATION_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != CLOUD_LOCAL_HISTORY_CORRELATION_AUTHORITY
            or type(record["local_mapping"]) is not str
            or record["local_mapping"] != CLOUD_LOCAL_HISTORY_MAPPING
            or record["local_mapping_proven"] is not False
            or type(record["minimum_aligned_samples"]) is not int
            or record["minimum_aligned_samples"] != _MIN_ALIGNED_SAMPLES
            or type(record["minimum_distinct_values"]) is not int
            or record["minimum_distinct_values"] != _MIN_DISTINCT_VALUES
            or type(record["candidates"]) is not list
        ):
            return None
        cloud_history = CloudHistorySeries.from_record(
            record["cloud_history"]
        )
        local_series = LocalRegisterSnapshotSeries.from_record(
            record["local_series"]
        )
        semantic = CloudSemanticObservation.from_record(record["semantic"])
        if cloud_history is None or local_series is None or semantic is None:
            return None
        candidates: list[CloudLocalHistoryCandidate] = []
        for raw_candidate in record["candidates"]:
            candidate = CloudLocalHistoryCandidate.from_record(raw_candidate)
            if candidate is None:
                return None
            candidates.append(candidate)
        try:
            report = cls(
                cloud_history=cloud_history,
                local_series=local_series,
                semantic=semantic,
                alignment_tolerance_seconds=record[
                    "alignment_tolerance_seconds"
                ],
                status=record["status"],
                candidates=tuple(candidates),
            )
        except (TypeError, ValueError):
            return None
        if (
            type(record["candidate_count"]) is not int
            or record["candidate_count"] != report.candidate_count
        ):
            return None
        return report


def build_cloud_local_history_correlation_report(
    cloud_history: CloudHistorySeries,
    local_series: LocalRegisterSnapshotSeries,
    *,
    alignment_tolerance_seconds: int,
) -> CloudLocalHistoryCorrelationReport:
    """Build an exact but explicitly non-authoritative candidate report."""

    if type(cloud_history) is not CloudHistorySeries:
        raise TypeError("cloud_local_history_cloud_series_invalid")
    if type(local_series) is not LocalRegisterSnapshotSeries:
        raise TypeError("cloud_local_history_local_series_invalid")
    _bounded_int(
        alignment_tolerance_seconds,
        minimum=1,
        maximum=_MAX_ALIGNMENT_TOLERANCE_SECONDS,
        reason="cloud_local_history_tolerance_invalid",
    )
    source = cloud_history
    if not source.title:
        raise ValueError("cloud_local_history_title_missing")
    semantic = classify_cloud_semantic_observation(
        field_kind=source.field_kind,
        field_id=source.series_key,
        title=source.title,
        value="",
        observed_unit=source.unit,
        source_action=source.source_action,
    )
    _validate_identity_and_semantic(cloud_history, local_series, semantic)
    status, candidates = _correlate(
        cloud_history,
        local_series,
        alignment_tolerance_seconds=alignment_tolerance_seconds,
    )
    return CloudLocalHistoryCorrelationReport(
        cloud_history=cloud_history,
        local_series=local_series,
        semantic=semantic,
        alignment_tolerance_seconds=alignment_tolerance_seconds,
        status=status,
        candidates=candidates,
    )


@dataclass(frozen=True, slots=True)
class CloudLocalHistoryReview:
    """Bounded review composition with no mapping or activation authority.

    The collection and local series are serialized once.  Compact verdicts are
    derived from them and re-created on direct construction and record parsing,
    avoiding both duplicated register dumps and forgeable candidate summaries.
    """

    history_collection: CloudHistoryCollection
    local_series: LocalRegisterSnapshotSeries
    reports: tuple[CloudLocalHistoryCorrelationReport, ...]

    def __post_init__(self) -> None:
        if type(self.history_collection) is not CloudHistoryCollection:
            raise TypeError("cloud_local_history_review_collection_invalid")
        if type(self.local_series) is not LocalRegisterSnapshotSeries:
            raise TypeError("cloud_local_history_review_local_series_invalid")
        if not pn_is_same_identity(
            self.history_collection.identity.pn,
            self.local_series.collector_pn,
        ):
            raise ValueError("cloud_local_history_review_identity_mismatch")
        if type(self.reports) is not tuple:
            raise TypeError("cloud_local_history_review_reports_invalid")
        if any(
            type(report) is not CloudLocalHistoryCorrelationReport
            for report in self.reports
        ):
            raise TypeError("cloud_local_history_review_report_invalid")
        expected = _review_reports(
            self.history_collection,
            self.local_series,
        )
        if self.reports != expected:
            raise ValueError("cloud_local_history_review_verdict_mismatch")

    @property
    def reviewed_series_count(self) -> int:
        return len(self.reports)

    @property
    def skipped_series_count(self) -> int:
        return (
            self.history_collection.collected_series_count
            - self.reviewed_series_count
        )

    @property
    def unique_candidate_count(self) -> int:
        return sum(
            report.status == CLOUD_LOCAL_HISTORY_STATUS_UNIQUE
            for report in self.reports
        )

    @property
    def ambiguous_candidate_count(self) -> int:
        return sum(
            report.status == CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS
            for report in self.reports
        )

    @property
    def no_candidate_count(self) -> int:
        return sum(
            report.status == CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE
            for report in self.reports
        )

    @property
    def insufficient_evidence_count(self) -> int:
        return sum(
            report.status
            in {
                CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES,
                CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION,
            }
            for report in self.reports
        )

    @property
    def status(self) -> str:
        if not self.reports:
            return CLOUD_LOCAL_HISTORY_REVIEW_STATUS_NO_ELIGIBLE
        if self.unique_candidate_count or self.ambiguous_candidate_count:
            return CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES
        return CLOUD_LOCAL_HISTORY_REVIEW_STATUS_REVIEWED

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": CLOUD_LOCAL_HISTORY_REVIEW_SCHEMA_VERSION,
            "authority": CLOUD_LOCAL_HISTORY_REVIEW_AUTHORITY,
            "read_only": True,
            "local_mapping": CLOUD_LOCAL_HISTORY_MAPPING,
            "local_mapping_proven": False,
            "activation_allowed": False,
            "history_collection": self.history_collection.to_record(),
            "local_series": self.local_series.to_record(),
            "verdicts": [_review_verdict(report) for report in self.reports],
            "status": self.status,
            "reviewed_series_count": self.reviewed_series_count,
            "skipped_series_count": self.skipped_series_count,
            "unique_candidate_count": self.unique_candidate_count,
            "ambiguous_candidate_count": self.ambiguous_candidate_count,
            "no_candidate_count": self.no_candidate_count,
            "insufficient_evidence_count": self.insufficient_evidence_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CloudLocalHistoryReview | None":
        if type(record) is not dict:
            return None
        collection = CloudHistoryCollection.from_record(
            record.get("history_collection")
        )
        local_series = LocalRegisterSnapshotSeries.from_record(
            record.get("local_series")
        )
        if collection is None or local_series is None:
            return None
        try:
            review = build_cloud_local_history_review(
                collection,
                local_series,
            )
        except (TypeError, ValueError):
            return None
        # Exact equality revalidates the closed authority fields, every compact
        # verdict, all derived counts, and the absence of extra keys.
        if review.to_record() != record:
            return None
        return review


def build_cloud_local_history_review(
    history_collection: CloudHistoryCollection,
    local_series: LocalRegisterSnapshotSeries,
) -> CloudLocalHistoryReview:
    """Compose fresh typed evidence into a review-only aggregate."""

    if type(history_collection) is not CloudHistoryCollection:
        raise TypeError("cloud_local_history_review_collection_invalid")
    if type(local_series) is not LocalRegisterSnapshotSeries:
        raise TypeError("cloud_local_history_review_local_series_invalid")
    if not pn_is_same_identity(
        history_collection.identity.pn,
        local_series.collector_pn,
    ):
        raise ValueError("cloud_local_history_review_identity_mismatch")
    return CloudLocalHistoryReview(
        history_collection=history_collection,
        local_series=local_series,
        reports=_review_reports(history_collection, local_series),
    )


def _review_reports(
    history_collection: CloudHistoryCollection,
    local_series: LocalRegisterSnapshotSeries,
) -> tuple[CloudLocalHistoryCorrelationReport, ...]:
    reports: list[CloudLocalHistoryCorrelationReport] = []
    for cloud_history in history_collection.series:
        tolerance = _review_alignment_tolerance_seconds(
            cloud_history,
            local_series,
        )
        try:
            report = build_cloud_local_history_correlation_report(
                cloud_history,
                local_series,
                alignment_tolerance_seconds=tolerance,
            )
        except ValueError as exc:
            # Missing/unknown/unit-conflicted semantic fields are valid cloud
            # evidence but are not eligible for register correlation.  All
            # structural, identity, and timestamp failures still propagate.
            if str(exc) not in {
                "cloud_local_history_title_missing",
                "cloud_local_history_semantic_untrusted",
            }:
                raise
            continue
        reports.append(report)
    return tuple(reports)


def _review_alignment_tolerance_seconds(
    cloud_history: CloudHistorySeries,
    local_series: LocalRegisterSnapshotSeries,
) -> int:
    """Derive the nearest-neighbour window from observed source cadences."""

    precision_minutes = cloud_history.precision_minutes
    cloud_cadence = (
        precision_minutes * 60
        if precision_minutes > 0
        else local_series.sample_interval_seconds
    )
    # Half of the tighter cadence is the largest non-overlapping nearest-point
    # window.  Exact midpoint ties remain rejected by the correlator.
    return max(
        1,
        min(cloud_cadence, local_series.sample_interval_seconds) // 2,
    )


def _review_verdict(
    report: CloudLocalHistoryCorrelationReport,
) -> dict[str, Any]:
    source = report.cloud_history
    return {
        "source_action": source.source_action,
        "series_key": source.series_key,
        "semantic": report.semantic.to_record(),
        "alignment_tolerance_seconds": report.alignment_tolerance_seconds,
        "status": report.status,
        "candidates": [candidate.to_record() for candidate in report.candidates],
        "candidate_count": report.candidate_count,
    }


def _validate_identity_and_semantic(
    cloud_history: CloudHistorySeries,
    local_series: LocalRegisterSnapshotSeries,
    semantic: CloudSemanticObservation,
) -> None:
    source = cloud_history
    if not pn_is_same_identity(
        source.identity.pn,
        local_series.collector_pn,
    ):
        raise ValueError("cloud_local_history_identity_mismatch")
    expected_semantic = classify_cloud_semantic_observation(
        field_kind=source.field_kind,
        field_id=source.series_key,
        title=source.title,
        value="",
        observed_unit=source.unit,
        source_action=source.source_action,
    )
    exact_power_scale = (
        expected_semantic.status == CLOUD_SEMANTIC_STATUS_UNIT_CONFLICT
        and expected_semantic.device_class == "power"
        and expected_semantic.observed_unit.casefold() == "kw"
        and expected_semantic.expected_unit.casefold() == "w"
    )
    if (
        semantic != expected_semantic
        or (
            semantic.status != CLOUD_SEMANTIC_STATUS_RECOGNIZED
            and not exact_power_scale
        )
        or semantic.semantic_kind not in {"read", "both"}
    ):
        raise ValueError("cloud_local_history_semantic_untrusted")


def _correlate(
    cloud_history: CloudHistorySeries,
    local_series: LocalRegisterSnapshotSeries,
    *,
    alignment_tolerance_seconds: int,
) -> tuple[str, tuple[CloudLocalHistoryCandidate, ...]]:
    cloud_points = tuple(
        (
            _aware_datetime(
                point.utc_timestamp,
                "cloud_local_history_utc_timestamp_invalid",
            ),
            Decimal(point.value),
        )
        for point in cloud_history.points
    )
    cloud_times = tuple(item[0] for item in cloud_points)
    local_samples = _local_register_samples(local_series)

    candidates: list[CloudLocalHistoryCandidate] = []
    max_aligned = 0
    max_distinct = 0
    for location, samples in local_samples.items():
        aligned: list[tuple[Decimal, int]] = []
        for observed_at, raw_value in samples:
            cloud_value = _nearest_cloud_value(
                cloud_points,
                cloud_times,
                observed_at,
                tolerance_seconds=alignment_tolerance_seconds,
            )
            if cloud_value is not None:
                aligned.append((cloud_value, raw_value))
        distinct = len({cloud_value for cloud_value, _raw in aligned})
        max_aligned = max(max_aligned, len(aligned))
        max_distinct = max(max_distinct, distinct)
        if (
            len(aligned) < _MIN_ALIGNED_SAMPLES
            or distinct < _MIN_DISTINCT_VALUES
        ):
            continue
        for divisor in _DIVISORS:
            for signed in (False, True):
                if _exact_transform_matches(
                    aligned,
                    divisor=divisor,
                    signed=signed,
                ):
                    candidates.append(
                        CloudLocalHistoryCandidate(
                            location=location,
                            divisor=divisor,
                            signed=signed,
                            aligned_sample_count=len(aligned),
                            distinct_cloud_value_count=distinct,
                        )
                    )

    ordered = tuple(
        sorted(
            candidates,
            key=lambda item: (
                item.location,
                item.divisor,
                item.signed,
            ),
        )
    )
    if max_aligned < _MIN_ALIGNED_SAMPLES:
        status = CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES
    elif max_distinct < _MIN_DISTINCT_VALUES:
        status = CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION
    elif not ordered:
        status = CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE
    elif len(ordered) == 1:
        status = CLOUD_LOCAL_HISTORY_STATUS_UNIQUE
    else:
        status = CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS
    return status, ordered


def _local_register_samples(
    series: LocalRegisterSnapshotSeries,
) -> dict[LocalRegisterLocation, list[tuple[datetime, int]]]:
    output: dict[LocalRegisterLocation, list[tuple[datetime, int]]] = {}
    for snapshot in series.snapshots:
        for block in snapshot.blocks:
            observed_at = _aware_datetime(
                block.observed_at,
                "cloud_local_history_local_timestamp_invalid",
            )
            plan = block.plan
            for offset, raw_value in enumerate(block.values):
                location = LocalRegisterLocation(
                    devcode=plan.devcode,
                    collector_addr=plan.collector_addr,
                    device_addr=plan.device_addr,
                    function=plan.function,
                    register=plan.start + offset,
                )
                output.setdefault(location, []).append(
                    (observed_at, raw_value)
                )
    return output


def _nearest_cloud_value(
    points: tuple[tuple[datetime, Decimal], ...],
    times: tuple[datetime, ...],
    observed_at: datetime,
    *,
    tolerance_seconds: int,
) -> Decimal | None:
    if not points:
        return None
    index = bisect_left(times, observed_at)
    candidates = []
    for candidate_index in (index - 1, index):
        if 0 <= candidate_index < len(points):
            point_time, point_value = points[candidate_index]
            delta = abs((point_time - observed_at).total_seconds())
            if delta <= tolerance_seconds:
                candidates.append((delta, point_value))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        return None
    return candidates[0][1]


def _exact_transform_matches(
    samples: list[tuple[Decimal, int]],
    *,
    divisor: int,
    signed: bool,
) -> bool:
    saw_negative = False
    for cloud_value, raw_value in samples:
        scaled = cloud_value * divisor
        if scaled != scaled.to_integral_value():
            return False
        expected = int(scaled)
        saw_negative = saw_negative or expected < 0
        observed = (
            raw_value - 0x10000
            if signed and raw_value & 0x8000
            else raw_value
        )
        if observed != expected:
            return False
    # Positive words do not prove signed encoding.  Keep their sole canonical
    # candidate unsigned; signed is evidence only when a negative sample was
    # actually observed.
    return signed == saw_negative


__all__ = [
    "CLOUD_LOCAL_HISTORY_CORRELATION_AUTHORITY",
    "CLOUD_LOCAL_HISTORY_CORRELATION_SCHEMA_VERSION",
    "CLOUD_LOCAL_HISTORY_MAPPING",
    "CLOUD_LOCAL_HISTORY_STATUS_AMBIGUOUS",
    "CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_SAMPLES",
    "CLOUD_LOCAL_HISTORY_STATUS_INSUFFICIENT_VARIATION",
    "CLOUD_LOCAL_HISTORY_STATUS_NO_EXACT_CANDIDATE",
    "CLOUD_LOCAL_HISTORY_STATUS_UNIQUE",
    "CLOUD_LOCAL_HISTORY_REVIEW_AUTHORITY",
    "CLOUD_LOCAL_HISTORY_REVIEW_SCHEMA_VERSION",
    "CLOUD_LOCAL_HISTORY_REVIEW_STATUS_CANDIDATES",
    "CLOUD_LOCAL_HISTORY_REVIEW_STATUS_NO_ELIGIBLE",
    "CLOUD_LOCAL_HISTORY_REVIEW_STATUS_REVIEWED",
    "CloudLocalHistoryCandidate",
    "CloudLocalHistoryCorrelationReport",
    "CloudLocalHistoryReview",
    "LocalRegisterLocation",
    "build_cloud_local_history_correlation_report",
    "build_cloud_local_history_review",
]
