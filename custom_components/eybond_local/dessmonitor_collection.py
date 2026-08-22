"""Bounded single-login DESSMonitor metadata and history collection.

History is supplemental read-only evidence.  Metadata remains usable when the
device timezone or an individual history action is unavailable, and no error
text, credential, session token, or signing secret enters the normalized
collection record.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any, Callable

from .dessmonitor_cloud import (
    DEFAULT_BASE_URL,
    DEFAULT_MAX_CONTROL_VALUES,
    DEFAULT_TIMEOUT,
    DessMonitorCloudError,
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    fetch_read_only_evidence_for_session,
    login_with_password,
)
from .dessmonitor_history import (
    DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
    DessMonitorHistorySeries,
    fetch_key_parameter_history,
    fetch_sole_chart_history,
)
from .dessmonitor_history_resolution import (
    DessMonitorResolvedHistorySeries,
    resolve_dessmonitor_history_time_basis,
)
from .dessmonitor_time_basis import (
    DessMonitorDeviceTimeBasis,
    fetch_device_time_basis,
)


DESSMONITOR_COLLECTION_SCHEMA_VERSION = 1
DESSMONITOR_COLLECTION_AUTHORITY = "bounded_read_only_history_collection"

DESSMONITOR_COLLECTION_STATUS_COMPLETE = "complete"
DESSMONITOR_COLLECTION_STATUS_PARTIAL = "partial"
DESSMONITOR_COLLECTION_STATUS_UNAVAILABLE = "unavailable"
DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE = (
    "time_basis_unavailable"
)

_MAX_HISTORY_SERIES = 8
_DEFAULT_HISTORY_BUDGET_SECONDS = 30.0
_COLLECTION_STATUSES = frozenset(
    {
        DESSMONITOR_COLLECTION_STATUS_COMPLETE,
        DESSMONITOR_COLLECTION_STATUS_PARTIAL,
        DESSMONITOR_COLLECTION_STATUS_UNAVAILABLE,
        DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
    }
)


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


def _positive_seconds(value: object, *, reason: str) -> float:
    if type(value) not in {int, float} or isinstance(value, bool):
        raise TypeError(reason)
    if value <= 0:
        raise ValueError(reason)
    return float(value)


def _requested_date(value: object, *, allow_empty: bool) -> str:
    if type(value) is not str:
        raise TypeError("dessmonitor_collection_date_invalid")
    if not value:
        if allow_empty:
            return value
        raise ValueError("dessmonitor_collection_date_invalid")
    if value != value.strip():
        raise ValueError("dessmonitor_collection_date_invalid")
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("dessmonitor_collection_date_invalid") from exc
    if parsed.isoformat() != value:
        raise ValueError("dessmonitor_collection_date_invalid")
    return value


@dataclass(frozen=True, slots=True)
class DessMonitorHistoryCollection:
    """One bounded history attempt for one exact cloud device identity."""

    identity: DessMonitorDeviceIdentity
    time_basis: DessMonitorDeviceTimeBasis | None
    requested_date: str
    attempted_series_count: int
    failed_series_count: int
    budget_exhausted: bool
    series: tuple[DessMonitorResolvedHistorySeries, ...]

    def __post_init__(self) -> None:
        if type(self.identity) is not DessMonitorDeviceIdentity:
            raise TypeError("dessmonitor_collection_identity_invalid")
        if self.time_basis is not None and type(
            self.time_basis
        ) is not DessMonitorDeviceTimeBasis:
            raise TypeError("dessmonitor_collection_time_basis_invalid")
        if (
            self.time_basis is not None
            and self.time_basis.identity != self.identity
        ):
            raise ValueError("dessmonitor_collection_identity_mismatch")
        _requested_date(
            self.requested_date,
            allow_empty=self.time_basis is None,
        )
        _bounded_int(
            self.attempted_series_count,
            minimum=0,
            maximum=_MAX_HISTORY_SERIES,
            reason="dessmonitor_collection_attempted_count_invalid",
        )
        _bounded_int(
            self.failed_series_count,
            minimum=0,
            maximum=_MAX_HISTORY_SERIES,
            reason="dessmonitor_collection_failed_count_invalid",
        )
        if type(self.budget_exhausted) is not bool:
            raise TypeError("dessmonitor_collection_budget_state_invalid")
        if type(self.series) is not tuple:
            raise TypeError("dessmonitor_collection_series_invalid")
        if len(self.series) > _MAX_HISTORY_SERIES:
            raise ValueError("dessmonitor_collection_series_limit_exceeded")
        keys: set[tuple[str, str]] = set()
        for item in self.series:
            if type(item) is not DessMonitorResolvedHistorySeries:
                raise TypeError("dessmonitor_collection_series_item_invalid")
            if (
                item.source_series.identity != self.identity
                or item.time_basis != self.time_basis
                or item.source_series.requested_date != self.requested_date
            ):
                raise ValueError("dessmonitor_collection_series_mismatch")
            key = (
                item.source_series.source_action,
                item.source_series.series_key,
            )
            if key in keys:
                raise ValueError("dessmonitor_collection_series_duplicate")
            keys.add(key)
        if self.time_basis is None:
            if (
                self.requested_date
                or self.attempted_series_count
                or self.failed_series_count
                or self.series
            ):
                raise ValueError("dessmonitor_collection_time_basis_shape_invalid")
        elif (
            self.attempted_series_count == 0
            and not self.budget_exhausted
        ):
            raise ValueError("dessmonitor_collection_empty_attempt_invalid")
        elif len(self.series) + self.failed_series_count != self.attempted_series_count:
            raise ValueError("dessmonitor_collection_count_mismatch")

    @property
    def status(self) -> str:
        if self.time_basis is None:
            return DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE
        if not self.series:
            return DESSMONITOR_COLLECTION_STATUS_UNAVAILABLE
        if self.failed_series_count or self.budget_exhausted:
            return DESSMONITOR_COLLECTION_STATUS_PARTIAL
        return DESSMONITOR_COLLECTION_STATUS_COMPLETE

    @property
    def collected_series_count(self) -> int:
        return len(self.series)

    @property
    def point_count(self) -> int:
        return sum(item.point_count for item in self.series)

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": DESSMONITOR_COLLECTION_SCHEMA_VERSION,
            "authority": DESSMONITOR_COLLECTION_AUTHORITY,
            "provider_id": "smartess",
            "source_id": "dessmonitor",
            "read_only": True,
            "local_mapping_proven": False,
            "activation_allowed": False,
            "identity": self.identity.to_record(),
            "time_basis": (
                self.time_basis.to_record()
                if self.time_basis is not None
                else None
            ),
            "requested_date": self.requested_date,
            "attempted_series_count": self.attempted_series_count,
            "failed_series_count": self.failed_series_count,
            "budget_exhausted": self.budget_exhausted,
            "series": [item.to_record() for item in self.series],
            "status": self.status,
            "collected_series_count": self.collected_series_count,
            "point_count": self.point_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "DessMonitorHistoryCollection | None":
        if type(record) is not dict or set(record) != {
            "schema_version",
            "authority",
            "provider_id",
            "source_id",
            "read_only",
            "local_mapping_proven",
            "activation_allowed",
            "identity",
            "time_basis",
            "requested_date",
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
            or record["schema_version"] != DESSMONITOR_COLLECTION_SCHEMA_VERSION
            or type(record["authority"]) is not str
            or record["authority"] != DESSMONITOR_COLLECTION_AUTHORITY
            or type(record["provider_id"]) is not str
            or record["provider_id"] != "smartess"
            or type(record["source_id"]) is not str
            or record["source_id"] != "dessmonitor"
            or record["read_only"] is not True
            or record["local_mapping_proven"] is not False
            or record["activation_allowed"] is not False
            or type(record["series"]) is not list
            or type(record["budget_exhausted"]) is not bool
            or type(record["status"]) is not str
            or record["status"] not in _COLLECTION_STATUSES
        ):
            return None
        identity_record = record["identity"]
        if type(identity_record) is not dict or set(identity_record) != {
            "pn",
            "sn",
            "devcode",
            "devaddr",
        }:
            return None
        try:
            identity = DessMonitorDeviceIdentity(
                pn=identity_record["pn"],
                sn=identity_record["sn"],
                devcode=identity_record["devcode"],
                devaddr=identity_record["devaddr"],
            )
        except (TypeError, ValueError, KeyError):
            return None
        raw_time_basis = record["time_basis"]
        if raw_time_basis is None:
            time_basis = None
        else:
            time_basis = DessMonitorDeviceTimeBasis.from_record(raw_time_basis)
            if time_basis is None:
                return None
        series: list[DessMonitorResolvedHistorySeries] = []
        for raw_series in record["series"]:
            item = DessMonitorResolvedHistorySeries.from_record(raw_series)
            if item is None:
                return None
            series.append(item)
        try:
            collection = cls(
                identity=identity,
                time_basis=time_basis,
                requested_date=record["requested_date"],
                attempted_series_count=record["attempted_series_count"],
                failed_series_count=record["failed_series_count"],
                budget_exhausted=record["budget_exhausted"],
                series=tuple(series),
            )
        except (TypeError, ValueError):
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


def _device_local_date(
    time_basis: DessMonitorDeviceTimeBasis,
    *,
    utc_now: datetime | None,
) -> str:
    if type(time_basis) is not DessMonitorDeviceTimeBasis:
        raise TypeError("dessmonitor_collection_time_basis_invalid")
    current = utc_now if utc_now is not None else datetime.now(timezone.utc)
    if type(current) is not datetime:
        raise TypeError("dessmonitor_collection_clock_invalid")
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("dessmonitor_collection_clock_invalid")
    device_zone = timezone(timedelta(seconds=time_basis.offset_seconds))
    return current.astimezone(device_zone).date().isoformat()


def _labeled_key_parameter_series(
    series: DessMonitorHistorySeries,
    *,
    title: str,
    unit: str,
) -> DessMonitorHistorySeries:
    if type(series) is not DessMonitorHistorySeries:
        raise TypeError("dessmonitor_collection_history_series_invalid")
    if series.source_action != DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER:
        raise ValueError("dessmonitor_collection_history_source_invalid")
    return DessMonitorHistorySeries(
        identity=series.identity,
        source_action=series.source_action,
        series_key=series.series_key,
        title=title,
        unit=unit,
        requested_date=series.requested_date,
        precision_minutes=series.precision_minutes,
        points=series.points,
    )


def fetch_read_only_evidence_with_history(
    *,
    username: str,
    password: str,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
    max_control_values: int = DEFAULT_MAX_CONTROL_VALUES,
    max_history_series: int = _MAX_HISTORY_SERIES,
    history_budget_seconds: float = _DEFAULT_HISTORY_BUDGET_SECONDS,
    utc_now: datetime | None = None,
    progress: Callable[[str], None] | None = None,
) -> tuple[DessMonitorEvidenceBundle, DessMonitorHistoryCollection]:
    """Fetch metadata plus bounded supplemental history with one login."""

    _bounded_int(
        max_history_series,
        minimum=1,
        maximum=_MAX_HISTORY_SERIES,
        reason="dessmonitor_collection_history_limit_invalid",
    )
    request_timeout = _positive_seconds(
        timeout,
        reason="dessmonitor_timeout_invalid",
    )
    history_budget = _positive_seconds(
        history_budget_seconds,
        reason="dessmonitor_collection_history_budget_invalid",
    )
    if utc_now is not None:
        if type(utc_now) is not datetime:
            raise TypeError("dessmonitor_collection_clock_invalid")
        if utc_now.tzinfo is None or utc_now.utcoffset() is None:
            raise ValueError("dessmonitor_collection_clock_invalid")
    if progress is not None and not callable(progress):
        raise TypeError("dessmonitor_collection_progress_invalid")

    def report(stage: str) -> None:
        if progress is not None:
            progress(stage)

    _, session = login_with_password(
        username=username,
        password=password,
        base_url=base_url,
        timeout=timeout,
    )
    report("authSource")
    bundle = fetch_read_only_evidence_for_session(
        session=session,
        collector_pn=collector_pn,
        base_url=base_url,
        timeout=timeout,
        max_control_values=max_control_values,
        progress=progress,
    )
    history_deadline = monotonic() + history_budget

    def remaining_timeout() -> float:
        return min(request_timeout, max(0.0, history_deadline - monotonic()))

    time_basis_timeout = remaining_timeout()
    if time_basis_timeout <= 0:
        report("history_complete")
        return bundle, DessMonitorHistoryCollection(
            identity=bundle.identity,
            time_basis=None,
            requested_date="",
            attempted_series_count=0,
            failed_series_count=0,
            budget_exhausted=True,
            series=(),
        )
    try:
        time_basis = fetch_device_time_basis(
            session=session,
            identity=bundle.identity,
            base_url=base_url,
            timeout=time_basis_timeout,
        )
    except (DessMonitorCloudError, TypeError, ValueError):
        report("queryDeviceInfo")
        report("history_complete")
        return bundle, DessMonitorHistoryCollection(
            identity=bundle.identity,
            time_basis=None,
            requested_date="",
            attempted_series_count=0,
            failed_series_count=0,
            budget_exhausted=remaining_timeout() <= 0,
            series=(),
        )
    report("queryDeviceInfo")

    requested_date = _device_local_date(time_basis, utc_now=utc_now)
    series: list[DessMonitorResolvedHistorySeries] = []
    attempted = 0
    failed = 0
    budget_exhausted = False

    seen_parameters: set[str] = set()
    for field in bundle.key_parameters:
        if attempted >= max_history_series or budget_exhausted:
            break
        if not field.field_id or field.field_id in seen_parameters:
            continue
        history_timeout = remaining_timeout()
        if history_timeout <= 0:
            budget_exhausted = True
            break
        seen_parameters.add(field.field_id)
        attempted += 1
        try:
            raw_series = fetch_key_parameter_history(
                session=session,
                identity=bundle.identity,
                parameter=field.field_id,
                requested_date=requested_date,
                base_url=base_url,
                timeout=history_timeout,
            )
            labeled = _labeled_key_parameter_series(
                raw_series,
                title=field.title,
                unit=field.unit,
            )
            resolved_parameter = resolve_dessmonitor_history_time_basis(
                labeled,
                time_basis,
            )
        except (DessMonitorCloudError, TypeError, ValueError):
            failed += 1
            budget_exhausted = remaining_timeout() <= 0
            report("queryDeviceKeyParameterOneDay")
            continue
        series.append(resolved_parameter)
        report("queryDeviceKeyParameterOneDay")

    # The current DESSMonitor web API publishes key-parameter history and no
    # longer serves the legacy sole-chart route.  Keep the documented chart
    # action only as a bounded fallback for accounts that expose no key list;
    # it must never consume the first attempt or the whole budget ahead of the
    # provider-advertised, working history series.
    if not bundle.key_parameters and not budget_exhausted:
        history_timeout = remaining_timeout()
        if history_timeout <= 0:
            budget_exhausted = True
        else:
            attempted += 1
            try:
                chart = fetch_sole_chart_history(
                    session=session,
                    identity=bundle.identity,
                    requested_date=requested_date,
                    base_url=base_url,
                    timeout=history_timeout,
                )
                resolved_chart = resolve_dessmonitor_history_time_basis(
                    chart,
                    time_basis,
                )
            except (DessMonitorCloudError, TypeError, ValueError):
                failed += 1
                budget_exhausted = remaining_timeout() <= 0
            else:
                series.append(resolved_chart)
            report("queryDeviceSoleChartEs")

    report("history_complete")
    return bundle, DessMonitorHistoryCollection(
        identity=bundle.identity,
        time_basis=time_basis,
        requested_date=requested_date,
        attempted_series_count=attempted,
        failed_series_count=failed,
        budget_exhausted=budget_exhausted,
        series=tuple(series),
    )


__all__ = [
    "DESSMONITOR_COLLECTION_AUTHORITY",
    "DESSMONITOR_COLLECTION_SCHEMA_VERSION",
    "DESSMONITOR_COLLECTION_STATUS_COMPLETE",
    "DESSMONITOR_COLLECTION_STATUS_PARTIAL",
    "DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE",
    "DESSMONITOR_COLLECTION_STATUS_UNAVAILABLE",
    "DessMonitorHistoryCollection",
    "fetch_read_only_evidence_with_history",
]
