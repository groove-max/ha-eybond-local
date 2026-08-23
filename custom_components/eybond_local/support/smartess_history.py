"""Bounded SmartESS metadata and daily-history collection.

The Android SmartESS client uses ``querySPKeyParameters`` followed by
``queryDeviceKeyParameterOneDay`` for its day chart.  This adapter reproduces
only those read-only calls, reuses one authenticated session, and emits the
same provider-neutral history evidence consumed by the local correlator.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging
from time import monotonic, sleep
from typing import Any, Callable
from urllib.parse import quote

from ..smartess_cloud import (
    CLOUD_ERROR_AUTH_FAILED,
    CLOUD_ERROR_NETWORK,
    CLOUD_ERROR_RATE_LIMITED,
    CLOUD_ERROR_TIMEOUT,
    CLOUD_ERROR_UNAVAILABLE,
    CLOUD_ERROR_UNEXPECTED,
    CONTROL_DISCOVERY_READ_ATTEMPTS,
    CONTROL_DISCOVERY_RETRY_DELAY_SECONDS,
    DEFAULT_BASE_URL,
    DEFAULT_TIMEOUT,
    SmartEssActionRejectedError,
    SmartEssCloudError,
    classify_smartess_cloud_error,
    fetch_device_bundle_for_collector_with_session,
    fetch_signed_action,
    login_for_control_discovery,
)
from .cloud_history_evidence import (
    CLOUD_HISTORY_STATUS_COMPLETE,
    CloudHistoryCollection,
    CloudHistoryIdentity,
    CloudHistoryPoint,
    CloudHistorySeries,
)
from .cloud_semantic_evidence import CLOUD_FIELD_KIND_KEY_PARAMETER


SMARTESS_HISTORY_SOURCE_ACTION = "queryDeviceKeyParameterOneDay"
SMARTESS_HISTORY_KEYS_ACTION = "querySPKeyParameters"
SMARTESS_HISTORY_TIME_BASIS_ACTION = "queryDeviceInfo"

SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE = "invalid_response"
SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS = "no_series_keys"
SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED = "budget_exhausted"
SMARTESS_HISTORY_FAILURE_ACTION_REJECTED = "action_rejected"

_MAX_HISTORY_SERIES = 8
_MAX_HISTORY_POINTS = 4096
_DEFAULT_HISTORY_BUDGET_SECONDS = 30.0
_MAX_DIAGNOSTIC_TOKEN_LENGTH = 128

_LOGGER = logging.getLogger(__name__)

_HISTORY_FAILURE_STAGES = frozenset(
    {
        "",
        SMARTESS_HISTORY_TIME_BASIS_ACTION,
        SMARTESS_HISTORY_KEYS_ACTION,
        SMARTESS_HISTORY_SOURCE_ACTION,
    }
)
_HISTORY_FAILURE_CODES = frozenset(
    {
        "",
        CLOUD_ERROR_AUTH_FAILED,
        CLOUD_ERROR_RATE_LIMITED,
        CLOUD_ERROR_UNAVAILABLE,
        CLOUD_ERROR_TIMEOUT,
        CLOUD_ERROR_NETWORK,
        CLOUD_ERROR_UNEXPECTED,
        SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE,
        SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
        SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED,
        SMARTESS_HISTORY_FAILURE_ACTION_REJECTED,
    }
)
_RETRYABLE_CLOUD_ERRORS = frozenset(
    {
        CLOUD_ERROR_NETWORK,
        CLOUD_ERROR_TIMEOUT,
        CLOUD_ERROR_UNAVAILABLE,
    }
)
_HISTORY_KEY_ROW_CONTAINERS = ("field", "pars", "parameters", "dat")

_HISTORY_PRESENTATION: dict[str, tuple[str, str]] = {
    "PV_OUTPUT_POWER": ("PV Power", "kW"),
    "LOAD_ACTIVE_POWER": ("Output Active Power", "kW"),
    "GRID_ACTIVE_POWER": ("Grid Power", "kW"),
    "BT_BATTERY_CAPACITY": ("Battery Capacity", "%"),
    "BATTERY_ACTIVE_POWER": ("Battery Power", "kW"),
}


def _diagnostic_token(
    value: object,
    *,
    allowed: frozenset[str],
    reason: str,
) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if (
        value != value.strip()
        or len(value) > _MAX_DIAGNOSTIC_TOKEN_LENGTH
        or value not in allowed
    ):
        raise ValueError(reason)
    return value


@dataclass(frozen=True, slots=True)
class SmartEssHistoryFetchResult:
    """SmartESS history result with non-secret provider diagnostics."""

    bundle: dict[str, Any]
    history_collection: CloudHistoryCollection
    failure_stage: str = ""
    failure_code: str = ""

    def __post_init__(self) -> None:
        if type(self.bundle) is not dict:
            raise TypeError("smartess_history_result_bundle_invalid")
        if type(self.history_collection) is not CloudHistoryCollection:
            raise TypeError("smartess_history_result_collection_invalid")
        stage = _diagnostic_token(
            self.failure_stage,
            allowed=_HISTORY_FAILURE_STAGES,
            reason="smartess_history_result_failure_stage_invalid",
        )
        code = _diagnostic_token(
            self.failure_code,
            allowed=_HISTORY_FAILURE_CODES,
            reason="smartess_history_result_failure_code_invalid",
        )
        if bool(stage) != bool(code):
            raise ValueError("smartess_history_result_failure_shape_invalid")
        if self.history_collection.status == CLOUD_HISTORY_STATUS_COMPLETE:
            if stage or code:
                raise ValueError("smartess_history_result_failure_shape_invalid")
        elif not stage or not code:
            raise ValueError("smartess_history_result_failure_shape_invalid")

    def to_diagnostics_record(self) -> dict[str, Any]:
        """Return bounded diagnostics without response bodies or credentials."""

        collection = self.history_collection
        return {
            "provider_id": "smartess",
            "source_id": "smartess",
            "history_status": collection.status,
            "failure_stage": self.failure_stage,
            "failure_code": self.failure_code,
            "attempted_series_count": collection.attempted_series_count,
            "failed_series_count": collection.failed_series_count,
            "collected_series_count": collection.collected_series_count,
            "point_count": collection.point_count,
            "budget_exhausted": collection.budget_exhausted,
        }


class _SmartEssHistoryStageFailure(RuntimeError):
    """One already-classified read-only history stage failure."""

    def __init__(self, *, stage: str, code: str) -> None:
        self.stage = stage
        self.code = code
        super().__init__(f"smartess_history_stage_failed:{stage}:{code}")


def _positive_seconds(value: object, reason: str) -> float:
    if type(value) not in {int, float} or isinstance(value, bool):
        raise TypeError(reason)
    if value <= 0:
        raise ValueError(reason)
    return float(value)


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


def _identity_from_bundle(bundle: object) -> CloudHistoryIdentity:
    if type(bundle) is not dict:
        raise TypeError("smartess_history_bundle_invalid")
    request = bundle.get("request")
    params = request.get("params") if type(request) is dict else None
    if type(params) is not dict:
        raise ValueError("smartess_history_identity_missing")
    return CloudHistoryIdentity(
        pn=params.get("pn"),
        sn=params.get("sn"),
        devcode=params.get("devcode"),
        devaddr=params.get("devaddr"),
    )


def _same_identity(row: object, identity: CloudHistoryIdentity) -> bool:
    return (
        type(row) is dict
        and type(row.get("pn")) is str
        and row.get("pn") == identity.pn
        and type(row.get("sn")) is str
        and row.get("sn") == identity.sn
        and type(row.get("devcode")) is int
        and row.get("devcode") == identity.devcode
        and type(row.get("devaddr")) is int
        and row.get("devaddr") == identity.devaddr
    )


def _timezone_offset(dat: object, identity: CloudHistoryIdentity) -> int:
    if type(dat) is not list:
        raise SmartEssCloudError("smartess_history_time_basis_invalid")
    matches = [row for row in dat if _same_identity(row, identity)]
    if len(matches) != 1:
        raise SmartEssCloudError(
            f"smartess_history_time_basis_ambiguous:{len(matches)}"
        )
    offset = matches[0].get("timezone")
    return _bounded_int(
        offset,
        minimum=-18 * 3600,
        maximum=18 * 3600,
        reason="smartess_history_timezone_invalid",
    )


def _history_keys(dat: object) -> tuple[str, ...]:
    values: list[object] | None = None
    rows: list[object] | None = None
    if type(dat) is list:
        rows = dat
    elif type(dat) is dict:
        if "keys" in dat:
            raw_values = dat["keys"]
            if type(raw_values) is not list:
                raise SmartEssCloudError("smartess_history_keys_invalid")
            if any(key in dat for key in _HISTORY_KEY_ROW_CONTAINERS):
                raise SmartEssCloudError("smartess_history_keys_ambiguous")
            values = raw_values
        else:
            present = [key for key in _HISTORY_KEY_ROW_CONTAINERS if key in dat]
            if len(present) != 1 or type(dat[present[0]]) is not list:
                raise SmartEssCloudError("smartess_history_keys_invalid")
            rows = dat[present[0]]
    else:
        raise SmartEssCloudError("smartess_history_keys_invalid")

    if rows is not None:
        values = []
        for row in rows:
            if type(row) is not dict:
                raise SmartEssCloudError("smartess_history_key_row_invalid")
            field_id: object | None = None
            for key in ("e0", "id", "field"):
                if key in row:
                    field_id = row[key]
                    break
            if field_id is None:
                raise SmartEssCloudError("smartess_history_key_id_invalid")
            values.append(field_id)

    if values is None:
        raise SmartEssCloudError("smartess_history_keys_invalid")
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if (
            type(value) is not str
            or not value
            or value != value.strip()
            or len(value) > 512
        ):
            raise SmartEssCloudError("smartess_history_key_id_invalid")
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
        if len(output) >= _MAX_HISTORY_SERIES:
            break
    return tuple(output)


def _presentation(series_key: str) -> tuple[str, str]:
    known = _HISTORY_PRESENTATION.get(series_key)
    if known is not None:
        return known
    title = " ".join(part.capitalize() for part in series_key.split("_") if part)
    return title or series_key, ""


def _diagnostic_series_label(series_key: str) -> str:
    """Return a bounded known label without logging provider-owned text."""

    return series_key if series_key in _HISTORY_PRESENTATION else "unrecognized"


def _numeric_text(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError("smartess_history_value_invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("smartess_history_value_invalid") from exc
    if not parsed.is_finite():
        raise ValueError("smartess_history_value_invalid")
    return value


def _history_points(
    dat: object,
    *,
    requested_date: str,
    offset_seconds: int,
) -> tuple[CloudHistoryPoint, ...]:
    raw = dat.get("parameter") if type(dat) is dict else None
    if type(raw) is not list:
        raise SmartEssCloudError("smartess_history_points_invalid")
    if not raw:
        raise SmartEssCloudError("smartess_history_points_empty")
    device_zone = timezone(timedelta(seconds=offset_seconds))
    points: list[CloudHistoryPoint] = []
    previous = ""
    for row in raw:
        if type(row) is not dict:
            raise SmartEssCloudError("smartess_history_point_invalid")
        timestamp = row.get("ts")
        value = row.get("val")
        if type(timestamp) is not str:
            raise SmartEssCloudError("smartess_history_timestamp_invalid")
        try:
            local = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise SmartEssCloudError(
                "smartess_history_timestamp_invalid"
            ) from exc
        if (
            local.strftime("%Y-%m-%d %H:%M:%S") != timestamp
            or local.date().isoformat() != requested_date
            or previous and timestamp <= previous
        ):
            raise SmartEssCloudError("smartess_history_timestamp_invalid")
        previous = timestamp
        utc_timestamp = local.replace(tzinfo=device_zone).astimezone(
            timezone.utc
        ).isoformat()
        points.append(
            CloudHistoryPoint(
                device_local_timestamp=timestamp,
                utc_timestamp=utc_timestamp,
                value=_numeric_text(value),
            )
        )
        if len(points) > _MAX_HISTORY_POINTS:
            raise SmartEssCloudError("smartess_history_points_limit_exceeded")
    return tuple(points)


def _fetch_history_action_dat(
    *,
    stage: str,
    action: str,
    session: object,
    base_url: str,
    remaining_timeout: Callable[[], float],
) -> object:
    """Fetch one history action with bounded transient retries."""

    for attempt in range(1, CONTROL_DISCOVERY_READ_ATTEMPTS + 1):
        request_timeout = remaining_timeout()
        if request_timeout <= 0:
            raise _SmartEssHistoryStageFailure(
                stage=stage,
                code=SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED,
            )
        try:
            return fetch_signed_action(
                action=action,
                session=session,
                base_url=base_url,
                timeout=request_timeout,
            ).dat
        except Exception as exc:
            code = (
                SMARTESS_HISTORY_FAILURE_ACTION_REJECTED
                if isinstance(exc, SmartEssActionRejectedError)
                else classify_smartess_cloud_error(exc)
            )
            if (
                code in _RETRYABLE_CLOUD_ERRORS
                and attempt < CONTROL_DISCOVERY_READ_ATTEMPTS
                and remaining_timeout()
                > CONTROL_DISCOVERY_RETRY_DELAY_SECONDS
            ):
                _LOGGER.warning(
                    "Retrying read-only SmartESS history stage=%s "
                    "attempt=%d/%d error=%s",
                    stage,
                    attempt + 1,
                    CONTROL_DISCOVERY_READ_ATTEMPTS,
                    code,
                )
                sleep(CONTROL_DISCOVERY_RETRY_DELAY_SECONDS)
                continue
            raise _SmartEssHistoryStageFailure(
                stage=stage,
                code=code,
            ) from exc
    raise AssertionError("unreachable")


def _device_action(
    identity: CloudHistoryIdentity,
    *,
    action: str,
    extra: tuple[tuple[str, str], ...] = (),
) -> str:
    parts = [
        f"&action={quote(action, safe='')}",
        f"&pn={quote(identity.pn, safe='')}",
        f"&sn={quote(identity.sn, safe='')}",
        f"&devcode={identity.devcode}",
        f"&devaddr={identity.devaddr}",
    ]
    parts.extend(
        f"&{quote(key, safe='')}={quote(value, safe='')}"
        for key, value in extra
    )
    return "".join(parts)


def fetch_smartess_evidence_with_history(
    *,
    username: str,
    password: str,
    collector_pn: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT,
    max_history_series: int = _MAX_HISTORY_SERIES,
    history_budget_seconds: float = _DEFAULT_HISTORY_BUDGET_SECONDS,
    utc_now: datetime | None = None,
    progress: Callable[[str], None] | None = None,
    progress_detail: Callable[[str, int, int], None] | None = None,
) -> SmartEssHistoryFetchResult:
    """Fetch SmartESS metadata plus bounded daily history with one login."""

    _bounded_int(
        max_history_series,
        minimum=1,
        maximum=_MAX_HISTORY_SERIES,
        reason="smartess_history_series_limit_invalid",
    )
    request_timeout = _positive_seconds(timeout, "smartess_history_timeout_invalid")
    history_budget = _positive_seconds(
        history_budget_seconds,
        "smartess_history_budget_invalid",
    )
    current = utc_now if utc_now is not None else datetime.now(timezone.utc)
    if (
        type(current) is not datetime
        or current.tzinfo is None
        or current.utcoffset() is None
    ):
        raise ValueError("smartess_history_clock_invalid")
    if progress is not None and not callable(progress):
        raise TypeError("smartess_history_progress_invalid")
    if progress_detail is not None and not callable(progress_detail):
        raise TypeError("smartess_history_progress_detail_invalid")

    def report(stage: str) -> None:
        if progress is not None:
            progress(stage)

    def fetch_result(
        collection: CloudHistoryCollection,
        *,
        failure_stage: str = "",
        failure_code: str = "",
    ) -> SmartEssHistoryFetchResult:
        return SmartEssHistoryFetchResult(
            bundle=bundle,
            history_collection=collection,
            failure_stage=failure_stage,
            failure_code=failure_code,
        )

    _, session = login_for_control_discovery(
        username=username,
        password=password,
        base_url=base_url,
        timeout=timeout,
    )
    report("authSource")
    bundle = fetch_device_bundle_for_collector_with_session(
        session=session,
        collector_pn=collector_pn,
        base_url=base_url,
        timeout=timeout,
        include_energy_flow=False,
        transient_attempts=CONTROL_DISCOVERY_READ_ATTEMPTS,
        retry_delay_seconds=CONTROL_DISCOVERY_RETRY_DELAY_SECONDS,
    )
    report("metadata_bundle")
    identity = _identity_from_bundle(bundle)
    deadline = monotonic() + history_budget

    def remaining_timeout() -> float:
        return min(request_timeout, max(0.0, deadline - monotonic()))

    device = ",".join(
        (
            identity.pn,
            str(identity.devcode),
            str(identity.devaddr),
            identity.sn,
        )
    )
    try:
        basis_dat = _fetch_history_action_dat(
            stage=SMARTESS_HISTORY_TIME_BASIS_ACTION,
            action=(
                f"&action={SMARTESS_HISTORY_TIME_BASIS_ACTION}"
                f"&device={quote(device, safe='')}"
            ),
            session=session,
            base_url=base_url,
            remaining_timeout=remaining_timeout,
        )
    except _SmartEssHistoryStageFailure as exc:
        report(SMARTESS_HISTORY_TIME_BASIS_ACTION)
        return fetch_result(
            CloudHistoryCollection(
                provider_id="smartess",
                source_id="smartess",
                identity=identity,
                requested_date="",
                timezone_offset_seconds=None,
                attempted_series_count=0,
                failed_series_count=0,
                budget_exhausted=(
                    exc.code == SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED
                ),
                series=(),
            ),
            failure_stage=exc.stage,
            failure_code=exc.code,
        )
    try:
        offset_seconds = _timezone_offset(basis_dat, identity)
    except (SmartEssCloudError, TypeError, ValueError):
        report(SMARTESS_HISTORY_TIME_BASIS_ACTION)
        return fetch_result(
            CloudHistoryCollection(
                provider_id="smartess",
                source_id="smartess",
                identity=identity,
                requested_date="",
                timezone_offset_seconds=None,
                attempted_series_count=0,
                failed_series_count=0,
                budget_exhausted=remaining_timeout() <= 0,
                series=(),
            ),
            failure_stage=SMARTESS_HISTORY_TIME_BASIS_ACTION,
            failure_code=SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE,
        )
    report(SMARTESS_HISTORY_TIME_BASIS_ACTION)

    device_zone = timezone(timedelta(seconds=offset_seconds))
    requested_date = current.astimezone(device_zone).date().isoformat()
    try:
        keys_dat = _fetch_history_action_dat(
            stage=SMARTESS_HISTORY_KEYS_ACTION,
            action=(
                f"&action={SMARTESS_HISTORY_KEYS_ACTION}"
                f"&devcode={identity.devcode}"
            ),
            session=session,
            base_url=base_url,
            remaining_timeout=remaining_timeout,
        )
    except _SmartEssHistoryStageFailure as exc:
        report(SMARTESS_HISTORY_KEYS_ACTION)
        return fetch_result(
            CloudHistoryCollection(
                provider_id="smartess",
                source_id="smartess",
                identity=identity,
                requested_date=requested_date,
                timezone_offset_seconds=offset_seconds,
                attempted_series_count=0,
                failed_series_count=0,
                budget_exhausted=(
                    exc.code == SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED
                ),
                series=(),
            ),
            failure_stage=exc.stage,
            failure_code=exc.code,
        )
    try:
        keys = _history_keys(keys_dat)[:max_history_series]
    except (SmartEssCloudError, TypeError, ValueError):
        report(SMARTESS_HISTORY_KEYS_ACTION)
        return fetch_result(
            CloudHistoryCollection(
                provider_id="smartess",
                source_id="smartess",
                identity=identity,
                requested_date=requested_date,
                timezone_offset_seconds=offset_seconds,
                attempted_series_count=0,
                failed_series_count=0,
                budget_exhausted=remaining_timeout() <= 0,
                series=(),
            ),
            failure_stage=SMARTESS_HISTORY_KEYS_ACTION,
            failure_code=SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE,
        )
    report(SMARTESS_HISTORY_KEYS_ACTION)
    if not keys:
        report("history_complete")
        return fetch_result(
            CloudHistoryCollection(
                provider_id="smartess",
                source_id="smartess",
                identity=identity,
                requested_date=requested_date,
                timezone_offset_seconds=offset_seconds,
                attempted_series_count=0,
                failed_series_count=0,
                budget_exhausted=False,
                series=(),
            ),
            failure_stage=SMARTESS_HISTORY_KEYS_ACTION,
            failure_code=SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS,
        )

    attempted = 0
    failed = 0
    budget_exhausted = False
    failure_stage = ""
    failure_code = ""
    series: list[CloudHistorySeries] = []
    for series_key in keys:
        attempted += 1
        try:
            dat = _fetch_history_action_dat(
                stage=SMARTESS_HISTORY_SOURCE_ACTION,
                action=_device_action(
                    identity,
                    action=SMARTESS_HISTORY_SOURCE_ACTION,
                    extra=(
                        ("parameter", series_key),
                        ("date", requested_date),
                    ),
                ),
                session=session,
                base_url=base_url,
                remaining_timeout=remaining_timeout,
            )
        except _SmartEssHistoryStageFailure as exc:
            failed += 1
            if not failure_code:
                failure_stage = exc.stage
                failure_code = exc.code
            if exc.code == SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED:
                budget_exhausted = True
            _LOGGER.info(
                "SmartESS history series finished series=%s "
                "outcome=failed error=%s",
                _diagnostic_series_label(series_key),
                exc.code,
            )
            if progress_detail is not None:
                progress_detail(
                    SMARTESS_HISTORY_SOURCE_ACTION,
                    attempted,
                    len(keys),
                )
            if budget_exhausted:
                break
            continue
        try:
            title, unit = _presentation(series_key)
            points = _history_points(
                dat,
                requested_date=requested_date,
                offset_seconds=offset_seconds,
            )
            series.append(
                CloudHistorySeries(
                    provider_id="smartess",
                    source_id="smartess",
                    source_action=SMARTESS_HISTORY_SOURCE_ACTION,
                    field_kind=CLOUD_FIELD_KIND_KEY_PARAMETER,
                    identity=identity,
                    series_key=series_key,
                    title=title,
                    unit=unit,
                    requested_date=requested_date,
                    # Key-parameter responses carry exact timestamps rather
                    # than a provider-declared fixed precision.
                    precision_minutes=0,
                    timezone_offset_seconds=offset_seconds,
                    points=points,
                )
            )
        except (SmartEssCloudError, TypeError, ValueError):
            failed += 1
            series_failure_code = SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE
            if not failure_code:
                failure_stage = SMARTESS_HISTORY_SOURCE_ACTION
                failure_code = series_failure_code
            budget_exhausted = remaining_timeout() <= 0
            if budget_exhausted:
                series_failure_code = SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED
                failure_code = SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED
            _LOGGER.info(
                "SmartESS history series finished series=%s "
                "outcome=failed error=%s",
                _diagnostic_series_label(series_key),
                series_failure_code,
            )
        else:
            _LOGGER.info(
                "SmartESS history series finished series=%s "
                "outcome=collected points=%d",
                _diagnostic_series_label(series_key),
                len(points),
            )
        if progress_detail is not None:
            progress_detail(
                SMARTESS_HISTORY_SOURCE_ACTION,
                attempted,
                len(keys),
            )
    if attempted:
        report(SMARTESS_HISTORY_SOURCE_ACTION)
    report("history_complete")
    collection = CloudHistoryCollection(
        provider_id="smartess",
        source_id="smartess",
        identity=identity,
        requested_date=requested_date,
        timezone_offset_seconds=offset_seconds,
        attempted_series_count=attempted,
        failed_series_count=failed,
        budget_exhausted=budget_exhausted,
        series=tuple(series),
    )
    return fetch_result(
        collection,
        failure_stage=failure_stage,
        failure_code=failure_code,
    )


__all__ = [
    "SMARTESS_HISTORY_KEYS_ACTION",
    "SMARTESS_HISTORY_SOURCE_ACTION",
    "SMARTESS_HISTORY_TIME_BASIS_ACTION",
    "SMARTESS_HISTORY_FAILURE_BUDGET_EXHAUSTED",
    "SMARTESS_HISTORY_FAILURE_ACTION_REJECTED",
    "SMARTESS_HISTORY_FAILURE_INVALID_RESPONSE",
    "SMARTESS_HISTORY_FAILURE_NO_SERIES_KEYS",
    "SmartEssHistoryFetchResult",
    "fetch_smartess_evidence_with_history",
]
