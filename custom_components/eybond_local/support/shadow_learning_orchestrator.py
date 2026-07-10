"""SmartESS shadow-learning planning, execution, and correlation helpers."""

from __future__ import annotations

from contextlib import suppress
from datetime import datetime
import asyncio
import time
from typing import Any, Awaitable, Callable

from ..smartess_cloud import (
    DEFAULT_APP_ID,
    DEFAULT_APP_VERSION,
    DEFAULT_BASE_URL,
    DEFAULT_LANGUAGE,
    DEFAULT_LEARN_NUMERIC_VALUE,
    DEFAULT_TIMEOUT,
    SessionCredentials,
    SmartEssCloudError,
    build_device_control_action,
    build_learn_settings_plan,
    fetch_signed_action,
)
from .shadow_learning import ShadowWriteObservation, utc_now_iso


_CONTROL_STATUS_PLANNED = "planned"
_CONTROL_STATUS_SENT = "sent"
_CONTROL_STATUS_ERROR = "error"
_CONTROL_STATUS_CAPTURED_NOT_APPLIED = "captured_not_applied"
_CONTROL_STATUS_DEGRADED = "degraded"
# SAFETY-CRITICAL: a write whose cloud response was a *success* (ERR_NONE) in observe-only
# mode AND that has no matching local proxy write observation. Some SmartESS server paths return
# success even after our proxy NACKs the locally observed write; only success without local
# observation proves the write bypassed our proxy and may have reached the real inverter.
_CONTROL_STATUS_LEAKED = "leaked"


def orchestrate_shadow_learning_settings(
    *,
    settings_dat: Any,
    session: SessionCredentials,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    dry_run: bool,
    confirm_cloud_write: bool,
    shadow_session_ready: bool,
    field_ids: list[str] | tuple[str, ...],
    include_numeric: bool,
    numeric_value: str = DEFAULT_LEARN_NUMERIC_VALUE,
    all_choice_values: bool = False,
    max_fields: int = 0,
    continue_on_error: bool = True,
    abort_on_unproxied_write: bool = True,
    delay_seconds: float = 0.0,
    observed_writes: list[ShadowWriteObservation] | tuple[ShadowWriteObservation, ...] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
    fetch_action: Callable[..., Any] = fetch_signed_action,
) -> dict[str, Any]:
    """Plan and optionally execute one SmartESS learn-settings orchestration run."""

    if not dry_run and not confirm_cloud_write:
        raise SmartEssCloudError("learn_settings_requires_confirm_cloud_write")
    if not dry_run and not shadow_session_ready:
        raise RuntimeError("shadow_learning_session_not_ready")

    plan = build_learn_settings_plan(
        settings_dat,
        field_ids=field_ids,
        include_numeric=include_numeric,
        numeric_value=numeric_value,
        all_choice_values=all_choice_values,
        max_fields=max_fields,
    )
    known_field_ids = _known_cloud_field_ids(settings_dat)

    attempts: list[dict[str, Any]] = []
    for sequence_index, item in enumerate(plan):
        action = build_device_control_action(
            pn=pn,
            sn=sn,
            devcode=devcode,
            devaddr=devaddr,
            field_id=str(item["field_id"]),
            value=str(item["value"]),
        )
        attempt: dict[str, Any] = {
            "sequence_index": sequence_index,
            "requested_at": utc_now_iso(),
            "field_id": str(item["field_id"]),
            "title": str(item.get("title") or ""),
            "field_name": str(item.get("title") or ""),
            "value": str(item["value"]),
            "requested_value": str(item["value"]),
            "value_label": str(item.get("value_label") or ""),
            "value_source": str(item["value_source"]),
            "action": "ctrlDevice",
            "dry_run": bool(dry_run),
            "unknown_field": str(item["field_id"]) not in known_field_ids,
        }
        if dry_run:
            attempt["status"] = _CONTROL_STATUS_PLANNED
            attempts.append(attempt)
            continue

        try:
            envelope = fetch_action(
                action=action,
                session=session,
                base_url=base_url,
                language=language,
                app_id=app_id,
                app_version=app_version,
                timeout=timeout,
            )
            attempt["status"] = _CONTROL_STATUS_SENT
            response_err = int(getattr(envelope, "err", -1))
            attempt["response"] = {
                "err": response_err,
                "desc": str(getattr(envelope, "desc", "")),
            }
            attempt["dat"] = getattr(envelope, "dat", None)
        except Exception as exc:  # pragma: no cover - exact cloud errors are caller-dependent
            attempt["status"] = _CONTROL_STATUS_ERROR
            attempt["error"] = str(exc)
            attempts.append(attempt)
            if not continue_on_error:
                break
            continue

        attempts.append(attempt)
        correlation = correlate_cloud_attempts_with_shadow_writes(
            attempts=attempts,
            observed_writes=observed_writes or (),
        )
        _normalize_correlated_captured_not_applied_attempts(attempts, correlation)
        if abort_on_unproxied_write and _attempt_is_unproxied_success_candidate(attempt):
            attempt["status"] = _CONTROL_STATUS_LEAKED
            attempt["reason"] = "control_leaked_unproxied"
            break
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    correlation = correlate_cloud_attempts_with_shadow_writes(
        attempts=attempts,
        observed_writes=observed_writes or (),
    )
    _normalize_correlated_captured_not_applied_attempts(attempts, correlation)

    return {
        "planned_write_count": len(plan),
        "executed_result_count": len(attempts),
        "sent_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_SENT),
        "captured_not_applied_count": sum(
            1 for item in attempts if item.get("status") == _CONTROL_STATUS_CAPTURED_NOT_APPLIED
        ),
        "error_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_ERROR),
        "leaked_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_LEAKED),
        "unknown_field_count": sum(1 for item in attempts if bool(item.get("unknown_field"))),
        "results": attempts,
        "correlation": correlation,
    }


async def async_orchestrate_shadow_learning_settings(
    *,
    settings_dat: Any,
    session: SessionCredentials,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    dry_run: bool,
    confirm_cloud_write: bool,
    shadow_session_state: str,
    field_ids: list[str] | tuple[str, ...],
    include_numeric: bool,
    numeric_value: str = DEFAULT_LEARN_NUMERIC_VALUE,
    all_choice_values: bool = False,
    max_fields: int = 0,
    continue_on_error: bool = True,
    abort_on_unproxied_write: bool = True,
    delay_seconds: float = 0.0,
    observed_writes: list[ShadowWriteObservation] | tuple[ShadowWriteObservation, ...] | None = None,
    observation_cursor: Callable[[], int] | None = None,
    wait_for_observations_since: Callable[[int, float], Awaitable[tuple[ShadowWriteObservation, ...]]] | None = None,
    current_observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None = None,
    is_session_ready: Callable[[], bool] | None = None,
    read_map_snapshot: Callable[[], dict[str, Any]] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_id: str = DEFAULT_APP_ID,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
    correlation_timeout_seconds: float = 2.0,
    fetch_action: Callable[..., Any] = fetch_signed_action,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Plan and execute one SmartESS learn-settings run with async per-attempt observation correlation.

    ``on_progress(completed, total)`` is invoked before each planned write so
    callers can surface live progress; it must not raise.
    """

    if not dry_run and not confirm_cloud_write:
        raise SmartEssCloudError("learn_settings_requires_confirm_cloud_write")
    normalized_state = str(shadow_session_state or "").strip().lower()
    if not dry_run and normalized_state not in {"ready", "learning"}:
        raise RuntimeError("shadow_learning_session_not_ready")

    plan = build_learn_settings_plan(
        settings_dat,
        field_ids=field_ids,
        include_numeric=include_numeric,
        numeric_value=numeric_value,
        all_choice_values=all_choice_values,
        max_fields=max_fields,
    )
    known_field_ids = _known_cloud_field_ids(settings_dat)
    run_cursor_start = int(observation_cursor()) if observation_cursor is not None else None
    run_observations: list[ShadowWriteObservation] = []

    attempts: list[dict[str, Any]] = []
    plan_total = len(plan)
    for sequence_index, item in enumerate(plan):
        if on_progress is not None:
            with suppress(Exception):
                on_progress(sequence_index, plan_total)
        action = build_device_control_action(
            pn=pn,
            sn=sn,
            devcode=devcode,
            devaddr=devaddr,
            field_id=str(item["field_id"]),
            value=str(item["value"]),
        )
        attempt: dict[str, Any] = {
            "sequence_index": sequence_index,
            "requested_at": utc_now_iso(),
            "field_id": str(item["field_id"]),
            "title": str(item.get("title") or ""),
            "field_name": str(item.get("title") or ""),
            "value": str(item["value"]),
            "requested_value": str(item["value"]),
            "value_label": str(item.get("value_label") or ""),
            "value_source": str(item["value_source"]),
            "action": "ctrlDevice",
            "dry_run": bool(dry_run),
            "unknown_field": str(item["field_id"]) not in known_field_ids,
        }
        if dry_run:
            attempt["status"] = _CONTROL_STATUS_PLANNED
            attempts.append(attempt)
            continue

        if is_session_ready is not None and not bool(is_session_ready()):
            attempt["status"] = _CONTROL_STATUS_DEGRADED
            attempt["reason"] = "session_not_ready"
            attempts.append(attempt)
            break

        cursor_start = _resolve_live_observation_cursor(
            observation_cursor=observation_cursor,
            current_observations_since=current_observations_since,
        )
        attempt["observation_cursor_start"] = cursor_start
        attempt["requested_at"] = utc_now_iso()
        try:
            envelope = await asyncio.to_thread(
                fetch_action,
                action=action,
                session=session,
                base_url=base_url,
                language=language,
                app_id=app_id,
                app_version=app_version,
                timeout=timeout,
            )
            attempt["status"] = _CONTROL_STATUS_SENT
            response_err = int(getattr(envelope, "err", -1))
            attempt["response"] = {
                "err": response_err,
                "desc": str(getattr(envelope, "desc", "")),
            }
            attempt["dat"] = getattr(envelope, "dat", None)
        except Exception as exc:  # pragma: no cover - exact cloud errors are caller-dependent
            # In observe-only (exception) mode SmartESS rejects every write with
            # an expected NACK, yet the Modbus write is still delivered to the
            # shadow and observed. Keep correlating that observation instead of
            # skipping the attempt, so the delivered write maps to its cloud
            # field. A genuine non-delivery simply yields no observation.
            attempt["status"] = _CONTROL_STATUS_ERROR
            attempt["error"] = str(exc)
            if not continue_on_error:
                attempts.append(attempt)
                break

        observations = await _wait_for_attempt_observations(
            cursor_start=cursor_start,
            timeout_seconds=correlation_timeout_seconds,
            wait_for_observations_since=wait_for_observations_since,
            current_observations_since=current_observations_since,
            is_session_ready=is_session_ready,
        )
        if observations is None:
            if abort_on_unproxied_write and _attempt_is_unproxied_success_candidate(attempt):
                attempt["status"] = _CONTROL_STATUS_LEAKED
                attempt["reason"] = "control_leaked_unproxied"
                attempts.append(attempt)
                break
            attempt["status"] = _CONTROL_STATUS_DEGRADED
            attempt["reason"] = "session_degraded_during_run"
            attempts.append(attempt)
            break

        _attach_attempt_observation(
            attempt=attempt,
            observations=observations,
        )
        _normalize_captured_not_applied_status(attempt)
        if abort_on_unproxied_write and _attempt_is_unproxied_success_candidate(attempt):
            attempt["status"] = _CONTROL_STATUS_LEAKED
            attempt["reason"] = "control_leaked_unproxied"
            attempts.append(attempt)
            break
        if observations:
            run_observations.extend(observations)

        attempts.append(attempt)
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    completion_observations = _collect_run_observations(
        run_cursor_start=run_cursor_start,
        current_observations_since=current_observations_since,
        attempt_observations=tuple(run_observations),
    )
    correlation = summarize_shadow_learning_attempts(
        attempts=attempts,
        all_observations=completion_observations,
    )

    return {
        "planned_write_count": len(plan),
        "executed_result_count": len(attempts),
        "sent_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_SENT),
        "captured_not_applied_count": sum(
            1 for item in attempts if item.get("status") == _CONTROL_STATUS_CAPTURED_NOT_APPLIED
        ),
        "error_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_ERROR),
        "degraded_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_DEGRADED),
        "leaked_count": sum(1 for item in attempts if item.get("status") == _CONTROL_STATUS_LEAKED),
        "unknown_field_count": sum(1 for item in attempts if bool(item.get("unknown_field"))),
        "results": attempts,
        "correlation": correlation,
        "read_map": _safe_read_map(read_map_snapshot),
    }


def _safe_read_map(read_map_snapshot: Callable[[], dict[str, Any]] | None) -> dict[str, Any]:
    """Snapshot the session read map without letting it fail the run."""

    if read_map_snapshot is None:
        return {}
    try:
        read_map = read_map_snapshot()
    except Exception:
        return {}
    return read_map if isinstance(read_map, dict) else {}


def summarize_shadow_learning_attempts(
    *,
    attempts: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    all_observations: tuple[ShadowWriteObservation, ...] = (),
) -> dict[str, Any]:
    """Build one correlation summary from attempt-level async observation outcomes."""

    normalized_attempts = sorted(
        [dict(item) for item in attempts],
        key=lambda item: (
            int(item.get("sequence_index", 0)),
            str(item.get("requested_at") or ""),
        ),
    )
    matched: list[dict[str, Any]] = []
    unmatched_attempts: list[dict[str, Any]] = []
    degraded_attempts: list[dict[str, Any]] = []

    matched_payload_hex: set[str] = set()
    for attempt in normalized_attempts:
        observation = attempt.get("observation")
        if isinstance(observation, dict):
            matched.append(
                {
                    "sequence_index": int(attempt.get("sequence_index", 0)),
                    "field_id": str(attempt.get("field_id") or ""),
                    "field_name": str(attempt.get("field_name") or ""),
                    "requested_value": str(attempt.get("requested_value") or ""),
                    # Carry the SmartESS option label + field kind so the overlay generator can
                    # classify the control from its real definition (enum labels, switch vs
                    # select vs button) instead of guessing from raw swept values.
                    "value_label": str(attempt.get("value_label") or ""),
                    "value_source": str(attempt.get("value_source") or ""),
                    "read_key": str(attempt.get("read_key") or ""),
                    "requested_at": str(attempt.get("requested_at") or ""),
                    "unknown_field": bool(attempt.get("unknown_field")),
                    "observation": observation,
                    "timestamp_delta_seconds": attempt.get("timestamp_delta_seconds"),
                    "match_mode": str(attempt.get("match_mode") or "post_attempt_cursor"),
                }
            )
            payload_hex = str(observation.get("raw_payload_hex") or "")
            if payload_hex:
                matched_payload_hex.add(payload_hex)
            continue

        status = str(attempt.get("status") or "")
        reason = str(attempt.get("reason") or "no_observed_write")
        target = degraded_attempts if status == _CONTROL_STATUS_DEGRADED else unmatched_attempts
        target.append({**attempt, "reason": reason})

    unmatched_writes: list[dict[str, Any]] = []
    for item in all_observations:
        payload = item.to_json_dict()
        payload_hex = str(payload.get("raw_payload_hex") or "")
        if payload_hex and payload_hex in matched_payload_hex:
            continue
        unmatched_writes.append(payload)

    return {
        "matched": matched,
        "unmatched_attempts": unmatched_attempts,
        "degraded_attempts": degraded_attempts,
        "unmatched_writes": unmatched_writes,
        "matched_count": len(matched),
        "unmatched_attempt_count": len(unmatched_attempts),
        "degraded_attempt_count": len(degraded_attempts),
        "unmatched_write_count": len(unmatched_writes),
        "unknown_field_attempt_count": sum(1 for item in normalized_attempts if bool(item.get("unknown_field"))),
    }


def correlate_cloud_attempts_with_shadow_writes(
    *,
    attempts: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    observed_writes: list[ShadowWriteObservation] | tuple[ShadowWriteObservation, ...],
) -> dict[str, Any]:
    """Correlate cloud field/value attempts with observed shadow writes by order and time."""

    normalized_attempts = sorted(
        [dict(item) for item in attempts],
        key=lambda item: (
            int(item.get("sequence_index", 0)),
            str(item.get("requested_at") or ""),
        ),
    )
    normalized_writes = sorted(
        list(observed_writes),
        key=lambda item: str(item.timestamp or ""),
    )

    matched: list[dict[str, Any]] = []
    unmatched_attempts: list[dict[str, Any]] = []

    write_index = 0
    for attempt in normalized_attempts:
        if attempt.get("status") == _CONTROL_STATUS_ERROR and not _is_expected_proxy_nack(
            str(attempt.get("error") or "")
        ):
            unmatched_attempts.append({**attempt, "reason": "control_error"})
            continue

        attempt_timestamp = _parse_iso_datetime(str(attempt.get("requested_at") or ""))
        candidate_index = _find_candidate_write_index(
            writes=normalized_writes,
            start=write_index,
            requested_at=attempt_timestamp,
        )
        if candidate_index is None:
            unmatched_attempts.append({**attempt, "reason": "no_observed_write"})
            continue

        observation = normalized_writes[candidate_index]
        write_index = candidate_index + 1

        observation_timestamp = _parse_iso_datetime(observation.timestamp)
        matched.append(
            {
                "sequence_index": int(attempt.get("sequence_index", 0)),
                "field_id": str(attempt.get("field_id") or ""),
                "field_name": str(attempt.get("field_name") or ""),
                "requested_value": str(attempt.get("requested_value") or ""),
                "value_label": str(attempt.get("value_label") or ""),
                "value_source": str(attempt.get("value_source") or ""),
                "read_key": str(attempt.get("read_key") or ""),
                "requested_at": str(attempt.get("requested_at") or ""),
                "unknown_field": bool(attempt.get("unknown_field")),
                "observation": observation.to_json_dict(),
                "timestamp_delta_seconds": _timestamp_delta_seconds(
                    requested_at=attempt_timestamp,
                    observed_at=observation_timestamp,
                ),
            }
        )

    unmatched_writes = [
        normalized_writes[index].to_json_dict()
        for index in range(write_index, len(normalized_writes))
    ]

    return {
        "matched": matched,
        "unmatched_attempts": unmatched_attempts,
        "unmatched_writes": unmatched_writes,
        "matched_count": len(matched),
        "unmatched_attempt_count": len(unmatched_attempts),
        "unmatched_write_count": len(unmatched_writes),
        "unknown_field_attempt_count": sum(1 for item in normalized_attempts if bool(item.get("unknown_field"))),
    }


def _known_cloud_field_ids(settings_dat: Any) -> set[str]:
    if not isinstance(settings_dat, dict):
        return set()
    fields = settings_dat.get("field")
    if not isinstance(fields, list):
        return set()
    known: set[str] = set()
    for item in fields:
        if not isinstance(item, dict):
            continue
        field_id = str(item.get("id") or "").strip()
        if field_id:
            known.add(field_id)
    return known


def _find_candidate_write_index(
    *,
    writes: list[ShadowWriteObservation],
    start: int,
    requested_at: datetime | None,
) -> int | None:
    if start >= len(writes):
        return None
    if requested_at is None:
        return start

    for index in range(start, len(writes)):
        observed_at = _parse_iso_datetime(writes[index].timestamp)
        if observed_at is None:
            return index
        if observed_at >= requested_at:
            return index
    return None


def _parse_iso_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _timestamp_delta_seconds(*, requested_at: datetime | None, observed_at: datetime | None) -> float | None:
    if requested_at is None or observed_at is None:
        return None
    return (observed_at - requested_at).total_seconds()


async def _wait_for_attempt_observations(
    *,
    cursor_start: int,
    timeout_seconds: float,
    wait_for_observations_since: Callable[[int, float], Awaitable[tuple[ShadowWriteObservation, ...]]] | None,
    current_observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None,
    is_session_ready: Callable[[], bool] | None,
) -> tuple[ShadowWriteObservation, ...] | None:
    if is_session_ready is not None and not bool(is_session_ready()):
        return None
    if current_observations_since is not None:
        existing = tuple(current_observations_since(cursor_start) or ())
        if existing:
            return existing
    if wait_for_observations_since is None:
        return ()

    deadline = asyncio.get_running_loop().time() + max(float(timeout_seconds), 0.0)
    while True:
        if is_session_ready is not None and not bool(is_session_ready()):
            return None
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return ()
        observations = tuple(
            await wait_for_observations_since(
                cursor_start,
                min(remaining, 0.25),
            )
            or ()
        )
        if observations:
            return observations


def _attach_attempt_observation(
    *,
    attempt: dict[str, Any],
    observations: tuple[ShadowWriteObservation, ...],
) -> None:
    if observations:
        observation = observations[0]
        attempt["observation_count"] = len(observations)
        attempt["observation"] = observation.to_json_dict()
        attempt["match_mode"] = "post_attempt_cursor"
        attempt["timestamp_delta_seconds"] = _timestamp_delta_seconds(
            requested_at=_parse_iso_datetime(str(attempt.get("requested_at") or "")),
            observed_at=_parse_iso_datetime(observation.timestamp),
        )
        return
    attempt["reason"] = "timeout_no_observed_write"


def _normalize_captured_not_applied_status(attempt: dict[str, Any]) -> None:
    """Reclassify locally observed proxy writes that did not reach the inverter."""

    if not isinstance(attempt.get("observation"), dict):
        return
    if attempt.get("status") == _CONTROL_STATUS_SENT and _attempt_has_success_response(attempt):
        attempt["status"] = _CONTROL_STATUS_CAPTURED_NOT_APPLIED
        attempt["proxy_capture_result"] = "captured_not_applied"
        attempt["cloud_ack_after_proxy_nack"] = True
        return
    if attempt.get("status") != _CONTROL_STATUS_ERROR:
        return
    error = str(attempt.get("error") or "")
    if not _is_expected_proxy_nack(error):
        return
    attempt["status"] = _CONTROL_STATUS_CAPTURED_NOT_APPLIED
    attempt["proxy_capture_result"] = "captured_not_applied"
    attempt["cloud_nack"] = error
    attempt.pop("error", None)


def _attempt_has_success_response(attempt: dict[str, Any]) -> bool:
    response = attempt.get("response")
    if not isinstance(response, dict):
        return False
    try:
        return int(response.get("err", -1)) == 0
    except (TypeError, ValueError):
        return False


def _attempt_is_unproxied_success_candidate(attempt: dict[str, Any]) -> bool:
    return (
        _attempt_has_success_response(attempt)
        and attempt.get("status") != _CONTROL_STATUS_CAPTURED_NOT_APPLIED
    )


def _normalize_correlated_captured_not_applied_attempts(
    attempts: list[dict[str, Any]],
    correlation: dict[str, Any],
) -> None:
    matched = correlation.get("matched") if isinstance(correlation, dict) else None
    if not isinstance(matched, list):
        return
    attempts_by_sequence = {
        int(attempt.get("sequence_index", 0)): attempt
        for attempt in attempts
        if isinstance(attempt, dict)
    }
    for item in matched:
        if not isinstance(item, dict):
            continue
        attempt = attempts_by_sequence.get(int(item.get("sequence_index", 0)))
        if attempt is None:
            continue
        observation = item.get("observation")
        if isinstance(observation, dict):
            attempt["observation"] = observation
            attempt["match_mode"] = "post_run_order"
            attempt["timestamp_delta_seconds"] = item.get("timestamp_delta_seconds")
        _normalize_captured_not_applied_status(attempt)


def _is_expected_proxy_nack(error: str) -> bool:
    normalized = str(error or "")
    return "ERR_FAIL(Read-Only Register)" in normalized or (
        "Read-Only Register" in normalized and "action_failed" in normalized
    )


def _resolve_live_observation_cursor(
    *,
    observation_cursor: Callable[[], int] | None,
    current_observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None,
) -> int:
    if observation_cursor is not None:
        return int(observation_cursor())
    if current_observations_since is not None:
        return len(tuple(current_observations_since(0) or ()))
    return 0


def _collect_run_observations(
    *,
    run_cursor_start: int | None,
    current_observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None,
    attempt_observations: tuple[ShadowWriteObservation, ...],
) -> tuple[ShadowWriteObservation, ...]:
    if run_cursor_start is not None and current_observations_since is not None:
        return tuple(current_observations_since(run_cursor_start) or ())
    return tuple(attempt_observations)
