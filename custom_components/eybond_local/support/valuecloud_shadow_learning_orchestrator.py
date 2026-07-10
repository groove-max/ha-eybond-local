"""ValueCloud shadow-learning planning and execution helpers."""

from __future__ import annotations

from contextlib import suppress
import asyncio
import re
from typing import Any, Awaitable, Callable

from ..eybond_g_ascii_settings import G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD
from ..valuecloud_cloud import (
    ctrl_device_value,
    DEFAULT_APP_VERSION,
    DEFAULT_BASE_URL,
    DEFAULT_LANGUAGE,
    DEFAULT_TIMEOUT,
    ValueCloudEnvelope,
    ValueCloudError,
    ValueCloudSession,
    setup_batch_control_value,
)
from .shadow_learning import ShadowWriteObservation, utc_now_iso
from .shadow_learning_orchestrator import summarize_shadow_learning_attempts


_CONTROL_STATUS_PLANNED = "planned"
_CONTROL_STATUS_SENT = "sent"
_CONTROL_STATUS_ERROR = "error"
_CONTROL_STATUS_CAPTURED_NOT_APPLIED = "captured_not_applied"
_CONTROL_STATUS_DEGRADED = "degraded"
_CONTROL_STATUS_LEAKED = "leaked"

_DESTRUCTIVE_RE = re.compile(
    r"\b(restore|default|factory|reset|clear|delete|erase|calibrate|time|date|rating|power rating)\b",
    re.IGNORECASE,
)


def build_valuecloud_learning_plan(
    batch_control: dict[str, Any] | None,
    *,
    control_strategy: dict[str, Any] | None = None,
    device_ctrl: dict[str, Any] | None = None,
    field_ids: list[str] | tuple[str, ...] = (),
    include_numeric: bool = True,
    all_choice_values: bool = True,
    max_fields: int = 0,
) -> list[dict[str, Any]]:
    """Build a bounded ValueCloud control-learning plan from provider metadata."""

    requested_ids = {str(item) for item in field_ids if str(item).strip()}
    plan: list[dict[str, Any]] = []
    planned_field_ids: set[str] = set()

    for group in _batch_groups(batch_control):
        if not isinstance(group, dict):
            continue
        control_item_id = group.get("controlItemId")
        for field in group.get("parameters") or []:
            _extend_plan_for_field(
                plan=plan,
                planned_field_ids=planned_field_ids,
                field=field,
                requested_ids=requested_ids,
                control_item_id=control_item_id,
                transport="batch_setUp",
                include_numeric=include_numeric,
                all_choice_values=all_choice_values,
            )
            if _plan_reached_limit(planned_field_ids, max_fields):
                return plan

    for field in _legacy_fields(control_strategy, device_ctrl):
        _extend_plan_for_field(
            plan=plan,
            planned_field_ids=planned_field_ids,
            field=field,
            requested_ids=requested_ids,
            control_item_id=None,
            transport="legacy_ctrlDevice",
            include_numeric=include_numeric,
            all_choice_values=all_choice_values,
        )
        if _plan_reached_limit(planned_field_ids, max_fields):
            return plan

    return plan


async def async_orchestrate_valuecloud_shadow_learning(
    *,
    batch_control: dict[str, Any] | None,
    session: ValueCloudSession,
    pn: str,
    sn: str,
    devcode: int,
    devaddr: int,
    control_strategy: dict[str, Any] | None = None,
    device_ctrl: dict[str, Any] | None = None,
    dry_run: bool,
    confirm_cloud_write: bool,
    shadow_session_state: str,
    field_ids: list[str] | tuple[str, ...],
    include_numeric: bool = True,
    all_choice_values: bool = True,
    max_fields: int = 0,
    continue_on_error: bool = True,
    abort_on_unproxied_write: bool = True,
    delay_seconds: float = 0.0,
    observation_cursor: Callable[[], int] | None = None,
    wait_for_observations_since: Callable[[int, float], Awaitable[tuple[ShadowWriteObservation, ...]]] | None = None,
    current_observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None = None,
    is_session_ready: Callable[[], bool] | None = None,
    read_map_snapshot: Callable[[], dict[str, Any]] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    app_version: str = DEFAULT_APP_VERSION,
    timeout: float = DEFAULT_TIMEOUT,
    correlation_timeout_seconds: float = 2.0,
    setup_action: Callable[..., ValueCloudEnvelope] = setup_batch_control_value,
    legacy_setup_action: Callable[..., ValueCloudEnvelope] = ctrl_device_value,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Run ValueCloud batch-control learning through the local shadow proxy."""

    if not dry_run and not confirm_cloud_write:
        raise ValueCloudError("valuecloud_learning_requires_confirm_cloud_write")
    normalized_state = str(shadow_session_state or "").strip().lower()
    if not dry_run and normalized_state not in {"ready", "learning"}:
        raise RuntimeError("shadow_learning_session_not_ready")

    plan = build_valuecloud_learning_plan(
        batch_control,
        control_strategy=control_strategy,
        device_ctrl=device_ctrl,
        field_ids=field_ids,
        include_numeric=include_numeric,
        all_choice_values=all_choice_values,
        max_fields=max_fields,
    )
    known_field_ids = _known_field_ids(batch_control, control_strategy, device_ctrl)
    run_cursor_start = int(observation_cursor()) if observation_cursor is not None else None
    run_observations: list[ShadowWriteObservation] = []
    attempts: list[dict[str, Any]] = []

    plan_total = len(plan)
    for sequence_index, item in enumerate(plan):
        if on_progress is not None:
            with suppress(Exception):
                on_progress(sequence_index, plan_total)
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
            "action": str(item.get("action") or "valuecloud_batch_setUp"),
            "transport": str(item.get("transport") or "batch_setUp"),
            "dry_run": bool(dry_run),
            "unknown_field": str(item["field_id"]) not in known_field_ids,
            "controlItemId": item.get("controlItemId"),
            "detailsId": item.get("detailsId"),
            "order": item.get("order"),
            "datatype": item.get("datatype"),
            "read_key": str(item.get("read_key") or ""),
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
            if item.get("transport") == "legacy_ctrlDevice":
                envelope = await asyncio.to_thread(
                    legacy_setup_action,
                    session=session,
                    pn=pn,
                    sn=sn,
                    devcode=devcode,
                    devaddr=devaddr,
                    control_id=item.get("field_id"),
                    value=str(item.get("value") or ""),
                    datatype=item.get("datatype"),
                    base_url=base_url,
                    language=language,
                    app_version=app_version,
                    timeout=timeout,
                )
            else:
                envelope = await asyncio.to_thread(
                    setup_action,
                    session=session,
                    pn=pn,
                    sn=sn,
                    devcode=devcode,
                    devaddr=devaddr,
                    control_item_id=item.get("controlItemId"),
                    control_id=item.get("field_id"),
                    details_id=item.get("detailsId"),
                    order=item.get("order"),
                    value=str(item.get("value") or ""),
                    base_url=base_url,
                    language=language,
                    app_version=app_version,
                    timeout=timeout,
                )
            attempt["status"] = _CONTROL_STATUS_SENT
            attempt["response"] = {
                "code": getattr(envelope, "code", None),
                "success": getattr(envelope, "success", None),
                "message": str(getattr(envelope, "message", "") or ""),
                "errorMessage": str(getattr(envelope, "error_message", "") or ""),
            }
        except Exception as exc:  # pragma: no cover - cloud failures are endpoint-dependent
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

        _attach_attempt_observation(attempt=attempt, observations=observations)
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
        "plan": plan,
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


def _plan_item(
    *,
    field: dict[str, Any],
    control_item_id: Any,
    value: str,
    value_label: str,
    value_source: str,
    transport: str,
) -> dict[str, Any]:
    action = (
        "valuecloud_legacy_ctrlDevice"
        if transport == "legacy_ctrlDevice"
        else "valuecloud_batch_setUp"
    )
    return {
        "field_id": str(field.get("id") or ""),
        "title": _field_title(field),
        "value": str(value),
        "value_label": str(value_label),
        "value_source": str(value_source),
        "transport": transport,
        "action": action,
        "controlItemId": field.get("controlItemId") or control_item_id,
        "detailsId": field.get("detailsId"),
        "order": field.get("order"),
        "datatype": field.get("datatype"),
        "read_key": _field_read_key(field),
    }


def _batch_groups(batch_control: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(batch_control, dict):
        return []
    groups = batch_control.get("groups")
    return [item for item in groups if isinstance(item, dict)] if isinstance(groups, list) else []


def _legacy_fields(
    control_strategy: dict[str, Any] | None,
    device_ctrl: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in (control_strategy, device_ctrl):
        if not isinstance(source, dict):
            continue
        raw_fields = source.get("fields")
        if not isinstance(raw_fields, list):
            continue
        for field in raw_fields:
            if not isinstance(field, dict):
                continue
            field_id = str(field.get("id") or "").strip()
            if not field_id or field_id in seen:
                continue
            seen.add(field_id)
            fields.append(field)
    return fields


def _extend_plan_for_field(
    *,
    plan: list[dict[str, Any]],
    planned_field_ids: set[str],
    field: Any,
    requested_ids: set[str],
    control_item_id: Any,
    transport: str,
    include_numeric: bool,
    all_choice_values: bool,
) -> None:
    if not isinstance(field, dict):
        return
    field_id = str(field.get("id") or "").strip()
    if not field_id or field_id in planned_field_ids:
        return
    if requested_ids and field_id not in requested_ids:
        return
    if not _field_is_writable(field):
        return
    if _field_is_destructive(field):
        return

    choices = _field_choices(field)
    if choices:
        selected = choices if all_choice_values else choices[:1]
        for value, label in selected:
            plan.append(
                _plan_item(
                    field=field,
                    control_item_id=control_item_id,
                    value=str(value),
                    value_label=label,
                    value_source="choice",
                    transport=transport,
                )
            )
        planned_field_ids.add(field_id)
        return

    if not include_numeric:
        return
    current_value = _field_current_value(field)
    if current_value is None:
        return
    plan.append(
        _plan_item(
            field=field,
            control_item_id=control_item_id,
            value=str(current_value),
            value_label="",
            value_source="current",
            transport=transport,
        )
    )
    planned_field_ids.add(field_id)


def _plan_reached_limit(planned_field_ids: set[str], max_fields: int) -> bool:
    return max_fields > 0 and len(planned_field_ids) >= max_fields


def _field_is_writable(field: dict[str, Any]) -> bool:
    readwrite = str(field.get("readwrite") or "").strip().upper()
    if readwrite in {"RW", "R/W", "WRITE", "WRITABLE"}:
        return True
    if readwrite in {"R", "RO", "READ", "READONLY", "READ_ONLY"}:
        return False
    return field.get("detailsId") not in (None, "")


def _field_is_destructive(field: dict[str, Any]) -> bool:
    text = " ".join(
        str(field.get(key) or "")
        for key in ("id", "name", "par", "hint", "tag")
    )
    return bool(_DESTRUCTIVE_RE.search(text))


def _field_choices(field: dict[str, Any]) -> list[tuple[str, str]]:
    raw = field.get("item") or field.get("enumMap")
    choices: list[tuple[str, str]] = []
    if isinstance(raw, dict):
        for key, value in raw.items():
            key_text = str(key).strip()
            if not key_text:
                continue
            choices.append((key_text, str(value)))
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            key_text = str(item.get("key") or item.get("value") or "").strip()
            if not key_text:
                continue
            label = item.get("val")
            if label in (None, ""):
                label = item.get("label") or item.get("name") or key_text
            choices.append((key_text, str(label)))
    return _canonicalized_field_choices(field, choices)


def _field_title(field: dict[str, Any]) -> str:
    field_id = str(field.get("id") or "").strip()
    definition = G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.get(field_id)
    if definition is not None and definition.title:
        return definition.title
    return str(field.get("name") or field.get("par") or field.get("id") or "")


def _canonicalized_field_choices(
    field: dict[str, Any],
    choices: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    field_id = str(field.get("id") or "").strip()
    definition = G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.get(field_id)
    if definition is None or not definition.choices:
        return choices
    output: list[tuple[str, str]] = []
    for key, label in choices:
        output.append((key, definition.choices.get(key, label)))
    return output


def _field_read_key(field: dict[str, Any]) -> str:
    field_id = str(field.get("id") or "").strip()
    definition = G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.get(field_id)
    return definition.read_key if definition is not None else ""


def _field_current_value(field: dict[str, Any]) -> str | None:
    for key in ("val", "displayValue"):
        value = field.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return None


def _known_field_ids(
    batch_control: dict[str, Any] | None,
    control_strategy: dict[str, Any] | None = None,
    device_ctrl: dict[str, Any] | None = None,
) -> set[str]:
    known: set[str] = set()
    for group in _batch_groups(batch_control):
        if not isinstance(group, dict):
            continue
        for field in group.get("parameters") or []:
            if not isinstance(field, dict):
                continue
            field_id = str(field.get("id") or "").strip()
            if field_id:
                known.add(field_id)
    for field in _legacy_fields(control_strategy, device_ctrl):
        field_id = str(field.get("id") or "").strip()
        if field_id:
            known.add(field_id)
    return known


def _safe_read_map(read_map_snapshot: Callable[[], dict[str, Any]] | None) -> dict[str, Any]:
    if read_map_snapshot is None:
        return {}
    try:
        read_map = read_map_snapshot()
    except Exception:
        return {}
    return read_map if isinstance(read_map, dict) else {}


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
            await wait_for_observations_since(cursor_start, min(remaining, 0.25)) or ()
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
        return
    attempt["reason"] = "timeout_no_observed_write"


def _normalize_captured_not_applied_status(attempt: dict[str, Any]) -> None:
    if not isinstance(attempt.get("observation"), dict):
        return
    if attempt.get("status") == _CONTROL_STATUS_SENT and _attempt_has_success_response(attempt):
        attempt["status"] = _CONTROL_STATUS_CAPTURED_NOT_APPLIED
        attempt["proxy_capture_result"] = "captured_not_applied"
        attempt["cloud_ack_after_proxy_nack"] = True
        return
    if attempt.get("status") == _CONTROL_STATUS_ERROR:
        attempt["status"] = _CONTROL_STATUS_CAPTURED_NOT_APPLIED
        attempt["proxy_capture_result"] = "captured_not_applied"
        attempt["cloud_nack"] = str(attempt.get("error") or "")
        attempt.pop("error", None)


def _attempt_has_success_response(attempt: dict[str, Any]) -> bool:
    response = attempt.get("response")
    if not isinstance(response, dict):
        return False
    if response.get("success") is True:
        return True
    try:
        return int(response.get("code", -1)) in {0, 200}
    except (TypeError, ValueError):
        return False


def _attempt_is_unproxied_success_candidate(attempt: dict[str, Any]) -> bool:
    return (
        _attempt_has_success_response(attempt)
        and attempt.get("status") != _CONTROL_STATUS_CAPTURED_NOT_APPLIED
    )


def _collect_run_observations(
    *,
    run_cursor_start: int | None,
    current_observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None,
    attempt_observations: tuple[ShadowWriteObservation, ...],
) -> tuple[ShadowWriteObservation, ...]:
    if run_cursor_start is not None and current_observations_since is not None:
        return tuple(current_observations_since(run_cursor_start) or ())
    return tuple(attempt_observations)


__all__ = [
    "async_orchestrate_valuecloud_shadow_learning",
    "build_valuecloud_learning_plan",
]
