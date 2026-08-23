"""DESSMonitor ``ctrlDevice`` planning and exact local-write correlation."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ...dessmonitor_cloud import (
    DEFAULT_BASE_URL,
    DEFAULT_LANGUAGE,
    DEFAULT_TIMEOUT,
    DessMonitorActionRejectedError,
    DessMonitorApiEnvelope,
    DessMonitorControlField,
    DessMonitorDeviceIdentity,
    DessMonitorSession,
    send_device_control,
)
from . import ShadowWriteObservation, utc_now_iso
from .cloud_dispatch import async_dispatch_cloud_action
from .orchestrator import summarize_shadow_learning_attempts


_LOGGER = logging.getLogger(__name__)

_STATUS_SENT = "sent"
_STATUS_ERROR = "error"
_STATUS_CAPTURED = "captured_not_applied"
_STATUS_DEGRADED = "degraded"
_STATUS_LEAKED = "leaked"

_DESTRUCTIVE_RE = re.compile(
    r"\b(restore|default|factory|reset|clear|delete|erase|calibrate|"
    r"time|date|rating|power rating|reboot|restart|shutdown|initialize|"
    r"initialise|format|firmware|upgrade)\b",
    re.IGNORECASE,
)


def _exact_field_ids(value: object) -> tuple[str, ...]:
    if type(value) not in {list, tuple}:
        raise TypeError("dessmonitor_learning_field_ids_invalid")
    output: list[str] = []
    seen: set[str] = set()
    for item in value:
        if type(item) is not str:
            raise TypeError("dessmonitor_learning_field_id_invalid")
        if not item or item != item.strip():
            raise ValueError("dessmonitor_learning_field_id_invalid")
        if item not in seen:
            output.append(item)
            seen.add(item)
    return tuple(output)


def _bounded_non_negative_int(value: object, reason: str) -> int:
    if type(value) is not int:
        raise TypeError(reason)
    if value < 0:
        raise ValueError(reason)
    return value


def _positive_number(value: object, reason: str) -> float:
    if type(value) not in {int, float}:
        raise TypeError(reason)
    if value <= 0:
        raise ValueError(reason)
    return float(value)


def _field_is_destructive(field: DessMonitorControlField) -> bool:
    return bool(
        _DESTRUCTIVE_RE.search(
            " ".join(
                (
                    field.field_id,
                    field.title,
                    field.hint,
                    *(label for _value, label in field.choices),
                )
            )
        )
    )


def build_dessmonitor_learning_plan(
    control_fields: object,
    *,
    field_ids: object = (),
    all_choice_values: bool = True,
    max_fields: int = 0,
) -> list[dict[str, Any]]:
    """Build a bounded plan without inventing provider control values."""

    if type(control_fields) is not tuple or any(
        type(item) is not DessMonitorControlField for item in control_fields
    ):
        raise TypeError("dessmonitor_learning_controls_invalid")
    requested = set(_exact_field_ids(field_ids))
    if type(all_choice_values) is not bool:
        raise TypeError("dessmonitor_learning_choice_policy_invalid")
    field_limit = _bounded_non_negative_int(
        max_fields,
        "dessmonitor_learning_max_fields_invalid",
    )

    plan: list[dict[str, Any]] = []
    planned_fields = 0
    for field in control_fields:
        if requested and field.field_id not in requested:
            continue
        if _field_is_destructive(field):
            continue
        if field_limit and planned_fields >= field_limit:
            break

        values = list(field.choices)
        if values:
            alternatives = [
                choice for choice in values if choice[0] != field.current_value
            ]
            selected = alternatives or values[:1]
            if not all_choice_values:
                selected = selected[:1]
            value_source = "choice"
        elif field.current_value:
            selected = [(field.current_value, "")]
            value_source = "current"
        else:
            continue

        for value, label in selected:
            plan.append(
                {
                    "field_id": field.field_id,
                    "title": field.title,
                    "field_name": field.title,
                    "value": value,
                    "requested_value": value,
                    "value_label": label,
                    "value_source": value_source,
                    "action": "dessmonitor_ctrlDevice",
                    "read_key": "",
                }
            )
        planned_fields += 1
    return plan


def _observation_cursor(
    callback: Callable[[], int] | None,
    observations_since: Callable[[int], tuple[ShadowWriteObservation, ...]] | None,
) -> int:
    if callback is not None:
        value = callback()
        return _bounded_non_negative_int(
            value,
            "dessmonitor_learning_observation_cursor_invalid",
        )
    if observations_since is None:
        return 0
    return len(_exact_observations(observations_since(0)))


def _exact_observations(value: object) -> tuple[ShadowWriteObservation, ...]:
    if type(value) is not tuple or any(
        type(item) is not ShadowWriteObservation for item in value
    ):
        raise TypeError("dessmonitor_learning_observations_invalid")
    return value


async def _session_ready(
    *,
    is_session_ready: Callable[[], bool] | None,
    wait_until_session_ready: Callable[[], Awaitable[bool]] | None,
) -> bool:
    if is_session_ready is None:
        return True
    ready = is_session_ready()
    if type(ready) is not bool:
        raise TypeError("dessmonitor_learning_session_state_invalid")
    if ready:
        return True
    if wait_until_session_ready is None:
        return False
    waited = await wait_until_session_ready()
    if type(waited) is not bool:
        raise TypeError("dessmonitor_learning_session_state_invalid")
    return waited


async def _wait_for_observations(
    *,
    cursor: int,
    timeout_seconds: float,
    wait_for_observations_since: Callable[
        [int, float], Awaitable[tuple[ShadowWriteObservation, ...]]
    ]
    | None,
    current_observations_since: Callable[
        [int], tuple[ShadowWriteObservation, ...]
    ]
    | None,
    is_session_ready: Callable[[], bool] | None,
) -> tuple[ShadowWriteObservation, ...] | None:
    if current_observations_since is not None:
        existing = _exact_observations(current_observations_since(cursor))
        if existing:
            return existing
    if is_session_ready is not None:
        ready = is_session_ready()
        if type(ready) is not bool:
            raise TypeError("dessmonitor_learning_session_state_invalid")
        if not ready:
            return None
    if wait_for_observations_since is None:
        return ()

    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        if is_session_ready is not None:
            ready = is_session_ready()
            if type(ready) is not bool:
                raise TypeError("dessmonitor_learning_session_state_invalid")
            if not ready:
                return None
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return ()
        observed = _exact_observations(
            await wait_for_observations_since(cursor, min(remaining, 0.25))
        )
        if observed:
            return observed


def _safe_read_map(
    callback: Callable[[], dict[str, Any]] | None,
) -> dict[str, Any]:
    if callback is None:
        return {}
    try:
        value = callback()
    except Exception:
        return {}
    return dict(value) if type(value) is dict else {}


def _elapsed_ms(started: float) -> int:
    return max(0, int(round((time.monotonic() - started) * 1000.0)))


async def async_orchestrate_dessmonitor_shadow_learning(
    *,
    control_fields: tuple[DessMonitorControlField, ...],
    session: DessMonitorSession,
    identity: DessMonitorDeviceIdentity,
    confirm_cloud_write: bool,
    shadow_session_state: str,
    field_ids: list[str] | tuple[str, ...] = (),
    all_choice_values: bool = True,
    max_fields: int = 0,
    continue_on_error: bool = True,
    abort_on_unproxied_write: bool = True,
    delay_seconds: float = 0.0,
    observation_cursor: Callable[[], int] | None = None,
    wait_for_observations_since: Callable[
        [int, float], Awaitable[tuple[ShadowWriteObservation, ...]]
    ]
    | None = None,
    current_observations_since: Callable[
        [int], tuple[ShadowWriteObservation, ...]
    ]
    | None = None,
    is_session_ready: Callable[[], bool] | None = None,
    wait_until_session_ready: Callable[[], Awaitable[bool]] | None = None,
    read_map_snapshot: Callable[[], dict[str, Any]] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    language: str = DEFAULT_LANGUAGE,
    timeout: float = DEFAULT_TIMEOUT,
    correlation_timeout_seconds: float = 2.0,
    action: Callable[..., DessMonitorApiEnvelope] = send_device_control,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Correlate explicit DESSMonitor actions with writes on the exact route."""

    if type(session) is not DessMonitorSession:
        raise TypeError("dessmonitor_session_invalid")
    if type(identity) is not DessMonitorDeviceIdentity:
        raise TypeError("dessmonitor_identity_invalid")
    if type(confirm_cloud_write) is not bool or not confirm_cloud_write:
        raise ValueError("dessmonitor_learning_requires_confirm_cloud_write")
    if type(shadow_session_state) is not str:
        raise TypeError("dessmonitor_learning_session_state_invalid")
    if shadow_session_state not in {"ready", "learning"}:
        raise RuntimeError("shadow_learning_session_not_ready")
    for value, reason in (
        (all_choice_values, "dessmonitor_learning_choice_policy_invalid"),
        (continue_on_error, "dessmonitor_learning_continue_policy_invalid"),
        (abort_on_unproxied_write, "dessmonitor_learning_abort_policy_invalid"),
    ):
        if type(value) is not bool:
            raise TypeError(reason)
    if type(delay_seconds) not in {int, float}:
        raise TypeError("dessmonitor_learning_delay_invalid")
    if delay_seconds < 0:
        raise ValueError("dessmonitor_learning_delay_invalid")
    request_timeout = _positive_number(
        timeout,
        "dessmonitor_learning_timeout_invalid",
    )
    correlation_timeout = _positive_number(
        correlation_timeout_seconds,
        "dessmonitor_learning_correlation_timeout_invalid",
    )
    if not callable(action):
        raise TypeError("dessmonitor_learning_action_invalid")
    for callback in (
        observation_cursor,
        wait_for_observations_since,
        current_observations_since,
        is_session_ready,
        wait_until_session_ready,
        read_map_snapshot,
        on_progress,
    ):
        if callback is not None and not callable(callback):
            raise TypeError("dessmonitor_learning_callback_invalid")

    plan = build_dessmonitor_learning_plan(
        control_fields,
        field_ids=field_ids,
        all_choice_values=all_choice_values,
        max_fields=max_fields,
    )
    run_cursor = _observation_cursor(
        observation_cursor,
        current_observations_since,
    )
    run_observations: list[ShadowWriteObservation] = []
    attempts: list[dict[str, Any]] = []

    total = len(plan)
    for sequence_index, item in enumerate(plan):
        if on_progress is not None:
            with suppress(Exception):
                on_progress(sequence_index, total)

        attempt: dict[str, Any] = {
            "sequence_index": sequence_index,
            "requested_at": utc_now_iso(),
            "field_id": item["field_id"],
            "title": item["title"],
            "field_name": item["field_name"],
            "value": item["value"],
            "requested_value": item["requested_value"],
            "value_label": item["value_label"],
            "value_source": item["value_source"],
            "action": item["action"],
            "read_key": item["read_key"],
            "unknown_field": False,
        }
        if not await _session_ready(
            is_session_ready=is_session_ready,
            wait_until_session_ready=wait_until_session_ready,
        ):
            attempt["status"] = _STATUS_DEGRADED
            attempt["reason"] = "session_not_ready"
            attempts.append(attempt)
            break

        cursor = _observation_cursor(
            observation_cursor,
            current_observations_since,
        )
        attempt["observation_cursor_start"] = cursor
        attempt["requested_at"] = utc_now_iso()
        started = time.monotonic()
        try:
            envelope = await async_dispatch_cloud_action(
                action,
                session=session,
                identity=identity,
                field_id=item["field_id"],
                value=item["value"],
                base_url=base_url,
                language=language,
                timeout=request_timeout,
            )
            if type(envelope) is not DessMonitorApiEnvelope:
                raise TypeError("dessmonitor_learning_response_invalid")
            attempt["status"] = _STATUS_SENT
            attempt["delivery_outcome"] = "response"
            attempt["cloud_duration_ms"] = _elapsed_ms(started)
            attempt["response"] = {
                "err": envelope.err,
                "desc": envelope.desc,
            }
        except DessMonitorActionRejectedError as exc:
            attempt["status"] = _STATUS_ERROR
            attempt["delivery_outcome"] = "definitive_rejection"
            attempt["cloud_duration_ms"] = _elapsed_ms(started)
            attempt["cloud_rejection"] = {
                "err": exc.err,
                "action": exc.action,
                "desc": exc.desc,
            }
        except Exception:
            _LOGGER.warning(
                "DESSMonitor control delivery indeterminate sequence=%s total=%s "
                "duration_ms=%s",
                sequence_index,
                total,
                _elapsed_ms(started),
            )
            raise

        observations = await _wait_for_observations(
            cursor=cursor,
            timeout_seconds=correlation_timeout,
            wait_for_observations_since=wait_for_observations_since,
            current_observations_since=current_observations_since,
            is_session_ready=is_session_ready,
        )
        if observations is None:
            attempt["status"] = _STATUS_DEGRADED
            attempt["reason"] = "session_degraded_during_run"
            attempts.append(attempt)
            break
        if observations:
            observation = observations[0]
            attempt["observation_count"] = len(observations)
            attempt["observation"] = observation.to_json_dict()
            attempt["match_mode"] = "post_attempt_cursor"
            attempt["status"] = _STATUS_CAPTURED
            attempt["proxy_capture_result"] = "captured_not_applied"
            if "cloud_rejection" in attempt:
                attempt["cloud_nack_response"] = dict(
                    attempt["cloud_rejection"]
                )
            else:
                attempt["cloud_ack_after_proxy_nack"] = True
            run_observations.extend(observations)
        else:
            attempt["reason"] = "timeout_no_observed_write"
            if attempt["status"] == _STATUS_SENT and abort_on_unproxied_write:
                attempt["status"] = _STATUS_LEAKED
                attempt["reason"] = "control_leaked_unproxied"

        attempts.append(attempt)
        if attempt["status"] in {_STATUS_LEAKED, _STATUS_DEGRADED}:
            break
        if attempt["status"] == _STATUS_ERROR and not continue_on_error:
            break
        if delay_seconds > 0:
            await asyncio.sleep(float(delay_seconds))

    if current_observations_since is not None:
        all_observations = _exact_observations(
            current_observations_since(run_cursor)
        )
    else:
        all_observations = tuple(run_observations)
    correlation = summarize_shadow_learning_attempts(
        attempts=attempts,
        all_observations=all_observations,
    )
    return {
        "planned_write_count": len(plan),
        "plan": plan,
        "executed_result_count": len(attempts),
        "sent_count": sum(1 for item in attempts if item["status"] == _STATUS_SENT),
        "captured_not_applied_count": sum(
            1 for item in attempts if item["status"] == _STATUS_CAPTURED
        ),
        "error_count": sum(1 for item in attempts if item["status"] == _STATUS_ERROR),
        "degraded_count": sum(
            1 for item in attempts if item["status"] == _STATUS_DEGRADED
        ),
        "leaked_count": sum(1 for item in attempts if item["status"] == _STATUS_LEAKED),
        "unknown_field_count": 0,
        "results": attempts,
        "correlation": correlation,
        "read_map": _safe_read_map(read_map_snapshot),
    }


__all__ = [
    "async_orchestrate_dessmonitor_shadow_learning",
    "build_dessmonitor_learning_plan",
]
