"""Pure polling projections used by the Home Assistant coordinator."""

from __future__ import annotations

from collections.abc import Mapping
import math

from ...const import DEFAULT_POLL_INTERVAL
from ...models import RuntimeSnapshot
from ..poll_scheduler import PollDecision, clamp_interval

POLL_INTERVAL_MIN_SECONDS = 2
POLL_INTERVAL_MAX_SECONDS = 3600
POLL_UTILIZATION_WARNING_RATIO = 0.9
POLL_OVERRUN_RATIO = 1.0
POLL_STABLE_STREAK_THRESHOLD = 3
POLL_RECOMMENDED_TARGET_UTILIZATION = 0.7
POLL_NOTIFICATION_COOLDOWN_SECONDS = 12 * 60 * 60
POLL_FIXED_RATE_MIN_DELAY_SECONDS = 1.0
RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE = "collector_offline"
RUNTIME_DRIVER_STATE_DRIVER_UNBOUND = "driver_unbound"
RUNTIME_DRIVER_STATE_DRIVER_BOUND = "driver_bound"
COLLECTOR_POLL_CONTEXT_COLLECTOR = "collector"
COLLECTOR_POLL_CONTEXT_DETECTION = "detection"
COLLECTOR_POLL_CONTEXT_RUNTIME = "runtime"


def clamp_poll_interval_seconds(value: object) -> int:
    interval = int(math.ceil(clamp_interval(value)))
    return min(
        POLL_INTERVAL_MAX_SECONDS,
        max(POLL_INTERVAL_MIN_SECONDS, interval),
    )


def poll_recommended_interval_seconds(
    *,
    current_interval: float,
    observed_duration: float,
) -> int:
    """Return a safe minimum poll interval for the observed refresh duration."""

    try:
        duration = max(0.0, float(observed_duration))
    except (TypeError, ValueError):
        duration = 0.0
    try:
        current = max(0.0, float(current_interval))
    except (TypeError, ValueError):
        current = float(DEFAULT_POLL_INTERVAL)
    if duration <= 0.0:
        return clamp_poll_interval_seconds(current)
    recommended = math.ceil(duration / POLL_RECOMMENDED_TARGET_UTILIZATION)
    if duration >= current:
        recommended = max(recommended, math.ceil(current) + 1)
    return clamp_poll_interval_seconds(recommended)


def runtime_driver_state_from_snapshot(snapshot: RuntimeSnapshot) -> str:
    values = getattr(snapshot, "values", None)
    if isinstance(values, Mapping):
        state = str(values.get("runtime_driver_state") or "").strip()
        if state in {
            RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE,
            RUNTIME_DRIVER_STATE_DRIVER_UNBOUND,
            RUNTIME_DRIVER_STATE_DRIVER_BOUND,
        }:
            return state
    if not bool(getattr(snapshot, "connected", False)):
        return RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE
    if getattr(snapshot, "inverter", None) is not None:
        return RUNTIME_DRIVER_STATE_DRIVER_BOUND
    return RUNTIME_DRIVER_STATE_DRIVER_UNBOUND


def poll_context_for_runtime_driver_state(runtime_driver_state: str) -> str:
    if runtime_driver_state == RUNTIME_DRIVER_STATE_DRIVER_BOUND:
        return COLLECTOR_POLL_CONTEXT_RUNTIME
    if runtime_driver_state == RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE:
        return COLLECTOR_POLL_CONTEXT_COLLECTOR
    return COLLECTOR_POLL_CONTEXT_DETECTION


def snapshot_reconnect_count(snapshot: object) -> int:
    values = getattr(snapshot, "values", None)
    if not isinstance(values, dict):
        return 0
    try:
        return int(values.get("runtime_reconnect_count", 0) or 0)
    except (TypeError, ValueError):
        return 0


def is_clean_runtime_poll_cycle(
    *,
    previous_runtime_driver_state: str,
    runtime_driver_state: str,
    previous_reconnect_count: int,
    reconnect_count: int,
) -> bool:
    """Return whether one cycle measured only a steady-state runtime poll."""

    return (
        runtime_driver_state == RUNTIME_DRIVER_STATE_DRIVER_BOUND
        and previous_runtime_driver_state == RUNTIME_DRIVER_STATE_DRIVER_BOUND
        and reconnect_count <= previous_reconnect_count
    )


def poll_non_runtime_retry_interval_seconds(
    *,
    current_interval: float,
    observed_duration: float,
    decision: PollDecision,
) -> int:
    """Return a temporary auto retry interval for non-runtime poll contexts."""

    current = clamp_interval(
        current_interval,
        minimum=decision.policy_min_interval,
        maximum=decision.policy_max_interval,
    )
    try:
        duration = max(0.0, float(observed_duration))
    except (TypeError, ValueError, OverflowError):
        duration = 0.0
    if not math.isfinite(duration):
        duration = 0.0
    if duration <= 0.0:
        return int(math.ceil(current))
    retry = max(current, math.ceil(duration * 1.3))
    return int(
        math.ceil(
            clamp_interval(
                retry,
                minimum=decision.policy_min_interval,
                maximum=decision.policy_max_interval,
            )
        )
    )


__all__ = [
    "COLLECTOR_POLL_CONTEXT_COLLECTOR",
    "COLLECTOR_POLL_CONTEXT_DETECTION",
    "COLLECTOR_POLL_CONTEXT_RUNTIME",
    "POLL_INTERVAL_MAX_SECONDS",
    "POLL_INTERVAL_MIN_SECONDS",
    "POLL_FIXED_RATE_MIN_DELAY_SECONDS",
    "POLL_NOTIFICATION_COOLDOWN_SECONDS",
    "POLL_OVERRUN_RATIO",
    "POLL_RECOMMENDED_TARGET_UTILIZATION",
    "POLL_STABLE_STREAK_THRESHOLD",
    "POLL_UTILIZATION_WARNING_RATIO",
    "RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE",
    "RUNTIME_DRIVER_STATE_DRIVER_BOUND",
    "RUNTIME_DRIVER_STATE_DRIVER_UNBOUND",
    "clamp_poll_interval_seconds",
    "is_clean_runtime_poll_cycle",
    "poll_context_for_runtime_driver_state",
    "poll_non_runtime_retry_interval_seconds",
    "poll_recommended_interval_seconds",
    "runtime_driver_state_from_snapshot",
    "snapshot_reconnect_count",
]
