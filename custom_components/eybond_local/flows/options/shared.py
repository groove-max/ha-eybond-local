"""Small presentation adapters shared by options-flow lifecycles."""

from __future__ import annotations

from homeassistant.helpers.selector import (
    BooleanSelector,
    TextSelector,
    TextSelectorConfig,
)

from ...const import (
    DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    MAX_PROXY_CAPTURE_DURATION_MINUTES,
    MIN_PROXY_CAPTURE_DURATION_MINUTES,
)

CONTROL_DISCOVERY_FAILURE_ROUTE_DROPPED = "control_discovery_route_dropped"


CONTROL_DISCOVERY_FAILURE_RUN_INCOMPLETE = "control_discovery_run_incomplete"


CONTROL_DISCOVERY_FAILURE_SAFETY_STOP = "control_discovery_safety_stop"


CONTROL_DISCOVERY_CLOUD_FAILURE_REASONS = {
    "auth_failed": "control_discovery_cloud_auth_failed",
    "rate_limited": "control_discovery_cloud_rate_limited",
    "unavailable": "control_discovery_cloud_unavailable",
    "timeout": "control_discovery_cloud_timeout",
    "network": "control_discovery_cloud_network",
    "unexpected": "control_discovery_cloud_unexpected",
}


def control_discovery_cloud_failure_reason(error_code: object) -> str:
    """Return one closed UI reason for a provider-owned cloud error code."""

    if type(error_code) is not str or error_code != error_code.strip():
        return ""
    return CONTROL_DISCOVERY_CLOUD_FAILURE_REASONS.get(error_code, "")


def _build_multiline_log_text_selector() -> TextSelector:
    try:
        return TextSelector(TextSelectorConfig(multiline=True, read_only=True))
    except TypeError:
        return TextSelector(TextSelectorConfig(multiline=True))


_MULTILINE_LOG_TEXT_SELECTOR = _build_multiline_log_text_selector()


_BOOLEAN_SELECTOR = BooleanSelector()


def _coerce_proxy_capture_duration_minutes(
    value: object,
    *,
    default: int = DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    minimum: int = MIN_PROXY_CAPTURE_DURATION_MINUTES,
) -> int:
    try:
        minutes = int(round(float(value)))
    except (TypeError, ValueError):
        minutes = int(default)
    return max(minimum, min(MAX_PROXY_CAPTURE_DURATION_MINUTES, minutes))
