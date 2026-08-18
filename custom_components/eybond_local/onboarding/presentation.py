"""Branch-aware onboarding/result presentation helpers."""

from __future__ import annotations

from ..connection.recovery.verification import CallbackRecoveryRoute
from ..models import OnboardingResult

_CONFIDENCE_SCORE = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def confidence_sort_score(confidence: str) -> int:
    """Return one stable numeric score for confidence-based ordering."""

    return _CONFIDENCE_SCORE.get(confidence, 0)


def has_smartess_collector_hint(result: OnboardingResult) -> bool:
    """Return true when onboarding captured SmartESS collector-side metadata."""

    collector = result.collector
    collector_info = collector.collector if collector is not None else None
    if collector_info is None:
        return False
    return any(
        str(value or "").strip()
        for value in (
            collector_info.smartess_collector_version,
            collector_info.smartess_protocol_asset_id,
            collector_info.smartess_protocol_profile_key,
        )
    )


def scan_result_status_code(result: OnboardingResult, already_added: bool = False) -> str:
    """Return the collector-first UI status for one scan result.

    Search establishes whether a device can continue through admission; inverter
    model/driver detection belongs to runtime.  Consequently a preview match,
    confidence and detection timeout must not create separate scan states.
    """

    collector = result.collector
    if already_added or result.last_error == "already_configured":
        return "already_added"
    if (
        result.observed_session is not None
        and type(result.callback_route) is not CallbackRecoveryRoute
    ):
        return "address_required"
    if result.match is not None:
        return "found"
    if (
        collector is not None
        and (
            collector.connected
            or (
                collector.collector is not None
                and collector.collector.collector_pn
            )
        )
    ):
        return "found"
    if collector is not None and collector.udp_reply:
        return "address_found"
    return "not_ready"


def scan_result_sort_key(
    result: OnboardingResult,
    *,
    already_added: bool = False,
) -> tuple[int, int, str, str, str]:
    """Return one stable sort key for one onboarding result."""

    status_code = scan_result_status_code(result, already_added)
    status_rank = {
        "found": 0,
        "address_required": 1,
        "already_added": 2,
        "address_found": 3,
        "not_ready": 4,
    }.get(status_code, 99)
    collector = result.collector
    collector_ip = collector.ip if collector is not None else ""
    collector_pn = ""
    if collector is not None and collector.collector is not None:
        collector_pn = collector.collector.collector_pn or ""
    target_ip = collector.target_ip if collector is not None else ""
    return (
        status_rank,
        0,
        collector_pn,
        collector_ip,
        target_ip,
    )


__all__ = [
    "confidence_sort_score",
    "has_smartess_collector_hint",
    "scan_result_sort_key",
    "scan_result_status_code",
]
