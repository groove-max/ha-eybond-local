"""Branch-aware onboarding/result presentation helpers."""

from __future__ import annotations

from collections.abc import Sequence

from ..connection.recovery.verification import CallbackRecoveryRoute
from ..connection.ui import ConnectionDisplayMetadata
from ..const import DRIVER_HINT_AUTO
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


def confidence_label(confidence: str) -> str:
    """Return one human-readable confidence label."""

    return {
        "high": "High confidence",
        "medium": "Medium confidence",
        "low": "Low confidence",
        "none": "No confidence",
    }.get(confidence, confidence)


def default_control_summary(confidence: str) -> str:
    """Return one default control summary for the current confidence level."""

    if confidence == "high":
        return "Tested controls will be enabled automatically."
    return "The integration will start in **monitoring-only** mode."


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


def scan_result_status_label(result: OnboardingResult, already_added: bool = False) -> str:
    """Return the human-readable status label for one onboarding result."""

    status_code = scan_result_status_code(result, already_added)
    return {
        "found": "Found",
        "address_required": "Needs confirmation",
        "address_found": "Check address",
        "already_added": "Already added",
        "not_ready": "Not identified",
    }.get(status_code, "Not identified")


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


def result_label(result: OnboardingResult, *, display: ConnectionDisplayMetadata) -> str:
    """Return one compact, collector-first selector label."""

    collector = result.collector
    collector_ip = collector.ip if collector is not None else "unknown"
    status_label = scan_result_status_label(result)
    collector_info = collector.collector if collector is not None else None
    collector_pn = collector_info.collector_pn if collector_info is not None else ""
    identity = f"PN {collector_pn} — " if collector_pn else ""
    return f"{status_label}: {identity}{collector_ip}"


def result_placeholders(
    result: OnboardingResult,
    *,
    display: ConnectionDisplayMetadata,
) -> dict[str, str]:
    """Return confirm-step placeholders for one onboarding result."""

    collector = result.collector
    match = result.match
    collector_ip = collector.ip if collector is not None else "unknown"
    collector_pn = ""
    if collector is not None and collector.collector is not None:
        collector_pn = collector.collector.collector_pn or ""
    return {
        "model_name": match.model_name if match is not None else display.unconfirmed_inverter_label,
        "serial_number": match.serial_number if match is not None else "Not available yet",
        "driver_key": match.driver_key if match is not None else DRIVER_HINT_AUTO,
        "collector_ip": collector_ip,
        "collector_pn": collector_pn or "Unknown",
        "confidence": confidence_label(result.confidence),
        "control_summary": default_control_summary(result.confidence),
    }


def build_scan_results_placeholders(
    *,
    display: ConnectionDisplayMetadata,
    selected_scan_interface: str,
    detected_count: int,
    available_count: int,
    already_added_count: int,
    ready_model_names: Sequence[str],
) -> dict[str, str]:
    """Return collector-first scan-results placeholders.

    ``ready_model_names`` remains in the compatibility signature for callers,
    but runtime owns inverter identification and the scan UI never reads it.
    """

    del ready_model_names

    if detected_count == 0:
        scan_summary = "No compatible devices were found."
        next_hint = "Refresh the scan or switch to manual setup."
    elif available_count == 0 and already_added_count == detected_count:
        scan_summary = f"Devices found: **{detected_count}**. All are already added."
        next_hint = (
            "Use **Refresh scan** to look again, or **Manual setup** if you intentionally "
            "need a different connection path."
        )
    elif available_count == 0:
        scan_summary = (
            f"Responses received: **{detected_count}**, but no device has been identified yet."
        )
        next_hint = "Select a responding address to identify it, refresh the scan, or use manual setup."
    else:
        scan_summary = (
            f"Devices found: **{detected_count}**. Ready to set up: **{available_count}**. "
            f"Already added: **{already_added_count}**."
        )
        next_hint = "Choose a device or address from the list."

    return {
        "scan_summary": scan_summary,
        "scan_next_hint": next_hint,
        "selected_scan_interface": selected_scan_interface,
    }


def build_choose_placeholders(available_count: int) -> dict[str, str]:
    """Return choose-step placeholders."""

    return {
        "choose_summary": (
            f"Ready to set up: **{available_count}**. "
            "Already added devices are hidden."
        ),
    }


def build_scan_result_line(
    index: int,
    result: OnboardingResult,
    *,
    display: ConnectionDisplayMetadata,
    existing_entry_title: str | None = None,
) -> str:
    """Return one human-readable scan result line for the review screen."""

    collector = result.collector
    collector_info = collector.collector if collector is not None else None
    collector_ip = collector.ip if collector is not None else "unknown"
    collector_pn = collector_info.collector_pn if collector_info is not None else ""
    status_label = scan_result_status_label(result, existing_entry_title is not None)

    status_code = scan_result_status_code(result, existing_entry_title is not None)
    if status_code == "address_required":
        parts = []
        if collector_pn:
            parts.append(f"PN {collector_pn}")
        parts.append(f"connection from {collector_ip}")
    else:
        parts = []
        if collector_pn:
            parts.append(f"PN {collector_pn}")
        parts.append(f"address {collector_ip}")

    line = f"{index}. **{status_label}** — " + " · ".join(parts)
    if existing_entry_title is not None:
        line += f' *(already added as "{existing_entry_title}")*'
    return line
