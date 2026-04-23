"""Branch-aware onboarding/result presentation helpers."""

from __future__ import annotations

from collections.abc import Sequence

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
    """Return the UI status code for one onboarding result."""

    collector = result.collector
    if already_added:
        return "already_added"
    if result.match is not None and result.confidence == "high":
        return "ready"
    if result.match is not None:
        return "review"
    if collector is not None and collector.connected and has_smartess_collector_hint(result):
        return "smartess_hint"
    if collector is not None and collector.connected:
        return "collector_only"
    if collector is not None and collector.udp_reply:
        return "collector_replied"
    return "unknown"


def scan_result_status_label(result: OnboardingResult, already_added: bool = False) -> str:
    """Return the human-readable status label for one onboarding result."""

    status_code = scan_result_status_code(result, already_added)
    return {
        "ready": "Ready",
        "review": "Review",
        "already_added": "Already added",
        "smartess_hint": "SmartESS hint",
        "collector_only": "Collector only",
        "collector_replied": "Collector replied",
        "unknown": "Unknown",
    }.get(status_code, "Unknown")


def scan_result_sort_key(
    result: OnboardingResult,
    *,
    already_added: bool = False,
) -> tuple[int, int, str, str, str]:
    """Return one stable sort key for one onboarding result."""

    status_code = scan_result_status_code(result, already_added)
    status_rank = {
        "ready": 0,
        "review": 1,
        "already_added": 2,
        "smartess_hint": 3,
        "collector_only": 4,
        "collector_replied": 5,
        "unknown": 6,
    }.get(status_code, 99)
    collector_ip = result.collector.ip if result.collector is not None else ""
    model_name = result.match.model_name if result.match is not None else ""
    serial_number = result.match.serial_number if result.match is not None else ""
    return (
        status_rank,
        -confidence_sort_score(result.confidence),
        model_name,
        serial_number,
        collector_ip,
    )


def result_label(result: OnboardingResult, *, display: ConnectionDisplayMetadata) -> str:
    """Return one compact selector label for one onboarding result."""

    match = result.match
    collector = result.collector
    collector_ip = collector.ip if collector is not None else "unknown"
    status_label = scan_result_status_label(result)
    if match is None:
        suffix = (
            "SmartESS metadata"
            if has_smartess_collector_hint(result)
            else f"{display.peer_label} connected"
            if collector is not None and collector.connected
            else f"{display.peer_label} only"
        )
        return f"{status_label}: {collector_ip} ({suffix})"
    serial = match.serial_number or "unknown serial"
    return (
        f"{status_label}: {match.model_name} ({serial}) on {collector_ip} — "
        f"{confidence_label(result.confidence)}"
    )


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
    """Return scan-results step placeholders."""

    if detected_count == 0:
        scan_summary = f"No reachable {display.peer_label_plural} or inverters were found."
        next_hint = "Refresh the scan or switch to manual setup."
    elif available_count == 0 and already_added_count == detected_count:
        scan_summary = (
            f"Found **{detected_count}** device candidate(s), but all of them are already configured."
        )
        next_hint = (
            "Use **Refresh scan** to look again, or **Manual setup** if you intentionally "
            "need a different connection path."
        )
    elif available_count == 0:
        scan_summary = (
            f"Found **{detected_count}** device candidate(s), but none are ready to add yet."
        )
        next_hint = "Use **Refresh scan** to try again, or **Manual setup** to override the connection settings."
    elif not ready_model_names:
        scan_summary = (
            f"Found **{detected_count}** device candidate(s). **{available_count}** collector candidate(s) "
            f"can be added now, but local inverter matching is still pending."
        )
        next_hint = (
            "Choose **Add detected device** to save the Pending Device now, or use **Refresh scan** "
            "or **Manual setup** to retry the local match."
        )
    else:
        ready_summary = ", ".join(dict.fromkeys(ready_model_names[:5])) or "detected inverters"
        scan_summary = (
            f"Found **{detected_count}** device candidate(s). **{available_count}** can be added now, "
            f"**{already_added_count}** already configured. Ready now: {ready_summary}."
        )
        next_hint = "Choose **Add detected device** to pick which inverter to add."

    return {
        "scan_summary": scan_summary,
        "scan_next_hint": next_hint,
        "selected_scan_interface": selected_scan_interface,
    }


def build_choose_placeholders(available_count: int) -> dict[str, str]:
    """Return choose-step placeholders."""

    return {
        "choose_summary": (
            f"**{available_count}** detected device candidate(s) can be added right now. "
            "Already configured devices are excluded."
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

    if result.match is not None:
        parts = [
            result.match.model_name,
            f"serial {result.match.serial_number or 'unknown'}",
            f"{display.peer_label} {collector_ip}",
            confidence_label(result.confidence),
        ]
    else:
        parts = [
            display.unconfirmed_inverter_label,
            f"{display.peer_label} {collector_ip}",
        ]
        if collector_pn:
            parts.append(f"PN {collector_pn}")
        if has_smartess_collector_hint(result):
            parts.append("SmartESS metadata")
        if collector is not None and collector.connected:
            parts.append(f"{display.peer_label} connected")
        elif collector is not None and collector.udp_reply:
            parts.append(f"{display.peer_label} replied, waiting for reverse connection")

    line = f"{index}. **{status_label}** — " + " · ".join(parts)
    if existing_entry_title is not None:
        line += f' *(already added as "{existing_entry_title}")*'
    return line
