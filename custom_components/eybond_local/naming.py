"""Naming helpers for collector-first Home Assistant presentation."""

from __future__ import annotations


def collector_display_name(*, collector_pn: str = "", collector_ip: str = "") -> str:
    """Return the preferred collector device name."""

    normalized_pn = str(collector_pn or "").strip()
    normalized_ip = str(collector_ip or "").strip()
    if normalized_pn:
        return f"Collector PN {normalized_pn}"
    if normalized_ip:
        return f"Collector {normalized_ip}"
    return "Collector"


def installation_title(
    *,
    collector_pn: str = "",
    collector_ip: str = "",
    detected_model: str = "",
    detected_serial: str = "",
) -> str:
    """Return the preferred config-entry title for one installation."""

    normalized_pn = str(collector_pn or "").strip()
    normalized_ip = str(collector_ip or "").strip()
    normalized_model = str(detected_model or "").strip()
    normalized_serial = str(detected_serial or "").strip()

    if normalized_pn or normalized_ip:
        return collector_display_name(
            collector_pn=normalized_pn,
            collector_ip=normalized_ip,
        )
    if normalized_model and normalized_serial:
        return f"{normalized_model} ({normalized_serial})"
    if normalized_model:
        return normalized_model
    return "EyeBond Local"


def legacy_installation_titles(
    *,
    detected_model: str = "",
    detected_serial: str = "",
    collector_ip: str = "",
    server_ip: str = "",
) -> set[str]:
    """Return known pre-collector-first auto-generated config-entry titles."""

    normalized_model = str(detected_model or "").strip()
    normalized_serial = str(detected_serial or "").strip()
    normalized_collector_ip = str(collector_ip or "").strip()
    normalized_server_ip = str(server_ip or "").strip()

    titles: set[str] = set()
    if normalized_model and normalized_serial:
        titles.add(f"{normalized_model} ({normalized_serial})")
    if normalized_model:
        titles.add(normalized_model)
    fallback_ip = normalized_collector_ip or normalized_server_ip
    if fallback_ip:
        titles.add(f"EyeBond Local ({fallback_ip})")
    return titles
