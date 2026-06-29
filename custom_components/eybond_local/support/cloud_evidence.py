"""Helpers for storing external cloud evidence under one HA config dir."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ..const import LOCAL_CLOUD_EVIDENCE_DIR, LOCAL_METADATA_DIR
from ..smartess_cloud import fetch_device_bundle_for_collector as fetch_smartess_device_bundle_for_collector
from ..valuecloud_cloud import fetch_device_bundle_for_collector as fetch_valuecloud_device_bundle_for_collector


@dataclass(frozen=True, slots=True)
class CloudEvidenceRecord:
    """One persisted cloud-evidence JSON file plus its parsed payload."""

    path: Path
    payload: dict[str, Any]


def fetch_and_export_smartess_device_bundle_cloud_evidence(
    *,
    config_dir: Path,
    username: str,
    password: str,
    collector_pn: str,
    source: str,
    entry_id: str = "",
) -> CloudEvidenceRecord:
    """Fetch one SmartESS device bundle by collector PN and persist it as cloud evidence."""

    normalized_collector_pn = str(collector_pn or "").strip()
    if not normalized_collector_pn:
        raise RuntimeError("smartess_collector_pn_not_available")

    bundle_payload = fetch_smartess_device_bundle_for_collector(
        username=username,
        password=password,
        collector_pn=normalized_collector_pn,
    )
    evidence = build_smartess_device_bundle_cloud_evidence(
        bundle_payload,
        source=source,
        entry_id=entry_id,
        collector_pn=normalized_collector_pn,
    )
    path = export_cloud_evidence(
        config_dir=config_dir,
        evidence=evidence,
    )
    return CloudEvidenceRecord(path=path, payload=evidence)


def fetch_and_export_valuecloud_device_bundle_cloud_evidence(
    *,
    config_dir: Path,
    username: str,
    password: str,
    collector_pn: str,
    source: str,
    entry_id: str = "",
) -> CloudEvidenceRecord:
    """Fetch one ValueCloud device bundle by collector PN and persist it as cloud evidence."""

    normalized_collector_pn = str(collector_pn or "").strip()
    if not normalized_collector_pn:
        raise RuntimeError("valuecloud_collector_pn_not_available")

    bundle_payload = fetch_valuecloud_device_bundle_for_collector(
        username=username,
        password=password,
        collector_pn=normalized_collector_pn,
    )
    evidence = build_valuecloud_device_bundle_cloud_evidence(
        bundle_payload,
        source=source,
        entry_id=entry_id,
        collector_pn=normalized_collector_pn,
    )
    path = export_cloud_evidence(
        config_dir=config_dir,
        evidence=evidence,
    )
    return CloudEvidenceRecord(path=path, payload=evidence)


def fetch_and_export_device_bundle_cloud_evidence(
    *,
    provider: str,
    config_dir: Path,
    username: str,
    password: str,
    collector_pn: str,
    source: str,
    entry_id: str = "",
) -> CloudEvidenceRecord:
    """Fetch and persist one provider-specific cloud-evidence bundle."""

    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider == "smartess":
        return fetch_and_export_smartess_device_bundle_cloud_evidence(
            config_dir=config_dir,
            username=username,
            password=password,
            collector_pn=collector_pn,
            source=source,
            entry_id=entry_id,
        )
    if normalized_provider == "valuecloud":
        return fetch_and_export_valuecloud_device_bundle_cloud_evidence(
            config_dir=config_dir,
            username=username,
            password=password,
            collector_pn=collector_pn,
            source=source,
            entry_id=entry_id,
        )
    raise RuntimeError(f"cloud_evidence_provider_not_supported:{normalized_provider or 'unknown'}")


def cloud_evidence_root(config_dir: Path) -> Path:
    """Return the cloud-evidence directory under one HA config dir."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_CLOUD_EVIDENCE_DIR


def build_cloud_evidence_payload(
    *,
    source: str,
    payload: dict[str, Any],
    collector_pn: str = "",
    entry_id: str = "",
    pn: str = "",
    sn: str = "",
    devcode: int | None = None,
    devaddr: int | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one transport-agnostic cloud evidence payload."""

    return {
        "evidence_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "match": {
            "entry_id": str(entry_id or "").strip(),
            "collector_pn": str(collector_pn or "").strip(),
        },
        "device_identity": {
            "pn": str(pn or "").strip(),
            "sn": str(sn or "").strip(),
            "devcode": devcode,
            "devaddr": devaddr,
        },
        "summary": dict(summary or {}),
        "payload": payload,
    }


def build_smartess_device_bundle_cloud_evidence(
    bundle_payload: dict[str, Any],
    *,
    source: str,
    entry_id: str = "",
    collector_pn: str = "",
) -> dict[str, Any]:
    """Build one normalized cloud-evidence payload from a SmartESS device bundle."""

    params = bundle_payload.get("request", {}).get("params", {})
    normalized = bundle_payload.get("normalized", {})
    normalized_list = normalized.get("device_list") if isinstance(normalized, dict) else None
    normalized_detail = normalized.get("device_detail") if isinstance(normalized, dict) else None
    normalized_settings = normalized.get("device_settings") if isinstance(normalized, dict) else None
    section_counts = (
        normalized_detail.get("section_counts") if isinstance(normalized_detail, dict) else None
    )
    return build_cloud_evidence_payload(
        source=source,
        payload=bundle_payload,
        entry_id=entry_id,
        collector_pn=collector_pn or str(params.get("pn") or ""),
        pn=str(params.get("pn") or ""),
        sn=str(params.get("sn") or ""),
        devcode=_maybe_int(params.get("devcode")),
        devaddr=_maybe_int(params.get("devaddr")),
        summary={
            "actions": list((bundle_payload.get("responses") or {}).keys()),
            "device_count": normalized_list.get("device_count") if isinstance(normalized_list, dict) else None,
            "detail_sections": sorted(section_counts.keys()) if isinstance(section_counts, dict) else [],
            "settings_field_count": (
                normalized_settings.get("field_count") if isinstance(normalized_settings, dict) else None
            ),
            "settings_mapped_field_count": (
                normalized_settings.get("mapped_field_count") if isinstance(normalized_settings, dict) else None
            ),
            "settings_exact_0925_field_count": (
                normalized_settings.get("exact_0925_field_count")
                if isinstance(normalized_settings, dict)
                else None
            ),
            "settings_probable_0925_field_count": (
                normalized_settings.get("probable_0925_field_count")
                if isinstance(normalized_settings, dict)
                else None
            ),
            "settings_cloud_only_field_count": (
                normalized_settings.get("cloud_only_field_count")
                if isinstance(normalized_settings, dict)
                else None
            ),
            "settings_current_values_included": (
                normalized_settings.get("current_values_included")
                if isinstance(normalized_settings, dict)
                else None
            ),
            "settings_write_action": (
                normalized_settings.get("write_action") if isinstance(normalized_settings, dict) else None
            ),
        },
    )


def build_valuecloud_device_bundle_cloud_evidence(
    bundle_payload: dict[str, Any],
    *,
    source: str,
    entry_id: str = "",
    collector_pn: str = "",
) -> dict[str, Any]:
    """Build one normalized cloud-evidence payload from a ValueCloud device bundle."""

    params = bundle_payload.get("request", {}).get("params", {})
    normalized = bundle_payload.get("normalized", {})
    normalized_list = normalized.get("device_list") if isinstance(normalized, dict) else None
    normalized_detail = normalized.get("device_detail") if isinstance(normalized, dict) else None
    normalized_pars = normalized.get("device_pars") if isinstance(normalized, dict) else None
    normalized_strategy = normalized.get("control_strategy") if isinstance(normalized, dict) else None
    normalized_ctrl = normalized.get("device_ctrl") if isinstance(normalized, dict) else None
    section_counts = (
        normalized_detail.get("section_counts") if isinstance(normalized_detail, dict) else None
    )
    responses = bundle_payload.get("responses") or {}
    optional_errors = sum(
        1
        for response in responses.values()
        if isinstance(response, dict) and response.get("status") == "error"
    )
    return build_cloud_evidence_payload(
        source=source,
        payload=bundle_payload,
        entry_id=entry_id,
        collector_pn=collector_pn or str(params.get("collector_pn") or params.get("pn") or ""),
        pn=str(params.get("pn") or ""),
        sn=str(params.get("sn") or ""),
        devcode=_maybe_int(params.get("devcode")),
        devaddr=_maybe_int(params.get("devaddr")),
        summary={
            "provider": "valuecloud",
            "actions": list(responses.keys()),
            "device_count": normalized_list.get("device_count") if isinstance(normalized_list, dict) else None,
            "detail_sections": sorted(section_counts.keys()) if isinstance(section_counts, dict) else [],
            "parameter_field_count": (
                normalized_pars.get("field_count") if isinstance(normalized_pars, dict) else None
            ),
            "control_strategy_field_count": (
                normalized_strategy.get("field_count") if isinstance(normalized_strategy, dict) else None
            ),
            "device_ctrl_field_count": (
                normalized_ctrl.get("field_count") if isinstance(normalized_ctrl, dict) else None
            ),
            "current_values_included": any(
                bool(item.get("current_values_included"))
                for item in (normalized_pars, normalized_strategy, normalized_ctrl)
                if isinstance(item, dict)
            ),
            "optional_action_error_count": optional_errors,
        },
    )
def export_cloud_evidence(
    *,
    config_dir: Path,
    evidence: dict[str, Any],
    overwrite: bool = False,
) -> Path:
    """Write one cloud-evidence JSON file under the HA config dir.

    Older files for the same identity stem are pruned so each collector identity
    keeps exactly one stored evidence file (the most recently exported one).
    """

    root = cloud_evidence_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)

    stem = _filename_stem(evidence)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = root / f"{stem}_{timestamp}.json"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    tmp_path = destination.with_suffix(destination.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(destination)
    _prune_older_files_for_stem(root, stem=stem, keep=destination)
    return destination


def _prune_older_files_for_stem(root: Path, *, stem: str, keep: Path) -> None:
    """Remove previous evidence files that share the destination's identity stem."""

    for path in root.glob(f"{stem}_*.json"):
        if path == keep:
            continue
        try:
            path.unlink()
        except OSError:
            continue


def remove_cloud_evidence_for_entry(
    config_dir: Path,
    *,
    entry_id: str = "",
    collector_pn: str = "",
) -> list[Path]:
    """Delete all cloud-evidence files matching the given identity.

    Returns the list of deleted paths (empty when nothing matched). Used from
    ``async_remove_entry`` so files containing collector PNs and masked tokens do
    not outlive the integration entry that produced them.
    """

    root = cloud_evidence_root(config_dir)
    if not root.exists():
        return []
    normalized_entry_id = str(entry_id or "").strip()
    normalized_collector_pn = str(collector_pn or "").strip()
    if not normalized_entry_id and not normalized_collector_pn:
        return []
    deleted: list[Path] = []
    for path in root.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if not _matches(
            payload,
            entry_id=normalized_entry_id,
            collector_pn=normalized_collector_pn,
        ):
            continue
        try:
            path.unlink()
        except OSError:
            continue
        deleted.append(path)
    return deleted


def load_latest_cloud_evidence(
    config_dir: Path,
    *,
    entry_id: str = "",
    collector_pn: str = "",
) -> CloudEvidenceRecord | None:
    """Return the latest matching cloud-evidence JSON file when available."""

    root = cloud_evidence_root(config_dir)
    if not root.exists():
        return None

    normalized_entry_id = str(entry_id or "").strip()
    normalized_collector_pn = str(collector_pn or "").strip()
    for path in sorted(root.glob("*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if _matches(payload, entry_id=normalized_entry_id, collector_pn=normalized_collector_pn):
            return CloudEvidenceRecord(path=path, payload=payload)
    return None


def _filename_stem(evidence: dict[str, Any]) -> str:
    match = evidence.get("match") if isinstance(evidence, dict) else None
    identity = evidence.get("device_identity") if isinstance(evidence, dict) else None
    raw = ""
    if isinstance(match, dict):
        raw = str(match.get("entry_id") or match.get("collector_pn") or "").strip()
    if not raw and isinstance(identity, dict):
        raw = str(identity.get("pn") or "").strip()
    return _slugify(raw or "cloud_evidence")


def _matches(payload: dict[str, Any], *, entry_id: str, collector_pn: str) -> bool:
    if not entry_id and not collector_pn:
        return True
    match = payload.get("match")
    if not isinstance(match, dict):
        return False
    payload_entry_id = str(match.get("entry_id") or "").strip()
    if entry_id and payload_entry_id == entry_id:
        return True
    if not collector_pn:
        return False

    payload_collector_pn = str(match.get("collector_pn") or "").strip()
    identity = payload.get("device_identity")
    payload_device_pn = ""
    if isinstance(identity, dict):
        payload_device_pn = str(identity.get("pn") or "").strip()

    return any(
        _collector_pn_matches(collector_pn, candidate)
        for candidate in (payload_collector_pn, payload_device_pn)
    )


def _collector_pn_matches(requested: str, candidate: str) -> bool:
    normalized_requested = str(requested or "").strip()
    normalized_candidate = str(candidate or "").strip()
    if not normalized_requested or not normalized_candidate:
        return False
    return (
        normalized_requested == normalized_candidate
        or normalized_requested.startswith(normalized_candidate)
        or normalized_candidate.startswith(normalized_requested)
    )


def _maybe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _slugify(value: str) -> str:
    cleaned = [char if char.isalnum() else "_" for char in str(value or "").strip()]
    collapsed = "".join(cleaned).strip("_")
    return collapsed or "cloud_evidence"
