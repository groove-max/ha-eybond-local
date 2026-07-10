"""Persistent local collector registry.

This registry intentionally lives outside Home Assistant config entries.  It is
keyed by collector PN and preserves facts that should survive deleting and
re-adding the integration entry, most importantly the original cloud callback
endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any


_REGISTRY_FILENAME = "eybond_local.collectors"
_PN_RE = re.compile(r"^[A-Za-z0-9_.:-]{4,128}$")


@dataclass(frozen=True, slots=True)
class CollectorRegistryRecord:
    """One persisted collector registry record."""

    collector_pn: str
    original_endpoint_raw: str
    cloud_profile_key: str
    source: str
    observed_at: str
    last_seen_ip: str = ""


def collector_registry_path(config_dir: Path) -> Path:
    """Return the Home Assistant storage path for the collector registry."""

    return Path(config_dir) / ".storage" / _REGISTRY_FILENAME


def build_collector_registry_record(
    *,
    collector_pn: str,
    original_endpoint_raw: str,
    cloud_profile_key: str = "",
    source: str = "",
    observed_at: str = "",
    last_seen_ip: str = "",
) -> CollectorRegistryRecord:
    """Build one normalized collector registry record."""

    normalized_pn = _normalize_collector_pn(collector_pn)
    if not normalized_pn:
        raise ValueError("collector_pn_invalid")
    normalized_endpoint = str(original_endpoint_raw or "").strip()
    if not normalized_endpoint:
        raise ValueError("original_endpoint_raw_invalid")
    timestamp = str(observed_at or "").strip() or datetime.now(timezone.utc).isoformat()
    return CollectorRegistryRecord(
        collector_pn=normalized_pn,
        original_endpoint_raw=normalized_endpoint,
        cloud_profile_key=str(cloud_profile_key or "").strip().lower(),
        source=str(source or "").strip() or "runtime_observed",
        observed_at=timestamp,
        last_seen_ip=str(last_seen_ip or "").strip(),
    )


def load_collector_registry(config_dir: Path) -> dict[str, CollectorRegistryRecord]:
    """Load the local collector registry.

    Malformed registry content is treated as empty so startup and support flows
    fail closed instead of crashing.
    """

    path = collector_registry_path(config_dir)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    records_payload = payload.get("collectors", {})
    if not isinstance(records_payload, dict):
        return {}

    records: dict[str, CollectorRegistryRecord] = {}
    for key, raw_record in records_payload.items():
        if not isinstance(raw_record, dict):
            continue
        collector_pn = _normalize_collector_pn(raw_record.get("collector_pn") or key)
        endpoint = str(raw_record.get("original_endpoint_raw") or "").strip()
        if not collector_pn or not endpoint:
            continue
        try:
            record = build_collector_registry_record(
                collector_pn=collector_pn,
                original_endpoint_raw=endpoint,
                cloud_profile_key=str(raw_record.get("cloud_profile_key") or ""),
                source=str(raw_record.get("source") or ""),
                observed_at=str(raw_record.get("observed_at") or ""),
                last_seen_ip=str(raw_record.get("last_seen_ip") or ""),
            )
        except ValueError:
            continue
        records[record.collector_pn] = record
    return records


def save_collector_registry(
    *,
    config_dir: Path,
    records: dict[str, CollectorRegistryRecord],
) -> Path:
    """Persist collector registry records atomically enough for HA local storage."""

    path = collector_registry_path(config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "version": 1,
        "collectors": {
            key: _record_to_payload(record)
            for key, record in sorted(records.items())
            if key and record.original_endpoint_raw
        },
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)
    return path


def get_collector_registry_record(
    *,
    config_dir: Path,
    collector_pn: str,
) -> CollectorRegistryRecord | None:
    """Return one registry record by collector PN."""

    normalized_pn = _normalize_collector_pn(collector_pn)
    if not normalized_pn:
        return None
    return load_collector_registry(config_dir).get(normalized_pn)


def get_collector_registry_record_by_last_seen_ip(
    *,
    config_dir: Path,
    last_seen_ip: str,
) -> CollectorRegistryRecord | None:
    """Return the only registry record last seen at this IP.

    IP based restore is deliberately fail-closed: a home network may reuse IPs or
    have several collectors behind the same visible peer.  The caller gets a
    record only when the match is unique.
    """

    normalized_ip = str(last_seen_ip or "").strip()
    if not normalized_ip:
        return None

    matches = [
        record
        for record in load_collector_registry(config_dir).values()
        if record.last_seen_ip == normalized_ip
    ]
    if len(matches) != 1:
        return None
    return matches[0]


def remember_collector_original_endpoint(
    *,
    config_dir: Path,
    collector_pn: str,
    original_endpoint_raw: str,
    cloud_profile_key: str = "",
    source: str = "",
    observed_at: str = "",
    last_seen_ip: str = "",
) -> CollectorRegistryRecord:
    """Persist one original endpoint fact without replacing an existing endpoint."""

    record = build_collector_registry_record(
        collector_pn=collector_pn,
        original_endpoint_raw=original_endpoint_raw,
        cloud_profile_key=cloud_profile_key,
        source=source,
        observed_at=observed_at,
        last_seen_ip=last_seen_ip,
    )
    records = load_collector_registry(config_dir)
    existing = records.get(record.collector_pn)
    if existing is not None and existing.original_endpoint_raw:
        if last_seen_ip and existing.last_seen_ip != record.last_seen_ip:
            existing = CollectorRegistryRecord(
                collector_pn=existing.collector_pn,
                original_endpoint_raw=existing.original_endpoint_raw,
                cloud_profile_key=existing.cloud_profile_key or record.cloud_profile_key,
                source=existing.source,
                observed_at=existing.observed_at,
                last_seen_ip=record.last_seen_ip,
            )
            records[existing.collector_pn] = existing
            save_collector_registry(config_dir=config_dir, records=records)
        return existing
    records[record.collector_pn] = record
    save_collector_registry(config_dir=config_dir, records=records)
    return record


def _record_to_payload(record: CollectorRegistryRecord) -> dict[str, str]:
    return {
        "collector_pn": record.collector_pn,
        "original_endpoint_raw": record.original_endpoint_raw,
        "cloud_profile_key": record.cloud_profile_key,
        "source": record.source,
        "observed_at": record.observed_at,
        "last_seen_ip": record.last_seen_ip,
    }


def _normalize_collector_pn(value: object) -> str:
    candidate = str(value or "").strip()
    if not candidate or _PN_RE.fullmatch(candidate) is None:
        return ""
    return candidate
