"""Standalone storage helpers for collector proxy trace artifacts and session state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import shutil
from typing import Any
import zipfile

from ..const import LOCAL_METADATA_DIR, LOCAL_PROXY_TRACES_DIR


@dataclass(frozen=True, slots=True)
class ProxyTraceRecord:
    """One persisted proxy-trace manifest plus its parsed payload."""

    path: Path
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ProxyCaptureSessionState:
    """One persisted active proxy capture session state."""

    entry_id: str
    route_owner_id: str
    collector_pn: str
    trace_path: str
    original_endpoint: str
    proxy_endpoint: str
    restore_required: bool
    anonymized: bool
    started_at: str
    expires_at: str
    status: str


_ACTIVE_PROXY_CAPTURE_SESSION_STATUSES = {"starting", "running", "stopping", "restoring"}
_SERIAL_TOKEN_RE = re.compile(r"(?<![A-Z0-9])([A-Z][A-Z0-9]{7,23})(?![A-Z0-9])")
_SERIAL_TOKEN_BYTES_RE = re.compile(rb"(?<![A-Z0-9])([A-Z][A-Z0-9]{7,23})(?![A-Z0-9])")
_DIRECTIONAL_RAW_DUMP_NAMES = {
    "collector_to_server": "collector_to_server.raw.hex",
    "server_to_collector": "server_to_collector.raw.hex",
}
_RAW_TRACE_KIND_HEX_KEYS = {
    "chunk": "chunk_hex",
    "restore_drain_chunk": "chunk_hex",
    "tail": "remaining_hex",
}
_RAW_TRACE_DIRECTION_MAP = {
    "collector_to_cloud": "collector_to_server",
    "collector_restore_drain": "collector_to_server",
    "cloud_to_collector": "server_to_collector",
}


def proxy_trace_root(config_dir: Path) -> Path:
    """Return the proxy trace output directory under one HA config dir."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_PROXY_TRACES_DIR


def proxy_trace_public_root(config_dir: Path) -> Path:
    """Return the Home Assistant static file directory for proxy trace artifacts."""

    return config_dir / "www" / LOCAL_METADATA_DIR / LOCAL_PROXY_TRACES_DIR


def proxy_trace_download_url(filename: str) -> str:
    """Return the Home Assistant `/local` URL for one saved proxy trace artifact."""

    return f"/local/{LOCAL_METADATA_DIR}/{LOCAL_PROXY_TRACES_DIR}/{filename}"


def active_proxy_capture_state_path(config_dir: Path) -> Path:
    """Return the active proxy capture state file path."""

    return proxy_trace_root(config_dir) / "active_proxy_capture.json"


def build_proxy_trace_manifest(
    *,
    source: str,
    trace_path: str,
    entry_id: str = "",
    collector_pn: str = "",
    anonymized: bool = True,
    session: dict[str, Any] | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one normalized proxy trace manifest payload."""

    return {
        "manifest_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source or "").strip(),
        "match": {
            "entry_id": str(entry_id or "").strip(),
            "collector_pn": str(collector_pn or "").strip(),
        },
        "trace": {
            "path": str(trace_path or "").strip(),
            "anonymized": bool(anonymized),
        },
        "session": dict(session or {}),
        "summary": dict(summary or {}),
    }


def anonymize_proxy_trace_line(payload: dict[str, Any]) -> dict[str, Any]:
    """Return one share-safe proxy trace event with only collector serials masked."""

    return _anonymize_value(payload)


def anonymize_proxy_trace_text(trace_path: Path) -> str:
    """Return anonymized JSONL text for one raw proxy trace."""

    lines: list[str] = []
    if not trace_path.exists():
        return ""
    with trace_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                lines.append(
                    json.dumps(
                        {"kind": "invalid_line", "raw": _mask_serials_in_text(stripped)},
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                continue
            lines.append(
                json.dumps(
                    anonymize_proxy_trace_line(payload)
                    if isinstance(payload, dict)
                    else _anonymize_value(payload),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
    return "\n".join(lines) + ("\n" if lines else "")


def export_proxy_trace_manifest(
    *,
    config_dir: Path,
    manifest: dict[str, Any],
    overwrite: bool = False,
) -> Path:
    """Write one proxy trace manifest JSON file under the HA config dir."""

    root = proxy_trace_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)

    stem = _filename_stem(manifest)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = root / f"{stem}_{timestamp}.json"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    destination.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return destination


def publish_proxy_trace_download_copy(
    *,
    config_dir: Path,
    source_path: Path,
) -> tuple[Path, str]:
    """Publish one saved proxy trace artifact under `/local/...` and return its URL."""

    public_root = proxy_trace_public_root(config_dir)
    public_root.mkdir(parents=True, exist_ok=True)
    destination = public_root / source_path.name
    shutil.copy2(source_path, destination)
    return destination, proxy_trace_download_url(source_path.name)


def export_proxy_trace_bundle(
    *,
    manifest_path: Path,
    overwrite: bool = False,
) -> Path:
    """Create one ZIP bundle containing the saved proxy manifest and trace JSONL."""

    if not manifest_path.exists():
        raise FileNotFoundError(manifest_path)

    bundle_path = manifest_path.with_suffix(".zip")
    if bundle_path.exists() and not overwrite:
        return bundle_path

    manifest_payload = _read_manifest_payload(manifest_path)
    trace_path = _trace_path_from_payload(manifest_payload)
    anonymized = _manifest_payload_marks_trace_anonymized(manifest_payload)
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        if anonymized:
            archive.writestr(
                manifest_path.name,
                _dump_json(_anonymize_value(manifest_payload)),
            )
        else:
            archive.write(manifest_path, arcname=manifest_path.name)
        if trace_path is not None and trace_path.exists():
            if anonymized:
                archive.writestr(
                    f"{trace_path.stem}.anonymized{trace_path.suffix}",
                    anonymize_proxy_trace_text(trace_path),
                )
            else:
                archive.write(trace_path, arcname=trace_path.name)
            for direction_name, dump_text in build_proxy_trace_directional_hex_dumps(
                trace_path,
                anonymized=anonymized,
            ).items():
                archive.writestr(
                    f"{trace_path.stem}.{_DIRECTIONAL_RAW_DUMP_NAMES[direction_name]}",
                    dump_text,
                )
    return bundle_path


def build_proxy_trace_directional_hex_dumps(
    trace_path: Path,
    *,
    anonymized: bool,
) -> dict[str, str]:
    """Build raw directional transport dumps from one JSONL proxy trace."""

    dumps: dict[str, list[str]] = {
        "collector_to_server": [],
        "server_to_collector": [],
    }
    if not trace_path.exists():
        return {name: "" for name in dumps}

    with trace_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue

            direction_name = _RAW_TRACE_DIRECTION_MAP.get(str(payload.get("direction") or "").strip())
            hex_key = _RAW_TRACE_KIND_HEX_KEYS.get(str(payload.get("kind") or "").strip())
            if not direction_name or not hex_key:
                continue

            chunk_hex = _normalize_hex_text(str(payload.get(hex_key) or ""))
            if not chunk_hex:
                continue
            dumps[direction_name].append(
                _mask_serials_in_hex(chunk_hex) if anonymized else chunk_hex
            )

    return {
        name: "\n".join(lines) + ("\n" if lines else "")
        for name, lines in dumps.items()
    }


def load_latest_proxy_trace_manifest(
    config_dir: Path,
    *,
    entry_id: str = "",
    collector_pn: str = "",
) -> ProxyTraceRecord | None:
    """Return the latest matching proxy trace manifest when available."""

    root = proxy_trace_root(config_dir)
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
            return ProxyTraceRecord(path=path, payload=payload)
    return None


def build_proxy_capture_session_state(
    *,
    entry_id: str,
    route_owner_id: str = "",
    collector_pn: str,
    trace_path: str = "",
    original_endpoint: str,
    proxy_endpoint: str,
    restore_required: bool,
    anonymized: bool,
    started_at: str,
    expires_at: str,
    status: str,
) -> ProxyCaptureSessionState:
    """Build one persisted active proxy capture session state."""

    return ProxyCaptureSessionState(
        entry_id=str(entry_id or "").strip(),
        route_owner_id=str(route_owner_id or "").strip(),
        collector_pn=str(collector_pn or "").strip(),
        trace_path=str(trace_path or "").strip(),
        original_endpoint=str(original_endpoint or "").strip(),
        proxy_endpoint=str(proxy_endpoint or "").strip(),
        restore_required=bool(restore_required),
        anonymized=bool(anonymized),
        started_at=str(started_at or "").strip(),
        expires_at=str(expires_at or "").strip(),
        status=str(status or "").strip(),
    )


def proxy_capture_session_is_active(state: ProxyCaptureSessionState | None) -> bool:
    """Return whether one persisted proxy session still represents an active flow."""

    if state is None:
        return False
    return str(state.status or "").strip() in _ACTIVE_PROXY_CAPTURE_SESSION_STATUSES


def build_proxy_capture_lease_deadline(
    *,
    lease_seconds: int,
    now: datetime | None = None,
) -> str:
    """Return one ISO deadline for the current proxy session lease."""

    current = _normalize_utc_datetime(now)
    lease = max(int(lease_seconds), 1)
    return (current + timedelta(seconds=lease)).isoformat()


def refresh_proxy_capture_session_lease(
    state: ProxyCaptureSessionState,
    *,
    lease_seconds: int,
    now: datetime | None = None,
    status: str | None = None,
) -> ProxyCaptureSessionState:
    """Return one updated proxy session state with a refreshed lease deadline."""

    return build_proxy_capture_session_state(
        entry_id=state.entry_id,
        route_owner_id=state.route_owner_id,
        collector_pn=state.collector_pn,
        trace_path=state.trace_path,
        original_endpoint=state.original_endpoint,
        proxy_endpoint=state.proxy_endpoint,
        restore_required=state.restore_required,
        anonymized=state.anonymized,
        started_at=state.started_at,
        expires_at=build_proxy_capture_lease_deadline(lease_seconds=lease_seconds, now=now),
        status=str(status or state.status or "").strip(),
    )


def proxy_capture_session_is_expired(
    state: ProxyCaptureSessionState,
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether one proxy session lease deadline has already passed."""

    deadline = parse_proxy_capture_session_timestamp(state.expires_at)
    if deadline is None:
        return False
    return deadline <= _normalize_utc_datetime(now)


def proxy_capture_restore_guard_reason(
    state: ProxyCaptureSessionState,
    *,
    current_endpoint: str,
) -> str:
    """Return one reason why auto-restore should be skipped, or an empty string when safe."""

    if not state.restore_required or not state.original_endpoint:
        return ""

    normalized_current = str(current_endpoint or "").strip()
    normalized_proxy = str(state.proxy_endpoint or "").strip()

    if not normalized_current:
        return "current_endpoint_unavailable"
    if not normalized_proxy:
        return "proxy_endpoint_unavailable"
    if normalized_current != normalized_proxy:
        return "current_endpoint_changed"
    return ""


def parse_proxy_capture_session_timestamp(value: str) -> datetime | None:
    """Parse one persisted proxy-session timestamp into UTC when possible."""

    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_utc_datetime(value: datetime | None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def save_proxy_capture_session_state(
    *,
    config_dir: Path,
    state: ProxyCaptureSessionState,
) -> Path:
    """Persist the active proxy capture session state."""

    root = proxy_trace_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)
    path = active_proxy_capture_state_path(config_dir)
    payload = {
        "entry_id": state.entry_id,
        "route_owner_id": state.route_owner_id,
        "collector_pn": state.collector_pn,
        "trace_path": state.trace_path,
        "original_endpoint": state.original_endpoint,
        "proxy_endpoint": state.proxy_endpoint,
        "restore_required": state.restore_required,
        "anonymized": state.anonymized,
        "started_at": state.started_at,
        "expires_at": state.expires_at,
        "status": state.status,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_proxy_capture_session_state(config_dir: Path) -> ProxyCaptureSessionState | None:
    """Load the persisted active proxy capture session state when present."""

    path = active_proxy_capture_state_path(config_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return build_proxy_capture_session_state(
        entry_id=str(payload.get("entry_id") or ""),
        route_owner_id=str(payload.get("route_owner_id") or ""),
        collector_pn=str(payload.get("collector_pn") or ""),
        trace_path=str(payload.get("trace_path") or ""),
        original_endpoint=str(payload.get("original_endpoint") or ""),
        proxy_endpoint=str(payload.get("proxy_endpoint") or ""),
        restore_required=bool(payload.get("restore_required")),
        anonymized=bool(payload.get("anonymized", True)),
        started_at=str(payload.get("started_at") or ""),
        expires_at=str(payload.get("expires_at") or ""),
        status=str(payload.get("status") or ""),
    )


def clear_proxy_capture_session_state(config_dir: Path) -> None:
    """Delete the persisted active proxy capture session state when present."""

    path = active_proxy_capture_state_path(config_dir)
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _filename_stem(manifest: dict[str, Any]) -> str:
    match = manifest.get("match") if isinstance(manifest, dict) else None
    trace = manifest.get("trace") if isinstance(manifest, dict) else None
    raw = ""
    if isinstance(match, dict):
        raw = str(match.get("entry_id") or match.get("collector_pn") or "").strip()
    if not raw and isinstance(trace, dict):
        raw = Path(str(trace.get("path") or "").strip()).stem
    return _slugify(raw or "proxy_trace")


def _trace_path_from_manifest(manifest_path: Path) -> Path | None:
    return _trace_path_from_payload(_read_manifest_payload(manifest_path))


def _trace_path_from_payload(payload: dict[str, Any] | None) -> Path | None:
    if not isinstance(payload, dict):
        return None
    trace = payload.get("trace")
    if not isinstance(trace, dict):
        return None
    raw = str(trace.get("path") or "").strip()
    return Path(raw) if raw else None


def _manifest_marks_trace_anonymized(manifest_path: Path) -> bool:
    return _manifest_payload_marks_trace_anonymized(_read_manifest_payload(manifest_path))


def _manifest_payload_marks_trace_anonymized(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    trace = payload.get("trace")
    if not isinstance(trace, dict):
        return False
    return bool(trace.get("anonymized", False))


def _read_manifest_payload(manifest_path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _dump_json(payload: dict[str, Any] | list[Any] | str | int | float | bool | None) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def _anonymize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _anonymize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_anonymize_value(item) for item in value]
    if isinstance(value, str):
        masked_text = _mask_serials_in_text(value)
        if masked_text != value:
            return masked_text
        return _mask_serials_in_hex(value) if _looks_like_hex_blob(value) else value
    return value


def _looks_like_hex_blob(value: str) -> bool:
    normalized = _normalize_hex_text(value)
    return bool(normalized) and len(normalized) >= 8


def _normalize_hex_text(value: str) -> str:
    normalized = "".join(str(value or "").split())
    if not normalized or len(normalized) % 2 != 0:
        return ""
    if any(char not in "0123456789abcdefABCDEF" for char in normalized):
        return ""
    return normalized


def _mask_serials_in_hex(value: str) -> str:
    normalized = _normalize_hex_text(value)
    if not normalized:
        return _mask_serials_in_text(value)
    try:
        payload = bytes.fromhex(normalized)
    except ValueError:
        return _mask_serials_in_text(value)
    masked = _mask_serials_in_bytes(payload)
    return masked.hex() if masked != payload else normalized


def _mask_serials_in_bytes(payload: bytes) -> bytes:
    def _replace(match: re.Match[bytes]) -> bytes:
        token = match.group(1)
        try:
            text = token.decode("ascii")
        except UnicodeDecodeError:
            return token
        if not _looks_like_serial_token(text):
            return token
        return _mask_serial_token(text).encode("ascii")

    return _SERIAL_TOKEN_BYTES_RE.sub(_replace, payload)


def _mask_serials_in_text(value: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        token = match.group(1)
        return _mask_serial_token(token) if _looks_like_serial_token(token) else token

    return _SERIAL_TOKEN_RE.sub(_replace, str(value or ""))


def _looks_like_serial_token(token: str) -> bool:
    normalized = str(token or "").strip()
    return bool(normalized) and any(char.isalpha() for char in normalized) and sum(
        char.isdigit() for char in normalized
    ) >= 4


def _mask_serial_token(token: str) -> str:
    normalized = str(token or "")
    if len(normalized) <= 4:
        return "*" * len(normalized)
    if len(normalized) <= 8:
        visible_prefix = 2
        visible_suffix = 2
    else:
        visible_prefix = 4
        visible_suffix = 4
    hidden = max(len(normalized) - visible_prefix - visible_suffix, 1)
    return (
        normalized[:visible_prefix]
        + ("*" * hidden)
        + normalized[-visible_suffix:]
    )


def _matches(payload: dict[str, Any], *, entry_id: str, collector_pn: str) -> bool:
    if not entry_id and not collector_pn:
        return True
    match = payload.get("match")
    if not isinstance(match, dict):
        return False
    payload_entry_id = str(match.get("entry_id") or "").strip()
    payload_collector_pn = str(match.get("collector_pn") or "").strip()
    return bool(
        (entry_id and payload_entry_id == entry_id)
        or (collector_pn and payload_collector_pn == collector_pn)
    )


def _slugify(value: str) -> str:
    cleaned = [char if char.isalnum() else "_" for char in str(value or "").strip()]
    collapsed = "".join(cleaned).strip("_")
    return collapsed or "proxy_trace"
