"""Persistence helpers for shadow-learning runtime session lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from ..const import LOCAL_METADATA_DIR


_SHADOW_SESSION_DIR = "shadow_learning_traces"
_SHADOW_SESSION_FILE = "active_shadow_learning.json"
_ACTIVE_SHADOW_LEARNING_SESSION_STATUSES = {
    "preflight",
    "starting",
    "waiting_for_collector",
    "connecting_upstream",
    "ready",
    "learning",
    "degraded",
    "restoring",
}


@dataclass(frozen=True, slots=True)
class ShadowLearningSessionState:
    """One persisted active shadow-learning session state."""

    entry_id: str
    route_owner_id: str
    collector_pn: str
    trace_path: str
    original_endpoint: str
    proxy_endpoint: str
    upstream_endpoint: str
    restore_required: bool
    started_at: str
    expires_at: str
    updated_at: str
    restore_attempt_count: int
    last_restore_attempt_at: str
    last_restore_error: str
    status: str


def shadow_learning_session_root(config_dir: Path) -> Path:
    """Return the root directory for shadow-learning traces and state."""

    return Path(config_dir) / LOCAL_METADATA_DIR / _SHADOW_SESSION_DIR


def active_shadow_learning_state_path(config_dir: Path) -> Path:
    """Return the persisted active shadow-learning session state path."""

    return shadow_learning_session_root(config_dir) / _SHADOW_SESSION_FILE


def build_shadow_learning_session_state(
    *,
    entry_id: str,
    route_owner_id: str = "",
    collector_pn: str,
    trace_path: str,
    original_endpoint: str,
    proxy_endpoint: str,
    upstream_endpoint: str,
    restore_required: bool,
    started_at: str,
    updated_at: str,
    expires_at: str = "",
    restore_attempt_count: int = 0,
    last_restore_attempt_at: str = "",
    last_restore_error: str = "",
    status: str = "",
) -> ShadowLearningSessionState:
    """Build one normalized shadow-learning session state record."""

    return ShadowLearningSessionState(
        entry_id=str(entry_id or "").strip(),
        route_owner_id=str(route_owner_id or "").strip(),
        collector_pn=str(collector_pn or "").strip(),
        trace_path=str(trace_path or "").strip(),
        original_endpoint=str(original_endpoint or "").strip(),
        proxy_endpoint=str(proxy_endpoint or "").strip(),
        upstream_endpoint=str(upstream_endpoint or "").strip(),
        restore_required=bool(restore_required),
        started_at=str(started_at or "").strip(),
        expires_at=str(expires_at or "").strip(),
        updated_at=str(updated_at or "").strip(),
        restore_attempt_count=max(0, int(restore_attempt_count)),
        last_restore_attempt_at=str(last_restore_attempt_at or "").strip(),
        last_restore_error=str(last_restore_error or "").strip(),
        status=str(status or "").strip(),
    )


def build_shadow_learning_lease_deadline(*, lease_seconds: int, now: datetime | None = None) -> str:
    """Return one ISO deadline for the current shadow-learning session lease."""

    seconds = max(int(lease_seconds), 1)
    base_now = now if now is not None else datetime.now(timezone.utc)
    if base_now.tzinfo is None:
        base_now = base_now.replace(tzinfo=timezone.utc)
    return (base_now.astimezone(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def parse_shadow_learning_session_timestamp(value: str) -> datetime | None:
    """Parse one persisted shadow-learning session timestamp in UTC."""

    candidate = str(value or "").strip()
    if not candidate:
        return None
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def shadow_learning_session_is_expired(
    state: ShadowLearningSessionState,
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether one shadow-learning session lease deadline has already passed."""

    deadline = parse_shadow_learning_session_timestamp(state.expires_at)
    if deadline is None:
        return False
    current = now if now is not None else datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    return current >= deadline


def shadow_learning_session_is_active(state: ShadowLearningSessionState | None) -> bool:
    """Return whether one shadow-learning session state is active."""

    if state is None:
        return False
    return str(state.status or "").strip() in _ACTIVE_SHADOW_LEARNING_SESSION_STATUSES


def save_shadow_learning_session_state(
    *,
    config_dir: Path,
    state: ShadowLearningSessionState,
) -> Path:
    """Persist the active shadow-learning session state."""

    root = shadow_learning_session_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)
    path = active_shadow_learning_state_path(config_dir)
    payload = {
        "entry_id": state.entry_id,
        "route_owner_id": state.route_owner_id,
        "collector_pn": state.collector_pn,
        "trace_path": state.trace_path,
        "original_endpoint": state.original_endpoint,
        "proxy_endpoint": state.proxy_endpoint,
        "upstream_endpoint": state.upstream_endpoint,
        "restore_required": state.restore_required,
        "started_at": state.started_at,
        "expires_at": state.expires_at,
        "updated_at": state.updated_at,
        "restore_attempt_count": state.restore_attempt_count,
        "last_restore_attempt_at": state.last_restore_attempt_at,
        "last_restore_error": state.last_restore_error,
        "status": state.status,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_shadow_learning_session_state(config_dir: Path) -> ShadowLearningSessionState | None:
    """Load the persisted active shadow-learning session state when present."""

    path = active_shadow_learning_state_path(config_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    restore_attempt_count = payload.get("restore_attempt_count") or 0
    try:
        parsed_restore_attempt_count = int(restore_attempt_count)
    except (TypeError, ValueError):
        parsed_restore_attempt_count = 0
    return build_shadow_learning_session_state(
        entry_id=str(payload.get("entry_id") or ""),
        route_owner_id=str(payload.get("route_owner_id") or ""),
        collector_pn=str(payload.get("collector_pn") or ""),
        trace_path=str(payload.get("trace_path") or ""),
        original_endpoint=str(payload.get("original_endpoint") or ""),
        proxy_endpoint=str(payload.get("proxy_endpoint") or ""),
        upstream_endpoint=str(payload.get("upstream_endpoint") or ""),
        restore_required=bool(payload.get("restore_required")),
        started_at=str(payload.get("started_at") or ""),
        expires_at=str(payload.get("expires_at") or ""),
        updated_at=str(payload.get("updated_at") or ""),
        restore_attempt_count=parsed_restore_attempt_count,
        last_restore_attempt_at=str(payload.get("last_restore_attempt_at") or ""),
        last_restore_error=str(payload.get("last_restore_error") or ""),
        status=str(payload.get("status") or ""),
    )


def clear_shadow_learning_session_state(config_dir: Path) -> None:
    """Delete the persisted active shadow-learning session state when present."""

    path = active_shadow_learning_state_path(config_dir)
    try:
        path.unlink()
    except FileNotFoundError:
        return


def shadow_learning_session_timestamp() -> str:
    """Return one UTC timestamp used for shadow session state transitions."""

    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "ShadowLearningSessionState",
    "build_shadow_learning_lease_deadline",
    "build_shadow_learning_session_state",
    "clear_shadow_learning_session_state",
    "load_shadow_learning_session_state",
    "parse_shadow_learning_session_timestamp",
    "save_shadow_learning_session_state",
    "shadow_learning_session_is_active",
    "shadow_learning_session_is_expired",
    "shadow_learning_session_timestamp",
]
