"""Persistence helpers for shadow-learning runtime session lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from threading import Lock

from ...const import LOCAL_METADATA_DIR
from .session_journal import (
    FINALIZATION_OUTCOMES,
    atomic_write_record,
    finalization_path,
    inspect_finalization,
    sync_directory,
)


_SHADOW_SESSION_DIR = "shadow_learning_traces"
_SHADOW_SESSION_FILE = "active_shadow_learning.json"
_SESSION_RECORD_LOCK = Lock()
_ACTIVE_SHADOW_LEARNING_SESSION_STATUSES = {
    "preflight",
    "starting",
    "waiting_for_collector",
    "connecting_upstream",
    "waiting_for_cloud_protocol",
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
    protection_version: int = 0
    cloud_source: str = ""
    cloud_commands_possible: bool = False


def shadow_learning_session_root(config_dir: Path) -> Path:
    """Return the root directory for shadow-learning traces and state."""

    return Path(config_dir) / LOCAL_METADATA_DIR / _SHADOW_SESSION_DIR


def active_shadow_learning_state_path(config_dir: Path, *, entry_id: str = "") -> Path:
    """Return the persisted active shadow-learning session state path."""

    name = ("active_" + hashlib.sha256(entry_id.encode()).hexdigest() + ".json"
            if entry_id else _SHADOW_SESSION_FILE)
    return shadow_learning_session_root(config_dir) / name


def _owned_session_path(config_dir: Path, entry_id: str) -> Path:
    """Retain an existing legacy owner's record; new owners use separate files.

    The unscoped API is retained for legacy readers/tools. Product callers supply
    their entry identity. No migration, overwrite or deletion of a foreign
    recovery record is needed to start another collector's learning session.
    """
    legacy = active_shadow_learning_state_path(config_dir)
    if not entry_id:
        return legacy
    scoped = active_shadow_learning_state_path(config_dir, entry_id=entry_id)
    if scoped.exists():
        return scoped
    if legacy.exists():
        state = load_shadow_learning_session_state(config_dir)
        if state is None or not state.entry_id or state.entry_id == entry_id:
            return legacy
    return scoped


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
    protection_version: int = 0,
    cloud_source: str = "",
    cloud_commands_possible: bool = False,
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
        protection_version=protection_version,
        cloud_source=cloud_source,
        cloud_commands_possible=cloud_commands_possible,
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
    entry_id: str = "",
) -> Path:
    """Persist the active shadow-learning session state."""

    with _SESSION_RECORD_LOCK:
        if entry_id and entry_id != state.entry_id:
            raise RuntimeError("shadow_learning_recovery_record_owned")
        path = _owned_session_path(config_dir, entry_id)
        current = load_shadow_learning_session_state(config_dir, entry_id=entry_id)
        legacy = load_shadow_learning_session_state(config_dir)
        if entry_id and path != active_shadow_learning_state_path(config_dir):
            if active_shadow_learning_state_path(config_dir).exists() and (
                legacy is None or not legacy.entry_id or not legacy.collector_pn
                or legacy.entry_id == entry_id or legacy.collector_pn == state.collector_pn
            ):
                raise RuntimeError("shadow_learning_recovery_record_owned")
        if path.exists() and (
            current is None or (current.entry_id, current.route_owner_id, current.collector_pn)
            != (state.entry_id, state.route_owner_id, state.collector_pn)
        ):
            raise RuntimeError("shadow_learning_recovery_record_owned")
        if current is not None and current.cloud_commands_possible and not state.cloud_commands_possible:
            from .protection_store import CloudLearningProtectionStore
            from ...connection.cloud_learning_protection import CloudLearningProtectionRequired
            store = CloudLearningProtectionStore(config_dir)
            try:
                hold = store.load(current.collector_pn)
                if (hold is None or current.protection_version != 1 or state.protection_version != 1
                        or hold.entry_id != current.entry_id
                        or current.route_owner_id != "shadow_learning:" + hold.study_id
                        or current.cloud_source != hold.source or state.cloud_source != hold.source):
                    raise ValueError("no_receipt")
                try:
                    store.require_no_request_receipt(hold)
                except CloudLearningProtectionRequired:
                    if current.status not in {"restoring", "restore_failed"} or state.status != current.status:
                        raise
                    from .completion_store import CompletedLearningStore
                    CompletedLearningStore(config_dir).require_completed_receipt(hold)
            except (ValueError, CloudLearningProtectionRequired):
                raise RuntimeError("shadow_learning_recovery_intent_regression") from None
        return _save_shadow_learning_session_state(config_dir=config_dir, state=state, entry_id=entry_id)


def _save_shadow_learning_session_state(*, config_dir: Path, state: ShadowLearningSessionState, entry_id: str = "") -> Path:
    root = shadow_learning_session_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)
    # Persist the parent links too when this is the first recovery record.
    sync_directory(Path(config_dir))
    sync_directory(root.parent)
    path = _owned_session_path(config_dir, entry_id)
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
        "protection_version": state.protection_version,
        "cloud_source": state.cloud_source,
        "cloud_commands_possible": state.cloud_commands_possible,
    }
    atomic_write_record(path, payload)
    return path


def replace_held_shadow_learning_session(*, config_dir: Path, previous: ShadowLearningSessionState,
                                       state: ShadowLearningSessionState, hold) -> Path:
    """Archive an exact stopped run before replacing its journal; retain the hold.

    Every durable prefix still has a blocking journal and the immutable PN hold.
    This does not grant endpoint mutation or cloud dispatch authority.
    """
    import hashlib
    from dataclasses import asdict
    from .protection_store import CloudLearningProtectionStore

    with _SESSION_RECORD_LOCK:
        CloudLearningProtectionStore(config_dir).require_exact_hold(hold)
        if (load_shadow_learning_session_state(config_dir, entry_id=previous.entry_id) != previous
                or previous.status != "restore_failed" or previous.protection_version != 1
                or not previous.cloud_commands_possible or not state.cloud_commands_possible
                or state.protection_version != 1 or state.status != "preflight"
                or previous.entry_id != state.entry_id or state.entry_id != hold.entry_id
                or previous.collector_pn != state.collector_pn or state.collector_pn != hold.collector_pn
                or previous.route_owner_id == state.route_owner_id
                or previous.proxy_endpoint != state.proxy_endpoint
                or previous.upstream_endpoint != state.upstream_endpoint
                or state.original_endpoint != state.proxy_endpoint or state.restore_required):
            raise RuntimeError("cloud_learning_retry_record_mismatch")
        archive = shadow_learning_session_root(config_dir) / "retained_runs"
        archive.mkdir(mode=0o700, exist_ok=True)
        sync_directory(archive.parent)
        key = hashlib.sha256(previous.route_owner_id.encode()).hexdigest()
        path = archive / (key + ".json")
        payload = asdict(previous)
        if path.exists():
            if json.loads(path.read_text()) != payload:
                raise RuntimeError("cloud_learning_retry_archive_conflict")
        else:
            atomic_write_record(path, payload)
        return _save_shadow_learning_session_state(config_dir=config_dir, state=state, entry_id=state.entry_id)


def load_shadow_learning_session_state(config_dir: Path, *, entry_id: str = "") -> ShadowLearningSessionState | None:
    """Load the persisted active shadow-learning session state when present."""

    path = _owned_session_path(config_dir, entry_id)
    return _load_session_path(path)


def find_shadow_learning_session_for_collector(config_dir: Path, *, collector_pn: str):
    """Find retained intent after entry recreation; this grants no ownership."""
    matches = []
    for path in sorted(shadow_learning_session_root(config_dir).glob("active_*.json")):
        state = _load_session_path(path)
        if state is not None and collector_pn and state.collector_pn == collector_pn:
            matches.append(state)
    return next((s for s in matches if s.cloud_commands_possible or s.protection_version != 1),
                matches[0] if matches else None)


def _load_session_path(path: Path) -> ShadowLearningSessionState | None:
    if not path.exists():
        return None
    try:
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            return None
        payload = json.loads(raw)
    except (OSError, UnicodeError, ValueError, RecursionError):
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
        protection_version=(1 if type(payload.get("protection_version")) is int
                            and payload["protection_version"] == 1
                            and type(payload.get("cloud_source")) is str
                            and payload.get("cloud_source") in {"smartess", "dessmonitor", "valuecloud"}
                            and type(payload.get("cloud_commands_possible")) is bool else 0),
        cloud_source=payload.get("cloud_source", ""),
        cloud_commands_possible=payload.get("cloud_commands_possible") is not False,
    )


def clear_shadow_learning_session_state(
    config_dir: Path,
    *,
    expected_state: ShadowLearningSessionState,
    outcome: str,
) -> None:
    """Persist a historical receipt BEFORE removing the matching active record.

    A failed receipt write leaves recovery pending. A stale owner cannot remove
    a newer/foreign record. Neither this receipt nor absence grants cloud access.
    Callers serialize the operation and drain the filesystem worker on cancel.
    """

    with _SESSION_RECORD_LOCK:
        _clear_shadow_learning_session_state(config_dir, expected_state=expected_state, outcome=outcome)


def _clear_shadow_learning_session_state(config_dir, *, expected_state, outcome):
    if type(expected_state) is not ShadowLearningSessionState:
        raise RuntimeError("shadow_learning_recovery_record_changed")
    path = _owned_session_path(config_dir, expected_state.entry_id)
    state = load_shadow_learning_session_state(config_dir, entry_id=expected_state.entry_id)
    if type(expected_state) is not ShadowLearningSessionState or state != expected_state:
        raise RuntimeError("shadow_learning_recovery_record_changed")
    if outcome not in FINALIZATION_OUTCOMES:
        raise ValueError("shadow_learning_finalization_outcome_invalid")
    atomic_write_record(
        finalization_path(path.parent, entry_id=state.entry_id, collector_pn=state.collector_pn),
        {
            "schema_version": 1,
            "entry_id": state.entry_id,
            "collector_pn": state.collector_pn,
            "outcome": outcome,
            "completed_at": shadow_learning_session_timestamp(),
        },
    )
    try:
        path.unlink()
    except FileNotFoundError:
        return
    sync_directory(path.parent)


def inspect_shadow_learning_recovery(
    config_dir: Path, *, entry_id: str, collector_pn: str,
) -> dict[str, object]:
    """Read diagnostic facts, never grant ownership or change recovery state.

    Do not export endpoints, identifiers, filenames, arbitrary errors or a foreign
    entry's record. A missing record is not proof of successful restoration.
    """
    historical = inspect_finalization(
        shadow_learning_session_root(config_dir), entry_id=entry_id, collector_pn=collector_pn,
    )
    return {
        **historical,
        **_inspect_active_recovery(config_dir, entry_id=entry_id, collector_pn=collector_pn),
    }


def _inspect_active_recovery(
    config_dir: Path, *, entry_id: str, collector_pn: str,
) -> dict[str, object]:
    prefix = "shadow_learning_recovery_"
    try:
        path = _owned_session_path(config_dir, entry_id)
        if not path.exists():
            path = active_shadow_learning_state_path(config_dir)
        with path.open("rb") as stream:
            raw = stream.read(65537)
    except FileNotFoundError:
        return {prefix + "disk_status": "missing"}
    except OSError:
        return {prefix + "disk_status": "unreadable"}
    if len(raw) > 65536:
        return {prefix + "disk_status": "oversized"}
    try:
        payload = json.loads(raw)
    except (UnicodeError, ValueError, RecursionError):
        return {prefix + "disk_status": "invalid_json"}
    if not isinstance(payload, dict):
        return {prefix + "disk_status": "invalid_record"}
    owner = payload.get("entry_id")
    pn = payload.get("collector_pn")
    if not isinstance(owner, str) or not isinstance(pn, str):
        return {prefix + "disk_status": "invalid_record"}
    if owner and owner != entry_id:
        return {prefix + "disk_status": "foreign_entry"}
    if collector_pn and pn and pn != collector_pn:
        return {prefix + "disk_status": "collector_mismatch"}
    if not owner or not pn or not collector_pn:
        return {prefix + "disk_status": "identity_incomplete"}
    status = payload.get("status")
    known_statuses = _ACTIVE_SHADOW_LEARNING_SESSION_STATUSES | {
        "restore_failed", "stopped", "failed",
    }
    status = status if isinstance(status, str) and status in known_statuses else "unknown"
    result: dict[str, object] = {
        prefix + "disk_status": "current_entry",
        prefix + "record_status": status,
        prefix + "pending": status in {"restoring", "restore_failed"},
        prefix + "restore_required": payload.get("restore_required") is True,
        prefix + "original_endpoint_present": bool(payload.get("original_endpoint")),
        prefix + "last_restore_error_present": bool(payload.get("last_restore_error")),
    }
    attempts = payload.get("restore_attempt_count")
    if type(attempts) is int and attempts >= 0:
        result[prefix + "attempt_count"] = attempts
    return result


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
