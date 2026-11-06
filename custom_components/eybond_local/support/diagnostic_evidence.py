"""Bounded, identity-scoped last diagnostic capture for support archives.

This is an optional evidence cache, not runtime state or routing authority.
Only a redacted projection is cached. No path from an input payload is opened.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile

from ..collector_identity import validated_collector_pn
from ..const import CONF_COLLECTOR_PN, LOCAL_METADATA_DIR
from .redaction import sanitize_support_payload


MAX_RECORD_BYTES = 1_048_576
MAX_AGE = timedelta(days=7)
DIAGNOSTIC_MEMBER = "evidence/diagnostic_run.json"
PROTOCOL_MEMBER = "evidence/protocol_candidates.json"


@dataclass(frozen=True, slots=True)
class DiagnosticEvidenceScope:
    fingerprint: str
    identity_level: str


def _field(obj: object, key: str, default=None):
    return obj.get(key, default) if isinstance(obj, Mapping) else getattr(obj, key, default)


def diagnostic_evidence_scope(
    entry_id: str, *, configured_pn: str = "", collector=None, inverter=None,
) -> DiagnosticEvidenceScope | None:
    """Bind to reported identity, not IP; unknown inverter stays explicitly so."""

    live = validated_collector_pn(_field(collector, "collector_pn", ""))
    configured = validated_collector_pn(configured_pn)
    pn = live or configured
    if live and configured and configured.startswith(live):
        pn = configured
    if not pn or not entry_id:
        return None
    identity = {"entry_id": entry_id, "collector_pn": pn, "inverter": None}
    level = "collector_only"
    if inverter is not None:
        target = _field(inverter, "probe_target")
        route = {key: _field(target, key) for key in ("devcode", "collector_addr", "device_addr")}
        if any(type(value) is not int for value in route.values()):
            return None
        fields = {key: _field(inverter, key, "") for key in (
            "driver_key", "serial_number", "model_name", "variant_key",
        )}
        if any(type(value) is not str for value in fields.values()):
            return None
        identity["inverter"] = {**fields, "route": route}
        level = "collector_with_inverter_snapshot"
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return DiagnosticEvidenceScope(hashlib.sha256(encoded).hexdigest(), level)


def scope_from_bundle(entry_id: str, bundle: dict) -> DiagnosticEvidenceScope | None:
    runtime = bundle.get("runtime") or {}
    entry = bundle.get("entry") or {}
    if not isinstance(runtime, Mapping) or not isinstance(entry, Mapping):
        return None
    data = entry.get("data") or {}
    if not isinstance(data, Mapping):
        return None
    return diagnostic_evidence_scope(
        entry_id, configured_pn=data.get(CONF_COLLECTOR_PN, ""),
        collector=runtime.get("collector"), inverter=runtime.get("inverter"),
    )


def _record_path(config_dir: Path, entry_id: str, *, create: bool = False) -> Path:
    root = config_dir / LOCAL_METADATA_DIR / "diagnostic_evidence"
    # Do not follow an optional evidence directory outside the config tree.
    if root.is_symlink() or root.parent.is_symlink():
        raise OSError("diagnostic_evidence_symlink")
    if create:
        root.mkdir(parents=True, exist_ok=True)
    name = hashlib.sha256(entry_id.encode()).hexdigest()
    return root / f"{name}.json"


def save_diagnostic_evidence(
    config_dir: Path, entry_id: str, scope: DiagnosticEvidenceScope | None,
    diagnostic: dict, protocol_report: dict,
) -> str:
    """Atomically replace the last attempt, including failed/unbound attempts."""

    record = {
        "version": 1,
        "scope": scope.fingerprint if scope is not None else None,
        "identity_level": scope.identity_level if scope is not None else "unbound",
        "finished_at": diagnostic.get("finished_at"),
        "status": "available" if scope is not None else "identity_unavailable_or_changed",
        "diagnostic": sanitize_support_payload(diagnostic),
        "protocol_report": sanitize_support_payload(protocol_report),
    }
    raw = json.dumps(record, ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > MAX_RECORD_BYTES:
        # Replace, not retain an older successful run as if it were the latest.
        record = {key: value for key, value in record.items() if key not in {"diagnostic", "protocol_report"}}
        record["status"] = "record_too_large"
        raw = json.dumps(record).encode()
    path = _record_path(config_dir, entry_id, create=True)
    name = None
    try:
        fd, name = tempfile.mkstemp(prefix=".diagnostic-", dir=path.parent)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
        os.replace(name, path)
    finally:
        if name is not None:
            Path(name).unlink(missing_ok=True)
    return record["status"]


def load_diagnostic_evidence(
    config_dir: Path, entry_id: str, scope: DiagnosticEvidenceScope | None,
    *, now: datetime | None = None,
) -> tuple[dict, dict]:
    """Return an archive manifest summary and members, without contacting devices."""

    def unavailable(reason: str) -> tuple[dict, dict]:
        return {"status": reason}, {}

    if scope is None:
        return unavailable("identity_unavailable")
    try:
        path = _record_path(config_dir, entry_id)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return unavailable("invalid_record")
            raw = stream.read(MAX_RECORD_BYTES + 1)
        if len(raw) > MAX_RECORD_BYTES:
            return unavailable("record_too_large")
        record = json.loads(raw)
        if not isinstance(record, dict) or record.get("version") != 1:
            return unavailable("invalid_record")
        if record.get("scope") != scope.fingerprint:
            return unavailable("identity_mismatch")
        if record.get("status") != "available":
            return unavailable("record_too_large" if record.get("status") == "record_too_large" else "invalid_record")
        finished = datetime.fromisoformat(record["finished_at"])
        if finished.tzinfo is None:
            return unavailable("invalid_record")
        age = (now or datetime.now(timezone.utc)) - finished
        if age < timedelta(0) or age > MAX_AGE:
            return unavailable("outside_retention_window")
        diagnostic = record.get("diagnostic")
        report = record.get("protocol_report")
        if not isinstance(diagnostic, dict) or not isinstance(report, dict):
            return unavailable("invalid_record")
        if diagnostic.get("finished_at") != record["finished_at"]:
            return unavailable("invalid_record")
    except FileNotFoundError:
        return unavailable("not_recorded")
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        return unavailable("invalid_record")
    return {
        "status": "included", "identity_level": scope.identity_level,
        "finished_at": record["finished_at"], "age_seconds": int(age.total_seconds()),
        "diagnostic_run": DIAGNOSTIC_MEMBER, "protocol_candidates": PROTOCOL_MEMBER,
        "historical_capture": True,
    }, {
        DIAGNOSTIC_MEMBER: sanitize_support_payload(diagnostic),
        PROTOCOL_MEMBER: sanitize_support_payload(report),
    }
