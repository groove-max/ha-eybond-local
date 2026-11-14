"""Explicit maintainer review of historical runs, NOT a reconstructed live ledger.

The reviewer establishes terminal delivery for EVERY run. Local records and
trace digests bind that decision to one immutable hold and one stopped history.
This module never contacts a collector or interprets a share ZIP as authority.
"""
from __future__ import annotations

from dataclasses import asdict, fields, replace
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import stat

from ...connection.cloud_learning_protection import CloudLearningHold
from . import session
from .protection_store import CloudLearningProtectionStore, _sync_directory, _unique_keys

_MAX_RUNS = 32
_MAX_RECORD = 65536
_MAX_TRACE = 16 * 1024 * 1024
_VOLATILE = frozenset({"status", "updated_at", "restore_attempt_count", "last_restore_attempt_at", "last_restore_error"})


def _read(path: Path, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("review_record_not_regular")
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("review_record_too_large")
    return data


def _json(path: Path) -> dict:
    raw = json.loads(_read(path, _MAX_RECORD), object_pairs_hook=_unique_keys)
    if type(raw) is not dict:
        raise ValueError("review_record_invalid")
    return raw


def _digest(raw: object) -> str:
    return hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _stamp(raw: str) -> datetime:
    stamp = datetime.fromisoformat(raw)
    if stamp.tzinfo is None:
        raise ValueError("review_timestamp_invalid")
    return stamp


def _state(raw: dict) -> session.ShadowLearningSessionState:
    if set(raw) != {f.name for f in fields(session.ShadowLearningSessionState)}:
        raise ValueError("review_state_shape")
    for key, value in raw.items():
        expected = bool if key in {"restore_required", "cloud_commands_possible"} else (
            int if key in {"protection_version", "restore_attempt_count"} else str)
        if type(value) is not expected:
            raise ValueError("review_state_type")
    result = session.ShadowLearningSessionState(**raw)
    if (result.protection_version != 1 or result.restore_attempt_count < 0
            or result.status not in {"restore_failed", "restoring"}
            or not result.route_owner_id.startswith("shadow_learning:")
            or result.cloud_source not in {"smartess", "dessmonitor", "valuecloud"}
            or not all((result.entry_id, result.collector_pn, result.trace_path,
                        result.original_endpoint, result.proxy_endpoint, result.upstream_endpoint))):
        raise ValueError("review_state_not_stopped")
    _stamp(result.started_at)
    return result


def _view(state) -> dict:
    return {k: v for k, v in asdict(state).items() if k not in _VOLATILE}


def _check_trace_scope(data, state) -> None:
    """Bind original local trace to a stopped run; this is NOT terminal proof."""
    phase = "manifest"
    for line in io.BytesIO(data):
        if not line.strip():
            continue
        event = json.loads(line, object_pairs_hook=_unique_keys)
        if type(event) is not dict:
            raise ValueError("review_trace_invalid")
        kind = event.get("kind")
        if phase == "manifest":
            if kind != "shadow_session_manifest":
                raise ValueError("review_trace_lifecycle_incomplete")
            if (event.get("entry_id") != state.entry_id or event.get("collector_pn") != state.collector_pn
                    or event.get("session_id") != state.route_owner_id.removeprefix("shadow_learning:")):
                raise ValueError("review_trace_identity_mismatch")
            phase = "active"
        elif kind == "shadow_session_manifest":
            raise ValueError("review_trace_lifecycle_incomplete")
        elif phase == "active":
            if kind == "shadow_session_stopped":
                phase = "summary"
        elif phase == "summary":
            if (kind != "shadow_completion_summary" or type(event.get("payload")) is not dict
                    or event["payload"].get("pending") is not False):
                raise ValueError("review_trace_not_drained")
            phase = "done"
        else:
            raise ValueError("review_trace_not_drained")
    if phase != "done":
        raise ValueError("review_trace_not_drained")


class ReviewedLearningRecoveryStore(CloudLearningProtectionStore):
    """Create-only review receipt; consumption stays under HA endpoint ownership."""

    def _review_path(self, state) -> Path:
        key = _digest([state.entry_id, state.collector_pn, state.route_owner_id])
        return self._root / "reviewed_history" / (key + ".json")

    def _current(self, entry_id):
        return _state(_json(session._owned_session_path(self._config_dir, entry_id)))

    def _history(self, hold, current) -> list[dict]:
        root = session.shadow_learning_session_root(self._config_dir).resolve()
        archive = root / "retained_runs"
        if archive.is_symlink():
            raise ValueError("review_history_symlink")
        states = [current]
        # The bounded inventory must include the original hold and every retained
        # later run for this collector. No glob-selected 'latest successful run'.
        paths = []
        if archive.exists():
            for path in archive.iterdir():
                if path.suffix != ".json":
                    continue
                paths.append(path)
                if len(paths) > 256:
                    raise ValueError("review_history_inventory_too_large")
        for path in paths:
            raw = _json(path)
            if raw.get("collector_pn") != hold.collector_pn:
                continue
            state = _state(raw)
            expected_name = hashlib.sha256(state.route_owner_id.encode()).hexdigest() + ".json"
            if path.name != expected_name:
                raise ValueError("review_history_owner_name")
            states.append(state)
        originals = [s for s in states if s.route_owner_id == "shadow_learning:" + hold.study_id]
        if len(originals) != 1:
            raise ValueError("review_original_run_missing_or_duplicate")
        original = originals[0]
        if original.cloud_source != hold.source:
            raise ValueError("review_original_source_mismatch")
        states = sorted((s for s in states if _stamp(s.started_at) >= _stamp(original.started_at)),
                        key=lambda s: _stamp(s.started_at))
        if (not 1 <= len(states) <= _MAX_RUNS or states[0] != original or states[-1] != current
                or len({s.route_owner_id for s in states}) != len(states)
                or len({_stamp(s.started_at) for s in states}) != len(states)):
            raise ValueError("review_history_order")
        history = []
        for index, state in enumerate(states):
            if (state.entry_id != hold.entry_id or state.collector_pn != hold.collector_pn
                    or not state.cloud_commands_possible
                    or state.proxy_endpoint != original.proxy_endpoint
                    or state.upstream_endpoint != original.upstream_endpoint
                    or (index and (state.original_endpoint != state.proxy_endpoint or state.restore_required))):
                raise ValueError("review_history_scope")
            trace = Path(state.trace_path)
            if not trace.is_absolute():
                raise ValueError("review_trace_path_invalid")
            if not trace.resolve().is_relative_to(root):
                raise ValueError("review_trace_outside_storage")
            data = _read(trace, _MAX_TRACE)
            if not data:
                raise ValueError("review_trace_empty")
            _check_trace_scope(data, state)
            history.append({"state": _view(state), "trace_sha256": hashlib.sha256(data).hexdigest()})
        return history

    def prepare(self, *, collector_pn, entry_id, study_id, evidence_files, command_counts) -> dict:
        """Preview a private scope-bound plan; no file/endpoint mutations."""
        hold = self.load(collector_pn)
        current = self._current(entry_id)
        if (hold is None or hold.entry_id != entry_id or hold.study_id != study_id
                or current.collector_pn != collector_pn or current.status != "restore_failed"):
            raise ValueError("review_scope_mismatch")
        history = self._history(hold, current)
        if (len(evidence_files) != len(history) or len(command_counts) != len(history)
                or any(type(n) is not int or not 1 <= n <= 10000 for n in command_counts)):
            raise ValueError("review_every_run_required")
        evidence = []
        for path, count in zip(evidence_files, command_counts, strict=True):
            data = _read(Path(path), _MAX_TRACE)
            if not data:
                raise ValueError("review_evidence_empty")
            evidence.append({"sha256": hashlib.sha256(data).hexdigest(), "reviewed_commands": count})
        return {"schema_version": 1, "outcome": "maintainer_reviewed_terminal_history",
                "hold": hold.to_record(), "history": history, "evidence": evidence}

    def inspect(self, collector_pn) -> dict:
        """Shareable inventory, not the private receipt, paths, PN or endpoints."""
        hold = self.load(collector_pn)
        if hold is None:
            return {"status": "no_hold"}
        current = self._current(hold.entry_id)
        history = self._history(hold, current)
        return {"status": "review_required", "run_count": len(history),
                "history_sha256": _digest({"hold": hold.to_record(), "history": history}),
                "runs": [{"index": index, "source": run["state"]["cloud_source"],
                          "started_at": _stamp(run["state"]["started_at"]).isoformat(),
                          "trace_sha256": run["trace_sha256"], "original": index == 0}
                         for index, run in enumerate(history)]}

    @staticmethod
    def review_digest(plan) -> str:
        return _digest(plan)

    def approve(self, *, plan, expected_digest: str, confirmed: bool) -> None:
        """Record explicit human review, never synthesize a modern wire receipt."""
        if confirmed is not True or expected_digest != _digest(plan):
            raise ValueError("explicit_terminal_history_review_required")
        with session._SESSION_RECORD_LOCK:
            hold = CloudLearningHold.from_record(plan["hold"])
            self.require_exact_hold(hold)
            current = self._current(hold.entry_id)
            self._validate(plan, hold, current)
            if current.status != "restore_failed":
                raise ValueError("review_scope_changed")
            path = self._review_path(current)
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _sync_directory(self._root)
            payload = (json.dumps(plan, sort_keys=True) + "\n").encode()
            if len(payload) > _MAX_RECORD:
                raise ValueError("review_receipt_too_large")
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            except FileExistsError:
                if _json(path) != plan:
                    raise ValueError("review_receipt_conflict") from None
                return
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            _sync_directory(path.parent)

    def _validate(self, plan, hold, current) -> None:
        if (type(plan) is not dict or set(plan) != {"schema_version", "outcome", "hold", "history", "evidence"}
                or type(plan["schema_version"]) is not int or plan["schema_version"] != 1
                or plan["outcome"] != "maintainer_reviewed_terminal_history"
                or plan["hold"] != hold.to_record()
                or type(plan["history"]) is not list or not 1 <= len(plan["history"]) <= _MAX_RUNS
                or type(plan["evidence"]) is not list or len(plan["evidence"]) != len(plan["history"])):
            raise ValueError("review_receipt_invalid")
        for proof in plan["evidence"]:
            if (type(proof) is not dict or set(proof) != {"sha256", "reviewed_commands"}
                    or type(proof["sha256"]) is not str or len(proof["sha256"]) != 64
                    or any(c not in "0123456789abcdef" for c in proof["sha256"])
                    or type(proof["reviewed_commands"]) is not int
                    or not 1 <= proof["reviewed_commands"] <= 10000):
                raise ValueError("review_evidence_invalid")
        for run in plan["history"]:
            if (type(run) is not dict or set(run) != {"state", "trace_sha256"}
                    or type(run["state"]) is not dict or set(run["state"]) != set(_view(current))
                    or type(run["trace_sha256"]) is not str or len(run["trace_sha256"]) != 64
                    or any(c not in "0123456789abcdef" for c in run["trace_sha256"])):
                raise ValueError("review_history_invalid")
            # Dict equality alone accepts 1 == True. Validate the stored view
            # with the same strict types as the actual on-host journal.
            _state(dict(run["state"], status="restore_failed", updated_at="",
                        restore_attempt_count=0, last_restore_attempt_at="", last_restore_error=""))
        before = plan["history"][-1]["state"]
        original = plan["history"][0]["state"]
        after = dict(before, cloud_commands_possible=False,
                     original_endpoint=original["original_endpoint"], restore_required=original["restore_required"])
        if _view(current) not in (before, after):
            raise ValueError("review_current_changed")
        # Reconstruct only the pre-migration local journal view, not a live wire
        # ledger. Its actual trace and every retained record must still match.
        previous = replace(current, **before)
        if self._history(hold, previous) != plan["history"]:
            raise ValueError("review_history_changed")

    def require_review(self, state) -> tuple[dict, CloudLearningHold]:
        plan = _json(self._review_path(state))
        hold = CloudLearningHold.from_record(plan["hold"])
        if (state.entry_id != hold.entry_id or state.collector_pn != hold.collector_pn
                or self._current(state.entry_id) != state):
            raise ValueError("review_scope_changed")
        self._validate(plan, hold, state)
        active_hold = self.load(hold.collector_pn)
        if active_hold is None:
            retired = self._root / "retired_holds" / self._receipt_path(hold).name
            if state.cloud_commands_possible or _json(retired) != hold.to_record():
                raise ValueError("review_retirement_missing")
        elif active_hold != hold:
            raise ValueError("review_hold_changed")
        return plan, hold

    def apply(self, *, expected_state):
        """CAS the journal then retire the hold; caller owns the stopped route.

        Every crash prefix is either held or retains a recoverable ORIGINAL
        endpoint. The same receipt finishes a partial migration after restart.
        """
        with session._SESSION_RECORD_LOCK:
            plan, hold = self.require_review(expected_state)
            original = plan["history"][0]["state"]
            corrected = replace(expected_state, cloud_commands_possible=False,
                                original_endpoint=original["original_endpoint"],
                                restore_required=original["restore_required"], last_restore_error="",
                                updated_at=session.shadow_learning_session_timestamp())
            session._save_shadow_learning_session_state(config_dir=self._config_dir,
                                                        state=corrected, entry_id=hold.entry_id)
            if self.load(hold.collector_pn) is not None:
                self._archive_hold(hold)
            return corrected, plan

    def receipt_present(self, state) -> bool:
        # lexists includes broken symlinks: corrupt receipt is an error, not absence.
        return os.path.lexists(self._review_path(state))

    def require_retired_review(self, state):
        plan, hold = self.require_review(state)
        self.require_no_hold(hold.collector_pn)
        return plan
