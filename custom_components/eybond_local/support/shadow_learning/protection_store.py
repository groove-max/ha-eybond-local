"""Durable, create-only cloud-learning holds, scoped to collector identity.

The wizard arms a hold only after proving its persistent local fallback route.
Exact no-request or completed-study receipts can retire a hold; there is no expiry.
Re-adding an entry for the same PN must not hide an old study's hold.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import re

from ...collector_identity import validated_collector_pn
from ...connection.cloud_learning_protection import (
    CloudLearningHold,
    CloudLearningProtectionRequired,
)
from ...const import LOCAL_METADATA_DIR
from ..owned_worker import async_run_owned_worker

_MAX_RECORD_BYTES = 4096


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_key")
        result[key] = value
    return result


class CloudLearningProtectionStore:
    """One immutable record per PN; conflicts/corruption block, never overwrite.

    An exclusive file creation publishes a conservative hold even while its
    contents are incomplete. Another reader then blocks on the incomplete file.
    The caller may dispatch only after BOTH file and directory fsync succeeded.
    A failed write is intentionally not removed: its possible intent is unknown.
    """

    def __init__(self, config_dir: Path) -> None:
        self._config_dir = Path(config_dir)
        self._root = self._config_dir / LOCAL_METADATA_DIR / "cloud_learning_protection"

    def _path(self, collector_pn: str) -> Path:
        if not validated_collector_pn(collector_pn):
            raise ValueError("cloud_learning_hold_identity_invalid")
        key = hashlib.sha256(collector_pn.encode("ascii")).hexdigest()
        return self._root / f"{key}.json"

    def create(self, *, hold: CloudLearningHold) -> None:
        if type(hold) is not CloudLearningHold:
            raise ValueError("cloud_learning_hold_record_invalid")
        path = self._path(hold.collector_pn)
        payload = (json.dumps(hold.to_record(), sort_keys=True) + "\n").encode("utf-8")
        try:
            self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Persist newly-created metadata/hold directory links too.
            _sync_directory(self._config_dir)
            _sync_directory(self._root.parent)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            _sync_directory(self._root)
        except FileExistsError:
            # Even the same study cannot silently re-arm after a restart.
            raise CloudLearningProtectionRequired("cloud_learning_hold_exists") from None
        except OSError:
            raise CloudLearningProtectionRequired("cloud_learning_hold_persist_failed") from None

    async def async_create(self, *, hold: CloudLearningHold) -> None:
        """Drain the owned filesystem worker on cancellation, then propagate it.

        Cancellation never permits a following HTTP dispatch. A completed write
        remains conservative evidence, even if the request was never sent.
        """
        await async_run_owned_worker(self.create, hold=hold)

    def _receipt_path(self, hold: CloudLearningHold) -> Path:
        if type(hold) is not CloudLearningHold:
            raise ValueError("cloud_learning_hold_record_invalid")
        digest = hashlib.sha256(json.dumps(hold.to_record(), sort_keys=True).encode()).hexdigest()
        return self._root / "no_request_receipts" / f"{digest}.json"

    def record_no_request(self, hold: CloudLearningHold, *, evidence_sha256: str = "") -> None:
        """Record worker proof, or an explicitly reviewed legacy trace digest.

        Internal recovery API, never a wizard force-clear. Callers must establish
        that the ENTIRE study sent no HTTP control request before recording it.
        It does not infer anything from age, HTTP status or endpoint restoration.
        """
        if evidence_sha256 and not re.fullmatch(r"[0-9a-f]{64}", evidence_sha256):
            raise ValueError("cloud_learning_no_request_evidence_invalid")
        self.require_exact_hold(hold)
        path = self._receipt_path(hold)
        record = {"schema_version": 1, "outcome": "no_request_sent", "hold": hold.to_record(),
                  "evidence_sha256": evidence_sha256}
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _sync_directory(self._root)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            self.require_no_request_receipt(hold)
            return
        with os.fdopen(fd, "w") as stream:
            json.dump(record, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        _sync_directory(path.parent)

    def require_no_request_receipt(self, hold: CloudLearningHold) -> None:
        path = self._receipt_path(hold)
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ValueError("not_regular")
                raw = stream.read(_MAX_RECORD_BYTES + 1)
            if len(raw) > _MAX_RECORD_BYTES:
                raise ValueError("too_large")
            record = json.loads(raw, object_pairs_hook=_unique_keys)
            if (type(record) is not dict or set(record) != {"schema_version", "outcome", "hold", "evidence_sha256"}
                    or type(record["schema_version"]) is not int or record["schema_version"] != 1
                    or record["outcome"] != "no_request_sent" or record["hold"] != hold.to_record()
                    or type(record["evidence_sha256"]) is not str
                    or (record["evidence_sha256"] and not re.fullmatch(r"[0-9a-f]{64}", record["evidence_sha256"]))):
                raise ValueError("invalid")
        except (OSError, ValueError, TypeError, RecursionError):
            raise CloudLearningProtectionRequired("cloud_learning_no_request_receipt_invalid") from None

    def retire_no_request(self, hold: CloudLearningHold) -> None:
        """Archive the exact hold after its journal's no-send correction is durable."""
        self.require_no_request_receipt(hold)
        self._archive_hold(hold)

    def _archive_hold(self, hold: CloudLearningHold) -> None:
        """Caller verified its specific durable receipt before journal correction."""
        self.require_exact_hold(hold)
        destination = self._root / "retired_holds" / self._receipt_path(hold).name
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _sync_directory(self._root)
        # Exact-run endpoint authority serializes callers with new study creation.
        # Preserve the old hold, rather than delete evidence or overwrite a study.
        if destination.exists():
            raise CloudLearningProtectionRequired("cloud_learning_retirement_conflict")
        os.rename(self._path(hold.collector_pn), destination)
        _sync_directory(destination.parent)
        _sync_directory(self._root)

    def load(self, collector_pn: str) -> CloudLearningHold | None:
        path = self._path(collector_pn)
        try:
            # No symlink following or blocking on an accidentally replaced FIFO.
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        except OSError:
            raise CloudLearningProtectionRequired("cloud_learning_hold_unreadable") from None
        try:
            with os.fdopen(fd, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ValueError("not_regular")
                payload = stream.read(_MAX_RECORD_BYTES + 1)
            if len(payload) > _MAX_RECORD_BYTES:
                raise ValueError("too_large")
            record = json.loads(payload, object_pairs_hook=_unique_keys)
            hold = CloudLearningHold.from_record(record)
            if hold.collector_pn != collector_pn:
                raise ValueError("foreign_identity")
            return hold
        except (OSError, ValueError, TypeError, RecursionError):
            raise CloudLearningProtectionRequired("cloud_learning_hold_invalid") from None

    def require_exact_hold(self, hold: CloudLearningHold) -> None:
        """Re-read disk; a cached marker is not permission after loss/replacement."""
        if type(hold) is not CloudLearningHold:
            raise ValueError("cloud_learning_hold_record_invalid")
        if self.load(hold.collector_pn) != hold:
            raise CloudLearningProtectionRequired("cloud_learning_hold_changed")

    def require_no_hold(self, collector_pn: str) -> None:
        """A hold blocks direct-cloud return, irrespective of its age/owner.

        Absence is NOT proof of safety for legacy active learning sessions.
        Lifecycle integration must handle those separately, before using this
        check. This method neither authorizes IO nor verifies a live route.
        """
        if self.load(collector_pn) is not None:
            raise CloudLearningProtectionRequired("cloud_learning_hold_pending")


class CloudLearningMutationFence:
    """Fresh-disk mutation check with a sticky, process-local failure floor.

    This is not a live-route certificate or permission to dispatch cloud writes.
    The HA composition root installs the same fence in lifecycle and management.
    Once a hold/corruption is seen, a later missing file cannot clear it in this
    process. Only explicit exact receipt-backed retirement can release that floor.
    """

    def __init__(self, config_dir: Path, collector_pn: str) -> None:
        self._store = CloudLearningProtectionStore(config_dir)
        self._collector_pn = collector_pn
        self._blocked_reason = ""
        self._release_generation = 0

    async def require_unheld(self) -> None:
        if self._blocked_reason:
            raise CloudLearningProtectionRequired(self._blocked_reason)
        generation = self._release_generation
        try:
            await async_run_owned_worker(
                self._store.require_no_hold, collector_pn=self._collector_pn,
            )
        except CloudLearningProtectionRequired as exc:
            if generation != self._release_generation:
                return await self.require_unheld()
            self._blocked_reason = str(exc)
            raise
        except ValueError:
            self._blocked_reason = "cloud_learning_hold_identity_invalid"
            raise CloudLearningProtectionRequired(self._blocked_reason) from None
        # Another concurrent check may have observed a hold while this read was
        # in flight. A stale absent result must not erase that observation.
        if self._blocked_reason:
            raise CloudLearningProtectionRequired(self._blocked_reason)

    async def accept_no_request_retirement(self, hold: CloudLearningHold) -> None:
        """Explicit exact-run recovery; missing files alone still never clear a fence."""
        if hold.collector_pn != self._collector_pn or self._blocked_reason not in ("", "cloud_learning_hold_pending"):
            raise CloudLearningProtectionRequired("cloud_learning_no_request_fence_conflict")
        await async_run_owned_worker(self._store.require_no_request_receipt, hold=hold)
        await async_run_owned_worker(self._store.require_no_hold, collector_pn=self._collector_pn)
        if self._blocked_reason not in ("", "cloud_learning_hold_pending"):
            raise CloudLearningProtectionRequired("cloud_learning_no_request_fence_conflict")
        self._release_generation += 1
        self._blocked_reason = ""

    async def accept_completed_retirement(self, hold: CloudLearningHold) -> None:
        """Only a validated completed-study receipt can clear this sticky floor."""
        from .completion_store import CompletedLearningStore
        if hold.collector_pn != self._collector_pn or self._blocked_reason not in ("", "cloud_learning_hold_pending"):
            raise CloudLearningProtectionRequired("cloud_learning_completion_fence_conflict")
        store = CompletedLearningStore(self._store._config_dir)
        await async_run_owned_worker(store.require_completed_receipt, hold=hold)
        await async_run_owned_worker(self._store.require_no_hold, collector_pn=self._collector_pn)
        if self._blocked_reason not in ("", "cloud_learning_hold_pending"):
            raise CloudLearningProtectionRequired("cloud_learning_completion_fence_conflict")
        self._release_generation += 1
        self._blocked_reason = ""

    async def hold_reason(self) -> str:
        try:
            await self.require_unheld()
        except CloudLearningProtectionRequired as exc:
            return str(exc)
        return ""
