"""Durable completed-study receipts; separate from proof that nothing was sent."""
import json
import os
import stat

from ...connection.cloud_learning_protection import CloudLearningProtectionRequired
from .completion import validate_completed_commands
from .protection_store import CloudLearningProtectionStore, _sync_directory, _unique_keys

_MAX_COMPLETION_BYTES = 2_000_000


class CompletedLearningStore(CloudLearningProtectionStore):
    def _completed_path(self, hold):
        return self._root / "completed_receipts" / self._receipt_path(hold).name

    def record_completed(self, hold, commands):
        self.require_exact_hold(hold)
        validate_completed_commands(commands, source=hold.source)
        record = {"schema_version": 2, "outcome": "commands_completed", "hold": hold.to_record(),
                  "commands": commands}
        payload = (json.dumps(record, sort_keys=True) + "\n").encode()
        if len(payload) > _MAX_COMPLETION_BYTES:
            raise ValueError("completion_receipt_too_large")
        path = self._completed_path(hold)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _sync_directory(self._root)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            if self.require_completed_receipt(hold) != record:
                raise CloudLearningProtectionRequired("cloud_learning_completion_conflict") from None
            return
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        _sync_directory(path.parent)

    def require_completed_receipt(self, hold):
        try:
            fd = os.open(self._completed_path(hold), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ValueError("not_regular")
                raw = stream.read(_MAX_COMPLETION_BYTES + 1)
            if len(raw) > _MAX_COMPLETION_BYTES:
                raise ValueError("too_large")
            record = json.loads(raw, object_pairs_hook=_unique_keys)
            if (type(record) is not dict or set(record) != {"schema_version", "outcome", "hold", "commands"}
                    or type(record["schema_version"]) is not int or record["schema_version"] != 2
                    or record["outcome"] != "commands_completed" or record["hold"] != hold.to_record()):
                raise ValueError("completion_receipt_invalid")
            validate_completed_commands(record["commands"], source=hold.source)
            return record
        except (OSError, ValueError, TypeError, RecursionError):
            raise CloudLearningProtectionRequired("cloud_learning_completion_receipt_invalid") from None

    def retire_completed(self, hold):
        self.require_completed_receipt(hold)
        self._archive_hold(hold)
