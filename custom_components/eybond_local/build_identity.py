"""Offline package identity, independent of inverter and collector firmware.

Archive commit metadata declares provenance; it is not proof of an unmodified
installation. A separate versioned source fingerprint identifies actual files.
Capture in an executor at integration setup and retain that startup snapshot:
an update on disk must not silently relabel the running process before restart.
No Git executable, repository lookup, HA storage or network access is needed.
"""

from __future__ import annotations

from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re

_SOURCE_SUFFIXES = frozenset({".py", ".json", ".yaml", ".yml"})
_MAX_FILES = 4096
_MAX_BYTES = 32 * 1024 * 1024
_MAX_FILE_BYTES = 8 * 1024 * 1024
_COMMIT = re.compile(r"(?:[0-9a-fA-F]{7}|[0-9a-fA-F]{12}|[0-9a-fA-F]{40})\Z")
_METADATA_PATTERNS = {
    "git_commit": _COMMIT,
    "git_describe": re.compile(r"[A-Za-z0-9][A-Za-z0-9._/+\-]{0,127}\Z"),
    "commit_date": re.compile(r"[0-9][0-9TtZz:+.\-]{7,39}\Z"),
    "built_at": re.compile(r"[0-9][0-9TtZz:+.\-]{7,39}\Z"),
}


def _bounded_text(path: Path, limit: int = 16384) -> str:
    if path.is_symlink():
        raise ValueError("metadata_symlink")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("metadata_too_large")
    return raw.decode("utf-8")


def _metadata(root: Path) -> tuple[bool, dict[str, str]]:
    try:
        text = _bounded_text(root / "BUILD_INFO.txt")
    except (OSError, ValueError, UnicodeError):
        return False, {}
    fields = {}
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        key = key.strip().lower().replace(" ", "_")
        value = value.strip()
        pattern = _METADATA_PATTERNS.get(key)
        if separator and pattern is not None and pattern.fullmatch(value):
            fields[key] = value.lower() if key == "git_commit" else value
    return True, fields


def _fingerprint(root: Path) -> tuple[str, int]:
    """Hash runtime sources with relative paths; exclude metadata and caches."""
    digest = hashlib.sha256(b"eybond-source-v1\0")
    count = total = 0
    paths: list[Path] = []
    visited = 0

    def fail(error: OSError) -> None:
        raise error

    for directory, directories, files in os.walk(root, onerror=fail, followlinks=False):
        directories[:] = [name for name in directories if name != "__pycache__" and not name.startswith(".")]
        visited += len(directories) + len(files)
        if visited > _MAX_FILES * 4:
            raise ValueError("tree_size_limit")
        if any((Path(directory) / name).is_symlink() for name in directories):
            raise ValueError("source_directory_symlink")
        for name in files:
            path = Path(directory) / name
            if not name.startswith(".") and path.suffix in _SOURCE_SUFFIXES:
                paths.append(path)
                if len(paths) > _MAX_FILES:
                    raise ValueError("source_count_limit")
    for path in sorted(paths):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ValueError("source_symlink")
        count += 1
        with path.open("rb") as stream:
            raw = stream.read(_MAX_FILE_BYTES + 1)
        total += len(raw)
        if len(raw) > _MAX_FILE_BYTES or total > _MAX_BYTES:
            raise ValueError("source_size_limit")
        digest.update(relative.as_posix().encode("utf-8") + b"\0")
        digest.update(str(len(raw)).encode("ascii") + b"\0" + raw)
    if count == 0:
        raise ValueError("no_source_files")
    return digest.hexdigest(), count


def read_build_runtime_values(package_dir: Path) -> dict[str, object]:
    """Read installed package identity; caller must run filesystem IO off-loop."""
    root = package_dir.resolve()
    try:
        manifest = json.loads(_bounded_text(root / "manifest.json"))
        version = str(manifest.get("version") or "") if isinstance(manifest, dict) else ""
    except (OSError, ValueError, UnicodeError):
        version = ""
    present, metadata = _metadata(root)
    values: dict[str, object] = {
        "integration_package_dir": str(root),
        "integration_manifest_version": version,
        "integration_build_info_present": present,
        "integration_build_fingerprint_scheme": "sha256-source-v1",
        "integration_build_metadata_status": "commit_declared" if metadata.get("git_commit") else "commit_unavailable",
    }
    values.update({f"integration_build_{key}": value for key, value in metadata.items()})
    try:
        fingerprint, count = _fingerprint(root)
    except (OSError, ValueError, UnicodeError):
        # Identity is diagnostic, never a reason to prevent local device access.
        values.update(integration_build_id="unknown", integration_build_fingerprint_status="unavailable")
    else:
        values.update(integration_build_id=f"sha256:{fingerprint}",
                      integration_build_fingerprint=fingerprint,
                      integration_build_source_file_count=count,
                      integration_build_fingerprint_status="available")
    return values


@lru_cache(maxsize=8)
def _startup_snapshot(root: Path) -> tuple[tuple[str, object], ...]:
    return tuple(read_build_runtime_values(root).items())


def build_runtime_values(package_dir: Path | None = None) -> dict[str, object]:
    """Return a caller-owned copy of the process-start package snapshot."""
    root = package_dir if package_dir is not None else Path(__file__).resolve().parent
    return dict(_startup_snapshot(root))


def build_label(values: dict[str, object]) -> str:
    """Compact UI/log label; never overload the collector's firmware version."""
    version = str(values.get("integration_manifest_version") or "unknown")
    commit = str(values.get("integration_build_git_commit") or "")
    fingerprint = str(values.get("integration_build_fingerprint") or "")
    return " / ".join([version] + ([f"git:{commit[:12]}"] if commit else [])
                      + ([f"sha256:{fingerprint[:12]}"] if fingerprint else ["build:unknown"]))
