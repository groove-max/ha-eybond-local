"""Helpers for local experimental profile and register-schema drafts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from ..const import (
    BUILTIN_SCHEMA_PREFIX,
    LOCAL_METADATA_DIR,
    LOCAL_PROFILES_DIR,
    LOCAL_REGISTER_SCHEMAS_DIR,
)
from .profile_loader import load_driver_profile_raw
from .register_schema_loader import builtin_register_schema_path, load_register_schema


@dataclass(frozen=True, slots=True)
class LocalMetadataRollbackPaths:
    """Active managed local override files that can be safely rolled back."""

    profile_path: Path | None = None
    schema_path: Path | None = None

    @property
    def paths(self) -> tuple[Path, ...]:
        """Return all active rollback targets in stable order."""

        return tuple(path for path in (self.profile_path, self.schema_path) if path is not None)


def clear_local_metadata_loader_caches() -> None:
    """Clear profile and register-schema loader caches used by local overrides."""

    from .profile_loader import clear_profile_loader_cache
    from .register_schema_loader import clear_register_schema_loader_cache

    clear_profile_loader_cache()
    clear_register_schema_loader_cache()


def local_metadata_root(config_dir: Path) -> Path:
    """Return the local EyeBond metadata root under one HA config directory."""

    return config_dir / LOCAL_METADATA_DIR


def local_profiles_root(config_dir: Path) -> Path:
    """Return the local profile root under one HA config directory."""

    return local_metadata_root(config_dir) / LOCAL_PROFILES_DIR


def local_profile_path(config_dir: Path, profile_name: str) -> Path:
    """Return one local profile path under the experimental metadata root."""

    return (local_profiles_root(config_dir) / profile_name).resolve()


def local_register_schemas_root(config_dir: Path) -> Path:
    """Return the local register schema root under one HA config directory."""

    return local_metadata_root(config_dir) / LOCAL_REGISTER_SCHEMAS_DIR


def local_register_schema_path(config_dir: Path, schema_name: str) -> Path:
    """Return one local register schema path under the experimental metadata root."""

    return (local_register_schemas_root(config_dir) / schema_name).resolve()


def ensure_local_metadata_dirs(config_dir: Path) -> dict[str, Path]:
    """Ensure that local metadata directories exist."""

    profiles_root = local_profiles_root(config_dir)
    schemas_root = local_register_schemas_root(config_dir)
    profiles_root.mkdir(parents=True, exist_ok=True)
    schemas_root.mkdir(parents=True, exist_ok=True)
    return {
        "root": local_metadata_root(config_dir),
        "profiles": profiles_root,
        "register_schemas": schemas_root,
    }


def draft_activates_automatically(
    source_name: str,
    output_name: str | None,
) -> bool:
    """Return whether one draft output name will override the built-in source automatically."""

    effective_output = _normalize_relative_name(output_name or source_name)
    return effective_output == _normalize_relative_name(source_name)


def local_profile_override_details(
    config_dir: Path,
    profile_name: str | None,
) -> dict[str, str | bool]:
    """Describe the current local override state for one built-in profile."""

    return _local_override_details(
        config_dir=config_dir,
        source_name=profile_name,
        root=local_profiles_root(config_dir),
        path_factory=local_profile_path,
        kind="profile",
    )


def local_register_schema_override_details(
    config_dir: Path,
    schema_name: str | None,
) -> dict[str, str | bool]:
    """Describe the current local override state for one built-in register schema."""

    return _local_override_details(
        config_dir=config_dir,
        source_name=schema_name,
        root=local_register_schemas_root(config_dir),
        path_factory=local_register_schema_path,
        kind="register schema",
    )


def resolve_local_metadata_rollback_paths(
    *,
    config_dir: Path,
    profile_name: str | None = None,
    schema_name: str | None = None,
    profile_metadata: Any = None,
    schema_metadata: Any = None,
) -> LocalMetadataRollbackPaths:
    """Resolve active managed local override files that can be removed safely."""

    profiles_root = local_profiles_root(config_dir)
    schemas_root = local_register_schemas_root(config_dir)
    return LocalMetadataRollbackPaths(
        profile_path=_resolve_active_local_override_path(
            config_dir=config_dir,
            source_name=profile_name,
            metadata=profile_metadata,
            root=profiles_root,
            path_factory=local_profile_path,
        ),
        schema_path=_resolve_active_local_override_path(
            config_dir=config_dir,
            source_name=schema_name,
            metadata=schema_metadata,
            root=schemas_root,
            path_factory=local_register_schema_path,
        ),
    )


def rollback_local_metadata_overrides(
    *,
    config_dir: Path,
    profile_name: str | None = None,
    schema_name: str | None = None,
    profile_metadata: Any = None,
    schema_metadata: Any = None,
) -> tuple[Path, ...]:
    """Remove the active managed local overrides for one entry."""

    rollback_paths = resolve_local_metadata_rollback_paths(
        config_dir=config_dir,
        profile_name=profile_name,
        schema_name=schema_name,
        profile_metadata=profile_metadata,
        schema_metadata=schema_metadata,
    )
    removed_paths: list[Path] = []

    if rollback_paths.profile_path is not None:
        rollback_paths.profile_path.unlink()
        removed_paths.append(rollback_paths.profile_path)
        _prune_empty_local_metadata_dirs(
            rollback_paths.profile_path.parent,
            local_profiles_root(config_dir),
        )

    if rollback_paths.schema_path is not None:
        rollback_paths.schema_path.unlink()
        removed_paths.append(rollback_paths.schema_path)
        _prune_empty_local_metadata_dirs(
            rollback_paths.schema_path.parent,
            local_register_schemas_root(config_dir),
        )

    if not removed_paths:
        raise RuntimeError("local_metadata_rollback_not_available")

    return tuple(removed_paths)


def create_local_profile_draft(
    *,
    config_dir: Path,
    source_profile_name: str,
    output_profile_name: str | None = None,
    overwrite: bool = False,
) -> Path:
    """Copy one built-in profile into the local experimental profile root."""

    ensure_local_metadata_dirs(config_dir)
    output_name = output_profile_name or source_profile_name
    destination = local_profile_path(config_dir, output_name)
    _ensure_can_write(destination, local_profiles_root(config_dir), overwrite=overwrite)

    raw = load_driver_profile_raw(source_profile_name)
    raw.setdefault("draft_of", source_profile_name)
    raw.setdefault("experimental", True)
    raw["title"] = str(raw.get("title", Path(source_profile_name).stem)) + " (Local Draft)"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(_dump_json(raw), encoding="utf-8")
    return destination


def create_local_schema_draft(
    *,
    config_dir: Path,
    source_schema_name: str,
    output_schema_name: str | None = None,
    overwrite: bool = False,
) -> Path:
    """Create one minimal local schema overlay that extends a built-in schema."""

    ensure_local_metadata_dirs(config_dir)
    output_name = output_schema_name or source_schema_name
    destination = local_register_schema_path(config_dir, output_name)
    _ensure_can_write(
        destination,
        local_register_schemas_root(config_dir),
        overwrite=overwrite,
    )

    builtin_register_schema_path(source_schema_name)
    schema = load_register_schema(f"{BUILTIN_SCHEMA_PREFIX}{source_schema_name}")
    raw = {
        "extends": f"{BUILTIN_SCHEMA_PREFIX}{source_schema_name}",
        "schema_key": f"local_{schema.key}",
        "title": f"{schema.title} (Local Draft)",
        "driver_key": schema.driver_key,
        "protocol_family": schema.protocol_family,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(_dump_json(raw), encoding="utf-8")
    return destination


def _ensure_can_write(path: Path, root: Path, *, overwrite: bool) -> None:
    if not _is_within_root(path, root):
        raise ValueError(f"path_outside_local_metadata_root:{path}")
    if path.exists() and not overwrite:
        raise FileExistsError(path)


def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _local_override_details(
    *,
    config_dir: Path,
    source_name: str | None,
    root: Path,
    path_factory,
    kind: str,
) -> dict[str, str | bool]:
    if not source_name:
        return {
            "exists": False,
            "path": "",
            "status": f"No built-in {kind} is available for this entry.",
        }

    path = path_factory(config_dir, source_name)
    if path.exists():
        return {
            "exists": True,
            "path": str(path),
            "status": f"Active local override at {path}.",
        }

    return {
        "exists": False,
        "path": str(path),
        "status": f"No active local override. Create {path.relative_to(root.parent)} to override the built-in {kind}.",
    }


def _normalize_relative_name(name: str) -> str:
    raw = str(name).replace("\\", "/").strip().lstrip("/")
    parts = [part for part in raw.split("/") if part and part != "."]
    return "/".join(parts)


def _resolve_active_local_override_path(
    *,
    config_dir: Path,
    source_name: str | None,
    metadata: Any,
    root: Path,
    path_factory,
) -> Path | None:
    if not source_name or metadata is None:
        return None
    if str(getattr(metadata, "source_scope", "") or "").strip() != "external":
        return None

    expected_path = path_factory(config_dir, source_name)
    if not _is_within_root(expected_path, root):
        return None

    source_path = str(getattr(metadata, "source_path", "") or "").strip()
    if source_path:
        try:
            resolved_source_path = Path(source_path).resolve()
        except OSError:
            return None
        if resolved_source_path != expected_path:
            return None

    if not expected_path.exists():
        return None
    return expected_path


def _prune_empty_local_metadata_dirs(path: Path, root: Path) -> None:
    current = path.resolve()
    resolved_root = root.resolve()
    while current != resolved_root and _is_within_root(current, resolved_root):
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _dump_json(raw: dict[str, Any]) -> str:
    return json.dumps(raw, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
