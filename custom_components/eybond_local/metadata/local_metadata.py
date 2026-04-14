"""Helpers for local experimental profile and register-schema drafts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..const import (
    BUILTIN_SCHEMA_PREFIX,
    LOCAL_METADATA_DIR,
    LOCAL_PROFILES_DIR,
    LOCAL_REGISTER_SCHEMAS_DIR,
)
from .profile_loader import builtin_profile_path
from .register_schema_loader import builtin_register_schema_path, load_register_schema


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


def create_local_profile_draft(
    *,
    config_dir: Path,
    source_profile_name: str,
    output_profile_name: str | None = None,
    overwrite: bool = False,
) -> Path:
    """Copy one built-in profile into the local experimental profile root."""

    ensure_local_metadata_dirs(config_dir)
    source_path = builtin_profile_path(source_profile_name)
    output_name = output_profile_name or source_profile_name
    destination = local_profile_path(config_dir, output_name)
    _ensure_can_write(destination, local_profiles_root(config_dir), overwrite=overwrite)

    raw = json.loads(source_path.read_text(encoding="utf-8"))
    raw.setdefault("draft_of", source_profile_name)
    raw.setdefault("experimental", True)
    raw["title"] = str(raw.get("title", source_path.stem)) + " (Local Draft)"
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


def _dump_json(raw: dict[str, Any]) -> str:
    return json.dumps(raw, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
