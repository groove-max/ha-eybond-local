"""Support archive export helpers for unsupported or partially supported inverters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any
import zipfile

from ..const import LOCAL_METADATA_DIR, LOCAL_SUPPORT_PACKAGES_DIR


@dataclass(frozen=True, slots=True)
class SupportPackageExportResult:
    """One exported support archive plus its optional HA download URL."""

    path: Path
    download_path: Path | None = None
    download_url: str | None = None


def support_packages_root(config_dir: Path) -> Path:
    """Return the support package output directory."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_SUPPORT_PACKAGES_DIR


def support_packages_public_root(config_dir: Path) -> Path:
    """Return the Home Assistant static file directory for support packages."""

    return config_dir / "www" / LOCAL_METADATA_DIR / LOCAL_SUPPORT_PACKAGES_DIR


def support_package_download_url(filename: str) -> str:
    """Return the Home Assistant `/local` URL for one exported support package."""

    return f"/local/{LOCAL_METADATA_DIR}/{LOCAL_SUPPORT_PACKAGES_DIR}/{filename}"


def export_support_package(
    *,
    config_dir: Path,
    entry_id: str,
    entry_title: str,
    support_bundle: dict[str, Any],
    raw_capture: dict[str, Any] | None,
    fixture: dict[str, Any] | None,
    anonymized_fixture: dict[str, Any] | None,
    profile_source: dict[str, Any] | None = None,
    register_schema_source: dict[str, Any] | None = None,
    overwrite: bool = False,
    publish_download_copy: bool = True,
) -> SupportPackageExportResult:
    """Write one combined support archive and publish one `/local` download copy."""

    packages_root = support_packages_root(config_dir)
    packages_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = packages_root / f"{entry_id}_{timestamp}.zip"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)

    created_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "archive_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entry": {
            "entry_id": entry_id,
            "title": entry_title,
        },
        "effective_metadata": {
            "profile_source": profile_source,
            "register_schema_source": register_schema_source,
        },
        "sharing_guidance": {
            "recommended_artifact": destination.name,
            "note": (
                "Send this ZIP file to the developer. It includes runtime metadata, "
                "raw capture evidence, and an anonymized replay fixture."
            ),
        },
    }

    archive_members = {
        "manifest.json": manifest,
        "support_bundle.json": support_bundle,
        "raw_capture.json": raw_capture,
        "fixture/raw_fixture.json": fixture,
        "fixture/anonymized_fixture.json": anonymized_fixture,
        "README.txt": (
            "EyeBond Local Support Archive\n\n"
            f"Created at: {created_at}\n"
            f"Entry: {entry_title} ({entry_id})\n\n"
            "Send this ZIP file to the developer. The main files are:\n"
            "- manifest.json\n"
            "- support_bundle.json\n"
            "- raw_capture.json\n"
            "- fixture/anonymized_fixture.json\n"
        ),
    }

    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member_name, payload in archive_members.items():
            if payload is None:
                continue
            if isinstance(payload, str):
                archive.writestr(member_name, payload)
                continue
            archive.writestr(
                member_name,
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
            )

    if not publish_download_copy:
        return SupportPackageExportResult(path=destination)

    public_root = support_packages_public_root(config_dir)
    public_root.mkdir(parents=True, exist_ok=True)
    public_destination = public_root / destination.name
    shutil.copy2(destination, public_destination)
    return SupportPackageExportResult(
        path=destination,
        download_path=public_destination,
        download_url=support_package_download_url(destination.name),
    )
