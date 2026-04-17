"""Support archive export helpers for unsupported or partially supported inverters."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any
import zipfile

from ..const import LOCAL_METADATA_DIR, LOCAL_SUPPORT_PACKAGES_DIR


_CLOUD_EVIDENCE_ARCHIVE_MEMBER = "evidence/cloud_evidence.json"


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

    cloud_evidence = _support_bundle_cloud_evidence(support_bundle)
    archived_support_bundle = _archive_support_bundle_payload(support_bundle)

    packages_root = support_packages_root(config_dir)
    packages_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = packages_root / f"{entry_id}_{timestamp}.zip"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)

    created_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "archive_version": 2,
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
                "raw capture evidence, an anonymized replay fixture, and any "
                "matching SmartESS cloud evidence exported into the HA config dir."
            ),
        },
        "archive_members": {
            "support_bundle": "support_bundle.json",
            "raw_capture": "raw_capture.json",
            "raw_fixture": "fixture/raw_fixture.json",
            "anonymized_fixture": "fixture/anonymized_fixture.json",
            "cloud_evidence": _CLOUD_EVIDENCE_ARCHIVE_MEMBER if cloud_evidence is not None else None,
        },
    }

    readme_lines = [
        "EyeBond Local Support Archive",
        "",
        f"Created at: {created_at}",
        f"Entry: {entry_title} ({entry_id})",
        "",
        "Send this ZIP file to the developer. The main files are:",
        "- manifest.json",
        "- support_bundle.json",
        "- raw_capture.json",
        "- fixture/anonymized_fixture.json",
    ]
    if cloud_evidence is not None:
        readme_lines.append("- evidence/cloud_evidence.json")
    readme_lines.extend(
        [
            "",
            "When SmartESS cloud evidence is present, support_bundle.json references evidence/cloud_evidence.json instead of duplicating the full payload.",
            "",
            "raw_capture.json may include supplemental family-level discovery ranges when the driver collects extra evidence for nearby variants.",
        ]
    )

    archive_members = {
        "manifest.json": manifest,
        "support_bundle.json": archived_support_bundle,
        "raw_capture.json": raw_capture,
        _CLOUD_EVIDENCE_ARCHIVE_MEMBER: cloud_evidence,
        "fixture/raw_fixture.json": fixture,
        "fixture/anonymized_fixture.json": anonymized_fixture,
        "README.txt": "\n".join(readme_lines) + "\n",
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


def _support_bundle_cloud_evidence(support_bundle: dict[str, Any]) -> dict[str, Any] | None:
    evidence = support_bundle.get("evidence") if isinstance(support_bundle, dict) else None
    cloud_evidence = evidence.get("cloud") if isinstance(evidence, dict) else None
    return cloud_evidence if isinstance(cloud_evidence, dict) else None


def _archive_support_bundle_payload(support_bundle: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(support_bundle, dict):
        return support_bundle

    archived_payload = deepcopy(support_bundle)
    evidence = archived_payload.get("evidence")
    if not isinstance(evidence, dict):
        return archived_payload

    cloud_evidence = evidence.get("cloud")
    if cloud_evidence is None:
        return archived_payload

    evidence["cloud"] = {
        "archive_member": _CLOUD_EVIDENCE_ARCHIVE_MEMBER,
    }
    return archived_payload
