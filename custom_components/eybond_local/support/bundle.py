"""Support bundle export helpers for troubleshooting and experimental onboarding."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any

from ..const import LOCAL_METADATA_DIR, LOCAL_SUPPORT_BUNDLES_DIR


def support_bundles_root(config_dir: Path) -> Path:
    """Return the support bundle output directory."""

    return config_dir / LOCAL_METADATA_DIR / LOCAL_SUPPORT_BUNDLES_DIR


def build_support_bundle_payload(
    *,
    entry_id: str,
    entry_title: str,
    connected: bool,
    collector: dict[str, Any] | None,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
    data: dict[str, Any],
    options: dict[str, Any],
    profile_name: str,
    register_schema_name: str,
    variant_key: str = "",
) -> dict[str, Any]:
    """Build one machine-readable support bundle payload."""

    return {
        "bundle_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entry": {
            "entry_id": entry_id,
            "title": entry_title,
            "data": data,
            "options": options,
        },
        "source_metadata": {
            "profile_name": profile_name,
            "register_schema_name": register_schema_name,
            "variant_key": variant_key,
        },
        "runtime": {
            "connected": connected,
            "collector": collector,
            "inverter": inverter,
            "values": values,
        },
    }


def export_support_bundle(
    *,
    config_dir: Path,
    entry_id: str,
    entry_title: str,
    connected: bool,
    collector: dict[str, Any] | None,
    inverter: dict[str, Any] | None,
    values: dict[str, Any],
    data: dict[str, Any],
    options: dict[str, Any],
    profile_name: str,
    register_schema_name: str,
    variant_key: str = "",
    overwrite: bool = False,
) -> Path:
    """Write one JSON support bundle under the local support bundle directory."""

    bundles_root = support_bundles_root(config_dir)
    bundles_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = bundles_root / f"{entry_id}_{timestamp}.json"
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)

    payload = build_support_bundle_payload(
        entry_id=entry_id,
        entry_title=entry_title,
        connected=connected,
        collector=collector,
        inverter=inverter,
        values=values,
        data=data,
        options=options,
        profile_name=profile_name,
        register_schema_name=register_schema_name,
        variant_key=variant_key,
    )
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return destination
