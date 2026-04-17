"""Helpers for generating SmartESS-derived local draft metadata."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from ..const import BUILTIN_SCHEMA_PREFIX
from ..support.cloud_evidence import load_latest_cloud_evidence
from .local_metadata import (
    _dump_json,
    _ensure_can_write,
    builtin_profile_path,
    create_local_schema_draft,
    ensure_local_metadata_dirs,
    local_profile_path,
    local_profiles_root,
    local_register_schema_path,
    local_register_schemas_root,
)
from .register_schema_loader import load_register_schema
from .smartess_protocol_catalog_loader import resolve_smartess_protocol_catalog_entry


_SMARTESS_0925_KNOWN_FAMILY_DETAIL_SECTIONS = frozenset({"bc_", "bt_", "gd_", "pv_", "sy_"})


@dataclass(frozen=True, slots=True)
class SmartEssKnownFamilyDraftPlan:
    """Resolved known-family SmartESS draft plan."""

    asset_id: str
    profile_key: str
    driver_label: str
    source_profile_name: str
    source_schema_name: str
    reason: str
    raw_profile_name: str = ""
    raw_schema_name: str = ""


def resolve_smartess_known_family_draft_plan(
    *,
    smartess_protocol_asset_id: str = "",
    smartess_profile_key: str = "",
    cloud_evidence: dict[str, Any] | None = None,
) -> SmartEssKnownFamilyDraftPlan | None:
    """Resolve a safe SmartESS known-family draft source from hints or evidence."""

    entry = resolve_smartess_protocol_catalog_entry(
        asset_id=str(smartess_protocol_asset_id or "").strip(),
        profile_key=str(smartess_profile_key or "").strip(),
    )
    if entry is not None and entry.profile_name and entry.register_schema_name:
        reason_parts = []
        if smartess_protocol_asset_id:
            reason_parts.append(f"asset {smartess_protocol_asset_id}")
        if smartess_profile_key:
            reason_parts.append(f"profile key {smartess_profile_key}")
        reason = "Known SmartESS catalog mapping resolved from " + ", ".join(reason_parts)
        return _plan_from_catalog_entry(entry, reason=reason)

    if _looks_like_0925_known_family(cloud_evidence):
        entry = resolve_smartess_protocol_catalog_entry(asset_id="0925")
        if entry is not None and entry.profile_name and entry.register_schema_name:
            return SmartEssKnownFamilyDraftPlan(
                asset_id=entry.asset_id,
                profile_key=entry.profile_key,
                driver_label="SmartESS 0925",
                source_profile_name=entry.profile_name,
                source_schema_name=entry.register_schema_name,
                raw_profile_name=entry.raw_profile_name,
                raw_schema_name=entry.raw_register_schema_name,
                reason=(
                    "Known-family inference matched the verified SmartESS 0925 "
                    "detail-section signature bc_/bt_/gd_/pv_/sy_."
                ),
            )

    return None


def create_smartess_known_family_draft(
    *,
    config_dir: Path,
    plan: SmartEssKnownFamilyDraftPlan,
    cloud_evidence: dict[str, Any],
    output_profile_name: str | None = None,
    output_schema_name: str | None = None,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Create local SmartESS draft profile and schema for one known family."""

    ensure_local_metadata_dirs(config_dir)

    profile_output_name = output_profile_name or plan.source_profile_name
    schema_output_name = output_schema_name or plan.source_schema_name

    profile_destination = local_profile_path(config_dir, profile_output_name)
    schema_destination = local_register_schema_path(config_dir, schema_output_name)
    _ensure_can_write(
        profile_destination,
        local_profiles_root(config_dir),
        overwrite=overwrite,
    )
    _ensure_can_write(
        schema_destination,
        local_register_schemas_root(config_dir),
        overwrite=overwrite,
    )

    annotation = _draft_annotation(plan=plan, cloud_evidence=cloud_evidence)
    profile_raw = json.loads(builtin_profile_path(plan.source_profile_name).read_text(encoding="utf-8"))
    profile_raw.setdefault("draft_of", plan.source_profile_name)
    profile_raw.setdefault("experimental", True)
    profile_raw["title"] = str(profile_raw.get("title", Path(plan.source_profile_name).stem)) + " (Local SmartESS Draft)"
    profile_raw["smartess_draft"] = annotation
    profile_destination.parent.mkdir(parents=True, exist_ok=True)
    profile_destination.write_text(_dump_json(profile_raw), encoding="utf-8")

    schema = load_register_schema(f"{BUILTIN_SCHEMA_PREFIX}{plan.source_schema_name}")
    schema_raw = {
        "extends": f"{BUILTIN_SCHEMA_PREFIX}{plan.source_schema_name}",
        "schema_key": f"local_{schema.key}",
        "title": f"{schema.title} (Local SmartESS Draft)",
        "driver_key": schema.driver_key,
        "protocol_family": schema.protocol_family,
        "draft_of": plan.source_schema_name,
        "experimental": True,
        "smartess_draft": annotation,
    }
    schema_destination.parent.mkdir(parents=True, exist_ok=True)
    schema_destination.write_text(_dump_json(schema_raw), encoding="utf-8")

    return profile_destination, schema_destination


def latest_smartess_known_family_draft_plan(
    *,
    config_dir: Path,
    entry_id: str = "",
    collector_pn: str = "",
    smartess_protocol_asset_id: str = "",
    smartess_profile_key: str = "",
) -> SmartEssKnownFamilyDraftPlan | None:
    """Resolve one draft plan against the latest matching cloud evidence."""

    record = load_latest_cloud_evidence(
        config_dir,
        entry_id=entry_id,
        collector_pn=collector_pn,
    )
    return resolve_smartess_known_family_draft_plan(
        smartess_protocol_asset_id=smartess_protocol_asset_id,
        smartess_profile_key=smartess_profile_key,
        cloud_evidence=record.payload if record is not None else None,
    )


def _plan_from_catalog_entry(entry, *, reason: str) -> SmartEssKnownFamilyDraftPlan:
    asset_label = entry.asset_id or entry.profile_key or "SmartESS"
    return SmartEssKnownFamilyDraftPlan(
        asset_id=entry.asset_id,
        profile_key=entry.profile_key,
        driver_label=f"SmartESS {asset_label}",
        source_profile_name=entry.profile_name,
        source_schema_name=entry.register_schema_name,
        reason=reason,
        raw_profile_name=entry.raw_profile_name,
        raw_schema_name=entry.raw_register_schema_name,
    )


def _looks_like_0925_known_family(cloud_evidence: dict[str, Any] | None) -> bool:
    if not isinstance(cloud_evidence, dict):
        return False
    summary = cloud_evidence.get("summary")
    if isinstance(summary, dict):
        detail_sections = summary.get("detail_sections")
        if isinstance(detail_sections, list):
            if _SMARTESS_0925_KNOWN_FAMILY_DETAIL_SECTIONS.issubset({str(item) for item in detail_sections}):
                return True

    payload = cloud_evidence.get("payload")
    if not isinstance(payload, dict):
        return False
    normalized = payload.get("normalized")
    if not isinstance(normalized, dict):
        return False
    detail = normalized.get("device_detail")
    if not isinstance(detail, dict):
        return False
    section_counts = detail.get("section_counts")
    if not isinstance(section_counts, dict):
        return False
    return _SMARTESS_0925_KNOWN_FAMILY_DETAIL_SECTIONS.issubset({str(key) for key in section_counts})


def _draft_annotation(
    *,
    plan: SmartEssKnownFamilyDraftPlan,
    cloud_evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "kind": "known_family",
        "asset_id": plan.asset_id,
        "profile_key": plan.profile_key,
        "driver_label": plan.driver_label,
        "source_profile_name": plan.source_profile_name,
        "source_schema_name": plan.source_schema_name,
        "raw_profile_name": plan.raw_profile_name,
        "raw_schema_name": plan.raw_schema_name,
        "reason": plan.reason,
        "cloud_source": str(cloud_evidence.get("source") or ""),
        "cloud_match": dict(cloud_evidence.get("match") or {}),
        "cloud_identity": dict(cloud_evidence.get("device_identity") or {}),
        "cloud_summary": dict(cloud_evidence.get("summary") or {}),
    }