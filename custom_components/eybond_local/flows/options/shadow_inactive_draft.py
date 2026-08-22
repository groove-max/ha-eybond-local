"""Narrow flow adapter for one inactive DESSMonitor read-draft artifact."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...support.cloud_local_history_draft_writer import (
    CloudLocalReadDraftArtifact,
    generate_inactive_cloud_local_read_schema_draft,
)
from ...support.cloud_local_history_representability import (
    LocalRegisterOverlayContext,
)
from .shadow_metadata_review import cloud_local_history_draft_plan


async def async_generate_inactive_read_draft(
    *,
    hass: Any,
    coordinator: Any,
    metadata: object,
) -> dict[str, object] | None:
    """Write one current-context draft and return sanitized transient state."""

    plan = cloud_local_history_draft_plan(metadata)
    if plan is None or not plan.draft_generation_allowed:
        return None
    try:
        current_context = coordinator.local_register_overlay_context
    except Exception:
        return None
    if (
        type(current_context) is not LocalRegisterOverlayContext
        or current_context != plan.representability.context
    ):
        return None
    source_schema_name = getattr(
        coordinator,
        "effective_register_schema_name",
        None,
    )
    if (
        type(source_schema_name) is not str
        or not source_schema_name
        or source_schema_name != source_schema_name.strip()
        or source_schema_name != current_context.register_schema_name
    ):
        return None
    try:
        artifact = await hass.async_add_executor_job(
            lambda: generate_inactive_cloud_local_read_schema_draft(
                config_dir=Path(hass.config.config_dir),
                source_schema_name=source_schema_name,
                plan=plan,
            )
        )
    except Exception:
        return None
    if type(artifact) is not CloudLocalReadDraftArtifact:
        return None
    return {
        "schema_name": artifact.schema_name,
        "schema_path": str(artifact.schema_path),
        "generated_read_count": artifact.generated_read_count,
        "evidence_sha256": artifact.evidence_sha256,
        "status": "inactive_review_required",
        "activation_allowed": False,
    }
