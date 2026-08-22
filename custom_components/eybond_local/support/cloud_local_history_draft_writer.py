"""Write one deterministic, inactive DESSMonitor local-read schema draft."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

from ..metadata.local_metadata import (
    _dump_json,
    _ensure_can_write,
    draft_activates_automatically,
    ensure_local_metadata_dirs,
    local_register_schema_path,
    local_register_schemas_root,
)
from ..metadata.register_schema_loader import load_register_schema
from ..models import ProbeTarget, decimals_for_divisor
from .cloud_local_history_draft import (
    CLOUD_LOCAL_READ_DRAFT_SOURCE,
    CloudLocalReadDraftItem,
    CloudLocalReadDraftPlan,
)
from .cloud_local_history_representability import (
    build_local_register_overlay_context,
)


CLOUD_LOCAL_READ_DRAFT_ARTIFACT_SCHEMA_VERSION = 1
CLOUD_LOCAL_READ_DRAFT_ARTIFACT_AUTHORITY = "inactive_review_artifact_only"

_OUT_OF_BLOCK_SPEC_SET = "aux_config"
_SAFE_TOKEN_RE = re.compile(r"[^a-z0-9]+")


def _required_token(value: object, reason: str) -> str:
    if type(value) is not str:
        raise TypeError(reason)
    if not value or value != value.strip():
        raise ValueError(reason)
    return value


def _slug(value: str) -> str:
    return _SAFE_TOKEN_RE.sub("_", value.casefold()).strip("_") or "item"


@dataclass(frozen=True, slots=True)
class CloudLocalReadDraftArtifact:
    """One on-disk schema artifact that remains disconnected from activation."""

    schema_name: str
    schema_path: Path
    generated_read_count: int
    evidence_sha256: str
    manifest: dict[str, Any]

    def __post_init__(self) -> None:
        _required_token(
            self.schema_name,
            "cloud_local_read_draft_artifact_name_invalid",
        )
        if not isinstance(self.schema_path, Path):
            raise TypeError("cloud_local_read_draft_artifact_path_invalid")
        if type(self.generated_read_count) is not int:
            raise TypeError("cloud_local_read_draft_artifact_count_invalid")
        if self.generated_read_count < 1:
            raise ValueError("cloud_local_read_draft_artifact_count_invalid")
        if (
            type(self.evidence_sha256) is not str
            or len(self.evidence_sha256) != 64
            or any(char not in "0123456789abcdef" for char in self.evidence_sha256)
        ):
            raise ValueError("cloud_local_read_draft_artifact_digest_invalid")
        if type(self.manifest) is not dict:
            raise TypeError("cloud_local_read_draft_artifact_manifest_invalid")


def generate_inactive_cloud_local_read_schema_draft(
    *,
    config_dir: Path,
    source_schema_name: str,
    plan: CloudLocalReadDraftPlan,
) -> CloudLocalReadDraftArtifact:
    """Create a reviewable schema without activating or selecting it."""

    if not isinstance(config_dir, Path):
        raise TypeError("cloud_local_read_draft_config_dir_invalid")
    source_schema_name = _required_token(
        source_schema_name,
        "cloud_local_read_draft_source_schema_invalid",
    )
    if type(plan) is not CloudLocalReadDraftPlan:
        raise TypeError("cloud_local_read_draft_plan_invalid")
    if not plan.draft_generation_allowed:
        raise ValueError("cloud_local_read_draft_empty")

    context = plan.representability.context
    if context.register_schema_name != source_schema_name:
        raise ValueError("cloud_local_read_draft_schema_context_mismatch")
    schema = load_register_schema(source_schema_name)
    if schema.driver_key != context.driver_key:
        raise ValueError("cloud_local_read_draft_driver_context_mismatch")

    # Rebuild the complete schema claim snapshot immediately before writing.
    # A directly forged context or any schema drift therefore refuses before
    # the local metadata directory is created.
    current_context = build_local_register_overlay_context(
        collector_pn=context.collector_pn,
        driver_key=context.driver_key,
        probe_target=ProbeTarget(
            devcode=context.devcode,
            collector_addr=context.collector_addr,
            device_addr=context.device_addr,
        ),
        register_schema_name=source_schema_name,
        register_schema=schema,
    )
    if current_context != context:
        raise ValueError("cloud_local_read_draft_schema_context_changed")

    plan_record = plan.to_record()
    evidence_sha256 = sha256(
        json.dumps(
            plan_record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    output_name = _output_schema_name(
        source_schema_name=source_schema_name,
        collector_pn=context.collector_pn,
        evidence_sha256=evidence_sha256,
    )
    if draft_activates_automatically(source_schema_name, output_name):
        raise ValueError("cloud_local_read_draft_would_activate")

    fragment, generated = _schema_fragment(schema, plan.items)
    manifest: dict[str, Any] = {
        "schema_version": CLOUD_LOCAL_READ_DRAFT_ARTIFACT_SCHEMA_VERSION,
        "authority": CLOUD_LOCAL_READ_DRAFT_ARTIFACT_AUTHORITY,
        "source_id": CLOUD_LOCAL_READ_DRAFT_SOURCE,
        "status": "inactive_review_required",
        "local_mapping": "candidate_not_proven",
        "local_mapping_proven": False,
        "activation_allowed": False,
        "source_schema_name": source_schema_name,
        "driver_key": context.driver_key,
        "collector_pn": context.collector_pn,
        "probe_target": {
            "devcode": context.devcode,
            "collector_addr": context.collector_addr,
            "device_addr": context.device_addr,
        },
        "evidence_sha256": evidence_sha256,
        "plan": plan_record,
        "generated_reads": generated,
    }
    schema_raw: dict[str, Any] = {
        "extends": source_schema_name,
        "schema_key": f"local_dessmonitor_review_{evidence_sha256[:16]}",
        "title": f"{schema.title} (DESSMonitor Review Draft)",
        "driver_key": schema.driver_key,
        "protocol_family": schema.protocol_family,
        "draft_of": source_schema_name,
        "experimental": True,
        "dessmonitor_read_draft": manifest,
        **fragment,
    }
    payload = _dump_json(schema_raw)

    destination = local_register_schema_path(config_dir, output_name)
    if destination.exists():
        if destination.read_text(encoding="utf-8") != payload:
            raise FileExistsError(destination)
    else:
        ensure_local_metadata_dirs(config_dir)
        _ensure_can_write(
            destination,
            local_register_schemas_root(config_dir),
            overwrite=False,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f".{destination.name}.{evidence_sha256[:16]}.tmp"
        )
        temporary.unlink(missing_ok=True)
        try:
            temporary.write_text(payload, encoding="utf-8")
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)

    return CloudLocalReadDraftArtifact(
        schema_name=output_name,
        schema_path=destination,
        generated_read_count=len(generated),
        evidence_sha256=evidence_sha256,
        manifest=manifest,
    )


def _output_schema_name(
    *,
    source_schema_name: str,
    collector_pn: str,
    evidence_sha256: str,
) -> str:
    source_stem = _slug(Path(source_schema_name).stem)
    collector_token = _slug(collector_pn)
    return (
        "learned/dessmonitor_review/"
        f"{collector_token}/{source_stem}_{evidence_sha256[:16]}.json"
    )


def _schema_fragment(
    schema: Any,
    items: tuple[CloudLocalReadDraftItem, ...],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    additions: dict[str, list[dict[str, Any]]] = {}
    measurements: list[dict[str, Any]] = []
    generated: list[dict[str, Any]] = []
    for item in items:
        candidate = item.candidate
        location = candidate.location
        semantic = item.semantic
        key = (
            f"learned_read_dessmonitor_fc{location.function}_"
            f"{location.register}_{_slug(semantic.semantic_key)}"
        )
        set_name = _spec_set_for_location(
            schema,
            function=location.function,
            register=location.register,
        )
        decimals = decimals_for_divisor(candidate.divisor)
        spec: dict[str, Any] = {
            "key": key,
            "function": location.function,
            "register": location.register,
        }
        if candidate.signed:
            spec["signed"] = True
        if candidate.divisor > 1:
            spec["divisor"] = candidate.divisor
            spec["decimals"] = decimals
        measurement: dict[str, Any] = {
            "key": key,
            "name": semantic.canonical_title,
            "translation_key": semantic.semantic_key,
            "enabled_default": False,
            "learned": True,
        }
        unit = semantic.expected_unit or semantic.observed_unit
        if unit:
            measurement["unit"] = unit
        if semantic.device_class:
            measurement["device_class"] = semantic.device_class
        if semantic.state_class:
            measurement["state_class"] = semantic.state_class
        if decimals:
            measurement["display_precision"] = decimals

        additions.setdefault(set_name, []).append(spec)
        measurements.append(measurement)
        generated.append(
            {
                "key": key,
                "semantic_key": semantic.semantic_key,
                "title": semantic.canonical_title,
                "function": location.function,
                "register": location.register,
                "spec_set": set_name,
                "divisor": candidate.divisor,
                "signed": candidate.signed,
            }
        )

    return (
        {
            "spec_sets": additions,
            "measurement_descriptions": measurements,
            "learned_read_registers": sorted(
                {item["register"] for item in generated}
            ),
            "learned_read_locations": sorted(
                {
                    (item["function"], item["register"])
                    for item in generated
                }
            ),
        },
        generated,
    )


def _spec_set_for_location(
    schema: Any,
    *,
    function: int,
    register: int,
) -> str:
    matches = tuple(
        block.key
        for block in schema.blocks
        if block.function == function
        and block.start <= register < block.start + block.count
    )
    return matches[0] if len(matches) == 1 else _OUT_OF_BLOCK_SPEC_SET


__all__ = [
    "CLOUD_LOCAL_READ_DRAFT_ARTIFACT_AUTHORITY",
    "CLOUD_LOCAL_READ_DRAFT_ARTIFACT_SCHEMA_VERSION",
    "CloudLocalReadDraftArtifact",
    "generate_inactive_cloud_local_read_schema_draft",
]
