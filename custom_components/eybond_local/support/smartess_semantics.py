"""Adapt normalized SmartESS metadata into provider-neutral semantic hints."""

from __future__ import annotations

from typing import Any

from .cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_READING,
    CLOUD_FIELD_KIND_SETTING,
    CloudSemanticEvidenceReport,
    CloudSemanticObservation,
    classify_cloud_semantic_observation,
)


_MAX_OBSERVATIONS = 512
_MAX_TEXT_LENGTH = 512


def _text(value: object) -> str:
    return str(value or "").strip()[:_MAX_TEXT_LENGTH]


def build_smartess_semantic_report(
    bundle: dict[str, Any],
) -> CloudSemanticEvidenceReport:
    """Return hint-only semantics from one normalized SmartESS bundle."""

    if type(bundle) is not dict:
        raise TypeError("smartess_semantic_bundle_invalid")
    normalized = bundle.get("normalized")
    if type(normalized) is not dict:
        raise ValueError("smartess_semantic_normalized_missing")

    observations: list[CloudSemanticObservation] = []
    detail = normalized.get("device_detail")
    sections = detail.get("sections") if type(detail) is dict else None
    if type(sections) is dict:
        for rows in sections.values():
            if type(rows) is not list:
                continue
            for row in rows:
                if len(observations) >= _MAX_OBSERVATIONS:
                    break
                if type(row) is not dict:
                    continue
                title = _text(row.get("par"))
                if not title:
                    continue
                observations.append(
                    classify_cloud_semantic_observation(
                        field_kind=CLOUD_FIELD_KIND_READING,
                        field_id=_text(row.get("id")),
                        title=title,
                        value=_text(row.get("val")),
                        observed_unit=_text(row.get("unit")),
                        source_action="querySPDeviceLastData",
                    )
                )

    settings = normalized.get("device_settings")
    fields = settings.get("fields") if type(settings) is dict else None
    if type(fields) is list:
        for field in fields:
            if len(observations) >= _MAX_OBSERVATIONS:
                break
            if type(field) is not dict:
                continue
            title = _text(field.get("title"))
            if not title:
                continue
            observations.append(
                classify_cloud_semantic_observation(
                    field_kind=CLOUD_FIELD_KIND_SETTING,
                    field_id=_text(field.get("cloud_id")),
                    title=title,
                    value=(
                        _text(field.get("current_value"))
                        if field.get("has_current_value") is True
                        else ""
                    ),
                    observed_unit=_text(field.get("unit")),
                    source_action="webQueryDeviceCtrlField",
                )
            )

    return CloudSemanticEvidenceReport(
        provider_id="smartess",
        source_id="smartess",
        observations=tuple(observations),
    )


__all__ = ["build_smartess_semantic_report"]
