"""Adapt typed DESSMonitor metadata into provider-neutral semantic hints."""

from __future__ import annotations

from ..dessmonitor_cloud import DessMonitorEvidenceBundle
from .cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_CHART,
    CLOUD_FIELD_KIND_KEY_PARAMETER,
    CLOUD_FIELD_KIND_READING,
    CLOUD_FIELD_KIND_SETTING,
    CloudSemanticEvidenceReport,
    CloudSemanticObservation,
    classify_cloud_semantic_observation,
)


def build_dessmonitor_semantic_report(
    bundle: DessMonitorEvidenceBundle,
) -> CloudSemanticEvidenceReport:
    """Return semantic hints for one exact typed DESSMonitor bundle."""

    if type(bundle) is not DessMonitorEvidenceBundle:
        raise TypeError("dessmonitor_semantic_bundle_invalid")
    observations: list[CloudSemanticObservation] = []
    for fields, field_kind in (
        (bundle.telemetry_fields, CLOUD_FIELD_KIND_READING),
        (bundle.chart_fields, CLOUD_FIELD_KIND_CHART),
        (bundle.key_parameters, CLOUD_FIELD_KIND_KEY_PARAMETER),
    ):
        for field in fields:
            observations.append(
                classify_cloud_semantic_observation(
                    field_kind=field_kind,
                    field_id=field.field_id,
                    title=field.title,
                    value=field.value,
                    observed_unit=field.unit,
                    source_action=field.source_action,
                )
            )
    for field in bundle.control_fields:
        observations.append(
            classify_cloud_semantic_observation(
                field_kind=CLOUD_FIELD_KIND_SETTING,
                field_id=field.field_id,
                title=field.title,
                value=field.current_value,
                observed_unit=field.unit,
                source_action="queryDeviceCtrlField",
            )
        )
    return CloudSemanticEvidenceReport(
        provider_id="smartess",
        source_id="dessmonitor",
        observations=tuple(observations),
    )


__all__ = ["build_dessmonitor_semantic_report"]
