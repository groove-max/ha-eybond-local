"""Read-only SmartESS cloud-evidence operation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..collector_identity import pn_is_same_identity
from .cloud_evidence import build_smartess_device_bundle_cloud_evidence
from .cloud_history_evidence import CloudHistoryCollection
from .cloud_learning_runner import CloudLearningOutcome
from .cloud_read_only_workflow import CloudReadOnlyEvidenceOperation
from .cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_KEY_PARAMETER,
    CloudSemanticEvidenceReport,
    classify_cloud_semantic_observation,
)
from .smartess_history import (
    SMARTESS_HISTORY_KEYS_ACTION,
    SMARTESS_HISTORY_SOURCE_ACTION,
    SMARTESS_HISTORY_TIME_BASIS_ACTION,
    fetch_smartess_evidence_with_history,
)
from .smartess_semantics import build_smartess_semantic_report


_LOGGER = logging.getLogger(__name__)

_FETCH_PROGRESS = {
    "authSource": 0.18,
    "metadata_bundle": 0.55,
    SMARTESS_HISTORY_TIME_BASIS_ACTION: 0.62,
    SMARTESS_HISTORY_KEYS_ACTION: 0.68,
    SMARTESS_HISTORY_SOURCE_ACTION: 0.78,
    "history_complete": 0.80,
}


def _semantic_report_with_history(
    bundle: dict[str, Any],
    history_collection: CloudHistoryCollection,
) -> CloudSemanticEvidenceReport:
    if type(history_collection) is not CloudHistoryCollection:
        raise TypeError("smartess_history_collection_invalid")
    report = build_smartess_semantic_report(bundle)
    observations = list(report.observations)
    observations.extend(
        classify_cloud_semantic_observation(
            field_kind=CLOUD_FIELD_KIND_KEY_PARAMETER,
            field_id=series.series_key,
            title=series.title,
            value=(series.points[-1].value if series.points else ""),
            observed_unit=series.unit,
            source_action=series.source_action,
        )
        for series in history_collection.series
    )
    return CloudSemanticEvidenceReport(
        provider_id=report.provider_id,
        source_id=report.source_id,
        observations=tuple(observations),
    )


class SmartEssReadOnlyEvidenceOperation(CloudReadOnlyEvidenceOperation):
    """Collect SmartESS metadata without route changes or control actions."""

    provider_id = "smartess"
    source_id = "smartess"

    async def async_collect(
        self,
        *,
        executor,
        collector_pn: str,
        username: str,
        password: str,
        max_fields: int,
        progress,
    ) -> CloudLearningOutcome:
        del max_fields
        loop = asyncio.get_running_loop()

        def report_fetch_progress(stage: str) -> None:
            fraction = _FETCH_PROGRESS.get(stage)
            if fraction is not None:
                loop.call_soon_threadsafe(progress, fraction, "fetching")

        def report_fetch_progress_detail(
            stage: str,
            completed: int,
            total: int,
        ) -> None:
            if (
                stage != SMARTESS_HISTORY_SOURCE_ACTION
                or type(completed) is not int
                or type(total) is not int
                or total <= 0
                or completed <= 0
                or completed > total
            ):
                return
            fraction = round(0.68 + (0.09 * (completed / total)), 4)
            loop.call_soon_threadsafe(progress, fraction, "fetching")

        history_fetch = await executor(
            lambda: fetch_smartess_evidence_with_history(
                username=username,
                password=password,
                collector_pn=collector_pn,
                progress=report_fetch_progress,
                progress_detail=report_fetch_progress_detail,
            )
        )
        bundle = history_fetch.bundle
        history_collection = history_fetch.history_collection
        history_diagnostics = history_fetch.to_diagnostics_record()
        progress(0.80, "building")
        evidence = await executor(
            lambda: build_smartess_device_bundle_cloud_evidence(
                bundle,
                source="smartess_read_only_learning",
                collector_pn=collector_pn,
            )
        )
        semantic_report = await executor(
            lambda: _semantic_report_with_history(bundle, history_collection)
        )
        identity = evidence.get("device_identity")
        if type(identity) is not dict:
            raise RuntimeError("smartess_read_only_identity_unavailable")
        pn = identity.get("pn")
        if (
            type(pn) is not str
            or not pn
            or pn != pn.strip()
            or not pn_is_same_identity(collector_pn, pn)
        ):
            raise RuntimeError("smartess_read_only_identity_mismatch")

        evidence = dict(evidence)
        evidence["semantic_report"] = semantic_report.to_record()
        evidence["history_collection"] = history_collection.to_record()
        evidence["history_diagnostics"] = history_diagnostics
        evidence["metadata_field_count"] = len(semantic_report.observations)
        result: dict[str, Any] = {
            "source": self.source_id,
            "metadata_only": True,
            "metadata_field_count": len(semantic_report.observations),
            "semantic_candidate_count": semantic_report.read_candidate_count,
            "semantic_unit_conflict_count": semantic_report.unit_conflict_count,
            "semantic_unknown_count": semantic_report.unknown_count,
            "control_metadata_count": semantic_report.control_metadata_count,
            "history_status": history_collection.status,
            "history_series_count": history_collection.collected_series_count,
            "history_point_count": history_collection.point_count,
            "history_failed_series_count": history_collection.failed_series_count,
            "history_failure_stage": history_fetch.failure_stage,
            "history_failure_code": history_fetch.failure_code,
            "plan": [],
            "planned_write_count": 0,
            "executed_result_count": 0,
            "sent_count": 0,
            "leaked_count": 0,
            "degraded_count": 0,
            "metadata_evidence": evidence,
        }
        _LOGGER.info(
            "SmartESS read-only analysis completed metadata_fields=%d "
            "semantic_candidates=%d semantic_unknown=%d history_status=%s "
            "history_series=%d history_points=%d history_failures=%d "
            "history_failure_stage=%s history_failure_code=%s",
            len(semantic_report.observations),
            semantic_report.read_candidate_count,
            semantic_report.unknown_count,
            history_collection.status,
            history_collection.collected_series_count,
            history_collection.point_count,
            history_collection.failed_series_count,
            history_fetch.failure_stage or "none",
            history_fetch.failure_code or "none",
        )
        return CloudLearningOutcome(
            identity=dict(identity),
            result=result,
            read_bindings=None,
            metadata_evidence=evidence,
        )


__all__ = ["SmartEssReadOnlyEvidenceOperation"]
