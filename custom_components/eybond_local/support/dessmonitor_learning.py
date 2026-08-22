"""Read-only DESSMonitor cloud-learning runner.

Unlike SmartESS/ValueCloud active learning this runner opens no shadow route,
sends no control action, and claims no local register binding.  It only returns
typed provider metadata for review and support evidence.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..dessmonitor_cloud import DEFAULT_MAX_CONTROL_VALUES
from ..dessmonitor_collection import fetch_read_only_evidence_with_history
from .cloud_learning_runner import CloudLearningOutcome, CloudLearningRunner
from .dessmonitor_semantics import build_dessmonitor_semantic_report


_LOGGER = logging.getLogger(__name__)
_FETCH_PROGRESS = {
    "authSource": 0.16,
    "webQueryDeviceEs": 0.23,
    "querySPDeviceLastData": 0.30,
    "queryDeviceChartField": 0.36,
    "querySPKeyParameters": 0.42,
    "queryDeviceCtrlField": 0.48,
    "queryDeviceLastRawData": 0.54,
    "queryDeviceCtrlValue": 0.60,
    "metadata_bundle": 0.64,
    "queryDeviceInfo": 0.68,
    "queryDeviceSoleChartEs": 0.73,
    "queryDeviceKeyParameterOneDay": 0.77,
    "history_complete": 0.80,
}


class DessMonitorReadOnlyLearningRunner(CloudLearningRunner):
    """Fetch DESSMonitor metadata without collector endpoint side effects."""

    provider_id = "smartess"
    source_id = "dessmonitor"

    async def async_run(
        self,
        *,
        executor,
        collector_pn,
        username,
        password,
        fallback_identity,
        max_fields,
        progress,
        orchestrator_callbacks,
        on_identity,
        start_shadow_route,
        on_learning,
    ) -> CloudLearningOutcome:
        del fallback_identity
        del orchestrator_callbacks
        del start_shadow_route
        del on_learning
        bounded_control_values = (
            min(max_fields, DEFAULT_MAX_CONTROL_VALUES)
            if type(max_fields) is int and max_fields >= 0
            else 0
        )
        progress(0.10, "fetching")
        loop = asyncio.get_running_loop()

        def report_fetch_progress(stage: str) -> None:
            fraction = _FETCH_PROGRESS.get(stage)
            if fraction is not None:
                loop.call_soon_threadsafe(progress, fraction, "fetching")

        bundle, history_collection = await executor(
            lambda: fetch_read_only_evidence_with_history(
                username=username,
                password=password,
                collector_pn=collector_pn,
                max_control_values=bounded_control_values,
                progress=report_fetch_progress,
            )
        )
        identity = bundle.identity.to_record()
        on_identity(identity)
        progress(0.82, "building")
        semantic_report = await executor(
            lambda: build_dessmonitor_semantic_report(bundle)
        )
        evidence = bundle.to_record()
        evidence["semantic_report"] = semantic_report.to_record()
        evidence["history_collection"] = history_collection.to_record()
        result: dict[str, Any] = {
            "source": self.source_id,
            "metadata_only": True,
            "metadata_field_count": bundle.metadata_field_count,
            "semantic_candidate_count": semantic_report.read_candidate_count,
            "semantic_unit_conflict_count": semantic_report.unit_conflict_count,
            "semantic_unknown_count": semantic_report.unknown_count,
            "control_metadata_count": semantic_report.control_metadata_count,
            "history_status": history_collection.status,
            "history_series_count": history_collection.collected_series_count,
            "history_point_count": history_collection.point_count,
            "history_failed_series_count": history_collection.failed_series_count,
            "plan": [],
            "planned_write_count": 0,
            "executed_result_count": 0,
            "sent_count": 0,
            "leaked_count": 0,
            "degraded_count": 0,
            "metadata_evidence": evidence,
        }
        _LOGGER.info(
            "DESSMonitor read-only analysis completed "
            "metadata_fields=%d unavailable_actions=%d semantic_candidates=%d "
            "semantic_unknown=%d history_status=%s history_series=%d "
            "history_points=%d history_failures=%d",
            bundle.metadata_field_count,
            len(bundle.unavailable_actions),
            semantic_report.read_candidate_count,
            semantic_report.unknown_count,
            history_collection.status,
            history_collection.collected_series_count,
            history_collection.point_count,
            history_collection.failed_series_count,
        )
        return CloudLearningOutcome(
            identity=identity,
            result=result,
            read_bindings=None,
            metadata_evidence=evidence,
        )


__all__ = ["DessMonitorReadOnlyLearningRunner"]
