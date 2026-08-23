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
from ..dessmonitor_collection import (
    DessMonitorHistoryCollection,
    fetch_read_only_evidence_with_history,
)
from ..dessmonitor_history import (
    DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER,
    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART,
)
from .cloud_history_evidence import (
    CloudHistoryCollection,
    CloudHistoryIdentity,
    CloudHistoryPoint,
    CloudHistorySeries,
)
from .cloud_semantic_evidence import (
    CLOUD_FIELD_KIND_CHART,
    CLOUD_FIELD_KIND_KEY_PARAMETER,
)
from .cloud_learning_runner import CloudLearningOutcome
from .cloud_read_only_workflow import CloudReadOnlyEvidenceOperation
from .dessmonitor_semantics import build_dessmonitor_semantic_report


_LOGGER = logging.getLogger(__name__)

_DESSMONITOR_HISTORY_FIELD_KINDS = {
    DESSMONITOR_HISTORY_SOURCE_KEY_PARAMETER: CLOUD_FIELD_KIND_KEY_PARAMETER,
    DESSMONITOR_HISTORY_SOURCE_SOLE_CHART: CLOUD_FIELD_KIND_CHART,
}


def _normalized_history_collection(
    collection: DessMonitorHistoryCollection,
) -> CloudHistoryCollection:
    """Adapt provider-owned DESS evidence to the neutral review boundary."""

    if type(collection) is not DessMonitorHistoryCollection:
        raise TypeError("dessmonitor_history_collection_invalid")
    identity = CloudHistoryIdentity(
        pn=collection.identity.pn,
        sn=collection.identity.sn,
        devcode=collection.identity.devcode,
        devaddr=collection.identity.devaddr,
    )
    offset = (
        collection.time_basis.offset_seconds
        if collection.time_basis is not None
        else None
    )
    series = tuple(
        CloudHistorySeries(
            provider_id="smartess",
            source_id="dessmonitor",
            source_action=item.source_series.source_action,
            field_kind=_DESSMONITOR_HISTORY_FIELD_KINDS[
                item.source_series.source_action
            ],
            identity=identity,
            series_key=item.source_series.series_key,
            title=item.source_series.title,
            unit=item.source_series.unit,
            requested_date=item.source_series.requested_date,
            precision_minutes=item.source_series.precision_minutes,
            timezone_offset_seconds=item.time_basis.offset_seconds,
            points=tuple(
                CloudHistoryPoint(
                    device_local_timestamp=point.device_local_timestamp,
                    utc_timestamp=point.utc_timestamp,
                    value=point.value,
                )
                for point in item.points
            ),
        )
        for item in collection.series
    )
    return CloudHistoryCollection(
        provider_id="smartess",
        source_id="dessmonitor",
        identity=identity,
        requested_date=collection.requested_date,
        timezone_offset_seconds=offset,
        attempted_series_count=collection.attempted_series_count,
        failed_series_count=collection.failed_series_count,
        budget_exhausted=collection.budget_exhausted,
        series=series,
    )
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

_FETCH_DETAIL_WINDOWS = {
    # Leave the endpoint fraction to the existing named stage so detailed
    # updates can never make the progress bar move backwards.
    "queryDeviceCtrlValue": (0.54, 0.59),
    "queryDeviceKeyParameterOneDay": (0.68, 0.76),
}


class DessMonitorReadOnlyEvidenceOperation(CloudReadOnlyEvidenceOperation):
    """Fetch DESSMonitor metadata without collector endpoint side effects."""

    provider_id = "smartess"
    source_id = "dessmonitor"

    async def async_collect(
        self,
        *,
        executor,
        collector_pn,
        username,
        password,
        max_fields,
        progress,
    ) -> CloudLearningOutcome:
        bounded_control_values = (
            min(max_fields, DEFAULT_MAX_CONTROL_VALUES)
            if type(max_fields) is int and max_fields >= 0
            else 0
        )
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
            window = _FETCH_DETAIL_WINDOWS.get(stage)
            if (
                window is None
                or type(completed) is not int
                or type(total) is not int
                or total <= 0
                or completed <= 0
                or completed > total
            ):
                return
            start, end = window
            fraction = round(
                start + ((end - start) * (completed / total)),
                4,
            )
            loop.call_soon_threadsafe(progress, fraction, "fetching")

        bundle, history_collection = await executor(
            lambda: fetch_read_only_evidence_with_history(
                username=username,
                password=password,
                collector_pn=collector_pn,
                max_control_values=bounded_control_values,
                progress=report_fetch_progress,
                progress_detail=report_fetch_progress_detail,
            )
        )
        identity = bundle.identity.to_record()
        semantic_report = await executor(
            lambda: build_dessmonitor_semantic_report(bundle)
        )
        normalized_history = _normalized_history_collection(history_collection)
        evidence = bundle.to_record()
        evidence["semantic_report"] = semantic_report.to_record()
        evidence["history_collection"] = normalized_history.to_record()
        result: dict[str, Any] = {
            "source": self.source_id,
            "metadata_only": True,
            "metadata_field_count": bundle.metadata_field_count,
            "semantic_candidate_count": semantic_report.read_candidate_count,
            "semantic_unit_conflict_count": semantic_report.unit_conflict_count,
            "semantic_unknown_count": semantic_report.unknown_count,
            "control_metadata_count": semantic_report.control_metadata_count,
            "history_status": normalized_history.status,
            "history_series_count": normalized_history.collected_series_count,
            "history_point_count": normalized_history.point_count,
            "history_failed_series_count": normalized_history.failed_series_count,
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
            normalized_history.status,
            normalized_history.collected_series_count,
            normalized_history.point_count,
            normalized_history.failed_series_count,
        )
        return CloudLearningOutcome(
            identity=identity,
            result=result,
            read_bindings=None,
            metadata_evidence=evidence,
        )


__all__ = ["DessMonitorReadOnlyEvidenceOperation"]
