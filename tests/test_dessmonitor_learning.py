from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    DessMonitorTelemetryField,
)
from custom_components.eybond_local.dessmonitor_collection import (  # noqa: E402
    DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
    DessMonitorHistoryCollection,
)
from custom_components.eybond_local.support.cloud_read_only_workflow import (  # noqa: E402
    ReadOnlyEvidenceWorkflowRunner,
)
from custom_components.eybond_local.support.cloud_history_evidence import (  # noqa: E402
    CLOUD_HISTORY_AUTHORITY,
    CloudHistoryCollection,
)
from custom_components.eybond_local.support.dessmonitor_learning import (  # noqa: E402
    DessMonitorReadOnlyEvidenceOperation,
    _FETCH_DETAIL_WINDOWS,
    _FETCH_PROGRESS,
)

# Named stages the mock emits via report(); omits queryDeviceSoleChartEs which
# production can emit but this fixture does not.
_MOCK_REPORT_STAGES = (
    "authSource",
    "webQueryDeviceEs",
    "querySPDeviceLastData",
    "queryDeviceChartField",
    "querySPKeyParameters",
    "queryDeviceCtrlField",
    "queryDeviceLastRawData",
    "queryDeviceCtrlValue",
    "metadata_bundle",
    "queryDeviceInfo",
    "queryDeviceKeyParameterOneDay",
    "history_complete",
)


class DessMonitorLearningRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_metadata_runner_never_opens_route_or_learning_writer(self) -> None:
        bundle = DessMonitorEvidenceBundle(
            identity=DessMonitorDeviceIdentity(
                pn="E50000200000000001",
                sn="90000000000001",
                devcode=2376,
                devaddr=1,
            ),
            telemetry_fields=(
                DessMonitorTelemetryField(
                    field_id="pv_voltage",
                    title="PV Voltage",
                    value="123.4",
                    unit="V",
                    section="pv_",
                    source_action="querySPDeviceLastData",
                ),
            ),
            chart_fields=(),
            key_parameters=(),
            control_fields=(),
            raw_packet_sha256=hashlib.sha256(b"raw").hexdigest(),
            raw_packet_length=3,
        )
        start_route = AsyncMock()
        on_learning = Mock()
        identities: list[dict] = []
        progress: list[tuple[float, str]] = []
        history_collection = DessMonitorHistoryCollection(
            identity=bundle.identity,
            time_basis=None,
            requested_date="",
            attempted_series_count=0,
            failed_series_count=0,
            budget_exhausted=False,
            series=(),
        )

        executor_calls = 0

        async def executor(operation):
            nonlocal executor_calls
            executor_calls += 1
            return await asyncio.to_thread(operation)

        def fetch_bundle(**kwargs):
            report = kwargs["progress"]
            detail = kwargs["progress_detail"]
            for stage in _MOCK_REPORT_STAGES:
                if stage in _FETCH_DETAIL_WINDOWS:
                    for completed in range(1, 5):
                        detail(stage, completed, 4)
                report(stage)
            return bundle, history_collection

        with patch(
            "custom_components.eybond_local.support.dessmonitor_learning.fetch_read_only_evidence_with_history",
            side_effect=fetch_bundle,
        ) as fetch:
            outcome = await ReadOnlyEvidenceWorkflowRunner(
                DessMonitorReadOnlyEvidenceOperation()
            ).async_run(
                executor=executor,
                collector_pn="E5000020000000",
                username="account",
                password="password",
                fallback_identity={"pn": "FOREIGN"},
                max_fields=40,
                progress=lambda fraction, stage, **_kwargs: progress.append(
                    (fraction, stage)
                ),
                orchestrator_callbacks={"write": object()},
                on_identity=identities.append,
                start_shadow_route=start_route,
                on_learning=on_learning,
            )

        # Executor awaits to_thread, so the sync fetch has joined before we
        # continue; call_soon_threadsafe progress callbacks can still be queued.
        # One sleep(0) drains them. Under suite load a late drain may still
        # append fetching after building — invariants below tolerate that.
        await asyncio.sleep(0)

        start_route.assert_not_awaited()
        on_learning.assert_not_called()
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(executor_calls, 2)
        self.assertEqual(fetch.call_args.kwargs["max_control_values"], 16)
        self.assertEqual(identities[0]["pn"], "E50000200000000001")
        self.assertTrue(outcome.result["metadata_only"])
        self.assertEqual(outcome.result["planned_write_count"], 0)
        self.assertEqual(outcome.result["semantic_candidate_count"], 1)
        self.assertEqual(outcome.result["semantic_unit_conflict_count"], 0)
        self.assertEqual(outcome.result["semantic_unknown_count"], 0)
        self.assertEqual(
            outcome.result["history_status"],
            DESSMONITOR_COLLECTION_STATUS_TIME_BASIS_UNAVAILABLE,
        )
        self.assertEqual(outcome.result["history_series_count"], 0)
        self.assertEqual(outcome.result["history_point_count"], 0)
        self.assertEqual(outcome.result["history_failed_series_count"], 0)
        assert outcome.metadata_evidence is not None
        self.assertEqual(outcome.metadata_evidence["metadata_field_count"], 1)
        history_record = outcome.metadata_evidence["history_collection"]
        self.assertEqual(history_record["authority"], CLOUD_HISTORY_AUTHORITY)
        normalized_history = CloudHistoryCollection.from_record(history_record)
        self.assertIsNotNone(normalized_history)
        assert normalized_history is not None
        self.assertEqual(normalized_history.source_id, "dessmonitor")
        self.assertEqual(normalized_history.identity.pn, bundle.identity.pn)
        semantic_report = outcome.metadata_evidence["semantic_report"]
        self.assertEqual(semantic_report["authority"], "semantic_hint_only")
        self.assertIs(semantic_report["local_mapping_proven"], False)
        self.assertEqual(semantic_report["recognized_count"], 1)
        self.assertEqual(
            semantic_report["observations"][0]["semantic_key"],
            "pv_voltage",
        )
        self.assertEqual(
            semantic_report["observations"][0]["local_mapping"],
            "unproven",
        )
        self.assertNotIn("register", str(semantic_report).casefold())
        # Progress invariants (not a brittle exact timeline): start bookend,
        # single building stage, all mock-emitted _FETCH_PROGRESS waypoints,
        # and non-decreasing fractions on the pre-building prefix.
        self.assertGreaterEqual(len(progress), 3)
        self.assertEqual(progress[0], (0.10, "fetching"))
        stages = [stage for _fraction, stage in progress]
        self.assertTrue(set(stages) <= {"fetching", "building"})
        self.assertEqual(stages.count("building"), 1)
        building_index = stages.index("building")
        self.assertEqual(progress[building_index], (0.82, "building"))
        pre_building = progress[:building_index]
        self.assertTrue(all(stage == "fetching" for _fraction, stage in pre_building))
        pre_fractions = [fraction for fraction, _stage in pre_building]
        self.assertEqual(pre_fractions, sorted(pre_fractions))
        self.assertTrue(
            all(stage == "fetching" for _fraction, stage in progress[building_index + 1 :])
        )
        fractions = [fraction for fraction, _stage in progress]
        expected_waypoints = {_FETCH_PROGRESS[stage] for stage in _MOCK_REPORT_STAGES}
        self.assertTrue(
            expected_waypoints <= set(fractions),
            msg=f"missing fetch waypoints: {sorted(expected_waypoints - set(fractions))}",
        )
        for stage, (start, _end) in _FETCH_DETAIL_WINDOWS.items():
            named = _FETCH_PROGRESS[stage]
            detail_fractions = [
                fraction for fraction in fractions if start < fraction < named
            ]
            self.assertGreaterEqual(
                len(detail_fractions),
                2,
                msg=(
                    f"expected ≥2 {stage} detail fractions in ({start}, {named}); "
                    f"got {detail_fractions!r}"
                ),
            )


if __name__ == "__main__":
    unittest.main()
