from __future__ import annotations

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
from custom_components.eybond_local.support.dessmonitor_learning import (  # noqa: E402
    DessMonitorReadOnlyLearningRunner,
)


class DessMonitorLearningRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_metadata_runner_never_opens_route_or_learning_writer(self) -> None:
        bundle = DessMonitorEvidenceBundle(
            identity=DessMonitorDeviceIdentity(
                pn="E50000200000000001",
                sn="92632511100118",
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

        async def executor(operation):
            return operation()

        with patch(
            "custom_components.eybond_local.support.dessmonitor_learning.fetch_read_only_evidence_with_history",
            return_value=(bundle, history_collection),
        ) as fetch:
            outcome = await DessMonitorReadOnlyLearningRunner().async_run(
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

        start_route.assert_not_awaited()
        on_learning.assert_not_called()
        self.assertEqual(fetch.call_count, 1)
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
        self.assertEqual(
            outcome.metadata_evidence["history_collection"],
            history_collection.to_record(),
        )
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
        self.assertEqual(progress, [(0.10, "fetching"), (0.82, "building")])


if __name__ == "__main__":
    unittest.main()
