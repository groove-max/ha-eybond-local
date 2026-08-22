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
from custom_components.eybond_local.support.dessmonitor_learning import (  # noqa: E402
    DessMonitorReadOnlyLearningRunner,
)


class DessMonitorLearningRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_metadata_runner_never_opens_route_or_learning_writer(self) -> None:
        bundle = DessMonitorEvidenceBundle(
            identity=DessMonitorDeviceIdentity(
                pn="E50000253884199645",
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

        async def executor(operation):
            return operation()

        with patch(
            "custom_components.eybond_local.support.dessmonitor_learning.fetch_read_only_evidence",
            return_value=bundle,
        ) as fetch:
            outcome = await DessMonitorReadOnlyLearningRunner().async_run(
                executor=executor,
                collector_pn="E5000025388419",
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
        self.assertEqual(identities[0]["pn"], "E50000253884199645")
        self.assertTrue(outcome.result["metadata_only"])
        self.assertEqual(outcome.result["planned_write_count"], 0)
        assert outcome.metadata_evidence is not None
        self.assertEqual(outcome.metadata_evidence["metadata_field_count"], 1)
        self.assertEqual(progress, [(0.10, "fetching"), (0.82, "building")])


if __name__ == "__main__":
    unittest.main()
