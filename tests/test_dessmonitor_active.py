from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorApiEnvelope,
    DessMonitorControlField,
    DessMonitorDeviceIdentity,
    DessMonitorEvidenceBundle,
    DessMonitorSession,
)
from custom_components.eybond_local.support import dessmonitor_active as active  # noqa: E402
from custom_components.eybond_local.support.cloud_learning_runner import (  # noqa: E402
    CloudLearningOutcome,
)


IDENTITY = DessMonitorDeviceIdentity(
    pn="E50000200000000001",
    sn="SN1",
    devcode=2304,
    devaddr=1,
)
BUNDLE = DessMonitorEvidenceBundle(
    identity=IDENTITY,
    telemetry_fields=(),
    chart_fields=(),
    key_parameters=(),
    control_fields=(
        DessMonitorControlField(
            field_id="mode",
            title="Mode",
            choices=(("1", "One"),),
        ),
    ),
)
ORCHESTRATION = {
    "planned_write_count": 1,
    "executed_result_count": 1,
    "sent_count": 0,
    "captured_not_applied_count": 1,
    "error_count": 0,
    "degraded_count": 0,
    "leaked_count": 0,
    "unknown_field_count": 0,
    "results": [],
    "correlation": {"matched_count": 1},
    "read_map": {},
}


async def _executor(callable_):
    return callable_()


class DessMonitorActiveOperationTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_and_metadata_precede_identity_route_and_actions(self) -> None:
        order = []
        session = DessMonitorSession(token="token", secret="secret")

        def login(**kwargs):
            order.append("auth")
            self.assertEqual(kwargs["username"], "user")
            self.assertEqual(kwargs["password"], "password")
            return DessMonitorApiEnvelope(err=0, desc="", dat={}), session

        def fetch(**kwargs):
            order.append("metadata")
            self.assertIs(kwargs["session"], session)
            self.assertEqual(kwargs["max_control_values"], 0)
            self.assertEqual(
                kwargs["required_actions"],
                ("queryDeviceCtrlField",),
            )
            return BUNDLE

        async def start_route():
            order.append("route")

        async def orchestrate(**kwargs):
            order.append("action")
            self.assertIs(kwargs["session"], session)
            self.assertIs(kwargs["identity"], IDENTITY)
            return dict(ORCHESTRATION)

        operation = active.DessMonitorActiveCorrelationOperation()
        with patch.object(active, "login_with_password", side_effect=login), patch.object(
            active,
            "fetch_read_only_evidence_for_session",
            side_effect=fetch,
        ), patch.object(
            active,
            "async_orchestrate_dessmonitor_shadow_learning",
            side_effect=orchestrate,
        ):
            outcome = await operation.async_correlate(
                executor=_executor,
                collector_pn=IDENTITY.pn,
                username="user",
                password="password",
                fallback_identity={"pn": "must-not-win"},
                max_fields=8,
                progress=lambda *_args: None,
                orchestrator_callbacks={},
                adopt_identity=lambda identity: order.append("identity"),
                start_shadow_route=start_route,
                on_learning=lambda: order.append("learning"),
            )

        self.assertEqual(
            order,
            ["auth", "metadata", "identity", "route", "learning", "action"],
        )
        self.assertIsInstance(outcome, CloudLearningOutcome)
        self.assertEqual(outcome.identity, IDENTITY.to_record())
        self.assertEqual(outcome.result["source"], "dessmonitor")
        self.assertIs(outcome.result["metadata_only"], False)
        self.assertEqual(outcome.metadata_evidence["source"], "dessmonitor")

    async def test_auth_or_metadata_failure_opens_no_route(self) -> None:
        route = AsyncMock()
        operation = active.DessMonitorActiveCorrelationOperation()
        for failed in ("auth", "metadata"):
            with self.subTest(failed=failed):
                route.reset_mock()
                login_result = (
                    DessMonitorApiEnvelope(err=0, desc="", dat={}),
                    DessMonitorSession(token="token", secret="secret"),
                )
                with patch.object(
                    active,
                    "login_with_password",
                    side_effect=(RuntimeError("auth") if failed == "auth" else None),
                    return_value=login_result,
                ), patch.object(
                    active,
                    "fetch_read_only_evidence_for_session",
                    side_effect=RuntimeError("metadata"),
                ):
                    with self.assertRaises(RuntimeError):
                        await operation.async_correlate(
                            executor=_executor,
                            collector_pn=IDENTITY.pn,
                            username="u",
                            password="p",
                            fallback_identity=None,
                            max_fields=8,
                            progress=lambda *_args: None,
                            orchestrator_callbacks={},
                            adopt_identity=lambda _identity: None,
                            start_shadow_route=route,
                            on_learning=lambda: None,
                        )
                route.assert_not_awaited()

    async def test_foreign_bundle_is_rejected_before_route(self) -> None:
        foreign = DessMonitorEvidenceBundle(
            identity=DessMonitorDeviceIdentity(
                pn="FOREIGN000000000001",
                sn="SN2",
                devcode=2304,
                devaddr=1,
            ),
            telemetry_fields=(),
            chart_fields=(),
            key_parameters=(),
            control_fields=(),
        )
        route = AsyncMock()
        operation = active.DessMonitorActiveCorrelationOperation()
        with patch.object(
            active,
            "login_with_password",
            return_value=(
                DessMonitorApiEnvelope(err=0, desc="", dat={}),
                DessMonitorSession(token="token", secret="secret"),
            ),
        ), patch.object(
            active,
            "fetch_read_only_evidence_for_session",
            return_value=foreign,
        ):
            with self.assertRaisesRegex(ValueError, "identity_mismatch"):
                await operation.async_correlate(
                    executor=_executor,
                    collector_pn=IDENTITY.pn,
                    username="u",
                    password="p",
                    fallback_identity=None,
                    max_fields=8,
                    progress=lambda *_args: None,
                    orchestrator_callbacks={},
                    adopt_identity=lambda _identity: None,
                    start_shadow_route=route,
                    on_learning=lambda: None,
                )
        route.assert_not_awaited()

    async def test_no_safe_control_plan_is_rejected_before_route(self) -> None:
        empty = DessMonitorEvidenceBundle(
            identity=IDENTITY,
            telemetry_fields=(),
            chart_fields=(),
            key_parameters=(),
            control_fields=(
                DessMonitorControlField(
                    field_id="factory_reset",
                    title="Restore factory defaults",
                    choices=(("1", "Run"),),
                ),
            ),
        )
        route = AsyncMock()
        operation = active.DessMonitorActiveCorrelationOperation()
        with patch.object(
            active,
            "login_with_password",
            return_value=(
                DessMonitorApiEnvelope(err=0, desc="", dat={}),
                DessMonitorSession(token="token", secret="secret"),
            ),
        ), patch.object(
            active,
            "fetch_read_only_evidence_for_session",
            return_value=empty,
        ):
            with self.assertRaisesRegex(RuntimeError, "no_safe_controls"):
                await operation.async_correlate(
                    executor=_executor,
                    collector_pn=IDENTITY.pn,
                    username="u",
                    password="p",
                    fallback_identity=None,
                    max_fields=8,
                    progress=lambda *_args: None,
                    orchestrator_callbacks={},
                    adopt_identity=lambda _identity: None,
                    start_shadow_route=route,
                    on_learning=lambda: None,
                )
        route.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
