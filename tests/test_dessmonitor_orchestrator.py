from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.dessmonitor_cloud import (  # noqa: E402
    DessMonitorActionRejectedError,
    DessMonitorApiEnvelope,
    DessMonitorControlField,
    DessMonitorDeviceIdentity,
    DessMonitorSession,
)
from custom_components.eybond_local.support.shadow_learning import (  # noqa: E402
    ShadowWriteObservation,
)
from custom_components.eybond_local.support.shadow_learning.dessmonitor_orchestrator import (  # noqa: E402
    async_orchestrate_dessmonitor_shadow_learning,
    build_dessmonitor_learning_plan,
)


SESSION = DessMonitorSession(token="token", secret="secret")
IDENTITY = DessMonitorDeviceIdentity(
    pn="E50000200000000001",
    sn="SN1",
    devcode=2304,
    devaddr=1,
)
MODE = DessMonitorControlField(
    field_id="mode",
    title="Output mode",
    choices=(("0", "Current"), ("1", "Solar"), ("2", "Utility")),
    current_value="0",
)
NUMERIC = DessMonitorControlField(
    field_id="limit",
    title="Charging limit",
    current_value="55",
)
DESTRUCTIVE = DessMonitorControlField(
    field_id="factory_reset",
    title="Restore factory defaults",
    choices=(("1", "Run"),),
)
HIDDEN_DESTRUCTIVE = DessMonitorControlField(
    field_id="system_action",
    title="System action",
    choices=(("1", "Upgrade firmware"),),
)
EMPTY = DessMonitorControlField(field_id="empty", title="Unknown value")


def _observation(register: int = 100) -> ShadowWriteObservation:
    return ShadowWriteObservation(
        register=register,
        values=(1,),
        function_code=6,
        devcode=2304,
        devaddr=1,
        raw_payload_hex=f"{register:04x}",
    )


class DessMonitorPlanTests(unittest.TestCase):
    def test_plan_uses_only_declared_values_and_excludes_destructive_fields(self) -> None:
        plan = build_dessmonitor_learning_plan(
            (MODE, NUMERIC, DESTRUCTIVE, HIDDEN_DESTRUCTIVE, EMPTY),
            max_fields=40,
        )

        self.assertEqual(
            [(item["field_id"], item["value"]) for item in plan],
            [("mode", "1"), ("mode", "2"), ("limit", "55")],
        )
        self.assertTrue(all(item["action"] == "dessmonitor_ctrlDevice" for item in plan))
        self.assertNotIn("factory_reset", {item["field_id"] for item in plan})
        self.assertNotIn("system_action", {item["field_id"] for item in plan})
        self.assertNotIn("empty", {item["field_id"] for item in plan})

    def test_plan_is_bounded_by_fields_and_supports_explicit_filter(self) -> None:
        self.assertEqual(
            [item["value"] for item in build_dessmonitor_learning_plan(
                (MODE, NUMERIC), max_fields=1
            )],
            ["1", "2"],
        )
        self.assertEqual(
            [item["field_id"] for item in build_dessmonitor_learning_plan(
                (MODE, NUMERIC), field_ids=("limit",), max_fields=40
            )],
            ["limit"],
        )

    def test_plan_boundary_is_exact_and_fail_closed(self) -> None:
        for malformed in ([MODE], (object(),), None):
            with self.subTest(malformed=malformed):
                with self.assertRaises(TypeError):
                    build_dessmonitor_learning_plan(malformed)
        for malformed in ((" mode",), (object(),), "mode"):
            with self.subTest(field_ids=malformed):
                with self.assertRaises((TypeError, ValueError)):
                    build_dessmonitor_learning_plan((MODE,), field_ids=malformed)
        for malformed in (True, -1):
            with self.subTest(max_fields=malformed):
                with self.assertRaises((TypeError, ValueError)):
                    build_dessmonitor_learning_plan((MODE,), max_fields=malformed)


class DessMonitorOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, *, action, observations=None, ready=True, **overrides):
        captured = [] if observations is None else observations

        def cursor():
            return len(captured)

        def current(since):
            return tuple(captured[since:])

        kwargs = dict(
            control_fields=(MODE,),
            session=SESSION,
            identity=IDENTITY,
            confirm_cloud_write=True,
            shadow_session_state="learning",
            max_fields=1,
            all_choice_values=False,
            action=action,
            observation_cursor=cursor,
            current_observations_since=current,
            is_session_ready=lambda: ready,
            correlation_timeout_seconds=0.01,
        )
        kwargs.update(overrides)
        return await async_orchestrate_dessmonitor_shadow_learning(**kwargs)

    async def test_success_is_accepted_only_with_post_cursor_observation(self) -> None:
        observations = []
        calls = []

        def action(**kwargs):
            calls.append(kwargs)
            observations.append(_observation())
            return DessMonitorApiEnvelope(err=0, desc="ok", dat={})

        result = await self._run(action=action, observations=observations)

        self.assertEqual(result["captured_not_applied_count"], 1)
        self.assertEqual(result["leaked_count"], 0)
        self.assertEqual(result["correlation"]["matched_count"], 1)
        self.assertIs(calls[0]["session"], SESSION)
        self.assertIs(calls[0]["identity"], IDENTITY)
        self.assertEqual(calls[0]["field_id"], "mode")
        self.assertNotIn("token", result)

    async def test_provider_rejection_still_correlates_an_observed_proxy_write(self) -> None:
        observations = []

        def action(**_kwargs):
            observations.append(_observation())
            raise DessMonitorActionRejectedError(
                err=9,
                action="ctrlDevice",
                desc="rejected",
            )

        result = await self._run(action=action, observations=observations)

        self.assertEqual(result["captured_not_applied_count"], 1)
        attempt = result["results"][0]
        self.assertEqual(attempt["delivery_outcome"], "definitive_rejection")
        self.assertEqual(attempt["cloud_nack_response"]["err"], 9)

    async def test_success_without_observation_is_leaked_and_stops(self) -> None:
        calls = []

        def action(**kwargs):
            calls.append(kwargs["value"])
            return DessMonitorApiEnvelope(err=0, desc="ok", dat={})

        result = await self._run(
            action=action,
            all_choice_values=True,
        )

        self.assertEqual(result["leaked_count"], 1)
        self.assertEqual(result["executed_result_count"], 1)
        self.assertEqual(calls, ["1"])

    async def test_unready_session_sends_nothing(self) -> None:
        calls = []

        def action(**kwargs):
            calls.append(kwargs)
            return DessMonitorApiEnvelope(err=0, desc="ok", dat={})

        result = await self._run(action=action, ready=False)

        self.assertEqual(result["degraded_count"], 1)
        self.assertEqual(calls, [])

    async def test_indeterminate_transport_error_propagates_without_second_action(self) -> None:
        calls = []

        def action(**kwargs):
            calls.append(kwargs["value"])
            raise TimeoutError("transport")

        with self.assertRaises(TimeoutError):
            await self._run(action=action, all_choice_values=True)
        self.assertEqual(calls, ["1"])

    async def test_strict_runtime_boundary_rejects_ducks_before_dispatch(self) -> None:
        calls = []

        def action(**kwargs):
            calls.append(kwargs)
            return DessMonitorApiEnvelope(err=0, desc="ok", dat={})

        for key, malformed in (
            ("session", object()),
            ("identity", object()),
            ("confirm_cloud_write", 1),
            ("shadow_session_state", " ready"),
            ("wait_for_observations_since", object()),
            ("read_map_snapshot", object()),
            ("delay_seconds", True),
        ):
            with self.subTest(key=key):
                with self.assertRaises((TypeError, ValueError, RuntimeError)):
                    await self._run(action=action, **{key: malformed})
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
