from __future__ import annotations

import asyncio
from pathlib import Path
import time
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.smartess_cloud import (
    SessionCredentials,
    SmartEssCloudError,
    build_learn_settings_plan,
)
from custom_components.eybond_local.support.shadow_learning import ShadowWriteObservation
from custom_components.eybond_local.support.shadow_learning_orchestrator import (
    _safe_read_map,
    async_orchestrate_shadow_learning_settings,
    correlate_cloud_attempts_with_shadow_writes,
    orchestrate_shadow_learning_settings,
)


class ShadowLearningOrchestratorTests(unittest.TestCase):
    def test_build_plan_is_deterministic_with_manual_enum_sweep_and_numeric_opt_in(self) -> None:
        settings_dat = {
            "field": [
                {
                    "id": "sys_eybond_ctrl_53",
                    "name": "Backlight Control",
                    "item": [
                        {"key": "0", "val": "Off"},
                        {"key": "1", "val": "On"},
                    ],
                },
                {
                    "id": "bat_eybond_ctrl_76",
                    "name": "Max Charging Current",
                    "hint": "1~120",
                },
            ]
        }

        without_numeric = build_learn_settings_plan(
            settings_dat,
            field_ids=["sys_eybond_ctrl_53", "bat_eybond_ctrl_76"],
            include_numeric=False,
            all_choice_values=True,
            max_fields=0,
        )
        with_numeric = build_learn_settings_plan(
            settings_dat,
            field_ids=["sys_eybond_ctrl_53", "bat_eybond_ctrl_76"],
            include_numeric=True,
            numeric_value="5",
            all_choice_values=True,
            max_fields=0,
        )

        self.assertEqual(
            without_numeric,
            [
                {
                    "field_id": "sys_eybond_ctrl_53",
                    "title": "Backlight Control",
                    "value": "0",
                    "value_label": "Off",
                    "value_source": "choice",
                },
                {
                    "field_id": "sys_eybond_ctrl_53",
                    "title": "Backlight Control",
                    "value": "1",
                    "value_label": "On",
                    "value_source": "choice",
                },
            ],
        )
        self.assertEqual(with_numeric[-1]["field_id"], "bat_eybond_ctrl_76")
        self.assertEqual(with_numeric[-1]["value"], "1")
        self.assertEqual(with_numeric[-1]["value_source"], "hint_min")
        self.assertEqual(with_numeric, build_learn_settings_plan(
            settings_dat,
            field_ids=["sys_eybond_ctrl_53", "bat_eybond_ctrl_76"],
            include_numeric=True,
            numeric_value="5",
            all_choice_values=True,
            max_fields=0,
        ))

    def test_orchestrator_dry_run_does_not_call_ctrl_device(self) -> None:
        settings_dat = {
            "field": [
                {
                    "id": "sys_eybond_ctrl_53",
                    "name": "Backlight Control",
                    "item": [{"key": "0", "val": "Off"}],
                }
            ]
        }

        def _failing_fetch(**_kwargs):
            raise AssertionError("fetch should not be called in dry-run")

        result = orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=True,
            confirm_cloud_write=False,
            shadow_session_ready=False,
            field_ids=[],
            include_numeric=False,
            fetch_action=_failing_fetch,
        )

        self.assertEqual(result["planned_write_count"], 1)
        self.assertEqual(result["sent_count"], 0)
        self.assertEqual(result["error_count"], 0)
        self.assertEqual(result["results"][0]["status"], "planned")

    def test_live_orchestrator_requires_confirm_and_ready_shadow_session(self) -> None:
        settings_dat = {
            "field": [
                {
                    "id": "sys_eybond_ctrl_53",
                    "name": "Backlight Control",
                    "item": [{"key": "0", "val": "Off"}],
                }
            ]
        }
        session = SessionCredentials(token="token", secret="secret")

        with self.assertRaisesRegex(SmartEssCloudError, "requires_confirm_cloud_write"):
            orchestrate_shadow_learning_settings(
                settings_dat=settings_dat,
                session=session,
                pn="E50000200000000001",
                sn="E50000200000000001000001",
                devcode=2376,
                devaddr=1,
                dry_run=False,
                confirm_cloud_write=False,
                shadow_session_ready=True,
                field_ids=[],
                include_numeric=False,
            )

        with self.assertRaisesRegex(RuntimeError, "shadow_learning_session_not_ready"):
            orchestrate_shadow_learning_settings(
                settings_dat=settings_dat,
                session=session,
                pn="E50000200000000001",
                sn="E50000200000000001000001",
                devcode=2376,
                devaddr=1,
                dry_run=False,
                confirm_cloud_write=True,
                shadow_session_ready=False,
                field_ids=[],
                include_numeric=False,
            )

    def test_live_orchestrator_executes_when_shadow_session_is_ready(self) -> None:
        settings_dat = {
            "field": [
                {
                    "id": "sys_eybond_ctrl_53",
                    "name": "Backlight Control",
                    "item": [{"key": "0", "val": "Off"}],
                }
            ]
        }

        def _ok_fetch(**_kwargs):
            return type("_Envelope", (), {"err": 0, "desc": "ok", "dat": {}})()

        result = orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_ready=True,
            field_ids=[],
            include_numeric=False,
            # Simulates a non-NACK cloud response to exercise execution/correlation; in real
            # observe-only runs a success response is a leak, covered by a dedicated test.
            abort_on_unproxied_write=False,
            fetch_action=_ok_fetch,
        )

        self.assertEqual(result["planned_write_count"], 1)
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(result["error_count"], 0)

    def test_live_orchestrator_marks_proxy_nack_with_observation_as_captured(self) -> None:
        settings_dat = {
            "field": [
                {
                    "id": "sys_eybond_ctrl_53",
                    "name": "Backlight Control",
                    "item": [{"key": "0", "val": "Off"}],
                }
            ]
        }

        def _nacking_fetch(**_kwargs):
            raise RuntimeError("action_failed:1:ERR_FAIL(Read-Only Register)")

        result = orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_ready=True,
            field_ids=[],
            include_numeric=False,
            observed_writes=[
                ShadowWriteObservation(
                    register=305,
                    values=(0,),
                    function_code=16,
                    devcode=2376,
                    devaddr=1,
                    raw_payload_hex="nacked-write",
                    timestamp="2999-06-05T12:00:00.050000+00:00",
                )
            ],
            fetch_action=_nacking_fetch,
        )

        self.assertEqual(result["results"][0]["status"], "captured_not_applied")
        self.assertEqual(result["captured_not_applied_count"], 1)
        self.assertEqual(result["error_count"], 0)
        self.assertEqual(result["correlation"]["matched_count"], 1)
        self.assertEqual(result["results"][0]["observation"]["raw_payload_hex"], "nacked-write")

    def test_live_orchestrator_treats_cloud_success_with_proxy_observation_as_captured(self) -> None:
        settings_dat = {
            "field": [
                {
                    "id": "sys_eybond_ctrl_53",
                    "name": "Backlight Control",
                    "item": [{"key": "0", "val": "Off"}],
                }
            ]
        }

        def _success_fetch(**_kwargs):
            return type("_Envelope", (), {"err": 0, "desc": "ERR_NONE", "dat": {}})()

        result = orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_ready=True,
            field_ids=[],
            include_numeric=False,
            observed_writes=[
                ShadowWriteObservation(
                    register=305,
                    values=(0,),
                    function_code=16,
                    devcode=2376,
                    devaddr=1,
                    raw_payload_hex="cloud-success-nacked-write",
                    timestamp="2999-06-05T12:00:00.050000+00:00",
                )
            ],
            fetch_action=_success_fetch,
        )

        self.assertEqual(result["results"][0]["status"], "captured_not_applied")
        self.assertEqual(result["captured_not_applied_count"], 1)
        self.assertEqual(result["leaked_count"], 0)
        self.assertTrue(result["results"][0]["cloud_ack_after_proxy_nack"])
        self.assertEqual(
            result["results"][0]["observation"]["raw_payload_hex"],
            "cloud-success-nacked-write",
        )

    def test_correlation_matches_by_sequence_and_timestamps(self) -> None:
        attempts = [
            {
                "sequence_index": 0,
                "requested_at": "2026-06-05T12:00:01+00:00",
                "field_id": "sys_eybond_ctrl_53",
                "field_name": "Backlight Control",
                "requested_value": "0",
                "status": "sent",
                "unknown_field": False,
            },
            {
                "sequence_index": 1,
                "requested_at": "2026-06-05T12:00:02+00:00",
                "field_id": "sys_unknown_cloud_field",
                "field_name": "Unknown",
                "requested_value": "1",
                "status": "sent",
                "unknown_field": True,
            },
        ]
        observations = [
            ShadowWriteObservation(
                register=305,
                values=(0,),
                function_code=16,
                devcode=2376,
                devaddr=1,
                raw_payload_hex="0110013100010200000000",
                timestamp="2026-06-05T12:00:01.200000+00:00",
            )
        ]

        correlation = correlate_cloud_attempts_with_shadow_writes(
            attempts=attempts,
            observed_writes=observations,
        )

        self.assertEqual(correlation["matched_count"], 1)
        self.assertEqual(correlation["matched"][0]["field_id"], "sys_eybond_ctrl_53")
        self.assertEqual(correlation["unmatched_attempt_count"], 1)
        self.assertEqual(correlation["unmatched_attempts"][0]["field_id"], "sys_unknown_cloud_field")
        self.assertEqual(correlation["unknown_field_attempt_count"], 1)

    def test_correlation_does_not_match_when_all_remaining_writes_are_before_request(self) -> None:
        attempts = [
            {
                "sequence_index": 0,
                "requested_at": "2026-06-05T12:00:02+00:00",
                "field_id": "sys_eybond_ctrl_53",
                "field_name": "Backlight Control",
                "requested_value": "0",
                "status": "sent",
                "unknown_field": False,
            }
        ]
        observations = [
            ShadowWriteObservation(
                register=305,
                values=(0,),
                function_code=16,
                devcode=2376,
                devaddr=1,
                raw_payload_hex="0110013100010200000000",
                timestamp="2026-06-05T12:00:01.200000+00:00",
            )
        ]

        correlation = correlate_cloud_attempts_with_shadow_writes(
            attempts=attempts,
            observed_writes=observations,
        )

        self.assertEqual(correlation["matched_count"], 0)
        self.assertEqual(correlation["unmatched_attempt_count"], 1)
        self.assertEqual(correlation["unmatched_attempts"][0]["reason"], "no_observed_write")
        self.assertEqual(correlation["unmatched_write_count"], 1)


if __name__ == "__main__":
    unittest.main()


class _FakeObservationSource:
    def __init__(self) -> None:
        self._observations: list[ShadowWriteObservation] = []

    def observation_cursor(self) -> int:
        return len(self._observations)

    def observations_since(self, cursor: int) -> tuple[ShadowWriteObservation, ...]:
        start = max(0, int(cursor))
        return tuple(self._observations[start:])

    async def wait_for_observations_since(self, cursor: int, timeout_seconds: float) -> tuple[ShadowWriteObservation, ...]:
        deadline = asyncio.get_running_loop().time() + max(float(timeout_seconds), 0.0)
        start = max(0, int(cursor))
        while asyncio.get_running_loop().time() < deadline:
            if start < len(self._observations):
                return tuple(self._observations[start:])
            await asyncio.sleep(0.01)
        return ()

    def add(self, observation: ShadowWriteObservation) -> None:
        self._observations.append(observation)

    @property
    def all(self) -> tuple[ShadowWriteObservation, ...]:
        return tuple(self._observations)


class ShadowLearningAsyncOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_orchestrator_keeps_event_loop_progress_during_cloud_fetch(self) -> None:
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [{"key": "0", "val": "Off"}]},
            ]
        }
        source = _FakeObservationSource()
        ticks = {"count": 0}
        running = {"value": True}

        async def _ticker() -> None:
            while running["value"]:
                ticks["count"] += 1
                await asyncio.sleep(0.01)

        def _fetch(**_kwargs):
            time.sleep(0.12)
            return type("_Envelope", (), {"err": 0, "desc": "ok", "dat": {}})()

        ticker_task = asyncio.create_task(_ticker())
        try:
            await async_orchestrate_shadow_learning_settings(
                settings_dat=settings_dat,
                session=SessionCredentials(token="token", secret="secret"),
                pn="E50000200000000001",
                sn="E50000200000000001000001",
                devcode=2376,
                devaddr=1,
                dry_run=False,
                confirm_cloud_write=True,
                shadow_session_state="ready",
                field_ids=[],
                include_numeric=False,
                observed_writes=source.all,
                observation_cursor=source.observation_cursor,
                current_observations_since=source.observations_since,
                wait_for_observations_since=source.wait_for_observations_since,
                is_session_ready=lambda: True,
                correlation_timeout_seconds=0.05,
                fetch_action=_fetch,
            )
        finally:
            running["value"] = False
            await ticker_task

        self.assertGreater(ticks["count"], 3)

    async def test_async_correlation_uses_post_attempt_cursor_and_skips_stale(self) -> None:
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [{"key": "0", "val": "Off"}]},
            ]
        }
        source = _FakeObservationSource()
        source.add(
            ShadowWriteObservation(
                register=305,
                values=(1,),
                function_code=16,
                devcode=2376,
                devaddr=1,
                raw_payload_hex="stale",
                timestamp="2026-06-05T11:59:59+00:00",
            )
        )
        loop = asyncio.get_running_loop()

        def _fetch(**_kwargs):
            loop.call_soon_threadsafe(
                source.add,
                ShadowWriteObservation(
                    register=305,
                    values=(0,),
                    function_code=16,
                    devcode=2376,
                    devaddr=1,
                    raw_payload_hex="first",
                    timestamp="2026-06-05T12:00:00.050000+00:00",
                ),
            )
            loop.call_soon_threadsafe(
                source.add,
                ShadowWriteObservation(
                    register=306,
                    values=(1,),
                    function_code=16,
                    devcode=2376,
                    devaddr=1,
                    raw_payload_hex="second",
                    timestamp="2026-06-05T12:00:00.060000+00:00",
                ),
            )
            return type("_Envelope", (), {"err": 0, "desc": "ok", "dat": {}})()

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            observed_writes=source.all,
            observation_cursor=source.observation_cursor,
            current_observations_since=source.observations_since,
            wait_for_observations_since=source.wait_for_observations_since,
            is_session_ready=lambda: True,
            correlation_timeout_seconds=0.3,
            abort_on_unproxied_write=False,  # simulated success exercises correlation, not safety
            fetch_action=_fetch,
        )

        self.assertEqual(result["correlation"]["matched_count"], 1)
        self.assertEqual(result["results"][0]["match_mode"], "post_attempt_cursor")
        self.assertEqual(result["results"][0]["observation"]["raw_payload_hex"], "first")
        self.assertEqual(result["results"][0]["observation_count"], 2)
        self.assertEqual(result["correlation"]["unmatched_write_count"], 1)
        self.assertEqual(result["correlation"]["unmatched_writes"][0]["raw_payload_hex"], "second")

    async def test_async_correlation_attaches_observation_when_cloud_rejects_write(self) -> None:
        # Observe-only (exception) mode: ctrlDevice raises an ERR_FAIL NACK, but
        # the Modbus write is still delivered and observed, so it must correlate.
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [{"key": "0", "val": "Off"}]},
            ]
        }
        source = _FakeObservationSource()
        loop = asyncio.get_running_loop()

        def _nacking_fetch(**_kwargs):
            loop.call_soon_threadsafe(
                source.add,
                ShadowWriteObservation(
                    register=305,
                    values=(0,),
                    function_code=16,
                    devcode=2376,
                    devaddr=1,
                    raw_payload_hex="nacked-write",
                    timestamp="2026-06-05T12:00:00.050000+00:00",
                ),
            )
            raise RuntimeError("action_failed:1:ERR_FAIL(Read-Only Register)")

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            observed_writes=source.all,
            observation_cursor=source.observation_cursor,
            current_observations_since=source.observations_since,
            wait_for_observations_since=source.wait_for_observations_since,
            is_session_ready=lambda: True,
            correlation_timeout_seconds=0.3,
            continue_on_error=True,
            fetch_action=_nacking_fetch,
        )

        self.assertEqual(result["results"][0]["status"], "captured_not_applied")
        self.assertEqual(result["captured_not_applied_count"], 1)
        self.assertEqual(result["error_count"], 0)
        self.assertIn("ERR_FAIL", result["results"][0]["cloud_nack"])
        self.assertNotIn("error", result["results"][0])
        self.assertEqual(result["correlation"]["matched_count"], 1)
        self.assertEqual(result["results"][0]["observation"]["raw_payload_hex"], "nacked-write")
        self.assertEqual(result["correlation"]["unmatched_write_count"], 0)

    async def test_async_cloud_success_with_proxy_observation_is_not_leak(self) -> None:
        # Some SmartESS server paths report success even when our proxy observed and NACKed the
        # write locally. The local observation is the safety signal: it proves the write reached
        # the fail-closed proxy instead of bypassing it to the real inverter.
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [{"key": "0", "val": "Off"}]},
            ]
        }
        source = _FakeObservationSource()
        loop = asyncio.get_running_loop()

        def _success_fetch(**_kwargs):
            loop.call_soon_threadsafe(
                source.add,
                ShadowWriteObservation(
                    register=305,
                    values=(0,),
                    function_code=16,
                    devcode=2376,
                    devaddr=1,
                    raw_payload_hex="cloud-success-nacked-write",
                    timestamp="2026-06-05T12:00:00.050000+00:00",
                ),
            )
            return type("_Envelope", (), {"err": 0, "desc": "ERR_NONE", "dat": {}})()

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            observed_writes=source.all,
            observation_cursor=source.observation_cursor,
            current_observations_since=source.observations_since,
            wait_for_observations_since=source.wait_for_observations_since,
            is_session_ready=lambda: True,
            correlation_timeout_seconds=0.3,
            fetch_action=_success_fetch,
        )

        self.assertEqual(result["results"][0]["status"], "captured_not_applied")
        self.assertEqual(result["captured_not_applied_count"], 1)
        self.assertEqual(result["leaked_count"], 0)
        self.assertTrue(result["results"][0]["cloud_ack_after_proxy_nack"])
        self.assertEqual(result["correlation"]["matched_count"], 1)
        self.assertEqual(
            result["results"][0]["observation"]["raw_payload_hex"],
            "cloud-success-nacked-write",
        )

    async def test_async_hard_aborts_on_unproxied_write_success(self) -> None:
        # SAFETY-CRITICAL: in observe-only mode every write must be observed locally by the
        # proxy. A cloud success with no local observation proves the write bypassed our proxy
        # and may have reached the real inverter. The run must hard-stop on the FIRST such write.
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [
                    {"key": "0", "val": "Off"}, {"key": "1", "val": "On"},
                ]},
                {"id": "sys_eybond_ctrl_54", "name": "Other", "item": [{"key": "0", "val": "Z"}]},
            ]
        }
        source = _FakeObservationSource()
        calls = {"value": 0}

        def _leaking_fetch(**_kwargs):
            calls["value"] += 1
            # Cloud accepts the write (no NACK) -> it was delivered to the real inverter.
            return type("_Envelope", (), {"err": 0, "desc": "ERR_NONE", "dat": {}})()

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            all_choice_values=True,
            observed_writes=source.all,
            observation_cursor=source.observation_cursor,
            current_observations_since=source.observations_since,
            wait_for_observations_since=source.wait_for_observations_since,
            is_session_ready=lambda: True,
            correlation_timeout_seconds=0.2,
            fetch_action=_leaking_fetch,
        )

        # Aborted after exactly one write; the leak is flagged and nothing else was attempted.
        self.assertEqual(calls["value"], 1)
        self.assertEqual(result["leaked_count"], 1)
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["status"], "leaked")
        self.assertEqual(result["results"][0]["reason"], "control_leaked_unproxied")

    async def test_on_progress_is_called_once_per_planned_write(self) -> None:
        settings_dat = {
            "field": [
                {"id": "f1", "name": "F1", "item": [{"key": "0", "val": "A"}]},
                {"id": "f2", "name": "F2", "item": [{"key": "0", "val": "B"}]},
            ]
        }
        calls: list[tuple[int, int]] = []
        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=True,
            confirm_cloud_write=False,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            on_progress=lambda done, total: calls.append((done, total)),
        )
        self.assertEqual(result["planned_write_count"], 2)
        self.assertEqual(calls, [(0, 2), (1, 2)])

    async def test_on_progress_errors_do_not_break_the_run(self) -> None:
        settings_dat = {
            "field": [
                {"id": "f1", "name": "F1", "item": [{"key": "0", "val": "A"}]},
            ]
        }

        def _boom(done: int, total: int) -> None:
            raise RuntimeError("progress sink failed")

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=True,
            confirm_cloud_write=False,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            on_progress=_boom,
        )
        self.assertEqual(result["planned_write_count"], 1)

    async def test_async_correlation_times_out_without_new_write(self) -> None:
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [{"key": "0", "val": "Off"}]},
            ]
        }
        source = _FakeObservationSource()

        def _fetch(**_kwargs):
            return type("_Envelope", (), {"err": 0, "desc": "ok", "dat": {}})()

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            observed_writes=source.all,
            observation_cursor=source.observation_cursor,
            current_observations_since=source.observations_since,
            wait_for_observations_since=source.wait_for_observations_since,
            is_session_ready=lambda: True,
            correlation_timeout_seconds=0.08,
            abort_on_unproxied_write=False,  # simulated success exercises correlation, not safety
            fetch_action=_fetch,
        )

        self.assertEqual(result["correlation"]["unmatched_attempt_count"], 1)
        self.assertEqual(result["correlation"]["unmatched_attempts"][0]["reason"], "timeout_no_observed_write")

    async def test_async_correlation_marks_degraded_when_session_drops(self) -> None:
        settings_dat = {
            "field": [
                {"id": "sys_eybond_ctrl_53", "name": "Backlight", "item": [{"key": "0", "val": "Off"}]},
            ]
        }
        source = _FakeObservationSource()
        call_count = {"value": 0}

        def _fetch(**_kwargs):
            call_count["value"] += 1
            if call_count["value"] == 1:
                pass
            return type("_Envelope", (), {"err": 0, "desc": "ok", "dat": {}})()

        result = await async_orchestrate_shadow_learning_settings(
            settings_dat=settings_dat,
            session=SessionCredentials(token="token", secret="secret"),
            pn="E50000200000000001",
            sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            dry_run=False,
            confirm_cloud_write=True,
            shadow_session_state="ready",
            field_ids=[],
            include_numeric=False,
            observed_writes=source.all,
            observation_cursor=source.observation_cursor,
            current_observations_since=source.observations_since,
            wait_for_observations_since=source.wait_for_observations_since,
            is_session_ready=lambda: call_count["value"] == 0,
            correlation_timeout_seconds=0.2,
            abort_on_unproxied_write=False,  # simulated success exercises degraded path, not safety
            fetch_action=_fetch,
        )

        self.assertGreaterEqual(result["correlation"]["degraded_attempt_count"], 1)
        self.assertIn(
            result["correlation"]["degraded_attempts"][0]["reason"],
            {"session_not_ready", "session_degraded_during_run"},
        )

class SafeReadMapTests(unittest.TestCase):
    def test_none_snapshot_yields_empty(self) -> None:
        self.assertEqual(_safe_read_map(None), {})

    def test_raising_snapshot_yields_empty(self) -> None:
        def _boom() -> dict:
            raise RuntimeError("backend gone")

        self.assertEqual(_safe_read_map(_boom), {})

    def test_non_dict_snapshot_yields_empty(self) -> None:
        self.assertEqual(_safe_read_map(lambda: "nope"), {})

    def test_valid_snapshot_passes_through(self) -> None:
        payload = {"read_blocks": [[200, 22, 1]], "registers": {"205": [2305]}}
        self.assertEqual(_safe_read_map(lambda: payload), payload)
