from __future__ import annotations

import ast
import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.local_register_evidence import (  # noqa: E402
    LocalRegisterBlockObservation,
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
)
from custom_components.eybond_local.drivers.local_register_series import (  # noqa: E402
    DEFAULT_LOCAL_REGISTER_SERIES_INTERVAL_SECONDS,
    DEFAULT_LOCAL_REGISTER_SERIES_SAMPLE_COUNT,
    LocalRegisterSeriesPlan,
    LocalRegisterSnapshotSeries,
)
import custom_components.eybond_local.support.local_register_collection as collection_module  # noqa: E402
from custom_components.eybond_local.support.local_register_collection import (  # noqa: E402
    LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED,
    LOCAL_REGISTER_COLLECTION_FAILURE_IDENTITY_CHANGED,
    LOCAL_REGISTER_COLLECTION_STATE_CANCELLED,
    LOCAL_REGISTER_COLLECTION_STATE_COMPLETE,
    LOCAL_REGISTER_COLLECTION_STATE_FAILED,
    LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
    LocalRegisterCollectionManager,
    LocalRegisterCollectionStatus,
)
from custom_components.eybond_local.support.bundle import (  # noqa: E402
    build_support_bundle_payload,
)
from custom_components.eybond_local.support.package import (  # noqa: E402
    export_support_package,
)


FULL_PN = "E50000200000000001"
SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "support"
    / "local_register_collection.py"
)


def _snapshot(index: int, *, pn: str = FULL_PN) -> LocalRegisterSnapshot:
    second = index * 10
    plan = LocalRegisterReadPlan(
        devcode=2376,
        collector_addr=1,
        device_addr=1,
        function=3,
        start=100,
        count=1,
    )
    return LocalRegisterSnapshot(
        collector_pn=pn,
        driver_key="smg_modbus",
        started_at=f"2026-08-22T10:00:{second:02d}+00:00",
        completed_at=f"2026-08-22T10:00:{second + 1:02d}+00:00",
        planned_block_count=1,
        failed_block_count=0,
        blocks=(
            LocalRegisterBlockObservation(
                plan=plan,
                observed_at=f"2026-08-22T10:00:{second + 1:02d}+00:00",
                values=(2300 + index,),
            ),
        ),
    )


def _series(plan: LocalRegisterSeriesPlan) -> LocalRegisterSnapshotSeries:
    return LocalRegisterSnapshotSeries(
        collector_pn=FULL_PN,
        driver_key="smg_modbus",
        sample_interval_seconds=plan.sample_interval_seconds,
        snapshots=tuple(_snapshot(index) for index in range(plan.sample_count)),
    )


class LocalRegisterSeriesPlanTests(unittest.TestCase):
    def test_default_plan_matches_five_minute_history_and_roundtrips(self) -> None:
        plan = LocalRegisterSeriesPlan(
            sample_count=DEFAULT_LOCAL_REGISTER_SERIES_SAMPLE_COUNT,
            sample_interval_seconds=DEFAULT_LOCAL_REGISTER_SERIES_INTERVAL_SECONDS,
        )
        self.assertEqual(plan.sample_count, 5)
        self.assertEqual(plan.sample_interval_seconds, 300)
        self.assertEqual(plan.duration_seconds, 1200)
        self.assertEqual(LocalRegisterSeriesPlan.from_record(plan.to_record()), plan)

    def test_plan_rejects_bool_out_of_range_and_forged_duration(self) -> None:
        with self.assertRaises(TypeError):
            LocalRegisterSeriesPlan(  # type: ignore[arg-type]
                sample_count=True,
                sample_interval_seconds=300,
            )
        with self.assertRaises(ValueError):
            LocalRegisterSeriesPlan(sample_count=2, sample_interval_seconds=300)
        record = LocalRegisterSeriesPlan(3, 1).to_record()
        record["duration_seconds"] = 999
        self.assertIsNone(LocalRegisterSeriesPlan.from_record(record))


class LocalRegisterCollectionStatusTests(unittest.TestCase):
    def test_status_record_is_read_only_and_never_activation_authority(self) -> None:
        plan = LocalRegisterSeriesPlan(3, 1)
        status = LocalRegisterCollectionStatus(
            state=LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
            plan=plan,
            started_at="2026-08-22T10:00:00+00:00",
            completed_at="",
            completed_sample_count=1,
            failure_reason="",
        )
        record = status.to_record()
        self.assertIs(record["read_only"], True)
        self.assertIs(record["cloud_mapping_proven"], False)
        self.assertIs(record["activation_allowed"], False)
        self.assertIs(record["series_available"], False)

    def test_status_constructor_rejects_impossible_shapes(self) -> None:
        plan = LocalRegisterSeriesPlan(3, 1)
        with self.assertRaises(ValueError):
            LocalRegisterCollectionStatus(
                state=LOCAL_REGISTER_COLLECTION_STATE_COMPLETE,
                plan=plan,
                started_at="2026-08-22T10:00:00+00:00",
                completed_at="2026-08-22T10:01:00+00:00",
                completed_sample_count=2,
                failure_reason="",
            )
        with self.assertRaises(TypeError):
            LocalRegisterCollectionStatus(  # type: ignore[arg-type]
                state=LOCAL_REGISTER_COLLECTION_STATE_RUNNING,
                plan=object(),
                started_at="2026-08-22T10:00:00+00:00",
                completed_at="",
                completed_sample_count=0,
                failure_reason="",
            )


class LocalRegisterCollectionManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._clock_value = datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc)

    def _clock(self) -> datetime:
        self._clock_value += timedelta(seconds=1)
        return self._clock_value

    async def test_start_returns_immediately_and_retains_complete_series(self) -> None:
        plan = LocalRegisterSeriesPlan(3, 1)
        updates: list[tuple[str, int, bool]] = []
        snapshots = [_snapshot(index) for index in range(3)]

        async def capture() -> LocalRegisterSnapshot:
            return snapshots.pop(0)

        async def immediate_series(**kwargs):
            captured = [await kwargs["capture_snapshot"]() for _ in range(3)]
            return LocalRegisterSnapshotSeries(
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                sample_interval_seconds=kwargs["sample_interval_seconds"],
                snapshots=tuple(captured),
            )

        manager = LocalRegisterCollectionManager(
            capture_snapshot=capture,
            clock=self._clock,
            on_update=lambda status, series: updates.append(
                (
                    status.state,
                    status.completed_sample_count,
                    series is not None,
                )
            ),
        )
        with patch.object(
            collection_module,
            "async_capture_local_register_series",
            side_effect=immediate_series,
        ):
            started = manager.start(plan)
            self.assertEqual(started.state, LOCAL_REGISTER_COLLECTION_STATE_RUNNING)
            self.assertIsNone(manager.latest_series)
            await asyncio.sleep(0)

        self.assertEqual(manager.status.state, LOCAL_REGISTER_COLLECTION_STATE_COMPLETE)
        self.assertEqual(manager.status.completed_sample_count, 3)
        self.assertEqual(manager.latest_series, _series(plan))
        self.assertEqual(updates[0], (LOCAL_REGISTER_COLLECTION_STATE_RUNNING, 0, False))
        self.assertEqual(updates[-1], (LOCAL_REGISTER_COLLECTION_STATE_COMPLETE, 3, True))

    async def test_busy_start_refuses_without_replacing_live_task(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        async def capture() -> LocalRegisterSnapshot:
            return _snapshot(0)

        async def blocked_series(**_kwargs):
            entered.set()
            await release.wait()
            return _series(LocalRegisterSeriesPlan(3, 1))

        manager = LocalRegisterCollectionManager(
            capture_snapshot=capture,
            clock=self._clock,
        )
        with patch.object(
            collection_module,
            "async_capture_local_register_series",
            side_effect=blocked_series,
        ):
            first = manager.start(LocalRegisterSeriesPlan(3, 1))
            await entered.wait()
            with self.assertRaisesRegex(RuntimeError, "collection_busy"):
                manager.start(LocalRegisterSeriesPlan(4, 1))
            self.assertIs(manager.status, first)
            await manager.async_cancel()
        self.assertEqual(manager.status.state, LOCAL_REGISTER_COLLECTION_STATE_CANCELLED)

    async def test_cancel_before_first_scheduler_turn_closes_running_state(self) -> None:
        async def capture() -> LocalRegisterSnapshot:
            return _snapshot(0)

        manager = LocalRegisterCollectionManager(
            capture_snapshot=capture,
            clock=self._clock,
        )
        manager.start(LocalRegisterSeriesPlan(3, 1))
        await manager.async_cancel()
        self.assertEqual(manager.status.state, LOCAL_REGISTER_COLLECTION_STATE_CANCELLED)
        self.assertFalse(manager.status.active)
        self.assertIsNone(manager.latest_series)

    async def test_private_failure_is_reduced_to_closed_reason(self) -> None:
        async def capture() -> LocalRegisterSnapshot:
            raise ConnectionError("secret route 192.0.2.55")

        manager = LocalRegisterCollectionManager(
            capture_snapshot=capture,
            clock=self._clock,
        )
        manager.start(LocalRegisterSeriesPlan(3, 1))
        await asyncio.sleep(0)
        self.assertEqual(manager.status.state, LOCAL_REGISTER_COLLECTION_STATE_FAILED)
        self.assertEqual(
            manager.status.failure_reason,
            LOCAL_REGISTER_COLLECTION_FAILURE_CAPTURE_FAILED,
        )
        self.assertNotIn("192.0.2.55", str(manager.status.to_record()))

    async def test_identity_change_is_typed_and_series_is_absent(self) -> None:
        plan = LocalRegisterSeriesPlan(3, 1)

        async def capture() -> LocalRegisterSnapshot:
            return _snapshot(0)

        async def identity_failure(**_kwargs):
            raise ValueError("local_register_series_identity_changed")

        manager = LocalRegisterCollectionManager(
            capture_snapshot=capture,
            clock=self._clock,
        )
        with patch.object(
            collection_module,
            "async_capture_local_register_series",
            side_effect=identity_failure,
        ):
            manager.start(plan)
            await asyncio.sleep(0)
        self.assertEqual(manager.status.state, LOCAL_REGISTER_COLLECTION_STATE_FAILED)
        self.assertEqual(
            manager.status.failure_reason,
            LOCAL_REGISTER_COLLECTION_FAILURE_IDENTITY_CHANGED,
        )
        self.assertIsNone(manager.latest_series)

    async def test_shutdown_awaits_cancel_and_observer_failure_is_supplemental(self) -> None:
        entered = asyncio.Event()

        async def capture() -> LocalRegisterSnapshot:
            entered.set()
            await asyncio.Event().wait()
            return _snapshot(0)

        manager = LocalRegisterCollectionManager(
            capture_snapshot=capture,
            clock=self._clock,
            on_update=lambda *_args: (_ for _ in ()).throw(RuntimeError("ui failed")),
        )
        manager.start(LocalRegisterSeriesPlan(3, 1))
        await entered.wait()
        await manager.async_shutdown()
        self.assertEqual(manager.status.state, LOCAL_REGISTER_COLLECTION_STATE_CANCELLED)


class LocalRegisterCollectionArchitectureTests(unittest.TestCase):
    def test_manager_is_local_evidence_only(self) -> None:
        source = SOURCE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")
        for forbidden in (
            "dessmonitor",
            "cloud",
            "history_correlation",
            "flows",
            "runtime",
            "overlay",
        ):
            self.assertFalse(any(forbidden in item for item in imports))
        for forbidden in (
            "async_activate_device_scoped_overlay",
            "read_bindings",
            "write_capability",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn('"cloud_mapping_proven": False', source)
        self.assertIn('"activation_allowed": False', source)

    def test_completed_series_is_masked_in_explicit_support_archive(self) -> None:
        plan = LocalRegisterSeriesPlan(3, 1)
        status = LocalRegisterCollectionStatus(
            state=LOCAL_REGISTER_COLLECTION_STATE_COMPLETE,
            plan=plan,
            started_at="2026-08-22T10:00:00+00:00",
            completed_at="2026-08-22T10:01:00+00:00",
            completed_sample_count=3,
            failure_reason="",
        )
        support_bundle = build_support_bundle_payload(
            entry_id="entry-local-series",
            entry_title="Local series",
            connected=True,
            collector={"collector_pn": FULL_PN},
            inverter={"driver_key": "smg_modbus"},
            values={
                "local_register_collection": status.to_record(),
                "local_register_series_evidence": _series(plan).to_record(),
            },
            data={"collector_pn": FULL_PN},
            options={},
            profile_name="smg_modbus.json",
            register_schema_name="modbus_smg/models/smg.json",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            result = export_support_package(
                config_dir=Path(temp_dir),
                entry_id="entry-local-series",
                entry_title="Local series",
                support_bundle=support_bundle,
                raw_capture=None,
                fixture=None,
                anonymized_fixture=None,
            )
            with zipfile.ZipFile(result.path) as archive:
                archived = json.loads(
                    archive.read("support_bundle.json").decode("utf-8")
                )
        serialized = json.dumps(archived)
        self.assertIn("local_register_series_evidence", serialized)
        self.assertIn("repeated_live_local_wire_observation", serialized)
        self.assertNotIn(FULL_PN, serialized)
        self.assertIn("*", serialized)


if __name__ == "__main__":
    unittest.main()
