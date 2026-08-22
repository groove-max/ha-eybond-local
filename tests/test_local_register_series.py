from __future__ import annotations

import ast
import asyncio
import json
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.local_register_evidence import (  # noqa: E402
    LocalRegisterBlockObservation,
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
)
from custom_components.eybond_local.drivers.local_register_series import (  # noqa: E402
    LOCAL_REGISTER_SERIES_AUTHORITY,
    LOCAL_REGISTER_SERIES_TIME_BASIS,
    LocalRegisterSnapshotSeries,
    async_capture_local_register_series,
)


FULL_PN = "E50000200000000001"
SOURCE = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "drivers"
    / "local_register_series.py"
)


def _snapshot(
    index: int,
    *,
    collector_pn: str = FULL_PN,
    driver_key: str = "smg_modbus",
) -> LocalRegisterSnapshot:
    minute = index * 2
    plan = LocalRegisterReadPlan(
        devcode=2376,
        collector_addr=1,
        device_addr=1,
        function=3,
        start=100,
        count=2,
    )
    return LocalRegisterSnapshot(
        collector_pn=collector_pn,
        driver_key=driver_key,
        started_at=f"2026-08-22T10:{minute:02d}:00+00:00",
        completed_at=f"2026-08-22T10:{minute:02d}:02+00:00",
        planned_block_count=1,
        failed_block_count=0,
        blocks=(
            LocalRegisterBlockObservation(
                plan=plan,
                observed_at=f"2026-08-22T10:{minute:02d}:01+00:00",
                values=(200 + index, 300 + index),
            ),
        ),
    )


def _series() -> LocalRegisterSnapshotSeries:
    return LocalRegisterSnapshotSeries(
        collector_pn=FULL_PN,
        driver_key="smg_modbus",
        sample_interval_seconds=120,
        snapshots=tuple(_snapshot(index) for index in range(3)),
    )


class LocalRegisterSnapshotSeriesModelTests(unittest.TestCase):
    def test_roundtrip_is_json_safe_and_explicitly_unproven(self) -> None:
        original = _series()
        record = original.to_record()
        parsed = LocalRegisterSnapshotSeries.from_record(
            json.loads(json.dumps(record))
        )

        self.assertEqual(record["authority"], LOCAL_REGISTER_SERIES_AUTHORITY)
        self.assertEqual(
            record["time_basis"],
            LOCAL_REGISTER_SERIES_TIME_BASIS,
        )
        self.assertIs(record["cloud_mapping_proven"], False)
        self.assertEqual(record["snapshot_count"], 3)
        self.assertEqual(record["observed_register_count"], 6)
        self.assertEqual(parsed, original)
        self.assertEqual(parsed.to_record(), record)

    def test_constructor_rejects_short_duck_or_mixed_series(self) -> None:
        with self.assertRaises(ValueError):
            LocalRegisterSnapshotSeries(
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                sample_interval_seconds=120,
                snapshots=(_snapshot(0), _snapshot(1)),
            )
        with self.assertRaises(TypeError):
            LocalRegisterSnapshotSeries(  # type: ignore[arg-type]
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                sample_interval_seconds=True,
                snapshots=tuple(_snapshot(index) for index in range(3)),
            )
        with self.assertRaises(ValueError):
            LocalRegisterSnapshotSeries(
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                sample_interval_seconds=120,
                snapshots=(
                    _snapshot(0),
                    _snapshot(1, collector_pn="V0000000000001"),
                    _snapshot(2),
                ),
            )
        with self.assertRaises(ValueError):
            LocalRegisterSnapshotSeries(
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                sample_interval_seconds=120,
                snapshots=(
                    _snapshot(0),
                    _snapshot(1, driver_key="srne_modbus"),
                    _snapshot(2),
                ),
            )

    def test_constructor_rejects_overlapping_snapshots(self) -> None:
        first = _snapshot(0)
        plan = first.blocks[0].plan
        overlapping = LocalRegisterSnapshot(
            collector_pn=FULL_PN,
            driver_key="smg_modbus",
            started_at="2026-08-22T10:00:01+00:00",
            completed_at="2026-08-22T10:00:03+00:00",
            planned_block_count=1,
            failed_block_count=0,
            blocks=(
                LocalRegisterBlockObservation(
                    plan=plan,
                    observed_at="2026-08-22T10:00:02+00:00",
                    values=(201, 301),
                ),
            ),
        )
        with self.assertRaisesRegex(ValueError, "snapshots_overlap"):
            LocalRegisterSnapshotSeries(
                collector_pn=FULL_PN,
                driver_key="smg_modbus",
                sample_interval_seconds=120,
                snapshots=(first, overlapping, _snapshot(2)),
            )

    def test_parser_rejects_forged_authority_and_derived_counts(self) -> None:
        class _DuckAuthority:
            def __eq__(self, _other):
                return True

        for key, value in (
            ("authority", _DuckAuthority()),
            ("time_basis", "host_clock"),
            ("cloud_mapping_proven", True),
            ("snapshot_count", 99),
            ("observed_register_count", 99),
        ):
            with self.subTest(key=key):
                record = _series().to_record()
                record[key] = value
                self.assertIsNone(LocalRegisterSnapshotSeries.from_record(record))


class LocalRegisterSnapshotSeriesCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_capture_is_bounded_ordered_and_uses_exact_interval(self) -> None:
        snapshots = [_snapshot(index) for index in range(3)]
        sleeps: list[float] = []

        async def capture() -> LocalRegisterSnapshot:
            return snapshots.pop(0)

        async def sleep(delay: float) -> None:
            sleeps.append(delay)

        series = await async_capture_local_register_series(
            capture_snapshot=capture,
            sample_count=3,
            sample_interval_seconds=120,
            sleep=sleep,
        )

        self.assertEqual(series.snapshot_count, 3)
        self.assertEqual(sleeps, [120.0, 120.0])
        self.assertEqual(series.collector_pn, FULL_PN)
        self.assertEqual(series.driver_key, "smg_modbus")

    async def test_capture_fails_immediately_on_identity_change(self) -> None:
        snapshots = [
            _snapshot(0),
            _snapshot(1, collector_pn="V0000000000001"),
            _snapshot(2),
        ]
        calls = 0

        async def capture() -> LocalRegisterSnapshot:
            nonlocal calls
            calls += 1
            return snapshots.pop(0)

        async def sleep(_delay: float) -> None:
            return None

        with self.assertRaisesRegex(ValueError, "identity_changed"):
            await async_capture_local_register_series(
                capture_snapshot=capture,
                sample_count=3,
                sample_interval_seconds=1,
                sleep=sleep,
            )
        self.assertEqual(calls, 2)

    async def test_cancellation_propagates_without_partial_series(self) -> None:
        calls = 0

        async def capture() -> LocalRegisterSnapshot:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise asyncio.CancelledError
            return _snapshot(calls - 1)

        async def sleep(_delay: float) -> None:
            return None

        with self.assertRaises(asyncio.CancelledError):
            await async_capture_local_register_series(
                capture_snapshot=capture,
                sample_count=3,
                sample_interval_seconds=1,
                sleep=sleep,
            )
        self.assertEqual(calls, 2)


class LocalRegisterSnapshotSeriesArchitectureTests(unittest.TestCase):
    def test_series_stays_driver_owned_and_cloud_neutral(self) -> None:
        source = SOURCE.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")

        for forbidden in (
            "cloud",
            "dessmonitor",
            "runtime",
            "flows",
            "support",
            "read_learning_binder",
            "overlay_generator",
        ):
            self.assertFalse(any(forbidden in item for item in imports))
        self.assertIn("repeated_live_local_wire_observation", source)
        self.assertIn('"cloud_mapping_proven": False', source)


if __name__ == "__main__":
    unittest.main()
