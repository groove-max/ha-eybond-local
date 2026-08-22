from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.drivers.local_register_evidence import (  # noqa: E402
    LocalRegisterBlockObservation,
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
    async_capture_local_register_snapshot,
    async_read_modbus_plan,
)
from custom_components.eybond_local.models import ProbeTarget  # noqa: E402
from custom_components.eybond_local.payload.modbus import (  # noqa: E402
    ModbusSession,
    crc16_modbus,
    decode_read_request,
)


PN = "E50000200000000001"
T0 = "2026-08-22T10:00:00+00:00"
T1 = "2026-08-22T10:00:01+00:00"
T2 = "2026-08-22T10:00:02+00:00"
T3 = "2026-08-22T10:00:03+00:00"


def _plan(*, start: int = 300, function: int = 3) -> LocalRegisterReadPlan:
    return LocalRegisterReadPlan(
        devcode=2376,
        collector_addr=1,
        device_addr=1,
        function=function,
        start=start,
        count=2,
    )


class LocalRegisterModelTests(unittest.TestCase):
    def test_plan_from_exact_target_preserves_route_and_address_spaces(self) -> None:
        target = ProbeTarget(devcode=2376, collector_addr=5, device_addr=1)
        holding = LocalRegisterReadPlan.for_target(
            target,
            function=3,
            start=300,
            count=2,
        )
        input_plan = LocalRegisterReadPlan.for_target(
            target,
            function=4,
            start=300,
            count=2,
        )

        self.assertEqual(holding.probe_target, target)
        self.assertNotEqual(holding, input_plan)
        self.assertEqual(holding.to_record()["collector_addr"], 5)

    def test_direct_constructors_reject_coercions_and_malformed_time(self) -> None:
        plan_fields = {
            "devcode": 2376,
            "collector_addr": 1,
            "device_addr": 1,
            "function": 3,
            "start": 300,
            "count": 2,
        }
        for field, value in (
            ("devcode", True),
            ("collector_addr", 256),
            ("device_addr", "1"),
            ("function", 6),
            ("start", -1),
            ("count", 126),
        ):
            with self.subTest(field=field):
                with self.assertRaises((TypeError, ValueError)):
                    LocalRegisterReadPlan(**(plan_fields | {field: value}))
        with self.assertRaises(TypeError):
            LocalRegisterReadPlan.for_target(  # type: ignore[arg-type]
                SimpleNamespace(devcode=2376, collector_addr=1, device_addr=1),
                function=3,
                start=300,
                count=2,
            )
        for timestamp in ("", "2026-08-22", "2026-08-22T10:00:00", " " + T1):
            with self.subTest(timestamp=timestamp):
                with self.assertRaises(ValueError):
                    LocalRegisterBlockObservation(
                        plan=_plan(),
                        observed_at=timestamp,
                        values=(1, 2),
                    )
        with self.assertRaises(TypeError):
            LocalRegisterBlockObservation(
                plan=_plan(),
                observed_at=T1,
                values=[1, 2],  # type: ignore[arg-type]
            )
        with self.assertRaises(ValueError):
            LocalRegisterBlockObservation(
                plan=_plan(),
                observed_at=T1,
                values=(1,),
            )
        with self.assertRaises(TypeError):
            LocalRegisterBlockObservation(
                plan=_plan(),
                observed_at=T1,
                values=(1, True),
            )

    def test_snapshot_roundtrip_and_forged_authority_fail_closed(self) -> None:
        snapshot = LocalRegisterSnapshot(
            collector_pn=PN,
            driver_key="smg",
            started_at=T0,
            completed_at=T2,
            planned_block_count=1,
            failed_block_count=0,
            blocks=(
                LocalRegisterBlockObservation(
                    plan=_plan(),
                    observed_at=T1,
                    values=(2305, 500),
                ),
            ),
        )
        record = snapshot.to_record()
        parsed = LocalRegisterSnapshot.from_record(json.loads(json.dumps(record)))

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.to_record(), record)
        self.assertEqual(parsed.observed_register_count, 2)
        self.assertEqual(record["authority"], "live_local_wire_observation")
        self.assertIs(record["cloud_mapping_proven"], False)

        for field, value in (
            ("schema_version", True),
            ("authority", "cloud_register_binding"),
            ("cloud_mapping_proven", True),
            ("observed_register_count", 99),
        ):
            malformed = dict(record)
            malformed[field] = value
            with self.subTest(field=field):
                self.assertIsNone(LocalRegisterSnapshot.from_record(malformed))

    def test_snapshot_rejects_count_duplicate_and_time_invariants(self) -> None:
        block = LocalRegisterBlockObservation(
            plan=_plan(), observed_at=T1, values=(1, 2)
        )
        base = {
            "collector_pn": PN,
            "driver_key": "smg",
            "started_at": T0,
            "completed_at": T2,
            "planned_block_count": 1,
            "failed_block_count": 0,
            "blocks": (block,),
        }
        invalid = (
            {"collector_pn": " " + PN},
            {"collector_pn": "bad\x03pn"},
            {"collector_pn": SimpleNamespace()},
            {"driver_key": " smg"},
            {"completed_at": "2026-08-22T09:59:59+00:00"},
            {"planned_block_count": True},
            {"failed_block_count": 1},
            {"planned_block_count": 2, "blocks": (block, block)},
        )
        for override in invalid:
            with self.subTest(override=override):
                with self.assertRaises((TypeError, ValueError)):
                    LocalRegisterSnapshot(**(base | override))


class LocalRegisterCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_modbus_reader_uses_the_exact_plan_route_and_function(self) -> None:
        calls: list[tuple[object, object]] = []

        class _Transport:
            async def async_send_payload(self, payload: bytes, *, route):
                request = decode_read_request(payload)
                calls.append((request, route))
                assert request is not None
                response = bytearray(
                    [
                        request.slave_id,
                        request.function_code,
                        request.count * 2,
                        0x09,
                        0x01,
                        0x01,
                        0xF4,
                    ]
                )
                response.extend(crc16_modbus(response).to_bytes(2, "little"))
                return bytes(response)

        transport = _Transport()
        plan = LocalRegisterReadPlan(
            devcode=2376,
            collector_addr=5,
            device_addr=1,
            function=4,
            start=300,
            count=2,
        )

        values = await async_read_modbus_plan(
            plan,
            session_factory=lambda target: ModbusSession(
                transport,
                route=target.link_route,
                slave_id=target.payload_address,
            ),
        )

        self.assertEqual(values, [2305, 500])
        self.assertEqual(len(calls), 1)
        request, route = calls[0]
        self.assertEqual(request.function_code, 4)
        self.assertEqual(request.address, 300)
        self.assertEqual(request.count, 2)
        self.assertEqual(route, plan.probe_target.link_route)

    async def test_executor_records_successes_failures_and_exact_times(self) -> None:
        plans = (_plan(start=300), _plan(start=400, function=4))
        timestamps = iter((T0, T1, T2))
        calls: list[LocalRegisterReadPlan] = []

        async def reader(plan: LocalRegisterReadPlan) -> list[int]:
            calls.append(plan)
            if plan.start == 400:
                raise ConnectionError("private wire detail")
            return [2305, 500]

        snapshot = await async_capture_local_register_snapshot(
            collector_pn=PN,
            driver_key="smg",
            plans=plans,
            reader=reader,
            timestamp_provider=lambda: next(timestamps),
        )

        self.assertEqual(calls, list(plans))
        self.assertEqual(snapshot.planned_block_count, 2)
        self.assertEqual(snapshot.failed_block_count, 1)
        self.assertEqual(len(snapshot.blocks), 1)
        self.assertEqual(snapshot.blocks[0].observed_at, T1)
        self.assertEqual(snapshot.completed_at, T2)
        self.assertNotIn("private wire detail", str(snapshot.to_record()))

    async def test_cancellation_propagates_without_partial_snapshot(self) -> None:
        entered = asyncio.Event()
        never = asyncio.Event()

        async def reader(_plan: LocalRegisterReadPlan) -> list[int]:
            entered.set()
            await never.wait()
            return [1, 2]

        task = asyncio.create_task(
            async_capture_local_register_snapshot(
                collector_pn=PN,
                driver_key="smg",
                plans=(_plan(),),
                reader=reader,
            )
        )
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_malformed_reader_output_and_clock_fail_closed(self) -> None:
        async def duck_reader(_plan):
            return (1, 2)

        with self.assertRaises(TypeError):
            await async_capture_local_register_snapshot(
                collector_pn=PN,
                driver_key="smg",
                plans=(_plan(),),
                reader=duck_reader,
            )

        async def reader(_plan):
            return [1, 2]

        with self.assertRaises(ValueError):
            await async_capture_local_register_snapshot(
                collector_pn=PN,
                driver_key="smg",
                plans=(_plan(),),
                reader=reader,
                timestamp_provider=lambda: "2026-08-22T10:00:00",
            )


if __name__ == "__main__":
    unittest.main()
