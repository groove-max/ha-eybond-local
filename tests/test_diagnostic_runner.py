from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.support.diagnostic_runner import (
    DiagnosticRuntimeContext,
    DiagnosticSingleFlight,
    run_scenario,
)


TARGET = ProbeTarget(devcode=1, collector_addr=255, device_addr=4)


class CountingTransport:
    """Wrap a FixtureTransport, counting transport ops and optionally delaying."""

    def __init__(self, inner: FixtureTransport, *, delay: float = 0.0) -> None:
        self.inner = inner
        self.calls = 0
        self.delay = delay

    @property
    def connected(self) -> bool:
        return True

    async def async_send_payload(self, payload: bytes, *, route) -> bytes:
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return await self.inner.async_send_payload(payload, route=route)


class PlainAsciiTransport:
    """Small transport for no-CRC ASCII-line diagnostic commands."""

    def __init__(self, responses: dict[str, str]) -> None:
        self.responses = dict(responses)
        self.calls = 0
        self.routes: list[object] = []

    @property
    def connected(self) -> bool:
        return True

    async def async_send_payload(self, payload: bytes, *, route) -> bytes:
        self.calls += 1
        self.routes.append(route)
        if not payload.endswith(b"\r"):
            raise RuntimeError("missing_terminator")
        command = payload[:-1].decode("ascii")
        if command not in self.responses:
            raise RuntimeError(f"missing_command:{command}")
        return f"({self.responses[command]}".encode("ascii") + b"\r"


class TimeoutAwarePlainAsciiTransport(PlainAsciiTransport):
    """Plain ASCII transport that records per-request timeout overrides."""

    def __init__(self, responses: dict[str, str]) -> None:
        super().__init__(responses)
        self.request_timeouts: list[float | None] = []

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route,
        request_timeout: float | None = None,
    ) -> bytes:
        self.request_timeouts.append(request_timeout)
        return await super().async_send_payload(payload, route=route)


def _modbus(registers: dict[int, int], **kwargs) -> CountingTransport:
    inner = FixtureTransport(registers=registers, command_responses=None, probe_target=TARGET)
    return CountingTransport(inner, **kwargs)


def _pi(responses: dict[str, str]) -> CountingTransport:
    command_responses = {(TARGET.devcode, TARGET.collector_addr, k): v for k, v in responses.items()}
    inner = FixtureTransport(registers=None, command_responses=command_responses, probe_target=TARGET)
    return CountingTransport(inner)


def _ctx(transport, **kwargs) -> DiagnosticRuntimeContext:
    base = dict(
        transport=transport,
        active_driver_key="modbus_smg",
        active_probe_target=TARGET,
        entry_id="entry-1",
        integration_version="0.2.0-test",
        # Most execution tests intentionally exercise writes; the write-gate is
        # covered explicitly by WriteConfirmationTests.
        confirm_write=True,
    )
    base.update(kwargs)
    return DiagnosticRuntimeContext(**base)


class DriverResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_active_runtime_driver_is_default(self) -> None:
        transport = _modbus({171: 100})
        result = await run_scenario("read 171\n", _ctx(transport))
        self.assertTrue(result.success)
        self.assertEqual(result.context["selected_driver_key"], "modbus_smg")
        self.assertEqual(result.context["driver_source"], "active_runtime")

    async def test_scenario_override_beats_active_runtime(self) -> None:
        transport = _pi({"QPI": "PI30"})
        result = await run_scenario("driver pi30\nascii QPI\n", _ctx(transport))
        self.assertTrue(result.success)
        self.assertEqual(result.context["selected_driver_key"], "pi30")
        self.assertEqual(result.context["driver_source"], "scenario_override")

    async def test_configured_hint_used_when_no_runtime(self) -> None:
        transport = _modbus({10: 1})
        ctx = _ctx(transport, active_driver_key=None, configured_driver_hint="modbus_smg")
        result = await run_scenario("read 10\n", ctx)
        self.assertTrue(result.success)
        self.assertEqual(result.context["driver_source"], "configured_hint")

    async def test_missing_driver_is_rejected(self) -> None:
        transport = _modbus({})
        ctx = _ctx(transport, active_driver_key=None, configured_driver_hint="auto")
        result = await run_scenario("read 10\n", ctx)
        self.assertFalse(result.success)
        self.assertIn("driver is required", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_unknown_driver_is_rejected(self) -> None:
        transport = _modbus({})
        result = await run_scenario("driver nope\nread 10\n", _ctx(transport))
        self.assertFalse(result.success)
        self.assertEqual(transport.calls, 0)


class TargetResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_address_overrides_apply(self) -> None:
        transport = _modbus({10: 1})
        result = await run_scenario(
            "devcode 1\ncollector_addr 0xFF\ndevice_addr 4\nread 10\n", _ctx(transport)
        )
        self.assertTrue(result.success)
        self.assertEqual(
            result.context["probe_target"],
            {"devcode": 1, "collector_addr": 255, "device_addr": 4},
        )

    async def test_runs_without_detected_inverter_via_driver_fallback(self) -> None:
        transport = _modbus({10: 7})
        ctx = _ctx(
            transport,
            active_driver_key=None,
            active_probe_target=None,
            configured_driver_hint="modbus_smg",
            driver_default_probe_target=lambda key: TARGET,
        )
        result = await run_scenario("read 10\n", ctx)
        self.assertTrue(result.success)
        self.assertEqual(result.results[0]["response"]["decimal"], [7])

    async def test_missing_route_field_is_rejected(self) -> None:
        transport = _modbus({10: 1})
        ctx = _ctx(
            transport,
            active_driver_key="modbus_smg",
            active_probe_target=None,
            driver_default_probe_target=lambda key: None,
        )
        result = await run_scenario("read 10\n", ctx)
        self.assertFalse(result.success)
        self.assertIn("devcode is required", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_route_wire_limits_are_rejected_during_preflight(self) -> None:
        cases = (
            ("devcode 65536\nread 10\n", "line 1: devcode"),
            ("collector_addr 256\nread 10\n", "line 1: collector_addr"),
            ("device_addr 256\nread 10\n", "line 1: device_addr"),
        )
        for script, expected in cases:
            with self.subTest(script=script):
                transport = _modbus({10: 1})
                result = await run_scenario(script, _ctx(transport))
                self.assertFalse(result.success)
                self.assertIn(expected, result.output)
                self.assertEqual(transport.calls, 0)


class CommandSupportTests(unittest.IsolatedAsyncioTestCase):
    async def test_ascii_unsupported_on_modbus_driver(self) -> None:
        transport = _modbus({})
        result = await run_scenario("ascii QPI\n", _ctx(transport))
        self.assertFalse(result.success)
        self.assertIn("command 'ascii' is not supported by driver 'modbus_smg'", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_write_bit_unsupported_on_pi_driver(self) -> None:
        transport = _pi({})
        result = await run_scenario("driver pi30\nwrite_bit 1 0 1\n", _ctx(transport))
        self.assertFalse(result.success)
        self.assertIn("command 'write_bit' is not supported by driver 'pi30'", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_parse_error_runs_no_transport_op(self) -> None:
        transport = _modbus({})
        result = await run_scenario("read 10\nreads 5\n", _ctx(transport))
        self.assertFalse(result.success)
        self.assertIn("line 2", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_modbus_wire_limits_are_rejected_during_preflight(self) -> None:
        cases = (
            ("read 0 126\n", "read count must not exceed 125"),
            (
                "write 0 " + " ".join(["1"] * 124) + "\n",
                "write count must not exceed 123",
            ),
            ("read 65535 2\n", "read range exceeds register 65535"),
            ("write 65535 1 2\n", "write range exceeds register 65535"),
        )
        for script, expected in cases:
            with self.subTest(expected=expected):
                transport = _modbus({10: 1})
                result = await run_scenario(script, _ctx(transport))
                self.assertFalse(result.success)
                self.assertIn("line 1", result.output)
                self.assertIn(expected, result.output)
                self.assertEqual(transport.calls, 0)


class WriteConfirmationTests(unittest.IsolatedAsyncioTestCase):
    async def test_write_rejected_without_confirm_write(self) -> None:
        transport = _modbus({354: 0})
        result = await run_scenario("write 354 1\n", _ctx(transport, confirm_write=False))
        self.assertFalse(result.success)
        self.assertIn("confirm_write=true", result.output)
        self.assertEqual(transport.calls, 0)  # rejected at preflight, no write

    async def test_write_bit_rejected_without_confirm_write(self) -> None:
        transport = _modbus({354: 1})
        result = await run_scenario("write_bit 354 0 1\n", _ctx(transport, confirm_write=False))
        self.assertFalse(result.success)
        self.assertIn("confirm_write=true", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_read_only_scenario_allowed_without_confirm_write(self) -> None:
        transport = _modbus({171: 7})
        result = await run_scenario("read 171\n", _ctx(transport, confirm_write=False))
        self.assertTrue(result.success)

    async def test_write_allowed_with_confirm_write(self) -> None:
        transport = _modbus({354: 0})
        result = await run_scenario("write 354 1\n", _ctx(transport, confirm_write=True))
        self.assertTrue(result.success)
        self.assertEqual(transport.inner._registers[354], 1)


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_sequential_execution_sees_prior_write(self) -> None:
        transport = _modbus({354: 0})
        result = await run_scenario("read 354\nwrite 354 5\nread 354\n", _ctx(transport))
        self.assertTrue(result.success)
        self.assertEqual([s["kind"] for s in result.results],
                         ["modbus_read", "modbus_write", "modbus_read"])
        self.assertEqual(result.results[0]["response"]["decimal"], [0])
        self.assertEqual(result.results[2]["response"]["decimal"], [5])

    async def test_write_single_does_no_pre_read_or_readback(self) -> None:
        transport = _modbus({354: 0})
        result = await run_scenario("write 354 1\n", _ctx(transport))
        self.assertTrue(result.success)
        # Exactly one transport op: the write. No read-before, no read-after.
        self.assertEqual(transport.calls, 1)

    async def test_write_multiple_registers(self) -> None:
        transport = _modbus({100: 0, 101: 0})
        result = await run_scenario("write 100 1 2\n", _ctx(transport))
        self.assertTrue(result.success)
        self.assertEqual(transport.inner._registers[100], 1)
        self.assertEqual(transport.inner._registers[101], 2)

    async def test_write_bit_does_pre_read_and_write_only(self) -> None:
        transport = _modbus({354: 0xABCE})
        result = await run_scenario("write_bit 354 0 1\n", _ctx(transport))
        self.assertTrue(result.success)
        # Pre-read + write == 2 ops. No automatic post-read.
        self.assertEqual(transport.calls, 2)
        response = result.results[0]["response"]
        self.assertEqual(response["before"]["hex"], "0xABCE")
        self.assertEqual(response["written"]["hex"], "0xABCF")

    async def test_stop_on_error_true_halts(self) -> None:
        transport = _modbus({10: 1})
        # Reading register 999 fails (missing); the second read must not run.
        result = await run_scenario("read 999\nread 10\n", _ctx(transport))
        self.assertFalse(result.success)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0]["status"], "error")

    async def test_stop_on_error_false_continues(self) -> None:
        transport = _modbus({10: 1})
        result = await run_scenario(
            "stop_on_error false\nread 999\nread 10\n", _ctx(transport)
        )
        self.assertFalse(result.success)  # an error occurred
        self.assertEqual(len(result.results), 2)
        self.assertEqual(result.results[0]["status"], "error")
        self.assertEqual(result.results[1]["status"], "ok")

    async def test_operation_timeout_shortens(self) -> None:
        transport = _modbus({10: 1}, delay=0.2)
        result = await run_scenario("operation_timeout 0.01\nread 10\n", _ctx(transport))
        self.assertFalse(result.success)
        self.assertEqual(result.results[0]["status"], "error")
        self.assertEqual(result.results[0]["error"], "operation_timeout")

    async def test_sleep_is_not_subject_to_operation_timeout(self) -> None:
        transport = _modbus({10: 1})
        result = await run_scenario(
            "operation_timeout 0.01\nsleep 50\nread 10\n", _ctx(transport)
        )
        self.assertTrue(result.success)
        self.assertEqual(result.results[0]["kind"], "sleep")
        self.assertEqual(result.results[1]["status"], "ok")

    async def test_not_connected_is_reported(self) -> None:
        transport = _modbus({10: 1})
        ctx = _ctx(transport, is_connected=lambda: False)
        result = await run_scenario("read 10\n", ctx)
        self.assertFalse(result.success)
        self.assertIn("collector_not_connected", result.output)
        self.assertEqual(transport.calls, 0)

    async def test_ascii_payload_is_returned(self) -> None:
        transport = _pi({"QPIGS": "1 2 3"})
        result = await run_scenario("driver pi30\nascii QPIGS\n", _ctx(transport))
        self.assertTrue(result.success)
        self.assertEqual(result.results[0]["response"]["payload"], "1 2 3")

    async def test_eybond_g_ascii_payload_is_returned(self) -> None:
        transport = PlainAsciiTransport({"GPDAT0": "B 0 5 4003 0 00"})
        result = await run_scenario(
            "driver eybond_g_ascii\nascii GPDAT0\n",
            _ctx(transport),
        )
        self.assertTrue(result.success)
        self.assertEqual(result.results[0]["request"]["command"], "GPDAT0")
        self.assertEqual(result.results[0]["response"]["payload"], "B 0 5 4003 0 00")

    async def test_eybond_g_ascii_uses_plain_ascii_payload_family(self) -> None:
        class SelectingPlainAsciiTransport(PlainAsciiTransport):
            def __init__(self, responses: dict[str, str]) -> None:
                super().__init__(responses)
                self.payload_families: list[str] = []

            def select_payload_route(self, route, *, payload_family: str = ""):
                self.payload_families.append(payload_family)
                return route

        transport = SelectingPlainAsciiTransport({"GPDAT0": "B 0 5 4003 0 00"})
        result = await run_scenario(
            "driver eybond_g_ascii\nascii GPDAT0\n",
            _ctx(transport),
        )
        self.assertTrue(result.success)
        self.assertEqual(transport.payload_families, ["eybond_g_ascii"])

    async def test_ascii_operation_timeout_is_forwarded_to_timeout_aware_transport(self) -> None:
        transport = TimeoutAwarePlainAsciiTransport({"GMOD": "B"})
        result = await run_scenario(
            "driver eybond_g_ascii\noperation_timeout 10\nascii GMOD\n",
            _ctx(transport),
        )

        self.assertTrue(result.success)
        self.assertEqual(transport.request_timeouts, [10.0])

    async def test_cancellation_propagates(self) -> None:
        transport = _modbus({10: 1}, delay=5.0)
        task = asyncio.ensure_future(run_scenario("read 10\n", _ctx(transport)))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class SingleFlightTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_concurrent_run(self) -> None:
        flight = DiagnosticSingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow() -> str:
            started.set()
            await release.wait()
            return "done"

        task = asyncio.ensure_future(flight.run(slow))
        await started.wait()
        self.assertTrue(flight.running)
        with self.assertRaises(RuntimeError):
            await flight.run(slow)
        release.set()
        self.assertEqual(await task, "done")
        self.assertFalse(flight.running)

    async def test_custom_busy_error(self) -> None:
        flight = DiagnosticSingleFlight(busy_error="support_package_export_in_progress")
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow() -> str:
            started.set()
            await release.wait()
            return "done"

        task = asyncio.ensure_future(flight.run(slow))
        await started.wait()
        with self.assertRaisesRegex(RuntimeError, "support_package_export_in_progress"):
            await flight.run(slow)
        release.set()
        self.assertEqual(await task, "done")

    async def test_on_start_and_finish_callbacks(self) -> None:
        flight = DiagnosticSingleFlight()
        states: list[str] = []

        async def work() -> int:
            states.append("run")
            return 1

        await flight.run(
            work,
            on_start=lambda: states.append("start"),
            on_finish=lambda: states.append("finish"),
        )
        self.assertEqual(states, ["start", "run", "finish"])

    async def test_cancel_aborts_and_releases_lock(self) -> None:
        flight = DiagnosticSingleFlight()
        started = asyncio.Event()
        finished: list[bool] = []

        async def hang() -> None:
            started.set()
            await asyncio.sleep(10)

        run_task = asyncio.ensure_future(
            flight.run(hang, on_finish=lambda: finished.append(True))
        )
        await started.wait()
        self.assertTrue(flight.running)
        await flight.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await run_task
        self.assertFalse(flight.running)  # lock released
        self.assertEqual(finished, [True])  # on_finish ran during teardown


if __name__ == "__main__":
    unittest.main()
