"""Quiet MPPT poll instrumentation: fail classes, skips, not_admitted."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from test_eybond_short_ascii import _Transport, _responses
from test_short_ascii_mppt import runtime_frame
from test_short_ascii_optional import _rb, _rh
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.drivers.short_ascii_mppt_optional import (
    ADMIT_OPTION_KEY, RUNTIME_QUERY_0200, classify_mppt_fail,
)
from custom_components.eybond_local.drivers.short_ascii_optional import STATE_KEY
from custom_components.eybond_local.metadata.register_schema_loader import (
    clear_register_schema_loader_cache, load_register_schema,
)
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget
from custom_components.eybond_local.payload.short_ascii import ShortAsciiError


QUIET_MPPT_KEYS = (
    "mppt_poll_attempts",
    "mppt_poll_ok",
    "mppt_poll_fail",
    "mppt_fail_reason",
    "mppt_fail_timeout",
    "mppt_fail_connection",
    "mppt_fail_decode",
    "mppt_retry_recovered",
    "mppt_not_admitted",
    "mppt_consecutive_failures",
    "mppt_due_this_cycle",
    "mppt_skipped_prefer_fc4",
    "mppt_forced_anti_starve",
    "mppt_last_success_age_s",
    "aux_fence_reason",
    "aux_last_error",
    "short_ascii_optional_status",
)


class ClassifyFailTests(unittest.TestCase):
    def test_fail_classes(self):
        self.assertEqual(classify_mppt_fail(asyncio.TimeoutError()), "timeout")
        self.assertEqual(classify_mppt_fail(ConnectionError("fence")), "connection")
        self.assertEqual(classify_mppt_fail(ValueError("bad")), "decode")
        self.assertEqual(classify_mppt_fail(ShortAsciiError("x")), "decode")
        self.assertEqual(classify_mppt_fail(TypeError("x")), "decode")
        self.assertEqual(
            classify_mppt_fail(TypeError("unsupported_auxiliary_transport:X")),
            "structural",
        )


class QuietSchemaTests(unittest.TestCase):
    def setUp(self):
        clear_register_schema_loader_cache()
        self.schema = load_register_schema(EybondShortAsciiDriver().register_schema_name)

    def test_quiet_mppt_instrumentation_default_off(self):
        for key in QUIET_MPPT_KEYS:
            with self.subTest(key=key):
                description = self.schema.measurement_description(key)
                self.assertTrue(description.diagnostic)
                self.assertFalse(description.enabled_default)
        aux = self.schema.binary_sensor_description("aux_connected")
        self.assertTrue(aux.diagnostic)
        self.assertFalse(aux.enabled_default)


class MpptInstrumentationDriverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.driver = EybondShortAsciiDriver()
        responses = _responses() | {
            "RB": _rb(), "F": b"#115.0 105 48.00 60.0\r", "RH": _rh(accuracy=1),
        }
        self.transport = _Transport(
            responses, aux_responses={RUNTIME_QUERY_0200: runtime_frame().wire},
        )
        self.inverter = await self.driver.async_probe(
            self.transport, ProbeTarget(767, 255, 1),
        )
        self.state = {ADMIT_OPTION_KEY: True}
        self.transport.requests.clear()
        self.transport.aux_requests.clear()

    async def read(self, now):
        return await self.driver.async_read_values(
            self.transport, self.inverter, runtime_state=self.state, now_monotonic=now,
        )

    async def _prime_fc4(self):
        await self.read(0)
        await self.read(1)
        await self.read(2)
        # Anti-starve may force MPPT during prime; normalize so later cases own
        # selection / counters / due flags.
        diag = self.state[STATE_KEY].mppt_diag
        diag.prefer_fc4_skip_streak = 0
        diag.forced_anti_starve = 0
        diag.skipped_prefer_fc4 = 0
        diag.poll_attempts = 0
        diag.poll_ok = 0
        diag.poll_fail = 0
        diag.fail_timeout = 0
        diag.fail_connection = 0
        diag.fail_decode = 0
        diag.retry_recovered = 0
        diag.consecutive_failures = 0
        diag.fail_reason = ""
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        mppt.next_due = 0
        self.transport.aux_requests.clear()

    def _hold_fc4(self):
        for sample in self.state[STATE_KEY].samples:
            if sample.command != "MPPT":
                sample.next_due = 10_000

    async def test_not_admitted_sets_reason_without_poll_attempts(self):
        self.state.pop(ADMIT_OPTION_KEY, None)
        result = await self.read(0)
        self.assertEqual(self.transport.aux_requests, [])
        self.assertEqual(result.diagnostics["mppt_fail_reason"], "not_admitted")
        self.assertEqual(result.diagnostics["mppt_poll_attempts"], 0)
        self.assertEqual(result.diagnostics["mppt_poll_fail"], 0)
        self.assertEqual(result.diagnostics["mppt_not_admitted"], 1)
        self.assertIn("MPPT=not_admitted", result.diagnostics["short_ascii_optional_status"])
        await self.read(1)
        self.assertEqual(self.state[STATE_KEY].mppt_diag.poll_attempts, 0)
        self.assertEqual(self.state[STATE_KEY].mppt_diag.not_admitted_cycles, 2)

    async def test_timeout_increments_fail_class_and_consecutive(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        result = await self.read(3)
        self.assertEqual(result.diagnostics["mppt_poll_attempts"], 1)
        self.assertEqual(result.diagnostics["mppt_poll_fail"], 1)
        self.assertEqual(result.diagnostics["mppt_poll_ok"], 0)
        self.assertEqual(result.diagnostics["mppt_fail_reason"], "timeout")
        self.assertEqual(result.diagnostics["mppt_fail_timeout"], 1)
        self.assertEqual(result.diagnostics["mppt_consecutive_failures"], 1)
        self.assertEqual(result.diagnostics["mppt_due_this_cycle"], 1)
        # In-cycle retry: still one attempt cycle, two wire sends.
        self.assertEqual(len(self.transport.aux_requests), 2)
        second = await self.read(4)
        self.assertEqual(second.diagnostics["mppt_poll_attempts"], 2)
        self.assertEqual(second.diagnostics["mppt_poll_fail"], 2)
        self.assertEqual(second.diagnostics["mppt_consecutive_failures"], 2)
        self.assertEqual(second.diagnostics["mppt_fail_timeout"], 2)

    async def test_connection_fail_class_distinct_from_timeout(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = ConnectionError(
            "collector_session_changed",
        )
        result = await self.read(3)
        self.assertEqual(result.diagnostics["mppt_fail_reason"], "connection")
        self.assertEqual(result.diagnostics["mppt_fail_connection"], 1)
        self.assertEqual(result.diagnostics["mppt_fail_timeout"], 0)
        self.assertEqual(result.diagnostics["aux_fence_reason"], "ConnectionError")
        self.assertIn("ConnectionError", result.diagnostics["aux_last_error"])

    async def test_decode_fail_class(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = b"not-an-aabb-frame"
        result = await self.read(3)
        self.assertEqual(result.diagnostics["mppt_fail_reason"], "decode")
        self.assertEqual(result.diagnostics["mppt_fail_decode"], 1)
        self.assertEqual(result.diagnostics["mppt_poll_fail"], 1)

    async def test_ok_resets_consecutive_and_retry_recovered(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = [
            asyncio.TimeoutError(),
            runtime_frame().wire,
        ]
        result = await self.read(3)
        self.assertEqual(result.values["pv_power"], 370)
        self.assertEqual(result.diagnostics["mppt_poll_attempts"], 1)
        self.assertEqual(result.diagnostics["mppt_poll_ok"], 1)
        self.assertEqual(result.diagnostics["mppt_poll_fail"], 0)
        self.assertEqual(result.diagnostics["mppt_retry_recovered"], 1)
        self.assertEqual(result.diagnostics["mppt_consecutive_failures"], 0)
        self.assertEqual(result.diagnostics["mppt_fail_reason"], "")
        self.assertEqual(result.diagnostics["mppt_last_success_age_s"], 0.0)

    async def test_consecutive_resets_after_prior_fails(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        await self.read(3)
        await self.read(4)
        self.assertEqual(self.state[STATE_KEY].mppt_diag.consecutive_failures, 2)
        self.transport.aux_responses[RUNTIME_QUERY_0200] = runtime_frame().wire
        recovered = await self.read(5)
        self.assertEqual(recovered.diagnostics["mppt_consecutive_failures"], 0)
        self.assertEqual(recovered.diagnostics["mppt_poll_ok"], 1)
        self.assertEqual(recovered.diagnostics["mppt_poll_fail"], 2)

    async def test_prefer_fc4_skip_counted_without_attempt(self):
        await self._prime_fc4()
        diag = self.state[STATE_KEY].mppt_diag
        # Fresh MPPT: prefer-FC4 remains default (anti-starve must not fire).
        diag.last_success_at = 2.0
        diag.reset_prefer_fc4_streak()
        before = diag.skipped_prefer_fc4
        attempts_before = diag.poll_attempts
        for sample in self.state[STATE_KEY].samples:
            sample.next_due = 3
        self.transport.requests.clear()
        self.transport.aux_requests.clear()
        result = await self.read(3)
        self.assertIn(b"RB\x01\r", self.transport.requests)
        self.assertEqual(self.transport.aux_requests, [])
        self.assertEqual(result.diagnostics["mppt_skipped_prefer_fc4"], before + 1)
        self.assertEqual(result.diagnostics["mppt_forced_anti_starve"], 0)
        self.assertEqual(result.diagnostics["mppt_due_this_cycle"], 1)
        self.assertEqual(result.diagnostics["mppt_poll_attempts"], attempts_before)

    async def test_last_success_age_advances(self):
        await self._prime_fc4()
        self._hold_fc4()
        await self.read(3)
        # Hold MPPT off the schedule so age grows without a new poll.
        mppt = next(s for s in self.state[STATE_KEY].samples if s.command == "MPPT")
        mppt.next_due = 10_000
        aged = await self.read(13)
        self.assertEqual(aged.diagnostics["mppt_poll_ok"], 1)
        self.assertAlmostEqual(aged.diagnostics["mppt_last_success_age_s"], 10.0, delta=1.0)

    async def test_fail_stamps_collector_disconnect(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.collector_info = CollectorInfo(
            collector_pn="I30000200000000001",
            disconnect_count=7,
            last_disconnect_reason="collector_eof",
        )
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        result = await self.read(3)
        self.assertEqual(result.diagnostics["mppt_fail_disconnect_count"], 7)
        self.assertEqual(result.diagnostics["mppt_fail_disconnect_reason"], "collector_eof")
        self.assertIs(result.diagnostics["aux_connected"], True)

    async def test_disconnect_fence_marks_aux_not_connected(self):
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        self.transport.connected = False
        result = await self.read(3)
        self.assertIs(result.diagnostics["aux_connected"], False)
        self.assertEqual(result.diagnostics["aux_fence_reason"], "disconnected")
        self.assertEqual(result.diagnostics["mppt_fail_reason"], "timeout")

    async def test_mppt_diag_survives_optional_clear_and_republishes(self):
        """Q1 wipe clears samples; counters/mppt_diag survive and still republish."""
        await self._prime_fc4()
        self._hold_fc4()
        self.transport.aux_responses[RUNTIME_QUERY_0200] = asyncio.TimeoutError()
        failed = await self.read(3)
        self.assertEqual(failed.diagnostics["mppt_poll_attempts"], 1)
        self.assertEqual(failed.diagnostics["mppt_poll_fail"], 1)
        self.assertEqual(failed.diagnostics["mppt_fail_timeout"], 1)
        self.assertIn("MPPT=timeout", failed.diagnostics["short_ascii_optional_status"])
        diag_before = self.state[STATE_KEY].mppt_diag

        # Mandatory Q1 miss → optional.clear() (sample wipe). Diag must remain.
        self.transport.responses["Q1"] = asyncio.TimeoutError()
        with self.assertRaises(asyncio.TimeoutError):
            await self.read(4)
        self.assertTrue(all(not sample.values for sample in self.state[STATE_KEY].samples))
        self.assertIs(self.state[STATE_KEY].mppt_diag, diag_before)
        self.assertEqual(diag_before.poll_attempts, 1)
        self.assertEqual(diag_before.poll_fail, 1)
        self.assertEqual(diag_before.fail_timeout, 1)

        # Next successful FULL cycle republishes surviving counters + status.
        self.transport.responses["Q1"] = _responses()["Q1"]
        republished = await self.read(5)
        # clear() makes MPPT due again; without a framing-kill poison the next
        # cycle polls once more. The counter object itself is not wiped.
        self.assertGreaterEqual(republished.diagnostics["mppt_poll_attempts"], 1)
        self.assertGreaterEqual(republished.diagnostics["mppt_poll_fail"], 1)
        self.assertEqual(republished.diagnostics["mppt_fail_reason"], "timeout")
        self.assertIn("short_ascii_optional_status", republished.diagnostics)
        self.assertIn("MPPT=", republished.diagnostics["short_ascii_optional_status"])


if __name__ == "__main__":
    unittest.main()
