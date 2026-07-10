from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.onboarding.link_sweep import (  # noqa: E402
    async_run_link_baud_sweep,
    catalog_link_baud_hints,
    is_silent_detection_error,
    parse_reported_baud,
)


@dataclass
class _Scan:
    candidates: tuple = ()


class StructuredSilenceTests(unittest.IsolatedAsyncioTestCase):
    def test_sweep_no_match_carries_silent_verdict(self) -> None:
        from custom_components.eybond_local.onboarding.driver_detection import (
            DriverSweepNoMatch,
        )

        silent_exc = DriverSweepNoMatch("pi18:probe_timeout", silent=True)
        self.assertIsInstance(silent_exc, RuntimeError)
        self.assertTrue(silent_exc.silent)

        # A hypothetical future error whose STRING would fool a suffix check
        # still carries the tracked verdict.
        tricky = DriverSweepNoMatch("answered_then_probe_timeout", silent=False)
        self.assertFalse(tricky.silent)

    async def test_response_tracking_transport_observes_payloads(self) -> None:
        from custom_components.eybond_local.onboarding.driver_detection import (
            _ResponseTrackingTransport,
        )

        class Inner:
            async def async_send_forward(self, payload, **kwargs):
                return b"\x01\x03\x02\x00\x2a"

            async def async_send_collector(self, **kwargs):
                return (7, b"payload")

            async def async_send_payload(self, payload, **kwargs):
                return b""

        tracked = _ResponseTrackingTransport(Inner())
        self.assertFalse(tracked.saw_response)
        await tracked.async_send_forward(b"req")
        self.assertEqual(tracked.responses, 1)
        await tracked.async_send_collector()
        self.assertEqual(tracked.responses, 2)
        # Empty payloads are not responses.
        await tracked.async_send_payload(b"req")
        self.assertEqual(tracked.responses, 2)

    async def test_answering_garbage_device_is_not_silent(self) -> None:
        # A device that RESPONDS with out-of-envelope registers must not be
        # classified as silence even though every driver returns no-match
        # (drivers swallow their own errors and return None).
        from custom_components.eybond_local.fixtures.transport import FixtureTransport
        from custom_components.eybond_local.models import ProbeTarget
        from custom_components.eybond_local.onboarding.driver_detection import (
            DriverSweepNoMatch,
            async_detect_inverter_candidates,
        )

        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        garbage_inputs = {register: 0xFEFE for register in range(0, 512)}
        transport = FixtureTransport(
            registers={},
            input_registers=garbage_inputs,
            command_responses=None,
            probe_target=target,
        )
        with self.assertRaises(DriverSweepNoMatch) as ctx:
            await async_detect_inverter_candidates(transport, driver_hint="modbus_catalog")
        self.assertFalse(ctx.exception.silent)
        outcomes = {entry["outcome"] for entry in ctx.exception.probe_log}
        self.assertIn("no_match", outcomes)

    async def test_fully_dead_link_is_silent(self) -> None:
        from custom_components.eybond_local.fixtures.transport import FixtureTransport
        from custom_components.eybond_local.models import ProbeTarget
        from custom_components.eybond_local.onboarding.driver_detection import (
            DriverSweepNoMatch,
            async_detect_inverter_candidates,
        )

        target = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
        transport = FixtureTransport(
            registers={},
            input_registers={},
            command_responses=None,
            probe_target=target,
        )
        with self.assertRaises(DriverSweepNoMatch) as ctx:
            await async_detect_inverter_candidates(transport, driver_hint="modbus_catalog")
        self.assertTrue(ctx.exception.silent)


class SilenceClassifierTests(unittest.TestCase):
    def test_link_down_is_silence(self) -> None:
        self.assertTrue(is_silent_detection_error("inverter_link_down"))

    def test_trailing_probe_timeout_is_silence(self) -> None:
        self.assertTrue(is_silent_detection_error("pi18:probe_timeout"))

    def test_answered_but_unmatched_is_not_silence(self) -> None:
        self.assertFalse(is_silent_detection_error("no_supported_driver_matched"))
        self.assertFalse(is_silent_detection_error("srne_modbus:error:crc_mismatch"))
        self.assertFalse(is_silent_detection_error(""))


class CatalogHintTests(unittest.TestCase):
    def test_driver_keys_for_link_baud_restrict_the_resweep(self) -> None:
        from custom_components.eybond_local.onboarding.link_sweep import (
            driver_keys_for_link_baud,
        )

        self.assertEqual(driver_keys_for_link_baud(2400), ("pi30",))
        self.assertEqual(driver_keys_for_link_baud(19200), ("must_pv_ph18",))
        at_9600 = driver_keys_for_link_baud(9600)
        self.assertIn("modbus_smg", at_9600)
        self.assertIn("srne_modbus", at_9600)
        self.assertIn("modbus_catalog", at_9600)
        self.assertNotIn("pi30", at_9600)
        self.assertEqual(driver_keys_for_link_baud(115200), ())

    def test_hints_are_distinct_and_sorted(self) -> None:
        hints = catalog_link_baud_hints()
        self.assertEqual(hints, tuple(sorted(set(hints))))
        self.assertIn(2400, hints)   # pi30
        self.assertIn(9600, hints)   # smg / srne / aohai
        self.assertIn(19200, hints)  # must


class ParseReportedBaudTests(unittest.TestCase):
    def test_parses_bare_and_framed_values(self) -> None:
        self.assertEqual(parse_reported_baud("9600"), 9600)
        self.assertEqual(parse_reported_baud("115200,8,1,NONE"), 115200)
        self.assertIsNone(parse_reported_baud(""))
        self.assertIsNone(parse_reported_baud("NONE,8"))
        self.assertIsNone(parse_reported_baud("0"))


class BaudSweepTests(unittest.IsolatedAsyncioTestCase):
    async def test_keeps_matching_baud_and_reports_it(self) -> None:
        set_calls: list[int] = []

        async def read_baud():
            return 115200

        async def set_baud(baud):
            set_calls.append(baud)
            return True

        async def run_sweep(baud):
            return _Scan(candidates=("ctx",)) if baud == 9600 else None

        outcome = await async_run_link_baud_sweep(
            candidate_bauds=(2400, 9600, 19200),
            read_baud=read_baud,
            set_baud=set_baud,
            run_sweep=run_sweep,
        )

        self.assertTrue(outcome.matched)
        self.assertEqual(outcome.matched_baud, 9600)
        self.assertEqual(outcome.original_baud, 115200)
        self.assertEqual(outcome.attempted_bauds, (2400, 9600))
        self.assertFalse(outcome.restored)
        # No restore after a match: the matching speed must stay.
        self.assertEqual(set_calls, [2400, 9600])

    async def test_restores_original_when_nothing_matches(self) -> None:
        set_calls: list[int] = []

        async def read_baud():
            return 2400

        async def set_baud(baud):
            set_calls.append(baud)
            return True

        async def run_sweep(baud):
            return None

        outcome = await async_run_link_baud_sweep(
            candidate_bauds=(2400, 9600, 19200),
            read_baud=read_baud,
            set_baud=set_baud,
            run_sweep=run_sweep,
        )

        self.assertFalse(outcome.matched)
        # 2400 skipped (already current), then 9600/19200 tried, then restore.
        self.assertEqual(set_calls, [9600, 19200, 2400])
        self.assertTrue(outcome.restored)
        self.assertEqual(outcome.attempted_bauds, (9600, 19200))

    async def test_rejected_set_is_skipped_without_sweep(self) -> None:
        sweeps: list[int] = []
        set_calls: list[int] = []

        async def read_baud():
            return 115200

        async def set_baud(baud):
            set_calls.append(baud)
            return baud != 9600  # collector rejects 9600

        async def run_sweep(baud):
            sweeps.append(baud)
            return None

        outcome = await async_run_link_baud_sweep(
            candidate_bauds=(9600, 19200),
            read_baud=read_baud,
            set_baud=set_baud,
            run_sweep=run_sweep,
        )

        self.assertEqual(sweeps, [19200])
        self.assertFalse(outcome.matched)
        self.assertEqual(outcome.attempted_bauds, (19200,))
        self.assertEqual(set_calls, [9600, 19200, 115200])

    async def test_unreadable_original_baud_fails_closed(self) -> None:
        # Without a known original speed the sweep cannot restore anything,
        # so it must not touch the collector at all.
        set_calls: list[int] = []

        async def read_baud():
            return None

        async def set_baud(baud):
            set_calls.append(baud)
            return True

        async def run_sweep(baud):
            return _Scan(candidates=("ctx",))

        outcome = await async_run_link_baud_sweep(
            candidate_bauds=(9600, 19200),
            read_baud=read_baud,
            set_baud=set_baud,
            run_sweep=run_sweep,
        )

        self.assertFalse(outcome.matched)
        self.assertEqual(set_calls, [])
        self.assertEqual(outcome.attempted_bauds, ())
        self.assertFalse(outcome.restored)

    async def test_admission_stops_the_sweep(self) -> None:
        admissions = iter((True, False))
        set_calls: list[int] = []

        async def read_baud():
            return 115200

        async def set_baud(baud):
            set_calls.append(baud)
            return True

        async def run_sweep(baud):
            return None

        outcome = await async_run_link_baud_sweep(
            candidate_bauds=(2400, 9600, 19200),
            read_baud=read_baud,
            set_baud=set_baud,
            run_sweep=run_sweep,
            admit=lambda: next(admissions),
        )

        self.assertFalse(outcome.matched)
        # One attempt admitted, then the budget said no; restore still runs.
        self.assertEqual(set_calls, [2400, 115200])


class BudgetStarvedSilenceTests(unittest.TestCase):
    def test_skipped_only_probe_log_is_not_silence(self) -> None:
        # Regression: budget exhausted before any driver ran must not read
        # as UART silence (it would trigger a baud walk on an unprobed link).
        probe_log = (
            {"driver": "modbus_smg", "elapsed_ms": 0, "outcome": "skipped_budget_exhausted"},
            {"driver": "pi30", "elapsed_ms": 0, "outcome": "skipped_budget_exhausted"},
        )
        real = tuple(
            e for e in probe_log if e.get("outcome") != "skipped_budget_exhausted"
        )
        silent = bool(real) and not any(e.get("saw_response") for e in real)
        self.assertFalse(silent)

    def test_real_timeouts_are_silence(self) -> None:
        probe_log = (
            {"driver": "pi30", "elapsed_ms": 4000, "outcome": "probe_timeout", "saw_response": False},
            {"driver": "modbus_smg", "elapsed_ms": 0, "outcome": "skipped_budget_exhausted"},
        )
        real = tuple(
            e for e in probe_log if e.get("outcome") != "skipped_budget_exhausted"
        )
        silent = bool(real) and not any(e.get("saw_response") for e in real)
        self.assertTrue(silent)


if __name__ == "__main__":
    unittest.main()
