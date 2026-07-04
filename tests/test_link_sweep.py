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


class StructuredSilenceTests(unittest.TestCase):
    def test_sweep_no_match_carries_silent_verdict(self) -> None:
        from custom_components.eybond_local.onboarding.driver_detection import (
            DriverSweepNoMatch,
            _sweep_saw_response,
        )

        silent_exc = DriverSweepNoMatch("pi18:probe_timeout", silent=True)
        self.assertIsInstance(silent_exc, RuntimeError)
        self.assertTrue(silent_exc.silent)

        # A hypothetical future error whose STRING would fool a suffix check
        # still carries the tracked verdict.
        tricky = DriverSweepNoMatch("answered_then_probe_timeout", silent=False)
        self.assertFalse(tricky.silent)

    def test_saw_response_classification(self) -> None:
        from custom_components.eybond_local.onboarding.driver_detection import (
            _sweep_saw_response,
        )

        self.assertFalse(
            _sweep_saw_response(
                ["pi30:probe_timeout", "smg:inverter_link_down"],
                matched_or_no_match=False,
            )
        )
        # A CRC error proves bytes arrived: not silence.
        self.assertTrue(
            _sweep_saw_response(
                ["pi30:probe_timeout", "srne_modbus:crc_mismatch"],
                matched_or_no_match=False,
            )
        )
        self.assertTrue(_sweep_saw_response([], matched_or_no_match=True))


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


if __name__ == "__main__":
    unittest.main()
