"""Cross-layer tests for the explicit driver read contract (Batch 1).

Proves the driver -> runtime snapshot boundary is safe for partial (DELTA)
updates: PI30 (whose single-cycle poll can omit measurements via optional
failures, unsupported skips, energy early-exit, or explicit removed_keys) no
longer reverts omitted measurements to detection-time
(``DetectedInverter.details``) values, while FULL drivers keep their previous
replace-everything behaviour.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.drivers import pi30 as pi30_module
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.drivers.read_result import (
    DriverReadMode,
    DriverReadResult,
    coerce_driver_read_result,
)
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.telemetry import (
    TelemetryFreshness,
    TelemetryOrigin,
)


class _FakeLink:
    """Minimal link manager: _build_snapshot only needs collector_info."""

    def __init__(self) -> None:
        self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
        self.transport = object()
        self.connected = True
        self.configured_collector_ip = "192.168.1.14"


def _hub() -> EybondHub:
    hub = EybondHub(
        connection=EybondConnectionSpec(
            server_ip="192.168.1.10",
            collector_ip="192.168.1.14",
            collector_pn="V001020SYN62344022",
            tcp_port=18899,
            udp_port=58899,
            discovery_target="192.168.1.255",
            discovery_interval=30,
            heartbeat_interval=60,
            request_timeout=5.0,
        ),
    )
    hub._link_manager = _FakeLink()
    return hub


def _inverter(*, serial: str = "55355535553555", details: dict | None = None) -> DetectedInverter:
    return DetectedInverter(
        driver_key="pi30",
        protocol_family="pi30",
        model_name="PI30 3500",
        variant_key="default",
        serial_number=serial,
        probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
        details=dict(details or {}),
    )


def _bind(hub: EybondHub, inverter: DetectedInverter) -> None:
    """Accept an inverter binding through the single cache-lifecycle boundary."""

    hub._driver = Pi30Driver()
    hub._inverter = inverter
    hub._accept_inverter_binding_identity()


# --- Contract-level: fail-closed typing ---------------------------------------


class DriverReadContractTests(unittest.TestCase):
    def test_bare_dict_is_full(self) -> None:
        result = coerce_driver_read_result({"a": 1}, driver_key="pi30")
        self.assertIsInstance(result, DriverReadResult)
        self.assertIs(result.mode, DriverReadMode.FULL)
        self.assertEqual(result.values, {"a": 1})

    def test_driver_read_result_passthrough(self) -> None:
        original = DriverReadResult(values={"a": 1}, mode=DriverReadMode.DELTA)
        self.assertIs(coerce_driver_read_result(original), original)

    def test_unknown_result_type_is_rejected(self) -> None:
        for bad in (None, 5, "x", ["a"], object()):
            with self.assertRaises(TypeError):
                coerce_driver_read_result(bad, driver_key="pi30")

    def test_invalid_mode_is_rejected(self) -> None:
        # A DriverReadResult with a non-enum mode must fail closed, never be
        # silently applied as FULL or DELTA.
        broken = DriverReadResult(values={}, mode="sideways")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            coerce_driver_read_result(broken, driver_key="pi30")


# --- Hub cache: FULL / DELTA / removal / merge --------------------------------


class HubMeasurementCacheTests(unittest.TestCase):
    def _resolve(self, hub, values, *, mode=DriverReadMode.DELTA, removed=frozenset()):
        return hub._resolve_runtime_measurements(
            DriverReadResult(values=dict(values), mode=mode, removed_keys=frozenset(removed))
        )

    def test_full_replaces_and_drops_missing(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(hub, {"battery_voltage": 27.3, "grid_voltage": 220.0}, mode=DriverReadMode.FULL)
        resolved = self._resolve(hub, {"battery_voltage": 27.4}, mode=DriverReadMode.FULL)
        self.assertEqual(resolved, {"battery_voltage": 27.4})
        self.assertNotIn("grid_voltage", resolved)  # missing on FULL => removed

    def test_delta_overlays_and_retains(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(hub, {"battery_voltage": 27.3, "grid_voltage": 220.0}, mode=DriverReadMode.FULL)
        resolved = self._resolve(hub, {"battery_voltage": 27.2}, mode=DriverReadMode.DELTA)
        self.assertEqual(resolved["battery_voltage"], 27.2)  # updated
        self.assertEqual(resolved["grid_voltage"], 220.0)  # retained

    def test_empty_delta_changes_nothing(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(hub, {"battery_voltage": 27.3}, mode=DriverReadMode.FULL)
        resolved = self._resolve(hub, {}, mode=DriverReadMode.DELTA)
        self.assertEqual(resolved, {"battery_voltage": 27.3})

    def test_removed_keys_delete_value(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(hub, {"battery_voltage": 27.3, "grid_voltage": 220.0}, mode=DriverReadMode.FULL)
        resolved = self._resolve(hub, {}, mode=DriverReadMode.DELTA, removed={"grid_voltage"})
        self.assertNotIn("grid_voltage", resolved)
        self.assertEqual(resolved["battery_voltage"], 27.3)

    def test_identity_change_clears_cache(self) -> None:
        hub = _hub()
        _bind(hub, _inverter(serial="AAA"))
        self._resolve(hub, {"battery_voltage": 27.3}, mode=DriverReadMode.FULL)
        # A different physical device (different serial) binds: the cache is
        # cleared at the bind boundary, so it cannot leak the previous device's
        # measurements even before the new device's first read.
        _bind(hub, _inverter(serial="BBB"))
        self.assertEqual(hub._runtime_measurement_values, {})
        resolved = self._resolve(hub, {"grid_voltage": 220.0}, mode=DriverReadMode.DELTA)
        self.assertEqual(resolved, {"grid_voltage": 220.0})
        self.assertNotIn("battery_voltage", resolved)

    def test_reconnect_same_identity_keeps_cache(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(hub, {"battery_voltage": 27.3}, mode=DriverReadMode.FULL)
        # A plain reconnect resets the driver READ state (group timers) but must
        # NOT drop last-good measurements for the same PN/driver.
        hub._reset_runtime_read_state()
        resolved = self._resolve(hub, {}, mode=DriverReadMode.DELTA)
        self.assertEqual(resolved["battery_voltage"], 27.3)

    def test_full_driver_replaces_everything(self) -> None:
        # A legacy full driver (bare dict -> FULL) keeps its previous behaviour:
        # each cycle replaces the whole measurement set.
        hub = _hub()
        _bind(hub, _inverter())
        r1 = hub._resolve_runtime_measurements(coerce_driver_read_result({"a": 1, "b": 2}))
        self.assertEqual(r1, {"a": 1, "b": 2})
        r2 = hub._resolve_runtime_measurements(coerce_driver_read_result({"a": 3}))
        self.assertEqual(r2, {"a": 3})

    def test_diagnostics_report_fresh_and_reused(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(hub, {"battery_voltage": 27.3, "grid_voltage": 220.0}, mode=DriverReadMode.FULL)
        self._resolve(hub, {"battery_voltage": 27.2}, mode=DriverReadMode.DELTA)
        diag = hub._runtime_measurement_diagnostics()
        self.assertEqual(diag["runtime_read_mode"], "delta")
        self.assertEqual(diag["runtime_measurement_fresh_count"], 1)  # battery_voltage
        self.assertEqual(diag["runtime_measurement_reused_count"], 1)  # grid_voltage
        self.assertEqual(diag["runtime_measurement_value_count"], 2)

    def test_typed_frame_tracks_full_delta_and_structured_diagnostics(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(
            hub,
            {
                "battery_voltage": 27.3,
                "grid_voltage": 220.0,
                "command_timings": [{"command": "QPI"}],
            },
            mode=DriverReadMode.FULL,
        )
        self._resolve(
            hub,
            {"battery_voltage": 27.4},
            mode=DriverReadMode.DELTA,
        )

        frame = hub._runtime_measurement_telemetry
        self.assertEqual(
            frame.values(),
            {"battery_voltage": 27.4, "grid_voltage": 220.0},
        )
        self.assertIs(
            frame.point("battery_voltage").freshness,
            TelemetryFreshness.FRESH,
        )
        self.assertIs(
            frame.point("grid_voltage").freshness,
            TelemetryFreshness.CARRIED,
        )
        self.assertIsNone(frame.point("command_timings"))
        # The compatibility mapping remains byte-for-byte broad.
        self.assertIn("command_timings", hub._runtime_measurement_values)

    def test_snapshot_carries_typed_frame_and_offline_marks_it_reused(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(
            hub,
            {"battery_voltage": 27.3},
            mode=DriverReadMode.FULL,
        )

        live = hub._build_snapshot(connected=True)
        offline = hub._build_snapshot(
            connected=False,
            last_error="collector_not_connected",
            preserve_inverter_values=True,
        )

        self.assertIs(
            live.telemetry.point("battery_voltage").freshness,
            TelemetryFreshness.FRESH,
        )
        self.assertIs(
            offline.telemetry.point("battery_voltage").freshness,
            TelemetryFreshness.CARRIED,
        )
        self.assertEqual(live.values["battery_voltage"], 27.3)
        self.assertEqual(offline.values["battery_voltage"], 27.3)

    def test_snapshot_projects_canonical_values_from_the_same_driver_frame(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        self._resolve(
            hub,
            {
                "input_voltage": 230.0,
                "output_active_power": 900,
                "pv_input_power": 700,
                "battery_voltage": 51.2,
                "battery_charge_current": 0.0,
                "battery_discharge_current": 2.0,
            },
            mode=DriverReadMode.FULL,
        )

        snapshot = hub._build_snapshot(connected=True)

        self.assertEqual(snapshot.values["grid_voltage"], 230.0)
        self.assertEqual(snapshot.telemetry.value("grid_voltage"), 230.0)
        point = snapshot.telemetry.point("grid_voltage")
        self.assertIs(point.origin, TelemetryOrigin.CANONICAL)
        self.assertEqual(point.source_keys, ("input_voltage",))


# --- Snapshot integration: no revert to detection details ---------------------


class SnapshotNeverRevertsToDetailsTests(unittest.TestCase):
    def _snapshot_value(self, hub, key):
        return hub._build_snapshot().values.get(key)

    def test_regression_partial_poll_keeps_live_value_not_detection(self) -> None:
        # The exact reported regression: detection says 27.2, the first live poll
        # says 27.3, the next (partial/empty) poll omits it -> must stay 27.3.
        hub = _hub()
        _bind(hub, _inverter(details={"battery_voltage": 27.2}))

        # Before any runtime poll, detection value bootstraps.
        self.assertEqual(self._snapshot_value(hub, "battery_voltage"), 27.2)

        hub._resolve_runtime_measurements(
            DriverReadResult(values={"battery_voltage": 27.3}, mode=DriverReadMode.FULL)
        )
        self.assertEqual(self._snapshot_value(hub, "battery_voltage"), 27.3)

        hub._resolve_runtime_measurements(
            DriverReadResult(values={}, mode=DriverReadMode.DELTA)
        )
        self.assertEqual(self._snapshot_value(hub, "battery_voltage"), 27.3)
        # And never the detection default again on subsequent partial polls.
        hub._resolve_runtime_measurements(
            DriverReadResult(values={}, mode=DriverReadMode.DELTA)
        )
        self.assertEqual(self._snapshot_value(hub, "battery_voltage"), 27.3)

    def test_full_snapshot_removes_missing_canonical_from_snapshot(self) -> None:
        hub = _hub()
        _bind(hub, _inverter(details={"grid_voltage": 218.0}))
        hub._resolve_runtime_measurements(
            DriverReadResult(
                values={"battery_voltage": 27.3, "grid_voltage": 220.0},
                mode=DriverReadMode.FULL,
            )
        )
        self.assertEqual(self._snapshot_value(hub, "grid_voltage"), 220.0)
        # A later FULL omits grid_voltage: it must be removed, NOT reverted to the
        # detection default (218.0).
        hub._resolve_runtime_measurements(
            DriverReadResult(values={"battery_voltage": 27.4}, mode=DriverReadMode.FULL)
        )
        self.assertIsNone(self._snapshot_value(hub, "grid_voltage"))

    def test_medium_slow_values_survive_a_fast_only_cycle(self) -> None:
        hub = _hub()
        _bind(hub, _inverter(details={"battery_voltage": 27.2}))
        # Cycle 1: FULL (all groups) seeds fast + medium + slow measurements.
        hub._resolve_runtime_measurements(
            DriverReadResult(
                values={
                    "battery_voltage": 27.3,  # fast
                    "alarm_status": "Line fail warning",  # medium
                    "pv_generation_sum": 12345,  # slow
                },
                mode=DriverReadMode.FULL,
            )
        )
        # Cycle 2: fast-only DELTA.
        hub._resolve_runtime_measurements(
            DriverReadResult(values={"battery_voltage": 27.4}, mode=DriverReadMode.DELTA)
        )
        snap = hub._build_snapshot().values
        self.assertEqual(snap["battery_voltage"], 27.4)  # fast updated
        self.assertEqual(snap["alarm_status"], "Line fail warning")  # medium retained
        self.assertEqual(snap["pv_generation_sum"], 12345)  # slow retained


# --- PI30 driver through the hub cache (real single-cycle read) ---------------


class _FakeTransport:
    def __init__(self, responses: dict) -> None:
        from custom_components.eybond_local.payload.pi30 import crc16_xmodem

        self._responses = responses
        self._crc16 = crc16_xmodem
        self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
        self.connected = True
        self.commands: list[str] = []

    def _frame(self, payload: str) -> bytes:
        body = f"({payload}".encode("ascii")
        crc = self._crc16(body)
        high, low = (crc >> 8) & 0xFF, crc & 0xFF
        if high in {0x28, 0x0D, 0x0A}:
            high += 1
        if low in {0x28, 0x0D, 0x0A}:
            low += 1
        return body + bytes((high, low)) + b"\r"

    async def async_send_forward(self, payload, *, devcode, collector_addr):
        command = payload[:-3].decode("ascii")
        self.commands.append(command)
        key = (devcode, collector_addr, command)
        if key not in self._responses:
            raise asyncio.TimeoutError()
        return self._frame(self._responses[key])


class Pi30ThroughHubCacheTests(unittest.TestCase):
    _QPIGS_A = "239.5 49.9 239.5 49.9 0927 0924 015 396 27.30 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 010"
    _QPIGS_B = "239.5 49.9 239.5 49.9 0927 0924 015 396 27.40 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 010"

    def _run(self, coro):
        return asyncio.run(coro)

    _FULL_SEQUENCE = [
        "QPIGS", "QMOD", "QPIWS", "Q1",
        "QET", "QLT", "QT",
        "QEY2026", "QEM202604", "QED20260407",
        "QLY2026", "QLM202604", "QLD20260407",
    ]

    async def _drive(self):
        driver = Pi30Driver()
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        probe_transport = _FakeTransport(
            {
                (0x0994, 0x01, "QPI"): "PI30",
                (0x0994, 0x01, "QID"): "553555355535552",
                (0x0994, 0x01, "QPIRI"): "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1",
            }
        )
        inverter = await driver.async_probe(probe_transport, target)
        assert inverter is not None
        runtime = _FakeTransport(
            {
                (0x0994, 0x01, "QPIGS"): self._QPIGS_A,
                (0x0994, 0x01, "QMOD"): "L",
                (0x0994, 0x01, "QPIWS"): "0000000000000000000000000000000000000000",
                (0x0994, 0x01, "Q1"): "10 20 0 25 30 28 22 0 0 0 40 0 2",
                (0x0994, 0x01, "QET"): "12345",
                (0x0994, 0x01, "QLT"): "2345",
                (0x0994, 0x01, "QT"): "20260407113059",
                (0x0994, 0x01, "QEY2026"): "456",
                (0x0994, 0x01, "QEM202604"): "7",
                (0x0994, 0x01, "QED20260407"): "9",
                (0x0994, 0x01, "QLY2026"): "54",
                (0x0994, 0x01, "QLM202604"): "7",
                (0x0994, 0x01, "QLD20260407"): "1",
            }
        )
        state: dict = {}

        # Cycle 1: one full sequential cycle.
        first = await driver.async_read_values(
            runtime, inverter, runtime_state=state, poll_interval=3.0, now_monotonic=100.0
        )
        first_commands = list(runtime.commands)
        runtime.commands.clear()
        # Cycle 2: same full cycle, but QPIGS reads a new value and QPIWS times out
        # transiently (single-cycle model: no fast-only cadence).
        runtime._responses[(0x0994, 0x01, "QPIGS")] = self._QPIGS_B
        del runtime._responses[(0x0994, 0x01, "QPIWS")]
        second = await driver.async_read_values(
            runtime, inverter, runtime_state=state, poll_interval=3.0, now_monotonic=103.0
        )
        return inverter, first, second, first_commands, list(runtime.commands)

    def test_single_cycle_reads_everything_and_retains_on_transient(self) -> None:
        inverter, first, second, first_commands, second_commands = self._run(self._drive())

        # 1 + 2: every poll runs the SAME single full sequence -- no fast-only cycle.
        self.assertIs(first.mode, DriverReadMode.DELTA)
        self.assertEqual(first_commands, self._FULL_SEQUENCE)
        self.assertEqual(second_commands, self._FULL_SEQUENCE)
        self.assertIn("alarm_status", first.values)
        alarm_value = first.values["alarm_status"]
        # QPIWS timed out transiently this cycle -> not in the delta.
        self.assertNotIn("alarm_status", second.values)

        hub = _hub()
        hub._driver = Pi30Driver()
        hub._inverter = inverter
        hub._accept_inverter_binding_identity()
        hub._resolve_runtime_measurements(first)
        hub._resolve_runtime_measurements(second)
        snap = hub._build_snapshot().values

        # Fresh QPIGS value applied; alarm_status retained (transient failure).
        self.assertEqual(snap["battery_voltage"], 27.4)
        self.assertEqual(snap["alarm_status"], alarm_value)

    def test_pi30_diagnostics_are_truthful(self) -> None:
        _inv, first, second, _first_cmds, _cmds = self._run(self._drive())
        # Honest mode + no scheduler-group field anymore.
        self.assertEqual(first.diagnostics["pi30_poll_mode"], "delta")
        self.assertEqual(second.diagnostics["pi30_poll_mode"], "delta")
        self.assertNotIn("pi30_poll_groups_run", first.diagnostics)
        self.assertNotIn("pi30_group_age_seconds", first.diagnostics)
        # Commands = the actual executed sequence; first cycle measured all 13.
        self.assertEqual(first.diagnostics["pi30_poll_commands"].split(", "), self._FULL_SEQUENCE)
        self.assertEqual(first.diagnostics["pi30_poll_attempted"], 13)
        self.assertEqual(first.diagnostics["pi30_poll_skipped"], 0)
        # Wall-clock total (not a sum of pieces) is present and non-negative.
        self.assertGreaterEqual(first.diagnostics["pi30_poll_total_ms"], 0)
        # Reachability-aware completeness: no unsupported => estimate complete.
        self.assertEqual(first.diagnostics["pi30_full_cycle_estimate_complete"], True)
        self.assertEqual(first.diagnostics["pi30_full_cycle_unmeasured_commands"], "")
        self.assertIn("pi30_full_cycle_unreachable_commands", first.diagnostics)
        self.assertIn("pi30_command_cumulative", first.diagnostics)


class RuntimeOwnedKeyRemovalTests(unittest.TestCase):
    """Snapshot removal of runtime-owned keys (canonical + non-canonical)."""

    def _commit(self, hub) -> dict:
        hub._last_snapshot = hub._build_snapshot()
        return hub._last_snapshot.values

    def test_full_snapshot_removes_missing_non_canonical_key(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        hub._resolve_runtime_measurements(
            DriverReadResult(values={"raw_optional": "old"}, mode=DriverReadMode.FULL)
        )
        self.assertEqual(self._commit(hub).get("raw_optional"), "old")
        hub._resolve_runtime_measurements(
            DriverReadResult(values={}, mode=DriverReadMode.FULL)
        )
        self.assertIsNone(self._commit(hub).get("raw_optional"))

    def test_removed_keys_delete_non_canonical_key_from_snapshot(self) -> None:
        hub = _hub()
        _bind(hub, _inverter())
        hub._resolve_runtime_measurements(
            DriverReadResult(values={"raw_optional": "old"}, mode=DriverReadMode.FULL)
        )
        self.assertEqual(self._commit(hub).get("raw_optional"), "old")
        hub._resolve_runtime_measurements(
            DriverReadResult(
                values={}, mode=DriverReadMode.DELTA, removed_keys=frozenset({"raw_optional"})
            )
        )
        self.assertIsNone(self._commit(hub).get("raw_optional"))

    def test_changed_identity_purges_old_runtime_keys_before_first_read(self) -> None:
        hub = _hub()
        _bind(hub, _inverter(serial="AAA"))
        hub._resolve_runtime_measurements(
            DriverReadResult(
                values={"battery_voltage": 27.3, "raw_optional": "old"},
                mode=DriverReadMode.FULL,
            )
        )
        old = self._commit(hub)
        self.assertEqual(old.get("battery_voltage"), 27.3)  # canonical
        self.assertEqual(old.get("raw_optional"), "old")  # non-canonical

        # Bind a DIFFERENT device; no read of the new device yet.
        _bind(hub, _inverter(serial="BBB", details={}))
        snap = self._commit(hub)
        # Neither the canonical nor the non-canonical old-identity value survives,
        # not via the cache and not via the carried _last_snapshot.
        self.assertIsNone(snap.get("battery_voltage"))
        self.assertIsNone(snap.get("raw_optional"))


class Pi30DiagnosticTruthfulnessTests(unittest.TestCase):
    _QPIGS = "239.5 49.9 239.5 49.9 0927 0924 015 396 27.30 000 100 0028 002.2 315.9 00.00 00000 00010000 00 00 00665 010"
    _QPIRI = "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1"

    def _run(self, coro):
        return asyncio.run(coro)

    async def _probe(self, driver):
        return await driver.async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): self._QPIRI,
                }
            ),
            ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
        )

    def _full_energy(self) -> dict:
        return {
            (0x0994, 0x01, "QPIGS"): self._QPIGS,
            (0x0994, 0x01, "QMOD"): "L",
            (0x0994, 0x01, "QPIWS"): "0000000000000000000000000000000000000000",
            (0x0994, 0x01, "Q1"): "00 00 00 0000 27 30 040 0000 0051 0074 0075 0",
            (0x0994, 0x01, "QET"): "12345",
            (0x0994, 0x01, "QLT"): "2345",
            (0x0994, 0x01, "QT"): "20260407113059",
            (0x0994, 0x01, "QEY2026"): "456",
            (0x0994, 0x01, "QEM202604"): "7",
            (0x0994, 0x01, "QED20260407"): "9",
            (0x0994, 0x01, "QLY2026"): "54",
            (0x0994, 0x01, "QLM202604"): "7",
            (0x0994, 0x01, "QLD20260407"): "1",
        }

    async def _run_single_cycle(self, *, responses, state=None):
        driver = Pi30Driver()
        inverter = await self._probe(driver)
        assert inverter is not None
        transport = _FakeTransport(responses)
        result = await driver.async_read_values(
            transport, inverter, runtime_state=state if state is not None else {},
            poll_interval=10.0, now_monotonic=100.0,
        )
        return result, transport.commands

    def test_optional_timeout_stays_delta_and_counts_timeout(self) -> None:
        # One full cycle, but Q1 (optional) times out -> not FULL, timeout counted,
        # and its measured duration feeds the estimate.
        responses = self._full_energy()
        del responses[(0x0994, 0x01, "Q1")]  # Q1 will time out
        result, _cmds = self._run(self._run_single_cycle(responses=responses))
        self.assertIs(result.mode, DriverReadMode.DELTA)
        self.assertGreaterEqual(result.diagnostics["pi30_poll_timeout"], 1)
        self.assertIn("Q1", result.diagnostics["pi30_command_cumulative"])
        # A timed-out command IS measured (its duration feeds the estimate), so it
        # is not listed among the not-yet-measured commands.
        self.assertNotIn("Q1", result.diagnostics["pi30_full_cycle_unmeasured_commands"])

    def test_skipped_unsupported_command_not_counted_as_attempted(self) -> None:
        from custom_components.eybond_local.drivers.command_support import (
            seed_unsupported_commands,
        )

        state: dict = {}
        seed_unsupported_commands(state, ("QPIWS",))
        result, cmds = self._run(
            self._run_single_cycle(responses=self._full_energy(), state=state)
        )
        self.assertIs(result.mode, DriverReadMode.DELTA)
        self.assertNotIn("QPIWS", cmds)  # never sent
        self.assertGreaterEqual(result.diagnostics["pi30_poll_skipped"], 1)
        self.assertIn("QPIWS", result.diagnostics["pi30_poll_skipped_commands"])
        self.assertIn("QPIWS", result.diagnostics["pi30_known_unsupported_commands"])
        # skipped is not in attempted
        self.assertNotIn("QPIWS", result.diagnostics["pi30_poll_commands"])

    def test_dynamic_energy_command_reported_by_actual_wire_name(self) -> None:
        result, cmds = self._run(self._run_single_cycle(responses=self._full_energy()))
        self.assertIn("QEY2026", cmds)  # actual dynamic command sent
        self.assertIn("QEY2026", result.diagnostics["pi30_poll_commands"])
        # Cumulative is keyed by the STABLE cache key, not the dynamic wire name.
        self.assertIn("QEY:", result.diagnostics["pi30_command_cumulative"])
        self.assertNotIn("QEY2026:", result.diagnostics["pi30_command_cumulative"])

    def test_energy_early_exit_marks_estimate_incomplete(self) -> None:
        responses = self._full_energy()
        del responses[(0x0994, 0x01, "QET")]  # QET fails -> energy chain exits early
        result, _cmds = self._run(self._run_single_cycle(responses=responses))
        self.assertFalse(result.diagnostics["pi30_full_cycle_estimate_complete"])
        unmeasured = result.diagnostics["pi30_full_cycle_unmeasured_commands"]
        self.assertIn("QT", unmeasured)  # never reached this cycle

    def _full_runtime(self) -> dict:
        full = self._full_energy()
        full[(0x0994, 0x01, "QPIGS")] = self._QPIGS
        full[(0x0994, 0x01, "QMOD")] = "L"
        full[(0x0994, 0x01, "QPIWS")] = "0000000000000000000000000000000000000000"
        full[(0x0994, 0x01, "Q1")] = "10 20 0 25 30 28 22 0 0 0 40 0 2"
        return full

    async def _energy_verdict(self, break_key):
        """Drive single cycles until ``break_key`` reaches a final unsupported verdict."""

        driver = Pi30Driver()
        inverter = await self._probe(driver)
        assert inverter is not None
        transport = _FakeTransport(self._full_runtime())
        state: dict = {}
        # Cycle 1: all commands succeed.
        await driver.async_read_values(
            transport, inverter, runtime_state=state, poll_interval=3.0, now_monotonic=100.0
        )
        del transport._responses[break_key]
        results = []
        t = 103.0
        for _ in range(4):
            result = await driver.async_read_values(
                transport, inverter, runtime_state=state, poll_interval=3.0, now_monotonic=t
            )
            results.append((result, None))
            t += 3.0
        return results

    def test_known_unsupported_qpiws_q1_skipped_rest_execute(self) -> None:
        from custom_components.eybond_local.drivers.command_support import (
            seed_unsupported_commands,
        )

        state: dict = {}
        seed_unsupported_commands(state, ("QPIWS", "Q1"))
        result, cmds = self._run(
            self._run_single_cycle(responses=self._full_runtime(), state=state)
        )
        self.assertNotIn("QPIWS", cmds)
        self.assertNotIn("Q1", cmds)
        # Everything else still runs in the one cycle.
        for command in ("QPIGS", "QMOD", "QET", "QLT", "QT", "QEY2026"):
            self.assertIn(command, cmds)
        self.assertEqual(result.diagnostics["pi30_poll_skipped"], 2)

    def test_poll_total_is_wall_clock_and_stats_match_actual_sequence(self) -> None:
        result, cmds = self._run(self._run_single_cycle(responses=self._full_runtime()))
        # command durations diagnostics list exactly the executed wire sequence.
        durations = result.diagnostics["pi30_poll_command_durations_ms"]
        listed = [entry.split("=")[0] for entry in durations.split(", ")]
        self.assertEqual(listed, cmds)
        self.assertEqual(result.diagnostics["pi30_poll_commands"], ", ".join(cmds))
        self.assertGreaterEqual(result.diagnostics["pi30_poll_total_ms"], 0)
        self.assertEqual(result.diagnostics["pi30_poll_attempted"], len(cmds))

    def test_qet_final_unsupported_reachability_estimate_complete(self) -> None:
        async def _run():
            result, _snap = (await self._energy_verdict((0x0994, 0x01, "QET")))[3]
            diag = result.diagnostics
            self.assertEqual(diag["pi30_known_unsupported_commands"], "QET")
            unreachable = diag["pi30_full_cycle_unreachable_commands"].split(", ")
            for key in ("QLT", "QT", "QEY", "QEM", "QED", "QLY", "QLM", "QLD"):
                self.assertIn(key, unreachable)
            # Unreachable commands are NOT counted as unmeasured, so the estimate
            # over reachable commands is complete.
            self.assertEqual(diag["pi30_full_cycle_unmeasured_commands"], "")
            self.assertTrue(diag["pi30_full_cycle_estimate_complete"])
            # No invented durations: only the 4 reachable non-unsupported commands
            # (QPIGS/QMOD/QPIWS/Q1) contribute to the estimate.
            self.assertEqual(diag["pi30_full_cycle_expected_commands"], 4)
            self.assertEqual(diag["pi30_estimated_full_cycle_measured_commands"], 4)

        asyncio.run(_run())

    def test_qt_final_unsupported_only_dynamic_unreachable(self) -> None:
        async def _run():
            result, _snap = (await self._energy_verdict((0x0994, 0x01, "QT")))[3]
            diag = result.diagnostics
            self.assertEqual(diag["pi30_known_unsupported_commands"], "QT")
            unreachable = set(diag["pi30_full_cycle_unreachable_commands"].split(", "))
            self.assertEqual(unreachable, {"QEY", "QEM", "QED", "QLY", "QLM", "QLD"})
            # QET/QLT stay reachable & measured; estimate stays complete.
            self.assertEqual(diag["pi30_full_cycle_unmeasured_commands"], "")
            self.assertTrue(diag["pi30_full_cycle_estimate_complete"])

        asyncio.run(_run())

    # --- energy request+parse atomicity (malformed payloads) -----------------

    def test_malformed_qet_recorded_as_error_not_ok(self) -> None:
        responses = self._full_runtime()
        responses[(0x0994, 0x01, "QET")] = "not-an-int"  # transport ok, parse fails
        result, cmds = self._run(self._run_single_cycle(responses=responses))
        # The value never appears; QET is not a false success.
        self.assertNotIn("pv_generation_sum", result.values)
        self.assertIn("QET:att1/ok0/to0/err1", result.diagnostics["pi30_command_cumulative"])
        self.assertGreaterEqual(result.diagnostics["pi30_poll_error"], 1)
        # QET was sent this cycle but stopped the chain (downstream not reached).
        self.assertIn("QET", cmds)
        self.assertNotIn("QLT", cmds)

    def test_malformed_qlt_does_not_block_qt_or_dynamic(self) -> None:
        responses = self._full_runtime()
        responses[(0x0994, 0x01, "QLT")] = "bad"
        result, cmds = self._run(self._run_single_cycle(responses=responses))
        self.assertNotIn("ac_in_generation_sum", result.values)  # QLT failed
        # QT + dynamics still ran and produced values.
        self.assertIn("pv_generation_year", result.values)
        self.assertIn("QT", cmds)
        self.assertIn("QEY2026", cmds)
        self.assertIn("QLT:att1/ok0/to0/err1", result.diagnostics["pi30_command_cumulative"])

    def test_malformed_qt_stops_only_dynamic_keeps_totals(self) -> None:
        responses = self._full_runtime()
        responses[(0x0994, 0x01, "QT")] = "xx"  # bad clock token
        result, cmds = self._run(self._run_single_cycle(responses=responses))
        self.assertEqual(result.values["pv_generation_sum"], 12345)  # QET
        self.assertEqual(result.values["ac_in_generation_sum"], 2345)  # QLT
        self.assertNotIn("pv_generation_year", result.values)  # dynamic blocked
        self.assertNotIn("QEY2026", cmds)
        self.assertIn("QT:att1/ok0/to0/err1", result.diagnostics["pi30_command_cumulative"])

    def test_malformed_single_dynamic_does_not_block_others(self) -> None:
        responses = self._full_runtime()
        responses[(0x0994, 0x01, "QEY2026")] = "oops"  # only QEY malformed
        result, cmds = self._run(self._run_single_cycle(responses=responses))
        self.assertNotIn("pv_generation_year", result.values)  # QEY failed
        # Independent dynamics still ran.
        self.assertEqual(result.values["pv_generation_month"], 7)  # QEM
        self.assertEqual(result.values["ac_in_generation_year"], 54)  # QLY
        self.assertIn("QLD2026040", cmds[-1] if cmds else "")  # last dynamic still sent
        self.assertIn("QEY:att1/ok0/to0/err1", result.diagnostics["pi30_command_cumulative"])

    def test_cycle_wall_ms_is_deterministic_from_the_clock(self) -> None:
        # pi30_poll_total_ms is the whole-cycle wall-clock (end monotonic minus
        # start monotonic). With a deterministic monotonic clock it is a pure
        # function of that clock -- identical across identical runs.
        class _Clock:
            def __init__(self) -> None:
                self.t = 0.0
                self.values: list[float] = []

            def __call__(self) -> float:
                v = self.t
                self.values.append(v)
                self.t += 0.001  # 1 ms per call
                return v

        def _one_run():
            driver = Pi30Driver()
            inverter = self._run(self._probe(driver))  # probe uses the real clock
            assert inverter is not None

            async def _read():
                transport = _FakeTransport(self._full_runtime())
                return await driver.async_read_values(
                    transport, inverter, runtime_state={}, poll_interval=3.0, now_monotonic=100.0
                )

            clock = _Clock()  # deterministic clock covers ONLY the read
            with mock.patch.object(pi30_module.time, "monotonic", clock):
                result = self._run(_read())
            span = int(round((clock.values[-1] - clock.values[0]) * 1000.0))
            return result.diagnostics["pi30_poll_total_ms"], span

        total_a, span_a = _one_run()
        total_b, span_b = _one_run()
        # Deterministic: identical runs under the same clock produce the same
        # wall-clock total, and it is a positive sub-span of the read's clock.
        self.assertEqual(total_a, total_b)
        self.assertGreater(total_a, 0)
        self.assertLessEqual(total_a, span_a)

    def test_parser_failure_never_reported_as_ok(self) -> None:
        responses = self._full_runtime()
        responses[(0x0994, 0x01, "QET")] = "garbage"
        result, _cmds = self._run(self._run_single_cycle(responses=responses))
        durations = result.diagnostics["pi30_poll_command_durations_ms"]
        # QET appears with an error outcome, never ":ok".
        self.assertIn("QET=", durations)
        self.assertNotIn("QET=0:ok", durations)
        self.assertRegex(durations, r"QET=\d+:error")

    def test_cumulative_counters_accumulate_across_single_cycles(self) -> None:
        driver = Pi30Driver()
        inverter = self._run(self._probe(driver))
        assert inverter is not None
        full = self._full_energy()
        full[(0x0994, 0x01, "QPIGS")] = self._QPIGS
        full[(0x0994, 0x01, "QMOD")] = "L"
        full[(0x0994, 0x01, "QPIWS")] = "0000000000000000000000000000000000000000"
        full[(0x0994, 0x01, "Q1")] = "10 20 0 25 30 28 22 0 0 0 40 0 2"

        async def _drive():
            transport = _FakeTransport(full)
            state: dict = {}
            # Cycle 1: Q1 succeeds (single full cycle).
            await driver.async_read_values(
                transport, inverter, runtime_state=state, poll_interval=3.0, now_monotonic=100.0
            )
            # Cycle 2: same full cycle, but Q1 now times out transiently.
            del transport._responses[(0x0994, 0x01, "Q1")]
            second = await driver.async_read_values(
                transport, inverter, runtime_state=state, poll_interval=3.0, now_monotonic=103.0
            )
            return second

        second = self._run(_drive())
        cumulative = second.diagnostics["pi30_command_cumulative"]
        # Q1 was attempted twice cumulatively: 1 ok (cycle 1) + 1 timeout (cycle 2).
        self.assertIn("Q1:att2/ok1/to1", cumulative)


class Pi30UnsupportedRemovalTests(unittest.TestCase):
    """Optional PI30 commands invalidate their values on a final unsupported verdict."""

    _QPIRI = "220.0 19.0 220.0 50.0 19.0 4200 4200 24.0 27.0 21.0 28.2 27.0 2 30 80 0 2 2 1 10 0 0 27.0 0 1"
    _QPIWS = "0100000000000000000000000000000000000000"
    _Q1 = "10 20 0 25 30 28 22 0 0 0 40 0 2"

    def _energy(self) -> dict:
        return {
            (0x0994, 0x01, "QET"): "12345",
            (0x0994, 0x01, "QLT"): "2345",
            (0x0994, 0x01, "QT"): "20260407113059",
            (0x0994, 0x01, "QEY2026"): "456",
            (0x0994, 0x01, "QEM202604"): "7",
            (0x0994, 0x01, "QED20260407"): "9",
            (0x0994, 0x01, "QLY2026"): "54",
            (0x0994, 0x01, "QLM202604"): "7",
            (0x0994, 0x01, "QLD20260407"): "1",
        }

    def _responses(self) -> dict:
        base = {
            (0x0994, 0x01, "QPIGS"): (
                "239.5 49.9 239.5 49.9 0927 0924 015 396 27.30 000 100 0028 002.2 315.9 "
                "00.00 00000 00010000 00 00 00665 010"
            ),
            (0x0994, 0x01, "QMOD"): "L",
            (0x0994, 0x01, "QPIWS"): self._QPIWS,
            (0x0994, 0x01, "Q1"): self._Q1,
        }
        base.update(self._energy())
        return base

    async def _make(self, responses):
        driver = Pi30Driver()
        inverter = await driver.async_probe(
            _FakeTransport(
                {
                    (0x0994, 0x01, "QPI"): "PI30",
                    (0x0994, 0x01, "QID"): "553555355535552",
                    (0x0994, 0x01, "QPIRI"): self._QPIRI,
                }
            ),
            ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
        )
        assert inverter is not None
        hub = _hub()
        hub._driver = driver
        hub._inverter = inverter
        hub._accept_inverter_binding_identity()
        transport = _FakeTransport(responses)
        return driver, inverter, hub, transport

    async def _cycle(self, driver, inverter, hub, transport, t):
        result = await driver.async_read_values(
            transport,
            inverter,
            runtime_state=hub._runtime_read_state,
            poll_interval=10.0,
            now_monotonic=float(t),
        )
        hub._resolve_runtime_measurements(result)
        hub._last_snapshot = hub._build_snapshot()
        return result, hub._last_snapshot.values

    def test_qpiws_lifecycle_success_transient_verdict_recheck(self) -> None:
        async def _run():
            responses = self._responses()
            driver, inverter, hub, transport = await self._make(responses)

            # 1: QPIWS success creates alarm values.
            _r, snap = await self._cycle(driver, inverter, hub, transport, 100)
            self.assertIn("alarm_bits_raw", snap)
            self.assertIn("alarm_status", snap)
            self.assertEqual(snap["qpiws_bit_count"], 40)
            baseline_alarm = snap["alarm_bits_raw"]

            # Break QPIWS; medium group polls at +30s.
            del transport._responses[(0x0994, 0x01, "QPIWS")]

            # 2: first three timeouts keep last-good alarm values.
            for i, t in enumerate((130, 160, 190), start=1):
                result, snap = await self._cycle(driver, inverter, hub, transport, t)
                self.assertNotIn("QPIWS", result.removed_keys, msg=f"strike {i} must not remove")
                self.assertEqual(snap["alarm_bits_raw"], baseline_alarm, msg=f"strike {i} keeps")
                self.assertIn("alarm_status", snap)

            # 3 + 4: fourth confirmed timeout -> QPIWS unsupported -> alarm gone.
            result, snap = await self._cycle(driver, inverter, hub, transport, 220)
            self.assertIn("alarm_bits_raw", result.removed_keys)
            self.assertIn("alarm_status", result.removed_keys)
            self.assertNotIn("alarm_bits_raw", snap)
            self.assertNotIn("alarm_active", snap)
            self.assertNotIn("qpiws_bit_count", snap)
            self.assertNotIn("alarm_status", snap)
            self.assertEqual(snap.get("driver_unsupported_commands"), "QPIWS")

            # 6: Re-check + successful QPIWS restores alarm values.
            from custom_components.eybond_local.drivers.command_support import (
                clear_unsupported_commands,
            )

            clear_unsupported_commands(hub._runtime_read_state)
            transport._responses[(0x0994, 0x01, "QPIWS")] = self._QPIWS
            _r, snap = await self._cycle(driver, inverter, hub, transport, 260)
            self.assertIn("alarm_bits_raw", snap)
            self.assertIn("alarm_status", snap)
            # 7: after re-check the diagnostics field disappears.
            self.assertNotIn("driver_unsupported_commands", snap)

        asyncio.run(_run())

    def test_persisted_qpiws_unsupported_removes_stale_alarm_after_reconnect(self) -> None:
        async def _run():
            responses = self._responses()
            driver, inverter, hub, transport = await self._make(responses)
            _r, snap = await self._cycle(driver, inverter, hub, transport, 100)
            self.assertIn("alarm_bits_raw", snap)  # alarm in the last-good cache

            # Reconnect: the persisted unsupported set re-seeds QPIWS, the read
            # state resets, but the measurement cache (Batch 1) survives.
            hub.set_persistent_unsupported_commands(("QPIWS",))
            hub._reset_runtime_read_state()
            del transport._responses[(0x0994, 0x01, "QPIWS")]

            # First runtime result after reconnect removes the stale alarm values.
            result, snap = await self._cycle(driver, inverter, hub, transport, 160)
            self.assertIn("alarm_bits_raw", result.removed_keys)
            self.assertNotIn("alarm_bits_raw", snap)
            self.assertNotIn("alarm_status", snap)

        asyncio.run(_run())

    def test_q1_unsupported_removes_direct_and_derived_values(self) -> None:
        async def _run():
            responses = self._responses()
            driver, inverter, hub, transport = await self._make(responses)
            _r, snap = await self._cycle(driver, inverter, hub, transport, 100)
            self.assertIn("inverter_temperature", snap)  # direct Q1
            self.assertIn("inverter_charge_state", snap)  # derived Q1

            del transport._responses[(0x0994, 0x01, "Q1")]
            for t in (130, 160, 190):
                await self._cycle(driver, inverter, hub, transport, t)
            result, snap = await self._cycle(driver, inverter, hub, transport, 220)
            self.assertIn("inverter_temperature", result.removed_keys)
            self.assertIn("inverter_charge_state", result.removed_keys)
            self.assertNotIn("inverter_temperature", snap)
            self.assertNotIn("inverter_charge_state", snap)

        asyncio.run(_run())

    async def _energy_verdict(self, break_key):
        """Drive the slow/energy group until ``break_key`` is finally unsupported."""

        responses = self._responses()
        driver, inverter, hub, transport = await self._make(responses)
        # Cycle 1 @ t=100: all groups -> energy totals + dynamics populated.
        _r, snap = await self._cycle(driver, inverter, hub, transport, 100)
        del transport._responses[break_key]
        # Slow group polls every 60s; 4 timeouts -> final verdict.
        results = []
        for t in (160, 220, 280, 340):
            result, snap = await self._cycle(driver, inverter, hub, transport, t)
            results.append((result, snap))
        return results

    def test_qet_transient_keeps_then_final_removes_whole_energy_chain(self) -> None:
        async def _run():
            results = await self._energy_verdict((0x0994, 0x01, "QET"))
            # Transient (first three) keep the old energy values.
            for result, snap in results[:3]:
                self.assertNotIn("pv_generation_sum", result.removed_keys)
                self.assertEqual(snap["pv_generation_sum"], 12345)
            # Final verdict removes the whole unreachable energy chain.
            final_result, final_snap = results[3]
            for key in (
                "pv_generation_sum",
                "ac_in_generation_sum",
                "pv_generation_year",
                "ac_in_generation_day",
            ):
                self.assertIn(key, final_result.removed_keys)
                self.assertNotIn(key, final_snap)

        asyncio.run(_run())

    def test_qt_final_removes_dynamic_keeps_totals(self) -> None:
        async def _run():
            _r, final_snap = (await self._energy_verdict((0x0994, 0x01, "QT")))[3]
            # Dynamic (clock-dependent) values gone.
            self.assertNotIn("pv_generation_year", final_snap)
            self.assertNotIn("ac_in_generation_month", final_snap)
            # Independent totals (QET/QLT, read before QT) retained.
            self.assertEqual(final_snap["pv_generation_sum"], 12345)
            self.assertEqual(final_snap["ac_in_generation_sum"], 2345)

        asyncio.run(_run())

    def test_single_dynamic_unsupported_removes_only_its_value(self) -> None:
        async def _run():
            _r, final_snap = (await self._energy_verdict((0x0994, 0x01, "QEY2026")))[3]
            self.assertNotIn("pv_generation_year", final_snap)  # QEY's value gone
            # Every other energy value survives.
            self.assertEqual(final_snap["pv_generation_sum"], 12345)
            self.assertEqual(final_snap["ac_in_generation_sum"], 2345)
            self.assertIn("pv_generation_month", final_snap)
            self.assertIn("ac_in_generation_year", final_snap)

        asyncio.run(_run())

    def test_transient_qet_parse_failure_retains_then_fourth_removes_chain(self) -> None:
        # A malformed (unparseable) QET payload is a command failure exactly like
        # a timeout: transient failures keep last-good, the 4th makes QET
        # unsupported and removes the whole unreachable energy chain.
        async def _run():
            responses = self._responses()
            driver, inverter, hub, transport = await self._make(responses)
            _r, snap = await self._cycle(driver, inverter, hub, transport, 100)
            self.assertEqual(snap["pv_generation_sum"], 12345)

            transport._responses[(0x0994, 0x01, "QET")] = "not-an-int"  # parse fails

            for i, t in enumerate((110, 120, 130), start=1):
                result, snap = await self._cycle(driver, inverter, hub, transport, t)
                self.assertNotIn("pv_generation_sum", result.removed_keys, msg=f"strike {i}")
                self.assertEqual(snap["pv_generation_sum"], 12345)  # retained

            result, snap = await self._cycle(driver, inverter, hub, transport, 140)
            self.assertIn("pv_generation_sum", result.removed_keys)
            self.assertNotIn("pv_generation_sum", snap)
            self.assertNotIn("ac_in_generation_sum", snap)
            self.assertNotIn("pv_generation_year", snap)

        asyncio.run(_run())


class NeutralLayerHasNoPi30KeyMappingTests(unittest.TestCase):
    def test_neutral_layers_hold_no_pi30_command_or_key_mapping(self) -> None:
        cc = REPO_ROOT / "custom_components" / "eybond_local"
        paths = (
            cc / "runtime" / "hub.py",
            cc / "runtime" / "coordinator.py",
            cc / "runtime" / "poll_scheduler.py",
            cc / "poll_policy.py",
        )
        for path in paths:
            source = path.read_text(encoding="utf-8")
            for token in (
                "QPIGS",
                "QPIWS",
                "alarm_bits_raw",
                "qpiws_bit_count",
                "pv_generation_sum",
                "ac_in_generation_year",
                "inverter_charge_state",
                "_pi30_removed_keys_for_unsupported",
                "_pi30_command_output_keys",
                "_PI30_RUNTIME_GROUPS",
            ):
                self.assertNotIn(
                    token, source, msg=f"{path.name} must not know PI30 command/key {token!r}"
                )


if __name__ == "__main__":
    unittest.main()
