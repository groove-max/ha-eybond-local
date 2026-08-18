from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import ast
import math
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import RuntimeSnapshot
from custom_components.eybond_local.telemetry import (
    TelemetryFreshness,
    TelemetryPoint,
    TelemetryValueKind,
    TypedTelemetryFrame,
    fold_driver_telemetry,
    is_telemetry_scalar,
)


class TelemetryPointBoundaryTests(unittest.TestCase):
    def test_exact_scalar_kinds_keep_bool_separate_from_int(self) -> None:
        cases = (
            (None, TelemetryValueKind.UNKNOWN),
            (True, TelemetryValueKind.BOOLEAN),
            (7, TelemetryValueKind.INTEGER),
            (7.5, TelemetryValueKind.NUMBER),
            ("7", TelemetryValueKind.TEXT),
        )

        for value, expected in cases:
            with self.subTest(value=value):
                point = TelemetryPoint(
                    key="sample",
                    value=value,
                    freshness=TelemetryFreshness.FRESH,
                )
                self.assertIs(point.kind, expected)

    def test_direct_constructor_rejects_untrusted_shapes(self) -> None:
        for key in ("", " padded", "padded ", 7, None):
            with self.subTest(key=key):
                with self.assertRaises((TypeError, ValueError)):
                    TelemetryPoint(  # type: ignore[arg-type]
                        key=key,
                        value=1,
                        freshness=TelemetryFreshness.FRESH,
                    )

        for value in (object(), [], {}, b"1", math.nan, math.inf, -math.inf):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises((TypeError, ValueError)):
                    TelemetryPoint(
                        key="sample",
                        value=value,  # type: ignore[arg-type]
                        freshness=TelemetryFreshness.FRESH,
                    )

        with self.assertRaises(TypeError):
            TelemetryPoint(
                key="sample",
                value=1,
                freshness="fresh",  # type: ignore[arg-type]
            )

    def test_scalar_gate_never_coerces_structured_diagnostics(self) -> None:
        self.assertTrue(is_telemetry_scalar(None))
        self.assertTrue(is_telemetry_scalar(False))
        self.assertTrue(is_telemetry_scalar(0))
        self.assertTrue(is_telemetry_scalar(0.0))
        self.assertTrue(is_telemetry_scalar(""))
        self.assertFalse(is_telemetry_scalar([]))
        self.assertFalse(is_telemetry_scalar({}))
        self.assertFalse(is_telemetry_scalar(SimpleNamespace()))
        self.assertFalse(is_telemetry_scalar(math.nan))


class TypedTelemetryFrameTests(unittest.TestCase):
    def test_frame_rejects_duck_points_duplicates_and_invalid_driver(self) -> None:
        point = TelemetryPoint(
            key="pv_power",
            value=900,
            freshness=TelemetryFreshness.FRESH,
        )
        with self.assertRaises(TypeError):
            TypedTelemetryFrame(  # type: ignore[arg-type]
                driver_key="pi30", points=[point]
            )
        with self.assertRaises(TypeError):
            TypedTelemetryFrame(
                driver_key="pi30",
                points=(SimpleNamespace(key="pv_power"),),  # type: ignore[arg-type]
            )
        with self.assertRaises(ValueError):
            TypedTelemetryFrame(driver_key="pi30", points=(point, point))
        with self.assertRaises(ValueError):
            TypedTelemetryFrame(driver_key=" pi30", points=(point,))
        with self.assertRaises(ValueError):
            TypedTelemetryFrame(driver_key="", points=(point,))

    def test_full_replaces_previous_and_skips_structured_diagnostics(self) -> None:
        previous = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"old": 1},
            replace=True,
        )
        raw = {
            "pv_power": 1200,
            "online": True,
            "mode": "Battery",
            "unknown": None,
            "command_timings": [{"command": "QPI"}],
        }

        frame = fold_driver_telemetry(
            previous,
            driver_key="pi30",
            values=raw,
            replace=True,
        )

        self.assertEqual(
            frame.values(),
            {
                "mode": "Battery",
                "online": True,
                "pv_power": 1200,
                "unknown": None,
            },
        )
        self.assertIsNone(frame.point("old"))
        self.assertIsNone(frame.point("command_timings"))
        self.assertEqual(frame.fresh_count, 4)
        # Projection is non-mutating: the broad legacy mapping keeps diagnostics.
        self.assertIn("command_timings", raw)

    def test_delta_marks_reused_points_carried_and_applies_removals(self) -> None:
        previous = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"pv_power": 1000, "battery_voltage": 51.2, "old": 3},
            replace=True,
        )

        frame = fold_driver_telemetry(
            previous,
            driver_key="pi30",
            values={"pv_power": 1100, "structured": {"not": "telemetry"}},
            replace=False,
            removed_keys=frozenset({"old"}),
        )

        self.assertEqual(
            frame.values(), {"battery_voltage": 51.2, "pv_power": 1100}
        )
        self.assertIs(
            frame.point("pv_power").freshness, TelemetryFreshness.FRESH
        )
        self.assertIs(
            frame.point("battery_voltage").freshness,
            TelemetryFreshness.CARRIED,
        )
        self.assertEqual(frame.fresh_count, 1)
        self.assertEqual(frame.carried_count, 1)

    def test_driver_change_fails_closed_to_a_new_frame(self) -> None:
        previous = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"pv_power": 1000},
            replace=True,
        )

        frame = fold_driver_telemetry(
            previous,
            driver_key="modbus_smg",
            values={"battery_voltage": 51.2},
            replace=False,
        )

        self.assertEqual(frame.driver_key, "modbus_smg")
        self.assertEqual(frame.values(), {"battery_voltage": 51.2})

    def test_fold_rejects_duck_previous_frame(self) -> None:
        with self.assertRaises(TypeError):
            fold_driver_telemetry(
                SimpleNamespace(driver_key="pi30", points=()),  # type: ignore[arg-type]
                driver_key="pi30",
                values={"pv_power": 1000},
                replace=True,
            )

    def test_offline_projection_marks_every_point_carried(self) -> None:
        frame = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"pv_power": 1000, "battery_voltage": 51.2},
            replace=True,
        )

        carried = frame.as_carried()

        self.assertEqual(carried.values(), frame.values())
        self.assertEqual(carried.fresh_count, 0)
        self.assertEqual(carried.carried_count, 2)

    def test_runtime_snapshot_default_is_neutral_and_not_shared(self) -> None:
        first = RuntimeSnapshot()
        second = RuntimeSnapshot()

        self.assertEqual(first.telemetry, TypedTelemetryFrame.empty())
        self.assertEqual(second.telemetry, TypedTelemetryFrame.empty())
        self.assertIsNot(first.values, second.values)


class TypedTelemetryArchitectureTests(unittest.TestCase):
    def test_model_is_neutral_and_has_no_ha_runtime_or_driver_dependency(self) -> None:
        path = REPO_ROOT / "custom_components/eybond_local/telemetry.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")

        self.assertFalse(
            any(
                name.startswith(("homeassistant", "runtime", "drivers", "models"))
                for name in imported
            ),
            imported,
        )


if __name__ == "__main__":
    unittest.main()
