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


from custom_components.eybond_local.models import (
    CapabilityCondition,
    RuntimeSnapshot,
    WriteCapability,
)
from custom_components.eybond_local.telemetry import (
    TelemetryFreshness,
    TelemetryOrigin,
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

    def test_origin_and_source_boundary_is_structural(self) -> None:
        canonical = TelemetryPoint(
            key="pv_power",
            value=900.0,
            freshness=TelemetryFreshness.FRESH,
            origin=TelemetryOrigin.CANONICAL,
            source_keys=("pv_voltage", "pv_current"),
        )
        self.assertIs(canonical.origin, TelemetryOrigin.CANONICAL)

        invalid = (
            {"origin": "canonical", "source_keys": ("pv_voltage",)},
            {"origin": TelemetryOrigin.DRIVER, "source_keys": ("pv_voltage",)},
            {"origin": TelemetryOrigin.CANONICAL, "source_keys": ()},
            {
                "origin": TelemetryOrigin.CANONICAL,
                "source_keys": ("pv_voltage", "pv_voltage"),
            },
            {"origin": TelemetryOrigin.CANONICAL, "source_keys": ("pv_power",)},
            {"origin": TelemetryOrigin.CANONICAL, "source_keys": ["pv_voltage"]},
        )
        for fields in invalid:
            with self.subTest(fields=fields):
                with self.assertRaises((TypeError, ValueError)):
                    TelemetryPoint(
                        key="pv_power",
                        value=900.0,
                        freshness=TelemetryFreshness.FRESH,
                        **fields,  # type: ignore[arg-type]
                    )


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

    def test_runtime_snapshot_prefers_typed_value_with_explicit_legacy_fallback(self) -> None:
        frame = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="pi30",
            values={"battery_voltage": 51.2, "unknown_value": None},
            replace=True,
        )
        snapshot = RuntimeSnapshot(
            values={
                "battery_voltage": 24.0,
                "collector_pn": "E50000200000000001",
                "unknown_value": "legacy",
            },
            telemetry=frame,
        )

        self.assertTrue(snapshot.has_runtime_value("battery_voltage"))
        self.assertEqual(snapshot.runtime_value("battery_voltage"), 51.2)
        self.assertTrue(snapshot.has_runtime_value("collector_pn"))
        self.assertEqual(
            snapshot.runtime_value("collector_pn"),
            "E50000200000000001",
        )
        self.assertTrue(snapshot.has_runtime_value("unknown_value"))
        self.assertIsNone(snapshot.runtime_value("unknown_value"))
        self.assertFalse(snapshot.has_runtime_value("missing"))
        self.assertEqual(snapshot.runtime_value("missing", "fallback"), "fallback")

        view = snapshot.runtime_values()
        self.assertEqual(view["battery_voltage"], 51.2)
        self.assertEqual(view["collector_pn"], "E50000200000000001")
        self.assertIsNone(view["unknown_value"])
        view["battery_voltage"] = 99.0
        self.assertEqual(snapshot.runtime_value("battery_voltage"), 51.2)

    def test_capability_view_combines_typed_state_with_legacy_blockers(self) -> None:
        frame = fold_driver_telemetry(
            TypedTelemetryFrame.empty(),
            driver_key="modbus_smg",
            values={"operating_mode": "Power On", "output_mode": "Single"},
            replace=True,
        )
        capability = WriteCapability(
            key="output_mode",
            register=300,
            value_kind="enum",
            note="",
            visible_if=(
                CapabilityCondition(
                    key="operating_mode",
                    operator="eq",
                    value="Power On",
                    effect="hide",
                ),
            ),
        )
        snapshot = RuntimeSnapshot(
            values={
                "operating_mode": "Fault",
                "output_mode": "3 Phase-P1",
                "capability_block_reason_output_mode": "write rejected",
            },
            telemetry=frame,
        )

        self.assertFalse(capability.runtime_state(snapshot.values).visible)
        state = capability.runtime_state(snapshot.runtime_values())
        self.assertTrue(state.visible)
        self.assertFalse(state.editable)
        self.assertEqual(state.reasons, ("write rejected",))
        self.assertEqual(snapshot.runtime_value(capability.value_key), "Single")


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

    def test_derived_energy_consumers_use_the_typed_first_snapshot_view(self) -> None:
        path = REPO_ROOT / "custom_components/eybond_local/sensor.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        classes = {
            node.name: node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
        }
        for class_name in (
            "EybondDerivedEnergySensor",
            "EybondDerivedEnergyCycleSensor",
        ):
            with self.subTest(class_name=class_name):
                handler = next(
                    node
                    for node in classes[class_name].body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "_handle_coordinator_update"
                )
                runtime_view_calls = [
                    node
                    for node in ast.walk(handler)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "runtime_values"
                ]
                broad_value_reads = [
                    node
                    for node in ast.walk(handler)
                    if isinstance(node, ast.Attribute)
                    and node.attr == "values"
                ]
                self.assertEqual(len(runtime_view_calls), 1)
                self.assertEqual(broad_value_reads, [])

    def test_capability_consumers_never_bypass_the_snapshot_view(self) -> None:
        class_names_by_path = {
            "number.py": ("EybondCapabilityNumber",),
            "select.py": ("EybondCapabilitySelect",),
            "switch.py": ("EybondCapabilitySwitch",),
            "button.py": ("EybondPresetButton", "EybondCapabilityButton"),
        }
        for filename, class_names in class_names_by_path.items():
            path = REPO_ROOT / f"custom_components/eybond_local/{filename}"
            tree = ast.parse(path.read_text(encoding="utf-8"))
            classes = {
                node.name: node
                for node in tree.body
                if isinstance(node, ast.ClassDef)
            }
            for class_name in class_names:
                with self.subTest(filename=filename, class_name=class_name):
                    broad_value_reads = [
                        node
                        for node in ast.walk(classes[class_name])
                        if isinstance(node, ast.Attribute)
                        and node.attr == "values"
                    ]
                    runtime_view_calls = [
                        node
                        for node in ast.walk(classes[class_name])
                        if isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in {"runtime_value", "runtime_values"}
                    ]
                    self.assertEqual(broad_value_reads, [])
                    self.assertTrue(runtime_view_calls)

    def test_tooling_measurement_attributes_use_the_typed_first_snapshot_view(self) -> None:
        path = REPO_ROOT / "custom_components/eybond_local/button.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tooling_class = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondToolingButton"
        )
        attributes_handler = next(
            node
            for node in tooling_class.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "extra_state_attributes"
        )
        broad_value_reads = [
            node
            for node in ast.walk(attributes_handler)
            if isinstance(node, ast.Attribute) and node.attr == "values"
        ]
        runtime_view_calls = [
            node
            for node in ast.walk(attributes_handler)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "runtime_values"
        ]

        self.assertEqual(broad_value_reads, [])
        self.assertEqual(len(runtime_view_calls), 1)

    def test_hub_write_authority_uses_typed_first_snapshot_values(self) -> None:
        path = REPO_ROOT / "custom_components/eybond_local/runtime/hub_management.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        hub_class = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "HubManagementMixin"
        )
        for method_name in ("async_write_capability", "async_apply_preset"):
            with self.subTest(method_name=method_name):
                method = next(
                    node
                    for node in hub_class.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name == method_name
                )
                snapshot_values_reads = [
                    node
                    for node in ast.walk(method)
                    if isinstance(node, ast.Attribute)
                    and node.attr == "values"
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "snapshot"
                ]
                typed_view_calls = [
                    node
                    for node in ast.walk(method)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in {"runtime_value", "runtime_values"}
                ]
                self.assertEqual(snapshot_values_reads, [])
                self.assertTrue(typed_view_calls)

    def test_sensor_measurement_projections_use_typed_first_snapshot_values(self) -> None:
        sensor_path = REPO_ROOT / "custom_components/eybond_local/sensor.py"
        sensor_tree = ast.parse(sensor_path.read_text(encoding="utf-8"))
        sensor_class = next(
            node
            for node in sensor_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondValueSensor"
        )
        broad_reads = [
            node
            for node in ast.walk(sensor_class)
            if isinstance(node, ast.Attribute) and node.attr == "values"
        ]
        typed_calls = [
            node
            for node in ast.walk(sensor_class)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"runtime_value", "runtime_values"}
        ]
        self.assertEqual(broad_reads, [])
        self.assertTrue(typed_calls)

        precision_path = (
            REPO_ROOT
            / "custom_components/eybond_local/integration_sensor_precision.py"
        )
        init_tree = ast.parse(precision_path.read_text(encoding="utf-8"))
        precision_repair = next(
            node
            for node in init_tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "_async_self_heal_sensor_display_precision"
        )
        broad_reads = [
            node
            for node in ast.walk(precision_repair)
            if isinstance(node, ast.Attribute) and node.attr == "values"
        ]
        typed_calls = [
            node
            for node in ast.walk(precision_repair)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "runtime_value"
        ]
        self.assertEqual(broad_reads, [])
        self.assertTrue(typed_calls)


if __name__ == "__main__":
    unittest.main()
