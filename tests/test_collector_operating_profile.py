from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.operating_profile import (  # noqa: E402
    CollectorOperatingProfile,
    OPERATING_PROFILE_CUSTOM,
    OPERATING_PROFILE_HA_ONLY,
    OPERATING_PROFILE_CLOUD_AND_HA,
    collector_operating_profile_from_entry,
    resolve_collector_operating_profile,
)
from custom_components.eybond_local.const import (  # noqa: E402
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
)


PRODUCTION_ROOT = REPO_ROOT / "custom_components" / "eybond_local"


def _lifecycle_paths(pattern: str) -> tuple[Path, ...]:
    if pattern == "config_*.py":
        return (
            PRODUCTION_ROOT / "config_flow.py",
            PRODUCTION_ROOT / "config_entry.py",
            *sorted((PRODUCTION_ROOT / "flows" / "config").glob("*.py")),
        )
    if pattern == "options_*.py":
        return (
            PRODUCTION_ROOT / "options_flow.py",
            *sorted((PRODUCTION_ROOT / "flows" / "options").glob("*.py")),
        )
    raise AssertionError(f"unknown lifecycle family: {pattern}")


def _lifecycle_method_source(pattern: str, name: str) -> str:
    for path in _lifecycle_paths(pattern):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"method not found: {name}")


def _coordinator_method_source(name: str) -> str:
    coordinator_root = PRODUCTION_ROOT / "runtime" / "coordinator"
    for path in sorted(coordinator_root.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        for node in ast.walk(ast.parse(source)):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name
            ):
                return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"coordinator method not found: {name}")


class CollectorOperatingProfileTests(unittest.TestCase):
    def test_callback_external_is_cloud_and_ha(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
        )

        self.assertEqual(
            profile.profile,
            OPERATING_PROFILE_CLOUD_AND_HA,
        )
        self.assertTrue(profile.stable)
        self.assertTrue(profile.cloud_tools_allowed)

    def test_managed_inbound_is_ha_only(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_HA_ONLY)
        self.assertTrue(profile.stable)
        self.assertFalse(profile.cloud_tools_allowed)

    def test_verified_user_managed_inbound_is_ha_only(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
            inbound_verified=True,
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_HA_ONLY)
        self.assertEqual(profile.reason, "inbound_verified")

    def test_unverified_inbound_external_is_custom(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_CUSTOM)
        self.assertFalse(profile.stable)
        self.assertFalse(profile.cloud_tools_allowed)

    def test_callback_with_managed_endpoint_is_custom(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_CUSTOM)

    def test_ha_only_capability_requires_inbound(self) -> None:
        inbound = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
            ha_only_required=True,
        )
        callback = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
            ha_only_required=True,
        )

        self.assertEqual(inbound.profile, OPERATING_PROFILE_HA_ONLY)
        self.assertEqual(callback.profile, OPERATING_PROFILE_CUSTOM)

    def test_pending_transition_is_custom(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
            transition_pending=True,
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_CUSTOM)
        self.assertEqual(profile.reason, "transition_pending")

    def test_entry_projection_uses_canonical_axes(self) -> None:
        profile = collector_operating_profile_from_entry(
            {
                "connection_strategy": CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                "endpoint_control_policy": ENDPOINT_CONTROL_EXTERNAL,
            }
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_CLOUD_AND_HA)

    def test_direct_constructor_is_strict(self) -> None:
        valid = CollectorOperatingProfile(
            profile=OPERATING_PROFILE_HA_ONLY,
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_INTEGRATION_MANAGED,
            reason="inbound_managed",
        )

        for field, value in (
            ("profile", " home_assistant_only"),
            ("connection_strategy", object()),
            ("endpoint_control_policy", "bogus"),
            ("reason", ""),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaises((TypeError, ValueError)):
                    replace(valid, **{field: value})

    def test_boolean_inputs_are_exact(self) -> None:
        for field in (
            "inbound_verified",
            "ha_only_required",
            "transition_pending",
        ):
            kwargs = {
                "connection_strategy": CONNECTION_STRATEGY_INBOUND,
                "endpoint_control_policy": ENDPOINT_CONTROL_EXTERNAL,
                field: 1,
            }
            with self.subTest(field=field):
                with self.assertRaises(TypeError):
                    resolve_collector_operating_profile(**kwargs)

    def test_cloud_tool_permission_is_a_read_only_profile_projection(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
        )

        self.assertTrue(profile.cloud_tools_allowed)
        self.assertNotIn("cloud_tools_allowed", profile.__dataclass_fields__)

    def test_direct_constructor_rejects_cross_field_contradictions(self) -> None:
        valid = CollectorOperatingProfile(
            profile=OPERATING_PROFILE_CLOUD_AND_HA,
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
            reason="callback_external",
        )

        for changes in (
            {"profile": OPERATING_PROFILE_HA_ONLY},
            {"connection_strategy": CONNECTION_STRATEGY_INBOUND},
            {"endpoint_control_policy": ENDPOINT_CONTROL_INTEGRATION_MANAGED},
            {"reason": "axis_mismatch"},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    replace(valid, **changes)


class CollectorOperatingProfileArchitectureTests(unittest.TestCase):
    def test_projection_module_is_neutral_and_has_no_legacy_mode_input(self) -> None:
        path = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "connection"
            / "operating_profile.py"
        )
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }

        self.assertFalse(
            any(
                token in imported
                for imported in imports
                for token in ("config_flow", "onboarding", "runtime")
            )
        )
        self.assertNotIn("CONF_COLLECTOR_OPERATION_MODE", source)
        self.assertNotIn("CONF_PROXY_ENABLED", source)

    def test_polling_step_cannot_start_a_profile_transition(self) -> None:
        runtime_source = _lifecycle_method_source("options_*.py", "async_step_runtime")
        connection_source = _lifecycle_method_source("options_*.py", "async_step_connection")

        self.assertNotIn("async_step_strategy_transition", runtime_source or "")
        self.assertNotIn("_stage_connection_strategy_transition", runtime_source or "")
        self.assertIn("_stage_connection_strategy_transition", connection_source or "")
        self.assertIn("async_step_strategy_transition", connection_source or "")

    def test_cloud_tools_use_one_shared_menu_path(self) -> None:
        def _method_source(name: str) -> str:
            return _lifecycle_method_source("options_*.py", name)

        new_operation_gate = _method_source("_cloud_tool_new_operations_allowed")
        proxy_lifecycle = _method_source("_proxy_capture_lifecycle_active")
        proxy_status = _method_source("_proxy_capture_status_available")
        cloud_tools_availability = _method_source("_cloud_tools_menu_available")
        init_step = _method_source("async_step_init")
        cloud_tools_step = _method_source("async_step_cloud_tools")
        proxy_step = _method_source("async_step_proxy_capture")
        shadow_step = _method_source("async_step_shadow_learning")
        diagnostics_menu = _method_source("_diagnostics_menu_options")

        self.assertIn("cloud_connection_supported", new_operation_gate)
        self.assertIn("cloud_tools_allowed", new_operation_gate)
        self.assertIn("proxy_capture_overview", proxy_lifecycle)
        self.assertNotIn("can_start", proxy_lifecycle)
        self.assertIn("can_stop", proxy_lifecycle)
        self.assertIn("critical_phase", proxy_lifecycle)
        self.assertIn("blocking_reason", proxy_status)
        self.assertIn("latest_proxy_trace_path", proxy_status)
        self.assertIn("_cloud_tool_new_operations_allowed", cloud_tools_availability)
        self.assertIn("_proxy_capture_lifecycle_active", cloud_tools_availability)
        self.assertNotIn("_proxy_capture_status_available", cloud_tools_availability)
        self.assertIn("_shadow_learning_lifecycle_active", cloud_tools_availability)
        self.assertIn('"cloud_tools"', init_step)
        self.assertNotIn('"proxy_capture"', init_step)
        self.assertNotIn('"shadow_learning"', init_step)
        self.assertIn('"proxy_capture"', cloud_tools_step)
        self.assertIn('"shadow_learning"', cloud_tools_step)
        self.assertIn("_cloud_tool_new_operations_allowed", proxy_step)
        self.assertIn("_proxy_capture_lifecycle_active", proxy_step)
        self.assertIn("_proxy_capture_status_available", proxy_step)
        self.assertIn("_cloud_tool_new_operations_allowed", shadow_step)
        self.assertIn("_shadow_learning_lifecycle_active", shadow_step)
        self.assertNotIn('"proxy_capture"', diagnostics_menu)

    def test_runtime_route_never_re_reads_legacy_operation_mode(self) -> None:
        route = _coordinator_method_source("collector_uses_home_assistant_route")
        reconcile = _coordinator_method_source(
            "_async_reconcile_managed_collector_endpoint"
        )
        pruning = _coordinator_method_source("_prune_collector_values_for_connection")

        self.assertIn("self.connection_strategy", route)
        for decision in (route, reconcile, pruning):
            self.assertNotIn("CONF_COLLECTOR_OPERATION_MODE", decision)
            self.assertNotIn("DEFAULT_COLLECTOR_OPERATION_MODE", decision)
            self.assertNotIn("collector_operation_mode ==", decision)

    def test_config_flow_has_no_hidden_operation_mode_state(self) -> None:
        source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in _lifecycle_paths("config_*.py")
        )

        self.assertNotIn("self._collector_operation_mode", source)
        self.assertNotIn("COLLECTOR_OPERATION_HA_ONLY", source)
        self.assertNotIn("DEFAULT_COLLECTOR_OPERATION_MODE", source)
        self.assertNotIn("collector_operation_mode_note", source)
        self.assertIn("self._collector_endpoint_bind_applied", source)

    def test_cloud_tools_share_one_profile_gate_without_blocking_cleanup(self) -> None:
        source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(
                (PRODUCTION_ROOT / "runtime" / "coordinator").glob("*.py")
            )
        )
        shadow_start = _coordinator_method_source("async_start_shadow_learning")
        shadow_stop = _coordinator_method_source("async_stop_shadow_learning")
        proxy_start = _coordinator_method_source("async_start_proxy_capture")
        proxy_stop = _coordinator_method_source("async_stop_proxy_capture")
        endpoint_context = _coordinator_method_source(
            "_async_prepare_cloud_tool_endpoint_context"
        )

        self.assertIn("collector_cloud_tools_allowed", shadow_start)
        self.assertIn("async_set_collector_server_endpoint", shadow_start)
        self.assertIn("restore_required", shadow_start)
        self.assertIn("proxy_capture_overview", proxy_start)
        self.assertEqual(
            shadow_start.count("_async_prepare_cloud_tool_endpoint_context"),
            1,
        )
        self.assertEqual(
            proxy_start.count("_async_prepare_cloud_tool_endpoint_context"),
            1,
        )
        self.assertIn(
            "collector_callback_endpoint=endpoint_context.target_endpoint",
            shadow_start,
        )
        self.assertNotIn(
            "collector_callback_endpoint=self.proxy_capture_target_endpoint",
            shadow_start,
        )
        self.assertIn(
            "async_get_collector_server_endpoint_state",
            endpoint_context,
        )
        self.assertNotIn(
            "_async_read_live_collector_server_endpoint",
            endpoint_context,
        )
        self.assertNotIn("collector_cloud_tools_allowed", shadow_stop)
        self.assertNotIn("collector_cloud_tools_allowed", proxy_stop)
        self.assertEqual(
            source.count(
                "cloud_tools_allowed=self.collector_cloud_tools_allowed"
            ),
            4,
        )


if __name__ == "__main__":
    unittest.main()
