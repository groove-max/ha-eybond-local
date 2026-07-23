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
    OPERATING_PROFILE_SMARTESS_AND_HA,
    collector_operating_profile_from_entry,
    resolve_collector_operating_profile,
)
from custom_components.eybond_local.const import (  # noqa: E402
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
)


class CollectorOperatingProfileTests(unittest.TestCase):
    def test_callback_external_is_smartess_and_ha(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            endpoint_control_policy=ENDPOINT_CONTROL_EXTERNAL,
        )

        self.assertEqual(
            profile.profile,
            OPERATING_PROFILE_SMARTESS_AND_HA,
        )
        self.assertTrue(profile.stable)
        self.assertFalse(profile.endpoint_tools_allowed)

    def test_managed_inbound_is_ha_only(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

        self.assertEqual(profile.profile, OPERATING_PROFILE_HA_ONLY)
        self.assertTrue(profile.stable)
        self.assertTrue(profile.endpoint_tools_allowed)

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
        self.assertFalse(profile.endpoint_tools_allowed)

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

        self.assertEqual(profile.profile, OPERATING_PROFILE_SMARTESS_AND_HA)

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

    def test_endpoint_tool_permission_is_a_read_only_profile_projection(self) -> None:
        profile = resolve_collector_operating_profile(
            connection_strategy=CONNECTION_STRATEGY_INBOUND,
            endpoint_control_policy=ENDPOINT_CONTROL_INTEGRATION_MANAGED,
        )

        self.assertTrue(profile.endpoint_tools_allowed)
        self.assertNotIn("endpoint_tools_allowed", profile.__dataclass_fields__)

    def test_direct_constructor_rejects_cross_field_contradictions(self) -> None:
        valid = CollectorOperatingProfile(
            profile=OPERATING_PROFILE_SMARTESS_AND_HA,
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
        path = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "config_flow.py"
        )
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        options_class = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "EybondLocalOptionsFlow"
        )

        def _method(name: str) -> ast.AsyncFunctionDef:
            return next(
                node
                for node in options_class.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == name
            )

        runtime_source = ast.get_source_segment(source, _method("async_step_runtime"))
        connection_source = ast.get_source_segment(
            source, _method("async_step_connection")
        )

        self.assertNotIn("async_step_strategy_transition", runtime_source or "")
        self.assertNotIn("_stage_connection_strategy_transition", runtime_source or "")
        self.assertIn("_stage_connection_strategy_transition", connection_source or "")
        self.assertIn("async_step_strategy_transition", connection_source or "")

    def test_endpoint_tools_share_one_profile_gate_without_blocking_cleanup(self) -> None:
        coordinator_path = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "runtime"
            / "coordinator.py"
        )
        source = coordinator_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        coordinator_class = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "EybondLocalCoordinator"
        )

        def _async_method_source(name: str) -> str:
            method = next(
                node
                for node in coordinator_class.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == name
            )
            return ast.get_source_segment(source, method) or ""

        shadow_start = _async_method_source("async_start_shadow_learning")
        shadow_stop = _async_method_source("async_stop_shadow_learning")
        proxy_start = _async_method_source("async_start_proxy_capture")
        proxy_stop = _async_method_source("async_stop_proxy_capture")

        self.assertIn("collector_endpoint_tools_allowed", shadow_start)
        self.assertNotIn("async_set_collector_server_endpoint", shadow_start)
        self.assertIn("proxy_capture_overview", proxy_start)
        self.assertNotIn("collector_endpoint_tools_allowed", shadow_stop)
        self.assertNotIn("collector_endpoint_tools_allowed", proxy_stop)
        self.assertEqual(
            source.count(
                "endpoint_tools_allowed=self.collector_endpoint_tools_allowed"
            ),
            3,
        )


if __name__ == "__main__":
    unittest.main()
