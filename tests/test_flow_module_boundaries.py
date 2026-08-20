"""Architecture guards for the data-entry flow decomposition."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "eybond_local"
CONFIG_FLOW = COMPONENT / "config_flow.py"
CONFIG_BASE = COMPONENT / "config_base.py"
CONFIG_NETWORK = COMPONENT / "config_network.py"
CONFIG_MODULES = {
    "config_base.py": "ConfigFlowBaseMixin",
    "config_admission.py": "CollectorAdmissionFlowMixin",
    "config_scan.py": "CollectorScanFlowMixin",
    "config_ble.py": "BluetoothProvisioningFlowMixin",
    "config_confirmation.py": "CollectorConfirmationFlowMixin",
    "config_manual.py": "ManualCollectorFlowMixin",
    "config_entry.py": "EntryCommitFlowMixin",
    "config_collector.py": "SelectedCollectorFlowMixin",
    "config_network.py": "ConfigNetworkFlowMixin",
    "config_results.py": "ScanResultPresentationMixin",
}
OPTIONS_FLOW = COMPONENT / "options_flow.py"
OPTIONS_RUNTIME = COMPONENT / "options_runtime.py"
OPTIONS_MODULES = {
    "options_base.py": "OptionsFlowBase",
    "options_runtime.py": "RuntimeOptionsMixin",
    "options_strategy.py": "StrategyTransitionOptionsMixin",
    "options_shadow_run.py": "ShadowLearningRunMixin",
    "options_shadow_review.py": "ShadowLearningReviewMixin",
    "options_shadow_runtime.py": "ShadowLearningRuntimeMixin",
    "options_proxy.py": "ProxyCaptureOptionsMixin",
    "options_diagnostics.py": "DiagnosticsOptionsMixin",
}
FLOW_TRANSLATION = COMPONENT / "flow_translation.py"
FLOW_PRESENTATION = COMPONENT / "flow_presentation.py"
LISTENER_OPTIONS_FLOW = COMPONENT / "listener_options_flow.py"
CONNECTION_FORM = COMPONENT / "connection_form.py"
NETWORK_INTERFACES = COMPONENT / "network_interfaces.py"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _defined_names(tree: ast.AST) -> set[str]:
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _imported_modules(tree: ast.AST) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _decorator_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    return {
        decorator.id
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Name)
    }


class FlowTranslationBoundaryTests(unittest.TestCase):
    def test_translation_authority_is_not_defined_in_config_flow(self) -> None:
        definitions = _defined_names(_tree(CONFIG_FLOW))
        for name in (
            "_translation_candidates",
            "_load_translation_bundle_from_dir",
            "_merge_translation_bundle",
            "_load_translation_bundle",
            "_translation_lookup",
            "_TranslationBundleMixin",
        ):
            self.assertNotIn(name, definitions)

    def test_translation_module_has_no_product_or_transport_dependency(self) -> None:
        modules = _imported_modules(_tree(FLOW_TRANSLATION))
        for forbidden in (
            "config_flow",
            "onboarding",
            "runtime",
            "collector",
            "connection",
            "homeassistant",
        ):
            self.assertFalse(
                any(forbidden in module.split(".") for module in modules),
                f"flow_translation imports forbidden layer {forbidden}",
            )

    def test_translation_authority_has_one_definition(self) -> None:
        definitions: dict[str, list[str]] = {
            "TranslationBundleMixin": [],
            "load_translation_bundle": [],
        }
        for path in COMPONENT.rglob("*.py"):
            names = _defined_names(_tree(path))
            for name in definitions:
                if name in names:
                    definitions[name].append(str(path.relative_to(COMPONENT)))
        self.assertEqual(
            definitions,
            {
                "TranslationBundleMixin": ["flow_translation.py"],
                "load_translation_bundle": ["flow_translation.py"],
            },
        )


class ListenerOptionsFlowBoundaryTests(unittest.TestCase):
    def test_listener_options_lifecycle_has_one_definition(self) -> None:
        definitions: list[str] = []
        for path in COMPONENT.rglob("*.py"):
            if "ListenerOptionsFlow" in _defined_names(_tree(path)):
                definitions.append(str(path.relative_to(COMPONENT)))
        self.assertEqual(definitions, ["listener_options_flow.py"])

    def test_config_flow_only_selects_the_listener_lifecycle(self) -> None:
        tree = _tree(CONFIG_BASE)
        self.assertNotIn("ListenerOptionsFlow", _defined_names(tree))
        factory = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "async_get_options_flow"
        )
        self.assertIn(
            "ListenerOptionsFlow",
            {node.id for node in ast.walk(factory) if isinstance(node, ast.Name)},
        )

    def test_listener_options_does_not_import_config_flow(self) -> None:
        modules = _imported_modules(_tree(LISTENER_OPTIONS_FLOW))
        self.assertFalse(any("config_flow" in module for module in modules))


class SharedConnectionFormBoundaryTests(unittest.TestCase):
    def test_options_flow_does_not_call_config_flow_connection_helpers(self) -> None:
        tree = _tree(OPTIONS_FLOW)
        options = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondLocalOptionsFlow"
        )
        for node in ast.walk(options):
            self.assertFalse(
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "EybondLocalConfigFlow",
                "options flow must not call config-flow implementation methods",
            )

    def test_shared_connection_form_does_not_import_flow_lifecycles(self) -> None:
        modules = _imported_modules(_tree(CONNECTION_FORM))
        for forbidden in ("config_flow", "listener_options_flow", "runtime"):
            self.assertFalse(any(forbidden in module for module in modules))

    def test_options_lifecycle_has_one_definition_and_no_config_back_import(
        self,
    ) -> None:
        definitions: list[str] = []
        for path in COMPONENT.rglob("*.py"):
            if "EybondLocalOptionsFlow" in _defined_names(_tree(path)):
                definitions.append(str(path.relative_to(COMPONENT)))
        self.assertEqual(definitions, ["options_flow.py"])
        modules = _imported_modules(_tree(OPTIONS_FLOW))
        self.assertFalse(any("config_flow" in module for module in modules))

    def test_shared_presentation_has_no_flow_lifecycle_dependency(self) -> None:
        modules = _imported_modules(_tree(FLOW_PRESENTATION))
        for forbidden in ("config_flow", "options_flow", "listener_options_flow"):
            self.assertFalse(any(forbidden in module for module in modules))

    def test_config_base_selects_options_lifecycles_without_module_back_edges(self) -> None:
        tree = _tree(CONFIG_BASE)
        imports = [
            node
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module == "options_flow"
        ]
        self.assertEqual(imports, [])
        factory = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "async_get_options_flow"
        )
        imported = {
            alias.name
            for node in ast.walk(factory)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        self.assertEqual(
            imported,
            {"EybondLocalOptionsFlow", "ListenerOptionsFlow"},
        )


class ConfigFlowDecompositionBoundaryTests(unittest.TestCase):
    EXPECTED_MRO = [
        "CollectorAdmissionFlowMixin",
        "CollectorScanFlowMixin",
        "BluetoothProvisioningFlowMixin",
        "CollectorConfirmationFlowMixin",
        "ManualCollectorFlowMixin",
        "EntryCommitFlowMixin",
        "SelectedCollectorFlowMixin",
        "ConfigNetworkFlowMixin",
        "ScanResultPresentationMixin",
        "ConfigFlowBaseMixin",
        "TranslationBundleMixin",
        "ConfigFlow",
    ]
    TRANSLATED_METHODS = {
        "_async_refresh_force_unsupported_override",
        "async_step_integration_discovery",
        "async_step_scan_collector_route",
        "async_step_verify_connection",
        "async_step_verify_connection_failed",
        "async_step_collector_network",
        "async_step_auto",
        "async_step_scanning",
        "async_step_scan_results",
        "async_step_advanced_setup",
        "async_step_change_scan_interface",
        "async_step_choose",
        "async_step_detection_summary",
        "async_step_bluetooth_setup",
        "async_step_confirm",
        "async_step_confirm_without_cloud_assist",
        "async_step_smartess_cloud_assist",
        "async_step_smartess_cloud_assist_summary",
        "async_step_confirm_poll_interval",
        "async_step_manual",
        "async_step_manual_confirm",
        "async_step_manual_smartess_cloud_assist",
        "async_step_manual_recovery_confirm",
        "async_step_manual_recovery_result",
        "async_step_manual_recovery_inbound_confirm",
        "async_step_manual_recovery_failed",
        "async_step_reconfigure",
    }
    STATIC_METHODS = {
        "_ble_device_name",
        "_ble_log_value",
        "_ble_flow_error_key",
        "_smartess_ble_candidate_from_hass_service_info",
        "_hass_bluetooth_service_info_summary",
        "_smartess_ble_candidate_from_hass_device",
        "_result_is_passive_callback",
        "_collector_identity_projection",
        "_validate_connection_inputs",
        "_escape_markdown_table_cell",
        "_is_visible_scan_result",
        "_is_route_scan_result",
        "_scan_result_priority",
    }

    def _lifecycle_classes(self) -> dict[str, ast.ClassDef]:
        classes: dict[str, ast.ClassDef] = {}
        for filename, class_name in CONFIG_MODULES.items():
            tree = _tree(COMPONENT / filename)
            definitions = [
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            ]
            self.assertEqual(len(definitions), 1, filename)
            classes[class_name] = definitions[0]
        return classes

    def test_composition_root_only_registers_the_real_ha_flow(self) -> None:
        tree = _tree(CONFIG_FLOW)
        root = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondLocalConfigFlow"
        )
        self.assertEqual(
            [base.id for base in root.bases if isinstance(base, ast.Name)],
            self.EXPECTED_MRO,
        )
        methods = [
            node
            for node in root.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        self.assertEqual(methods, [])
        assignments = [node for node in root.body if isinstance(node, ast.Assign)]
        self.assertEqual(
            [
                target.id
                for node in assignments
                for target in node.targets
                if isinstance(target, ast.Name)
            ],
            ["VERSION"],
        )

    def test_config_lifecycles_never_import_the_composition_root(self) -> None:
        for filename in CONFIG_MODULES:
            modules = _imported_modules(_tree(COMPONENT / filename))
            self.assertFalse(
                any("config_flow" in module.split(".") for module in modules),
                filename,
            )

    def test_config_lifecycles_do_not_depend_on_peer_lifecycles(self) -> None:
        support_modules = {"config_common", "config_result_model"}
        for filename in CONFIG_MODULES:
            peer_imports = {
                module
                for module in _imported_modules(_tree(COMPONENT / filename))
                if module.startswith("config_")
            }
            self.assertTrue(
                peer_imports <= support_modules,
                f"{filename} imports peer lifecycle modules {sorted(peer_imports - support_modules)}",
            )

    def test_each_config_lifecycle_method_has_one_owner(self) -> None:
        owners: dict[str, str] = {}
        for class_name, class_node in self._lifecycle_classes().items():
            for node in class_node.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                self.assertNotIn(
                    node.name,
                    owners,
                    f"{node.name} is defined by both {owners.get(node.name)} and {class_name}",
                )
                owners[node.name] = class_name

    def test_config_method_decorators_survived_extraction(self) -> None:
        methods: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        for class_node in self._lifecycle_classes().values():
            methods.update(
                {
                    node.name: node
                    for node in class_node.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                }
            )
        for name in self.TRANSLATED_METHODS:
            self.assertIn(
                "_with_translation_bundle", _decorator_names(methods[name]), name
            )
        for name in self.STATIC_METHODS:
            self.assertIn("staticmethod", _decorator_names(methods[name]), name)
        self.assertIn(
            "classmethod", _decorator_names(methods["_ble_device_log_summary"])
        )

    def test_config_lifecycle_modules_remain_reviewable(self) -> None:
        for filename in CONFIG_MODULES:
            line_count = len(
                (COMPONENT / filename).read_text(encoding="utf-8").splitlines()
            )
            self.assertLessEqual(line_count, 1500, filename)


class OptionsFlowDecompositionBoundaryTests(unittest.TestCase):
    EXPECTED_MRO = [
        "StrategyTransitionOptionsMixin",
        "RuntimeOptionsMixin",
        "ShadowLearningRunMixin",
        "ShadowLearningReviewMixin",
        "ShadowLearningRuntimeMixin",
        "ProxyCaptureOptionsMixin",
        "DiagnosticsOptionsMixin",
        "OptionsFlowBase",
    ]
    TRANSLATED_STEPS = {
        "async_step_init",
        "async_step_cloud_tools",
        "async_step_connection",
        "async_step_inverter_protocol",
        "async_step_collector_wifi",
        "async_step_collector_uart",
        "async_step_runtime",
        "async_step_diagnostics",
        "async_step_runtime_poll_interval",
        "async_step_strategy_transition",
        "async_step_strategy_transition_rollback",
        "async_step_strategy_transition_result",
        "async_step_strategy_transition_repair_result",
        "async_step_strategy_transition_activation_incomplete",
        "async_step_strategy_transition_failed",
        "async_step_diagnostic_commands",
        "async_step_shadow_learning",
        "async_step_shadow_learning_credentials",
        "async_step_shadow_learning_progress",
        "async_step_shadow_learning_review",
        "async_step_shadow_learning_result",
        "async_step_proxy_capture",
        "async_step_create_support_package",
        "async_step_reload_local_metadata",
        "async_step_rollback_local_metadata",
        "async_step_diagnostics_result",
    }
    STATIC_METHODS = {
        "_await_critical",
        "_control_discovery_failure_reason",
        "_control_discovery_cloud_provider",
        "_preflight_effective_metadata",
        "_shadow_learning_runtime",
        "_shadow_learning_runtime_view",
        "_collector_query_response_text",
        "_parse_collector_wifi_scan_response",
        "_normalize_collector_uart_baudrate",
    }

    def _lifecycle_classes(self) -> dict[str, ast.ClassDef]:
        classes: dict[str, ast.ClassDef] = {}
        for filename, class_name in OPTIONS_MODULES.items():
            tree = _tree(COMPONENT / filename)
            definitions = [
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            ]
            self.assertEqual(len(definitions), 1, filename)
            classes[class_name] = definitions[0]
        return classes

    def test_composition_root_is_declarative_and_has_exact_mro(self) -> None:
        tree = _tree(OPTIONS_FLOW)
        root = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondLocalOptionsFlow"
        )
        self.assertEqual(
            [base.id for base in root.bases if isinstance(base, ast.Name)],
            self.EXPECTED_MRO,
        )
        self.assertFalse(
            any(
                isinstance(
                    node,
                    (ast.FunctionDef, ast.AsyncFunctionDef, ast.Assign, ast.AnnAssign),
                )
                for node in root.body
            ),
            "the options composition root must not regain lifecycle implementation",
        )

    def test_lifecycle_modules_have_no_flow_back_dependency(self) -> None:
        for filename in OPTIONS_MODULES:
            modules = _imported_modules(_tree(COMPONENT / filename))
            for forbidden in ("config_flow", "options_flow"):
                self.assertFalse(
                    any(forbidden in module.split(".") for module in modules),
                    f"{filename} imports lifecycle root {forbidden}",
                )

    def test_options_lifecycles_do_not_depend_on_peer_lifecycles(self) -> None:
        for filename in OPTIONS_MODULES:
            peer_imports = {
                module
                for module in _imported_modules(_tree(COMPONENT / filename))
                if module.startswith("options_")
            }
            self.assertTrue(
                peer_imports <= {"options_shared"},
                f"{filename} imports peer lifecycle modules {sorted(peer_imports - {'options_shared'})}",
            )

    def test_each_lifecycle_method_has_one_owner(self) -> None:
        owners: dict[str, str] = {}
        for class_name, class_node in self._lifecycle_classes().items():
            for node in class_node.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                self.assertNotIn(
                    node.name,
                    owners,
                    f"{node.name} is defined by both {owners.get(node.name)} and {class_name}",
                )
                owners[node.name] = class_name

    def test_required_method_decorators_survived_extraction(self) -> None:
        methods: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        for class_node in self._lifecycle_classes().values():
            methods.update(
                {
                    node.name: node
                    for node in class_node.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                }
            )
        for name in self.TRANSLATED_STEPS:
            self.assertIn(name, methods)
            self.assertIn(
                "_with_translation_bundle", _decorator_names(methods[name]), name
            )
        for name in self.STATIC_METHODS:
            self.assertIn(name, methods)
            self.assertIn("staticmethod", _decorator_names(methods[name]), name)

    def test_lifecycle_modules_remain_reviewable(self) -> None:
        for filename in OPTIONS_MODULES:
            line_count = len(
                (COMPONENT / filename).read_text(encoding="utf-8").splitlines()
            )
            self.assertLessEqual(line_count, 1500, filename)


class NetworkInterfaceBoundaryTests(unittest.TestCase):
    def test_network_discovery_has_no_flow_lifecycle_dependency(self) -> None:
        modules = _imported_modules(_tree(NETWORK_INTERFACES))
        for forbidden in ("config_flow", "options_flow", "homeassistant"):
            self.assertFalse(any(forbidden in module for module in modules))

    def test_both_lifecycles_use_one_module_level_network_seam(self) -> None:
        for path in (CONFIG_NETWORK, OPTIONS_RUNTIME):
            tree = _tree(path)
            relative_imports = {
                alias.name
                for node in tree.body
                if isinstance(node, ast.ImportFrom)
                and node.level == 1
                and node.module is None
                for alias in node.names
            }
            self.assertIn("network_interfaces", relative_imports, path.name)
            attributes = {
                node.attr
                for node in ast.walk(tree)
                if isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "network_interfaces"
            }
            self.assertIn("get_ipv4_interfaces", attributes, path.name)
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("_get_ipv4_interfaces", source, path.name)


if __name__ == "__main__":
    unittest.main()
