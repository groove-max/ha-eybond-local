"""Architecture guards for the provider / cloud-evidence ownership boundary.

Proves the runtime coordinator owns HA orchestration only: it imports no provider
HTTP client, builds no provider request/endpoint, resolves no SMG/model draft
policy, and hardcodes no provider allow-list. Proves provider isolation (one
provider's code never runs another's), fail-closed selection, and that
credentials / raw cloud payloads never enter runtime diagnostics.
"""

from __future__ import annotations

import ast
import inspect
import sys
import unittest
from dataclasses import fields
from pathlib import Path


def _code_identifiers(source: str) -> set[str]:
    """Return every identifier used in CODE (never docstrings/comments/def names)."""

    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.keyword) and node.arg:
            names.add(node.arg)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.update(node.module.split("."))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.update(alias.name.split("."))
    return names


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.cloud_evidence_providers import (  # noqa: E402
    CloudEvidenceContext,
    SmartEssCloudEvidenceProvider,
    ValueCloudCloudEvidenceProvider,
)

_CC = REPO_ROOT / "custom_components" / "eybond_local"
_COORDINATOR_PACKAGE = _CC / "runtime" / "coordinator"
_COORDINATOR = _COORDINATOR_PACKAGE / "root.py"
_COORDINATOR_LIFECYCLE = tuple(sorted(_COORDINATOR_PACKAGE.glob("*.py")))
_COORDINATOR_SUPPORT = _CC / "runtime" / "coordinator" / "support.py"
_PROVIDERS = _CC / "support" / "cloud_evidence_providers.py"
_CONST = _CC / "const.py"
_CONFIG_FLOW = _CC / "config_flow.py"
_CONFIG_LIFECYCLE = tuple(sorted((_CC / "flows" / "config").glob("*.py")))
_OPTIONS_SHADOW_RUN = _CC / "flows" / "options" / "shadow_run.py"
_OPTIONS_SHADOW_METADATA_REVIEW = (
    _CC / "flows" / "options" / "shadow_metadata_review.py"
)
_OPTIONS_SHADOW_INACTIVE_DRAFT = (
    _CC / "flows" / "options" / "shadow_inactive_draft.py"
)
_OPTIONS_SHADOW_RUNTIME = _CC / "flows" / "options" / "shadow_runtime.py"
_OPTIONS_LIFECYCLE = tuple(sorted((_CC / "flows" / "options").glob("*.py")))
_CLOUD_LEARNING_RUNNER = _CC / "support" / "cloud_learning_runner.py"
_CLOUD_LEARNING_MODELS = _CC / "support" / "cloud_learning_models.py"
_CLOUD_API_ADAPTERS = _CC / "support" / "cloud_api_adapters.py"
_CLOUD_LEARNING_ENGINES = _CC / "support" / "cloud_learning_engines.py"
_CLOUD_ACTIVE_WORKFLOW = _CC / "support" / "cloud_active_workflow.py"
_CLOUD_READ_ONLY_WORKFLOW = _CC / "support" / "cloud_read_only_workflow.py"
_ACTIVE_CLOUD_LEARNING = _CC / "support" / "cloud_control_discovery.py"
_ACTIVE_READ_BINDER = _CC / "support" / "read_learning_binder.py"
_SHADOW_READ_EVIDENCE = (
    _CC / "support" / "shadow_learning" / "read_evidence.py"
)
_SHADOW_BACKEND = _CC / "support" / "shadow_learning" / "backend.py"
_SHADOW_OVERLAY_GENERATOR = (
    _CC / "support" / "shadow_learning" / "overlay_generator.py"
)
_DESSMONITOR_LEARNING = _CC / "support" / "dessmonitor_learning.py"
_DESSMONITOR_ACTIVE = _CC / "support" / "dessmonitor_active.py"
_DESSMONITOR_ORCHESTRATOR = (
    _CC / "support" / "shadow_learning" / "dessmonitor_orchestrator.py"
)
_SMARTESS_READ_ONLY = _CC / "support" / "smartess_read_only.py"
_SMARTESS_HISTORY = _CC / "support" / "smartess_history.py"
_CLOUD_HISTORY_EVIDENCE = _CC / "support" / "cloud_history_evidence.py"
_DESSMONITOR_CLIENT = _CC / "dessmonitor_cloud.py"
_DESSMONITOR_COLLECTION = _CC / "dessmonitor_collection.py"
_DESSMONITOR_HISTORY = _CC / "dessmonitor_history.py"
_DESSMONITOR_TIME_BASIS = _CC / "dessmonitor_time_basis.py"
_DESSMONITOR_HISTORY_RESOLUTION = _CC / "dessmonitor_history_resolution.py"
_DESSMONITOR_SEMANTICS = _CC / "support" / "dessmonitor_semantics.py"
_CLOUD_SEMANTIC_EVIDENCE = _CC / "support" / "cloud_semantic_evidence.py"
_CLOUD_LOCAL_COVERAGE = _CC / "support" / "cloud_local_coverage.py"
_CLOUD_LOCAL_HISTORY_CORRELATION = (
    _CC / "support" / "cloud_local_history_correlation.py"
)
_CLOUD_LOCAL_HISTORY_REPRESENTABILITY = (
    _CC / "support" / "cloud_local_history_representability.py"
)
_CLOUD_LOCAL_HISTORY_DRAFT = (
    _CC / "support" / "cloud_local_history_draft.py"
)
_CLOUD_LOCAL_HISTORY_DRAFT_WRITER = (
    _CC / "support" / "cloud_local_history_draft_writer.py"
)
_LOCAL_REGISTER_EVIDENCE = _CC / "drivers" / "local_register_evidence.py"
_LOCAL_REGISTER_SERIES = _CC / "drivers" / "local_register_series.py"
_LOCAL_REGISTER_COLLECTION = _CC / "support" / "local_register_collection.py"
_LOCAL_REGISTER_OBSERVATION_FLOW = (
    _CC / "flows" / "options" / "local_register_observation.py"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _coordinator_source() -> str:
    return "\n".join(_read(path) for path in _COORDINATOR_LIFECYCLE)


def _coordinator_identifiers() -> set[str]:
    return set().union(
        *(_code_identifiers(_read(path)) for path in _COORDINATOR_LIFECYCLE)
    )


class CoordinatorOwnershipGuardTests(unittest.TestCase):
    def test_coordinator_imports_no_provider_http_clients(self) -> None:
        source = _coordinator_source()
        for token in ("smartess_cloud", "valuecloud_cloud"):
            self.assertNotIn(
                f"import {token}", source, msg=f"coordinator must not import {token}"
            )
            self.assertNotIn(f".{token} import", source)

    def test_coordinator_builds_no_provider_requests_or_endpoints(self) -> None:
        identifiers = _coordinator_identifiers()
        for token in (
            "login_with_password",
            "fetch_device_bundle_for_collector",
            "fetch_smartess_device_bundle_for_collector",
            "fetch_valuecloud_device_bundle_for_collector",
            "fetch_and_export_smartess_device_bundle_cloud_evidence",
            "fetch_and_export_valuecloud_device_bundle_cloud_evidence",
            "fetch_and_export_device_bundle_cloud_evidence",
            "build_login_url",
            "build_signed_action_url",
        ):
            self.assertNotIn(
                token, identifiers, msg=f"coordinator must not name provider wire {token!r}"
            )

    def test_coordinator_resolves_no_provider_or_smg_draft_policy(self) -> None:
        identifiers = _coordinator_identifiers()
        for token in (
            "resolve_smartess_known_family_draft_plan",
            "create_smartess_known_family_draft",
            "resolve_smartess_smg_bridge_plan",
            "create_smartess_smg_bridge_draft",
        ):
            self.assertNotIn(
                token, identifiers, msg=f"coordinator must not own draft policy {token!r}"
            )
        # The model-family serial rule moved to the driver layer: the coordinator
        # consumes the neutral driver-dispatched answer, never the literal.
        self.assertNotIn('"smartess_0925"', _coordinator_source())
        self.assertIn("serial_is_stable", identifiers)

    def test_coordinator_hardcodes_no_provider_allow_list(self) -> None:
        source = _coordinator_source()
        # The duplicated ``{"smartess", "valuecloud"}`` allow-list is gone; the
        # registry answers "is this provider supported".
        self.assertNotIn('"smartess",\n            "valuecloud"', source)
        self.assertIn("cloud_evidence_provider_supported", _coordinator_identifiers())


class ConfigFlowProviderBoundaryGuardTests(unittest.TestCase):
    """config_flow owns NO provider policy -- end to end.

    It imports no SmartESS/ValueCloud HTTP client, constructs/parses no provider
    request/response, and reaches cloud work only through the neutral provider
    and control-discovery contracts. This covers BOTH onboarding assist AND
    shadow-learning cloud control discovery.
    """

    def test_config_flow_imports_no_provider_http_client(self) -> None:
        source = "\n".join(
            _read(path) for path in (*_CONFIG_LIFECYCLE, *_OPTIONS_LIFECYCLE)
        )
        for token in (
            "from .smartess_cloud import",
            "import smartess_cloud",
            "from . import valuecloud_cloud",
            "import valuecloud_cloud",
        ):
            self.assertNotIn(token, source, msg=f"config_flow must not import {token!r}")

    def test_config_flow_constructs_no_provider_requests(self) -> None:
        identifiers = set().union(
            *(
                _code_identifiers(_read(path))
                for path in (*_CONFIG_LIFECYCLE, *_OPTIONS_LIFECYCLE)
            )
        )
        for token in (
            "login_with_password",
            "fetch_device_bundle_for_collector",
            "fetch_device_bundle_for_collector_with_session",
            "fetch_signed_action",
            "build_device_settings_action",
            "build_device_detail_action",
            "build_learn_settings_plan",
            "async_orchestrate_shadow_learning_settings",
            "async_orchestrate_valuecloud_shadow_learning",
            "bind_cloud_labels_to_registers",
            "valuecloud_cloud_module",
            "fetch_and_export_smartess_device_bundle_cloud_evidence",
            "resolve_smartess_known_family_draft_plan",
        ):
            self.assertNotIn(
                token, identifiers, msg=f"config_flow must not name provider wire {token!r}"
            )

    def test_config_flow_parses_no_raw_provider_payload(self) -> None:
        source = "\n".join(
            _read(path) for path in (*_CONFIG_LIFECYCLE, *_OPTIONS_LIFECYCLE)
        )
        for helper in (
            "_smartess_cloud_bundle_payload",
            "_smartess_cloud_device_preview",
            "_smartess_cloud_detail_sections",
            "_smartess_cloud_highlight_settings",
            "_shadow_learning_settings_dat_from_bundle",
            "_shadow_learning_cloud_identity_from_bundle",
        ):
            self.assertNotIn(helper, source, msg=f"config_flow must not parse payloads via {helper}")

    def test_config_flow_uses_the_neutral_contracts(self) -> None:
        config_identifiers = set().union(
            *(_code_identifiers(_read(path)) for path in _CONFIG_LIFECYCLE)
        )
        options_identifiers = _code_identifiers(
            _read(_OPTIONS_SHADOW_RUN) + "\n" + _read(_OPTIONS_SHADOW_RUNTIME)
        )
        self.assertIn("resolve_cloud_evidence_provider", config_identifiers)
        self.assertIn("build_onboarding_assist", config_identifiers)
        self.assertIn("resolve_cloud_learning_selection", options_identifiers)
        self.assertIn("learning_runner", options_identifiers)

    def test_control_discovery_provider_has_no_family_heuristic_or_default(self) -> None:
        tree = ast.parse(_read(_OPTIONS_SHADOW_RUNTIME))
        method = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_control_discovery_cloud_provider"
        )
        source = ast.unparse(method)
        self.assertNotIn("collector_cloud_family", source)
        self.assertNotIn("'smartess'", source)
        self.assertNotIn("'valuecloud'", source)
        self.assertIn("cloud_evidence_provider", source)

        source_method = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_control_discovery_learning_source"
        )
        source_code = ast.unparse(source_method)
        self.assertIn("wizard_source", source_code)
        self.assertNotIn("default_cloud_learning_source", source_code)
        self.assertNotIn("'smartess'", source_code)
        self.assertNotIn("'valuecloud'", source_code)

        method_method = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_control_discovery_learning_method"
        )
        method_code = ast.unparse(method_method)
        self.assertIn("wizard_method", method_code)
        self.assertNotIn("default_cloud_learning_method", method_code)


class CloudLearningBoundaryGuardTests(unittest.TestCase):
    def test_selection_models_are_provider_and_orchestration_neutral(self) -> None:
        identifiers = _code_identifiers(_read(_CLOUD_LEARNING_MODELS))
        for forbidden in (
            "config_flow",
            "runtime",
            "coordinator",
            "smartess_cloud",
            "valuecloud_cloud",
            "dessmonitor_cloud",
            "CloudLearningRunner",
        ):
            self.assertNotIn(forbidden, identifiers)

    def test_api_adapters_do_not_own_runner_or_workflow_policy(self) -> None:
        source = _read(_CLOUD_API_ADAPTERS)
        identifiers = _code_identifiers(source)
        for forbidden in (
            "config_flow",
            "runtime",
            "coordinator",
            "CloudLearningRunner",
            "requires_shadow_route",
            "requires_control_consent",
        ):
            self.assertNotIn(forbidden, identifiers)

        engines = _read(_CLOUD_LEARNING_ENGINES)
        self.assertNotIn("smartess_cloud", _code_identifiers(engines))
        self.assertNotIn("dessmonitor_cloud", _code_identifiers(engines))

    def test_runner_contract_is_neutral(self) -> None:
        identifiers = _code_identifiers(_read(_CLOUD_LEARNING_RUNNER))
        for forbidden in (
            "config_flow",
            "runtime",
            "coordinator",
            "smartess_cloud",
            "valuecloud_cloud",
            "dessmonitor_cloud",
            "collector_endpoint",
            "async_start_shadow_learning",
            "async_stop_shadow_learning",
        ):
            self.assertNotIn(forbidden, identifiers)

    def test_dessmonitor_read_only_operation_has_no_endpoint_or_control_writer(self) -> None:
        identifiers = _code_identifiers(
            "\n".join(
                _read(path)
                for path in (
                    _DESSMONITOR_LEARNING,
                    _DESSMONITOR_SEMANTICS,
                    _CLOUD_SEMANTIC_EVIDENCE,
                )
            )
        )
        for forbidden in (
            "async_start_shadow_learning",
            "async_stop_shadow_learning",
            "async_set_collector_server_endpoint",
            "async_apply_collector_server_endpoint",
            "async_reboot_collector",
            "ctrlDevice",
            "send_device_control",
            "set_collector",
        ):
            self.assertNotIn(forbidden, identifiers)

        client = _read(_DESSMONITOR_CLIENT)
        self.assertIn("def build_device_control_action", client)
        self.assertIn("def send_device_control", client)
        self.assertIn('"ctrlDevice"', client)
        client_identifiers = _code_identifiers(client)
        for forbidden in (
            "async_start_shadow_learning",
            "async_stop_shadow_learning",
            "async_set_collector_server_endpoint",
            "async_apply_collector_server_endpoint",
            "async_reboot_collector",
        ):
            self.assertNotIn(forbidden, client_identifiers)

    def test_dessmonitor_active_is_provider_owned_and_uses_only_common_lifecycle(self) -> None:
        active = _read(_DESSMONITOR_ACTIVE)
        active_identifiers = _code_identifiers(active)
        self.assertIn("CloudActiveCorrelationOperation", active_identifiers)
        self.assertIn(
            "async_orchestrate_dessmonitor_shadow_learning",
            active_identifiers,
        )
        self.assertIn("fetch_read_only_evidence_for_session", active_identifiers)
        for forbidden in (
            "smartess_cloud",
            "valuecloud_cloud",
            "config_flow",
            "runtime",
            "coordinator",
            "async_set_collector_server_endpoint",
            "async_start_shadow_learning",
            "async_stop_shadow_learning",
        ):
            self.assertNotIn(forbidden, active_identifiers)

        orchestrator = _read(_DESSMONITOR_ORCHESTRATOR)
        orchestrator_identifiers = _code_identifiers(orchestrator)
        self.assertIn("send_device_control", orchestrator_identifiers)
        self.assertIn("async_dispatch_cloud_action", orchestrator_identifiers)
        self.assertIn("summarize_shadow_learning_attempts", orchestrator_identifiers)
        for forbidden in (
            "smartess_cloud",
            "valuecloud_cloud",
            "config_flow",
            "runtime",
            "coordinator",
            "async_set_collector_server_endpoint",
            "async_start_shadow_learning",
            "async_stop_shadow_learning",
        ):
            self.assertNotIn(forbidden, orchestrator_identifiers)

    def test_dessmonitor_semantics_cannot_mint_local_bindings_or_overlays(self) -> None:
        identifiers = _code_identifiers(
            _read(_DESSMONITOR_LEARNING)
            + "\n"
            + _read(_DESSMONITOR_SEMANTICS)
            + "\n"
            + _read(_CLOUD_SEMANTIC_EVIDENCE)
        )
        for forbidden in (
            "bind_cloud_labels_to_registers",
            "generate_shadow_learning_overlay_drafts",
            "ReadLabelBinding",
            "ReadBindingCandidate",
            "async_activate_device_scoped_overlay",
            "driver_key",
        ):
            self.assertNotIn(forbidden, identifiers)
        runner = _read(_DESSMONITOR_LEARNING)
        self.assertIn("read_bindings=None", runner)
        self.assertIn("local_mapping_proven", _read(_CLOUD_SEMANTIC_EVIDENCE))

    def test_local_coverage_is_presence_only_not_a_binding_authority(self) -> None:
        identifiers = _code_identifiers(_read(_CLOUD_LOCAL_COVERAGE))
        for forbidden in (
            "bind_cloud_labels_to_registers",
            "generate_shadow_learning_overlay_drafts",
            "ReadLabelBinding",
            "ReadBindingCandidate",
            "async_activate_device_scoped_overlay",
            "register",
        ):
            self.assertNotIn(forbidden, identifiers)
        source = _read(_CLOUD_LOCAL_COVERAGE)
        self.assertIn("runtime_semantic_presence_only", source)
        self.assertIn('"local_mapping_proven": False', source)

    def test_local_register_evidence_is_driver_owned_not_support_dict_derived(self) -> None:
        source = _read(_LOCAL_REGISTER_EVIDENCE)
        identifiers = _code_identifiers(source)
        for forbidden in (
            "config_flow",
            "runtime",
            "support",
            "dessmonitor",
            "cloud",
            "bind_cloud_labels_to_registers",
            "generate_shadow_learning_overlay_drafts",
            "fixture_ranges",
            "captured_ranges",
            "raw_capture",
            "async_capture_support_evidence",
        ):
            self.assertNotIn(forbidden, identifiers)
        self.assertIn("LocalRegisterReadPlan", identifiers)
        self.assertIn("observed_at", identifiers)
        self.assertIn('"cloud_mapping_proven": False', source)

    def test_local_register_series_is_repeated_driver_owned_evidence(self) -> None:
        source = _read(_LOCAL_REGISTER_SERIES)
        identifiers = _code_identifiers(source)
        self.assertIn("LocalRegisterSnapshot", identifiers)
        self.assertIn("repeated_live_local_wire_observation", source)
        self.assertIn("aware_utc_snapshot_timestamps", source)
        self.assertIn('"cloud_mapping_proven": False', source)
        for forbidden in (
            "dessmonitor",
            "cloud",
            "runtime",
            "config_flow",
            "read_learning_binder",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
        ):
            self.assertNotIn(forbidden, identifiers)

    def test_background_local_collection_is_coordinator_owned_and_read_only(
        self,
    ) -> None:
        manager = _read(_LOCAL_REGISTER_COLLECTION)
        identifiers = _code_identifiers(manager)
        self.assertIn("LocalRegisterSnapshotSeries", identifiers)
        self.assertIn("coordinator_lifetime_read_only_collection", manager)
        self.assertIn('"cloud_mapping_proven": False', manager)
        self.assertIn('"activation_allowed": False', manager)
        for forbidden in (
            "dessmonitor",
            "cloud_local_history_correlation",
            "async_activate_device_scoped_overlay",
            "read_bindings",
            "write_capability",
        ):
            self.assertNotIn(forbidden, identifiers)

        coordinator = _coordinator_source()
        self.assertIn("LocalRegisterCollectionManager", coordinator)
        self.assertIn(
            "await self._local_register_collection.async_shutdown()",
            _read(_CC / "runtime" / "coordinator" / "lifecycle.py"),
        )
        flow = _read(_LOCAL_REGISTER_OBSERVATION_FLOW)
        self.assertIn("start_local_register_collection", flow)
        self.assertIn("async_cancel_local_register_collection", flow)
        for forbidden in (
            "cloud_local_history_correlation",
            "async_activate_device_scoped_overlay",
            "generate_shadow_learning_overlay_drafts",
        ):
            self.assertNotIn(forbidden, flow)

    def test_dessmonitor_local_snapshot_is_capability_gated_and_never_bound(self) -> None:
        flow = _read(_OPTIONS_SHADOW_RUN)
        self.assertIn(
            "learning_engine.evidence_capabilities.local_register_snapshot",
            flow,
        )
        self.assertIn("async_capture_local_register_snapshot", flow)
        self.assertIn("pn_is_same_identity", flow)
        self.assertNotIn("read_bindings=local_register_snapshot", flow)

        runner = _read(_DESSMONITOR_LEARNING)
        self.assertIn("read_bindings=None", runner)
        self.assertNotIn("LocalRegisterSnapshot", runner)

    def test_dessmonitor_history_is_bounded_read_only_and_not_a_mapping(self) -> None:
        history = _read(_DESSMONITOR_HISTORY)
        identifiers = _code_identifiers(history)
        for forbidden in (
            "astimezone",
            "fromtimestamp",
            "bind_cloud_labels_to_registers",
            "generate_shadow_learning_overlay_drafts",
            "ReadLabelBinding",
            "async_activate_device_scoped_overlay",
            "driver_key",
            "register_address",
        ):
            self.assertNotIn(forbidden, identifiers)
        self.assertIn("device_local_timezone_unresolved", history)
        self.assertIn('"local_mapping_proven": False', history)

        adapters = _read(_CC / "support" / "cloud_api_adapters.py")
        dessmonitor_adapter = adapters.split(
            "class DessMonitorCloudApiAdapter",
            1,
        )[1].split("class UnavailableCloudApiAdapter", 1)[0]
        self.assertIn("history=True", dessmonitor_adapter)

        collection = _read(_DESSMONITOR_COLLECTION)
        collection_identifiers = _code_identifiers(collection)
        for required in (
            "fetch_read_only_evidence_for_session",
            "fetch_device_time_basis",
            "fetch_key_parameter_history",
            "fetch_sole_chart_history",
            "resolve_dessmonitor_history_time_basis",
        ):
            self.assertIn(required, collection_identifiers)
        self.assertIn('"read_only": True', collection)
        self.assertIn('"local_mapping_proven": False', collection)
        self.assertIn('"activation_allowed": False', collection)
        for forbidden in (
            "runtime",
            "config_flow",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "read_bindings",
            "write_capability",
            "cloud_local_history_correlation",
            "LocalRegisterSnapshotSeries",
        ):
            self.assertNotIn(forbidden, collection_identifiers)

        runner = _read(_DESSMONITOR_LEARNING)
        self.assertIn("fetch_read_only_evidence_with_history", runner)
        self.assertNotIn("cloud_local_history_correlation", runner)
        self.assertNotIn("LocalRegisterSnapshotSeries", runner)

    def test_metadata_review_helper_has_no_lifecycle_or_activation_authority(
        self,
    ) -> None:
        source = _read(_OPTIONS_SHADOW_METADATA_REVIEW)
        identifiers = _code_identifiers(source)
        self.assertIn("CloudHistoryCollection", identifiers)
        self.assertIn("CloudSemanticEvidenceReport", identifiers)
        self.assertIn("CloudLocalCoverageReport", identifiers)
        self.assertIn("CloudLocalHistoryReview", identifiers)
        self.assertIn("build_cloud_local_history_review", identifiers)
        for forbidden in (
            "config_flow",
            "runtime",
            "coordinator",
            "async_show_form",
            "async_activate_device_scoped_overlay",
            "generate_shadow_learning_overlay_drafts",
            "read_bindings",
            "write_capability",
        ):
            self.assertNotIn(forbidden, identifiers)

    def test_smartess_history_is_provider_owned_read_only_evidence(self) -> None:
        adapter = _read(_SMARTESS_HISTORY)
        identifiers = _code_identifiers(adapter)
        for required in (
            "login_for_control_discovery",
            "fetch_device_bundle_for_collector_with_session",
            "fetch_signed_action",
            "CloudHistoryCollection",
        ):
            self.assertIn(required, identifiers)
        for required in (
            "queryDeviceInfo",
            "querySPKeyParameters",
            "queryDeviceKeyParameterOneDay",
        ):
            self.assertIn(required, adapter)
        for forbidden in (
            "runtime",
            "config_flow",
            "read_learning_binder",
            "async_activate_device_scoped_overlay",
            "write_capability",
        ):
            self.assertNotIn(forbidden, identifiers)

        neutral = _read(_CLOUD_HISTORY_EVIDENCE)
        neutral_identifiers = _code_identifiers(neutral)
        self.assertIn("provider_normalized_history_observation", neutral)
        self.assertIn('"local_mapping_proven": False', neutral)
        self.assertIn('"activation_allowed": False', neutral)
        for forbidden in (
            "dessmonitor",
            "smartess_cloud",
            "runtime",
            "config_flow",
            "register_address",
        ):
            self.assertNotIn(forbidden, neutral_identifiers)

    def test_dessmonitor_time_basis_is_exact_identity_provider_evidence(self) -> None:
        source = _read(_DESSMONITOR_TIME_BASIS)
        identifiers = _code_identifiers(source)
        self.assertIn("queryDeviceInfo", source)
        self.assertIn("pn_is_same_identity", identifiers)
        self.assertIn("provider_exact_device_timezone_offset", source)
        for forbidden in (
            "datetime.now",
            "time.time",
            "bind_cloud_labels_to_registers",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "driver_key",
            "register_address",
        ):
            self.assertNotIn(forbidden, source)

    def test_dessmonitor_history_resolution_is_neutral_and_unproven(self) -> None:
        source = _read(_DESSMONITOR_HISTORY_RESOLUTION)
        identifiers = _code_identifiers(source)
        self.assertIn("DessMonitorHistorySeries", identifiers)
        self.assertIn("DessMonitorDeviceTimeBasis", identifiers)
        self.assertIn("provider_identity_bound_time_resolution", source)
        self.assertIn('"local_mapping_proven": False', source)
        for forbidden in (
            "bind_cloud_labels_to_registers",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "driver_key",
            "register_address",
            "read_bindings",
            "write_capability",
        ):
            self.assertNotIn(forbidden, identifiers)

    def test_history_correlator_is_review_only_and_flow_composed(self) -> None:
        source = _read(_CLOUD_LOCAL_HISTORY_CORRELATION)
        identifiers = _code_identifiers(source)
        self.assertIn("CloudHistorySeries", identifiers)
        self.assertIn("CloudHistoryCollection", identifiers)
        self.assertIn("LocalRegisterSnapshotSeries", identifiers)
        self.assertIn("review_candidate_only", source)
        self.assertIn("candidate_not_proven", source)
        self.assertIn("review_composition_only", source)
        self.assertIn('"local_mapping_proven": False', source)
        self.assertIn('"activation_allowed": False', source)
        for forbidden in (
            "runtime",
            "config_flow",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "read_bindings",
            "write_capability",
            "DessMonitorResolvedHistorySeries",
        ):
            self.assertNotIn(forbidden, identifiers)
        self.assertNotIn(
            "cloud_local_history_correlation",
            _read(_OPTIONS_SHADOW_RUN),
        )
        self.assertIn(
            "metadata_with_cloud_local_history_review",
            _read(_OPTIONS_SHADOW_RUN),
        )
        self.assertNotIn(
            "cloud_local_history_correlation",
            _coordinator_source(),
        )
        self.assertNotIn(
            "cloud_local_history_correlation",
            _read(_DESSMONITOR_LEARNING),
        )

    def test_full_route_representability_is_review_only_and_publicly_projected(
        self,
    ) -> None:
        source = _read(_CLOUD_LOCAL_HISTORY_REPRESENTABILITY)
        identifiers = _code_identifiers(source)
        for required in (
            "ProbeTarget",
            "RegisterSchemaMetadata",
            "CloudLocalHistoryReview",
            "current_context_review_only",
            '"draft_generation_allowed": False',
            '"activation_allowed": False',
        ):
            self.assertIn(required, source)
        for forbidden in (
            "runtime",
            "config_flow",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "write_text",
            "async_update_entry",
        ):
            self.assertNotIn(forbidden, identifiers)

        coordinator = _read(_COORDINATOR_SUPPORT)
        self.assertIn("local_register_overlay_context", coordinator)
        self.assertIn("build_local_register_overlay_context", coordinator)
        self.assertNotIn(
            "build_cloud_local_history_representability_review",
            coordinator,
        )
        flow = _read(_OPTIONS_SHADOW_RUN)
        self.assertIn(
            "metadata_with_cloud_local_history_representability",
            flow,
        )
        self.assertNotIn("support.cloud_local_history_representability", flow)
        helper = _read(_OPTIONS_SHADOW_METADATA_REVIEW)
        self.assertIn(
            "build_cloud_local_history_representability_review",
            helper,
        )
        self.assertNotIn("generate_shadow_learning_overlay_drafts", helper)

    def test_history_draft_plan_is_inactive_recomputed_and_writer_free(self) -> None:
        source = _read(_CLOUD_LOCAL_HISTORY_DRAFT)
        identifiers = _code_identifiers(source)
        for required in (
            "CloudLocalHistoryRepresentabilityReview",
            "CloudLocalHistoryCandidate",
            "inactive_review_draft_plan_only",
            '"local_mapping_proven": False',
            '"draft_generation_allowed": self.draft_generation_allowed',
            '"activation_allowed": False',
        ):
            self.assertIn(required, source)
        for forbidden in (
            "runtime",
            "config_flow",
            "generate_shadow_learning_overlay_drafts",
            "async_activate_device_scoped_overlay",
            "write_text",
            "async_update_entry",
            "local_metadata",
        ):
            self.assertNotIn(forbidden, identifiers)

        flow = _read(_OPTIONS_SHADOW_RUN)
        self.assertIn("metadata_with_cloud_local_history_draft_plan", flow)
        helper = _read(_OPTIONS_SHADOW_METADATA_REVIEW)
        self.assertIn("build_cloud_local_read_draft_plan", helper)
        self.assertNotIn("generate_shadow_learning_overlay_drafts", helper)

    def test_history_draft_writer_creates_only_an_inactive_schema_artifact(
        self,
    ) -> None:
        source = _read(_CLOUD_LOCAL_HISTORY_DRAFT_WRITER)
        identifiers = _code_identifiers(source)
        for required in (
            "CloudLocalReadDraftPlan",
            "build_local_register_overlay_context",
            "draft_activates_automatically",
            "inactive_review_artifact_only",
            '"local_mapping_proven": False',
            '"activation_allowed": False',
            "temporary.replace",
        ):
            self.assertIn(required, source)
        for forbidden in (
            "runtime",
            "config_flow",
            "async_update_entry",
            "async_reload",
            "async_activate_device_scoped_overlay",
            "generate_shadow_learning_overlay_drafts",
            "create_local_profile_draft",
            "local_profile_path",
        ):
            self.assertNotIn(forbidden, identifiers)

        adapter = _read(_OPTIONS_SHADOW_INACTIVE_DRAFT)
        tree = ast.parse(adapter)
        function = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "async_generate_inactive_read_draft"
        )
        function_source = ast.unparse(function)
        self.assertIn(
            "generate_inactive_cloud_local_read_schema_draft",
            function_source,
        )
        self.assertIn("async_add_executor_job", function_source)
        self.assertIn("LocalRegisterOverlayContext", function_source)
        for forbidden in (
            "async_activate_device_scoped_overlay",
            "async_reload",
            "async_update_entry",
            "generate_shadow_learning_overlay_drafts",
        ):
            self.assertNotIn(forbidden, function_source)

        review = _read(_CC / "flows" / "options" / "shadow_review.py")
        self.assertIn("async_generate_inactive_read_draft", review)
        self.assertNotIn("generate_inactive_cloud_local_read_schema_draft", review)

    def test_active_and_metadata_implementations_share_only_neutral_contract(self) -> None:
        active = _read(_ACTIVE_CLOUD_LEARNING)
        dessmonitor_active = _read(_DESSMONITOR_ACTIVE)
        metadata = _read(_DESSMONITOR_LEARNING)
        self.assertIn("from .cloud_active_workflow import", active)
        self.assertIn("from .cloud_active_workflow import", dessmonitor_active)
        self.assertIn("from .cloud_read_only_workflow import", metadata)
        self.assertNotIn("from .cloud_control_discovery import", metadata)
        self.assertNotIn("from .cloud_control_discovery import", dessmonitor_active)
        active_workflow = _read(_CLOUD_ACTIVE_WORKFLOW)
        self.assertIn("from .cloud_learning_runner import", active_workflow)
        self.assertNotIn("smartess_cloud", active_workflow)
        self.assertNotIn("valuecloud_cloud", active_workflow)
        workflow = _read(_CLOUD_READ_ONLY_WORKFLOW)
        self.assertIn("from .cloud_learning_runner import", workflow)
        self.assertNotIn("cloud_control_discovery", workflow)
        smartess = _read(_SMARTESS_READ_ONLY)
        self.assertIn("from .cloud_read_only_workflow import", smartess)
        self.assertNotIn("cloud_control_discovery", smartess)

    def test_flow_gates_both_failure_cleanup_and_route_start_by_capability(self) -> None:
        source = _read(_OPTIONS_SHADOW_RUN)
        self.assertEqual(
            source.count("_control_discovery_requires_shadow_route(coordinator)"),
            2,
        )
        self.assertIn(
            "learning_engine.method.requires_shadow_route",
            source,
        )
        self.assertIn("cloud_learning_shadow_route_forbidden", source)

    def test_api_source_does_not_own_workflow_policy(self) -> None:
        models = _read(_CC / "support" / "cloud_learning_models.py")
        source_class = models.split("class CloudApiSource", 1)[1].split(
            "class CloudLearningMethod", 1
        )[0]
        capabilities_class = models.split("class CloudApiCapabilities", 1)[1].split(
            "class CloudApiSource", 1
        )[0]
        for forbidden in ("requires_shadow_route", "requires_control_consent"):
            self.assertNotIn(forbidden, source_class)
            self.assertNotIn(forbidden, capabilities_class)

        method_class = models.split("class CloudLearningMethod", 1)[1].split(
            "class CloudLearningSelection", 1
        )[0]
        self.assertIn("requires_shadow_route", method_class)
        self.assertIn("requires_control_consent", method_class)

    def test_api_capabilities_do_not_claim_local_evidence_capabilities(self) -> None:
        models = _read(_CLOUD_LEARNING_MODELS)
        api_capabilities = models.split("class CloudApiCapabilities", 1)[1].split(
            "class CloudApiSource", 1
        )[0]
        self.assertNotIn("local_register_snapshot", api_capabilities)
        self.assertNotIn("local_register_series", api_capabilities)
        evidence = models.split(
            "class CloudLearningEvidenceCapabilities", 1
        )[1].split("class CloudLearningMethod", 1)[0]
        self.assertIn("local_register_snapshot", evidence)
        self.assertIn("local_register_series", evidence)

    def test_flow_requires_explicit_method_and_source_selection(self) -> None:
        runtime = _read(_OPTIONS_SHADOW_RUNTIME)
        run = _read(_OPTIONS_SHADOW_RUN)
        self.assertNotIn("resolve_cloud_learning_engine", runtime)
        self.assertNotIn("current_cloud_learning_selection", runtime)
        self.assertIn("resolve_cloud_learning_selection", runtime)
        self.assertIn('"learning_method"', run)
        self.assertIn('"learning_source"', run)

    def test_flow_reads_route_and_consent_only_from_method(self) -> None:
        run = _read(_OPTIONS_SHADOW_RUN)
        runtime = _read(_OPTIONS_SHADOW_RUNTIME)
        for source in (run, runtime):
            self.assertNotIn("source.capabilities.requires_shadow_route", source)
            self.assertNotIn("source.capabilities.requires_control_consent", source)
        self.assertIn("engine.method.requires_control_consent", run)
        self.assertIn("learning_engine.method.requires_shadow_route", run)
        self.assertIn("engine.method.requires_shadow_route", runtime)


class NoUnscopedEvidenceReadGuardTests(unittest.TestCase):
    """Every production cloud-evidence READ is provider-scoped.

    The generic ``load_latest_cloud_evidence`` may be called without a provider
    only by identity-scoped deletion; every other production caller must pass an
    explicit ``provider=``. The provider base is the single scoped reader.
    """

    def test_generic_loader_called_only_provider_scoped_or_for_deletion(self) -> None:
        import ast

        production_root = _CC
        offenders: list[str] = []
        for path in production_root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            if "load_latest_cloud_evidence" not in source:
                continue
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                name = getattr(func, "id", "") or getattr(func, "attr", "")
                if name != "load_latest_cloud_evidence":
                    continue
                has_provider = any(kw.arg == "provider" for kw in node.keywords)
                if not has_provider:
                    offenders.append(f"{path.relative_to(production_root)}:{node.lineno}")
        # The ONLY allowed unscoped call is inside the provider base's scoped
        # wrapper (which passes provider=self.provider_id) -- so there must be
        # zero unscoped calls anywhere in production.
        self.assertEqual(offenders, [], msg=f"unscoped evidence reads: {offenders}")

    def test_dead_unscoped_draft_reader_removed(self) -> None:
        self.assertNotIn(
            "latest_smartess_known_family_draft_plan",
            _read(_CC / "metadata" / "smartess_draft.py"),
        )

    def test_disk_loader_is_reachable_only_through_executor_cache_warm(self) -> None:
        """No synchronous UI/property path may reach provider.load_latest()."""

        source = _read(_COORDINATOR_SUPPORT)
        tree = ast.parse(source)
        callers: list[str] = []

        class _Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.functions: list[str] = []

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                self.functions.append(node.name)
                self.generic_visit(node)
                self.functions.pop()

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
                self.functions.append(node.name)
                self.generic_visit(node)
                self.functions.pop()

            def visit_Call(self, node: ast.Call) -> None:
                if (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "load_latest"
                ):
                    callers.append(self.functions[-1] if self.functions else "")
                self.generic_visit(node)

        _Visitor().visit(tree)

        self.assertEqual(callers, ["_async_warm_smartess_cloud_evidence_cache"])
        warm = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "_async_warm_smartess_cloud_evidence_cache"
        )
        warm_source = ast.unparse(warm)
        self.assertIn("async_add_executor_job", warm_source)
        self.assertIn("provider.load_latest(context)", warm_source)

        cached_accessor = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "_latest_smartess_cloud_evidence_record"
        )
        accessor_source = ast.unparse(cached_accessor)
        self.assertNotIn("load_latest", accessor_source)
        self.assertNotIn("read_text", accessor_source)
        self.assertNotIn("glob", accessor_source)


class ProviderIsolationGuardTests(unittest.TestCase):
    def test_smartess_export_names_only_smartess_fetch(self) -> None:
        source = inspect.getsource(SmartEssCloudEvidenceProvider.export)
        self.assertIn("fetch_and_export_smartess_device_bundle_cloud_evidence", source)
        self.assertNotIn("valuecloud", source)

    def test_valuecloud_export_names_only_valuecloud_fetch(self) -> None:
        source = inspect.getsource(ValueCloudCloudEvidenceProvider.export)
        self.assertIn("fetch_and_export_valuecloud_device_bundle_cloud_evidence", source)
        self.assertNotIn("smartess", source)

    def test_provider_module_imports_no_transport_or_link(self) -> None:
        # Cloud evidence is NOT transport: the provider module must not import the
        # wire/link/session/transport layers (no family/IP ever selects transport).
        source = _read(_PROVIDERS)
        for token in ("runtime.link", "collector.transport", "session_handle", "session_registry"):
            self.assertNotIn(token, source, msg=f"provider module must not import {token}")


class ActiveLearnedReadRouteGuards(unittest.TestCase):
    def test_active_read_binding_never_consumes_address_only_register_projection(self) -> None:
        runner = _read(_ACTIVE_CLOUD_LEARNING)
        self.assertIn("read_register_evidence_from_map(read_map)", runner)
        self.assertIn("register_evidence=register_evidence", runner)
        self.assertNotIn('read_map.get("registers")', runner)

        binder = _read(_ACTIVE_READ_BINDER)
        self.assertIn("ShadowReadRegisterEvidence", binder)
        self.assertIn("ShadowReadRoute", binder)
        self.assertNotIn("def _normalize_registers", binder)

    def test_backend_records_every_local_read_address_axis(self) -> None:
        source = _read(_SHADOW_BACKEND)
        for token in (
            "devcode=header.devcode",
            "collector_addr=header.devaddr",
            "device_addr=read_request.unit",
            "function=read_request.function_code",
            '"register_series"',
        ):
            self.assertIn(token, source)

    def test_overlay_and_activation_require_typed_read_context(self) -> None:
        generator = _read(_SHADOW_OVERLAY_GENERATOR)
        self.assertIn("LearnedReadActivationContext", generator)
        self.assertIn('"function": function', generator)
        self.assertIn('"learned_read_context"', generator)
        self.assertIn("binding[\"route\"] != learned_read_context.route", generator)

        coordinator = _read(_COORDINATOR_SUPPORT)
        self.assertIn("validate_learned_read_activation", coordinator)
        self.assertIn("load_register_schema_raw", coordinator)
        self.assertIn('activation["learned_read_context"]', coordinator)

    def test_read_evidence_model_has_no_overlay_or_activation_side_effects(self) -> None:
        source = _read(_SHADOW_READ_EVIDENCE)
        for forbidden in (
            "async_update_entry",
            "async_reload",
            "write_text",
            "generate_shadow_learning_overlay_drafts",
            "load_driver_profile",
            "load_register_schema",
        ):
            self.assertNotIn(forbidden, source)


class CredentialAndPayloadSafetyGuardTests(unittest.TestCase):
    def test_no_credential_config_keys_exist(self) -> None:
        # Credentials are ephemeral method arguments; there is no CONF_*PASSWORD /
        # USERNAME key, so config_entry.data (embedded verbatim in support bundles)
        # can never carry them.
        source = _read(_CONST)
        tree = ast.parse(source)
        conf_keys = {
            node.targets[0].id
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id.startswith("CONF_")
        }
        offenders = {
            key
            for key in conf_keys
            if any(marker in key for marker in ("PASSWORD", "USERNAME", "CREDENTIAL", "SECRET"))
        }
        self.assertEqual(offenders, set(), msg=f"no credential CONF keys allowed: {offenders}")

    def test_context_carries_no_credentials_or_peer_ip(self) -> None:
        names = {field.name for field in fields(CloudEvidenceContext)}
        for forbidden in ("username", "password", "credential", "secret", "peer_ip", "hostname"):
            offenders = {name for name in names if forbidden in name}
            self.assertEqual(offenders, set(), msg=f"context must not carry {forbidden!r}")


class TransportNeutralityGuardTests(unittest.TestCase):
    def test_cloud_family_does_not_select_payload_transport(self) -> None:
        # Payload/wire selection lives in the link layer and turns ONLY on the
        # negotiated wire (Phase 3/4). Prove the cloud-evidence provider concept
        # never leaks into those resolvers.
        import custom_components.eybond_local.runtime.link as link_module

        for method_name in (
            "_collector_management_selection",
            "collector_metadata_routes",
            "_inverter_forward_adapter",
        ):
            method = getattr(link_module.EybondRuntimeLinkManager, method_name)
            identifiers = _code_identifiers(
                "\n".join(
                    line[4:] if line.startswith("    ") else line
                    for line in inspect.getsource(method).splitlines()
                )
            )
            for token in ("cloud_evidence_provider", "cloud_family", "hostname"):
                offenders = {name for name in identifiers if token in name}
                self.assertEqual(
                    offenders,
                    set(),
                    msg=f"{method_name} must not select transport by {token!r}: {offenders}",
                )


if __name__ == "__main__":
    unittest.main()
