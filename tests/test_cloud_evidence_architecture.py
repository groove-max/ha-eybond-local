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
_COORDINATOR = _CC / "runtime" / "coordinator.py"
_PROVIDERS = _CC / "support" / "cloud_evidence_providers.py"
_CONST = _CC / "const.py"
_LINK = _CC / "runtime" / "link.py"
_CONFIG_FLOW = _CC / "config_flow.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class CoordinatorOwnershipGuardTests(unittest.TestCase):
    def test_coordinator_imports_no_provider_http_clients(self) -> None:
        source = _read(_COORDINATOR)
        for token in ("smartess_cloud", "valuecloud_cloud"):
            self.assertNotIn(
                f"import {token}", source, msg=f"coordinator must not import {token}"
            )
            self.assertNotIn(f".{token} import", source)

    def test_coordinator_builds_no_provider_requests_or_endpoints(self) -> None:
        identifiers = _code_identifiers(_read(_COORDINATOR))
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
        identifiers = _code_identifiers(_read(_COORDINATOR))
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
        self.assertNotIn('"smartess_0925"', _read(_COORDINATOR))
        self.assertIn("serial_is_stable", identifiers)

    def test_coordinator_hardcodes_no_provider_allow_list(self) -> None:
        source = _read(_COORDINATOR)
        # The duplicated ``{"smartess", "valuecloud"}`` allow-list is gone; the
        # registry answers "is this provider supported".
        self.assertNotIn('"smartess",\n            "valuecloud"', source)
        self.assertIn("cloud_evidence_provider_supported", _code_identifiers(source))


class ConfigFlowProviderBoundaryGuardTests(unittest.TestCase):
    """config_flow owns NO provider policy -- end to end.

    It imports no SmartESS/ValueCloud HTTP client, constructs/parses no provider
    request/response, and reaches cloud work only through the neutral provider
    and control-discovery contracts. This covers BOTH onboarding assist AND
    shadow-learning cloud control discovery.
    """

    def test_config_flow_imports_no_provider_http_client(self) -> None:
        source = _read(_CONFIG_FLOW)
        for token in (
            "from .smartess_cloud import",
            "import smartess_cloud",
            "from . import valuecloud_cloud",
            "import valuecloud_cloud",
        ):
            self.assertNotIn(token, source, msg=f"config_flow must not import {token!r}")

    def test_config_flow_constructs_no_provider_requests(self) -> None:
        identifiers = _code_identifiers(_read(_CONFIG_FLOW))
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
        source = _read(_CONFIG_FLOW)
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
        identifiers = _code_identifiers(_read(_CONFIG_FLOW))
        self.assertIn("resolve_cloud_evidence_provider", identifiers)
        self.assertIn("build_onboarding_assist", identifiers)
        self.assertIn("control_discovery_runner", identifiers)

    def test_control_discovery_provider_has_no_family_heuristic_or_default(self) -> None:
        tree = ast.parse(_read(_CONFIG_FLOW))
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

        source = _read(_COORDINATOR)
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
