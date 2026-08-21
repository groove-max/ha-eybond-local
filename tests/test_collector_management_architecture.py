"""Architecture guard for CollectorManagementAdapter ownership.

Confirms the generic runtime holds no collector-management wire, the adapter
selection depends ONLY on the negotiated wire (never collector kind / hostname /
cloud family / peer IP / expected protocol), the management module is
provider-neutral, there is no dead metadata API, and unknown/conflict fail closed.
The source-token checks scan the WHOLE file, so moving wire code into a neighbour
helper cannot bypass them.
"""

from __future__ import annotations

import ast
import inspect
import sys
import unittest
from pathlib import Path


def _code_identifiers(source: str) -> set[str]:
    """Return every identifier used in CODE (never docstrings/comments)."""

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


from custom_components.eybond_local.collector import management as management_module
from custom_components.eybond_local.collector.management import (
    AtTextCollectorManagementAdapter,
    CollectorManagementAdapter,
    FramedCollectorManagementAdapter,
    UnavailableCollectorManagementAdapter,
    select_collector_management_adapter,
)
from custom_components.eybond_local.connection.session_handle import (
    ADAPTER_COLLECTOR_AT_COMMANDS,
    ADAPTER_COLLECTOR_FRAMED_COMMANDS,
    ADAPTER_NONE,
)

_CC = REPO_ROOT / "custom_components" / "eybond_local"
_HUB_ROOT = _CC / "runtime" / "hub"
_HUB = _HUB_ROOT / "root.py"
_HUB_FAMILY = tuple(sorted(_HUB_ROOT.glob("*.py")))
_COORDINATOR_ROOT = _CC / "runtime" / "coordinator"
_COORDINATOR = _COORDINATOR_ROOT / "root.py"
_COORDINATOR_FAMILY = tuple(sorted(_COORDINATOR_ROOT.glob("*.py")))
_MANAGEMENT = _CC / "collector" / "management.py"
# Collector-management ACTION wire knowledge that must live only in the adapter.
_FORBIDDEN_WIRE_TOKENS = (
    "SmartEssLocalSession",
    "CLDSRVHOST1",
    "INTPARA",
    "SET_SERVER_ENDPOINT",
    "SET_REBOOT_OR_APPLY",
    "async_send_collector_reboot_or_apply",
)

# Provider / non-wire discriminators that must never influence adapter selection.
# Code-attribute tokens (not prose words), so a docstring mention is not a match.
_FORBIDDEN_SELECTION_TOKENS = (
    "cloud_family",
    "collector_kind",
    "collector_cloud",
    "hostname",
    "peer_ip",
    "driver_key",
    "_configured_collector_session_protocol",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class WireEncodingGuardTests(unittest.TestCase):
    def test_hub_holds_no_management_wire_tokens(self) -> None:
        source = "\n".join(_read(path) for path in _HUB_FAMILY)
        for token in _FORBIDDEN_WIRE_TOKENS:
            self.assertNotIn(token, source, msg=f"hub.py must not name wire token {token!r}")

    def test_coordinator_holds_no_management_wire_tokens(self) -> None:
        source = "\n".join(_read(path) for path in _COORDINATOR_FAMILY)
        for token in _FORBIDDEN_WIRE_TOKENS:
            self.assertNotIn(
                token, source, msg=f"coordinator.py must not name wire token {token!r}"
            )


class SelectionAuthorityGuardTests(unittest.TestCase):
    def test_link_selection_uses_only_negotiated_management_adapter(self) -> None:
        # The ONE resolver (id + provenance) reads the negotiated
        # collector_management_adapter (live handle / confirmed binding) -- nothing
        # else. Inspecting the resolver itself prevents a bypass via a helper.
        import custom_components.eybond_local.runtime.link as link_module

        source = inspect.getsource(
            link_module.EybondRuntimeLinkManager._collector_management_selection
        )
        # De-indent so the method parses standalone.
        source = "\n".join(line[4:] if line.startswith("    ") else line for line in source.splitlines())
        identifiers = _code_identifiers(source)
        self.assertIn("collector_management_adapter", identifiers)
        for token in _FORBIDDEN_SELECTION_TOKENS:
            offenders = {name for name in identifiers if token in name}
            self.assertEqual(
                offenders,
                set(),
                msg=f"management selection must not depend on {token!r}: {offenders}",
            )

    def test_factory_has_no_provider_inputs(self) -> None:
        params = set(inspect.signature(select_collector_management_adapter).parameters)
        self.assertEqual(
            params,
            {"adapter_id", "framed_transport_provider", "at_transport_provider"},
        )
        for token in _FORBIDDEN_SELECTION_TOKENS + ("collector_cloud_family",):
            self.assertNotIn(token, params)


class ManagementModuleProviderNeutralityTests(unittest.TestCase):
    def test_management_module_is_provider_neutral(self) -> None:
        # CODE identifiers only (docstrings/comments may name what it avoids).
        identifiers = _code_identifiers(_read(_MANAGEMENT))
        for token in (
            "cloud_family",
            "cloud_provider",
            "collector_kind",
            "smartess",
            "catalog",
            "hostname",
            "peer_ip",
        ):
            offenders = {name for name in identifiers if token in name}
            self.assertEqual(
                offenders,
                set(),
                msg=f"management.py code must not depend on {token!r}: {offenders}",
            )

    def test_no_dead_metadata_api(self) -> None:
        self.assertFalse(hasattr(management_module, "CollectorMetadataSnapshot"))
        self.assertFalse(hasattr(CollectorManagementAdapter, "async_read_metadata"))
        for cls in (
            FramedCollectorManagementAdapter,
            AtTextCollectorManagementAdapter,
            UnavailableCollectorManagementAdapter,
        ):
            self.assertFalse(hasattr(cls, "async_read_metadata"), msg=cls.__name__)
        self.assertNotIn("async_read_metadata", _read(_MANAGEMENT))


class FailClosedBehaviourGuardTests(unittest.TestCase):
    def _providers(self):
        return {
            "framed_transport_provider": lambda: object(),
            "at_transport_provider": lambda: object(),
        }

    def test_none_and_conflict_ids_fail_closed(self) -> None:
        # ADAPTER_NONE (the value the link returns for conflict/unknown/no-evidence)
        # and any non-negotiated id select the fail-closed unavailable adapter.
        for adapter_id in (ADAPTER_NONE, "", "unknown", "eybond_framed", "at_text"):
            adapter = select_collector_management_adapter(adapter_id, **self._providers())
            self.assertIsInstance(adapter, UnavailableCollectorManagementAdapter)
            caps = adapter.capabilities
            self.assertFalse(
                any((caps.read_endpoint_state, caps.write_endpoint, caps.apply_changes, caps.reboot))
            )

    def test_only_negotiated_ids_select_a_real_adapter(self) -> None:
        self.assertIsInstance(
            select_collector_management_adapter(
                ADAPTER_COLLECTOR_FRAMED_COMMANDS, **self._providers()
            ),
            FramedCollectorManagementAdapter,
        )
        self.assertIsInstance(
            select_collector_management_adapter(
                ADAPTER_COLLECTOR_AT_COMMANDS, **self._providers()
            ),
            AtTextCollectorManagementAdapter,
        )


if __name__ == "__main__":
    unittest.main()
