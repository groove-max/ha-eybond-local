"""Architecture guard for the collector-metadata ownership boundary (Phase 4).

Confirms the generic hub holds no metadata wire (FC parameter constants / AT
command strings) and never selects a metadata transport via hasattr/getattr; the
route authority never routes by collector kind / cloud family / hostname / peer
IP / driver key / expected protocol; the management module has no metadata API;
the metadata service never writes a collector setting and the readers never run
an endpoint/apply/reboot action; and peer IP is not a channel ownership key.
"""

from __future__ import annotations

import ast
import inspect
import sys
import unittest
from dataclasses import fields
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

from custom_components.eybond_local.collector.metadata import (  # noqa: E402
    AT_METADATA_CHANNEL,
    FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
    FRAMED_METADATA_CHANNEL,
    CollectorMetadataRoute,
    CollectorMetadataRouteSet,
)

_CC = REPO_ROOT / "custom_components" / "eybond_local"
_HUB = _CC / "runtime" / "hub.py"
_MANAGEMENT = _CC / "collector" / "management.py"
_SERVICE = _CC / "runtime" / "collector_metadata.py"
_READERS = _CC / "collector" / "metadata.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# FC parameter constants / AT command strings / wire helpers that the hub must
# no longer name: metadata wire knowledge lives in collector.metadata and the
# parameter/AT registries, not in the generic runtime hub.
_HUB_FORBIDDEN_METADATA_WIRE_TOKENS = (
    "QUERY_HARDWARE_VERSION",
    "COLLECTOR_PARAMETER_DEFINITION_BY_ID",
    "query_runtime_collector_values",
    "query_runtime_collector_at_values",
    "CollectorWireManagementSession",
    "parse_query_collector_response",
    "async_query_bridge_hardware_version",
    "collector:fc_metadata",
    "collector:at_metadata",
    "CLDSRVHOST1",
    "DTUPN",
)

# Discriminators that must never influence metadata route authority. ``peer_ip``
# AND the configured ``collector_ip`` are both forbidden: a network target is not
# ownership evidence.
_FORBIDDEN_ROUTE_TOKENS = (
    "cloud_family",
    "collector_kind",
    "hostname",
    "peer_ip",
    "collector_ip",
    "driver_key",
    "_expected_collector_session_protocol",
)


class HubMetadataWireGuardTests(unittest.TestCase):
    def test_hub_holds_no_metadata_wire_tokens(self) -> None:
        source = _read(_HUB)
        for token in _HUB_FORBIDDEN_METADATA_WIRE_TOKENS:
            self.assertNotIn(
                token, source, msg=f"hub.py must not name metadata wire token {token!r}"
            )

    def test_hub_metadata_read_does_not_select_transport(self) -> None:
        # The metadata read is a thin delegate: it must not hasattr/getattr a
        # transport, name a transport-capability method, or pick framed/AT.
        from custom_components.eybond_local.runtime import hub as hub_module

        source = inspect.getsource(
            hub_module.EybondHub._async_read_collector_runtime_values
        )
        source = "\n".join(
            line[4:] if line.startswith("    ") else line for line in source.splitlines()
        )
        identifiers = _code_identifiers(source)
        for token in (
            "hasattr",
            "getattr",
            "active_transport",
            "active_collector_at_transport",
            "collector_at_transport",
            "async_send_collector",
        ):
            self.assertNotIn(
                token,
                identifiers,
                msg=f"metadata read must not select a transport via {token!r}",
            )
        # It DOES consume the link's public route facade.
        self.assertIn("collector_metadata_routes", identifiers)


class RouteAuthorityGuardTests(unittest.TestCase):
    def test_route_facade_uses_no_discriminators(self) -> None:
        import custom_components.eybond_local.runtime.link as link_module

        source = inspect.getsource(
            link_module.EybondRuntimeLinkManager.collector_metadata_routes
        )
        source += inspect.getsource(
            link_module.EybondRuntimeLinkManager._collector_bootstrap_claimable
        )
        source = "\n".join(
            line[4:] if line.startswith("    ") else line for line in source.splitlines()
        )
        identifiers = _code_identifiers(source)
        for token in _FORBIDDEN_ROUTE_TOKENS:
            offenders = {name for name in identifiers if token in name}
            self.assertEqual(
                offenders,
                set(),
                msg=f"metadata route authority must not depend on {token!r}: {offenders}",
            )


class ManagementModuleHasNoMetadataApiTests(unittest.TestCase):
    def test_management_module_has_no_metadata_api(self) -> None:
        source = _read(_MANAGEMENT)
        for token in (
            "collector_metadata",
            "CollectorMetadataService",
            "async_read_framed_metadata",
            "async_read_at_metadata",
            "async_read_framed_hardware_bootstrap",
            "query_runtime_collector_values",
            "query_runtime_collector_at_values",
        ):
            self.assertNotIn(
                token, source, msg=f"management.py must not use metadata API {token!r}"
            )


class MetadataServiceIsolationTests(unittest.TestCase):
    def test_service_does_not_import_driver_command_support(self) -> None:
        source = _read(_SERVICE)
        self.assertNotIn("drivers.command_support", source)
        self.assertNotIn("command_support", source)
        identifiers = _code_identifiers(source)
        for token in (
            "record_command_failure",
            "record_command_success",
            "command_skipped_as_unsupported",
            "seed_unsupported_commands",
            "commit_cycle_failures",
        ):
            self.assertNotIn(token, identifiers, msg=token)

    def test_service_metadata_state_does_not_ride_driver_option(self) -> None:
        source = _read(_SERVICE)
        # The driver negative-cache table name must never appear in the metadata
        # service: metadata dead channels persist under their OWN option key.
        self.assertNotIn("driver_unsupported_commands", source)

    def test_service_keys_identity_on_pn_not_ip(self) -> None:
        identifiers = _code_identifiers(_read(_SERVICE))
        self.assertIn("pn_is_same_identity", identifiers)
        for token in ("collector_ip", "peer_ip", "remote_ip"):
            offenders = {name for name in identifiers if token in name}
            self.assertEqual(offenders, set(), msg=f"service must not key on {token!r}: {offenders}")


class MetadataServiceDoesNotWriteSettingsTests(unittest.TestCase):
    def test_service_never_writes_a_collector_setting(self) -> None:
        identifiers = _code_identifiers(_read(_SERVICE))
        for token in (
            "async_write",
            "set_collector",
            "async_send_collector_reboot_or_apply",
            "SET_SERVER_ENDPOINT",
            "SET_REBOOT_OR_APPLY",
            "async_write_endpoint",
            "async_apply_changes",
            "async_reboot",
        ):
            self.assertNotIn(
                token,
                identifiers,
                msg=f"metadata service must not name write/action token {token!r}",
            )


class MetadataReaderRunsNoActionsTests(unittest.TestCase):
    def test_readers_run_no_endpoint_apply_or_reboot_action(self) -> None:
        identifiers = _code_identifiers(_read(_READERS))
        for token in (
            "async_write",
            "set_collector",
            "async_send_collector_reboot_or_apply",
            "async_write_endpoint",
            "async_apply_changes",
            "async_reboot",
            "INTPARA",
        ):
            self.assertNotIn(
                token,
                identifiers,
                msg=f"metadata reader must not run action token {token!r}",
            )


class PeerIpNotChannelOwnershipKeyTests(unittest.TestCase):
    def test_route_models_have_no_peer_ip_field(self) -> None:
        for model in (CollectorMetadataRoute, CollectorMetadataRouteSet):
            names = {field.name for field in fields(model)}
            self.assertNotIn("peer_ip", names, msg=model.__name__)

    def test_reader_module_does_not_name_peer_ip(self) -> None:
        self.assertNotIn("peer_ip", _read(_READERS))
        self.assertNotIn("peer_ip", _read(_SERVICE))


class ChannelIdNamespaceTests(unittest.TestCase):
    def test_metadata_channel_ids_are_collector_namespaced(self) -> None:
        # A metadata dead-channel verdict shares the negative-cache table with the
        # driver's unsupported-command set; the ``collector:`` namespace is what
        # keeps a metadata verdict from ever colliding with a driver command key.
        for channel in (
            FRAMED_METADATA_CHANNEL,
            FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
            AT_METADATA_CHANNEL,
        ):
            self.assertTrue(channel.startswith("collector:"), msg=channel)


if __name__ == "__main__":
    unittest.main()
