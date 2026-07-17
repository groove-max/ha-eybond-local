"""Cross-layer architecture guards (Phase 6 final audit).

Focused invariants that survived the responsibility-boundary refactor:

* provider provenance-inference set and the provider registry cannot drift;
* a provider never returns another provider's control-discovery runner;
* the observed-protocol -> transport-profile map lives in the transport-profile
  authority, not the runtime coordinator;
* the collector-management adapter and inverter-forward adapter are selected only
  from negotiated session evidence -- never cloud family / hostname / peer IP.

AST/attribute checks are used instead of brittle whole-file string scans wherever
possible.
"""

from __future__ import annotations

import ast
import inspect
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support import cloud_evidence as cloud_evidence_module  # noqa: E402
from custom_components.eybond_local.support import (  # noqa: E402
    cloud_evidence_providers as providers_module,
)
from custom_components.eybond_local.support.cloud_evidence_providers import (  # noqa: E402
    resolve_cloud_evidence_provider,
    supported_cloud_evidence_providers,
)

_CC = REPO_ROOT / "custom_components" / "eybond_local"
_COORDINATOR = _CC / "runtime" / "coordinator.py"
_TRANSPORT_PROFILE = _CC / "collector" / "transport_profile.py"


def _code_identifiers(source: str) -> set[str]:
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
    return names


def _code_string_literals(source: str) -> set[str]:
    """String literals in real code, excluding docstrings.

    _code_identifiers only sees AST names, so a rule smuggled in as a literal
    (a state name, a wire name) is invisible to it. Docstrings are excluded so a
    module may still DESCRIBE the rules it delegates.
    """

    tree = ast.parse(source)
    docstrings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                docstrings.add(doc)
    literals: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in docstrings:
                continue
            literals.add(node.value)
    return literals


def _imported_modules(source: str) -> set[str]:
    """Every module named by an import (dotted parts included).

    _code_identifiers cannot see these: `import x.y` binds no ast.Name.
    """

    modules: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.update(alias.name.split("."))
        elif isinstance(node, ast.ImportFrom):
            modules.update((node.module or "").split("."))
            modules.update(alias.name for alias in node.names)
    return {part for part in modules if part}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class ProviderAuthorityDriftGuardTests(unittest.TestCase):
    def test_provenance_set_matches_provider_registry(self) -> None:
        # The provenance-inference known set (cloud_evidence, a lower layer) and
        # the provider registry (cloud_evidence_providers) MUST enumerate the same
        # providers. If they drift, a new provider's records would be judged
        # unknown-provenance and its own load_latest would refuse them.
        registry = set(supported_cloud_evidence_providers())
        provenance = set(cloud_evidence_module._KNOWN_EVIDENCE_PROVIDERS)
        self.assertEqual(
            registry,
            provenance,
            msg="provider registry and provenance-inference set drifted",
        )

    def test_provider_registry_is_the_only_impl_map(self) -> None:
        self.assertEqual(set(providers_module._PROVIDERS), set(supported_cloud_evidence_providers()))


class ProviderControlDiscoveryIsolationGuardTests(unittest.TestCase):
    def test_provider_returns_only_its_own_control_discovery_runner(self) -> None:
        for provider_id in (*supported_cloud_evidence_providers(), "nope", ""):
            provider = resolve_cloud_evidence_provider(provider_id)
            runner = provider.control_discovery_runner()
            # A provider never hands out a FOREIGN provider's runner: the runner
            # is either its own or the fail-closed unavailable runner (id "").
            self.assertIn(
                runner.provider_id,
                {provider.provider_id, ""},
                msg=f"{provider_id!r} returned foreign runner {runner.provider_id!r}",
            )
            # An unsupported provider must not expose a usable runner.
            if not provider.provider_id:
                self.assertFalse(provider.control_discovery_available)


class TransportProfileAuthorityGuardTests(unittest.TestCase):
    def test_coordinator_holds_no_protocol_transport_policy_map(self) -> None:
        source = _read(_COORDINATOR)
        identifiers = _code_identifiers(source)
        # The coordinator delegates the observed-protocol -> profile map; it must
        # not construct the profile nor hold the protocol-policy literals.
        self.assertNotIn("CollectorTransportProfile", identifiers)
        self.assertIn("apply_observed_collector_session_protocol", identifiers)
        for literal in ('"at_dtupn"', '"uart_write_same_value"', '"framed_heartbeat_then_fc2_pn"'):
            self.assertNotIn(
                literal, source, msg=f"coordinator must not hold transport-policy literal {literal}"
            )

    def test_transport_profile_authority_owns_the_map(self) -> None:
        source = _read(_TRANSPORT_PROFILE)
        for literal in ("at_dtupn", "uart_write_same_value", "framed_heartbeat_then_fc2_pn"):
            self.assertIn(literal, source)


class AdapterSelectionAuthorityGuardTests(unittest.TestCase):
    def test_adapter_selection_uses_no_family_host_or_ip(self) -> None:
        import custom_components.eybond_local.runtime.link as link_module

        for method_name in ("_collector_management_selection", "_inverter_forward_adapter"):
            method = getattr(link_module.EybondRuntimeLinkManager, method_name)
            source = "\n".join(
                line[4:] if line.startswith("    ") else line
                for line in inspect.getsource(method).splitlines()
            )
            identifiers = _code_identifiers(source)
            for token in ("cloud_family", "hostname", "peer_ip", "remote_ip", "collector_kind"):
                offenders = {name for name in identifiers if token in name}
                self.assertEqual(
                    offenders,
                    set(),
                    msg=f"{method_name} must not select by {token!r}: {offenders}",
                )


_CALLBACK_IDENTITY = (
    REPO_ROOT / "custom_components/eybond_local/onboarding/callback_identity.py"
)


class WireNegotiationAuthorityGuardTests(unittest.TestCase):
    """There is exactly ONE authority that decides a session's live wire.

    negotiate_session_adapters / SessionHandle own every rule: untrusted
    lifecycle states, state-vs-shape conflicts, sniffed shape never overriding an
    untrusted state, and "persisted expected protocol is not live evidence". A
    second resolver in the onboarding layer would be free to drift from those
    rules -- and drift here means writing the wrong frame to a stranger's socket.
    """

    def test_callback_identity_does_not_use_the_transport_profile_resolver(self) -> None:
        source = _read(_CALLBACK_IDENTITY)
        identifiers = _code_identifiers(source)
        imports = _imported_modules(source)
        for banned in (
            "collector_session_protocol_from_inventory_state",
            "normalize_collector_session_protocol",
        ):
            self.assertNotIn(banned, identifiers)
            self.assertNotIn(banned, imports)
        # The resolver's whole MODULE is off limits, not just the one function.
        self.assertNotIn("transport_profile", imports)

    def test_callback_identity_delegates_to_the_session_handle_authority(self) -> None:
        identifiers = _code_identifiers(_read(_CALLBACK_IDENTITY))
        self.assertIn("negotiate_session_adapters", identifiers)

    def test_callback_identity_re_derives_no_wire_rule_of_its_own(self) -> None:
        literals = _code_string_literals(_read(_CALLBACK_IDENTITY))
        # The wire vocabulary and the untrusted-state list live in session_handle.
        # Holding any of them here means a second rule set that can drift.
        for literal in (
            "routed_framed",
            "routed_at_text",
            "waiting_for_route_identity",
            "parked_waiting_for_identity",
            "route_identity_mismatch",
            "eybond_framed_or_binary",
            "closed_no_payload",
            "raw_tcp",
        ):
            self.assertNotIn(
                literal,
                literals,
                msg=f"callback_identity must not re-derive wire rule {literal!r}",
            )


class CallbackIdentityIsolationGuardTests(unittest.TestCase):
    def test_callback_identity_has_no_driver_or_provider_dependency(self) -> None:
        source = _read(_CALLBACK_IDENTITY)
        code = _code_identifiers(source) | _imported_modules(source)
        for banned in (
            # driver detection must not exist before identity is confirmed
            "async_auto_detect",
            "async_deep_detect",
            "create_onboarding_manager",
            "driver_detection",
            "link_sweep",
            "DRIVER_HINT",
            # nor provider / cloud / collector-kind concerns
            "SmartEssLocalSession",
            "smartess",
            "cloud_family",
            "collector_kind",
            "bridge_kind",
        ):
            self.assertNotIn(
                banned, code, msg=f"callback_identity must not depend on {banned}"
            )

    def test_callback_identity_uses_the_shared_neutral_reader(self) -> None:
        # The ONE session-pinned reader lives in collector/session_identity_reader
        # (shared with the inbound recovery verifier); the transaction must use
        # it, and the reader itself must use the NEUTRAL management session --
        # never the SmartESS subclass.
        code = _code_identifiers(_read(_CALLBACK_IDENTITY)) | _imported_modules(
            _read(_CALLBACK_IDENTITY)
        )
        self.assertIn("SessionPinnedIdentityReader", code)
        reader_source = _read(_CC / "collector" / "session_identity_reader.py")
        reader_code = _code_identifiers(reader_source) | _imported_modules(reader_source)
        self.assertIn("CollectorWireManagementSession", reader_code)
        self.assertNotIn("SmartEssLocalSession", reader_code)
        self.assertNotIn("smartess_local", reader_code)


class CallbackIdentityIsNotRecoveryProofTests(unittest.TestCase):
    """IDENTITY proof != RECOVERY proof.

    A certified identity outcome states only that ONE live session belongs to
    ONE full PN. It says nothing about being able to re-establish contact after
    that session is gone, so the transaction has no business producing (or even
    naming) a connection strategy, strategy evidence, endpoint write, or any
    other recovery-shaped artifact. Recovery is a SEPARATE future proof (the
    RecoveryContract); these guards keep it from leaking in early.
    """

    def test_transaction_writes_no_strategy_evidence_or_endpoint(self) -> None:
        source = _read(_CALLBACK_IDENTITY)
        code = _code_identifiers(source) | _imported_modules(source)
        for banned in (
            # strategy / evidence vocabulary
            "CONF_CONNECTION_STRATEGY",
            "CONF_CONNECTION_STRATEGY_EVIDENCE",
            "CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER",
            "CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT",
            "CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION",
            "EVIDENCE_CALLBACK_TRIGGER",
            "EVIDENCE_REBOOT_RECONNECT",
            "EVIDENCE_USER_CONFIRMED_SESSION",
            "strategy_verification",
            # endpoint ownership / endpoint writes
            "CONF_ENDPOINT_CONTROL_POLICY",
            "CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT",
            "collector_endpoint",
            # entry mutation API -- the transaction owns no ConfigEntry
            "async_update_entry",
            "async_create_entry",
        ):
            self.assertNotIn(
                banned,
                code,
                msg=f"callback_identity must not name {banned}: identity is not recovery",
            )
        literals = _code_string_literals(source)
        for banned_literal in (
            "connection_strategy",
            "connection_strategy_evidence",
            "endpoint_control_policy",
            "collector_original_server_endpoint",
        ):
            self.assertNotIn(
                banned_literal,
                literals,
                msg=f"callback_identity must not smuggle key {banned_literal!r} as a literal",
            )

    def test_outcome_carries_identity_fields_only(self) -> None:
        from dataclasses import fields

        from custom_components.eybond_local.onboarding.callback_identity import (
            CallbackIdentityOutcome,
        )

        # A frozen field list: any new recovery-shaped field must be a
        # deliberate, reviewed change here first. ``silent_bootstrap_offer`` is
        # identity-attempt state (the typed continuation target a
        # user-selected bootstrap probe binds to on retry) -- it carries no
        # identity, no wire and no recovery claim of its own.
        self.assertEqual(
            {field.name for field in fields(CallbackIdentityOutcome)},
            {
                "result",
                "collector_pn",
                "session_id",
                "session_protocol",
                "identity_source",
                "handoff_owner",
                "silent_bootstrap_offer",
            },
        )
        outcome = CallbackIdentityOutcome(result="")
        for forbidden in (
            "connection_strategy",
            "connection_strategy_evidence",
            "evidence",
            "endpoint",
            "recovery_verified",
            "inbound",
            # the old ambiguous name: it read as "the callback way of
            # (re)connecting is confirmed", which identity cannot claim
            "confirmed",
        ):
            self.assertFalse(
                hasattr(outcome, forbidden),
                msg=f"CallbackIdentityOutcome must not expose {forbidden!r}",
            )

    def test_transaction_consults_no_persisted_protocol_or_peer_identity(self) -> None:
        source = _read(_CALLBACK_IDENTITY)
        code = _code_identifiers(source) | _imported_modules(source)
        literals = _code_string_literals(source)
        # A persisted/expected protocol is not live evidence; peer IP, hostname
        # and endpoint are route/diagnostics, never identity and never a wire
        # switch.
        # NOTE: the bare identifier ``collector_session_protocol`` is allowed --
        # the AT reader passes the LIVE negotiated wire under that kwarg. What
        # is banned is reading the PERSISTED entry key (the CONF_ constant / its
        # literal value) as if it were live evidence.
        for banned in (
            "CONF_COLLECTOR_SESSION_PROTOCOL",
            "hostname",
            # peer IP is route/diagnostics, never identity: the transaction may
            # not even read it (neither as an attribute nor as a mapping key).
            "peer_ip",
        ):
            self.assertNotIn(banned, code | literals, msg=f"banned: {banned}")


class IdentityConsumersNeverMintRecoveryEvidenceTests(unittest.TestCase):
    """No CONSUMER of the identity transaction may mint recovery evidence.

    The transaction-module guards above cannot see a mapper one layer up: a
    flow/pending consumer that takes ``identity_certified`` and stamps
    ``callback_trigger`` evidence re-creates the false recovery proof outside
    the guarded module. So the ban follows the IMPORT: every production module
    that imports ``callback_identity`` is forbidden from even naming the
    callback-trigger evidence vocabulary. Persisting the user's CHOSEN
    ``connection_strategy`` remains legal everywhere -- intent is not evidence.
    Legacy READERS (connection_policy's derivation for old entries) do not
    import the transaction and are deliberately untouched.
    """

    def _identity_consumers(self) -> list[tuple[Path, str]]:
        consumers: list[tuple[Path, str]] = []
        for path in sorted(_CC.rglob("*.py")):
            source = _read(path)
            if path.name == "callback_identity.py":
                continue  # the transaction itself has its own, stricter guards
            if "callback_identity" in _imported_modules(source):
                consumers.append((path, source))
        return consumers

    def test_the_guard_sees_the_real_consumers(self) -> None:
        # The guard must never silently go blind: the two known mappers (the
        # config flow's manual/reconfigure paths and the pending attempt) have
        # to be in its scope, or the ban below proves nothing.
        names = {path.name for path, _source in self._identity_consumers()}
        self.assertIn("config_flow.py", names)
        self.assertIn("pending_attempt.py", names)

    def test_identity_consumers_never_name_callback_trigger_evidence(self) -> None:
        for path, source in self._identity_consumers():
            code = _code_identifiers(source) | _imported_modules(source)
            literals = _code_string_literals(source)
            for banned in (
                "EVIDENCE_CALLBACK_TRIGGER",
                "CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER",
            ):
                self.assertNotIn(
                    banned,
                    code,
                    msg=(
                        f"{path.name} imports the identity transaction and must "
                        f"not name {banned}: identity_certified is never a "
                        "recovery proof"
                    ),
                )
            self.assertNotIn(
                "callback_trigger",
                literals,
                msg=(
                    f"{path.name} must not smuggle the callback_trigger evidence "
                    "value as a literal"
                ),
            )

    def test_pending_identity_mapper_never_touches_the_evidence_key(self) -> None:
        # The pending mapper has no legitimate evidence business at all (the
        # config flow, by contrast, still writes REAL inbound evidence from the
        # behavioral restart/reconnect verification).
        source = _read(_CC / "onboarding" / "pending_attempt.py")
        code = (
            _code_identifiers(source)
            | _imported_modules(source)
            | _code_string_literals(source)
        )
        for banned in (
            "CONF_CONNECTION_STRATEGY_EVIDENCE",
            "connection_strategy_evidence",
        ):
            self.assertNotIn(
                banned,
                code,
                msg=f"pending_attempt must not touch {banned}",
            )


if __name__ == "__main__":
    unittest.main()
