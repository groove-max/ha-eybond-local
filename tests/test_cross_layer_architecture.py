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
import textwrap
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
from custom_components.eybond_local.collector import transport_profile  # noqa: E402

_CC = REPO_ROOT / "custom_components" / "eybond_local"
_COORDINATOR = _CC / "runtime" / "coordinator.py"
_COORDINATOR_MODULES = tuple(
    sorted((_CC / "runtime").glob("coordinator*.py"))
)
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


def _coordinator_family_source() -> str:
    """All coordinator implementation modules as searchable source text."""

    return "\n".join(_read(path) for path in _COORDINATOR_MODULES)


def _coordinator_method_source(method_name: str) -> str:
    """Return one coordinator-family method body without importing runtime code."""

    for path in _COORDINATOR_MODULES:
        source = _read(path)
        tree = ast.parse(source)
        lines = source.splitlines()
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == method_name
            ):
                return textwrap.dedent(
                    "\n".join(lines[node.lineno - 1 : node.end_lineno])
                )
    raise AssertionError(f"coordinator method not found: {method_name}")


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
        source = _coordinator_family_source()
        identifiers = _code_identifiers(source)
        # The coordinator delegates the observed-protocol -> profile map; it must
        # not construct the profile nor hold the protocol-policy literals.
        self.assertNotIn("CollectorTransportProfile", identifiers)
        self.assertIn("apply_observed_collector_session_protocol", identifiers)
        for literal in ('"at_dtupn"', '"uart_write_same_value"', '"framed_heartbeat_then_fc2_pn"'):
            self.assertNotIn(
                literal, source, msg=f"coordinator must not hold transport-policy literal {literal}"
            )

    def test_confirmed_protocol_authority_owns_the_map(self) -> None:
        neutral_source = inspect.getsource(
            transport_profile.resolve_collector_transport_profile
        )
        observed_source = inspect.getsource(
            transport_profile.apply_observed_collector_session_protocol
        )

        # Entry/cloud metadata alone is deliberately wire-neutral.
        for literal in ("at_dtupn", "framed_heartbeat_then_fc2_pn"):
            self.assertNotIn(literal, neutral_source)
        for resolver in (
            "resolve_collector_cloud_raw_passthrough_bootstrap",
            "resolve_collector_cloud_raw_passthrough_frame_format",
            "resolve_collector_cloud_raw_passthrough_min_interval_ms",
        ):
            self.assertNotIn(resolver, neutral_source)
            # Cloud metadata may refine forwarding details only inside the
            # function that already received a confirmed observed protocol.
            self.assertIn(resolver, observed_source)

        source = _read(_TRANSPORT_PROFILE)
        for literal in ("at_dtupn", "framed_heartbeat_then_fc2_pn"):
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
    REPO_ROOT / "custom_components/eybond_local/connection/callback_identity.py"
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
            "async_scan",
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

        from custom_components.eybond_local.connection.callback_identity import (
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
        # The guard must never silently go blind: the config flow's
        # manual/reconfigure mapper has to remain in its scope.
        names = {path.name for path, _source in self._identity_consumers()}
        self.assertTrue(
            {"config_manual.py", "config_admission.py"}.issubset(names),
            msg=f"identity consumer guard lost the flow lifecycle: {names}",
        )

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

class StrategyTransitionAuthorityGuardTests(unittest.TestCase):
    """Batch 8: ONE authority writes connection_strategy on live entries.

    Allowed writer boundaries (each a different LIFECYCLE, not a different
    implementation): entry creation (config_flow), schema migration
    (__init__ / connection_policy), and the verified transition authority
    (connection/strategy_transition.py). Every OTHER production module —
    the runtime coordinator, selects, buttons, services, discovery — must
    contain NO write of the key at all.
    """

    ALLOWED_WRITER_FILES = {
        "config_flow.py",
        "config_entry.py",
        "options_runtime.py",
        "options_strategy.py",
        "integration_migration.py",
        "connection_policy.py",
        "strategy_transition.py",
        # The coordinator-independent degraded-repair orchestrator is part of
        # the ONE transition authority (it commits strategy only on a proven
        # callback recovery, same rule as strategy_transition.py).
        "strategy_transition_repair.py",
        "const.py",
        # Diagnostics snapshot builder: it MIRRORS the axes into a support
        # bundle dict; it never writes entry.data.
        "bundle.py",
    }

    @staticmethod
    def _strategy_key_writes(source: str) -> int:
        """Count writes of the connection_strategy key (assign or dict key)."""

        tree = ast.parse(source)

        def _is_key(node: ast.AST) -> bool:
            if isinstance(node, ast.Name):
                return node.id == "CONF_CONNECTION_STRATEGY"
            if isinstance(node, ast.Constant):
                return node.value == "connection_strategy"
            return False

        count = 0
        for node in ast.walk(tree):
            if isinstance(node, (ast.Assign, ast.AugAssign)):
                targets = (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
                for target in targets:
                    if isinstance(target, ast.Subscript) and _is_key(target.slice):
                        count += 1
            elif isinstance(node, ast.Dict):
                for key in node.keys:
                    if key is not None and _is_key(key):
                        count += 1
        return count

    def test_only_the_authority_and_lifecycle_boundaries_write_strategy(self) -> None:
        offenders: dict[str, int] = {}
        for path in sorted(_CC.rglob("*.py")):
            writes = self._strategy_key_writes(_read(path))
            if writes and path.name not in self.ALLOWED_WRITER_FILES:
                offenders[str(path.relative_to(_CC))] = writes
        self.assertEqual(
            offenders,
            {},
            msg=(
                "connection_strategy writes outside the allowed lifecycle "
                f"boundaries: {offenders}"
            ),
        )

    @staticmethod
    def _method_source(module_source: str, method_name: str) -> str:
        """Extract one method body via AST (no runtime import of the module)."""

        tree = ast.parse(module_source)
        lines = module_source.splitlines()
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == method_name
            ):
                return "\n".join(lines[node.lineno - 1 : node.end_lineno])
        raise AssertionError(f"method not found: {method_name}")

    def test_coordinator_has_no_strategy_write_and_no_writable_mode_setter(self) -> None:
        source = _coordinator_family_source()
        self.assertEqual(self._strategy_key_writes(source), 0)
        # CP2A: the writable collector operation-mode setter was removed. The
        # only user way to change the connection method is the options-flow
        # strategy transition (with mandatory risk consent). No coordinator
        # method may survive as a second writable operation-mode authority.
        with self.assertRaises(AssertionError):
            self._method_source(source, "async_set_collector_operation_mode")

    def test_operation_mode_has_no_production_writer(self) -> None:
        """CP2A: the legacy mode is input/projection only, never newly written."""

        def _mode_writes(source: str) -> int:
            tree = ast.parse(source)

            def _is_mode_key(node: ast.AST) -> bool:
                # Persisted entry writers use the canonical constant. Literal
                # runtime-value keys are the intentional read-only projection
                # published into snapshots/diagnostics and are checked below.
                return (
                    isinstance(node, ast.Name)
                    and node.id == "CONF_COLLECTOR_OPERATION_MODE"
                )

            count = 0
            for node in ast.walk(tree):
                if isinstance(node, (ast.Assign, ast.AugAssign)):
                    targets = (
                        node.targets if isinstance(node, ast.Assign) else [node.target]
                    )
                    count += sum(
                        isinstance(target, ast.Subscript)
                        and _is_mode_key(target.slice)
                        for target in targets
                    )
                elif isinstance(node, ast.Dict):
                    count += sum(
                        key is not None and _is_mode_key(key) for key in node.keys
                    )
            return count

        offenders = {
            str(path.relative_to(_CC)): writes
            for path in sorted(_CC.rglob("*.py"))
            if (writes := _mode_writes(_read(path)))
        }
        self.assertEqual(offenders, {})

        init_source = _read(_CC / "__init__.py")
        coordinator_source = _coordinator_family_source()
        self.assertNotIn("_async_self_heal_collector_operation_mode", init_source)
        self.assertNotIn("_sync_forced_collector_operation_mode", coordinator_source)
        self.assertNotIn("collector_operation_mode_change_reason", coordinator_source)
        self.assertEqual(
            coordinator_source.count('"collector_operation_mode"'),
            3,
            "only the three read-only runtime/snapshot projections may remain",
        )

    def test_full_control_endpoint_edit_never_touches_axes(self) -> None:
        # G: a raw endpoint edit is a low-level write, never a strategy switch
        # and never an axis writer of any kind.
        for method_name in (
            "async_set_raw_collector_server_endpoint",
            "async_set_collector_server_endpoint",
        ):
            method_source = _coordinator_method_source(method_name)
            for banned in (
                "CONF_CONNECTION_STRATEGY",
                "connection_strategy",
                "_persist_connection_axes",
            ):
                self.assertNotIn(
                    banned,
                    method_source,
                    msg=f"{method_name} must not write {banned}",
                )

    def test_bind_and_rollback_record_facts_never_strategy(self) -> None:
        for method_name in (
            "async_bind_collector_to_home_assistant",
            "async_rollback_collector_server_endpoint",
        ):
            method_source = _coordinator_method_source(method_name)
            self.assertNotIn(
                "CONF_CONNECTION_STRATEGY",
                method_source,
                msg=f"{method_name} may record endpoint facts, never strategy",
            )

    def test_authority_writes_strategy_only_in_success_updates(self) -> None:
        # Structural: inside strategy_transition.py the ONLY strategy writes
        # are the two success-commit ``updates`` dicts (inbound + callback).
        source = _read(_CC / "connection" / "strategy_transition.py")
        self.assertEqual(self._strategy_key_writes(source), 2)


class DegradedRepairBoundaryGuards(unittest.TestCase):
    """Batch 8B.1: the cold-repair transaction owns no lower-layer internals.

    The repair delegates ALL listener/wire/trigger/projection I/O to the public
    ``CallbackBootstrapChannel`` and ALL matching to the shared matcher, and the
    config flow holds no listener internals.
    """

    _REPAIR = _CC / "connection" / "strategy_transition_repair.py"
    _CONFIG_FLOW = _CC / "config_flow.py"

    def test_repair_imports_no_private_onboarding_or_transport_names(self) -> None:
        tree = ast.parse(_read(self._REPAIR))
        offenders: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            module = node.module
            if not ("onboarding" in module or "transport" in module):
                continue
            for alias in node.names:
                if alias.name.startswith("_"):
                    offenders.append(f"{module}.{alias.name}")
        self.assertEqual(
            offenders, [], f"repair imports private lower-layer names: {offenders}"
        )

    def test_repair_defines_no_second_matcher_and_uses_the_shared_one(self) -> None:
        source = _read(self._REPAIR)
        tree = ast.parse(source)
        defined = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        # The old parallel matcher is gone...
        self.assertNotIn("_new_strong_same_pn_session_id", defined)
        # ...and the ONE shared matcher is used instead of a re-derived rule.
        self.assertIn("match_callback_answer", _code_identifiers(source))

    def test_repair_has_no_raw_wire_switch_or_peer_or_family_identity(self) -> None:
        source = _read(self._REPAIR)
        seen = _code_identifiers(source) | _code_string_literals(source)
        # No raw protocol_shape/session_protocol wire selection; no peer IP /
        # cloud family / collector kind / hostname ever in identity or wire.
        forbidden = {
            "protocol_shape",
            "session_protocol",
            "peer_ip",
            "cloud_family",
            "collector_kind",
            "hostname",
        }
        self.assertEqual(
            forbidden & seen, set(), f"repair leaks lower-layer wire/identity: {forbidden & seen}"
        )

    def test_config_flow_touches_no_listener_internals(self) -> None:
        seen = set().union(
            *(
                _code_identifiers(_read(path))
                for path in sorted(_CC.glob("config_*.py"))
            )
        )
        for internal in (
            "_acquire_shared_listener",
            "_release_shared_listener",
            "async_identify_pending_session",
        ):
            self.assertNotIn(
                internal, seen, f"config_flow must not touch listener internal {internal}"
            )

    def test_repair_uses_the_channel_ledger_not_a_global(self) -> None:
        # BLOCKER 3 (8B.1 follow-up): ONE causality authority. The repair never
        # fetches the process-global ledger itself -- it uses channel.ledger, the
        # exact ledger the channel's sender records through -- so the lease that
        # admits the send and the counter that attributes it cannot diverge.
        ids = _code_identifiers(_read(self._REPAIR))
        self.assertNotIn("get_callback_trigger_ledger", ids)

    def test_repair_binds_identity_only_before_proof(self) -> None:
        # BLOCKER 2 (8B.1 follow-up): the pre-UDP intent is identity-only; a
        # socket-binding claim()/claim_session() is not used for it.
        ids = _code_identifiers(_read(self._REPAIR))
        self.assertIn("claim_identity", ids)


class ColdRepairHaTestGuards(unittest.TestCase):
    """Batch 8B.2A: the real-HA repair test proves a TRUE cold repair.

    Structurally bans the pre-8B.2A shortcuts that let the old test pass without
    exercising production wiring.
    """

    _HA_TEST = REPO_ROOT / "tests_ha" / "test_ha_strategy_transition_repair.py"

    def test_ha_repair_test_uses_no_cold_repair_shortcuts(self) -> None:
        source = _read(self._HA_TEST)
        banned = {
            # Manual listener wiring / private discovery internals.
            "_acquire_shared_listener": "manual shared-listener acquire",
            "_release_shared_listener": "manual shared-listener release",
            "._listeners": "private discovery listener injection",
            # Bypassing the real flow manager.
            "async_step_": "direct flow step call",
            ".async_remove(": "flow removal instead of options.async_abort",
            # Hand-written verification outcome / proof.
            "RecoveryVerificationOutcome(": "hand-built verification outcome",
            "CallbackRecoveryProof(": "hand-built recovery proof",
            # Manual registry claim / session pin / certification.
            "pin_owner_claim_to_session": "manual session pin",
            ".claim_session(": "manual transient session claim",
            "certify_permanent_owned_session": "manual certification",
            "retarget_claim_to_reconnected_session": "manual retarget",
        }
        found = [why for token, why in banned.items() if token in source]
        self.assertEqual(found, [], f"cold-repair HA test uses shortcuts: {found}")


class DegradedRepairLoadedLifecycleGuards(unittest.TestCase):
    """Batch 8B.2A loaded-lifecycle: a LOADED degraded entry is repaired by the
    ONE orchestrator (no LOADED refusal, no second matcher, no private coordinator
    access), and the activation retry re-runs ONLY the load -- never the repair or
    the proof state."""

    _OPTIONS_STRATEGY = _CC / "options_strategy.py"
    _OPTIONS_BASE = _CC / "options_base.py"
    _FLOW_PRESENTATION = _CC / "flow_presentation.py"

    @classmethod
    def _options_source(cls) -> str:
        return "\n".join(
            _read(path) for path in sorted(_CC.glob("options_*.py"))
        )

    @staticmethod
    def _method_source(module_source: str, method_name: str) -> str:
        tree = ast.parse(module_source)
        lines = module_source.splitlines()
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == method_name
            ):
                return "\n".join(lines[node.lineno - 1 : node.end_lineno])
        raise AssertionError(f"method not found: {method_name}")

    def test_no_dead_end_loaded_refusal_reason_anywhere(self) -> None:
        # The dead-end LOADED refusal is gone from the flow AND every locale: a
        # LOADED entry is repaired, never refused.
        self.assertNotIn(
            "transition_repair_requires_unloaded_entry", self._options_source()
        )
        for locale in ("en", "ru", "uk"):
            self.assertNotIn(
                "transition_fail_requires_unloaded_entry",
                _read(_CC / "flow_translations" / f"{locale}.json"),
            )

    def test_config_flow_never_calls_private_coordinator_persist(self) -> None:
        # The flow reaches the runtime only through public boundaries; it never
        # calls the coordinator's private without-reload persist helper.
        self.assertNotIn(
            "_async_update_entry_without_reload", self._options_source()
        )

    def test_loaded_repair_reuses_one_orchestrator_and_suspends_runtime(self) -> None:
        task = self._method_source(
            _read(self._OPTIONS_STRATEGY), "_async_run_degraded_repair_task"
        )
        # The ONE orchestrator -- never a second matcher/bootstrap/proof pipeline
        # inlined for the LOADED path.
        self.assertIn("async_run_degraded_recovery_repair", task)
        # The LOADED path SUSPENDS the competing runtime through the fail-closed
        # helper so the ONE orchestrator runs with exclusive session access.
        self.assertIn("_suspend_runtime_for_repair", task)
        for banned in (
            "async_persist_proven_callback_transition",
            "async_run_callback_bootstrap_transaction",
            "async_run_callback_recovery_transaction",
        ):
            self.assertNotIn(banned, task, f"repair task must not use {banned}")

    def test_suspend_is_fail_closed_before_ensure(self) -> None:
        source = _read(self._OPTIONS_STRATEGY)
        task = self._method_source(source, "_async_run_degraded_repair_task")
        suspend = self._method_source(source, "_suspend_runtime_for_repair")
        # The suspend runs BEFORE the listener ensure, and a refusal returns early
        # (0 ensure / UDP / commit downstream).
        i_suspend = task.index("_suspend_runtime_for_repair")
        i_ensure = task.index("async_ensure_observed_listener")
        self.assertLess(i_suspend, i_ensure, "suspend must precede the ensure")
        gate = task[i_suspend:i_ensure]
        self.assertIn("if refusal:", gate)
        self.assertIn("return", gate)
        # The suspend checks the unload RESULT + entry STATE, restores a partial
        # unload, and returns a typed reason -- never a bare unload.
        self.assertIn("async_unload", suspend)
        self.assertIn("ConfigEntryState", suspend)
        self.assertIn("transition_suspend_failed", suspend)

    def test_lifecycle_finalization_is_awaited_not_fire_and_forget(self) -> None:
        source = _read(self._OPTIONS_STRATEGY)
        task = self._method_source(source, "_async_run_degraded_repair_task")
        fin = self._method_source(source, "_finalize_repair_lifecycle")
        # ONE finalization boundary, AWAITED through the cancellation-safe helper.
        self.assertIn("_await_critical", task)
        self.assertIn("_finalize_repair_lifecycle", task)
        # No fire-and-forget restore/activation in the task or the finalization.
        for body, name in ((task, "repair task"), (fin, "finalization")):
            self.assertNotIn("async_create_task", body, f"{name}: no fire-and-forget")
            self.assertNotIn("ensure_future", body, f"{name}: no fire-and-forget")
        # The finalization owns the token release and the typed restore failure.
        self.assertIn("async_release_observed_listener", fin)
        self.assertIn("transition_restore_failed", fin)

    def test_no_suppress_around_restore_or_activation(self) -> None:
        source = _read(self._OPTIONS_STRATEGY)
        for method in (
            "_async_run_degraded_repair_task",
            "_finalize_repair_lifecycle",
            "_suspend_runtime_for_repair",
        ):
            body = self._method_source(source, method)
            self.assertNotIn(
                "suppress(", body,
                f"{method} must surface restore/activation errors, not suppress them",
            )

    def test_typed_suspend_and_restore_reasons_are_localized(self) -> None:
        flow = _read(self._FLOW_PRESENTATION)
        for code in ("transition_suspend_failed", "transition_restore_failed"):
            self.assertIn(code, flow)  # typed reasons in the explanations table
        for locale in ("en", "ru", "uk"):
            dynamic = _read(_CC / "flow_translations" / f"{locale}.json")
            for key in (
                "transition_fail_suspend_failed",
                "transition_fail_restore_failed",
            ):
                self.assertIn(key, dynamic, f"{key} missing from {locale}")

    def test_suspend_is_inside_the_cancellation_safe_boundary(self) -> None:
        # The suspend must run INSIDE the try whose finally awaits the ONE
        # finalization -- a cancel mid-suspend restores through that boundary.
        task = self._method_source(
            _read(self._OPTIONS_STRATEGY), "_async_run_degraded_repair_task"
        )
        i_try = task.index("\n        try:")
        i_suspend = task.index("_suspend_runtime_for_repair")
        i_finally = task.index("\n        finally:")
        i_await_critical = task.index("_await_critical")
        self.assertLess(i_try, i_suspend, "suspend must be inside the try")
        self.assertLess(i_suspend, i_finally, "suspend must precede the finally")
        self.assertLess(i_finally, i_await_critical, "finally awaits _await_critical")

    def test_suspend_marks_attempt_before_unload(self) -> None:
        suspend = self._method_source(
            _read(self._OPTIONS_STRATEGY), "_suspend_runtime_for_repair"
        )
        i_mark = suspend.index('"suspend_attempted"')
        i_unload = suspend.index("config_entries.async_unload")  # the CALL, not docs
        self.assertLess(
            i_mark, i_unload, "suspend_attempted must be set BEFORE the unload"
        )

    def test_await_critical_reraises_cancellation_after_completion(self) -> None:
        helper = self._method_source(_read(self._OPTIONS_STRATEGY), "_await_critical")
        # Tracks a cancel and re-raises it AFTER the shielded work completes -- a
        # successful result never turns a cancelled task into a normal completion.
        self.assertIn("cancelled = True", helper)
        self.assertIn("raise asyncio.CancelledError", helper)
        self.assertIn("asyncio.shield", helper)

    def test_init_menu_recovery_beats_capability_filtering(self) -> None:
        # Several classes define async_step_init; anchor on the unique options-flow
        # menu logic in the whole file rather than a mis-picked method.
        flow = _read(self._OPTIONS_BASE)
        i_proven = flow.index("not marker_present and self._callback_proven_but_not_loaded()")
        i_bridge = flow.index("if capabilities.virtual_bridge:")
        i_repair = flow.index('menu_options.insert(0, "strategy_transition_repair")')
        # The proven-but-unloaded activation-only menu is decided BEFORE the
        # capability (virtual-bridge) filtering...
        self.assertLess(i_proven, i_bridge, "activation-only menu precedes filtering")
        # ...and the repair is inserted AFTER the bridge branch, so the bridge can
        # never drop it.
        self.assertLess(i_bridge, i_repair, "bridge branch must not drop the repair")

    def test_listener_acquire_is_cancellation_safe(self) -> None:
        listener_source = _read(_CC / "collector" / "transport_listener.py")
        # The bind decrements the reserved refcount on ANY failure incl. cancel.
        acquire = self._method_source(listener_source, "acquire")
        self.assertIn("except BaseException", acquire)
        self.assertIn("_ref_count", acquire)
        # The locked get-or-create drops a never-bound listener on cancel too.
        locked = self._method_source(listener_source, "_acquire_listener_locked")
        self.assertIn("except BaseException", locked)
        # The public ensure releases the refcount if it never hands out a token.
        ensure = self._method_source(
            _read(_CC / "passive_discovery.py"), "async_ensure_observed_listener"
        )
        self.assertIn("_release_shared_listener", ensure)
        self.assertIn("except BaseException", ensure)

    def test_activation_retry_reloads_only_never_repairs_or_touches_proof(self) -> None:
        retry = self._method_source(
            _read(self._OPTIONS_STRATEGY),
            "async_step_strategy_transition_activation_retry",
        )
        # It re-runs ONLY the load...
        self.assertIn("async_reload", retry)
        # ...never the degraded-repair orchestrator...
        self.assertNotIn("async_run_degraded_recovery_repair", retry)
        # ...and never writes the RecoveryContract / recovery-state fields.
        for banned in (
            "merge_recovery_contract",
            "CONF_STRATEGY_TRANSITION_STATE",
            "recovery_contract",
            "async_update_entry",
        ):
            self.assertNotIn(
                banned, retry, f"activation retry must not touch {banned}"
            )


class CloudRollbackConvergenceGuardTests(unittest.TestCase):
    """CP2B.1: the cloud rollback context is a neutral, read-only convergence.

    It reuses the existing endpoint parser, registry and durable/observed facts;
    it introduces no second parser, no wire side effect and no new persistence.
    """

    _CONTEXT = _CC / "connection" / "strategy_transition_context.py"

    @staticmethod
    def _method_source(module_source: str, method_name: str) -> str:
        tree = ast.parse(module_source)
        lines = module_source.splitlines()
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == method_name
            ):
                return "\n".join(lines[node.lineno - 1 : node.end_lineno])
        raise AssertionError(f"method not found: {method_name}")

    def test_context_module_stays_neutral(self) -> None:
        # The neutral module imports nothing from config_flow / onboarding /
        # runtime / provider or cloud catalog layers.
        modules = _imported_modules(_read(self._CONTEXT))
        for forbidden in (
            "config_flow",
            "onboarding",
            "runtime",
            "hub",
            "coordinator",
            "metadata",
            "collector_cloud_profile_catalog_loader",
            "cloud_family",
        ):
            self.assertNotIn(forbidden, modules, f"neutral module imports {forbidden}")

    def test_resolver_reuses_the_single_endpoint_parser(self) -> None:
        # No second parser/normalizer: the resolver reuses the existing
        # provider-neutral inspect_collector_server_endpoint from the ONE parser
        # module, and imports no cloud catalog / default-port helper.
        source = _read(self._CONTEXT)
        identifiers = _code_identifiers(source)
        self.assertIn("inspect_collector_server_endpoint", identifiers)
        for banned in (
            "default_collector_server_port",
            "resolve_collector_cloud_default_port",
            "load_collector_cloud_profile_catalog",
            "collector_cloud_family_observation_from_endpoint",
        ):
            self.assertNotIn(banned, identifiers, f"resolver uses cloud helper {banned}")

    def test_read_only_boundary_has_no_wire_or_persistence(self) -> None:
        # The coordinator boundary gathers facts and returns a typed context: it
        # never writes an endpoint/reboot/UDP, never persists an entry/registry,
        # and never mints a proof / RecoveryContract / recovery state.
        boundary = self._method_source(
            _coordinator_method_source("collector_cloud_rollback_context"),
            "collector_cloud_rollback_context",
        )
        for banned in (
            "write_endpoint",
            "apply_changes",
            "reboot",
            "async_send_callback_trigger",
            "async_schedule_reload",
            "_async_update_entry",
            "remember_collector_original_endpoint",
            "save_collector_registry",
            "CONF_ENDPOINT_WRITTEN_VALUE",
            "CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE =",
            "RecoveryContract.empty_for_pn",
            ".with_inbound_proof",
            ".with_callback_proof",
            ".write_to",
            "StrategyTransitionRecoveryState",
            "_default_cloud_upstream_endpoint",
            "collector_cloud_family",
        ):
            self.assertNotIn(banned, boundary, f"boundary references {banned}")
        # It DOES delegate to the neutral resolver and reuse the existing
        # read-only registry reader.
        self.assertIn("resolve_cloud_rollback_endpoint", boundary)
        self.assertIn("RecoveryContract.from_entry_data", boundary)
        self.assertIn("get_collector_registry_record", boundary)


class CloudRollbackSelectionAuthorityGuardTests(unittest.TestCase):
    """CP2B.2: the typed CloudRollbackSelection is the ONE restore authority."""

    _CATALOG = _CC / "collector" / "cloud_rollback_catalog.py"

    @staticmethod
    def _facade_ast():
        tree = ast.parse(_coordinator_method_source("async_run_connection_strategy_transition"))
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == "async_run_connection_strategy_transition"
            ):
                return node
        raise AssertionError("transition facade not found")

    def test_callback_restore_endpoint_comes_only_from_typed_selection(self) -> None:
        facade = self._facade_ast()
        attrs = {n.attr for n in ast.walk(facade) if isinstance(n, ast.Attribute)}
        names = {n.id for n in ast.walk(facade) if isinstance(n, ast.Name)}
        args = {a.arg for a in ast.walk(facade) if isinstance(a, ast.arg)}
        # No PARALLEL authority read: the rollback_target property is never read
        # in the production transition (AST ignores the explanatory comment); it
        # stays a read-only diagnostic surface elsewhere.
        self.assertNotIn("collector_server_endpoint_rollback_target", attrs)
        # The facade forwards the capability without extracting a loose
        # endpoint; only the core authority may dereference endpoint_value.
        self.assertNotIn("endpoint_value", attrs)
        self.assertIn("cloud_rollback_selection", names | args)
        transition_tree = ast.parse(
            _read(_CC / "connection" / "strategy_transition.py")
        )
        callback = next(
            node
            for node in ast.walk(transition_tree)
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "_async_transition_to_callback"
        )
        callback_attrs = {
            n.attr for n in ast.walk(callback) if isinstance(n, ast.Attribute)
        }
        self.assertIn("endpoint_value", callback_attrs)

    def test_catalog_adapter_has_no_ui_or_runtime_dependency(self) -> None:
        modules = _imported_modules(_read(self._CATALOG))
        for forbidden in ("config_flow", "onboarding", "runtime", "hub", "coordinator"):
            self.assertNotIn(forbidden, modules)

    def test_selection_path_never_infers_endpoint_from_family_or_host(self) -> None:
        ids = _code_identifiers(_read(self._CATALOG))
        for banned in (
            "resolve_collector_cloud_family_by_host",
            "collector_cloud_family_observation_from_endpoint",
            "collector_cloud_family",
        ):
            self.assertNotIn(banned, ids, f"selection path infers via {banned}")

    def test_writable_operation_mode_select_not_reintroduced(self) -> None:
        # CP2A stays enforced: no writable operation-mode select machinery returns.
        select_source = _read(_CC / "select.py")
        self.assertNotIn('key="collector_operation_mode"', select_source)
        self.assertNotIn("async_set_collector_operation_mode", select_source)


class CollectorEndpointOperationAuthorityGuardTests(unittest.TestCase):
    """CP2C: exactly ONE endpoint-operation authority; every writer owns it."""

    _AUTHORITY = _CC / "connection" / "collector_endpoint_operation.py"
    _STRATEGY = _CC / "connection" / "strategy_transition.py"

    @staticmethod
    def _method_source(module_source: str, method_name: str) -> str:
        tree = ast.parse(module_source)
        lines = module_source.splitlines()
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == method_name
            ):
                return "\n".join(lines[node.lineno - 1 : node.end_lineno])
        raise AssertionError(f"method not found: {method_name}")

    def test_exactly_one_authority_type_and_singleton(self) -> None:
        src = _read(self._AUTHORITY)
        self.assertEqual(src.count("class CollectorEndpointOperationAuthority"), 1)
        # The module-level singleton is instantiated exactly once.
        self.assertEqual(src.count("CollectorEndpointOperationAuthority()"), 1)

    def test_authority_is_neutral(self) -> None:
        # The authority coordinates only: no wire, no strategy, no address facts.
        modules = _imported_modules(_read(self._AUTHORITY))
        for forbidden in ("config_flow", "onboarding", "runtime", "hub", "coordinator"):
            self.assertNotIn(forbidden, modules)
        ids = _code_identifiers(_read(self._AUTHORITY))
        for banned in ("async_write_endpoint", "cloud_family", "peer_ip", "hostname"):
            self.assertNotIn(banned, ids)

    def test_strategy_lease_facade_delegates_to_the_one_authority(self) -> None:
        src = _read(self._STRATEGY)
        self.assertIn("COLLECTOR_ENDPOINT_OPERATION_AUTHORITY", src)
        # The old standalone boolean lease set is gone (single authority now).
        self.assertNotIn("self._held: set[str]", src)

    # ---- CP2C blocker 8: the AST-enumerated writer guard ----
    #
    # Any raw runtime mutation of the collector endpoint/route/system flows through
    # one of these ``self._runtime`` methods. The guard finds EVERY coordinator
    # method that calls one (so a NEW writer cannot slip in behind a manual list)
    # and proves each is owned -- directly, or (for a private helper) only through
    # callers that are themselves owned.
    _RUNTIME_WIRE_METHODS = frozenset(
        {
            "async_set_collector_server_endpoint",
            "async_reboot_collector",
            "async_trigger_reverse_discovery",
            "async_apply_collector_changes",
        }
    )
    # Owning the ONE authority == a body containing any of these call markers.
    _OWNERSHIP_MARKERS = (
        "_collector_endpoint_operation(",  # transient guard context manager
        "_OP_AUTHORITY.acquire",  # long-lived start + reconcile
        "_OP_AUTHORITY.adopt",  # long-lived stop / recovery
        "STRATEGY_TRANSITION_LEASES.acquire",  # transition facade
        "STRATEGY_REPAIR_LEASES.acquire",  # degraded-repair facade
    )

    @staticmethod
    def _coordinator_methods() -> tuple[ast.FunctionDef | ast.AsyncFunctionDef, ...]:
        methods: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
        for path in _COORDINATOR_MODULES:
            tree = ast.parse(_read(path))
            for node in tree.body:
                if not isinstance(node, ast.ClassDef):
                    continue
                if not (
                    node.name == "EybondLocalCoordinator"
                    or node.name.startswith("Coordinator")
                ):
                    continue
                methods.extend(
                    method
                    for method in node.body
                    if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                )
        if not methods:
            raise AssertionError("coordinator-family methods not found")
        return tuple(methods)

    @classmethod
    def _direct_endpoint_writers(
        cls, methods: tuple[ast.FunctionDef | ast.AsyncFunctionDef, ...]
    ) -> set[str]:
        """Every coordinator method whose body calls a raw ``self._runtime`` writer."""

        writers: set[str] = set()
        for method in methods:
            for call in ast.walk(method):
                if (
                    isinstance(call, ast.Attribute)
                    and call.attr in cls._RUNTIME_WIRE_METHODS
                    and isinstance(call.value, ast.Attribute)
                    and call.value.attr == "_runtime"
                ):
                    writers.add(method.name)
                    break
        return writers

    @staticmethod
    def _self_call_graph(
        methods: tuple[ast.FunctionDef | ast.AsyncFunctionDef, ...]
    ) -> dict[str, set[str]]:
        """method -> set of sibling methods it calls via ``self.<name>(...)``."""

        method_names = {method.name for method in methods}
        graph: dict[str, set[str]] = {}
        for method in methods:
            callees: set[str] = set()
            for call in ast.walk(method):
                if (
                    isinstance(call, ast.Attribute)
                    and isinstance(call.value, ast.Name)
                    and call.value.id == "self"
                    and call.attr in method_names
                ):
                    callees.add(call.attr)
            graph[method.name] = callees
        return graph

    def test_every_runtime_endpoint_writer_is_owned(self) -> None:
        # Enumerate EVERY raw runtime endpoint/route/system writer from the AST and
        # prove each is owned by the ONE authority. A private helper is allowed to
        # skip self-acquisition ONLY if every path that reaches it passes through a
        # guarded owner (proven by the recursive closure below). A NEW unguarded
        # writer -- public or private -- fails here.
        methods = self._coordinator_methods()
        writers = self._direct_endpoint_writers(methods)
        graph = self._self_call_graph(methods)
        callers: dict[str, set[str]] = {name: set() for name in graph}
        for caller, callees in graph.items():
            for callee in callees:
                callers.setdefault(callee, set()).add(caller)

        def self_guards(method_name: str) -> bool:
            body = _coordinator_method_source(method_name)
            return any(marker in body for marker in self._OWNERSHIP_MARKERS)

        # A method is owned if it self-guards, OR it is a private helper whose
        # EVERY caller is itself owned (and it has at least one caller -- an
        # orphan private writer with no owned entry point is a hole).
        _resolving: set[str] = set()
        _cache: dict[str, bool] = {}

        def is_owned(method_name: str) -> bool:
            if method_name in _cache:
                return _cache[method_name]
            if self_guards(method_name):
                _cache[method_name] = True
                return True
            if not method_name.startswith("_"):
                # A PUBLIC writer must own the authority itself -- it is an API
                # entry point and cannot borrow ownership from a caller.
                _cache[method_name] = False
                return False
            if method_name in _resolving:
                # A cycle among private helpers proves no guarded entry -> unowned.
                return False
            _resolving.add(method_name)
            method_callers = callers.get(method_name, set())
            owned = bool(method_callers) and all(
                is_owned(c) for c in method_callers
            )
            _resolving.discard(method_name)
            _cache[method_name] = owned
            return owned

        unowned = sorted(w for w in writers if not is_owned(w))
        self.assertEqual(
            unowned,
            [],
            f"unguarded collector endpoint writer(s) with no owned entry: {unowned}",
        )
        # Sanity: the guard actually saw the known writers (a silent empty set
        # would make the assertion above vacuously pass).
        self.assertIn("async_set_raw_collector_server_endpoint", writers)
        self.assertIn("async_apply_collector_changes", writers)
        self.assertIn("async_reboot_collector", writers)
        self.assertIn("async_trigger_collector_rediscovery", writers)
        self.assertIn("_async_reconcile_managed_collector_endpoint", writers)
        # The transition facade maps a busy authority to the typed reason.
        facade = _coordinator_method_source("async_run_connection_strategy_transition")
        self.assertIn("COLLECTOR_ENDPOINT_OPERATION_BUSY", facade)

    def test_no_dead_operation_kinds(self) -> None:
        # Every declared operation kind must be exercised by production code (as
        # its constant name), so a kind cannot be declared and then never owned --
        # e.g. OPERATION_STRATEGY_REPAIR must be wired to the repair facade.
        auth_src = _read(self._AUTHORITY)
        kinds = {
            node.targets[0].id
            for node in ast.walk(ast.parse(auth_src))
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id.startswith("OPERATION_")
        }
        self.assertIn("OPERATION_STRATEGY_REPAIR", kinds)
        production = "\n".join(
            _read(path)
            for path in (
                _COORDINATOR,
                *_COORDINATOR_MODULES,
                self._STRATEGY,
                _CC / "connection" / "strategy_transition_repair.py",
            )
        )
        dead = sorted(k for k in kinds if k not in production)
        self.assertEqual(dead, [], f"declared but never used operation kind(s): {dead}")


if __name__ == "__main__":
    unittest.main()
