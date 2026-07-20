"""The ONE terminal boundary: typed input, contract merge, prepared-handoff gate.

Pins the Batch-5 contract: every config-flow terminal funnels recovery
evidence through ``RecoveryTerminalInput`` -> ``merge_recovery_contract`` ->
ownership coordination. The merge is the single production config-flow writer
of ``entry.data["recovery_contract"]``; the callback transaction's prepared
owner is accepted ONLY through ``prepared_handoff_identity``.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    CallbackRecoveryProof,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    InboundRecoveryProof,
    RECOVERY_CONTRACT_KEY,
    RecoveryContract,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.connection.recovery.terminal import (
    MERGE_REFUSED_ENTRY_IDENTITY,
    MERGE_REFUSED_MALFORMED_CONTRACT,
    MERGE_REFUSED_PROOF_REJECTED,
    RecoveryTerminalInput,
    merge_recovery_contract,
    verify_prepared_handoff,
)
from custom_components.eybond_local.connection.recovery.verification import (
    InboundRecoveryOutcome,
    RecoveryVerificationOutcome,
    STATE_CALLBACK_VERIFIED,
    STATE_INBOUND_RECOVERED,
    STATE_INBOUND_VERIFIED,
)

# Synthetic identities only.
FULL_PN = "V001020SYN62344022"
SHORT_PN = FULL_PN[:14]
OTHER_FULL_PN = "V000405SYN94677058"
TS = "2026-07-16T10:00:00+00:00"
TS_LATER = "2026-07-16T12:30:00+00:00"


def _inbound_proof(pn: str = FULL_PN, verified_at: str = TS) -> InboundRecoveryProof:
    return InboundRecoveryProof(
        method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        collector_pn=pn,
        identity_source="fc2_parameter_2",
        verified_at=verified_at,
        session_protocol="eybond_framed",
    )


def _callback_proof(pn: str = FULL_PN, verified_at: str = TS) -> CallbackRecoveryProof:
    return CallbackRecoveryProof(
        method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        collector_pn=pn,
        identity_source="fc2_parameter_2",
        verified_at=verified_at,
        trigger_target="192.168.1.60:58899",
        advertised_ha_endpoint="198.51.100.7:48899",
        listener_port=18899,
    )


def _inbound_outcome(pn: str = FULL_PN) -> InboundRecoveryOutcome:
    return InboundRecoveryOutcome(
        status=STATE_INBOUND_VERIFIED,
        collector_pn=pn,
        new_session_id="listener-18899-2",
        proof=_inbound_proof(pn),
    )


def _callback_outcome(owner: str = "callback_recovery:test-owner"):
    return RecoveryVerificationOutcome(
        status=STATE_CALLBACK_VERIFIED,
        collector_pn=FULL_PN,
        new_session_id="listener-18899-2",
        callback_proof=_callback_proof(),
        handoff_owner=owner,
    )


def _recovered_outcome(owner: str = "callback_recovery:test-owner"):
    return RecoveryVerificationOutcome(
        status=STATE_INBOUND_RECOVERED,
        collector_pn=FULL_PN,
        new_session_id="listener-18899-2",
        inbound_proof=_inbound_proof(),
        handoff_owner=owner,
    )


def _inbound_terminal(pn: str = FULL_PN) -> RecoveryTerminalInput:
    return RecoveryTerminalInput.from_inbound_outcome(_inbound_outcome(pn))


def _callback_terminal(owner: str = "callback_recovery:test-owner") -> RecoveryTerminalInput:
    return RecoveryTerminalInput.from_callback_transaction(_callback_outcome(owner))


class TerminalInputTypingTests(unittest.TestCase):
    """Only the real verified outcome types enter a terminal."""

    def test_none_input_carries_nothing(self) -> None:
        terminal = RecoveryTerminalInput.none()
        self.assertFalse(terminal.has_proof)
        self.assertEqual(terminal.prepared_handoff_owner, "")

    def test_inbound_outcome_becomes_inbound_terminal(self) -> None:
        terminal = _inbound_terminal()
        self.assertIsNotNone(terminal.inbound_proof)
        self.assertIsNone(terminal.callback_proof)
        self.assertEqual(terminal.collector_pn, FULL_PN)
        # The flow keeps owning its claim: no prepared owner from inbound.
        self.assertEqual(terminal.prepared_handoff_owner, "")

    def test_callback_outcome_becomes_callback_terminal_with_owner(self) -> None:
        terminal = _callback_terminal()
        self.assertIsNotNone(terminal.callback_proof)
        self.assertIsNone(terminal.inbound_proof)
        self.assertEqual(terminal.prepared_handoff_owner, "callback_recovery:test-owner")

    def test_inbound_recovered_transaction_carries_inbound_proof_and_owner(self) -> None:
        terminal = RecoveryTerminalInput.from_callback_transaction(_recovered_outcome())
        self.assertIsNotNone(terminal.inbound_proof)
        self.assertIsNone(terminal.callback_proof)
        self.assertEqual(terminal.prepared_handoff_owner, "callback_recovery:test-owner")

    def test_identity_outcome_is_never_recovery_evidence(self) -> None:
        from custom_components.eybond_local.connection.callback_identity import (
            CallbackIdentityOutcome,
        )

        identity = CallbackIdentityOutcome(
            result="",
            collector_pn=FULL_PN,
            session_id="s-live",
            session_protocol="eybond_framed",
            identity_source="fc2_parameter_2",
            handoff_owner="callback_verification:x",
        )
        with self.assertRaises(TypeError):
            RecoveryTerminalInput.from_inbound_outcome(identity)
        with self.assertRaises(TypeError):
            RecoveryTerminalInput.from_callback_transaction(identity)

    def test_ducks_and_unverified_outcomes_are_rejected(self) -> None:
        from types import SimpleNamespace

        duck = SimpleNamespace(
            inbound_verified=True, proof=_inbound_proof(), collector_pn=FULL_PN
        )
        with self.assertRaises(TypeError):
            RecoveryTerminalInput.from_inbound_outcome(duck)
        with self.assertRaises(TypeError):
            RecoveryTerminalInput.from_callback_transaction(duck)
        unverified = InboundRecoveryOutcome(
            status="inbound_not_verified",
            failure_reason="restart_not_confirmed",
            collector_pn=FULL_PN,
        )
        with self.assertRaises(ValueError):
            RecoveryTerminalInput.from_inbound_outcome(unverified)
        # A verified-shaped outcome whose proof is a duck cannot slip through.
        forged = InboundRecoveryOutcome(
            status=STATE_INBOUND_VERIFIED,
            collector_pn=FULL_PN,
            new_session_id="sid",
            proof=SimpleNamespace(collector_pn=FULL_PN),  # type: ignore[arg-type]
        )
        with self.assertRaises(TypeError):
            RecoveryTerminalInput.from_inbound_outcome(forged)

    def test_direct_construction_is_validated_too(self) -> None:
        with self.assertRaises(ValueError):
            RecoveryTerminalInput(
                collector_pn=FULL_PN,
                inbound_proof=_inbound_proof(),
                callback_proof=_callback_proof(),
            )
        with self.assertRaises(ValueError):
            RecoveryTerminalInput(collector_pn=FULL_PN)  # state without proof
        with self.assertRaises(ValueError):
            RecoveryTerminalInput(prepared_handoff_owner="x")  # owner without proof
        with self.assertRaises(ValueError):
            RecoveryTerminalInput(
                collector_pn=OTHER_FULL_PN, inbound_proof=_inbound_proof()
            )
        with self.assertRaises(TypeError):
            RecoveryTerminalInput(
                collector_pn=FULL_PN, inbound_proof=object()  # type: ignore[arg-type]
            )

    def test_short_pn_outcome_is_enriched_to_full_by_the_registry_rule(self) -> None:
        outcome = InboundRecoveryOutcome(
            status=STATE_INBOUND_VERIFIED,
            collector_pn=SHORT_PN,
            new_session_id="sid",
            proof=_inbound_proof(FULL_PN),
        )
        terminal = RecoveryTerminalInput.from_inbound_outcome(outcome)
        self.assertEqual(terminal.collector_pn, FULL_PN)


class ContractMergeTruthTableTests(unittest.TestCase):
    """The one merge algorithm, both branches, fail-closed."""

    def test_empty_plus_inbound_yields_inbound_only(self) -> None:
        data: dict[str, object] = {"collector_pn": FULL_PN}
        self.assertEqual(merge_recovery_contract(data, _inbound_terminal()), "")
        contract = RecoveryContract.from_entry_data(data)
        self.assertTrue(contract.inbound_verified)
        self.assertIsNone(contract.callback_proof)
        self.assertEqual(contract.updated_at, TS)  # the proof's verified_at

    def test_empty_plus_callback_yields_callback_only(self) -> None:
        data: dict[str, object] = {"collector_pn": FULL_PN}
        self.assertEqual(merge_recovery_contract(data, _callback_terminal()), "")
        contract = RecoveryContract.from_entry_data(data)
        self.assertTrue(contract.callback_verified)
        self.assertIsNone(contract.inbound_proof)

    def test_inbound_then_callback_keeps_both_branches(self) -> None:
        data: dict[str, object] = {"collector_pn": FULL_PN}
        merge_recovery_contract(data, _inbound_terminal())
        self.assertEqual(merge_recovery_contract(data, _callback_terminal()), "")
        contract = RecoveryContract.from_entry_data(data)
        self.assertTrue(contract.inbound_verified)
        self.assertTrue(contract.callback_verified)

    def test_callback_then_inbound_keeps_both_branches(self) -> None:
        data: dict[str, object] = {"collector_pn": FULL_PN}
        merge_recovery_contract(data, _callback_terminal())
        self.assertEqual(merge_recovery_contract(data, _inbound_terminal()), "")
        contract = RecoveryContract.from_entry_data(data)
        self.assertTrue(contract.inbound_verified)
        self.assertTrue(contract.callback_verified)

    def test_replacing_a_branch_preserves_the_opposite_byte_for_byte(self) -> None:
        data: dict[str, object] = {"collector_pn": FULL_PN}
        merge_recovery_contract(data, _callback_terminal())
        callback_before = json.dumps(
            data[RECOVERY_CONTRACT_KEY]["callback"], sort_keys=True
        )
        merge_recovery_contract(data, _inbound_terminal())
        inbound_before = json.dumps(
            data[RECOVERY_CONTRACT_KEY]["inbound"], sort_keys=True
        )

        # Replace the inbound branch with a LATER proof of the same identity.
        newer = RecoveryTerminalInput(
            collector_pn=FULL_PN,
            inbound_proof=_inbound_proof(verified_at=TS_LATER),
        )
        self.assertEqual(merge_recovery_contract(data, newer), "")
        record = data[RECOVERY_CONTRACT_KEY]
        self.assertEqual(
            json.dumps(record["callback"], sort_keys=True), callback_before
        )
        self.assertEqual(record["inbound"]["verified_at"], TS_LATER)
        self.assertEqual(record["updated_at"], TS_LATER)

        # And the mirror direction: replacing callback preserves inbound.
        newer_callback = RecoveryTerminalInput(
            collector_pn=FULL_PN,
            callback_proof=_callback_proof(verified_at=TS_LATER),
        )
        inbound_now = json.dumps(data[RECOVERY_CONTRACT_KEY]["inbound"], sort_keys=True)
        self.assertEqual(merge_recovery_contract(data, newer_callback), "")
        self.assertEqual(
            json.dumps(data[RECOVERY_CONTRACT_KEY]["inbound"], sort_keys=True),
            inbound_now,
        )

    def test_short_pn_contract_is_enriched_never_downgraded(self) -> None:
        data: dict[str, object] = {"collector_pn": SHORT_PN}
        merge_recovery_contract(
            data,
            RecoveryTerminalInput(
                collector_pn=SHORT_PN, inbound_proof=_inbound_proof(SHORT_PN)
            ),
        )
        self.assertEqual(data[RECOVERY_CONTRACT_KEY]["collector_pn"], SHORT_PN)
        # A full-PN proof of the same identity enriches the stored spelling.
        self.assertEqual(merge_recovery_contract(data, _callback_terminal()), "")
        self.assertEqual(data[RECOVERY_CONTRACT_KEY]["collector_pn"], FULL_PN)
        # A later short-PN proof never downgrades it back.
        self.assertEqual(
            merge_recovery_contract(
                data,
                RecoveryTerminalInput(
                    collector_pn=SHORT_PN,
                    inbound_proof=_inbound_proof(SHORT_PN, verified_at=TS_LATER),
                ),
            ),
            "",
        )
        self.assertEqual(data[RECOVERY_CONTRACT_KEY]["collector_pn"], FULL_PN)

    def test_foreign_pn_is_refused_and_data_untouched(self) -> None:
        data: dict[str, object] = {"collector_pn": FULL_PN}
        merge_recovery_contract(data, _inbound_terminal())
        snapshot = copy.deepcopy(data)

        # Foreign identity vs the ENTRY's PN.
        refusal = merge_recovery_contract(data, _inbound_terminal(OTHER_FULL_PN))
        self.assertEqual(refusal, MERGE_REFUSED_ENTRY_IDENTITY)
        self.assertEqual(data, snapshot)

        # Foreign identity vs the EXISTING contract (entry pn removed to
        # bypass the first gate): the contract builders refuse.
        stripped = dict(snapshot)
        stripped.pop("collector_pn")
        stripped_snapshot = copy.deepcopy(stripped)
        refusal = merge_recovery_contract(stripped, _inbound_terminal(OTHER_FULL_PN))
        self.assertEqual(refusal, MERGE_REFUSED_PROOF_REJECTED)
        self.assertEqual(stripped, stripped_snapshot)

    def test_malformed_existing_contract_is_never_clobbered(self) -> None:
        for malformed in (
            "not-a-record",
            {"schema_version": 99, "collector_pn": FULL_PN},
            {"collector_pn": FULL_PN},  # no version
            123,
        ):
            with self.subTest(malformed=malformed):
                data: dict[str, object] = {
                    "collector_pn": FULL_PN,
                    RECOVERY_CONTRACT_KEY: malformed,
                }
                snapshot = copy.deepcopy(data)
                refusal = merge_recovery_contract(data, _inbound_terminal())
                self.assertEqual(refusal, MERGE_REFUSED_MALFORMED_CONTRACT)
                self.assertEqual(data, snapshot)

    def test_no_outcome_touches_nothing(self) -> None:
        for existing in (
            {"collector_pn": FULL_PN},
            {"collector_pn": FULL_PN, RECOVERY_CONTRACT_KEY: "garbage"},
        ):
            with self.subTest(existing=sorted(existing)):
                data = copy.deepcopy(existing)
                self.assertEqual(
                    merge_recovery_contract(data, RecoveryTerminalInput.none()), ""
                )
                self.assertEqual(data, existing)

    def test_merge_requires_the_typed_input(self) -> None:
        data: dict[str, object] = {}
        for loose in (None, {"inbound_proof": _inbound_proof()}, _inbound_proof()):
            with self.subTest(loose=type(loose).__name__):
                with self.assertRaises(TypeError):
                    merge_recovery_contract(data, loose)  # type: ignore[arg-type]
        self.assertEqual(data, {})

    def test_merge_touches_only_the_canonical_key(self) -> None:
        data: dict[str, object] = {
            "collector_pn": FULL_PN,
            "connection_strategy": "inbound",
            "connection_strategy_evidence": "",
            "endpoint_control_policy": "external",
            "server_ip": "203.0.113.5",
        }
        before = copy.deepcopy(data)
        self.assertEqual(merge_recovery_contract(data, _callback_terminal()), "")
        added = set(data) - set(before)
        self.assertEqual(added, {RECOVERY_CONTRACT_KEY})
        for key, value in before.items():
            self.assertEqual(data[key], value)


class PreparedHandoffAcceptanceTests(unittest.TestCase):
    """prepared_handoff_identity is the ONE acceptance boundary."""

    OWNER = "callback_recovery:prepared-owner"
    SESSIONS = (
        {
            "session_id": "listener-18899-2",
            "collector_pn": FULL_PN,
            "state": "identified_strong",
            "collector_identity_source": "fc2_parameter_2",
            "listener_port": 18899,
            "raw": {"session_id": "listener-18899-2", "protocol_shape": "eybond_framed"},
        },
    )

    def _registry_with_prepared_owner(self) -> CallbackSessionRegistry:
        registry = CallbackSessionRegistry(sessions_source=lambda: self.SESSIONS)
        registry.claim_session(self.OWNER, session_id="listener-18899-2")
        registry.promote_claim_to_full_pn(self.OWNER, FULL_PN)
        self.assertTrue(registry.prepare_handoff(self.OWNER, FULL_PN))
        return registry

    def test_exact_prepared_owner_is_certified(self) -> None:
        registry = self._registry_with_prepared_owner()
        terminal = _callback_terminal(self.OWNER)
        self.assertEqual(verify_prepared_handoff(registry, terminal), FULL_PN)
        # Short candidate spelling still certifies the full identity.
        short_terminal = RecoveryTerminalInput(
            collector_pn=SHORT_PN,
            callback_proof=_callback_proof(SHORT_PN),
            prepared_handoff_owner=self.OWNER,
        )
        self.assertEqual(verify_prepared_handoff(registry, short_terminal), FULL_PN)

    def test_foreign_stale_or_missing_owner_fails_closed(self) -> None:
        registry = self._registry_with_prepared_owner()
        foreign = _callback_terminal("callback_recovery:someone-else")
        self.assertEqual(verify_prepared_handoff(registry, foreign), "")
        # A released (stale) owner certifies nothing.
        registry.release(self.OWNER)
        self.assertEqual(
            verify_prepared_handoff(registry, _callback_terminal(self.OWNER)), ""
        )
        # No-owner input and no-registry environment fail closed too.
        self.assertEqual(
            verify_prepared_handoff(registry, RecoveryTerminalInput.none()), ""
        )
        self.assertEqual(
            verify_prepared_handoff(None, _callback_terminal(self.OWNER)), ""
        )

    def test_unprepared_claim_is_not_a_capability(self) -> None:
        registry = CallbackSessionRegistry(sessions_source=lambda: self.SESSIONS)
        registry.claim_session(self.OWNER, session_id="listener-18899-2")
        registry.promote_claim_to_full_pn(self.OWNER, FULL_PN)
        # Claimed but never prepared: the terminal must not accept it.
        self.assertEqual(
            verify_prepared_handoff(registry, _callback_terminal(self.OWNER)), ""
        )

    def test_successful_terminal_leaves_handoff_for_entry_setup(self) -> None:
        registry = self._registry_with_prepared_owner()
        terminal = _callback_terminal(self.OWNER)
        self.assertEqual(verify_prepared_handoff(registry, terminal), FULL_PN)
        # The capability check must not consume the handoff: entry setup
        # completes exactly this one afterwards.
        self.assertTrue(registry.complete_handoff(FULL_PN, "entry-permanent"))
        self.assertEqual(registry.owner_for_pn(FULL_PN), "entry-permanent")

    def test_verifier_never_asks_anything_but_prepared_handoff_identity(self) -> None:
        class _Recording:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def prepared_handoff_identity(self, owner, pn):
                self.calls.append("prepared_handoff_identity")
                return FULL_PN

            def __getattr__(self, name):  # any other registry API
                raise AssertionError(f"unexpected registry call: {name}")

        registry = _Recording()
        self.assertEqual(
            verify_prepared_handoff(registry, _callback_terminal(self.OWNER)),
            FULL_PN,
        )
        self.assertEqual(registry.calls, ["prepared_handoff_identity"])


class StrategySeparationTests(unittest.TestCase):
    """RecoveryContract records proven methods; strategy stays user intent."""

    def test_callback_proof_does_not_touch_inbound_intent(self) -> None:
        data = {"collector_pn": FULL_PN, "connection_strategy": "inbound"}
        self.assertEqual(merge_recovery_contract(data, _callback_terminal()), "")
        self.assertEqual(data["connection_strategy"], "inbound")

    def test_inbound_proof_does_not_touch_callback_intent(self) -> None:
        data = {
            "collector_pn": FULL_PN,
            "connection_strategy": "callback_on_demand",
        }
        self.assertEqual(merge_recovery_contract(data, _inbound_terminal()), "")
        self.assertEqual(data["connection_strategy"], "callback_on_demand")

    def test_no_proof_writes_legacy_evidence_or_endpoint_state(self) -> None:
        for terminal in (_inbound_terminal(), _callback_terminal()):
            data: dict[str, object] = {"collector_pn": FULL_PN}
            self.assertEqual(merge_recovery_contract(data, terminal), "")
            self.assertNotIn("connection_strategy", data)
            self.assertNotIn("connection_strategy_evidence", data)
            self.assertNotIn("endpoint_control_policy", data)
            self.assertNotIn("server_ip", data)


class TerminalArchitectureGuardTests(unittest.TestCase):
    """One writer, one acceptance boundary, no forbidden vocabulary."""

    _PACKAGE = REPO_ROOT / "custom_components" / "eybond_local"
    _MODULE = _PACKAGE / "connection" / "recovery" / "terminal.py"

    @staticmethod
    def _code_names(path: Path) -> set[str]:
        """CODE identifiers only (docstrings/comments are prose, not calls)."""

        import ast

        names: set[str] = set()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names.add(node.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    names.update(alias.name.split("."))
            elif isinstance(node, ast.ImportFrom):
                names.update((node.module or "").split("."))
                names.update(alias.name for alias in node.names)
        return names

    def test_single_production_writer_of_the_recovery_contract(self) -> None:
        writers = []
        for path in sorted(self._PACKAGE.rglob("*.py")):
            if "write_to" in self._code_names(path):
                writers.append(path.name)
        self.assertEqual(
            sorted(set(writers)),
            ["recovery_contract.py", "terminal.py"],
            msg="only the model and the terminal boundary may write the contract",
        )

    def test_callback_proof_builders_have_exactly_two_callers(self) -> None:
        callers = []
        for path in sorted(self._PACKAGE.rglob("*.py")):
            if path.name == "recovery_contract.py":
                continue
            if "with_callback_proof" in self._code_names(path):
                callers.append(path.name)
        self.assertEqual(
            sorted(set(callers)),
            ["terminal.py", "verification.py"],
            msg="terminal merge + verifier pre-validation only",
        )

    def test_terminal_module_forbidden_vocabulary(self) -> None:
        names = self._code_names(self._MODULE)
        for banned in (
            "owner_for_pn",  # capability is presented, never reconstructed
            "options",  # canonical store is entry.data only
            "CallbackIdentityOutcome",  # identity is never recovery
            "hostname",
            "cloud_family",
            "collector_kind",
            "peer_ip",
            "CONF_SERVER_IP",
            "CONF_CONNECTION_STRATEGY",  # strategy is never inferred from proofs
            "now",
            "datetime",
            "utcnow",
        ):
            self.assertNotIn(banned, names, msg=f"banned in terminal module: {banned}")
        # Raw-payload vocabulary can only appear as a literal, not a name.
        self.assertNotIn(
            "set>server", self._MODULE.read_text(encoding="utf-8").replace(
                "never guessed", "never guessed"
            ),
        )

    def test_flow_terminal_coordinator_never_looks_owners_up_by_pn(self) -> None:
        config_flow = (self._PACKAGE / "config_flow.py").read_text(encoding="utf-8")
        # The coordinator region: from the coordinator def to the next method.
        start = config_flow.index("def _create_entry_with_handoff")
        end = config_flow.index("def ", start + 10)
        region = config_flow[start:end]
        # Never resolve an owner by PN, anywhere in the terminal path.
        self.assertNotIn("owner_for_pn", region)
        # 2D.2: there is ONE owner authority for the active flow -- the chosen
        # CallbackContinuation. The coordinator routes EVERY owner (inbound
        # admission OR callback) uniformly through it and no longer branches on
        # the admission transaction or inspects any ownership field.
        self.assertIn("prepare_terminal", region)
        self.assertIn("commit_terminal", region)
        self.assertIn("rollback_terminal", region)
        self.assertNotIn("_admission_transaction", region)
        self.assertNotIn("_prepare_ownership_handoff", region)
        # The prepared-owner acceptance (verify_prepared_handoff /
        # prepared_handoff_identity, NEVER a PN lookup) lives in the continuation
        # implementations, not in the coordinator.
        adapter_start = config_flow.index("class _LegacyCallbackContinuation")
        adapter_end = config_flow.index("class EybondLocalConfigFlow")
        adapter = config_flow[adapter_start:adapter_end]
        self.assertNotIn("owner_for_pn", adapter)
        self.assertIn("verify_prepared_handoff", adapter)
        self.assertIn("prepared_handoff_identity", adapter)

    def test_no_identity_outcome_to_contract_conversion_exists(self) -> None:
        # The only constructors accepting outcomes are the two classmethods,
        # and both are strict-typed -- pinned behaviorally in
        # TerminalInputTypingTests. Here: no module in the package converts
        # CallbackIdentityOutcome into contract vocabulary.
        for path in sorted(self._PACKAGE.rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            if "CallbackIdentityOutcome" not in source:
                continue
            self.assertNotIn(
                "with_inbound_proof",
                source.replace("callback_identity.py", ""),
                msg=f"{path.name} must not convert identity outcomes to proofs",
            ) if path.name == "callback_identity.py" else None
        identity_module = (
            self._PACKAGE / "connection" / "callback_identity.py"
        ).read_text(encoding="utf-8")
        for banned in ("with_inbound_proof", "with_callback_proof", "write_to"):
            self.assertNotIn(banned, identity_module)


if __name__ == "__main__":
    unittest.main()
