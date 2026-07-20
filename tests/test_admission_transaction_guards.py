"""Batch 2B architecture guards: the observed-session admission lifecycle lives
in ONE neutral CollectorAdmissionTransaction, not inline in config_flow.

They read production SOURCE by path (AST); no import, so no HA stubs needed.
"""

from __future__ import annotations

import ast
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
PKG = REPO_ROOT / "custom_components" / "eybond_local"
CONFIG_FLOW = PKG / "config_flow.py"
TRANSACTION = PKG / "connection" / "admission_transaction.py"


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"))


def _called_names(tree: ast.AST) -> set[str]:
    """Names/attrs that appear in a CALL position."""

    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def _attr_accesses(tree: ast.AST) -> set[str]:
    return {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }


def _class_method(tree: ast.AST, class_name: str, method: str):
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == method:
                    return item
    return None


def _init_assigned_attrs(tree: ast.AST, class_name: str) -> set[str]:
    init = _class_method(tree, class_name, "__init__")
    attrs: set[str] = set()
    if init is None:
        return attrs
    for node in ast.walk(init):
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if (
                    isinstance(tgt, ast.Attribute)
                    and isinstance(tgt.value, ast.Name)
                    and tgt.value.id == "self"
                ):
                    attrs.add(tgt.attr)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Attribute):
            if isinstance(node.target.value, ast.Name) and node.target.value.id == "self":
                attrs.add(node.target.attr)
    return attrs


class AdmissionTransactionGuards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cf = _tree(CONFIG_FLOW)
        cls.txn = _tree(TRANSACTION)

    def test_config_flow_constructs_no_verifier_channel_or_probe(self):
        called = _called_names(self.cf)
        for banned in (
            "InboundRecoveryVerifier",
            "ObservedSessionRestartChannel",
            "SilentSessionIdentityProbeChannel",
        ):
            self.assertNotIn(
                banned,
                called,
                msg=f"config_flow must not construct {banned} (extracted to the transaction)",
            )

    def test_config_flow_makes_no_admission_registry_claim_ops(self):
        # retarget/handle are admission-run-only registry ops -- fully extracted.
        # (claim_session/promote remain for the MANUAL callback path.)
        called = _called_names(self.cf)
        for banned in (
            "retarget_claim_to_reconnected_session",
            "session_handle_for_claimed_session",
        ):
            self.assertNotIn(
                banned,
                called,
                msg=f"config_flow must not call {banned} for observed admission",
            )

    def test_removed_admission_methods_are_gone(self):
        for gone in ("_async_run_strategy_verification", "_verification_sessions_source"):
            self.assertIsNone(
                _class_method(self.cf, "EybondLocalConfigFlow", gone),
                msg=f"{gone} must be removed from config_flow",
            )

    def test_admission_mutable_fields_shrank(self):
        attrs = _init_assigned_attrs(self.cf, "EybondLocalConfigFlow")
        # The new, smaller admission surface exists ...
        for present in ("_admission_transaction", "_admission_task", "_admission_next_step"):
            self.assertIn(present, attrs, msg=f"{present} should be a flow field")
        # ... and the old admission-specific fields are gone (replaced, not renamed
        # 1:1). 4 admission-specific fields -> 3.
        for gone in (
            "_admission_request",
            "_verification_task",
            "_verification_result",
            "_verification_next_step",
        ):
            self.assertNotIn(
                gone, attrs, msg=f"{gone} must no longer be a flow field"
            )

    def test_exactly_one_transaction_type(self):
        defs = [
            n
            for tree in (self.cf, self.txn)
            for n in ast.walk(tree)
            if isinstance(n, ast.ClassDef) and n.name == "CollectorAdmissionTransaction"
        ]
        self.assertEqual(len(defs), 1)
        # ... and it lives in the neutral transaction module.
        txn_defs = [
            n
            for n in ast.walk(self.txn)
            if isinstance(n, ast.ClassDef) and n.name == "CollectorAdmissionTransaction"
        ]
        self.assertEqual(len(txn_defs), 1)

    def test_transaction_module_imports_nothing_upward(self):
        imported: set[str] = set()
        for node in ast.walk(self.txn):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.update(node.module.split("."))
            elif isinstance(node, ast.Import):
                for a in node.names:
                    imported.update(a.name.split("."))
        for banned in ("config_flow", "onboarding", "runtime", "homeassistant"):
            self.assertNotIn(
                banned,
                imported,
                msg=f"admission_transaction must not import {banned}",
            )

    def test_admission_algorithm_reads_no_provenance_or_topology(self):
        # The run + enrichment are the algorithm; they must key on the observed
        # identity only -- never origin / peer_hint / connection_mode / cloud /
        # kind / hostname / model.
        for method in ("async_run", "_adopt_enriched_pn_from_outcome"):
            node = _class_method(self.txn, "CollectorAdmissionTransaction", method)
            self.assertIsNotNone(node, msg=method)
            attrs = _attr_accesses(node)
            for banned in (
                "origin",
                "peer_hint",
                "connection_mode",
                "collector_cloud_family",
                "hostname",
                "model",
                "collector_kind",
            ):
                self.assertNotIn(
                    banned,
                    attrs,
                    msg=f"{method} must not read {banned!r}",
                )


if __name__ == "__main__":
    unittest.main()
