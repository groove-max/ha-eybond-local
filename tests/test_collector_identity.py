"""Load-bearing tests for the neutral collector identity authority."""

from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector_identity import (  # noqa: E402
    identity_source_is_strong,
    normalize_pn,
    pn_is_same_identity,
    prefer_full_pn,
    prefer_identity_source,
    reconcile_durable_pn,
    reconcile_pn,
    validated_collector_pn,
)
from custom_components.eybond_local.connection import session_registry  # noqa: E402


SHORT_PN = "V001020SYN6234"
FULL_PN = "V001020SYN62344022"
FOREIGN_PN = "V001020ABC99999999"

IDENTITY_HELPERS = {
    "identity_source_is_strong",
    "normalize_pn",
    "pn_is_same_identity",
    "prefer_full_pn",
    "prefer_identity_source",
    "reconcile_durable_pn",
    "reconcile_pn",
}


class CollectorIdentityBehaviorTests(unittest.TestCase):
    def test_wire_safe_validation_stays_strict_and_non_coercing(self) -> None:
        self.assertEqual(validated_collector_pn(FULL_PN), FULL_PN)
        for value in (None, 123, b"V001020", " padded ", "bad\x00pn"):
            with self.subTest(value=value):
                self.assertEqual(validated_collector_pn(value), "")

    def test_short_full_identity_matrix_is_unchanged(self) -> None:
        self.assertEqual(normalize_pn(f" {FULL_PN} "), FULL_PN)
        self.assertTrue(pn_is_same_identity(SHORT_PN, FULL_PN))
        self.assertTrue(pn_is_same_identity(FULL_PN, SHORT_PN))
        self.assertFalse(pn_is_same_identity(FULL_PN, FOREIGN_PN))
        self.assertEqual(prefer_full_pn(SHORT_PN, FULL_PN), FULL_PN)
        self.assertEqual(reconcile_pn(SHORT_PN, FULL_PN), FULL_PN)
        self.assertEqual(reconcile_pn(FULL_PN, FOREIGN_PN), FULL_PN)

    def test_strong_identity_source_never_downgrades_to_heartbeat(self) -> None:
        self.assertTrue(identity_source_is_strong("fc2_parameter_2"))
        self.assertTrue(identity_source_is_strong("at_dtupn"))
        self.assertFalse(identity_source_is_strong("framed_heartbeat"))
        self.assertEqual(
            prefer_identity_source("fc2_parameter_2", "framed_heartbeat"),
            "fc2_parameter_2",
        )
        self.assertEqual(
            prefer_identity_source("framed_heartbeat", "at_dtupn"),
            "at_dtupn",
        )

    def test_durable_reconciliation_uses_the_canonical_prefix_boundary(self) -> None:
        self.assertEqual(
            reconcile_durable_pn(FULL_PN, SHORT_PN),
            (FULL_PN, False),
        )
        self.assertEqual(
            reconcile_durable_pn(SHORT_PN, FULL_PN),
            (FULL_PN, False),
        )
        self.assertEqual(
            reconcile_durable_pn(FULL_PN, FOREIGN_PN),
            (FULL_PN, True),
        )
        self.assertEqual(reconcile_durable_pn("", SHORT_PN), (SHORT_PN, False))
        self.assertEqual(
            reconcile_durable_pn(FULL_PN, FULL_PN[:5]),
            (FULL_PN, True),
        )


class CollectorIdentityArchitectureTests(unittest.TestCase):
    def test_identity_authority_is_neutral(self) -> None:
        path = REPO_ROOT / "custom_components/eybond_local/collector_identity.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.add(node.module or "")

        self.assertLessEqual(imports, {"__future__", "annotations", "re"})

    def test_identity_helpers_have_one_definition(self) -> None:
        definitions: dict[str, list[str]] = {name: [] for name in IDENTITY_HELPERS}
        root = REPO_ROOT / "custom_components/eybond_local"
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if node.name in definitions:
                        definitions[node.name].append(str(path.relative_to(REPO_ROOT)))

        expected = "custom_components/eybond_local/collector_identity.py"
        self.assertEqual(
            definitions,
            {name: [expected] for name in IDENTITY_HELPERS},
        )

    def test_registry_does_not_reexport_the_identity_helpers(self) -> None:
        for name in IDENTITY_HELPERS:
            with self.subTest(name=name):
                self.assertFalse(hasattr(session_registry, name))

    def test_production_never_imports_identity_helpers_from_registry(self) -> None:
        root = REPO_ROOT / "custom_components/eybond_local"
        violations: list[str] = []
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                if not (node.module or "").endswith("session_registry"):
                    continue
                imported = {alias.name for alias in node.names}
                overlap = imported & IDENTITY_HELPERS
                if overlap:
                    violations.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}:{sorted(overlap)}"
                    )

        self.assertEqual(violations, [])

    def test_runtime_and_transport_do_not_wrap_reconciliation(self) -> None:
        forbidden = {
            "_prefer_more_complete_collector_pn",
            "_prefer_more_complete_identity",
            "_reconcile_durable_collector_pn",
        }
        root = REPO_ROOT / "custom_components/eybond_local"
        definitions: list[str] = []
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if node.name in forbidden:
                        definitions.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno}:{node.name}"
                        )

        self.assertEqual(definitions, [])


if __name__ == "__main__":
    unittest.main()
