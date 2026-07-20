"""Batch 2C architecture guards: callback identity authority lives in the neutral
connection layer, with exactly one production home.

They read production SOURCE by path (AST) and a clean-interpreter subprocess; no
HA stubs needed.
"""

from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
PKG = REPO_ROOT / "custom_components" / "eybond_local"
NEW_MODULE = PKG / "connection" / "callback_identity.py"
OLD_MODULE = PKG / "onboarding" / "callback_identity.py"

UPWARD_LAYERS = {"onboarding", "config_flow", "runtime"}
PUBLIC_API = [
    "CallbackIdentityRequest",
    "CallbackIdentityOutcome",
    "SilentSessionBootstrapOffer",
    "OnboardingWireProbeIntent",
    "async_run_callback_identity_transaction",
]


def _import_targets(py_path: Path) -> set[str]:
    """Every module string an ``ImportFrom``/``Import`` references (any depth)."""

    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            targets.add(node.module)
            for alias in node.names:
                targets.add(f"{node.module}.{alias.name}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                targets.add(alias.name)
    return targets


def _import_segments(py_path: Path) -> set[str]:
    segments: set[str] = set()
    for target in _import_targets(py_path):
        segments.update(target.split("."))
    return segments


def _module_level_def_names(py_path: Path) -> list[str]:
    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    names.append(tgt.id)
    return names


class CallbackIdentityRelocationGuards(unittest.TestCase):
    def test_new_module_imports_nothing_upward(self) -> None:
        segments = _import_segments(NEW_MODULE)
        offenders = segments & UPWARD_LAYERS
        self.assertEqual(
            offenders,
            set(),
            msg=(
                "connection/callback_identity.py must import nothing from "
                f"onboarding/config_flow/runtime: {offenders}"
            ),
        )

    def test_new_module_does_not_route_policy_through_onboarding_timeouts(self) -> None:
        targets = _import_targets(NEW_MODULE)
        self.assertNotIn("onboarding.timeouts", targets)
        # The policy comes from the neutral top-level module.
        self.assertTrue(
            any(t == "timeout_policy" or t.endswith(".timeout_policy") for t in targets),
            msg="policy must come from timeout_policy directly",
        )

    def test_no_connection_or_runtime_module_imports_old_callback_identity(self) -> None:
        offenders: dict[str, set[str]] = {}
        for rel in ("connection", "runtime"):
            for path in sorted((PKG / rel).rglob("*.py")):
                hits = {
                    t
                    for t in _import_targets(path)
                    if t == "onboarding.callback_identity"
                    or t.endswith(".onboarding.callback_identity")
                }
                if hits:
                    offenders[str(path.relative_to(PKG))] = hits
        self.assertEqual(offenders, {}, msg=str(offenders))

    def test_old_module_is_gone_with_no_wrapper_or_reexport(self) -> None:
        self.assertFalse(
            OLD_MODULE.exists(),
            msg="onboarding/callback_identity.py must be deleted, not wrapped",
        )
        # No onboarding module re-exports the moved public API under any name.
        for path in sorted((PKG / "onboarding").rglob("*.py")):
            names = set(_module_level_def_names(path))
            for symbol in PUBLIC_API:
                self.assertNotIn(
                    symbol,
                    names,
                    msg=f"{path.name} must not re-export {symbol}",
                )

    def test_each_public_symbol_defined_exactly_once(self) -> None:
        definitions: dict[str, list[str]] = {s: [] for s in PUBLIC_API}
        for path in sorted(PKG.rglob("*.py")):
            for name in _module_level_def_names(path):
                if name in definitions:
                    definitions[name].append(str(path.relative_to(PKG)))
        for symbol, where in definitions.items():
            self.assertEqual(
                len(where),
                1,
                msg=f"{symbol} must be defined once; found in {where}",
            )
            self.assertEqual(where[0], "connection/callback_identity.py")

    def test_clean_interpreter_import_pulls_no_onboarding_callback_identity(self) -> None:
        code = (
            "import sys\n"
            f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
            "import custom_components.eybond_local.connection.callback_identity\n"
            "bad = sorted(m for m in sys.modules if m.endswith("
            "('onboarding.callback_identity', 'onboarding.timeouts')))\n"
            "print('BAD:' + ','.join(bad) if bad else 'CLEAN')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr[-800:])
        self.assertIn("CLEAN", result.stdout, msg=result.stdout)


if __name__ == "__main__":
    unittest.main()
