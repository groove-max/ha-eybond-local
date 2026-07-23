"""Batch 2A architecture guards: recovery authority lives in ONE neutral home.

Recovery execution (the controlled-reset engine, verifiers, restart channel, the
recovery-contract terminal merge) and the shared timeout policy now live in the
neutral ``connection`` layer, NOT in ``onboarding``. These guards fail loudly if
a back-dependency ``connection/runtime -> onboarding`` for recovery ever returns,
if the neutral recovery package reaches up into config_flow/runtime/onboarding,
if a recovery type gets a second definition, or if the timeout defaults drift or
split.

They read production SOURCE by path (AST) and import only HA-free neutral
modules, so they need none of the HA runtime stubs.
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

# The recovery-execution modules Batch 2A relocated out of onboarding.
RELOCATED_ONBOARDING_MODULES = {
    "onboarding.strategy_verification",
    "onboarding.recovery_terminalization",
    "onboarding.timeouts",
    "onboarding.callback_matching",
}
UPWARD_LAYERS = {"onboarding", "config_flow", "runtime"}

# Neutral connection-layer recovery primitives that must stay import-clean
# (relative to PKG).
NEUTRAL_RECOVERY_FILES = ("connection/recovery", "connection/callback_matching.py")


def _import_from_targets(py_path: Path) -> set[str]:
    """Every module/name an ``ImportFrom``/``Import`` in a file references.

    Relative imports contribute ``node.module`` verbatim (``onboarding.timeouts``
    for ``from ..onboarding.timeouts import X``), so a target can be matched
    regardless of the leading-dot depth.
    """

    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod:
                targets.add(mod)
            for alias in node.names:
                targets.add(f"{mod}.{alias.name}" if mod else alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                targets.add(alias.name)
    return targets


def _import_segments(py_path: Path) -> set[str]:
    """Every dotted segment appearing in any import module of a file."""

    segments: set[str] = set()
    for target in _import_from_targets(py_path):
        segments.update(target.split("."))
    return segments


def _module_level_def_names(py_path: Path) -> list[str]:
    """Names defined at MODULE level (classes, funcs, simple assignments)."""

    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.append(target.id)
    return names


def _py_files(*rel_dirs: str) -> list[Path]:
    files: list[Path] = []
    for rel in rel_dirs:
        files.extend(sorted((PKG / rel).rglob("*.py")))
    return files


class RecoveryRelocationGuards(unittest.TestCase):
    # 1 -------------------------------------------------------------------
    def test_connection_and_runtime_never_import_onboarding_recovery(self) -> None:
        offenders: dict[str, set[str]] = {}
        for path in _py_files("connection", "runtime"):
            hits = {
                target
                for target in _import_from_targets(path)
                for relocated in RELOCATED_ONBOARDING_MODULES
                if target == relocated or target.endswith("." + relocated)
            }
            if hits:
                offenders[str(path.relative_to(PKG))] = hits
        self.assertEqual(
            offenders,
            {},
            msg=(
                "connection/runtime must not import onboarding recovery-execution "
                f"(engine/terminal) or the onboarding timeout policy: {offenders}"
            ),
        )

    # 2 -------------------------------------------------------------------
    def _neutral_recovery_files(self) -> list[Path]:
        files = list((PKG / "connection" / "recovery").rglob("*.py"))
        files.append(PKG / "connection" / "callback_matching.py")
        return files

    def test_neutral_recovery_imports_nothing_upward(self) -> None:
        offenders: dict[str, set[str]] = {}
        for path in self._neutral_recovery_files():
            up = _import_segments(path) & UPWARD_LAYERS
            if up:
                offenders[str(path.relative_to(PKG))] = up
        self.assertEqual(
            offenders,
            {},
            msg=(
                "the neutral recovery primitives (connection/recovery + "
                "connection/callback_matching) must import nothing from "
                f"onboarding/config_flow/runtime: {offenders}"
            ),
        )

    # 3 -------------------------------------------------------------------
    def test_old_onboarding_recovery_modules_are_gone(self) -> None:
        for gone in (
            "strategy_verification.py",
            "recovery_terminalization.py",
            "callback_matching.py",
        ):
            self.assertFalse(
                (PKG / "onboarding" / gone).exists(),
                msg=f"onboarding/{gone} must be deleted, not left as a wrapper",
            )
        # And no re-export shim under a different name either.
        for path in _py_files("onboarding"):
            names = _module_level_def_names(path)
            # A wrapper would re-export the moved public API from this layer.
            self.assertNotIn("InboundRecoveryVerifier", names)
            self.assertNotIn("merge_recovery_contract", names)
            self.assertNotIn("match_callback_answer", names)

    # 4 -------------------------------------------------------------------
    def test_each_recovery_symbol_defined_exactly_once(self) -> None:
        symbols = [
            "InboundRecoveryOutcome",
            "RecoveryVerificationOutcome",
            "CallbackRecoveryRoute",
            "RecoveryWireProbeAuthority",
            "InboundRecoveryVerifier",
            "CallbackRecoveryVerifier",
            "ObservedSessionRestartChannel",
            "async_run_callback_recovery_transaction",
            "registry_sessions_projection",
            "match_callback_answer",
            "RecoveryTerminalInput",
            "merge_recovery_contract",
            "verify_prepared_handoff",
            "OnboardingTimeoutPolicy",
        ]
        definitions: dict[str, list[str]] = {s: [] for s in symbols}
        for path in _py_files(""):
            for name in _module_level_def_names(path):
                if name in definitions:
                    definitions[name].append(str(path.relative_to(PKG)))
        for symbol, where in definitions.items():
            self.assertEqual(
                len(where), 1, msg=f"{symbol} must be defined once, found in {where}"
            )

    # 5 -------------------------------------------------------------------
    def test_no_cross_module_import_of_private_projection(self) -> None:
        offenders = []
        for path in _py_files(""):
            if any(
                "_registry_sessions_projection" in target
                for target in _import_from_targets(path)
            ):
                offenders.append(str(path.relative_to(PKG)))
        self.assertEqual(
            offenders,
            [],
            msg=f"the private _registry_sessions_projection must not be imported: {offenders}",
        )
        # The public seam exists.
        from custom_components.eybond_local.connection.recovery.verification import (
            registry_sessions_projection,
        )

        self.assertTrue(callable(registry_sessions_projection))

    # 6 -------------------------------------------------------------------
    def test_importing_neutral_recovery_loads_no_upper_layer(self) -> None:
        code = (
            "import sys\n"
            f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
            "import custom_components.eybond_local.connection.recovery.verification\n"
            "import custom_components.eybond_local.connection.recovery.terminal\n"
            "import custom_components.eybond_local.connection.callback_matching\n"
            "bad = sorted(m for m in sys.modules if any(s in m for s in "
            "('.onboarding.', '.config_flow', '.runtime.')))\n"
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

    # 7 -------------------------------------------------------------------
    def test_timeout_defaults_have_one_neutral_authority(self) -> None:
        from custom_components.eybond_local import timeout_policy as tp
        from custom_components.eybond_local.onboarding import timeouts as ot

        # ONE definition, ONE default object -- onboarding re-exports the neutral one.
        self.assertIs(ot.OnboardingTimeoutPolicy, tp.OnboardingTimeoutPolicy)
        self.assertIs(
            ot.DEFAULT_ONBOARDING_TIMEOUT_POLICY,
            tp.DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        )
        # The policy CLASS is defined in exactly one file (guarded in #4 too).
        default = tp.DEFAULT_ONBOARDING_TIMEOUT_POLICY
        # Representative load-bearing subset spanning scan, callback and
        # inbound-recovery budgets.  The callback-recovery link window is longer
        # than the identity-only window because it starts after a physical reboot;
        # production E500 hardware can need more than 30 seconds to dial back in.
        expected = {
            "discovery_timeout": 1.5,
            "connect_timeout": 5.0,
            "connect_timeout_without_udp_reply": 0.75,
            "heartbeat_timeout": 2.0,
            "manual_total_timeout": 45.0,
            "auto_total_timeout": 30.0,
            "callback_identity_session_wait": 20.0,
            "callback_causality_lease_wait": 30.0,
            "callback_recovery_session_wait": 60.0,
            "inbound_strong_identity_timeout": 30.0,
            "inbound_restart_disconnect_timeout": 65.0,
            "inbound_reconnect_timeout": 60.0,
        }
        for field, value in expected.items():
            self.assertEqual(getattr(default, field), value, msg=field)


if __name__ == "__main__":
    unittest.main()
