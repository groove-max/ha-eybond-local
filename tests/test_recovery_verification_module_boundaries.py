"""Architecture guards for the typed recovery-verification module family."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_RECOVERY = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "connection"
    / "recovery"
)
_FAMILY_NAMES = (
    "verification.py",
    "verification_models.py",
    "verification_engine.py",
    "verification_transaction.py",
    "verification_channel.py",
)
_FAMILY = tuple(_RECOVERY / name for name in _FAMILY_NAMES)
_ORIGINAL_DEFINITION_DIGEST = (
    "a7e76e8244e208b54b0869cfb758adefe24887af777b00f6eb59ebccd66cdf28"
)


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _definitions(path: Path) -> list[tuple[str, str]]:
    return [
        (type(node).__name__, node.name)
        for node in ast.walk(_tree(path))
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    ]


class RecoveryVerificationModuleBoundaryTests(unittest.TestCase):
    def test_verification_root_is_a_small_definition_free_api_facade(self) -> None:
        root = _RECOVERY / "verification.py"
        self.assertLessEqual(len(root.read_text(encoding="utf-8").splitlines()), 120)
        self.assertEqual(_definitions(root), [])

    def test_original_definition_multiset_is_preserved_exactly_once(self) -> None:
        definitions = [item for path in _FAMILY for item in _definitions(path)]
        payload = "\n".join(
            f"{kind}:{name}" for kind, name in sorted(definitions)
        ).encode()
        self.assertEqual(len(definitions), 80)
        self.assertEqual(len(set(definitions)), 67)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), _ORIGINAL_DEFINITION_DIGEST)

    def test_recovery_responsibilities_have_one_concrete_owner(self) -> None:
        expected = {
            "RecoveryVerificationOutcome": "verification_models.py",
            "CallbackRecoveryRoute": "verification_models.py",
            "_ControlledResetRecoveryEngine": "verification_engine.py",
            "InboundRecoveryVerifier": "verification_engine.py",
            "CallbackRecoveryVerifier": "verification_engine.py",
            "ObservedSessionRestartChannel": "verification_channel.py",
            "registry_sessions_projection": "verification_transaction.py",
            "async_run_callback_recovery_transaction": "verification_transaction.py",
        }
        actual: dict[str, list[str]] = {name: [] for name in expected}
        for path in _FAMILY:
            for node in _tree(path).body:
                if (
                    isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name in actual
                ):
                    actual[node.name].append(path.name)
        self.assertEqual(actual, {name: [owner] for name, owner in expected.items()})

    def test_implementation_modules_do_not_import_the_facade_backwards(self) -> None:
        for path in _FAMILY[1:]:
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("from .verification import", source, msg=path.name)

    def test_channel_owns_no_callback_trigger_or_recovery_state_machine(self) -> None:
        source = (_RECOVERY / "verification_channel.py").read_text(encoding="utf-8")
        self.assertNotIn("async_send_callback_trigger", source)
        self.assertNotIn("get_callback_trigger_ledger", source)
        self.assertNotIn("_ControlledResetRecoveryEngine", source)

    def test_facade_exports_exact_concrete_types(self) -> None:
        from custom_components.eybond_local.connection.recovery import verification
        from custom_components.eybond_local.connection.recovery.verification_channel import (
            ObservedSessionRestartChannel,
        )
        from custom_components.eybond_local.connection.recovery.verification_engine import (
            CallbackRecoveryVerifier,
            InboundRecoveryVerifier,
        )
        from custom_components.eybond_local.connection.recovery.verification_models import (
            RecoveryVerificationOutcome,
        )

        self.assertIs(
            verification.ObservedSessionRestartChannel,
            ObservedSessionRestartChannel,
        )
        self.assertIs(verification.CallbackRecoveryVerifier, CallbackRecoveryVerifier)
        self.assertIs(verification.InboundRecoveryVerifier, InboundRecoveryVerifier)
        self.assertIs(
            verification.RecoveryVerificationOutcome,
            RecoveryVerificationOutcome,
        )


if __name__ == "__main__":
    unittest.main()
