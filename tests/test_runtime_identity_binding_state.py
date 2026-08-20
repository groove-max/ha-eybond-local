"""Item 8: a PN-less collector entry reports identity_binding_required at runtime.

The live runtime snapshot must publish ``collector_identity_binding_required`` so
the live state agrees with the support bundle's
``entry_axis_diagnostics.collector_identity_binding_required``. It is a pure
DIAGNOSTIC value: it must not feed ``runtime_driver_state`` or the poll scheduler,
so polling is unchanged (item 10).

This is a source-level guard (no coordinator import) to avoid the module-stub
pollution that a direct coordinator import causes under ``unittest discover``. The
underlying predicate ``collector_identity_binding_required`` is verified
behaviorally in ``tests/test_connection_architecture.py``.
"""

from __future__ import annotations

import ast
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
COORDINATOR = REPO_ROOT / "custom_components" / "eybond_local" / "runtime" / "coordinator.py"
POLL_PROJECTION = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "runtime"
    / "coordinator_poll_projection.py"
)
POLLING = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "runtime"
    / "coordinator_polling.py"
)
STARTUP = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "runtime"
    / "coordinator_startup.py"
)


class IdentityBindingRuntimeStateWiringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = COORDINATOR.read_text(encoding="utf-8")
        self.tree = ast.parse(self.source)
        self.polling_source = POLLING.read_text(encoding="utf-8")
        self.polling_tree = ast.parse(self.polling_source)
        self.startup_source = STARTUP.read_text(encoding="utf-8")

    def _func(self, name: str) -> ast.FunctionDef:
        for tree in (self.tree, self.polling_tree):
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == name:
                    return node
        self.fail(f"{name} not defined in the coordinator lifecycle")

    def test_flag_helper_uses_the_binding_required_predicate(self) -> None:
        func = self._func("_identity_binding_required_flag")
        called = {
            n.func.id
            for n in ast.walk(func)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
        self.assertIn("collector_identity_binding_required", called)

    def test_snapshot_publishes_the_flag(self) -> None:
        # The diagnostic value is published into the snapshot values dict(s).
        self.assertGreaterEqual(
            (self.source + self.polling_source + self.startup_source).count(
                '"collector_identity_binding_required": self._identity_binding_required_flag()'
            ),
            2,
            "flag must be published in both the startup and live snapshot values",
        )

    def test_poll_context_mapping_ignores_binding_required(self) -> None:
        # Item 10: the poll-context mapping must NOT branch on a binding-required
        # state, so the diagnostic flag cannot change polling.
        source = POLL_PROJECTION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "poll_context_for_runtime_driver_state"
        )
        poll_fn = ast.get_source_segment(source, function)
        self.assertNotIn("identity_binding_required", poll_fn)

    def test_flag_helper_returns_the_predicate_value(self) -> None:
        # The helper's only Return of substance is the predicate call (a pure
        # diagnostic), never a poll context or driver-state literal.
        func = self._func("_identity_binding_required_flag")
        returns = [n for n in ast.walk(func) if isinstance(n, ast.Return)]
        self.assertTrue(
            any(
                isinstance(r.value, ast.Call)
                and isinstance(r.value.func, ast.Name)
                and r.value.func.id == "collector_identity_binding_required"
                for r in returns
            )
        )


if __name__ == "__main__":
    unittest.main()
