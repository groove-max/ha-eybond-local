"""Architecture guards for the collector-transport module family."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_COLLECTOR = REPO_ROOT / "custom_components" / "eybond_local" / "collector"
_TRANSPORT = _COLLECTOR / "transport"
_FAMILY_NAMES = (
    "__init__.py",
    "common.py",
    "connections.py",
    "listener.py",
    "proxy.py",
    "shared_at.py",
    "shared_framed.py",
)
_FAMILY = tuple(_TRANSPORT / name for name in _FAMILY_NAMES)
_ORIGINAL_DEFINITION_DIGEST = (
    "825b2f218518326c39566545f06f5a4c680a061162d6258e4f13400568c14f3c"
)


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _definitions(path: Path) -> list[tuple[str, str]]:
    return [
        (type(node).__name__, node.name)
        for node in ast.walk(_tree(path))
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _assigned_names(path: Path) -> set[str]:
    names: set[str] = set()
    for node in _tree(path).body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets.extend(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets.append(node.target)
        for target in targets:
            if isinstance(target, ast.Name):
                names.add(target.id)
    return names


class TransportModuleBoundaryTests(unittest.TestCase):
    def test_transport_root_is_a_small_definition_free_facade(self) -> None:
        root = _TRANSPORT / "__init__.py"
        self.assertLessEqual(len(root.read_text(encoding="utf-8").splitlines()), 80)
        self.assertEqual(_definitions(root), [])

    def test_original_definition_multiset_is_preserved_exactly_once(self) -> None:
        definitions = [item for path in _FAMILY for item in _definitions(path)]
        payload = "\n".join(
            f"{kind}:{name}" for kind, name in sorted(definitions)
        ).encode()
        self.assertEqual(len(definitions), 251)
        self.assertEqual(len(set(definitions)), 193)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), _ORIGINAL_DEFINITION_DIGEST)

    def test_concrete_authorities_have_one_owner_module(self) -> None:
        expected = {
            "_CollectorConnection": "connections.py",
            "_CollectorAtConnection": "connections.py",
            "_SharedEybondListener": "listener.py",
            "SharedProxyCaptureRoute": "proxy.py",
            "SharedEybondTransport": "shared_framed.py",
            "SharedCollectorAtTransport": "shared_at.py",
        }
        actual: dict[str, list[str]] = {name: [] for name in expected}
        for path in _FAMILY:
            for node in _tree(path).body:
                if isinstance(node, ast.ClassDef) and node.name in actual:
                    actual[node.name].append(path.name)
        self.assertEqual(actual, {name: [owner] for name, owner in expected.items()})

    def test_mutable_listener_and_task_registries_have_one_owner(self) -> None:
        assignments = {path.name: _assigned_names(path) for path in _FAMILY}
        for name, owner in {
            "_LISTENERS": "listener.py",
            "_LISTENERS_LOCK": "listener.py",
            "_BACKGROUND_TASKS": "common.py",
        }.items():
            self.assertEqual(
                [module for module, names in assignments.items() if name in names],
                [owner],
            )

    def test_implementation_modules_do_not_import_the_facade_backwards(self) -> None:
        for path in _FAMILY[1:]:
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("from . import", source, msg=path.name)

    def test_facade_exports_the_exact_concrete_types(self) -> None:
        from custom_components.eybond_local.collector import transport
        from custom_components.eybond_local.collector.transport.listener import (
            _SharedEybondListener,
        )
        from custom_components.eybond_local.collector.transport.proxy import (
            SharedProxyCaptureRoute,
        )
        from custom_components.eybond_local.collector.transport.shared_at import (
            SharedCollectorAtTransport,
        )
        from custom_components.eybond_local.collector.transport.shared_framed import (
            SharedEybondTransport,
        )

        self.assertIs(transport._SharedEybondListener, _SharedEybondListener)
        self.assertIs(transport.SharedProxyCaptureRoute, SharedProxyCaptureRoute)
        self.assertIs(transport.SharedCollectorAtTransport, SharedCollectorAtTransport)
        self.assertIs(transport.SharedEybondTransport, SharedEybondTransport)


if __name__ == "__main__":
    unittest.main()
