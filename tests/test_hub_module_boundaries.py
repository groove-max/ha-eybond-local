"""Architecture guards for the runtime-hub decomposition."""

from __future__ import annotations

import ast
from collections import Counter
import hashlib
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "custom_components" / "eybond_local" / "runtime"
HUB = RUNTIME / "hub.py"

MIXINS = {
    "hub_lifecycle.py": "HubLifecycleMixin",
    "hub_refresh.py": "HubRefreshMixin",
    "hub_management.py": "HubManagementMixin",
    "hub_support.py": "HubSupportMixin",
    "hub_detection.py": "HubDetectionMixin",
    "hub_snapshot.py": "HubSnapshotMixin",
}

EXPECTED_MRO = [
    "HubLifecycleMixin",
    "HubRefreshMixin",
    "HubManagementMixin",
    "HubSupportMixin",
    "HubDetectionMixin",
    "HubSnapshotMixin",
]

EXPECTED_METHOD_MULTISET_SHA256 = (
    "1e64d4429024a075207a2c0b54268a5e32c0dec725a19edad6c570a629bcb393"
)


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _class(path: Path, name: str) -> ast.ClassDef:
    return next(
        node
        for node in _tree(path).body
        if isinstance(node, ast.ClassDef) and node.name == name
    )


def _methods(node: ast.ClassDef) -> tuple[str, ...]:
    return tuple(
        child.name
        for child in node.body
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
    )


def _imported_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


class HubCompositionBoundaryTests(unittest.TestCase):
    def test_composition_root_contains_only_constructor_logic(self) -> None:
        hub = _class(HUB, "EybondHub")
        self.assertEqual(_methods(hub), ("__init__",))
        self.assertLessEqual(len(HUB.read_text(encoding="utf-8").splitlines()), 200)

    def test_mro_is_explicit_and_stable(self) -> None:
        hub = _class(HUB, "EybondHub")
        bases = [
            base.id if isinstance(base, ast.Name) else ast.unparse(base)
            for base in hub.bases
        ]
        self.assertEqual(bases, EXPECTED_MRO)

    def test_every_original_method_has_one_owner_and_matching_multiplicity(self) -> None:
        owners: dict[str, set[str]] = {}
        counts: Counter[str] = Counter()
        classes = [("hub.py", _class(HUB, "EybondHub"))]
        classes.extend(
            (filename, _class(RUNTIME / filename, class_name))
            for filename, class_name in MIXINS.items()
        )
        for filename, lifecycle in classes:
            for method in _methods(lifecycle):
                owners.setdefault(method, set()).add(filename)
                counts[method] += 1

        duplicates = {name: paths for name, paths in owners.items() if len(paths) != 1}
        self.assertEqual(duplicates, {})
        payload = "\n".join(f"{name}:{count}" for name, count in sorted(counts.items()))
        self.assertEqual(sum(counts.values()), 105)
        self.assertEqual(len(counts), 100)
        self.assertEqual(
            hashlib.sha256(payload.encode()).hexdigest(),
            EXPECTED_METHOD_MULTISET_SHA256,
        )

    def test_mixins_have_no_constructor_or_hub_back_import(self) -> None:
        for filename, class_name in MIXINS.items():
            path = RUNTIME / filename
            self.assertNotIn("__init__", _methods(_class(path, class_name)), filename)
            imports = _imported_modules(path)
            self.assertFalse(
                any(module == "hub" or module.endswith(".hub") for module in imports),
                filename,
            )

    def test_one_class_owns_each_stateful_family(self) -> None:
        expected = {
            "async_refresh": "hub_refresh.py",
            "_resolve_runtime_measurements": "hub_refresh.py",
            "async_write_capability": "hub_management.py",
            "async_set_collector_server_endpoint": "hub_management.py",
            "async_capture_support_evidence": "hub_support.py",
            "_async_detect_driver": "hub_detection.py",
            "_build_snapshot": "hub_snapshot.py",
            "async_start_proxy_capture_route": "hub_lifecycle.py",
            "async_start_shadow_learning_route": "hub_lifecycle.py",
        }
        actual: dict[str, str] = {}
        for filename, class_name in MIXINS.items():
            for method in _methods(_class(RUNTIME / filename, class_name)):
                if method in expected:
                    actual[method] = filename
        self.assertEqual(actual, expected)

    def test_shared_module_is_not_a_second_hub_or_state_owner(self) -> None:
        common = RUNTIME / "hub_common.py"
        classes = [node.name for node in _tree(common).body if isinstance(node, ast.ClassDef)]
        self.assertNotIn("EybondHub", classes)
        self.assertFalse(any(name.startswith("Hub") for name in classes))
        imports = _imported_modules(common)
        self.assertFalse(any(module == "hub" or module.endswith(".hub") for module in imports))


if __name__ == "__main__":
    unittest.main()
