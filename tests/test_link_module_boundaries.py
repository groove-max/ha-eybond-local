"""Architecture guards for the runtime-link decomposition."""

from __future__ import annotations

import ast
from collections import Counter
import hashlib
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "custom_components" / "eybond_local" / "runtime"
LINK_PACKAGE = RUNTIME / "link"
LINK = LINK_PACKAGE / "__init__.py"
LINK_WIRE_AUTHORITY = LINK_PACKAGE / "wire_authority.py"
POLL_SCHEDULER = RUNTIME / "poll_scheduler.py"
POLL_POLICY_SHIM = RUNTIME / "poll_policy.py"
SESSION_HANDLE = ROOT / "custom_components" / "eybond_local" / "connection" / "session_handle.py"

MIXINS = {
    "session_projection.py": "LinkSessionProjectionMixin",
    "lifecycle.py": "LinkLifecycleMixin",
    "callback.py": "LinkCallbackMixin",
    "cloud_routes.py": "LinkCloudRoutesMixin",
    "connection.py": "LinkConnectionMixin",
    "wire_authority.py": "LinkWireAuthorityMixin",
    "transport_lifecycle.py": "LinkTransportLifecycleMixin",
}

EXPECTED_MRO = [
    "LinkSessionProjectionMixin",
    "LinkLifecycleMixin",
    "LinkCallbackMixin",
    "LinkCloudRoutesMixin",
    "LinkConnectionMixin",
    "LinkWireAuthorityMixin",
    "LinkTransportLifecycleMixin",
]

EXPECTED_METHOD_MULTISET_SHA256 = (
    "5f2f8c893414c94ad7639d001cf4c382030f368f1130169363add4e4a7da267c"
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


class LinkCompositionBoundaryTests(unittest.TestCase):
    def test_composition_root_contains_only_constructor_logic(self) -> None:
        link = _class(LINK, "EybondRuntimeLinkManager")
        self.assertEqual(_methods(link), ("__init__",))
        self.assertLessEqual(len(LINK.read_text(encoding="utf-8").splitlines()), 220)

    def test_mro_is_explicit_and_stable(self) -> None:
        link = _class(LINK, "EybondRuntimeLinkManager")
        bases = [
            base.id if isinstance(base, ast.Name) else ast.unparse(base)
            for base in link.bases
        ]
        self.assertEqual(bases, EXPECTED_MRO)

    def test_every_original_method_has_one_owner_and_matching_multiplicity(self) -> None:
        owners: dict[str, set[str]] = {}
        counts: Counter[str] = Counter()
        classes = [("__init__.py", _class(LINK, "EybondRuntimeLinkManager"))]
        classes.extend(
            (filename, _class(LINK_PACKAGE / filename, class_name))
            for filename, class_name in MIXINS.items()
        )
        for filename, lifecycle in classes:
            for method in _methods(lifecycle):
                owners.setdefault(method, set()).add(filename)
                counts[method] += 1

        self.assertEqual(
            {name: paths for name, paths in owners.items() if len(paths) != 1},
            {},
        )
        payload = "\n".join(f"{name}:{count}" for name, count in sorted(counts.items()))
        self.assertEqual(sum(counts.values()), 118)
        self.assertEqual(len(counts), 118)
        self.assertEqual(
            hashlib.sha256(payload.encode()).hexdigest(),
            EXPECTED_METHOD_MULTISET_SHA256,
        )

    def test_mixins_have_no_constructor_or_link_back_import(self) -> None:
        for filename, class_name in MIXINS.items():
            path = LINK_PACKAGE / filename
            self.assertNotIn("__init__", _methods(_class(path, class_name)), filename)
            imports = _imported_modules(path)
            self.assertFalse(
                any(module == "link" or module.endswith(".link") for module in imports),
                filename,
            )

    def test_one_class_owns_each_stateful_family(self) -> None:
        expected = {
            "async_trigger_reverse_discovery": "callback.py",
            "_async_callback_connect_within_causality": "connection.py",
            "async_start_proxy_capture_route": "cloud_routes.py",
            "async_start_shadow_learning_route": "cloud_routes.py",
            "_live_session_handle": "wire_authority.py",
            "_effective_wire_binding": "wire_authority.py",
            "_apply_live_wire_to_transports": "wire_authority.py",
            "_build_transport_pair": "transport_lifecycle.py",
            "async_reconcile_collector_session_profile": "lifecycle.py",
        }
        actual: dict[str, str] = {}
        for filename, class_name in MIXINS.items():
            for method in _methods(_class(LINK_PACKAGE / filename, class_name)):
                if method in expected:
                    actual[method] = filename
        self.assertEqual(actual, expected)

    def test_common_module_is_not_a_second_link_or_session_authority(self) -> None:
        common = LINK_PACKAGE / "common.py"
        class_names = {
            node.name for node in _tree(common).body if isinstance(node, ast.ClassDef)
        }
        self.assertNotIn("EybondRuntimeLinkManager", class_names)
        self.assertFalse(any(name.startswith("Link") and name.endswith("Mixin") for name in class_names))
        imports = _imported_modules(common)
        self.assertFalse(any(module == "link" or module.endswith(".link") for module in imports))

    def test_retired_wire_and_polling_compatibility_shims_do_not_return(self) -> None:
        session_tree = _tree(SESSION_HANDLE)
        module_functions = {
            node.name
            for node in session_tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        module_assignments = {
            target.id
            for node in session_tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        session_handle = next(
            node
            for node in session_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "SessionHandle"
        )

        self.assertNotIn("negotiate_wire", module_functions)
        self.assertTrue(
            {
                "ADAPTER_FRAMED_FORWARD",
                "ADAPTER_FRAMED_COLLECTOR_COMMANDS",
                "ADAPTER_AT_COMMANDS",
                "ADAPTER_RAW_PASSTHROUGH",
            }.isdisjoint(module_assignments)
        )
        self.assertTrue({"wire", "identity_source"}.isdisjoint(_methods(session_handle)))
        self.assertNotIn(
            "_uses_at_text_payload",
            _methods(_class(LINK_WIRE_AUTHORITY, "LinkWireAuthorityMixin")),
        )

        self.assertFalse(POLL_POLICY_SHIM.exists())
        self.assertTrue(
            any(
                isinstance(node, ast.ImportFrom)
                and node.level == 2
                and node.module == "poll_policy"
                for node in _tree(POLL_SCHEDULER).body
            )
        )


if __name__ == "__main__":
    unittest.main()
