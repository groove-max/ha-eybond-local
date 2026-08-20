"""Architecture guards for the integration package lifecycle family."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_PACKAGE = REPO_ROOT / "custom_components" / "eybond_local"
_FAMILY_NAMES = (
    "__init__.py",
    "integration_common.py",
    "integration_registration.py",
    "integration_metadata.py",
    "integration_entities.py",
    "integration_sensor_precision.py",
    "integration_migration.py",
)
_FAMILY = tuple(_PACKAGE / name for name in _FAMILY_NAMES)
_ORIGINAL_DEFINITION_DIGEST = (
    "9e05b20267b4258b089e9d9cfa967ffd51eceff71ef656ea5e6cc929b011dfb1"
)


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _definitions(path: Path) -> list[tuple[str, str]]:
    return [
        (type(node).__name__, node.name)
        for node in ast.walk(_tree(path))
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    ]


class IntegrationModuleBoundaryTests(unittest.TestCase):
    def test_package_root_is_a_small_home_assistant_lifecycle_composition(self) -> None:
        root = _PACKAGE / "__init__.py"
        self.assertLessEqual(len(root.read_text(encoding="utf-8").splitlines()), 400)
        top_level = {
            node.name
            for node in _tree(root).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertEqual(
            top_level,
            {
                "async_setup",
                "async_setup_entry",
                "async_unload_entry",
                "async_remove_entry",
                "_async_ensure_listener_entry",
                "_async_update_listener",
            },
        )

    def test_original_definition_multiset_is_preserved_exactly_once(self) -> None:
        definitions = [item for path in _FAMILY for item in _definitions(path)]
        payload = "\n".join(
            f"{kind}:{name}" for kind, name in sorted(definitions)
        ).encode()
        self.assertEqual(len(definitions), 54)
        self.assertEqual(len(set(definitions)), 54)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), _ORIGINAL_DEFINITION_DIGEST)

    def test_responsibility_families_have_one_top_level_owner(self) -> None:
        expected = {
            "_register_entry_callback_session_claim": "integration_registration.py",
            "_register_entry_network_reconcile": "integration_registration.py",
            "_async_self_heal_collector_cloud_family": "integration_metadata.py",
            "_async_initial_refresh_for_setup": "integration_metadata.py",
            "_async_cleanup_obsolete_entities": "integration_entities.py",
            "_async_self_heal_sensor_display_precision": "integration_sensor_precision.py",
            "async_migrate_entry": "integration_migration.py",
            "async_setup_entry": "__init__.py",
            "async_remove_entry": "__init__.py",
        }
        actual: dict[str, list[str]] = {name: [] for name in expected}
        for path in _FAMILY:
            for node in _tree(path).body:
                if (
                    isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name in actual
                ):
                    actual[node.name].append(path.name)
        self.assertEqual(actual, {name: [owner] for name, owner in expected.items()})

    def test_implementation_modules_do_not_import_package_root_backwards(self) -> None:
        for path in _FAMILY[1:]:
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("from . import", source, msg=path.name)
            self.assertNotIn("from custom_components.eybond_local import", source, msg=path.name)

    def test_package_exports_the_exact_migration_and_exception_types(self) -> None:
        import custom_components.eybond_local as integration
        from custom_components.eybond_local.integration_common import (
            ConfigEntryError,
            ConfigEntryNotReady,
        )
        from custom_components.eybond_local.integration_migration import (
            async_migrate_entry,
        )

        self.assertIs(integration.ConfigEntryError, ConfigEntryError)
        self.assertIs(integration.ConfigEntryNotReady, ConfigEntryNotReady)
        self.assertIs(integration.async_migrate_entry, async_migrate_entry)

    def test_entity_and_migration_policy_do_not_leak_back_into_root(self) -> None:
        source = (_PACKAGE / "__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("homeassistant.helpers.entity_registry", source)
        self.assertNotIn("connection.connection_policy", source)
        self.assertNotIn("drivers.registry", source)


if __name__ == "__main__":
    unittest.main()
