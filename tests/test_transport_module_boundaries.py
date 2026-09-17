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
    "bf3c1f383e607e3ea019b1ddde0456f02a37414acf1df0aadd6272266125045d"
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
        # Explicit extension, not a relaxation of the decomposition baseline.
        # Two socket owners + SharedEybondTransport facade delegate.
        extension = ("AsyncFunctionDef", "async_send_auxiliary_read")
        self.assertEqual(definitions.count(extension), 3)
        definitions = [item for item in definitions if item != extension]
        payload = "\n".join(
            f"{kind}:{name}" for kind, name in sorted(definitions)
        ).encode()
        self.assertEqual(len(definitions), 264)
        self.assertEqual(len(set(definitions)), 204)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), _ORIGINAL_DEFINITION_DIGEST)

    def test_auxiliary_reads_are_owned_by_both_socket_implementations(self) -> None:
        owners = []
        for node in _tree(_TRANSPORT / "connections.py").body:
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    if isinstance(child, ast.AsyncFunctionDef):
                        if child.name == "async_send_auxiliary_read":
                            owners.append(node.name)
        self.assertEqual(owners, ["_CollectorConnection", "_CollectorAtConnection"])
        source = (_TRANSPORT / "auxiliary_session.py").read_text(encoding="utf-8")
        for forbidden in ("...models", "...drivers", "...metadata", "...payload"):
            self.assertNotIn(forbidden, source)

    def test_framed_facade_exposes_auxiliary_read_as_delegate_only(self) -> None:
        framed = _tree(_TRANSPORT / "shared_framed.py")
        method = None
        for node in framed.body:
            if isinstance(node, ast.ClassDef) and node.name == "SharedEybondTransport":
                for child in node.body:
                    if (
                        isinstance(child, ast.AsyncFunctionDef)
                        and child.name == "async_send_auxiliary_read"
                    ):
                        method = child
                        break
        self.assertIsNotNone(method)
        assert method is not None
        source = ast.unparse(method)
        self.assertIn("async_send_auxiliary_read", source)
        self.assertIn("_active_connection_for_send", source)
        self.assertNotIn("BinaryGrammar", source)
        self.assertNotIn("auxiliary_session", source)
        self.assertNotIn("AA BB", source)
        self.assertNotIn("\\xaa\\xbb", source)

    def test_drivers_must_not_infer_or_call_auxiliary_reads(self) -> None:
        drivers_root = _COLLECTOR.parent / "drivers"
        forbidden = (
            "async_send_auxiliary_read",
            "auxiliary_session",
            "BinaryGrammar.AABB",
            "\\xaa\\xbb",
            "AA BB",
        )
        for path in sorted(drivers_root.glob("*.py")):
            source = path.read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(
                    token,
                    source,
                    msg=f"{path.name} must not infer or call aux via {token!r}",
                )

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
        self.assertTrue(callable(SharedEybondTransport.async_send_auxiliary_read))


class FramedFacadeAuxiliaryAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_facade_delegates_auxiliary_read_timeout_and_payload(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from custom_components.eybond_local.collector.transport.shared_framed import (
            SharedEybondTransport,
        )

        transport = SharedEybondTransport(
            host="127.0.0.1",
            port=18899,
            request_timeout=3.0,
            heartbeat_interval=60.0,
            collector_ip="192.0.2.10",
        )
        connection = MagicMock()
        connection.async_send_auxiliary_read = AsyncMock(return_value=b"\xaa\xbb")
        transport._active_connection_for_send = AsyncMock(return_value=connection)

        query = b"\x5a\xa5\x02\x00" + bytes(16) + bytes([0x02])
        reply = await transport.async_send_auxiliary_read(query, request_timeout=1.5)

        self.assertEqual(reply, b"\xaa\xbb")
        connection.async_send_auxiliary_read.assert_awaited_once_with(
            query,
            request_timeout=1.5,
        )


if __name__ == "__main__":
    unittest.main()
