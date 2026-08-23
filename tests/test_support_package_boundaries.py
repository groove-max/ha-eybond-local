"""Architecture guards for proxy-capture and shadow-learning support packages."""

from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SUPPORT = ROOT / "custom_components" / "eybond_local" / "support"
PROXY_CAPTURE = SUPPORT / "proxy_capture"
SHADOW_LEARNING = SUPPORT / "shadow_learning"


def _definitions(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _imported_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    ):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    return modules


class SupportPackageBoundaryTests(unittest.TestCase):
    def test_flat_compatibility_modules_do_not_return(self) -> None:
        retired = (
            "proxy_capture.py",
            "proxy_session.py",
            "proxy_trace.py",
            "shadow_learning.py",
            "shadow_learning_backend.py",
            "shadow_learning_orchestrator.py",
            "shadow_learning_overlay_generator.py",
            "shadow_learning_protocol.py",
            "shadow_learning_proxy.py",
            "shadow_learning_review_model.py",
            "shadow_learning_runtime.py",
            "shadow_learning_session.py",
            "valuecloud_shadow_learning_orchestrator.py",
        )
        self.assertEqual([name for name in retired if (SUPPORT / name).exists()], [])

    def test_packages_have_the_exact_implementation_inventory(self) -> None:
        self.assertEqual(
            {path.name for path in PROXY_CAPTURE.glob("*.py")},
            {"__init__.py", "session.py", "trace.py"},
        )
        self.assertEqual(
            {path.name for path in SHADOW_LEARNING.glob("*.py")},
            {
                "__init__.py",
                "backend.py",
                "cloud_dispatch.py",
                "dessmonitor_orchestrator.py",
                "orchestrator.py",
                "overlay_generator.py",
                "protocol.py",
                "proxy.py",
                "read_evidence.py",
                "review_model.py",
                "runtime.py",
                "session.py",
                "valuecloud_orchestrator.py",
            },
        )

    def test_concrete_types_have_one_owner(self) -> None:
        expected = {
            "ProxyCaptureSessionState": PROXY_CAPTURE / "trace.py",
            "InProcessProxyCaptureHandler": PROXY_CAPTURE / "session.py",
            "ShadowWriteObservation": SHADOW_LEARNING / "__init__.py",
            "InProcessShadowLearningHandler": SHADOW_LEARNING / "backend.py",
            "InProcessFailClosedShadowProxyHandler": SHADOW_LEARNING / "proxy.py",
        }
        all_paths = tuple(PROXY_CAPTURE.glob("*.py")) + tuple(
            SHADOW_LEARNING.glob("*.py")
        )
        for name, owner in expected.items():
            self.assertEqual(
                [path for path in all_paths if name in _definitions(path)],
                [owner],
                name,
            )

    def test_lifecycle_packages_remain_separate(self) -> None:
        proxy_imports = set().union(
            *(_imported_modules(path) for path in PROXY_CAPTURE.glob("*.py"))
        )
        shadow_imports = set().union(
            *(_imported_modules(path) for path in SHADOW_LEARNING.glob("*.py"))
        )
        self.assertFalse(any("shadow_learning" in name for name in proxy_imports))
        self.assertFalse(any("proxy_capture" in name for name in shadow_imports))

    def test_file_relative_resources_still_resolve_from_nested_packages(self) -> None:
        from custom_components.eybond_local.support.proxy_capture.session import (
            build_proxy_capture_command,
        )
        from custom_components.eybond_local.support.shadow_learning.protocol import (
            _EYBOND_G_ASCII_COMMAND_SCHEMA_PATH,
        )

        command = build_proxy_capture_command(
            listen_host="127.0.0.1",
            listen_port=18899,
            upstream_host="example.com",
            upstream_port=18899,
            output_path=Path("trace.jsonl"),
        )
        self.assertEqual(Path(command[2]), SUPPORT / "collector_cloud_proxy.py")
        self.assertTrue(_EYBOND_G_ASCII_COMMAND_SCHEMA_PATH.is_file())


if __name__ == "__main__":
    unittest.main()
