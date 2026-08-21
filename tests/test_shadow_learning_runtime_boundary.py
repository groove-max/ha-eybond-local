from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support.cloud_evidence import (
    CloudEvidenceRecord,
)
from custom_components.eybond_local.support.shadow_learning import (
    ShadowWriteObservation,
)
from custom_components.eybond_local.support.shadow_learning_runtime import (
    ShadowLearningRouteStatus,
    ShadowLearningRuntimeView,
)
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager
from custom_components.eybond_local.runtime.shadow_learning_facade import (
    ShadowLearningRuntimeFacade,
)

PRODUCTION_ROOT = REPO_ROOT / "custom_components" / "eybond_local"


class ShadowLearningRuntimeModelTests(unittest.TestCase):
    def test_route_status_sanitizes_untyped_lower_layer_values(self) -> None:
        status = ShadowLearningRouteStatus.from_mapping(
            {
                "running": "false",
                "collector_connected": 1,
                "collector_protocol_ingress": True,
                "route_protocol_activity": False,
                "upstream_connected": object(),
                "ready": True,
                "upstream_error": object(),
            }
        )

        self.assertFalse(status.running)
        self.assertFalse(status.collector_connected)
        self.assertTrue(status.collector_protocol_ingress)
        self.assertFalse(status.route_protocol_activity)
        self.assertFalse(status.upstream_connected)
        self.assertTrue(status.ready)
        self.assertEqual(status.upstream_error, "")

    def test_direct_route_status_constructor_is_strict(self) -> None:
        with self.assertRaises(TypeError):
            ShadowLearningRouteStatus(running=1)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            ShadowLearningRouteStatus(upstream_error=object())  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            ShadowLearningRouteStatus(upstream_error=" padded ")

    def test_runtime_view_accepts_only_exact_public_models(self) -> None:
        observation = ShadowWriteObservation(
            register=7,
            values=(1,),
            function_code=16,
            devcode=None,
            devaddr=None,
            raw_payload_hex="",
        )
        evidence = CloudEvidenceRecord(
            path=Path("evidence.json"),
            payload={"provider": "smartess"},
        )
        view = ShadowLearningRuntimeView(
            route_status=ShadowLearningRouteStatus(running=True),
            cloud_evidence=evidence,
            write_observations=(observation,),
        )

        self.assertTrue(view.route_status.running)
        self.assertIs(view.cloud_evidence, evidence)
        self.assertEqual(view.write_observations, (observation,))

        with self.assertRaises(TypeError):
            ShadowLearningRuntimeView(
                route_status=SimpleNamespace(running=True)  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            ShadowLearningRuntimeView(
                cloud_evidence=SimpleNamespace(  # type: ignore[arg-type]
                    path=Path("evidence.json"),
                    payload={},
                )
            )
        with self.assertRaises(TypeError):
            ShadowLearningRuntimeView(
                write_observations=[observation]  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            ShadowLearningRuntimeView(
                write_observations=(SimpleNamespace(register=7),)  # type: ignore[arg-type]
            )


class ShadowLearningObservationPortTests(unittest.IsolatedAsyncioTestCase):
    async def test_link_port_projects_active_handler_without_exposing_it(self) -> None:
        observation = ShadowWriteObservation(
            register=7,
            values=(1,),
            function_code=16,
            devcode=None,
            devaddr=None,
            raw_payload_hex="",
        )

        class _Handler:
            write_observations = (observation,)
            read_map = {"registers": {"7": [1]}}

            @staticmethod
            def observation_cursor() -> int:
                return 1

            @staticmethod
            def observations_since(
                cursor: int,
            ) -> tuple[ShadowWriteObservation, ...]:
                return (observation,) if cursor == 0 else ()

            @staticmethod
            async def wait_for_observations_since(
                cursor: int,
                *,
                timeout_seconds: float,
            ) -> tuple[ShadowWriteObservation, ...]:
                del timeout_seconds
                return (observation,) if cursor == 0 else ()

        manager = object.__new__(EybondRuntimeLinkManager)
        manager._shadow_learning_handler = _Handler()

        self.assertEqual(manager.shadow_learning_write_observations(), (observation,))
        self.assertEqual(manager.shadow_learning_observation_cursor(), 1)
        self.assertEqual(
            manager.shadow_learning_observations_since(0),
            (observation,),
        )
        self.assertEqual(
            await manager.async_wait_for_shadow_learning_observations_since(
                0,
                timeout_seconds=0.1,
            ),
            (observation,),
        )
        read_map = manager.shadow_learning_read_map_snapshot()
        self.assertEqual(read_map, {"registers": {"7": [1]}})
        self.assertIsNot(read_map, _Handler.read_map)

    async def test_link_port_is_empty_without_an_active_handler(self) -> None:
        manager = object.__new__(EybondRuntimeLinkManager)
        manager._shadow_learning_handler = None

        self.assertEqual(manager.shadow_learning_write_observations(), ())
        self.assertEqual(manager.shadow_learning_observation_cursor(), 0)
        self.assertEqual(manager.shadow_learning_observations_since(0), ())
        self.assertEqual(
            await manager.async_wait_for_shadow_learning_observations_since(
                0,
                timeout_seconds=0.0,
            ),
            (),
        )
        self.assertEqual(manager.shadow_learning_read_map_snapshot(), {})

    async def test_link_port_rejects_malformed_cursor_and_timeout(self) -> None:
        manager = object.__new__(EybondRuntimeLinkManager)
        manager._shadow_learning_handler = None

        for cursor in (-1, True, 1.0, "1", None):
            with self.subTest(cursor=cursor):
                with self.assertRaises(ValueError):
                    manager.shadow_learning_observations_since(cursor)  # type: ignore[arg-type]
        for timeout in (-1, True, "1", None):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    await manager.async_wait_for_shadow_learning_observations_since(
                        0,
                        timeout_seconds=timeout,  # type: ignore[arg-type]
                    )


class ShadowLearningRuntimeFacadeTests(unittest.IsolatedAsyncioTestCase):
    async def test_facade_exposes_one_typed_public_boundary(self) -> None:
        observation = ShadowWriteObservation(
            register=7,
            values=(1,),
            function_code=16,
            devcode=None,
            devaddr=None,
            raw_payload_hex="",
        )
        evidence = CloudEvidenceRecord(
            path=Path("evidence.json"),
            payload={"provider": "smartess"},
        )

        class _Runtime:
            @staticmethod
            def shadow_learning_route_status() -> dict[str, object]:
                return {"running": True, "ready": True}

            @staticmethod
            def shadow_learning_write_observations():
                return (observation,)

            @staticmethod
            async def async_capture_support_evidence():
                return {"captured": True}

            @staticmethod
            def shadow_learning_observation_cursor() -> int:
                return 1

            @staticmethod
            def shadow_learning_observations_since(cursor: int):
                return (observation,) if cursor == 0 else ()

            @staticmethod
            async def async_wait_for_shadow_learning_observations_since(
                cursor: int,
                *,
                timeout_seconds: float,
            ):
                del timeout_seconds
                return (observation,) if cursor == 0 else ()

            @staticmethod
            def shadow_learning_read_map_snapshot():
                return {"registers": {"7": [1]}}

        facade = ShadowLearningRuntimeFacade(
            runtime=_Runtime(),
            cloud_evidence_provider=lambda: evidence,
        )

        self.assertTrue(facade.view.route_status.running)
        self.assertIs(facade.view.cloud_evidence, evidence)
        self.assertEqual(facade.view.write_observations, (observation,))
        self.assertEqual(
            await facade.async_capture_support_evidence(),
            {"captured": True},
        )
        self.assertEqual(facade.observation_cursor(), 1)
        self.assertEqual(facade.observations_since(0), (observation,))
        self.assertEqual(
            await facade.async_wait_for_observations_since(0, 0.1),
            (observation,),
        )
        self.assertEqual(
            facade.read_map_snapshot(),
            {"registers": {"7": [1]}},
        )

    async def test_facade_fails_closed_when_optional_runtime_port_is_absent(
        self,
    ) -> None:
        facade = ShadowLearningRuntimeFacade(
            runtime=object(),
            cloud_evidence_provider=lambda: None,
        )

        self.assertEqual(facade.view, ShadowLearningRuntimeView())
        self.assertEqual(await facade.async_capture_support_evidence(), {})
        self.assertEqual(facade.observation_cursor(), 0)
        self.assertEqual(facade.observations_since(0), ())
        self.assertEqual(
            await facade.async_wait_for_observations_since(0, 0.0),
            (),
        )
        self.assertEqual(facade.read_map_snapshot(), {})

        for cursor in (-1, True, 1.0, "1", None):
            with self.subTest(cursor=cursor):
                with self.assertRaises(ValueError):
                    facade.observations_since(cursor)  # type: ignore[arg-type]
        for timeout in (-1, True, "1", None):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    await facade.async_wait_for_observations_since(
                        0,
                        timeout,  # type: ignore[arg-type]
                    )

    async def test_optional_projection_failures_do_not_erase_live_route(self) -> None:
        class _Runtime:
            @staticmethod
            def shadow_learning_route_status() -> dict[str, object]:
                return {
                    "running": True,
                    "collector_connected": True,
                    "collector_protocol_ingress": True,
                    "route_protocol_activity": True,
                    "upstream_connected": True,
                    "ready": True,
                    "upstream_error": "",
                }

            @staticmethod
            def shadow_learning_write_observations():
                return (SimpleNamespace(register=7),)

        def _broken_evidence_provider():
            raise RuntimeError("supplemental_evidence_unavailable")

        facade = ShadowLearningRuntimeFacade(
            runtime=_Runtime(),
            cloud_evidence_provider=_broken_evidence_provider,
        )

        view = facade.view
        self.assertTrue(view.route_status.running)
        self.assertTrue(view.route_status.ready)
        self.assertIsNone(view.cloud_evidence)
        self.assertEqual(view.write_observations, ())


class ShadowLearningRuntimeArchitectureTests(unittest.TestCase):
    def test_production_consumers_do_not_access_coordinator_privates(self) -> None:
        violations: list[str] = []
        coordinator_package = PRODUCTION_ROOT / "runtime" / "coordinator"

        for path in sorted(PRODUCTION_ROOT.rglob("*.py")):
            if coordinator_package in path.parents:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "coordinator"
                    and node.attr.startswith("_")
                ):
                    violations.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}:"
                        f"coordinator.{node.attr}"
                    )
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "getattr"
                    and len(node.args) >= 2
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id == "coordinator"
                    and isinstance(node.args[1], ast.Constant)
                    and isinstance(node.args[1].value, str)
                    and node.args[1].value.startswith("_")
                ):
                    violations.append(
                        f"{path.relative_to(REPO_ROOT)}:{node.lineno}:"
                        f"getattr(coordinator, {node.args[1].value!r})"
                    )

        self.assertEqual(violations, [])

    def test_shadow_handler_is_not_exposed_to_config_flow(self) -> None:
        onboarding_source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(PRODUCTION_ROOT.glob("config_*.py"))
        )
        options_source = (PRODUCTION_ROOT / "options_shadow_runtime.py").read_text(
            encoding="utf-8"
        )
        lifecycle_source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(PRODUCTION_ROOT.glob("options_*.py"))
        )
        source = onboarding_source + "\n" + lifecycle_source

        self.assertNotIn("_shadow_learning_observation_source", source)
        self.assertNotIn("_shadow_learning_handler", source)
        self.assertNotIn("._link_manager", source)
        self.assertIn("ShadowLearningRuntimeFacade", options_source)
        self.assertIn("_shadow_learning_runtime", options_source)

    def test_public_observation_port_exists_at_each_runtime_layer(self) -> None:
        required_methods = {
            "shadow_learning_write_observations",
            "shadow_learning_observation_cursor",
            "shadow_learning_observations_since",
            "async_wait_for_shadow_learning_observations_since",
            "shadow_learning_read_map_snapshot",
        }
        files_and_classes = (
            (PRODUCTION_ROOT / "runtime" / "manager.py", "RuntimeManager"),
            (
                PRODUCTION_ROOT / "runtime" / "link" / "cloud_routes.py",
                "LinkCloudRoutesMixin",
            ),
            (
                PRODUCTION_ROOT / "runtime" / "hub" / "lifecycle.py",
                "HubLifecycleMixin",
            ),
        )

        for path, class_name in files_and_classes:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            target = next(
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            )
            defined = {
                node.name
                for node in target.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            self.assertTrue(
                required_methods <= defined,
                f"{class_name} missing {sorted(required_methods - defined)}",
            )

        coordinator_source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted(
                (PRODUCTION_ROOT / "runtime" / "coordinator").glob("*.py")
            )
        )
        self.assertIn("def shadow_learning_runtime(", coordinator_source)
        for method_name in required_methods:
            self.assertNotIn(f"def {method_name}(", coordinator_source)


if __name__ == "__main__":
    unittest.main()
