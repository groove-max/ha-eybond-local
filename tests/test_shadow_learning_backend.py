from __future__ import annotations

import ast
import asyncio
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.metadata.effective_metadata_snapshot import (
    EffectiveMetadataSnapshot,
)
from custom_components.eybond_local.payload.modbus import build_write_multiple_request
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager
from custom_components.eybond_local.support.shadow_learning_backend import (
    InProcessShadowLearningHandler,
    ShadowLearningSeed,
    build_shadow_learning_preflight,
    build_shadow_learning_seed,
)


def _sample_snapshot() -> EffectiveMetadataSnapshot:
    return EffectiveMetadataSnapshot(
        effective_owner_key="modbus_smg",
        effective_owner_name="Modbus SMG",
        variant_key="default",
        profile_name="modbus_smg/default.json",
        register_schema_name="modbus_smg/default.json",
        confidence="high",
        generation=1,
        generated_at="2026-06-05T12:00:00+00:00",
    )


def _sample_raw_capture() -> dict[str, object]:
    return {
        "capture_kind": "generic_register_dump",
        "responses": {"WFSS": "1"},
        "captures": [
            {
                "driver_key": "modbus_smg",
                "fixture_ranges": [
                    {"start": 300, "count": 2, "values": [1, 2]},
                    {"start": 305, "count": 1, "values": [10]},
                ],
                "range_failures": [],
            }
        ],
    }


class ShadowLearningBackendTests(unittest.TestCase):
    def test_coordinator_exposes_shadow_learning_lifecycle_methods(self) -> None:
        coordinator_path = REPO_ROOT / "custom_components/eybond_local/runtime/coordinator.py"
        tree = ast.parse(coordinator_path.read_text(encoding="utf-8"))

        class_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondLocalCoordinator"
        )
        method_names = {
            node.name
            for node in class_node.body
            if isinstance(node, ast.AsyncFunctionDef)
        }

        self.assertIn("async_start_shadow_learning", method_names)
        self.assertIn("async_stop_shadow_learning", method_names)

    def test_seed_builder_uses_raw_capture_and_synthesizes_required_at_responses(self) -> None:
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000025388419",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.command_responses["CLDSRVHOST1"], "192.168.1.50,18899,TCP")
        self.assertEqual(seed.command_responses["QID"], "E5000025388419")
        self.assertEqual(seed.command_responses["WFSS"], "1")
        self.assertEqual(seed.register_bank[300], 1)
        self.assertEqual(seed.register_bank[301], 2)
        self.assertEqual(seed.register_bank[305], 10)

    def test_seed_builder_reports_explicit_register_blocker_when_seed_is_missing(self) -> None:
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000025388419",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=None,
            write_response_mode="exception",
        )

        self.assertEqual(seed.register_bank, {})
        self.assertIn("missing_register_seed", blockers)
        self.assertEqual(build_shadow_learning_preflight(seed).blockers, blockers)

    def test_exception_mode_logs_write_without_mutating_register_bank(self) -> None:
        seed = ShadowLearningSeed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000025388419",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot().as_dict(),
            command_responses={"CLDSRVHOST1": "192.168.1.50,18899,TCP", "QID": "E5000025388419"},
            register_bank={300: 1, 301: 2},
            latest_support_evidence=_sample_raw_capture(),
            write_response_mode="exception",
            allow_ack_writes=False,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = InProcessShadowLearningHandler(
                seed=seed,
                output_path=Path(temp_dir) / "shadow.jsonl",
            )

            async def _run() -> None:
                await handler.start()
                frame = build_collector_request(
                    1,
                    build_write_multiple_request(1, 300, [7]),
                    devcode=2376,
                    collector_addr=1,
                    fcode=4,
                )
                response = await handler._handle_frame(frame, remote="192.168.1.15:50000")
                self.assertIsNotNone(response)
                assert response is not None
                self.assertEqual(handler.register_bank_snapshot[300], 1)
                self.assertEqual(handler.register_bank_snapshot[301], 2)
                await handler.stop()

            asyncio.run(_run())

            log_text = (Path(temp_dir) / "shadow.jsonl").read_text(encoding="utf-8")
            self.assertIn("shadow_modbus_write_observation", log_text)
            self.assertIn("shadow_modbus_write_response", log_text)

    def test_ack_mode_mutates_register_bank_and_returns_ack(self) -> None:
        seed = ShadowLearningSeed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000025388419",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot().as_dict(),
            command_responses={"CLDSRVHOST1": "192.168.1.50,18899,TCP", "QID": "E5000025388419"},
            register_bank={300: 1, 301: 2},
            latest_support_evidence=_sample_raw_capture(),
            write_response_mode="ack",
            allow_ack_writes=True,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = InProcessShadowLearningHandler(
                seed=seed,
                output_path=Path(temp_dir) / "shadow.jsonl",
            )

            async def _run() -> None:
                await handler.start()
                frame = build_collector_request(
                    1,
                    build_write_multiple_request(1, 300, [7]),
                    devcode=2376,
                    collector_addr=1,
                    fcode=4,
                )
                response = await handler._handle_frame(frame, remote="192.168.1.15:50000")
                self.assertIsNotNone(response)
                assert response is not None
                self.assertEqual(handler.register_bank_snapshot[300], 7)
                await handler.stop()

            asyncio.run(_run())

    def test_link_manager_shadow_route_lifecycle_isolated_from_proxy_capture(self) -> None:
        manager = EybondRuntimeLinkManager(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.15",
            tcp_port=18899,
            udp_port=18898,
            discovery_target="192.168.1.15",
            discovery_interval=30,
            heartbeat_interval=10,
        )

        events: list[tuple[str, object]] = []

        class _Handler:
            def __init__(self, *, upstream_host, upstream_port, seed, output_path) -> None:
                events.append(("handler_init", (upstream_host, upstream_port, output_path)))
                self.running = False
                self.ready = False

            async def start(self) -> None:
                self.running = True
                events.append(("handler_start", None))

            async def stop(self) -> None:
                self.running = False
                events.append(("handler_stop", None))

            async def handle_client(self, reader, writer) -> None:
                pass

            def status(self) -> dict[str, object]:
                return {
                    "running": self.running,
                    "collector_connected": False,
                    "upstream_connected": False,
                    "ready": self.ready,
                    "upstream_error": "",
                }

        class _Route:
            def __init__(self, **kwargs) -> None:
                events.append(("route_init", kwargs))

            async def start(self) -> None:
                events.append(("route_start", None))

            async def stop(self) -> None:
                events.append(("route_stop", None))

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000025388419",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
        )
        self.assertFalse(blockers)

        async def _run() -> None:
            with patch("custom_components.eybond_local.runtime.link.InProcessFailClosedShadowProxyHandler", _Handler), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_shadow_learning_route(
                    collector_ip="192.168.1.15",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/shadow.jsonl"),
                    seed=seed,
                )
                self.assertTrue(manager.shadow_learning_route_running())
                await manager.async_stop_shadow_learning_route()
                self.assertFalse(manager.shadow_learning_route_running())

        asyncio.run(_run())

        self.assertEqual(
            [event for event, _ in events],
            ["handler_init", "handler_start", "route_init", "route_start", "route_stop", "handler_stop"],
        )
        route_kwargs = dict(events[2][1])
        self.assertEqual(route_kwargs["host"], "0.0.0.0")
        self.assertEqual(route_kwargs["port"], 502)
        self.assertEqual(route_kwargs["collector_ip"], "192.168.1.15")

    def test_shadow_start_blocks_when_proxy_capture_is_running(self) -> None:
        manager = EybondRuntimeLinkManager(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.15",
            tcp_port=18899,
            udp_port=18898,
            discovery_target="192.168.1.15",
            discovery_interval=30,
            heartbeat_interval=10,
        )
        stop_calls: list[str] = []

        class _ExistingProxyHandler:
            running = True

            async def stop(self) -> None:
                stop_calls.append("handler_stop")

        class _ExistingProxyRoute:
            async def stop(self) -> None:
                stop_calls.append("route_stop")

        manager._proxy_capture_handler = _ExistingProxyHandler()  # type: ignore[attr-defined]
        manager._proxy_capture_route = _ExistingProxyRoute()  # type: ignore[attr-defined]

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000025388419",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
        )
        self.assertFalse(blockers)

        async def _run() -> None:
            with self.assertRaisesRegex(RuntimeError, "proxy_capture_route_running"):
                await manager.async_start_shadow_learning_route(
                    collector_ip="192.168.1.15",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/shadow.jsonl"),
                    seed=seed,
                )

        asyncio.run(_run())
        self.assertEqual(stop_calls, [])

    def test_proxy_capture_start_blocks_when_shadow_learning_is_running(self) -> None:
        manager = EybondRuntimeLinkManager(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.15",
            tcp_port=18899,
            udp_port=18898,
            discovery_target="192.168.1.15",
            discovery_interval=30,
            heartbeat_interval=10,
        )
        stop_calls: list[str] = []

        class _ExistingShadowHandler:
            running = True

            async def stop(self) -> None:
                stop_calls.append("handler_stop")

        class _ExistingShadowRoute:
            async def stop(self) -> None:
                stop_calls.append("route_stop")

        manager._shadow_learning_handler = _ExistingShadowHandler()  # type: ignore[attr-defined]
        manager._shadow_learning_route = _ExistingShadowRoute()  # type: ignore[attr-defined]

        async def _run() -> None:
            with self.assertRaisesRegex(RuntimeError, "shadow_learning_route_running"):
                await manager.async_start_proxy_capture_route(
                    collector_ip="192.168.1.15",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/proxy.jsonl"),
                    masked_endpoint="cloud.example,18899,TCP",
                    restore_trigger_path=Path("/tmp/proxy.restore"),
                )

        asyncio.run(_run())
        self.assertEqual(stop_calls, [])


if __name__ == "__main__":
    unittest.main()
