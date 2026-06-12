from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.support.shadow_learning_backend import ShadowLearningSeed
from custom_components.eybond_local.support.shadow_learning_proxy import InProcessFailClosedShadowProxyHandler
from custom_components.eybond_local.support.shadow_learning_session import (
    build_shadow_learning_lease_deadline,
    build_shadow_learning_session_state,
    clear_shadow_learning_session_state,
    load_shadow_learning_session_state,
    shadow_learning_session_is_expired,
    save_shadow_learning_session_state,
    shadow_learning_session_timestamp,
)


def _seed() -> ShadowLearningSeed:
    return ShadowLearningSeed(
        session_id="entry-1_20260605T120000Z",
        entry_id="entry-1",
        collector_pn="E5000020000000",
        collector_cloud_profile_key="smartess_at",
        collector_cloud_profile_label="SmartESS AT",
        collector_cloud_profile_source="runtime_observed",
        collector_cloud_profile_confidence="high",
        collector_callback_endpoint="192.168.1.50,18899,TCP",
        effective_metadata_snapshot={
            "profile_name": "modbus_smg/default.json",
            "register_schema_name": "modbus_smg/default.json",
        },
        command_responses={
            "QID": "E5000020000000",
            "CLDSRVHOST1": "192.168.1.50,18899,TCP",
        },
        register_bank={300: 10, 301: 11, 305: 12},
        latest_support_evidence=None,
        write_response_mode="exception",
        allow_ack_writes=False,
    )


async def _wait_for_ready(handler: InProcessFailClosedShadowProxyHandler, timeout: float = 1.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if handler.ready:
            return True
        await asyncio.sleep(0.02)
    return False


class ShadowLearningRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_and_readiness_with_fake_endpoints(self) -> None:
        collector_frames: list[bytes] = []

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            collector_frames.append(await reader.readexactly(12))
            await asyncio.sleep(0.05)
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_runtime.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            writer.write(build_collector_request(7, b"ping", devcode=2376, collector_addr=1, fcode=1))
            await writer.drain()
            self.assertTrue(await _wait_for_ready(handler, timeout=1.0))
            self.assertTrue(handler.status()["collector_protocol_ingress"])

            writer.close()
            await writer.wait_closed()
            await reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

        self.assertEqual(len(collector_frames), 1)

    async def test_silent_collector_socket_is_running_but_not_ready(self) -> None:
        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await asyncio.sleep(0.2)
            writer.close()
            await writer.wait_closed()
            await reader.read()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_runtime.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            await asyncio.sleep(0.05)
            status = handler.status()
            self.assertTrue(status["running"])
            self.assertTrue(status["collector_connected"])
            self.assertFalse(status["collector_protocol_ingress"])
            self.assertTrue(status["upstream_connected"])
            self.assertFalse(status["ready"])

            writer.close()
            await writer.wait_closed()
            await reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

    async def test_server_first_protocol_activity_marks_route_ready(self) -> None:
        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            writer.write(b"AT+DTUPN?\r\n")
            await writer.drain()
            await reader.readline()
            await asyncio.sleep(0.05)
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_runtime.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            self.assertTrue(await _wait_for_ready(handler, timeout=1.0))
            status = handler.status()
            self.assertFalse(status["collector_protocol_ingress"])
            self.assertTrue(status["route_protocol_activity"])
            self.assertTrue(status["ready"])

            writer.close()
            await writer.wait_closed()
            await reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

    async def test_ready_drops_after_upstream_disconnect(self) -> None:
        disconnect_gate = asyncio.Event()

        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.readexactly(12)
            disconnect_gate.set()
            await asyncio.sleep(0.1)
            writer.close()
            await writer.wait_closed()
            await reader.read()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_runtime.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            writer.write(build_collector_request(7, b"ping", devcode=2376, collector_addr=1, fcode=1))
            await writer.drain()
            self.assertTrue(await _wait_for_ready(handler, timeout=1.0))
            await asyncio.wait_for(disconnect_gate.wait(), timeout=1.0)

            deadline = asyncio.get_running_loop().time() + 1.0
            while asyncio.get_running_loop().time() < deadline:
                status = handler.status()
                if not status["upstream_connected"]:
                    self.assertFalse(status["ready"])
                    break
                await asyncio.sleep(0.02)
            else:
                self.fail("upstream disconnect was not reflected in route status")

            writer.close()
            await writer.wait_closed()
            await reader.read()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

    async def test_upstream_failure_sets_status_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=9,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_runtime.jsonl",
            )
            await handler.start()
            proxy_server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            proxy_port = proxy_server.sockets[0].getsockname()[1]

            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            self.assertEqual(await asyncio.wait_for(reader.read(1), timeout=0.5), b"")
            status = handler.status()
            self.assertFalse(status["ready"])
            self.assertFalse(status["upstream_connected"])
            self.assertIn("Error", str(status["upstream_error"]))

            writer.close()
            await writer.wait_closed()
            proxy_server.close()
            await proxy_server.wait_closed()
            await handler.stop()

    async def test_collector_reconnect_timeout_path(self) -> None:
        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await asyncio.sleep(0.2)
            writer.close()
            await writer.wait_closed()
            await reader.read()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=Path(tmp) / "shadow_runtime.jsonl",
            )
            await handler.start()
            self.assertFalse(await _wait_for_ready(handler, timeout=0.15))
            await handler.stop()

        upstream_server.close()
        await upstream_server.wait_closed()

    async def test_explicit_stop_unload_reload_sequence(self) -> None:
        async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.read()
            writer.close()
            await writer.wait_closed()

        upstream_server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
        upstream_port = upstream_server.sockets[0].getsockname()[1]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "shadow_runtime.jsonl"
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1",
                upstream_port=upstream_port,
                seed=_seed(),
                output_path=trace_path,
            )
            await handler.start()
            self.assertTrue(handler.running)
            await handler.stop()
            self.assertFalse(handler.running)

            await handler.start()
            self.assertTrue(handler.running)
            await handler.stop()
            self.assertFalse(handler.running)

        upstream_server.close()
        await upstream_server.wait_closed()

    def test_restart_recovery_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            now = shadow_learning_session_timestamp()
            state = build_shadow_learning_session_state(
                entry_id="entry-1",
                route_owner_id="shadow-learning-entry-1",
                collector_pn="E5000020000000",
                trace_path=str(config_dir / "trace.jsonl"),
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                upstream_endpoint="eu.smartess.io,18899,TCP",
                restore_required=True,
                started_at=now,
                expires_at="2026-06-05T12:10:00+00:00",
                updated_at=now,
                restore_attempt_count=2,
                last_restore_attempt_at="2026-06-05T12:05:00+00:00",
                last_restore_error="collector_write_failed",
                status="starting",
            )
            save_shadow_learning_session_state(config_dir=config_dir, state=state)
            loaded = load_shadow_learning_session_state(config_dir)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.status, "starting")
            self.assertEqual(loaded.route_owner_id, "shadow-learning-entry-1")
            self.assertEqual(loaded.proxy_endpoint, "192.168.1.50,18899,TCP")
            self.assertEqual(loaded.expires_at, "2026-06-05T12:10:00+00:00")
            self.assertEqual(loaded.restore_attempt_count, 2)
            self.assertEqual(loaded.last_restore_error, "collector_write_failed")

            clear_shadow_learning_session_state(config_dir)
            self.assertIsNone(load_shadow_learning_session_state(config_dir))

    def test_restore_failure_state_survives_restart_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            state = build_shadow_learning_session_state(
                entry_id="entry-1",
                collector_pn="E5000020000000",
                trace_path=str(config_dir / "trace.jsonl"),
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                upstream_endpoint="eu.smartess.io,18899,TCP",
                restore_required=True,
                started_at="2026-06-05T12:00:00+00:00",
                expires_at="2026-06-05T12:20:00+00:00",
                updated_at="2026-06-05T12:06:00+00:00",
                restore_attempt_count=1,
                last_restore_attempt_at="2026-06-05T12:06:00+00:00",
                last_restore_error="restore_unconfirmed",
                status="restore_failed",
            )
            save_shadow_learning_session_state(config_dir=config_dir, state=state)

            loaded = load_shadow_learning_session_state(config_dir)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.status, "restore_failed")
            self.assertTrue(loaded.restore_required)
            self.assertEqual(loaded.original_endpoint, "eu.smartess.io,18899,TCP")
            self.assertEqual(loaded.restore_attempt_count, 1)

    def test_shadow_session_expiry_uses_expires_at_deadline(self) -> None:
        state = build_shadow_learning_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            trace_path="/tmp/trace.jsonl",
            original_endpoint="eu.smartess.io,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            upstream_endpoint="eu.smartess.io,18899,TCP",
            restore_required=True,
            started_at="2026-06-05T12:00:00+00:00",
            expires_at="2026-06-05T12:10:00+00:00",
            updated_at="2026-06-05T12:00:00+00:00",
            status="ready",
        )

        self.assertFalse(
            shadow_learning_session_is_expired(
                state,
                now=datetime.fromisoformat("2026-06-05T12:09:59+00:00"),
            )
        )
        self.assertTrue(
            shadow_learning_session_is_expired(
                state,
                now=datetime.fromisoformat("2026-06-05T12:10:01+00:00"),
            )
        )

    def test_build_shadow_learning_lease_deadline_uses_utc_now(self) -> None:
        deadline = build_shadow_learning_lease_deadline(
            lease_seconds=120,
            now=datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(deadline, "2026-06-05T12:02:00+00:00")

    def test_confirmed_restore_cleanup_clears_restart_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            state = build_shadow_learning_session_state(
                entry_id="entry-1",
                collector_pn="E5000020000000",
                trace_path=str(config_dir / "trace.jsonl"),
                original_endpoint="eu.smartess.io,18899,TCP",
                proxy_endpoint="192.168.1.50,18899,TCP",
                upstream_endpoint="eu.smartess.io,18899,TCP",
                restore_required=True,
                started_at="2026-06-05T12:00:00+00:00",
                expires_at="2026-06-05T12:20:00+00:00",
                updated_at="2026-06-05T12:01:00+00:00",
                status="restoring",
            )
            save_shadow_learning_session_state(config_dir=config_dir, state=state)
            clear_shadow_learning_session_state(config_dir)
            self.assertIsNone(load_shadow_learning_session_state(config_dir))


if __name__ == "__main__":
    unittest.main()
