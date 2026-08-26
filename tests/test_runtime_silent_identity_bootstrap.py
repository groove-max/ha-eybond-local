from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import socket
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
HELPERS_DIR = REPO_ROOT / "tests" / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.connection.spec_factory import (
    build_connection_spec,
)
from custom_components.eybond_local.collector.discovery import (
    async_send_callback_trigger,
)
from custom_components.eybond_local.collector.transport import (
    _acquire_shared_listener,
    _release_shared_listener,
)
from custom_components.eybond_local.const import (
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONF_DISCOVERY_TARGET,
    CONF_HEARTBEAT_INTERVAL,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
)
from custom_components.eybond_local.runtime.factory import create_runtime_manager
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager
from custom_components.eybond_local.drivers.catalog_identity import (
    async_probe_catalog_identity,
)
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import ModbusSession, crc16_modbus
from fake_collector import FakeCollectorService
from fake_collector_lib import (
    CollectorProfile,
    QUERY_MODE_FAIL,
    resolve_scenario,
)


FULL_PN = "E50000200000000001"
FOREIGN_PN = "E50000200000009777"
ISSUE37_SHORT_PN = FULL_PN[:14]
ISSUE37_FULL_PN = FULL_PN
ISSUE37_FOREIGN_FULL_PN = FOREIGN_PN


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _weak_framed_collector(
    *,
    udp_port: int,
    pn: str,
    answer_fc2: bool = True,
) -> FakeCollectorService:
    """Return a real framed collector that volunteers only a weak heartbeat PN."""

    scenario = resolve_scenario(
        preset="collector_only",
        profile=CollectorProfile(pn=pn),
        first_heartbeat_delay=0.05,
    )
    if not answer_fc2:
        scenario = replace(
            scenario,
            fc2_query_modes={**dict(scenario.fc2_query_modes), 2: QUERY_MODE_FAIL},
        )
    return FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=udp_port,
        tcp_bind_ip="127.0.0.1",
        heartbeat_interval=30.0,
        connect_timeout=2.0,
        udp_reply="rsp>server=1;",
        scenario=scenario,
    )


class _SilentAtCollector:
    """Fully silent callback socket that answers only the read-only DTUPN."""

    def __init__(self, pn: str, *, answer: bool = True) -> None:
        self._pn = pn
        self._answer = answer
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._task: asyncio.Task[None] | None = None
        self.dtupn_queries = 0
        self.query_seen = asyncio.Event()

    async def connect(self, port: int) -> None:
        self._reader, self._writer = await asyncio.open_connection(
            "127.0.0.1", port
        )
        self._task = asyncio.create_task(self._serve())

    async def _serve(self) -> None:
        assert self._reader is not None
        assert self._writer is not None
        try:
            while True:
                line = await self._reader.readuntil(b"\n")
                if line.decode("ascii", errors="replace").strip().upper().startswith(
                    "AT+DTUPN"
                ):
                    self.dtupn_queries += 1
                    self.query_seen.set()
                    if not self._answer:
                        continue
                    self._writer.write(
                        f"AT+DTUPN:{self._pn}\r\n".encode("ascii")
                    )
                    await self._writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            return

    async def stop(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


class _SilentAtModbusCollector(_SilentAtCollector):
    """AT identity/management plus raw Modbus RTU on the same TCP stream."""

    def __init__(self, pn: str, *, registers: dict[int, int]) -> None:
        super().__init__(pn)
        self._registers = dict(registers)
        self.modbus_queries = 0

    async def _serve(self) -> None:
        assert self._reader is not None
        assert self._writer is not None
        try:
            while True:
                first = await self._reader.readexactly(1)
                if first == b"A":
                    line = first + await self._reader.readuntil(b"\n")
                    normalized = line.decode("ascii", errors="replace").strip().upper()
                    if normalized.startswith("AT+DTUPN"):
                        self.dtupn_queries += 1
                        self.query_seen.set()
                        self._writer.write(
                            f"AT+DTUPN:{self._pn}\r\n".encode("ascii")
                        )
                    elif normalized == "AT+UART?":
                        self._writer.write(b"AT+UART:9600,8,1,NONE\r\n")
                    elif normalized.startswith("AT+UART="):
                        self._writer.write(b"AT+UART:W000\r\n")
                    await self._writer.drain()
                    continue

                request = first + await self._reader.readexactly(7)
                function = request[1]
                address = int.from_bytes(request[2:4], "big")
                count = int.from_bytes(request[4:6], "big")
                self.modbus_queries += 1
                response = bytearray((request[0], function, count * 2))
                for register in range(address, address + count):
                    response.extend(
                        int(self._registers.get(register, 0)).to_bytes(2, "big")
                    )
                response.extend(crc16_modbus(response).to_bytes(2, "little"))
                self._writer.write(response)
                await self._writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            return


class RuntimeSilentIdentityBootstrapTests(unittest.IsolatedAsyncioTestCase):
    def _manager(self, *, port: int, challenge: str = "at_text"):
        return EybondRuntimeLinkManager(
            server_ip="127.0.0.1",
            collector_ip="127.0.0.1",
            collector_pn=FULL_PN,
            collector_identity_challenge_protocol=challenge,
            tcp_port=port,
            udp_port=58899,
            discovery_target="127.0.0.1",
            discovery_interval=30,
            heartbeat_interval=60,
        )

    async def _exercise(self, *, response_pn: str, challenge: str = "at_text"):
        port = _free_port()
        manager = self._manager(port=port, challenge=challenge)
        collector = _SilentAtCollector(response_pn)
        await manager.async_start()
        await collector.connect(port)
        # Let the listener park the socket as fully silent before the runtime
        # attempt. This proves that no new TCP connection is required.
        await asyncio.sleep(0.35)
        fake_probe = types.SimpleNamespace(reply="", reply_from="")
        try:
            with patch(
                "custom_components.eybond_local.runtime.link.callback."
                "async_send_callback_trigger",
                new=AsyncMock(return_value=fake_probe),
            ):
                connected = await manager.async_try_connect(timeout=2.5)
            return (
                connected,
                collector.dtupn_queries,
                manager.session_handle,
                manager.listener_diagnostics(),
            )
        finally:
            # Assertions inspect the live manager before this cleanup.
            await collector.stop()
            await manager.async_stop()

    async def test_existing_silent_at_socket_is_identified_and_routed(self) -> None:
        connected, queries, handle, diagnostics = await self._exercise(
            response_pn=FULL_PN
        )

        self.assertTrue(connected)
        self.assertEqual(queries, 1)
        self.assertEqual(handle.collector_pn, FULL_PN)
        self.assertTrue(handle.uses_at_text_wire)
        self.assertEqual(diagnostics["collector_identity_challenge_protocol"], "at_text")
        self.assertEqual(diagnostics["collector_identity_challenge_active"], "")

    async def test_foreign_strong_pn_never_becomes_the_entry_wire(self) -> None:
        connected, queries, handle, diagnostics = await self._exercise(
            response_pn=FOREIGN_PN
        )

        self.assertFalse(connected)
        self.assertEqual(queries, 1)
        self.assertFalse(handle.observed)
        self.assertEqual(
            diagnostics["collector_identity_challenge_active"],
            "",
        )

    async def test_no_candidate_sends_no_identity_query(self) -> None:
        connected, queries, handle, _diagnostics = await self._exercise(
            response_pn=FULL_PN,
            challenge="",
        )

        self.assertFalse(connected)
        self.assertEqual(queries, 0)
        self.assertFalse(handle.observed)

    async def test_cancellation_releases_probe_lease_and_clears_active_state(self) -> None:
        port = _free_port()
        manager = self._manager(port=port)
        collector = _SilentAtCollector(FULL_PN, answer=False)
        await manager.async_start()
        await collector.connect(port)
        await asyncio.sleep(0.35)
        listener = manager._transport._listener
        assert listener is not None
        baseline_ref_count = listener._ref_count
        fake_probe = types.SimpleNamespace(reply="", reply_from="")
        try:
            with patch(
                "custom_components.eybond_local.runtime.link.callback."
                "async_send_callback_trigger",
                new=AsyncMock(return_value=fake_probe),
            ):
                task = asyncio.create_task(manager.async_try_connect(timeout=10.0))
                await asyncio.wait_for(collector.query_seen.wait(), timeout=2.0)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            self.assertEqual(
                manager.listener_diagnostics()["collector_identity_challenge_active"],
                "",
            )
            self.assertEqual(listener._ref_count, baseline_ref_count)
            self.assertFalse(manager.session_handle.observed)
        finally:
            await collector.stop()
            await manager.async_stop()


class Issue37WeakHeartbeatRuntimeRegressionTests(
    unittest.IsolatedAsyncioTestCase
):
    """Issue #37: a weak heartbeat is a candidate, never a runtime claim proof."""

    @staticmethod
    async def _wait_for_weak_session(
        registry: CallbackSessionRegistry,
    ) -> str:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2.0
        while loop.time() < deadline:
            for session in registry.observed_sessions_per_socket():
                if (
                    session.collector_pn == ISSUE37_SHORT_PN
                    and session.identity_source == "framed_heartbeat"
                ):
                    return session.session_id
            await asyncio.sleep(0.02)
        raise AssertionError("weak framed-heartbeat session did not appear")

    async def _exercise(
        self,
        *,
        durable_pn: str,
        collector_pn: str,
        answer_fc2: bool,
        session_on_primary: bool = False,
        callback_on_demand: bool = True,
    ) -> tuple[bool, tuple, str, int]:
        session_port = _free_port()
        primary_port = session_port if session_on_primary else _free_port()
        udp_port = _free_udp_port()
        manager = EybondRuntimeLinkManager(
            server_ip="127.0.0.1",
            collector_ip="127.0.0.1",
            collector_pn=durable_pn,
            collector_identity_challenge_protocol="eybond_framed",
            tcp_port=primary_port,
            advertised_tcp_port=session_port,
            udp_port=udp_port,
            discovery_target="127.0.0.1",
            discovery_interval=30,
            heartbeat_interval=60,
        )
        registry = CallbackSessionRegistry(
            sessions_source=lambda: tuple(
                {**row, "listener_port": session_port}
                for row in listener.discovered_collector_sessions()
            )
        )
        registry.claim("issue-37-entry", collector_pn=durable_pn)
        manager.set_callback_ownership(registry, "issue-37-entry")
        manager.set_reverse_discovery_enabled(callback_on_demand)
        collector = _weak_framed_collector(
            udp_port=udp_port,
            pn=collector_pn,
            answer_fc2=answer_fc2,
        )
        listener = await _acquire_shared_listener("0.0.0.0", session_port)
        await collector.start()
        try:
            # Reproduce the support archive faithfully: the callback session is
            # already open and the listener has learned only the 14-character
            # heartbeat prefix before the entry runtime starts and registers
            # its route. It therefore remains parked instead of being routed
            # opportunistically by an already-running transport owner.
            await async_send_callback_trigger(
                bind_ip="127.0.0.1",
                advertised_server_ip="127.0.0.1",
                advertised_server_port=session_port,
                target_ip="127.0.0.1",
                udp_port=udp_port,
                timeout=0.2,
                source="issue37_preexisting_session",
            )
            await manager.async_start()
            weak_session_id = await self._wait_for_weak_session(registry)
            trigger = AsyncMock(
                return_value=types.SimpleNamespace(reply="", reply_from="")
            )
            with patch(
                "custom_components.eybond_local.runtime.link.callback."
                "async_send_callback_trigger",
                new=trigger,
            ):
                connected = await manager.async_try_connect(timeout=2.5)
            return (
                connected,
                registry.observed_sessions_per_socket(),
                weak_session_id,
                trigger.await_count,
            )
        finally:
            await collector.stop()
            await manager.async_stop()
            await _release_shared_listener(
                listener,
                close_pending=True,
                close_payload=True,
                close_at=True,
            )

    async def test_weak_heartbeat_is_upgraded_on_the_exact_session(self) -> None:
        connected, sessions, weak_session_id, trigger_count = await self._exercise(
            durable_pn=ISSUE37_SHORT_PN,
            collector_pn=ISSUE37_FULL_PN,
            answer_fc2=True,
        )

        self.assertTrue(connected)
        strong = next(
            session for session in sessions if session.session_id == weak_session_id
        )
        self.assertTrue(strong.has_strong_identity)
        self.assertEqual(strong.identity_source, "fc2_parameter_2")
        self.assertEqual(strong.collector_pn, ISSUE37_FULL_PN)
        self.assertEqual(trigger_count, 1)

    async def test_weak_heartbeat_on_primary_listener_is_not_routed_early(
        self,
    ) -> None:
        connected, sessions, weak_session_id, trigger_count = await self._exercise(
            durable_pn=ISSUE37_SHORT_PN,
            collector_pn=ISSUE37_FULL_PN,
            answer_fc2=True,
            session_on_primary=True,
        )

        self.assertTrue(connected)
        strong = next(
            session for session in sessions if session.session_id == weak_session_id
        )
        self.assertTrue(strong.has_strong_identity)
        self.assertEqual(strong.identity_source, "fc2_parameter_2")
        self.assertEqual(strong.collector_pn, ISSUE37_FULL_PN)
        self.assertEqual(trigger_count, 1)

    async def test_weak_heartbeat_with_failed_fc2_is_not_claimed(self) -> None:
        connected, sessions, weak_session_id, trigger_count = await self._exercise(
            durable_pn=ISSUE37_SHORT_PN,
            collector_pn=ISSUE37_FULL_PN,
            answer_fc2=False,
        )

        self.assertFalse(connected)
        weak = next(
            session for session in sessions if session.session_id == weak_session_id
        )
        self.assertFalse(weak.has_strong_identity)
        self.assertEqual(weak.identity_source, "framed_heartbeat")
        self.assertEqual(weak.collector_pn, ISSUE37_SHORT_PN)
        self.assertEqual(trigger_count, 1)

    async def test_matching_weak_prefix_cannot_hide_foreign_full_pn(self) -> None:
        connected, sessions, weak_session_id, trigger_count = await self._exercise(
            durable_pn=ISSUE37_FULL_PN,
            collector_pn=ISSUE37_FOREIGN_FULL_PN,
            answer_fc2=True,
        )

        self.assertFalse(connected)
        foreign = next(
            session for session in sessions if session.session_id == weak_session_id
        )
        self.assertTrue(foreign.has_strong_identity)
        self.assertEqual(foreign.identity_source, "fc2_parameter_2")
        self.assertEqual(foreign.collector_pn, ISSUE37_FOREIGN_FULL_PN)
        self.assertEqual(trigger_count, 1)

    async def test_inbound_weak_heartbeat_is_upgraded_without_udp(self) -> None:
        connected, sessions, weak_session_id, trigger_count = await self._exercise(
            durable_pn=ISSUE37_SHORT_PN,
            collector_pn=ISSUE37_FULL_PN,
            answer_fc2=True,
            callback_on_demand=False,
        )

        self.assertTrue(connected)
        strong = next(
            session for session in sessions if session.session_id == weak_session_id
        )
        self.assertTrue(strong.has_strong_identity)
        self.assertEqual(strong.identity_source, "fc2_parameter_2")
        self.assertEqual(strong.collector_pn, ISSUE37_FULL_PN)
        self.assertEqual(trigger_count, 0)

    async def test_inbound_failed_fc2_stays_parked_without_udp(self) -> None:
        connected, sessions, weak_session_id, trigger_count = await self._exercise(
            durable_pn=ISSUE37_SHORT_PN,
            collector_pn=ISSUE37_FULL_PN,
            answer_fc2=False,
            callback_on_demand=False,
        )

        self.assertFalse(connected)
        weak = next(
            session for session in sessions if session.session_id == weak_session_id
        )
        self.assertFalse(weak.has_strong_identity)
        self.assertEqual(weak.identity_source, "framed_heartbeat")
        self.assertEqual(weak.collector_pn, ISSUE37_SHORT_PN)
        self.assertEqual(trigger_count, 0)


class Issue13RuntimeRegressionTests(unittest.IsolatedAsyncioTestCase):
    """Load-bearing entry-to-runtime regression for the issue #13 failure.

    The entry deliberately contains cloud-family metadata and a durable PN, but
    no persisted/confirmed session protocol.  The test must therefore derive a
    read-only identity challenge through every production composition layer;
    it must not inject a wire or challenge directly into the runtime link.
    """

    async def test_legacy_entry_recovers_existing_silent_same_pn_session(self) -> None:
        port = _free_port()
        spec = build_connection_spec(
            {
                CONF_SERVER_IP: "127.0.0.1",
                CONF_TCP_PORT: port,
                CONF_UDP_PORT: 58899,
                CONF_COLLECTOR_IP: "127.0.0.1",
                CONF_COLLECTOR_PN: FULL_PN,
                CONF_COLLECTOR_CLOUD_FAMILY: "smartess_at",
                CONF_DISCOVERY_TARGET: "127.0.0.1",
                CONF_HEARTBEAT_INTERVAL: 60,
            },
            {},
        )
        self.assertIsNotNone(spec)
        assert spec is not None
        # Metadata may select one read-only query format, but it must not forge
        # either a confirmed wire or persisted live-session evidence.
        self.assertEqual(spec.collector_identity_challenge_protocol, "at_text")
        self.assertEqual(spec.collector_configured_session_protocol, "")
        self.assertIsNone(spec.confirmed_session_protocol_evidence)

        runtime = create_runtime_manager(spec, driver_hint="auto")
        collector = _SilentAtModbusCollector(
            FULL_PN,
            registers={171: 32768, 184: 4},
        )
        await runtime.async_start()
        await collector.connect(port)
        # Reproduce the support archive: the TCP session already exists and is
        # completely silent before runtime attempts a callback connection.
        await asyncio.sleep(0.35)
        fake_probe = types.SimpleNamespace(reply="", reply_from="")
        try:
            with patch(
                "custom_components.eybond_local.runtime.link.callback."
                "async_send_callback_trigger",
                new=AsyncMock(return_value=fake_probe),
            ):
                connected = await runtime._link_manager.async_try_connect(
                    timeout=2.5
                )

            self.assertTrue(connected)
            self.assertEqual(collector.dtupn_queries, 1)
            protocol, pn = runtime.confirmed_session_protocol_evidence()
            self.assertEqual((protocol, pn), ("at_text", FULL_PN))
            handle = runtime._link_manager.session_handle
            self.assertTrue(handle.observed)
            self.assertEqual(handle.collector_pn, FULL_PN)
            self.assertTrue(handle.uses_at_text_wire)
            diagnostics = runtime.listener_diagnostics()
            self.assertEqual(
                diagnostics["collector_identity_challenge_protocol"],
                "at_text",
            )
            self.assertEqual(
                diagnostics["collector_identity_challenge_active"],
                "",
            )

            target = ProbeTarget(
                devcode=1,
                collector_addr=255,
                device_addr=1,
            )
            identity = await async_probe_catalog_identity(
                ModbusSession(
                    runtime._link_manager.transport,
                    route=target.link_route,
                    slave_id=target.payload_address,
                )
            )
            self.assertIsNotNone(identity)
            assert identity is not None
            self.assertEqual(identity.match.entry.entry_key, "anenji_anj_11kw")
            self.assertEqual(identity.layout_code, 4)
            self.assertEqual(identity.model_code, 32768)
            self.assertEqual(collector.modbus_queries, 2)
        finally:
            await collector.stop()
            await runtime.async_stop()


if __name__ == "__main__":
    unittest.main()
