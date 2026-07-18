"""Cold-repair Phase-A bootstrap against REAL sockets (Batch 8B.1, BLOCKER 1).

Closes the gap the fake-channel unit tests cannot: a FULLY-SILENT callback
socket (zero volunteered bytes -- invisible to the registry) must reach the
bootstrap transaction through the merged listener projection, be read exactly
once over the confirmed-evidence wire, gain a strong identity in the REAL
listener inventory, be certified by the shared matcher, and pin the permanent
claim. Framed (FC=2 parameter 2) and at_text (AT+DTUPN) are both proven.

Each test FAILS on the pre-corrective implementation, where
``CallbackBootstrapChannel.sessions()`` read only PN-bearing registry
observations: the silent socket was never in the projection, so the transaction
returned ``no_session`` and never performed the read.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import socket
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
HELPERS_DIR = REPO_ROOT / "tests" / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

from custom_components.eybond_local.collector.callback_bootstrap import (  # noqa: E402
    CallbackBootstrapChannel,
)
from custom_components.eybond_local.collector.transport import (  # noqa: E402
    _acquire_shared_listener,
    _release_shared_listener,
)
from custom_components.eybond_local.collector.transport_profile import (  # noqa: E402
    collector_session_protocol_from_inventory_state,
)
from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    CallbackSessionRegistry,
    PermanentOwnedSessionCertification,
)
from custom_components.eybond_local.connection.strategy_transition_repair import (  # noqa: E402
    BOOTSTRAP_CERTIFIED,
    async_run_callback_bootstrap_transaction,
)
from custom_components.eybond_local.connection.strategy_transition_recovery import (  # noqa: E402
    StrategyTransitionRecoveryState,
)
from custom_components.eybond_local.const import (  # noqa: E402
    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
)
from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)
from fake_collector import FakeCollectorService  # noqa: E402
from fake_collector_lib import CollectorProfile, resolve_scenario  # noqa: E402

FULL_PN = "V001020SYN62344022"
AT_FULL_PN = "E50000200000000001"
TS = "2026-07-17T10:00:00+00:00"
_HARNESS_TIMEOUT = 8.0

# Real sockets need a real dial-in + round trip; keep the waits generous but
# bounded so a hang fails rather than stalls.
_WIRE_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    callback_recovery_session_wait=4.0,
    callback_causality_lease_wait=4.0,
    discovery_timeout=1.0,
)


def _free_port(kind: int) -> int:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _registry_for_listener(listener, port: int) -> CallbackSessionRegistry:
    def _sessions():
        out = []
        for session in listener.discovered_collector_sessions():
            if not isinstance(session, dict):
                continue
            enriched = dict(session)
            enriched.setdefault("listener_port", int(port))
            enriched.setdefault(
                "session_protocol",
                collector_session_protocol_from_inventory_state(
                    state=session.get("state"),
                    protocol_shape=session.get("protocol_shape"),
                ),
            )
            out.append(enriched)
        return tuple(out)

    return CallbackSessionRegistry(sessions_source=_sessions)


def _evidence(protocol: str, pn: str) -> dict:
    return {
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: protocol,
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: pn,
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: (
            COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE
        ),
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT: TS,
    }


class _SilentAtTextCollector:
    """A fully SILENT at_text collector: dials in, says nothing, answers DTUPN."""

    def __init__(self, pn: str) -> None:
        self._pn = pn
        self._reader = None
        self._writer = None
        self._task = None
        self.dtupn_queries = 0

    async def connect(self, host: str, port: int) -> None:
        self._reader, self._writer = await asyncio.open_connection(host, port)
        self._task = asyncio.create_task(self._serve(), name="silent_at_collector")

    async def _serve(self) -> None:
        try:
            while True:
                line = await self._reader.readuntil(b"\n")
                text = line.decode("ascii", errors="replace").strip().upper()
                if text.startswith("AT+DTUPN"):
                    self.dtupn_queries += 1
                    self._writer.write(f"AT+DTUPN:{self._pn}\r\n".encode("ascii"))
                    await self._writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            return
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass


class BootstrapProductionWireHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tcp_port = _free_port(socket.SOCK_STREAM)
        self._listener = await _acquire_shared_listener("127.0.0.1", self._tcp_port)
        self._registry = _registry_for_listener(self._listener, self._tcp_port)

    async def asyncTearDown(self) -> None:
        await _release_shared_listener(
            self._listener, close_pending=True, close_payload=True, close_at=True
        )

    def _state(self, *, udp_port: int, pn: str) -> StrategyTransitionRecoveryState:
        return StrategyTransitionRecoveryState.create(
            collector_pn=pn,
            now=TS,
            trigger_target_host="127.0.0.1",
            trigger_udp_port=udp_port,
            advertised_host="127.0.0.1",
            advertised_port=self._tcp_port,
            trigger_bind_host="127.0.0.1",
            listener_bind_host="127.0.0.1",
            local_listener_port=self._tcp_port,
        )

    def _channel(self, *, protocol: str, pn: str) -> CallbackBootstrapChannel:
        return CallbackBootstrapChannel(
            registry=self._registry,
            host="127.0.0.1",
            port=self._tcp_port,
            entry_data=_evidence(protocol, pn),
            entry_options={},
            entry_pn=pn,
            trigger_timeout=1.0,
        )

    def _session_view(self, session_id: str):
        for session in self._registry.observed_sessions_per_socket():
            if session.session_id == session_id:
                return session
        raise AssertionError(f"session {session_id} not observed")

    async def _run(self, *, state, channel, owner_id="entry-repair"):
        return await asyncio.wait_for(
            async_run_callback_bootstrap_transaction(
                registry=self._registry,
                owner_id=owner_id,
                state=state,
                route=state.callback_route(),
                channel=channel,
                policy=_WIRE_POLICY,
                poll_interval=0.05,
            ),
            timeout=_HARNESS_TIMEOUT,
        )


class FramedBootstrapWireTests(BootstrapProductionWireHarness):
    async def test_silent_framed_socket_bootstraps_via_fc2(self) -> None:
        udp_port = _free_port(socket.SOCK_DGRAM)
        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=udp_port,
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=30.0,
            connect_timeout=2.0,
            udp_reply="",
            scenario=resolve_scenario(
                preset="collector_only",
                profile=CollectorProfile(pn=FULL_PN),
                first_heartbeat_delay=3600.0,  # NEVER volunteers a byte
            ),
        )
        await service.start()
        try:
            channel = self._channel(protocol="eybond_framed", pn=FULL_PN)
            outcome = await self._run(
                state=self._state(udp_port=udp_port, pn=FULL_PN), channel=channel
            )
        finally:
            await service.stop()

        self.assertEqual(outcome.kind, BOOTSTRAP_CERTIFIED, outcome.kind)
        self.assertIsInstance(outcome.certification, PermanentOwnedSessionCertification)
        self.assertEqual(outcome.certification.collector_pn, FULL_PN)
        # The read stamped the REAL inventory: only bytes on the socket can do it.
        session = self._session_view(outcome.session_id)
        self.assertTrue(session.has_strong_identity)
        self.assertEqual(session.identity_source, "fc2_parameter_2")
        self.assertEqual(session.collector_pn, FULL_PN)
        self.assertEqual(
            self._registry.claimed_session_id("entry-repair"), outcome.session_id
        )
        self.assertTrue(
            self._registry.reverify_permanent_owned_session(outcome.certification)
        )


class AtTextBootstrapWireTests(BootstrapProductionWireHarness):
    async def test_silent_at_socket_bootstraps_via_dtupn(self) -> None:
        udp_port = _free_port(socket.SOCK_DGRAM)  # nothing listens; fire-and-forget
        collector = _SilentAtTextCollector(AT_FULL_PN)
        channel = self._channel(protocol="at_text", pn=AT_FULL_PN)

        async def _dials_in() -> None:
            await asyncio.sleep(0.4)  # after the transaction's baseline
            await collector.connect("127.0.0.1", self._tcp_port)

        dial = asyncio.get_running_loop().create_task(_dials_in())
        try:
            outcome = await self._run(
                state=self._state(udp_port=udp_port, pn=AT_FULL_PN), channel=channel
            )
        finally:
            await dial
            await collector.stop()

        self.assertEqual(outcome.kind, BOOTSTRAP_CERTIFIED, outcome.kind)
        self.assertEqual(outcome.certification.collector_pn, AT_FULL_PN)
        # The read demonstrably hit THIS socket: only real bytes increment it.
        self.assertGreaterEqual(collector.dtupn_queries, 1)
        session = self._session_view(outcome.session_id)
        self.assertTrue(session.has_strong_identity)
        self.assertEqual(session.identity_source, "at_dtupn")


if __name__ == "__main__":
    unittest.main()
