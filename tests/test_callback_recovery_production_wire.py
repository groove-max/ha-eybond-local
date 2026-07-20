"""The callback recovery transaction against REAL sockets -- production wiring.

tests/test_callback_recovery.py proves the engine's state machine with a
scripted channel and sender. This harness closes the production gap with NO
injected reader, sender, or ledger: the REAL shared framed transport, the
registry claim owned by the public wrapper, the negotiated SessionHandle, the
management adapter's FC=3/29 reboot, a collector that stays genuinely SILENT
after the reset (its NAT hole is gone) until a REAL ``set>server`` unicast
datagram from the production trigger facade reaches its UDP listener, the
authoritative FC=2 PN read on the new socket, the claim retarget, and a
CallbackRecoveryProof the strict contract accepts verbatim.

The second scenario reboots a collector that autonomously re-dials: the SAME
armed transaction must settle for ``inbound_recovered`` with ZERO callback
datagrams on the wire.
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

from custom_components.eybond_local.collector.transport import (
    _acquire_shared_listener,
    _release_shared_listener,
)
from custom_components.eybond_local.collector.transport_profile import (
    collector_session_protocol_from_inventory_state,
)
from custom_components.eybond_local.connection.callback_ledger import (
    get_callback_trigger_ledger,
)
from custom_components.eybond_local.connection.recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    RecoveryContract,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
    STATE_CALLBACK_VERIFIED,
    STATE_INBOUND_RECOVERED,
    async_run_callback_recovery_transaction,
)
from custom_components.eybond_local.onboarding.timeouts import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)
from fake_collector import FakeCollectorService
from fake_collector_lib import CollectorProfile, resolve_scenario

# Synthetic identities only. 18 characters: the framed heartbeat carries only
# the first 14, so the full spelling exists ONLY in the FC=2 read.
FULL_PN = "V001020SYN62344022"
SHORT_HEARTBEAT_PN = FULL_PN[:14]
TS = "2026-07-16T10:00:00+00:00"

_HARNESS_TIMEOUT = 25.0


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _registry_for_listener(listener, port: int) -> CallbackSessionRegistry:
    """The production wiring: the registry reads the REAL listener inventory."""

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


class CallbackRecoveryProductionWireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tcp_port = _free_tcp_port()
        self._listener = await _acquire_shared_listener("127.0.0.1", self._tcp_port)
        self._registry = _registry_for_listener(self._listener, self._tcp_port)
        self._service: FakeCollectorService | None = None

    async def asyncTearDown(self) -> None:
        if self._service is not None:
            await self._service.stop()
        await _release_shared_listener(
            self._listener, close_pending=True, close_payload=True, close_at=True
        )

    async def _start_service(self, *, set_29_mode: str) -> FakeCollectorService:
        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=0,  # a REAL UDP listener on an ephemeral port
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=30.0,
            connect_timeout=2.0,
            udp_reply="",
            scenario=resolve_scenario(
                preset="collector_only",
                profile=CollectorProfile(pn=FULL_PN),
                set_29_mode=set_29_mode,
                reboot_reconnect_delay=0.3,
            ),
        )
        await service.start()
        self._service = service
        return service

    def _service_udp_port(self) -> int:
        transport = self._service._udp_transport
        assert transport is not None
        return int(transport.get_extra_info("sockname")[1])

    async def _dial_in_and_wait(self, service: FakeCollectorService) -> str:
        """Plumbing dial-in (the passive-discovery topology), NOT a trigger.

        The transaction's precondition is an already-identified live session
        (onboarding certified it), so the FIRST socket may announce itself.
        Every socket the collector opens AFTER this point is FULLY SILENT:
        the engine's pre-reboot trusted-wire authority must drive the single
        session-pinned FC=2 probe in BOTH recovery phases.
        """

        import dataclasses as _dc

        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        await service.handle_discovery(redirect, ("127.0.0.1", 0))
        deadline = asyncio.get_running_loop().time() + 5.0
        while True:
            for session in self._registry.observed_sessions_per_socket():
                if not session.state.startswith("closed"):
                    service._scenario = _dc.replace(
                        service._scenario, first_heartbeat_delay=3600.0
                    )
                    service.pre_rx_heartbeats = 0
                    return session.session_id
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("collector never dialed in")
            await asyncio.sleep(0.05)

    def _route(self) -> CallbackRecoveryRoute:
        return CallbackRecoveryRoute(
            bind_ip="127.0.0.1",
            trigger_target_ip="127.0.0.1",
            trigger_udp_port=self._service_udp_port(),
            advertised_ha_host="127.0.0.1",
            advertised_ha_port=self._tcp_port,
            listener_port=self._tcp_port,
        )

    async def test_silent_reboot_then_real_unicast_yields_callback_proof(self) -> None:
        """reset ack -> socket drops -> full inbound window stays EMPTY ->
        production trigger facade sends a real unicast -> collector dials in ->
        FC=2 read -> retarget -> CallbackRecoveryProof accepted by the
        contract."""

        service = await self._start_service(set_29_mode="reboot_silent")
        old_session_id = await self._dial_in_and_wait(service)
        rx_before = service.discovery_rx_count
        generation_before = get_callback_trigger_ledger().snapshot_generation()

        outcome = await asyncio.wait_for(
            async_run_callback_recovery_transaction(
                registry=self._registry,
                collector_pn=SHORT_HEARTBEAT_PN,  # discovery knew the prefix
                session_id=old_session_id,
                route=self._route(),
                clock=lambda: TS,
                policy=replace(
                    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
                    inbound_strong_identity_timeout=5.0,
                    inbound_restart_disconnect_timeout=5.0,
                    inbound_reconnect_timeout=1.5,  # a real (brief) window
                    callback_recovery_session_wait=8.0,
                    callback_causality_lease_wait=2.0,
                ),
                listener_host="127.0.0.1",
                poll_interval=0.05,
            ),
            timeout=_HARNESS_TIMEOUT,
        )

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(outcome.status, STATE_CALLBACK_VERIFIED)
        self.assertIsNone(outcome.inbound_proof)
        self.assertNotEqual(outcome.new_session_id, old_session_id)

        # The trigger REALLY went over the wire: the fake collector's UDP
        # listener saw our datagrams, and the ledger advanced.
        self.assertGreater(service.discovery_rx_count, rx_before)
        self.assertGreater(
            get_callback_trigger_ledger().snapshot_generation(), generation_before
        )

        # The silent reconnect answered ONLY the engine's authorized FC=2
        # probe: the collector volunteered zero unsolicited bytes after reset.
        self.assertEqual(getattr(service, "pre_rx_heartbeats", 0), 0)
        proof = outcome.callback_proof
        self.assertEqual(proof.method, CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT)
        self.assertEqual(proof.collector_pn, FULL_PN)
        self.assertEqual(proof.identity_source, "fc2_parameter_2")
        self.assertEqual(proof.verified_at, TS)
        self.assertEqual(
            proof.trigger_target, f"127.0.0.1:{self._service_udp_port()}"
        )
        self.assertEqual(
            proof.advertised_ha_endpoint, f"127.0.0.1:{self._tcp_port}"
        )
        self.assertEqual(proof.listener_port, self._tcp_port)

        # SUCCESS is atomically handoff-ready: the outcome carries the EXACT
        # owner token, the claim sits COMMITTED on the NEW callback session,
        # and the registry itself certifies the identity for that token --
        # even when the caller only holds the short heartbeat prefix.
        self.assertTrue(
            outcome.handoff_owner.startswith("callback_recovery:"),
            outcome.handoff_owner,
        )
        self.assertEqual(
            self._registry.owner_for_pn(FULL_PN), outcome.handoff_owner
        )
        self.assertEqual(
            self._registry.claimed_session_id(outcome.handoff_owner),
            outcome.new_session_id,
        )
        self.assertEqual(
            self._registry.prepared_handoff_identity(
                outcome.handoff_owner, SHORT_HEARTBEAT_PN
            ),
            FULL_PN,
        )
        self.assertEqual(
            self._registry.prepared_handoff_identity(
                outcome.handoff_owner, FULL_PN
            ),
            FULL_PN,
        )

        # The strict contract model accepts the proof verbatim -- and survives
        # a serialize -> parse round trip.
        contract = RecoveryContract.empty_for_pn(
            proof.collector_pn, identity_source=proof.identity_source
        ).with_callback_proof(proof, updated_at=proof.verified_at)
        self.assertTrue(contract.callback_verified)
        data: dict[str, object] = {}
        contract.write_to(data)
        self.assertIsNotNone(RecoveryContract.from_entry_data(data))

    async def test_autonomous_reconnect_settles_for_inbound_with_zero_datagrams(
        self,
    ) -> None:
        """The collector re-dials on its own: the ARMED callback transaction
        must keep the inbound proof from the same reset and never touch UDP."""

        service = await self._start_service(set_29_mode="reboot")
        old_session_id = await self._dial_in_and_wait(service)
        rx_before = service.discovery_rx_count
        generation_before = get_callback_trigger_ledger().snapshot_generation()

        outcome = await asyncio.wait_for(
            async_run_callback_recovery_transaction(
                registry=self._registry,
                collector_pn=SHORT_HEARTBEAT_PN,
                session_id=old_session_id,
                route=self._route(),  # armed, but must stay unused
                clock=lambda: TS,
                policy=replace(
                    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
                    inbound_strong_identity_timeout=5.0,
                    inbound_restart_disconnect_timeout=5.0,
                    inbound_reconnect_timeout=8.0,
                    callback_recovery_session_wait=8.0,
                    callback_causality_lease_wait=2.0,
                ),
                listener_host="127.0.0.1",
                poll_interval=0.05,
            ),
            timeout=_HARNESS_TIMEOUT,
        )

        self.assertTrue(outcome.inbound_recovered, outcome.failure_reason)
        self.assertEqual(outcome.status, STATE_INBOUND_RECOVERED)
        self.assertIsNone(outcome.callback_proof)
        self.assertNotEqual(outcome.new_session_id, old_session_id)

        # ZERO callback datagrams: the collector's UDP listener stayed silent
        # and the process-wide ledger never advanced.
        self.assertEqual(service.discovery_rx_count, rx_before)
        self.assertEqual(
            get_callback_trigger_ledger().snapshot_generation(), generation_before
        )

        self.assertEqual(getattr(service, "pre_rx_heartbeats", 0), 0)
        proof = outcome.inbound_proof
        self.assertEqual(proof.method, INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER)
        self.assertEqual(proof.collector_pn, FULL_PN)
        self.assertEqual(proof.identity_source, "fc2_parameter_2")

        # inbound_recovered is a SUCCESS: handoff-ready exactly like
        # callback_verified -- the exact owner token, the claim committed on
        # the new socket, the identity certifiable by the registry.
        self.assertTrue(
            outcome.handoff_owner.startswith("callback_recovery:"),
            outcome.handoff_owner,
        )
        self.assertEqual(
            self._registry.owner_for_pn(FULL_PN), outcome.handoff_owner
        )
        self.assertEqual(
            self._registry.claimed_session_id(outcome.handoff_owner),
            outcome.new_session_id,
        )
        self.assertEqual(
            self._registry.prepared_handoff_identity(
                outcome.handoff_owner, SHORT_HEARTBEAT_PN
            ),
            FULL_PN,
        )


if __name__ == "__main__":
    unittest.main()
