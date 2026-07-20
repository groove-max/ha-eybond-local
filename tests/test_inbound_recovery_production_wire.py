"""The inbound recovery transaction against REAL sockets -- production wiring.

tests/test_strategy_verification.py proves the verifier's state machine with a
scripted channel. This harness closes the production gap: the REAL shared
framed transport, the exact registry session-id claim, the negotiated
SessionHandle, the management adapter's ``async_reboot()`` (FC=3 parameter 29
inside the framed adapter), the authoritative FC=2 PN read through the shared
session-pinned reader, the genuine disconnect -> autonomous re-dial lifecycle
of the fake collector, and the typed proof at the end. No verifier method is
substituted; only the collector itself is a scripted peer process.
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
    CallbackTriggerLedger,
)
from custom_components.eybond_local.connection.recovery_contract import (
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    RecoveryContract,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.connection.recovery.verification import (
    InboundRecoveryVerifier,
    ObservedSessionRestartChannel,
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

_HARNESS_TIMEOUT = 15.0

_FAST_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    inbound_strong_identity_timeout=5.0,
    inbound_restart_disconnect_timeout=5.0,
    inbound_reconnect_timeout=8.0,
    callback_causality_lease_wait=2.0,
)


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


def _verifier_sessions_source(registry):
    """Mirror the config flow's projection (identity source + raw included)."""

    def _sessions():
        return tuple(
            {
                "session_id": session.session_id,
                "collector_pn": session.collector_pn,
                "state": session.state,
                "has_strong_identity": session.has_strong_identity,
                "collector_identity_source": session.identity_source,
                "raw": dict(session.raw),
            }
            for session in registry.observed_sessions_per_socket()
        )

    return _sessions


class InboundRecoveryProductionWireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tcp_port = _free_tcp_port()
        self._listener = await _acquire_shared_listener("127.0.0.1", self._tcp_port)
        self._registry = _registry_for_listener(self._listener, self._tcp_port)

    async def asyncTearDown(self) -> None:
        await _release_shared_listener(
            self._listener, close_pending=True, close_payload=True, close_at=True
        )

    async def _wait_for_live_session(self, *, timeout: float = 5.0) -> str:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            for session in self._registry.observed_sessions_per_socket():
                if not session.state.startswith("closed"):
                    return session.session_id
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("collector never dialed in")
            await asyncio.sleep(0.05)

    async def test_full_reboot_reconnect_cycle_yields_proof(self) -> None:
        """baseline -> adapter reboot -> old closed -> new same-PN socket ->
        claim retarget -> typed InboundRecoveryProof -> RecoveryContract."""

        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=0,  # UDP listener unused: dial-ins are driven directly
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=30.0,
            connect_timeout=2.0,
            udp_reply="",
            scenario=resolve_scenario(
                preset="collector_only",
                profile=CollectorProfile(pn=FULL_PN),
                set_29_mode="reboot",
                reboot_reconnect_delay=0.3,
            ),
        )
        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        # The collector is already connected when onboarding starts (the
        # passive-discovery topology). This dial-in is the FAKE's plumbing, not
        # a verification-window trigger.
        await service.handle_discovery(redirect, ("127.0.0.1", 0))
        old_session_id = await self._wait_for_live_session()

        # The flow's claim, exactly as production creates it.
        owner = "strategy_verification:harness"
        self._registry.claim_session(owner, session_id=old_session_id)

        def _claimed_session_id() -> str:
            return self._registry.claimed_session_id(owner)

        def _promote(full_pn: str) -> None:
            self._registry.promote_claim_to_full_pn(owner, full_pn)

        def _retarget(new_sid: str) -> bool:
            if self._registry.claimed_session_id(owner) == new_sid:
                return True
            try:
                return bool(
                    self._registry.retarget_claim_to_reconnected_session(
                        owner, new_sid
                    )
                )
            except ValueError:
                return False

        channel = ObservedSessionRestartChannel(
            host="127.0.0.1",
            port=self._tcp_port,
            collector_pn="",
            session_id=old_session_id,
            session_id_provider=_claimed_session_id,
            handle_provider=lambda: self._registry.session_handle_for_claimed_session(owner),
        )

        async def _probe_reconnected(_new_sid: str) -> str:
            return await channel.async_probe_identity()

        verifier = InboundRecoveryVerifier(
            collector_pn=SHORT_HEARTBEAT_PN,  # discovery knew only the prefix
            session_id=old_session_id,
            restart_channel=channel,
            sessions_source=_verifier_sessions_source(self._registry),
            clock=lambda: TS,
            policy=_FAST_POLICY,
            ledger=CallbackTriggerLedger(),
            promote_claim=_promote,
            retarget_claim=_retarget,
            probe_reconnected_identity=_probe_reconnected,
            poll_interval=0.05,
        )

        try:
            outcome = await asyncio.wait_for(
                verifier.async_verify(), timeout=_HARNESS_TIMEOUT
            )
        finally:
            await service.stop()

        self.assertTrue(outcome.inbound_verified, outcome.failure_reason)
        # The heartbeat only ever advertised the 14-char prefix: the durable
        # identity was enriched by the REAL FC=2 read on the wire.
        self.assertEqual(outcome.collector_pn, FULL_PN)
        self.assertNotEqual(outcome.new_session_id, old_session_id)

        proof = outcome.proof
        self.assertEqual(proof.method, INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER)
        self.assertEqual(proof.collector_pn, FULL_PN)
        self.assertEqual(proof.identity_source, "fc2_parameter_2")
        self.assertEqual(proof.verified_at, TS)
        self.assertEqual(proof.session_protocol, "eybond_framed")

        # The claim ended up on the NEW socket, ready for the entry handoff.
        self.assertEqual(self._registry.owner_for_pn(FULL_PN), owner)
        self.assertEqual(
            self._registry.claimed_session_id(owner), outcome.new_session_id
        )
        # And the strict contract model accepts the proof verbatim.
        contract = RecoveryContract.empty_for_pn(
            proof.collector_pn, identity_source=proof.identity_source
        ).with_inbound_proof(proof, updated_at=proof.verified_at)
        self.assertTrue(contract.inbound_verified)
        data: dict[str, object] = {}
        contract.write_to(data)
        self.assertIsNotNone(RecoveryContract.from_entry_data(data))

    async def test_refused_reboot_yields_no_proof_and_old_socket_survives(self) -> None:
        """The legacy refusal shape: FC=3/29 unacknowledged -> typed failure."""

        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=0,
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=30.0,
            connect_timeout=2.0,
            udp_reply="",
            scenario=resolve_scenario(
                preset="collector_only",
                profile=CollectorProfile(pn=FULL_PN),
                set_29_mode="fail",
            ),
        )
        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        await service.handle_discovery(redirect, ("127.0.0.1", 0))
        old_session_id = await self._wait_for_live_session()
        owner = "strategy_verification:harness2"
        self._registry.claim_session(owner, session_id=old_session_id)

        channel = ObservedSessionRestartChannel(
            host="127.0.0.1",
            port=self._tcp_port,
            collector_pn="",
            session_id=old_session_id,
            session_id_provider=lambda: self._registry.claimed_session_id(owner),
            handle_provider=lambda: self._registry.session_handle_for_claimed_session(owner),
        )
        def _retarget(new_sid: str) -> bool:
            try:
                if self._registry.claimed_session_id(owner) == new_sid:
                    return True
                return bool(
                    self._registry.retarget_claim_to_reconnected_session(
                        owner, new_sid
                    )
                )
            except ValueError:
                return False

        verifier = InboundRecoveryVerifier(
            collector_pn=SHORT_HEARTBEAT_PN,
            session_id=old_session_id,
            restart_channel=channel,
            sessions_source=_verifier_sessions_source(self._registry),
            clock=lambda: TS,
            policy=replace(_FAST_POLICY, inbound_restart_disconnect_timeout=1.0),
            ledger=CallbackTriggerLedger(),
            promote_claim=lambda pn: self._registry.promote_claim_to_full_pn(owner, pn),
            retarget_claim=_retarget,
            poll_interval=0.05,
        )

        try:
            outcome = await asyncio.wait_for(
                verifier.async_verify(), timeout=_HARNESS_TIMEOUT
            )
        finally:
            await service.stop()

        self.assertFalse(outcome.inbound_verified)
        self.assertIsNone(outcome.proof)
        # An unacknowledged FC=3/29 is a confirmation failure, honestly typed.
        self.assertEqual(outcome.failure_reason, "restart_not_confirmed")


if __name__ == "__main__":
    unittest.main()
