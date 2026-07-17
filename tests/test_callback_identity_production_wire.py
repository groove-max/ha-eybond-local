"""The callback identity transaction against REAL sockets -- no injected reader.

tests/test_callback_identity.py proves the transaction's logic with the two
wire edges (UDP trigger, on-session PN read) stubbed. These tests close the
remaining gap: the PRODUCTION reader and sender against a real shared listener
and a real accepted collector socket, end to end:

* a real ``_SharedEybondListener`` acquired exactly the way passive discovery
  acquires it (ownerless -- inbound sockets are parked, the manual-flow
  topology);
* a real accepted socket from a scripted collector (framed and at_text);
* the transient claim taken on the exact ``session_id``;
* the wire negotiated from the live observation;
* the authoritative PN read over that wire -- framed FC=2 parameter 2 /
  ``AT+DTUPN`` -- performed by the PRODUCTION ``_SessionPinnedIdentityReader``
  (``reader=None``), pinned to the claimed session id;
* the full PN returned by the transaction and the socket transferred through
  the prepared handoff to a permanent entry id.

Each test is arranged so it FAILS if the production transaction quietly:

* reads an IP-routed / "current" session instead of the claimed ``session_id``
  (a second live same-IP collector would then answer);
* trusts an expected/persisted protocol instead of the live negotiated wire
  (the read would use the wrong framing and never produce a PN);
* substitutes an injected fake reader (the listener inventory would never gain
  the strong ``fc2_parameter_2`` / ``at_dtupn`` identity the matcher requires);
* reaches for the provider-specific ``SmartEssLocalSession`` (patched to
  explode) instead of the neutral ``CollectorWireManagementSession``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

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
from custom_components.eybond_local.connection.session_handle import (
    WIRE_AT_TEXT,
    WIRE_FRAMED,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.const import (
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
)
from custom_components.eybond_local.onboarding.callback_identity import (
    CallbackIdentityRequest,
    async_run_callback_identity_transaction,
)
from fake_collector import FakeCollectorService
from fake_collector_lib import CollectorProfile, resolve_scenario

# Synthetic identities only. 18 characters, so the framed heartbeat (which
# carries only the first 14) is a strict SHORT prefix and the full spelling can
# come ONLY from the authoritative FC=2 read.
FULL_PN = "V001020SYN62344022"
SHORT_HEARTBEAT_PN = FULL_PN[:14]
OTHER_FULL_PN = "V000405SYN94677058"
AT_FULL_PN = "E50000200000000001"  # allowlisted synthetic (see test_no_real_identifiers)

# Real-socket harness bound: generous next to the unit-test 1-2s watchdog rule
# because two TCP handshakes + one UDP round trip are involved, but still small
# enough that a hang fails the test instead of stalling the run.
_HARNESS_TIMEOUT = 6.0


def _free_port(kind: int) -> int:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class _FakeHass:
    def __init__(self, registry) -> None:
        self.data = {"eybond_local": {"callback_session_registry": registry}}


def _registry_for_listener(listener, port: int) -> CallbackSessionRegistry:
    """The production wiring: the registry reads the REAL listener inventory.

    Mirrors ``PassiveCallbackDiscovery.iter_observed_sessions`` exactly (the
    raw ``discovered_collector_sessions`` shape enriched with the listener port
    and the derived session protocol).
    """

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


class _ScriptedAtTextCollector:
    """Minimal at_text collector: dials in, announces DTUPN, answers DTUPN?.

    Real at_text collectors identify themselves in their first bytes (the
    listener's initial-chunk parser has an ``AT+DTUPN`` rule for exactly that),
    and the listener only publishes sessions that have shown SOME identity --
    a completely silent at_text socket is not observable to the registry yet.
    The banner makes the session observable; the transaction's own
    authoritative read is proven separately by ``dtupn_queries`` (only real
    bytes on this exact socket can increment it).
    """

    def __init__(self, pn: str) -> None:
        self._pn = pn
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._task: asyncio.Task | None = None
        self.dtupn_queries = 0

    async def connect(self, host: str, port: int) -> None:
        self._reader, self._writer = await asyncio.open_connection(host, port)
        self._writer.write(f"AT+DTUPN:{self._pn}\r\n".encode("ascii"))
        await self._writer.drain()
        self._task = asyncio.create_task(self._serve(), name="scripted_at_collector")

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
        task = self._task
        self._task = None
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


def _framed_service(*, udp_port: int, pn: str, udp_reply: str = "ACK") -> FakeCollectorService:
    return FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=udp_port,
        tcp_bind_ip="127.0.0.1",
        heartbeat_interval=30.0,
        connect_timeout=2.0,
        udp_reply=udp_reply,
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=pn),
        ),
    )


class ProductionWireHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tcp_port = _free_port(socket.SOCK_STREAM)
        self._listener = await _acquire_shared_listener("127.0.0.1", self._tcp_port)
        self._registry = _registry_for_listener(self._listener, self._tcp_port)
        self._hass = _FakeHass(self._registry)

    async def asyncTearDown(self) -> None:
        await _release_shared_listener(self._listener, close_pending=True, close_payload=True, close_at=True)

    async def _wait_for_session_count(self, count: int, *, timeout: float = 3.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if len(self._registry.observed_sessions_per_socket()) >= count:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f"never observed {count} session(s)")
            await asyncio.sleep(0.02)

    def _assert_certified_handoff_transfers_socket(self, outcome) -> None:
        """The prepared handoff certifies the PN and carries the exact socket."""

        owner = outcome.handoff_owner
        self.assertEqual(self._registry.owner_for_pn(outcome.collector_pn), owner)
        self.assertEqual(self._registry.claimed_session_id(owner), outcome.session_id)
        self.assertEqual(
            self._registry.prepared_handoff_identity(owner, outcome.collector_pn),
            outcome.collector_pn,
        )
        self.assertTrue(self._registry.complete_handoff(outcome.collector_pn, "entry-e2e"))
        self.assertEqual(self._registry.claimed_session_id("entry-e2e"), outcome.session_id)
        self.assertEqual(self._registry.claimed_identity(owner), "")
        handle = self._registry.session_handle_for_entry("entry-e2e")
        self.assertIsNotNone(handle)
        self.assertEqual(handle.session_id, outcome.session_id)

    def _session_view(self, session_id: str):
        for session in self._registry.observed_sessions_per_socket():
            if session.session_id == session_id:
                return session
        raise AssertionError(f"session {session_id} not observed")


class FramedProductionWireTests(ProductionWireHarness):
    async def test_framed_identity_certified_end_to_end(self) -> None:
        """Real UDP trigger -> real dial-in -> production FC=2 read -> handoff.

        The heartbeat advertises only the 14-character SHORT PN; the certified
        outcome must carry the FULL PN, which exists nowhere but in the FC=2
        parameter-2 reply on the wire. SmartEssLocalSession is patched to
        explode, so only the neutral CollectorWireManagementSession can have
        performed that read.
        """

        udp_port = _free_port(socket.SOCK_DGRAM)
        service = _framed_service(udp_port=udp_port, pn=FULL_PN)
        await service.start()
        try:
            with patch(
                "custom_components.eybond_local.collector.smartess_local."
                "SmartEssLocalSession.__init__",
                side_effect=AssertionError(
                    "identity read must use the neutral wire session"
                ),
            ):
                outcome = await asyncio.wait_for(
                    async_run_callback_identity_transaction(
                        self._hass,
                        CallbackIdentityRequest(
                            server_ip="127.0.0.1",
                            tcp_port=self._tcp_port,
                            udp_port=udp_port,
                            target_ip="127.0.0.1",
                            strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                            session_wait_timeout=4.0,
                        ),
                    ),
                    timeout=_HARNESS_TIMEOUT,
                )
        finally:
            await service.stop()

        self.assertTrue(outcome.identity_certified, outcome.result)
        self.assertEqual(outcome.collector_pn, FULL_PN)
        self.assertEqual(outcome.session_protocol, WIRE_FRAMED)
        self.assertEqual(outcome.identity_source, "fc2_parameter_2")
        # The read stamped the REAL listener inventory: only bytes on the real
        # socket can do that, so an injected/false reader cannot fake this.
        session = self._session_view(outcome.session_id)
        self.assertTrue(session.has_strong_identity)
        self.assertEqual(session.identity_source, "fc2_parameter_2")
        self.assertEqual(session.collector_pn, FULL_PN)
        self.assertNotEqual(FULL_PN, SHORT_HEARTBEAT_PN)  # enrichment was real
        self._assert_certified_handoff_transfers_socket(outcome)
        handle = self._registry.session_handle_for_entry("entry-e2e")
        self.assertTrue(handle.uses_framed_wire)

    async def test_read_lands_on_the_claimed_socket_not_a_same_ip_current_one(self) -> None:
        """Two collectors behind ONE peer IP; the read must hit the claimed one.

        Collector A is connected and ACTIVE before the attempt (it is the
        listener's "current connection" and its only IP-routed one). Collector B
        dials in during the attempt. A transaction that resolves the read by
        peer IP or "current connection" reads A and yields A's PN; the claimed
        session id is B's, so only a session-pinned read can certify B.
        """

        from custom_components.eybond_local.collector.transport import (
            SharedEybondTransport,
        )

        udp_a = _free_port(socket.SOCK_DGRAM)
        udp_b = _free_port(socket.SOCK_DGRAM)
        service_a = _framed_service(udp_port=udp_a, pn=OTHER_FULL_PN)
        service_b = _framed_service(udp_port=udp_b, pn=FULL_PN)
        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")

        # A dials in pre-baseline and is ACTIVATED (IP-routed, like a runtime
        # would): the dangerous "current connection" now exists and is A.
        await service_a.handle_discovery(redirect, ("127.0.0.1", 0))
        await self._wait_for_session_count(1)
        keeper = SharedEybondTransport(
            host="127.0.0.1",
            port=self._tcp_port,
            request_timeout=2.0,
            heartbeat_interval=30.0,
            collector_ip="127.0.0.1",
        )
        await keeper.start()
        try:
            self.assertTrue(await keeper.wait_until_connected(timeout=3.0))

            async def _b_dials_in() -> None:
                await asyncio.sleep(0.3)  # after the attempt's baseline
                await service_b.handle_discovery(redirect, ("127.0.0.1", 0))

            dial_task = asyncio.create_task(_b_dials_in())
            try:
                outcome = await asyncio.wait_for(
                    async_run_callback_identity_transaction(
                        self._hass,
                        CallbackIdentityRequest(
                            server_ip="127.0.0.1",
                            tcp_port=self._tcp_port,
                            udp_port=udp_b,
                            strategy=CONNECTION_STRATEGY_INBOUND,
                            session_wait_timeout=4.0,
                        ),
                    ),
                    timeout=_HARNESS_TIMEOUT,
                )
            finally:
                await dial_task
        finally:
            await service_a.stop()
            await service_b.stop()
            await keeper.stop()

        self.assertTrue(outcome.identity_certified, outcome.result)
        # The certified identity is B's -- an IP-/current-routed read would have
        # answered with A's PN and failed the matcher (or certified a stranger).
        self.assertEqual(outcome.collector_pn, FULL_PN)
        self.assertNotEqual(outcome.collector_pn, OTHER_FULL_PN)
        # A stayed exactly as it was: not claimed, not disturbed.
        self.assertEqual(self._registry.owner_for_pn(OTHER_FULL_PN), "")


class AtTextProductionWireTests(ProductionWireHarness):
    async def test_at_text_identity_certified_end_to_end(self) -> None:
        """A real at_text socket: claim by session id -> AT+DTUPN? -> handoff.

        The PRODUCTION reader must activate exactly the claimed session and
        read DTUPN on it -- ``collector.dtupn_queries`` proves those bytes hit
        this very socket (an injected reader leaves it at zero and fails the
        test). A reader that trusted a persisted/expected protocol instead of
        the live negotiated wire would speak framed here and never obtain a PN.
        """

        collector = _ScriptedAtTextCollector(AT_FULL_PN)

        async def _dials_in() -> None:
            await asyncio.sleep(0.3)  # after the attempt's baseline
            await collector.connect("127.0.0.1", self._tcp_port)

        dial_task = asyncio.create_task(_dials_in())
        try:
            outcome = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass,
                    CallbackIdentityRequest(
                        server_ip="127.0.0.1",
                        tcp_port=self._tcp_port,
                        udp_port=0,
                        strategy=CONNECTION_STRATEGY_INBOUND,
                        session_wait_timeout=4.0,
                    ),
                ),
                timeout=_HARNESS_TIMEOUT,
            )
        finally:
            await dial_task
            await collector.stop()

        self.assertTrue(outcome.identity_certified, outcome.result)
        self.assertEqual(outcome.collector_pn, AT_FULL_PN)
        self.assertEqual(outcome.session_protocol, WIRE_AT_TEXT)
        self.assertEqual(outcome.identity_source, "at_dtupn")
        # The authoritative read demonstrably hit THIS socket: only real bytes
        # on the wire increment the scripted collector's query counter.
        self.assertGreaterEqual(collector.dtupn_queries, 1)
        session = self._session_view(outcome.session_id)
        self.assertTrue(session.has_strong_identity)
        self.assertEqual(session.identity_source, "at_dtupn")
        self._assert_certified_handoff_transfers_socket(outcome)
        handle = self._registry.session_handle_for_entry("entry-e2e")
        self.assertTrue(handle.uses_at_text_wire)


if __name__ == "__main__":
    unittest.main()


class _SilentAtTextCollector:
    """A fully SILENT at_text collector: dials in, says nothing, answers DTUPN."""

    def __init__(self, pn: str) -> None:
        self._pn = pn
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._task: asyncio.Task | None = None
        self.dtupn_queries = 0

    async def connect(self, host: str, port: int) -> None:
        self._reader, self._writer = await asyncio.open_connection(host, port)
        # ZERO unsolicited bytes -- the pcap shape. Only the serve loop runs.
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
        task = self._task
        self._task = None
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


class SilentBootstrapProductionWireTests(ProductionWireHarness):
    """First-ever fully silent sockets: the explicit bootstrap intent path."""

    def _request(self, udp_port: int, **kwargs) -> CallbackIdentityRequest:
        return CallbackIdentityRequest(
            server_ip="127.0.0.1",
            tcp_port=self._tcp_port,
            udp_port=udp_port,
            target_ip="127.0.0.1",
            strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            session_wait_timeout=3.0,
            **kwargs,
        )

    async def test_silent_framed_socket_bootstraps_via_explicit_fc2(self) -> None:
        from custom_components.eybond_local.onboarding.callback_identity import (
            IDENTITY_SESSION_SILENT,
            OnboardingWireProbeIntent,
        )

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
            # Attempt 1 (no intent): the session ARRIVES and stays silent --
            # the honest typed result carries the exact causally-new socket.
            first = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass, self._request(udp_port)
                ),
                timeout=10.0,
            )
            self.assertEqual(first.result, IDENTITY_SESSION_SILENT)
            self.assertIsNotNone(first.silent_bootstrap_offer)
            self.assertFalse(first.identity_certified)
            self.assertEqual(getattr(service, "pre_rx_heartbeats", 0), 0)
            # No claim leaked from the silent attempt.
            self.assertEqual(self._registry.owner_for_pn(FULL_PN), "")

            # Attempt 2: the user explicitly picked the framed protocol. ONE
            # read-only FC=2 query on exactly the bound socket certifies the
            # full PN end to end.
            second = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass,
                    self._request(
                        udp_port,
                        bootstrap_probe=OnboardingWireProbeIntent(
                            protocol=WIRE_FRAMED,
                            session_id=first.silent_bootstrap_offer.session_id,
                        ),
                    ),
                ),
                timeout=10.0,
            )
        finally:
            await service.stop()

        self.assertTrue(second.identity_certified, second.result)
        self.assertEqual(second.collector_pn, FULL_PN)
        self.assertEqual(second.session_id, first.silent_bootstrap_offer.session_id)
        self.assertEqual(second.identity_source, "fc2_parameter_2")
        self.assertEqual(getattr(service, "pre_rx_heartbeats", 0), 0)
        self._assert_certified_handoff_transfers_socket(second)

    async def test_silent_at_socket_bootstraps_via_explicit_dtupn(self) -> None:
        from custom_components.eybond_local.onboarding.callback_identity import (
            IDENTITY_SESSION_SILENT,
            OnboardingWireProbeIntent,
        )

        collector = _SilentAtTextCollector(AT_FULL_PN)
        udp_port = _free_port(socket.SOCK_DGRAM)

        class _DialingSender:
            async def async_send(self, _request) -> None:
                from custom_components.eybond_local.connection.callback_ledger import (
                    get_callback_trigger_ledger,
                )

                ledger = get_callback_trigger_ledger()
                with ledger.callback_send_scope():
                    ledger.record(target="127.0.0.1", source="test_silent_at")
                await collector.connect("127.0.0.1", self._tcp_port)

        sender = _DialingSender()
        sender._tcp_port = self._tcp_port

        try:
            first = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass, self._request(udp_port), sender=sender
                ),
                timeout=10.0,
            )
            self.assertEqual(first.result, IDENTITY_SESSION_SILENT)
            self.assertIsNotNone(first.silent_bootstrap_offer)
            self.assertEqual(collector.dtupn_queries, 0)

            class _NoDialSender:
                async def async_send(self, _request) -> None:
                    from custom_components.eybond_local.connection.callback_ledger import (
                        get_callback_trigger_ledger,
                    )

                    ledger = get_callback_trigger_ledger()
                    with ledger.callback_send_scope():
                        ledger.record(target="127.0.0.1", source="test_silent_at2")

            second = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass,
                    self._request(
                        udp_port,
                        bootstrap_probe=OnboardingWireProbeIntent(
                            protocol=WIRE_AT_TEXT,
                            session_id=first.silent_bootstrap_offer.session_id,
                        ),
                    ),
                    sender=_NoDialSender(),
                ),
                timeout=10.0,
            )
        finally:
            await collector.stop()

        self.assertTrue(second.identity_certified, second.result)
        self.assertEqual(second.collector_pn, AT_FULL_PN)
        self.assertEqual(second.identity_source, "at_dtupn")
        self.assertEqual(second.session_id, first.silent_bootstrap_offer.session_id)
        self.assertGreaterEqual(collector.dtupn_queries, 1)
        self._assert_certified_handoff_transfers_socket(second)

    async def test_wrong_protocol_intent_fails_typed_without_fallback(self) -> None:
        # An AT collector, but the user picked FRAMED: the single framed FC=2
        # probe gets no valid answer -> typed failure, NO automatic DTUPN
        # attempt, no claim, no evidence.
        from custom_components.eybond_local.onboarding.callback_identity import (
            IDENTITY_SESSION_SILENT,
            IDENTITY_WIRE_PROBE_FAILED,
            OnboardingWireProbeIntent,
        )

        collector = _SilentAtTextCollector(AT_FULL_PN)
        udp_port = _free_port(socket.SOCK_DGRAM)

        class _DialingSender:
            def __init__(self, tcp_port: int) -> None:
                self._tcp_port = tcp_port

            async def async_send(self, _request) -> None:
                from custom_components.eybond_local.connection.callback_ledger import (
                    get_callback_trigger_ledger,
                )

                ledger = get_callback_trigger_ledger()
                with ledger.callback_send_scope():
                    ledger.record(target="127.0.0.1", source="test_wrong_wire")
                if collector._writer is None:
                    await collector.connect("127.0.0.1", self._tcp_port)

        try:
            first = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass,
                    self._request(udp_port),
                    sender=_DialingSender(self._tcp_port),
                ),
                timeout=10.0,
            )
            self.assertEqual(first.result, IDENTITY_SESSION_SILENT)

            second = await asyncio.wait_for(
                async_run_callback_identity_transaction(
                    self._hass,
                    self._request(
                        udp_port,
                        bootstrap_probe=OnboardingWireProbeIntent(
                            protocol=WIRE_FRAMED,  # deliberately wrong
                            session_id=first.silent_bootstrap_offer.session_id,
                        ),
                    ),
                    sender=_DialingSender(self._tcp_port),
                ),
                timeout=10.0,
            )
        finally:
            await collector.stop()

        self.assertEqual(second.result, IDENTITY_WIRE_PROBE_FAILED)
        self.assertFalse(second.identity_certified)
        # The wrong wire was never silently retried as AT.
        self.assertEqual(collector.dtupn_queries, 0)
        # Nothing was claimed and no identity leaked into the inventory.
        self.assertEqual(self._registry.owner_for_pn(AT_FULL_PN), "")
