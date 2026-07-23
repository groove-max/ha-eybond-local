"""Automatic normal/deep onboarding must identify a FULLY-SILENT callback
collector by an attempt-scoped, exact-session identity probe -- even when a
stale same-peer-IP silent session is still parked in the shared listener.

Regression context (see the batch report): commit 614dae7 un-armed onboarding's
FC=2 silent-identity probe and commit 5b7d70c removed the same-peer-IP
``parked_replaced`` eviction, so two silent sockets from one peer IP now
coexist. The automatic scan, still attributing the pending socket by peer IP
(``_select_pending_socket(collector_ip)`` -> ``None`` when ``len(exact) > 1``)
and skipping PN-less sessions in ``_session_inventory_results``, no longer gets
the collector PN -- the search returns only ``udp_reply`` and the terminal
defence aborts with ``collector_identity_required``.

These are PRODUCTION-WIRE tests: a real ``_SharedEybondListener``, real accepted
sockets from real fake collectors, a real UDP callback trigger, and the
production FC=2 parameter-2 / ``AT+DTUPN`` read. No injected reader, no private
session-inventory substitution. Synthetic identities only.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import AsyncMock, patch

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
from custom_components.eybond_local.onboarding.detection import DiscoveryTarget
from custom_components.eybond_local.onboarding.eybond import OnboardingDetector
from fake_collector import FakeCollectorService
from fake_collector_lib import CollectorProfile, resolve_scenario

# Synthetic, 18-char PNs: the framed heartbeat carries only the first 14, so the
# full spelling can come ONLY from the authoritative FC=2 parameter-2 read.
TARGET_PN = "V001020SYN62344022"   # the fresh S2 collector this scan must identify
STALE_PN = "V000405SYN94677058"   # the stale S1 park (same peer IP), must be left alone

_HARNESS_TIMEOUT = 12.0


def _free_port(kind: int) -> int:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _silent_framed_service(
    *,
    udp_port: int,
    pn: str,
    udp_reply: str = "rsp>server=1;",
    listen_ip: str = "127.0.0.1",
    tcp_bind_ip: str = "127.0.0.1",
    first_heartbeat_delay: float = 3600.0,
) -> FakeCollectorService:
    """A framed collector that dials in on set>server and answers a framed FC=2
    parameter-2 identity query. With the default ``first_heartbeat_delay`` it
    never volunteers a byte (fully silent); with a small value it volunteers a
    WEAK short framed-heartbeat PN shortly after connecting.

    ``listen_ip`` is the UDP ROUTE address (where set>server is delivered and the
    reply comes from); ``tcp_bind_ip`` is the reverse-TCP source, i.e. the TCP
    PEER the shared listener observes. Real NAT topology keeps these DIFFERENT.
    """

    return FakeCollectorService(
        listen_ip=listen_ip,
        udp_port=udp_port,
        tcp_bind_ip=tcp_bind_ip,
        heartbeat_interval=30.0,
        connect_timeout=2.0,
        udp_reply=udp_reply,
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=pn),
            first_heartbeat_delay=first_heartbeat_delay,
        ),
    )


class _RewrittenUdpReplyService(FakeCollectorService):
    """Reply from a gateway address while the addressed route stays distinct."""

    def __init__(self, *args, udp_reply_bind_ip: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._udp_reply_bind_ip = udp_reply_bind_ip

    async def handle_discovery(self, data: bytes, addr: tuple[str, int]) -> None:
        # Model a hairpin-NAT/UDP-proxy topology: HA addressed ``listen_ip``,
        # while the reply source is a different local address.  The reverse TCP
        # source is independently controlled by ``tcp_bind_ip``.
        reply = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            reply.bind((self._udp_reply_bind_ip, 0))
            reply.sendto(b"rsp>server=1;", addr)
        finally:
            reply.close()
        original = self._udp_reply
        self._udp_reply = ""
        try:
            await super().handle_discovery(data, addr)
        finally:
            self._udp_reply = original


def _result_pns(results) -> set[str]:
    pns: set[str] = set()
    for result in results:
        collector = getattr(result, "collector", None)
        if collector is None:
            continue
        info = getattr(collector, "collector", None)
        pn = str(getattr(info, "collector_pn", "") or "").strip() if info else ""
        if pn:
            pns.add(pn)
    return pns


def _result_with_pn(results, expected_pn: str):
    """Return the one result carrying the exact synthetic collector PN."""

    matches = []
    for result in results:
        collector = getattr(result, "collector", None)
        info = getattr(collector, "collector", None) if collector is not None else None
        if getattr(info, "collector_pn", None) == expected_pn:
            matches.append(result)
    if len(matches) > 1:
        raise AssertionError(f"duplicate collector result for PN {expected_pn}")
    return matches[0] if matches else None


class SilentScanIdentityHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tcp_port = _free_port(socket.SOCK_STREAM)
        # Acquire the SAME (host, port) the detector's transport binds
        # (_LISTENER_BIND_HOST = "0.0.0.0"), so the refcounted shared listener is
        # reused rather than fighting a second bind. Collectors still dial in on
        # 127.0.0.1, which a 0.0.0.0-bound listener accepts.
        self._listener = await _acquire_shared_listener("0.0.0.0", self._tcp_port)

    async def asyncTearDown(self) -> None:
        await _release_shared_listener(
            self._listener, close_pending=True, close_payload=True, close_at=True
        )

    def _silent_session_ids(self) -> set[str]:
        return {
            str(s.get("session_id") or "")
            for s in self._listener.silent_pending_collector_sessions()
        }

    async def _wait_for_silent_count(self, count: int, *, timeout: float = 4.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if len(self._silent_session_ids()) >= count:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(
                    f"never observed {count} silent session(s); "
                    f"have {self._silent_session_ids()}"
                )
            await asyncio.sleep(0.02)


class SilentFramedScanRegressionTests(SilentScanIdentityHarness):
    async def test_broadcast_created_socket_is_identified_before_target_phase(
        self,
    ) -> None:
        """The real E500 sequence is two-phase unless identity is captured at
        broadcast expansion: the first set>server creates one silent socket;
        a later targeted command is answered as "already connected" and creates
        no second socket. The first trigger window must therefore certify and
        hand off the exact session so target detection sends no second sequence.
        """

        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        route = "127.0.0.2"
        udp_port = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=udp_port,
            pn=TARGET_PN,
            listen_ip=route,
            tcp_bind_ip="127.0.0.1",
        )
        await target.start()
        ledger = get_callback_trigger_ledger()
        generation_before = ledger.snapshot_generation()
        try:
            from unittest.mock import AsyncMock

            detector = OnboardingDetector(
                server_ip="127.0.0.1",
                tcp_port=self._tcp_port,
                udp_port=udp_port,
            )
            # A collector-only result normally starts the unrelated /24 fallback
            # sweep. This regression concerns the broadcast->target handoff, so
            # keep the harness bounded after that production path completes.
            detector._async_unicast_fallback_targets = AsyncMock(
                return_value=()
            )
            results = await asyncio.wait_for(
                detector.async_scan(
                    # Source=broadcast exercises expansion even though the
                    # loopback route is unicast-addressable in this harness.
                    discovery_targets=(
                        DiscoveryTarget(ip=route, source="broadcast"),
                    ),
                    total_timeout=_HARNESS_TIMEOUT,
                ),
                timeout=_HARNESS_TIMEOUT + 2.0,
            )
        finally:
            await target.stop()

        result = _result_with_pn(results, TARGET_PN)
        self.assertIsNotNone(
            result,
            f"first-trigger socket was not handed off; PNs={_result_pns(results)}",
        )
        self.assertEqual(result.collector.ip, route)
        self.assertEqual(result.collector.collector.remote_ip, "127.0.0.1")
        # One logical set>server sequence: target detection activated the exact
        # broadcast-created socket instead of sending a second trigger (rsp=2).
        self.assertEqual(ledger.snapshot_generation() - generation_before, 1)

    async def test_unicast_fallback_created_socket_is_identified_before_target_phase(
        self,
    ) -> None:
        """When subnet broadcast gets no reply, the /24 fallback trigger owns
        the same first-socket handoff. Detection must not send a second sequence
        to a collector that keeps its already-open callback socket.
        """

        from unittest.mock import AsyncMock, patch

        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        route = "127.0.0.2"
        udp_port = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=udp_port,
            pn=TARGET_PN,
            listen_ip=route,
            tcp_bind_ip="127.0.0.1",
        )
        await target.start()
        ledger = get_callback_trigger_ledger()
        generation_before = ledger.snapshot_generation()
        try:
            detector = OnboardingDetector(
                server_ip="127.0.0.1",
                tcp_port=self._tcp_port,
                udp_port=udp_port,
            )
            with (
                patch(
                    "custom_components.eybond_local.onboarding.eybond."
                    "async_send_callback_trigger_replies",
                    new=AsyncMock(return_value=()),
                ),
                patch(
                    "custom_components.eybond_local.onboarding.eybond."
                    "iter_unicast_fallback_targets",
                    return_value=(
                        DiscoveryTarget(ip=route, source="subnet_unicast"),
                    ),
                ),
            ):
                results = await asyncio.wait_for(
                    detector.async_scan(
                        discovery_targets=(
                            DiscoveryTarget(ip="127.0.0.255", source="broadcast"),
                        ),
                        total_timeout=_HARNESS_TIMEOUT,
                    ),
                    timeout=_HARNESS_TIMEOUT + 2.0,
                )
        finally:
            await target.stop()

        result = _result_with_pn(results, TARGET_PN)
        self.assertIsNotNone(
            result,
            f"fallback-created socket was not handed off; PNs={_result_pns(results)}",
        )
        self.assertEqual(result.collector.ip, route)
        self.assertEqual(result.collector.collector.remote_ip, "127.0.0.1")
        self.assertEqual(ledger.snapshot_generation() - generation_before, 1)

    async def test_scan_identifies_fresh_silent_framed_collector_behind_stale_same_ip_park(
        self,
    ) -> None:
        stale_udp = _free_port(socket.SOCK_DGRAM)
        target_udp = _free_port(socket.SOCK_DGRAM)
        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")

        stale = _silent_framed_service(udp_port=stale_udp, pn=STALE_PN)
        target = _silent_framed_service(udp_port=target_udp, pn=TARGET_PN)
        await stale.start()
        await target.start()
        try:
            # S1: the stale same-peer-IP silent park is already present, and it
            # is in the attempt's baseline (it MUST be ignored, never claimed).
            await stale.handle_discovery(redirect, ("127.0.0.1", 0))
            await self._wait_for_silent_count(1)
            stale_ids = self._silent_session_ids()
            self.assertEqual(len(stale_ids), 1)

            # One normal scan attempt. Its single set>server trigger makes the
            # fresh S2 dial in silent; the attempt must probe exactly S2 (framed
            # FC=2) and surface its full PN.
            detector = OnboardingDetector(
                server_ip="127.0.0.1",
                tcp_port=self._tcp_port,
                udp_port=target_udp,
            )
            results = await asyncio.wait_for(
                detector.async_scan(
                    discovery_targets=(
                        DiscoveryTarget(ip="127.0.0.1", source="unicast"),
                    ),
                    total_timeout=_HARNESS_TIMEOUT,
                ),
                timeout=_HARNESS_TIMEOUT + 2.0,
            )
        finally:
            await stale.stop()
            await target.stop()

        # THE regression assertion: the scan surfaces the fresh collector's FULL
        # PN (18 chars) -- the value that exists ONLY in the FC=2 parameter-2 reply.
        self.assertIn(
            TARGET_PN,
            _result_pns(results),
            f"scan did not identify the silent collector; result PNs={_result_pns(results)}",
        )
        # The stale same-IP park was NEVER read for identity, never claimed, and
        # never mistaken for the target: its PN appears nowhere in the result. The
        # attempt-scoped baseline excluded S1, so only the fresh S2 was probed --
        # identity came from the exact-session FC=2 read, never a peer-IP pick.
        self.assertNotIn(STALE_PN, _result_pns(results))
        # Exactly one collector was identified, and it is the fresh target.
        self.assertEqual(_result_pns(results), {TARGET_PN})


# Synthetic AT PN (allowlisted in test_no_real_identifiers) for the AT-parity case.
AT_TARGET_PN = "E50000200000000001"


class _AnnouncingService(FakeCollectorService):
    """A collector that VOLUNTEERS an identity frame as its FIRST bytes on connect
    (unsolicited), so the shared listener observes a STRONG identity before HA
    probes anything. Used for the already-strong fc2 / at_dtupn full-call-graph
    cases; the collector otherwise refuses framed FC=2 (a probe would fail), so a
    returned full PN proves the strong-accept path took NO probe.
    """

    def __init__(self, *args, announce_bytes, **kwargs):
        super().__init__(*args, **kwargs)
        self._announce_bytes = announce_bytes

    async def _ensure_reverse_tcp(self, server_ip: str, server_port: int) -> None:
        await super()._ensure_reverse_tcp(server_ip, server_port)
        writer = self._writer
        if writer is not None and not writer.is_closing():
            writer.write(self._announce_bytes)  # the FIRST bytes the listener sniffs
            await writer.drain()


def _fc2_announce_frame(pn: str) -> bytes:
    from custom_components.eybond_local.collector.protocol import (
        FC_QUERY_COLLECTOR,
        build_collector_request,
    )

    return build_collector_request(
        1,
        b"\x00\x02" + pn.encode("ascii"),
        devcode=1,
        collector_addr=1,
        fcode=FC_QUERY_COLLECTOR,
    )


def _announcing_service(*, udp_port: int, announce_bytes: bytes, listen_ip="127.0.0.1", tcp_bind_ip="127.0.0.1") -> "_AnnouncingService":
    from dataclasses import replace

    from fake_collector_lib import QUERY_MODE_FAIL

    scenario = resolve_scenario(
        preset="collector_only",
        profile=CollectorProfile(pn="V000000SYN00000000"),  # PN comes from the announce
        first_heartbeat_delay=3600.0,  # only the announce, no heartbeat
    )
    # Refuse framed FC=2 queries: a probe would FAIL, so a returned full PN proves
    # the resolver accepted the already-strong identity WITHOUT probing.
    scenario = replace(
        scenario,
        fc2_query_modes={**dict(scenario.fc2_query_modes), 2: QUERY_MODE_FAIL},
    )
    return _AnnouncingService(
        listen_ip=listen_ip,
        udp_port=udp_port,
        tcp_bind_ip=tcp_bind_ip,
        heartbeat_interval=30.0,
        connect_timeout=2.0,
        udp_reply="rsp>server=1;",
        scenario=scenario,
        announce_bytes=announce_bytes,
    )


def _obs(session_id, *, pn="", source="", protocol_shape="", state=""):
    from custom_components.eybond_local.collector.silent_session_probe import (
        SessionObservation,
    )

    return SessionObservation(
        session_id=session_id,
        collector_pn=pn,
        identity_source=source,
        protocol_shape=protocol_shape,
        state=state,
    )


class _RecordingProbeChannel:
    """A probe-channel double that records identity queries (unit level).

    ``snapshot_session_observations`` is driven by a caller-supplied sequence of
    observation lists so a test can model the UNION view evolving over polls.
    ``async_identify_exact_session`` records the (session_id, wire) call and
    returns the configured probe PN.
    """

    def __init__(self, observation_snapshots, *, pn="", available=True):
        self._snapshots = list(observation_snapshots)
        self._pn = pn
        self.available = available
        self.identify_calls = []

    def snapshot_session_observations(self):
        if len(self._snapshots) > 1:
            return tuple(self._snapshots.pop(0))
        return tuple(self._snapshots[0]) if self._snapshots else ()

    async def async_identify_exact_session(self, session_id, *, session_protocol):
        self.identify_calls.append((session_id, session_protocol))
        return self._pn


class SilentResolutionSemanticsTests(unittest.IsolatedAsyncioTestCase):
    """The ONE unified selector over the union view, exercised directly on the
    production resolver -- silent, weak-heartbeat and already-strong sessions are
    all handled by session id (never by peer IP / order / prefix)."""

    async def _resolve(
        self, channel, *, baseline=frozenset(), deadline_after=0.4, wire_intent=None
    ):
        from custom_components.eybond_local.onboarding.silent_scan_probe import (
            AutomaticFramedIdentityIntent,
            async_resolve_silent_session_identity,
        )

        loop = asyncio.get_running_loop()
        return await async_resolve_silent_session_identity(
            channel,
            wire_intent=(
                AutomaticFramedIdentityIntent() if wire_intent is None else wire_intent
            ),
            baseline=baseline,
            deadline=loop.time() + deadline_after,
            poll_interval=0.02,
        )

    async def test_resolution_is_identified_only_with_strong_normalized_evidence(
        self,
    ) -> None:
        from custom_components.eybond_local.onboarding.silent_scan_probe import (
            SilentIdentityResolution,
        )

        self.assertTrue(
            SilentIdentityResolution(
                session_id="fresh",
                collector_pn=TARGET_PN,
                identity_source="fc2_parameter_2",
            ).identified
        )
        for resolution in (
            SilentIdentityResolution(
                session_id="fresh",
                collector_pn=TARGET_PN[:14],
                identity_source="framed_heartbeat",
            ),
            SilentIdentityResolution(
                session_id=" fresh",
                collector_pn=TARGET_PN,
                identity_source="fc2_parameter_2",
            ),
            SilentIdentityResolution(
                session_id="fresh",
                collector_pn=TARGET_PN,
                identity_source=" fc2_parameter_2",
            ),
        ):
            self.assertFalse(resolution.identified)

    async def test_fresh_silent_session_is_framed_probed(self) -> None:
        channel = _RecordingProbeChannel([[_obs("fresh")]], pn=TARGET_PN)
        r = await self._resolve(channel)
        self.assertTrue(r.identified)
        self.assertEqual(r.session_id, "fresh")
        self.assertEqual(r.collector_pn, TARGET_PN)
        self.assertEqual(channel.identify_calls, [("fresh", "eybond_framed")])

    async def test_fresh_weak_heartbeat_session_is_upgraded_by_framed_fc2(self) -> None:
        # THE reported hole: a fresh session carrying only a WEAK short
        # framed-heartbeat PN is neither silent nor strong. The union selector
        # finds it by session id and UPGRADES it to the full PN with ONE framed
        # FC=2 read on the SAME session id.
        channel = _RecordingProbeChannel(
            [[_obs("fresh", pn=TARGET_PN[:14], source="framed_heartbeat")]],
            pn=TARGET_PN,
        )
        r = await self._resolve(channel)
        self.assertTrue(r.identified)
        self.assertEqual(r.session_id, "fresh")
        self.assertEqual(r.collector_pn, TARGET_PN)  # upgraded short -> full
        self.assertEqual(channel.identify_calls, [("fresh", "eybond_framed")])

    async def test_fresh_strong_fc2_session_accepted_without_reprobe(self) -> None:
        channel = _RecordingProbeChannel(
            [[_obs("fresh", pn=TARGET_PN, source="fc2_parameter_2")]],
            pn="SHOULD-NOT-BE-READ",
        )
        r = await self._resolve(channel)
        self.assertTrue(r.identified)
        self.assertEqual(r.session_id, "fresh")
        self.assertEqual(r.collector_pn, TARGET_PN)
        self.assertEqual(channel.identify_calls, [])  # NO re-probe

    async def test_fresh_strong_at_dtupn_session_accepted_without_framed_probe(
        self,
    ) -> None:
        # Passive AT+DTUPN is preserved: a bannered at_text session is adopted as
        # is; the framed-only automatic scan never probes it.
        channel = _RecordingProbeChannel(
            [[_obs("fresh", pn=AT_TARGET_PN, source="at_dtupn", protocol_shape="at_text")]],
            pn="SHOULD-NOT-BE-READ",
        )
        r = await self._resolve(channel)
        self.assertTrue(r.identified)
        self.assertEqual(r.collector_pn, AT_TARGET_PN)
        self.assertEqual(channel.identify_calls, [])  # no framed probe

    async def test_two_fresh_sessions_are_typed_ambiguity(self) -> None:
        # One silent + one strong fresh session -> typed ambiguity, never a first/
        # last/peer-IP tiebreak, and NOTHING is probed.
        channel = _RecordingProbeChannel(
            [[_obs("a"), _obs("b", pn=TARGET_PN, source="fc2_parameter_2")]], pn="X"
        )
        r = await self._resolve(channel)
        self.assertTrue(r.ambiguous)
        self.assertFalse(r.identified)
        self.assertEqual(channel.identify_calls, [])

    async def test_only_post_baseline_sessions_are_eligible(self) -> None:
        channel = _RecordingProbeChannel(
            [[_obs("stale", pn="OLDPNSYN000000", source="fc2_parameter_2"), _obs("fresh")]],
            pn="PN-FRESH",
        )
        r = await self._resolve(channel, baseline=frozenset({"stale"}))
        self.assertTrue(r.identified)
        self.assertEqual(r.session_id, "fresh")
        self.assertEqual(channel.identify_calls, [("fresh", "eybond_framed")])

    async def test_pre_baseline_strong_observed_session_is_excluded(self) -> None:
        # A strong session already present before the trigger is in the baseline
        # and never eligible (no first/last pick, no probe).
        channel = _RecordingProbeChannel(
            [[_obs("pre", pn="OLDPNSYN000000", source="fc2_parameter_2")]], pn="X"
        )
        r = await self._resolve(
            channel, baseline=frozenset({"pre"}), deadline_after=0.2
        )
        self.assertFalse(r.identified)
        self.assertFalse(r.ambiguous)
        self.assertEqual(channel.identify_calls, [])

    async def test_no_new_session_returns_unidentified_at_deadline(self) -> None:
        channel = _RecordingProbeChannel([[]], pn="X")
        r = await self._resolve(channel, deadline_after=0.2)
        self.assertFalse(r.identified)
        self.assertFalse(r.ambiguous)
        self.assertEqual(channel.identify_calls, [])

    async def test_closed_probe_channel_is_inert(self) -> None:
        channel = _RecordingProbeChannel([[_obs("fresh")]], pn="X", available=False)
        r = await self._resolve(channel)
        self.assertFalse(r.identified)
        self.assertEqual(channel.identify_calls, [])

    async def test_cancellation_propagates_and_sends_no_probe(self) -> None:
        channel = _RecordingProbeChannel([[]], pn="X")
        task = asyncio.ensure_future(self._resolve(channel, deadline_after=30.0))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(channel.identify_calls, [])

    async def test_wrong_wire_yields_no_pn_and_never_falls_back(self) -> None:
        channel = _RecordingProbeChannel([[_obs("fresh")]], pn="")  # probe -> no PN
        r = await self._resolve(channel)
        self.assertFalse(r.identified)
        self.assertEqual(channel.identify_calls, [("fresh", "eybond_framed")])

    async def test_resolver_rejects_non_typed_wire_intent(self) -> None:
        # The REAL invariant (was mis-named "cannot be minted from a raw string"):
        # the resolver authorizes a probe ONLY for the strict typed capability, so
        # a duck / raw object cannot mint probe authority.
        from types import SimpleNamespace

        channel = _RecordingProbeChannel(
            [[_obs("fresh", pn=TARGET_PN, source="fc2_parameter_2")]], pn="X"
        )
        r = await self._resolve(
            channel,
            wire_intent=SimpleNamespace(source="automatic_onboarding_attempt"),
        )
        self.assertFalse(r.identified)
        self.assertEqual(channel.identify_calls, [])


class SilentScanArchitectureGuards(unittest.TestCase):
    """Guards that keep the silent-scan fix inside its lane (see the batch report).

    None of these may regress silently: they read the REAL production source and
    the REAL typed contracts, and they name the exact bans.
    """

    @staticmethod
    def _code_identifiers(source: str) -> set:
        # Real code identifiers only -- Name/Attribute/kwarg/import -- so a docstring
        # that DESCRIBES what is not used ("never reads peer IP") is not a match.
        import ast
        import textwrap

        tree = ast.parse(textwrap.dedent(source))
        ids: set = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                ids.add(node.id)
            elif isinstance(node, ast.Attribute):
                ids.add(node.attr)
            elif isinstance(node, ast.keyword) and node.arg:
                ids.add(node.arg)
            elif isinstance(node, ast.arg):
                ids.add(node.arg)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    ids.add(alias.name)
        return ids

    def _probe_module_source(self) -> str:
        import inspect

        from custom_components.eybond_local.onboarding import silent_scan_probe

        return inspect.getsource(silent_scan_probe)

    def _helper_source(self) -> str:
        import inspect

        from custom_components.eybond_local.onboarding.eybond import OnboardingDetector

        return inspect.getsource(OnboardingDetector._async_trigger_connect_identify)

    def test_wire_authority_is_the_typed_intent_never_metadata(self) -> None:
        # The resolution helper never chooses the wire from collector kind / cloud
        # family / hostname / peer IP / PN prefix / a persisted expected protocol.
        # ``def _volunteered_snapshot`` deliberately reads session_id/collector_pn
        # (identity), never peer IP -- prove peer_ip/remote_ip are absent from CODE.
        ids = self._code_identifiers(
            self._probe_module_source()
        ) | self._code_identifiers(self._helper_source())
        for banned in (
            "peer_ip",
            "remote_ip",
            "collector_kind",
            "cloud_family",
            "hostname",
            "collector_cloud_profiles",
            "collector_session_protocol_from_inventory_state",
            "CONF_COLLECTOR_SESSION_PROTOCOL",
            "startswith",  # no PN-prefix wire inference
        ):
            self.assertNotIn(
                banned,
                ids,
                msg=f"silent-scan wire authority must not use {banned!r}",
            )
        # protocol_shape is copied from the exact observation into the typed
        # admission carrier. It must not choose the automatic query wire: that
        # remains the framed-only intent passed to async_identify_exact_session.
        helper = self._helper_source()
        self.assertIn("AutomaticFramedIdentityIntent", helper)

    def test_uses_the_one_ledger_and_the_one_probe_boundary(self) -> None:
        ids = self._code_identifiers(self._helper_source())
        # The ONE shared callback ledger/lease -- never a second ledger/matcher.
        self.assertIn("get_callback_trigger_ledger", ids)
        self.assertIn("causality_lease", ids)
        for banned in (
            "CallbackTriggerLedger",  # no fresh ledger instance
            "match_callback_answer",  # no second matcher in onboarding scan
            "_pending_sockets",  # no private listener internals
            "_session_inventory",
            "register_session_protocol_owner",  # no listener-wide protocol owner
        ):
            self.assertNotIn(banned, ids, msg=f"silent-scan must not use {banned!r}")
        # The ONE narrow public silent-session boundary.
        self.assertIn("SilentSessionIdentityProbeChannel", ids)

    def test_intent_is_a_framed_only_typed_capability(self) -> None:
        # The automatic capability is FRAMED-only by construction (no speculative
        # AT authority) and its provenance is pinned.
        from custom_components.eybond_local.onboarding.silent_scan_probe import (
            AUTOMATIC_FRAMED_IDENTITY_SOURCE,
            AutomaticFramedIdentityIntent,
        )

        AutomaticFramedIdentityIntent()  # valid, framed-only, no protocol argument
        with self.assertRaises(ValueError):
            AutomaticFramedIdentityIntent(source="explicit_user_selection")
        self.assertEqual(
            AUTOMATIC_FRAMED_IDENTITY_SOURCE, "automatic_onboarding_attempt"
        )
        # There is no ``protocol`` field to smuggle an AT wire through.
        self.assertNotIn("protocol", AutomaticFramedIdentityIntent.__slots__)

    def test_resolution_reads_only_the_probe_channels_public_surface(self) -> None:
        # The resolution touches ONLY snapshot_session_observations /
        # async_identify_exact_session / available -- no listener internals.
        import ast

        source = self._probe_module_source()
        tree = ast.parse(source)
        attrs = {
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
        }
        # The probe-channel attributes it is allowed to touch.
        self.assertIn("snapshot_session_observations", attrs)
        self.assertIn("async_identify_exact_session", attrs)
        for banned in ("_pending_sockets", "_session_inventory", "_listener"):
            self.assertNotIn(banned, attrs)


def _result_with_pn(results, pn):
    for result in results:
        collector = getattr(result, "collector", None)
        info = getattr(collector, "collector", None) if collector else None
        if info and str(getattr(info, "collector_pn", "") or "").strip() == pn:
            return result
    return None


class RouteVsPeerAndLifecycleTests(SilentScanIdentityHarness):
    """Real NAT topology: the UDP ROUTE hint and the TCP PEER are different
    addresses, and a stale collector of ANOTHER route shares the peer IP."""

    async def test_route_hint_differs_from_tcp_peer_full_pn_no_owner_leak(
        self,
    ) -> None:
        """A + B + C: full async_scan. UDP route 127.0.0.2, TCP peer
        127.0.0.1, a stale S1 of route 127.0.0.3 on the SAME peer. The scan
        identifies S2 by its EXACT session id + full FC=2 PN; the route stays
        collector.ip and the owner, the TCP peer lives ONLY in
        CollectorInfo.remote_ip; S1 survives the target's cleanup; payload owners
        return to baseline (no leak for the route address)."""

        route_s2 = "127.0.0.2"
        peer = "127.0.0.1"
        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        s2_udp = _free_port(socket.SOCK_DGRAM)
        stale = _silent_framed_service(
            udp_port=_free_port(socket.SOCK_DGRAM),
            pn=STALE_PN,
            listen_ip="127.0.0.3",
            tcp_bind_ip=peer,
        )
        target = _silent_framed_service(
            udp_port=s2_udp, pn=TARGET_PN, listen_ip=route_s2, tcp_bind_ip=peer
        )
        await stale.start()
        await target.start()
        try:
            # Stale S1 (different route, same peer) parked before the attempt.
            await stale.handle_discovery(redirect, ("127.0.0.1", 0))
            await self._wait_for_silent_count(1)
            (stale_sid,) = tuple(self._silent_session_ids())
            owner_baseline = dict(self._listener._payload_owner_counts)

            detector = OnboardingDetector(
                server_ip="127.0.0.1", tcp_port=self._tcp_port, udp_port=s2_udp
            )
            results = await asyncio.wait_for(
                detector.async_scan(
                    discovery_targets=(
                        DiscoveryTarget(ip=route_s2, source="unicast"),
                    ),
                    total_timeout=_HARNESS_TIMEOUT,
                ),
                timeout=_HARNESS_TIMEOUT + 2.0,
            )
            # Snapshot lifecycle state BEFORE stopping the fakes (which reap sockets).
            post_silent = self._silent_session_ids()
            owner_after = dict(self._listener._payload_owner_counts)
        finally:
            await stale.stop()
            await target.stop()

        result = _result_with_pn(results, TARGET_PN)
        self.assertIsNotNone(
            result, f"S2 not identified; result PNs={_result_pns(results)}"
        )
        # Route hint stays the collector address; the TCP peer is ONLY in
        # CollectorInfo.remote_ip -- ownership never transferred to the peer.
        self.assertEqual(result.collector.ip, route_s2)
        self.assertEqual(result.collector.collector.remote_ip, peer)
        self.assertNotEqual(result.collector.ip, peer)
        self.assertEqual(result.observed_session.session_id.startswith("listener-"), True)
        self.assertEqual(result.observed_session.collector_pn, TARGET_PN)
        self.assertEqual(result.observed_session.identity_source, "fc2_parameter_2")
        self.assertEqual(result.observed_session.protocol_shape, "eybond_framed")
        self.assertEqual(result.callback_route.trigger_target_ip, route_s2)
        self.assertEqual(result.callback_route.advertised_ha_host, "127.0.0.1")
        # Stale S1 of another route survived the target's cleanup, still parked.
        self.assertIn(stale_sid, post_silent)
        # No leaked payload owner for the route address; owners back to baseline.
        self.assertEqual(owner_after.get(route_s2, 0), owner_baseline.get(route_s2, 0))
        self.assertEqual(owner_after, owner_baseline)

    async def test_no_udp_reply_still_identifies_via_tcp_evidence(self) -> None:
        """D: the UDP reply is dropped, but the callback TCP session still arrives.
        The exact-session probe reads the full PN off TCP; the trigger runs exactly
        ONCE; route != peer; no IP matching."""

        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        route = "127.0.0.2"
        peer = "127.0.0.1"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=s2_udp,
            pn=TARGET_PN,
            listen_ip=route,
            tcp_bind_ip=peer,
            udp_reply="",  # NO UDP reply at all
        )
        await target.start()
        ledger = get_callback_trigger_ledger()
        gen_before = ledger.snapshot_generation()
        try:
            detector = OnboardingDetector(
                server_ip="127.0.0.1", tcp_port=self._tcp_port, udp_port=s2_udp
            )
            result = await asyncio.wait_for(
                detector._async_detect_target(
                    DiscoveryTarget(ip=route, source="unicast"),
                    discovery_timeout=0.3,
                    connect_timeout=4.0,
                    heartbeat_timeout=0.3,
                ),
                timeout=_HARNESS_TIMEOUT,
            )
        finally:
            await target.stop()
        gen_after = ledger.snapshot_generation()

        info = result.collector.collector
        self.assertEqual(str(getattr(info, "collector_pn", "") or ""), TARGET_PN)
        self.assertEqual(result.collector.udp_reply, "")     # no reply was received
        self.assertEqual(result.collector.ip, route)         # route, not peer
        self.assertEqual(info.remote_ip, peer)
        self.assertEqual(gen_after - gen_before, 1)          # exactly one trigger

    async def test_unicast_route_is_not_replaced_by_rewritten_udp_reply(self) -> None:
        """The attempted route is durable; reply source and TCP peer are facts only."""

        route = "127.0.0.2"
        reply_source = "127.0.0.4"
        peer = "127.0.0.1"
        udp_port = _free_port(socket.SOCK_DGRAM)
        service = _RewrittenUdpReplyService(
            listen_ip=route,
            udp_port=udp_port,
            tcp_bind_ip=peer,
            heartbeat_interval=30.0,
            connect_timeout=2.0,
            udp_reply="rsp>server=1;",
            scenario=resolve_scenario(
                preset="collector_only",
                profile=CollectorProfile(pn=TARGET_PN),
                first_heartbeat_delay=3600.0,
            ),
            udp_reply_bind_ip=reply_source,
        )
        await service.start()
        try:
            detector = OnboardingDetector(
                server_ip="127.0.0.1",
                tcp_port=self._tcp_port,
                udp_port=udp_port,
            )
            result = await asyncio.wait_for(
                detector._async_detect_target(
                    DiscoveryTarget(ip=route, source="subnet_unicast"),
                    discovery_timeout=0.4,
                    connect_timeout=4.0,
                    heartbeat_timeout=0.2,
                ),
                timeout=_HARNESS_TIMEOUT,
            )
        finally:
            await service.stop()

        self.assertEqual(result.collector.ip, route)
        self.assertEqual(result.collector.target_ip, route)
        self.assertTrue(result.collector.udp_reply_from.startswith(reply_source + ":"))
        self.assertEqual(result.collector.collector.remote_ip, peer)
        self.assertEqual(result.collector.collector.collector_pn, TARGET_PN)

    async def test_lease_busy_is_distinct_retryable_outcome_zero_sends(self) -> None:
        """Lease-busy: a FOREIGN owner holds the callback causality lease. THIS
        attempt sends NOTHING (an out-of-lease send is refused by
        callback_send_scope; the raw error never escapes) and returns a DISTINCT
        typed ``callback_causality_lease_busy`` outcome -- never
        collector_not_connected/reverse_tcp_not_connected -- that a later scan
        attempt can retry. Zero callback sends for the target."""

        from custom_components.eybond_local.connection.callback_ledger import (
            get_callback_trigger_ledger,
        )

        route = "127.0.0.2"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=s2_udp, pn=TARGET_PN, listen_ip=route, tcp_bind_ip="127.0.0.1"
        )
        await target.start()
        ledger = get_callback_trigger_ledger()
        holding = asyncio.Event()
        release = asyncio.Event()

        async def _foreign_owner() -> None:
            async with ledger.causality_lease("foreign-scan-owner", timeout=5.0):
                holding.set()
                await release.wait()

        holder = asyncio.ensure_future(_foreign_owner())
        try:
            await asyncio.wait_for(holding.wait(), timeout=3.0)
            gen_before = ledger.snapshot_generation()
            detector = OnboardingDetector(
                server_ip="127.0.0.1", tcp_port=self._tcp_port, udp_port=s2_udp
            )
            result = await asyncio.wait_for(
                detector._async_detect_target(
                    DiscoveryTarget(ip=route, source="unicast"),
                    discovery_timeout=0.2,
                    connect_timeout=0.3,
                    heartbeat_timeout=0.1,
                ),
                timeout=_HARNESS_TIMEOUT,
            )
            gen_after = ledger.snapshot_generation()
        finally:
            release.set()
            await holder
            await target.stop()

        # ZERO sends: the ledger generation never advanced for our target.
        self.assertEqual(gen_after, gen_before)
        # DISTINCT typed lease-busy outcome, NOT collector_not_connected, and
        # retryable (not budget-exhausted). No match, no adopted identity.
        self.assertEqual(result.last_error, "callback_causality_lease_busy")
        self.assertEqual(result.detection.status, "callback_causality_lease_busy")
        self.assertNotIn(
            result.last_error,
            ("collector_not_connected", "reverse_tcp_not_connected"),
        )
        self.assertFalse(result.detection.budget_exhausted)
        self.assertIsNone(result.match)

    async def test_foreign_activation_pn_fails_closed(self) -> None:
        """F: the exact-session probe reads TARGET_PN, but the activated connection
        reports a FOREIGN PN (a same-peer sibling / re-identification). PN
        reconciliation fails closed -- no result identity, no claim, no handoff."""

        from dataclasses import replace
        from unittest.mock import patch

        from custom_components.eybond_local.collector.transport import (
            SharedEybondTransport,
        )

        route = "127.0.0.2"
        peer = "127.0.0.1"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=s2_udp, pn=TARGET_PN, listen_ip=route, tcp_bind_ip=peer
        )
        await target.start()

        class _ForeignInfoTransport(SharedEybondTransport):
            @property
            def collector_info(self):
                base = super().collector_info
                # The activated session reports an identity foreign to the probe.
                return replace(base, collector_pn=STALE_PN)

        try:
            detector = OnboardingDetector(
                server_ip="127.0.0.1", tcp_port=self._tcp_port, udp_port=s2_udp
            )
            with patch(
                "custom_components.eybond_local.onboarding.eybond.SharedEybondTransport",
                _ForeignInfoTransport,
            ):
                result = await asyncio.wait_for(
                    detector._async_detect_target(
                        DiscoveryTarget(ip=route, source="unicast"),
                        discovery_timeout=0.3,
                        connect_timeout=4.0,
                        heartbeat_timeout=0.3,
                    ),
                    timeout=_HARNESS_TIMEOUT,
                )
        finally:
            await target.stop()

        self.assertEqual(result.last_error, "collector_identity_mismatch")
        info = result.collector.collector
        self.assertEqual(str(getattr(info, "collector_pn", "") or ""), "")
        self.assertEqual(_result_pns([result]), set())

    async def test_active_scan_session_reaches_callback_recovery_handoff(self) -> None:
        """Regression for an added E500 entry whose sensors stayed offline.

        The real active scan's exact session is adopted with ZERO extra identity
        trigger; the shared recovery authority then reboots and sends one
        addressed callback, returning a prepared callback proof on the new
        fully-silent session.
        """

        from dataclasses import replace
        from types import SimpleNamespace

        from custom_components.eybond_local.connection.admission import (
            CollectorAdmissionRequest,
        )
        from custom_components.eybond_local.connection.admission_transaction import (
            CollectorAdmissionTransaction,
        )
        from custom_components.eybond_local.connection.callback_identity import (
            CallbackIdentityRequest,
        )
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )
        from custom_components.eybond_local.const import (
            CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            DOMAIN,
        )
        from custom_components.eybond_local.onboarding.timeouts import (
            DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        )

        route = "127.0.0.2"
        udp_port = _free_port(socket.SOCK_DGRAM)
        service = FakeCollectorService(
            listen_ip=route,
            udp_port=udp_port,
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=3600.0,
            connect_timeout=2.0,
            udp_reply="rsp>server=1;",
            scenario=resolve_scenario(
                preset="collector_only",
                profile=CollectorProfile(pn=TARGET_PN),
                first_heartbeat_delay=3600.0,
                set_29_mode="reboot_silent",
                reboot_reconnect_delay=0.2,
            ),
        )
        await service.start()
        try:
            detector = OnboardingDetector(
                server_ip="127.0.0.1",
                tcp_port=self._tcp_port,
                udp_port=udp_port,
            )
            result = await asyncio.wait_for(
                detector._async_detect_target(
                    DiscoveryTarget(ip=route, source="subnet_unicast"),
                    discovery_timeout=0.3,
                    connect_timeout=4.0,
                    heartbeat_timeout=0.1,
                ),
                timeout=_HARNESS_TIMEOUT,
            )
            self.assertIsNotNone(result.observed_session)
            self.assertIsNotNone(result.callback_route)

            registry = CallbackSessionRegistry(
                sessions_source=lambda: tuple(
                    {**session, "listener_port": self._tcp_port}
                    for session in self._listener.discovered_collector_sessions()
                )
            )
            hass = SimpleNamespace(
                data={DOMAIN: {"callback_session_registry": registry}}
            )
            policy = replace(
                DEFAULT_ONBOARDING_TIMEOUT_POLICY,
                callback_identity_session_wait=2.0,
                callback_recovery_session_wait=4.0,
                inbound_restart_disconnect_timeout=3.0,
                inbound_reconnect_timeout=0.5,
                callback_causality_lease_wait=3.0,
            )
            transaction = CollectorAdmissionTransaction(
                CollectorAdmissionRequest(
                    observed_session=result.observed_session,
                    origin="active_scan",
                    callback_route=result.callback_route,
                ),
                registry_provider=lambda: registry,
                listener_host="0.0.0.0",
                policy_provider=lambda: policy,
                hass_provider=lambda: hass,
            )
            transaction.begin_observed_callback_continuation()
            context = transaction.identity_context
            identity = await asyncio.wait_for(
                transaction.async_run_identity(
                    CallbackIdentityRequest(
                        server_ip=result.callback_route.bind_ip,
                        tcp_port=result.callback_route.listener_port,
                        udp_port=result.callback_route.trigger_udp_port,
                        target_ip=result.callback_route.trigger_target_ip,
                        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                        expected_pn=context.expected_pn,
                        old_session_id=context.old_session_id,
                        bootstrap_probe=transaction.observed_wire_probe_intent(),
                    )
                ),
                timeout=5.0,
            )
            self.assertTrue(identity.identity_certified, identity.result)
            transaction.adopt_certified_pn(identity.collector_pn)
            outcome = await asyncio.wait_for(
                transaction.async_run_recovery(result.callback_route), timeout=8.0
            )
            self.assertTrue(outcome.callback_verified, outcome.failure_reason)
            self.assertNotEqual(outcome.new_session_id, result.observed_session.session_id)
            consumed = transaction.consume_recovery_outcome()
            self.assertIs(consumed, outcome)
            self.assertTrue(transaction.adopt_recovery(consumed))
            self.assertTrue(transaction.terminal_input.callback_proof)
        finally:
            await service.stop()


class UnionSelectorFullCallGraphTests(SilentScanIdentityHarness):
    """Full ``async_scan`` call graph for the weak-heartbeat and
    already-strong cases -- route != peer, stale same-peer S1 present."""

    async def _scan_target(
        self,
        target_service,
        route,
        s2_udp,
    ):
        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        stale = _silent_framed_service(
            udp_port=_free_port(socket.SOCK_DGRAM),
            pn=STALE_PN,
            listen_ip="127.0.0.4",
            tcp_bind_ip="127.0.0.1",
        )
        await stale.start()
        await target_service.start()
        try:
            await stale.handle_discovery(redirect, ("127.0.0.1", 0))
            await self._wait_for_silent_count(1)
            (stale_sid,) = tuple(self._silent_session_ids())
            detector = OnboardingDetector(
                server_ip="127.0.0.1", tcp_port=self._tcp_port, udp_port=s2_udp
            )
            results = await asyncio.wait_for(
                detector.async_scan(
                    discovery_targets=(DiscoveryTarget(ip=route, source="unicast"),),
                    total_timeout=_HARNESS_TIMEOUT,
                ),
                timeout=_HARNESS_TIMEOUT + 2.0,
            )
            post_silent = self._silent_session_ids()
        finally:
            await stale.stop()
            await target_service.stop()
        return results, stale_sid, post_silent

    async def test_weak_heartbeat_session_is_upgraded_to_full_pn(self) -> None:
        """A: route 127.0.0.2, peer 127.0.0.1, stale S1 of another route. S2 dials
        in and IMMEDIATELY volunteers a WEAK short framed-heartbeat PN. The union
        selector picks S2 by session id and UPGRADES it to the full FC=2 PN; route
        stays the route; S1 survives."""

        route = "127.0.0.2"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=s2_udp,
            pn=TARGET_PN,
            listen_ip=route,
            tcp_bind_ip="127.0.0.1",
            first_heartbeat_delay=0.05,  # volunteers a WEAK heartbeat right away
        )
        results, stale_sid, post_silent = await self._scan_target(target, route, s2_udp)

        result = _result_with_pn(results, TARGET_PN)
        self.assertIsNotNone(
            result, f"weak S2 not upgraded to full PN; PNs={_result_pns(results)}"
        )
        self.assertEqual(result.collector.ip, route)                 # route, not peer
        self.assertEqual(result.collector.collector.remote_ip, "127.0.0.1")
        self.assertNotIn(STALE_PN, _result_pns(results))
        self.assertIn(stale_sid, post_silent)                        # S1 survived

    async def test_collector_only_scan_stops_before_every_driver_probe(self) -> None:
        """The setup scan proves the exact collector and leaves inversion to runtime."""

        route = "127.0.0.2"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _silent_framed_service(
            udp_port=s2_udp,
            pn=TARGET_PN,
            listen_ip=route,
            tcp_bind_ip="127.0.0.1",
            first_heartbeat_delay=0.05,
        )
        self.assertFalse(
            hasattr(OnboardingDetector, "_async_detect_driver_with_retries")
        )
        results, stale_sid, post_silent = await self._scan_target(
            target,
            route,
            s2_udp,
        )

        result = _result_with_pn(results, TARGET_PN)
        self.assertIsNotNone(result)
        self.assertIsNone(result.match)
        self.assertEqual(result.next_action, "confirm_collector")
        self.assertEqual(result.detection.reason, "collector_identity_only_scan")
        self.assertEqual(result.collector.ip, route)
        self.assertIn(stale_sid, post_silent)

    async def test_strong_fc2_session_is_accepted_full_pn(self) -> None:
        """B: S2 announces a STRONG framed FC=2 identity as its first bytes (and
        refuses framed FC=2 queries, so a probe would fail). A returned full PN
        proves the resolver accepted it on its exact session, without re-probing."""

        route = "127.0.0.2"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _announcing_service(
            udp_port=s2_udp,
            announce_bytes=_fc2_announce_frame(TARGET_PN),
            listen_ip=route,
        )
        results, stale_sid, post_silent = await self._scan_target(target, route, s2_udp)

        result = _result_with_pn(results, TARGET_PN)
        self.assertIsNotNone(
            result, f"strong fc2 S2 not accepted; PNs={_result_pns(results)}"
        )
        self.assertEqual(result.collector.ip, route)
        self.assertIn(stale_sid, post_silent)

    async def test_passive_at_dtupn_session_is_accepted_without_framed_probe(
        self,
    ) -> None:
        """C: S2 banners AT+DTUPN as its first bytes (strong at_dtupn) and refuses
        framed FC=2. It is adopted with its full AT PN on the exact session -- a
        framed probe (which would fail on this AT collector) is never sent, so
        passive AT+DTUPN is genuinely preserved."""

        route = "127.0.0.2"
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _announcing_service(
            udp_port=s2_udp,
            announce_bytes=f"AT+DTUPN:{AT_TARGET_PN}\r\n".encode("ascii"),
            listen_ip=route,
        )
        results, stale_sid, post_silent = await self._scan_target(target, route, s2_udp)

        result = _result_with_pn(results, AT_TARGET_PN)
        self.assertIsNotNone(
            result, f"passive AT S2 not adopted; PNs={_result_pns(results)}"
        )
        self.assertIn(stale_sid, post_silent)


def _weak_heartbeat_service(
    *,
    udp_port: int,
    pn: str,
    listen_ip: str = "127.0.0.1",
    tcp_bind_ip: str = "127.0.0.1",
    refuse_fc2: bool,
) -> FakeCollectorService:
    """A framed collector that volunteers a WEAK short framed-heartbeat PN right
    after dialling in. With ``refuse_fc2`` the subsequent FC=2 parameter-2 identity
    query returns an error frame (no PN), modelling the reported race where the
    upgrade fails and the low-level route probe can only fall back to the weak PN.
    """

    from dataclasses import replace

    from fake_collector_lib import QUERY_MODE_FAIL

    scenario = resolve_scenario(
        preset="collector_only",
        profile=CollectorProfile(pn=pn),
        first_heartbeat_delay=0.05,  # volunteer a WEAK heartbeat almost at once
    )
    if refuse_fc2:
        scenario = replace(
            scenario,
            fc2_query_modes={**dict(scenario.fc2_query_modes), 2: QUERY_MODE_FAIL},
        )
    return FakeCollectorService(
        listen_ip=listen_ip,
        udp_port=udp_port,
        tcp_bind_ip=tcp_bind_ip,
        heartbeat_interval=30.0,  # one heartbeat in the window, no rapid re-arm
        connect_timeout=2.0,
        udp_reply="rsp>server=1;",
        scenario=scenario,
    )


class _FakeListenerForChannel:
    """A listener double for the channel's strong-boundary re-read (unit level).

    It answers the ONLY two public methods the channel touches:
    ``async_identify_pending_session`` (the low-level probe result, which MAY be a
    weak PN) and ``discovered_collector_sessions`` (the post-probe inventory the
    boundary re-reads). ``silent_pending_collector_sessions`` is empty -- the
    session carries a PN by then. No peer IP, no private internals.
    """

    def __init__(self, *, identify_pn: str, discovered: tuple[dict, ...]):
        self._identify_pn = identify_pn
        self._discovered = discovered
        self.identify_calls: list = []

    async def async_identify_pending_session(self, session_id, *, session_protocol):
        self.identify_calls.append((session_id, session_protocol))
        return self._identify_pn

    def silent_pending_collector_sessions(self):
        return ()

    def discovered_collector_sessions(self):
        return self._discovered


def _channel_over(listener):
    from custom_components.eybond_local.collector.silent_session_probe import (
        SilentSessionIdentityProbeChannel,
    )

    channel = SilentSessionIdentityProbeChannel(host="0.0.0.0", port=1)
    channel._listener = listener  # test-only injection (production never does this)
    return channel


class ChannelStrongBoundaryTests(SilentScanIdentityHarness):
    """The trust boundary in ``SilentSessionIdentityProbeChannel``: a WEAK short
    ``framed_heartbeat`` PN that the low-level route probe fell back to must NEVER
    be returned as an exact-session STRONG identity. Only a post-probe inventory
    identity that is strong AND the same identity crosses the boundary. Real
    listener/socket for the race cases; a listener double for the deterministic
    branch cases."""

    async def _resolve_real(self, channel, *, deadline_after=4.0):
        from custom_components.eybond_local.onboarding.silent_scan_probe import (
            AutomaticFramedIdentityIntent,
            async_resolve_silent_session_identity,
        )

        loop = asyncio.get_running_loop()
        return await async_resolve_silent_session_identity(
            channel,
            wire_intent=AutomaticFramedIdentityIntent(),
            baseline=frozenset(),
            deadline=loop.time() + deadline_after,
            poll_interval=0.02,
        )

    async def _wait_for_source(self, channel, source, *, timeout=4.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            obs = channel.snapshot_session_observations()
            match = [o for o in obs if o.identity_source == source]
            if match:
                return match[0]
            if loop.time() >= deadline:
                raise AssertionError(
                    f"never observed source={source!r}; have {obs}"
                )
            await asyncio.sleep(0.02)

    def _inventory_entry(self, session_id):
        for entry in self._listener.discovered_collector_sessions():
            if str(entry.get("session_id") or "") == session_id:
                return entry
        return None

    async def test_A_weak_heartbeat_fc2_failure_does_not_cross_strong_boundary(
        self,
    ) -> None:
        """A: PN-less session, a WEAK heartbeat lands, the FC=2 upgrade FAILS. The
        channel returns "" (no weak PN promoted), the resolver is NOT identified,
        and the weak short PN survives INTACT in the inventory as framed_heartbeat.
        """

        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _weak_heartbeat_service(udp_port=s2_udp, pn=TARGET_PN, refuse_fc2=True)
        await target.start()
        channel = _channel_over(self._listener)
        try:
            await target.handle_discovery(redirect, ("127.0.0.1", 0))
            obs = await self._wait_for_source(channel, "framed_heartbeat")
            sid = obs.session_id
            # The weak observation carries only the 14-char heartbeat PN.
            self.assertEqual(obs.collector_pn, TARGET_PN[:14])

            resolution = await self._resolve_real(channel)
        finally:
            await target.stop()

        # The boundary refused to promote the weak PN: no strong identity.
        self.assertFalse(resolution.identified)
        self.assertEqual(resolution.collector_pn, "")
        # The weak fact is LEFT INTACT in the inventory (honest for route callers).
        entry = self._inventory_entry(sid)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.get("collector_pn"), TARGET_PN[:14])
        self.assertEqual(entry.get("collector_identity_source"), "framed_heartbeat")

    async def test_B_weak_heartbeat_fc2_success_upgrades_to_full_pn(self) -> None:
        """B: same race, but the FC=2 upgrade ANSWERS. The channel returns the full
        PN, the inventory identity becomes strong fc2_parameter_2, and the resolver
        is identified with the full (short->full upgraded) PN."""

        redirect = f"set>server=127.0.0.1:{self._tcp_port};".encode("ascii")
        s2_udp = _free_port(socket.SOCK_DGRAM)
        target = _weak_heartbeat_service(udp_port=s2_udp, pn=TARGET_PN, refuse_fc2=False)
        await target.start()
        channel = _channel_over(self._listener)
        try:
            await target.handle_discovery(redirect, ("127.0.0.1", 0))
            obs = await self._wait_for_source(channel, "framed_heartbeat")
            sid = obs.session_id

            resolution = await self._resolve_real(channel)
        finally:
            await target.stop()

        self.assertTrue(resolution.identified)
        self.assertEqual(resolution.session_id, sid)
        self.assertEqual(resolution.collector_pn, TARGET_PN)  # short -> full
        entry = self._inventory_entry(sid)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.get("collector_pn"), TARGET_PN)
        self.assertEqual(entry.get("collector_identity_source"), "fc2_parameter_2")

    async def test_C_low_level_weak_pn_with_weak_post_observation_fails_closed(
        self,
    ) -> None:
        """C: the low-level probe returns a PN, but the post-probe observation of
        the SAME session is still weak (framed_heartbeat). Fail closed to ""."""

        from custom_components.eybond_local.connection.session_handle import (
            WIRE_FRAMED,
        )

        listener = _FakeListenerForChannel(
            identify_pn=TARGET_PN[:14],  # the weak fallback the route probe returned
            discovered=(
                {
                    "session_id": "s2",
                    "collector_pn": TARGET_PN[:14],
                    "collector_identity_source": "framed_heartbeat",
                },
            ),
        )
        channel = _channel_over(listener)
        result = await channel.async_identify_exact_session(
            "s2", session_protocol=WIRE_FRAMED
        )
        self.assertEqual(result, "")
        # The one read still happened -- the gate is post-validation, not a skip.
        self.assertEqual(listener.identify_calls, [("s2", WIRE_FRAMED)])

    async def test_D_low_level_pn_with_foreign_post_observation_fails_closed(
        self,
    ) -> None:
        """D: the low-level probe returns TARGET_PN, but the post-probe observation
        of the same session id carries a FOREIGN strong PN (a same-peer sibling
        re-identified between probe and re-read). ``pn_is_same_identity`` rejects
        it -- fail closed to "" rather than return a disagreeing identity."""

        from custom_components.eybond_local.connection.session_handle import (
            WIRE_FRAMED,
        )

        listener = _FakeListenerForChannel(
            identify_pn=TARGET_PN,
            discovered=(
                {
                    "session_id": "s2",
                    "collector_pn": STALE_PN,  # foreign, though strong
                    "collector_identity_source": "fc2_parameter_2",
                },
            ),
        )
        channel = _channel_over(listener)
        result = await channel.async_identify_exact_session(
            "s2", session_protocol=WIRE_FRAMED
        )
        self.assertEqual(result, "")

    async def test_E_strong_same_identity_observation_is_returned(self) -> None:
        """E: a strong (fc2_parameter_2) post-probe observation whose PN is the same
        identity as the probe result crosses the boundary and returns the full PN --
        the existing strong path stays green (its at_dtupn twin is exercised by the
        full-call-graph passive-AT test)."""

        from custom_components.eybond_local.connection.session_handle import (
            WIRE_FRAMED,
        )

        listener = _FakeListenerForChannel(
            identify_pn=TARGET_PN,
            discovered=(
                {
                    "session_id": "s2",
                    "collector_pn": TARGET_PN,
                    "collector_identity_source": "fc2_parameter_2",
                },
            ),
        )
        channel = _channel_over(listener)
        result = await channel.async_identify_exact_session(
            "s2", session_protocol=WIRE_FRAMED
        )
        self.assertEqual(result, TARGET_PN)

    async def test_cancellation_is_not_swallowed_by_the_boundary(self) -> None:
        """Cancellation during the low-level probe propagates -- the strong gate
        never turns a CancelledError into a "" identity result."""

        from custom_components.eybond_local.connection.session_handle import (
            WIRE_FRAMED,
        )

        class _HangingListener(_FakeListenerForChannel):
            async def async_identify_pending_session(
                self, session_id, *, session_protocol
            ):
                await asyncio.Event().wait()  # never returns
                return ""

        listener = _HangingListener(identify_pn="", discovered=())
        channel = _channel_over(listener)
        task = asyncio.ensure_future(
            channel.async_identify_exact_session("s2", session_protocol=WIRE_FRAMED)
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_missing_post_observation_fails_closed(self) -> None:
        """If the session id is gone from the post-probe view entirely, the boundary
        fails closed (a probe PN with no confirming observation is not trusted)."""

        from custom_components.eybond_local.connection.session_handle import (
            WIRE_FRAMED,
        )

        listener = _FakeListenerForChannel(identify_pn=TARGET_PN, discovered=())
        channel = _channel_over(listener)
        result = await channel.async_identify_exact_session(
            "s2", session_protocol=WIRE_FRAMED
        )
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()
