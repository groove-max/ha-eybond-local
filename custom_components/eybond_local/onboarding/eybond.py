"""EyeBond-specific collector discovery for setup flows."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from itertools import islice
import ipaddress
import logging
import uuid
from dataclasses import dataclass, replace
from typing import Any, Sequence

from ..collector.discovery import (
    DiscoveryProbeResult,
    async_send_callback_trigger,
    async_send_callback_trigger_replies,
)
from ..collector.transport import (
    SharedEybondTransport,
    _acquire_shared_listener,
    _release_shared_listener,
)
from ..collector.transport_profile import (
    collector_session_protocol_from_inventory_state,
)
from ..connection.models import EybondConnectionSpec
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
)
from .timeouts import OnboardingDeadline
from .silent_scan_probe import (
    DEFAULT_SILENT_IDENTITY_WAIT_SECONDS,
    SilentIdentityResolution,
)
from ..connection.admission import ObservedCollectorSession
from ..connection.recovery.verification import CallbackRecoveryRoute
from ..models import (
    CollectorCandidate,
    CollectorInfo,
    OnboardingResult,
    TargetDetectionEvidence,
)

logger = logging.getLogger(__name__)

_LISTENER_BIND_HOST = "0.0.0.0"

_CONFIDENCE_SCORE = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}

_UNICAST_FALLBACK_PROBE_TIMEOUT = 0.35
_UNICAST_FALLBACK_CONCURRENCY = 32
_CONNECT_TIMEOUT_WITHOUT_UDP_REPLY = 0.75
_TARGET_DETECTION_CONCURRENCY = 8
_BROADCAST_FANOUT_SETTLE_TIMEOUT = 3.0
_BROADCAST_FANOUT_POLL_INTERVAL = 0.1


def _collector_identity_matches(left: str, right: str) -> bool:
    """Return whether two collector PN values look like the same collector."""

    normalized_left = str(left or "").strip()
    normalized_right = str(right or "").strip()
    if not normalized_left or not normalized_right:
        return False
    if normalized_left == normalized_right:
        return True
    if min(len(normalized_left), len(normalized_right)) < 10:
        return False
    return bool(
        normalized_left.startswith(normalized_right)
        or normalized_right.startswith(normalized_left)
    )


@dataclass(frozen=True, slots=True)
class DiscoveryTarget:
    """One onboarding discovery target.

    An onboarding target never carries an inferred/expected session protocol: a
    hint may not arm an active probe or register a confirmed owner. An automatic
    broadcast expansion may carry one strict ``SilentIdentityResolution`` that
    was observed on an exact session inside that same trigger window. It is an
    identity handoff only, not connection-strategy evidence.
    """

    ip: str
    source: str
    collector_pn: str = ""
    observed_session: SilentIdentityResolution | None = None
    # A unicast fallback target is not merely scheduled work: returning it from
    # the probe means this exact attempted route already answered.  Keep the
    # typed wire observation attached while later identity work enriches (or
    # times out on) the target; otherwise a valid responder disappears whenever
    # a sibling wins the serialized callback-identity phase.
    observed_probe: DiscoveryProbeResult | None = None


def _already_configured_result(target: DiscoveryTarget) -> OnboardingResult:
    """Mark one target as owned by an existing entry without probing it.

    Probing a configured collector would steal its callback session from the
    running entry and contend with its polling, so the scan reports it
    as already added instead.
    """

    return _with_detection_evidence(
        OnboardingResult(
            collector=CollectorCandidate(
                target_ip=target.ip,
                source=target.source,
                ip=target.ip,
            ),
            connection_type=CONNECTION_TYPE_EYBOND,
            connection_mode=target.source,
            next_action="",
            last_error="already_configured",
        ),
        status="already_configured",
        reason="configured_entry_owns_collector",
    )


def _collector_candidate_from_target(target: DiscoveryTarget) -> CollectorCandidate:
    """Project one target without discarding an earlier exact-route UDP reply."""

    probe = target.observed_probe
    if (
        type(probe) is DiscoveryProbeResult
        and probe.target_ip == target.ip
        and bool(probe.reply)
    ):
        udp_reply = probe.reply
        udp_reply_from = probe.reply_from
    else:
        udp_reply = ""
        udp_reply_from = ""
    return CollectorCandidate(
        target_ip=target.ip,
        source=target.source,
        ip=target.ip,
        udp_reply=udp_reply,
        udp_reply_from=udp_reply_from,
    )


@dataclass(slots=True)
class _TargetDetectionState:
    target: DiscoveryTarget
    candidate: CollectorCandidate | None = None


def _with_detection_evidence(
    result: OnboardingResult,
    *,
    status: str,
    reason: str = "",
    budget_exhausted: bool = False,
    details: dict[str, Any] | None = None,
) -> OnboardingResult:
    """Attach structured target-detection evidence to one onboarding result."""

    return replace(
        result,
        detection=TargetDetectionEvidence(
            status=status,
            reason=reason,
            budget_exhausted=budget_exhausted,
            details=dict(details or {}),
        ),
    )


def build_default_discovery_targets(
    *,
    collector_ip: str = "",
    discovery_target: str = DEFAULT_DISCOVERY_TARGET,
) -> tuple[DiscoveryTarget, ...]:
    """Build the default onboarding target order."""

    targets: list[DiscoveryTarget] = []
    if collector_ip:
        targets.append(DiscoveryTarget(ip=collector_ip, source="known_ip"))
    if discovery_target and discovery_target not in {collector_ip, ""}:
        targets.append(DiscoveryTarget(ip=discovery_target, source="broadcast"))
    return tuple(targets)


def build_unicast_fallback_targets(
    *,
    server_ip: str,
    collector_ip: str = "",
) -> tuple[DiscoveryTarget, ...]:
    """Build the bounded local-/24 fallback for broadcast-unfriendly networks."""

    return tuple(
        iter_unicast_fallback_targets(
            server_ip=server_ip,
            collector_ip=collector_ip,
        )
    )


def iter_unicast_fallback_targets(
    *,
    server_ip: str,
    collector_ip: str = "",
):
    """Yield the bounded local-/24 fallback targets."""

    if collector_ip:
        return

    try:
        network = ipaddress.ip_network(f"{server_ip}/24", strict=False)
    except ValueError:
        return

    excluded = {server_ip, collector_ip, str(network.network_address), str(network.broadcast_address), ""}
    for host in network.hosts():
        host_ip = str(host)
        if host_ip in excluded:
            continue
        yield DiscoveryTarget(ip=host_ip, source="subnet_unicast")


def _dedupe_discovery_targets(targets: Sequence[DiscoveryTarget]) -> tuple[DiscoveryTarget, ...]:
    deduped: list[DiscoveryTarget] = []
    seen: set[str] = set()
    for target in targets:
        if target.ip in seen:
            continue
        seen.add(target.ip)
        deduped.append(target)
    return tuple(deduped)


def _concrete_detection_targets(targets: Sequence[DiscoveryTarget]) -> tuple[DiscoveryTarget, ...]:
    return tuple(target for target in targets if not _is_broadcast_detection_placeholder(target))


def _is_broadcast_detection_placeholder(target: DiscoveryTarget) -> bool:
    if target.source != "broadcast":
        return False
    try:
        address = ipaddress.ip_address(target.ip)
    except ValueError:
        return False
    return address.version == 4 and str(address).endswith(".255")


async def async_probe_fallback_targets(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    udp_port: int,
    targets: Iterable[DiscoveryTarget],
    timeout: float = _UNICAST_FALLBACK_PROBE_TIMEOUT,
    concurrency: int = _UNICAST_FALLBACK_CONCURRENCY,
    identity_listener_host: str = "",
    identity_listener_port: int = 0,
    identity_wait_seconds: float = 0.0,
) -> tuple[DiscoveryTarget, ...]:
    """Probe direct unicast targets and retain an unambiguous exact-session fact.

    Targets stay concurrent inside one batch. Production callers provide the
    shared listener coordinates; then one exclusive trigger window covers the
    whole batch. Exactly one responder plus exactly one post-baseline session may
    carry a strong typed handoff into detection. Multiple responders/sessions
    remain fail-closed, without a peer-IP or arrival-order tiebreak.
    """

    async def _probe(target: DiscoveryTarget) -> DiscoveryTarget | None:
        try:
            probe = await async_send_callback_trigger(
                bind_ip=bind_ip,
                advertised_server_ip=advertised_server_ip,
                advertised_server_port=advertised_server_port,
                target_ip=target.ip,
                udp_port=udp_port,
                timeout=timeout,
                source="onboarding_fallback_probe",
            )
        except Exception as exc:
            logger.debug("Fallback unicast probe failed target=%s error=%s", target.ip, exc)
            return None

        if not probe.reply:
            return None

        # This is an addressed unicast attempt: the address HA successfully
        # sent to is the route capability.  ``reply_from`` is only an observed
        # diagnostic address and may be rewritten by hairpin NAT, a UDP proxy,
        # or the collector's gateway.  Replacing the attempted route with that
        # address creates entries which can identify the first callback socket
        # but can never trigger the collector again after it disconnects.
        return replace(target, observed_probe=probe)

    iterator = iter(targets)
    deduped: dict[str, DiscoveryTarget] = {}
    batch_size = max(1, concurrency)
    probe_channel = None
    if identity_listener_port > 0:
        from ..collector.silent_session_probe import (
            SilentSessionIdentityProbeChannel,
        )

        probe_channel = SilentSessionIdentityProbeChannel(
            host=identity_listener_host or _LISTENER_BIND_HOST,
            port=identity_listener_port,
        )
        await probe_channel.async_open()
    try:
        while True:
            batch = tuple(islice(iterator, batch_size))
            if not batch:
                break

            async def _send_batch() -> tuple[DiscoveryTarget | None, ...]:
                return tuple(await asyncio.gather(*(_probe(target) for target in batch)))

            resolution = SilentIdentityResolution()
            if probe_channel is not None:
                from ..connection.callback_ledger import CallbackCausalityBusyError
                from .silent_scan_probe import (
                    async_run_automatic_framed_identity_window,
                )

                try:
                    discovered, resolution = (
                        await async_run_automatic_framed_identity_window(
                            probe_channel,
                            trigger=_send_batch,
                            identify_if=lambda replies: sum(
                                target is not None for target in replies
                            )
                            == 1,
                            lease_owner=f"onboarding_fallback_identity:{uuid.uuid4().hex}",
                            lease_timeout=max(
                                0.1,
                                timeout + max(0.0, identity_wait_seconds),
                            ),
                            identity_wait_seconds=identity_wait_seconds,
                        )
                    )
                except CallbackCausalityBusyError:
                    # This batch sent nothing; a later scan can retry it without
                    # misreporting an unidentified collector.
                    discovered = ()
            else:
                discovered = await _send_batch()

            responding = tuple(target for target in discovered if target is not None)
            if len(responding) == 1 and resolution.identified:
                responding = (
                    replace(
                        responding[0],
                        collector_pn=resolution.collector_pn,
                        observed_session=resolution,
                    ),
                )
            for target in responding:
                deduped[target.ip] = target
    finally:
        if probe_channel is not None:
            await probe_channel.async_close()
    return tuple(deduped.values())


class OnboardingDetector:
    """Run one-shot EyeBond collector discovery for setup flows."""

    def __init__(
        self,
        *,
        connection: EybondConnectionSpec | None = None,
        server_ip: str = "",
        tcp_port: int = DEFAULT_TCP_PORT,
        udp_port: int = DEFAULT_UDP_PORT,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self._connection = connection or EybondConnectionSpec(
            server_ip=server_ip,
            tcp_port=tcp_port,
            udp_port=udp_port,
            discovery_target=DEFAULT_DISCOVERY_TARGET,
            discovery_interval=30,
            heartbeat_interval=heartbeat_interval,
            request_timeout=request_timeout,
        )

    async def async_passive_detect(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_targets: Sequence[DiscoveryTarget] | None = None,
        settle_timeout: float = 0.1,
    ) -> tuple[OnboardingResult, ...]:
        """Materialize already-connected callback sessions without active probing."""

        targets = tuple(
            discovery_targets
            if discovery_targets is not None
            else build_default_discovery_targets(
                collector_ip=collector_ip,
                discovery_target=discovery_target,
            )
        )
        if not targets:
            targets = (
                DiscoveryTarget(
                    ip=discovery_target or collector_ip or DEFAULT_DISCOVERY_TARGET,
                    source="callback_listener",
                ),
            )
        listener = None
        try:
            listener = await _acquire_shared_listener(
                _LISTENER_BIND_HOST,
                self._connection.tcp_port,
            )
            if settle_timeout > 0:
                await asyncio.sleep(min(float(settle_timeout), 1.0))
            return self._session_inventory_results(
                listener=listener,
                discovery_targets=targets,
                results=(),
                passive_only=True,
                listener_port=self._connection.tcp_port,
            )
        except Exception as exc:
            logger.debug(
                "Passive callback discovery unavailable port=%s error=%s",
                self._connection.tcp_port,
                exc,
            )
            return ()
        finally:
            if listener is not None:
                await _release_shared_listener(listener)

    async def _async_detect_targets(
        self,
        targets: Sequence[DiscoveryTarget],
        *,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        cleanup_new_shared_connection: bool = False,
        total_timeout: float | None = None,
        concurrency: int = _TARGET_DETECTION_CONCURRENCY,
        skip_probe_ips: frozenset[str] = frozenset(),
        deadline: OnboardingDeadline | None = None,
    ) -> tuple[OnboardingResult, ...]:
        """Run one-shot detection against a list of discovery targets."""

        if deadline is None:
            deadline = OnboardingDeadline.from_timeout(total_timeout)
        semaphore = asyncio.Semaphore(max(1, int(concurrency)))
        results: list[OnboardingResult] = []
        task_states: dict[asyncio.Task[OnboardingResult], _TargetDetectionState] = {}

        async def _run_target(state: _TargetDetectionState) -> OnboardingResult:
            async with semaphore:
                remaining = deadline.remaining_seconds()
                if remaining is not None and remaining <= 0:
                    return self._timeout_result_for_state(state)
                try:
                    return await deadline.wait_for(
                        self._async_detect_target(
                            state.target,
                            discovery_timeout=discovery_timeout,
                            connect_timeout=connect_timeout,
                            heartbeat_timeout=heartbeat_timeout,
                            cleanup_new_shared_connection=cleanup_new_shared_connection,
                            detection_state=state,
                            deadline=deadline,
                        )
                    )
                except TimeoutError:
                    return self._timeout_result_for_state(state)
                except Exception as exc:
                    target = state.target
                    logger.warning(
                        "Onboarding detection failed target=%s source=%s error=%s",
                        target.ip,
                        target.source,
                        exc,
                    )
                    return _with_detection_evidence(
                        OnboardingResult(
                            collector=CollectorCandidate(target_ip=target.ip, source=target.source, ip=target.ip),
                            connection_type=CONNECTION_TYPE_EYBOND,
                            connection_mode=target.source,
                            next_action="manual_input",
                            last_error=str(exc),
                        ),
                        status="error",
                        reason=str(exc),
                    )

        for target in targets:
            if target.ip and target.ip in skip_probe_ips:
                results.append(_already_configured_result(target))
                continue
            state = _TargetDetectionState(target=target)
            task = asyncio.create_task(_run_target(state), name=f"eybond_detect_{target.ip}")
            task_states[task] = state

        pending = set(task_states)
        while pending:
            remaining = deadline.remaining_seconds()
            if remaining is not None and remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending,
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                # Re-snapshot the fixed shared deadline; the top-of-loop guard
                # is the single terminator for unfinished inventory targets.
                continue
            for task in done:
                result = task.result()
                results.append(result)

        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in pending:
                state = task_states[task]
                results.append(self._timeout_result_for_state(state))

        return tuple(self._dedupe_results(results))

    async def async_scan(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = DEFAULT_DISCOVERY_TARGET,
        discovery_targets: Sequence[DiscoveryTarget] | None = None,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        total_timeout: float | None = None,
        skip_probe_ips: frozenset[str] = frozenset(),
        deadline: OnboardingDeadline | None = None,
    ) -> tuple[OnboardingResult, ...]:
        """Return one bounded inventory of collectors on the local /24.

        The scan has one product meaning and one execution order: broadcast,
        already-observed callback sessions, concrete routes learned from that
        fan-out, then a bounded direct-unicast fallback across the local /24.
        It always finishes the inventory instead of stopping after the first
        identity. Inverter protocols are runtime work after entry creation.
        """

        if deadline is None:
            deadline = OnboardingDeadline.from_timeout(total_timeout)
        targets = tuple(
            discovery_targets
            or build_default_discovery_targets(
                collector_ip=collector_ip,
                discovery_target=discovery_target,
            )
        )
        listener = None
        if any(target.source == "broadcast" for target in targets):
            try:
                listener = await _acquire_shared_listener(
                    _LISTENER_BIND_HOST,
                    self._connection.tcp_port,
                )
            except Exception as exc:
                logger.debug(
                    "Collector-scan fan-out listener unavailable host=%s port=%s error=%s",
                    _LISTENER_BIND_HOST,
                    self._connection.tcp_port,
                    exc,
                )
        aggregated: list[OnboardingResult] = []
        try:
            targets = await self._async_expand_broadcast_targets(
                targets,
                discovery_timeout=discovery_timeout,
                deadline=deadline,
            )

            fanout_targets = await self._async_wait_for_fanout_targets(
                listener=listener,
                discovery_targets=targets,
                results=aggregated,
                timeout=deadline.bounded_timeout(
                    min(connect_timeout, _BROADCAST_FANOUT_SETTLE_TIMEOUT)
                ),
            )
            targets = _dedupe_discovery_targets((*targets, *fanout_targets))

            detection_targets = _concrete_detection_targets(targets)
            if detection_targets:
                aggregated.extend(
                    await self._async_detect_targets(
                        detection_targets,
                        discovery_timeout=discovery_timeout,
                        connect_timeout=connect_timeout,
                        heartbeat_timeout=heartbeat_timeout,
                        total_timeout=deadline.remaining_seconds(),
                        skip_probe_ips=skip_probe_ips,
                        deadline=deadline,
                    )
                )

            late_fanout_targets = await self._async_wait_for_fanout_targets(
                listener=listener,
                discovery_targets=targets,
                results=aggregated,
                timeout=deadline.bounded_timeout(
                    min(connect_timeout, _BROADCAST_FANOUT_SETTLE_TIMEOUT)
                ),
            )
            late_fanout_targets = tuple(
                target
                for target in late_fanout_targets
                if target.ip not in {
                    known.ip for known in _concrete_detection_targets(targets)
                }
            )
            if late_fanout_targets:
                targets = _dedupe_discovery_targets(
                    (*targets, *late_fanout_targets)
                )
                aggregated.extend(
                    await self._async_detect_targets(
                        late_fanout_targets,
                        discovery_timeout=discovery_timeout,
                        connect_timeout=connect_timeout,
                        heartbeat_timeout=heartbeat_timeout,
                        total_timeout=deadline.remaining_seconds(),
                        skip_probe_ips=skip_probe_ips,
                        deadline=deadline,
                    )
                )

            aggregated.extend(
                self._session_inventory_results(
                    listener=listener,
                    discovery_targets=targets,
                    results=aggregated,
                    listener_port=self._connection.tcp_port,
                )
            )

            fallback_targets = await self._async_unicast_fallback_targets(
                resolved_targets=targets,
                results=self._dedupe_results(aggregated),
                discovery_timeout=discovery_timeout,
                deadline=deadline,
            )
            if fallback_targets:
                aggregated.extend(
                    await self._async_detect_targets(
                        fallback_targets,
                        discovery_timeout=discovery_timeout,
                        connect_timeout=connect_timeout,
                        heartbeat_timeout=heartbeat_timeout,
                        total_timeout=deadline.remaining_seconds(),
                        skip_probe_ips=skip_probe_ips,
                        deadline=deadline,
                    )
                )

            aggregated.extend(
                self._session_inventory_results(
                    listener=listener,
                    discovery_targets=targets,
                    results=aggregated,
                    listener_port=self._connection.tcp_port,
                )
            )
            return tuple(self._dedupe_results(aggregated))
        finally:
            if listener is not None:
                await _release_shared_listener(listener)

    async def _async_trigger_connect_identify(
        self,
        *,
        target: DiscoveryTarget,
        transport: Any,
        candidate: CollectorCandidate,
        discovery_timeout: float,
        connect_timeout: float,
    ) -> tuple[Any, bool, "SilentIdentityResolution", bool]:
        """Send THIS attempt's ONE callback trigger UNDER the exclusive callback
        causality lease, then identify the collector on its EXACT post-baseline
        session -- fully silent, weak (short framed heartbeat) or already-strong --
        by at most ONE framed FC=2 parameter-2 read.

        A collector that has ALREADY volunteered a STRONG identity
        (``fc2_parameter_2`` / ``at_dtupn``) is accepted by the exact-session
        selector on its EXACT post-baseline session and is NEVER re-probed. A
        collector that is fully silent, or that volunteered only a WEAK short
        framed-heartbeat PN, is UPGRADED on that SAME exact session id by at most
        ONE framed FC=2 parameter-2 read -- even when a stale same-peer-IP park
        makes ``_select_pending_socket`` ambiguous (the transport is then pinned to
        that session id; NO second trigger, NO peer-IP identity). Only
        post-baseline session observations are eligible; two of them are typed
        ambiguity (never a peer-IP / arrival-order pick).

        HONEST causality: the lease serialises only THIS process's competing
        callback sends. It does NOT exclude a delayed earlier callback, an
        external trigger or a self-initiated inbound connect, so a post-baseline
        session is merely one *observed inside an integration-serialised trigger
        window* -- the strong identity is proven by the exact-session FC=2 read
        (or an already-recorded strong source), not by the lease. This is NOT a
        RecoveryProof and NOT proof of any
        ``connection_strategy``; no RecoveryContract and no inbound/callback
        strategy is created here.

        Fail-closed: if another owner already holds the trigger lease, THIS
        attempt sends NOTHING (an out-of-lease send is refused by
        ``callback_send_scope``) and returns an honest not-connected outcome. The
        exact-session read is TCP evidence -- it does not depend on a UDP reply --
        while the connect budget stays bounded by the timeout policy. The wire is
        the attempt's own typed authority: nothing reads collector kind, cloud,
        hostname, peer IP, PN prefix or a persisted protocol, and no listener-wide
        owner is registered.

        Returns ``(probe, connected, silent_identity, lease_busy)`` -- ``lease_busy``
        is a DISTINCT signal (not not-connected) so the caller can surface a typed
        ``callback_causality_lease_busy`` outcome that a later scan attempt retries.
        """

        from ..collector.silent_session_probe import (
            SilentSessionIdentityProbeChannel,
        )
        from ..connection.callback_ledger import (
            CallbackCausalityBusyError,
            get_callback_trigger_ledger,
        )
        from .silent_scan_probe import (
            DEFAULT_SILENT_IDENTITY_WAIT_SECONDS,
            AutomaticFramedIdentityIntent,
            SilentIdentityResolution,
            async_resolve_silent_session_identity,
        )

        async def _send_trigger() -> Any:
            probe = await async_send_callback_trigger(
                bind_ip=self._connection.server_ip,
                advertised_server_ip=self._connection.effective_advertised_server_ip,
                advertised_server_port=self._connection.effective_advertised_tcp_port,
                target_ip=target.ip,
                udp_port=self._connection.udp_port,
                timeout=discovery_timeout,
                source="onboarding_detect_probe",
            )
            # A later retry may legitimately receive no datagram even though
            # the fallback sweep already proved this exact route. Enrichment is
            # monotonic: replace earlier wire evidence only with a new reply,
            # never erase it with an empty retry result.
            if probe.reply:
                candidate.udp_reply = probe.reply
                candidate.udp_reply_from = probe.reply_from
            # ``target.ip`` is the route this addressed attempt exercised.
            # Preserve it verbatim.  UDP reply source and TCP peer are observed
            # diagnostics only; either can be a router/NAT address and neither
            # is authority to retarget the next callback attempt.
            candidate.ip = target.ip
            transport.set_collector_ip(target.ip)
            return probe

        def _effective_connect_timeout(probe: Any) -> float:
            if not probe.reply and target.source != "known_ip":
                return min(connect_timeout, _CONNECT_TIMEOUT_WITHOUT_UDP_REPLY)
            return connect_timeout

        supports_silent_identity = callable(
            getattr(transport, "observed_collector_sessions", None)
        ) and callable(getattr(transport, "set_claimed_session_provider", None))
        if not supports_silent_identity:
            # A transport without the session-inventory facade cannot support
            # exact-session identity (nothing to snapshot, nothing to pin). Take
            # the unchanged normal path.
            probe = await _send_trigger()
            connected = await transport.wait_until_connected(
                _effective_connect_timeout(probe)
            )
            return probe, connected, SilentIdentityResolution(), False

        probe_channel = SilentSessionIdentityProbeChannel(
            host=_LISTENER_BIND_HOST, port=self._connection.tcp_port
        )
        await probe_channel.async_open()
        ledger = get_callback_trigger_ledger()
        owner = f"onboarding_silent_identity:{uuid.uuid4().hex}"
        silent_identity = SilentIdentityResolution()
        connected = False
        lease_busy = False
        probe: Any = None
        try:
            try:
                # ONE framed attempt, ONE trigger, all UNDER the exclusive lease.
                # A busy lease is bounded by the connect budget so it fails fast.
                async with ledger.causality_lease(
                    owner, timeout=max(0.1, connect_timeout)
                ):
                    # ONE union-view baseline (PN-less pending AND already-identified
                    # sessions, by session id) BEFORE the trigger; only post-baseline
                    # session ids are eligible.
                    baseline = frozenset(
                        obs.session_id
                        for obs in probe_channel.snapshot_session_observations()
                    )
                    probe = await _send_trigger()
                    effective = _effective_connect_timeout(probe)
                    # Identity on the EXACT post-baseline session -- silent, weak
                    # (short framed heartbeat) or already-strong -- never a peer-IP
                    # claim (which would grab the stale same-peer park). TCP evidence
                    # that does NOT depend on a UDP reply; bounded by the connect
                    # budget so a deliberately slow reverse dial-in still times out.
                    if probe_channel.available:
                        loop = asyncio.get_running_loop()
                        silent_identity = await async_resolve_silent_session_identity(
                            probe_channel,
                            wire_intent=AutomaticFramedIdentityIntent(),
                            baseline=baseline,
                            deadline=(
                                loop.time()
                                + min(DEFAULT_SILENT_IDENTITY_WAIT_SECONDS, effective)
                            ),
                        )
                    if silent_identity.identified:
                        _sid = silent_identity.session_id
                        # Pin the transport to the EXACT identified session id so
                        # the connect + driver sweep proceed on it -- never a
                        # peer-IP pick, NO second trigger.
                        transport.set_claimed_session_provider(lambda: _sid)
                    connected = await transport.wait_until_connected(effective)
            except CallbackCausalityBusyError:
                # LEASE BUSY -- distinct from not-connected. Another owner holds the
                # trigger lease, so THIS attempt sends NOTHING (an out-of-lease send
                # is refused by callback_send_scope) and signals a typed lease-busy
                # outcome so a later scan attempt can retry on a clean window. Zero
                # callback sends for this target; the raw lease error never escapes.
                lease_busy = True
                probe = DiscoveryProbeResult(
                    target_ip=target.ip, message="", local_port=0
                )
                connected = False
        finally:
            await probe_channel.async_close()
        return probe, connected, silent_identity, lease_busy

    async def _async_detect_target(
        self,
        target: DiscoveryTarget,
        *,
        discovery_timeout: float,
        connect_timeout: float,
        heartbeat_timeout: float,
        cleanup_new_shared_connection: bool = False,
        detection_state: _TargetDetectionState | None = None,
        deadline: OnboardingDeadline | None = None,
    ) -> OnboardingResult:
        transport_kwargs: dict[str, Any] = {
            "host": _LISTENER_BIND_HOST,
            "port": self._connection.tcp_port,
            "request_timeout": self._connection.request_timeout,
            "heartbeat_interval": float(self._connection.heartbeat_interval),
            "collector_ip": target.ip,
        }
        if target.collector_pn:
            transport_kwargs["collector_pn"] = target.collector_pn
        # Onboarding never passes a session protocol: an inferred/expected hint
        # must not register a durable confirmed owner. The transport observes the
        # collector's real wire passively; active-probe authority is the runtime's
        # validated confirmed evidence alone.
        transport = SharedEybondTransport(**transport_kwargs)
        observed_session = (
            target.observed_session
            if type(target.observed_session) is SilentIdentityResolution
            and target.observed_session.identified
            else None
        )
        preidentified_session_id = (
            observed_session.session_id if observed_session is not None else ""
        )
        preidentified_pn = (
            observed_session.collector_pn if observed_session is not None else ""
        )
        preidentified_source = (
            observed_session.identity_source if observed_session is not None else ""
        )
        preidentified_protocol_shape = (
            observed_session.protocol_shape if observed_session is not None else ""
        )
        preidentified = observed_session is not None
        if preidentified:
            # The broadcast expansion already proved this exact socket inside
            # the first trigger window. Pin it before start/activation and do
            # NOT send a second set>server (collectors that answer rsp=2 keep the
            # first socket and would otherwise be excluded by a later baseline).
            transport.set_claimed_session_provider(
                lambda: preidentified_session_id
            )
        candidate = _collector_candidate_from_target(target)
        if detection_state is not None:
            detection_state.candidate = candidate

        existing_shared_connection = None
        if cleanup_new_shared_connection:
            existing_shared_connection = await transport.async_snapshot_shared_connection()
        preserve_observed_session_id = ""

        # start() is INSIDE the try so a start failure still runs the finally
        # (releases the shared listener): cleanup covers success / error / cancel
        # / timeout / start failure alike.
        try:
            await transport.start()
            if preidentified:
                probe = DiscoveryProbeResult(
                    target_ip=target.ip,
                    message="",
                    local_port=0,
                )
                silent_identity = SilentIdentityResolution(
                    session_id=preidentified_session_id,
                    collector_pn=preidentified_pn,
                    identity_source=preidentified_source,
                    protocol_shape=preidentified_protocol_shape,
                )
                lease_busy = False
                connected = await transport.wait_until_connected(connect_timeout)
            else:
                # ONE callback trigger, then EXACT post-baseline session identity
                # (a fully-silent, weak-heartbeat or already-strong collector the
                # peer-IP claim cannot pick is pinned by its exact session id).
                (
                    probe,
                    connected,
                    silent_identity,
                    lease_busy,
                ) = await self._async_trigger_connect_identify(
                    target=target,
                    transport=transport,
                    candidate=candidate,
                    discovery_timeout=discovery_timeout,
                    connect_timeout=connect_timeout,
                )
            if lease_busy:
                # A concurrent owner held the callback causality lease: nothing was
                # triggered and the collector was NOT checked. This is a distinct,
                # RETRYABLE condition -- never reported as collector_not_connected /
                # reverse_tcp_not_connected. Zero callback sends.
                return _with_detection_evidence(
                    OnboardingResult(
                        collector=candidate,
                        connection_type=CONNECTION_TYPE_EYBOND,
                        connection_mode=target.source,
                        next_action="manual_input",
                        last_error="callback_causality_lease_busy",
                    ),
                    status="callback_causality_lease_busy",
                    reason="callback_causality_lease_busy",
                )
            if not connected:
                warnings: list[str] = []
                if probe.reply:
                    warnings.append("collector_replied_but_no_reverse_tcp")
                return _with_detection_evidence(
                    OnboardingResult(
                        collector=candidate,
                        connection_type=CONNECTION_TYPE_EYBOND,
                        connection_mode=target.source,
                        warnings=tuple(warnings),
                        next_action="manual_input",
                        last_error="collector_not_connected",
                    ),
                    status="collector_not_connected",
                    reason="reverse_tcp_not_connected",
                )

            candidate.connected = True
            heartbeat_seen = await transport.wait_until_heartbeat(timeout=heartbeat_timeout)
            candidate.collector = transport.collector_info
            if candidate.collector.remote_ip and not silent_identity.identified:
                # A volunteering socket changes no route authority either.  Its
                # peer remains available as ``CollectorInfo.remote_ip`` only.
                # This is load-bearing for collectors behind NAT: using the peer
                # here makes the first scan succeed and every later callback fail.
                transport.set_collector_ip(target.ip)
            if silent_identity.identified and candidate.collector is not None:
                # Reconcile the exact-session FC=2 identity with whatever the
                # activated connection reports, through the ONE centralized short/
                # full-PN authority -- never a peer-IP or prefix decision here.
                from ..collector_identity import (
                    pn_is_same_identity,
                    prefer_full_pn,
                )

                current_pn = str(candidate.collector.collector_pn or "").strip()
                probed_pn = silent_identity.collector_pn
                if not current_pn:
                    # No volunteered PN: accept the strong probed identity.
                    candidate.collector = replace(
                        candidate.collector, collector_pn=probed_pn
                    )
                elif pn_is_same_identity(current_pn, probed_pn):
                    # Same collector, short vs full: keep the fuller spelling.
                    candidate.collector = replace(
                        candidate.collector,
                        collector_pn=prefer_full_pn(current_pn, probed_pn),
                    )
                else:
                    # The activated session reports a FOREIGN identity vs the
                    # exact-session read: the socket the transport ended up on is
                    # not the collector we identified. Fail closed -- no result, no
                    # claim, no handoff, no peer-IP identity decision.
                    logger.info(
                        "Silent identity probe read a PN the activated session "
                        "contradicts; failing closed (no identity adopted)"
                    )
                    # Adopt NO identity: clear the PN off the candidate so no
                    # result, claim or handoff carries the conflicting collector.
                    candidate.collector = replace(candidate.collector, collector_pn="")
                    return _with_detection_evidence(
                        OnboardingResult(
                            collector=candidate,
                            connection_type=CONNECTION_TYPE_EYBOND,
                            connection_mode=target.source,
                            next_action="manual_input",
                            last_error="collector_identity_mismatch",
                        ),
                        status="collector_identity_mismatch",
                        reason="probed_identity_conflicts_with_activated_session",
                    )

            admission_kwargs: dict[str, object] = {}
            if silent_identity.identified and candidate.collector is not None:
                collector_pn = str(candidate.collector.collector_pn or "").strip()
                protocol_shape = str(silent_identity.protocol_shape or "").strip()
                callback_route = CallbackRecoveryRoute(
                    bind_ip=self._connection.server_ip,
                    trigger_target_ip=target.ip,
                    trigger_udp_port=self._connection.udp_port,
                    advertised_ha_host=(
                        self._connection.effective_advertised_server_ip
                    ),
                    advertised_ha_port=(
                        self._connection.effective_advertised_tcp_port
                    ),
                    listener_port=self._connection.tcp_port,
                )
                if collector_pn and protocol_shape and not callback_route.invalid_reason():
                    admission_kwargs = {
                        "observed_session": ObservedCollectorSession(
                            collector_pn=collector_pn,
                            identity_source=silent_identity.identity_source,
                            session_id=silent_identity.session_id,
                            listener_port=self._connection.tcp_port,
                            protocol_shape=protocol_shape,
                            peer_hint=str(candidate.collector.remote_ip or "").strip(),
                        ),
                        "callback_route": callback_route,
                    }

            warnings = []
            if not heartbeat_seen:
                warnings.append("collector_heartbeat_not_observed")

            # Config-entry onboarding has one responsibility here: identify the
            # collector and preserve the exact callback-session/route capability
            # required by admission. Inverter probing belongs to runtime after
            # the entry owns the session.
            if admission_kwargs:
                preserve_observed_session_id = silent_identity.session_id
            return _with_detection_evidence(
                OnboardingResult(
                    collector=candidate,
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    warnings=tuple(warnings),
                    next_action="confirm_collector",
                    last_error="collector_detected_without_driver",
                    **admission_kwargs,
                ),
                status="collector_only",
                reason="collector_identity_only_scan",
            )

        finally:
            if cleanup_new_shared_connection:
                try:
                    await transport.async_disconnect_if_new_shared_connection(
                        existing_shared_connection
                    )
                except Exception as exc:
                    logger.debug(
                        "Onboarding shared-connection cleanup failed target=%s source=%s error=%s",
                        target.ip,
                        target.source,
                        exc,
                    )
            if preserve_observed_session_id:
                await transport.stop(
                    preserve_session_id=preserve_observed_session_id,
                )
            else:
                await transport.stop()

    async def _async_expand_broadcast_targets(
        self,
        targets: Sequence[DiscoveryTarget],
        *,
        discovery_timeout: float,
        deadline: OnboardingDeadline,
    ) -> tuple[DiscoveryTarget, ...]:
        expanded: list[DiscoveryTarget] = []
        known_ips: set[str] = set()

        for target in targets:
            if target.source != "broadcast":
                if target.ip not in known_ips:
                    known_ips.add(target.ip)
                    expanded.append(target)
                continue

            # The broadcast set>server is often the trigger that creates a
            # collector's ONLY callback socket. Some collectors answer a later
            # targeted set>server with rsp>server=2 and keep that first socket,
            # so taking the identity baseline only in _async_detect_target is one
            # phase too late. Capture and identify the exact session in THIS
            # trigger window, while holding the same process-wide causality
            # lease as every other callback-producing operation.
            from ..collector.silent_session_probe import (
                SilentSessionIdentityProbeChannel,
            )
            from ..connection.callback_ledger import (
                CallbackCausalityBusyError,
            )
            from .silent_scan_probe import (
                async_run_automatic_framed_identity_window,
            )

            resolution = SilentIdentityResolution()
            replies = ()
            probe_channel = SilentSessionIdentityProbeChannel(
                host=_LISTENER_BIND_HOST,
                port=self._connection.tcp_port,
            )
            await probe_channel.async_open()
            try:
                timeout = deadline.bounded_timeout(discovery_timeout)
                if timeout is None or timeout > 0:
                    owner = f"onboarding_broadcast_identity:{uuid.uuid4().hex}"
                    lease_timeout = deadline.bounded_timeout(
                        discovery_timeout + DEFAULT_SILENT_IDENTITY_WAIT_SECONDS
                    )
                    try:
                        async def _send_broadcast():
                            return await async_send_callback_trigger_replies(
                                bind_ip=self._connection.server_ip,
                                advertised_server_ip=self._connection.effective_advertised_server_ip,
                                advertised_server_port=self._connection.effective_advertised_tcp_port,
                                target_ip=target.ip,
                                udp_port=self._connection.udp_port,
                                timeout=timeout or discovery_timeout,
                                source="onboarding_broadcast_scan",
                            )

                        remaining = deadline.remaining_seconds()
                        wait_seconds = DEFAULT_SILENT_IDENTITY_WAIT_SECONDS
                        if remaining is not None:
                            wait_seconds = min(wait_seconds, max(0.0, remaining))
                        replies, resolution = (
                            await async_run_automatic_framed_identity_window(
                                probe_channel,
                                trigger=_send_broadcast,
                                identify_if=lambda observed_replies: len(
                                    {
                                        reply.reply_from.split(":", 1)[0]
                                        for reply in observed_replies
                                        if reply.reply_from
                                    }
                                )
                                == 1,
                                lease_owner=owner,
                                lease_timeout=(
                                    lease_timeout
                                    if lease_timeout is not None
                                    else discovery_timeout
                                    + DEFAULT_SILENT_IDENTITY_WAIT_SECONDS
                                ),
                                identity_wait_seconds=wait_seconds,
                            )
                        )
                    except CallbackCausalityBusyError:
                        # Another callback-producing operation owns the window;
                        # this scan sends nothing and may be retried normally.
                        replies = ()
                    except Exception as exc:
                        logger.debug(
                            "Broadcast discovery expansion failed target=%s error=%s",
                            target.ip,
                            exc,
                        )
                        replies = ()
            finally:
                await probe_channel.async_close()

            reply_ips = tuple(
                reply.reply_from.split(":", 1)[0]
                for reply in replies
                if reply.reply_from
            )
            if not reply_ips:
                if target.ip not in known_ips:
                    known_ips.add(target.ip)
                    expanded.append(target)
                continue

            for reply_ip in reply_ips:
                if reply_ip in known_ips:
                    continue
                known_ips.add(reply_ip)
                if len(set(reply_ips)) == 1 and resolution.identified:
                    expanded.append(
                        DiscoveryTarget(
                            ip=reply_ip,
                            source=target.source,
                            collector_pn=resolution.collector_pn,
                            observed_session=resolution,
                        )
                    )
                else:
                    expanded.append(DiscoveryTarget(ip=reply_ip, source=target.source))

        return tuple(expanded)

    async def _async_unicast_fallback_targets(
        self,
        *,
        resolved_targets: Sequence[DiscoveryTarget],
        results: Sequence[OnboardingResult],
        discovery_timeout: float,
        deadline: OnboardingDeadline,
    ) -> tuple[DiscoveryTarget, ...]:
        if not any(target.source == "broadcast" for target in resolved_targets):
            return ()

        timeout = deadline.bounded_timeout(min(discovery_timeout, _UNICAST_FALLBACK_PROBE_TIMEOUT))
        if timeout is not None and timeout <= 0:
            return ()

        replied_targets = await async_probe_fallback_targets(
            bind_ip=self._connection.server_ip,
            advertised_server_ip=self._connection.effective_advertised_server_ip,
            advertised_server_port=self._connection.effective_advertised_tcp_port,
            udp_port=self._connection.udp_port,
            targets=iter_unicast_fallback_targets(
                server_ip=self._connection.server_ip,
                collector_ip="",
            ),
            timeout=timeout or min(discovery_timeout, _UNICAST_FALLBACK_PROBE_TIMEOUT),
            identity_listener_host=_LISTENER_BIND_HOST,
            identity_listener_port=self._connection.tcp_port,
            identity_wait_seconds=DEFAULT_SILENT_IDENTITY_WAIT_SECONDS,
        )
        # Seeing an address earlier is not evidence that its addressed UDP
        # reply was preserved.  In particular, a causality-lease-busy target is
        # only a retryable placeholder.  Suppress a fallback target solely when
        # the exact route reply is already present; otherwise the later strong
        # observation must be allowed to enrich/replace the weak result.
        preserved_reply_ips = _observed_route_reply_ips(results)
        return tuple(
            target for target in replied_targets if target.ip not in preserved_reply_ips
        )

    async def _async_wait_for_fanout_targets(
        self,
        *,
        listener: Any,
        discovery_targets: Sequence[DiscoveryTarget],
        results: Sequence[OnboardingResult],
        timeout: float | None,
    ) -> tuple[DiscoveryTarget, ...]:
        if listener is None:
            return ()

        fanout_deadline = OnboardingDeadline.from_timeout(timeout)
        while True:
            fanout_targets = self._fanout_broadcast_targets(
                listener=listener,
                discovery_targets=discovery_targets,
                results=results,
            )
            if fanout_targets:
                return fanout_targets

            remaining = fanout_deadline.remaining_seconds()
            if remaining is not None and remaining <= 0:
                return ()
            await asyncio.sleep(
                min(
                    _BROADCAST_FANOUT_POLL_INTERVAL,
                    remaining if remaining is not None else _BROADCAST_FANOUT_POLL_INTERVAL,
                )
            )

    @staticmethod
    def _timeout_result_for_state(state: _TargetDetectionState) -> OnboardingResult:
        candidate = state.candidate
        if candidate is None:
            candidate = _collector_candidate_from_target(state.target)
            next_action = "manual_input"
        else:
            next_action = "manual_driver_selection" if candidate.connected else "manual_input"

        return _with_detection_evidence(
            OnboardingResult(
                collector=candidate,
                connection_type=CONNECTION_TYPE_EYBOND,
                connection_mode=state.target.source,
                next_action=next_action,
                last_error="target_detection_timeout",
            ),
            status="target_timeout",
            reason="deadline_exhausted",
            budget_exhausted=True,
        )

    @staticmethod
    def _fanout_broadcast_targets(
        *,
        listener: Any,
        discovery_targets: Sequence[DiscoveryTarget],
        results: Sequence[OnboardingResult],
    ) -> tuple[DiscoveryTarget, ...]:
        if listener is None:
            return ()

        known_ips = {
            result.collector.ip
            for result in results
            if result.collector is not None and result.collector.ip
        }
        known_ips.update(
            target.ip
            for target in discovery_targets
            if target.source != "broadcast"
        )

        fanout_targets: list[DiscoveryTarget] = []
        for target in discovery_targets:
            if target.source != "broadcast":
                continue
            for remote_ip in listener.matching_callback_ips(target.ip):
                if remote_ip in known_ips:
                    continue
                known_ips.add(remote_ip)
                fanout_targets.append(DiscoveryTarget(ip=remote_ip, source=target.source))
        return tuple(fanout_targets)

    @staticmethod
    def _session_inventory_results(
        *,
        listener: Any,
        discovery_targets: Sequence[DiscoveryTarget],
        results: Sequence[OnboardingResult],
        passive_only: bool = False,
        listener_port: int = 0,
    ) -> tuple[OnboardingResult, ...]:
        if listener is None:
            return ()

        inventory_provider = getattr(listener, "discovered_collector_sessions", None)
        if not callable(inventory_provider):
            return ()

        source_target = next(
            (target for target in discovery_targets if target.source == "broadcast"),
            None,
        )
        if source_target is None and discovery_targets:
            source_target = discovery_targets[0]
        if source_target is None:
            source_target = DiscoveryTarget(
                ip=DEFAULT_DISCOVERY_TARGET,
                source="callback_listener",
            )

        known_pns: set[str] = set()
        for result in results:
            collector = result.collector
            if collector is None:
                continue
            collector_info = collector.collector
            collector_pn = str(
                (collector_info.collector_pn if collector_info is not None else "")
                or (result.match.details.get("collector_pn", "") if result.match is not None else "")
            ).strip()
            if collector_pn:
                known_pns.add(collector_pn)

        try:
            sessions = tuple(inventory_provider())
        except Exception as exc:
            logger.debug("Failed to read onboarding callback session inventory: %s", exc)
            return ()

        materialized: list[OnboardingResult] = []
        for session in sessions:
            collector_pn = str(session.get("collector_pn") or "").strip()
            peer_ip = str(session.get("peer_ip") or "").strip()
            if (
                not collector_pn
                or not peer_ip
                or any(_collector_identity_matches(known_pn, collector_pn) for known_pn in known_pns)
            ):
                continue

            state = str(session.get("state") or "").strip()
            if state in {"route_identity_mismatch", "waiting_for_route_identity"}:
                continue

            peer_port_raw = session.get("peer_port")
            peer_port = peer_port_raw if isinstance(peer_port_raw, int) else None
            protocol_shape = str(session.get("protocol_shape") or "").strip().lower()
            collector_session_protocol = collector_session_protocol_from_inventory_state(
                state=state,
                protocol_shape=protocol_shape,
            )
            source = "callback_listener" if passive_only else source_target.source
            session_id = str(session.get("session_id") or "").strip()
            identity_source = str(
                session.get("collector_identity_source") or ""
            ).strip()
            # Project the EXACT physical session into a typed, immutable carrier.
            # This -- not the free-form details dict -- is what the config-flow
            # admission boundary trusts to restart/verify THIS session before a
            # passive callback candidate may become an inbound entry (PN/peer-IP
            # re-selection is not an equivalent proof).
            observed_session = (
                ObservedCollectorSession(
                    collector_pn=collector_pn,
                    identity_source=identity_source,
                    session_id=session_id,
                    listener_port=int(listener_port),
                    protocol_shape=protocol_shape,
                    peer_hint=peer_ip,
                )
                if session_id and int(listener_port) > 0
                else None
            )
            materialized.append(
                _with_detection_evidence(
                    OnboardingResult(
                        collector=CollectorCandidate(
                            target_ip=source_target.ip,
                            source=source,
                            ip=peer_ip,
                            session_protocol=collector_session_protocol,
                            connected=state in {"claimed", "routed_framed", "routed_at_text"},
                            collector=CollectorInfo(
                                remote_ip=peer_ip,
                                remote_port=peer_port,
                                collector_pn=collector_pn,
                            ),
                        ),
                        connection_type=CONNECTION_TYPE_EYBOND,
                        connection_mode=source,
                        next_action="manual_driver_selection",
                        last_error="collector_detected_without_driver",
                        observed_session=observed_session,
                    ),
                    status="collector_only",
                    reason="callback_session_inventory",
                    details={
                        # Diagnostics only -- retained for support bundles. The
                        # config flow must NOT read session authority from here;
                        # it uses the typed ``observed_session`` above.
                        "session_id": session_id,
                        "collector_identity_source": identity_source,
                        "session_state": state,
                        "collector_session_protocol": collector_session_protocol,
                        "collector_session_protocol_shape": protocol_shape,
                    },
                )
            )
            known_pns.add(collector_pn)

        return tuple(materialized)

    @staticmethod
    def _dedupe_results(results: Sequence[OnboardingResult]) -> list[OnboardingResult]:
        deduped: dict[str, OnboardingResult] = {}
        for result in results:
            collector_key = ""
            if result.collector is not None:
                collector_info = result.collector.collector
                collector_key = (
                    (collector_info.collector_pn if collector_info else "")
                    or result.collector.ip
                    or result.collector.target_ip
                )
            if not collector_key:
                collector_key = "unknown_target"

            key = collector_key
            for existing_key in deduped:
                if _collector_identity_matches(existing_key, collector_key):
                    key = existing_key
                    break
            existing = deduped.get(key)
            if existing is None or _result_priority(result) > _result_priority(existing):
                deduped[key] = result
        return sorted(
            deduped.values(),
            key=lambda result: (
                -_CONFIDENCE_SCORE.get(result.confidence, 0),
                result.collector.ip if result.collector else "",
                result.match.model_name if result.match else "",
            ),
        )


def _observed_route_reply_ips(results: Sequence[OnboardingResult]) -> set[str]:
    """Return exact attempted routes whose UDP replies survived projection."""

    return {
        collector.target_ip
        for result in results
        if (collector := result.collector) is not None
        and collector.target_ip
        and collector.udp_reply
    }


def _result_priority(result: OnboardingResult) -> tuple[int, int, int, int]:
    return (
        _CONFIDENCE_SCORE.get(result.confidence, 0),
        1 if result.match is not None else 0,
        1 if result.collector is not None and result.collector.connected else 0,
        1 if result.collector is not None and result.collector.udp_reply else 0,
    )
