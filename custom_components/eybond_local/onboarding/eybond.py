"""EyeBond-specific onboarding discovery built on top of generic driver detection."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from itertools import islice
import ipaddress
import logging
from dataclasses import dataclass, replace
from typing import Any, Sequence

from ..canonical_telemetry import apply_canonical_measurements
from ..collector.at_runtime import query_runtime_collector_at_values
from ..collector.capabilities import (
    collector_capability_profile,
    parse_esp_collector_hardware_token,
)
from ..collector.cloud_family import (
    apply_collector_cloud_family_observation,
    collector_cloud_family_observation_from_endpoint,
)
from ..collector.discovery import async_probe_target, async_probe_target_replies
from ..collector.parameter_registry import RUNTIME_COLLECTOR_PARAMETERS, query_runtime_collector_values
from ..collector.smartess_local import SmartEssLocalSession, SmartEssProtocolDescriptor
from ..collector.transport import (
    SharedCollectorAtTransport,
    SharedEybondTransport,
    _acquire_shared_listener,
    _release_shared_listener,
)
from ..connection.models import EybondConnectionSpec
from ..const import (
    CONNECTION_TYPE_EYBOND,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DRIVER_HINT_AUTO,
)
from ..metadata.smartess_protocol_catalog_loader import SmartEssProtocolCatalogEntry, load_smartess_protocol_catalog
from .link_sweep import (
    async_run_link_baud_sweep,
    catalog_link_baud_hints,
    driver_keys_for_link_baud,
    is_silent_detection_error,
    parse_reported_baud,
)
from .driver_detection import (
    DetectedDriverContext,
    DriverCandidateScan,
    async_detect_inverter,
    async_detect_inverter_candidates,
    driver_keys_for_profile_prefixes,
)
from .timeouts import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    ExtendableOnboardingDeadline,
    OnboardingDeadline,
    default_deep_driver_sweep_seconds,
)
from ..models import CollectorCandidate, CollectorInfo, OnboardingResult, TargetDetectionEvidence

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
DETECTION_DEPTH_FAST = "fast"
DETECTION_DEPTH_DEEP = "deep"
_DEEP_SWEEP_FINALIZE_MARGIN = 10.0
_ONBOARDING_RUNTIME_DETAIL_KEYS = {
    "battery_connected",
    "battery_connection_state",
    "battery_percent",
    "collector_pn",
    "collector_signal_strength",
    "collector_signal_strength_raw",
    "collector_signal_strength_source",
    "collector_server_endpoint",
    "collector_cloud_family",
    "collector_cloud_family_source",
    "collector_cloud_family_confidence",
    "output_rating_active_power",
    "rated_power",
    # Virtual-bridge verdict carried from FC=2 parameter 6
    # (collector_hardware_version="esp-collector/<version>/<platform...>") so
    # the confirm step can gate the collector operation-mode UI before the
    # entry exists.
    "collector_virtual_bridge",
    "collector_bridge_kind",
    "collector_bridge_version",
}

_ONBOARDING_RUNTIME_COLLECTOR_PARAMETERS = tuple(
    definition
    for definition in RUNTIME_COLLECTOR_PARAMETERS
    if definition.parameter != 41
)


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


def _apply_bridge_hardware_token_to_collector(collector: Any, hardware_version: object) -> None:
    """Carry a positive hardware-version bridge token into CollectorInfo."""

    token = parse_esp_collector_hardware_token(hardware_version)
    if collector is None or not token.is_bridge:
        return
    collector.collector_virtual_bridge = True
    collector.collector_bridge_kind = "esp-collector"
    if token.version:
        collector.collector_bridge_version = token.version


def _apply_collector_cloud_endpoint_details_to_collector(
    collector: Any,
    details: dict[str, object],
) -> None:
    """Carry an observed CLDSRVHOST1 endpoint/family verdict into CollectorInfo."""

    if collector is None:
        return
    endpoint = str(details.get("collector_server_endpoint") or "").strip()
    if endpoint:
        collector.collector_server_endpoint = endpoint
        apply_collector_cloud_family_observation(
            collector,
            collector_cloud_family_observation_from_endpoint(endpoint),
        )
    family = str(details.get("collector_cloud_family") or "").strip()
    if family:
        collector.collector_cloud_family = family
        collector.collector_cloud_family_source = str(
            details.get("collector_cloud_family_source") or ""
        ).strip()
        collector.collector_cloud_family_confidence = str(
            details.get("collector_cloud_family_confidence") or ""
        ).strip()


@dataclass(frozen=True, slots=True)
class DiscoveryTarget:
    """One onboarding discovery target."""

    ip: str
    source: str


@dataclass(frozen=True, slots=True)
class SmartEssOnboardingProbe:
    """Best-effort SmartESS collector metadata captured during onboarding."""

    collector_version: str = ""
    protocol_descriptor: SmartEssProtocolDescriptor | None = None
    known_protocol: SmartEssProtocolCatalogEntry | None = None

    @property
    def selected_device_address(self) -> int | None:
        if self.known_protocol is None or not self.known_protocol.device_addresses:
            return None
        return self.known_protocol.device_addresses[0]


def _smartess_preferred_driver_keys(
    smartess_probe: SmartEssOnboardingProbe | None,
) -> tuple[str, ...]:
    """Derive the driver probe-order hint from SmartESS collector metadata."""

    if smartess_probe is None or smartess_probe.known_protocol is None:
        return ()
    known = smartess_probe.known_protocol
    return driver_keys_for_profile_prefixes(
        (known.profile_name, known.raw_profile_name)
    )


def _already_configured_result(target: DiscoveryTarget, depth: str) -> OnboardingResult:
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
        depth=depth,
        status="already_configured",
        reason="configured_entry_owns_collector",
    )


@dataclass(slots=True)
class _TargetDetectionState:
    target: DiscoveryTarget
    depth: str = DETECTION_DEPTH_FAST
    candidate: CollectorCandidate | None = None


def _with_detection_evidence(
    result: OnboardingResult,
    *,
    depth: str,
    status: str,
    reason: str = "",
    budget_exhausted: bool = False,
    details: dict[str, Any] | None = None,
) -> OnboardingResult:
    """Attach structured target-detection evidence to one onboarding result."""

    return replace(
        result,
        detection=TargetDetectionEvidence(
            depth=depth,
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
    network_cidr: str = "",
) -> tuple[DiscoveryTarget, ...]:
    """Build one unicast sweep target list for broadcast-unfriendly networks."""

    return tuple(
        iter_unicast_fallback_targets(
            server_ip=server_ip,
            collector_ip=collector_ip,
            network_cidr=network_cidr,
        )
    )


def iter_unicast_fallback_targets(
    *,
    server_ip: str,
    collector_ip: str = "",
    network_cidr: str = "",
):
    """Yield one unicast sweep target list for the selected IPv4 network."""

    if collector_ip:
        return

    try:
        network = ipaddress.ip_network(network_cidr or f"{server_ip}/24", strict=False)
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
) -> tuple[DiscoveryTarget, ...]:
    """Probe one list of direct unicast targets concurrently and keep responders only."""

    async def _probe(target: DiscoveryTarget) -> DiscoveryTarget | None:
        try:
            probe = await async_probe_target(
                bind_ip=bind_ip,
                advertised_server_ip=advertised_server_ip,
                advertised_server_port=advertised_server_port,
                target_ip=target.ip,
                udp_port=udp_port,
                timeout=timeout,
            )
        except Exception as exc:
            logger.debug("Fallback unicast probe failed target=%s error=%s", target.ip, exc)
            return None

        if not probe.reply:
            return None

        responder_ip = probe.reply_from.split(":", 1)[0] if probe.reply_from else target.ip
        return DiscoveryTarget(ip=responder_ip, source=target.source)

    iterator = iter(targets)
    deduped: dict[str, DiscoveryTarget] = {}
    batch_size = max(1, concurrency)
    while True:
        batch = tuple(islice(iterator, batch_size))
        if not batch:
            break
        discovered = await asyncio.gather(*(_probe(target) for target in batch))
        for target in discovered:
            if target is None:
                continue
            deduped[target.ip] = target
    return tuple(deduped.values())


class OnboardingDetector:
    """Run one-shot EyeBond collector discovery and driver probing for setup flows."""

    def __init__(
        self,
        *,
        connection: EybondConnectionSpec | None = None,
        server_ip: str = "",
        tcp_port: int = DEFAULT_TCP_PORT,
        udp_port: int = DEFAULT_UDP_PORT,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        driver_hint: str = DRIVER_HINT_AUTO,
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
        self._driver_hint = driver_hint

    async def async_detect_targets(
        self,
        targets: Sequence[DiscoveryTarget],
        *,
        depth: str = DETECTION_DEPTH_FAST,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        enrich_runtime_details: bool = True,
        cleanup_new_shared_connection: bool = False,
        total_timeout: float | None = None,
        concurrency: int = _TARGET_DETECTION_CONCURRENCY,
        return_after_first_match: bool = False,
        skip_probe_ips: frozenset[str] = frozenset(),
        deadline: OnboardingDeadline | ExtendableOnboardingDeadline | None = None,
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
                            enrich_runtime_details=enrich_runtime_details,
                            cleanup_new_shared_connection=cleanup_new_shared_connection,
                            detection_state=state,
                            depth=state.depth,
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
                        depth=state.depth,
                        status="error",
                        reason=str(exc),
                    )

        for target in targets:
            if target.ip and target.ip in skip_probe_ips:
                results.append(_already_configured_result(target, depth))
                continue
            state = _TargetDetectionState(target=target, depth=depth)
            task = asyncio.create_task(_run_target(state), name=f"eybond_detect_{target.ip}")
            task_states[task] = state

        pending = set(task_states)
        stopped_after_first_match = False
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
                # The wait timed out against a STALE snapshot of the deadline:
                # a target admitted mid-wait may have extended it. Loop and
                # re-snapshot — the top-of-loop guard is the real terminator.
                continue
            should_stop = False
            for task in done:
                result = task.result()
                results.append(result)
                if return_after_first_match and result.match is not None:
                    should_stop = True
            if should_stop:
                stopped_after_first_match = True
                break

        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in pending:
                state = task_states[task]
                if stopped_after_first_match:
                    # Deliberately cancelled because another target already
                    # matched — not a timeout, and it must not read like one.
                    results.append(self._cancelled_after_match_result_for_state(state))
                else:
                    results.append(self._timeout_result_for_state(state))

        return tuple(self._dedupe_results(results))

    async def async_auto_detect(
        self,
        *,
        depth: str = DETECTION_DEPTH_FAST,
        collector_ip: str = "",
        discovery_target: str = DEFAULT_DISCOVERY_TARGET,
        discovery_targets: Sequence[DiscoveryTarget] | None = None,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
        enrich_runtime_details: bool = True,
        total_timeout: float | None = None,
        return_after_first_match: bool = True,
        skip_probe_ips: frozenset[str] = frozenset(),
        deadline: OnboardingDeadline | ExtendableOnboardingDeadline | None = None,
    ) -> tuple[OnboardingResult, ...]:
        """Run the default EyeBond onboarding discovery order."""

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
                    "Quick-scan fan-out listener unavailable host=%s port=%s error=%s",
                    _LISTENER_BIND_HOST,
                    self._connection.tcp_port,
                    exc,
                )
        aggregated: list[OnboardingResult] = []
        try:
            for attempt_index in range(max(1, attempts)):
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
                    results = await self.async_detect_targets(
                        detection_targets,
                        depth=depth,
                        discovery_timeout=discovery_timeout,
                        connect_timeout=connect_timeout,
                        heartbeat_timeout=heartbeat_timeout,
                        enrich_runtime_details=enrich_runtime_details,
                        total_timeout=deadline.remaining_seconds(),
                        return_after_first_match=return_after_first_match,
                        skip_probe_ips=skip_probe_ips,
                        deadline=deadline,
                    )
                    aggregated.extend(results)

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
                    if target.ip not in {known.ip for known in _concrete_detection_targets(targets)}
                )
                if late_fanout_targets:
                    targets = _dedupe_discovery_targets((*targets, *late_fanout_targets))
                    aggregated.extend(
                        await self.async_detect_targets(
                            late_fanout_targets,
                            depth=depth,
                            discovery_timeout=discovery_timeout,
                            connect_timeout=connect_timeout,
                            heartbeat_timeout=heartbeat_timeout,
                            enrich_runtime_details=enrich_runtime_details,
                            total_timeout=deadline.remaining_seconds(),
                            return_after_first_match=return_after_first_match,
                            skip_probe_ips=skip_probe_ips,
                            deadline=deadline,
                        )
                    )

                aggregated.extend(
                    self._session_inventory_results(
                        listener=listener,
                        discovery_targets=targets,
                        results=aggregated,
                        depth=depth,
                    )
                )

                deduped = self._dedupe_results(aggregated)
                best = deduped[0] if deduped else None
                if best is not None and best.match is not None:
                    aggregated = list(deduped)
                    break
                if attempt_index < max(1, attempts) - 1:
                    await deadline.sleep(attempt_delay)

            deduped = self._dedupe_results(aggregated)
            best = deduped[0] if deduped else None
            if best is None or best.match is None:
                fallback_targets = await self._async_auto_unicast_fallback_targets(
                    resolved_targets=targets,
                    results=deduped,
                    discovery_timeout=discovery_timeout,
                    deadline=deadline,
                )
                if fallback_targets:
                    aggregated.extend(
                        await self.async_detect_targets(
                            fallback_targets,
                            depth=depth,
                            discovery_timeout=discovery_timeout,
                            connect_timeout=connect_timeout,
                            heartbeat_timeout=heartbeat_timeout,
                            enrich_runtime_details=enrich_runtime_details,
                            total_timeout=deadline.remaining_seconds(),
                            return_after_first_match=return_after_first_match,
                            skip_probe_ips=skip_probe_ips,
                            deadline=deadline,
                        )
                    )
                    deduped = self._dedupe_results(aggregated)
            aggregated.extend(
                self._session_inventory_results(
                    listener=listener,
                    discovery_targets=targets,
                    results=aggregated,
                    depth=depth,
                )
            )
            deduped = self._dedupe_results(aggregated)
            return tuple(deduped)
        finally:
            if listener is not None:
                await _release_shared_listener(listener)

    async def async_handoff_detect(
        self,
        *,
        collector_ip: str,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
        enrich_runtime_details: bool = True,
        cleanup_new_shared_connection: bool = False,
    ) -> OnboardingResult | None:
        """Retry direct known-IP detection for post-provisioning handoff.

        This keeps the BLE handoff path narrow: it probes only the collector IP that
        just received Wi-Fi credentials and does not reopen broadcast discovery.
        """

        if not str(collector_ip or "").strip():
            raise ValueError("collector_ip_required")

        targets = build_default_discovery_targets(
            collector_ip=collector_ip,
            discovery_target="",
        )
        aggregated: list[OnboardingResult] = []

        for attempt_index in range(max(1, attempts)):
            results = await self.async_detect_targets(
                targets,
                discovery_timeout=discovery_timeout,
                connect_timeout=connect_timeout,
                heartbeat_timeout=heartbeat_timeout,
                enrich_runtime_details=enrich_runtime_details,
                cleanup_new_shared_connection=cleanup_new_shared_connection,
            )
            aggregated.extend(results)
            deduped = self._dedupe_results(aggregated)
            best = deduped[0] if deduped else None
            if best is not None and best.match is not None:
                return best
            if attempt_index < max(1, attempts) - 1:
                await asyncio.sleep(attempt_delay)

        deduped = self._dedupe_results(aggregated)
        return deduped[0] if deduped else None

    async def async_deep_detect(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = DEFAULT_DISCOVERY_TARGET,
        discovery_targets: Sequence[DiscoveryTarget] | None = None,
        unicast_network_cidr: str = "",
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
        enrich_runtime_details: bool = True,
        total_timeout: float | None = None,
        skip_probe_ips: frozenset[str] = frozenset(),
    ) -> tuple[OnboardingResult, ...]:
        """Run broadcast discovery first, then sweep the full selected IPv4 network."""

        # The scan budget must follow the discovered work: every connected
        # collector admitted for identification extends this deadline by one
        # full driver-sweep budget (bounded by the policy hard ceiling), so a
        # site with many inverters does not starve the late ones.
        deadline = ExtendableOnboardingDeadline(
            base_timeout_seconds=total_timeout,
            # Admission headroom sits ON TOP of the discovery budget: a /16
            # sweep legitimately spends ~14 minutes on discovery alone and
            # must not eat the identification budget.
            hard_ceiling_seconds=(
                float(total_timeout or 0.0)
                + DEFAULT_ONBOARDING_TIMEOUT_POLICY.deep_scan_hard_ceiling_seconds
            ),
        )
        resolved_targets = tuple(
            discovery_targets
            or build_default_discovery_targets(
                collector_ip=collector_ip,
                discovery_target=discovery_target,
            )
        )
        aggregated = list(
            await self.async_auto_detect(
                depth=DETECTION_DEPTH_DEEP,
                collector_ip=collector_ip,
                discovery_target=discovery_target,
                discovery_targets=resolved_targets,
                discovery_timeout=discovery_timeout,
                connect_timeout=connect_timeout,
                heartbeat_timeout=heartbeat_timeout,
                attempts=attempts,
                attempt_delay=attempt_delay,
                enrich_runtime_details=enrich_runtime_details,
                total_timeout=deadline.remaining_seconds(),
                return_after_first_match=False,
                skip_probe_ips=skip_probe_ips,
                deadline=deadline,
            )
        )

        listener = None
        try:
            listener = await _acquire_shared_listener(
                _LISTENER_BIND_HOST,
                self._connection.tcp_port,
            )
        except Exception as exc:
            logger.debug(
                "Deep-scan fallback listener unavailable host=%s port=%s error=%s",
                _LISTENER_BIND_HOST,
                self._connection.tcp_port,
                exc,
            )
        try:
            fallback_timeout = deadline.bounded_timeout(
                min(discovery_timeout, _UNICAST_FALLBACK_PROBE_TIMEOUT)
            )
            if fallback_timeout is not None and fallback_timeout <= 0:
                replied_targets = ()
            else:
                replied_targets = await async_probe_fallback_targets(
                    bind_ip=self._connection.server_ip,
                    advertised_server_ip=self._connection.effective_advertised_server_ip,
                    advertised_server_port=self._connection.effective_advertised_tcp_port,
                    udp_port=self._connection.udp_port,
                    targets=iter_unicast_fallback_targets(
                        server_ip=self._connection.server_ip,
                        collector_ip=collector_ip,
                        network_cidr=unicast_network_cidr,
                    ),
                    timeout=fallback_timeout or min(discovery_timeout, _UNICAST_FALLBACK_PROBE_TIMEOUT),
                )
        finally:
            if listener is not None:
                await _release_shared_listener(listener)
        if not replied_targets:
            return tuple(self._dedupe_results(aggregated))

        known_ips = {
            result.collector.ip
            for result in aggregated
            if result.collector is not None and result.collector.ip
        }
        known_ips.update(target.ip for target in resolved_targets)
        replied_targets = tuple(target for target in replied_targets if target.ip not in known_ips)
        if not replied_targets:
            return tuple(self._dedupe_results(aggregated))

        fallback_results = await self.async_detect_targets(
            replied_targets,
            depth=DETECTION_DEPTH_DEEP,
            discovery_timeout=discovery_timeout,
            connect_timeout=connect_timeout,
            heartbeat_timeout=heartbeat_timeout,
            enrich_runtime_details=enrich_runtime_details,
            total_timeout=deadline.remaining_seconds(),
            return_after_first_match=False,
            skip_probe_ips=skip_probe_ips,
            deadline=deadline,
        )
        aggregated.extend(fallback_results)
        return tuple(self._dedupe_results(aggregated))

    async def _async_detect_target(
        self,
        target: DiscoveryTarget,
        *,
        depth: str = DETECTION_DEPTH_FAST,
        discovery_timeout: float,
        connect_timeout: float,
        heartbeat_timeout: float,
        enrich_runtime_details: bool = True,
        cleanup_new_shared_connection: bool = False,
        detection_state: _TargetDetectionState | None = None,
        deadline: OnboardingDeadline | None = None,
    ) -> OnboardingResult:
        transport = SharedEybondTransport(
            host=_LISTENER_BIND_HOST,
            port=self._connection.tcp_port,
            request_timeout=self._connection.request_timeout,
            heartbeat_interval=float(self._connection.heartbeat_interval),
            collector_ip=target.ip,
        )
        candidate = CollectorCandidate(
            target_ip=target.ip,
            source=target.source,
            ip=target.ip,
        )
        if detection_state is not None:
            detection_state.candidate = candidate

        existing_shared_connection = None
        if cleanup_new_shared_connection:
            existing_shared_connection = await transport.async_snapshot_shared_connection()

        await transport.start()
        try:
            probe = await async_probe_target(
                bind_ip=self._connection.server_ip,
                advertised_server_ip=self._connection.effective_advertised_server_ip,
                advertised_server_port=self._connection.effective_advertised_tcp_port,
                target_ip=target.ip,
                udp_port=self._connection.udp_port,
                timeout=discovery_timeout,
            )
            candidate.udp_reply = probe.reply
            candidate.udp_reply_from = probe.reply_from
            if probe.reply_from:
                candidate.ip = probe.reply_from.split(":", 1)[0]
                transport.set_collector_ip(candidate.ip)

            effective_connect_timeout = connect_timeout
            if not probe.reply and target.source != "known_ip":
                effective_connect_timeout = min(connect_timeout, _CONNECT_TIMEOUT_WITHOUT_UDP_REPLY)
            connected = await transport.wait_until_connected(timeout=effective_connect_timeout)
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
                    depth=depth,
                    status="collector_not_connected",
                    reason="reverse_tcp_not_connected",
                )

            candidate.connected = True
            heartbeat_seen = await transport.wait_until_heartbeat(timeout=heartbeat_timeout)
            candidate.collector = transport.collector_info
            if candidate.collector.remote_ip:
                candidate.ip = candidate.collector.remote_ip
                transport.set_collector_ip(candidate.ip)

            smartess_probe = await _async_probe_smartess_onboarding(transport)
            if candidate.collector is not None and smartess_probe is not None:
                _apply_smartess_probe_to_collector(candidate.collector, smartess_probe)

            warnings = []
            if not heartbeat_seen:
                warnings.append("collector_heartbeat_not_observed")

            sweep_deadline = deadline
            if depth == DETECTION_DEPTH_DEEP:
                # Each connected target gets its own sweep budget so one slow
                # target cannot starve the identification of another. On an
                # extendable scan deadline this ADMITS the collector: the
                # shared budget grows by one full sweep (up to the hard
                # ceiling), so ten connected collectors get ten sweeps instead
                # of starving on a budget sized for one.
                sweep_budget = default_deep_driver_sweep_seconds()
                ensure_remaining = getattr(deadline, "ensure_remaining", None)
                if callable(ensure_remaining):
                    ensure_remaining(sweep_budget + _DEEP_SWEEP_FINALIZE_MARGIN)
                sweep_deadline = (
                    deadline.nested(sweep_budget)
                    if deadline is not None
                    else OnboardingDeadline.from_timeout(sweep_budget)
                )
            sweep_outcome = None
            try:
                scan = await self._async_detect_driver_with_retries(
                    transport,
                    depth=depth,
                    deadline=sweep_deadline,
                    preferred_driver_keys=_smartess_preferred_driver_keys(smartess_probe),
                )
            except TimeoutError:
                if candidate.collector is not None:
                    await self._async_enrich_collector_bridge_details(
                        transport,
                        candidate.collector,
                        collector_ip=candidate.ip,
                    )
                return _with_detection_evidence(
                    OnboardingResult(
                        collector=candidate,
                        connection_type=CONNECTION_TYPE_EYBOND,
                        connection_mode=target.source,
                        warnings=tuple(warnings),
                        next_action="manual_driver_selection",
                        last_error="target_detection_timeout",
                    ),
                    depth=depth,
                    status="target_timeout",
                    reason="driver_detection_timeout",
                    budget_exhausted=True,
                )
            except RuntimeError as exc:
                scan = None
                exc_silent = getattr(exc, "silent", None)
                if exc_silent is None:
                    exc_silent = is_silent_detection_error(str(exc))
                if depth == DETECTION_DEPTH_DEEP and exc_silent:
                    sweep_outcome = await self._async_attempt_link_baud_sweep(
                        transport,
                        deadline=deadline,
                        preferred_driver_keys=_smartess_preferred_driver_keys(smartess_probe),
                    )
                if sweep_outcome is not None and sweep_outcome.matched:
                    scan = sweep_outcome.scan
                    warnings = [
                        *warnings,
                        "link_baud_changed",
                    ]
                else:
                    if candidate.collector is not None:
                        await self._async_enrich_collector_bridge_details(
                            transport,
                            candidate.collector,
                            collector_ip=candidate.ip,
                        )
                    silent_details: dict[str, Any] = {}
                    exc_probe_log = tuple(getattr(exc, "probe_log", ()) or ())
                    if exc_probe_log:
                        silent_details["probe_log"] = list(exc_probe_log)
                    if exc_silent:
                        silent_details["link_baud_hints"] = list(catalog_link_baud_hints())
                    if sweep_outcome is not None:
                        silent_details["link_baud_sweep"] = sweep_outcome.as_details()
                    return _with_detection_evidence(
                        OnboardingResult(
                            collector=candidate,
                            connection_type=CONNECTION_TYPE_EYBOND,
                            connection_mode=target.source,
                            warnings=tuple(warnings),
                            next_action="manual_driver_selection",
                            last_error=str(exc),
                        ),
                        depth=depth,
                        status="collector_only",
                        reason=str(exc),
                        details=silent_details or None,
                    )

            if not scan.candidates:
                # Deep sweep ran out of budget before any driver confirmed.
                # A silent target usually lands HERE, not in the RuntimeError
                # branch: on a dead UART every probe burns its full timeout,
                # so the sweep budget exhausts before the registry completes.
                # The link baud walk must therefore trigger from this branch
                # too when the probe log shows no observed response at all.
                real_probes = tuple(
                    entry
                    for entry in scan.probe_log
                    if entry.get("outcome") != "skipped_budget_exhausted"
                )
                # Only probes that actually RAN can testify to silence: a log
                # of budget-skipped entries means the flashed speed was never
                # tried, and walking baud rates from that would rewrite the
                # UART of a collector we never listened to.
                scan_silent = bool(real_probes) and not any(
                    entry.get("saw_response") for entry in real_probes
                )
                if depth == DETECTION_DEPTH_DEEP and scan_silent:
                    sweep_outcome = await self._async_attempt_link_baud_sweep(
                        transport,
                        deadline=deadline,
                        preferred_driver_keys=_smartess_preferred_driver_keys(smartess_probe),
                    )
                if sweep_outcome is not None and sweep_outcome.matched:
                    scan = sweep_outcome.scan
                    warnings = [*warnings, "link_baud_changed"]
                else:
                    if candidate.collector is not None:
                        await self._async_enrich_collector_bridge_details(
                            transport,
                            candidate.collector,
                            collector_ip=candidate.ip,
                        )
                    budget_details: dict[str, Any] = {}
                    if scan.probe_log:
                        budget_details["probe_log"] = list(scan.probe_log)
                    if scan_silent:
                        budget_details["link_baud_hints"] = list(catalog_link_baud_hints())
                    if sweep_outcome is not None:
                        budget_details["link_baud_sweep"] = sweep_outcome.as_details()
                    return _with_detection_evidence(
                        OnboardingResult(
                            collector=candidate,
                            connection_type=CONNECTION_TYPE_EYBOND,
                            connection_mode=target.source,
                            warnings=tuple(warnings),
                            next_action="manual_driver_selection",
                            last_error="target_detection_timeout",
                        ),
                        depth=depth,
                        status="target_timeout",
                        reason="driver_detection_budget_exhausted",
                        budget_exhausted=True,
                        details=budget_details or None,
                    )

            context = scan.candidates[0]
            alternative_contexts = tuple(scan.candidates[1:])
            if smartess_probe is not None:
                _apply_smartess_probe_to_match(context.match.details, smartess_probe)
                _apply_smartess_probe_to_match(context.inverter.details, smartess_probe)
                for alternative in alternative_contexts:
                    _apply_smartess_probe_to_match(alternative.match.details, smartess_probe)
                    _apply_smartess_probe_to_match(alternative.inverter.details, smartess_probe)
            if enrich_runtime_details:
                await self._async_enrich_onboarding_runtime_details(
                    transport,
                    context,
                    collector_ip=candidate.ip,
                    collector=candidate.collector,
                )

            details: dict[str, Any] = {
                "candidate_driver_count": 1 + len(alternative_contexts),
                "candidate_drivers": [
                    context.match.driver_key,
                    *[alternative.match.driver_key for alternative in alternative_contexts],
                ],
            }
            if scan.probe_log:
                details["probe_log"] = list(scan.probe_log)
            if sweep_outcome is not None and sweep_outcome.matched:
                details["link_baud_sweep"] = sweep_outcome.as_details()
            return _with_detection_evidence(
                OnboardingResult(
                    collector=candidate,
                    match=context.match,
                    alternative_matches=tuple(
                        alternative.match for alternative in alternative_contexts
                    ),
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    warnings=tuple(warnings),
                    next_action="create_entry",
                ),
                depth=depth,
                status=(
                    "driver_choice_required"
                    if alternative_contexts
                    else "matched"
                ),
                reason=context.match.driver_key,
                budget_exhausted=scan.budget_exhausted,
                details=details,
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
            await transport.stop()

    async def _async_detect_driver_with_retries(
        self,
        transport: Any,
        *,
        depth: str = DETECTION_DEPTH_FAST,
        deadline: OnboardingDeadline | None = None,
        preferred_driver_keys: tuple[str, ...] = (),
        allowed_driver_keys: tuple[str, ...] = (),
    ) -> DriverCandidateScan:
        """Retry one-shot driver probing when the collector responds too early."""

        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
        last_error: RuntimeError | None = None
        attempts = max(1, int(policy.driver_detection_attempts))
        for attempt in range(attempts):
            try:
                if depth == DETECTION_DEPTH_DEEP:
                    return await async_detect_inverter_candidates(
                        transport,
                        driver_hint=self._driver_hint,
                        depth=depth,
                        remaining_seconds=(
                            deadline.remaining_seconds if deadline is not None else None
                        ),
                        preferred_driver_keys=preferred_driver_keys,
                        allowed_driver_keys=allowed_driver_keys,
                    )
                context = await async_detect_inverter(
                    transport,
                    driver_hint=self._driver_hint,
                    depth=depth,
                    preferred_driver_keys=preferred_driver_keys,
                    remaining_seconds=(
                        deadline.remaining_seconds if deadline is not None else None
                    ),
                )
                probe_log = tuple(
                    entry
                    for entry in context.inverter.details.get("probe_log", ())
                    if isinstance(entry, dict)
                )
                return DriverCandidateScan(candidates=(context,), probe_log=probe_log)
            except RuntimeError as exc:
                last_error = exc
                if attempt >= attempts - 1 or not _is_retryable_detection_error(str(exc)):
                    raise
                await asyncio.sleep(max(0.0, float(policy.driver_retry_delay)))
        raise last_error or RuntimeError("no_supported_driver_matched")

    async def _async_attempt_link_baud_sweep(
        self,
        transport: Any,
        *,
        deadline: OnboardingDeadline | None,
        preferred_driver_keys: tuple[str, ...] = (),
    ):
        """Walk catalog baud rates on a runtime-UART-capable esp bridge.

        Returns a ``LinkBaudSweepOutcome`` or ``None`` when the collector is
        not eligible (factory collector, fixed-UART build, no FC channel, or
        no catalog hints to try).
        """

        if not hasattr(transport, "async_send_collector"):
            return None
        candidate_bauds = catalog_link_baud_hints()
        if not candidate_bauds:
            return None

        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
        query_timeout = max(1.0, float(policy.collector_query_timeout))
        session = SmartEssLocalSession(transport)

        try:
            hardware = await asyncio.wait_for(
                session.query_collector(6), timeout=query_timeout
            )
        except Exception as exc:
            logger.debug("Link baud sweep: hardware token read failed error=%s", exc)
            return None
        token = parse_esp_collector_hardware_token(getattr(hardware, "text", ""))
        if not token.is_bridge:
            return None
        profile = collector_capability_profile(
            virtual_bridge=True,
            hardware_version=getattr(hardware, "text", ""),
        )
        if not profile.uart_runtime_speed_change:
            logger.debug(
                "Link baud sweep skipped: platform %s has fixed UART", token.platform
            )
            return None

        async def read_baud() -> int | None:
            try:
                response = await asyncio.wait_for(
                    session.query_collector(34), timeout=query_timeout
                )
            except Exception as exc:
                logger.debug("Link baud sweep: baud read failed error=%s", exc)
                return None
            return parse_reported_baud(getattr(response, "text", ""))

        async def set_baud(baud: int) -> bool:
            try:
                response = await asyncio.wait_for(
                    session.set_collector(34, str(baud)), timeout=query_timeout
                )
            except Exception as exc:
                logger.debug("Link baud sweep: set %s failed error=%s", baud, exc)
                return False
            return int(getattr(response, "code", 1)) == 0

        sweep_budget = default_deep_driver_sweep_seconds()

        def admit() -> bool:
            ensure_remaining = getattr(deadline, "ensure_remaining", None)
            if callable(ensure_remaining):
                try:
                    ensure_remaining(sweep_budget + _DEEP_SWEEP_FINALIZE_MARGIN)
                except Exception:
                    return False
                return True
            if deadline is not None:
                remaining = deadline.remaining_seconds()
                return remaining is None or remaining > sweep_budget
            return True

        async def run_sweep(baud: int):
            allowed = driver_keys_for_link_baud(baud)
            if not allowed:
                return None
            sweep_deadline = (
                deadline.nested(sweep_budget)
                if deadline is not None
                else OnboardingDeadline.from_timeout(sweep_budget)
            )
            try:
                return await self._async_detect_driver_with_retries(
                    transport,
                    depth=DETECTION_DEPTH_DEEP,
                    deadline=sweep_deadline,
                    preferred_driver_keys=preferred_driver_keys,
                    allowed_driver_keys=allowed,
                )
            except (TimeoutError, RuntimeError) as exc:
                logger.debug(
                    "Link baud sweep: no driver at %s error=%s", baud, exc
                )
                return None

        return await async_run_link_baud_sweep(
            candidate_bauds=candidate_bauds,
            read_baud=read_baud,
            set_baud=set_baud,
            run_sweep=run_sweep,
            admit=admit,
        )

    async def _async_enrich_collector_bridge_details(
        self,
        transport: Any,
        collector,
        *,
        collector_ip: str,
    ) -> None:
        """Best-effort bridge detection for collector-only results."""

        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
        at_timeout = min(self._connection.request_timeout, policy.collector_query_timeout)
        if at_timeout <= 0:
            return

        details: dict[str, object] = {}
        if hasattr(transport, "async_send_collector"):
            try:
                details.update(
                    await asyncio.wait_for(
                        query_runtime_collector_values(
                            SmartEssLocalSession(transport),
                            parameters=_ONBOARDING_RUNTIME_COLLECTOR_PARAMETERS,
                        ),
                        timeout=at_timeout,
                    )
                )
            except Exception as exc:
                logger.debug(
                    "Onboarding collector bridge FC query failed ip=%s error=%s",
                    collector_ip,
                    exc,
                )
        try:
            at_transport = SharedCollectorAtTransport(
                host=_LISTENER_BIND_HOST,
                port=self._connection.tcp_port,
                request_timeout=at_timeout,
                collector_ip=collector_ip,
            )
            await at_transport.start()
            try:
                details.update(
                    await asyncio.wait_for(
                        query_runtime_collector_at_values(at_transport),
                        timeout=at_timeout,
                    )
                )
            finally:
                await at_transport.stop()
        except Exception as exc:
            logger.debug(
                "Onboarding collector bridge AT query failed ip=%s error=%s",
                collector_ip,
                exc,
            )
            return

        _apply_collector_cloud_endpoint_details_to_collector(collector, details)
        _apply_bridge_hardware_token_to_collector(
            collector,
            details.get("collector_hardware_version"),
        )

    async def _async_enrich_onboarding_runtime_details(
        self,
        transport: Any,
        context: DetectedDriverContext,
        *,
        collector_ip: str,
        collector: CollectorInfo | None = None,
    ) -> None:
        """Best-effort collector/inverter reads used only to enrich onboarding UI data."""

        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
        deadline = OnboardingDeadline.from_timeout(policy.runtime_enrichment_timeout)
        details: dict[str, object] = {}

        if hasattr(transport, "async_send_collector"):
            try:
                details.update(
                    await deadline.wait_for(
                        query_runtime_collector_values(
                            SmartEssLocalSession(transport),
                            parameters=_ONBOARDING_RUNTIME_COLLECTOR_PARAMETERS,
                        ),
                        timeout_seconds=policy.collector_query_timeout,
                    )
                )
            except Exception as exc:
                logger.debug("Onboarding collector FC query failed ip=%s error=%s", collector_ip, exc)

        at_timeout = deadline.bounded_timeout(policy.collector_query_timeout)
        try:
            if at_timeout is not None and at_timeout > 0:
                at_transport = SharedCollectorAtTransport(
                    host=_LISTENER_BIND_HOST,
                    port=self._connection.tcp_port,
                    request_timeout=min(self._connection.request_timeout, at_timeout),
                    collector_ip=collector_ip,
                )
                await at_transport.start()
                try:
                    details.update(
                        await deadline.wait_for(
                            query_runtime_collector_at_values(at_transport),
                            timeout_seconds=at_timeout,
                        )
                    )
                except Exception as exc:
                    logger.debug("Onboarding collector AT query failed ip=%s error=%s", collector_ip, exc)
                finally:
                    await at_transport.stop()
        except Exception as exc:
            logger.debug("Onboarding collector AT transport unavailable ip=%s error=%s", collector_ip, exc)

        _apply_collector_cloud_endpoint_details_to_collector(collector, details)
        hardware_token = parse_esp_collector_hardware_token(
            details.get("collector_hardware_version")
        )
        if hardware_token.is_bridge:
            _apply_bridge_hardware_token_to_collector(
                collector,
                details.get("collector_hardware_version"),
            )
            details["collector_virtual_bridge"] = True
            details["collector_bridge_kind"] = "esp-collector"
            if hardware_token.version:
                details["collector_bridge_version"] = hardware_token.version

        try:
            runtime_values = await deadline.wait_for(
                context.driver.async_read_values(transport, context.inverter),
                timeout_seconds=policy.driver_onboarding_read_timeout,
            )
        except Exception as exc:
            logger.debug(
                "Onboarding inverter runtime read failed model=%s serial=%s error=%s",
                context.inverter.model_name,
                context.inverter.serial_number,
                exc,
            )
        else:
            apply_canonical_measurements(
                context.inverter.driver_key,
                runtime_values,
                variant_key=context.inverter.variant_key,
            )
            for key in _ONBOARDING_RUNTIME_DETAIL_KEYS:
                value = runtime_values.get(key)
                if value not in (None, ""):
                    details[key] = value

        if not details:
            return

        filtered_details = {
            key: value
            for key, value in details.items()
            if key in _ONBOARDING_RUNTIME_DETAIL_KEYS and value not in (None, "")
        }
        if not filtered_details:
            return

        context.inverter.details.update(filtered_details)
        context.match.details.update(filtered_details)

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

            timeout = deadline.bounded_timeout(discovery_timeout)
            if timeout is None or timeout > 0:
                try:
                    replies = await async_probe_target_replies(
                        bind_ip=self._connection.server_ip,
                        advertised_server_ip=self._connection.effective_advertised_server_ip,
                        advertised_server_port=self._connection.effective_advertised_tcp_port,
                        target_ip=target.ip,
                        udp_port=self._connection.udp_port,
                        timeout=timeout or discovery_timeout,
                    )
                except Exception as exc:
                    logger.debug("Broadcast discovery expansion failed target=%s error=%s", target.ip, exc)
                    replies = ()
            else:
                replies = ()

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
                expanded.append(DiscoveryTarget(ip=reply_ip, source=target.source))

        return tuple(expanded)

    async def _async_auto_unicast_fallback_targets(
        self,
        *,
        resolved_targets: Sequence[DiscoveryTarget],
        results: Sequence[OnboardingResult],
        discovery_timeout: float,
        deadline: OnboardingDeadline,
    ) -> tuple[DiscoveryTarget, ...]:
        if not any(target.source == "broadcast" for target in resolved_targets):
            return ()

        known_ips = {
            result.collector.ip
            for result in results
            if result.collector is not None and result.collector.ip
        }
        known_ips.update(target.ip for target in resolved_targets)

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
                network_cidr="",
            ),
            timeout=timeout or min(discovery_timeout, _UNICAST_FALLBACK_PROBE_TIMEOUT),
        )
        return tuple(target for target in replied_targets if target.ip not in known_ips)

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
            candidate = CollectorCandidate(
                target_ip=state.target.ip,
                source=state.target.source,
                ip=state.target.ip,
            )
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
            depth=state.depth,
            status="target_timeout",
            reason="deadline_exhausted",
            budget_exhausted=True,
        )

    @staticmethod
    def _cancelled_after_match_result_for_state(
        state: _TargetDetectionState,
    ) -> OnboardingResult:
        """Result for a probe cancelled because another target already matched.

        Unlike a timeout this is not a failure: the candidate keeps whatever
        was learned (reply, connection), no budget-exhausted flag is raised,
        and the evidence names the real cause for diagnostics.
        """

        candidate = state.candidate
        if candidate is None:
            candidate = CollectorCandidate(
                target_ip=state.target.ip,
                source=state.target.source,
                ip=state.target.ip,
            )
            next_action = "manual_input"
        else:
            next_action = "manual_driver_selection" if candidate.connected else "manual_input"

        return _with_detection_evidence(
            OnboardingResult(
                collector=candidate,
                connection_type=CONNECTION_TYPE_EYBOND,
                connection_mode=state.target.source,
                next_action=next_action,
                last_error="cancelled_first_match_found",
            ),
            depth=state.depth,
            status="cancelled_first_match_found",
            reason="another_target_matched_first",
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
        depth: str = DETECTION_DEPTH_FAST,
    ) -> tuple[OnboardingResult, ...]:
        if listener is None:
            return ()

        inventory_provider = getattr(listener, "discovered_collector_sessions", None)
        if not callable(inventory_provider):
            return ()

        broadcast_target = next(
            (target for target in discovery_targets if target.source == "broadcast"),
            None,
        )
        if broadcast_target is None:
            return ()

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
            materialized.append(
                _with_detection_evidence(
                    OnboardingResult(
                        collector=CollectorCandidate(
                            target_ip=broadcast_target.ip,
                            source=broadcast_target.source,
                            ip=peer_ip,
                            connected=state in {"claimed", "routed_framed", "routed_at_text"},
                            collector=CollectorInfo(
                                remote_ip=peer_ip,
                                remote_port=peer_port,
                                collector_pn=collector_pn,
                            ),
                        ),
                        connection_type=CONNECTION_TYPE_EYBOND,
                        connection_mode=broadcast_target.source,
                        next_action="manual_driver_selection",
                        last_error="collector_detected_without_driver",
                    ),
                    depth=depth,
                    status="collector_only",
                    reason="callback_session_inventory",
                    details={"session_state": state},
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


def _result_priority(result: OnboardingResult) -> tuple[int, int, int]:
    return (
        _CONFIDENCE_SCORE.get(result.confidence, 0),
        1 if result.match is not None else 0,
        1 if result.collector is not None and result.collector.connected else 0,
    )


def _is_retryable_detection_error(error: str) -> bool:
    """Return whether one onboarding probe error is likely transient."""

    return any(
        marker in error
        for marker in (
            "response_too_short",
            "collector_disconnected",
            "crc_mismatch",
            "unexpected_length",
        )
    )


async def _async_probe_smartess_onboarding(transport: Any) -> SmartEssOnboardingProbe | None:
    """Collect SmartESS query 5 and query 14 metadata without affecting onboarding success."""

    session = SmartEssLocalSession(transport)
    collector_version = ""
    protocol_descriptor: SmartEssProtocolDescriptor | None = None
    known_protocol: SmartEssProtocolCatalogEntry | None = None

    try:
        collector_version = await session.query_collector_version()
    except Exception as exc:
        logger.debug("SmartESS onboarding query 5 failed error=%s", exc)

    try:
        protocol_descriptor = await session.query_protocol_descriptor()
        known_protocol = load_smartess_protocol_catalog().protocols.get(protocol_descriptor.asset_id)
    except Exception as exc:
        logger.debug("SmartESS onboarding query 14 failed error=%s", exc)

    if not collector_version and protocol_descriptor is None:
        return None

    return SmartEssOnboardingProbe(
        collector_version=collector_version,
        protocol_descriptor=protocol_descriptor,
        known_protocol=known_protocol,
    )


def _apply_smartess_probe_to_collector(
    collector: Any,
    probe: SmartEssOnboardingProbe,
) -> None:
    """Persist SmartESS onboarding metadata onto collector info."""

    if probe.collector_version:
        collector.smartess_collector_version = probe.collector_version

    descriptor = probe.protocol_descriptor
    if descriptor is not None:
        collector.smartess_protocol_raw_id = descriptor.raw_id
        collector.smartess_protocol_asset_id = descriptor.asset_id
        collector.smartess_protocol_asset_name = descriptor.asset_name
        collector.smartess_protocol_suffix = descriptor.suffix

    if probe.known_protocol is not None:
        collector.smartess_protocol_profile_key = probe.known_protocol.profile_key
        collector.smartess_protocol_name = (
            probe.known_protocol.proto_name or collector.smartess_protocol_asset_name
        )
        collector.smartess_device_address = probe.selected_device_address


def _apply_smartess_probe_to_match(
    details: dict[str, Any],
    probe: SmartEssOnboardingProbe,
) -> None:
    """Persist SmartESS onboarding metadata onto one details mapping."""

    if probe.collector_version:
        details.setdefault("smartess_collector_version", probe.collector_version)

    descriptor = probe.protocol_descriptor
    if descriptor is not None:
        details.setdefault("smartess_protocol_raw_id", descriptor.raw_id)
        details.setdefault("smartess_protocol_asset_id", descriptor.asset_id)
        details.setdefault("smartess_protocol_asset_name", descriptor.asset_name)
        if descriptor.suffix:
            details.setdefault("smartess_protocol_suffix", descriptor.suffix)

    if probe.known_protocol is not None:
        details.setdefault("smartess_profile_key", probe.known_protocol.profile_key)
        if probe.known_protocol.proto_name:
            details.setdefault("smartess_protocol_name", probe.known_protocol.proto_name)
        if probe.selected_device_address is not None:
            details.setdefault("smartess_device_address", probe.selected_device_address)
