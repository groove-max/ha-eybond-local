"""EyeBond-specific onboarding discovery built on top of generic driver detection."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from itertools import islice
import ipaddress
import logging
from dataclasses import dataclass
from typing import Any, Sequence

from ..canonical_telemetry import apply_canonical_measurements
from ..collector.at_runtime import parse_collector_vdtu, query_runtime_collector_at_values
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
from .driver_detection import DetectedDriverContext, async_detect_inverter
from .timeouts import DEFAULT_ONBOARDING_TIMEOUT_POLICY, OnboardingDeadline
from ..models import CollectorCandidate, CollectorInfo, OnboardingResult

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
    # Virtual-bridge verdict carried from the AT+VDTU probe so the confirm step
    # can gate the collector operation-mode UI before the entry exists. The
    # raw VDTU string itself is deliberately NOT carried — only the parsed
    # boolean/identity, populated below in _async_enrich_onboarding_runtime_details.
    "collector_virtual_bridge",
    "collector_bridge_kind",
    "collector_bridge_version",
    "collector_bridge_features",
    "collector_bridge_attributes",
    "collector_bridge_uart",
    "collector_bridge_spacing_ms",
    "collector_bridge_queue",
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


def _apply_virtual_bridge_info_to_collector(collector: Any, bridge: Any) -> None:
    """Carry a positive AT+VDTU bridge verdict into CollectorInfo."""

    if collector is None or not bridge.is_virtual_bridge:
        return
    collector.collector_virtual_bridge = True
    if bridge.kind:
        collector.collector_bridge_kind = bridge.kind
    if bridge.version:
        collector.collector_bridge_version = bridge.version
    if bridge.features:
        collector.collector_bridge_features = tuple(bridge.features)
    if bridge.attributes:
        collector.collector_bridge_attributes = tuple(bridge.attributes)


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


@dataclass(slots=True)
class _TargetDetectionState:
    target: DiscoveryTarget
    candidate: CollectorCandidate | None = None


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
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        enrich_runtime_details: bool = True,
        cleanup_new_shared_connection: bool = False,
        total_timeout: float | None = None,
        concurrency: int = _TARGET_DETECTION_CONCURRENCY,
        return_after_first_match: bool = False,
    ) -> tuple[OnboardingResult, ...]:
        """Run one-shot detection against a list of discovery targets."""

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
                    return OnboardingResult(
                        collector=CollectorCandidate(target_ip=target.ip, source=target.source, ip=target.ip),
                        connection_type=CONNECTION_TYPE_EYBOND,
                        connection_mode=target.source,
                        next_action="manual_input",
                        last_error=str(exc),
                    )

        for target in targets:
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
                break
            should_stop = False
            for task in done:
                result = task.result()
                results.append(result)
                if return_after_first_match and result.match is not None:
                    should_stop = True
            if should_stop:
                break

        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in pending:
                results.append(self._timeout_result_for_state(task_states[task]))

        return tuple(self._dedupe_results(results))

    async def async_auto_detect(
        self,
        *,
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
    ) -> tuple[OnboardingResult, ...]:
        """Run the default EyeBond onboarding discovery order."""

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
                        discovery_timeout=discovery_timeout,
                        connect_timeout=connect_timeout,
                        heartbeat_timeout=heartbeat_timeout,
                        enrich_runtime_details=enrich_runtime_details,
                        total_timeout=deadline.remaining_seconds(),
                        return_after_first_match=True,
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
                            discovery_timeout=discovery_timeout,
                            connect_timeout=connect_timeout,
                            heartbeat_timeout=heartbeat_timeout,
                            enrich_runtime_details=enrich_runtime_details,
                            total_timeout=deadline.remaining_seconds(),
                            return_after_first_match=True,
                        )
                    )

                aggregated.extend(
                    self._session_inventory_results(
                        listener=listener,
                        discovery_targets=targets,
                        results=aggregated,
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
                            discovery_timeout=discovery_timeout,
                            connect_timeout=connect_timeout,
                            heartbeat_timeout=heartbeat_timeout,
                            enrich_runtime_details=enrich_runtime_details,
                            total_timeout=deadline.remaining_seconds(),
                            return_after_first_match=True,
                        )
                    )
                    deduped = self._dedupe_results(aggregated)
            aggregated.extend(
                self._session_inventory_results(
                    listener=listener,
                    discovery_targets=targets,
                    results=aggregated,
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
    ) -> tuple[OnboardingResult, ...]:
        """Run broadcast discovery first, then sweep the full selected IPv4 network."""

        deadline = OnboardingDeadline.from_timeout(total_timeout)
        resolved_targets = tuple(
            discovery_targets
            or build_default_discovery_targets(
                collector_ip=collector_ip,
                discovery_target=discovery_target,
            )
        )
        aggregated = list(
            await self.async_auto_detect(
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
            discovery_timeout=discovery_timeout,
            connect_timeout=connect_timeout,
            heartbeat_timeout=heartbeat_timeout,
            enrich_runtime_details=enrich_runtime_details,
            total_timeout=deadline.remaining_seconds(),
        )
        aggregated.extend(fallback_results)
        return tuple(self._dedupe_results(aggregated))

    async def _async_detect_target(
        self,
        target: DiscoveryTarget,
        *,
        discovery_timeout: float,
        connect_timeout: float,
        heartbeat_timeout: float,
        enrich_runtime_details: bool = True,
        cleanup_new_shared_connection: bool = False,
        detection_state: _TargetDetectionState | None = None,
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
                return OnboardingResult(
                    collector=candidate,
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    warnings=tuple(warnings),
                    next_action="manual_input",
                    last_error="collector_not_connected",
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

            try:
                context = await asyncio.wait_for(
                    self._async_detect_driver_with_retries(transport),
                    timeout=DEFAULT_ONBOARDING_TIMEOUT_POLICY.driver_detection_timeout,
                )
            except TimeoutError:
                if candidate.collector is not None:
                    await self._async_enrich_collector_bridge_details(
                        candidate.collector,
                        collector_ip=candidate.ip,
                    )
                return OnboardingResult(
                    collector=candidate,
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    warnings=tuple(warnings),
                    next_action="manual_driver_selection",
                    last_error="target_detection_timeout",
                )
            except RuntimeError as exc:
                if candidate.collector is not None:
                    await self._async_enrich_collector_bridge_details(
                        candidate.collector,
                        collector_ip=candidate.ip,
                    )
                return OnboardingResult(
                    collector=candidate,
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    warnings=tuple(warnings),
                    next_action="manual_driver_selection",
                    last_error=str(exc),
                )

            if smartess_probe is not None:
                _apply_smartess_probe_to_match(context.match.details, smartess_probe)
                _apply_smartess_probe_to_match(context.inverter.details, smartess_probe)
            if enrich_runtime_details:
                await self._async_enrich_onboarding_runtime_details(
                    transport,
                    context,
                    collector_ip=candidate.ip,
                    collector=candidate.collector,
                )

            return OnboardingResult(
                collector=candidate,
                match=context.match,
                connection_type=CONNECTION_TYPE_EYBOND,
                connection_mode=target.source,
                warnings=tuple(warnings),
                next_action="create_entry",
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

    async def _async_detect_driver_with_retries(self, transport: Any) -> DetectedDriverContext:
        """Retry one-shot driver probing when the collector responds too early."""

        last_error: RuntimeError | None = None
        for attempt in range(3):
            try:
                return await async_detect_inverter(transport, driver_hint=self._driver_hint)
            except RuntimeError as exc:
                last_error = exc
                if attempt >= 2 or not _is_retryable_detection_error(str(exc)):
                    raise
                await asyncio.sleep(0.35)
        raise last_error or RuntimeError("no_supported_driver_matched")

    async def _async_enrich_collector_bridge_details(
        self,
        collector,
        *,
        collector_ip: str,
    ) -> None:
        """Best-effort AT+VDTU bridge detection for collector-only results."""

        policy = DEFAULT_ONBOARDING_TIMEOUT_POLICY
        at_timeout = min(self._connection.request_timeout, policy.collector_query_timeout)
        if at_timeout <= 0:
            return

        details: dict[str, object] = {}
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

        bridge = parse_collector_vdtu(details.get("collector_vdtu_raw"))
        _apply_collector_cloud_endpoint_details_to_collector(collector, details)
        if not bridge.is_virtual_bridge:
            return

        _apply_virtual_bridge_info_to_collector(collector, bridge)

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

        # Parse the AT+VDTU probe (if it answered) into the bridge verdict and
        # carry it to the confirm step. Positive-only: a factory collector or an
        # unanswered probe leaves no carrier keys, so onboarding behaves exactly
        # as before. The raw VDTU string is intentionally dropped here.
        bridge = parse_collector_vdtu(details.pop("collector_vdtu_raw", None))
        _apply_collector_cloud_endpoint_details_to_collector(collector, details)
        if bridge.is_virtual_bridge:
            _apply_virtual_bridge_info_to_collector(collector, bridge)
            details["collector_virtual_bridge"] = True
            if bridge.kind:
                details["collector_bridge_kind"] = bridge.kind
            if bridge.version:
                details["collector_bridge_version"] = bridge.version
            if bridge.features:
                details["collector_bridge_features"] = ", ".join(bridge.features)
            if bridge.attributes:
                details["collector_bridge_attributes"] = ", ".join(
                    f"{key}={value}" for key, value in bridge.attributes
                )
                bridge_attributes = dict(bridge.attributes)
                for key, detail_key in (
                    ("uart", "collector_bridge_uart"),
                    ("spacing_ms", "collector_bridge_spacing_ms"),
                    ("queue", "collector_bridge_queue"),
                ):
                    if bridge_attributes.get(key):
                        details[detail_key] = bridge_attributes[key]

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
            apply_canonical_measurements(context.inverter.driver_key, runtime_values)
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

        return OnboardingResult(
            collector=candidate,
            connection_type=CONNECTION_TYPE_EYBOND,
            connection_mode=state.target.source,
            next_action=next_action,
            last_error="target_detection_timeout",
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
