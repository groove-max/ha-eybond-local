"""EyeBond-specific onboarding discovery built on top of generic driver detection."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from dataclasses import dataclass
from typing import Any, Sequence

from ..collector.discovery import async_probe_target
from ..collector.transport import SharedEybondTransport
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
from .driver_detection import DetectedDriverContext, async_detect_inverter
from ..models import CollectorCandidate, OnboardingResult

logger = logging.getLogger(__name__)

_CONFIDENCE_SCORE = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}

_UNICAST_FALLBACK_PROBE_TIMEOUT = 0.35
_UNICAST_FALLBACK_CONCURRENCY = 32


@dataclass(frozen=True, slots=True)
class DiscoveryTarget:
    """One onboarding discovery target."""

    ip: str
    source: str


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
    """Build one /24 unicast sweep target list for broadcast-unfriendly networks."""

    if collector_ip:
        return ()

    try:
        network = ipaddress.ip_network(f"{server_ip}/24", strict=False)
    except ValueError:
        return ()

    excluded = {server_ip, collector_ip, str(network.network_address), str(network.broadcast_address), ""}
    return tuple(
        DiscoveryTarget(ip=str(host), source="subnet_unicast")
        for host in network.hosts()
        if str(host) not in excluded
    )


async def async_probe_fallback_targets(
    *,
    bind_ip: str,
    advertised_server_ip: str,
    advertised_server_port: int,
    udp_port: int,
    targets: Sequence[DiscoveryTarget],
    timeout: float = _UNICAST_FALLBACK_PROBE_TIMEOUT,
    concurrency: int = _UNICAST_FALLBACK_CONCURRENCY,
) -> tuple[DiscoveryTarget, ...]:
    """Probe one list of direct unicast targets concurrently and keep responders only."""

    if not targets:
        return ()

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _probe(target: DiscoveryTarget) -> DiscoveryTarget | None:
        async with semaphore:
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

    discovered = await asyncio.gather(*(_probe(target) for target in targets))
    deduped: dict[str, DiscoveryTarget] = {}
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
    ) -> tuple[OnboardingResult, ...]:
        """Run one-shot detection against a list of discovery targets."""

        results: list[OnboardingResult] = []
        for target in targets:
            try:
                result = await self._async_detect_target(
                    target,
                    discovery_timeout=discovery_timeout,
                    connect_timeout=connect_timeout,
                    heartbeat_timeout=heartbeat_timeout,
                )
            except Exception as exc:
                logger.warning("Onboarding detection failed target=%s source=%s error=%s", target.ip, target.source, exc)
                result = OnboardingResult(
                    collector=CollectorCandidate(target_ip=target.ip, source=target.source, ip=target.ip),
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    next_action="manual_input",
                    last_error=str(exc),
                )
            results.append(result)
        return tuple(self._dedupe_results(results))

    async def async_auto_detect(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = DEFAULT_DISCOVERY_TARGET,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
    ) -> tuple[OnboardingResult, ...]:
        """Run the default EyeBond onboarding discovery order."""

        targets = build_default_discovery_targets(
            collector_ip=collector_ip,
            discovery_target=discovery_target,
        )
        aggregated: list[OnboardingResult] = []
        for attempt_index in range(max(1, attempts)):
            results = await self.async_detect_targets(
                targets,
                discovery_timeout=discovery_timeout,
                connect_timeout=connect_timeout,
                heartbeat_timeout=heartbeat_timeout,
            )
            aggregated.extend(results)
            deduped = self._dedupe_results(aggregated)
            best = deduped[0] if deduped else None
            if best is not None and best.match is not None:
                aggregated = list(deduped)
                break
            if attempt_index < max(1, attempts) - 1:
                await asyncio.sleep(attempt_delay)

        fallback_targets = build_unicast_fallback_targets(
            server_ip=self._connection.server_ip,
            collector_ip=collector_ip,
        )
        if fallback_targets:
            replied_targets = await async_probe_fallback_targets(
                bind_ip=self._connection.server_ip,
                advertised_server_ip=self._connection.effective_advertised_server_ip,
                advertised_server_port=self._connection.effective_advertised_tcp_port,
                udp_port=self._connection.udp_port,
                targets=fallback_targets,
                timeout=min(discovery_timeout, _UNICAST_FALLBACK_PROBE_TIMEOUT),
            )
            known_ips = {
                result.collector.ip
                for result in aggregated
                if result.collector is not None and result.collector.ip
            }
            known_ips.update(target.ip for target in targets)
            replied_targets = tuple(target for target in replied_targets if target.ip not in known_ips)
            if replied_targets:
                fallback_results = await self.async_detect_targets(
                    replied_targets,
                    discovery_timeout=discovery_timeout,
                    connect_timeout=connect_timeout,
                    heartbeat_timeout=heartbeat_timeout,
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
    ) -> OnboardingResult:
        transport = SharedEybondTransport(
            host=self._connection.server_ip,
            port=self._connection.tcp_port,
            request_timeout=self._connection.request_timeout,
            heartbeat_interval=float(self._connection.heartbeat_interval),
            collector_ip="",
        )
        candidate = CollectorCandidate(
            target_ip=target.ip,
            source=target.source,
            ip=target.ip,
        )

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

            connected = await transport.wait_until_connected(timeout=connect_timeout)
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

            warnings = []
            if not heartbeat_seen:
                warnings.append("collector_heartbeat_not_observed")

            try:
                context = await self._async_detect_driver_with_retries(transport)
            except RuntimeError as exc:
                return OnboardingResult(
                    collector=candidate,
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode=target.source,
                    warnings=tuple(warnings),
                    next_action="manual_driver_selection",
                    last_error=str(exc),
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

    @staticmethod
    def _dedupe_results(results: Sequence[OnboardingResult]) -> list[OnboardingResult]:
        deduped: dict[tuple[str, str], OnboardingResult] = {}
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
            match_key = result.match.driver_key if result.match is not None else ""
            key = (collector_key, match_key)
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
