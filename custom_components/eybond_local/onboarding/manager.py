"""Generic onboarding-manager contract for connection-specific setup flows."""

from __future__ import annotations

from typing import Any, Protocol, Sequence

from ..models import OnboardingResult


class OnboardingManager(Protocol):
    """Onboarding detection contract shared by all future connection branches."""

    async def async_passive_detect(
        self,
        *,
        depth: str = "fast",
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_targets: Sequence[Any] | None = None,
        settle_timeout: float = 0.1,
    ) -> tuple[OnboardingResult, ...]:
        ...

    async def async_auto_detect(
        self,
        *,
        depth: str = "fast",
        skip_probe_ips: frozenset[str] = frozenset(),
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_targets: Sequence[Any] | None = None,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
        enrich_runtime_details: bool = True,
        identify_collector_only: bool = False,
        total_timeout: float | None = None,
    ) -> tuple[OnboardingResult, ...]:
        ...

    async def async_handoff_detect(
        self,
        *,
        collector_ip: str,
        collector_pn: str = "",
        collector_session_protocol: str = "",
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
        enrich_runtime_details: bool = True,
        cleanup_new_shared_connection: bool = False,
    ) -> OnboardingResult | None:
        ...

    async def async_deep_detect(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_targets: Sequence[Any] | None = None,
        unicast_network_cidr: str = "",
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
        enrich_runtime_details: bool = True,
        identify_collector_only: bool = False,
        total_timeout: float | None = None,
    ) -> tuple[OnboardingResult, ...]:
        ...
