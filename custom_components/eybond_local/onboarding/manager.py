"""Generic onboarding-manager contract for connection-specific setup flows."""

from __future__ import annotations

from typing import Any, Protocol, Sequence

from ..models import OnboardingResult


class OnboardingManager(Protocol):
    """Onboarding detection contract shared by all future connection branches."""

    async def async_passive_detect(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_targets: Sequence[Any] | None = None,
        settle_timeout: float = 0.1,
    ) -> tuple[OnboardingResult, ...]:
        ...

    async def async_scan(
        self,
        *,
        skip_probe_ips: frozenset[str] = frozenset(),
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_targets: Sequence[Any] | None = None,
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        total_timeout: float | None = None,
    ) -> tuple[OnboardingResult, ...]:
        ...
