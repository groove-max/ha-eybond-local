"""Generic onboarding-manager contract for connection-specific setup flows."""

from __future__ import annotations

from typing import Protocol

from ..models import OnboardingResult


class OnboardingManager(Protocol):
    """Onboarding detection contract shared by all future connection branches."""

    async def async_auto_detect(
        self,
        *,
        collector_ip: str = "",
        discovery_target: str = "",
        discovery_timeout: float = 1.5,
        connect_timeout: float = 5.0,
        heartbeat_timeout: float = 2.0,
        attempts: int = 3,
        attempt_delay: float = 0.75,
    ) -> tuple[OnboardingResult, ...]:
        ...
