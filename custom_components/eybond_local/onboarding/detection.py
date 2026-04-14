"""Compatibility exports for generic driver detection and EyeBond onboarding."""

from .driver_detection import DetectedDriverContext, async_detect_inverter
from .eybond import (
    DiscoveryTarget,
    OnboardingDetector,
    async_probe_fallback_targets,
    build_default_discovery_targets,
    build_unicast_fallback_targets,
)

__all__ = [
    "DetectedDriverContext",
    "DiscoveryTarget",
    "OnboardingDetector",
    "async_detect_inverter",
    "async_probe_fallback_targets",
    "build_default_discovery_targets",
    "build_unicast_fallback_targets",
]
