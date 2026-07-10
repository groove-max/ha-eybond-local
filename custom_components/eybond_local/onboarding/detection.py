"""Compatibility exports for generic driver detection and EyeBond onboarding."""

from .driver_detection import (
    DetectedDriverContext,
    DriverCandidateScan,
    async_detect_inverter,
    async_detect_inverter_candidates,
)
from .eybond import (
    DETECTION_DEPTH_DEEP,
    DETECTION_DEPTH_FAST,
    DiscoveryTarget,
    OnboardingDetector,
    async_probe_fallback_targets,
    build_default_discovery_targets,
    build_unicast_fallback_targets,
)

__all__ = [
    "DetectedDriverContext",
    "DriverCandidateScan",
    "DETECTION_DEPTH_DEEP",
    "DETECTION_DEPTH_FAST",
    "DiscoveryTarget",
    "OnboardingDetector",
    "async_detect_inverter",
    "async_detect_inverter_candidates",
    "async_probe_fallback_targets",
    "build_default_discovery_targets",
    "build_unicast_fallback_targets",
]
