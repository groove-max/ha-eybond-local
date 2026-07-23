"""Public EyeBond collector-onboarding surface."""

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
    "DETECTION_DEPTH_DEEP",
    "DETECTION_DEPTH_FAST",
    "DiscoveryTarget",
    "OnboardingDetector",
    "async_probe_fallback_targets",
    "build_default_discovery_targets",
    "build_unicast_fallback_targets",
]
