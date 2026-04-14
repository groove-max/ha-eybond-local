"""Confidence-aware control exposure policy."""

from __future__ import annotations

from .const import (
    CONTROL_MODE_AUTO,
    CONTROL_MODE_FULL,
    CONTROL_MODE_READ_ONLY,
    DEFAULT_CONTROL_MODE,
)
from .models import CapabilityPreset, WriteCapability

_CONFIDENCE_SCORE = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def control_mode_options() -> list[str]:
    """Return supported control mode values."""

    return [CONTROL_MODE_AUTO, CONTROL_MODE_READ_ONLY, CONTROL_MODE_FULL]


def normalize_confidence(confidence: str | None) -> str:
    """Normalize detection confidence into a known value."""

    if not confidence:
        return "none"
    if confidence in _CONFIDENCE_SCORE:
        return confidence
    return "none"


def controls_enabled(
    *,
    control_mode: str = DEFAULT_CONTROL_MODE,
    detection_confidence: str | None = None,
) -> bool:
    """Return whether writes are globally enabled for this entry."""

    confidence = normalize_confidence(detection_confidence)
    if control_mode == CONTROL_MODE_FULL:
        return True
    if control_mode == CONTROL_MODE_READ_ONLY:
        return False
    return confidence == "high"


def controls_reason(
    *,
    control_mode: str = DEFAULT_CONTROL_MODE,
    detection_confidence: str | None = None,
) -> str:
    """Return a user-facing reason for the current access policy."""

    confidence = normalize_confidence(detection_confidence)
    if control_mode == CONTROL_MODE_FULL:
        return "manual_full_override"
    if control_mode == CONTROL_MODE_READ_ONLY:
        return "forced_read_only"
    if confidence == "high":
        return "autodetected_high_confidence"
    if confidence in {"medium", "low"}:
        return "autodetected_below_write_threshold"
    return "no_confirmed_detection"


def controls_summary(
    *,
    control_mode: str = DEFAULT_CONTROL_MODE,
    detection_confidence: str | None = None,
) -> str:
    """Return a short human-readable summary of the current access policy."""

    confidence = normalize_confidence(detection_confidence)
    if control_mode == CONTROL_MODE_FULL:
        return "All controls enabled by manual override."
    if control_mode == CONTROL_MODE_READ_ONLY:
        return "Monitoring only. Controls are disabled by read-only mode."
    if confidence == "high":
        return "Tested controls are enabled automatically."
    if confidence in {"medium", "low"}:
        return "Monitoring only. Detection confidence is below the write threshold."
    return "Monitoring only. No confirmed inverter detection is available yet."


def can_expose_capability(
    capability: WriteCapability,
    *,
    control_mode: str = DEFAULT_CONTROL_MODE,
    detection_confidence: str | None = None,
) -> bool:
    """Return whether a capability should be exposed as a control entity."""

    if control_mode == CONTROL_MODE_FULL:
        return True
    if control_mode == CONTROL_MODE_READ_ONLY:
        return False
    if normalize_confidence(detection_confidence) != "high":
        return False
    return capability.tested


def can_expose_preset(
    preset: CapabilityPreset,
    *,
    capabilities_by_key: dict[str, WriteCapability],
    control_mode: str = DEFAULT_CONTROL_MODE,
    detection_confidence: str | None = None,
) -> bool:
    """Return whether a preset should be exposed as a control entity."""

    if control_mode == CONTROL_MODE_FULL:
        return True
    if control_mode == CONTROL_MODE_READ_ONLY:
        return False
    if normalize_confidence(detection_confidence) != "high":
        return False
    for item in preset.items:
        capability = capabilities_by_key.get(item.capability_key)
        if capability is None or not capability.tested:
            return False
    return True
