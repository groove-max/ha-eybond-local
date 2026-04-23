"""Generic driver-probing helpers independent of one physical transport."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from ..drivers.base import InverterDriver
from ..drivers.registry import iter_drivers
from ..models import DetectedInverter, DriverMatch

logger = logging.getLogger(__name__)


_SMG_READ_ONLY_PROFILE_NAME = "modbus_smg/family_fallback.json"
_SMG_UNVERIFIED_VARIANT_KEYS = {"anenji_4200_protocol_1"}


@dataclass(slots=True)
class DetectedDriverContext:
    """The concrete matched driver plus a serializable match summary."""

    driver: InverterDriver
    inverter: DetectedInverter
    match: DriverMatch


async def async_detect_inverter(
    transport: Any,
    *,
    driver_hint: str,
) -> DetectedDriverContext:
    """Probe all drivers against one active transport and return the first match."""

    errors: list[str] = []

    for driver in iter_drivers(driver_hint):
        for target in driver.probe_targets:
            try:
                inverter = await driver.async_probe(transport, target)
            except Exception as exc:
                errors.append(f"{driver.key}:{exc}")
                logger.debug("Probe failed driver=%s target=%s error=%s", driver.key, target, exc)
                continue

            if inverter is None:
                continue

            return DetectedDriverContext(
                driver=driver,
                inverter=inverter,
                match=_build_driver_match(driver, inverter),
            )

    raise RuntimeError(errors[-1] if errors else "no_supported_driver_matched")


def _build_driver_match(driver: InverterDriver, inverter: DetectedInverter) -> DriverMatch:
    reasons = []
    confidence = "medium"
    if inverter.protocol_family:
        reasons.append("protocol_family_present")
    if inverter.model_name:
        reasons.append("model_name_present")
    if inverter.serial_number:
        reasons.append("serial_number_present")
    if inverter.details.get("rated_power"):
        reasons.append("rated_power_present")
    if inverter.variant_key == "family_fallback":
        reasons.append("family_fallback_variant")
    elif inverter.variant_key in _SMG_UNVERIFIED_VARIANT_KEYS:
        reasons.append("unverified_variant")
    elif inverter.profile_name == _SMG_READ_ONLY_PROFILE_NAME:
        reasons.append("read_only_profile")
    elif inverter.protocol_family and inverter.model_name and inverter.serial_number:
        confidence = "high"

    return DriverMatch(
        driver_key=driver.key,
        protocol_family=inverter.protocol_family,
        model_name=inverter.model_name,
        variant_key=inverter.variant_key,
        serial_number=inverter.serial_number,
        probe_target=inverter.probe_target,
        confidence=confidence,
        reasons=tuple(reasons),
        details=dict(inverter.details),
    )
