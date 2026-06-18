"""Generic driver-probing helpers independent of one physical transport."""

from __future__ import annotations

import logging
import asyncio
from dataclasses import dataclass
from typing import Any

from ..drivers.base import InverterDriver
from ..drivers.catalog_identity import (
    ERROR_INVERTER_LINK_DOWN,
    InverterIdentityNoDataError,
)
from ..drivers.registry import iter_drivers
from ..metadata.compiled_detection_catalog import (
    CompiledDeviceDescriptor,
    CompiledSurfaceDescriptor,
    load_compiled_detection_catalog,
)
from ..models import DetectedInverter, DriverMatch, ProbeTarget

logger = logging.getLogger(__name__)


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
    inverter_link_down = False
    driver_targets = tuple(
        (driver, _ordered_probe_targets(driver, transport))
        for driver in iter_drivers(driver_hint)
    )
    driver_targets = await _ordered_driver_targets_by_signature(
        driver_targets,
        transport,
    )

    for driver, targets in driver_targets:
        try:
            inverter = await _async_probe_driver_with_budget(
                driver,
                transport,
                targets,
            )
        except asyncio.TimeoutError:
            errors.append(f"{driver.key}:probe_timeout")
            logger.debug("Probe timed out driver=%s timeout=%s", driver.key, driver.probe_timeout)
            continue
        except InverterIdentityNoDataError:
            # The identity registers read as zeros: the collector currently has
            # no inverter link. Remaining drivers still get their chance, but a
            # fully failed detection must surface the link problem instead of
            # the misleading "no supported driver" verdict.
            errors.append(f"{driver.key}:{ERROR_INVERTER_LINK_DOWN}")
            inverter_link_down = True
            logger.debug("Identity region read as zeros driver=%s", driver.key)
            continue
        except Exception as exc:
            errors.append(f"{driver.key}:{exc}")
            logger.debug("Probe failed driver=%s error=%s", driver.key, exc)
            continue

        if inverter is not None:
            return DetectedDriverContext(
                driver=driver,
                inverter=inverter,
                match=_build_driver_match(driver, inverter),
            )

    if inverter_link_down:
        raise RuntimeError(ERROR_INVERTER_LINK_DOWN)
    raise RuntimeError(errors[-1] if errors else "no_supported_driver_matched")


async def _ordered_driver_targets_by_signature(
    driver_targets: tuple[tuple[InverterDriver, tuple[ProbeTarget, ...]], ...],
    transport: Any,
) -> tuple[tuple[InverterDriver, tuple[ProbeTarget, ...]], ...]:
    signed: list[tuple[InverterDriver, tuple[ProbeTarget, ...]]] = []
    unsigned: list[tuple[InverterDriver, tuple[ProbeTarget, ...]]] = []

    for driver, targets in driver_targets:
        try:
            matched = await _async_probe_driver_signature(driver, transport, targets)
        except Exception as exc:
            logger.debug("Signature probe failed driver=%s error=%s", driver.key, exc)
            matched = False
        if matched:
            signed.append((driver, targets))
        else:
            unsigned.append((driver, targets))

    return tuple((*signed, *unsigned))


async def _async_probe_driver_signature(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
) -> bool:
    timeout = getattr(driver, "signature_timeout", None)
    if timeout is None or timeout <= 0:
        return False
    try:
        return await asyncio.wait_for(
            _async_probe_driver_signature_targets(driver, transport, targets),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.debug("Signature probe timed out driver=%s timeout=%s", driver.key, timeout)
        return False


async def _async_probe_driver_signature_targets(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
) -> bool:
    for target in targets:
        try:
            if await driver.async_probe_signature(transport, target):
                return True
        except Exception as exc:
            logger.debug("Signature probe failed driver=%s target=%s error=%s", driver.key, target, exc)
    return False


async def _async_probe_driver_with_budget(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
) -> DetectedInverter | None:
    timeout = getattr(driver, "probe_timeout", None)
    if timeout is None or timeout <= 0:
        return await _async_probe_driver_targets(driver, transport, targets)
    return await asyncio.wait_for(
        _async_probe_driver_targets(driver, transport, targets),
        timeout=timeout,
    )


async def _async_probe_driver_targets(
    driver: InverterDriver,
    transport: Any,
    targets: tuple[ProbeTarget, ...],
) -> DetectedInverter | None:
    for target in targets:
        try:
            inverter = await driver.async_probe(transport, target)
        except InverterIdentityNoDataError:
            raise
        except Exception as exc:
            logger.debug("Probe failed driver=%s target=%s error=%s", driver.key, target, exc)
            continue

        if inverter is not None:
            return inverter
    return None


def _ordered_probe_targets(driver: InverterDriver, transport: Any) -> tuple[ProbeTarget, ...]:
    probe_targets = tuple(getattr(driver, "probe_targets", ()))
    if not probe_targets:
        return ()
    collector_info = getattr(transport, "collector_info", None)
    preferred_devcodes: tuple[int, ...] = ()
    if collector_info is not None:
        seen_devcodes: list[int] = []
        for attribute in ("heartbeat_devcode", "last_devcode"):
            value = getattr(collector_info, attribute, None)
            if isinstance(value, int) and value not in seen_devcodes:
                seen_devcodes.append(value)
        preferred_devcodes = tuple(seen_devcodes)

    original_order = {
        (target.devcode, target.collector_addr, target.device_addr): index
        for index, target in enumerate(probe_targets)
    }

    def _sort_key(target: ProbeTarget) -> tuple[int, int, int]:
        original_index = original_order[(target.devcode, target.collector_addr, target.device_addr)]
        devcode_rank = 0
        if preferred_devcodes:
            devcode_rank = 0 if target.devcode in preferred_devcodes else 1
        return (devcode_rank, original_index, original_index)

    return tuple(sorted(probe_targets, key=_sort_key))


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
    surface, candidates = _catalog_surface_context(inverter)
    if surface is not None and surface.read_only and inverter.variant_key == "family_fallback":
        reasons.append("family_fallback_variant")
    elif candidates and all(
        candidate.provenance_confidence == "rule-migrated"
        for candidate in candidates
    ):
        reasons.append("unverified_variant")
    elif (
        (surface is not None and surface.read_only)
        or str(inverter.profile_name or "").strip().endswith("/family_fallback.json")
    ):
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


def _catalog_surface_context(
    inverter: DetectedInverter,
) -> tuple[
    CompiledSurfaceDescriptor | None,
    tuple[CompiledDeviceDescriptor, ...],
]:
    catalog = load_compiled_detection_catalog()
    surfaces = tuple(
        surface
        for surface in catalog.surfaces.values()
        if surface.driver_key == inverter.driver_key
        and surface.variant_key == inverter.variant_key
        and surface.profile_name == inverter.profile_name
        and (
            not inverter.register_schema_name
            or surface.register_schema_name == inverter.register_schema_name
        )
    )
    if len(surfaces) != 1:
        return None, ()
    surface = surfaces[0]
    candidates = tuple(
        catalog.devices[key]
        for key in catalog.devices_by_surface.get(surface.key, ())
        if key in catalog.devices
    )
    return surface, candidates
