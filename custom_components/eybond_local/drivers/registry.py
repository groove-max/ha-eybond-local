"""Driver registry and helper functions."""

from __future__ import annotations

from dataclasses import replace

from ..canonical_telemetry import all_canonical_measurements, canonical_measurements_for_driver
from ..const import DRIVER_HINT_AUTO
from ..entity_descriptions import (
    BASE_BINARY_SENSOR_DESCRIPTIONS,
    BASE_SENSOR_DESCRIPTIONS,
    merge_descriptions,
)
from ..metadata.model_binding_catalog_loader import load_driver_model_binding_catalog
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..metadata.smartess_protocol_catalog_loader import load_smartess_protocol_catalog
from ..models import (
    BinarySensorDescription,
    CapabilityGroup,
    CapabilityPreset,
    MeasurementDescription,
    WriteCapability,
)
from .base import InverterDriver
from .pi18 import Pi18Driver
from .pi30 import Pi30Driver
from .smg import SmgModbusDriver

_DRIVERS: tuple[InverterDriver, ...] = (
    SmgModbusDriver(),
    Pi30Driver(),
)

_EXPERIMENTAL_REPLAY_DRIVERS: tuple[InverterDriver, ...] = (
    Pi18Driver(),
)


def driver_options() -> list[str]:
    """Return user-facing driver hints."""

    return [DRIVER_HINT_AUTO, *[driver.key for driver in _DRIVERS]]


def iter_drivers(driver_hint: str) -> tuple[InverterDriver, ...]:
    """Return the ordered driver probe list."""

    if driver_hint != DRIVER_HINT_AUTO:
        return tuple(driver for driver in _DRIVERS if driver.key == driver_hint)
    return _DRIVERS


def get_driver(driver_key: str) -> InverterDriver:
    """Return one registered driver by key."""

    for driver in _DRIVERS:
        if driver.key == driver_key:
            return driver
    raise KeyError(driver_key)


def iter_replay_drivers(driver_hint: str) -> tuple[InverterDriver, ...]:
    """Return the offline replay probe list including experimental-only drivers."""

    replay_drivers = _DRIVERS + _EXPERIMENTAL_REPLAY_DRIVERS
    if driver_hint != DRIVER_HINT_AUTO:
        return tuple(driver for driver in replay_drivers if driver.key == driver_hint)
    return replay_drivers


def get_replay_driver(driver_key: str) -> InverterDriver:
    """Return one registered or replay-only experimental driver by key."""

    for driver in _DRIVERS + _EXPERIMENTAL_REPLAY_DRIVERS:
        if driver.key == driver_key:
            return driver
    raise KeyError(driver_key)


def measurements_for_driver(driver_key: str | None = None) -> tuple[MeasurementDescription, ...]:
    """Return shared measurements plus those for one driver when specified."""

    driver_measurements = [BASE_SENSOR_DESCRIPTIONS]
    if driver_key is None:
        driver_measurements.extend(driver.measurements for driver in _DRIVERS)
        driver_measurements.append(all_canonical_measurements())
        write_capabilities = all_write_capabilities()
    else:
        driver_measurements.append(get_driver(driver_key).measurements)
        driver_measurements.append(canonical_measurements_for_driver(driver_key))
        write_capabilities = get_driver(driver_key).write_capabilities
    return _promote_readback_defaults(
        merge_descriptions(*driver_measurements),
        write_capabilities,
    )


def _promote_readback_defaults(
    measurements: tuple[MeasurementDescription, ...],
    write_capabilities: tuple[WriteCapability, ...],
) -> tuple[MeasurementDescription, ...]:
    """Enable readback sensors by default when the matching value can be written."""

    default_readback_keys = {
        capability.value_key
        for capability in write_capabilities
        if capability.value_kind != "action"
    }
    if not default_readback_keys:
        return measurements
    return tuple(
        replace(description, enabled_default=True)
        if description.key in default_readback_keys and not description.enabled_default
        else description
        for description in measurements
    )


def binary_sensors_for_driver(driver_key: str | None = None) -> tuple[BinarySensorDescription, ...]:
    """Return shared binary sensors plus those for one driver when specified."""

    driver_binary_sensors = [BASE_BINARY_SENSOR_DESCRIPTIONS]
    if driver_key is None:
        driver_binary_sensors.extend(driver.binary_sensors for driver in _DRIVERS)
    else:
        driver_binary_sensors.append(get_driver(driver_key).binary_sensors)
    return merge_descriptions(*driver_binary_sensors)


def all_measurements() -> tuple[MeasurementDescription, ...]:
    """Return the union of shared and driver-specific measurements."""

    return measurements_for_driver()


def all_binary_sensors() -> tuple[BinarySensorDescription, ...]:
    """Return the union of shared and driver-specific binary sensors."""

    return binary_sensors_for_driver()


def all_write_capabilities() -> tuple[WriteCapability, ...]:
    """Return the union of declared write capabilities keyed by capability key."""

    merged: dict[str, WriteCapability] = {}
    for driver in _DRIVERS:
        for capability in driver.write_capabilities:
            merged.setdefault(capability.key, capability)
    return tuple(
        sorted(
            merged.values(),
            key=lambda capability: (capability.group, capability.order, capability.display_name),
        )
    )


def all_capability_groups() -> tuple[CapabilityGroup, ...]:
    """Return the union of declared capability groups keyed by group key."""

    merged: dict[str, CapabilityGroup] = {}
    for driver in _DRIVERS:
        for group in driver.capability_groups:
            merged.setdefault(group.key, group)
    return tuple(sorted(merged.values(), key=lambda group: (group.order, group.title)))


def all_capability_presets() -> tuple[CapabilityPreset, ...]:
    """Return the union of declared capability presets keyed by preset key."""

    merged: dict[str, CapabilityPreset] = {}
    for driver in _DRIVERS:
        for preset in driver.capability_presets:
            merged.setdefault(preset.key, preset)
    return tuple(sorted(merged.values(), key=lambda preset: (preset.group, preset.order, preset.title)))


def prime_metadata_caches() -> None:
    """Warm profile/schema-backed metadata before async startup code touches it."""

    all_measurements()
    all_binary_sensors()
    all_write_capabilities()
    all_capability_groups()
    all_capability_presets()
    _prime_catalog_driven_metadata()


def _prime_catalog_driven_metadata() -> None:
    """Warm JSON-backed metadata that is only reached through runtime catalogs."""

    for binding in load_driver_model_binding_catalog().bindings.values():
        if binding.profile_name:
            load_driver_profile(binding.profile_name)
        if binding.register_schema_name:
            load_register_schema(binding.register_schema_name)

    for protocol in load_smartess_protocol_catalog().protocols.values():
        for profile_name in (protocol.raw_profile_name, protocol.profile_name):
            if profile_name:
                load_driver_profile(profile_name)
        for schema_name in (
            protocol.raw_register_schema_name,
            protocol.register_schema_name,
        ):
            if schema_name:
                load_register_schema(schema_name)
