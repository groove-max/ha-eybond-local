"""Driver registry and helper functions."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from ..canonical_telemetry import all_canonical_measurements, canonical_measurements_for_driver
from ..collector.entity_scope import is_collector_entity_key
from ..const import DRIVER_HINT_AUTO
from ..entity_descriptions import (
    BASE_BINARY_SENSOR_DESCRIPTIONS,
    BASE_SENSOR_DESCRIPTIONS,
    merge_descriptions,
)
from ..metadata.collector_cloud_profile_catalog_loader import (
    load_collector_cloud_profile_catalog,
)
from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.device_catalog_loader import load_device_catalog
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..metadata.smartess_protocol_catalog_loader import load_smartess_protocol_catalog
from ..metadata.smartess_semantic_catalog_loader import load_smartess_semantic_catalog
from ..models import (
    BinarySensorDescription,
    CapabilityGroup,
    CapabilityPreset,
    MeasurementDescription,
    WriteCapability,
    decimals_for_divisor,
)
from ..poll_policy import DEFAULT_POLL_POLICY, PollPolicy
from .base import InverterDriver
from .modbus_catalog import ModbusCatalogDriver
from .must import MustPvPh18Driver
from .pi18 import Pi18Driver
from .pi30 import Pi30Driver
from .smartess_local import SmartEssLocalDriver
from .smg import SmgModbusDriver
from .srne import SrneModbusDriver
from .eybond_g_ascii import EybondGAsciiDriver

if TYPE_CHECKING:
    from .support_marker import DriverSupportMarker

_DRIVERS: tuple[InverterDriver, ...] = (
    SmgModbusDriver(),
    SrneModbusDriver(),
    MustPvPh18Driver(),
    ModbusCatalogDriver(),
    Pi30Driver(),
    EybondGAsciiDriver(),
    SmartEssLocalDriver(),
    Pi18Driver(),
)

_EXPERIMENTAL_REPLAY_DRIVERS: tuple[InverterDriver, ...] = ()

_COLLECTOR_ONLY_BASE_SENSOR_EXTRA_KEYS: frozenset[str] = frozenset({"last_error"})


def serial_is_stable(driver_key: object, inverter=None) -> bool:
    """Dispatch the stable-serial question to the OWNING driver.

    The registry holds no variant allow/deny set: whether a model exposes a
    stable device serial is the selected driver's own policy
    (``InverterDriver.serial_is_stable``, default ``True``). An unknown driver
    key keeps the neutral default.
    """

    key = str(driver_key or "").strip()
    if not key:
        return True
    try:
        driver = get_driver(key)
    except Exception:
        return True
    return bool(driver.serial_is_stable(inverter))


def support_marker(
    driver_key: object,
    *,
    variant_key: str = "",
    profile_name: str = "",
) -> "DriverSupportMarker | None":
    """Dispatch the special support-state marker to the OWNING driver.

    The registry holds no model allow list: whether a detected identity is in a
    special support state (e.g. an SMG read-only unverified fallback) is the
    selected driver's own policy (``InverterDriver.support_marker``, default
    ``None``). An unknown or empty driver key yields no marker.

    Only an unknown driver key (``KeyError``) is treated as "no marker". Any
    error raised by an existing driver's ``support_marker`` implementation
    propagates -- it is a real defect and must not be silently swallowed.
    """

    key = str(driver_key or "").strip()
    if not key:
        return None
    try:
        driver = get_driver(key)
    except KeyError:
        return None
    return driver.support_marker(variant_key=variant_key, profile_name=profile_name)


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


def poll_policy_for_driver_key(driver_key: object, inverter=None) -> PollPolicy:
    """Resolve the adaptive polling policy for a driver key (composition point).

    The runtime holds no catalog of driver policies: it passes the driver key
    (and optionally the detected model) here, and each driver declares its own
    policy via ``poll_policy_for``. Before a driver is known -- ``auto`` or an
    unregistered key -- the neutral default applies.
    """

    key = str(driver_key or "").strip()
    if not key or key == DRIVER_HINT_AUTO:
        return DEFAULT_POLL_POLICY
    try:
        driver = get_driver(key)
    except KeyError:
        return DEFAULT_POLL_POLICY
    return driver.poll_policy_for(inverter)


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


def measurements_for_runtime(
    *,
    driver_key: str | None = None,
    register_schema_name: str = "",
    variant_key: str | None = None,
    write_capabilities: tuple[WriteCapability, ...] | None = None,
    include_all_drivers_when_unknown: bool = True,
    collector_only_mode: bool = False,
) -> tuple[MeasurementDescription, ...]:
    """Return measurements for one concrete runtime schema selection."""

    driver_measurements = [
        tuple(
            description
            for description in BASE_SENSOR_DESCRIPTIONS
            if not collector_only_mode
            or is_collector_entity_key(description.key)
            or description.key in _COLLECTOR_ONLY_BASE_SENSOR_EXTRA_KEYS
        )
    ]
    if collector_only_mode:
        # No inverter identity yet (e.g. a manual driver hint on a collector
        # with nothing attached): schema, driver, and canonical measurements
        # describe an inverter that does not exist and would materialize as
        # unavailable entities on the collector device. The entry reloads
        # once detection persists an identity, so nothing is lost.
        return _promote_readback_defaults(
            merge_descriptions(*driver_measurements),
            (),
        )
    if register_schema_name:
        driver_measurements.append(load_register_schema(register_schema_name).measurement_descriptions)
    elif driver_key is None:
        if include_all_drivers_when_unknown:
            driver_measurements.extend(driver.measurements for driver in _DRIVERS)
    else:
        driver_measurements.append(get_driver(driver_key).measurements)

    if driver_key is None:
        if include_all_drivers_when_unknown:
            driver_measurements.append(all_canonical_measurements())
            resolved_write_capabilities = (
                all_write_capabilities() if write_capabilities is None else write_capabilities
            )
        else:
            resolved_write_capabilities = () if write_capabilities is None else write_capabilities
    else:
        driver_measurements.append(
            canonical_measurements_for_driver(driver_key, variant_key=variant_key)
        )
        resolved_write_capabilities = (
            get_driver(driver_key).write_capabilities
            if write_capabilities is None
            else write_capabilities
        )

    return _promote_readback_defaults(
        merge_descriptions(*driver_measurements),
        resolved_write_capabilities,
    )


def measurements_for_driver(driver_key: str | None = None) -> tuple[MeasurementDescription, ...]:
    """Return shared measurements plus those for one driver when specified."""

    return measurements_for_runtime(driver_key=driver_key)


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
    precision_by_key: dict[str, int] = {}
    for capability in write_capabilities:
        if capability.value_kind == "action":
            continue
        precision = _capability_display_precision(capability)
        if precision is None:
            continue
        current = precision_by_key.get(capability.value_key)
        if current is None or precision > current:
            precision_by_key[capability.value_key] = precision

    if not default_readback_keys and not precision_by_key:
        return measurements
    return tuple(
        replace(
            description,
            enabled_default=(description.key in default_readback_keys or description.enabled_default),
            suggested_display_precision=(
                description.suggested_display_precision
                if description.suggested_display_precision is not None
                else precision_by_key.get(description.key)
            ),
        )
        if (
            (description.key in default_readback_keys and not description.enabled_default)
            or (
                description.suggested_display_precision is None
                and description.key in precision_by_key
            )
        )
        else description
        for description in measurements
    )


def _capability_display_precision(capability: WriteCapability) -> int | None:
    """Infer sensor precision from one writable capability's native scale."""

    if capability.divisor:
        precision = decimals_for_divisor(capability.divisor)
        if precision > 0 and 10**precision == capability.divisor:
            return precision
    step = capability.step
    if step is None:
        return None
    text = format(float(step), ".6f").rstrip("0")
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1])


def binary_sensors_for_runtime(
    *,
    driver_key: str | None = None,
    register_schema_name: str = "",
    include_all_drivers_when_unknown: bool = True,
    collector_only_mode: bool = False,
) -> tuple[BinarySensorDescription, ...]:
    """Return binary sensors for one concrete runtime schema selection."""

    driver_binary_sensors = [BASE_BINARY_SENSOR_DESCRIPTIONS]
    if collector_only_mode:
        # Same rule as measurements_for_runtime: without an inverter
        # identity, schema/driver binary sensors describe a phantom device.
        return merge_descriptions(*driver_binary_sensors)
    if register_schema_name:
        driver_binary_sensors.append(load_register_schema(register_schema_name).binary_sensor_descriptions)
    elif driver_key is None:
        if include_all_drivers_when_unknown:
            driver_binary_sensors.extend(driver.binary_sensors for driver in _DRIVERS)
    else:
        driver_binary_sensors.append(get_driver(driver_key).binary_sensors)
    return merge_descriptions(*driver_binary_sensors)


def binary_sensors_for_driver(driver_key: str | None = None) -> tuple[BinarySensorDescription, ...]:
    """Return shared binary sensors plus those for one driver when specified."""

    return binary_sensors_for_runtime(driver_key=driver_key)


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

    # Warm the identity/cloud-profile catalogs too: their first read otherwise
    # lands in the event loop during driver detection (HA's blocking-call
    # warning), which needlessly prolongs startup on slow/throttled hosts.
    load_device_catalog()
    compiled_catalog = load_compiled_detection_catalog()
    load_collector_cloud_profile_catalog()
    load_smartess_semantic_catalog()

    for surface in compiled_catalog.surfaces.values():
        if surface.profile_name:
            load_driver_profile(surface.profile_name)
        if surface.register_schema_name:
            load_register_schema(surface.register_schema_name)

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
