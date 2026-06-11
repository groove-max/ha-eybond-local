"""Helpers for entity-platform setup before the first runtime snapshot is ready."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .const import (
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DRIVER_HINT,
    DRIVER_HINT_AUTO,
)
from .models import CapabilityPreset, WriteCapability
from .drivers.registry import get_driver


def persisted_inverter_identity(entry: Any) -> bool:
    """Return whether persisted config-entry data already knows the inverter."""

    data = getattr(entry, "data", {}) or {}
    return bool(
        str(data.get(CONF_DETECTED_MODEL) or "").strip()
        or str(data.get(CONF_DETECTED_SERIAL) or "").strip()
    )


def persisted_driver(entry: Any):
    """Return the persisted driver hint when runtime detection is still warming up."""

    data = getattr(entry, "data", {}) or {}
    options = getattr(entry, "options", {}) or {}
    driver_hint = str(
        options.get(CONF_DRIVER_HINT, data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO))
        or DRIVER_HINT_AUTO
    ).strip()
    if not driver_hint or driver_hint == DRIVER_HINT_AUTO:
        return None
    try:
        return get_driver(driver_hint)
    except KeyError:
        return None


@dataclass(frozen=True, slots=True)
class _SnapshotBackedInverterMetadata:
    driver_key: str
    protocol_family: str
    snapshot_backed: bool = True
    name: str = ""
    model_name: str = ""
    serial_number: str = ""
    profile_name: str = ""
    register_schema_name: str = ""
    capability_groups: tuple[Any, ...] = ()
    capabilities: tuple[WriteCapability, ...] = ()
    capability_presets: tuple[CapabilityPreset, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)

    def get_capability(self, capability_key: str) -> WriteCapability:
        for capability in self.capabilities:
            if capability.key == capability_key:
                return capability
        raise KeyError(capability_key)

    def get_capability_preset(self, preset_key: str) -> CapabilityPreset:
        for preset in self.capability_presets:
            if preset.key == preset_key:
                return preset
        raise KeyError(preset_key)

    @property
    def key(self) -> str:
        return self.driver_key

    @property
    def write_capabilities(self) -> tuple[WriteCapability, ...]:
        return self.capabilities


def _snapshot_backed_inverter_metadata(coordinator: Any):
    snapshot = getattr(coordinator, "effective_metadata_snapshot", None)
    if snapshot is None or not bool(getattr(snapshot, "is_valid", False)):
        return None

    effective_metadata = getattr(coordinator, "effective_metadata", None)
    profile_metadata = getattr(effective_metadata, "profile_metadata", None)
    register_schema_metadata = getattr(effective_metadata, "register_schema_metadata", None)
    if profile_metadata is None or register_schema_metadata is None:
        return None

    driver_key = str(
        getattr(effective_metadata, "effective_owner_key", "")
        or getattr(profile_metadata, "driver_key", "")
        or ""
    ).strip()
    protocol_family = str(getattr(profile_metadata, "protocol_family", "") or "").strip()
    if not driver_key:
        return None

    return _SnapshotBackedInverterMetadata(
        driver_key=driver_key,
        protocol_family=protocol_family,
        profile_name=str(getattr(effective_metadata, "profile_name", "") or "").strip(),
        register_schema_name=str(
            getattr(effective_metadata, "register_schema_name", "") or ""
        ).strip(),
        capability_groups=tuple(getattr(profile_metadata, "groups", ()) or ()),
        capabilities=tuple(getattr(profile_metadata, "capabilities", ()) or ()),
        capability_presets=tuple(getattr(profile_metadata, "presets", ()) or ()),
    )


def entity_setup_context(entry: Any, coordinator: Any):
    """Resolve driver, inverter, and identity for entity-platform construction."""

    inverter = getattr(coordinator, "identified_inverter", None)
    driver = getattr(coordinator, "current_driver", None)
    if inverter is None:
        snapshot_inverter = _snapshot_backed_inverter_metadata(coordinator)
        if snapshot_inverter is not None:
            inverter = snapshot_inverter
            try:
                snapshot_driver = get_driver(snapshot_inverter.driver_key)
            except KeyError:
                snapshot_driver = None
            if snapshot_driver is not None and (
                driver is None or getattr(driver, "key", "") != snapshot_driver.key
            ):
                driver = snapshot_driver
            elif driver is None:
                driver = snapshot_inverter
    if driver is None:
        driver = persisted_driver(entry)

    has_inverter_identity = bool(
        getattr(coordinator, "has_inverter_identity", False)
        or (inverter is not None and not getattr(inverter, "snapshot_backed", False))
        or persisted_inverter_identity(entry)
    )
    inverter = _merge_active_device_overlay(coordinator, inverter)
    return driver, inverter, has_inverter_identity


def _merge_active_device_overlay(coordinator: Any, inverter: Any):
    """Ensure the inverter that platforms set up from carries the activated learned controls.

    The runtime inverter is detected against built-in bindings (and the snapshot-backed
    fallback can be built before the overlay resolves active), so its capabilities may not
    include the activated device-scoped learned controls at the moment entities are created
    -- and platforms set up entities only once. Merging here, at the single point every
    platform reads, makes the learned controls materialize regardless of detection timing.
    Idempotent and a no-op when no overlay is active; failures never block entity setup.
    """

    if inverter is None:
        return inverter
    applier = getattr(coordinator, "_apply_device_overlay_to_inverter", None)
    if not callable(applier):
        return inverter
    collector = getattr(getattr(coordinator, "data", None), "collector", None)
    try:
        merged = applier(inverter, collector)
    except Exception:  # pragma: no cover - defensive; never block entity setup
        return inverter
    return merged if merged is not None else inverter
