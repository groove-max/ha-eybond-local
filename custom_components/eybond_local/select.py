"""Select platform for writable runtime settings and enum inverter capabilities."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .runtime.coordinator import EybondLocalCoordinator
from .models import WriteCapability
from .platform_context import entity_setup_context
from .schema import serialize_capability

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _RuntimeSelectSpec:
    key: str
    translation_key: str
    name: str
    options: tuple[str, ...]
    device_scope: str
    enabled_default: bool = True


def runtime_select_keys_for_runtime(*, has_inverter_identity: bool = True) -> tuple[str, ...]:
    """Return non-capability integration setting select keys exposed for every runtime."""

    return tuple(spec.key for spec in _runtime_select_specs(has_inverter_identity=has_inverter_identity))


def default_enabled_runtime_select_keys_for_runtime(
    *,
    has_inverter_identity: bool = True,
) -> tuple[str, ...]:
    """Return non-capability integration setting select keys enabled by default."""

    return tuple(
        spec.key
        for spec in _runtime_select_specs(has_inverter_identity=has_inverter_identity)
        if spec.enabled_default
    )


def _runtime_select_specs(*, has_inverter_identity: bool = True) -> tuple[_RuntimeSelectSpec, ...]:
    del has_inverter_identity
    # CP2A: the writable collector operation-mode select was removed. Changing
    # the connection method is now exclusively the options-flow strategy
    # transition (with mandatory risk consent); the operation mode is a
    # read-only projection of the canonical connection strategy, never a
    # standalone writable authority. No runtime setting select is exposed.
    return ()


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create select entities for runtime settings and enum write capabilities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    # Without an inverter identity, capability selects would describe a
    # phantom inverter on the collector device (manual driver hint, nothing
    # attached yet). The entry reloads once detection persists an identity.
    capabilities = (
        (
            inverter.capabilities
            if inverter is not None
            else (driver.write_capabilities if driver is not None else ())
        )
        if has_inverter_identity
        else ()
    )
    runtime_specs = _runtime_select_specs(has_inverter_identity=has_inverter_identity)
    exposable_capabilities = tuple(
        capability
        for capability in capabilities
        if capability.value_kind == "enum" and capability.enum_value_map
        if coordinator.can_expose_capability(capability)
    )
    if has_inverter_identity and not exposable_capabilities:
        enum_capabilities = tuple(
            capability
            for capability in capabilities
            if capability.value_kind == "enum" and capability.enum_value_map
        )
        exposure_context = {}
        context_getter = getattr(coordinator, "_write_exposure_context", None)
        if callable(context_getter):
            try:
                exposure_context = context_getter()
            except Exception as exc:  # pragma: no cover - diagnostic only
                exposure_context = {"error": f"{type(exc).__name__}:{exc}"}
        _LOGGER.debug(
            "EyeBond select setup has inverter identity but no enum controls: entry=%s driver=%s inverter=%s capabilities=%d enum_capabilities=%d controls_enabled=%s reason=%s context=%s first_enum=%s first_enum_allowed=%s",
            entry.entry_id,
            getattr(driver, "key", None),
            getattr(inverter, "model_name", None),
            len(tuple(capabilities or ())),
            len(enum_capabilities),
            getattr(coordinator, "controls_enabled", None),
            getattr(coordinator, "controls_reason", None),
            exposure_context,
            getattr(enum_capabilities[0], "key", None) if enum_capabilities else None,
            coordinator.can_expose_capability(enum_capabilities[0]) if enum_capabilities else None,
        )
    async_add_entities(
        [
            *[
                EybondRuntimeSettingSelect(coordinator, spec)
                for spec in runtime_specs
            ],
            *[
                EybondCapabilitySelect(coordinator, capability)
                for capability in exposable_capabilities
            ],
        ]
    )


class EybondRuntimeSettingSelect(CoordinatorEntity[EybondLocalCoordinator], SelectEntity):
    """One integration setting select backed by config-entry state."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        spec: _RuntimeSelectSpec,
    ) -> None:
        super().__init__(coordinator)
        self._spec = spec

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_select_{spec.key}"
        self._attr_name = spec.name
        self._attr_translation_key = spec.translation_key
        self._attr_options = list(spec.options)
        self._attr_entity_registry_enabled_default = spec.enabled_default

    @property
    def device_info(self):
        if self._spec.device_scope == "collector":
            return self.coordinator.collector_device_info()
        return self.coordinator.inverter_device_info()

    @property
    def available(self) -> bool:
        return True

    @property
    def current_option(self) -> str | None:
        if self._spec.key == "control_mode":
            return self.coordinator.control_mode
        return None

    @property
    def extra_state_attributes(self) -> dict[str, object]:
        return {
            "setting_scope": "integration",
            "write_enabled": True,
            "controls_enabled": self.coordinator.controls_enabled,
            "control_policy_reason": self.coordinator.controls_reason,
            "control_policy_summary": self.coordinator.controls_summary,
        }

    async def async_select_option(self, option: str) -> None:
        if self._spec.key == "control_mode":
            await self.coordinator.async_set_control_mode(option)
            return
        raise ValueError(f"unknown_runtime_select:{self._spec.key}")


class EybondCapabilitySelect(CoordinatorEntity[EybondLocalCoordinator], SelectEntity):
    """One writable enum capability backed by the driver capability map."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG
    _attr_entity_registry_enabled_default = False

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        capability: WriteCapability,
    ) -> None:
        super().__init__(coordinator)
        self._capability = capability

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_select_{capability.key}"
        self._attr_name = capability.display_name
        self._attr_options = capability.enum_options
        self._attr_entity_registry_enabled_default = coordinator.capability_enabled_by_default(
            capability
        )

    @property
    def device_info(self):
        return self.coordinator.inverter_device_info()

    @property
    def available(self) -> bool:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if not snapshot.connected or inverter is None:
            return False
        if not any(cap.key == self._capability.key for cap in inverter.capabilities):
            return False
        return self._capability.runtime_state(snapshot.values).visible

    @property
    def current_option(self) -> str | None:
        value = self.coordinator.data.values.get(self._capability.value_key)
        return value if isinstance(value, str) else None

    @property
    def extra_state_attributes(self) -> dict[str, object]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_capability(self._capability, inverter, snapshot.values)

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_write_capability(self._capability.key, option)
