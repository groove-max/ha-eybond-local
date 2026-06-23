"""Text platform for collector-scoped configuration strings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .collector_endpoint import normalize_collector_server_endpoint
from .const import CONTROL_MODE_FULL

try:
    from homeassistant.components.text import TextEntity
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.helpers.entity import EntityCategory
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.update_coordinator import CoordinatorEntity
except ModuleNotFoundError:  # Local tooling imports this module without Home Assistant installed.
    class TextEntity:
        pass

    class ConfigEntry:
        pass

    class EntityCategory:
        CONFIG = "config"

    class AddEntitiesCallback:
        pass

    class CoordinatorEntity:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, coordinator):
            self.coordinator = coordinator
if TYPE_CHECKING:
    from .runtime.coordinator import EybondLocalCoordinator
else:
    EybondLocalCoordinator = Any


@dataclass(frozen=True, slots=True)
class _CollectorTextSpec:
    key: str
    translation_key: str
    name: str
    icon: str
    enabled_default: bool = False


def collector_text_keys_for_runtime() -> tuple[str, ...]:
    """Return collector text entity keys exposed for the current runtime."""

    return tuple(spec.key for spec in _collector_text_specs_for_runtime())


def default_enabled_collector_text_keys_for_runtime() -> tuple[str, ...]:
    """Return collector text entity keys that should be enabled by default."""

    return tuple(spec.key for spec in _collector_text_specs_for_runtime() if spec.enabled_default)


def _collector_text_specs_for_runtime() -> tuple[_CollectorTextSpec, ...]:
    return (
        _CollectorTextSpec(
            key="collector_callback_endpoint",
            translation_key="collector_callback_endpoint",
            name="Collector Callback Endpoint Override",
            icon="mdi:lan-pending",
            enabled_default=False,
        ),
    )


def _collector_text_availability_reason(coordinator: EybondLocalCoordinator) -> str | None:
    lock_reason = getattr(coordinator, "collector_configuration_lock_reason", lambda: None)()
    if lock_reason is not None:
        return lock_reason
    if not coordinator.data.connected:
        return "Collector is not connected."
    if coordinator.control_mode == CONTROL_MODE_FULL:
        return None
    return "Requires Full Control."


def _collector_text_write_reason(coordinator: EybondLocalCoordinator) -> str | None:
    lock_reason = getattr(coordinator, "collector_configuration_lock_reason", lambda: None)()
    if lock_reason is not None:
        return lock_reason
    if not coordinator.data.connected:
        return "Collector is not connected."
    if coordinator.control_mode != CONTROL_MODE_FULL:
        return "Requires Full Control for write access."
    return None


def _normalize_endpoint_value(value: str) -> str:
    try:
        return normalize_collector_server_endpoint(
            value,
            require_explicit_port=False,
            require_explicit_protocol=False,
            require_tcp=True,
            preserve_shape=True,
        )
    except ValueError as exc:
        message = str(exc)
        if "port" in message:
            raise ValueError("collector_endpoint_port_invalid") from exc
        raise ValueError("collector_endpoint_format_invalid") from exc


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create collector-scoped text entities."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    async_add_entities(
        EybondCollectorText(coordinator, spec)
        for spec in _collector_text_specs_for_runtime()
    )


class EybondCollectorText(CoordinatorEntity[EybondLocalCoordinator], TextEntity):
    """One collector-scoped writable text entity."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        spec: _CollectorTextSpec,
    ) -> None:
        super().__init__(coordinator)
        self._spec = spec

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_text_{spec.key}"
        self._attr_name = spec.name
        self._attr_icon = spec.icon
        self._attr_translation_key = spec.translation_key
        self._attr_entity_registry_enabled_default = spec.enabled_default

    @property
    def device_info(self):
        return self.coordinator.collector_device_info()

    @property
    def available(self) -> bool:
        return _collector_text_availability_reason(self.coordinator) is None

    @property
    def native_value(self) -> str:
        values = self.coordinator.data.values
        value = (
            values.get("collector_callback_endpoint_pending")
            or values.get("collector_server_endpoint")
        )
        return str(value or "")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        availability_reason = _collector_text_availability_reason(self.coordinator)
        write_reason = _collector_text_write_reason(self.coordinator)
        values = self.coordinator.data.values
        return {
            "action_scope": "collector",
            "apply_required": True,
            "pending_apply_action": "apply_collector_changes",
            "requires_control_mode": "full_for_write",
            "expert_action": True,
            "control_surface": "raw_parameter_21_override",
            "current_callback_endpoint": values.get("collector_server_endpoint"),
            "pending_callback_endpoint": values.get("collector_callback_endpoint_pending"),
            "pending_apply_required": bool(
                values.get("collector_callback_endpoint_pending_apply_required")
            ),
            "target_callback_endpoint": self.coordinator.collector_callback_target_endpoint,
            "collector_cloud_family": getattr(
                self.coordinator,
                "collector_cloud_family",
                values.get("collector_cloud_family", ""),
            ),
            "collector_cloud_family_source": values.get("collector_cloud_family_source"),
            "collector_cloud_family_confidence": values.get("collector_cloud_family_confidence"),
            "availability_reason": availability_reason or "Ready",
            "read_only": availability_reason is None and write_reason is not None,
            "write_enabled": write_reason is None,
            "action_summary": (
                "Expert override for raw collector parameter 21. Use Bind Collector to Home Assistant or Restore SmartESS Access for normal callback changes."
            ),
        }

    async def async_set_value(self, value: str) -> None:
        write_reason = _collector_text_write_reason(self.coordinator)
        if write_reason is not None:
            raise PermissionError(write_reason)
        normalized_value = _normalize_endpoint_value(value)
        await self.coordinator.async_set_raw_collector_server_endpoint(
            endpoint=normalized_value,
            apply_changes=False,
            confirm_redirect=True,
        )
