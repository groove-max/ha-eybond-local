"""Button platform for declarative inverter presets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .runtime.coordinator import EybondLocalCoordinator
from .models import CapabilityPreset, WriteCapability
from .schema import serialize_capability, serialize_preset
from .tooling import supports_clock_sync, tooling_button_keys_for_runtime


@dataclass(frozen=True, slots=True)
class _ToolingButtonSpec:
    key: str
    name: str
    icon: str
    entity_category: EntityCategory = EntityCategory.DIAGNOSTIC
    enabled_default: bool = False


async def async_setup_entry(
    hass,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create button entities for declarative presets."""

    coordinator: EybondLocalCoordinator = entry.runtime_data
    driver = coordinator.current_driver
    inverter = coordinator.data.inverter
    capabilities = (
        inverter.capabilities if inverter is not None else (driver.write_capabilities if driver is not None else ())
    )
    capability_keys = {capability.key for capability in capabilities}
    profile_name = getattr(inverter, "profile_name", "") or coordinator.effective_profile_name
    presets = (
        inverter.capability_presets if inverter is not None else (driver.capability_presets if driver is not None else ())
    )
    async_add_entities(
        [
            *[
                EybondToolingButton(coordinator, spec)
                for spec in _tooling_button_specs_for_runtime(capability_keys, profile_name)
            ],
            *[
                EybondPresetButton(coordinator, preset)
                for preset in presets
                if coordinator.can_expose_preset(preset)
            ],
            *[
                EybondCapabilityButton(coordinator, capability)
                for capability in capabilities
                if capability.value_kind == "action"
                if coordinator.can_expose_capability(capability)
            ],
        ]
    )


def _tooling_button_specs() -> tuple[_ToolingButtonSpec, ...]:
    return (
        _ToolingButtonSpec(
            key="create_support_package",
            name="Create Support Archive",
            icon="mdi:package-variant-closed",
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="export_support_bundle",
            name="Export Support Bundle",
            icon="mdi:archive-arrow-down-outline",
        ),
        _ToolingButtonSpec(
            key="reload_local_metadata",
            name="Reload Local Metadata",
            icon="mdi:reload",
        ),
        _ToolingButtonSpec(
            key="create_local_profile_draft",
            name="Create Local Profile Draft",
            icon="mdi:file-document-edit-outline",
        ),
        _ToolingButtonSpec(
            key="create_local_schema_draft",
            name="Create Local Register Schema Draft",
            icon="mdi:file-tree-outline",
        ),
        _ToolingButtonSpec(
            key="sync_inverter_clock",
            name="Sync Inverter Clock",
            icon="mdi:clock-sync",
            entity_category=EntityCategory.CONFIG,
            enabled_default=True,
        ),
    )


def _tooling_button_specs_for_runtime(
    capability_keys: set[str] | frozenset[str],
    profile_name: str,
) -> tuple[_ToolingButtonSpec, ...]:
    allowed_keys = set(tooling_button_keys_for_runtime(capability_keys, profile_name))
    return tuple(spec for spec in _tooling_button_specs() if spec.key in allowed_keys)


def _clock_sync_capabilities(coordinator: EybondLocalCoordinator) -> tuple[WriteCapability, WriteCapability] | None:
    inverter = coordinator.data.inverter
    if inverter is None:
        return None
    try:
        date_capability = inverter.get_capability("inverter_date_write")
        time_capability = inverter.get_capability("inverter_time_write")
    except KeyError:
        return None
    return date_capability, time_capability


class EybondPresetButton(CoordinatorEntity[EybondLocalCoordinator], ButtonEntity):
    """One preset button backed by the declarative preset schema."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        preset: CapabilityPreset,
    ) -> None:
        super().__init__(coordinator)
        self._preset = preset

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_preset_{preset.key}"
        self._attr_name = f"Apply {preset.title}"
        self._attr_icon = preset.icon
        self._attr_entity_registry_enabled_default = not preset.advanced

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if not snapshot.connected or inverter is None:
            return False
        if not any(preset.key == self._preset.key for preset in inverter.capability_presets):
            return False
        return self._preset.runtime_state(inverter, snapshot.values).visible

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_preset(self._preset, inverter, snapshot.values)

    async def async_press(self) -> None:
        await self.coordinator.async_apply_preset(self._preset.key)


class EybondCapabilityButton(CoordinatorEntity[EybondLocalCoordinator], ButtonEntity):
    """One one-shot action capability backed by the driver capability map."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        capability: WriteCapability,
    ) -> None:
        super().__init__(coordinator)
        self._capability = capability

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_button_{capability.key}"
        self._attr_name = capability.display_name
        self._attr_entity_registry_enabled_default = capability.enabled_default

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if not snapshot.connected or inverter is None:
            return False
        if not any(cap.key == self._capability.key for cap in inverter.capabilities):
            return False
        runtime_state = self._capability.runtime_state(snapshot.values)
        return runtime_state.visible and runtime_state.editable

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        snapshot = self.coordinator.data
        inverter = snapshot.inverter
        if inverter is None:
            return {}
        return serialize_capability(self._capability, inverter, snapshot.values)

    async def async_press(self) -> None:
        await self.coordinator.async_write_capability(self._capability.key, None)


class EybondToolingButton(CoordinatorEntity[EybondLocalCoordinator], ButtonEntity):
    """One integration-level diagnostic/tooling button."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: EybondLocalCoordinator,
        spec: _ToolingButtonSpec,
    ) -> None:
        super().__init__(coordinator)
        self._spec = spec

        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_tool_{spec.key}"
        self._attr_name = spec.name
        self._attr_icon = spec.icon
        self._attr_entity_category = spec.entity_category
        self._attr_entity_registry_enabled_default = spec.enabled_default

    @property
    def device_info(self):
        return self.coordinator.device_info()

    @property
    def available(self) -> bool:
        if self._spec.key == "create_support_package":
            return True
        if self._spec.key == "export_support_bundle":
            return True
        if self._spec.key == "reload_local_metadata":
            return True
        if self._spec.key == "create_local_profile_draft":
            return bool(self.coordinator.effective_profile_name)
        if self._spec.key == "create_local_schema_draft":
            return bool(self.coordinator.effective_register_schema_name)
        if self._spec.key == "sync_inverter_clock":
            snapshot = self.coordinator.data
            if not snapshot.connected:
                return False
            inverter = snapshot.inverter
            profile_name = getattr(inverter, "profile_name", "") or self.coordinator.effective_profile_name
            capabilities = _clock_sync_capabilities(self.coordinator)
            capability_keys = {capability.key for capability in capabilities} if capabilities is not None else set()
            if not supports_clock_sync(capability_keys, profile_name):
                return False
            if capabilities is None:
                return False
            for capability in capabilities:
                if not self.coordinator.can_expose_capability(capability):
                    return False
                runtime_state = capability.runtime_state(snapshot.values)
                if not (runtime_state.visible and runtime_state.editable):
                    return False
            return True
        return True

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        values = self.coordinator.data.values
        attributes = {
            "tool_key": self._spec.key,
            "profile_name": self.coordinator.effective_profile_name,
            "register_schema_name": self.coordinator.effective_register_schema_name,
            "support_package_path": values.get("support_package_path"),
            "support_package_download_path": values.get("support_package_download_path"),
            "support_package_download_url": values.get("support_package_download_url"),
            "support_package_download_relative_url": values.get("support_package_download_relative_url"),
            "support_bundle_path": values.get("support_bundle_path"),
            "cloud_evidence_path": values.get("cloud_evidence_path"),
            "local_profile_draft_path": values.get("local_profile_draft_path"),
            "local_schema_draft_path": values.get("local_schema_draft_path"),
            "local_metadata_status": values.get("local_metadata_status"),
            "support_workflow_level": values.get("support_workflow_level"),
            "support_workflow_level_label": values.get("support_workflow_level_label"),
            "support_workflow_summary": values.get("support_workflow_summary"),
            "support_workflow_next_action": values.get("support_workflow_next_action"),
            "support_workflow_primary_action": values.get("support_workflow_primary_action"),
            "support_workflow_step_1": values.get("support_workflow_step_1"),
            "support_workflow_step_2": values.get("support_workflow_step_2"),
            "support_workflow_step_3": values.get("support_workflow_step_3"),
            "support_workflow_plan": values.get("support_workflow_plan"),
            "support_workflow_advanced_hint": values.get("support_workflow_advanced_hint"),
        }
        if self._spec.key == "sync_inverter_clock":
            now = dt_util.now().replace(microsecond=0)
            attributes["target_inverter_date"] = now.strftime("%Y-%m-%d")
            attributes["target_inverter_time"] = now.strftime("%H:%M:%S")
            attributes["current_inverter_date"] = values.get("inverter_date")
            attributes["current_inverter_time"] = values.get("inverter_time")
        return attributes

    async def async_press(self) -> None:
        if self._spec.key == "create_support_package":
            await self.coordinator.async_export_support_package()
            return
        if self._spec.key == "export_support_bundle":
            await self.coordinator.async_export_support_bundle()
            return
        if self._spec.key == "create_local_profile_draft":
            await self.coordinator.async_create_local_profile_draft()
            return
        if self._spec.key == "create_local_schema_draft":
            await self.coordinator.async_create_local_schema_draft()
            return
        if self._spec.key == "reload_local_metadata":
            await self.coordinator.async_reload_local_metadata()
            return
        if self._spec.key == "sync_inverter_clock":
            await self.coordinator.async_sync_inverter_clock()
            return
        raise ValueError(f"unknown_tool_button:{self._spec.key}")
