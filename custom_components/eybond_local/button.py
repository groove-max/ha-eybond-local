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

from .collector_endpoint import normalize_collector_server_endpoint
from .collector.entity_scope import is_collector_tooling_key
from .metadata.collector_cloud_profile_catalog_loader import load_collector_cloud_profile_catalog
from .runtime.coordinator import EybondLocalCoordinator
from .models import CapabilityPreset, WriteCapability
from .platform_context import entity_setup_context
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
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    # Without an inverter identity, capability presets would describe a
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
    capability_keys = {capability.key for capability in capabilities}
    profile_name = getattr(inverter, "profile_name", "") or coordinator.effective_profile_name
    presets = (
        (
            inverter.capability_presets
            if inverter is not None
            else (driver.capability_presets if driver is not None else ())
        )
        if has_inverter_identity
        else ()
    )
    collector_capabilities = getattr(coordinator, "collector_capabilities", None)
    collector_proxy_capture_allowed = bool(
        getattr(collector_capabilities, "proxy_capture", True)
    )
    async_add_entities(
        [
            *[
                EybondToolingButton(coordinator, spec)
                for spec in _tooling_button_specs_for_runtime(
                    capability_keys,
                    profile_name,
                    has_inverter_identity=has_inverter_identity,
                    collector_proxy_capture_allowed=collector_proxy_capture_allowed,
                )
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
            key="apply_collector_changes",
            name="Apply Collector Changes",
            icon="mdi:check-network-outline",
            entity_category=EntityCategory.DIAGNOSTIC,
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="rediscover_collector",
            name="Request Collector Callback",
            icon="mdi:radar",
            entity_category=EntityCategory.DIAGNOSTIC,
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="reboot_collector",
            name="Restart Collector",
            icon="mdi:restart",
            entity_category=EntityCategory.CONFIG,
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="start_proxy_capture",
            name="Start Proxy Capture",
            icon="mdi:transit-connection-variant",
            entity_category=EntityCategory.CONFIG,
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="stop_proxy_capture",
            name="Stop Proxy Capture",
            icon="mdi:stop-circle-outline",
            entity_category=EntityCategory.CONFIG,
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="sync_inverter_clock",
            name="Sync Inverter Clock",
            icon="mdi:clock-sync",
            entity_category=EntityCategory.CONFIG,
            enabled_default=True,
        ),
        _ToolingButtonSpec(
            key="recheck_supported_commands",
            name="Re-check Supported Commands",
            icon="mdi:playlist-check",
            entity_category=EntityCategory.DIAGNOSTIC,
            enabled_default=True,
        ),
    )


def _tooling_button_specs_for_runtime(
    capability_keys: set[str] | frozenset[str],
    profile_name: str,
    has_inverter_identity: bool = True,
    collector_proxy_capture_allowed: bool = True,
) -> tuple[_ToolingButtonSpec, ...]:
    allowed_keys = set(
        tooling_button_keys_for_runtime(
            capability_keys,
            profile_name,
            has_inverter_identity=has_inverter_identity,
            collector_proxy_capture_allowed=collector_proxy_capture_allowed,
        )
    )
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


def _normalize_collector_endpoint(endpoint: object) -> str:
    raw = str(endpoint or "").strip()
    if not raw:
        return ""
    try:
        return normalize_collector_server_endpoint(
            raw,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return raw


def _callback_owner_label_from_family(
    cloud_family: object,
    *,
    fallback: str,
    include_current_suffix: bool = False,
) -> str:
    normalized = str(cloud_family or "").strip().lower()
    if not normalized:
        return fallback
    catalog = load_collector_cloud_profile_catalog()
    if normalized not in catalog.profiles:
        return fallback
    if include_current_suffix:
        return f"{normalized}_or_current"
    return normalized


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
        return self.coordinator.inverter_device_info()

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
        if is_collector_tooling_key(spec.key):
            self._attr_translation_key = spec.key

    def _collector_action_availability_reason(self) -> str | None:
        overview = self.coordinator.proxy_capture_overview
        if self._spec.key == "rediscover_collector":
            overview_status = str(getattr(overview, "status", "") or "").strip()
            if overview_status in {"starting", "stopping", "restoring"}:
                return (
                    "Proxy capture is changing the collector callback. "
                    "Wait for the transition to finish."
                )
            if overview_status == "running":
                return "Stop proxy capture before requesting collector callback."
            config_entry = getattr(self.coordinator, "config_entry", None)
            collector_ip = str(
                getattr(config_entry, "data", {}).get("collector_ip", "")
            ).strip()
            if not collector_ip:
                return "Collector IP is not configured."
            return None
        if self._spec.key == "start_proxy_capture":
            mode_apply_lock_reason = getattr(
                self.coordinator,
                "collector_operation_mode_apply_lock_reason",
                lambda: None,
            )()
            if mode_apply_lock_reason is not None:
                return mode_apply_lock_reason
            if overview.can_start:
                return None
            if overview.blocking_reason == "collector_control_disabled":
                return "Proxy capture needs Auto or Full Control to redirect the callback endpoint."
            if overview.blocking_reason == "collector_proxy_capture_unavailable":
                return "Proxy capture is not available for this collector."
            if overview.blocking_reason == "collector_not_connected":
                return "Collector is not connected."
            if overview.blocking_reason == "upstream_endpoint_unavailable":
                return "No upstream callback endpoint is available yet. Restore cloud access first or wait for one external callback endpoint to be detected."
            if overview.blocking_reason == "target_endpoint_unavailable":
                return "Proxy target endpoint is not available."
            if overview.blocking_reason == "current_endpoint_unavailable":
                return "Current collector callback endpoint is not available yet."
            if overview.blocking_reason == "session_active":
                return "Proxy capture session is already active."
            return "Proxy capture is not ready."
        if self._spec.key == "stop_proxy_capture":
            if overview.can_stop:
                return None
            if overview.status in {"starting", "stopping", "restoring"}:
                return "Proxy capture is in a critical phase and cannot be stopped yet."
            return "No proxy capture session is active."
        if self._spec.key in {
            "bind_collector_to_home_assistant",
            "apply_collector_changes",
            "reboot_collector",
            "rollback_collector_server_endpoint",
        }:
            lock_reason = getattr(
                self.coordinator,
                "collector_configuration_lock_reason",
                lambda: None,
            )()
            if lock_reason is not None:
                return lock_reason
        if not self.coordinator.collector_actions_enabled:
            return "Requires Auto or Full Control."
        if not self.coordinator.data.connected:
            return "Collector is not connected."
        if self._spec.key == "bind_collector_to_home_assistant":
            current_endpoint = _normalize_collector_endpoint(
                self.coordinator.data.values.get("collector_server_endpoint")
            )
            if current_endpoint == _normalize_collector_endpoint(
                self.coordinator.collector_callback_target_endpoint
            ):
                return "Collector already points to Home Assistant."
        if self._spec.key == "rollback_collector_server_endpoint":
            rollback_endpoint = _normalize_collector_endpoint(
                self.coordinator.collector_server_endpoint_rollback_target
            )
            if not rollback_endpoint:
                return (
                    "No cached callback endpoint is available yet. "
                    "Rollback becomes available after one collector redirect change."
                )
            current_endpoint = _normalize_collector_endpoint(
                self.coordinator.data.values.get("collector_server_endpoint")
            )
            if current_endpoint and current_endpoint == rollback_endpoint:
                return "Collector already matches the source-of-truth callback endpoint."
        return None

    @property
    def device_info(self):
        if is_collector_tooling_key(self._spec.key):
            return self.coordinator.collector_device_info()
        return self.coordinator.inverter_device_info()

    @property
    def available(self) -> bool:
        if self._spec.key == "create_support_package":
            return not bool(
                getattr(self.coordinator, "support_package_export_running", False)
            )
        if self._spec.key == "reload_local_metadata":
            return True
        if self._spec.key == "create_local_profile_draft":
            return bool(self.coordinator.effective_profile_name)
        if self._spec.key == "create_local_schema_draft":
            return bool(self.coordinator.effective_register_schema_name)
        if self._spec.key in {
            "rediscover_collector",
            "bind_collector_to_home_assistant",
            "apply_collector_changes",
            "reboot_collector",
            "rollback_collector_server_endpoint",
            "start_proxy_capture",
            "stop_proxy_capture",
        }:
            return self._collector_action_availability_reason() is None
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
            "support_package_export_running": values.get(
                "support_package_export_running",
                bool(getattr(self.coordinator, "support_package_export_running", False)),
            ),
            "support_package_export_status": values.get("support_package_export_status"),
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
            "proxy_capture_status": values.get("proxy_capture_status"),
            "proxy_capture_status_label": values.get("proxy_capture_status_label"),
            "proxy_capture_summary": values.get("proxy_capture_summary"),
            "proxy_capture_blocking_reason": values.get("proxy_capture_blocking_reason"),
            "proxy_capture_can_start": values.get("proxy_capture_can_start"),
            "proxy_capture_can_stop": values.get("proxy_capture_can_stop"),
            "proxy_capture_redirect_required": values.get("proxy_capture_redirect_required"),
            "proxy_trace_path": values.get("proxy_trace_path"),
            "proxy_trace_manifest_path": values.get("proxy_trace_manifest_path"),
            "proxy_trace_line_count": values.get("proxy_trace_line_count"),
            "proxy_trace_kind_summary": values.get("proxy_trace_kind_summary"),
            "proxy_trace_recent_kinds": values.get("proxy_trace_recent_kinds"),
            "proxy_trace_last_timestamp": values.get("proxy_trace_last_timestamp"),
        }
        if self._spec.key == "sync_inverter_clock":
            now = dt_util.now().replace(microsecond=0)
            attributes["target_inverter_date"] = now.strftime("%Y-%m-%d")
            attributes["target_inverter_time"] = now.strftime("%H:%M:%S")
            attributes["current_inverter_date"] = values.get("inverter_date")
            attributes["current_inverter_time"] = values.get("inverter_time")
        if is_collector_tooling_key(self._spec.key):
            availability_reason = self._collector_action_availability_reason()
            attributes["collector_server_endpoint"] = values.get("collector_server_endpoint")
            attributes["collector_cloud_family"] = values.get("collector_cloud_family")
            attributes["collector_cloud_family_source"] = values.get("collector_cloud_family_source")
            attributes["collector_cloud_family_confidence"] = values.get("collector_cloud_family_confidence")
            attributes["collector_reboot_required"] = values.get("collector_reboot_required")
            attributes["collector_callback_target_endpoint"] = (
                self.coordinator.collector_callback_target_endpoint
            )
            attributes["collector_rollback_endpoint"] = (
                self.coordinator.collector_server_endpoint_rollback_target
            )
            attributes["control_mode"] = self.coordinator.control_mode
            attributes["control_policy_reason"] = self.coordinator.controls_reason
            attributes["proxy_capture_collector_cloud_family"] = values.get("proxy_capture_collector_cloud_family")
            attributes["expert_action"] = False
            attributes["action_scope"] = "collector"
            attributes["requires_control_mode"] = "auto_or_full"
            attributes["confirmation_mode"] = "baked_in"
            current_endpoint = _normalize_collector_endpoint(
                values.get("collector_server_endpoint")
            )
            rollback_endpoint = _normalize_collector_endpoint(
                self.coordinator.collector_server_endpoint_rollback_target
            )
            attributes["rollback_ready"] = bool(
                rollback_endpoint and (not current_endpoint or current_endpoint != rollback_endpoint)
            )
            attributes["availability_reason"] = availability_reason or "Ready"
            if self._spec.key == "bind_collector_to_home_assistant":
                attributes["action_summary"] = (
                    "Moves the collector callback to this Home Assistant listener and applies the redirect immediately."
                )
                attributes["collector_redirect_expected"] = True
                attributes["target_callback_owner"] = "home_assistant"
            elif self._spec.key == "rediscover_collector":
                attributes["action_summary"] = (
                    "Sends one bootstrap UDP request asking the collector to connect back to the Home Assistant listener."
                )
                attributes["collector_redirect_expected"] = True
                attributes["target_callback_owner"] = "bootstrap"
                attributes["requires_control_mode"] = "none"
                attributes["confirmation_mode"] = "none"
            elif self._spec.key == "apply_collector_changes":
                attributes["action_summary"] = "Applies pending collector changes and confirms restart."
                attributes["collector_restart_expected"] = True
            elif self._spec.key == "reboot_collector":
                attributes["action_summary"] = "Restarts the collector immediately with built-in confirmation."
                attributes["collector_restart_expected"] = True
            elif self._spec.key == "rollback_collector_server_endpoint":
                attributes["action_summary"] = (
                    "Restores cloud access by putting back the callback endpoint captured before the last redirect."
                )
                attributes["rollback_requires_cached_endpoint"] = True
                attributes["target_callback_owner"] = "smartess"
                attributes["target_callback_owner_label"] = _callback_owner_label_from_family(
                    values.get("collector_cloud_family"),
                    fallback="smartess",
                )
            elif self._spec.key == "start_proxy_capture":
                attributes["action_summary"] = (
                    "Starts live collector proxy capture and writes a standalone JSONL trace artifact."
                )
                attributes["target_callback_owner"] = "home_assistant"
                attributes["confirmation_mode"] = "baked_in"
            elif self._spec.key == "stop_proxy_capture":
                attributes["action_summary"] = (
                    "Stops live collector proxy capture, restores the callback endpoint if needed, and writes a manifest."
                )
                attributes["target_callback_owner"] = "smartess_or_current"
                attributes["target_callback_owner_label"] = _callback_owner_label_from_family(
                    values.get("proxy_capture_collector_cloud_family")
                    or values.get("collector_cloud_family"),
                    fallback="smartess_or_current",
                    include_current_suffix=True,
                )
                attributes["confirmation_mode"] = "none"
        return attributes

    async def async_press(self) -> None:
        if self._spec.key == "create_support_package":
            if bool(getattr(self.coordinator, "support_package_export_running", False)):
                raise RuntimeError("support_package_export_in_progress")
            await self.coordinator.async_export_support_package()
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
        if self._spec.key == "recheck_supported_commands":
            await self.coordinator.async_recheck_supported_commands()
            return
        if self._spec.key == "rediscover_collector":
            await self.coordinator.async_trigger_collector_rediscovery()
            return
        if self._spec.key == "bind_collector_to_home_assistant":
            await self.coordinator.async_bind_collector_to_home_assistant(
                confirm_redirect=True,
            )
            return
        if self._spec.key == "apply_collector_changes":
            await self.coordinator.async_apply_collector_changes(confirm_restart=True)
            return
        if self._spec.key == "reboot_collector":
            await self.coordinator.async_reboot_collector(confirm_restart=True)
            return
        if self._spec.key == "rollback_collector_server_endpoint":
            await self.coordinator.async_rollback_collector_server_endpoint(
                apply_changes=True,
                confirm_redirect=True,
            )
            return
        if self._spec.key == "start_proxy_capture":
            overview = self.coordinator.proxy_capture_overview
            await self.coordinator.async_start_proxy_capture(
                confirm_redirect=bool(getattr(overview, "redirect_required", False)),
            )
            return
        if self._spec.key == "stop_proxy_capture":
            try:
                await self.coordinator.async_stop_proxy_capture()
            except RuntimeError as exc:
                if str(exc or "").strip() != "proxy_capture_not_running":
                    raise
                refresh = getattr(self.coordinator, "async_request_refresh", None)
                if refresh is not None:
                    await refresh()
            return
        if self._spec.key == "sync_inverter_clock":
            await self.coordinator.async_sync_inverter_clock()
            return
        raise ValueError(f"unknown_tool_button:{self._spec.key}")
