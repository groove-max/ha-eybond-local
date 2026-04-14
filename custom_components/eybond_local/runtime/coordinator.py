"""Home Assistant coordinator for the EyeBond Local integration."""

from __future__ import annotations

from datetime import timedelta
import logging
from pathlib import Path
from typing import Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import network
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from ..const import (
    CONF_COLLECTOR_IP,
    CONF_CONNECTION_TYPE,
    CONF_CONNECTION_MODE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_DISCOVERY_INTERVAL,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_HINT,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_CONTROL_MODE,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DOMAIN,
    DRIVER_HINT_AUTO,
)
from ..connection.models import build_connection_spec
from ..control_policy import (
    can_expose_capability,
    can_expose_preset,
    controls_enabled,
    controls_reason,
    controls_summary,
)
from ..drivers.registry import get_driver
from ..drivers.registry import all_write_capabilities
from ..fixtures.utils import anonymize_fixture_json, build_command_fixture_responses
from ..metadata.local_metadata import create_local_profile_draft, create_local_schema_draft
from ..models import CapabilityPreset, RuntimeSnapshot, WriteCapability
from .factory import create_runtime_manager
from .manager import RuntimeManager
from ..schema import build_runtime_ui_schema
from ..support.bundle import build_support_bundle_payload, export_support_bundle
from ..support.package import export_support_package
from ..support.workflow import build_support_workflow_state

logger = logging.getLogger(__name__)


class EybondLocalCoordinator(DataUpdateCoordinator[RuntimeSnapshot]):
    """Owns the hub and exposes its snapshots to Home Assistant entities."""

    config_entry: ConfigEntry

    def __init__(self, hass, entry: ConfigEntry) -> None:
        self.config_entry = entry
        connection_spec = build_connection_spec(entry.data, entry.options)
        self._runtime: RuntimeManager = create_runtime_manager(
            connection_spec,
            driver_hint=entry.options.get(CONF_DRIVER_HINT, entry.data.get(CONF_DRIVER_HINT, "auto")),
            connection_mode=entry.data.get(CONF_CONNECTION_MODE, ""),
        )
        super().__init__(
            hass,
            logger,
            name=DOMAIN,
            update_interval=timedelta(
                seconds=entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            ),
        )
        self.data = RuntimeSnapshot()
        self._last_synced_device_meta: tuple[str, str, str, str] = ("", "", "", "")
        self._tooling_values: dict[str, Any] = {}

    async def async_setup(self) -> None:
        """Start the underlying hub."""

        await self._runtime.async_start()

    async def async_shutdown(self) -> None:
        """Stop the underlying hub."""

        await self._runtime.async_stop()

    async def _async_update_data(self) -> RuntimeSnapshot:
        snapshot = await self._runtime.async_refresh(
            poll_interval=float(
                self.config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
            )
        )
        snapshot.values["connection_type"] = self.config_entry.data.get(CONF_CONNECTION_TYPE, "eybond")
        snapshot.values["detection_confidence"] = self.detection_confidence
        snapshot.values["control_mode"] = self.control_mode
        snapshot.values["controls_enabled"] = self.controls_enabled
        snapshot.values["control_policy_reason"] = self.controls_reason
        snapshot.values["control_policy_summary"] = self.controls_summary
        snapshot.values.update(self._support_workflow_values(snapshot))
        snapshot.values.update(self._tooling_values)
        self.async_sync_device_registry(snapshot)
        return snapshot

    async def async_write_capability(self, capability_key: str, value: Any) -> Any:
        """Write one inverter capability and refresh coordinator state."""

        inverter = self.data.inverter
        if inverter is None:
            raise RuntimeError("inverter_not_detected")
        capability = inverter.get_capability(capability_key)
        if not self.can_expose_capability(capability):
            raise PermissionError(
                f"capability_control_disabled:{capability.key}:{self.controls_reason}"
            )
        try:
            written_value = await self._runtime.async_write_capability(capability_key, value)
        except Exception:
            await self.async_request_refresh()
            raise
        await self.async_request_refresh()
        return written_value

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        """Apply one declarative preset and refresh coordinator state."""

        inverter = self.data.inverter
        if inverter is None:
            raise RuntimeError("inverter_not_detected")
        preset = inverter.get_capability_preset(preset_key)
        if not self.can_expose_preset(preset):
            raise PermissionError(
                f"preset_control_disabled:{preset.key}:{self.controls_reason}"
            )
        try:
            result = await self._runtime.async_apply_preset(preset_key)
        except Exception:
            await self.async_request_refresh()
            raise
        await self.async_request_refresh()
        return result

    @property
    def detection_confidence(self) -> str:
        """Return the saved detection confidence for this entry."""

        return self.config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none")

    @property
    def control_mode(self) -> str:
        """Return the configured control mode override."""

        return self.config_entry.options.get(
            CONF_CONTROL_MODE,
            self.config_entry.data.get(CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE),
        )

    @property
    def controls_enabled(self) -> bool:
        """Whether writes are globally enabled for this entry."""

        return controls_enabled(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
        )

    @property
    def controls_reason(self) -> str:
        """Why writes are enabled or disabled for this entry."""

        return controls_reason(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
        )

    @property
    def controls_summary(self) -> str:
        """Human-readable summary of the current control policy."""

        return controls_summary(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
        )

    def can_expose_capability(self, capability: WriteCapability) -> bool:
        """Whether one capability should exist as a writable HA entity."""

        return can_expose_capability(
            capability,
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
        )

    def can_expose_preset(self, preset: CapabilityPreset) -> bool:
        """Whether one preset should exist as a writable HA entity."""

        inverter = self.data.inverter
        if inverter is None:
            capabilities_by_key = {
                capability.key: capability
                for capability in all_write_capabilities()
            }
        else:
            capabilities_by_key = {capability.key: capability for capability in inverter.capabilities}
        return can_expose_preset(
            preset,
            capabilities_by_key=capabilities_by_key,
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
        )

    @property
    def current_driver(self):
        """Return the registered driver for the detected inverter, if any."""

        inverter = self.data.inverter
        try:
            if inverter is not None:
                return get_driver(inverter.driver_key)
            driver_hint = self.config_entry.options.get(
                CONF_DRIVER_HINT,
                self.config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            if driver_hint and driver_hint != DRIVER_HINT_AUTO:
                return get_driver(driver_hint)
        except KeyError:
            pass
        return None

    @property
    def effective_profile_name(self) -> str:
        """Return the effective detected profile name when available."""

        inverter = self.data.inverter
        if inverter is not None and inverter.profile_name:
            return inverter.profile_name
        driver = self.current_driver
        return getattr(driver, "profile_name", "") if driver is not None else ""

    @property
    def effective_register_schema_name(self) -> str:
        """Return the effective detected register schema name when available."""

        inverter = self.data.inverter
        if inverter is not None and inverter.register_schema_name:
            return inverter.register_schema_name
        driver = self.current_driver
        return getattr(driver, "register_schema_name", "") if driver is not None else ""

    async def async_export_support_bundle(self) -> str:
        """Export one JSON support bundle for the current entry."""

        support_bundle_payload = self._build_support_bundle_payload()
        path = await self.hass.async_add_executor_job(
            lambda: export_support_bundle(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self.config_entry.title,
                connected=support_bundle_payload["runtime"]["connected"],
                collector=support_bundle_payload["runtime"]["collector"],
                inverter=support_bundle_payload["runtime"]["inverter"],
                values=support_bundle_payload["runtime"]["values"],
                data=support_bundle_payload["entry"]["data"],
                options=support_bundle_payload["entry"]["options"],
                profile_name=support_bundle_payload["source_metadata"]["profile_name"],
                register_schema_name=support_bundle_payload["source_metadata"]["register_schema_name"],
            )
        )
        self._publish_tooling_values(
            support_bundle_path=str(path),
            local_metadata_status="Support bundle exported",
        )
        return str(path)

    async def async_export_support_package(self) -> str:
        """Export one combined support archive with raw capture and replay fixture."""

        support_bundle_payload = self._build_support_bundle_payload()
        driver = self.current_driver
        try:
            raw_capture = await self._runtime.async_capture_support_evidence()
        except Exception as exc:
            raw_capture = {
                "capture_kind": "unsupported_or_failed",
                "error": str(exc),
                "captured_ranges": [],
                "range_failures": [],
            }
        fixture = self._build_support_fixture(raw_capture)
        anonymized_fixture = anonymize_fixture_json(fixture) if fixture is not None else None
        profile_metadata = getattr(driver, "profile_metadata", None)
        register_schema_metadata = getattr(driver, "register_schema_metadata", None)

        export_result = await self.hass.async_add_executor_job(
            lambda: export_support_package(
                config_dir=Path(self.hass.config.config_dir),
                entry_id=self.config_entry.entry_id,
                entry_title=self.config_entry.title,
                support_bundle=support_bundle_payload,
                raw_capture=raw_capture,
                fixture=fixture,
                anonymized_fixture=anonymized_fixture,
                profile_source=self._metadata_source_payload(profile_metadata),
                register_schema_source=self._metadata_source_payload(register_schema_metadata),
            )
        )
        path = export_result.path
        relative_download_url = str(export_result.download_url or "")
        absolute_download_url = self._absolute_local_download_url(relative_download_url)
        download_url = absolute_download_url or relative_download_url
        self._publish_tooling_values(
            support_package_path=str(path),
            support_package_download_path=str(export_result.download_path or ""),
            support_package_download_url=download_url,
            support_package_download_relative_url=relative_download_url,
            local_metadata_status="Support archive exported",
        )
        if download_url:
            persistent_notification.async_create(
                self.hass,
                (
                    "Your support archive is ready.\n\n"
                    f"[Download support archive]({download_url})\n\n"
                    f"Saved file: `{path}`"
                ),
                title="EyeBond Local Support Archive",
                notification_id=f"{DOMAIN}_support_package_{self.config_entry.entry_id}",
            )
        return str(path)

    async def async_create_local_profile_draft(self) -> str:
        """Create or refresh one local experimental profile draft."""

        return await self.async_create_local_profile_draft_named()

    async def async_create_local_profile_draft_named(
        self,
        output_profile_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> str:
        """Create or refresh one local experimental profile draft."""

        source_profile_name = self.effective_profile_name
        if not source_profile_name:
            raise RuntimeError("driver_profile_not_available")
        path = await self.hass.async_add_executor_job(
            lambda: create_local_profile_draft(
                config_dir=Path(self.hass.config.config_dir),
                source_profile_name=source_profile_name,
                output_profile_name=output_profile_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            local_profile_draft_path=str(path),
            local_metadata_status="Local profile draft created",
        )
        return str(path)

    async def async_create_local_schema_draft(self) -> str:
        """Create or refresh one local experimental register schema draft."""

        return await self.async_create_local_schema_draft_named()

    async def async_create_local_schema_draft_named(
        self,
        output_schema_name: str | None = None,
        *,
        overwrite: bool = True,
    ) -> str:
        """Create or refresh one local experimental register schema draft."""

        source_schema_name = self.effective_register_schema_name
        if not source_schema_name:
            raise RuntimeError("driver_register_schema_not_available")
        path = await self.hass.async_add_executor_job(
            lambda: create_local_schema_draft(
                config_dir=Path(self.hass.config.config_dir),
                source_schema_name=source_schema_name,
                output_schema_name=output_schema_name,
                overwrite=overwrite,
            )
        )
        self._publish_tooling_values(
            local_schema_draft_path=str(path),
            local_metadata_status="Local register schema draft created",
        )
        return str(path)

    async def async_reload_local_metadata(self) -> None:
        """Reload the current config entry after local metadata changes."""

        self._publish_tooling_values(local_metadata_status="Reloading local metadata")
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)

    def _publish_tooling_values(self, **values: Any) -> None:
        """Publish in-memory tooling results into coordinator snapshot values."""

        self._tooling_values.update(values)
        snapshot = self.data
        snapshot.values.update(self._tooling_values)
        self.async_set_updated_data(snapshot)

    def _absolute_local_download_url(self, relative_url: str) -> str:
        """Return an absolute HA URL for one `/local/...` path when possible."""

        if not relative_url:
            return ""
        try:
            base_url = network.get_url(
                self.hass,
                allow_internal=True,
                allow_external=True,
                allow_cloud=False,
                prefer_external=True,
            ).rstrip("/")
        except network.NoURLAvailableError:
            return relative_url
        return f"{base_url}{relative_url}"

    def _support_workflow_values(self, snapshot: RuntimeSnapshot | None = None) -> dict[str, Any]:
        """Return user-facing support workflow guidance for the current entry."""

        snapshot = snapshot or self.data
        driver = self.current_driver
        workflow = build_support_workflow_state(
            has_inverter=snapshot.inverter is not None,
            driver_name=getattr(driver, "name", ""),
            detection_confidence=self.detection_confidence,
            profile_source_scope=getattr(getattr(driver, "profile_metadata", None), "source_scope", ""),
            schema_source_scope=getattr(getattr(driver, "register_schema_metadata", None), "source_scope", ""),
        )
        return {
            "support_workflow_level": workflow["level"],
            "support_workflow_level_label": workflow["level_label"],
            "support_workflow_summary": workflow["summary"],
            "support_workflow_next_action": workflow["next_action"],
            "support_workflow_primary_action": workflow["primary_action"],
            "support_workflow_step_1": workflow["step_1"],
            "support_workflow_step_2": workflow["step_2"],
            "support_workflow_step_3": workflow["step_3"],
            "support_workflow_plan": workflow["plan"],
            "support_workflow_advanced_hint": workflow["advanced_hint"],
        }

    def _build_support_bundle_payload(self) -> dict[str, Any]:
        inverter = self.data.inverter
        driver = self.current_driver
        values = dict(self.data.values)
        inverter_payload = None
        if inverter is not None:
            values["ui_schema"] = build_runtime_ui_schema(inverter, self.data.values)
            inverter_payload = self._inverter_payload(inverter)
        return build_support_bundle_payload(
            entry_id=self.config_entry.entry_id,
            entry_title=self.config_entry.title,
            connected=self.data.connected,
            collector=self._collector_payload(),
            inverter=inverter_payload,
            values=values,
            data=dict(self.config_entry.data),
            options=dict(self.config_entry.options),
            profile_name=getattr(inverter, "profile_name", "") or getattr(driver, "profile_name", ""),
            register_schema_name=getattr(inverter, "register_schema_name", "") or getattr(driver, "register_schema_name", ""),
            variant_key=getattr(inverter, "variant_key", ""),
        )

    def _collector_payload(self) -> dict[str, Any] | None:
        if self.data.collector is None:
            return None
        return {
            "remote_ip": self.data.collector.remote_ip,
            "remote_port": self.data.collector.remote_port,
            "collector_pn": self.data.collector.collector_pn,
            "profile_key": self.data.collector.profile_key,
            "profile_name": self.data.collector.profile_name,
            "last_udp_reply": self.data.collector.last_udp_reply,
            "last_udp_reply_from": self.data.collector.last_udp_reply_from,
            "last_devcode": self.data.collector.last_devcode,
        }

    @staticmethod
    def _inverter_payload(inverter) -> dict[str, Any]:
        return {
            "driver_key": inverter.driver_key,
            "protocol_family": inverter.protocol_family,
            "model_name": inverter.model_name,
            "variant_key": inverter.variant_key,
            "serial_number": inverter.serial_number,
            "profile_name": inverter.profile_name,
            "register_schema_name": inverter.register_schema_name,
            "probe_target": {
                "devcode": inverter.probe_target.devcode,
                "collector_addr": inverter.probe_target.collector_addr,
                "device_addr": inverter.probe_target.device_addr,
            },
            "details": dict(inverter.details),
        }

    def _build_support_fixture(
        self,
        raw_capture: dict[str, Any],
    ) -> dict[str, Any] | None:
        inverter = self.data.inverter
        ranges = list(raw_capture.get("fixture_ranges") or [])
        command_responses = build_command_fixture_responses(raw_capture)
        probe_target = None
        fixture_name = ""
        if inverter is not None:
            probe_target = {
                "devcode": inverter.probe_target.devcode,
                "collector_addr": inverter.probe_target.collector_addr,
                "device_addr": inverter.probe_target.device_addr,
            }
            fixture_name = f"{inverter.driver_key}_support_capture"
        elif raw_capture.get("capture_kind") == "generic_register_dump":
            best_capture = self._best_generic_capture(raw_capture)
            if best_capture is not None:
                ranges = list(best_capture.get("fixture_ranges") or ranges)
                probe_target = dict(best_capture.get("probe_target") or {})
                fixture_name = f"{best_capture.get('driver_key', 'unknown')}_support_capture"
        if not ranges and not command_responses:
            return None

        collector_payload = self._collector_payload() or {}
        fixture: dict[str, Any] = {
            "fixture_version": 1,
            "name": fixture_name or "unknown_driver_support_capture",
            "collector": {
                "remote_ip": collector_payload.get("remote_ip"),
                "collector_pn": collector_payload.get("collector_pn"),
                "last_devcode": collector_payload.get("last_devcode"),
                "profile_key": collector_payload.get("profile_key"),
                "profile_name": collector_payload.get("profile_name"),
            },
            "probe_target": probe_target,
        }
        if ranges:
            fixture["ranges"] = ranges
        if command_responses:
            fixture["command_responses"] = command_responses
        return fixture

    @staticmethod
    def _best_generic_capture(raw_capture: dict[str, Any]) -> dict[str, Any] | None:
        captures = list(raw_capture.get("captures") or [])
        if not captures:
            return None
        return max(
            captures,
            key=lambda capture: (
                len(capture.get("fixture_ranges") or []),
                -len(capture.get("range_failures") or []),
            ),
        )


    @staticmethod
    def _metadata_source_payload(metadata) -> dict[str, Any] | None:
        if metadata is None:
            return None
        return {
            "name": getattr(metadata, "source_name", ""),
            "scope": getattr(metadata, "source_scope", ""),
            "path": getattr(metadata, "source_path", ""),
        }

    def _build_device_info(self, snapshot: RuntimeSnapshot | None = None) -> DeviceInfo:
        """Build stable device metadata for entities and registry sync."""

        snapshot = snapshot or self.data
        name = "EyeBond Inverter"
        model = None
        serial_number = None
        detected_model = self.config_entry.data.get(CONF_DETECTED_MODEL)
        detected_serial = self.config_entry.data.get(CONF_DETECTED_SERIAL)

        if snapshot.inverter is not None:
            name = snapshot.inverter.model_name
            model = snapshot.inverter.model_name
            serial_number = snapshot.inverter.serial_number
        else:
            if detected_model:
                name = detected_model
                model = detected_model
            elif self.config_entry.title:
                name = self.config_entry.title
            if detected_serial:
                serial_number = detected_serial

        info: dict[str, object] = {
            "identifiers": {(DOMAIN, self.config_entry.entry_id)},
            "name": name,
            "manufacturer": "OEM / EyeBond",
        }
        if model:
            info["model"] = model
        if serial_number:
            info["serial_number"] = serial_number
        return DeviceInfo(**info)

    def device_info(self) -> DeviceInfo:
        """Build stable device metadata for entities."""

        return self._build_device_info(self.data)

    def async_sync_device_registry(self, snapshot: RuntimeSnapshot | None = None) -> None:
        """Update the existing HA device entry with the latest model metadata."""

        info = self._build_device_info(snapshot)
        identifiers = info.get("identifiers")
        if not identifiers:
            return

        registry = dr.async_get(self.hass)
        device = registry.async_get_device(identifiers=identifiers)
        if device is None:
            return

        desired_name = info.get("name") or ""
        desired_model = info.get("model") or ""
        desired_serial = info.get("serial_number") or ""
        desired_manufacturer = info.get("manufacturer") or ""
        meta = (
            desired_name,
            desired_model,
            desired_serial,
            desired_manufacturer,
        )
        if meta == self._last_synced_device_meta:
            return

        registry.async_update_device(
            device.id,
            name=desired_name or None,
            model=desired_model or None,
            serial_number=desired_serial or None,
            manufacturer=desired_manufacturer or None,
        )
        self._last_synced_device_meta = meta
