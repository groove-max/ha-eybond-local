"""Home Assistant device-registry lifecycle for the runtime coordinator."""

from __future__ import annotations

import logging

from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo

from ...collector.entity_scope import is_collector_entity_key
from ...const import (
    CONF_COLLECTOR_IP,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    DOMAIN,
)
from ...models import RuntimeSnapshot
from ..device_projection import (
    build_collector_device_info_payload,
    build_inverter_device_info_payload,
)

logger = logging.getLogger(__name__)


class CoordinatorDeviceRegistryMixin:
    """Project runtime identity into Home Assistant's device registry."""

    def inverter_device_info(self) -> DeviceInfo:
        """Build stable device metadata for inverter-owned entities."""

        if not self.has_inverter_identity:
            return self.collector_device_info()
        return DeviceInfo(
            **build_inverter_device_info_payload(
                entry_id=self.config_entry.entry_id,
                entry_title=self.config_entry.title,
                detected_model=self.config_entry.data.get(CONF_DETECTED_MODEL),
                detected_serial=self.config_entry.data.get(CONF_DETECTED_SERIAL),
                inverter=self.data.inverter,
            )
        )

    def collector_device_info(self) -> DeviceInfo:
        """Build stable device metadata for collector-owned entities."""

        return DeviceInfo(**self._collector_device_info_payload(self.data))

    def _collector_device_info_payload(
        self,
        snapshot: RuntimeSnapshot,
    ) -> dict[str, object]:
        """Supply coordinator-owned context to the pure collector projector."""

        return build_collector_device_info_payload(
            entry_id=self.config_entry.entry_id,
            collector_ip=self.config_entry.data.get(CONF_COLLECTOR_IP),
            collector_pn=self._preferred_collector_pn(snapshot),
            collector=snapshot.collector,
            values=snapshot.values,
            entry_data=self.config_entry.data,
            entry_options=self.config_entry.options,
        )

    def device_info_for_key(self, key: str) -> DeviceInfo:
        """Return the owning device metadata for one entity key."""

        if is_collector_entity_key(key):
            return self.collector_device_info()
        return self.inverter_device_info()

    def async_sync_device_registry(self, snapshot: RuntimeSnapshot | None = None) -> None:
        """Update existing HA device entries with the latest metadata."""

        self._async_sync_collector_device_registry(snapshot)
        self._async_sync_inverter_device_registry(snapshot)

    def _async_sync_inverter_device_registry(
        self,
        snapshot: RuntimeSnapshot | None = None,
    ) -> None:
        """Update the inverter HA device entry with the latest model metadata."""

        if not self.has_inverter_identity:
            registry = dr.async_get(self.hass)
            device = registry.async_get_device(identifiers={(DOMAIN, self.config_entry.entry_id)})
            remove_device = getattr(registry, "async_remove_device", None)
            if device is not None and callable(remove_device):
                try:
                    remove_device(device.id)
                except Exception:
                    logger.debug(
                        "Failed to remove stale inverter device for entry %s",
                        self.config_entry.entry_id,
                        exc_info=True,
                    )
            self._last_synced_device_meta = ("", "", "", "", "")
            return

        snapshot = snapshot or self.data
        info = build_inverter_device_info_payload(
            entry_id=self.config_entry.entry_id,
            entry_title=self.config_entry.title,
            detected_model=self.config_entry.data.get(CONF_DETECTED_MODEL),
            detected_serial=self.config_entry.data.get(CONF_DETECTED_SERIAL),
            inverter=snapshot.inverter,
        )
        identifiers = info.get("identifiers")
        if not identifiers:
            return

        registry = dr.async_get(self.hass)
        desired_name = info.get("name") or ""
        desired_model = info.get("model") or ""
        desired_serial = info.get("serial_number") or ""
        desired_manufacturer = info.get("manufacturer") or ""
        desired_via_device = info.get("via_device")
        desired_via_device_id = None
        if desired_via_device:
            collector_device = registry.async_get_device(identifiers={desired_via_device})
            if collector_device is not None:
                desired_via_device_id = collector_device.id
        meta = (
            desired_name,
            desired_model,
            desired_serial,
            desired_manufacturer,
            desired_via_device_id or "",
        )
        if meta == self._last_synced_device_meta:
            return

        registry.async_get_or_create(config_entry_id=self.config_entry.entry_id, **info)
        self._last_synced_device_meta = meta

    def _async_sync_collector_device_registry(
        self,
        snapshot: RuntimeSnapshot | None = None,
    ) -> None:
        """Update the collector HA device entry with the latest metadata."""

        info = self._collector_device_info_payload(snapshot or self.data)
        identifiers = info.get("identifiers")
        if not identifiers:
            return

        registry = dr.async_get(self.hass)
        desired_name = info.get("name") or ""
        desired_model = info.get("model") or ""
        desired_serial = info.get("serial_number") or ""
        desired_manufacturer = info.get("manufacturer") or ""
        desired_sw_version = info.get("sw_version") or ""
        desired_hw_version = info.get("hw_version") or ""
        meta = (
            desired_name,
            desired_model,
            desired_serial,
            desired_manufacturer,
            desired_sw_version,
            desired_hw_version,
        )
        if meta == self._last_synced_collector_device_meta:
            return

        device = registry.async_get_or_create(config_entry_id=self.config_entry.entry_id, **info)
        if not desired_manufacturer and getattr(device, "manufacturer", "") == "OEM / EyeBond":
            update_device = getattr(registry, "async_update_device", None)
            if callable(update_device):
                update_device(device.id, manufacturer=None)
        self._last_synced_collector_device_meta = meta


__all__ = ["CoordinatorDeviceRegistryMixin"]
