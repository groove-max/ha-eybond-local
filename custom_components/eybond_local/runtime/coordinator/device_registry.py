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


def _registry_owner_counts(hass: object, config_entries: set[object]) -> tuple[int, int] | None:
    """Return live/missing config-entry owner counts, or fail closed."""

    config_entry_manager = getattr(hass, "config_entries", None)
    async_get_entry = getattr(config_entry_manager, "async_get_entry", None)
    if not callable(async_get_entry):
        return None
    live = 0
    missing = 0
    for config_entry_id in config_entries:
        if (
            type(config_entry_id) is not str
            or not config_entry_id
            or config_entry_id != config_entry_id.strip()
        ):
            return None
        try:
            owner = async_get_entry(config_entry_id)
        except Exception:
            return None
        if owner is None:
            missing += 1
        else:
            live += 1
    return live, missing


def _registry_entity_entries(hass: object, device_id: object) -> tuple[object, ...] | None:
    """Return all entities for one device, including disabled ones."""

    if type(device_id) is not str or not device_id:
        return None
    try:
        from homeassistant.helpers import entity_registry as er

        entity_registry = er.async_get(hass)
        return tuple(
            er.async_entries_for_device(
                entity_registry,
                device_id,
                include_disabled_entities=True,
            )
        )
    except Exception:
        return None


def _is_foreign_eybond_inverter_child(
    entry_id: str,
    device: object,
    *,
    collector_device_id: str,
    inverter_device_id: str,
) -> bool:
    """Return whether a child belongs outside the current entry boundary."""

    device_id = getattr(device, "id", None)
    if (
        type(device_id) is not str
        or not device_id
        or device_id == inverter_device_id
        or getattr(device, "via_device_id", None) != collector_device_id
    ):
        return False
    identifiers = set(getattr(device, "identifiers", ()))
    if not identifiers or any(
        type(identifier) is not tuple
        or len(identifier) != 2
        or identifier[0] != DOMAIN
        or type(identifier[1]) is not str
        or not identifier[1]
        or identifier[1] != identifier[1].strip()
        for identifier in identifiers
    ):
        return False
    return entry_id not in set(getattr(device, "config_entries", ()))


def _is_safe_orphaned_inverter_child(
    hass: object,
    entry_id: str,
    device: object,
    *,
    collector_device_id: str,
    inverter_device_id: str,
) -> bool:
    """Return whether a stale direct child is safe to remove automatically."""

    if not _is_foreign_eybond_inverter_child(
        entry_id,
        device,
        collector_device_id=collector_device_id,
        inverter_device_id=inverter_device_id,
    ):
        return False
    owner_counts = _registry_owner_counts(
        hass,
        set(getattr(device, "config_entries", ())),
    )
    if owner_counts is None or owner_counts[0] != 0:
        return False
    entity_entries = _registry_entity_entries(hass, device.id)
    if entity_entries is None or entity_entries:
        return False
    if (
        str(getattr(device, "name_by_user", "") or "").strip()
        or getattr(device, "area_id", None) is not None
        or bool(getattr(device, "labels", ()))
        or getattr(device, "disabled_by", None) is not None
    ):
        return False
    return True


def _reconcile_inverter_children(hass: object, entry_id: str) -> None:
    """Unlink foreign children and remove only provably safe orphans."""

    try:
        registry = dr.async_get(hass)
        devices = tuple(registry.devices.values())
        collector_device = registry.async_get_device(
            identifiers={(DOMAIN, f"{entry_id}:collector")}
        )
        inverter_device = registry.async_get_device(identifiers={(DOMAIN, entry_id)})
    except Exception:
        return
    collector_device_id = getattr(collector_device, "id", None)
    inverter_device_id = getattr(inverter_device, "id", None)
    if (
        type(collector_device_id) is not str
        or not collector_device_id
        or type(inverter_device_id) is not str
        or not inverter_device_id
    ):
        return
    remove_device = getattr(registry, "async_remove_device", None)
    update_device = getattr(registry, "async_update_device", None)
    if not callable(remove_device) or not callable(update_device):
        return
    removed = 0
    unlinked = 0
    for device in devices:
        if not _is_foreign_eybond_inverter_child(
            entry_id,
            device,
            collector_device_id=collector_device_id,
            inverter_device_id=inverter_device_id,
        ):
            continue
        if _is_safe_orphaned_inverter_child(
            hass,
            entry_id,
            device,
            collector_device_id=collector_device_id,
            inverter_device_id=inverter_device_id,
        ):
            try:
                remove_device(device.id)
            except Exception:
                logger.debug(
                    "Failed to remove an orphaned inverter device for entry %s",
                    entry_id,
                    exc_info=True,
                )
            else:
                removed += 1
        else:
            try:
                update_device(device.id, via_device_id=None)
            except Exception:
                logger.debug(
                    "Failed to unlink a foreign inverter device from entry %s",
                    entry_id,
                    exc_info=True,
                )
            else:
                unlinked += 1
    if removed:
        logger.info(
            "Removed %d orphaned inverter device-registry child record(s) for entry %s",
            removed,
            entry_id,
        )
    if unlinked:
        logger.info(
            "Unlinked %d foreign inverter device-registry child record(s) from entry %s",
            unlinked,
            entry_id,
        )


class CoordinatorDeviceRegistryMixin:
    """Project runtime identity into Home Assistant's device registry."""

    def device_registry_diagnostics(self) -> dict[str, object]:
        """Return a redacted topology view for support packages.

        One EyeBond config entry currently owns exactly one collector and at
        most one inverter.  Include every registry device owned by this entry
        plus every direct child of its canonical collector, so stale children
        from another/removed entry remain observable.  Opaque registry ids,
        identifiers, serials and user-assigned names never leave this boundary.
        """

        try:
            from homeassistant.helpers import entity_registry as er

            device_registry = dr.async_get(self.hass)
            entity_registry = er.async_get(self.hass)
            devices = tuple(device_registry.devices.values())
        except Exception:
            return {"available": False, "reason": "registry_unavailable"}

        entry_id = self.config_entry.entry_id
        collector_identifier = (DOMAIN, f"{entry_id}:collector")
        inverter_identifier = (DOMAIN, entry_id)

        collector_device = next(
            (
                device
                for device in devices
                if collector_identifier in set(getattr(device, "identifiers", ()))
            ),
            None,
        )
        inverter_device = next(
            (
                device
                for device in devices
                if inverter_identifier in set(getattr(device, "identifiers", ()))
            ),
            None,
        )
        collector_device_id = getattr(collector_device, "id", None)
        inverter_device_id = getattr(inverter_device, "id", None)

        relevant_devices = tuple(
            device
            for device in devices
            if entry_id in set(getattr(device, "config_entries", ()))
            or (
                collector_device_id is not None
                and getattr(device, "via_device_id", None) == collector_device_id
            )
            or getattr(device, "id", None) in {collector_device_id, inverter_device_id}
        )
        direct_children = tuple(
            device
            for device in relevant_devices
            if collector_device_id is not None
            and getattr(device, "via_device_id", None) == collector_device_id
        )
        extra_children = tuple(
            device
            for device in direct_children
            if getattr(device, "id", None) != inverter_device_id
        )

        records: list[dict[str, object]] = []
        role_order = {
            "canonical_collector": 0,
            "canonical_inverter": 1,
            "unexpected_collector_child": 2,
            "unexpected_entry_device": 3,
        }
        for device in relevant_devices:
            device_id = getattr(device, "id", None)
            if device_id == collector_device_id:
                role = "canonical_collector"
            elif device_id == inverter_device_id:
                role = "canonical_inverter"
            elif getattr(device, "via_device_id", None) == collector_device_id:
                role = "unexpected_collector_child"
            else:
                role = "unexpected_entry_device"

            try:
                entity_entries = tuple(
                    er.async_entries_for_device(
                        entity_registry,
                        device_id,
                        include_disabled_entities=True,
                    )
                )
            except Exception:
                entity_entries = ()
            identifiers = set(getattr(device, "identifiers", ()))
            config_entries = set(getattr(device, "config_entries", ()))
            owner_counts = _registry_owner_counts(self.hass, config_entries)
            records.append(
                {
                    "role": role,
                    "belongs_to_current_entry": entry_id in config_entries,
                    "config_entry_count": len(config_entries),
                    "live_config_entry_count": (
                        owner_counts[0] if owner_counts is not None else None
                    ),
                    "missing_config_entry_count": (
                        owner_counts[1] if owner_counts is not None else None
                    ),
                    "eybond_identifier_count": sum(
                        1
                        for identifier in identifiers
                        if type(identifier) is tuple
                        and len(identifier) == 2
                        and identifier[0] == DOMAIN
                    ),
                    "entity_count": len(entity_entries),
                    "disabled_entity_count": sum(
                        1
                        for entity in entity_entries
                        if getattr(entity, "disabled_by", None) is not None
                    ),
                    "model": str(getattr(device, "model", "") or "").strip(),
                    "serial_present": bool(
                        str(getattr(device, "serial_number", "") or "").strip()
                    ),
                    "user_name_present": bool(
                        str(getattr(device, "name_by_user", "") or "").strip()
                    ),
                    "safe_cleanup_candidate": (
                        type(collector_device_id) is str
                        and type(inverter_device_id) is str
                        and _is_safe_orphaned_inverter_child(
                            self.hass,
                            entry_id,
                            device,
                            collector_device_id=collector_device_id,
                            inverter_device_id=inverter_device_id,
                        )
                    ),
                }
            )

        records.sort(
            key=lambda record: (
                role_order[str(record["role"])],
                str(record["model"]),
                int(record["entity_count"]),
            )
        )
        if extra_children:
            topology_status = "duplicate_inverter_children"
        elif collector_device is None:
            topology_status = "canonical_collector_missing"
        elif inverter_device is None and self.has_inverter_identity:
            topology_status = "canonical_inverter_missing"
        else:
            topology_status = "ok"

        return {
            "available": True,
            "topology_status": topology_status,
            "canonical_collector_present": collector_device is not None,
            "canonical_inverter_present": inverter_device is not None,
            "direct_child_count": len(direct_children),
            "unexpected_direct_child_count": len(extra_children),
            "relevant_device_count": len(relevant_devices),
            "devices": records,
        }

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

        device = registry.async_get_or_create(
            config_entry_id=self.config_entry.entry_id,
            **info,
        )
        if not desired_serial and getattr(device, "serial_number", None):
            update_device = getattr(registry, "async_update_device", None)
            if callable(update_device):
                update_device(device.id, serial_number=None)
        _reconcile_inverter_children(self.hass, self.config_entry.entry_id)
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
