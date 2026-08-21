"""Inverter identity and effective-metadata projections."""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

from ...const import (
    CONF_COLLECTOR_PN,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DRIVER_HINT,
    DRIVER_HINT_AUTO,
)
from ...drivers.registry import get_driver
from ...metadata.effective_metadata import resolve_effective_metadata_selection
from ...metadata.effective_metadata_snapshot import (
    EffectiveMetadataSnapshot,
    effective_metadata_snapshot_from_dict,
)
from ...models import RuntimeSnapshot

logger = logging.getLogger(__name__)

_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY = "effective_metadata_snapshot"


class CoordinatorInverterProfileMixin:
    """Project one coherent inverter identity and metadata selection."""

    @property
    def current_driver(self):
        """Return the registered driver for the detected inverter, if any."""

        inverter = self.identified_inverter
        try:
            if inverter is not None:
                driver_key = str(getattr(inverter, "driver_key", "") or "").strip()
                if driver_key:
                    return get_driver(driver_key)
            if not self.has_inverter_identity:
                return None
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
    def inverter_protocol_candidates(self):
        """Return protocols observed by runtime on this entry's owned session."""

        candidates = getattr(self._runtime, "inverter_protocol_candidates", ())
        return candidates if isinstance(candidates, tuple) else ()

    @property
    def identified_inverter(self):
        """Return the runtime inverter only when it has a usable identity."""

        inverter = self.data.inverter
        if inverter is None:
            return None

        model_name = str(getattr(inverter, "model_name", "") or "").strip()
        serial_number = str(getattr(inverter, "serial_number", "") or "").strip()
        if model_name or serial_number:
            return inverter

        detected_model = str(self.config_entry.data.get(CONF_DETECTED_MODEL) or "").strip()
        detected_serial = str(self.config_entry.data.get(CONF_DETECTED_SERIAL) or "").strip()
        if detected_model or detected_serial:
            return inverter
        return None

    def _apply_device_overlay_to_inverter(self, inverter, collector):
        """Merge activated device-scoped learned controls into the detected inverter.

        The runtime detects the inverter against built-in bindings, so its capabilities
        never include the learned overlay controls. This hook (invoked by the runtime
        right after detection) resolves the effective metadata for the detected device
        and, when a device-scoped overlay is active, appends the activated learned
        capabilities (plus any capability group they require) so they materialize as
        entities and are writable. Detected capabilities are preserved; none are removed.
        """

        if inverter is None:
            self._device_overlay_merge_status = "inverter_none"
            return inverter
        try:
            metadata = resolve_effective_metadata_selection(
                inverter=inverter,
                driver=None,
                collector=collector,
                entry_data=self.config_entry.data,
                entry_options=self.config_entry.options,
                persisted_snapshot=self.effective_metadata_snapshot,
            )
            if not metadata.device_scoped_overlay_active:
                self._device_overlay_merge_status = "inactive"
                return inverter
            profile = metadata.profile_metadata
            if profile is None:
                self._device_overlay_merge_status = "no_profile_metadata"
                return inverter
            existing_keys = {capability.key for capability in inverter.capabilities}
            learned = tuple(
                capability
                for capability in profile.capabilities
                if capability.is_device_scoped_experimental
                and capability.key not in existing_keys
            )
            if not learned:
                already = sum(
                    1
                    for capability in inverter.capabilities
                    if getattr(capability, "is_device_scoped_experimental", False)
                )
                self._device_overlay_merge_status = f"no_new_learned(already={already})"
                return inverter
            needed_group_keys = {capability.group for capability in learned}
            existing_group_keys = {group.key for group in inverter.capability_groups}
            extra_groups = tuple(
                group
                for group in profile.groups
                if group.key in needed_group_keys and group.key not in existing_group_keys
            )
            self._device_overlay_merge_status = (
                f"merged({'+'.join(capability.key for capability in learned)})"
            )
            return dataclasses.replace(
                inverter,
                capabilities=inverter.capabilities + learned,
                capability_groups=inverter.capability_groups + extra_groups,
            )
        except Exception as exc:
            self._device_overlay_merge_status = f"error:{type(exc).__name__}:{exc}"
            logger.warning(
                "Failed to merge device-scoped learned controls into the detected "
                "inverter; activated controls will not appear this cycle",
                exc_info=True,
            )
            return inverter

    def apply_device_overlay_to_inverter(self, inverter, collector):
        """Apply the active device overlay through the public coordinator boundary."""

        return self._apply_device_overlay_to_inverter(inverter, collector)

    @property
    def has_inverter_identity(self) -> bool:
        """Return whether this entry has a confirmed or persisted inverter identity."""

        if self.identified_inverter is not None:
            return True
        detected_model = str(self.config_entry.data.get(CONF_DETECTED_MODEL) or "").strip()
        detected_serial = str(self.config_entry.data.get(CONF_DETECTED_SERIAL) or "").strip()
        return bool(detected_model or detected_serial)

    @property
    def effective_metadata(self):
        """Return the effective metadata selection for the current entry state."""

        cached = getattr(self, "_cached_effective_metadata", None)
        if cached is not None:
            return cached
        return resolve_effective_metadata_selection(
            inverter=self.identified_inverter,
            driver=self.current_driver,
            collector=self.data.collector,
            entry_data=self.config_entry.data,
            entry_options=self.config_entry.options,
            persisted_snapshot=self.effective_metadata_snapshot,
        )

    @property
    def effective_metadata_snapshot(self) -> EffectiveMetadataSnapshot:
        """Return the persisted effective metadata snapshot when one is stored."""

        options = getattr(self.config_entry, "options", {}) or {}
        return effective_metadata_snapshot_from_dict(
            options.get(_EFFECTIVE_METADATA_SNAPSHOT_OPTION_KEY)
        )

    @property
    def effective_owner_key(self) -> str:
        """Return the actual runtime owner key for the selected effective metadata."""

        return self.effective_metadata.effective_owner_key

    @property
    def effective_owner_name(self) -> str:
        """Return the internal runtime-path label for the selected effective metadata."""

        return self.effective_metadata.effective_owner_name

    @property
    def smartess_family_name(self) -> str:
        """Return the SmartESS family label when collector hints resolved one."""

        return self.effective_metadata.smartess_family_name

    @property
    def smartess_raw_profile_name(self) -> str:
        """Return the raw SmartESS asset profile name when available."""

        return self.effective_metadata.raw_profile_name

    @property
    def smartess_raw_register_schema_name(self) -> str:
        """Return the raw SmartESS asset schema name when available."""

        return self.effective_metadata.raw_register_schema_name

    @property
    def effective_profile_metadata(self):
        """Return the loaded effective profile metadata when available."""

        return self.effective_metadata.profile_metadata

    @property
    def effective_register_schema_metadata(self):
        """Return the loaded effective register schema metadata when available."""

        return self.effective_metadata.register_schema_metadata

    @property
    def effective_profile_name(self) -> str:
        """Return the effective detected profile name when available."""

        return self.effective_metadata.profile_name

    @property
    def effective_register_schema_name(self) -> str:
        """Return the effective detected register schema name when available."""

        return self.effective_metadata.register_schema_name

    @property
    def shadow_learning_effective_metadata(self) -> Any:
        """Return the effective metadata a shadow-learning seed should carry.

        Prefer the persisted snapshot, but the partial / unidentified tier never
        persists one (it has no controls profile by design), so fall back to the
        LIVE effective metadata (the family base schema). Without this fallback
        the start path blocks with ``missing_effective_metadata_snapshot`` on
        exactly the devices learning exists for. This is the single source of
        truth shared with the config-flow preflight so the preview and the
        actual start can never drift.
        """

        snapshot = self.effective_metadata_snapshot
        if str(getattr(snapshot, "register_schema_name", "") or "").strip():
            return snapshot
        return {
            "effective_owner_key": self.effective_owner_key,
            "profile_name": self.effective_profile_name,
            "register_schema_name": self.effective_register_schema_name,
        }

    @property
    def smartess_collector_pn(self) -> str:
        """Return the collector PN used for SmartESS cloud evidence matching."""

        return self._preferred_collector_pn(self.data)

    def _preferred_collector_pn(self, snapshot: RuntimeSnapshot | None = None) -> str:
        """Return the most complete collector PN available from config and runtime."""

        snapshot = snapshot or self.data
        configured_pn = str(self.config_entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
        live_pn = str(getattr(snapshot.collector, "collector_pn", "") or "").strip()
        if not live_pn:
            return configured_pn
        if not configured_pn:
            return live_pn
        if configured_pn == live_pn:
            return live_pn
        if configured_pn.startswith(live_pn):
            return configured_pn
        if live_pn.startswith(configured_pn):
            return live_pn
        return live_pn



__all__ = ["CoordinatorInverterProfileMixin"]
