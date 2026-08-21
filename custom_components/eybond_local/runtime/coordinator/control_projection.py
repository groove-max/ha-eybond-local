"""Control availability and write-exposure projections."""

from __future__ import annotations

from typing import Any

from ...const import CONTROL_MODE_FULL
from ...control_policy import controls_enabled, controls_reason, controls_summary
from ...drivers.registry import all_write_capabilities
from ...models import CapabilityPreset, WriteCapability
from ...schema import (
    capability_write_exposure_allowed,
    preset_write_exposure_allowed,
)


class CoordinatorControlProjectionMixin:
    """Project control policy without owning writes or transport state."""

    @property
    def controls_enabled(self) -> bool:
        """Whether writes are globally enabled for this entry."""

        return controls_enabled(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            write_capability_count=self._current_write_capability_count(),
        )

    @property
    def collector_actions_enabled(self) -> bool:
        """Whether collector-scoped actions are allowed for this entry."""

        return self.control_mode in {"auto", CONTROL_MODE_FULL}

    @property
    def controls_reason(self) -> str:
        """Why writes are enabled or disabled for this entry."""

        return controls_reason(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            write_capability_count=self._current_write_capability_count(),
        )

    @property
    def controls_summary(self) -> str:
        """Human-readable summary of the current control policy."""

        return controls_summary(
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            write_capability_count=self._current_write_capability_count(),
        )

    def _current_write_capability_count(self) -> int | None:
        """Return the number of writable controls known for the current runtime."""

        inverter = self.identified_inverter
        if inverter is not None:
            return len(tuple(getattr(inverter, "capabilities", ()) or ()))

        driver = self.current_driver
        if driver is not None:
            return len(tuple(getattr(driver, "write_capabilities", ()) or ()))

        return None

    def can_expose_capability(self, capability: WriteCapability) -> bool:
        """Whether one capability should exist as a writable HA entity."""

        context = self._write_exposure_context()
        return capability_write_exposure_allowed(
            capability,
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            variant_key=context["variant_key"],
            profile_source_scope=context["profile_source_scope"],
            schema_source_scope=context["schema_source_scope"],
            profile_name=context["profile_name"],
            device_scoped_overlay_active=context["device_scoped_overlay_active"],
            selected_control_keys=context["selected_control_keys"],
        )

    def capability_enabled_by_default(self, capability: WriteCapability) -> bool:
        """Entity-registry default-enabled state for one capability.

        Learned overlay capabilities are generated with ``enabled_default=False`` so they
        stay inactive until activation. Once the user has selected and activated a device-
        scoped learned control (so it is exposable), enable it by default -- otherwise the
        entity would be created but registered disabled and stay hidden on the device page.
        Every other capability keeps its declared default.
        """

        if capability.is_device_scoped_experimental and self.can_expose_capability(capability):
            return True
        return capability.enabled_default

    def can_expose_preset(self, preset: CapabilityPreset) -> bool:
        """Whether one preset should exist as a writable HA entity."""

        inverter = self.identified_inverter
        if inverter is None:
            capabilities_by_key = {
                capability.key: capability
                for capability in all_write_capabilities()
            }
        else:
            capabilities_by_key = {capability.key: capability for capability in inverter.capabilities}
        context = self._write_exposure_context()
        return preset_write_exposure_allowed(
            preset,
            capabilities_by_key=capabilities_by_key,
            control_mode=self.control_mode,
            detection_confidence=self.detection_confidence,
            variant_key=context["variant_key"],
            profile_source_scope=context["profile_source_scope"],
            schema_source_scope=context["schema_source_scope"],
            profile_name=context["profile_name"],
            device_scoped_overlay_active=context["device_scoped_overlay_active"],
            selected_control_keys=context["selected_control_keys"],
        )

    def _write_exposure_context(self) -> dict[str, Any]:
        """Return normalized metadata context shared by write exposure checks."""

        metadata = self.effective_metadata
        inverter = self.identified_inverter
        snapshot = self.effective_metadata_snapshot
        variant_key = str(
            getattr(inverter, "variant_key", "") or getattr(snapshot, "variant_key", "") or ""
        ).strip()
        return {
            "variant_key": variant_key,
            "profile_name": str(getattr(metadata, "profile_name", "") or "").strip(),
            "profile_source_scope": str(
                getattr(getattr(metadata, "profile_metadata", None), "source_scope", "") or ""
            ).strip(),
            "schema_source_scope": str(
                getattr(getattr(metadata, "register_schema_metadata", None), "source_scope", "")
                or ""
            ).strip(),
            "device_scoped_overlay_active": bool(
                getattr(metadata, "device_scoped_overlay_active", False)
            ),
            "device_scoped_overlay_scope": str(
                getattr(metadata, "device_scoped_overlay_scope", "") or ""
            ).strip(),
            "selected_control_keys": getattr(
                metadata, "device_scoped_overlay_selected_control_keys", None
            ),
            "effective_capabilities_experimental": bool(
                getattr(metadata, "device_scoped_overlay_active", False)
            ),
        }

    @property
    def write_exposure_context(self) -> dict[str, Any]:
        """Return a detached diagnostic view of capability exposure inputs."""

        return dict(self._write_exposure_context())



__all__ = ["CoordinatorControlProjectionMixin"]
