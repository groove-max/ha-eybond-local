"""Entity-platform metadata drift and reload lifecycle."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import EVENT_COMPONENT_LOADED

from ..const import DOMAIN
from ..metadata.effective_metadata_snapshot import EffectiveMetadataSnapshot

logger = logging.getLogger(__name__)

_COMPONENT_SETUP_COMPLETE_KEY = "component_setup_complete"


class CoordinatorEntityReloadMixin:
    """Coordinate one HA entry reload after runtime metadata changes."""

    def mark_entity_platforms_initialized(
        self,
        *,
        has_inverter_identity: bool | None = None,
        has_driver_fallback: bool | None = None,
    ) -> None:
        """Record that Home Assistant entity platforms finished loading."""

        self._entity_platforms_initialized = True
        loaded_with_inverter_identity = (
            self.has_inverter_identity
            if has_inverter_identity is None
            else bool(has_inverter_identity)
        )
        loaded_with_driver_fallback = bool(has_driver_fallback)
        self._entity_platforms_loaded_with_inverter_identity = loaded_with_inverter_identity
        self._entity_platforms_loaded_with_driver_fallback = loaded_with_driver_fallback
        self._platform_loaded_effective_metadata_signature = (
            self._effective_metadata_reload_signature_from_snapshot(
                self.effective_metadata_snapshot
            )
        )
        if self.has_inverter_identity and not loaded_with_inverter_identity:
            self._request_entry_reload_for_late_identity()

    def _effective_metadata_reload_signature_from_snapshot(
        self,
        snapshot: EffectiveMetadataSnapshot,
    ) -> tuple[str, str, str]:
        """Return one strict drift signature used for controlled reload checks."""

        if not snapshot.is_valid:
            return ("", "", "")
        variant_key = str(getattr(snapshot, "variant_key", "") or "").strip()
        profile_name = str(getattr(snapshot, "profile_name", "") or "").strip()
        register_schema_name = str(
            getattr(snapshot, "register_schema_name", "") or ""
        ).strip()
        if not (variant_key and profile_name and register_schema_name):
            return ("", "", "")
        return (variant_key, profile_name, register_schema_name)

    def _request_entry_reload_for_metadata_drift(
        self,
        *,
        setup_signature: tuple[str, str, str],
        runtime_signature: tuple[str, str, str],
    ) -> None:
        """Reload once when effective metadata drifts after platforms are loaded."""

        if not getattr(self, "_entity_platforms_initialized", False):
            return
        if getattr(self, "_entity_platform_reload_requested", False):
            return
        if not (
            getattr(self, "_entity_platforms_loaded_with_inverter_identity", False)
            or getattr(self, "_entity_platforms_loaded_with_driver_fallback", False)
        ):
            return
        if not all(runtime_signature):
            return

        first_runtime_signature = not any(setup_signature)
        if not first_runtime_signature and not all(setup_signature):
            return
        if not first_runtime_signature and setup_signature == runtime_signature:
            return

        self._entity_platform_reload_requested = True
        if first_runtime_signature:
            logger.info(
                "Reloading EyeBond entry %s after first confirmed effective metadata snapshot (%s)",
                self.config_entry.entry_id,
                "/".join(runtime_signature),
            )
        else:
            logger.info(
                "Reloading EyeBond entry %s after effective metadata drift (%s -> %s)",
                self.config_entry.entry_id,
                "/".join(setup_signature),
                "/".join(runtime_signature),
            )
        self._dispatch_entry_reload_when_loaded()

    def _dispatch_entry_reload_when_loaded(self) -> None:
        """Dispatch one requested reload only after the current setup is complete."""

        if getattr(self, "_entity_platform_reload_dispatched", False) or getattr(
            self, "_shutdown_complete", False
        ):
            return
        domain_data = getattr(self.hass, "data", {}).get(DOMAIN, {})
        if domain_data.get(_COMPONENT_SETUP_COMPLETE_KEY, True) is not True:
            if getattr(self, "_component_loaded_reload_unsub", None) is None:

                def _component_loaded(event) -> None:
                    if event.data.get("component") != DOMAIN:
                        return
                    if self.hass.loop.is_closed():
                        return
                    self.hass.loop.call_soon_threadsafe(
                        self._dispatch_entry_reload_when_loaded
                    )

                self._component_loaded_reload_unsub = self.hass.bus.async_listen(
                    EVENT_COMPONENT_LOADED, _component_loaded
                )
            return
        if getattr(self, "_component_loaded_reload_unsub", None) is not None:
            self._component_loaded_reload_unsub()
            self._component_loaded_reload_unsub = None
        entry_state = getattr(self.config_entry, "state", None)
        on_state_change = getattr(self.config_entry, "async_on_state_change", None)
        if entry_state is None or entry_state is ConfigEntryState.LOADED:
            self._entity_platform_reload_dispatched = True
            self.hass.async_create_task(
                self.hass.config_entries.async_reload(self.config_entry.entry_id)
            )
            return
        if not callable(on_state_change):
            return
        if getattr(self, "_entry_loaded_reload_unsub", None) is not None:
            return

        def _entry_state_changed() -> None:
            if (
                getattr(self, "_shutdown_complete", False)
                or getattr(self, "_entity_platform_reload_dispatched", False)
                or self.config_entry.state is not ConfigEntryState.LOADED
            ):
                return
            self._entity_platform_reload_dispatched = True
            # A zero-delay timer runs in the next event-loop iteration, after
            # the ready queue has finished unwinding both ConfigEntry setup and
            # (on first load) the enclosing integration-component setup. Using
            # call_soon or eager task creation can race their success return.
            self.hass.loop.call_later(0, self._start_deferred_entry_reload)

        self._entry_loaded_reload_unsub = on_state_change(_entry_state_changed)

    def _start_deferred_entry_reload(self) -> None:
        """Start a state-gated reload after the current HA setup lifecycle."""

        if (
            getattr(self, "_shutdown_complete", False)
            or self.config_entry.state is not ConfigEntryState.LOADED
        ):
            return
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self.config_entry.entry_id)
        )

    def _request_entry_reload_for_late_identity(self) -> None:
        """Reload once when runtime confirms an inverter after platform setup."""

        if not getattr(self, "_entity_platforms_initialized", False):
            return
        if getattr(self, "_entity_platform_reload_requested", False):
            return
        self._entity_platform_reload_requested = True
        logger.info(
            "Reloading EyeBond entry %s after late runtime inverter confirmation",
            self.config_entry.entry_id,
        )
        self._dispatch_entry_reload_when_loaded()

    def _request_entry_reload_for_collector_capability_change(self) -> None:
        """Reload once when collector kind changes after entity platforms loaded."""

        if not getattr(self, "_entity_platforms_initialized", False):
            return
        if getattr(self, "_entity_platform_reload_requested", False):
            return
        self._entity_platform_reload_requested = True
        logger.info(
            "Reloading EyeBond entry %s after collector capability profile changed",
            self.config_entry.entry_id,
        )
        self._dispatch_entry_reload_when_loaded()



__all__ = ["CoordinatorEntityReloadMixin"]
