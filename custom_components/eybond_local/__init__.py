"""EyeBond Local integration lifecycle composition."""

from __future__ import annotations

import asyncio
from functools import partial
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import homeassistant.helpers.config_validation as cv
except ModuleNotFoundError:
    cv = None

from .collector.transport import CollectorListenerBindError
from .const import (
    CONF_COLLECTOR_PN,
    CONF_ENTRY_ROLE,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
    PLATFORMS,
)
from .integration_common import (
    ConfigEntryError,
    ConfigEntryNotReady,
    EVENT_COMPONENT_LOADED,
)
from .integration_entities import (
    _async_cleanup_obsolete_entities,
    _async_finalize_expert_entity_migration,
    _async_remove_legacy_runtime_select_entities,
    _async_self_heal_enabled_defaults,
    _async_self_heal_expert_defaults,
    _async_self_heal_sensor_display_precision,
    _cleanup_obsolete_entities_allowed,
    _default_enabled_unique_ids,
    _default_enabled_unique_ids_for_current_runtime,
    _infer_sensor_display_precision,
    _is_integration_disabled,
)
from .integration_metadata import (
    _SETUP_INITIAL_REFRESH_TIMEOUT,
    _async_initial_refresh_for_setup,
    _async_remove_obsolete_pending_entries,
    _async_self_heal_collector_cloud_family,
    _async_self_heal_entry_title,
    _async_self_heal_server_ip,
    _async_self_heal_valuecloud_driver_hint,
    _cloud_family_from_entry_endpoint_shape,
    _collector_cloud_family_for_entity_filter,
    _configure_local_metadata_roots,
    _coordinator_has_inverter_identity,
    _entity_unique_id,
    _entry_has_startup_entity_fallback,
    _eybond_config_data_root,
    _is_obsolete_pending_entry,
    _known_collector_cloud_family,
    _preset_unique_id,
    _prime_metadata_caches,
    _register_background_refresh_task,
    _start_background_refresh_for_setup,
    _text_unique_id,
    _tool_unique_id,
)
from .integration_migration import _ENTRY_SCHEMA_VERSION, async_migrate_entry
from .integration_registration import (
    _is_transient_listener_bind_error,
    _register_entry_callback_session_claim,
    _register_entry_network_reconcile,
    _register_entry_stop_shutdown,
)
from .platform_context import entity_setup_context

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_COMPONENT_SETUP_COMPLETE_KEY = "component_setup_complete"

CONFIG_SCHEMA: Any = (
    cv.config_entry_only_config_schema("eybond_local")
    if cv is not None
    else None
)

async def async_setup(hass: HomeAssistant, _config: dict) -> bool:
    """Initialize shared loader state for the integration."""

    from .services import async_setup_services
    from .support.download import async_register_download_views
    from .passive_discovery import async_start_passive_callback_discovery

    domain_data = hass.data.setdefault(DOMAIN, {})
    domain_data[_COMPONENT_SETUP_COMPLETE_KEY] = False

    def _component_loaded(event) -> None:
        if event.data.get("component") != DOMAIN:
            return
        if hass.loop.is_closed():
            return
        # The event is fired at the end of HA's integration setup. Updating on
        # the next ready turn avoids mutating the listener list while it is
        # being iterated and gives deferred entry reloads one stable boundary.
        hass.loop.call_soon_threadsafe(_mark_component_setup_complete)

    def _mark_component_setup_complete() -> None:
        domain_data[_COMPONENT_SETUP_COMPLETE_KEY] = True
        remove_component_listener()

    remove_component_listener = hass.bus.async_listen(
        EVENT_COMPONENT_LOADED, _component_loaded
    )

    try:
        await _async_remove_obsolete_pending_entries(hass)
        _configure_local_metadata_roots(hass)
        await hass.async_add_executor_job(
            _prime_metadata_caches, _eybond_config_data_root(hass)
        )
        await async_setup_services(hass)
        async_register_download_views(hass)
        await async_start_passive_callback_discovery(hass)
    except Exception:
        remove_component_listener()
        logger.exception("Failed to initialize EyeBond Local integration bootstrap")
        raise
    return True

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up EyeBond Local from a config entry."""

    if str(entry.data.get(CONF_ENTRY_ROLE) or "") == ENTRY_ROLE_LISTENER:
        # ``async_setup`` owns the shared passive listeners. This entry merely
        # keeps the integration loaded when there are no collector entries.
        entry.runtime_data = None
        return True

    if _is_obsolete_pending_entry(entry):
        # Domain setup removes these beta2 tombstones before entry setup. Refuse
        # fail-closed if an external race/storage error made one survive; never
        # reinterpret it as a normal PN-less collector or perform network I/O.
        raise ConfigEntryError("obsolete_pending_entry_not_removed")

    from .runtime.coordinator import EybondLocalCoordinator
    from .services import async_setup_services
    from .support.download import async_register_download_views

    coordinator = None
    try:
        _configure_local_metadata_roots(hass)
        await async_setup_services(hass)
        async_register_download_views(hass)
        await _async_self_heal_server_ip(hass, entry)
        await _async_self_heal_collector_cloud_family(hass, entry)
        await _async_self_heal_valuecloud_driver_hint(hass, entry)
        await _async_self_heal_entry_title(hass, entry)
        # Establish permanent registry ownership (complete the config-flow handoff
        # or claim the durable PN) BEFORE the coordinator starts, so the runtime
        # sees a registry where this entry already owns its PN/session. Also
        # registers the unload hook that frees the claim if setup fails below.
        _register_entry_callback_session_claim(hass, entry)
        coordinator = EybondLocalCoordinator(hass, entry)
        await coordinator.async_setup()
        entry.runtime_data = coordinator
        _register_entry_stop_shutdown(hass, entry, coordinator)
        _register_entry_network_reconcile(hass, entry, coordinator)
        refresh_deferred_until_platform_setup = await _async_initial_refresh_for_setup(
            hass,
            entry,
            coordinator,
        )
        await _async_self_heal_enabled_defaults(hass, entry, coordinator)
        await _async_cleanup_obsolete_entities(hass, entry, coordinator)

        setup_driver, _setup_inverter, platforms_started_with_inverter_identity = (
            entity_setup_context(entry, coordinator)
        )
        platforms_started_with_driver_fallback = bool(
            setup_driver is not None and not platforms_started_with_inverter_identity
        )
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        coordinator.mark_entity_platforms_initialized(
            has_inverter_identity=platforms_started_with_inverter_identity,
            has_driver_fallback=platforms_started_with_driver_fallback,
        )
        await _async_finalize_expert_entity_migration(hass, entry)
        if refresh_deferred_until_platform_setup:
            _start_background_refresh_for_setup(hass, entry, coordinator)
        coordinator.async_sync_device_registry()
        entry.async_on_unload(entry.add_update_listener(_async_update_listener))
        await _async_ensure_listener_entry(hass)
    except CollectorListenerBindError as exc:
        if coordinator is not None:
            try:
                await coordinator.async_shutdown()
            except Exception:
                logger.exception(
                    "Failed to clean up EyeBond Local entry %s after listener bind failure",
                    entry.entry_id,
                )
        if _is_transient_listener_bind_error(exc):
            logger.warning(
                "EyeBond listener is temporarily unavailable for entry %s on %s:%d: %s",
                entry.entry_id,
                exc.host,
                exc.port,
                exc.error,
            )
            raise ConfigEntryNotReady(
                f"EyeBond listener is not ready on {exc.host}:{exc.port}: {exc.error}"
            ) from exc
        logger.exception("Failed to set up EyeBond Local entry %s", entry.entry_id)
        raise
    except (ConfigEntryNotReady, ConfigEntryError):
        # Ownership could not be established (a competing verification/duplicate).
        # The claim ran before the coordinator was constructed, so nothing runs;
        # propagate the typed, (non-)retryable result unchanged and quietly.
        raise
    except Exception:
        if coordinator is not None and getattr(entry, "runtime_data", None) is None:
            try:
                await coordinator.async_shutdown()
            except Exception:
                logger.exception(
                    "Failed to clean up EyeBond Local entry %s after setup failure",
                    entry.entry_id,
                )
        logger.exception("Failed to set up EyeBond Local entry %s", entry.entry_id)
        raise
    # Reaching this point -- not merely reclaiming the PN near the beginning of
    # setup -- proves that an unload was a reload/recovery rather than a
    # permanent removal. A later setup failure must leave the exact old socket
    # quarantined; otherwise it can leak into interactive discovery while the
    # entry itself is still unavailable.
    from .passive_discovery import get_passive_callback_discovery

    discovery = get_passive_callback_discovery(hass)
    if discovery is not None:
        discovery.resume_entry_sessions(entry.entry_id)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""

    if str(entry.data.get(CONF_ENTRY_ROLE) or "") == ENTRY_ROLE_LISTENER:
        return True

    if _is_obsolete_pending_entry(entry):
        # Obsolete beta2 tombstone: it never started a coordinator or platforms.
        # Domain setup normally removes it before this hook can run.
        return True

    from .runtime.coordinator import EybondLocalCoordinator

    coordinator: EybondLocalCoordinator = entry.runtime_data
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        await coordinator.async_shutdown()
    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Finalize the exact owned collector session and remove entry evidence."""

    if str(entry.data.get(CONF_ENTRY_ROLE) or "") == ENTRY_ROLE_LISTENER:
        # Removing this explicit service entry is how the user disables the
        # integration completely. Never recreate it from its own removal.
        return

    if _is_obsolete_pending_entry(entry):
        # Obsolete beta2 tombstone: it never owned cloud evidence or a permanent
        # registry claim, so removal deliberately has no network side effects.
        return

    from .connection.removal_finalization import (
        REMOVAL_RESTART_CONFIRMED,
        async_finalize_collector_entry_removal,
    )
    from .passive_discovery import get_passive_callback_discovery
    from .support.cloud_evidence import remove_cloud_evidence_for_entry

    discovery = get_passive_callback_discovery(hass)
    if discovery is not None:
        ticket = discovery.take_entry_removal_ticket(entry.entry_id)
        if ticket is not None:
            try:
                result = await async_finalize_collector_entry_removal(
                    ticket,
                    discovery.registry,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Home Assistant removes the config entry regardless of cleanup
                # callback errors. Keep the exact socket quarantined and make the
                # failure explicit instead of exposing it as a valid scan result.
                logger.exception(
                    "Failed to finalize collector session while removing entry %s",
                    entry.entry_id,
                )
            else:
                log = (
                    logger.info
                    if result.status == REMOVAL_RESTART_CONFIRMED
                    else logger.warning
                )
                log(
                    "Collector removal finalization entry=%s status=%s restarted=%s disconnect_observed=%s",
                    entry.entry_id,
                    result.status,
                    result.restarted,
                    result.disconnect_observed,
                )

    config_dir = Path(hass.config.path())
    collector_pn = str(entry.data.get(CONF_COLLECTOR_PN) or "").strip()
    deleted = await hass.async_add_executor_job(
        partial(
            remove_cloud_evidence_for_entry,
            config_dir,
            entry_id=entry.entry_id,
            collector_pn=collector_pn,
        )
    )
    if deleted:
        logger.debug(
            "Removed %d cloud-evidence file(s) for entry %s", len(deleted), entry.entry_id
        )
    await _async_ensure_listener_entry(hass, excluding_entry_id=entry.entry_id)


async def _async_ensure_listener_entry(
    hass: HomeAssistant,
    *,
    excluding_entry_id: str = "",
) -> None:
    """Ensure one persistent integration-level passive-listener entry exists."""

    async_entries = getattr(hass.config_entries, "async_entries", None)
    if not callable(async_entries):
        return
    entries = tuple(async_entries(DOMAIN))
    if any(
        str(candidate.data.get(CONF_ENTRY_ROLE) or "") == ENTRY_ROLE_LISTENER
        and candidate.entry_id != excluding_entry_id
        for candidate in entries
    ):
        return
    flow_manager = getattr(hass.config_entries, "flow", None)
    async_init = getattr(flow_manager, "async_init", None)
    if not callable(async_init):
        return
    try:
        await async_init(
            DOMAIN,
            context={"source": "import"},
            data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
        )
    except Exception:
        # The collector entry remains valid even if an older HA core or a
        # concurrent setup flow rejects the service-entry bootstrap.
        logger.exception("Failed to ensure EyeBond passive-discovery listener entry")


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry after options changes."""

    coordinator = getattr(entry, "runtime_data", None)
    consume_reload_suppression = getattr(
        coordinator,
        "consume_entry_reload_suppression",
        None,
    )
    if callable(consume_reload_suppression) and consume_reload_suppression():
        return

    await hass.config_entries.async_reload(entry.entry_id)
