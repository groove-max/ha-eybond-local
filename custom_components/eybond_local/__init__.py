"""EyeBond Local integration."""

from __future__ import annotations

import asyncio
import errno
from functools import partial
import logging
from math import isfinite
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import homeassistant.helpers.config_validation as cv
except ModuleNotFoundError:  # Local tooling imports the package without Home Assistant installed.
    cv = None

try:
    from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, EVENT_HOMEASSISTANT_STOP
except ModuleNotFoundError:  # Local tooling imports the package without Home Assistant installed.
    EVENT_HOMEASSISTANT_STARTED = "homeassistant_started"
    EVENT_HOMEASSISTANT_STOP = "homeassistant_stop"

try:
    from homeassistant.exceptions import ConfigEntryNotReady
except ModuleNotFoundError:  # Local tooling imports the package without Home Assistant installed.
    class ConfigEntryNotReady(Exception):
        """Fallback used by local tooling when Home Assistant is unavailable."""

from .naming import installation_title, legacy_installation_titles
from .collector.signal import is_legacy_disabled_signal_entity_key
from .collector.transport import CollectorListenerBindError
from .device_scoped_overlay import filter_learned_read_measurements_for_activation
from .const import (
    COLLECTOR_OPERATION_MODES,
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_OPERATION_MODE,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_COLLECTOR_PN,
    CONF_CONTROL_MODE,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DRIVER_HINT,
    CONF_CONNECTION_TYPE,
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    CONF_SERVER_IP,
    CONNECTION_TYPE_EYBOND,
    CONTROL_MODE_FULL,
    DEFAULT_COLLECTOR_OPERATION_MODE,
    DRIVER_HINT_AUTO,
    PLATFORMS,
)
from .platform_context import entity_setup_context

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_SETUP_INITIAL_REFRESH_TIMEOUT = 20.0
_STOP_SHUTDOWN_TIMEOUT = 15.0
_EXPERT_ENTITY_MIGRATION_SETTLE_TIMEOUT = 1.0
_FLOAT_PRECISION_DEVICE_CLASSES = {
    "current",
    "frequency",
    "temperature",
    "voltage",
}
_DEFAULT_ENABLED_RUNTIME_SELECT_KEYS = (
    CONF_COLLECTOR_OPERATION_MODE,
)
_TRANSIENT_LISTENER_BIND_ERRNOS = {
    errno.EADDRNOTAVAIL,
    errno.ENETUNREACH,
    errno.EHOSTUNREACH,
    errno.ENODEV,
}

CONFIG_SCHEMA: Any = (
    cv.config_entry_only_config_schema("eybond_local")
    if cv is not None
    else None
)


def _cancel_task_callback(task: asyncio.Task) -> None:
    """Cancel one background task from a Home Assistant unload callback."""

    task.cancel()


def _log_abandoned_shutdown_result(task: asyncio.Task) -> None:
    """Retrieve the result of a shutdown task abandoned after its timeout."""

    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Abandoned EyeBond shutdown task failed: %s", exc, exc_info=exc)


def _register_entry_stop_shutdown(hass: HomeAssistant, entry: ConfigEntry, coordinator) -> None:
    """Stop the runtime explicitly when Home Assistant is shutting down."""

    async def _async_shutdown_on_stop(_event) -> None:
        # Bounded: the process exits right after HA stop, so an unfinished
        # network teardown dies with it anyway — but an unbounded await here
        # keeps this task pending through every shutdown stage (observed
        # hanging >60s on a shielded listener release). asyncio.wait, not
        # wait_for: wait_for would await the cancelled task, and the shielded
        # cleanup inside re-awaits its future on cancel — the hang would
        # simply move here.
        task = asyncio.ensure_future(coordinator.async_shutdown())
        done, pending = await asyncio.wait({task}, timeout=_STOP_SHUTDOWN_TIMEOUT)
        if pending:
            # The abandoned task still needs its result retrieved, or a late
            # failure surfaces as a contextless "exception was never
            # retrieved" during interpreter teardown.
            task.add_done_callback(_log_abandoned_shutdown_result)
            logger.warning(
                "EyeBond runtime shutdown for entry %s did not finish within %.0fs on Home Assistant stop; abandoning cleanup",
                entry.entry_id,
                _STOP_SHUTDOWN_TIMEOUT,
            )
            return
        try:
            task.result()
        except Exception:
            logger.exception("Failed to shut down EyeBond runtime for entry %s on Home Assistant stop", entry.entry_id)

    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_shutdown_on_stop)
    )


def _register_entry_network_reconcile(hass: HomeAssistant, entry: ConfigEntry, coordinator) -> None:
    """Ask the runtime to re-check listener network state after HA/network events."""

    async def _async_reconcile_on_event(event) -> None:
        reconcile = getattr(coordinator, "async_reconcile_network", None)
        if reconcile is None:
            return
        reason = str(getattr(event, "event_type", "") or "homeassistant_started")
        try:
            await reconcile(reason=reason)
        except Exception:
            logger.exception(
                "Failed to reconcile EyeBond listener network state for entry %s after %s",
                entry.entry_id,
                reason,
            )

    # A one-time listener auto-removes itself when it fires. Registering its
    # unsub on async_on_unload then double-removes it on unload (HA logs
    # "Unable to remove unknown job listener ... list.remove(x): x not in
    # list"). Track the fired state and skip the redundant removal.
    started = {"fired": False}

    async def _async_reconcile_on_started(event) -> None:
        started["fired"] = True
        await _async_reconcile_on_event(event)

    _unsub_started = hass.bus.async_listen_once(
        EVENT_HOMEASSISTANT_STARTED, _async_reconcile_on_started
    )

    def _unsub_started_if_pending() -> None:
        if not started["fired"]:
            _unsub_started()

    entry.async_on_unload(_unsub_started_if_pending)
    async_listen = getattr(hass.bus, "async_listen", None)
    if async_listen is not None:
        entry.async_on_unload(
            async_listen("core_config_updated", _async_reconcile_on_event)
        )


def _is_transient_listener_bind_error(exc: CollectorListenerBindError) -> bool:
    """Return whether one listener bind failure should be retried by HA."""

    if exc.errno in _TRANSIENT_LISTENER_BIND_ERRNOS:
        return True
    if exc.errno == errno.EADDRINUSE:
        return False
    message = str(exc.error).lower()
    if "address already in use" in message:
        return False
    return "could not bind" in message or "cannot assign requested address" in message


def _eybond_config_data_root(hass: HomeAssistant) -> Path:
    """Return the integration's external data dir under the HA config directory."""

    return Path(hass.config.path("eybond_local")).resolve()


def _configure_local_metadata_roots(hass: HomeAssistant) -> None:
    """Configure external profile/schema roots under the HA config directory."""

    from .metadata.profile_loader import set_external_profile_roots
    from .metadata.register_schema_loader import set_external_register_schema_roots

    custom_root = _eybond_config_data_root(hass)
    set_external_profile_roots((custom_root / "profiles",))
    set_external_register_schema_roots((custom_root / "register_schemas",))


def _prime_metadata_caches(config_data_root: Path | None = None) -> None:
    """Warm metadata loaders so async startup paths do not hit disk directly."""

    from .drivers.registry import prime_metadata_caches
    from .metadata.device_catalog_loader import refresh_force_unsupported_override

    prime_metadata_caches()
    # Read the on-device force-unsupported sentinel here (executor) so the
    # detection path never stats it inside the event loop.
    refresh_force_unsupported_override(config_data_root)


async def async_setup(hass: HomeAssistant, _config: dict) -> bool:
    """Initialize shared loader state for the integration."""

    from .services import async_setup_services
    from .support.download import async_register_support_package_download_view

    try:
        _configure_local_metadata_roots(hass)
        await hass.async_add_executor_job(
            _prime_metadata_caches, _eybond_config_data_root(hass)
        )
        await async_setup_services(hass)
        async_register_support_package_download_view(hass)
    except Exception:
        logger.exception("Failed to initialize EyeBond Local integration bootstrap")
        raise
    return True


async def _async_initial_refresh_for_setup(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> None:
    """Run the first coordinator refresh without letting startup hang forever."""

    primed = False
    prime = getattr(coordinator, "prime_startup_snapshot", None)
    if callable(prime):
        try:
            primed = bool(prime())
        except Exception:
            logger.debug(
                "Failed to prime EyeBond startup snapshot for entry %s",
                entry.entry_id,
                exc_info=True,
            )

    refresh_task = hass.async_create_task(coordinator.async_refresh())
    _register_background_refresh_task(hass, entry, refresh_task)
    if primed:
        logger.info(
            "Primed EyeBond startup snapshot for entry %s; live refresh continues in background",
            entry.entry_id,
        )
        return

    try:
        await asyncio.wait_for(
            asyncio.shield(refresh_task),
            timeout=_SETUP_INITIAL_REFRESH_TIMEOUT,
        )
    except asyncio.TimeoutError:
        log = logger.info if _entry_has_startup_entity_fallback(entry) else logger.warning
        log(
            "Initial EyeBond refresh timed out after %.1fs for entry %s; continuing setup while refresh finishes in background",
            _SETUP_INITIAL_REFRESH_TIMEOUT,
            entry.entry_id,
        )



def _register_background_refresh_task(
    hass: HomeAssistant,
    entry: ConfigEntry,
    refresh_task: asyncio.Task,
) -> None:
    """Track one setup background refresh and log late failures."""

    def _log_background_refresh_result(task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception(
                "Background EyeBond refresh failed during setup for entry %s",
                entry.entry_id,
            )

    refresh_task.add_done_callback(_log_background_refresh_result)
    entry.async_on_unload(partial(_cancel_task_callback, refresh_task))


def _entry_has_startup_entity_fallback(entry: ConfigEntry) -> bool:
    """Return whether entity setup can proceed from persisted metadata."""

    data = getattr(entry, "data", {}) or {}
    options = getattr(entry, "options", {}) or {}
    driver_hint = str(
        options.get(CONF_DRIVER_HINT, data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO))
        or DRIVER_HINT_AUTO
    ).strip()
    if driver_hint and driver_hint != DRIVER_HINT_AUTO:
        return True
    return bool(
        str(data.get(CONF_DETECTED_MODEL) or "").strip()
        or str(data.get(CONF_DETECTED_SERIAL) or "").strip()
    )


async def _async_self_heal_server_ip(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Persist a valid local listener IP if the stored one has gone stale."""

    from .runtime.link import resolve_server_ip

    if entry.data.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND) != CONNECTION_TYPE_EYBOND:
        return

    configured_server_ip = entry.options.get(
        CONF_SERVER_IP,
        entry.data.get(CONF_SERVER_IP, ""),
    )
    collector_ip = str(entry.data.get("collector_ip", "") or "").strip()
    resolved_server_ip = await hass.async_add_executor_job(
        partial(resolve_server_ip, configured_server_ip, collector_ip=collector_ip),
    )
    if not resolved_server_ip or resolved_server_ip == configured_server_ip:
        return

    data = dict(entry.data)
    options = dict(entry.options)
    changed = False

    if data.get(CONF_SERVER_IP) != resolved_server_ip:
        data[CONF_SERVER_IP] = resolved_server_ip
        changed = True
    if CONF_SERVER_IP in options and options.get(CONF_SERVER_IP) != resolved_server_ip:
        options[CONF_SERVER_IP] = resolved_server_ip
        changed = True

    if not changed:
        return

    logger.warning(
        "Healing stale EyeBond server_ip from %s to %s for entry %s",
        configured_server_ip,
        resolved_server_ip,
        entry.entry_id,
    )
    hass.config_entries.async_update_entry(
        entry,
        data=data,
        options=options,
    )


async def _async_self_heal_collector_operation_mode(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Persist a valid collector callback ownership mode on older entries."""

    raw_mode = str(
        entry.options.get(
            CONF_COLLECTOR_OPERATION_MODE,
            entry.data.get(CONF_COLLECTOR_OPERATION_MODE, DEFAULT_COLLECTOR_OPERATION_MODE),
        )
        or DEFAULT_COLLECTOR_OPERATION_MODE
    ).strip()
    mode = raw_mode if raw_mode in COLLECTOR_OPERATION_MODES else DEFAULT_COLLECTOR_OPERATION_MODE

    data = dict(entry.data)
    options = dict(entry.options)
    changed = False
    if data.get(CONF_COLLECTOR_OPERATION_MODE) != mode:
        data[CONF_COLLECTOR_OPERATION_MODE] = mode
        changed = True
    if options.get(CONF_COLLECTOR_OPERATION_MODE) != mode:
        options[CONF_COLLECTOR_OPERATION_MODE] = mode
        changed = True
    if not changed:
        return

    update_entry = getattr(hass.config_entries, "async_update_entry", None)
    if update_entry is None:
        return

    update_entry(
        entry,
        data=data,
        options=options,
    )


def _known_collector_cloud_family(value: object) -> str:
    from .collector.cloud_family import COLLECTOR_CLOUD_FAMILY_UNKNOWN

    family = str(value or "").strip().lower()
    if not family or family == COLLECTOR_CLOUD_FAMILY_UNKNOWN:
        return ""
    return family


def _cloud_family_from_entry_endpoint_shape(entry: ConfigEntry) -> str:
    from .collector.cloud_family import (
        COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY,
        collector_cloud_family_observation_from_endpoint,
    )
    from .collector_endpoint import inspect_collector_server_endpoint

    endpoint = entry.options.get(
        CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
        entry.data.get("collector_server_endpoint", ""),
    )
    observation = collector_cloud_family_observation_from_endpoint(endpoint)
    family = _known_collector_cloud_family(observation.family)
    if family:
        return family

    try:
        parsed = inspect_collector_server_endpoint(
            str(endpoint or ""),
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return ""

    if not parsed.has_explicit_port:
        return COLLECTOR_CLOUD_FAMILY_LEGACY_BINARY
    return ""


def _collector_cloud_family_for_entity_filter(entry: ConfigEntry | None, coordinator) -> str:
    """Return the best collector family available while filtering entity surfaces."""

    family = _known_collector_cloud_family(
        getattr(coordinator, "collector_cloud_family", "")
    )
    if family:
        return family

    snapshot = getattr(coordinator, "data", None)
    values = getattr(snapshot, "values", {}) if snapshot is not None else {}
    if isinstance(values, dict):
        family = _known_collector_cloud_family(values.get(CONF_COLLECTOR_CLOUD_FAMILY))
        if family:
            return family

    if entry is not None:
        data = getattr(entry, "data", {}) or {}
        family = _known_collector_cloud_family(data.get(CONF_COLLECTOR_CLOUD_FAMILY))
        if family:
            return family
        if hasattr(entry, "data") and hasattr(entry, "options"):
            return _cloud_family_from_entry_endpoint_shape(entry)
    return ""


async def _async_self_heal_collector_cloud_family(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Restore callback cloud family when older runtime state persisted unknown."""

    if _known_collector_cloud_family(entry.data.get(CONF_COLLECTOR_CLOUD_FAMILY)):
        return

    family = _cloud_family_from_entry_endpoint_shape(entry)
    registry_record = None
    if not family:
        from .support.collector_registry import (
            get_collector_registry_record,
            get_collector_registry_record_by_last_seen_ip,
        )

        collector_pn = str(entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
        collector_ip = str(entry.data.get(CONF_COLLECTOR_IP, "") or "").strip()
        hass_config = getattr(hass, "config", None)
        config_dir_raw = str(getattr(hass_config, "config_dir", "") or "").strip()
        if not config_dir_raw:
            return
        config_dir = Path(config_dir_raw)
        try:
            registry_record = await hass.async_add_executor_job(
                lambda: (
                    get_collector_registry_record(
                        config_dir=config_dir,
                        collector_pn=collector_pn,
                    )
                    if collector_pn
                    else None
                )
            )
            if registry_record is None and collector_ip:
                registry_record = await hass.async_add_executor_job(
                    lambda: get_collector_registry_record_by_last_seen_ip(
                        config_dir=config_dir,
                        last_seen_ip=collector_ip,
                    )
                )
        except Exception as exc:
            logger.debug("Could not read EyeBond collector registry during family self-heal: %s", exc)
            registry_record = None

        if registry_record is not None:
            from .collector.transport_profile import known_collector_cloud_family

            family = known_collector_cloud_family(registry_record.cloud_profile_key)
            if not family:
                from .collector.cloud_family import collector_cloud_family_observation_from_endpoint

                observation = collector_cloud_family_observation_from_endpoint(
                    registry_record.original_endpoint_raw
                )
                family = _known_collector_cloud_family(observation.family)

    if not family:
        return

    data = dict(entry.data)
    options = dict(entry.options)
    data[CONF_COLLECTOR_CLOUD_FAMILY] = family
    if registry_record is not None and registry_record.original_endpoint_raw:
        options.setdefault(
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
            registry_record.original_endpoint_raw,
        )
        options.setdefault(
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
            registry_record.cloud_profile_key or family,
        )
        options.setdefault(
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
            registry_record.source or "collector_registry",
        )
        options.setdefault(
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
            registry_record.observed_at,
        )
    hass.config_entries.async_update_entry(entry, data=data, options=options)


async def _async_self_heal_valuecloud_driver_hint(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Migrate stale ValueCloud pre-architecture driver hints to the canonical driver key."""

    family = _known_collector_cloud_family(entry.data.get(CONF_COLLECTOR_CLOUD_FAMILY))
    if family != "valuecloud_at":
        return

    data = dict(entry.data)
    options = dict(entry.options)
    changed = False
    for source in (data, options):
        hint = str(source.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO) or DRIVER_HINT_AUTO).strip()
        if hint != "valuecloud_pi30":
            continue
        source[CONF_DRIVER_HINT] = "eybond_g_ascii"
        changed = True

    if not changed:
        return

    logger.warning(
        "Migrating stale EyeBond ValueCloud driver_hint from valuecloud_pi30 to eybond_g_ascii for entry %s",
        entry.entry_id,
    )
    hass.config_entries.async_update_entry(entry, data=data, options=options)


def _entity_unique_id(entry_id: str, domain: str, key: str) -> str:
    """Return the unique_id format used by one HA entity platform."""

    if domain == "sensor":
        return f"{entry_id}_{key}"
    return f"{entry_id}_{domain}_{key}"


def _preset_unique_id(entry_id: str, key: str) -> str:
    """Return the unique_id format used by preset buttons."""

    return f"{entry_id}_preset_{key}"


def _tool_unique_id(entry_id: str, key: str) -> str:
    """Return the unique_id format used by tooling buttons."""

    return f"{entry_id}_tool_{key}"


def _text_unique_id(entry_id: str, key: str) -> str:
    """Return the unique_id format used by text entities."""

    return f"{entry_id}_text_{key}"


def _coordinator_has_inverter_identity(coordinator, inverter=None) -> bool:
    """Return inverter identity state while tolerating lightweight test doubles."""

    has_identity = getattr(coordinator, "has_inverter_identity", None)
    if has_identity is not None:
        return bool(has_identity)
    if inverter is None:
        inverter = getattr(getattr(coordinator, "data", None), "inverter", None)
    return inverter is not None


async def _async_self_heal_entry_title(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Migrate legacy inverter-first config-entry titles to collector-first titles."""

    preferred_title = installation_title(
        collector_pn=entry.data.get("collector_pn", ""),
        collector_ip=entry.data.get("collector_ip", ""),
        detected_model=entry.data.get("detected_model", ""),
        detected_serial=entry.data.get("detected_serial", ""),
    )
    current_title = str(entry.title or "").strip()
    if not preferred_title or current_title == preferred_title:
        return

    legacy_titles = legacy_installation_titles(
        detected_model=entry.data.get("detected_model", ""),
        detected_serial=entry.data.get("detected_serial", ""),
        collector_ip=entry.data.get("collector_ip", ""),
        server_ip=entry.data.get(CONF_SERVER_IP, ""),
    )
    if current_title not in legacy_titles:
        return

    logger.warning(
        "Updating EyeBond entry title from %s to %s for entry %s",
        current_title,
        preferred_title,
        entry.entry_id,
    )
    hass.config_entries.async_update_entry(entry, title=preferred_title)


def _is_integration_disabled(disabled_by: object, integration_disabler: object) -> bool:
    """Return whether one entity-registry disabled_by marker means integration-disabled."""

    if disabled_by is None:
        return False

    normalized_disabled_by = str(disabled_by).strip().lower()
    expected = {"integration"}

    normalized_disabler = str(integration_disabler).strip().lower()
    if normalized_disabler:
        expected.add(normalized_disabler)

    disabler_value = getattr(integration_disabler, "value", None)
    if disabler_value is not None:
        normalized_value = str(disabler_value).strip().lower()
        if normalized_value:
            expected.add(normalized_value)

    return normalized_disabled_by in expected


def _default_enabled_unique_ids(entry_id: str) -> set[str]:
    """Return all entity unique_ids that should be enabled by default."""

    from .derived_energy import default_enabled_derived_energy_keys
    from .text import default_enabled_collector_text_keys_for_runtime
    from .drivers.registry import (
        all_binary_sensors,
        all_capability_presets,
        all_measurements,
        all_write_capabilities,
    )
    from .schema import entity_kind_for_capability

    expected: set[str] = set()
    for measurement in all_measurements():
        if measurement.enabled_default:
            expected.add(_entity_unique_id(entry_id, "sensor", measurement.key))

    for key in default_enabled_derived_energy_keys():
        expected.add(_entity_unique_id(entry_id, "sensor", key))

    for description in all_binary_sensors():
        if description.enabled_default:
            expected.add(_entity_unique_id(entry_id, "binary_sensor", description.key))

    for key in default_enabled_collector_text_keys_for_runtime():
        expected.add(_text_unique_id(entry_id, key))

    for key in _DEFAULT_ENABLED_RUNTIME_SELECT_KEYS:
        expected.add(_entity_unique_id(entry_id, "select", key))

    expected.add(_entity_unique_id(entry_id, "number", CONF_PROXY_CAPTURE_DURATION_MINUTES))

    for capability in all_write_capabilities():
        if not capability.enabled_default:
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button"}:
            expected.add(_entity_unique_id(entry_id, entity_kind, capability.key))

    for preset in all_capability_presets():
        if not preset.advanced:
            expected.add(_preset_unique_id(entry_id, preset.key))

    return expected


def _coordinator_proxy_capture_allowed(coordinator: object) -> bool:
    capabilities = getattr(coordinator, "collector_capabilities", None)
    return bool(getattr(capabilities, "proxy_capture", True))


def _default_enabled_unique_ids_for_current_runtime(
    entry_id: str,
    coordinator,
    driver,
    inverter,
    can_expose_capability,
    can_expose_preset,
    has_inverter_identity: bool | None = None,
) -> set[str]:
    """Return default-enabled unique_ids for the currently detected runtime metadata."""

    from .derived_energy import default_enabled_derived_energy_keys
    from .drivers.registry import binary_sensors_for_runtime, measurements_for_runtime
    from .select import default_enabled_runtime_select_keys_for_runtime
    from .schema import entity_kind_for_capability
    from .text import default_enabled_collector_text_keys_for_runtime
    from .tooling import default_enabled_tooling_button_keys_for_runtime

    driver_key = driver.key if driver is not None else None
    register_schema_name = getattr(inverter, "register_schema_name", "") if inverter is not None else ""
    capabilities = (
        inverter.capabilities
        if inverter is not None
        else (driver.write_capabilities if driver is not None else ())
    )
    capability_keys = {capability.key for capability in capabilities}
    profile_name = getattr(inverter, "profile_name", "") if inverter is not None else ""
    if has_inverter_identity is None:
        has_inverter_identity = _coordinator_has_inverter_identity(coordinator, inverter)
    presets = (
        inverter.capability_presets
        if inverter is not None
        else (driver.capability_presets if driver is not None else ())
    )
    measurement_descriptions = measurements_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        variant_key=(getattr(inverter, "variant_key", "") or None) if inverter is not None else None,
        write_capabilities=capabilities,
        include_all_drivers_when_unknown=False,
        collector_only_mode=not has_inverter_identity,
    )
    measurement_descriptions = filter_learned_read_measurements_for_activation(
        measurement_descriptions,
        entry_data=getattr(getattr(coordinator, "config_entry", None), "data", None),
        entry_options=getattr(getattr(coordinator, "config_entry", None), "options", None),
    )
    binary_sensor_descriptions = binary_sensors_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        include_all_drivers_when_unknown=False,
    )

    expected: set[str] = set()
    collector_cloud_family = _collector_cloud_family_for_entity_filter(
        getattr(coordinator, "config_entry", None),
        coordinator,
    )
    for measurement in measurement_descriptions:
        if is_legacy_disabled_signal_entity_key(measurement.key, collector_cloud_family):
            continue
        if measurement.enabled_default:
            expected.add(_entity_unique_id(entry_id, "sensor", measurement.key))

    for key in default_enabled_derived_energy_keys():
        expected.add(_entity_unique_id(entry_id, "sensor", key))

    for description in binary_sensor_descriptions:
        if description.enabled_default:
            expected.add(_entity_unique_id(entry_id, "binary_sensor", description.key))

    for key in default_enabled_collector_text_keys_for_runtime():
        expected.add(_text_unique_id(entry_id, key))

    for key in default_enabled_runtime_select_keys_for_runtime(
        has_inverter_identity=has_inverter_identity,
    ):
        expected.add(_entity_unique_id(entry_id, "select", key))

    collector_proxy_capture_allowed = _coordinator_proxy_capture_allowed(coordinator)

    if (
        collector_proxy_capture_allowed
        and hasattr(coordinator, "async_set_proxy_capture_duration_minutes")
    ):
        expected.add(_entity_unique_id(entry_id, "number", CONF_PROXY_CAPTURE_DURATION_MINUTES))

    for key in default_enabled_tooling_button_keys_for_runtime(
        capability_keys,
        profile_name,
        has_inverter_identity=has_inverter_identity,
        collector_proxy_capture_allowed=collector_proxy_capture_allowed,
    ):
        expected.add(_tool_unique_id(entry_id, key))

    for capability in capabilities:
        if not capability.enabled_default:
            continue
        if not can_expose_capability(capability):
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button"}:
            expected.add(_entity_unique_id(entry_id, entity_kind, capability.key))

    for preset in presets:
        if preset.advanced:
            continue
        if not can_expose_preset(preset):
            continue
        expected.add(_preset_unique_id(entry_id, preset.key))

    return expected


async def _async_self_heal_enabled_defaults(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> None:
    """Re-enable newly default-enabled entities that were previously auto-disabled."""

    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers.entity_registry import RegistryEntryDisabler

    registry = er.async_get(hass)
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    expected_unique_ids = await hass.async_add_executor_job(
        _default_enabled_unique_ids_for_current_runtime,
        entry.entry_id,
        coordinator,
        driver,
        inverter,
        coordinator.can_expose_capability,
        coordinator.can_expose_preset,
        has_inverter_identity,
    )
    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in expected_unique_ids:
            continue
        if not _is_integration_disabled(
            entity_entry.disabled_by,
            RegistryEntryDisabler.INTEGRATION,
        ):
            continue
        logger.warning(
            "Re-enabling newly default-enabled entity %s for entry %s",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_update_entity(entity_entry.entity_id, disabled_by=None)


async def _async_self_heal_expert_defaults(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Reconcile expert-only entities against the current control mode."""

    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers.entity_registry import RegistryEntryDisabler

    registry = er.async_get(hass)
    coordinator = getattr(entry, "runtime_data", None)
    expose_expert_entities = getattr(coordinator, "control_mode", "") == CONTROL_MODE_FULL
    expert_only_unique_ids: set[str] = {
        _text_unique_id(entry.entry_id, "collector_callback_endpoint"),
    }
    if not expert_only_unique_ids:
        return

    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in expert_only_unique_ids:
            continue
        if expose_expert_entities:
            if not _is_integration_disabled(
                entity_entry.disabled_by,
                RegistryEntryDisabler.INTEGRATION,
            ):
                continue
            logger.warning(
                "Re-enabling full-control expert entity %s for entry %s",
                entity_entry.entity_id,
                entry.entry_id,
            )
            registry.async_update_entity(entity_entry.entity_id, disabled_by=None)
            continue
        if entity_entry.disabled_by is not None:
            continue
        logger.warning(
            "Disabling newly expert-only entity %s for entry %s",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_update_entity(
            entity_entry.entity_id,
            disabled_by=RegistryEntryDisabler.INTEGRATION,
        )


async def _async_remove_legacy_runtime_select_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Remove runtime select entities that were migrated into config flow options."""

    from homeassistant.helpers import entity_registry as er

    registry = er.async_get(hass)
    legacy_unique_ids = {
        _entity_unique_id(entry.entry_id, "select", CONF_CONTROL_MODE),
    }

    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id not in legacy_unique_ids:
            continue
        logger.warning(
            "Removing legacy runtime select %s for entry %s after config-flow migration",
            entity_entry.entity_id,
            entry.entry_id,
        )
        registry.async_remove(entity_entry.entity_id)


def _infer_sensor_display_precision(value: float) -> int | None:
    """Infer a stable display precision for one float-like sensor value."""

    if not isfinite(value):
        return None
    if value.is_integer():
        return 1
    text = format(value, ".6f").rstrip("0")
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1])


async def _async_self_heal_sensor_display_precision(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Repair stale zero-precision sensor overrides after runtime values are known."""

    from homeassistant.helpers import entity_registry as er

    from .drivers.registry import measurements_for_runtime

    coordinator = getattr(entry, "runtime_data", None)
    if coordinator is None:
        return

    registry = er.async_get(hass)
    update_entity_options = getattr(registry, "async_update_entity_options", None)
    if not callable(update_entity_options):
        return

    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    driver_key = driver.key if driver is not None else None
    register_schema_name = getattr(inverter, "register_schema_name", "") if inverter is not None else ""
    write_capabilities = (
        inverter.capabilities
        if inverter is not None
        else (driver.write_capabilities if driver is not None else ())
    )
    descriptions_by_key = {
        description.key: description
        for description in measurements_for_runtime(
            driver_key=driver_key,
            register_schema_name=register_schema_name,
            variant_key=(getattr(inverter, "variant_key", "") or None) if inverter is not None else None,
            write_capabilities=write_capabilities,
            include_all_drivers_when_unknown=False,
            collector_only_mode=not has_inverter_identity,
        )
    }
    values = coordinator.data.values
    unique_id_prefix = f"{entry.entry_id}_"

    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        entity_id = getattr(entity_entry, "entity_id", None)
        unique_id = str(getattr(entity_entry, "unique_id", "") or "")
        if not entity_id or not unique_id.startswith(unique_id_prefix):
            continue

        description = descriptions_by_key.get(unique_id[len(unique_id_prefix) :])
        if description is None:
            continue

        desired_precision = description.suggested_display_precision
        if desired_precision is None and description.device_class in _FLOAT_PRECISION_DEVICE_CLASSES:
            native_value = values.get(description.key)
            if isinstance(native_value, float):
                desired_precision = _infer_sensor_display_precision(native_value)
        if desired_precision is None:
            continue

        options = dict(getattr(entity_entry, "options", {}) or {})
        sensor_options = dict(options.get("sensor") or {})
        current_precision = sensor_options.get("suggested_display_precision")
        if current_precision == desired_precision:
            continue
        if current_precision not in (None, 0):
            continue

        sensor_options["suggested_display_precision"] = desired_precision
        update_entity_options(entity_id, "sensor", sensor_options)


async def _async_finalize_expert_entity_migration(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Run expert-only entity migration after platform setup finishes."""

    async_block_till_done = getattr(hass, "async_block_till_done", None)
    if async_block_till_done is not None:
        try:
            await asyncio.wait_for(
                async_block_till_done(),
                timeout=_EXPERT_ENTITY_MIGRATION_SETTLE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.info(
                "Timed out waiting to finalize EyeBond expert entity migration for entry %s; continuing best-effort cleanup",
                entry.entry_id,
            )
    await _async_self_heal_expert_defaults(hass, entry)
    if getattr(entry, "runtime_data", None) is not None:
        await _async_remove_legacy_runtime_select_entities(hass, entry)
    await _async_self_heal_sensor_display_precision(hass, entry)


async def _async_cleanup_obsolete_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> None:
    """Remove entity-registry entries that no longer belong to this entry's driver."""
    cleanup_allowed, cleanup_reason = _cleanup_obsolete_entities_allowed(coordinator)
    if not cleanup_allowed:
        logger.debug(
            "Skipping obsolete entity cleanup for entry %s: %s",
            entry.entry_id,
            cleanup_reason,
        )
        return

    from homeassistant.helpers import entity_registry as er

    from .button import _tooling_button_specs
    from .derived_energy import (
        derived_energy_cycle_descriptions_for_keys,
        derived_energy_descriptions_for_keys,
        derived_energy_entity_descriptions_for_keys,
    )
    from .drivers.registry import binary_sensors_for_runtime, measurements_for_runtime
    from .select import runtime_select_keys_for_runtime
    from .schema import entity_kind_for_capability
    from .text import collector_text_keys_for_runtime
    from .tooling import tooling_button_keys_for_runtime

    registry = er.async_get(hass)
    driver, inverter, has_inverter_identity = entity_setup_context(entry, coordinator)
    driver_key = driver.key if driver is not None else None
    register_schema_name = getattr(inverter, "register_schema_name", "") if inverter is not None else ""
    capabilities = (
        inverter.capabilities
        if inverter is not None
        else (driver.write_capabilities if driver is not None else ())
    )
    capability_keys = {capability.key for capability in capabilities}
    profile_name = getattr(inverter, "profile_name", "") if inverter is not None else ""
    presets = (
        inverter.capability_presets
        if inverter is not None
        else (driver.capability_presets if driver is not None else ())
    )
    measurement_descriptions = measurements_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        variant_key=(getattr(inverter, "variant_key", "") or None) if inverter is not None else None,
        write_capabilities=capabilities,
        include_all_drivers_when_unknown=False,
        collector_only_mode=not has_inverter_identity,
    )
    measurement_descriptions = filter_learned_read_measurements_for_activation(
        measurement_descriptions,
        entry_data=getattr(entry, "data", None),
        entry_options=getattr(entry, "options", None),
    )
    binary_sensor_descriptions = binary_sensors_for_runtime(
        driver_key=driver_key,
        register_schema_name=register_schema_name,
        include_all_drivers_when_unknown=False,
    )
    collector_proxy_capture_allowed = _coordinator_proxy_capture_allowed(coordinator)
    measurement_keys = {description.key for description in measurement_descriptions}
    runtime_keys = measurement_keys | {
        description.key for description in binary_sensor_descriptions
    }
    derived_energy_source_descriptions = derived_energy_descriptions_for_keys(
        measurement_keys
    )
    derived_energy_descriptions = derived_energy_entity_descriptions_for_keys(
        measurement_keys
    )
    expected_unique_ids: set[str] = {
        _entity_unique_id(entry.entry_id, "sensor", description.key)
        for description in measurement_descriptions
        if not is_legacy_disabled_signal_entity_key(
            description.key,
            _collector_cloud_family_for_entity_filter(entry, coordinator),
        )
    }
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "sensor", description.key)
        for description in derived_energy_descriptions
    )
    derived_energy_keys = {
        description.key
        for description in derived_energy_source_descriptions
    }
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "sensor", description.key)
        for description in derived_energy_cycle_descriptions_for_keys(
            runtime_keys | derived_energy_keys
        )
    )
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "binary_sensor", description.key)
        for description in binary_sensor_descriptions
    )
    expected_unique_ids.update(
        _tool_unique_id(entry.entry_id, key)
        for key in tooling_button_keys_for_runtime(
            capability_keys,
            profile_name,
            has_inverter_identity=has_inverter_identity,
            collector_proxy_capture_allowed=collector_proxy_capture_allowed,
        )
    )
    expected_unique_ids.update(
        _text_unique_id(entry.entry_id, key)
        for key in collector_text_keys_for_runtime()
    )
    expected_unique_ids.update(
        _entity_unique_id(entry.entry_id, "select", key)
        for key in runtime_select_keys_for_runtime(
            has_inverter_identity=has_inverter_identity,
        )
    )
    if (
        collector_proxy_capture_allowed
        and hasattr(coordinator, "async_set_proxy_capture_duration_minutes")
    ):
        expected_unique_ids.add(
            _entity_unique_id(entry.entry_id, "number", CONF_PROXY_CAPTURE_DURATION_MINUTES)
        )
    for capability in capabilities:
        if not coordinator.can_expose_capability(capability):
            continue
        entity_kind = entity_kind_for_capability(capability)
        if entity_kind in {"select", "number", "switch", "button"}:
            expected_unique_ids.add(_entity_unique_id(entry.entry_id, entity_kind, capability.key))
    for preset in presets:
        if not coordinator.can_expose_preset(preset):
            continue
        expected_unique_ids.add(_preset_unique_id(entry.entry_id, preset.key))

    removable = []
    for entity_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity_entry.unique_id in expected_unique_ids:
            continue
        removable.append(entity_entry.entity_id)

    for entity_id in removable:
        logger.warning(
            "Removing obsolete entity %s for entry %s after driver-specific metadata refresh",
            entity_id,
            entry.entry_id,
        )
        registry.async_remove(entity_id)


def _cleanup_obsolete_entities_allowed(coordinator) -> tuple[bool, str]:
    """Return whether destructive cleanup can safely run for current metadata state."""

    if getattr(coordinator, "identified_inverter", None) is not None:
        return True, "live_inverter_identity"

    snapshot = getattr(coordinator, "effective_metadata_snapshot", None)
    if snapshot is None or not bool(getattr(snapshot, "is_valid", False)):
        return False, "missing_valid_effective_metadata_snapshot"

    effective_metadata = getattr(coordinator, "effective_metadata", None)
    if effective_metadata is None:
        return False, "effective_metadata_unavailable"

    effective_owner_key = str(getattr(effective_metadata, "effective_owner_key", "") or "").strip()
    effective_profile_name = str(getattr(effective_metadata, "profile_name", "") or "").strip()
    effective_register_schema_name = str(
        getattr(effective_metadata, "register_schema_name", "") or ""
    ).strip()
    if not (effective_owner_key and effective_profile_name and effective_register_schema_name):
        return False, "effective_metadata_incomplete"

    snapshot_owner_key = str(getattr(snapshot, "effective_owner_key", "") or "").strip()
    snapshot_profile_name = str(getattr(snapshot, "profile_name", "") or "").strip()
    snapshot_register_schema_name = str(
        getattr(snapshot, "register_schema_name", "") or ""
    ).strip()
    if (
        effective_owner_key != snapshot_owner_key
        or effective_profile_name != snapshot_profile_name
        or effective_register_schema_name != snapshot_register_schema_name
    ):
        return False, "effective_metadata_mismatch_from_snapshot"

    profile_metadata = getattr(effective_metadata, "profile_metadata", None)
    register_schema_metadata = getattr(effective_metadata, "register_schema_metadata", None)
    if profile_metadata is None or register_schema_metadata is None:
        return False, "effective_metadata_assets_unresolved"

    return True, "snapshot_metadata_consistent"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up EyeBond Local from a config entry."""

    from .runtime.coordinator import EybondLocalCoordinator
    from .services import async_setup_services
    from .support.download import async_register_support_package_download_view

    coordinator = None
    try:
        _configure_local_metadata_roots(hass)
        await async_setup_services(hass)
        async_register_support_package_download_view(hass)
        await _async_self_heal_server_ip(hass, entry)
        await _async_self_heal_collector_operation_mode(hass, entry)
        await _async_self_heal_collector_cloud_family(hass, entry)
        await _async_self_heal_valuecloud_driver_hint(hass, entry)
        await _async_self_heal_entry_title(hass, entry)
        coordinator = EybondLocalCoordinator(hass, entry)
        await coordinator.async_setup()
        entry.runtime_data = coordinator
        _register_entry_stop_shutdown(hass, entry, coordinator)
        _register_entry_network_reconcile(hass, entry, coordinator)
        await _async_initial_refresh_for_setup(hass, entry, coordinator)
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
        expert_migration_task = hass.async_create_task(
            _async_finalize_expert_entity_migration(hass, entry)
        )
        entry.async_on_unload(partial(_cancel_task_callback, expert_migration_task))
        coordinator.async_sync_device_registry()
        entry.async_on_unload(entry.add_update_listener(_async_update_listener))
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
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""

    from .runtime.coordinator import EybondLocalCoordinator

    coordinator: EybondLocalCoordinator = entry.runtime_data
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        await coordinator.async_shutdown()
    return unload_ok


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Drop saved cloud-evidence files for the entry being removed."""

    from .support.cloud_evidence import remove_cloud_evidence_for_entry

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
