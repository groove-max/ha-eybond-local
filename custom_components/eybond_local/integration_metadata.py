"""Config-entry metadata bootstrap and self-healing helpers."""

from __future__ import annotations

import asyncio
from functools import partial
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from .const import (
    CONF_COLLECTOR_CLOUD_FAMILY,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_TYPE,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DRIVER_HINT,
    CONF_ENTRY_ROLE,
    CONF_SERVER_IP,
    CONNECTION_TYPE_EYBOND,
    DOMAIN,
    DRIVER_HINT_AUTO,
    ENTRY_ROLE_PENDING_COLLECTOR,
)
from .integration_common import _cancel_task_callback
from .naming import installation_title, legacy_installation_titles

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_SETUP_INITIAL_REFRESH_TIMEOUT = 20.0

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

async def _async_remove_obsolete_pending_entries(hass: HomeAssistant) -> None:
    """Delete incomplete beta2 drafts before any entry/network setup runs.

    ``pending_collector`` was never a configured device: it had no durable PN,
    runtime, entities, endpoint ownership, or permanent session claim. Current
    onboarding keeps incomplete work inside its config flow instead. Removing
    these exact-role tombstones is therefore a one-shot storage cleanup, not a
    compatibility lifecycle, and deliberately performs no collector I/O.
    """

    config_entries = getattr(hass, "config_entries", None)
    async_entries = getattr(config_entries, "async_entries", None)
    async_remove = getattr(config_entries, "async_remove", None)
    if not callable(async_entries) or not callable(async_remove):
        return
    obsolete: list[ConfigEntry] = []
    for entry in async_entries(DOMAIN):
        if _is_obsolete_pending_entry(entry):
            obsolete.append(entry)
    for entry in obsolete:
        logger.info(
            "Removing obsolete incomplete EyeBond setup entry %s",
            entry.entry_id,
        )
        await async_remove(entry.entry_id)


def _is_obsolete_pending_entry(entry: object) -> bool:
    """Recognize only the exact persisted beta2 tombstone role."""

    data = getattr(entry, "data", None)
    get = getattr(data, "get", None)
    if not callable(get):
        return False
    role = get(CONF_ENTRY_ROLE)
    return type(role) is str and role == ENTRY_ROLE_PENDING_COLLECTOR


async def _async_initial_refresh_for_setup(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> bool:
    """Prepare setup data and report whether live refresh must start afterward.

    A persisted startup snapshot is the stable metadata authority for entity
    construction. Starting a live refresh before platform forwarding can race
    that snapshot and replace it with a disconnected collector-only result.
    Callers must start the deferred refresh only after entity setup completes.
    """

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

    if primed:
        logger.info(
            "Primed EyeBond startup snapshot for entry %s; deferring live refresh until entity setup completes",
            entry.entry_id,
        )
        return True

    refresh_task = _start_background_refresh_for_setup(hass, entry, coordinator)

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
    return False


def _start_background_refresh_for_setup(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator,
) -> asyncio.Task:
    """Start and track one setup refresh after its metadata boundary is safe."""

    refresh_task = hass.async_create_task(coordinator.async_refresh())
    _register_background_refresh_task(hass, entry, refresh_task)
    return refresh_task

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
