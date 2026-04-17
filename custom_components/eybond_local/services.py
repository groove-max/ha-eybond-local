"""Home Assistant services for experimental local metadata workflows."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .const import (
    DOMAIN,
    SERVICE_CREATE_LOCAL_PROFILE_DRAFT,
    SERVICE_CREATE_LOCAL_SCHEMA_DRAFT,
    SERVICE_RELOAD_LOCAL_METADATA,
)
from .metadata.local_metadata import (
    clear_local_metadata_loader_caches,
    create_local_profile_draft,
    create_local_schema_draft,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant, ServiceCall


_SERVICES_READY_KEY = "services_ready"


async def async_setup_services(hass: HomeAssistant) -> None:
    """Register EyeBond Local domain services once."""

    domain_data = hass.data.setdefault(DOMAIN, {})
    if domain_data.get(_SERVICES_READY_KEY):
        return

    from homeassistant.core import SupportsResponse

    async def handle_create_local_profile_draft(
        call: ServiceCall,
    ) -> dict[str, str]:
        return await _async_handle_create_local_profile_draft(hass, call)

    async def handle_create_local_schema_draft(
        call: ServiceCall,
    ) -> dict[str, str]:
        return await _async_handle_create_local_schema_draft(hass, call)

    async def handle_reload_local_metadata(
        call: ServiceCall,
    ) -> dict[str, str | int]:
        return await _async_handle_reload_local_metadata(hass, call)

    hass.services.async_register(
        DOMAIN,
        SERVICE_CREATE_LOCAL_PROFILE_DRAFT,
        handle_create_local_profile_draft,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_CREATE_LOCAL_SCHEMA_DRAFT,
        handle_create_local_schema_draft,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_RELOAD_LOCAL_METADATA,
        handle_reload_local_metadata,
        supports_response=SupportsResponse.ONLY,
    )
    domain_data[_SERVICES_READY_KEY] = True


async def _async_handle_create_local_profile_draft(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, str]:
    config_dir = Path(hass.config.config_dir)
    path = await hass.async_add_executor_job(
        lambda: create_local_profile_draft(
            config_dir=config_dir,
            source_profile_name=str(call.data["source_profile"]),
            output_profile_name=call.data.get("output_profile"),
            overwrite=bool(call.data.get("overwrite", False)),
        )
    )
    return {"created_path": str(path)}


async def _async_handle_create_local_schema_draft(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, str]:
    config_dir = Path(hass.config.config_dir)
    path = await hass.async_add_executor_job(
        lambda: create_local_schema_draft(
            config_dir=config_dir,
            source_schema_name=str(call.data["source_schema"]),
            output_schema_name=call.data.get("output_schema"),
            overwrite=bool(call.data.get("overwrite", False)),
        )
    )
    return {"created_path": str(path)}


async def _async_handle_reload_local_metadata(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, str | int]:
    clear_local_metadata_loader_caches()

    reloaded = 0
    for entry in hass.config_entries.async_entries(DOMAIN):
        await hass.config_entries.async_reload(entry.entry_id)
        reloaded += 1

    return {"status": "reloaded", "entries_reloaded": reloaded}
