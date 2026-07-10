"""Home Assistant services for local metadata workflows and guarded collector actions."""

from __future__ import annotations

from contextlib import suppress
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import voluptuous as vol

from .const import (
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    SERVICE_APPLY_COLLECTOR_CHANGES,
    SERVICE_BIND_COLLECTOR_TO_HOME_ASSISTANT,
    DOMAIN,
    SERVICE_CREATE_LOCAL_PROFILE_DRAFT,
    SERVICE_CREATE_LOCAL_SCHEMA_DRAFT,
    SERVICE_REBOOT_COLLECTOR,
    SERVICE_RELOAD_LOCAL_METADATA,
    SERVICE_ROLLBACK_COLLECTOR_SERVER_ENDPOINT,
    SERVICE_RUN_DIAGNOSTIC_COMMANDS,
    SERVICE_SET_COLLECTOR_SERVER_ENDPOINT,
    SERVICE_START_PROXY_CAPTURE,
    SERVICE_STOP_PROXY_CAPTURE,
)
from .metadata.local_metadata import (
    clear_local_metadata_loader_caches,
    create_local_profile_draft,
    create_local_schema_draft,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant, ServiceCall


_SERVICES_READY_KEY = "services_ready"

logger = logging.getLogger(__name__)


_BIND_COLLECTOR_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
        vol.Required("confirm_redirect"): bool,
    }
)
_APPLY_COLLECTOR_CHANGES_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
        vol.Required("confirm_restart"): bool,
    }
)
_REBOOT_COLLECTOR_SCHEMA = _APPLY_COLLECTOR_CHANGES_SCHEMA
_ROLLBACK_COLLECTOR_ENDPOINT_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
        vol.Optional("apply_changes", default=True): bool,
        vol.Required("confirm_redirect"): bool,
    }
)
_SET_COLLECTOR_ENDPOINT_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
        vol.Required("server_host"): str,
        vol.Required("server_port"): vol.All(int, vol.Range(min=1, max=65535)),
        vol.Optional("server_protocol", default="TCP"): vol.In(("TCP", "UDP")),
        vol.Optional("apply_changes", default=True): bool,
        vol.Required("confirm_redirect"): bool,
    }
)
_START_PROXY_CAPTURE_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
        vol.Optional(CONF_PROXY_CAPTURE_DURATION_MINUTES): vol.All(
            int, vol.Range(min=1, max=1440)
        ),
        vol.Optional("anonymized", default=True): bool,
        vol.Required("confirm_redirect"): bool,
    }
)
_STOP_PROXY_CAPTURE_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
    }
)
_RUN_DIAGNOSTIC_COMMANDS_SCHEMA = vol.Schema(
    {
        vol.Required("entry_id"): str,
        vol.Required("commands"): str,
        vol.Optional("stop_on_error", default=True): bool,
        # Required to be true before a scenario containing write/write_bit runs;
        # read-only scenarios ignore it.
        vol.Optional("confirm_write", default=False): bool,
        # Positivity is enforced in the handler so the schema stays within the
        # validator subset shared with the test stubs.
        vol.Optional("operation_timeout"): vol.All(vol.Range(min=0)),
    }
)


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

    async def handle_set_collector_server_endpoint(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_set_collector_server_endpoint(hass, call)

    async def handle_bind_collector_to_home_assistant(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_bind_collector_to_home_assistant(hass, call)

    async def handle_apply_collector_changes(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_apply_collector_changes(hass, call)

    async def handle_reboot_collector(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_reboot_collector(hass, call)

    async def handle_rollback_collector_server_endpoint(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_rollback_collector_server_endpoint(hass, call)

    async def handle_start_proxy_capture(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_start_proxy_capture(hass, call)

    async def handle_stop_proxy_capture(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_stop_proxy_capture(hass, call)

    async def handle_run_diagnostic_commands(
        call: ServiceCall,
    ) -> dict[str, object]:
        return await _async_handle_run_diagnostic_commands(hass, call)

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
    hass.services.async_register(
        DOMAIN,
        SERVICE_SET_COLLECTOR_SERVER_ENDPOINT,
        handle_set_collector_server_endpoint,
        schema=_SET_COLLECTOR_ENDPOINT_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_BIND_COLLECTOR_TO_HOME_ASSISTANT,
        handle_bind_collector_to_home_assistant,
        schema=_BIND_COLLECTOR_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_APPLY_COLLECTOR_CHANGES,
        handle_apply_collector_changes,
        schema=_APPLY_COLLECTOR_CHANGES_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_REBOOT_COLLECTOR,
        handle_reboot_collector,
        schema=_REBOOT_COLLECTOR_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_ROLLBACK_COLLECTOR_SERVER_ENDPOINT,
        handle_rollback_collector_server_endpoint,
        schema=_ROLLBACK_COLLECTOR_ENDPOINT_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_START_PROXY_CAPTURE,
        handle_start_proxy_capture,
        schema=_START_PROXY_CAPTURE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_STOP_PROXY_CAPTURE,
        handle_stop_proxy_capture,
        schema=_STOP_PROXY_CAPTURE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_RUN_DIAGNOSTIC_COMMANDS,
        handle_run_diagnostic_commands,
        schema=_RUN_DIAGNOSTIC_COMMANDS_SCHEMA,
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


async def _async_handle_set_collector_server_endpoint(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    return await coordinator.async_set_collector_server_endpoint(
        server_host=str(call.data.get("server_host") or ""),
        server_port=int(call.data.get("server_port") or 0),
        server_protocol=str(call.data.get("server_protocol") or "TCP"),
        apply_changes=bool(call.data.get("apply_changes", True)),
        confirm_redirect=bool(call.data.get("confirm_redirect", False)),
    )


async def _async_handle_bind_collector_to_home_assistant(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    return await coordinator.async_bind_collector_to_home_assistant(
        confirm_redirect=bool(call.data.get("confirm_redirect", False)),
    )


async def _async_handle_apply_collector_changes(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    return await coordinator.async_apply_collector_changes(
        confirm_restart=bool(call.data.get("confirm_restart", False)),
    )


async def _async_handle_reboot_collector(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    return await coordinator.async_reboot_collector(
        confirm_restart=bool(call.data.get("confirm_restart", False)),
    )


async def _async_handle_rollback_collector_server_endpoint(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    return await coordinator.async_rollback_collector_server_endpoint(
        apply_changes=bool(call.data.get("apply_changes", True)),
        confirm_redirect=bool(call.data.get("confirm_redirect", False)),
    )


async def _async_handle_start_proxy_capture(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    kwargs: dict[str, object] = {
        "anonymized": bool(call.data.get("anonymized", True)),
        "confirm_redirect": bool(call.data.get("confirm_redirect", False)),
    }
    if CONF_PROXY_CAPTURE_DURATION_MINUTES in call.data:
        kwargs["duration_minutes"] = call.data.get(CONF_PROXY_CAPTURE_DURATION_MINUTES)
    return await coordinator.async_start_proxy_capture(**kwargs)


async def _async_handle_stop_proxy_capture(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    return await coordinator.async_stop_proxy_capture()


async def _async_handle_run_diagnostic_commands(
    hass: HomeAssistant,
    call: ServiceCall,
) -> dict[str, object]:
    coordinator = _resolve_entry_coordinator(hass, call)
    integration_version = ""
    with suppress(Exception):
        from homeassistant.loader import async_get_integration

        integration = await async_get_integration(hass, DOMAIN)
        integration_version = str(getattr(integration, "version", "") or "")
    operation_timeout = call.data.get("operation_timeout")
    resolved_timeout: float | None = None
    if operation_timeout is not None:
        resolved_timeout = float(operation_timeout)
        if resolved_timeout <= 0:
            raise ValueError("operation_timeout_must_be_positive")
    return await coordinator.async_run_diagnostic_commands(
        commands=str(call.data.get("commands") or ""),
        stop_on_error=bool(call.data.get("stop_on_error", True)),
        operation_timeout=resolved_timeout,
        integration_version=integration_version,
        confirm_write=bool(call.data.get("confirm_write", False)),
        publish_download_copy=bool(call.data.get("publish_download_copy", False)),
    )


def _resolve_entry_coordinator(hass: HomeAssistant, call: ServiceCall):
    entry_id = str(call.data.get("entry_id") or "").strip()
    if not entry_id:
        raise ValueError("entry_id_required")

    entry = hass.config_entries.async_get_entry(entry_id)
    if entry is None or entry.domain != DOMAIN:
        raise ValueError("eybond_entry_not_found")

    coordinator = getattr(entry, "runtime_data", None)
    if coordinator is None:
        raise RuntimeError("eybond_entry_runtime_not_ready")
    return coordinator
