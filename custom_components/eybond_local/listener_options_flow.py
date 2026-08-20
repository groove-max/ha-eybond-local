"""Options lifecycle for the passive-discovery listener entry."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult, OptionsFlow
from homeassistant.helpers.selector import BooleanSelector

CONF_CONFIRM_REDISCOVER_DEVICES = "confirm_rediscover_devices"


class ListenerOptionsFlow(OptionsFlow):
    """Service tools for the passive-discovery entry."""

    def __init__(self, config_entry) -> None:
        self._config_entry = config_entry
        self._rediscovery_connected_count = 0

    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        return await self.async_step_listener(user_input)

    async def async_step_listener(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Show user-facing passive-discovery maintenance actions."""

        return self.async_show_menu(
            step_id="listener",
            menu_options=["rediscover_devices"],
        )

    async def async_step_rediscover_devices(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Clear transient discovery suppression after explicit confirmation."""

        errors: dict[str, str] = {}
        if user_input is not None:
            if not bool(user_input.get(CONF_CONFIRM_REDISCOVER_DEVICES)):
                errors[CONF_CONFIRM_REDISCOVER_DEVICES] = "required"
            else:
                from .passive_discovery import get_passive_callback_discovery

                discovery = get_passive_callback_discovery(self.hass)
                if discovery is None:
                    errors["base"] = "passive_discovery_unavailable"
                else:
                    result = await discovery.async_show_discovered_devices_again()
                    self._rediscovery_connected_count = result.connected_unclaimed_count
                    return await self.async_step_rediscover_devices_done()

        return self.async_show_form(
            step_id="rediscover_devices",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_CONFIRM_REDISCOVER_DEVICES,
                        default=False,
                    ): BooleanSelector(),
                }
            ),
            errors=errors,
        )

    async def async_step_rediscover_devices_done(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Report the completed refresh before closing the options flow."""

        if user_input is not None:
            return self.async_create_entry(data=dict(self._config_entry.options))
        return self.async_show_form(
            step_id="rediscover_devices_done",
            data_schema=vol.Schema({}),
            description_placeholders={
                "connected_count": str(self._rediscovery_connected_count),
            },
        )


__all__ = ["ListenerOptionsFlow"]
