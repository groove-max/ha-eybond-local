"""Options step that writes short-ASCII auxiliary 0200 admission."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import BooleanSelector

from ...const import CONF_ADMIT_SHORT_ASCII_MPPT, DEFAULT_ADMIT_SHORT_ASCII_MPPT


class ShortAsciiMpptOptionsMixin:
    """Admit or revoke the short-ASCII auxiliary 0200 poll for this entry."""

    async def async_step_short_ascii_mppt(
        self, user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if user_input is not None:
            admitted = user_input.get(
                CONF_ADMIT_SHORT_ASCII_MPPT, DEFAULT_ADMIT_SHORT_ASCII_MPPT,
            )
            options = dict(self._config_entry.options)
            options[CONF_ADMIT_SHORT_ASCII_MPPT] = bool(admitted)
            return self.async_create_entry(data=options)

        current = self._config_entry.options.get(
            CONF_ADMIT_SHORT_ASCII_MPPT, DEFAULT_ADMIT_SHORT_ASCII_MPPT,
        )
        return self.async_show_form(
            step_id="short_ascii_mppt",
            data_schema=vol.Schema({
                vol.Optional(
                    CONF_ADMIT_SHORT_ASCII_MPPT,
                    description={"suggested_value": bool(current)},
                ): BooleanSelector(),
            }),
        )
