"""Presentation-only OEM branding independent of inverter protocol selection."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import TextSelector

from ...const import CONF_DETECTED_MODEL
from ...inverter_display import (
    INVERTER_DISPLAY_FIELDS,
    inverter_display_labels,
    normalize_display_label,
    with_inverter_display_labels,
)


class InverterDisplayOptionsMixin:
    """Let the owner label the enclosure without changing detected identity."""

    async def async_step_inverter_display(
        self, user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        coordinator = self._coordinator()
        values = inverter_display_labels(self._config_entry.options)
        if user_input is not None:
            for key in INVERTER_DISPLAY_FIELDS:
                try:
                    values[key] = normalize_display_label(user_input.get(key, ""))
                except ValueError:
                    errors[key] = "invalid_inverter_display_label"
            if not errors:
                if coordinator is not None:
                    coordinator.async_set_inverter_display_labels(values)
                    options = dict(self._config_entry.options)
                else:
                    options = with_inverter_display_labels(
                        self._config_entry.options, values,
                    )
                # For a loaded entry the coordinator has already persisted this
                # exact mapping with reload suppression. HA's normal options
                # commit is a no-op, not a second transport restart.
                return self.async_create_entry(data=options)

        metadata = getattr(coordinator, "effective_metadata", None)
        saved = self._config_entry.options.get("effective_metadata_snapshot", {})
        if not isinstance(saved, dict):
            saved = {}
        model = str(self._config_entry.data.get(CONF_DETECTED_MODEL) or "—")
        live_inverter = getattr(getattr(coordinator, "data", None), "inverter", None)
        model = str(getattr(live_inverter, "model_name", "") or model)
        profile = getattr(metadata, "profile_name", "") or saved.get("profile_name")
        schema = getattr(metadata, "register_schema_name", "") or saved.get("register_schema_name")
        return self.async_show_form(
            step_id="inverter_display",
            data_schema=vol.Schema({
                vol.Optional(
                    key, description={"suggested_value": values.get(key, "")},
                ): TextSelector()
                for key in INVERTER_DISPLAY_FIELDS
            }),
            errors=errors,
            description_placeholders={
                "detected_model": model,
                "profile": str(profile or "—"),
                "schema": str(schema or "—"),
            },
        )
