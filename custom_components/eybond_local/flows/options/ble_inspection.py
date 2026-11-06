"""Explicit, query-only collector inspection through HA Bluetooth."""

from __future__ import annotations

import asyncio
from html import escape

import voluptuous as vol
from homeassistant.helpers.selector import (
    BooleanSelector, SelectSelector, SelectSelectorConfig, SelectSelectorMode,
    TextSelector, TextSelectorConfig,
)

from ...runtime.ble_inspection import async_inspect_collector_ble, inspection_candidates
from ..common.translation import with_translation_bundle as _with_translation_bundle


def _choices(values, key):
    return SelectSelector(SelectSelectorConfig(
        options=values, mode=SelectSelectorMode.DROPDOWN, translation_key=key,
    ))


class BleInspectionOptionsMixin:
    """An options dialog owns its bounded BLE task, including abort cleanup."""

    @_with_translation_bundle
    async def async_step_ble_inspection(self, user_input=None):
        if self._ble_inspection_task is not None and not self._ble_inspection_task.done():
            return await self.async_step_ble_inspection_progress()
        errors = {}
        try:
            candidates = inspection_candidates(self.hass)
        except Exception:
            candidates = {}
        if user_input is not None:
            action = user_input.get("ble_inspection_action", "read")
            if action == "done":
                return await self.async_step_diagnostics()
            if action == "read":
                address = user_input.get("ble_address", "")
                ending = user_input.get("ble_line_ending", "none")
                if address not in candidates:
                    errors["base"] = "ble_inspection_device_unavailable"
                elif ending not in {"none", "crlf"}:
                    errors["base"] = "invalid_selection"
                else:
                    self._ble_inspection_result = {}
                    self._ble_inspection_task = self.hass.async_create_task(
                        self._async_run_ble_inspection(
                            address, ending == "crlf", bool(user_input.get("ble_download", False)),
                        )
                    )
                    return await self.async_step_ble_inspection_progress()
            elif action != "refresh":
                errors["base"] = "invalid_selection"
        if not candidates:
            errors.setdefault("base", "ble_inspection_device_unavailable")
        fields = {
            vol.Required("ble_inspection_action", default="read" if candidates else "refresh"):
                _choices(["read", "refresh", "done"], "ble_inspection_action"),
        }
        if candidates:
            fields[vol.Optional("ble_address")] = SelectSelector(SelectSelectorConfig(
                options=[{"value": address, "label": f"{item.preferred_name} — {address}"}
                         for address, item in sorted(candidates.items())],
                mode=SelectSelectorMode.DROPDOWN,
            ))
        fields.update({
            vol.Required("ble_line_ending", default="none"):
                _choices(["none", "crlf"], "ble_line_ending"),
            vol.Optional("ble_download", default=False): BooleanSelector(),
        })
        return self.async_show_form(
            step_id="ble_inspection", data_schema=vol.Schema(fields), errors=errors,
        )

    async def _async_run_ble_inspection(self, address, append_crlf, publish_download):
        try:
            self._ble_inspection_result = await async_inspect_collector_ble(
                self.hass, self._config_entry, address=address, append_crlf=append_crlf,
                publish_download=publish_download,
            )
        except asyncio.CancelledError:
            raise
        except ValueError as exc:
            code = str(exc)
            self._ble_inspection_result = {"error": code if code in {
                "ble_inspection_busy", "ble_inspection_device_unavailable",
            } else "ble_inspection_failed"}
        except Exception:
            self._ble_inspection_result = {"error": "ble_inspection_failed"}

    @_with_translation_bundle
    async def async_step_ble_inspection_progress(self, user_input=None):
        task = self._ble_inspection_task
        if task is None:
            return await self.async_step_ble_inspection()
        if not task.done():
            return self.async_show_progress(
                step_id="ble_inspection_progress", progress_action="ble_inspection",
                progress_task=task,
            )
        return self.async_show_progress_done(next_step_id="ble_inspection_result")

    @_with_translation_bundle
    async def async_step_ble_inspection_result(self, user_input=None):
        if user_input is not None:
            action = user_input.get("ble_inspection_action")
            if action == "read":
                return await self.async_step_ble_inspection()
            if action == "done":
                return await self.async_step_diagnostics()
        result = self._ble_inspection_result
        url = result.get("download_url", "")
        return self.async_show_form(
            step_id="ble_inspection_result",
            data_schema=vol.Schema({
                vol.Required("ble_inspection_action", default="done"):
                    _choices(["read", "done"], "ble_inspection_action"),
                vol.Optional("ble_result", default=result.get("output") or result.get("error", "")):
                    TextSelector(TextSelectorConfig(multiline=True)),
            }),
            description_placeholders={
                "download": f"[JSON]({escape(url, quote=True)})" if url else "",
                "archive_status": str(result.get("cache_status", "not_recorded")),
                "export_status": str(result.get("export_error") or result.get("download_error") or ""),
            },
        )

    def async_remove(self):
        task = self._ble_inspection_task
        if task is not None and not task.done():
            task.cancel()
        super().async_remove()
