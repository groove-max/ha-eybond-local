"""Explicit, read-only compatibility check before a cloud-protocol switch."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from time import monotonic

import voluptuous as vol
from homeassistant.helpers.selector import SelectSelector, SelectSelectorConfig, SelectSelectorMode

from ...const import CONF_DRIVER_HINT
from ...runtime.protocol_check import PROBE_ROUTE_OPTION, protocol_evidence
from ..common.presentation import _clear_runtime_inverter_facts
from ..common.translation import with_translation_bundle
from .shared import _BOOLEAN_SELECTOR


class ShadowProtocolOptionsMixin:
    """Never turn cloud request evidence into an automatic profile change."""

    def _cloud_modbus_check_available(self, *, restore_pending: bool, protection_held: bool) -> bool:
        return (
            not restore_pending and not protection_held
            and self._shadow_learning_state.get("discovery", {}).get("status") == "error"
            and protocol_evidence(self._shadow_learning_state.get("cloud_protocol_evidence")
                                  or self._shadow_learning_state.get("cloud_modbus_read")) is not None
        )

    async def _async_cloud_modbus_check_allowed(self, coordinator) -> bool:
        if coordinator is None:
            return False
        pending = await self._async_control_discovery_restore_pending(coordinator)
        inverter = coordinator.identified_inverter
        return (
            self._cloud_modbus_check_available(
                restore_pending=pending,
                protection_held=self._shadow_learning_state.get("protection_held") is True,
            ) and inverter is not None
        )

    @with_translation_bundle
    async def async_step_shadow_modbus_check(self, user_input=None):
        coordinator = self._coordinator()
        task = getattr(self, "_cloud_modbus_check_task", None)
        if not await self._async_cloud_modbus_check_allowed(coordinator):
            if task is not None:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                return self.async_show_progress_done(next_step_id="shadow_learning_result")
            return await self.async_step_shadow_learning_result()
        if task is None:
            self._cloud_modbus_check_owner = coordinator
            self._cloud_modbus_check_inverter = coordinator.identified_inverter
            self._cloud_modbus_check_task = task = self.hass.async_create_task(
                self._async_run_cloud_modbus_check(coordinator),
            )
        if task.done():
            task.result()
            return self.async_show_progress_done(next_step_id="shadow_modbus_review")
        return self.async_show_progress(
            step_id="shadow_modbus_check", progress_action="cloud_modbus_check",
            progress_task=task,
        )

    async def _async_run_cloud_modbus_check(self, coordinator) -> None:
        try:
            result = await coordinator.async_check_cloud_modbus_profile(
                self._shadow_learning_state.get("cloud_protocol_evidence")
                or self._shadow_learning_state["cloud_modbus_read"],
            )
        except Exception:
            result = {"candidates": [], "attempts": [], "status": "unavailable"}
        self._shadow_learning_state["modbus_profile_check"] = result
        self._cloud_modbus_checked_at = monotonic()
        self._publish_shadow_learning_artifacts(coordinator)

    @with_translation_bundle
    async def async_step_shadow_modbus_review(self, user_input=None):
        coordinator = self._coordinator()
        if (not await self._async_cloud_modbus_check_allowed(coordinator)
                or coordinator is not getattr(self, "_cloud_modbus_check_owner", None)
                or coordinator.identified_inverter is not getattr(self, "_cloud_modbus_check_inverter", None)):
            return await self.async_step_shadow_learning_result()
        result = self._shadow_learning_state.get("modbus_profile_check", {})
        candidates = result.get("candidates", [])
        fresh = monotonic() - getattr(self, "_cloud_modbus_checked_at", 0) < 300
        choices = {item["driver_key"]: item for item in candidates} if fresh else {}
        errors = {}
        # HA can carry the originating menu/result action through an eagerly
        # completed progress step. It is not a submission of this review form.
        if user_input is not None and "driver" in user_input:
            selected = user_input.get("driver", "keep")
            if selected == "keep":
                return await self.async_step_shadow_learning_result()
            if type(selected) is not str or selected not in choices:
                errors["driver"] = "invalid_selection"
            elif user_input.get("confirm_readings") is not True:
                errors["confirm_readings"] = "invalid_selection"
            else:
                # Save intent, not a fabricated detection/identity. The ordinary
                # config-entry update listener reloads and probes the new route.
                data = dict(self._config_entry.data)
                options = dict(self._config_entry.options)
                # A previous driver's schema is not evidence for this choice.
                # The normal probe must rebuild the new binding after reload.
                options.pop("effective_metadata_snapshot", None)
                options[CONF_DRIVER_HINT] = selected
                options[PROBE_ROUTE_OPTION] = dict(choices[selected]["route"])
                _clear_runtime_inverter_facts(data)
                self.hass.config_entries.async_update_entry(self._config_entry, data=data, options=options)
                return self.async_create_entry(data=options)

        options = [{"value": "keep", "label": self._tr(
            "common.dynamic.cloud_modbus_keep", "Keep current profile (no changes)",
        )}]
        details = []
        for key, candidate in choices.items():
            options.append({"value": key, "label": f"{candidate['model_name']} — {key}"})
            details.append(f"{candidate['model_name']} ({key})")
            details.extend(f"- {row['key']}: {coordinator.identified_inverter.driver_key} {row['local']} → {key} {row['modbus']}"
                           for row in candidate["comparisons"])
            if candidate.get("learning_supported") is not True:
                details.append(self._tr("common.dynamic.cloud_protocol_read_only",
                    "This profile supports a read-only compatibility check, but not Write Controls learning."))
        message = self._tr(
            "common.dynamic.cloud_modbus_confirm" if choices else "common.dynamic.cloud_modbus_no_match",
            ("Compare these readings with the inverter before switching. Switching reloads "
             "the integration; available entities may change. Start Write Controls only if "
             "the selected profile supports it, after checking telemetry. This check did not test writes."
             if choices else "No compatible profile with comparable live readings was "
             "confirmed. Your current profile is unchanged. Download a Support Archive; "
             "do not select an arbitrary map."),
        )
        return self.async_show_form(
            step_id="shadow_modbus_review",
            data_schema=vol.Schema({
                vol.Required("driver", default="keep"): SelectSelector(SelectSelectorConfig(
                    options=options, mode=SelectSelectorMode.DROPDOWN,
                )),
                **({vol.Optional("confirm_readings", default=False): _BOOLEAN_SELECTOR} if choices else {}),
            }),
            errors=errors, description_placeholders={"result": message + "\n\n" + "\n".join(details)},
        )
