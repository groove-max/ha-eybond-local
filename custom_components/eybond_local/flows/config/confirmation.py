"""Collector confirmation and optional cloud-evidence lifecycle."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigFlowResult,
)

from .result_model import (
    _result_collector_capabilities,
    _result_is_virtual_bridge,
)
from ...const import (
    CONF_DRIVER_DETECTION_STRATEGY,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_MODE,
    DRIVER_DETECTION_STRATEGIES,
    DRIVER_HINT_AUTO,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from ..common.presentation import (
    _driver_detection_strategy_selector,
    _exception_detail,
    _flatten_sections,
    _poll_interval_selector,
    _poll_mode_selector,
    _smartess_credential_schema_fields,
)
from ..common.translation import (
    with_translation_bundle as _with_translation_bundle,
)
from ...models import (
    OnboardingResult,
)
from ...support.cloud_evidence_providers import (
    CloudEvidenceContext,
    CloudEvidenceOnboardingAssist,
    CloudEvidenceSettingHighlight,
    resolve_cloud_evidence_provider,
)

_SmartEssCloudSettingHighlight = CloudEvidenceSettingHighlight

_SmartEssCloudAssistState = CloudEvidenceOnboardingAssist


class CollectorConfirmationFlowMixin:
    """Collector confirmation and optional cloud-evidence lifecycle."""

    @_with_translation_bundle
    async def async_step_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        # Cloud assist is no longer an interstitial that auto-pops before the
        # confirm form; it is an explicit choice on the detection summary.
        return await self._async_show_confirm_form(
            step_id="confirm", user_input=user_input
        )

    @_with_translation_bundle
    async def async_step_confirm_without_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        return await self._async_show_confirm_form(
            step_id="confirm_without_cloud_assist",
            user_input=user_input,
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        result = self._smartess_cloud_assist_context_result()
        if result is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_auto()

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                self._smartess_cloud_assist = (
                    await self._async_run_smartess_cloud_assist(
                        result,
                        username=str(user_input.get("username") or "").strip(),
                        password=str(user_input.get("password") or ""),
                    )
                )
                self._smartess_cloud_assist_last_error = ""
                self._smartess_cloud_assist_last_error_code = ""
            except Exception as exc:
                self._smartess_cloud_assist_last_error = str(exc)
                self._smartess_cloud_assist_last_error_code = (
                    resolve_cloud_evidence_provider("smartess").classify_error(exc)
                )
                errors["base"] = "smartess_cloud_assist_failed"
            else:
                return await self.async_step_smartess_cloud_assist_summary()

        return self.async_show_form(
            step_id="smartess_cloud_assist",
            data_schema=vol.Schema(_smartess_credential_schema_fields()),
            description_placeholders=self._smartess_cloud_assist_placeholders(result),
            errors=errors or None,
        )

    @_with_translation_bundle
    async def async_step_smartess_cloud_assist_summary(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        result = self._smartess_cloud_assist_context_result()
        if result is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_confirm()

        if self._smartess_cloud_assist_state_for_result(result) is None:
            if self._smartess_cloud_assist_mode == "manual":
                return await self.async_step_manual_confirm()
            return await self.async_step_confirm()

        menu_options = (
            ["manual_confirm"]
            if self._smartess_cloud_assist_mode == "manual"
            else ["confirm"]
        )
        return self.async_show_menu(
            step_id="smartess_cloud_assist_summary",
            menu_options=menu_options,
            description_placeholders=self._smartess_cloud_assist_summary_placeholders(
                result
            ),
        )

    async def _async_show_confirm_form(
        self,
        *,
        step_id: str,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            return await self.async_step_auto()

        if not self._selected_result_is_passive_callback():
            await self._async_refresh_selected_result_collector_capabilities()

        # First add is intentionally not the place to choose the collector
        # connection strategy.
        # Discovery evidence can be partial at this point, especially for local
        # bridges and collector-only candidates. Keep the confirm form stable:
        # only collect the poll interval here. Runtime/options flow owns the
        # runtime UX once the entry has had a chance to read endpoint and
        # capability metadata. A detected virtual bridge is still forced to the
        # inbound strategy because it connects to Home Assistant on its own.
        selected_capabilities = _result_collector_capabilities(self._selected_result)
        is_bridge = selected_capabilities.virtual_bridge

        errors: dict[str, str] = {}
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            poll_mode = str(
                flat_input.get(CONF_POLL_MODE, DEFAULT_POLL_MODE) or DEFAULT_POLL_MODE
            )
            if poll_mode not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
                errors[CONF_POLL_MODE] = "invalid_selection"
            elif poll_mode == POLL_MODE_MANUAL and CONF_POLL_INTERVAL not in flat_input:
                self._confirm_poll_interval_pending_input = dict(flat_input)
                self._confirm_poll_interval_pending_step_id = step_id
                return await self.async_step_confirm_poll_interval()
            detection_strategy = flat_input.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                DEFAULT_DRIVER_DETECTION_STRATEGY,
            )
            if (
                type(detection_strategy) is not str
                or detection_strategy not in DRIVER_DETECTION_STRATEGIES
            ):
                errors[CONF_DRIVER_DETECTION_STRATEGY] = "invalid_selection"
            requires_ha_endpoint = (
                self._selected_result_is_passive_callback() or is_bridge
            )
            if errors:
                pass
            elif self._selected_result_is_passive_callback():
                self._collector_endpoint_bind_applied = True
                return await self._async_create_entry_from_result(flat_input)
            if (
                not errors
                and requires_ha_endpoint
                and not self._collector_endpoint_bind_applied
            ):
                self._reset_collector_endpoint_binding_state()
                try:
                    # For a bridge, writing the HA server endpoint is still how
                    # the bridge is told where to connect — keep the bind.
                    # Modern bridge firmware accepts and persists the FC=3
                    # param-21 endpoint write. Older bridge firmware may refuse
                    # it; keep that refusal non-fatal for bridge upgrades.
                    await self._async_bind_selected_collector_to_home_assistant(
                        allow_refused_endpoint_write=is_bridge,
                    )
                except Exception as exc:
                    self._collector_endpoint_error = _exception_detail(exc)
                    errors["base"] = "collector_endpoint_write_failed"
                else:
                    self._collector_endpoint_bind_applied = True
                    return await self._async_create_entry_from_result(flat_input)
            elif not errors:
                return await self._async_create_entry_from_result(flat_input)

        description_placeholders = dict(self._collector_connection_placeholders())
        if is_bridge:
            description_placeholders["collector_connection_note"] = self._tr(
                "common.dynamic.collector_connection_bridge_note",
                "Local bridge — it connects to Home Assistant on its own.",
            )
        else:
            description_placeholders.setdefault("collector_connection_note", "")
        schema: dict[Any, Any] = {
            vol.Required(
                CONF_DRIVER_DETECTION_STRATEGY,
                default=DEFAULT_DRIVER_DETECTION_STRATEGY,
            ): _driver_detection_strategy_selector(self._translation_bundle),
            vol.Required(
                CONF_POLL_MODE, default=DEFAULT_POLL_MODE
            ): _poll_mode_selector(
                self._translation_bundle,
            ),
        }
        return self.async_show_form(
            step_id=step_id,
            data_schema=vol.Schema(schema),
            errors=errors,
            description_placeholders=description_placeholders,
        )

    @_with_translation_bundle
    async def async_step_confirm_poll_interval(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            return await self.async_step_auto()
        pending = dict(self._confirm_poll_interval_pending_input)
        step_id = self._confirm_poll_interval_pending_step_id or "confirm"
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            pending[CONF_POLL_INTERVAL] = flat_input.get(
                CONF_POLL_INTERVAL,
                DEFAULT_POLL_INTERVAL,
            )
            return await self._async_show_confirm_form(
                step_id=step_id,
                user_input=pending,
            )
        return self.async_show_form(
            step_id="confirm_poll_interval",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_POLL_INTERVAL,
                        default=pending.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL),
                    ): _poll_interval_selector(
                        self._selected_poll_policy_driver_key(),
                        inverter=self._selected_poll_policy_match(),
                    ),
                }
            ),
            errors={},
        )

    def _smartess_cloud_assist_context_result(self) -> OnboardingResult | None:
        if self._smartess_cloud_assist_mode == "manual":
            return self._manual_result
        return self._selected_result

    def _smartess_cloud_assist_state_for_result(
        self,
        result: OnboardingResult | None,
    ) -> _SmartEssCloudAssistState | None:
        collector_pn = self._collector_pn_for_result(result)
        if not collector_pn or self._smartess_cloud_assist is None:
            return None
        if self._smartess_cloud_assist.collector_pn != collector_pn:
            return None
        return self._smartess_cloud_assist

    def _can_offer_smartess_cloud_assist(self, result: OnboardingResult | None) -> bool:
        return False

    def _smartess_cloud_summary(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        placeholders = {
            "family_label": state.inferred_family_label,
            "driver_key": state.inferred_driver_key or DRIVER_HINT_AUTO,
            "exact_count": state.exact_field_count,
            "probable_count": state.probable_field_count,
            "cloud_only_count": state.cloud_only_field_count,
        }
        if state.inferred_family_label:
            return self._tr(
                "common.dynamic.smartess_cloud_summary_known_family",
                "**SmartESS cloud:** suggests **{family_label}** and pre-fills local metadata hints for `{driver_key}`. Settings surface: exact {exact_count}, probable {probable_count}, cloud-only {cloud_only_count}. Local controls stay disabled until a high-confidence local detection is confirmed.",
                placeholders,
            )
        return self._tr(
            "common.dynamic.smartess_cloud_summary_generic",
            "**SmartESS cloud:** evidence was saved for this collector, but no safe local family mapping was resolved yet. Settings surface: exact {exact_count}, probable {probable_count}, cloud-only {cloud_only_count}.",
            placeholders,
        )

    def _smartess_cloud_offer_summary(self, result: OnboardingResult | None) -> str:
        collector_pn = self._collector_pn_for_result(result)
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is not None:
            return self._smartess_cloud_summary(result)
        return self._tr(
            "common.dynamic.smartess_cloud_offer_summary",
            "Local detection is not yet high-confidence for collector `{collector_pn}`. SmartESS cloud assist can fetch extra identity and settings evidence before the entry is created.",
            {
                "collector_pn": collector_pn
                or self._tr("common.dynamic.not_available", "Not available")
            },
        )

    def _smartess_cloud_identity_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        not_available = self._tr("common.dynamic.not_available", "Not available")
        lines = [
            self._tr(
                "common.dynamic.smartess_cloud_identity_heading", "**Cloud identity**"
            ),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_collector_pn_label', 'Collector PN')} | {self._collector_pn_for_result(result) or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_pn_label', 'Device PN')} | {state.device_pn or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_sn_label', 'Device SN')} | {state.device_sn or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_device_name_label', 'Device')} | {state.device_name or not_available} |",
        ]
        if state.device_alias:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_alias_label', 'Alias')} | {state.device_alias} |"
            )
        if state.device_status:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_status_label', 'Status')} | {state.device_status} |"
            )
        if state.device_brand:
            lines.append(
                f"| {self._tr('common.dynamic.smartess_cloud_device_brand_label', 'Brand')} | {state.device_brand} |"
            )
        address_value = (
            self._smartess_cloud_device_address_preview(state) or not_available
        )
        lines.append(
            f"| {self._tr('common.dynamic.smartess_cloud_device_address_label', 'Cloud address')} | {address_value} |"
        )
        return "\n".join(lines)

    def _smartess_cloud_mapping_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""

        not_available = self._tr("common.dynamic.not_available", "Not available")
        reason = state.inferred_reason or self._tr(
            "common.dynamic.smartess_cloud_mapping_reason_missing",
            "No safe local family mapping was resolved yet. The evidence is still saved for later diagnostics and support work.",
        )
        lines = [
            self._tr(
                "common.dynamic.smartess_cloud_mapping_heading",
                "**Local interpretation**",
            ),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_family_label', 'Suggested family')} | {state.inferred_family_label or not_available} |",
            f"| {self._tr('common.dynamic.smartess_cloud_driver_label', 'Local driver hint')} | {state.inferred_driver_key or DRIVER_HINT_AUTO} |",
            f"| {self._tr('common.dynamic.smartess_cloud_mapping_reason_label', 'Reason')} | {reason} |",
        ]
        return "\n".join(lines)

    def _smartess_cloud_detail_summary(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        if state.detail_sections:
            return self._tr(
                "common.dynamic.smartess_cloud_detail_sections_found",
                "**Cloud detail sections:** {sections}",
                {"sections": ", ".join(state.detail_sections)},
            )
        return self._tr(
            "common.dynamic.smartess_cloud_detail_sections_missing",
            "**Cloud detail sections:** no normalized section breakdown was captured.",
        )

    def _smartess_cloud_settings_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        lines = [
            self._tr(
                "common.dynamic.smartess_cloud_settings_heading", "**Settings digest**"
            ),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_table_label', 'Detail')} | {self._tr('common.dynamic.smartess_cloud_table_value', 'Value')} |",
            "|---|---|",
            f"| {self._tr('common.dynamic.smartess_cloud_total_fields_label', 'Total fields')} | {state.total_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_mapped_fields_label', 'Mapped local fields')} | {state.mapped_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_current_values_label', 'Fields with current value')} | {state.fields_with_current_value} |",
            f"| {self._tr('common.dynamic.smartess_cloud_exact_fields_label', 'Exact local matches')} | {state.exact_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_probable_fields_label', 'Probable local matches')} | {state.probable_field_count} |",
            f"| {self._tr('common.dynamic.smartess_cloud_cloud_only_fields_label', 'Cloud-only fields')} | {state.cloud_only_field_count} |",
        ]
        return "\n".join(lines)

    def _smartess_cloud_highlights_table(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is None:
            return ""
        if not state.highlight_settings:
            return self._tr(
                "common.dynamic.smartess_cloud_highlights_empty",
                "**Highlighted SmartESS fields:** no compact field preview was captured.",
            )

        def _escape_cell(value: str) -> str:
            return str(value).replace("|", "\\|").replace("\n", " ")

        lines = [
            self._tr(
                "common.dynamic.smartess_cloud_highlights_heading",
                "**Highlighted SmartESS fields**",
            ),
            "",
            f"| {self._tr('common.dynamic.smartess_cloud_highlight_field_label', 'Field')} | {self._tr('common.dynamic.smartess_cloud_highlight_value_label', 'Value')} | {self._tr('common.dynamic.smartess_cloud_highlight_local_use_label', 'Local use')} |",
            "|---|---|---|",
        ]
        not_available = self._tr("common.dynamic.not_available", "Not available")
        for highlight in state.highlight_settings:
            lines.append(
                f"| {_escape_cell(highlight.title)} | {_escape_cell(highlight.current_value or not_available)} | {_escape_cell(self._smartess_cloud_local_use_preview(highlight))} |"
            )
        return "\n".join(lines)

    def _smartess_cloud_device_address_preview(
        self,
        state: _SmartEssCloudAssistState,
    ) -> str:
        if state.device_devcode in (None, "") and state.device_devaddr in (None, ""):
            return ""

        devcode = ""
        if isinstance(state.device_devcode, int):
            devcode = self._tr(
                "common.dynamic.smartess_cloud_device_devcode_value",
                "devcode {devcode} (0x{devcode_hex})",
                {
                    "devcode": state.device_devcode,
                    "devcode_hex": f"{state.device_devcode:04X}",
                },
            )
        devaddr = ""
        if isinstance(state.device_devaddr, int):
            devaddr = self._tr(
                "common.dynamic.smartess_cloud_device_devaddr_value",
                "devaddr {devaddr}",
                {"devaddr": state.device_devaddr},
            )
        return ", ".join(part for part in (devcode, devaddr) if part)

    def _smartess_cloud_bucket_label(self, bucket: str) -> str:
        if bucket == "exact_0925":
            return self._tr(
                "common.dynamic.smartess_cloud_bucket_exact", "Exact local match"
            )
        if bucket == "probable_0925":
            return self._tr(
                "common.dynamic.smartess_cloud_bucket_probable", "Probable local match"
            )
        if bucket == "cloud_only":
            return self._tr(
                "common.dynamic.smartess_cloud_bucket_cloud_only", "Cloud-only"
            )
        return self._tr("common.dynamic.unknown", "Unknown")

    def _smartess_cloud_local_use_preview(
        self,
        highlight: _SmartEssCloudSettingHighlight,
    ) -> str:
        bucket_label = self._smartess_cloud_bucket_label(highlight.bucket)
        if highlight.register is None:
            return bucket_label
        return self._tr(
            "common.dynamic.smartess_cloud_local_use_register",
            "{bucket_label}, reg {register}",
            {"bucket_label": bucket_label, "register": highlight.register},
        )

    def _smartess_cloud_status_line(self, result: OnboardingResult | None) -> str:
        state = self._smartess_cloud_assist_state_for_result(result)
        if state is not None and state.evidence_path:
            return self._tr(
                "common.dynamic.smartess_cloud_status_saved",
                "Last SmartESS cloud evidence: {path}",
                {"path": state.evidence_path},
            )
        if self._smartess_cloud_assist_last_error:
            error_code = (
                getattr(self, "_smartess_cloud_assist_last_error_code", "")
                or "unexpected"
            )
            translation_key = (
                f"common.dynamic.smartess_cloud_status_failed_{error_code}"
            )
            fallback = "Last SmartESS cloud assist attempt failed: {error}"
            return self._tr(
                translation_key,
                fallback,
                {"error": self._smartess_cloud_assist_last_error},
            )
        return ""

    def _smartess_cloud_assist_placeholders(
        self,
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        state = self._smartess_cloud_assist_state_for_result(result)
        return {
            "collector_pn": self._collector_pn_for_result(result)
            or self._tr("common.dynamic.not_available", "Not available"),
            "cloud_evidence_path": (
                state.evidence_path
                if state is not None and state.evidence_path
                else self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "smartess_cloud_offer_summary": self._smartess_cloud_offer_summary(result),
            "smartess_cloud_status_line": self._smartess_cloud_status_line(result),
        }

    def _smartess_cloud_assist_summary_placeholders(
        self,
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        placeholders = self._smartess_cloud_assist_placeholders(result)
        placeholders.update(
            {
                "smartess_cloud_identity_table": self._smartess_cloud_identity_table(
                    result
                ),
                "smartess_cloud_mapping_table": self._smartess_cloud_mapping_table(
                    result
                ),
                "smartess_cloud_detail_summary": self._smartess_cloud_detail_summary(
                    result
                ),
                "smartess_cloud_settings_table": self._smartess_cloud_settings_table(
                    result
                ),
                "smartess_cloud_highlights_table": self._smartess_cloud_highlights_table(
                    result
                ),
            }
        )
        return placeholders

    def _config_dir_path(self) -> Path:
        config_dir = str(
            getattr(getattr(self.hass, "config", None), "config_dir", "") or ""
        ).strip()
        if not config_dir:
            raise RuntimeError("config_dir_not_available")
        return Path(config_dir)

    async def _async_run_smartess_cloud_assist(
        self,
        result: OnboardingResult,
        *,
        username: str,
        password: str,
    ) -> _SmartEssCloudAssistState:
        if _result_is_virtual_bridge(result):
            raise RuntimeError("smartess_cloud_unavailable_for_virtual_bridge")

        collector_pn = self._collector_pn_for_result(result)
        if not collector_pn:
            raise RuntimeError("smartess_collector_pn_not_available")

        # Gather the explicit hints (data), then let the SmartESS provider fetch,
        # interpret, and normalize. The flow imports no provider client / draft
        # resolver and parses no raw provider payload.
        asset_id, profile_key = self._smartess_detected_hint_values(result)
        context = CloudEvidenceContext(
            config_dir=self._config_dir_path(),
            entry_id="",
            collector_pn=collector_pn,
            protocol_asset_id=asset_id,
            protocol_profile_key=profile_key,
        )
        provider = resolve_cloud_evidence_provider("smartess")
        return await self.hass.async_add_executor_job(
            lambda: provider.build_onboarding_assist(
                context, username=username, password=password
            )
        )
