"""Extracted EyeBond options-flow lifecycle: DiagnosticsOptionsMixin."""

from __future__ import annotations

import logging
from html import escape as html_escape
from pathlib import Path
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from .const import (
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
)
from .flow_presentation import _smartess_credential_schema_fields
from .flow_translation import with_translation_bundle as _with_translation_bundle
from .metadata.local_metadata import (
    local_profile_override_details,
    local_register_schema_override_details,
    resolve_local_metadata_rollback_paths,
)
from .options_shared import (
    _BOOLEAN_SELECTOR,
    _MULTILINE_LOG_TEXT_SELECTOR,
    _coerce_proxy_capture_duration_minutes,
)

logger = logging.getLogger(__name__)


CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE = "smartess_cloud_mode"


SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED = "use_saved"


SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH = "refresh"


SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY = "archive_only"


_LOCAL_METADATA_STATUS_TRANSLATION_KEYS = {
    "Starting collector proxy capture": "starting_proxy_capture",
    "Collector proxy capture failed to start": "proxy_capture_failed_to_start",
    "Collector proxy capture running": "proxy_capture_running",
    "Stopping collector proxy capture": "stopping_proxy_capture",
    "Collector proxy capture stopped": "proxy_capture_stopped",
    "Recovered interrupted collector proxy capture": "recovered_interrupted_proxy_capture",
    "SmartESS cloud evidence exported": "smartess_cloud_evidence_exported",
    "Cloud evidence exported": "cloud_evidence_exported",
    "Cloud evidence refresh failed; using last saved evidence": "cloud_evidence_refresh_failed_using_saved",
    "Support bundle exported": "support_bundle_exported",
    "Support archive exported": "support_archive_exported",
    "Local profile draft created": "local_profile_draft_created",
    "Local register schema draft created": "local_register_schema_draft_created",
    "Reloading local metadata": "reloading_local_metadata",
    "Rolling back local metadata": "rolling_back_local_metadata",
    "SmartESS local draft created": "smartess_local_draft_created",
    "SmartESS SMG bridge created": "smartess_smg_bridge_created",
}


_MULTILINE_TEXT_SELECTOR = TextSelector(TextSelectorConfig(multiline=True))


class DiagnosticsOptionsMixin:
    """DiagnosticsOptions lifecycle."""

    @_with_translation_bundle
    async def async_step_diagnostics(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        placeholders = self._diagnostics_placeholders()
        primary_action = placeholders["support_workflow_primary_action"]
        menu_options = self._diagnostics_menu_options(primary_action)

        return self.async_show_menu(
            step_id="diagnostics",
            menu_options=menu_options,
            description_placeholders=placeholders,
        )

    @_with_translation_bundle
    async def async_step_diagnostic_commands(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Run a one-off multiline diagnostic scenario from the options UI."""

        errors: dict[str, str] = {}
        submitted = dict(user_input or {})
        commands = str(
            submitted.get("diagnostic_commands", self._diagnostic_commands_text) or ""
        )
        stop_on_error = bool(submitted.get("diagnostic_stop_on_error", True))
        confirm_write = bool(submitted.get("diagnostic_confirm_write", False))
        publish_download_copy = bool(
            submitted.get(
                "diagnostic_publish_download_copy",
                self._diagnostic_publish_download_copy,
            )
        )

        if user_input is not None:
            self._diagnostic_commands_text = commands
            self._diagnostic_publish_download_copy = publish_download_copy
            if not commands.strip():
                errors["diagnostic_commands"] = "diagnostic_commands_required"
            else:
                coordinator = self._coordinator()
                if coordinator is None or not callable(
                    getattr(coordinator, "async_run_diagnostic_commands", None)
                ):
                    errors["base"] = "diagnostic_commands_unavailable"
                else:
                    try:
                        result = await coordinator.async_run_diagnostic_commands(
                            commands=commands,
                            stop_on_error=stop_on_error,
                            confirm_write=confirm_write,
                            publish_download_copy=publish_download_copy,
                        )
                    except Exception:
                        logger.exception("Diagnostic command scenario failed")
                        errors["base"] = "diagnostic_commands_failed"
                    else:
                        self._diagnostic_commands_output = str(
                            result.get("output") or ""
                        )
                        self._diagnostic_commands_download_url = str(
                            result.get("download_url") or ""
                        )
                        self._diagnostic_commands_result_path = str(
                            result.get("result_path") or ""
                        )

        schema: dict[Any, Any] = {
            vol.Required(
                "diagnostic_commands",
                default=commands,
            ): _MULTILINE_TEXT_SELECTOR,
            vol.Required(
                "diagnostic_stop_on_error",
                default=stop_on_error,
            ): _BOOLEAN_SELECTOR,
            vol.Required(
                "diagnostic_confirm_write",
                default=confirm_write,
            ): _BOOLEAN_SELECTOR,
            vol.Required(
                "diagnostic_publish_download_copy",
                default=publish_download_copy,
            ): _BOOLEAN_SELECTOR,
        }
        if self._diagnostic_commands_output:
            schema[
                vol.Optional(
                    "diagnostic_result",
                    default=self._diagnostic_commands_output,
                )
            ] = _MULTILINE_LOG_TEXT_SELECTOR

        download_markdown = (
            self._tr(
                "common.dynamic.download_file",
                "[Download file]({url})",
                {"url": self._diagnostic_commands_download_url},
            )
            if self._diagnostic_commands_download_url
            else self._tr("common.dynamic.not_available", "Not available")
        )
        return self.async_show_form(
            step_id="diagnostic_commands",
            data_schema=vol.Schema(schema),
            errors=errors,
            description_placeholders={
                "diagnostic_result_path": self._diagnostic_commands_result_path
                or self._tr("common.dynamic.not_created_yet", "Not created yet"),
                "diagnostic_download_markdown": download_markdown,
            },
        )

    @_with_translation_bundle
    async def async_step_create_support_package(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_archive_title",
                    "Support Archive",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded and the inverter has been detected, then try again.",
                ),
            )

        capabilities = self._collector_capabilities()
        is_bridge = capabilities.virtual_bridge
        can_refresh_cloud_evidence = (
            self._cloud_evidence_export_available(coordinator)
            and capabilities.cloud_evidence
        )
        saved_cloud_evidence_path = self._current_cloud_evidence_path(coordinator)
        had_saved_cloud_evidence = (
            bool(saved_cloud_evidence_path) and capabilities.cloud_evidence
        )

        if user_input is None and can_refresh_cloud_evidence:
            return self._show_create_support_package_form(
                coordinator=coordinator,
                saved_cloud_evidence_path=saved_cloud_evidence_path,
            )

        archive_cloud_mode = self._default_support_archive_cloud_mode(
            had_saved_cloud_evidence=had_saved_cloud_evidence,
        )
        smartess_username = ""
        smartess_password = ""
        wants_inline_refresh = False

        if can_refresh_cloud_evidence:
            form_input = user_input or {}
            archive_cloud_mode = str(
                form_input.get(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE)
                or self._default_support_archive_cloud_mode(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                )
            )
            smartess_username = str(form_input.get("username") or "").strip()
            smartess_password = str(form_input.get("password") or "").strip()
            wants_inline_refresh = (
                archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
            )
            errors: dict[str, str] = {}
            if wants_inline_refresh:
                if not smartess_username:
                    errors["username"] = "required"
                if not smartess_password:
                    errors["password"] = "required"
            if errors:
                return self._show_create_support_package_form(
                    coordinator=coordinator,
                    saved_cloud_evidence_path=saved_cloud_evidence_path,
                    user_input=form_input,
                    errors=errors,
                )

        try:
            path = await coordinator.async_export_support_package_with_cloud_refresh(
                smartess_username=smartess_username,
                smartess_password=smartess_password,
                wants_refresh=wants_inline_refresh,
            )
        except Exception as exc:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "support_archive_title",
                    "Support Archive",
                ),
                status=self._diagnostics_result_tr(
                    "support_archive_failed_status",
                    "Support archive export failed: {error}",
                    {"error": str(exc)},
                ),
                next_step=self._diagnostics_result_tr(
                    (
                        "support_archive_failed_next_refresh"
                        if archive_cloud_mode
                        == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
                        else "support_archive_failed_next"
                    ),
                    (
                        "Check the cloud account credentials, or rerun Create support archive and choose a different cloud evidence mode."
                        if archive_cloud_mode
                        == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH
                        else "Check whether the entry is loaded and the Home Assistant config directory is writable, then try again."
                    ),
                ),
            )

        download_url = str(
            coordinator.data.values.get("support_package_download_relative_url")
            or coordinator.data.values.get("support_package_download_url")
            or ""
        )
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "support_archive_created_title",
                "Support Archive Created",
            ),
            status=self._diagnostics_result_tr(
                "support_archive_created_status",
                "A combined support archive with runtime data, raw capture evidence, an anonymized replay fixture, and matching cloud evidence when available was written to the Home Assistant config directory.\n\n{support_archive_cloud_detail}",
                {
                    "support_archive_cloud_detail": self._support_archive_cloud_result_detail(
                        archive_cloud_mode=archive_cloud_mode,
                        had_saved_cloud_evidence=had_saved_cloud_evidence,
                    )
                },
            ),
            path=path,
            download_url=download_url,
            next_step=self._diagnostics_result_tr(
                "support_archive_created_next",
                "Send this single ZIP file to the developer. Create local experimental drafts only after the archive has been reviewed.",
            ),
        )

    @_with_translation_bundle
    async def async_step_reload_local_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "reload_local_metadata_title",
                    "Reload Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "wait_for_entry_loaded",
                    "Wait for the entry to finish loading, then try again.",
                ),
            )

        await coordinator.async_reload_local_metadata()
        return await self._async_show_diagnostics_result(
            action_title=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_title",
                "Local Metadata Reload Triggered",
            ),
            status=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_status",
                "Local metadata caches were cleared and the entry reload was requested.",
            ),
            next_step=self._diagnostics_result_tr(
                "reload_local_metadata_triggered_next",
                "Refresh the device page after the entry reconnects to confirm whether local overrides were applied.",
            ),
        )

    @_with_translation_bundle
    async def async_step_rollback_local_metadata(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        rollback_paths = self._local_metadata_rollback_paths()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_title",
                    "Rollback Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "wait_for_entry_loaded",
                    "Wait for the entry to finish loading, then try again.",
                ),
            )

        if not rollback_paths.paths:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_title",
                    "Rollback Local Metadata",
                ),
                status=self._diagnostics_result_tr(
                    "rollback_local_metadata_unavailable_status",
                    "No active managed local metadata override is available to roll back for this entry.",
                ),
                next_step=self._diagnostics_result_tr(
                    "rollback_local_metadata_unavailable_next",
                    "Create or activate a local override first, or use Reload local metadata if the files were already removed manually.",
                ),
            )

        if user_input is not None:
            try:
                removed_paths = await coordinator.async_rollback_local_metadata()
            except Exception as exc:
                return await self._async_show_diagnostics_result(
                    action_title=self._diagnostics_result_tr(
                        "rollback_local_metadata_title",
                        "Rollback Local Metadata",
                    ),
                    status=self._diagnostics_result_tr(
                        "rollback_local_metadata_failed_status",
                        "Local metadata rollback failed: {error}",
                        {"error": str(exc)},
                    ),
                    next_step=self._diagnostics_result_tr(
                        "rollback_local_metadata_failed_next",
                        "Check whether the active override files still exist under /config/eybond_local/, then try again.",
                    ),
                )

            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_title",
                    "Local Metadata Rolled Back",
                ),
                status=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_status",
                    "The active managed local override files were removed and the entry reload was requested.",
                ),
                path=" ; ".join(removed_paths),
                next_step=self._diagnostics_result_tr(
                    "rollback_local_metadata_done_next",
                    "Refresh the device page after the entry reconnects to confirm that the built-in metadata is active again.",
                ),
            )

        not_available = self._tr("common.dynamic.not_available", "Not available")
        return self.async_show_form(
            step_id="rollback_local_metadata",
            data_schema=vol.Schema({}),
            description_placeholders={
                "rollback_target_count": str(len(rollback_paths.paths)),
                "rollback_profile_path": str(
                    rollback_paths.profile_path or not_available
                ),
                "rollback_schema_path": str(
                    rollback_paths.schema_path or not_available
                ),
            },
        )

    @_with_translation_bundle
    async def async_step_diagnostics_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if user_input is not None:
            return await self.async_step_diagnostics()

        return self.async_show_form(
            step_id="diagnostics_result",
            data_schema=vol.Schema({}),
            description_placeholders=self._diagnostics_result,
        )

    def _metadata_source_summary(self, metadata) -> str:
        if metadata is None:
            return self._tr("common.dynamic.not_available", "Not available")
        source_path = getattr(metadata, "source_path", "") or self._tr(
            "common.dynamic.unknown_path", "Unknown path"
        )
        source_scope = getattr(metadata, "source_scope", "") or "unknown"
        if source_scope == "builtin":
            return self._tr(
                "common.dynamic.built_in_metadata",
                "Built-in metadata ({path})",
                {"path": source_path},
            )
        if source_scope == "external":
            return self._tr(
                "common.dynamic.local_override",
                "Local override ({path})",
                {"path": source_path},
            )
        return self._tr(
            "common.dynamic.external_metadata",
            "External metadata ({path})",
            {"path": source_path},
        )

    def _diagnostics_menu_options(self, primary_action: str) -> list[str]:
        rollback_paths = self._local_metadata_rollback_paths()
        menu_options: list[str] = [
            "create_support_package",
        ]

        # The free-form diagnostic command runner issues raw reads/writes/AT
        # commands directly on the device; expose its UI form only in Home
        # Assistant Advanced Mode. The run_diagnostic_commands action stays
        # available and is itself write-gated by confirm_write.
        if getattr(self, "show_advanced_options", False):
            menu_options.append("diagnostic_commands")

        if primary_action == "reload_local_metadata":
            menu_options.append("reload_local_metadata")

        if rollback_paths.paths and "rollback_local_metadata" not in menu_options:
            menu_options.append("rollback_local_metadata")

        return menu_options

    def _cloud_evidence_export_available(self, coordinator) -> bool:
        """Return whether this entry can fetch provider-specific cloud evidence."""

        return bool(getattr(coordinator, "cloud_evidence_export_available", False))

    def _current_cloud_evidence_path(self, coordinator=None) -> str:
        """Return the latest cloud evidence path visible to diagnostics."""

        coordinator = coordinator or self._coordinator()
        if coordinator is None:
            return ""

        live_path = str(
            getattr(coordinator, "smartess_cloud_evidence_path", "") or ""
        ).strip()
        if live_path:
            return live_path

        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        return str(values.get("cloud_evidence_path") or "").strip()

    def _default_support_archive_cloud_mode(
        self, *, had_saved_cloud_evidence: bool
    ) -> str:
        if had_saved_cloud_evidence:
            return SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED
        return SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY

    def _support_archive_cloud_mode_label(self, archive_cloud_mode: str) -> str:
        return {
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED: self._tr(
                "common.dynamic.support_archive_cloud_mode_use_saved",
                "Use saved cloud evidence",
            ),
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH: self._tr(
                "common.dynamic.support_archive_cloud_mode_refresh",
                "Fetch or refresh cloud evidence now",
            ),
            SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY: self._tr(
                "common.dynamic.support_archive_cloud_mode_archive_only",
                "Create the archive without cloud evidence",
            ),
        }.get(archive_cloud_mode, archive_cloud_mode)

    def _support_archive_cloud_mode_selector(
        self,
        *,
        had_saved_cloud_evidence: bool,
        can_refresh_cloud_evidence: bool = True,
    ) -> SelectSelector:
        options: list[SelectOptionDict] = []
        if had_saved_cloud_evidence:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_USE_SAVED,
                    ),
                )
            )
        else:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_ARCHIVE_ONLY,
                    ),
                )
            )
        if can_refresh_cloud_evidence:
            options.append(
                SelectOptionDict(
                    value=SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                    label=self._support_archive_cloud_mode_label(
                        SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                    ),
                )
            )
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    def _support_archive_cloud_plan_summary(
        self,
        *,
        had_saved_cloud_evidence: bool,
        can_refresh_cloud_evidence: bool,
    ) -> str:
        if had_saved_cloud_evidence and can_refresh_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_saved_refreshable",
                "Saved cloud evidence will be included automatically, or you can refresh it in this same step before the archive is built.",
            )
        if had_saved_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_saved_only",
                "Saved cloud evidence will be included automatically when it matches this entry.",
            )
        if can_refresh_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_plan_refreshable",
                "No cloud evidence is saved yet. You can fetch it in this step and include it in the same archive, or continue without it.",
            )
        return self._tr(
            "common.dynamic.support_archive_cloud_plan_unavailable",
            "No cloud evidence is currently available for this entry.",
        )

    def _support_archive_cloud_result_detail(
        self,
        *,
        archive_cloud_mode: str,
        had_saved_cloud_evidence: bool,
    ) -> str:
        if archive_cloud_mode == SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH:
            return self._tr(
                "common.dynamic.support_archive_cloud_result_refreshed",
                "Fresh cloud evidence was fetched in this step and included in the archive.",
            )
        if had_saved_cloud_evidence:
            return self._tr(
                "common.dynamic.support_archive_cloud_result_saved",
                "Saved cloud evidence was included in the archive.",
            )
        return self._tr(
            "common.dynamic.support_archive_cloud_result_none",
            "No cloud evidence was included in the archive.",
        )

    def _show_create_support_package_form(
        self,
        *,
        coordinator,
        saved_cloud_evidence_path: str,
        user_input: dict[str, Any] | None = None,
        errors: dict[str, str] | None = None,
    ) -> ConfigFlowResult:
        capabilities = self._collector_capabilities()
        had_saved_cloud_evidence = (
            bool(saved_cloud_evidence_path) and capabilities.cloud_evidence
        )
        can_refresh_cloud_evidence = (
            self._cloud_evidence_export_available(coordinator)
            and capabilities.cloud_evidence
        )
        if not capabilities.cloud_evidence:
            saved_cloud_evidence_path = ""
            had_saved_cloud_evidence = False
        defaults = {
            CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE: str(
                (user_input or {}).get(CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE)
                or self._default_support_archive_cloud_mode(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                )
            ),
            "username": str((user_input or {}).get("username") or ""),
            "password": str((user_input or {}).get("password") or ""),
        }
        not_available = self._tr("common.dynamic.not_available", "Not available")
        not_created_yet = self._tr("common.dynamic.not_created_yet", "Not created yet")
        return self.async_show_form(
            step_id="create_support_package",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE,
                        default=defaults[CONF_SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE],
                    ): self._support_archive_cloud_mode_selector(
                        had_saved_cloud_evidence=had_saved_cloud_evidence,
                        can_refresh_cloud_evidence=can_refresh_cloud_evidence,
                    ),
                    **_smartess_credential_schema_fields(
                        required=False,
                        username_default=defaults["username"],
                        password_default=defaults["password"],
                    ),
                }
            ),
            errors=errors or {},
            description_placeholders={
                "collector_pn": str(
                    getattr(coordinator, "smartess_collector_pn", "") or not_available
                ),
                "cloud_evidence_path": saved_cloud_evidence_path or not_created_yet,
                "smartess_archive_plan_summary": self._support_archive_cloud_plan_summary(
                    had_saved_cloud_evidence=had_saved_cloud_evidence,
                    can_refresh_cloud_evidence=can_refresh_cloud_evidence,
                ),
                "refresh_mode_label": self._support_archive_cloud_mode_label(
                    SUPPORT_ARCHIVE_SMARTESS_CLOUD_MODE_REFRESH,
                ),
            },
        )

    def _smartess_cloud_diagnostics_hint(self) -> str:
        coordinator = self._coordinator()
        if (
            coordinator is None
            or self._collector_is_virtual_bridge()
            or not bool(getattr(coordinator, "cloud_evidence_export_available", False))
        ):
            return ""

        values = getattr(getattr(coordinator, "data", None), "values", {}) or {}
        cloud_evidence_path = str(values.get("cloud_evidence_path") or "").strip()

        if getattr(coordinator, "smartess_smg_bridge_plan", None) is not None:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_bridge",
                "Current SmartESS cloud evidence is ready to generate a SmartESS SMG bridge for this runtime.",
            )
        elif getattr(coordinator, "smartess_known_family_draft_plan", None) is not None:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_draft",
                "Current SmartESS cloud evidence is ready to generate a SmartESS draft for this runtime.",
            )
        elif cloud_evidence_path:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_refresh",
                "SmartESS cloud evidence is already saved for this entry and can be refreshed after app-side changes.",
            )
        else:
            detail = self._tr(
                "common.dynamic.smartess_cloud_diagnostics_detail_available",
                "SmartESS cloud evidence is available for this entry even if local detection is already high-confidence.",
            )

        return self._tr(
            "common.dynamic.smartess_cloud_diagnostics_hint",
            "**SmartESS cloud:** {detail} It can still refine local metadata or re-enable bridge-backed entities for an existing device. The visible entity count may stay the same when existing entities are upgraded instead of creating new IDs. **Create support archive** can include saved cloud evidence directly and can refresh it inline before the ZIP is built. Open **Advanced metadata tools** when you need to export the cloud evidence separately or generate drafts from it.",
            {"detail": detail},
        )

    def _localized_local_override_status(
        self,
        details: dict[str, Any],
        *,
        kind: str,
    ) -> str:
        path = str(details.get("path") or "").strip()
        kind_label = self._tr(
            f"common.dynamic.local_override_kind_{kind}",
            kind.replace("_", " "),
        )
        if bool(details.get("exists")) and path:
            return self._tr(
                "common.dynamic.local_override_status_active",
                "Active local override at {path}.",
                {"path": path},
            )
        if path:
            return self._tr(
                "common.dynamic.local_override_status_missing",
                "No active local override. Create {path} to override the built-in {kind}.",
                {"path": path, "kind": kind_label},
            )
        return self._tr(
            "common.dynamic.local_override_status_unavailable",
            "No built-in {kind} is available for this entry.",
            {"kind": kind_label},
        )

    def _localized_local_metadata_status(self, values: dict[str, Any]) -> str:
        raw_status = str(values.get("local_metadata_status") or "").strip()
        if not raw_status:
            return self._tr(
                "common.dynamic.no_diagnostics_action",
                "No diagnostics action has been run yet.",
            )
        translation_key = _LOCAL_METADATA_STATUS_TRANSLATION_KEYS.get(raw_status)
        if translation_key is None:
            return raw_status
        return self._tr(
            f"common.dynamic.local_metadata_status_{translation_key}",
            raw_status,
        )

    def _support_action_label(self, action: str) -> str:
        return {
            "create_support_package": self._tr(
                "common.dynamic.action_create_support_package",
                "Create support archive",
            ),
            "reload_local_metadata": self._tr(
                "common.dynamic.action_reload_local_metadata",
                "Reload local metadata",
            ),
            "rollback_local_metadata": self._tr(
                "common.dynamic.action_rollback_local_metadata",
                "Rollback local metadata",
            ),
            "proxy_capture": self._tr(
                "common.dynamic.action_proxy_capture",
                "Collector traffic capture",
            ),
        }.get(action, action)

    def _local_metadata_rollback_paths(self):
        coordinator = self._coordinator()
        return resolve_local_metadata_rollback_paths(
            config_dir=Path(self.hass.config.config_dir),
            profile_name=(getattr(coordinator, "effective_profile_name", "") or None),
            schema_name=(
                getattr(coordinator, "effective_register_schema_name", "") or None
            ),
            profile_metadata=getattr(coordinator, "effective_profile_metadata", None),
            schema_metadata=getattr(
                coordinator, "effective_register_schema_metadata", None
            ),
        )

    def _support_workflow_translation_key(self, level: str, field: str) -> str:
        return f"common.dynamic.support_workflow_{level}_{field}"

    def _diagnostics_result_tr(
        self,
        field: str,
        default: str,
        placeholders: dict[str, Any] | None = None,
    ) -> str:
        return self._tr(
            f"common.dynamic.diagnostics_result_{field}",
            default,
            placeholders,
        )

    def _localized_support_workflow(self, values: dict[str, Any]) -> dict[str, str]:
        level = str(values.get("support_workflow_level") or "unknown")
        primary_action = str(
            values.get("support_workflow_primary_action") or "create_support_package"
        )
        step_1 = self._tr(
            self._support_workflow_translation_key(level, "step_1"),
            str(
                values.get("support_workflow_step_1")
                or "Run the primary diagnostics action."
            ),
        )
        step_2 = self._tr(
            self._support_workflow_translation_key(level, "step_2"),
            str(
                values.get("support_workflow_step_2")
                or "Send the ZIP file to the developer."
            ),
        )
        step_3 = self._tr(
            self._support_workflow_translation_key(level, "step_3"),
            str(
                values.get("support_workflow_step_3")
                or "Use advanced metadata tools only if requested."
            ),
        )
        return {
            "support_workflow_level": level,
            "support_workflow_level_label": self._tr(
                self._support_workflow_translation_key(level, "level_label"),
                str(values.get("support_workflow_level_label") or "Unknown support"),
            ),
            "support_workflow_summary": self._tr(
                self._support_workflow_translation_key(level, "summary"),
                str(
                    values.get("support_workflow_summary")
                    or "Support status is not available yet."
                ),
            ),
            "support_workflow_next_action": self._tr(
                self._support_workflow_translation_key(level, "next_action"),
                str(
                    values.get("support_workflow_next_action")
                    or "Run detection or create a support archive when the inverter is available."
                ),
            ),
            "support_workflow_step_1": step_1,
            "support_workflow_step_2": step_2,
            "support_workflow_step_3": step_3,
            "support_workflow_plan": self._tr(
                "common.dynamic.plan_template",
                "Step 1: {step_1} Step 2: {step_2} Step 3: {step_3}",
                {"step_1": step_1, "step_2": step_2, "step_3": step_3},
            ),
            "support_workflow_advanced_hint": self._tr(
                self._support_workflow_translation_key(level, "advanced_hint"),
                str(
                    values.get("support_workflow_advanced_hint")
                    or "Advanced metadata tools are secondary and should be used only after the primary support path is complete."
                ),
            ),
            "support_workflow_primary_action": primary_action,
            "support_workflow_primary_action_label": self._support_action_label(
                primary_action
            ),
        }

    def _diagnostics_placeholders(self) -> dict[str, str]:
        coordinator = self._coordinator()
        values = coordinator.data.values if coordinator is not None else {}
        proxy_capture_download_url = self._fresh_proxy_capture_download_url(values)
        effective_owner_name = (
            coordinator.effective_owner_name if coordinator is not None else ""
        )
        effective_owner_key = (
            coordinator.effective_owner_key if coordinator is not None else ""
        )
        smartess_family_name = (
            coordinator.smartess_family_name if coordinator is not None else ""
        )
        effective_profile_name = (
            coordinator.effective_profile_name if coordinator is not None else ""
        )
        effective_register_schema_name = (
            coordinator.effective_register_schema_name
            if coordinator is not None
            else ""
        )
        profile_metadata = (
            coordinator.effective_profile_metadata if coordinator is not None else None
        )
        register_schema_metadata = (
            coordinator.effective_register_schema_metadata
            if coordinator is not None
            else None
        )
        config_dir = Path(self.hass.config.config_dir)
        profile_override = local_profile_override_details(
            config_dir,
            effective_profile_name or None,
        )
        schema_override = local_register_schema_override_details(
            config_dir,
            effective_register_schema_name or None,
        )
        placeholders = {
            "model_name": self._config_entry.data.get(
                CONF_DETECTED_MODEL,
                self._tr("common.dynamic.unknown", "Unknown"),
            ),
            "serial_number": self._config_entry.data.get(
                CONF_DETECTED_SERIAL,
                self._tr("common.dynamic.unknown", "Unknown"),
            ),
            "effective_owner_name": effective_owner_name
            or self._tr("common.dynamic.not_available", "Not available"),
            "effective_owner_key": effective_owner_key
            or self._tr("common.dynamic.not_available", "Not available"),
            "smartess_family_name": smartess_family_name,
            "smartess_family_line": (
                self._tr(
                    "common.dynamic.smartess_family_line",
                    "\n**SmartESS family:** {family}",
                    {"family": smartess_family_name},
                )
                if smartess_family_name
                else ""
            ),
            "profile_name": effective_profile_name
            or self._tr("common.dynamic.not_available", "Not available"),
            "register_schema_name": effective_register_schema_name
            or self._tr("common.dynamic.not_available", "Not available"),
            "support_archive_action_label": self._support_action_label(
                "create_support_package"
            ),
            "effective_profile_source": self._metadata_source_summary(profile_metadata),
            "effective_schema_source": self._metadata_source_summary(
                register_schema_metadata
            ),
            "profile_override_status": self._localized_local_override_status(
                profile_override,
                kind="profile",
            ),
            "schema_override_status": self._localized_local_override_status(
                schema_override,
                kind="register_schema",
            ),
            "suggested_profile_output": effective_profile_name
            or self._tr("common.dynamic.not_available", "Not available"),
            "suggested_schema_output": effective_register_schema_name
            or self._tr("common.dynamic.not_available", "Not available"),
            "support_package_path": str(
                values.get("support_package_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "support_package_download_url": str(
                values.get("support_package_download_relative_url")
                or values.get("support_package_download_url")
                or ""
            ),
            "support_package_download_markdown": (
                self._download_link_markup(
                    str(
                        values.get("support_package_download_relative_url")
                        or values.get("support_package_download_url")
                        or ""
                    ),
                    label=self._tr(
                        "common.dynamic.download_support_archive_label",
                        "Download support archive",
                    ),
                )
                if values.get("support_package_download_url")
                or values.get("support_package_download_relative_url")
                else self._tr("common.dynamic.not_available_yet", "Not available yet")
            ),
            "cloud_evidence_path": self._current_cloud_evidence_path(coordinator)
            or self._tr("common.dynamic.not_created_yet", "Not created yet"),
            "proxy_capture_status_label": self._localized_proxy_capture_status_label(
                values
            ),
            "proxy_capture_summary": str(
                values.get("proxy_capture_summary")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_capture_blocking_reason": self._localized_proxy_capture_blocking_reason(
                values
            ),
            "proxy_capture_current_endpoint": str(
                values.get("proxy_capture_current_endpoint")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_capture_target_endpoint": str(
                values.get("proxy_capture_target_endpoint")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_capture_masked_endpoint": str(
                values.get("proxy_capture_masked_endpoint")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_capture_redirect_required": (
                self._tr("common.dynamic.yes", "Yes")
                if values.get("proxy_capture_redirect_required")
                else self._tr("common.dynamic.no", "No")
            ),
            "proxy_capture_can_stop": (
                self._tr("common.dynamic.yes", "Yes")
                if values.get("proxy_capture_can_stop")
                else self._tr("common.dynamic.no", "No")
            ),
            "proxy_trace_path": str(
                values.get("proxy_trace_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "proxy_trace_manifest_path": str(
                values.get("proxy_trace_saved_result_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "proxy_trace_manifest_download_url": proxy_capture_download_url,
            "proxy_trace_manifest_download_markdown": (
                self._tr(
                    "common.dynamic.download_proxy_capture_result",
                    "[Download saved result]({url})",
                    {"url": proxy_capture_download_url},
                )
                if proxy_capture_download_url
                else self._tr("common.dynamic.not_available_yet", "Not available yet")
            ),
            "proxy_capture_saved_result_section": self._proxy_capture_saved_result_section(
                saved_result_download_url=proxy_capture_download_url,
                status=str(values.get("proxy_capture_status") or ""),
            ),
            "proxy_trace_line_count": str(values.get("proxy_trace_line_count") or 0),
            "proxy_trace_kind_summary": str(
                values.get("proxy_trace_kind_summary")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_trace_recent_kinds": str(
                values.get("proxy_trace_recent_kinds")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_trace_recent_events": str(
                values.get("proxy_trace_recent_events") or ""
            ),
            "proxy_capture_live_log": self._proxy_capture_live_log(values),
            "proxy_capture_user_plan": self._proxy_capture_user_plan(values),
            "proxy_capture_timer_summary": self._proxy_capture_timer_summary(values),
            "proxy_capture_duration_minutes": str(
                _coerce_proxy_capture_duration_minutes(
                    values.get(CONF_PROXY_CAPTURE_DURATION_MINUTES),
                    default=DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
                )
            ),
            "proxy_capture_remaining_minutes": str(
                _coerce_proxy_capture_duration_minutes(
                    values.get("proxy_capture_remaining_minutes"),
                    default=0,
                    minimum=0,
                )
            ),
            "proxy_trace_last_timestamp": str(
                values.get("proxy_trace_last_timestamp")
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "proxy_capture_session_expires_at": self._format_proxy_capture_session_expires_at(
                values.get("proxy_capture_session_expires_at")
            ),
            "proxy_capture_action_result": str(
                getattr(self, "_proxy_capture_action_result", "")
                or self._tr("common.dynamic.not_run_yet", "Not run yet")
            ),
            "local_profile_draft_path": str(
                values.get("local_profile_draft_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "local_schema_draft_path": str(
                values.get("local_schema_draft_path")
                or self._tr("common.dynamic.not_created_yet", "Not created yet")
            ),
            "local_metadata_status": self._localized_local_metadata_status(values),
            "smartess_cloud_diagnostics_hint": self._smartess_cloud_diagnostics_hint(),
        }
        placeholders.update(self._localized_support_workflow(values))
        return placeholders

    async def _async_show_diagnostics_result(
        self,
        *,
        action_title: str,
        status: str,
        path: str = "",
        download_url: str = "",
        next_step: str = "",
    ) -> ConfigFlowResult:
        self._diagnostics_result = {
            "action_title": action_title,
            "status": status,
            "path": path or self._tr("common.dynamic.not_applicable", "Not applicable"),
            "download_url": download_url or "",
            "download_markdown": (
                self._download_link_markup(
                    download_url,
                    label=self._tr(
                        "common.dynamic.download_file_label",
                        "Download file",
                    ),
                )
                if download_url
                else self._tr("common.dynamic.not_available", "Not available")
            ),
            "next_step": next_step
            or self._tr(
                "common.dynamic.return_to_diagnostics",
                "Return to diagnostics to run another action.",
            ),
        }
        return await self.async_step_diagnostics_result()

    def _download_link_markup(self, url: str, *, label: str) -> str:
        """Return a browser download link that HA frontend should not SPA-route."""

        raw_url = str(url or "").strip()
        safe_url = html_escape(raw_url, quote=True)
        safe_label = html_escape(str(label or "").strip() or "Download", quote=False)
        if not raw_url:
            return self._tr("common.dynamic.not_available", "Not available")
        return (
            f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer" '
            f"download>{safe_label}</a>"
        )
