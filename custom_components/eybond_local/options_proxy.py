"""Extracted EyeBond options-flow lifecycle: ProxyCaptureOptionsMixin."""

from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .const import (
    CONF_PROXY_CAPTURE_DURATION_MINUTES,
    DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
    MAX_PROXY_CAPTURE_DURATION_MINUTES,
    MIN_PROXY_CAPTURE_DURATION_MINUTES,
)
from .flow_translation import with_translation_bundle as _with_translation_bundle
from .options_shared import (
    _MULTILINE_LOG_TEXT_SELECTOR,
    _coerce_proxy_capture_duration_minutes,
)
from .support.download import sign_proxy_capture_download_url

logger = logging.getLogger(__name__)


PROXY_CAPTURE_ACTION_RESET_TIMER = "reset_timer"


_PROXY_CAPTURE_DURATION_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=MIN_PROXY_CAPTURE_DURATION_MINUTES,
        max=MAX_PROXY_CAPTURE_DURATION_MINUTES,
        step=1,
        unit_of_measurement="min",
        mode=NumberSelectorMode.BOX,
    )
)


class ProxyCaptureOptionsMixin:
    """ProxyCaptureOptions lifecycle."""

    @_with_translation_bundle
    async def async_step_proxy_capture(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "proxy_capture_title",
                    "Collector Proxy Capture",
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

        if not (
            (
                self._cloud_tool_new_operations_allowed()
                and self._collector_capabilities().proxy_capture
            )
            or self._proxy_capture_lifecycle_active(coordinator)
            or (
                self._collector_capabilities().proxy_capture
                and self._proxy_capture_status_available(coordinator)
            )
        ):
            return await self._async_cloud_tools_unavailable()

        errors: dict[str, str] = {}
        action = ""
        touch_proxy_capture_lease = getattr(
            coordinator, "async_touch_proxy_capture_lease", None
        )
        if user_input is not None:
            action = str(user_input.get("proxy_capture_action") or "refresh").strip()
            duration_minutes = user_input.get(
                CONF_PROXY_CAPTURE_DURATION_MINUTES,
                getattr(
                    coordinator,
                    "proxy_capture_configured_duration_minutes",
                    self._config_entry.options.get(
                        CONF_PROXY_CAPTURE_DURATION_MINUTES,
                        DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
                    ),
                ),
            )
            try:
                if action == "start":
                    overview = coordinator.proxy_capture_overview
                    await coordinator.async_start_proxy_capture(
                        anonymized=True,
                        confirm_redirect=bool(
                            getattr(overview, "redirect_required", False)
                        ),
                        duration_minutes=duration_minutes,
                    )
                    self._proxy_capture_action_result = self._tr(
                        "common.dynamic.proxy_capture_action_started",
                        "Capture started.",
                    )
                elif action == PROXY_CAPTURE_ACTION_RESET_TIMER:
                    expires_at = ""
                    set_duration = getattr(
                        coordinator,
                        "async_set_proxy_capture_duration_minutes",
                        None,
                    )
                    if callable(set_duration):
                        await set_duration(duration_minutes)
                    if touch_proxy_capture_lease is not None:
                        expires_at = str(
                            await touch_proxy_capture_lease(extend=True) or ""
                        ).strip()
                    if expires_at:
                        self._proxy_capture_action_result = self._tr(
                            "common.dynamic.proxy_capture_action_timer_reset",
                            "Proxy timer reset.",
                        )
                    else:
                        self._proxy_capture_action_result = self._tr(
                            "common.dynamic.proxy_capture_action_already_stopped",
                            "Capture was already stopped. Status refreshed.",
                        )
                elif action == "stop":
                    await coordinator.async_stop_proxy_capture()
                    self._proxy_capture_action_result = self._tr(
                        "common.dynamic.proxy_capture_action_stopped",
                        "Capture stopped.",
                    )
                else:
                    refresh = getattr(coordinator, "async_request_refresh", None)
                    if refresh is not None:
                        await refresh()
                    self._proxy_capture_action_result = self._tr(
                        "common.dynamic.proxy_capture_action_refreshed",
                        "Live log refreshed.",
                    )
            except Exception as exc:  # pragma: no cover - HA renders the error key.
                logger.exception(
                    "Cloud traffic capture action %s failed for entry %s",
                    action,
                    self.config_entry.entry_id,
                )
                if await self._handle_proxy_capture_action_error(
                    coordinator, action, exc
                ):
                    errors.clear()
                else:
                    errors.setdefault("base", "proxy_capture_action_failed")
                    self._proxy_capture_action_result = (
                        self._proxy_capture_action_error_message(exc)
                    )

        if touch_proxy_capture_lease is not None and user_input is None:
            await touch_proxy_capture_lease(extend=False)

        return self._show_proxy_capture_form(coordinator, errors=errors)

    async def _handle_proxy_capture_action_error(
        self,
        coordinator,
        action: str,
        exc: Exception,
    ) -> bool:
        if action != "stop":
            return False
        if str(exc or "").strip() != "proxy_capture_not_running":
            return False

        refresh = getattr(coordinator, "async_request_refresh", None)
        if refresh is not None:
            await refresh()
        self._proxy_capture_action_result = self._tr(
            "common.dynamic.proxy_capture_action_already_stopped",
            "Capture was already stopped. Status refreshed.",
        )
        return True

    def _proxy_capture_action_error_message(self, exc: Exception) -> str:
        raw_error = str(exc or "").strip()
        if not raw_error:
            return self._tr(
                "common.dynamic.proxy_capture_action_error_internal",
                "Collector proxy capture could not be started. Check the Home Assistant log and try again.",
            )

        error_code, _separator, detail = raw_error.partition(":")
        if error_code == "proxy_capture_route_stopped":
            return self._tr(
                "common.dynamic.proxy_capture_action_error_route_stopped",
                "Collector proxy route stopped before the collector reconnected. Check the Home Assistant log and try again.",
            )
        if error_code == "proxy_capture_collector_reconnect_timeout":
            return self._tr(
                "common.dynamic.proxy_capture_action_error_reconnect_timeout",
                "Collector did not reconnect through the proxy in time. Check the collector callback settings and try again.",
            )
        if error_code in {
            "proxy_capture_upstream_connect_failed",
            "proxy_capture_upstream_unreachable",
        }:
            return self._tr(
                "common.dynamic.proxy_capture_action_error_upstream_connect_failed",
                "Home Assistant could not connect to the current upstream collector endpoint: {detail}.",
                {
                    "detail": detail
                    or self._tr("common.dynamic.not_available", "Not available"),
                },
            )
        if error_code == "proxy_capture_not_running":
            return self._tr(
                "common.dynamic.proxy_capture_action_already_stopped",
                "Capture was already stopped. Status refreshed.",
            )
        if " " not in raw_error and raw_error.lower() == raw_error:
            return self._tr(
                "common.dynamic.proxy_capture_action_error_internal",
                "Collector proxy capture could not be started. Check the Home Assistant log and try again.",
            )
        return raw_error

    def _proxy_capture_action_options(self, coordinator) -> list[SelectOptionDict]:
        overview = coordinator.proxy_capture_overview
        options: list[SelectOptionDict] = []
        if overview.can_stop:
            options.append(
                SelectOptionDict(
                    value="stop",
                    label=self._tr(
                        "common.dynamic.proxy_capture_action_stop", "Stop proxy capture"
                    ),
                )
            )
            options.append(
                SelectOptionDict(
                    value=PROXY_CAPTURE_ACTION_RESET_TIMER,
                    label=self._tr(
                        "common.dynamic.proxy_capture_action_reset_timer",
                        "Reset proxy timer",
                    ),
                )
            )
        if overview.can_start:
            options.append(
                SelectOptionDict(
                    value="start",
                    label=self._tr(
                        "common.dynamic.proxy_capture_action_start",
                        "Start proxy capture",
                    ),
                )
            )
        options.append(
            SelectOptionDict(
                value="refresh",
                label=self._tr(
                    "common.dynamic.proxy_capture_action_refresh",
                    "Refresh live log",
                ),
            )
        )
        return options

    def _default_proxy_capture_action(
        self, coordinator, options: list[SelectOptionDict]
    ) -> str:
        """Return the default proxy-capture action for the current form state."""

        option_values = {str(option["value"]) for option in options}
        overview = coordinator.proxy_capture_overview
        if overview.can_start and "start" in option_values:
            return "start"
        if overview.can_stop and "refresh" in option_values:
            return "refresh"
        if "refresh" in option_values:
            return "refresh"
        return str(options[0]["value"] if options else "refresh")

    def _show_proxy_capture_form(
        self,
        coordinator,
        *,
        errors: dict[str, str] | None = None,
    ) -> ConfigFlowResult:
        options = self._proxy_capture_action_options(coordinator)
        default_action = self._default_proxy_capture_action(coordinator, options)
        placeholders = self._diagnostics_placeholders()
        return self.async_show_form(
            step_id="proxy_capture",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        "proxy_capture_live_log_view",
                        default=placeholders.get("proxy_capture_live_log") or "",
                    ): _MULTILINE_LOG_TEXT_SELECTOR,
                    vol.Required(
                        "proxy_capture_action", default=default_action
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=options, mode=SelectSelectorMode.DROPDOWN
                        )
                    ),
                    vol.Required(
                        CONF_PROXY_CAPTURE_DURATION_MINUTES,
                        default=getattr(
                            coordinator,
                            "proxy_capture_display_duration_minutes",
                            self._config_entry.options.get(
                                CONF_PROXY_CAPTURE_DURATION_MINUTES,
                                DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
                            ),
                        ),
                    ): _PROXY_CAPTURE_DURATION_SELECTOR,
                }
            ),
            errors=errors or {},
            description_placeholders=placeholders,
        )

    def _fresh_proxy_capture_download_url(self, values: dict[str, Any]) -> str:
        """Mint a fresh signed API URL each time the proxy form is rendered."""

        bundle_path = values.get("proxy_trace_saved_result_path")
        if (
            type(bundle_path) is str
            and bundle_path == bundle_path.strip()
            and bundle_path
        ):
            filename = Path(bundle_path).name
            entry_id = getattr(self._config_entry, "entry_id", "")
            if (
                type(entry_id) is str
                and entry_id == entry_id.strip()
                and entry_id
                and filename.lower().endswith(".zip")
            ):
                with suppress(Exception):
                    return sign_proxy_capture_download_url(
                        self.hass,
                        entry_id,
                        filename,
                    )

        # Compatibility for a live coordinator that has not yet published the
        # archive path. New captures always take the path branch above.
        existing = values.get("proxy_trace_saved_result_download_url")
        return (
            existing.strip()
            if type(existing) is str and existing == existing.strip()
            else ""
        )

    def _localized_proxy_capture_status_label(self, values: dict[str, Any]) -> str:
        status = str(values.get("proxy_capture_status") or "").strip()
        fallback = str(values.get("proxy_capture_status_label") or "").strip()
        if not status and fallback:
            status = fallback.lower()
        return self._tr(
            f"common.dynamic.proxy_capture_status_{status}",
            fallback or self._tr("common.dynamic.not_available", "Not available"),
        )

    def _localized_proxy_capture_blocking_reason(self, values: dict[str, Any]) -> str:
        reason = str(values.get("proxy_capture_blocking_reason") or "").strip()
        if not reason:
            return self._tr("common.dynamic.not_applicable", "Not applicable")
        return self._tr(
            f"common.dynamic.proxy_capture_blocking_{reason}",
            reason,
        )

    def _format_proxy_capture_session_expires_at(self, value: object) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""

        normalized = f"{raw[:-1]}+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return raw

        localized = parsed
        timezone_name = str(
            getattr(getattr(self.hass, "config", None), "time_zone", "") or ""
        ).strip()
        if parsed.tzinfo is not None and timezone_name:
            try:
                localized = parsed.astimezone(ZoneInfo(timezone_name))
            except (ValueError, ZoneInfoNotFoundError):
                localized = parsed

        formatted = localized.strftime("%d.%m.%Y %H:%M")
        if localized.tzinfo is None:
            return formatted

        timezone_label = (localized.tzname() or "").strip()
        if timezone_label in {"+00:00", "UTC+00:00"}:
            timezone_label = "UTC"
        return f"{formatted} {timezone_label}".strip()

    def _proxy_capture_user_plan(self, values: dict[str, Any]) -> str:
        blocking_reason = self._localized_proxy_capture_blocking_reason(values)
        if values.get("proxy_capture_can_stop"):
            expires_at = self._format_proxy_capture_session_expires_at(
                values.get("proxy_capture_session_expires_at")
            )
            remaining = self._format_proxy_capture_remaining_time(
                values.get("proxy_capture_remaining_seconds")
            )
            if expires_at:
                return self._tr(
                    "common.dynamic.proxy_capture_plan_running_with_lease",
                    "Capture is in progress. Refresh live log updates the events shown here. Use Reset proxy timer to extend the current session. Home Assistant will stop the capture and restore the collector connection in {remaining_time}, no later than {expires_at}. When you have enough data, choose Stop capture.",
                    {
                        "expires_at": expires_at,
                        "remaining_time": remaining or expires_at,
                    },
                )
            return self._tr(
                "common.dynamic.proxy_capture_plan_running",
                "Capture is in progress. Leave this page open and use Refresh live log to see new events. Use Reset proxy timer to extend the current session when needed. When you have enough data, choose Stop capture.",
            )
        if str(values.get("proxy_capture_blocking_reason") or "").strip():
            return self._tr(
                "common.dynamic.proxy_capture_plan_blocked",
                "Capture cannot start yet: {reason}",
                {"reason": blocking_reason},
            )
        if (
            str(values.get("proxy_trace_saved_result_download_url") or "").strip()
            or str(values.get("proxy_trace_saved_result_path") or "").strip()
        ):
            return self._tr(
                "common.dynamic.proxy_capture_plan_ready_after_session",
                "The previous capture is complete. Download the saved result below or start a new capture when you need another session.",
            )
        return self._tr(
            "common.dynamic.proxy_capture_plan_start",
            "Start capture will have Home Assistant accept collector traffic on the proxy endpoint and record it here.",
        )

    def _format_proxy_capture_remaining_time(self, value: object) -> str:
        try:
            seconds = max(0, int(float(value)))
        except (TypeError, ValueError):
            return ""
        if seconds <= 0:
            return self._tr(
                "common.dynamic.proxy_capture_remaining_less_than_minute",
                "less than 1 min",
            )
        minutes = max(1, (seconds + 59) // 60)
        unit = self._tr("common.dynamic.duration_minutes_short", "min")
        return f"{minutes} {unit}"

    def _proxy_capture_timer_summary(self, values: dict[str, Any]) -> str:
        configured_minutes = _coerce_proxy_capture_duration_minutes(
            values.get(CONF_PROXY_CAPTURE_DURATION_MINUTES),
            default=DEFAULT_PROXY_CAPTURE_DURATION_MINUTES,
        )
        if values.get("proxy_capture_can_stop"):
            remaining = self._format_proxy_capture_remaining_time(
                values.get("proxy_capture_remaining_seconds")
            )
            expires_at = self._format_proxy_capture_session_expires_at(
                values.get("proxy_capture_session_expires_at")
            )
            if remaining and expires_at:
                return self._tr(
                    "common.dynamic.proxy_capture_timer_running_with_deadline",
                    "Remaining: {remaining_time}. Auto-stop: {expires_at}.",
                    {"remaining_time": remaining, "expires_at": expires_at},
                )
            if remaining:
                return self._tr(
                    "common.dynamic.proxy_capture_timer_running",
                    "Remaining: {remaining_time}.",
                    {"remaining_time": remaining},
                )
        return self._tr(
            "common.dynamic.proxy_capture_timer_configured",
            "Session duration: {duration_minutes} min.",
            {"duration_minutes": configured_minutes},
        )

    def _proxy_capture_saved_result_section(
        self,
        *,
        saved_result_download_url: str,
        status: str,
    ) -> str:
        normalized_status = str(status or "").strip()
        if normalized_status in {"starting", "running", "stopping", "restoring"}:
            return ""
        if not saved_result_download_url:
            return ""
        download_markdown = (
            self._tr(
                "common.dynamic.download_proxy_capture_result",
                "[Download saved result]({url})",
                {"url": saved_result_download_url},
            )
            if saved_result_download_url
            else self._tr("common.dynamic.not_available_yet", "Not available yet")
        )
        return self._tr(
            "common.dynamic.proxy_capture_saved_result_section",
            "**Saved result:** {download}",
            {
                "download": download_markdown,
            },
        )

    def _proxy_capture_live_log(self, values: dict[str, Any]) -> str:
        status = str(values.get("proxy_capture_status") or "").strip()
        if status not in {"starting", "running", "stopping", "restoring"}:
            return self._tr(
                "common.dynamic.proxy_capture_live_log_not_started",
                "The live log is empty. Start capture, then use Refresh live log to show new events here.",
            )
        live_log = str(values.get("proxy_trace_live_log") or "").strip()
        if live_log:
            return live_log
        recent_events = str(values.get("proxy_trace_recent_events") or "").strip()
        if recent_events:
            return recent_events
        return self._tr(
            "common.dynamic.proxy_capture_live_log_waiting",
            "Capture is running. No traffic has reached the log yet. Use Refresh live log after the collector reconnects.",
        )
