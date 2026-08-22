"""Extracted EyeBond options-flow lifecycle: RuntimeOptionsMixin."""

from __future__ import annotations

from contextlib import suppress
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.data_entry_flow import section
from homeassistant.helpers.selector import (
    BooleanSelector,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from ... import network_interfaces
from ...collector.smartess_ble import SmartEssBleWifiNetwork, parse_wifi_scan_response
from ...collector.smartess_local import (
    QUERY_HARDWARE_VERSION,
    QUERY_NETWORK_DIAGNOSTICS,
    QUERY_SERIAL_BAUDRATE,
    QUERY_WIFI_SCAN_LIST,
    SET_TARGET_PASSWORD,
    SET_TARGET_SSID,
)
from ...connection.branch_registry import get_connection_branch
from ...connection.connection_policy import resolve_connection_strategy
from ...connection.entry import build_runtime_option_settings
from ..common.connection_form import (
    DRIVER_DISPLAY_LABELS as _DRIVER_DISPLAY_LABELS,
)
from ..common.connection_form import (
    validate_connection_inputs as _validate_shared_connection_inputs,
)
from ...const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_CONNECTION_STRATEGY,
    CONF_CONNECTION_TYPE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_DRIVER,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_DRIVER_DETECTION_STRATEGY,
    CONF_DRIVER_HINT,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    CONF_PROXY_ENABLED,
    CONF_SERVER_IP,
    CONNECTION_STRATEGIES,
    CONNECTION_TYPE_EYBOND,
    CONTROL_MODE_FULL,
    CONTROL_MODE_READ_ONLY,
    DEFAULT_CONTROL_MODE,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DEFAULT_POLL_INTERVAL,
    DRIVER_DETECTION_STRATEGIES,
    DRIVER_HINT_AUTO,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from ...control_policy import control_mode_options
from ...drivers.registry import driver_options
from ..common.presentation import (
    _PASSWORD_TEXT_SELECTOR,
    CONF_WIFI_PASSWORD,
    CONF_WIFI_SSID,
    _ble_wifi_selector,
    _clear_runtime_inverter_facts,
    _driver_detection_strategy_selector,
    _exception_detail,
    _flatten_sections,
    _poll_interval_selector,
    _poll_mode_selector,
)
from ..common.translation import (
    selector_option_label as _selector_option_label,
)
from ..common.translation import (
    with_translation_bundle as _with_translation_bundle,
)

CONF_COLLECTOR_WIFI_ACTION = "collector_wifi_action"


CONF_CONFIRM_COLLECTOR_WIFI_APPLY = "confirm_collector_wifi_apply"


CONF_COLLECTOR_UART_ACTION = "collector_uart_action"


CONF_COLLECTOR_UART_BAUDRATE = "collector_uart_baudrate"


CONF_CONFIRM_COLLECTOR_UART_APPLY = "confirm_collector_uart_apply"


COLLECTOR_WIFI_ACTION_REFRESH = "refresh"


COLLECTOR_WIFI_ACTION_APPLY = "apply"


COLLECTOR_UART_ACTION_REFRESH = "refresh"


COLLECTOR_UART_ACTION_APPLY = "apply"


COLLECTOR_UART_BAUDRATES = ("2400", "4800", "9600", "19200", "38400", "57600", "115200")


def _control_mode_selector(bundle: dict[str, Any] | None = None) -> SelectSelector:
    labels = {"auto": "Auto", "read_only": "Read only", "full": "Full control"}
    options = [
        SelectOptionDict(
            value=opt,
            label=_selector_option_label(
                bundle, "control_mode", opt, labels.get(opt, opt)
            ),
        )
        for opt in control_mode_options()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_wifi_action_selector(
    *, refresh_label: str, apply_label: str
) -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(
                    value=COLLECTOR_WIFI_ACTION_REFRESH, label=refresh_label
                ),
                SelectOptionDict(value=COLLECTOR_WIFI_ACTION_APPLY, label=apply_label),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_uart_baudrate_selector() -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=value, label=value)
                for value in COLLECTOR_UART_BAUDRATES
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _collector_uart_action_selector(
    *,
    refresh_label: str,
    apply_label: str,
    include_apply: bool = True,
) -> SelectSelector:
    options = [
        SelectOptionDict(value=COLLECTOR_UART_ACTION_REFRESH, label=refresh_label)
    ]
    if include_apply:
        options.append(
            SelectOptionDict(value=COLLECTOR_UART_ACTION_APPLY, label=apply_label)
        )
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


class RuntimeOptionsMixin:
    """RuntimeOptions lifecycle."""

    @_with_translation_bundle
    async def async_step_inverter_protocol(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Record explicit protocol intent after runtime proved an ambiguity.

        The selection is deliberately not persisted as detected identity.  We
        clear the old runtime fact and reload; the new runtime then probes only
        the selected driver on the entry-owned session.  Inverter entities are
        created only after that live confirmation is persisted.
        """

        candidates = self._runtime_inverter_protocol_candidates()
        if len(candidates) <= 1:
            return await self.async_step_init()

        candidate_by_key = {candidate.driver_key: candidate for candidate in candidates}
        errors: dict[str, str] = {}
        if user_input is not None:
            selected = user_input.get(CONF_DRIVER_HINT)
            if type(selected) is not str or selected not in candidate_by_key:
                errors[CONF_DRIVER_HINT] = "invalid_selection"
            else:
                data = dict(self._config_entry.data)
                options = dict(self._config_entry.options)
                options[CONF_DRIVER_HINT] = selected
                options[CONF_CONTROL_MODE] = CONTROL_MODE_READ_ONLY
                _clear_runtime_inverter_facts(data)
                self.hass.config_entries.async_update_entry(
                    self._config_entry,
                    data=data,
                    options=options,
                )
                # The integration's ordinary config-entry update listener owns
                # the one reload.  ``async_create_entry`` receives the already
                # persisted options, so Home Assistant sees no second change.
                return self.async_create_entry(data=options)

        selector_options = [
            SelectOptionDict(
                value=candidate.driver_key,
                label=" — ".join(
                    part
                    for part in (
                        candidate.model_name or candidate.protocol_family,
                        candidate.protocol_family,
                    )
                    if part
                ),
            )
            for candidate in candidates
        ]
        return self.async_show_form(
            step_id="inverter_protocol",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_DRIVER_HINT,
                        default=candidates[0].driver_key,
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=selector_options,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
            errors=errors,
            description_placeholders={"count": str(len(candidates))},
        )

    @_with_translation_bundle
    async def async_step_collector_wifi(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        selected_action = str(
            defaults.get(CONF_COLLECTOR_WIFI_ACTION, COLLECTOR_WIFI_ACTION_APPLY)
            or COLLECTOR_WIFI_ACTION_APPLY
        ).strip()
        if selected_action not in {
            COLLECTOR_WIFI_ACTION_REFRESH,
            COLLECTOR_WIFI_ACTION_APPLY,
        }:
            selected_action = COLLECTOR_WIFI_ACTION_APPLY

        refresh_requested = (
            user_input is not None and selected_action == COLLECTOR_WIFI_ACTION_REFRESH
        )
        apply_requested = (
            user_input is not None and selected_action == COLLECTOR_WIFI_ACTION_APPLY
        )
        submitted_ssid = str(defaults.get(CONF_WIFI_SSID, "") or "").strip()
        submitted_password = str(defaults.get(CONF_WIFI_PASSWORD, "") or "")

        if user_input is None or refresh_requested:
            try:
                await self._async_refresh_collector_wifi_status()
            except Exception as exc:
                self._collector_wifi_last_error = _exception_detail(exc)
                errors["base"] = "collector_wifi_read_failed"
            else:
                self._collector_wifi_last_error = ""
                if refresh_requested:
                    self._collector_wifi_last_result = self._tr(
                        "common.dynamic.collector_wifi_refresh_done",
                        "Wi-Fi status refreshed.",
                    )
                    selected_action = COLLECTOR_WIFI_ACTION_APPLY

        if apply_requested:
            if not submitted_ssid:
                errors[CONF_WIFI_SSID] = "collector_wifi_ssid_required"
            elif not submitted_ssid.isascii():
                errors[CONF_WIFI_SSID] = "collector_wifi_ssid_not_ascii"
            if not submitted_password:
                errors[CONF_WIFI_PASSWORD] = "collector_wifi_password_required"
            elif not submitted_password.isascii():
                errors[CONF_WIFI_PASSWORD] = "collector_wifi_password_not_ascii"
            if not bool(defaults.get(CONF_CONFIRM_COLLECTOR_WIFI_APPLY)):
                errors[CONF_CONFIRM_COLLECTOR_WIFI_APPLY] = (
                    "collector_wifi_apply_not_confirmed"
                )

            if not errors:
                try:
                    await self._async_apply_collector_wifi_settings(
                        ssid=submitted_ssid,
                        password=submitted_password,
                    )
                except Exception as exc:
                    self._collector_wifi_last_error = _exception_detail(exc)
                    errors["base"] = "collector_wifi_write_failed"
                else:
                    self._collector_wifi_last_error = ""
                    self._collector_wifi_last_result = self._tr(
                        "common.dynamic.collector_wifi_apply_done",
                        "Wi-Fi settings were accepted by the collector.",
                    )
                    return self.async_create_entry(
                        data=dict(self._config_entry.options)
                    )

        default_wifi_ssid = submitted_ssid or self._collector_wifi_current_ssid
        password_default = submitted_password if errors and apply_requested else ""
        return self.async_show_form(
            step_id="collector_wifi",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_WIFI_SSID, default=default_wifi_ssid
                    ): _ble_wifi_selector(
                        self._collector_wifi_networks,
                    ),
                    vol.Optional(
                        CONF_WIFI_PASSWORD, default=password_default
                    ): _PASSWORD_TEXT_SELECTOR,
                    vol.Required(
                        CONF_COLLECTOR_WIFI_ACTION, default=selected_action
                    ): _collector_wifi_action_selector(
                        refresh_label=self._collector_wifi_refresh_action_label(),
                        apply_label=self._collector_wifi_apply_action_label(),
                    ),
                    vol.Required(
                        CONF_CONFIRM_COLLECTOR_WIFI_APPLY, default=False
                    ): BooleanSelector(),
                }
            ),
            errors=errors,
            description_placeholders=self._collector_wifi_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_collector_uart(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._collector_capabilities().uart_management:
            return await self.async_step_init()

        errors: dict[str, str] = {}
        defaults = dict(user_input or {})
        selected_action = str(
            defaults.get(CONF_COLLECTOR_UART_ACTION, COLLECTOR_UART_ACTION_APPLY)
            or COLLECTOR_UART_ACTION_APPLY
        ).strip()
        if selected_action not in {
            COLLECTOR_UART_ACTION_REFRESH,
            COLLECTOR_UART_ACTION_APPLY,
        }:
            selected_action = COLLECTOR_UART_ACTION_APPLY

        refresh_requested = (
            user_input is not None and selected_action == COLLECTOR_UART_ACTION_REFRESH
        )
        apply_requested = (
            user_input is not None and selected_action == COLLECTOR_UART_ACTION_APPLY
        )
        submitted_baudrate = self._normalize_collector_uart_baudrate(
            defaults.get(CONF_COLLECTOR_UART_BAUDRATE, "")
        )

        if user_input is None or refresh_requested:
            try:
                await self._async_refresh_collector_uart_status()
            except Exception as exc:
                self._collector_uart_last_error = _exception_detail(exc)
                errors["base"] = "collector_uart_read_failed"
            else:
                self._collector_uart_last_error = ""
                if refresh_requested:
                    self._collector_uart_last_result = self._tr(
                        "common.dynamic.collector_uart_refresh_done",
                        "Collector UART status has been refreshed.",
                    )
                    selected_action = COLLECTOR_UART_ACTION_APPLY

        if apply_requested:
            if self._collector_uart_runtime_change_unavailable():
                errors["base"] = "collector_uart_runtime_unavailable"
            elif submitted_baudrate not in COLLECTOR_UART_BAUDRATES:
                errors[CONF_COLLECTOR_UART_BAUDRATE] = "collector_uart_baudrate_invalid"
            if not bool(defaults.get(CONF_CONFIRM_COLLECTOR_UART_APPLY)):
                errors[CONF_CONFIRM_COLLECTOR_UART_APPLY] = (
                    "collector_uart_apply_not_confirmed"
                )

            if not errors:
                try:
                    await self._async_apply_collector_uart_baudrate(submitted_baudrate)
                except Exception as exc:
                    self._collector_uart_last_error = _exception_detail(exc)
                    errors["base"] = "collector_uart_write_failed"
                else:
                    self._collector_uart_last_error = ""
                    self._collector_uart_last_result = self._tr(
                        "common.dynamic.collector_uart_apply_done",
                        "The collector accepted the new UART speed.",
                    )
                    return self.async_create_entry(
                        data=dict(self._config_entry.options)
                    )

        default_baudrate = (
            submitted_baudrate
            or self._collector_uart_current_baudrate
            or self._normalize_collector_uart_baudrate(
                self._runtime_collector_uart_settings()
            )
            or "2400"
        )
        runtime_change_unavailable = self._collector_uart_runtime_change_unavailable()
        if runtime_change_unavailable:
            selected_action = COLLECTOR_UART_ACTION_REFRESH
            schema_fields = {
                vol.Required(
                    CONF_COLLECTOR_UART_ACTION, default=selected_action
                ): _collector_uart_action_selector(
                    refresh_label=self._collector_uart_refresh_action_label(),
                    apply_label=self._collector_uart_apply_action_label(),
                    include_apply=False,
                ),
            }
        else:
            schema_fields = {
                vol.Required(
                    CONF_COLLECTOR_UART_BAUDRATE, default=default_baudrate
                ): _collector_uart_baudrate_selector(),
                vol.Required(
                    CONF_COLLECTOR_UART_ACTION, default=selected_action
                ): _collector_uart_action_selector(
                    refresh_label=self._collector_uart_refresh_action_label(),
                    apply_label=self._collector_uart_apply_action_label(),
                ),
                vol.Required(
                    CONF_CONFIRM_COLLECTOR_UART_APPLY, default=False
                ): BooleanSelector(),
            }
        return self.async_show_form(
            step_id="collector_uart",
            data_schema=vol.Schema(schema_fields),
            errors=errors,
            description_placeholders=self._collector_uart_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_runtime(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._interface_options:
            self._interface_options = await self.hass.async_add_executor_job(
                network_interfaces.get_ipv4_interfaces
            )
        # The operating profile is edited only in ``async_step_connection``.
        # Runtime/polling saves carry the unchanged strategy so this form can
        # never look like (or accidentally become) a connection transition.
        errors: dict[str, str] = {}
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            # Ignore a stale/forged strategy field posted by an older cached
            # runtime form. Only ``async_step_connection`` may stage a profile
            # transition; polling is not a second writer.
            flat_input[CONF_CONNECTION_STRATEGY] = resolve_connection_strategy(
                self._config_entry.data,
                self._config_entry.options,
            )
            flat_input.setdefault(
                CONF_POLL_MODE,
                self._config_entry.options.get(CONF_POLL_MODE, POLL_MODE_MANUAL),
            )
            if flat_input.get(CONF_POLL_MODE) not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
                errors[CONF_POLL_MODE] = "invalid_selection"
            detection_strategy = flat_input.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                self._config_entry.options.get(
                    CONF_DRIVER_DETECTION_STRATEGY,
                    self._config_entry.data.get(
                        CONF_DRIVER_DETECTION_STRATEGY,
                        DEFAULT_DRIVER_DETECTION_STRATEGY,
                    ),
                ),
            )
            if (
                type(detection_strategy) is not str
                or detection_strategy not in DRIVER_DETECTION_STRATEGIES
            ):
                errors[CONF_DRIVER_DETECTION_STRATEGY] = "invalid_selection"
            connection_type = self._config_entry.data.get(
                CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND
            )
            branch = get_connection_branch(connection_type)
            errors.update(
                _validate_shared_connection_inputs(
                    flat_input,
                    fields=branch.form_layout.runtime_fields,
                )
            )
            if flat_input.get(CONF_CONNECTION_STRATEGY) not in CONNECTION_STRATEGIES:
                errors[CONF_CONNECTION_STRATEGY] = "invalid_selection"
            if not errors:
                if (
                    flat_input.get(CONF_POLL_MODE) == POLL_MODE_MANUAL
                    and CONF_POLL_INTERVAL not in flat_input
                ):
                    self._runtime_poll_interval_pending_input = dict(flat_input)
                    return await self.async_step_runtime_poll_interval()
                return self._async_commit_runtime_options(flat_input)

        connection_type = self._config_entry.data.get(
            CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND
        )
        branch = get_connection_branch(connection_type)
        connection_values = branch.build_runtime_option_values(
            data=self._config_entry.data,
            options=self._config_entry.options,
            default_server_ip=self._config_entry.data[CONF_SERVER_IP],
            default_broadcast=DEFAULT_DISCOVERY_TARGET,
        )
        poll_interval = self._config_entry.options.get(
            CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL
        )
        poll_mode = self._config_entry.options.get(CONF_POLL_MODE, POLL_MODE_MANUAL)
        if poll_mode not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
            poll_mode = POLL_MODE_MANUAL
        control_mode = self._config_entry.options.get(
            CONF_CONTROL_MODE,
            self._config_entry.data.get(CONF_CONTROL_MODE, DEFAULT_CONTROL_MODE),
        )
        detection_strategy = self._config_entry.options.get(
            CONF_DRIVER_DETECTION_STRATEGY,
            self._config_entry.data.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                DEFAULT_DRIVER_DETECTION_STRATEGY,
            ),
        )
        if (
            type(detection_strategy) is not str
            or detection_strategy not in DRIVER_DETECTION_STRATEGIES
        ):
            detection_strategy = DEFAULT_DRIVER_DETECTION_STRATEGY
        driver_intent = self._config_entry.options.get(
            CONF_DRIVER_HINT,
            self._config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
        )
        if type(driver_intent) is not str or driver_intent not in driver_options():
            driver_intent = DRIVER_HINT_AUTO
        schema_fields: dict[Any, Any] = {
            vol.Required(
                CONF_DRIVER_HINT,
                default=driver_intent,
            ): self._runtime_driver_selector(),
            vol.Required(
                CONF_DRIVER_DETECTION_STRATEGY,
                default=detection_strategy,
            ): _driver_detection_strategy_selector(self._translation_bundle),
            vol.Required(CONF_POLL_MODE, default=poll_mode): _poll_mode_selector(
                self._translation_bundle,
            ),
            vol.Required(
                CONF_CONTROL_MODE, default=control_mode
            ): _control_mode_selector(
                self._translation_bundle,
            ),
        }
        if poll_mode == POLL_MODE_MANUAL:
            schema_fields[vol.Required(CONF_POLL_INTERVAL, default=poll_interval)] = (
                _poll_interval_selector(self._poll_policy_driver_key())
            )
        schema_fields[vol.Required("connection")] = section(
            vol.Schema(
                self._build_connection_fields_schema(
                    connection_type,
                    fields=tuple(
                        field
                        for field in branch.form_layout.runtime_fields
                        if field.key != CONF_DRIVER_HINT
                    ),
                    values=connection_values,
                )
            ),
            {"collapsed": True},
        )
        data_schema = vol.Schema(schema_fields)

        return self.async_show_form(
            step_id="runtime",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "model_name": self._config_entry.data.get(
                    CONF_DETECTED_MODEL, "Unknown"
                ),
                "serial_number": self._config_entry.data.get(
                    CONF_DETECTED_SERIAL, "Unknown"
                ),
                "confidence": self._confidence_label(
                    self._config_entry.data.get(CONF_DETECTION_CONFIDENCE, "none")
                ),
                "control_summary": self._control_summary(
                    control_mode=control_mode,
                    confidence=self._config_entry.data.get(
                        CONF_DETECTION_CONFIDENCE, "none"
                    ),
                ),
                "collector_connection_note": "",
            },
        )

    def _async_commit_runtime_options(
        self, flat_input: dict[str, Any]
    ) -> ConfigFlowResult:
        """Persist polling/runtime options without accepting a profile change."""

        options = self._build_runtime_options_from_flat_input(flat_input)
        current_strategy = resolve_connection_strategy(
            self._config_entry.data,
            self._config_entry.options,
        )
        data = dict(self._config_entry.data)
        if current_strategy in CONNECTION_STRATEGIES:
            # Canonicalize a legacy entry while preserving its effective
            # strategy. The runtime caller overwrites any stale posted strategy
            # with this value before reaching this boundary.
            data[CONF_CONNECTION_STRATEGY] = current_strategy
        current_driver_intent = str(
            self._config_entry.options.get(
                CONF_DRIVER_HINT,
                self._config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            or DRIVER_HINT_AUTO
        ).strip()
        submitted_driver_intent = str(
            options.get(CONF_DRIVER_HINT, current_driver_intent) or DRIVER_HINT_AUTO
        ).strip()
        current_detection_strategy = self._config_entry.options.get(
            CONF_DRIVER_DETECTION_STRATEGY,
            self._config_entry.data.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                DEFAULT_DRIVER_DETECTION_STRATEGY,
            ),
        )
        submitted_detection_strategy = options.get(
            CONF_DRIVER_DETECTION_STRATEGY,
            current_detection_strategy,
        )
        if (
            submitted_driver_intent != current_driver_intent
            or submitted_detection_strategy != current_detection_strategy
        ):
            # A driver selector changes user intent, never runtime fact.  Clear
            # the old binding atomically with either driver intent or automatic
            # scan depth so the reloaded runtime really performs the requested
            # identification instead of merely saving a decorative preference.
            _clear_runtime_inverter_facts(data)
            options[CONF_CONTROL_MODE] = CONTROL_MODE_READ_ONLY
        self.hass.config_entries.async_update_entry(
            self._config_entry,
            data=data,
            options=options,
        )
        return self.async_create_entry(data=options)

    def _build_runtime_options_from_flat_input(
        self, flat_input: dict[str, Any]
    ) -> dict[str, Any]:
        connection_type = self._config_entry.data.get(
            CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND
        )
        persisted_options = build_runtime_option_settings(connection_type, flat_input)
        # An absent optional advertised route is not a persisted fact. Older
        # runtime forms wrote the exact pair ("", ""), which later looked like
        # a PRESENT malformed explicit route and correctly blocked lower-priority
        # transition hints. Drop only that complete legacy-empty shape; partial
        # or malformed values are never normalized into absence here.
        if (
            type(persisted_options.get(CONF_ADVERTISED_SERVER_IP)) is str
            and persisted_options.get(CONF_ADVERTISED_SERVER_IP) == ""
            and type(persisted_options.get(CONF_ADVERTISED_TCP_PORT)) is str
            and persisted_options.get(CONF_ADVERTISED_TCP_PORT) == ""
        ):
            persisted_options.pop(CONF_ADVERTISED_SERVER_IP)
            persisted_options.pop(CONF_ADVERTISED_TCP_PORT)
        persisted_options[CONF_POLL_INTERVAL] = flat_input.get(
            CONF_POLL_INTERVAL,
            self._config_entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL),
        )
        persisted_options[CONF_POLL_MODE] = flat_input.get(
            CONF_POLL_MODE,
            self._config_entry.options.get(CONF_POLL_MODE, POLL_MODE_MANUAL),
        )
        persisted_options[CONF_CONTROL_MODE] = flat_input[CONF_CONTROL_MODE]
        detection_strategy = flat_input.get(
            CONF_DRIVER_DETECTION_STRATEGY,
            self._config_entry.options.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                self._config_entry.data.get(
                    CONF_DRIVER_DETECTION_STRATEGY,
                    DEFAULT_DRIVER_DETECTION_STRATEGY,
                ),
            ),
        )
        if (
            type(detection_strategy) is not str
            or detection_strategy not in DRIVER_DETECTION_STRATEGIES
        ):
            detection_strategy = DEFAULT_DRIVER_DETECTION_STRATEGY
        persisted_options[CONF_DRIVER_DETECTION_STRATEGY] = detection_strategy
        # NOTE: connection_strategy is deliberately NOT persisted into options.
        # entry.data is its single canonical owner (schema v4) -- the explicit
        # endpoint actions (HA-only / Cloud+HA switch, bind, rollback) write it
        # there, and an options copy would shadow them. The submitted value is
        # committed to data by _async_commit_runtime_options().
        # The former steady cloud-proxy toggle never had a runtime consumer.
        # Keep the persisted compatibility axis fail-closed, but do not expose a
        # control that promises simultaneous HA + cloud forwarding. Temporary
        # traffic capture remains a separate, explicit diagnostics action.
        persisted_options[CONF_PROXY_ENABLED] = False
        # CP2A: the runtime options save no longer persists a collector operation
        # mode into options. The mode is a read-only projection of the canonical
        # connection strategy, which this same save commits to entry.data
        # (_async_commit_runtime_options); an options copy would be a guaranteed
        # stale shadow the projection ignores. Endpoint reconcile is driven by
        # the strategy-derived collector_uses_home_assistant_route /
        # collector_callback_listener_required, not by a persisted mode value.
        for key in (
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
        ):
            if key in self._config_entry.options:
                persisted_options[key] = self._config_entry.options[key]
        return persisted_options

    def _runtime_driver_selector(self) -> SelectSelector:
        """Build the driver selector with live detection evidence highlighted."""

        detected_driver = self._config_entry.data.get(CONF_DETECTED_DRIVER, "")
        detected_driver = (
            detected_driver
            if type(detected_driver) is str
            and detected_driver == detected_driver.strip()
            else ""
        )
        candidate_keys = {
            candidate.driver_key
            for candidate in self._runtime_inverter_protocol_candidates()
        }
        options: list[SelectOptionDict] = []
        for key in driver_options():
            label = _selector_option_label(
                self._translation_bundle,
                "driver_hint",
                key,
                _DRIVER_DISPLAY_LABELS.get(
                    key,
                    key.replace("_", " ").title(),
                ),
            )
            if key == detected_driver:
                label = self._tr(
                    "common.dynamic.runtime_driver_current",
                    "{driver} — currently detected",
                    {"driver": label},
                )
            elif key in candidate_keys:
                label = self._tr(
                    "common.dynamic.runtime_driver_candidate",
                    "{driver} — also detected",
                    {"driver": label},
                )
            options.append(SelectOptionDict(value=key, label=label))
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    @_with_translation_bundle
    async def async_step_runtime_poll_interval(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        pending = dict(self._runtime_poll_interval_pending_input)
        if not pending:
            return await self.async_step_runtime()
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            pending[CONF_POLL_INTERVAL] = flat_input.get(
                CONF_POLL_INTERVAL,
                self._config_entry.options.get(
                    CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL
                ),
            )
            return self._async_commit_runtime_options(pending)
        return self.async_show_form(
            step_id="runtime_poll_interval",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_POLL_INTERVAL,
                        default=self._config_entry.options.get(
                            CONF_POLL_INTERVAL,
                            DEFAULT_POLL_INTERVAL,
                        ),
                    ): _poll_interval_selector(self._poll_policy_driver_key()),
                }
            ),
            errors={},
        )

    def _control_summary(self, *, control_mode: str, confidence: str) -> str:
        if control_mode == CONTROL_MODE_FULL:
            return self._tr("common.dynamic.control_full", "All controls are enabled.")
        if control_mode == CONTROL_MODE_READ_ONLY:
            return self._tr(
                "common.dynamic.control_read_only",
                "Monitoring only — no control entities are exposed.",
            )
        if confidence == "high":
            return self._tr(
                "common.dynamic.control_auto",
                "Tested controls are enabled automatically.",
            )
        return self._tr(
            "common.dynamic.control_waiting",
            "Monitoring only until a high-confidence detection is confirmed.",
        )

    def _confidence_label(self, confidence: str) -> str:
        return {
            "high": self._tr("common.dynamic.confidence_high", "High confidence"),
            "medium": self._tr("common.dynamic.confidence_medium", "Medium confidence"),
            "low": self._tr("common.dynamic.confidence_low", "Low confidence"),
            "none": self._tr("common.dynamic.confidence_none", "No confidence"),
        }.get(confidence, confidence)

    def _coordinator(self):
        return getattr(self._config_entry, "runtime_data", None)

    async def _async_refresh_collector_wifi_status(self) -> None:
        coordinator = self._coordinator()
        query = getattr(coordinator, "async_query_collector_parameters", None)
        if not callable(query):
            raise RuntimeError("collector_local_management_not_supported")
        values = await query(
            (
                SET_TARGET_SSID,
                QUERY_NETWORK_DIAGNOSTICS,
                QUERY_WIFI_SCAN_LIST,
            )
        )
        current_ssid = str(values.get(SET_TARGET_SSID) or "")
        network_diagnostics = str(values.get(QUERY_NETWORK_DIAGNOSTICS) or "")
        scan_text = str(values.get(QUERY_WIFI_SCAN_LIST) or "")

        self._collector_wifi_current_ssid = current_ssid
        self._collector_wifi_network_diagnostics = network_diagnostics
        self._collector_wifi_networks = self._parse_collector_wifi_scan_response(
            scan_text
        )

    async def _async_apply_collector_wifi_settings(
        self, *, ssid: str, password: str
    ) -> None:
        coordinator = self._coordinator()
        writer = getattr(coordinator, "async_set_collector_wifi_credentials", None)
        if not callable(writer):
            raise RuntimeError("collector_local_management_not_supported")
        self._collector_wifi_current_ssid = str(
            await writer(
                ssid=ssid,
                password=password,
                ssid_parameter=SET_TARGET_SSID,
                password_parameter=SET_TARGET_PASSWORD,
            )
            or ""
        )

    async def _async_refresh_collector_uart_status(self) -> None:
        coordinator = self._coordinator()
        query = getattr(coordinator, "async_query_collector_parameters", None)
        if not callable(query):
            raise RuntimeError("collector_local_management_not_supported")
        values = await query((QUERY_HARDWARE_VERSION, QUERY_SERIAL_BAUDRATE))
        hardware_version = str(values.get(QUERY_HARDWARE_VERSION) or "")
        current_settings = str(values.get(QUERY_SERIAL_BAUDRATE) or "")

        if not hardware_version:
            hardware_version = self._runtime_collector_hardware_version()
        if not current_settings:
            current_settings = self._runtime_collector_uart_settings()
        self._collector_uart_hardware_version = hardware_version
        self._collector_uart_current_settings = current_settings
        self._collector_uart_current_baudrate = self._normalize_collector_uart_baudrate(
            current_settings
        )

    async def _async_apply_collector_uart_baudrate(self, baudrate: str) -> None:
        if self._collector_uart_runtime_change_unavailable():
            raise RuntimeError("collector_uart_runtime_unavailable")

        baudrate = self._normalize_collector_uart_baudrate(baudrate)
        if baudrate not in COLLECTOR_UART_BAUDRATES:
            raise ValueError(f"unsupported_collector_uart_baudrate:{baudrate}")

        coordinator = self._coordinator()
        writer = getattr(coordinator, "async_set_collector_uart_baudrate", None)
        if not callable(writer):
            raise RuntimeError("collector_local_management_not_supported")
        await writer(baudrate)
        invalidator = getattr(coordinator, "invalidate_collector_runtime_values", None)
        if callable(invalidator):
            invalidator()
        refresh = getattr(coordinator, "async_request_refresh", None)
        if callable(refresh):
            await refresh()

    @staticmethod
    def _collector_query_response_text(response) -> str:
        text = str(response.text or "").strip().strip("\x00")
        if text and all(
            character.isprintable() or character in "\r\n\t" for character in text
        ):
            return text
        raw = bytes(getattr(response, "data", b"") or b"").rstrip(b"\x00")
        return raw.hex() if raw else text

    @staticmethod
    def _parse_collector_wifi_scan_response(
        scan_text: str,
    ) -> tuple[SmartEssBleWifiNetwork, ...]:
        text = str(scan_text or "").strip()
        if text.startswith("["):
            text = f"49,{text}"
        return parse_wifi_scan_response(text)

    def _collector_wifi_placeholders(self) -> dict[str, str]:
        return {
            "collector_ip": str(
                self._config_entry.options.get(
                    CONF_COLLECTOR_IP,
                    self._config_entry.data.get(CONF_COLLECTOR_IP, ""),
                )
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "current_ssid": self._collector_wifi_current_ssid
            or self._tr("common.dynamic.not_available", "Not available"),
            "status_updates": self._collector_wifi_status_updates(),
        }

    def _collector_wifi_status_updates(self) -> str:
        lines: list[str] = []
        if self._collector_wifi_last_result:
            lines.append(
                self._tr(
                    "common.dynamic.collector_wifi_last_action_line",
                    "**Last action:** {value}",
                    {"value": self._collector_wifi_last_result},
                )
            )
        if self._collector_wifi_last_error:
            lines.append(
                self._tr(
                    "common.dynamic.collector_wifi_last_error_line",
                    "**Last error:** {value}",
                    {"value": self._collector_wifi_last_error},
                )
            )
        if not lines:
            return ""
        return "\n\n" + "\n".join(lines)

    def _collector_wifi_refresh_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_wifi_action_refresh",
            "Refresh Wi-Fi list and status",
        )

    def _collector_wifi_apply_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_wifi_action_apply",
            "Apply Wi-Fi settings to the current collector",
        )

    def _runtime_collector_uart_settings(self) -> str:
        coordinator = self._coordinator()
        data = getattr(coordinator, "data", None)
        values = getattr(data, "values", None)
        if isinstance(values, dict):
            return str(values.get("collector_serial_baudrate") or "")
        return ""

    def _runtime_collector_hardware_version(self) -> str:
        coordinator = self._coordinator()
        data = getattr(coordinator, "data", None)
        values = getattr(data, "values", None)
        if isinstance(values, dict):
            return str(values.get("collector_hardware_version") or "")
        return ""

    def _collector_uart_runtime_change_unavailable(self) -> bool:
        capabilities = self._collector_capabilities()
        if capabilities.uart_management:
            return not capabilities.uart_runtime_speed_change
        hardware = (
            self._collector_uart_hardware_version
            or self._runtime_collector_hardware_version()
        ).lower()
        return any(
            marker in hardware for marker in ("bk72", "bk723", "rtl87", "libretiny")
        )

    @staticmethod
    def _normalize_collector_uart_baudrate(value: object) -> str:
        text = str(value or "").strip().strip("\x00")
        if not text:
            return ""
        baudrate = text.split(",", 1)[0].strip()
        return baudrate if baudrate in COLLECTOR_UART_BAUDRATES else ""

    def _collector_uart_placeholders(self) -> dict[str, str]:
        raw_settings = (
            self._collector_uart_current_settings
            or self._runtime_collector_uart_settings()
        )
        current_uart = raw_settings or self._collector_uart_current_baudrate
        hardware_version = (
            self._collector_uart_hardware_version
            or self._runtime_collector_hardware_version()
        )
        return {
            "collector_ip": str(
                self._config_entry.options.get(
                    CONF_COLLECTOR_IP,
                    self._config_entry.data.get(CONF_COLLECTOR_IP, ""),
                )
                or self._tr("common.dynamic.not_available", "Not available")
            ),
            "current_uart": current_uart
            or self._tr("common.dynamic.not_available", "Not available"),
            "hardware_version": hardware_version
            or self._tr("common.dynamic.not_available", "Not available"),
            "runtime_unavailable_note": self._collector_uart_runtime_unavailable_note(),
            "status_updates": self._collector_uart_status_updates(),
        }

    def _collector_uart_status_updates(self) -> str:
        lines: list[str] = []
        if self._collector_uart_last_result:
            lines.append(
                self._tr(
                    "common.dynamic.collector_uart_last_action_line",
                    "**Last action:** {value}",
                    {"value": self._collector_uart_last_result},
                )
            )
        if self._collector_uart_last_error:
            lines.append(
                self._tr(
                    "common.dynamic.collector_uart_last_error_line",
                    "**Last error:** {value}",
                    {"value": self._collector_uart_last_error},
                )
            )
        if not lines:
            return ""
        return "\n\n" + "\n".join(lines)

    def _collector_uart_refresh_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_uart_action_refresh",
            "Refresh UART status",
        )

    def _collector_uart_apply_action_label(self) -> str:
        return self._tr(
            "common.dynamic.collector_uart_action_apply",
            "Apply UART speed to the current collector",
        )

    def _collector_uart_runtime_unavailable_note(self) -> str:
        if not self._collector_uart_runtime_change_unavailable():
            return ""
        return self._tr(
            "common.dynamic.collector_uart_runtime_unavailable_note",
            "\n\nThis collector reports BK72xx/LibreTiny hardware. Runtime UART speed switching is not available on this platform. Change `baud_rate:` in the ESPHome YAML and reflash the collector.",
        )
