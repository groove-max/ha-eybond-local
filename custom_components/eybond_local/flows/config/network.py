"""Network defaults, connection forms, and scan-progress presentation."""

from __future__ import annotations

import asyncio
import time
from typing import Any, MutableMapping

from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
)

from ... import network_interfaces
from .common import (
    _AUTO_SCAN_TIMEOUT,
    _compute_broadcast_24,
)
from ...connection.branch_registry import (
    get_connection_branch,
    supported_connection_types,
)
from ...connection.ui import ConnectionFormField
from ..common.connection_form import (
    IP_TEXT_SELECTOR as _IP_TEXT_SELECTOR,
)
from ..common.connection_form import (
    build_connection_fields_schema as _build_shared_connection_fields_schema,
)
from ..common.connection_form import (
    interface_selector as _interface_selector,
)
from ..common.connection_form import (
    selector_for_connection_field as _shared_connection_field_selector,
)
from ..common.connection_form import (
    validate_connection_inputs as _validate_shared_connection_inputs,
)
from ...const import (
    CONF_COLLECTOR_IP,
    CONF_CONNECTION_TYPE,
    CONF_DISCOVERY_TARGET,
    CONF_SERVER_IP,
    CONNECTION_TYPE_EYBOND,
    DEFAULT_DISCOVERY_TARGET,
    DRIVER_HINT_AUTO,
)
from ..common.presentation import (
    MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
    MANUAL_CONFIRM_ACTION_ENABLE_DISCOVERY,
    MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
    MANUAL_CONFIRM_ACTION_SAVE,
    _collector_network_status_selector,
    _flatten_sections,
)
from ...models import (
    OnboardingResult,
)
from ...onboarding.detection import DiscoveryTarget

_SCAN_PROGRESS_BAR_WIDTH = 12


class ConfigNetworkFlowMixin:
    """Network defaults, connection forms, and scan-progress presentation."""

    async def _async_ensure_network_defaults(self) -> None:
        if not self._local_ip or not self._interface_options:
            self._interface_options = await self.hass.async_add_executor_job(
                network_interfaces.get_ipv4_interfaces
            )
            detected_local_ip = await self.hass.async_add_executor_job(
                network_interfaces.get_local_ip
            )

            if self._interface_options:
                preferred = next(
                    (
                        item["ip"]
                        for item in self._interface_options
                        if item["ip"] == detected_local_ip
                    ),
                    self._interface_options[0]["ip"],
                )
                self._local_ip = preferred
            elif detected_local_ip:
                self._local_ip = detected_local_ip

        if self._local_ip:
            self._default_broadcast = self._selected_interface_broadcast(self._local_ip)

        if not isinstance(self._auto_config, dict):
            self._auto_config = {}

        interface_ips = {
            str(item.get("ip") or "").strip()
            for item in self._interface_options
            if str(item.get("ip") or "").strip()
        }
        configured_server_ip = str(
            self._auto_config.get(CONF_SERVER_IP, "") or ""
        ).strip()
        if self._local_ip and (
            not configured_server_ip or configured_server_ip not in interface_ips
        ):
            self._auto_config[CONF_SERVER_IP] = self._local_ip

    def _hass_bluetooth_device_from_address(self, address: str) -> object | None:
        bluetooth = self._home_assistant_bluetooth_module()
        if bluetooth is None:
            return None

        resolve_device = getattr(bluetooth, "async_ble_device_from_address", None)
        if not callable(resolve_device):
            return None

        normalized_address = str(address or "").strip()
        if not normalized_address:
            return None

        try:
            return resolve_device(self.hass, normalized_address, connectable=True)
        except TypeError:
            try:
                return resolve_device(self.hass, normalized_address)
            except Exception:
                return None
        except Exception:
            return None

    def _build_manual_defaults(
        self,
        user_input: dict[str, Any] | None,
        result: OnboardingResult | None,
    ) -> dict[str, Any]:
        collector_ip = ""
        driver_hint = DRIVER_HINT_AUTO
        if result is not None and result.collector is not None:
            collector_ip = result.collector.ip
        scan_route_address = getattr(self, "_manual_scan_route_address", "")
        if scan_route_address:
            collector_ip = scan_route_address
        if result is not None and result.match is not None:
            driver_hint = result.match.driver_key
        defaults = self._connection_branch().build_manual_base_values(
            server_ip=str(
                self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip
            ),
            default_broadcast=self._selected_interface_broadcast(),
            stored_defaults=self._manual_defaults,
            collector_ip=collector_ip,
            driver_hint=driver_hint,
        )
        if self._auto_config:
            defaults[CONF_SERVER_IP] = self._auto_config.get(
                CONF_SERVER_IP, defaults[CONF_SERVER_IP]
            )
        if user_input is not None:
            flat = _flatten_sections(user_input)
            self._normalize_current_server_ip(flat)
            defaults.update(flat)
        self._manual_defaults = defaults
        return defaults

    def _normalize_current_server_ip(self, values: MutableMapping[str, Any]) -> None:
        if not self._local_ip:
            return
        interface_ips = {
            str(item.get("ip") or "").strip()
            for item in self._interface_options
            if str(item.get("ip") or "").strip()
        }
        if not interface_ips:
            return
        configured_server_ip = str(values.get(CONF_SERVER_IP, "") or "").strip()
        if configured_server_ip and configured_server_ip in interface_ips:
            return
        values[CONF_SERVER_IP] = self._local_ip

    def _server_ip_field(self) -> SelectSelector | TextSelector:
        """Return the most user-friendly selector for choosing the local server IP."""

        if not self._interface_options:
            return _IP_TEXT_SELECTOR
        return _interface_selector(self._interface_options)

    def _connection_type_selector(self) -> SelectSelector:
        """Return a selector for supported connection branches."""

        options = [
            SelectOptionDict(
                value=connection_type,
                label=get_connection_branch(connection_type).display.integration_name,
            )
            for connection_type in supported_connection_types()
        ]
        return SelectSelector(
            SelectSelectorConfig(
                options=options,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )

    def _collector_network_status_selector(self) -> SelectSelector:
        return _collector_network_status_selector(
            self._tr(
                "common.dynamic.collector_network_already_connected",
                "Yes, the collector is already on this network",
            ),
            self._tr(
                "common.dynamic.collector_network_needs_bluetooth",
                "No, connect the collector to Wi-Fi using Bluetooth first (test mode, only for collectors with Bluetooth support)",
            ),
        )

    def _current_connection_type(self) -> str:
        """Return the active connection type for the current setup branch."""

        if self._selected_result is not None and self._selected_result.connection_type:
            return self._selected_result.connection_type
        if self._manual_result is not None and self._manual_result.connection_type:
            return self._manual_result.connection_type
        return str(
            self._auto_config.get(CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND)
            or CONNECTION_TYPE_EYBOND
        )

    def _connection_branch(self):
        """Return branch metadata for the active connection type."""

        return get_connection_branch(self._current_connection_type())

    def _connection_display(self):
        """Return branch-aware display metadata for the active connection type."""

        return self._connection_branch().display

    def _selected_interface_option(
        self, server_ip: str | None = None
    ) -> dict[str, str] | None:
        selected_ip = str(
            server_ip
            or self._auto_config.get(CONF_SERVER_IP, self._local_ip)
            or self._local_ip
        )
        return next(
            (item for item in self._interface_options if item.get("ip") == selected_ip),
            None,
        )

    def _selected_interface_label(self, server_ip: str | None = None) -> str:
        interface = self._selected_interface_option(server_ip)
        if interface is not None:
            return (
                interface.get("label")
                or interface.get("ip")
                or self._tr("common.dynamic.unknown", "Unknown")
            )
        selected_ip = str(
            server_ip
            or self._auto_config.get(CONF_SERVER_IP, self._local_ip)
            or self._local_ip
        )
        return selected_ip or self._tr("common.dynamic.unknown", "Unknown")

    def _selected_interface_broadcast(self, server_ip: str | None = None) -> str:
        interface = self._selected_interface_option(server_ip)
        broadcast = str(
            interface.get("broadcast", "") if interface is not None else ""
        ).strip()
        if broadcast:
            return broadcast
        selected_ip = str(
            server_ip
            or self._auto_config.get(CONF_SERVER_IP, self._local_ip)
            or self._local_ip
        )
        if selected_ip:
            return _compute_broadcast_24(selected_ip)
        return DEFAULT_DISCOVERY_TARGET

    def _scan_discovery_targets(self) -> tuple[DiscoveryTarget, ...]:
        selected_broadcast = self._selected_interface_broadcast()
        addresses = (
            [selected_broadcast] if selected_broadcast else [DEFAULT_DISCOVERY_TARGET]
        )
        return tuple(
            DiscoveryTarget(ip=address, source="broadcast")
            for address in addresses
            if address
        )

    def _auto_connection_defaults(self) -> dict[str, Any]:
        """Return branch-aware defaults for the auto-scan flow."""

        server_ip = str(
            self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip
        )
        defaults = self._connection_branch().build_auto_values(
            server_ip=server_ip,
            default_broadcast=self._selected_interface_broadcast(server_ip)
            if server_ip
            else self._default_broadcast,
        )
        defaults.update(self._auto_config)
        return defaults

    def _refresh_scan_action_label(self) -> str:
        """Return the label for repeating the single collector search."""

        return self._scan_action_label("refresh_scan", "Refresh scan results")

    def _scan_action_label(self, action: str, default: str) -> str:
        # change_scan_interface / manual live under the advanced_setup submenu;
        # resolve their labels from either step.
        label = self._tr(f"config.step.scan_results.menu_options.{action}", "")
        if not label:
            label = self._tr(f"config.step.advanced_setup.menu_options.{action}", "")
        return label or default

    def _manual_confirm_action_label(self, action: str, default: str) -> str:
        return self._tr(
            f"config.step.manual_confirm.menu_options.{action}",
            default,
        )

    async def _async_update_scan_progress_loop(self) -> None:
        """Periodically publish determinate progress updates while one scan runs."""

        while True:
            started = self._scan_started_monotonic
            now = time.monotonic()
            elapsed_seconds = max(0.0, now - started) if started is not None else 0.0
            self.async_update_progress(self._scan_progress_fraction(elapsed_seconds))
            await asyncio.sleep(0.35)

    def _scan_progress_fraction(self, elapsed_seconds: float) -> float:
        scan_timeout = (
            self._scan_timeout_seconds
            if self._scan_timeout_seconds > 0
            else _AUTO_SCAN_TIMEOUT
        )
        bounded_elapsed = min(max(elapsed_seconds, 0.0), scan_timeout)
        time_fraction = bounded_elapsed / scan_timeout if scan_timeout > 0 else 0.0
        if self._scan_progress_stage == "preparing":
            return 0.0
        if self._scan_progress_stage == "discovering":
            return min(0.82, 0.02 + (time_fraction * 0.8))
        if self._scan_progress_stage == "analyzing":
            return 0.9
        if self._scan_progress_stage == "finalizing":
            return 0.97
        return min(0.82, 0.02 + (time_fraction * 0.8))

    def _scan_progress_placeholders(self, selected_label: str) -> dict[str, str]:
        now = time.monotonic()
        started = (
            self._scan_started_monotonic
            if self._scan_started_monotonic is not None
            else now
        )
        elapsed_seconds_float = max(0.0, now - started)
        scan_timeout = (
            self._scan_timeout_seconds
            if self._scan_timeout_seconds > 0
            else _AUTO_SCAN_TIMEOUT
        )
        bounded_elapsed = min(elapsed_seconds_float, scan_timeout)
        elapsed_seconds = int(round(bounded_elapsed))
        progress_fraction = self._scan_progress_fraction(elapsed_seconds_float)
        percent = max(0, min(99, int(round(progress_fraction * 100))))
        filled = max(
            0,
            min(
                _SCAN_PROGRESS_BAR_WIDTH,
                int(round(progress_fraction * _SCAN_PROGRESS_BAR_WIDTH)),
            ),
        )
        progress_bar = (
            "["
            + ("#" * filled)
            + ("-" * (_SCAN_PROGRESS_BAR_WIDTH - filled))
            + f"] {percent}%"
        )
        stage_label = self._tr(
            f"common.dynamic.scan_progress_stage_{self._scan_progress_stage}",
            "Preparing scan",
        )
        return {
            "selected_scan_interface": selected_label,
            "scan_progress_phase": stage_label,
            "scan_progress_bar": progress_bar,
            "scan_progress_detail": self._tr(
                "common.dynamic.scan_progress_detail",
                "{elapsed_seconds}s elapsed.",
                {
                    "elapsed_seconds": elapsed_seconds,
                },
            ),
            "scan_progress_hint": self._tr(
                "common.dynamic.scan_progress_hint",
                "Home Assistant is looking for collectors on the selected local network.",
            ),
        }

    def _peer_label(self) -> str:
        return self._tr(
            "common.dynamic.peer_label",
            self._connection_display().peer_label,
        )

    def _unconfirmed_inverter_label(self) -> str:
        return self._tr(
            "common.dynamic.unconfirmed_inverter",
            self._connection_display().unconfirmed_inverter_label,
        )

    def _selector_for_connection_field(self, field: ConnectionFormField):
        """Resolve the concrete HA selector for one branch-aware connection field."""

        return _shared_connection_field_selector(
            field,
            server_ip_selector=self._server_ip_field(),
            translation_bundle=self._translation_bundle,
        )

    def _build_connection_fields_schema(
        self,
        connection_type: str,
        *,
        fields: tuple[ConnectionFormField, ...],
        values: dict[str, Any],
    ) -> dict[Any, Any]:
        """Build a voluptuous schema mapping for branch-aware connection fields."""

        return _build_shared_connection_fields_schema(
            connection_type,
            fields=fields,
            values=values,
            server_ip_selector=self._server_ip_field(),
            translation_bundle=self._translation_bundle,
        )

    def _collector_network_placeholders(self) -> dict[str, str]:
        return {
            "selected_scan_interface": self._selected_interface_label(),
            "peer_label": self._peer_label(),
        }

    def _collector_connection_placeholders(self) -> dict[str, str]:
        if self._selected_result is None:
            return {}
        placeholders = self._result_placeholders(self._selected_result)
        placeholders.update(
            {
                "collector_callback_target_endpoint": self._collector_callback_target_endpoint(),
            }
        )
        return placeholders

    def _auto_description_placeholders(self, single_interface: bool) -> dict[str, str]:
        if single_interface and self._interface_options:
            item = self._interface_options[0]
            return {
                "interface_hint": self._tr(
                    "common.dynamic.auto_interface_hint_single",
                    "Home Assistant will use **{selected_interface}** automatically.",
                    {"selected_interface": item["label"]},
                ),
            }
        return {
            "interface_hint": self._tr(
                "common.dynamic.auto_interface_hint_multi",
                "Choose which Home Assistant interface the {peer_label} should connect back to.",
                {"peer_label": self._peer_label()},
            ),
        }

    def _welcome_description_placeholders(self) -> dict[str, str]:
        display = self._connection_display()
        if len(self._interface_options) > 1:
            return {
                "welcome_hint": self._tr(
                    "common.dynamic.welcome_connection_type_multi",
                    "Choose the connection type first. The wizard will then continue with collector network setup and the next onboarding steps.",
                    {
                        "integration_name": display.integration_name,
                    },
                ),
            }
        return {
            "welcome_hint": self._tr(
                "common.dynamic.welcome_connection_type_single",
                "Choose the connection type first. The wizard will then continue with collector network setup and the next onboarding steps.",
                {
                    "integration_name": display.integration_name,
                },
            ),
        }

    def _manual_confirm_placeholders(
        self,
        manual_config: dict[str, Any],
        result: OnboardingResult | None,
    ) -> dict[str, str]:
        collector_ip = ""
        collector_pn = ""
        smartess_collector_version = ""
        smartess_protocol_asset_id = ""
        model_name = self._unconfirmed_inverter_label()
        serial_number = self._tr(
            "common.dynamic.not_available_yet", "Not available yet"
        )

        if result is not None and result.collector is not None:
            collector_ip = result.collector.ip
            collector = result.collector.collector
            if collector is not None:
                smartess_collector_version = collector.smartess_collector_version or ""
                smartess_protocol_asset_id = collector.smartess_protocol_asset_id or ""
        collector_pn = self._collector_pn_for_result(result)
        if not collector_ip:
            collector_ip = manual_config.get(CONF_COLLECTOR_IP) or manual_config.get(
                CONF_DISCOVERY_TARGET, ""
            )

        smartess_hint_available = bool(
            smartess_collector_version or smartess_protocol_asset_id
        )

        if result is not None and result.match is not None:
            model_name = result.match.model_name
            serial_number = result.match.serial_number or serial_number

        callback_failure = str(
            getattr(result, "last_error", "") if result is not None else ""
        ).strip()
        callback_failure_summaries = {
            "callback_timeout": (
                "common.dynamic.manual_probe_callback_timeout",
                "The collector did not call back during this attempt.",
            ),
            "callback_identity_mismatch": (
                "common.dynamic.manual_probe_callback_identity_mismatch",
                "A callback connection appeared, but its collector identity did not match this attempt.",
            ),
            "callback_identity_conflict": (
                "common.dynamic.manual_probe_callback_identity_conflict",
                "The collector identity is already owned by another entry or setup flow.",
            ),
            "callback_identity_ambiguous": (
                "common.dynamic.manual_probe_callback_identity_ambiguous",
                "More than one collector answered during this attempt, so none was selected.",
            ),
            "callback_trigger_interference": (
                "common.dynamic.manual_probe_callback_trigger_interference",
                "Another callback request overlapped this attempt, so its answer could not be attributed safely.",
            ),
            # HONEST taxonomy: the TCP session DID arrive -- calling this
            # "did not call back" sends users debugging the wrong layer.
            "callback_session_silent": (
                "common.dynamic.manual_probe_callback_session_silent",
                "The collector CONNECTED to Home Assistant but stayed silent: it sent no identity on its own. Choose its protocol below so one read-only identity query can be sent.",
            ),
            "onboarding_wire_probe_failed": (
                "common.dynamic.manual_probe_onboarding_wire_probe_failed",
                "The silent collector did not answer the identity query on the selected protocol. It may use the other protocol -- this is never guessed automatically.",
            ),
            "callback_silent_session_unavailable": (
                "common.dynamic.manual_probe_callback_silent_session_unavailable",
                "The silent connection this attempt was bound to has closed. Run a new attempt to let the collector connect again.",
            ),
            "inbound_awaiting_session": (
                "common.dynamic.manual_inbound_awaiting_session",
                "The collector has not connected to Home Assistant yet.",
            ),
        }

        if callback_failure in callback_failure_summaries:
            key, fallback = callback_failure_summaries[callback_failure]
            probe_summary = self._tr(key, fallback)
        elif result is not None and result.match is not None:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_confirmed",
                "{peer_label_capitalized} and inverter were confirmed with the manual settings.",
                {"peer_label_capitalized": self._peer_label().capitalize()},
            )
        elif (
            result is not None
            and result.collector is not None
            and result.collector.connected
            and smartess_hint_available
        ):
            probe_summary = self._tr(
                "common.dynamic.manual_probe_smartess_hint",
                "The {peer_label} responded and exposed SmartESS metadata, but the local inverter model is still unconfirmed.",
                {"peer_label": self._peer_label()},
            )
        elif (
            result is not None
            and result.collector is not None
            and result.collector.connected
        ):
            probe_summary = self._tr(
                "common.dynamic.manual_probe_unconfirmed_model",
                "The {peer_label} responded, but the inverter model is still unconfirmed.",
                {"peer_label": self._peer_label()},
            )
        else:
            probe_summary = self._tr(
                "common.dynamic.manual_probe_none",
                "No {peer_label} or inverter was confirmed yet.",
                {"peer_label": self._peer_label()},
            )

        if self._manual_entry_ready_to_save():
            control_summary = self._tr(
                "common.dynamic.manual_ready_to_save_summary",
                "The collector identity is confirmed. You can add it now; inverter detection continues in runtime.",
            )
            next_actions_hint = self._tr(
                "common.dynamic.manual_ready_to_save_actions",
                "Choose **{save_action_label}** to add the device, **{probe_again_action_label}** to verify again, or **{edit_settings_action_label}** to change the values.",
                {
                    "save_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_SAVE,
                        "Add device",
                    ),
                    "probe_again_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
                        "Probe again",
                    ),
                    "edit_settings_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
                        "Edit settings",
                    ),
                },
            )
        elif callback_failure == "inbound_awaiting_session":
            control_summary = self._tr(
                "common.dynamic.manual_inbound_waiting_summary",
                "No device was added. Connect the collector to Home Assistant and try again, or enable background discovery so it appears automatically when its identity is available.",
            )
            next_actions_hint = self._tr(
                "common.dynamic.manual_inbound_waiting_actions",
                "Choose **{probe_again_action_label}** to check again, **{discovery_action_label}** to wait through discovery, or **{edit_settings_action_label}** to change the values.",
                {
                    "probe_again_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
                        "Check again",
                    ),
                    "discovery_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_ENABLE_DISCOVERY,
                        "Enable background discovery",
                    ),
                    "edit_settings_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
                        "Edit settings",
                    ),
                },
            )
        else:
            control_summary = self._tr(
                "common.dynamic.manual_verification_required_summary",
                "No device was added because the collector identity or recovery path was not confirmed.",
            )
            next_actions_hint = self._tr(
                "common.dynamic.manual_verification_required_actions",
                "Choose **{probe_again_action_label}** to try again or **{edit_settings_action_label}** to change the address or connection settings.",
                {
                    "probe_again_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
                        "Probe again",
                    ),
                    "edit_settings_action_label": self._manual_confirm_action_label(
                        MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
                        "Edit settings",
                    ),
                },
            )

        return {
            "probe_summary": probe_summary,
            "collector_ip": collector_ip
            or self._tr("common.dynamic.unknown", "Unknown"),
            "collector_pn": collector_pn
            or self._tr("common.dynamic.unknown", "Unknown"),
            "model_name": model_name,
            "serial_number": serial_number,
            "smartess_cloud_summary": self._smartess_cloud_summary(result),
            "control_summary": control_summary,
            "next_actions_hint": next_actions_hint,
        }

    @staticmethod
    def _validate_connection_inputs(
        user_input: dict[str, Any],
        *,
        fields: tuple[ConnectionFormField, ...],
    ) -> dict[str, str]:
        return _validate_shared_connection_inputs(user_input, fields=fields)
