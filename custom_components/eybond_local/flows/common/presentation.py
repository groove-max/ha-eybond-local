"""Presentation helpers shared by EyeBond config and options flows."""

from __future__ import annotations

from typing import (
    Any,
)

import voluptuous as vol
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from ...collector.smartess_ble import (
    SmartEssBleWifiNetwork,
)
from .connection_form import (
    IP_TEXT_SELECTOR as _IP_TEXT_SELECTOR,
)
from ...const import (
    CONF_ADVERTISED_TCP_PORT,
    CONF_CONTROL_MODE,
    CONF_DETECTED_DRIVER,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_DEVICE_CATALOG_ENTRY,
    CONF_DEVICE_CATALOG_KIND,
    CONF_DEVICE_CATALOG_TIER,
    CONF_DISCOVERY_INTERVAL,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    CONTROL_MODE_READ_ONLY,
    DRIVER_DETECTION_FIRST_MATCH,
    DRIVER_DETECTION_FULL_SCAN,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from ...drivers.registry import (
    poll_policy_for_driver_key,
)
from .translation import (
    selector_option_label as _selector_option_label,
)

CONF_WIFI_SSID = "wifi_ssid"


CONF_WIFI_PASSWORD = "wifi_password"


MANUAL_CONFIRM_ACTION_PROBE_AGAIN = "manual_probe_again"

MANUAL_CONFIRM_ACTION_EDIT_SETTINGS = "manual_edit_settings"

MANUAL_CONFIRM_ACTION_SAVE = "manual_save"

MANUAL_CONFIRM_ACTION_ENABLE_DISCOVERY = "manual_enable_background_discovery"


COLLECTOR_NETWORK_ALREADY_CONNECTED = "already_connected"

COLLECTOR_NETWORK_NEEDS_BLUETOOTH = "needs_bluetooth"


def _collector_network_status_selector(
    already_connected_label: str,
    needs_bluetooth_label: str,
) -> SelectSelector:
    """Return a selector for choosing the collector network onboarding path."""

    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(
                    value=COLLECTOR_NETWORK_ALREADY_CONNECTED,
                    label=already_connected_label,
                ),
                SelectOptionDict(
                    value=COLLECTOR_NETWORK_NEEDS_BLUETOOTH,
                    label=needs_bluetooth_label,
                ),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


_INT_FIELDS = {
    CONF_ADVERTISED_TCP_PORT,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONF_DISCOVERY_INTERVAL,
    CONF_HEARTBEAT_INTERVAL,
    CONF_POLL_INTERVAL,
}


def _exception_detail(exc: BaseException) -> str:
    return str(exc) or type(exc).__name__


_PRE_ENTRY_INVERTER_METADATA_KEYS = frozenset(
    {
        CONF_DEVICE_CATALOG_KIND,
        CONF_DEVICE_CATALOG_TIER,
        CONF_DEVICE_CATALOG_ENTRY,
        "detected_probe_route",
        "detection_depth",
        "detection_status",
        "detection_budget_exhausted",
        "detection_candidate_drivers",
        "detection_probe_log",
    }
)


def _clear_runtime_inverter_facts(data: dict[str, Any]) -> None:
    """Clear persisted runtime facts before a new owned-session detection."""

    data[CONF_DETECTED_DRIVER] = ""
    data[CONF_DETECTION_CONFIDENCE] = "none"
    data[CONF_DETECTED_MODEL] = ""
    data[CONF_DETECTED_SERIAL] = ""
    data[CONF_CONTROL_MODE] = CONTROL_MODE_READ_ONLY
    for key in _PRE_ENTRY_INVERTER_METADATA_KEYS:
        data.pop(key, None)


def _poll_interval_selector(
    driver_key: object, inverter: object = None
) -> NumberSelector:
    """Build the manual interval selector from the driver's polling policy.

    ``inverter`` is the detected model identity (a ``DriverMatch`` during
    onboarding) forwarded to the driver so a catalog driver can pick a
    model-specific policy; ``None`` when identity is not yet known.
    """

    policy = poll_policy_for_driver_key(driver_key, inverter=inverter)
    return NumberSelector(
        NumberSelectorConfig(
            min=policy.min_manual_interval,
            max=3600,
            step=1,
            unit_of_measurement="s",
            mode=NumberSelectorMode.BOX,
        )
    )


_PASSWORD_TEXT_SELECTOR = TextSelector(TextSelectorConfig(type="password"))


def _smartess_credential_schema_fields(
    *,
    required: bool = True,
    username_default: str = "",
    password_default: str = "",
) -> dict:
    """Return one shared SmartESS-credential schema fragment for cloud-assist forms.

    Centralizes the username + password fields used in the cloud-assist step,
    the standalone evidence-export form, and the create-support-package form so
    selector wiring stays consistent across the three call sites.
    """

    marker = vol.Required if required else vol.Optional
    return {
        marker("username", default=username_default): _IP_TEXT_SELECTOR,
        marker("password", default=password_default): _PASSWORD_TEXT_SELECTOR,
    }


def _poll_mode_selector(bundle: dict[str, Any] | None = None) -> SelectSelector:
    labels = {POLL_MODE_AUTO: "Automatic", POLL_MODE_MANUAL: "Manual"}
    options = [
        SelectOptionDict(
            value=opt,
            label=_selector_option_label(
                bundle, "poll_mode", opt, labels.get(opt, opt)
            ),
        )
        for opt in (POLL_MODE_AUTO, POLL_MODE_MANUAL)
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _driver_detection_strategy_selector(
    bundle: dict[str, Any] | None = None,
) -> SelectSelector:
    labels = {
        DRIVER_DETECTION_FIRST_MATCH: "Stop after the first confirmed protocol",
        DRIVER_DETECTION_FULL_SCAN: "Check all supported protocols",
    }
    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(
                    value=value,
                    label=_selector_option_label(
                        bundle,
                        "driver_detection_strategy",
                        value,
                        labels[value],
                    ),
                )
                for value in (
                    DRIVER_DETECTION_FIRST_MATCH,
                    DRIVER_DETECTION_FULL_SCAN,
                )
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _connection_strategy_selector(
    inbound_label: str,
    callback_on_demand_label: str,
) -> SelectSelector:
    """Return the primary connection-strategy selector (how HA gets the session).

    - ``inbound``: the collector dials Home Assistant on its own; HA sends no UDP
      callback trigger.
    - ``callback_on_demand``: HA asks the collector to connect (a single UDP
      trigger per connect attempt).

    This replaces the old "Cloud + Home Assistant / Home Assistant only"
    (Cloud+HA / HA-only) wording as the main user-facing choice.
    """

    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(
                    value=CONNECTION_STRATEGY_INBOUND, label=inbound_label
                ),
                SelectOptionDict(
                    value=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                    label=callback_on_demand_label,
                ),
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _ble_wifi_network_label(network: SmartEssBleWifiNetwork) -> str:
    signal_label = (
        f"{network.signal}%" if 0 <= network.signal <= 100 else f"{network.signal} dBm"
    )
    return f"{network.ssid} ({signal_label})"


def _ble_wifi_selector(networks: tuple[SmartEssBleWifiNetwork, ...]) -> SelectSelector:
    seen_ssids: set[str] = set()
    options: list[SelectOptionDict] = []
    for network in networks:
        ssid = str(network.ssid or "").strip()
        if not ssid or ssid in seen_ssids:
            continue
        seen_ssids.add(ssid)
        options.append(
            SelectOptionDict(value=ssid, label=_ble_wifi_network_label(network))
        )
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            custom_value=True,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def _flatten_sections(user_input: dict[str, Any]) -> dict[str, Any]:
    """Flatten section-nested user input into a flat dict."""

    flat: dict[str, Any] = {}
    for key, value in user_input.items():
        if isinstance(value, dict):
            flat.update(value)
        else:
            flat[key] = value
    for key in _INT_FIELDS:
        value = flat.get(key)
        if isinstance(value, (int, float)):
            flat[key] = int(value)
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                flat[key] = int(stripped)
    return flat


_RECOVERY_FAILURE_EXPLANATIONS = {
    # The selected-route admission screen owns both the callback-identity phase
    # and the recovery phase.  Identity failures must remain honest here instead
    # of falling through to the generic recovery message.
    "callback_timeout": (
        "common.dynamic.manual_probe_callback_timeout",
        "The collector did not connect during this attempt.",
    ),
    "callback_identity_mismatch": (
        "common.dynamic.manual_probe_callback_identity_mismatch",
        "A collector connected, but its identity did not match this attempt.",
    ),
    "callback_identity_conflict": (
        "common.dynamic.manual_probe_callback_identity_conflict",
        "This collector is already owned by another entry or setup flow.",
    ),
    "callback_identity_ambiguous": (
        "common.dynamic.manual_probe_callback_identity_ambiguous",
        "More than one collector answered, so none was selected.",
    ),
    "callback_trigger_not_sent": (
        "common.dynamic.manual_probe_callback_trigger_not_sent",
        "The connection request could not be sent.",
    ),
    "callback_trigger_interference": (
        "common.dynamic.manual_probe_callback_trigger_interference",
        "Another connection request overlapped this attempt.",
    ),
    "callback_identity_unverified": (
        "common.dynamic.manual_probe_callback_identity_unverified",
        "The collector connected but did not report a verifiable identity.",
    ),
    "callback_session_silent": (
        "common.dynamic.manual_probe_callback_session_silent",
        "The collector connected but did not identify itself.",
    ),
    "onboarding_wire_probe_failed": (
        "common.dynamic.manual_probe_onboarding_wire_probe_failed",
        "The collector did not answer the identity query.",
    ),
    "callback_silent_session_unavailable": (
        "common.dynamic.manual_probe_callback_silent_session_unavailable",
        "The previously observed connection has closed.",
    ),
    "recovery_silent_session_ambiguous": (
        "common.dynamic.recovery_fail_silent_ambiguous",
        "More than one collector connected silently at the same time, "
        "so none could be identified. Make sure only the collector you "
        "are adding can reach Home Assistant, then try again.",
    ),
    "recovery_identity_mismatch": (
        "common.dynamic.recovery_fail_identity_mismatch",
        "A different collector answered on the reconnected connection. "
        "Check that the address reaches the collector you are adding, "
        "then try again.",
    ),
    "recovery_silent_probe_failed": (
        "common.dynamic.recovery_fail_silent_probe_failed",
        "The collector reconnected but did not answer the identity "
        "query. It may use the other protocol, or it may need another "
        "attempt.",
    ),
    "recovery_silent_probe_unavailable": (
        "common.dynamic.recovery_fail_silent_probe_unavailable",
        "Home Assistant could not open the connection needed to "
        "identify the reconnected collector. Check the listener "
        "address/port, then try again.",
    ),
    "callback_recovery_timeout": (
        "common.dynamic.recovery_fail_callback_timeout",
        "The collector did not reconnect after the restart. Check the "
        "address and that the collector can reach Home Assistant, then "
        "try again.",
    ),
    "inbound_reconnect_timeout": (
        "common.dynamic.recovery_fail_inbound_timeout",
        "The collector did not reconnect on its own after the restart. "
        "Try again after the collector reconnects, or enable background discovery.",
    ),
    "restart_not_supported": (
        "common.dynamic.recovery_fail_restart_unsupported",
        "This collector cannot be restarted for verification over its "
        "connection. Change the connection settings and try again.",
    ),
    "recovery_ownership_unavailable": (
        "common.dynamic.recovery_fail_ownership_unavailable",
        "The verified connection is no longer held for this collector. "
        "Run the verification again.",
    ),
    # Inbound autonomous-reconnect verification (passive discovery)
    # failures, surfaced honestly on the discovery failure screen.
    "strong_identity_timeout": (
        "common.dynamic.recovery_fail_strong_identity_timeout",
        "The collector's identity could not be confirmed before the "
        "check timed out. Try again, or configure the connection by "
        "hand.",
    ),
    "restart_not_confirmed": (
        "common.dynamic.recovery_fail_restart_not_confirmed",
        "The collector did not confirm the restart request, so its "
        "reconnection could not be verified. Try again, or configure "
        "the connection by hand.",
    ),
    "disconnect_not_observed": (
        "common.dynamic.recovery_fail_disconnect_not_observed",
        "The collector did not drop and re-establish its connection "
        "after the restart, so automatic reconnection was not proven. "
        "Try again, or configure a callback connection by hand.",
    ),
    "reconnected_session_untrusted": (
        "common.dynamic.recovery_fail_reconnected_untrusted",
        "A connection came back after the restart but its identity "
        "could not be trusted. Try again, or configure the connection "
        "by hand.",
    ),
    # Verified strategy-transition preflight refusals (Batch 8). These happen
    # BEFORE any collector side effect.
    "transition_session_unavailable": (
        "common.dynamic.transition_fail_session_unavailable",
        "The collector is not connected right now, so the switch cannot be "
        "verified. Wait for the collector to connect, then try again.",
    ),
    "transition_endpoint_required": (
        "common.dynamic.transition_fail_endpoint_required",
        "The Home Assistant address to write into the collector is missing. "
        "Enter the address and port the collector can reach.",
    ),
    "transition_callback_route_required": (
        "common.dynamic.transition_fail_callback_route_required",
        "The collector address and the advertised Home Assistant callback "
        "address are required. Enter them and try again.",
    ),
    "transition_rollback_endpoint_unavailable": (
        "common.dynamic.transition_fail_rollback_unavailable",
        "The integration manages the collector's endpoint, but no saved "
        "previous endpoint exists to hand control back to. The switch was "
        "not started.",
    ),
    "transition_rollback_selection_required": (
        "common.dynamic.transition_fail_rollback_selection_required",
        "No cloud endpoint was chosen to hand the collector back to, so the "
        "switch was not started. Pick the previously saved endpoint, a catalog "
        "entry, or enter one manually.",
    ),
    "transition_rollback_selection_invalid": (
        "common.dynamic.transition_fail_rollback_selection_invalid",
        "The chosen cloud endpoint could not be validated, so nothing was "
        "changed. Choose the endpoint again.",
    ),
    "transition_rollback_selection_stale": (
        "common.dynamic.transition_fail_rollback_selection_stale",
        "The saved endpoint candidate changed after it was shown. Nothing was "
        "changed; review the current candidate and confirm it again.",
    ),
    "transition_rollback_persist_failed": (
        "common.dynamic.transition_fail_rollback_persist_failed",
        "The chosen cloud endpoint could not be saved, so it was NOT written to "
        "the collector and the connection strategy was not changed. Try again.",
    ),
    "transition_rollback_registry_pn_required": (
        "common.dynamic.transition_fail_rollback_registry_pn_required",
        "This collector could not be identified well enough to durably save the "
        "chosen cloud endpoint, so nothing was written. Confirm the collector is "
        "connected and identified, then try again.",
    ),
    "transition_management_unavailable": (
        "common.dynamic.transition_fail_management_unavailable",
        "The collector's current connection does not provide the endpoint "
        "write/apply operations required for this switch. Nothing was changed.",
    ),
    "transition_inbound_rollback_persist_failed": (
        "common.dynamic.transition_fail_inbound_rollback_persist_failed",
        "The current external endpoint could not be saved before redirecting "
        "the collector to Home Assistant, so the switch was not started.",
    ),
    "transition_endpoint_write_failed": (
        "common.dynamic.transition_fail_endpoint_write_failed",
        "Writing the endpoint to the collector failed, so nothing was "
        "verified and the connection strategy was not changed. Try again.",
    ),
    "transition_inbound_recovered_instead": (
        "common.dynamic.transition_fail_inbound_recovered_instead",
        "The collector reconnected on its own instead of waiting for the "
        "callback, so on-demand operation could not be proven. Its endpoint "
        "still points at Home Assistant.",
    ),
    "transition_already_running": (
        "common.dynamic.transition_fail_already_running",
        "Another connection switch is already in progress for this "
        "collector. Wait for it to finish.",
    ),
    "collector_endpoint_operation_busy": (
        "common.dynamic.collector_endpoint_operation_busy",
        "Another operation is currently changing this collector's connection "
        "(for example proxy capture, control discovery, or an endpoint action). "
        "Nothing was changed. Wait for it to finish, then try again.",
    ),
    "transition_runtime_unavailable": (
        "common.dynamic.transition_fail_runtime_unavailable",
        "The collector runtime is not loaded, so the switch cannot be "
        "verified right now. Try again after the integration finishes "
        "starting.",
    ),
    "transition_not_required": (
        "common.dynamic.transition_fail_not_required",
        "The collector already uses this connection strategy. Nothing to change.",
    ),
    "transition_inbound_recovered_after_restore": (
        "common.dynamic.transition_fail_recovered_after_restore",
        "The collector reconnected on its own even though its endpoint was "
        "already handed back to the external target, so on-demand operation "
        "could not be proven. The connection keeps working; you can retry "
        "the switch.",
    ),
    "transition_payload_forbidden": (
        "common.dynamic.transition_fail_payload_forbidden",
        "The switch request carried settings that only the verification "
        "itself may change. Nothing was modified.",
    ),
    # The repair proved and committed the on-demand connection, but Home
    # Assistant could not finish loading the entry afterwards. The proof is kept
    # -- only LOADING is retried; the verification and collector reconfiguration
    # are never repeated.
    "transition_activation_incomplete": (
        "common.dynamic.transition_fail_activation_incomplete",
        "The on-demand connection was verified and saved, but Home Assistant "
        "could not finish loading the integration. The verification and the "
        "collector reconfiguration are not repeated -- Home Assistant just "
        "retries loading the already-proven callback configuration.",
    ),
    # The running integration could not be stopped to run the repair (it stays
    # loaded). Nothing was verified or changed; try again.
    "transition_suspend_failed": (
        "common.dynamic.transition_fail_suspend_failed",
        "Home Assistant could not pause the running integration to repair the "
        "connection, so nothing was changed. Try again in a moment.",
    ),
    # The repair was stopped before it changed anything, but reloading the
    # previous configuration afterwards did not succeed -- surfaced honestly so
    # the user knows the integration needs a manual reload.
    "transition_restore_failed": (
        "common.dynamic.transition_fail_restore_failed",
        "The repair was stopped without changing anything, but the previous "
        "configuration did not load again automatically. Reload the integration "
        "to bring it back.",
    ),
}


def _shared_recovery_failure_explanation(tr, code: str) -> str:
    """A localized, human sentence for one typed recovery/transition failure.

    ONE table for every surface (config flow, options flow): never shows the
    raw code; unknown codes fall back to the localized generic sentence.
    """

    key, fallback = _RECOVERY_FAILURE_EXPLANATIONS.get(
        code,
        (
            "common.dynamic.recovery_fail_generic",
            "The recovery verification did not succeed. You can try again or "
            "change the settings.",
        ),
    )
    return tr(key, fallback)


__all__ = [
    "CONF_WIFI_PASSWORD",
    "CONF_WIFI_SSID",
    "_INT_FIELDS",
    "_PASSWORD_TEXT_SELECTOR",
    "_PRE_ENTRY_INVERTER_METADATA_KEYS",
    "_RECOVERY_FAILURE_EXPLANATIONS",
    "_ble_wifi_network_label",
    "_ble_wifi_selector",
    "_clear_runtime_inverter_facts",
    "_connection_strategy_selector",
    "_driver_detection_strategy_selector",
    "_exception_detail",
    "_flatten_sections",
    "_poll_interval_selector",
    "_poll_mode_selector",
    "_shared_recovery_failure_explanation",
    "_smartess_credential_schema_fields",
]
