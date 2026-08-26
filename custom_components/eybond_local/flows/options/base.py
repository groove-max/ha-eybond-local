"""Extracted EyeBond options-flow lifecycle: OptionsFlowBase."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigFlowResult, OptionsFlow
from homeassistant.helpers.selector import SelectSelector, TextSelector

from ...collector.capabilities import (
    CollectorCapabilityProfile,
    collector_capability_profile_from_runtime,
)
from ...collector.smartess_ble import SmartEssBleWifiNetwork
from ...connection.operating_profile import (
    OPERATING_PROFILE_CLOUD_AND_HA,
    OPERATING_PROFILE_CUSTOM,
    OPERATING_PROFILE_HA_ONLY,
    CollectorOperatingProfile,
    collector_operating_profile_from_entry,
)
from ...support.acquisition import (
    SupportAcquisitionReadiness,
    resolve_support_acquisition_readiness,
)
from ...support.cloud_evidence_providers import cloud_evidence_provider_supported
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
from ...const import (
    CONF_COLLECTOR_PN,
    CONF_DETECTED_DRIVER,
    CONF_DRIVER_HINT,
    CONF_STRATEGY_TRANSITION_STATE,
    DRIVER_HINT_AUTO,
)
from ..common.translation import (
    TranslationBundleMixin as _TranslationBundleMixin,
)
from ..common.translation import (
    with_translation_bundle as _with_translation_bundle,
)
from ...runtime.manager import RuntimeInverterCandidate


class OptionsFlowBase(_TranslationBundleMixin, OptionsFlow):
    """OptionsFlowBase lifecycle."""

    def __init__(self, config_entry) -> None:
        self._config_entry = config_entry
        self._translation_bundle: dict[str, Any] = {}
        self._translation_bundle_language = ""
        self._interface_options: list[dict[str, str]] = []
        self._diagnostics_result: dict[str, str] = {}
        self._diagnostic_commands_text = ""
        self._diagnostic_commands_output = ""
        self._diagnostic_commands_download_url = ""
        self._diagnostic_commands_result_path = ""
        self._diagnostic_publish_download_copy = False
        self._runtime_poll_interval_pending_input: dict[str, Any] = {}
        # Verified strategy-transition state: the target selected on the
        # dedicated connection/profile step and the progress task/result.
        self._transition_target_strategy = ""
        self._transition_options_payload: dict[str, Any] = {}
        self._transition_task = None
        self._transition_result = None
        self._transition_error = ""
        # CP2B.2: the ONE typed cloud rollback selection the chooser produced for
        # an integration-managed callback restore (the single authority passed to
        # the coordinator; never a loose endpoint string + separate provenance).
        self._transition_rollback_selection: Any = None
        self._transition_rollback_candidate_snapshot: Any = None
        self._transition_rollback_candidate_pinned = False
        # A proven+committed repair whose HA lifecycle activation did not stick
        # (async_setup returned falsy / retry / error). The proof is durable and
        # must NEVER be rolled back or re-run; only a plain setup/reload remains.
        self._transition_activation_incomplete = False
        self._collector_wifi_current_ssid = ""
        self._collector_wifi_network_diagnostics = ""
        self._collector_wifi_last_error = ""
        self._collector_wifi_last_result = ""
        self._collector_wifi_networks: tuple[SmartEssBleWifiNetwork, ...] = ()
        self._collector_uart_current_settings = ""
        self._collector_uart_current_baudrate = ""
        self._collector_uart_hardware_version = ""
        self._collector_uart_last_error = ""
        self._collector_uart_last_result = ""
        self._shadow_learning_state: dict[str, Any] = {}

    def _poll_policy_driver_key(self) -> str:
        """Return the persisted detected driver that owns poll limits."""

        driver_intent = str(
            self._config_entry.options.get(
                CONF_DRIVER_HINT,
                self._config_entry.data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO),
            )
            or DRIVER_HINT_AUTO
        )
        if driver_intent != DRIVER_HINT_AUTO:
            return driver_intent
        return str(
            self._config_entry.data.get(CONF_DETECTED_DRIVER) or DRIVER_HINT_AUTO
        )

    def _runtime_inverter_protocol_candidates(
        self,
    ) -> tuple[RuntimeInverterCandidate, ...]:
        """Read the typed ambiguity projection from the live coordinator."""

        coordinator = self._coordinator()
        candidates = getattr(coordinator, "inverter_protocol_candidates", ())
        if not isinstance(candidates, tuple):
            return ()
        if not all(type(item) is RuntimeInverterCandidate for item in candidates):
            return ()
        return candidates

    def _server_ip_field(self) -> SelectSelector | TextSelector:
        """Return the user-friendly selector for one local server IP."""

        if not self._interface_options:
            return _IP_TEXT_SELECTOR
        return _interface_selector(self._interface_options)

    def _selector_for_connection_field(self, field: ConnectionFormField):
        """Resolve one selector for branch-aware connection fields."""

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
        """Build one schema mapping for options-flow connection sections."""

        return _build_shared_connection_fields_schema(
            connection_type,
            fields=fields,
            values=values,
            server_ip_selector=self._server_ip_field(),
            translation_bundle=self._translation_bundle,
        )

    def _collector_is_virtual_bridge(self) -> bool:
        """Return True when the entry's collector is a detected virtual bridge.

        Detection is positive-only: it reads the runtime snapshot's parsed
        hardware-version token. When the coordinator/snapshot is unavailable (older
        firmware, factory collector, or a missed query) this returns False, so
        the menu behaves exactly as before — the gate only ever removes
        cloud-only options, never adds restrictions to factory collectors.
        """

        return self._collector_capabilities().virtual_bridge

    def _collector_capabilities(self) -> CollectorCapabilityProfile:
        """Return current collector capability profile for options-flow gating."""

        coordinator = self._coordinator()
        data = getattr(coordinator, "data", None)
        collector = getattr(data, "collector", None)
        values = getattr(data, "values", None)
        return collector_capability_profile_from_runtime(
            collector=collector,
            values=values if isinstance(values, dict) else {},
            data=dict(getattr(self._config_entry, "data", {}) or {}),
            options=dict(getattr(self._config_entry, "options", {}) or {}),
            hardware_version=self._collector_uart_hardware_version,
        )

    def _collector_operating_profile(self) -> CollectorOperatingProfile:
        """Return the read-only user-facing profile for this entry."""

        return collector_operating_profile_from_entry(
            dict(self._config_entry.data),
            dict(self._config_entry.options),
            ha_only_required=self._collector_capabilities().ha_only_required,
        )

    def _support_acquisition_readiness(self) -> SupportAcquisitionReadiness:
        """Return support-tool readiness independent of inverter recognition."""

        coordinator = self._coordinator()
        projected = getattr(coordinator, "support_acquisition_readiness", None)
        if type(projected) is SupportAcquisitionReadiness:
            return projected

        entry_data = dict(getattr(self._config_entry, "data", {}) or {})
        entry_options = dict(getattr(self._config_entry, "options", {}) or {})
        values = getattr(getattr(coordinator, "data", None), "values", None)
        runtime_values = values if isinstance(values, dict) else {}
        raw_pn = getattr(coordinator, "smartess_collector_pn", None)
        if raw_pn is None:
            raw_pn = entry_data.get(CONF_COLLECTOR_PN, "")
        raw_provider = getattr(coordinator, "cloud_evidence_provider", "")
        provider = (
            raw_provider
            if type(raw_provider) is str and raw_provider == raw_provider.strip()
            else ""
        )
        route_profile = collector_operating_profile_from_entry(
            entry_data,
            entry_options,
            ha_only_required=False,
        )
        return resolve_support_acquisition_readiness(
            collector_pn=raw_pn,
            inverter_identified=bool(
                entry_data.get("detected_model")
                or entry_data.get("detected_serial")
                or entry_options.get("detected_model")
                or entry_options.get("detected_serial")
                or runtime_values.get("model_name")
                or runtime_values.get("serial_number")
            ),
            virtual_bridge=self._collector_capabilities().virtual_bridge,
            cloud_provider=provider,
            cloud_provider_supported=cloud_evidence_provider_supported(provider),
            cloud_route_allowed=route_profile.cloud_tools_allowed,
        )

    def _proxy_capture_lifecycle_active(self, coordinator=None) -> bool:
        """Return whether proxy cleanup/recovery must remain reachable."""

        coordinator = coordinator or self._coordinator()
        overview = getattr(coordinator, "proxy_capture_overview", None)
        return bool(
            getattr(overview, "can_stop", False)
            or getattr(overview, "critical_phase", False)
        )

    def _proxy_capture_status_available(self, coordinator=None) -> bool:
        """Return whether a supported collector has proxy status/history to show."""

        # A positively identified local bridge has no vendor-cloud side.  A
        # stale overview/blocking reason must not manufacture a proxy path for
        # it; only a genuinely active lifecycle may remain reachable for
        # mandatory cleanup through the separate lifecycle gate.
        if self._collector_capabilities().virtual_bridge:
            return False

        coordinator = coordinator or self._coordinator()
        overview = getattr(coordinator, "proxy_capture_overview", None)
        return bool(
            getattr(overview, "can_start", False)
            or str(getattr(overview, "blocking_reason", "") or "").strip()
            or str(getattr(coordinator, "latest_proxy_trace_path", "") or "").strip()
            or str(
                getattr(coordinator, "latest_proxy_trace_manifest_path", "") or ""
            ).strip()
        )

    def _shadow_learning_lifecycle_active(self, coordinator=None) -> bool:
        """Return whether shadow cleanup/recovery must remain reachable."""

        coordinator = coordinator or self._coordinator()
        if coordinator is None:
            return False
        return self._shadow_learning_session_state(coordinator) in {
            "preflight",
            "starting",
            "ready",
            "learning",
            "waiting_for_collector",
            "connecting_upstream",
            "degraded",
            "restoring",
            "restore_failed",
        }

    def _cloud_tools_menu_available(self, coordinator=None) -> bool:
        """Return whether the shared support-acquisition path is reachable."""

        coordinator = coordinator or self._coordinator()
        readiness = self._support_acquisition_readiness()
        return bool(
            readiness.cloud_metadata_read.visible
            or readiness.proxy_capture.visible
            or self._proxy_capture_lifecycle_active(coordinator)
            or self._proxy_capture_status_available(coordinator)
            or self._shadow_learning_lifecycle_active(coordinator)
        )

    async def _async_cloud_tools_unavailable(self) -> ConfigFlowResult:
        """Route an unavailable deep link without proposing an impossible mode."""

        if not self._collector_capabilities().cloud_connection_supported:
            return await self.async_step_init()
        return await self.async_step_connection()

    def _operating_profile_label(self, profile: str) -> str:
        """Return one localized product-level operating-profile label."""

        labels = {
            OPERATING_PROFILE_CLOUD_AND_HA: self._tr(
                "common.dynamic.operating_profile_cloud_and_ha",
                "Cloud + Home Assistant",
            ),
            OPERATING_PROFILE_HA_ONLY: self._tr(
                "common.dynamic.operating_profile_ha_only",
                "Home Assistant only",
            ),
            OPERATING_PROFILE_CUSTOM: self._tr(
                "common.dynamic.operating_profile_custom",
                "Custom configuration",
            ),
        }
        return labels.get(profile, labels[OPERATING_PROFILE_CUSTOM])

    def _stage_connection_strategy_transition(self, target: str) -> None:
        """Stage one strategy target without creating a second authority."""

        self._transition_target_strategy = target
        self._transition_rollback_selection = None
        self._transition_rollback_candidate_snapshot = None
        self._transition_rollback_candidate_pinned = False
        self._transition_options_payload = {}
        self._transition_result = None
        self._transition_task = None
        self._transition_error = ""

    @_with_translation_bundle
    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        capabilities = self._collector_capabilities()
        marker_present = bool(
            str(
                self._config_entry.data.get(CONF_STRATEGY_TRANSITION_STATE) or ""
            ).strip()
        )

        # HIGHEST PRIORITY -- a PROVEN-but-unloaded entry (valid RecoveryContract,
        # no recovery marker, not LOADED) gets a DEDICATED activation-only menu:
        # it just needs loading, so nothing else (runtime / shadow / Wi-Fi /
        # diagnostics / physical repair) applies. Recovery/activation actions
        # always win over capability filtering.
        if not marker_present and self._callback_proven_but_not_loaded():
            return self.async_show_menu(
                step_id="init",
                menu_options=[
                    "strategy_transition_activation_retry",
                    "strategy_transition_cancel",
                ],
                description_placeholders={"bridge_note": ""},
            )

        # Capability-filtered BASE menu (a local bridge exposes no vendor-cloud
        # actions, so cloud-only control discovery / shadow learning is hidden).
        bridge_note = ""
        if capabilities.virtual_bridge:
            menu_options = [
                "collector_endpoint",
                "runtime",
                "collector_wifi",
                "diagnostics",
            ]
            if capabilities.uart_management:
                menu_options.insert(3, "collector_uart")
            bridge_note = self._tr(
                "common.dynamic.collector_virtual_bridge_note",
                "\n\nThis collector is a local ESP EyeBond Collector bridge with no "
                "vendor-cloud side. Cloud-only actions (control discovery / "
                "shadow learning) are hidden; Wi-Fi, UART, and runtime settings remain "
                "available.",
            )
        else:
            menu_options = [
                "connection",
                "runtime",
                "collector_wifi",
                "diagnostics",
            ]

        if self._cloud_tools_menu_available():
            menu_options.insert(menu_options.index("diagnostics"), "cloud_tools")
        local_observation_visible = getattr(
            self,
            "_local_register_observation_visible",
            None,
        )
        if callable(local_observation_visible) and local_observation_visible():
            menu_options.insert(
                menu_options.index("diagnostics"),
                "local_register_observation",
            )

        # RECOVERY takes priority OVER the capability filter: a degraded entry
        # (recovery marker present) offers the repair FIRST -- even a virtual
        # bridge can be degraded, and the bridge branch must NEVER drop it.
        if marker_present:
            menu_options.insert(0, "strategy_transition_repair")

        if len(self._runtime_inverter_protocol_candidates()) > 1:
            menu_options.insert(0 if not marker_present else 1, "inverter_protocol")

        return self.async_show_menu(
            step_id="init",
            menu_options=menu_options,
            description_placeholders={"bridge_note": bridge_note},
        )

    @_with_translation_bundle
    async def async_step_cloud_tools(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Expose evidence acquisition and support export through one path."""

        coordinator = self._coordinator()
        if coordinator is None:
            return await self._async_show_diagnostics_result(
                action_title=self._diagnostics_result_tr(
                    "cloud_tools_title",
                    "Expand device support",
                ),
                status=self._diagnostics_result_tr(
                    "coordinator_not_loaded",
                    "Coordinator is not loaded.",
                ),
                next_step=self._diagnostics_result_tr(
                    "ensure_entry_loaded",
                    "Ensure the entry is loaded, then try again.",
                ),
            )
        if not self._cloud_tools_menu_available(coordinator):
            return await self._async_cloud_tools_unavailable()

        readiness = self._support_acquisition_readiness()
        menu_options: list[str] = []
        if (
            readiness.cloud_metadata_read.can_start
            or readiness.active_control_learning.can_start
        ) or self._shadow_learning_lifecycle_active(coordinator):
            menu_options.append("shadow_learning")
        if (
            readiness.proxy_capture.visible
            or self._proxy_capture_lifecycle_active(coordinator)
            or self._proxy_capture_status_available(coordinator)
        ):
            menu_options.append("proxy_capture")
        if not menu_options:
            return await self.async_step_init()
        menu_options.append("create_support_package")
        return self.async_show_menu(
            step_id="cloud_tools",
            menu_options=menu_options,
        )
