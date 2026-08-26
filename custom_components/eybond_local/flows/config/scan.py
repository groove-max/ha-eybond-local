"""Collector network selection and active/passive scan lifecycle."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Sequence
from contextlib import suppress
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigFlowResult,
)
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from ...collector.transport_profile import (
    collector_session_protocol_from_inventory_state,
)
from .common import (
    _async_timeout,
    _is_ipv4,
)
from .result_model import (
    _result_indicates_inverter_link_down,
)
from ...connection.admission import ObservedCollectorSession
from ...connection.spec_factory import (
    build_connection_spec_from_values,
)
from ...const import (
    CONF_ENTRY_ROLE,
    CONF_SERVER_IP,
    CONNECTION_TYPE_EYBOND,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
)
from ..common.translation import (
    with_translation_bundle as _with_translation_bundle,
)
from ...models import (
    CollectorCandidate,
    CollectorInfo,
    OnboardingResult,
)
from ...onboarding.factory import create_onboarding_manager

logger = logging.getLogger(__name__)

CONF_RESULT_KEY = "result_key"

_SCAN_RESULTS_ACTION_REFRESH = "action:refresh_scan"

_SCAN_RESULTS_ACTION_ADVANCED = "action:advanced_setup"

CONF_COLLECTOR_NETWORK_STATUS = "collector_network_status"


def _result_selector(result_options: dict[str, str]) -> SelectSelector:
    """Return a selector for scan results."""

    options = [
        SelectOptionDict(value=key, label=label)
        for key, label in result_options.items()
    ]
    return SelectSelector(
        SelectSelectorConfig(
            options=options,
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


class CollectorScanFlowMixin:
    """Collector network selection and active/passive scan lifecycle."""

    @_with_translation_bundle
    async def async_step_collector_network(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        return self.async_show_menu(
            step_id="collector_network",
            menu_options=["auto", "bluetooth_setup", "listener"],
            description_placeholders=self._collector_network_placeholders(),
        )

    def _listener_entry_exists(self) -> bool:
        """Return whether background discovery already has its bootstrap entry."""

        listener_unique_id = f"{DOMAIN}:listener"
        return any(
            str((getattr(entry, "data", {}) or {}).get(CONF_ENTRY_ROLE) or "")
            == ENTRY_ROLE_LISTENER
            or str(getattr(entry, "unique_id", "") or "") == listener_unique_id
            for entry in self.hass.config_entries.async_entries(DOMAIN)
        )

    async def _async_create_listener_entry(self) -> ConfigFlowResult:
        """Create the integration-level passive-discovery bootstrap entry."""

        await self.async_set_unique_id(f"{DOMAIN}:listener")
        abort = self._abort_if_unique_id_configured()
        if abort is not None:
            return abort
        return self.async_create_entry(
            title="EyeBond Local — Discovery",
            data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
        )

    async def async_step_listener(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Enable persistent passive discovery without a collector entry."""

        del user_input
        # This is an explicit user request to make currently connected,
        # unconfigured collectors visible. The domain service is already owned by
        # integration async_setup, so refresh its edge-triggered publication state
        # before creating (or discovering that we already have) the bootstrap
        # entry. Import-created listener entries deliberately do not do this.
        from ...passive_discovery import get_passive_callback_discovery

        discovery = get_passive_callback_discovery(self.hass)
        if discovery is not None:
            await discovery.async_show_discovered_devices_again()
        # In real Home Assistant ``_abort_if_unique_id_configured`` raises the
        # generic ``already_configured`` abort instead of returning it. Resolve
        # the explicit listener action before that framework boundary so the user
        # gets an honest discovery-specific result. The unique-id check in the
        # create helper remains the race guard for concurrent flows.
        if self._listener_entry_exists():
            return self.async_abort(reason="background_discovery_refreshed")
        return await self._async_create_listener_entry()

    async def async_step_import(
        self,
        import_data: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Create the internal listener entry after device-entry migration/removal."""

        if str((import_data or {}).get(CONF_ENTRY_ROLE) or "") != ENTRY_ROLE_LISTENER:
            return self.async_abort(reason="invalid_import")
        return await self._async_create_listener_entry()

    @_with_translation_bundle
    async def async_step_auto(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if self._scan_error:
            errors = {"base": "cannot_autodetect"}
            self._scan_error = False

        single_interface = len(self._interface_options) == 1

        def _start_auto_scan() -> ConfigFlowResult:
            self._reset_scan_progress()
            return self.async_step_scanning()

        if user_input is not None:
            effective = dict(user_input)
            effective.setdefault(CONF_SERVER_IP, self._local_ip)
            self._normalize_current_server_ip(effective)
            input_errors = self._validate_connection_inputs(
                effective,
                fields=self._connection_branch().form_layout.auto_fields,
            )
            if input_errors:
                errors.update(input_errors)
            else:
                self._auto_config.update(effective)
                return await _start_auto_scan()
        elif single_interface and not errors:
            # One interface and nothing to ask: start scanning immediately so
            # the happy path is Welcome -> (collector ready) -> results.
            self._auto_config.setdefault(CONF_SERVER_IP, self._local_ip)
            self._normalize_current_server_ip(self._auto_config)
            return await _start_auto_scan()

        data_schema = vol.Schema(
            self._build_connection_fields_schema(
                self._current_connection_type(),
                fields=self._connection_branch().form_layout.auto_fields,
                values=self._auto_connection_defaults(),
            )
        )

        return self.async_show_form(
            step_id="auto",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._auto_description_placeholders(
                single_interface
            ),
        )

    @_with_translation_bundle
    async def async_step_scanning(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._scan_task is None:
            self._scan_started_monotonic = time.monotonic()
            self._scan_progress_stage = "preparing"
            self._scan_progress_visible = False
            self.async_update_progress(0.0)
            self._scan_task = self.hass.async_create_task(self._async_do_scan())

        selected_ip = self._auto_config.get(CONF_SERVER_IP, self._local_ip)
        selected_label = self._selected_interface_label(selected_ip)

        if not self._scan_progress_visible:
            self._scan_progress_visible = True
            return self.async_show_progress(
                step_id="scanning",
                progress_action="scanning_network",
                progress_task=self._scan_task,
                description_placeholders=self._scan_progress_placeholders(
                    selected_label
                ),
            )

        if self._scan_task.done():
            self._scan_started_monotonic = None
            self._scan_progress_visible = False
            if self._scan_task.exception() or not self._autodetect_results:
                self._scan_error = True
            return self.async_show_progress_done(next_step_id="scan_results")

        return self.async_show_progress(
            step_id="scanning",
            progress_action="scanning_network",
            progress_task=self._scan_task,
            description_placeholders=self._scan_progress_placeholders(selected_label),
        )

    async def _async_do_scan(self) -> None:
        """Run auto-detection in the background."""

        # Direct scan invocations (including resumed flows) must cross the same
        # cold-start executor boundary as the top-level user/discovery steps.
        await self._async_prepare_metadata_caches()
        self._scan_responded_addresses.clear()
        effective_input = self._auto_config
        server_ip = str(
            effective_input.get(CONF_SERVER_IP, self._local_ip) or self._local_ip
        )
        discovery_targets = self._scan_discovery_targets()
        scan_timeout = self._scan_timeout_seconds
        detector_timeout = max(5.0, scan_timeout - 5.0)
        self._scan_progress_stage = "discovering"
        detector = create_onboarding_manager(
            build_connection_spec_from_values(
                self._current_connection_type(),
                dict(self._auto_connection_defaults(), **effective_input),
            ),
        )
        listener_passive_results = await self._async_passive_scan_results(
            detector,
            discovery_targets=discovery_targets,
        )
        skip_probe_ips = self._configured_collector_probe_skip_ips()
        # Passive discovery shares this scan's callback listener. Mark sockets
        # accepted while the active UDP probe runs as results of this flow so
        # HA does not publish a duplicate integration-discovery card for them.
        from ...passive_discovery import get_passive_callback_discovery

        passive_discovery = get_passive_callback_discovery(self.hass)
        probe_scope_id = f"config_flow_scan:{id(self)}:{uuid.uuid4().hex}"
        if passive_discovery is not None:
            passive_discovery.begin_active_probe_scope(probe_scope_id)
        # Only the active scan needs a periodic updater.  Starting it before
        # the passive shortcut used to leak an infinite updater whenever an
        # already-observed collector completed the scan early.  The leaked
        # task kept publishing 97% over the next scan's live progress.
        progress_updater = asyncio.create_task(self._async_update_scan_progress_loop())
        try:
            async with _async_timeout(scan_timeout):
                results = await detector.async_scan(
                    discovery_targets=discovery_targets,
                    total_timeout=detector_timeout,
                    skip_probe_ips=skip_probe_ips,
                )
        except TimeoutError:
            logger.warning(
                "Collector scan timed out after %.1fs server_ip=%s discovery_targets=%s",
                scan_timeout,
                server_ip,
                ",".join(target.ip for target in discovery_targets),
            )
            self._scan_progress_stage = "finalizing"
            self._autodetect_results = {}
            return
        finally:
            if passive_discovery is not None:
                passive_discovery.end_active_probe_scope(probe_scope_id)
            progress_updater.cancel()
            with suppress(asyncio.CancelledError):
                await progress_updater
        self._scan_progress_stage = "analyzing"
        self.async_update_progress(0.9)
        await asyncio.sleep(0.08)
        for result in results:
            if not self._is_route_scan_result(result):
                continue
            collector = result.collector
            address = collector.ip if collector is not None else ""
            if (
                type(address) is str
                and address == address.strip()
                and _is_ipv4(address)
            ):
                self._scan_responded_addresses.add(address)
        # Read the shared registry at the result boundary, after the active scan.
        # A collector may dial in at any point while the scan is running; taking
        # this snapshot only before ``async_scan`` made that live strong-PN
        # session invisible until the next search. Background discovery and the
        # interactive result now converge on the same current inventory.
        shared_registry_results = self._shared_registry_scan_results()
        visible_results = self._collapse_scan_results(
            result
            for result in (
                *results,
                *listener_passive_results,
                *shared_registry_results,
            )
            if self._is_visible_scan_result(result)
        )

        if not visible_results:
            self._scan_progress_stage = "finalizing"
            self._autodetect_results = {}
            return

        connected_collectors = [
            result
            for result in visible_results
            if result.collector is not None and result.collector.connected
        ]
        matched = [result for result in visible_results if result.match is not None]

        self._autodetect_results = {
            str(index): result
            for index, result in enumerate(self._sort_scan_results(visible_results))
        }
        self._scan_progress_stage = "finalizing"
        self.async_update_progress(0.99)
        await asyncio.sleep(0.08)
        self._set_selected_result(None)

        if not matched and not connected_collectors:
            best_result = visible_results[0] if visible_results else None
            self._manual_defaults = self._build_manual_defaults(
                effective_input, best_result
            )
        self.async_update_progress(1.0)
        await asyncio.sleep(0.12)

    async def _async_passive_scan_results(
        self,
        detector: Any,
        *,
        discovery_targets: Sequence[Any],
    ) -> list[OnboardingResult]:
        passive_detect = getattr(detector, "async_passive_detect", None)
        if not callable(passive_detect):
            return []
        try:
            results = await passive_detect(
                discovery_targets=discovery_targets,
                settle_timeout=0.05,
            )
        except Exception as exc:
            logger.debug("Passive callback scan failed: %s", exc)
            return []
        return self._collapse_scan_results(
            result for result in results if self._is_visible_scan_result(result)
        )

    def _shared_registry_scan_results(self) -> list[OnboardingResult]:
        """Project the domain's shared callback inventory into scan candidates.

        Interactive search and background discovery must observe the same live
        sessions. The domain snapshot intentionally ignores background
        publication history, so a user-started search can show an open session
        retired after entry removal without also creating a spontaneous discovery
        card. Admission still verifies the exact typed session before persisting a
        connection strategy.
        """

        from ...passive_discovery import get_passive_callback_discovery

        discovery = get_passive_callback_discovery(self.hass)
        snapshot = getattr(discovery, "snapshot_unclaimed_collector_sessions", None)
        if not callable(snapshot):
            return []
        try:
            observations = tuple(snapshot())
        except Exception as exc:
            logger.debug("Shared callback candidate snapshot failed: %s", exc)
            return []

        target_ip = str(
            self._auto_config.get(CONF_SERVER_IP, self._local_ip) or self._local_ip
        ).strip()
        results: list[OnboardingResult] = []
        for observed in observations:
            if type(observed) is not ObservedCollectorSession:
                continue
            session_protocol = collector_session_protocol_from_inventory_state(
                state="",
                protocol_shape=observed.protocol_shape,
            )
            results.append(
                OnboardingResult(
                    connection_type=CONNECTION_TYPE_EYBOND,
                    connection_mode="callback_listener",
                    collector=CollectorCandidate(
                        target_ip=target_ip,
                        source="callback_listener",
                        ip=observed.peer_hint,
                        session_protocol=session_protocol,
                        connected=True,
                        collector=CollectorInfo(
                            remote_ip=observed.peer_hint,
                            collector_pn=observed.collector_pn,
                        ),
                    ),
                    next_action="manual_driver_selection",
                    last_error="collector_detected_without_driver",
                    observed_session=observed,
                )
            )
        return self._collapse_scan_results(results)

    @_with_translation_bundle
    async def async_step_scan_results(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """One screen: pick a found device directly, or pick a follow-up action."""

        # Refresh only while rendering. Selector values are positional keys;
        # rebuilding the result map after the user submits could make a key
        # point at a different collector if a session arrived in between.
        if user_input is None:
            self._refresh_live_registry_scan_results()
        selectable_results = self._selectable_autodetect_results()

        errors: dict[str, str] = {}
        if user_input is not None:
            selection = str(user_input.get(CONF_RESULT_KEY) or "")
            if selection == _SCAN_RESULTS_ACTION_REFRESH:
                return await self.async_step_refresh_scan()
            if selection == _SCAN_RESULTS_ACTION_ADVANCED:
                return await self.async_step_advanced_setup()
            result = selectable_results.get(selection)
            if result is None:
                errors["base"] = "invalid_selection"
            elif self._existing_entry_for_result(result) is not None:
                errors["base"] = "already_added_candidate"
            elif _result_indicates_inverter_link_down(result):
                errors["base"] = "inverter_link_down"
            else:
                self._set_selected_result(result)
                self._detection_summary_context = "auto"
                return await self._async_continue_selected_scan_result()

        options: dict[str, str] = {
            key: self._result_label(result)
            for key, result in selectable_results.items()
        }
        options[_SCAN_RESULTS_ACTION_REFRESH] = self._refresh_scan_action_label()
        options[_SCAN_RESULTS_ACTION_ADVANCED] = self._scan_action_label(
            "advanced_setup", "Device not found? Advanced setup"
        )
        data_schema = vol.Schema(
            {
                vol.Required(CONF_RESULT_KEY): _result_selector(options),
            }
        )
        return self.async_show_form(
            step_id="scan_results",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._scan_results_placeholders(),
        )

    def _refresh_live_registry_scan_results(self) -> None:
        """Merge late live sessions into this scan's immutable observations.

        A scan result is a record of evidence observed during this user action,
        not a mirror that discards an active result when the socket later closes.
        Admission still re-verifies the selected exact session.  The merge adds
        sessions that arrived after the detector's final snapshot while keeping
        active route evidence and stable selector semantics.
        """

        refreshed = self._collapse_scan_results(
            (*self._autodetect_results.values(), *self._shared_registry_scan_results())
        )
        self._autodetect_results = {
            str(index): result
            for index, result in enumerate(self._sort_scan_results(refreshed))
        }

    @_with_translation_bundle
    async def async_step_advanced_setup(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Power-user fallbacks when auto-scan did not find the device."""

        menu_options: list[str] = []
        if len(self._interface_options) > 1:
            menu_options.append("change_scan_interface")
        menu_options.append("manual")
        menu_options.append("refresh_scan")
        return self.async_show_menu(
            step_id="advanced_setup",
            menu_options=menu_options,
            description_placeholders=self._scan_results_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_change_scan_interface(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}

        if user_input is not None:
            effective = dict(self._auto_config)
            effective.update(user_input)
            input_errors = self._validate_connection_inputs(
                effective,
                fields=self._connection_branch().form_layout.auto_fields,
            )
            if input_errors:
                errors.update(input_errors)
            else:
                self._auto_config.update(user_input)
                self._reset_scan_progress()
                return await self.async_step_scanning()

        data_schema = vol.Schema(
            self._build_connection_fields_schema(
                self._current_connection_type(),
                fields=self._connection_branch().form_layout.auto_fields,
                values=self._auto_connection_defaults(),
            )
        )
        return self.async_show_form(
            step_id="change_scan_interface",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._auto_description_placeholders(False),
        )

    async def async_step_refresh_scan(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        if not self._auto_config:
            self._auto_config = self._auto_connection_defaults()
        self._reset_scan_progress()
        return await self.async_step_scanning()

    @_with_translation_bundle
    async def async_step_choose(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if not self._autodetect_results:
            return await self.async_step_auto()

        selectable_results = self._selectable_autodetect_results()
        if not selectable_results:
            return await self.async_step_scan_results()

        errors: dict[str, str] = {}
        if user_input is None and len(selectable_results) == 1:
            candidate = next(iter(selectable_results.values()))
            if _result_indicates_inverter_link_down(candidate):
                # Collector answered but the inverter link is down: never
                # classify, let the user fix cabling/power and rescan.
                errors["base"] = "inverter_link_down"
            else:
                self._set_selected_result(candidate)
                self._detection_summary_context = "auto"
                return await self._async_continue_selected_scan_result()

        if user_input is not None:
            selected_key = user_input[CONF_RESULT_KEY]
            result = selectable_results.get(selected_key)
            if result is None:
                errors["base"] = "invalid_selection"
            elif self._existing_entry_for_result(result) is not None:
                errors["base"] = "already_added_candidate"
            elif _result_indicates_inverter_link_down(result):
                errors["base"] = "inverter_link_down"
            else:
                self._set_selected_result(result)
                self._detection_summary_context = "auto"
                return await self._async_continue_selected_scan_result()

        options = {
            key: self._result_label(result)
            for key, result in selectable_results.items()
        }
        data_schema = vol.Schema(
            {
                vol.Required(CONF_RESULT_KEY): _result_selector(options),
            }
        )
        return self.async_show_form(
            step_id="choose",
            data_schema=data_schema,
            errors=errors,
            description_placeholders=self._choose_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_detection_summary(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Tell the user WHAT was identified and WHICH support tier applies."""

        if self._detection_summary_result() is None:
            if self._detection_summary_context == "manual":
                return await self.async_step_manual()
            return await self.async_step_auto()
        if user_input is not None:
            if self._detection_summary_context == "manual":
                if not self._manual_config:
                    return await self.async_step_manual()
                return await self._async_create_manual_entry(
                    self._manual_config, self._manual_result
                )
            return await self.async_step_confirm()

        # Auto path: when SmartESS cloud assist can refine the identification,
        # offer it here as an optional choice instead of forcing it on the user.
        if (
            self._detection_summary_context == "auto"
            and self._can_offer_smartess_cloud_assist(self._detection_summary_result())
        ):
            self._smartess_cloud_assist_mode = "auto"
            return self.async_show_menu(
                step_id="detection_summary",
                menu_options=["confirm", "smartess_cloud_assist"],
                description_placeholders=self._detection_summary_placeholders(),
            )

        return self.async_show_form(
            step_id="detection_summary",
            data_schema=vol.Schema({}),
            description_placeholders=self._detection_summary_placeholders(),
        )

    def _detection_summary_result(self) -> OnboardingResult | None:
        if self._detection_summary_context == "manual":
            return self._manual_result
        return self._selected_result

    def _detection_summary_placeholders(self) -> dict[str, str]:
        result = self._detection_summary_result()
        if result is None:
            return {"model": "", "tier_headline": "", "tier_details": ""}

        match = result.match
        catalog: dict[str, Any] = {}
        if match is not None and isinstance(match.details.get("device_catalog"), dict):
            catalog = match.details["device_catalog"]
        kind = str(catalog.get("kind") or "")
        tier = str(catalog.get("tier") or "")

        if kind == "device" and tier == "full":
            headline = self._tr(
                "common.dynamic.detection_tier_full_headline",
                "Full support",
            )
            details = self._tr(
                "common.dynamic.detection_tier_full_details",
                "This model is in the built-in device catalog. Read sensors and "
                "controls will be added out of the box.",
            )
        elif kind in ("device", "family") and tier == "partial":
            headline = self._tr(
                "common.dynamic.detection_tier_partial_headline",
                "Partial support (family match)",
            )
            details = self._tr(
                "common.dynamic.detection_tier_partial_details",
                "The inverter family is recognized, but this exact model is not in "
                "the catalog yet. Base read sensors will be added; controls stay "
                "locked for safety.\n\n"
                "Next step: after you finish here, open this integration and choose "
                "**Configure → Expand device support** to discover extra "
                "controls and sensor evidence.",
            )
        elif match is not None:
            headline = self._tr(
                "common.dynamic.detection_tier_driver_headline",
                "Detected by protocol driver",
            )
            details = self._tr(
                "common.dynamic.detection_tier_driver_details",
                "The device was identified by its protocol driver. The standard "
                "sensor set for this driver will be added.",
            )
        elif (
            verified_route := self._verified_callback_route_for_result(result)
        ) is not None:
            observed = result.observed_session
            peer_ip = (
                observed.peer_hint
                if type(observed) is ObservedCollectorSession and observed.peer_hint
                else self._tr("common.dynamic.unknown", "Unknown")
            )
            headline = self._tr(
                "common.dynamic.detection_tier_verified_callback_headline",
                "Collector verified",
            )
            details = self._tr(
                "common.dynamic.detection_tier_verified_callback_details",
                "The callback address **{callback_address}** and collector PN were "
                "verified. The incoming connection was observed from "
                "**{peer_ip}**, which may be a router/NAT address.\n\n"
                "The inverter was not identified during setup. You can add the "
                "collector now; runtime detection will continue after the entry "
                "takes ownership of the verified session.",
                {
                    "callback_address": verified_route.trigger_target_ip,
                    "peer_ip": peer_ip,
                },
            )
        elif self._result_is_passive_callback(result):
            headline = self._tr(
                "common.dynamic.detection_tier_passive_callback_headline",
                "Collector connected",
            )
            details = self._tr(
                "common.dynamic.detection_tier_passive_callback_details",
                "This collector is already connecting to Home Assistant. The setup "
                "wizard will add it as a callback-connected device; inverter "
                "detection will continue after the entry is created and the "
                "runtime owns this session.\n\n"
                "If an inverter is connected, sensors and controls may appear a "
                "little later after the first successful runtime detection cycle.",
            )
        else:
            headline = self._tr(
                "common.dynamic.detection_tier_unidentified_headline",
                "Device not recognized",
            )
            details = self._tr(
                "common.dynamic.detection_tier_unidentified_details",
                "The collector responds, but no inverter was detected through the "
                "selected driver/catalog path. This can mean the inverter is not "
                "connected, is powered off, uses an unsupported protocol, or only "
                "the collector is present.\n\n"
                "No entry is created until the collector has a confirmed identity. "
                "Device learning is useful only after an inverter has actually "
                "been detected.\n\n"
                "Next step: check the inverter connection, retry detection, then "
                "create a Support Archive if the device still cannot be identified.",
            )

        model = ""
        if match is not None and match.model_name:
            model = match.model_name
        else:
            model = self._result_label(result)
        return {"model": model, "tier_headline": headline, "tier_details": details}
