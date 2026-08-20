"""Config-flow state ownership and top-level entry routing."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from pathlib import Path
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigFlowResult,
)
from homeassistant.core import callback

from .collector.smartess_ble import (
    SmartEssBleWifiNetwork,
)
from .collector.transport_profile import (
    collector_session_protocol_from_inventory_state,
    normalize_collector_session_protocol,
)
from .config_common import (
    _AUTO_SCAN_TIMEOUT,
    _ONBOARDING_TIMEOUT_POLICY,
    _PASSIVE_LISTENER_HOST,
)
from .connection.admission import CollectorAdmissionRequest, ObservedCollectorSession
from .connection.admission_transaction import (
    CollectorAdmissionTransaction,
    ManualCallbackContinuationTransaction,
)
from .connection.branch_registry import (
    supported_connection_types,
)
from .connection.callback_continuation import (
    CallbackContinuation,
    CallbackIdentityContext,
)
from .const import (
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_TYPE,
    CONF_ENTRY_ROLE,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONNECTION_TYPE_EYBOND,
    DEFAULT_DISCOVERY_TARGET,
    DEFAULT_TCP_PORT,
    ENTRY_ROLE_LISTENER,
)
from .flow_translation import (
    with_translation_bundle as _with_translation_bundle,
)
from .models import (
    CollectorCandidate,
    CollectorInfo,
    OnboardingResult,
)
from .support.cloud_evidence_providers import (
    CloudEvidenceOnboardingAssist as _SmartEssCloudAssistState,
)

logger = logging.getLogger(__name__)


class ConfigFlowBaseMixin:
    """Config-flow state ownership and top-level entry routing."""

    def __init__(self) -> None:
        self._translation_bundle: dict[str, Any] = {}
        self._translation_bundle_language = ""
        self._local_ip = ""
        self._default_broadcast = DEFAULT_DISCOVERY_TARGET
        self._interface_options: list[dict[str, str]] = []
        self._auto_config: dict[str, Any] = {}
        self._manual_defaults: dict[str, Any] = {}
        self._manual_config: dict[str, Any] = {}
        self._manual_result: OnboardingResult | None = None
        self._autodetect_results: dict[str, OnboardingResult] = {}
        # Route evidence is scan-wide and independent from collector identity.
        # Collapsing a PN-bearing TCP result with a PN-less UDP response must
        # not erase the address that actually answered this scan.
        self._scan_responded_addresses: set[str] = set()
        self._selected_result: OnboardingResult | None = None
        self._selected_result_collector_capabilities_attempted = False
        self._scan_task: asyncio.Task | None = None
        self._scan_error: bool = False
        self._scan_timeout_seconds = _AUTO_SCAN_TIMEOUT
        self._scan_started_monotonic: float | None = None
        self._scan_progress_stage = "preparing"
        self._scan_progress_visible = False
        self._ble_last_error = ""
        self._ble_local_adapter_available = False
        self._ble_ha_backend_available = False
        self._ble_selected_address = ""
        self._ble_wifi_networks_by_address: dict[
            str, tuple[SmartEssBleWifiNetwork, ...]
        ] = {}
        self._ble_fw_version_by_address: dict[str, str] = {}
        self._ble_wifi_scan_attempted_addresses: set[str] = set()
        self._ble_wifi_scan_failed_addresses: set[str] = set()
        self._collector_original_server_endpoint = ""
        self._collector_current_server_endpoint = ""
        self._collector_target_server_endpoint = ""
        self._collector_endpoint_error = ""
        self._collector_endpoint_bind_applied = False
        self._smartess_cloud_assist: _SmartEssCloudAssistState | None = None
        self._smartess_cloud_assist_mode = ""
        self._smartess_cloud_assist_last_error = ""
        self._smartess_cloud_assist_last_error_code = ""
        self._detection_summary_context = "auto"
        self._confirm_poll_interval_pending_input: dict[str, Any] = {}
        self._confirm_poll_interval_pending_step_id = "confirm"
        # The ONE neutral admission transaction in flight, or ``None``. It OWNS
        # the observed-session -> restart -> reconnect -> InboundRecoveryProof
        # lifecycle (request, working PN + enrichment, session resolution,
        # registry claim, restart channel, silent probe, verifier, outcome,
        # retry, cleanup, terminal handoff). The flow keeps only this reference,
        # the HA progress task, the UI continuation and result display.
        self._admission_transaction: CollectorAdmissionTransaction | None = None
        self._admission_task: asyncio.Task | None = None
        self._admission_callback_error = ""
        # Reconfigure of an existing PN-less entry has no prior identity to match:
        # bind the FIRST freshly-triggered NEW strong session's full PN instead.
        self._verification_bind_any = False
        # The strategy the user picked on the manual form. It is the source of
        # truth BEFORE any active operation runs (inbound must never probe) and
        # it is what gets persisted as the canonical entry.data strategy.
        self._manual_chosen_strategy = ""
        # A strategy pre-selected on the manual form's selector purely because
        # the user chose an explicit action that implies it (e.g. "configure a
        # callback connection by hand" after inbound verification failed). It
        # is only a form DEFAULT -- the user still confirms it, and it is never
        # inferred from discovery/peer IP/cloud family.
        self._manual_preselected_strategy = ""
        # A route selected from scan results is an identification attempt. Keep
        # the chosen address in one place so every manual/recovery menu applies
        # the same fail-closed policy and the ordinary manual flow stays unchanged.
        self._manual_scan_route_address = ""
        # Callback-trigger ledger generation sampled immediately BEFORE this
        # flow's own active callback attempt, so the shared matcher can prove the
        # attempt's trigger provenance (exactly one trigger: ours).
        # NOTE: do NOT name this `_reconfigure_entry_id` -- Home Assistant's own
        # ConfigFlow base class defines that as a read-only property (it returns
        # context["entry_id"] for SOURCE_RECONFIGURE), so assigning to it raises
        # AttributeError and breaks every flow instantiation.
        self._repair_entry_id = ""
        self._verified_connection_strategy = ""
        self._verified_strategy_evidence = ""
        # Manual callback recovery verification (the second, user-consented
        # transaction after a certified identity). The neutral callback
        # transaction owns the outcome and its exact prepared owner; the flow owns
        # only this HA progress task and a presentation error.
        self._manual_recovery_task: asyncio.Task | None = None
        self._manual_recovery_error = ""
        # The expectation DECLARED before this flow's first callback attempt
        # (passive discovery, or the PN an entry already stores), as opposed to
        # one a previous attempt adopted from its own probe result. ``None`` =
        # not captured yet. Restored at the start of every attempt so retries are
        # gated by durable evidence only. See _async_run_manual_callback_attempt.
        self._manual_declared_expected_pn: str | None = None
        # A one-shot error surfaced on the next manual form render when a callback
        # verification reached entry creation without a durable strong PN.
        self._manual_verification_error = ""
        # Manual, route-only and reconfigure paths start directly in the SAME
        # neutral callback transaction lifecycle used after observed-session
        # admission.  No callback owner/proof/session state lives on the flow.
        self._callback_continuation: CallbackContinuation = (
            self._new_manual_callback_continuation()
        )

    def _new_manual_callback_continuation(
        self,
        *,
        expected_pn: str = "",
        old_session_id: str = "",
    ) -> ManualCallbackContinuationTransaction:
        """Construct one neutral manual/reconfigure callback transaction."""

        return ManualCallbackContinuationTransaction(
            CallbackIdentityContext(
                expected_pn=expected_pn,
                old_session_id=old_session_id,
            ),
            registry_provider=self._callback_session_registry,
            listener_host=_PASSIVE_LISTENER_HOST,
            policy_provider=lambda: _ONBOARDING_TIMEOUT_POLICY,
            hass_provider=lambda: self.hass,
        )

    def _replace_manual_callback_continuation(
        self,
        *,
        expected_pn: str = "",
        old_session_id: str = "",
    ) -> None:
        """Release the current attempt and start a fresh manual transaction."""

        self._callback_continuation.release_unadopted_recovery()
        self._callback_continuation.release_terminal_owner()
        self._callback_continuation = self._new_manual_callback_continuation(
            expected_pn=expected_pn,
            old_session_id=old_session_id,
        )

    def async_remove(self) -> None:
        """Flow finished or was aborted: release any verification resources.

        Cancelling the admission progress task makes the transaction's own
        ``async_run`` finally close the restart channel/silent probe and release
        its registry claim, so no temporary claim or background wait survives a
        cancelled/completed flow. The direct transaction ``release`` below covers
        removal before/after the task runs (a no-op once the claim was handed off
        to entry setup, so a successful create keeps its owner).
        """

        task = self._admission_task
        self._admission_task = None
        transaction = self._admission_transaction
        self._admission_transaction = None
        if task is not None and not task.done():
            # The claim is released by the task's ``finally`` AFTER the restart
            # channel is closed -- never release it early here, or another owner
            # could take the identity while our socket is still open.
            task.cancel()
        elif transaction is not None:
            transaction.release()
        recovery_task = self._manual_recovery_task
        self._manual_recovery_task = None
        if recovery_task is not None and not recovery_task.done():
            # The recovery wrapper's own finally releases ITS claim; the flow
            # only cancels the producer task here.
            recovery_task.cancel()
        # Callback-continuation claim + unadopted recovery outcome: released via
        # the seam on removal (an uncommitted owner must never survive the flow).
        self._callback_continuation.release_terminal_owner()
        self._callback_continuation.release_unadopted_recovery()

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        role = str(config_entry.data.get(CONF_ENTRY_ROLE) or "")
        if role == ENTRY_ROLE_LISTENER:
            from .listener_options_flow import ListenerOptionsFlow

            return ListenerOptionsFlow(config_entry)
        from .options_flow import EybondLocalOptionsFlow

        return EybondLocalOptionsFlow(config_entry)

    @_with_translation_bundle
    async def _async_refresh_force_unsupported_override(self) -> None:
        """Re-read the on-device force-unsupported sentinel for flow-time detection.

        The integration's async_setup only runs after the first entry exists, so
        on a fresh install the very first config flow would otherwise ignore the
        force_unsupported.flag sentinel. Refresh it once here (in an executor —
        it stats a file) so the validation toggle works during onboarding too.
        """

        if getattr(self, "_force_unsupported_refreshed", False):
            return
        self._force_unsupported_refreshed = True
        from .metadata.device_catalog_loader import refresh_force_unsupported_override

        with suppress(Exception):
            config_root = Path(self.hass.config.path("eybond_local")).resolve()
            await self.hass.async_add_executor_job(
                refresh_force_unsupported_override, config_root
            )

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_refresh_force_unsupported_override()
        await self._async_ensure_network_defaults()

        def _select_connection_type(connection_type: str) -> None:
            self._auto_config = {CONF_CONNECTION_TYPE: connection_type}
            if len(self._interface_options) == 1:
                self._auto_config[CONF_SERVER_IP] = self._local_ip

        if user_input is not None:
            _select_connection_type(
                str(
                    user_input.get(
                        CONF_CONNECTION_TYPE,
                        self._auto_config.get(
                            CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND
                        ),
                    )
                )
            )
            return await self.async_step_collector_network()

        supported = supported_connection_types()
        if len(supported) == 1:
            # One connection type: a welcome screen with a single-option
            # dropdown asks nothing — go straight to network readiness.
            _select_connection_type(str(supported[0]))
            return await self.async_step_collector_network()

        data_schema = vol.Schema(
            {
                vol.Required(
                    CONF_CONNECTION_TYPE,
                    default=self._auto_config.get(
                        CONF_CONNECTION_TYPE, CONNECTION_TYPE_EYBOND
                    ),
                ): self._connection_type_selector(),
            }
        )
        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            description_placeholders=self._welcome_description_placeholders(),
        )

    @_with_translation_bundle
    async def async_step_integration_discovery(
        self,
        discovery_info: dict[str, Any],
    ) -> ConfigFlowResult:
        """Handle a collector that dialed into the passive callback listener."""

        await self._async_refresh_force_unsupported_override()
        await self._async_ensure_network_defaults()

        collector_pn = str(discovery_info.get(CONF_COLLECTOR_PN) or "").strip()
        peer_ip = str(discovery_info.get("peer_ip") or "").strip()
        tcp_port = int(discovery_info.get(CONF_TCP_PORT) or DEFAULT_TCP_PORT)
        self.context["eybond_discovery"] = {
            CONF_COLLECTOR_PN: collector_pn,
            CONF_TCP_PORT: tcp_port,
            "peer_ip": peer_ip,
            "session_id": str(discovery_info.get("session_id") or "").strip(),
            "collector_identity_source": str(
                discovery_info.get("collector_identity_source") or ""
            ).strip(),
        }
        discovery_title = (
            f"Collector PN {collector_pn}"
            if collector_pn
            else f"Collector {peer_ip}"
            if peer_ip
            else "EyeBond collector"
        )
        self.context["title_placeholders"] = {"name": discovery_title}
        if collector_pn:
            await self.async_set_unique_id(f"collector:{collector_pn}")
            abort = self._abort_if_unique_id_configured()
            if abort is not None:
                return abort
        self._auto_config = {
            **self._auto_connection_defaults(),
            CONF_CONNECTION_TYPE: CONNECTION_TYPE_EYBOND,
            CONF_SERVER_IP: self._local_ip,
            CONF_TCP_PORT: tcp_port,
        }

        if not collector_pn or not peer_ip:
            return self.async_abort(reason="already_configured")

        protocol_shape = str(discovery_info.get("protocol_shape") or "").strip().lower()
        collector_session_protocol = normalize_collector_session_protocol(
            discovery_info.get("collector_session_protocol")
        ) or collector_session_protocol_from_inventory_state(
            state=discovery_info.get("session_state"),
            protocol_shape=protocol_shape,
        )
        candidates = [
            OnboardingResult(
                connection_type=CONNECTION_TYPE_EYBOND,
                connection_mode="callback_listener",
                collector=CollectorCandidate(
                    target_ip=self._local_ip,
                    source="callback_listener",
                    ip=peer_ip,
                    session_protocol=collector_session_protocol,
                    connected=True,
                    collector=CollectorInfo(collector_pn=collector_pn),
                ),
                next_action="manual_driver_selection",
                last_error="collector_detected_without_driver",
            )
        ]

        candidates = [
            result
            for result in self._collapse_scan_results(candidates)
            if self._is_addable_scan_result(result)
            if self._existing_entry_for_result(result) is None
        ]
        if not candidates:
            return self.async_abort(reason="already_configured")

        self._autodetect_results = {
            str(index): result
            for index, result in enumerate(self._sort_scan_results(candidates))
        }
        if len(self._autodetect_results) > 1:
            return await self.async_step_scan_results()

        self._set_selected_result(next(iter(self._autodetect_results.values())))
        self._detection_summary_context = "auto"
        observed_session_id = str(discovery_info.get("session_id") or "").strip()
        if observed_session_id:
            # An observed inbound TCP session is NOT proof of a permanent
            # inbound configuration (a factory collector may only be connected
            # because of an earlier temporary UDP callback). Build the typed
            # observed session and admit it through the ONE entrypoint; the
            # strategy is verified behaviorally before persisting it.
            observed = ObservedCollectorSession(
                collector_pn=collector_pn,
                identity_source=str(
                    discovery_info.get("collector_identity_source") or ""
                ).strip(),
                session_id=observed_session_id,
                listener_port=int(tcp_port),
                protocol_shape=protocol_shape,
                peer_hint=peer_ip,
            )
            return await self._async_begin_collector_admission(
                CollectorAdmissionRequest(
                    observed_session=observed,
                    origin="integration_discovery",
                )
            )
        # Discovery payload without a session id: reboot verification is
        # impossible, and an unverified session must NEVER become an inbound
        # entry. Continue on the existing manual callback step (peer IP is only
        # an editable hint); the entry is created only after the callback proof.
        self._replace_manual_callback_continuation(expected_pn=collector_pn)
        logger.info(
            "Passive discovery payload for %s has no session id; requiring a "
            "callback proof on the manual step instead of assuming inbound",
            collector_pn,
        )
        return await self.async_step_manual()
