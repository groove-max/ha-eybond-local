"""Config-entry creation, reconfiguration, and handoff commit lifecycle."""

from __future__ import annotations

import logging
from contextlib import suppress
from dataclasses import (
    replace,
)
from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigFlowResult,
)
from homeassistant.data_entry_flow import section

from .collector.capabilities import (
    parse_esp_collector_hardware_token,
)
from .collector.smartess_local import (
    QUERY_HARDWARE_VERSION,
    parse_query_collector_response,
)
from .collector.transport import (
    SharedCollectorAtTransport,
)
from .config_common import (
    _async_timeout,
    _compute_broadcast_24,
    _sanitize_collector_route_hint,
)
from .config_result_model import (
    _apply_collector_cloud_family_metadata,
    _apply_collector_first_entry_semantics,
    _apply_collector_profile_metadata,
    _apply_confirmed_session_protocol_evidence,
    _apply_smartess_detection_metadata,
    _result_collector_capabilities,
)
from .connection.admission_transaction import (
    CollectorAdmissionTransaction,
)
from .connection.connection_policy import (
    collector_identity_binding_required,
    resolve_connection_strategy,
    resolve_endpoint_control_policy,
)
from .connection.entry import (
    build_detected_entry_settings,
    build_manual_entry_settings,
    with_driver_hint,
)
from .connection.recovery.terminal import (
    RecoveryTerminalInput,
    merge_recovery_contract,
)
from .connection.recovery.verification import (
    EVIDENCE_USER_CONFIRMED_SESSION,
)
from .const import (
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_MODE,
    CONF_CONNECTION_STRATEGY,
    CONF_CONNECTION_STRATEGY_EVIDENCE,
    CONF_CONNECTION_TYPE,
    CONF_CONTROL_MODE,
    CONF_DETECTED_MODEL,
    CONF_DETECTED_SERIAL,
    CONF_DETECTION_CONFIDENCE,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_DETECTION_STRATEGY,
    CONF_POLL_INTERVAL,
    CONF_POLL_MODE,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    CONTROL_MODE_READ_ONLY,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_MODE,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_TCP_PORT,
    DOMAIN,
    DRIVER_DETECTION_STRATEGIES,
    DRIVER_HINT_AUTO,
    ENDPOINT_CONTROL_EXTERNAL,
    FLOW_CONTEXT_ENTRY_COMMIT_IN_PROGRESS,
    POLL_MODE_AUTO,
    POLL_MODE_MANUAL,
)
from .flow_presentation import (
    _flatten_sections,
)
from .flow_translation import (
    with_translation_bundle as _with_translation_bundle,
)
from .models import (
    CollectorCandidate,
    CollectorInfo,
    OnboardingResult,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
from .naming import installation_title

logger = logging.getLogger(__name__)


class EntryCommitFlowMixin:
    """Config-entry creation, reconfiguration, and handoff commit lifecycle."""

    def _reconfigure_target_entry(self) -> ConfigEntry | None:
        entry_id = str(
            self.context.get("entry_id") or self._repair_entry_id or ""
        ).strip()
        if not entry_id:
            return None
        return self.hass.config_entries.async_get_entry(entry_id)

    @_with_translation_bundle
    async def async_step_reconfigure(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Repair a collector entry that has no durable identity binding.

        Runs the SAME callback identity transaction as onboarding: the user
        enters the reachable collector address, we trigger a one-shot callback
        and take the strong full PN off the NEW session it answers on. The
        existing entry is then updated in place (``async_update_reload_and_abort``)
        -- preserving the entry's canonical connection strategy (the user's
        choice) and completing the ownership handoff -- never deleted and
        re-added. Identity repair records NO recovery evidence: it proves which
        collector this is, not how it can be reached again. Until repair
        succeeds the entry keeps its identity_binding_required state and does
        not masquerade as a normal waiting_for_collector entry.
        """

        entry = self._reconfigure_target_entry()
        if entry is None:
            return self.async_abort(reason="reconfigure_entry_missing")

        # Identity repair runs ONLY for an entry that actually lacks its durable
        # binding. A listener entry, or a healthy PN-bound inbound/callback entry,
        # must NOT be pushed through callback verification -- that would flip a
        # genuine inbound entry to callback_on_demand and emit a UDP trigger it
        # never needed. Finish honestly without changing strategy or triggering.
        if not collector_identity_binding_required(entry.data, entry.options):
            return self.async_abort(reason="reconfigure_not_required")

        self._repair_entry_id = entry.entry_id
        await self._async_ensure_network_defaults()

        # A PN-less entry has no prior identity to match -> bind ANY freshly
        # triggered strong session. An entry that still carries a (short) PN must
        # re-confirm exactly it.
        existing_pn = str(entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
        self._replace_manual_callback_continuation(expected_pn=existing_pn)
        self._verification_bind_any = not existing_pn

        errors: dict[str, str] = {}
        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            self._normalize_current_server_ip(flat_input)
            errors = self._validate_connection_inputs(
                flat_input,
                fields=self._connection_branch().form_layout.manual_fields
                + self._connection_branch().form_layout.manual_advanced_fields,
            )
            if not errors:
                self._manual_config = dict(flat_input)
                # Repair is an active callback attempt too: same shared lifecycle
                # (fresh baseline + ledger generation + probe + matcher + claim),
                # so a repeated repair can never reuse a previous attempt's proof.
                verification_error = await self._async_run_manual_callback_attempt(
                    flat_input
                )
                if verification_error:
                    errors["base"] = verification_error
                else:
                    applied = self._async_apply_reconfigure(entry)
                    if applied is not None:
                        return applied
                    # No durable strong PN was bound: refuse to "repair" into
                    # another doomed PN-less entry; re-prompt.
                    errors["base"] = "callback_identity_unverified"

        return self._async_show_reconfigure_form(user_input, errors)

    def _async_show_reconfigure_form(
        self,
        user_input: dict[str, Any] | None,
        errors: dict[str, str],
    ) -> ConfigFlowResult:
        defaults = self._build_manual_defaults(user_input, None)
        data_schema = vol.Schema(
            {
                **self._build_connection_fields_schema(
                    self._current_connection_type(),
                    fields=self._connection_branch().form_layout.manual_fields,
                    values=defaults,
                ),
                vol.Required("advanced_connection"): section(
                    vol.Schema(
                        self._build_connection_fields_schema(
                            self._current_connection_type(),
                            fields=self._connection_branch().form_layout.manual_advanced_fields,
                            values=defaults,
                        )
                    ),
                    {"collapsed": True},
                ),
            }
        )
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "verification_note": self._manual_verification_note(),
            },
        )

    def _async_apply_reconfigure(self, entry: ConfigEntry) -> ConfigFlowResult | None:
        """Update the existing entry in place with the verified durable identity.

        Returns the terminal abort result on success, or ``None`` when no
        registry-certified strong PN was bound (the caller re-prompts). Never
        deletes/re-adds the entry.
        """

        verified_full_pn = self._callback_continuation.certified_pn
        if not verified_full_pn:
            self._callback_continuation.release_terminal_owner()
            return None

        collector_ip = _sanitize_collector_route_hint(
            str(self._manual_config.get(CONF_COLLECTOR_IP, "")),
            server_ip=str(self._manual_config.get(CONF_SERVER_IP, "")),
            discovery_target=str(self._manual_config.get(CONF_DISCOVERY_TARGET, "")),
        )
        # Collision guard (item 7): a DIFFERENT config entry may already own
        # collector:{pn}. Refuse to repair into a duplicate; leave this entry
        # PN-less and release the attempt's claim.
        for other in self.hass.config_entries.async_entries(DOMAIN):
            if getattr(other, "entry_id", None) == entry.entry_id:
                continue
            if (
                str(getattr(other, "unique_id", "") or "")
                == f"collector:{verified_full_pn}"
            ):
                self._callback_continuation.release_terminal_owner()
                return self.async_abort(reason="already_configured")

        new_data = dict(entry.data)
        new_data[CONF_COLLECTOR_PN] = verified_full_pn
        new_data[CONF_COLLECTOR_IP] = collector_ip
        # Identity repair re-binds the durable PN; it does NOT re-decide how the
        # collector connects. The entry's canonical strategy (the user's choice)
        # is preserved untouched. Only an entry from before the canonical axis
        # (no strategy in data) gets one stamped now -- callback_on_demand, the
        # explicit repair action the user just ran -- still intent, not an
        # inference. And no recovery evidence either way: the answered one-shot
        # certifies THIS session's identity, not a repeatable recovery route. Only
        # the dedicated recovery transaction may add that RecoveryContract proof.
        has_canonical_strategy = (
            str(entry.data.get(CONF_CONNECTION_STRATEGY) or "").strip()
            in CONNECTION_STRATEGIES
        )
        self._verified_connection_strategy = (
            "" if has_canonical_strategy else CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        )
        self._verified_strategy_evidence = ""
        self._apply_verified_connection_strategy(new_data)
        # The SAME terminal boundary: reconfigure carries no recovery outcome
        # today (identity repair is not recovery evidence), so the merge is a
        # typed no-op that also guarantees an existing valid contract in
        # entry.data survives the update untouched.
        refusal = merge_recovery_contract(
            new_data, self._callback_continuation.terminal_input
        )
        if refusal:
            logger.info("Recovery terminal refused the contract merge: %s", refusal)
            self._callback_continuation.release_terminal_owner()
            return self.async_abort(reason="recovery_contract_conflict")
        # Commit the ownership handoff (setup completes it after reload) and update
        # the entry in place, rolling the handoff back if the terminal helper
        # throws (item 4).
        return self._create_entry_with_handoff(
            verified_full_pn,
            lambda: self.async_update_reload_and_abort(
                entry,
                unique_id=f"collector:{verified_full_pn}",
                data=new_data,
            ),
            recovery=self._callback_continuation.terminal_input,
        )

    async def _async_create_entry_from_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        if self._selected_result is None:
            raise RuntimeError("no_selected_result")

        result = self._selected_result
        existing_entry = self._existing_entry_for_result(result)
        if existing_entry is not None:
            return self.async_abort(reason="already_configured")
        verified_callback_route = self._verified_callback_route_for_result(result)
        observed_peer_ip = result.collector.ip if result.collector is not None else ""
        collector_ip = (
            verified_callback_route.trigger_target_ip
            if verified_callback_route is not None
            else observed_peer_ip
        )
        collector_pn = self._collector_pn_for_result(result)
        # The scanner may have observed an inverter, but only the runtime on the
        # owned collector session may select and persist its driver.
        driver_hint = DRIVER_HINT_AUTO

        unique_id = self._result_unique_id(result)
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        title = installation_title(
            collector_pn=collector_pn,
            collector_ip=collector_ip or self._auto_config.get(CONF_COLLECTOR_IP, ""),
            detected_model="",
            detected_serial="",
        )

        connection_type = result.connection_type or self._current_connection_type()
        collector_capabilities = _result_collector_capabilities(result)
        result_connection_mode = str(result.connection_mode or "").strip()
        is_verified_callback_route = verified_callback_route is not None
        is_passive_callback = bool(
            not is_verified_callback_route
            and result_connection_mode == "callback_listener"
        )
        is_callback_listener = bool(
            not is_verified_callback_route
            and (
                is_passive_callback
                or (
                    self._collector_endpoint_bind_applied
                    and collector_capabilities.ha_only_required
                )
            )
        )
        durable_collector_ip = (
            ""
            if is_passive_callback and collector_pn
            else collector_ip or self._auto_config.get(CONF_COLLECTOR_IP, "")
        )
        connection_overrides = dict(self._auto_config)
        if is_verified_callback_route:
            # The terminal callback proof, not the TCP peer, owns the runtime
            # trigger target. Pin it over every stale scan/default value.
            connection_overrides[CONF_COLLECTOR_IP] = collector_ip
        if is_passive_callback and collector_pn:
            connection_overrides.pop(CONF_COLLECTOR_IP, None)
        connection_settings = with_driver_hint(
            build_detected_entry_settings(
                connection_type,
                server_ip=self._auto_config[CONF_SERVER_IP],
                collector_ip=durable_collector_ip,
                default_broadcast=_compute_broadcast_24(
                    self._auto_config[CONF_SERVER_IP]
                ),
                overrides=connection_overrides,
            ),
            driver_hint=driver_hint,
        )
        if is_callback_listener:
            stored_connection_mode = "callback_listener"
        else:
            stored_connection_mode = (
                "known_ip" if collector_ip else result_connection_mode
            )
        data = {
            CONF_CONNECTION_TYPE: connection_type,
            **connection_settings,
            CONF_CONNECTION_MODE: stored_connection_mode,
            CONF_CONTROL_MODE: CONTROL_MODE_READ_ONLY,
            CONF_COLLECTOR_PN: collector_pn,
            CONF_DETECTION_CONFIDENCE: "none",
            CONF_DETECTED_MODEL: "",
            CONF_DETECTED_SERIAL: "",
        }
        _apply_confirmed_session_protocol_evidence(data, result)
        if collector_capabilities.virtual_bridge:
            data["collector_virtual_bridge"] = True
            data["collector_bridge_kind"] = "esp-collector"
        _apply_collector_profile_metadata(data, result)
        _apply_smartess_detection_metadata(data, result)
        _apply_collector_cloud_family_metadata(data, result)
        _apply_collector_first_entry_semantics(data)
        detection_strategy = (user_input or {}).get(
            CONF_DRIVER_DETECTION_STRATEGY,
            DEFAULT_DRIVER_DETECTION_STRATEGY,
        )
        data[CONF_DRIVER_DETECTION_STRATEGY] = (
            detection_strategy
            if type(detection_strategy) is str
            and detection_strategy in DRIVER_DETECTION_STRATEGIES
            else DEFAULT_DRIVER_DETECTION_STRATEGY
        )
        poll_interval = int(
            (user_input or {}).get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )
        poll_mode = str(
            (user_input or {}).get(CONF_POLL_MODE, DEFAULT_POLL_MODE)
            or DEFAULT_POLL_MODE
        )
        if poll_mode not in {POLL_MODE_AUTO, POLL_MODE_MANUAL}:
            poll_mode = DEFAULT_POLL_MODE
        options = {
            CONF_POLL_INTERVAL: poll_interval,
            CONF_POLL_MODE: poll_mode,
        }
        _apply_collector_profile_metadata(options, result)
        remembered_endpoint = str(
            self._collector_original_server_endpoint or ""
        ).strip()
        target_endpoint = str(
            self._collector_target_server_endpoint
            or self._collector_callback_target_endpoint()
        ).strip()
        if (
            self._collector_endpoint_bind_applied
            and remembered_endpoint
            and remembered_endpoint != target_endpoint
        ):
            original_endpoint_options = self._collector_original_endpoint_options(
                remembered_endpoint
            )
            if not collector_capabilities.virtual_bridge:
                options.update(original_endpoint_options)
                with suppress(Exception):
                    await self._async_remember_collector_original_endpoint_in_registry(
                        collector_pn=collector_pn,
                        endpoint=remembered_endpoint,
                        options=original_endpoint_options,
                    )
        # Stamp the explicit connection architecture axes onto the new entry so
        # its transport ownership / endpoint control is opaque state, not derived
        # from hostnames at runtime. A passive-callback (inbound) entry resolves
        # to inbound/external; an HA-only bind that wrote the endpoint resolves
        # to integration_managed via its original-endpoint provenance.
        from .connection.connection_policy import migrate_entry_axes

        self._apply_verified_connection_strategy(data)
        data.update(migrate_entry_axes(data, options))
        # The typed recovery evidence (if this flow verified any) becomes the
        # entry's RecoveryContract HERE, through the ONE terminal merge
        # boundary -- written only via the model's single-writer API, never as
        # loose fields and never into options. A refusal (foreign identity /
        # malformed existing record) is typed, leaves the staged data
        # untouched, and is detected BEFORE any ownership handoff is prepared.
        # An entry-create failure below therefore persists nothing.
        refusal = merge_recovery_contract(
            data, self._callback_continuation.terminal_input
        )
        if refusal:
            logger.info("Recovery terminal refused the contract merge: %s", refusal)
            self._callback_continuation.release_terminal_owner()
            return self.async_abort(reason="recovery_contract_conflict")
        if self._fresh_observed_session_entry_is_unverified(data, options):
            # Fresh entries must carry an explicit authority for their recovery
            # strategy.  ``migrate_entry_axes`` may still derive legacy entries,
            # but its callback_listener/HA-only fallback is never sufficient to
            # create a new external inbound entry.
            logger.info(
                "Refusing fresh unverified external inbound entry for collector %s",
                collector_pn,
            )
            self._callback_continuation.release_terminal_owner()
            return self.async_abort(reason="recovery_ownership_unavailable")
        # Item 1 -- strict collector-PN invariant on the auto/passive path too: no
        # normal collector entry without a durable PN (peer IP is never identity).
        if collector_identity_binding_required(data, options):
            self._callback_continuation.release_terminal_owner()
            return self.async_abort(reason="collector_identity_required")
        # Item 2 (symmetry with the manual path): commit the handoff of the claim
        # held since the successful inbound restart/reconnect proof, then create;
        # roll back if the terminal helper throws (item 4).
        return self._create_entry_with_handoff(
            collector_pn,
            lambda: self.async_create_entry(title=title, data=data, options=options),
            recovery=self._callback_continuation.terminal_input,
        )

    def _fresh_observed_session_entry_is_unverified(
        self, data: dict[str, Any], options: dict[str, Any]
    ) -> bool:
        """Whether an observed collector-session admission reached the terminal
        unverified.

        SOURCE-NEUTRAL defense-in-depth, keyed ONLY on the TYPED in-flight
        ``self._admission_transaction`` -- the single carrier BOTH source adapters
        set through the ONE entrypoint. It is deliberately not keyed on
        ``self._selected_result.observed_session`` (integration discovery's
        selected result carries none, so that projection would silently miss the
        discovery source), nor on ``connection_mode`` / a ``detection`` reason /
        origin / collector kind / cloud family / hostname / peer IP / model. Other
        explicit/manual and virtual bridge terminals (which run without an
        admission transaction) retain their existing semantics.

        An admitted observed session may only become a fresh external inbound
        entry when it carries a REAL verified ``InboundRecoveryProof``
        (``migrate_entry_axes``'s callback_listener/HA-only fallback is never
        sufficient); the explicit user-confirmed-session evidence is the one
        behavioral proof that also satisfies it.
        """

        if not isinstance(self._admission_transaction, CollectorAdmissionTransaction):
            return False

        if resolve_connection_strategy(data, options) != CONNECTION_STRATEGY_INBOUND:
            return False
        if resolve_endpoint_control_policy(data, options) != ENDPOINT_CONTROL_EXTERNAL:
            return False
        if (
            str(data.get(CONF_CONNECTION_STRATEGY_EVIDENCE) or "").strip()
            == EVIDENCE_USER_CONFIRMED_SESSION
        ):
            return False
        from .connection.recovery_contract import RecoveryContract

        contract = RecoveryContract.from_entry_data(data)
        return contract is None or not contract.inbound_verified

    def _apply_verified_connection_strategy(self, data: dict[str, Any]) -> None:
        """Persist the canonical strategy -- and evidence ONLY when one exists.

        Two very different sources feed this, and they carry different proof:

        * ``inbound`` from the passive-discovery verification -- a BEHAVIORAL
          recovery proof (a genuine restart/reconnect dial-in, or the user
          explicitly binding an observed session). These set real evidence
          (``reboot_reconnect`` / ``user_confirmed_session``).
        * ``callback_on_demand`` from the manual/reconfigure paths -- the USER'S
          CHOSEN strategy, re-affirmed after a successful callback identity
          transaction. That transaction certifies session<->PN identity only;
          one answered one-shot trigger is NOT a behaviorally verified recovery
          route, so these paths set NO evidence and this method writes none.

        The explicit axis takes precedence over any legacy-field derivation;
        endpoint ownership is NOT touched (``endpoint_control_policy`` still
        requires real write provenance to become integration_managed).
        """

        if not self._verified_connection_strategy:
            return
        data[CONF_CONNECTION_STRATEGY] = self._verified_connection_strategy
        if self._verified_strategy_evidence:
            data[CONF_CONNECTION_STRATEGY_EVIDENCE] = self._verified_strategy_evidence
        if self._verified_connection_strategy == CONNECTION_STRATEGY_INBOUND:
            # The observed peer IP is where the connection came FROM (possibly a
            # router/NAT), not a verified collector address. An inbound entry
            # never dials or triggers, so persist no unverified address.
            data[CONF_COLLECTOR_IP] = ""

    async def _async_create_manual_entry(
        self,
        user_input: dict[str, Any],
        result: OnboardingResult | None = None,
    ) -> ConfigFlowResult:
        result = result or self._manual_result
        result = await self._async_enrich_manual_collector_profile(
            user_input,
            result,
        )
        if result is not None:
            existing_entry = self._existing_entry_for_result(result)
            if existing_entry is not None:
                return self.async_abort(reason="already_configured")
        collector_ip = user_input.get(CONF_COLLECTOR_IP, "")
        collector_pn = ""
        detected_model = ""
        detected_serial = ""
        driver_hint = DRIVER_HINT_AUTO
        connection_mode = "manual"

        if result is not None:
            connection_mode = result.connection_mode or connection_mode
            if result.collector is not None:
                collector_ip = result.collector.ip or collector_ip
                collector_info = result.collector.collector
                if collector_info is not None and collector_info.collector_pn:
                    collector_pn = collector_info.collector_pn

        # Durable-identity invariant: a manual/known-IP callback verification that
        # observed the collector's strong full PN must persist it. Without it the
        # created callback entry can never own the inbound session it triggers and
        # is doomed to collector_offline. Only a REGISTRY-CERTIFIED strong full PN
        # (fc2_parameter_2 / at_dtupn, held under the handoff owner) is
        # durable evidence -- never the discovery-time short/expected PN alone,
        # which is transient and unproven.
        verified_full_pn = self._callback_continuation.certified_pn
        if self._manual_chosen_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
            # On the callback path the VERIFIED identity is the ONLY identity the
            # entry may carry -- it fully REPLACES the probe's own result PN.
            # That result is not evidence of identity: it says what answered the
            # address, not that the collector answered THIS attempt's trigger on
            # a new strong session, and the registry claim is held for the
            # verified PN alone. Trusting the raw result would let an attempt
            # that FAILED verification (timeout, mismatch, interference) still
            # name the entry -- binding an identity this flow owns no claim for,
            # and in a retry it would resurrect the previous attempt's collector.
            # Unverified -> no PN -> the strict invariant below keeps the flow
            # open without creating an entry.
            collector_pn = verified_full_pn
        elif not collector_pn and verified_full_pn:
            # Inbound: the strong probe result already set ``collector_pn``; this
            # only fills in the observed/claimed-session case.
            collector_pn = verified_full_pn

        collector_ip = _sanitize_collector_route_hint(
            collector_ip,
            server_ip=str(user_input.get(CONF_SERVER_IP, "")),
            discovery_target=str(user_input.get(CONF_DISCOVERY_TARGET, "")),
        )
        # No address/serial fallback: a normal collector entry is keyed only by
        # its durable PN. Two collectors behind one NAT therefore remain distinct,
        # while an unidentified response remains inside this flow.
        if not collector_pn:
            self._callback_continuation.release_terminal_owner()
            self._manual_verification_error = (
                "callback_identity_unverified"
                if self._callback_continuation.identity_context.expected_pn
                else "collector_identity_required"
            )
            return await self.async_step_manual()

        await self.async_set_unique_id(f"collector:{collector_pn}")
        self._abort_if_unique_id_configured()

        title = installation_title(
            collector_pn=collector_pn,
            collector_ip=collector_ip,
            detected_model=detected_model,
            detected_serial=detected_serial,
        )

        connection_type = (
            result.connection_type
            if result is not None
            else self._current_connection_type()
        )
        data = with_driver_hint(
            build_manual_entry_settings(connection_type, user_input),
            driver_hint=driver_hint,
        )
        data.setdefault(CONF_CONNECTION_TYPE, connection_type)
        data[CONF_CONTROL_MODE] = CONTROL_MODE_READ_ONLY
        collector_capabilities = _result_collector_capabilities(result)
        data[CONF_COLLECTOR_IP] = collector_ip
        data[CONF_DETECTION_CONFIDENCE] = "none"
        data[CONF_CONNECTION_MODE] = connection_mode
        data[CONF_COLLECTOR_PN] = collector_pn
        data[CONF_DETECTED_MODEL] = detected_model
        data[CONF_DETECTED_SERIAL] = detected_serial
        if collector_capabilities.virtual_bridge:
            data["collector_virtual_bridge"] = True
            data["collector_bridge_kind"] = "esp-collector"
        _apply_collector_profile_metadata(data, result)
        _apply_smartess_detection_metadata(data, result)
        _apply_collector_cloud_family_metadata(data, result)
        _apply_confirmed_session_protocol_evidence(data, result)
        _apply_collector_first_entry_semantics(data)
        detection_strategy = user_input.get(
            CONF_DRIVER_DETECTION_STRATEGY,
            DEFAULT_DRIVER_DETECTION_STRATEGY,
        )
        data[CONF_DRIVER_DETECTION_STRATEGY] = (
            detection_strategy
            if type(detection_strategy) is str
            and detection_strategy in DRIVER_DETECTION_STRATEGIES
            else DEFAULT_DRIVER_DETECTION_STRATEGY
        )
        options = {
            CONF_POLL_INTERVAL: DEFAULT_POLL_INTERVAL,
            CONF_POLL_MODE: DEFAULT_POLL_MODE,
        }
        _apply_collector_profile_metadata(options, result)
        # The user explicitly stated how this collector connects, so that value is
        # the canonical strategy for EVERY terminal path -- an immediately
        # recognised NORMAL entry. It goes to
        # entry.data (never options) and wins over any legacy
        # connection_mode / operation-mode derivation.
        if self._manual_chosen_strategy in CONNECTION_STRATEGIES:
            data[CONF_CONNECTION_STRATEGY] = self._manual_chosen_strategy
        # Re-affirm the same user choice through the shared helper. On the
        # callback path this adds NO evidence -- identity success is not a
        # recovery proof (see _apply_verified_connection_strategy).
        self._apply_verified_connection_strategy(data)

        # Item 1 -- strict collector-PN invariant: a normal collector entry is
        # created ONLY with a durable collector PN. A detected inverter
        # model/serial or a virtual-bridge tag is NOT a session identity (the
        # registry owns sessions by PN only), so it can never substitute. The
        # detection stays in the flow and is shown to the user, but no normal
        # runtime entry is created until identity verification yields the PN;
        # legacy PN-less entries are fixed via reconfigure. No IP/session
        # fallback. (Only the integration listener entry is exempt, and it is
        # created on its own path, not here.)
        if collector_identity_binding_required(data, options):
            self._callback_continuation.release_terminal_owner()
            return self.async_abort(reason="collector_identity_required")

        # The SAME terminal boundary as the passive path: today the manual
        # flow carries no recovery outcome (a certified callback identity is
        # NOT recovery evidence), so this is a typed no-op -- but every
        # terminal funnels through the one merge writer.
        refusal = merge_recovery_contract(
            data, self._callback_continuation.terminal_input
        )
        if refusal:
            logger.info("Recovery terminal refused the contract merge: %s", refusal)
            self._callback_continuation.release_terminal_owner()
            return self.async_abort(reason="recovery_contract_conflict")

        # Commit the ownership handoff (the unique-id collision check above already
        # guaranteed collector:{pn} is free) and create the entry, rolling the
        # handoff back if the terminal helper throws (item 4).
        return self._create_entry_with_handoff(
            collector_pn,
            lambda: self.async_create_entry(title=title, data=data, options=options),
            recovery=self._callback_continuation.terminal_input,
        )

    def _create_entry_with_handoff(
        self,
        collector_pn: str,
        terminal,
        *,
        recovery: RecoveryTerminalInput | None = None,
    ):
        """The ONE terminal ownership coordinator for create/update.

        ``terminal`` is a zero-arg callable returning the ConfigFlowResult (an
        ``async_create_entry`` / ``async_update_reload_and_abort`` call). There is
        a SINGLE owner authority for the active flow: the chosen
        ``CallbackContinuation``. For an admission-origin flow that is the ONE
        transaction (it owns the inbound claim OR, after a callback continuation,
        the identity/recovery owner); manual/reconfigure uses its neutral manual
        specialization. This coordinator therefore does NOT branch on the
        admission transaction and reads no ownership field directly -- it asks the
        continuation to decide (prepare-vs-verify, inbound vs recovery owner),
        commits after the terminal returns, and rolls back exactly this attempt's
        owner if it raises. No other owner/entry is ever touched.
        """

        recovery = recovery if recovery is not None else RecoveryTerminalInput.none()
        if type(recovery) is not RecoveryTerminalInput:
            raise TypeError("recovery_terminal_input_required")
        decision = self._callback_continuation.prepare_terminal(collector_pn, recovery)
        if decision.abort_reason:
            return self.async_abort(reason=decision.abort_reason)
        if not decision.owns:
            # No claim to commit: the terminal runs with no ownership bookkeeping.
            result = terminal()
            self._mark_entry_commit_in_progress(collector_pn, result)
            return result
        try:
            result = terminal()
        except Exception:
            self._callback_continuation.rollback_terminal()
            raise
        self._callback_continuation.commit_terminal()
        self._mark_entry_commit_in_progress(collector_pn, result)
        return result

    def _mark_entry_commit_in_progress(
        self, collector_pn: str, result: ConfigFlowResult
    ) -> None:
        """Protect this CREATE_ENTRY flow until Home Assistant finishes it.

        ``ConfigEntriesFlowManager.async_finish_flow`` adds and fully sets up the
        entry before removing the flow from its progress registry. During that
        await, passive discovery can already observe the entry and its live
        session. The marker lets discovery distinguish this exact terminalizing
        flow from stale sibling discovery cards without owning HA's lifecycle.
        """

        if result.get("type") == "create_entry":
            self.context[FLOW_CONTEXT_ENTRY_COMMIT_IN_PROGRESS] = collector_pn

    async def _async_enrich_manual_collector_profile(
        self,
        user_input: dict[str, Any],
        result: OnboardingResult | None,
    ) -> OnboardingResult | None:
        """Resolve collector profile before creating a manual collector entry.

        Manual probing may time out at inverter detection while the collector is
        still reachable.  Entity platforms are created immediately after entry
        creation, so collector kind must be resolved here instead of waiting for
        runtime cleanup.  The only positive bridge signal accepted here is the
        hardware-version token read via FC=2 parameter 6.
        """

        if _result_collector_capabilities(result).virtual_bridge:
            return result

        collector_ip = str(user_input.get(CONF_COLLECTOR_IP, "") or "").strip()
        if result is not None and result.collector is not None:
            collector_ip = str(result.collector.ip or collector_ip).strip()
        collector_ip = _sanitize_collector_route_hint(
            collector_ip,
            server_ip=str(user_input.get(CONF_SERVER_IP, "")),
            discovery_target=str(user_input.get(CONF_DISCOVERY_TARGET, "")),
        )
        if not collector_ip:
            return result

        request_timeout = min(
            float(
                user_input.get("request_timeout", DEFAULT_REQUEST_TIMEOUT)
                or DEFAULT_REQUEST_TIMEOUT
            ),
            4.0,
        )
        transport = SharedCollectorAtTransport(
            host="0.0.0.0",
            port=int(
                user_input.get(CONF_TCP_PORT, DEFAULT_TCP_PORT) or DEFAULT_TCP_PORT
            ),
            request_timeout=request_timeout,
            collector_ip=collector_ip,
            collector_pn=self._collector_pn_for_result(result)
            if result is not None
            else "",
        )
        try:
            await transport.start()
            async with _async_timeout(request_timeout + 1.0):
                _header, payload = await transport.async_query_bridge_hardware_version()
            response = parse_query_collector_response(payload)
        except Exception as exc:
            logger.debug(
                "Manual collector profile probe failed collector_ip=%s error=%s",
                collector_ip,
                exc,
            )
            return result
        finally:
            with suppress(Exception):
                await transport.stop()

        if response.code != 0 or response.parameter != QUERY_HARDWARE_VERSION:
            return result
        hardware_version = str(response.text or "").strip().strip("\x00")
        token = parse_esp_collector_hardware_token(hardware_version)
        if not token.is_bridge:
            return result

        collector_candidate = result.collector if result is not None else None
        if collector_candidate is None:
            collector_candidate = CollectorCandidate(
                target_ip=collector_ip,
                source="manual_profile_probe",
                ip=collector_ip,
                connected=True,
                collector=CollectorInfo(remote_ip=collector_ip),
            )
        collector_info = collector_candidate.collector
        if collector_info is None:
            collector_info = CollectorInfo(remote_ip=collector_ip)
            collector_candidate.collector = collector_info
        collector_info.collector_virtual_bridge = True
        collector_info.collector_bridge_kind = "esp-collector"
        if token.version:
            collector_info.collector_bridge_version = token.version
        if not collector_candidate.ip:
            collector_candidate.ip = collector_ip
        if not collector_candidate.target_ip:
            collector_candidate.target_ip = collector_ip
        collector_candidate.connected = True

        if result is None:
            result = OnboardingResult(
                collector=collector_candidate,
                connection_type=self._current_connection_type(),
                connection_mode="manual",
                next_action="create_entry",
                last_error="manual_collector_profile_resolved",
            )
        elif result.collector is None:
            result = replace(result, collector=collector_candidate)

        logger.info(
            "Resolved manual collector %s as ESP EyeBond bridge via hardware token %s",
            collector_ip,
            hardware_version,
        )
        return result
