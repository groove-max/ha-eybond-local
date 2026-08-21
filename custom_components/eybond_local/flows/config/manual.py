"""Manual collector identity and recovery lifecycle."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.data_entry_flow import section

from ...collector_identity import (
    pn_is_same_identity,
)
from ...connection.admission import ObservedCollectorSession
from ...connection.callback_identity import (
    CallbackIdentityOutcome,
    CallbackIdentityRequest,
    OnboardingWireProbeIntent,
)
from ...connection.recovery.verification import (
    EVIDENCE_USER_CONFIRMED_SESSION,
    CallbackRecoveryRoute,
)
from ...connection.session_handle import WIRE_AT_TEXT, WIRE_FRAMED
from ...const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_CONNECTION_STRATEGY,
    CONF_DRIVER_DETECTION_STRATEGY,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    DEFAULT_DRIVER_DETECTION_STRATEGY,
    DRIVER_DETECTION_STRATEGIES,
)
from ..common.presentation import (
    MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
    MANUAL_CONFIRM_ACTION_ENABLE_DISCOVERY,
    MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
    MANUAL_CONFIRM_ACTION_SAVE,
    _connection_strategy_selector,
    _driver_detection_strategy_selector,
    _flatten_sections,
    _shared_recovery_failure_explanation,
)
from ..common.translation import with_translation_bundle as _with_translation_bundle
from ...models import (
    CollectorCandidate,
    CollectorInfo,
    OnboardingResult,
)

logger = logging.getLogger(__name__)


class ManualCollectorFlowMixin:
    """Manual collector identity and recovery lifecycle."""

    @_with_translation_bundle
    async def async_step_manual(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        await self._async_ensure_network_defaults()
        errors: dict[str, str] = {}
        if self._manual_verification_error:
            # Fail-closed retry from _async_create_manual_entry: verification
            # produced no durable strong PN, so no entry was created.
            errors["base"] = self._manual_verification_error
            self._manual_verification_error = ""

        if user_input is not None:
            flat_input = _flatten_sections(user_input)
            self._normalize_current_server_ip(flat_input)
            errors = self._validate_connection_inputs(
                flat_input,
                fields=self._connection_branch().form_layout.manual_fields
                + self._connection_branch().form_layout.manual_advanced_fields,
            )
            detection_strategy = flat_input.get(
                CONF_DRIVER_DETECTION_STRATEGY,
                DEFAULT_DRIVER_DETECTION_STRATEGY,
            )
            if (
                type(detection_strategy) is not str
                or detection_strategy not in DRIVER_DETECTION_STRATEGIES
            ):
                errors[CONF_DRIVER_DETECTION_STRATEGY] = "invalid_selection"
            if not errors:
                self._manual_config = dict(flat_input)
                # The user's CHOSEN strategy is the source of truth BEFORE any
                # active operation. It decides whether this attempt may reach out
                # at all -- never the connection_mode/hostname/IP derivation.
                chosen_strategy = str(
                    flat_input.get(CONF_CONNECTION_STRATEGY) or ""
                ).strip()
                if chosen_strategy not in CONNECTION_STRATEGIES:
                    chosen_strategy = CONNECTION_STRATEGY_INBOUND
                self._manual_chosen_strategy = chosen_strategy

                if chosen_strategy == CONNECTION_STRATEGY_INBOUND:
                    # INBOUND: the collector dials Home Assistant on its own.
                    # Home Assistant must send NOTHING -- no UDP callback trigger
                    # and no active auto-detect probe. Only passively observe what
                    # the shared listener already has.
                    return await self._async_manual_inbound_observe(flat_input)

                # CALLBACK_ON_DEMAND: one bounded attempt, and it needs a target.
                if not str(flat_input.get(CONF_COLLECTOR_IP) or "").strip():
                    errors[CONF_COLLECTOR_IP] = "callback_target_required"
                    return self._async_show_manual_form(user_input, errors)

                # ONE attempt, whole lifecycle owned by the shared helper:
                # fresh baseline + fresh ledger generation + probe + matcher +
                # claim. Nothing from a previous attempt survives.
                verification_error = await self._async_run_manual_callback_attempt(
                    flat_input
                )
                if verification_error:
                    # A callback attempt is an observation, not form validation.
                    # Keep the flow open so the user can retry or edit the
                    # address. No PN/session/claim is carried into the next try.
                    return await self._async_route_after_manual_callback_failure(
                        verification_error
                    )
                else:
                    return await self._async_route_after_manual_callback_success()

        return self._async_show_manual_form(user_input, errors)

    async def _async_run_manual_callback_attempt(
        self,
        settings: dict[str, Any],
        *,
        bootstrap_probe: "OnboardingWireProbeIntent | None" = None,
    ) -> str:
        """Run ONE complete manual callback attempt. Returns a typed error, or "".

        Every active manual callback path goes through here -- the first submit,
        "probe again", and reconfigure repair -- so no caller assembles half a
        proof and a retry can never mix a new result with the previous attempt's.

        The PROOF is the shared identity transaction, which is the ONE callback
        identity path in production: it takes the exclusive causality lease,
        sends exactly one trigger sequence, waits for the socket, claims it, reads
        the full PN authoritatively over the negotiated wire, and returns a
        registry-certified prepared handoff.

        Driver detection is NOT part of it, and does not run in this flow AT ALL.
        It used to run FIRST, with identity inferred from whatever PN it surfaced
        -- which is why an attempt outlived the very session it was identifying.
        It is now deferred entirely to the normal runtime after the entry is set
        up: nothing before entry creation probes for an inverter, a model or a
        driver.

        On any failure the flow is left holding nothing: no verified PN and no
        claim, so a stale identity can never leak into an entry.
        """

        # The DECLARED expectation (passive discovery / an entry's stored PN) is
        # durable and gates every attempt; one a previous attempt of this flow
        # adopted from its own result is not evidence and must not gate the next.
        # The initial value is derived from the seam's typed identity context; on
        # a retry the durable declared PN is restored so a
        # possibly-adopted expectation cannot leak into the next attempt.
        if self._manual_declared_expected_pn is None:
            self._manual_declared_expected_pn = (
                self._callback_continuation.identity_context.expected_pn
            )
        # Presentation state belongs to the flow, so reset it explicitly at the
        # same boundary where the transaction resets all identity/recovery state.
        self._manual_recovery_error = ""

        # The reset (a FULL new transaction -- no prior PN/session/offer/owner/held
        # recovery outcome survives), the PROOF, and the identity-owner adoption
        # are ALL the seam's job: it runs the ONE callback identity authority,
        # captures the silent bootstrap offer on a non-certified outcome, and
        # adopts the transaction's prepared owner (+ the certified full PN /
        # session id) on a certified one. The flow only builds the request -- from
        # the seam's typed identity context, not flow-owned lifecycle fields -- and maps the
        # typed outcome onto the next step.
        identity_context = self._callback_continuation.identity_context_for_attempt(
            self._manual_declared_expected_pn
        )
        outcome = await self._callback_continuation.async_run_identity(
            CallbackIdentityRequest(
                server_ip=str(settings.get(CONF_SERVER_IP) or ""),
                tcp_port=int(settings.get(CONF_TCP_PORT) or 0),
                udp_port=int(settings.get(CONF_UDP_PORT) or 0),
                target_ip=str(settings.get(CONF_COLLECTOR_IP) or ""),
                strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                expected_pn=identity_context.expected_pn,
                old_session_id=identity_context.old_session_id,
                owner_prefix="callback_verification",
                bootstrap_probe=bootstrap_probe,
            ),
        )
        if not outcome.identity_certified:
            return outcome.result or "callback_timeout"

        # NO detection. Not here, not after: this attempt has proven a collector
        # and that is the whole job. Running a driver sweep now would send a
        # SECOND callback trigger at the collector we just claimed, take tens of
        # seconds on a session we already own, and re-open the causal ambiguity
        # the lease just closed -- all to learn something the runtime will read
        # for itself, better, once the entry exists.
        #
        # So the flow carries an honest collector-only result: what we actually
        # know (this exact collector, on this exact wire) and nothing invented.
        # match/confidence stay empty, which routes to manual_confirm rather than
        # a detection summary. Inverter model/driver identification belongs to the
        # normal runtime after setup.
        self._manual_result = self._collector_only_result(settings, outcome)
        return ""

    def _collector_only_result(
        self,
        settings: dict[str, Any],
        outcome: CallbackIdentityOutcome,
    ) -> OnboardingResult:
        """Build the minimal onboarding result a certified identity proves.

        Everything here is evidence from the transaction: the durable full PN it
        read, the wire it negotiated, and the fact that the collector connected.
        No model, no serial, no driver, no confidence -- the flow must not imply
        knowledge it does not have.
        """

        return OnboardingResult(
            connection_type=self._current_connection_type(),
            connection_mode="manual",
            collector=CollectorCandidate(
                # The collector we triggered -- NOT Home Assistant's own address.
                # CONF_SERVER_IP is where the collector dials IN to; putting it
                # here would record HA as the collector's target. The address is
                # a target only: identity is the PN the transaction read, and peer
                # IP is never identity.
                target_ip=str(settings.get(CONF_COLLECTOR_IP, "") or ""),
                source="callback_identity",
                ip=str(settings.get(CONF_COLLECTOR_IP, "") or ""),
                session_protocol=outcome.session_protocol,
                connected=True,
                collector=CollectorInfo(collector_pn=outcome.collector_pn),
            ),
            observed_session=ObservedCollectorSession(
                collector_pn=outcome.collector_pn,
                identity_source=outcome.identity_source,
                session_id=outcome.session_id,
                listener_port=int(settings.get(CONF_TCP_PORT) or 0),
                protocol_shape=outcome.session_protocol,
                peer_hint="",
            ),
        )

    async def _async_route_after_manual_callback_success(self) -> ConfigFlowResult:
        """Adopt the verified identity and route on, after a confirmed attempt."""

        certified_pn = self._callback_continuation.certified_pn
        if certified_pn:
            # Behavioral proof: the collector answered THIS attempt's one-shot
            # trigger with a NEW strong session of that exact full PN, and the
            # matcher confirmed it. The claim for that session is already held.
            previous_pn = self._callback_continuation.adopt_certified_pn(certified_pn)
            abort = await self._async_propagate_enriched_pn(
                certified_pn, previous=previous_pn
            )
            if abort is not None:
                return abort
            # The strategy is the USER'S CHOICE (the manual form's
            # connection_strategy selector), persisted canonically as intent --
            # re-affirmed here only when the user actually chose
            # callback_on_demand, never inferred from the attempt itself. NO
            # recovery evidence is recorded from an identity outcome: one
            # answered one-shot trigger certifies the session<->PN identity of
            # THIS session, not that the callback route will reach the
            # collector again after the session is lost. Only the dedicated
            # recovery transaction may produce that RecoveryContract proof, so
            # CONF_CONNECTION_STRATEGY_EVIDENCE stays unset here.
            self._verified_connection_strategy = (
                CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
                if self._manual_chosen_strategy
                == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
                else ""
            )
            self._verified_strategy_evidence = ""
            if (
                self._manual_chosen_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
                and not self._callback_continuation.terminal_input.has_proof
            ):
                # An answered one-shot certifies THIS session's identity; it
                # does NOT prove the callback route will regain the collector
                # after the session is lost. A normal callback_on_demand entry
                # is created only after the recovery verification below -- the
                # user consents explicitly first (it reboots the collector).
                return await self.async_step_manual_recovery_confirm()
        if (
            self._manual_result is not None
            and self._manual_result.match is not None
            and self._manual_result.confidence == "high"
        ):
            self._detection_summary_context = "manual"
            return await self.async_step_detection_summary()
        return await self.async_step_manual_confirm()

    async def _async_route_after_manual_callback_failure(
        self,
        verification_error: str,
    ) -> ConfigFlowResult:
        """Show callback failure without creating an incomplete config entry.

        A failed one-shot proves only that the collector was not confirmed during
        THIS attempt. The result is deliberately identity-free: the callback
        matcher already released this attempt's claim and the transaction cleared
        its certified identity; stale successful-attempt strategy evidence is
        cleared here as an additional terminal-path guard. The flow remains open
        with retry/edit actions; no PN-less entry is persisted.

        Reconfigure does not call this helper and remains fail-closed on its own
        form.
        """

        reason = str(verification_error or "callback_timeout").strip()
        result = self._manual_result or OnboardingResult(
            connection_type=self._current_connection_type(),
            connection_mode="manual",
        )
        self._manual_result = replace(
            result,
            # The raw detector result is not attributable to THIS callback once
            # verification failed. Do not display or later enrich from its PN,
            # model, or serial as though they were confirmed; the entered target
            # address remains available from ``_manual_config``.
            collector=None,
            match=None,
            alternative_matches=(),
            next_action="retry_verification",
            last_error=reason,
        )
        # The certified PN/session were already cleared by async_run_identity's
        # reset (a non-certified attempt leaves them empty), so this failure route
        # touches no callback-continuation lifecycle field -- only the strategy.
        self._verified_connection_strategy = ""
        self._verified_strategy_evidence = ""
        return await self.async_step_manual_confirm()

    def _async_show_manual_form(
        self,
        user_input: dict[str, Any] | None,
        errors: dict[str, str],
    ) -> ConfigFlowResult:
        defaults = self._build_manual_defaults(user_input, self._selected_result)
        data_schema = vol.Schema(
            {
                **self._build_connection_fields_schema(
                    self._current_connection_type(),
                    fields=self._connection_branch().form_layout.manual_fields,
                    values=defaults,
                ),
                # The user states HOW the collector connects. This is the CANONICAL
                # connection_strategy and is saved straight into entry.data. An
                # explicit user action may PRE-SELECT it (e.g. "configure a
                # callback connection by hand"), but the user still confirms.
                vol.Required(
                    CONF_CONNECTION_STRATEGY,
                    default=str(
                        defaults.get(CONF_CONNECTION_STRATEGY)
                        or self._manual_preselected_strategy
                        or CONNECTION_STRATEGY_INBOUND
                    ),
                ): _connection_strategy_selector(
                    self._tr(
                        "common.dynamic.connection_strategy_inbound",
                        "Collector connects to Home Assistant on its own",
                    ),
                    self._tr(
                        "common.dynamic.connection_strategy_callback_on_demand",
                        "Ask the collector to connect when needed",
                    ),
                ),
                vol.Required(
                    CONF_DRIVER_DETECTION_STRATEGY,
                    default=str(
                        defaults.get(
                            CONF_DRIVER_DETECTION_STRATEGY,
                            DEFAULT_DRIVER_DETECTION_STRATEGY,
                        )
                    ),
                ): _driver_detection_strategy_selector(self._translation_bundle),
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
            step_id="manual",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "verification_note": self._manual_verification_note(),
            },
        )

    async def _async_manual_inbound_observe(
        self,
        flat_input: dict[str, Any],
    ) -> ConfigFlowResult:
        """Inbound onboarding: observe only. Send ZERO UDP, run NO active probe.

        The user stated the collector dials Home Assistant on its own, so Home
        Assistant must not trigger it and must not run an active auto-detect. It
        only looks at what the shared listener already observed:

        * a strong, unclaimed session whose durable full PN is already known ->
          create the NORMAL collector entry through the existing path;
        * otherwise -> keep this flow open and direct the user to retry or enable
          background discovery. No incomplete config entry is created.

        The collector address is optional here and is kept only as a hint.
        """

        candidates = self._strong_unclaimed_session_observations()
        expected = self._callback_continuation.identity_context.expected_pn
        chosen: ObservedCollectorSession | None = None
        if expected:
            # ONLY a passive-discovery context knows WHICH collector this flow is
            # for. Bind that identity and nothing else.
            #
            # A generic manual inbound flow deliberately has NO such link: a live
            # unclaimed collector -- even the only one on the network right now --
            # is not user consent to bind that identity. It is shown by the same
            # shared inventory in background discovery and every network scan.
            for candidate in candidates:
                if pn_is_same_identity(expected, candidate.collector_pn):
                    chosen = candidate
                    break

        if chosen is None:
            self._manual_result = OnboardingResult(
                connection_type=self._current_connection_type(),
                connection_mode="manual",
                next_action="await_inbound_session",
                last_error="inbound_awaiting_session",
            )
            return await self.async_step_manual_confirm()

        # A durable identity this flow is explicitly FOR is already observable.
        # Own that exact session first: claim_session -> promote to the full PN
        # (prepare_handoff then happens at entry creation).
        if not self._callback_continuation.adopt_passive_inbound_identity(
            chosen.collector_pn, chosen.session_id
        ):
            # Owned by another entry/flow: fail closed and wait for discovery to
            # expose an unclaimed session. Never bind by peer address.
            self._manual_result = OnboardingResult(
                connection_type=self._current_connection_type(),
                connection_mode="manual",
                next_action="await_inbound_session",
                last_error="inbound_awaiting_session",
            )
            return await self.async_step_manual_confirm()
        previous_pn = self._callback_continuation.adopt_certified_pn(
            chosen.collector_pn
        )
        abort = await self._async_propagate_enriched_pn(
            chosen.collector_pn, previous=previous_pn
        )
        if abort is not None:
            return abort
        # The user's choice is the canonical strategy; the evidence is honest --
        # the collector demonstrably dialed in, but nothing was restarted here.
        self._verified_connection_strategy = CONNECTION_STRATEGY_INBOUND
        self._verified_strategy_evidence = EVIDENCE_USER_CONFIRMED_SESSION
        self._manual_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip=str(flat_input.get(CONF_COLLECTOR_IP, "") or ""),
                source="inbound_observed",
                ip=str(flat_input.get(CONF_COLLECTOR_IP, "") or ""),
                connected=True,
                collector=CollectorInfo(collector_pn=chosen.collector_pn),
            ),
            observed_session=chosen,
            connection_type=self._current_connection_type(),
            connection_mode="manual",
            next_action="create_entry",
        )
        return await self.async_step_manual_confirm()

    def _strong_unclaimed_session_observations(
        self,
    ) -> tuple[ObservedCollectorSession, ...]:
        """Return the shared live strong-PN inventory, fail-closed."""

        from ...passive_discovery import get_passive_callback_discovery

        discovery = get_passive_callback_discovery(self.hass)
        snapshot = getattr(discovery, "snapshot_unclaimed_collector_sessions", None)
        if not callable(snapshot):
            return ()
        try:
            observations = tuple(snapshot())
        except Exception:
            logger.debug("Shared inbound candidate snapshot failed", exc_info=True)
            return ()
        return tuple(
            observed
            for observed in observations
            if type(observed) is ObservedCollectorSession
            and observed.has_strong_identity
        )

    def _manual_verification_note(self) -> str:
        """Honest labeling for the prefilled address in the verification context.

        The prefilled value is only the address the observed connection came
        FROM -- behind a router, VPN, or port forward that is not the collector
        itself. Empty outside the verification context.

        The "verification context" is the continuation transaction's expected PN,
        shared by ordinary manual/reconfigure and admission-origin callback paths,
        so the note shows for both.
        """

        scan_route = getattr(self, "_manual_scan_route_address", "")
        if scan_route:
            return self._tr(
                "common.dynamic.manual_scan_route_stage_note",
                "Step 1 of 2: identify the collector reachable at **{collector_ip}**. After it answers, the next step verifies recovery before the device is saved.",
                {"collector_ip": scan_route},
            )
        if not self._callback_continuation.identity_context.expected_pn:
            return ""
        return self._tr(
            "common.dynamic.manual_peer_address_note",
            "The suggested address is only where the collector's connection "
            "came from. If the collector is behind a router, VPN, or port "
            "forwarding, this may be the router's address - enter the "
            "collector address that is reachable from Home Assistant.",
        )

    @_with_translation_bundle
    async def async_step_manual_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        menu_options = []
        last_error = str(getattr(self._manual_result, "last_error", "") or "")
        if (
            last_error in ("callback_session_silent", "onboarding_wire_probe_failed")
            and self._callback_continuation.silent_bootstrap_offer is not None
        ):
            # Advanced recovery for a genuinely SILENT device: the user (and
            # only the user) may pick the bootstrap protocol for exactly one
            # read-only identity query on the next attempt's new session.
            menu_options.append("manual_bootstrap_framed")
            menu_options.append("manual_bootstrap_at")
        menu_options.extend(
            [
                MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
                MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
            ]
        )
        if self._can_offer_smartess_cloud_assist(self._manual_result):
            menu_options.append("manual_smartess_cloud_assist")
        if self._manual_entry_ready_to_save():
            menu_options.insert(0, MANUAL_CONFIRM_ACTION_SAVE)
        if last_error == "inbound_awaiting_session":
            menu_options.append(MANUAL_CONFIRM_ACTION_ENABLE_DISCOVERY)

        return self.async_show_menu(
            step_id="manual_confirm",
            menu_options=menu_options,
            description_placeholders=self._manual_confirm_placeholders(
                self._manual_config,
                self._manual_result,
            ),
        )

    @_with_translation_bundle
    async def async_step_manual_smartess_cloud_assist(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()
        self._smartess_cloud_assist_mode = "manual"
        return await self.async_step_smartess_cloud_assist()

    async def async_step_manual_probe_again(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        # A retry is a FULL new attempt, not a bare re-probe. It must never reuse
        # the previous attempt's baseline, ledger generation, verified PN or
        # claim -- otherwise a second probe that reaches collector B could be
        # combined with the first attempt's proof/claim for collector A.
        if self._manual_chosen_strategy == CONNECTION_STRATEGY_INBOUND:
            return await self._async_manual_inbound_observe(self._manual_config)
        verification_error = await self._async_run_manual_callback_attempt(
            self._manual_config
        )
        if verification_error:
            # Failure leaves nothing behind: no verified PN and no claim. Keep it
            # as an honest result and preserve retry/edit actions.
            return await self._async_route_after_manual_callback_failure(
                verification_error
            )
        return await self._async_route_after_manual_callback_success()

    async def async_step_manual_bootstrap_framed(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """The user explicitly chose the EyeBond framed bootstrap protocol."""

        del user_input
        return await self._async_manual_bootstrap_retry(WIRE_FRAMED)

    async def async_step_manual_bootstrap_at(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """The user explicitly chose the AT command bootstrap protocol."""

        del user_input
        return await self._async_manual_bootstrap_retry(WIRE_AT_TEXT)

    async def _async_manual_bootstrap_retry(self, protocol: str) -> ConfigFlowResult:
        """One FULL new attempt whose silent socket may be probed on ``protocol``.

        The typed intent is the ONLY wire authority for a first-ever silent
        socket: explicit user selection, one attempt, one causally-new session,
        one read-only identity query. Everything else (trigger, claim, matcher,
        reader, prepared handoff) is the one shared identity transaction.
        """

        if not self._manual_config:
            return await self.async_step_manual()
        offer = self._callback_continuation.silent_bootstrap_offer
        if offer is None:
            return await self.async_step_manual_confirm()
        intent = OnboardingWireProbeIntent.for_offer(offer, protocol=protocol)
        verification_error = await self._async_run_manual_callback_attempt(
            self._manual_config,
            bootstrap_probe=intent,
        )
        if verification_error:
            return await self._async_route_after_manual_callback_failure(
                verification_error
            )
        return await self._async_route_after_manual_callback_success()

    async def async_step_manual_edit_settings(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if not self._manual_config:
            return await self.async_step_manual()

        self._callback_continuation.release_unadopted_recovery()
        self._callback_continuation.release_terminal_owner()
        self._manual_defaults = dict(self._manual_config)
        self._manual_result = None
        return await self.async_step_manual()

    async def async_step_manual_save(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Create a normal collector entry only after durable identity proof."""

        del user_input
        if not self._manual_config:
            return await self.async_step_manual()
        if not self._manual_entry_ready_to_save():
            return await self.async_step_manual_confirm()
        return await self._async_create_manual_entry(
            self._manual_config, self._manual_result
        )

    async def async_step_manual_enable_background_discovery(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Continue through the one background-discovery entry path."""

        del user_input
        self._callback_continuation.release_unadopted_recovery()
        self._callback_continuation.release_terminal_owner()
        return await self.async_step_listener()

    def _manual_entry_ready_to_save(self) -> bool:
        """Whether this flow can create a fully identified normal entry."""

        if not self._manual_config:
            return False
        collector_pn = self._callback_continuation.certified_pn
        if not collector_pn and self._manual_result is not None:
            collector_pn = self._collector_pn_for_result(self._manual_result)
        if not collector_pn:
            return False
        if self._manual_chosen_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
            return self._callback_continuation.terminal_input.has_proof
        return True

    @_with_translation_bundle
    async def async_step_manual_recovery_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Explicit consent BEFORE the recovery experiment touches the collector.

        The identity transaction proved WHO is on the wire; it did not prove
        the callback route can regain the collector after the session is lost
        (the exact gap behind the silent-socket deadlock regression). The user
        is told honestly what verification does -- reboot, a bounded inbound
        wait, at most ONE addressed trigger -- and nothing runs without their
        explicit choice.
        """

        del user_input
        if not self._manual_config or not self._callback_continuation.certified_pn:
            return await self.async_step_manual()
        menu_options = [
            "manual_recovery_verify",
            MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
            MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
        ]
        return self.async_show_menu(
            step_id="manual_recovery_confirm",
            menu_options=menu_options,
            description_placeholders={
                "collector_pn": self._callback_continuation.certified_pn,
                "collector_ip": str(
                    self._manual_config.get(CONF_COLLECTOR_IP) or ""
                ).strip(),
            },
        )

    async def async_step_manual_recovery_verify(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Run the callback recovery transaction as a bounded progress task."""

        del user_input
        if not self._manual_config or not self._callback_continuation.certified_pn:
            return await self.async_step_manual()
        task = self._manual_recovery_task
        if task is None:
            task = self.hass.async_create_task(
                self._async_run_manual_recovery_transaction()
            )
            self._manual_recovery_task = task
        if task.done():
            self._manual_recovery_task = None
            return self.async_show_progress_done(next_step_id="manual_recovery_result")
        return self.async_show_progress(
            step_id="manual_recovery_verify",
            progress_action="manual_recovery_verify",
            progress_task=task,
            description_placeholders={
                "collector_pn": self._callback_continuation.certified_pn,
            },
        )

    def _manual_callback_recovery_route(
        self, settings: dict[str, Any]
    ) -> CallbackRecoveryRoute:
        """Build the recovery route ONLY from validated form/listener config.

        * ``trigger_target_ip`` -- the explicitly entered collector address;
        * ``trigger_udp_port`` -- the configured UDP port;
        * ``advertised_ha_host``/``advertised_ha_port`` -- the explicit
          advertised endpoint when configured, else the canonical configured
          HA server host / listener TCP port (NAT: the advertised value goes
          into the payload verbatim and is never replaced by a bind address);
        * ``listener_port`` -- the real configured listener port;
        * ``bind_ip`` -- the configured server host the existing listener
          setup binds its sender on.

        NOTHING here comes from the connected socket's peer IP, hostnames,
        cloud family, endpoint classification, collector kind/driver or a
        persisted expected session protocol.
        """

        server_ip = str(settings.get(CONF_SERVER_IP) or "").strip()
        tcp_port = int(settings.get(CONF_TCP_PORT) or 0)
        advertised_host = (
            str(settings.get(CONF_ADVERTISED_SERVER_IP) or "").strip() or server_ip
        )
        advertised_port = int(settings.get(CONF_ADVERTISED_TCP_PORT) or 0) or tcp_port
        return CallbackRecoveryRoute(
            bind_ip=server_ip,
            trigger_target_ip=str(settings.get(CONF_COLLECTOR_IP) or "").strip(),
            trigger_udp_port=int(settings.get(CONF_UDP_PORT) or 0),
            advertised_ha_host=advertised_host,
            advertised_ha_port=advertised_port,
            listener_port=tcp_port,
        )

    async def _async_run_manual_recovery_transaction(self) -> None:
        """THE production caller of the callback recovery transaction.

        Immutable attempt input: the durable full PN and the exact live
        session id certified by THIS attempt's identity transaction. The
        identity transaction's prepared owner is released first (its job --
        session<->PN identity -- is done); the recovery wrapper then claims
        exactly the saved session id under its own ``callback_recovery:<uuid>``
        owner. The socket is never re-found by IP or PN.
        """

        self._manual_recovery_error = ""
        route = self._manual_callback_recovery_route(self._manual_config)
        # The seam owns the certified-session validation, the ownership hand-over
        # (identity owner out, recovery owner in), the ONE callback recovery
        # authority, and holding the typed outcome. This step only builds the
        # typed route, calls the seam, and maps its typed result.
        try:
            outcome = await self._callback_continuation.async_run_recovery(route)
        except asyncio.CancelledError:
            # The wrapper's own finally released its claim; the flow holds
            # nothing (the identity owner was released inside the seam).
            raise
        except Exception:
            logger.exception("Manual callback recovery transaction failed")
            self._manual_recovery_error = "recovery_transaction_failed"
            return
        if outcome is None:
            # No certified PN/session/registry: the wire was never touched.
            self._manual_recovery_error = "recovery_session_unavailable"

    @_with_translation_bundle
    async def async_step_manual_recovery_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Map the typed recovery outcome; never guess a strategy."""

        del user_input
        outcome = self._callback_continuation.recovery_outcome
        if outcome is not None and outcome.callback_verified:
            # The proven route matches the user's chosen strategy: adopt and
            # create in one explicit continuation.
            return await self._async_finalize_recovery_entry(
                CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
            )
        if outcome is not None and outcome.inbound_recovered:
            # The collector proved AUTONOMOUS reconnection -- zero triggers
            # were sent. This is honestly surfaced; it never silently becomes
            # a callback_on_demand entry.
            return await self.async_step_manual_recovery_inbound_confirm()
        failure = (
            self._manual_recovery_error
            or str(getattr(outcome, "failure_reason", "") or "")
            or "callback_recovery_timeout"
        )
        self._callback_continuation.release_unadopted_recovery()
        self._manual_recovery_error = failure
        return await self.async_step_manual_recovery_failed()

    async def _async_finalize_recovery_entry(self, strategy: str) -> ConfigFlowResult:
        """Adopt the outcome's exact prepared owner, then run the terminal path.

        The producer owns the prepared owner until adoption succeeds: a False/
        raising adoption releases exactly ``outcome.handoff_owner`` through the
        same domain registry (never by PN lookup) and routes to the typed
        failure menu. Only AFTER successful adoption is the user's explicit
        strategy intent recorded; the existing Batch-5 terminal then owns the
        whole persistence + handoff lifecycle.
        """

        outcome = self._callback_continuation.consume_recovery_outcome()
        if outcome is None:
            self._manual_recovery_error = "recovery_session_unavailable"
            return await self.async_step_manual_recovery_failed()
        try:
            adopted = self._callback_continuation.adopt_recovery(outcome)
        except Exception:
            logger.exception("Callback recovery adoption failed")
            adopted = False
        if not adopted:
            self._callback_continuation.release_exact_recovery_owner(outcome)
            self._manual_recovery_error = "recovery_ownership_unavailable"
            return await self.async_step_manual_recovery_failed()
        self._verified_connection_strategy = strategy
        self._verified_strategy_evidence = ""
        return await self._async_create_manual_entry(
            self._manual_config, self._manual_result
        )

    @_with_translation_bundle
    async def async_step_manual_recovery_inbound_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """The collector reconnected on its own: ask before saving it as inbound."""

        del user_input
        outcome = self._callback_continuation.recovery_outcome
        if outcome is None or not outcome.inbound_recovered:
            return await self.async_step_manual_recovery_failed()
        return self.async_show_menu(
            step_id="manual_recovery_inbound_confirm",
            menu_options=[
                "manual_recovery_accept_inbound",
                "manual_recovery_decline_inbound",
            ],
            description_placeholders={
                "collector_pn": str(outcome.collector_pn or ""),
            },
        )

    async def async_step_manual_recovery_accept_inbound(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        # The user explicitly confirmed the proven autonomous reconnection:
        # THAT confirmation is the inbound intent (never inferred from the
        # proof itself). The inbound proof travels through the same
        # RecoveryTerminalInput/terminal path.
        return await self._async_finalize_recovery_entry(CONNECTION_STRATEGY_INBOUND)

    async def async_step_manual_recovery_decline_inbound(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._callback_continuation.release_unadopted_recovery()
        self._manual_recovery_error = "recovery_inbound_declined"
        return await self.async_step_manual_recovery_failed()

    def _recovery_failure_explanation(self, code: str) -> str:
        return _shared_recovery_failure_explanation(self._tr, code)

    @_with_translation_bundle
    async def async_step_manual_recovery_failed(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Typed recovery failure with the explicit next actions preserved."""

        del user_input
        self._callback_continuation.release_unadopted_recovery()
        menu_options = [
            MANUAL_CONFIRM_ACTION_PROBE_AGAIN,
            MANUAL_CONFIRM_ACTION_EDIT_SETTINGS,
        ]
        return self.async_show_menu(
            step_id="manual_recovery_failed",
            menu_options=menu_options,
            description_placeholders={
                "collector_pn": self._callback_continuation.certified_pn
                or self._callback_continuation.identity_context.expected_pn,
                "failure_explanation": self._recovery_failure_explanation(
                    self._manual_recovery_error or ""
                ),
            },
        )
