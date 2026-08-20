"""Observed-session admission and verification lifecycle."""

from __future__ import annotations

import asyncio
import logging
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

from .config_common import (
    _ONBOARDING_TIMEOUT_POLICY,
    _PASSIVE_LISTENER_HOST,
    _is_ipv4,
)
from .connection.admission import CollectorAdmissionRequest, ObservedCollectorSession
from .connection.admission_transaction import (
    CollectorAdmissionTransaction,
)
from .connection.callback_identity import (
    IDENTITY_SILENT_SESSION_STALE,
    CallbackIdentityOutcome,
    CallbackIdentityRequest,
    ObservedSessionWireProbeIntent,
)
from .connection.recovery.verification import (
    CallbackRecoveryRoute,
)
from .connection.spec_factory import (
    build_connection_spec_from_values,
)
from .const import (
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
)
from .flow_translation import (
    with_translation_bundle as _with_translation_bundle,
)
from .models import (
    OnboardingResult,
)

logger = logging.getLogger(__name__)


class CollectorAdmissionFlowMixin:
    """Observed-session admission and verification lifecycle."""

    async def _async_begin_collector_admission(
        self,
        request: CollectorAdmissionRequest,
    ) -> ConfigFlowResult:
        """The ONE config-flow entrypoint that admits an observed collector session.

        Integration discovery and the passive phase of a user-started scan are
        merely two source adapters for the same physical fact: each builds a
        :class:`CollectorAdmissionRequest` and calls this. From here the
        algorithm cannot tell where the request came from -- ``origin`` is a
        diagnostic label only. Neither source may persist ``inbound`` from an
        observed ``callback_listener`` socket alone: the EXACT session must
        survive the existing controlled restart/reconnect transaction, which
        this entrypoint drives through the ONE inbound verifier (it never mints a
        second verifier or a source-specific admission algorithm).
        """

        if type(request) is not CollectorAdmissionRequest:
            # Trust boundary: a duck-typed request cannot authorize admission.
            raise TypeError("collector_admission_request_required")
        # The neutral transaction owns the request, working PN, session
        # resolution, claim, channel, probe, verifier, outcome and handoff. The
        # flow injects only the registry lookup, the listener host and the
        # (patchable) timeout policy -- never reads the transaction's registry or
        # owner directly.
        transaction = CollectorAdmissionTransaction(
            request,
            registry_provider=self._callback_session_registry,
            listener_host=_PASSIVE_LISTENER_HOST,
            policy_provider=lambda: _ONBOARDING_TIMEOUT_POLICY,
            hass_provider=lambda: self.hass,
        )
        self._admission_transaction = transaction
        # THE source boundary: for an admission-origin flow the ONE
        # transaction owns the whole attempt -- inbound AND, if the user continues
        # by callback, identity + recovery + terminal. Choosing the continuation
        # here (once) is why no shared callback/recovery/terminal step ever
        # branches on the admission transaction; they all use
        # ``self._callback_continuation``.
        self._callback_continuation = transaction
        return await self.async_step_verify_connection()

    async def _async_admit_selected_scan_result(
        self,
        *,
        callback_route: CallbackRecoveryRoute | None = None,
    ) -> ConfigFlowResult | None:
        """Admit a selected scan identity only with an exact typed route.

        A passive TCP observation alone can never enter this adapter.  Its route
        either came from the active scan that exercised it for this exact
        session, or from the user's explicit editable route choice.  Only that
        typed route plus the exact observed session may enter the shared
        admission/recovery transaction.
        """

        result = self._selected_result
        observed = getattr(result, "observed_session", None) if result else None
        # EXACT type identity, never isinstance: a duck / subclass observation
        # must fail closed (continue the normal scan, start NO admission) rather
        # than reach the strict CollectorAdmissionRequest constructor and raise an
        # unhandled TypeError into the flow.
        if (
            type(observed) is not ObservedCollectorSession
            or type(callback_route) is not CallbackRecoveryRoute
        ):
            return None
        return await self._async_begin_collector_admission(
            CollectorAdmissionRequest(
                observed_session=observed,
                origin="scan_selected_route",
                callback_route=callback_route,
            )
        )

    async def _async_continue_selected_scan_result(self) -> ConfigFlowResult:
        """One continuation for auto, selected and driver-choice scan results."""

        result = self._selected_result
        observed = getattr(result, "observed_session", None) if result else None
        if type(observed) is ObservedCollectorSession:
            # An active scan may already carry the exact typed route it
            # exercised to obtain this exact session.  Preserve that causal
            # result and enter the shared admission transaction immediately;
            # asking the user to choose the same address again only lets the
            # short-lived scan session expire before verification starts.
            #
            # A passive/inventory session has no such route authority (its TCP
            # peer may be a router/NAT address), so it still needs the explicit
            # editable route step below.  No PN<->peer-IP guess is introduced.
            callback_route = getattr(result, "callback_route", None)
            if type(callback_route) is CallbackRecoveryRoute:
                admission = await self._async_admit_selected_scan_result(
                    callback_route=callback_route
                )
                if admission is not None:
                    return admission
            matching_route = self._single_scan_route_matching_observed_peer(result)
            if matching_route:
                admission = await self._async_admit_selected_scan_result(
                    callback_route=self._scan_callback_route(
                        result=result,
                        observed=observed,
                        address=matching_route,
                    )
                )
                if admission is not None:
                    return admission
            return await self.async_step_scan_collector_route()
        if self._selected_result is not None and self._is_route_scan_result(
            self._selected_result
        ):
            # A UDP reply proves only that this ROUTE is worth trying; it does
            # not identify a collector and can never enter entry creation.  The
            # user's selection explicitly asks to identify that address, so
            # continue through the existing callback identity + controlled
            # recovery transaction with the route prefilled.  No PN/peer-IP
            # association and no second matcher is introduced here.
            route_address = str(
                self._selected_result.collector.ip
                if self._selected_result.collector is not None
                else ""
            ).strip()
            self._prepare_scan_route_manual(
                address=route_address,
            )
            return await self.async_step_manual()
        # A scan-time inverter match is useful display evidence, but it does not
        # own the durable binding.  Every identified collector continues to the
        # same collector confirmation; runtime detection follows after setup.
        return await self.async_step_confirm()

    def _scan_collector_route_options(self, result: OnboardingResult) -> dict[str, str]:
        """Return independent route hints for one identified scan session.

        No item is associated with the PN here.  UDP targets and the TCP peer
        are separate observations; selecting one merely authorizes a callback
        identity attempt whose expected PN is already fixed by ``result``.
        """

        options: dict[str, str] = {}

        def add(address: object, *, peer: bool = False) -> None:
            if type(address) is not str:
                return
            normalized = address.strip()
            if not normalized or normalized != address or not _is_ipv4(normalized):
                return
            if normalized in options:
                return
            if peer:
                options[normalized] = self._tr(
                    "common.dynamic.scan_route_peer_option",
                    "{address} — source of the incoming connection (may be a router)",
                    {"address": normalized},
                )
            else:
                options[normalized] = self._tr(
                    "common.dynamic.scan_route_responded_option",
                    "{address} — address responded during the scan",
                    {"address": normalized},
                )

        # Preserve every address observation as its own choice.  A route carried
        # by an active result is still only an observed target here, never an
        # automatic PN<->IP association.
        route = result.callback_route
        if type(route) is CallbackRecoveryRoute:
            add(route.trigger_target_ip)
        responded_addresses = set(self._scan_responded_addresses)
        for candidate in self._autodetect_results.values():
            if not self._is_route_scan_result(candidate):
                continue
            collector = candidate.collector
            address = collector.ip if collector is not None else ""
            if type(address) is str:
                responded_addresses.add(address)
        for address in sorted(responded_addresses):
            add(address)

        observed = result.observed_session
        if type(observed) is ObservedCollectorSession:
            add(observed.peer_hint, peer=True)
        elif result.collector is not None:
            add(result.collector.ip, peer=True)
        return options

    def _single_scan_route_matching_observed_peer(
        self,
        result: OnboardingResult,
    ) -> str:
        """Return one unambiguous scan target equal to the exact session peer.

        This is only a UX shortcut. It requires an independently observed,
        single UDP-responsive address equal to the selected exact session peer.
        The ordinary callback identity and recovery transaction still has to
        prove the same PN before entry creation.

        The peer itself is never counted as a response, so a passive or stale
        callback session cannot manufacture its own route authority.
        """

        observed = result.observed_session
        if type(observed) is not ObservedCollectorSession:
            return ""
        peer = observed.peer_hint
        if type(peer) is not str or peer != peer.strip() or not _is_ipv4(peer):
            return ""

        responded_addresses = set(self._scan_responded_addresses)
        for candidate in self._autodetect_results.values():
            if not self._is_route_scan_result(candidate):
                continue
            collector = candidate.collector
            address = collector.ip if collector is not None else ""
            if (
                type(address) is str
                and address == address.strip()
                and _is_ipv4(address)
            ):
                responded_addresses.add(address)
        return peer if responded_addresses == {peer} else ""

    def _prepare_scan_route_manual(
        self,
        *,
        address: str,
    ) -> None:
        """Prepare identification of one route-only scan result."""

        self._replace_manual_callback_continuation()
        self._manual_declared_expected_pn = None
        self._manual_result = None
        self._manual_preselected_strategy = CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        self._manual_scan_route_address = address
        self._manual_defaults[CONF_COLLECTOR_IP] = address

    def _scan_callback_route(
        self,
        *,
        result: OnboardingResult,
        observed: ObservedCollectorSession,
        address: str,
    ) -> CallbackRecoveryRoute:
        """Build the typed route selected for an already identified session."""

        existing = result.callback_route
        if type(existing) is CallbackRecoveryRoute:
            return CallbackRecoveryRoute(
                bind_ip=existing.bind_ip,
                trigger_target_ip=address,
                trigger_udp_port=existing.trigger_udp_port,
                advertised_ha_host=existing.advertised_ha_host,
                advertised_ha_port=existing.advertised_ha_port,
                listener_port=observed.listener_port,
            )

        values = dict(self._auto_connection_defaults(), **self._auto_config)
        spec = build_connection_spec_from_values(
            self._current_connection_type(), values
        )
        return CallbackRecoveryRoute(
            bind_ip=spec.server_ip,
            trigger_target_ip=address,
            trigger_udp_port=spec.udp_port,
            advertised_ha_host=spec.effective_advertised_server_ip,
            advertised_ha_port=spec.effective_advertised_tcp_port,
            listener_port=observed.listener_port,
        )

    @_with_translation_bundle
    async def async_step_scan_collector_route(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Choose a reachable route for one strongly identified scan session."""

        result = self._selected_result
        observed = getattr(result, "observed_session", None) if result else None
        if type(observed) is not ObservedCollectorSession:
            return await self.async_step_scan_results()

        options = self._scan_collector_route_options(result)
        errors: dict[str, str] = {}
        if user_input is not None:
            raw_address = user_input.get(CONF_COLLECTOR_IP)
            if (
                type(raw_address) is not str
                or raw_address != raw_address.strip()
                or not _is_ipv4(raw_address)
            ):
                errors[CONF_COLLECTOR_IP] = "invalid_ip"
            else:
                route = self._scan_callback_route(
                    result=result,
                    observed=observed,
                    address=raw_address,
                )
                # Preserve the explicit choice for the failure/edit path. It
                # remains a scan-origin attempt and must identify a collector
                # before any entry can be created.
                self._manual_scan_route_address = raw_address
                self._manual_defaults[CONF_COLLECTOR_IP] = raw_address
                admission = await self._async_admit_selected_scan_result(
                    callback_route=route
                )
                if admission is not None:
                    return admission
                errors[CONF_COLLECTOR_IP] = "invalid_selection"

        selector = SelectSelector(
            SelectSelectorConfig(
                options=[
                    SelectOptionDict(value=address, label=label)
                    for address, label in options.items()
                ],
                custom_value=True,
                mode=SelectSelectorMode.DROPDOWN,
            )
        )
        field = (
            vol.Required(CONF_COLLECTOR_IP, default=next(iter(options)))
            if len(options) == 1
            else vol.Required(CONF_COLLECTOR_IP)
        )
        return self.async_show_form(
            step_id="scan_collector_route",
            data_schema=vol.Schema({field: selector}),
            errors=errors,
            description_placeholders={
                "collector_pn": observed.collector_pn,
                "peer_ip": observed.peer_hint
                or self._tr("common.dynamic.unknown", "Unknown"),
                "route_candidates": "\n".join(
                    f"- {label}" for label in options.values()
                )
                or self._tr(
                    "common.dynamic.scan_route_no_candidates",
                    "No route address was observed; enter one manually.",
                ),
            },
        )

    async def _async_continue_after_verification(self) -> ConfigFlowResult:
        return await self.async_step_confirm()

    @_with_translation_bundle
    async def async_step_verify_connection(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Consent step: verifying restarts the collector and interrupts the link."""

        transaction = self._admission_transaction
        if transaction is None:
            return await self._async_continue_after_verification()
        if user_input is not None:
            return await self.async_step_verify_connection_progress()
        verification_values = {
            "collector_pn": str(transaction.expected_pn or ""),
            "peer_ip": str(transaction.peer_hint or ""),
            "selected_route": str(
                transaction.request.callback_route.trigger_target_ip
                if transaction.request.callback_route is not None
                else ""
            ),
        }
        verification_explanation = (
            self._tr(
                (
                    "common.dynamic.verify_active_callback_retry_explanation"
                    if transaction.state == "callback_ready"
                    else "common.dynamic.verify_active_callback_explanation"
                ),
                (
                    "Checking address **{selected_route}** again for collector **{collector_pn}**. Home Assistant will verify the connection and its recovery after a restart."
                    if transaction.state == "callback_ready"
                    else "Collector **{collector_pn}** was found. Home Assistant will verify the selected address **{selected_route}** and make sure the same collector restores its connection after a restart."
                ),
                verification_values,
            )
            if transaction.request.callback_route is not None
            else self._tr(
                "common.dynamic.verify_inbound_explanation",
                "Collector **{collector_pn}** was found at {peer_ip}. Home Assistant will restart it and verify that it reconnects on its own. After the current connection closes, Home Assistant waits up to one minute for the replacement.",
                verification_values,
            )
        )
        return self.async_show_form(
            step_id="verify_connection",
            data_schema=vol.Schema({}),
            description_placeholders={
                # The expected PN follows any weak->strong enrichment; the peer
                # is only an editable route hint, never identity.
                "collector_pn": str(transaction.expected_pn or ""),
                "peer_ip": str(transaction.peer_hint or ""),
                "verification_explanation": verification_explanation,
            },
        )

    async def async_step_verify_connection_progress(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Run the restart/reconnect verification as a bounded progress task."""

        del user_input
        transaction = self._admission_transaction
        task = self._admission_task
        if task is None and transaction is not None:
            if transaction.request.callback_route is not None:
                task = self.hass.async_create_task(
                    self._async_run_observed_callback_admission(transaction)
                )
            else:
                task = self.hass.async_create_task(transaction.async_run())
            self._admission_task = task
        if task is None or task.done():
            self._admission_task = None
            return self.async_show_progress_done(
                next_step_id="verify_connection_result"
            )
        return self.async_show_progress(
            step_id="verify_connection_progress",
            progress_action=(
                "active_scan_recovery_verify"
                if transaction is not None
                and transaction.request.callback_route is not None
                else "verify_connection"
            ),
            progress_task=task,
            description_placeholders={
                "collector_pn": str(
                    transaction.expected_pn if transaction is not None else ""
                ),
            },
        )

    async def _async_run_observed_callback_admission(
        self, transaction: CollectorAdmissionTransaction
    ) -> None:
        """Prove the selected route, then prove recovery through that route.

        A still-live exact scan session is adopted without a second trigger and
        enters the controlled recovery transaction, whose post-reset callback
        proves the selected route.  If that scan session has already closed, its
        zero-send bootstrap is followed immediately by one normal addressed
        identity attempt.  Every accepted session must carry the same PN.
        """

        self._admission_callback_error = ""
        route = transaction.request.callback_route
        if type(route) is not CallbackRecoveryRoute:
            self._admission_callback_error = "callback_route_invalid"
            return
        try:
            bootstrap_probe = None
            if transaction.state == "ready":
                transaction.begin_observed_callback_continuation()
                bootstrap_probe = transaction.observed_wire_probe_intent()
            elif transaction.state != "callback_ready":
                raise RuntimeError("selected_route_transaction_not_retryable")
            context = transaction.identity_context

            async def _run_identity(probe: object = None) -> CallbackIdentityOutcome:
                return await transaction.async_run_identity(
                    CallbackIdentityRequest(
                        server_ip=route.bind_ip,
                        tcp_port=route.listener_port,
                        udp_port=route.trigger_udp_port,
                        target_ip=route.trigger_target_ip,
                        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                        expected_pn=context.expected_pn,
                        old_session_id=context.old_session_id,
                        owner_prefix="active_scan_identity",
                        bootstrap_probe=probe,
                    )
                )

            identity = await _run_identity(bootstrap_probe)
            if (
                bootstrap_probe is not None
                and not identity.identity_certified
                and identity.result == IDENTITY_SILENT_SESSION_STALE
            ):
                # The scan's exact socket disappeared between rendering the
                # results and the user's confirmation.  That bootstrap sent
                # zero datagrams and owns nothing, so immediately perform the
                # normal addressed identity attempt the user authorized instead
                # of presenting an instant failure before testing the selected
                # route.  No other failure (wire, conflict, mismatch, ambiguity)
                # may take this continuation.
                context = transaction.identity_context
                identity = await _run_identity()
            if (
                bootstrap_probe is not None
                and not identity.identity_certified
                and identity.silent_bootstrap_offer is not None
            ):
                # The addressed attempt produced exactly one causally-bound
                # silent socket.  Reuse only the wire observed on the selected
                # scan session and probe that exact offer with zero additional
                # UDP sends.  This is not a model/PN/IP protocol guess.
                identity = await _run_identity(
                    ObservedSessionWireProbeIntent.for_silent_offer(
                        identity.silent_bootstrap_offer,
                        observed=bootstrap_probe,
                    )
                )
            if not identity.identity_certified:
                self._admission_callback_error = identity.result or "callback_timeout"
                return
            transaction.adopt_certified_pn(identity.collector_pn)
            outcome = await transaction.async_run_recovery(route)
            if outcome is None:
                self._admission_callback_error = "recovery_session_unavailable"
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Active-scan collector admission failed")
            self._admission_callback_error = "recovery_transaction_failed"

    async def async_step_verify_connection_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Route on the verification outcome; never guess a strategy."""

        del user_input
        transaction = self._admission_transaction
        if transaction is None:
            return await self._async_continue_after_verification()
        # The transaction already adopted any weak->strong enriched FULL PN into
        # its working identity; the flow only PROPAGATES that PN through its own
        # models (unique_id, candidates, title) and aborts on a late collision.
        # ``previous`` is the ORIGINAL observation PN the candidates still carry.
        abort = await self._async_propagate_enriched_pn(
            transaction.expected_pn,
            previous=transaction.request.observed_session.collector_pn,
        )
        if abort is not None:
            return abort
        if transaction.request.callback_route is not None:
            outcome = transaction.recovery_outcome
            if outcome is not None and (
                outcome.callback_verified or outcome.inbound_recovered
            ):
                consumed = transaction.consume_recovery_outcome()
                adopted = bool(
                    consumed is not None and transaction.adopt_recovery(consumed)
                )
                if adopted:
                    self._verified_connection_strategy = (
                        CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
                        if outcome.callback_verified
                        else CONNECTION_STRATEGY_INBOUND
                    )
                    self._verified_strategy_evidence = ""
                    return await self._async_continue_after_verification()
                self._admission_callback_error = "recovery_ownership_unavailable"
            else:
                self._admission_callback_error = (
                    self._admission_callback_error
                    or str(getattr(outcome, "failure_reason", "") or "")
                    or "callback_recovery_timeout"
                )
                transaction.release_unadopted_recovery()
            logger.info(
                "Active-scan recovery not confirmed (%s)",
                self._admission_callback_error,
            )
            return await self.async_step_verify_connection_failed()
        if transaction.verified:
            # The strategy is the INTENT of this passive-discovery inbound flow;
            # the verifier proved RECOVERY, not strategy. No legacy
            # reboot_reconnect evidence is written anymore -- the typed proof
            # becomes the entry's RecoveryContract at creation.
            self._verified_connection_strategy = CONNECTION_STRATEGY_INBOUND
            self._verified_strategy_evidence = ""
            # The transaction (chosen as this flow's continuation) owns the
            # verified inbound proof and the successful claim: the terminal reads
            # ``seam.terminal_input`` (== ``transaction.terminal_input``) and
            # prepares/commits through the same continuation. No admission-origin
            # recovery/owner state is copied onto the flow. The transaction is deliberately
            # NOT cleared here -- it holds the claim until the terminal handoff and
            # is cleared on the cancel / async_remove paths.
            return await self._async_continue_after_verification()
        # Inbound is NOT confirmed. Do not auto-classify as callback_on_demand:
        # continue this same flow on the existing manual step, whose one-shot
        # callback attempt provides the behavioral proof for that strategy. The
        # address field is prefilled with the observed peer IP purely as an
        # editable hint (it may be a router/NAT address, not the collector).
        logger.info(
            "Passive discovery inbound verification not confirmed (%s); offering retry or manual callback",
            transaction.failure_reason or "verification_unavailable",
        )
        return await self.async_step_verify_connection_failed()

    @_with_translation_bundle
    async def async_step_verify_connection_failed(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Keep a failed inbound verification in this flow, honestly explained.

        The autonomous-reconnect check did not prove a permanent inbound
        collector. The exact typed reason is surfaced in the user's language
        (the SAME recovery-explanation mapping the manual callback path uses),
        and three EXPLICIT actions are offered -- retry the inbound check,
        deliberately configure a callback connection by hand, or cancel. The
        flow never auto-classifies the collector as callback_on_demand and
        never sends a UDP trigger from here.
        """

        del user_input
        transaction = self._admission_transaction
        failure_reason = self._admission_callback_error or (
            transaction.failure_reason if transaction is not None else ""
        )
        return self.async_show_menu(
            step_id="verify_connection_failed",
            menu_options=[
                "verify_connection_retry",
                "verify_connection_manual_callback",
                "verify_connection_cancel",
            ],
            description_placeholders={
                "collector_pn": (
                    transaction.expected_pn if transaction is not None else ""
                ),
                "failure_explanation": self._recovery_failure_explanation(
                    failure_reason
                ),
            },
        )

    async def async_step_verify_connection_retry(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Retry the behavioral check from its consent step in the same flow.

        The transaction fully releases its old failed claim and clears the
        outcome, KEEPING the weak->strong enriched full PN so the retry resolves
        the replacement full-PN session.
        """

        del user_input
        transaction = self._admission_transaction
        if transaction is not None and transaction.request.callback_route is not None:
            # Retry the SAME explicitly selected route.  The failed identity or
            # recovery attempt has already returned the transaction to
            # callback_ready and released its owner.  Re-scanning here was both
            # surprising UX and discarded the route the user had just chosen.
            # The next progress task performs a full new addressed identity
            # attempt; it never reuses the previous proof/session.
            self._admission_callback_error = ""
            self._admission_task = None
            return await self.async_step_verify_connection()
        if transaction is not None:
            transaction.reset_for_retry()
        self._admission_task = None
        return await self.async_step_verify_connection()

    async def async_step_verify_connection_manual_callback(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """EXPLICIT user intent: configure a callback connection by hand.

        This is the only bridge from a failed inbound verification to the
        callback path, and it happens ONLY because the user chose it. The SAME
        admission transaction continues to own the attempt (it was chosen as the
        continuation at the source boundary): it is transitioned from the failed
        inbound attempt into its callback-ready lifecycle -- NOT closed, and its
        identity/session authority is not copied into flow state. The
        existing manual step then runs the callback identity/recovery path through
        that same continuation, with callback_on_demand pre-selected (the user
        asked for it, never inferred) and the observed peer IP prefilled purely as
        an editable route hint.
        """

        del user_input
        transaction = self._admission_transaction
        self._admission_task = None
        if transaction is not None:
            if transaction.request.callback_route is not None:
                # The automatic recovery released its failed outcome back to
                # CALLBACK_READY. Keep the SAME transaction/identity context;
                # the manual submit runs a full new one-trigger attempt through
                # the shared continuation, with no PN/session hand-across.
                self._manual_preselected_strategy = (
                    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
                )
                return await self.async_step_manual()
            # Clear the completed inbound attempt and enter the callback-ready
            # lifecycle IN the same transaction (its inbound owner was already
            # released and channels closed on the failure path). No hand-across,
            # no transaction close and no owner/PN/session copy.
            transaction.begin_callback_continuation()
        # Only a form DEFAULT; the manual step still requires the user to
        # submit, and the transaction's callback identity/recovery path proves it.
        self._manual_preselected_strategy = CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        return await self.async_step_manual()

    async def async_step_verify_connection_cancel(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Cancel the discovery flow cleanly: no claim, task, channel or card.

        The inbound verification claim was already released on the failure
        path; release any stray recovery state and abort so the discovery card
        disappears and the exact same session is not immediately republished.
        """

        del user_input
        transaction = self._admission_transaction
        self._admission_transaction = None
        self._admission_task = None
        if transaction is not None:
            # Close channels then release the admission owner (idempotent).
            await transaction.async_close()
        self._callback_continuation.release_unadopted_recovery()
        return self.async_abort(reason="discovery_cancelled")

    async def _async_propagate_enriched_pn(self, enriched: str, *, previous: str):
        """Propagate one enriched FULL PN through the flow's OWN models.

        The flow unique_id, the selected onboarding candidate (whose CollectorInfo
        feeds CONF_COLLECTOR_PN and the entry title), every matching autodetect
        candidate, and the dialog title placeholders adopt ``enriched``. Candidates
        that already carry a DIFFERENT collector's PN (``!= previous``) are left
        untouched. Returns an ``already_configured`` abort when the full PN is now
        configured, else ``None``. Never starts a second flow.
        """

        enriched = str(enriched or "").strip()
        previous = str(previous or "").strip()
        if not enriched or enriched == previous:
            return None
        discovery_context = self.context.get("eybond_discovery")
        if isinstance(discovery_context, dict):
            discovery_context[CONF_COLLECTOR_PN] = enriched
        for candidate in (
            self._selected_result,
            *self._autodetect_results.values(),
        ):
            collector_info = getattr(
                getattr(candidate, "collector", None), "collector", None
            )
            if collector_info is None:
                continue
            candidate_pn = str(
                getattr(collector_info, "collector_pn", "") or ""
            ).strip()
            if candidate_pn and previous and candidate_pn != previous:
                continue
            collector_info.collector_pn = enriched
        self.context["title_placeholders"] = {"name": f"Collector PN {enriched}"}
        await self.async_set_unique_id(f"collector:{enriched}")
        return self._abort_if_unique_id_configured()

    def _callback_session_registry(self):
        from .passive_discovery import get_callback_session_registry

        try:
            return get_callback_session_registry(self.hass)
        except Exception:
            return None
