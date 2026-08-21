"""Extracted EyeBond options-flow lifecycle: StrategyTransitionOptionsMixin."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import (
    BooleanSelector,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from ...collector_identity import pn_is_same_identity
from ...collector_endpoint import (
    CollectorEndpointWriteShape,
    resolve_collector_endpoint_write_shape,
)
from ...connection.connection_policy import (
    resolve_connection_strategy,
)
from ...connection.operating_profile import (
    OPERATING_PROFILE_CLOUD_AND_HA,
    OPERATING_PROFILE_CUSTOM,
    OPERATING_PROFILE_HA_ONLY,
)
from ...connection.strategy_transition_context import (
    PROVENANCE_EXPLICIT_ADVERTISED,
    TransitionEndpointCandidate,
    earned_advertised_route,
    normalized_advertised_host,
    parse_advertised_port,
    resolve_cloud_rollback_endpoint,
    resolve_default_ha_endpoint,
)
from ..common.connection_form import (
    IP_TEXT_SELECTOR as _IP_TEXT_SELECTOR,
)
from ..common.connection_form import (
    PORT_SELECTOR as _PORT_SELECTOR,
)
from ...const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_STRATEGY,
    CONF_ENDPOINT_CONTROL_POLICY,
    CONF_ENDPOINT_WRITTEN_AT,
    CONF_ENDPOINT_WRITTEN_VALUE,
    CONF_SERVER_IP,
    CONF_STRATEGY_TRANSITION_STATE,
    CONF_TCP_PORT,
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    DEFAULT_TCP_PORT,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
)
from ..common.presentation import (
    _connection_strategy_selector,
    _flatten_sections,
    _shared_recovery_failure_explanation,
)
from ..common.translation import with_translation_bundle as _with_translation_bundle
from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY

logger = logging.getLogger(__name__)


CONF_CONFIRM_CONNECTION_STRATEGY_RISK = "confirm_connection_strategy_risk"


_PORT_EMPTY_TEXT_SELECTOR = TextSelector(TextSelectorConfig())


class StrategyTransitionOptionsMixin:
    """StrategyTransitionOptions lifecycle."""

    @_with_translation_bundle
    async def async_step_collector_endpoint(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Move a local bridge to one user-confirmed Home Assistant endpoint.

        ESP EyeBond Collector persists a successful ``set>server`` redirect, so
        callback-on-demand is not a stable product profile for that firmware.
        Both a callback-origin entry and an already-inbound bridge therefore use
        the existing verified inbound transition.  This step is presentation
        only: endpoint write, restart, same-PN proof and the atomic entry commit
        remain owned by the one strategy-transition authority.
        """

        if not self._collector_capabilities().virtual_bridge:
            return await self.async_step_init()
        if self._transition_target_strategy != CONNECTION_STRATEGY_INBOUND:
            self._stage_connection_strategy_transition(CONNECTION_STRATEGY_INBOUND)
        return await self.async_step_strategy_transition(user_input)

    @_with_translation_bundle
    async def async_step_connection(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Choose the product operating profile, then use the one transition authority."""

        capabilities = self._collector_capabilities()
        if capabilities.ha_only_required:
            return await self.async_step_init()

        current_strategy = self._transition_current_strategy()
        profile = self._collector_operating_profile()
        errors: dict[str, str] = {}
        if user_input is not None:
            target = user_input.get(CONF_CONNECTION_STRATEGY)
            if type(target) is not str or target not in CONNECTION_STRATEGIES:
                errors[CONF_CONNECTION_STRATEGY] = "invalid_selection"
            elif target == current_strategy:
                if self._transition_route_metadata_requires_verification(target):
                    self._stage_connection_strategy_transition(target)
                    return await self.async_step_strategy_transition()
                if profile.stable:
                    return self.async_create_entry(
                        data=dict(self._config_entry.options)
                    )
                errors["base"] = "operating_profile_inconsistent"
            else:
                self._stage_connection_strategy_transition(target)
                return await self.async_step_strategy_transition()

        return self.async_show_form(
            step_id="connection",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_CONNECTION_STRATEGY,
                        default=current_strategy,
                    ): _connection_strategy_selector(
                        self._tr(
                            "common.dynamic.operating_profile_ha_only",
                            "Home Assistant only",
                        ),
                        self._tr(
                            "common.dynamic.operating_profile_cloud_and_ha",
                            "Cloud + Home Assistant",
                        ),
                    )
                }
            ),
            errors=errors,
            description_placeholders={
                "current_profile": self._operating_profile_label(profile.profile),
                "profile_summary": self._tr(
                    {
                        OPERATING_PROFILE_CLOUD_AND_HA: (
                            "common.dynamic.operating_profile_summary_cloud_and_ha"
                        ),
                        OPERATING_PROFILE_HA_ONLY: (
                            "common.dynamic.operating_profile_summary_home_assistant_only"
                        ),
                        OPERATING_PROFILE_CUSTOM: (
                            "common.dynamic.operating_profile_summary_custom"
                        ),
                    }[profile.profile],
                    {
                        OPERATING_PROFILE_CLOUD_AND_HA: (
                            "The collector normally remains connected to its cloud service. "
                            "Home Assistant asks it to connect when data is needed. "
                            "Temporary traffic capture and control discovery are "
                            "available from this profile."
                        ),
                        OPERATING_PROFILE_HA_ONLY: (
                            "The collector connects directly to Home Assistant. "
                            "Its cloud service does not receive its data."
                        ),
                        OPERATING_PROFILE_CUSTOM: (
                            "The saved connection settings do not match either "
                            "normal profile. Choose the intended profile to run "
                            "a verified transition."
                        ),
                    }[profile.profile],
                ),
            },
        )

    def _transition_current_strategy(self) -> str:
        return resolve_connection_strategy(
            self._config_entry.data,
            self._config_entry.options,
        )

    def _transition_endpoint_write_shape(self) -> CollectorEndpointWriteShape:
        """Return the coordinator's typed endpoint shape, fail-safe editable by default."""

        coordinator = self._coordinator()
        shape = getattr(coordinator, "collector_endpoint_write_shape", None)
        if type(shape) is CollectorEndpointWriteShape:
            return shape
        return resolve_collector_endpoint_write_shape()

    def _transition_route_metadata_requires_verification(self, target: object) -> bool:
        """Whether a host-only inbound record carries the wrong implicit port.

        This is only a routing decision into the existing verified transition;
        it never repairs entry data itself.  The correction is committed only
        after the same collector disconnects and reconnects on the catalog-
        defined listener port.
        """

        if (
            target != CONNECTION_STRATEGY_INBOUND
            or self._transition_current_strategy() != CONNECTION_STRATEGY_INBOUND
        ):
            return False
        shape = self._transition_endpoint_write_shape()
        if not shape.port_is_fixed:
            return False
        data = self._config_entry.data
        host = normalized_advertised_host(data.get(CONF_ADVERTISED_SERVER_IP))
        port = parse_advertised_port(data.get(CONF_ADVERTISED_TCP_PORT))
        return bool(host and port != shape.fixed_port)

    def _transition_prefill(self) -> dict[str, Any]:
        """Resolve the default advertised endpoint via the CP1a typed resolver.

        The prefill is an editable SUGGESTION only (its provenance is shown in
        placeholders, never stored as recovery evidence). It is derived through
        ``resolve_default_ha_endpoint`` from strictly-separated sources:

        * explicit advertised route -- the WHOLE record from ONE source:
          ``entry.data`` when any advertised key is present there, else
          ``entry.options`` as a legacy fallback, else strictly absent. A field
          is never taken from data and its pair from options; a partial/malformed
          record fails closed (no fall-through);
        * the advertised endpoint of the entry's VALIDATED, PN-bound callback
          proof (read only via ``RecoveryContract.from_entry_data``);
        * no confirmed-HA-endpoint role authority exists yet -> ``None``;
        * for a virtual bridge only, the exact endpoint currently exposed by the
          live collector-management snapshot, as an editable observation;
        * the effective runtime route (``server_ip:tcp_port``) as an editable
          local hint, which the resolver only offers for a callback entry.
        """

        from ...connection.recovery_contract import (
            RECOVERY_CONTRACT_KEY,
            RecoveryContract,
        )

        data = self._config_entry.data
        options = self._config_entry.options
        if CONF_ADVERTISED_SERVER_IP in data or CONF_ADVERTISED_TCP_PORT in data:
            # Canonical data must carry the complete pair. A missing half is
            # PRESENT-but-malformed and must reach the strict resolver as such.
            explicit_host = data.get(CONF_ADVERTISED_SERVER_IP)
            explicit_port = data.get(CONF_ADVERTISED_TCP_PORT)
        elif (
            CONF_ADVERTISED_SERVER_IP in options or CONF_ADVERTISED_TCP_PORT in options
        ):
            option_host = options.get(CONF_ADVERTISED_SERVER_IP)
            option_port = options.get(CONF_ADVERTISED_TCP_PORT)
            # The retired runtime-options UI serialized an ABSENT optional pair
            # as two exact empty strings. Canonicalize only that complete legacy
            # shape. Partial, padded, duck or otherwise malformed values remain
            # present and fail closed in ``resolve_default_ha_endpoint``.
            if (
                CONF_ADVERTISED_SERVER_IP in options
                and CONF_ADVERTISED_TCP_PORT in options
                and type(option_host) is str
                and option_host == ""
                and type(option_port) is str
                and option_port == ""
            ):
                explicit_host, explicit_port = "", 0
            else:
                explicit_host, explicit_port = option_host, option_port
        else:
            explicit_host, explicit_port = "", 0

        # The callback proof comes ONLY from a valid RecoveryContract whose PN is
        # this entry's own collector PN -- an EXACT normalized string (a duck /
        # non-string / padded / empty entry PN alongside a present contract is
        # never trusted). A foreign-PN contract, an untrusted entry PN, or a
        # present-but-unparseable contract record never yields a route AND fails
        # closed: absent a valid HIGHER-priority explicit route, it forbids every
        # lower-priority source (including a future confirmed HA endpoint), not
        # only the local runtime hint.
        raw_entry_pn = data.get(CONF_COLLECTOR_PN)
        entry_pn = (
            raw_entry_pn
            if type(raw_entry_pn) is str
            and raw_entry_pn != ""
            and raw_entry_pn == raw_entry_pn.strip()
            else None
        )
        contract = RecoveryContract.from_entry_data(dict(data))
        proof_endpoint = ""
        contract_fail_closed = False
        if contract is not None:
            if entry_pn is not None and pn_is_same_identity(
                entry_pn, contract.collector_pn
            ):
                if contract.callback_proof is not None:
                    proof_endpoint = contract.callback_proof.advertised_ha_endpoint
            else:
                contract_fail_closed = True
        elif RECOVERY_CONTRACT_KEY in data:
            contract_fail_closed = True

        # Effective runtime hint uses the REAL options-over-data runtime config,
        # passed raw (the resolver validates types -- no int()/bool coercion here).
        server_ip = (
            options[CONF_SERVER_IP]
            if CONF_SERVER_IP in options
            else data.get(CONF_SERVER_IP, "")
        )
        tcp_port = (
            options[CONF_TCP_PORT]
            if CONF_TCP_PORT in options
            else data.get(CONF_TCP_PORT)
        )

        observed_current_endpoint: object = ""
        if self._collector_capabilities().virtual_bridge:
            coordinator = self._coordinator()
            runtime_data = getattr(coordinator, "data", None)
            values = getattr(runtime_data, "values", None)
            if type(values) is dict:
                observed_current_endpoint = values.get(
                    "collector_server_endpoint", ""
                )

        candidate = resolve_default_ha_endpoint(
            explicit_advertised_host=explicit_host,
            explicit_advertised_port=explicit_port,
            callback_proof_endpoint=proof_endpoint,
            confirmed_ha_endpoint=None,
            observed_current_endpoint=observed_current_endpoint,
            current_strategy=resolve_connection_strategy(data, options),
            server_ip=server_ip,
            tcp_port=tcp_port,
        )
        # A fail-closed contract permits ONLY a valid higher-priority explicit
        # route; every other resolved source (callback proof, a future confirmed
        # HA endpoint, or the local runtime hint) is refused.
        if (
            contract_fail_closed
            and candidate.provenance != PROVENANCE_EXPLICIT_ADVERTISED
        ):
            candidate = TransitionEndpointCandidate.none()
        endpoint_shape = self._transition_endpoint_write_shape()
        if (
            self._transition_target_strategy == CONNECTION_STRATEGY_INBOUND
            and candidate.has_candidate
            and endpoint_shape.port_is_fixed
        ):
            # Endpoint serialization applies only to the persistent HA-only
            # route. A host-only CLDSRVHOST1 value cannot carry an editable
            # port, so its catalog default is the direct dial route semantics.
            # It says nothing about a later set>server callback route.
            candidate = TransitionEndpointCandidate(
                host=candidate.host,
                port=endpoint_shape.fixed_port,
                provenance=candidate.provenance,
            )
        collector_ip = str(
            options.get(CONF_COLLECTOR_IP) or data.get(CONF_COLLECTOR_IP) or ""
        ).strip()
        return {
            "host": candidate.host,
            "port": candidate.port,
            "provenance": candidate.provenance,
            "collector_ip": collector_ip,
        }

    async def _resolve_cloud_rollback_context(self):
        """Return the entry's typed CloudRollbackEndpoint, or ``None``.

        READ-ONLY: delegates to the coordinator's read-only boundary (which reuses
        existing endpoint/registry facts). Never writes and never triggers a wire
        operation. When the entry has no live coordinator the context is unknown.
        """

        coordinator = self._coordinator()
        boundary = getattr(coordinator, "collector_cloud_rollback_context", None)
        if not callable(boundary):
            return None
        try:
            return await boundary()
        except Exception:  # pragma: no cover - defensive read-only path
            return None

    def _connection_strategy_rollback_note(self, *, to_inbound: bool, rollback) -> str:
        """Build the honest, read-only rollback summary shown in the transition form.

        Presentation only: it never edits a field, changes the submitted payload,
        persists the context, or stands in for a proof. The endpoint shown belongs
        to the user's own entry.
        """

        known = rollback is not None and getattr(rollback, "known", False)
        if to_inbound:
            if known:
                return self._tr(
                    "common.dynamic.connection_strategy_rollback_inbound_known",
                    "The cloud address is saved and can be restored later.",
                )
            return self._tr(
                "common.dynamic.connection_strategy_rollback_inbound_unknown",
                "No cloud address is saved yet. You will need to enter one if "
                "you return to cloud mode.",
            )
        if known:
            template = self._tr(
                "common.dynamic.connection_strategy_rollback_callback_known",
                "Cloud address to restore: {endpoint}.",
            )
            try:
                return template.format(endpoint=rollback.endpoint)
            except (KeyError, IndexError, ValueError):
                return template
        return self._tr(
            "common.dynamic.connection_strategy_rollback_callback_unknown",
            "Choose the cloud address on the next step.",
        )

    @_with_translation_bundle
    async def async_step_strategy_transition(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Explicit confirmation: the addresses this transition will use.

        Every address is editable user input (prefilled only from the entry's
        previously-confirmed configuration): behind NAT the advertised host /
        port may be a public address that looks nothing like this machine.
        Nothing is derived from peer IPs or hostname shapes, and submitting
        this form is the explicit consent for the controlled restart.
        """

        target = self._transition_target_strategy
        if target not in CONNECTION_STRATEGIES:
            return await self.async_step_runtime()
        if user_input is None:
            # A newly opened confirmation attempt gets a newly pinned rollback
            # read-model snapshot.  Form validation retries keep their existing
            # snapshot, but returning here from the failure menu must not reuse
            # a candidate from the completed attempt.
            self._transition_rollback_selection = None
            self._transition_rollback_candidate_snapshot = None
            self._transition_rollback_candidate_pinned = False
        to_inbound = target == CONNECTION_STRATEGY_INBOUND
        local_bridge_endpoint = bool(
            to_inbound and self._collector_capabilities().virtual_bridge
        )
        errors: dict[str, str] = {}
        prefill = self._transition_prefill()
        endpoint_shape = self._transition_endpoint_write_shape()
        fixed_direct_endpoint = bool(to_inbound and endpoint_shape.port_is_fixed)

        if user_input is not None:
            flat = _flatten_sections(user_input)
            # Mandatory risk consent. STRICT identity check: only the exact bool
            # ``True`` is consent. A missing value, ``False``, a truthy int such
            # as ``1``, the string ``"true"`` or any other object is refused with
            # a form error and NEVER coerced. Without consent nothing downstream
            # runs: no ``_transition_confirmed_input`` is staged, the progress
            # step is not entered, so ``_transition_task`` stays ``None`` and
            # there is no endpoint write, reboot, UDP trigger, config-entry write
            # or transition task. The consent is user intent only -- it is never
            # persisted, and never stands in for transition proof or a
            # RecoveryContract.
            consent = flat.get(CONF_CONFIRM_CONNECTION_STRATEGY_RISK)
            if consent is not True:
                errors[CONF_CONFIRM_CONNECTION_STRATEGY_RISK] = (
                    "connection_strategy_risk_unconfirmed"
                )
            # No coercion: the RAW submitted value reaches the validator, so a
            # non-string / wildcard / padded host cannot be str()-normalized into
            # a valid one. Empty -> required; anything else invalid ->
            # invalid_selection. The port field may be a NumberSelector value OR
            # (when no default was known) a free text field -- a malformed value
            # is a form error, never a 500.
            raw_host = flat.get(CONF_ADVERTISED_SERVER_IP)
            host = normalized_advertised_host(raw_host)
            if fixed_direct_endpoint:
                raw_port = flat.get(CONF_ADVERTISED_TCP_PORT)
                port = endpoint_shape.fixed_port
                if raw_port is not None and parse_advertised_port(raw_port) != port:
                    errors[CONF_ADVERTISED_TCP_PORT] = "invalid_selection"
            else:
                port = parse_advertised_port(flat.get(CONF_ADVERTISED_TCP_PORT))
            collector_ip = str(flat.get(CONF_COLLECTOR_IP) or "").strip()
            if raw_host is None or raw_host == "":
                errors[CONF_ADVERTISED_SERVER_IP] = "required"
            elif host is None:
                errors[CONF_ADVERTISED_SERVER_IP] = "invalid_selection"
            if port is None:
                errors[CONF_ADVERTISED_TCP_PORT] = "invalid_selection"
            if not to_inbound and not collector_ip:
                errors[CONF_COLLECTOR_IP] = "callback_target_required"
            if not errors:
                # Consent is NOT stored here: only the endpoint inputs travel to
                # the verified transition. The checkbox never reaches data/options.
                self._transition_confirmed_input = {
                    "host": host,
                    "port": port,
                    "collector_ip": collector_ip,
                }
                self._transition_rollback_selection = None
                self._transition_rollback_candidate_snapshot = None
                self._transition_rollback_candidate_pinned = False
                # CP2B.2: a callback restore that must physically hand the
                # collector back to a cloud endpoint (integration_managed) needs
                # the user to choose that endpoint first. If the endpoint is
                # already external, no restore/write is needed and the chooser is
                # skipped.
                if self._transition_needs_cloud_rollback():
                    return await self.async_step_strategy_transition_rollback()
                return await self.async_step_strategy_transition_progress()

        schema_fields: dict[Any, Any] = {}
        if prefill["host"]:
            schema_fields[
                vol.Required(CONF_ADVERTISED_SERVER_IP, default=prefill["host"])
            ] = _IP_TEXT_SELECTOR
        else:
            schema_fields[vol.Required(CONF_ADVERTISED_SERVER_IP)] = _IP_TEXT_SELECTOR
        if not fixed_direct_endpoint:
            if prefill["port"]:
                schema_fields[
                    vol.Required(CONF_ADVERTISED_TCP_PORT, default=prefill["port"])
                ] = _PORT_SELECTOR
            elif not to_inbound:
                # set>server callback routing is independent from the
                # collector's persistent endpoint serialization. The local
                # listener default is the useful same-LAN suggestion; users
                # behind NAT may still enter the externally forwarded port.
                schema_fields[
                    vol.Required(CONF_ADVERTISED_TCP_PORT, default=DEFAULT_TCP_PORT)
                ] = _PORT_SELECTOR
            else:
                # No known port: an empty text field (never the NumberSelector
                # minimum 1 presented as a chosen default).
                schema_fields[vol.Required(CONF_ADVERTISED_TCP_PORT)] = (
                    _PORT_EMPTY_TEXT_SELECTOR
                )
        if not to_inbound:
            schema_fields[
                vol.Required(CONF_COLLECTOR_IP, default=prefill["collector_ip"])
            ] = _IP_TEXT_SELECTOR
        # Mandatory risk consent checkbox, always defaulting to unchecked so the
        # user must make an explicit choice each time. Placed last, after the
        # addresses it refers to.
        schema_fields[
            vol.Required(CONF_CONFIRM_CONNECTION_STRATEGY_RISK, default=False)
        ] = BooleanSelector()
        if local_bridge_endpoint:
            risk_note = self._tr(
                "common.dynamic.connection_endpoint_local_risk",
                "Home Assistant will save this address in the collector and "
                "verify that the same collector reconnects. Make sure the "
                "address and port are reachable from the collector.",
            )
        elif to_inbound:
            risk_note = self._tr(
                "common.dynamic.connection_strategy_risk_inbound",
                "The collector will connect directly to Home Assistant. Cloud "
                "services will stop receiving its data. Make sure the Home "
                "Assistant address and port are reachable from the collector.",
            )
        else:
            risk_note = self._tr(
                "common.dynamic.connection_strategy_risk_callback",
                "The collector will use the cloud again. Home Assistant will "
                "request a connection only when it needs data.",
            )
        if fixed_direct_endpoint:
            fixed_port_note = self._tr(
                "common.dynamic.connection_endpoint_fixed_port",
                "This collector uses fixed TCP port {port}; only the address is stored.",
            )
            try:
                fixed_port_note = fixed_port_note.format(
                    port=endpoint_shape.fixed_port
                )
            except (KeyError, IndexError, ValueError):
                pass
            risk_note = f"{risk_note} {fixed_port_note}"
        rollback_note = (
            ""
            if local_bridge_endpoint
            else self._connection_strategy_rollback_note(
                to_inbound=to_inbound,
                rollback=await self._resolve_cloud_rollback_context(),
            )
        )
        return self.async_show_form(
            step_id="strategy_transition",
            data_schema=vol.Schema(schema_fields),
            errors=errors,
            description_placeholders={
                "target_strategy": (
                    self._tr(
                        "common.dynamic.connection_endpoint_local_target",
                        "Home Assistant connection address",
                    )
                    if local_bridge_endpoint
                    else self._operating_profile_label(
                        OPERATING_PROFILE_HA_ONLY
                        if target == CONNECTION_STRATEGY_INBOUND
                        else OPERATING_PROFILE_CLOUD_AND_HA
                    )
                ),
                "connection_strategy_risk": risk_note,
                "connection_strategy_rollback": rollback_note,
            },
        )

    def _transition_needs_cloud_rollback(self) -> bool:
        """Whether this transition must physically restore an external endpoint.

        True only for a callback transition on an integration-managed entry (the
        integration currently points the collector at Home Assistant, so handing
        control back requires writing a cloud endpoint). An already-external entry
        needs no restore and skips the chooser.
        """

        if self._transition_target_strategy != CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
            return False
        from ...connection.connection_policy import resolve_endpoint_control_policy

        policy = resolve_endpoint_control_policy(
            dict(self._config_entry.data), dict(self._config_entry.options)
        )
        return policy == ENDPOINT_CONTROL_INTEGRATION_MANAGED

    @_with_translation_bundle
    async def async_step_strategy_transition_rollback(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """CP2B.2 chooser: pick the cloud endpoint to hand the collector back to.

        Builds exactly ONE typed ``CloudRollbackSelection`` (confirmed candidate,
        catalog, or manual). This is the collector's CLOUD endpoint role -- kept
        strictly separate from the advertised HA callback route and the collector
        trigger target already collected. Nothing here writes the collector; the
        selection is passed to the coordinator which persists it before any write.
        """

        from ...collector.cloud_rollback_catalog import (
            cloud_rollback_selection_from_candidate,
            cloud_rollback_selection_from_catalog_key,
            cloud_rollback_selection_from_manual,
            writable_cloud_rollback_catalog_options,
        )

        if self._transition_target_strategy not in CONNECTION_STRATEGIES:
            return await self.async_step_runtime()

        from ...connection.strategy_transition_context import CloudRollbackEndpoint

        # Pin the exact read-model fact shown on the first render.  A submit must
        # never silently substitute a newer endpoint that the user did not see;
        # the execution boundary independently re-resolves it and rejects a
        # changed candidate as stale.
        if not self._transition_rollback_candidate_pinned:
            resolved = await self._resolve_cloud_rollback_context()
            self._transition_rollback_candidate_snapshot = (
                resolved
                if type(resolved) is CloudRollbackEndpoint and resolved.known
                else None
            )
            self._transition_rollback_candidate_pinned = True
        candidate = self._transition_rollback_candidate_snapshot
        candidate_known = type(candidate) is CloudRollbackEndpoint and candidate.known
        catalog_options = writable_cloud_rollback_catalog_options()
        errors: dict[str, str] = {}

        if user_input is not None:
            flat = _flatten_sections(user_input)
            selected_endpoint = flat.get("rollback_endpoint")
            selection = None
            if candidate_known and selected_endpoint == candidate.endpoint:
                selection = cloud_rollback_selection_from_candidate(candidate)
            else:
                catalog_match = next(
                    (
                        option
                        for option in catalog_options
                        if option.endpoint == selected_endpoint
                    ),
                    None,
                )
                selection = (
                    cloud_rollback_selection_from_catalog_key(catalog_match.key)
                    if catalog_match is not None
                    else cloud_rollback_selection_from_manual(selected_endpoint)
                )
            if selection is None:
                errors["rollback_endpoint"] = "rollback_endpoint_invalid"
            if not errors and selection is not None:
                self._transition_rollback_selection = selection
                return await self.async_step_strategy_transition_progress()

        options: list[SelectOptionDict] = []
        if candidate_known:
            provenance_label = self._tr(
                f"common.dynamic.rollback_provenance_{candidate.provenance}",
                candidate.provenance,
            )
            options.append(
                SelectOptionDict(
                    value=candidate.endpoint,
                    label=(
                        f"{self._tr('common.dynamic.rollback_choice_confirmed', 'Use the saved endpoint')}"
                        f": {candidate.endpoint} ({provenance_label})"
                    ),
                )
            )
        for option in catalog_options:
            options.append(
                SelectOptionDict(
                    value=option.endpoint,
                    label=f"{option.label} ({option.provider}) — {option.endpoint}",
                )
            )
        default_endpoint = candidate.endpoint if candidate_known else ""
        schema_fields: dict[Any, Any] = {
            vol.Required("rollback_endpoint", default=default_endpoint): SelectSelector(
                SelectSelectorConfig(
                    options=options,
                    custom_value=True,
                    mode=SelectSelectorMode.DROPDOWN,
                )
            )
        }
        return self.async_show_form(
            step_id="strategy_transition_rollback",
            data_schema=vol.Schema(schema_fields),
            errors=errors,
            description_placeholders={
                "candidate_endpoint": candidate.endpoint if candidate_known else "",
            },
        )

    async def async_step_strategy_transition_progress(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if self._transition_task is None:
            self._transition_task = self.hass.async_create_task(
                self._async_run_strategy_transition_task()
            )
        if not self._transition_task.done():
            return self.async_show_progress(
                step_id="strategy_transition_progress",
                progress_action="strategy_transition_running",
                progress_task=self._transition_task,
            )
        return self.async_show_progress_done(next_step_id="strategy_transition_result")

    async def _async_run_strategy_transition_task(self) -> None:
        """The progress task: assemble facade inputs, run THE authority."""

        coordinator = self._coordinator()
        if coordinator is None:
            self._transition_error = "transition_runtime_unavailable"
            return
        confirmed = getattr(self, "_transition_confirmed_input", None) or {}
        target = self._transition_target_strategy
        host = str(confirmed.get("host") or "")
        port = int(confirmed.get("port") or 0)
        try:
            if target == CONNECTION_STRATEGY_INBOUND:
                endpoint = coordinator.format_home_assistant_callback_endpoint(
                    host, port
                )
                # Also pass the RAW advertised host/port: the authority persists
                # them as the confirmed advertised route ONLY after the inbound
                # reconnect is verified (never a second config-flow write).
                result = await coordinator.async_run_connection_strategy_transition(
                    target_strategy=target,
                    inbound_endpoint=endpoint,
                    advertised_host=host,
                    advertised_port=port,
                    option_payload=dict(self._transition_options_payload),
                )
            else:
                result = await coordinator.async_run_connection_strategy_transition(
                    target_strategy=target,
                    callback_target_ip=str(confirmed.get("collector_ip") or ""),
                    advertised_host=host,
                    advertised_port=port,
                    option_payload=dict(self._transition_options_payload),
                    # CP2B.2: the SINGLE typed authority for the cloud rollback
                    # endpoint. None on the already-external path (no restore).
                    cloud_rollback_selection=self._transition_rollback_selection,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # typed upstream codes travel as messages
            logger.info("Strategy transition task failed: %s", exc)
            self._transition_error = str(exc) or "transition_failed"
            return
        self._transition_result = result
        self._transition_error = (
            "" if result.success else str(result.failure_reason or "transition_failed")
        )

    @_with_translation_bundle
    async def async_step_strategy_transition_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._transition_task = None
        if not self._transition_error and self._transition_result is not None:
            # Success: the authority already committed the FULL data+options
            # (strategy, RecoveryContract, endpoint policy, compatibility axes,
            # the earned advertised route, and the dropped stale route shadow) and
            # scheduled the single reload. The options-flow terminal create_entry
            # REPLACES options, so it must write the SAME complete options HA now
            # holds -- NOT the staged poll/control subset (which would drop the
            # original endpoint, control settings and every other option and
            # trigger a second semantic reload).
            payload = dict(self._config_entry.options)
            self._transition_target_strategy = ""
            self._transition_rollback_selection = None
            self._transition_rollback_candidate_snapshot = None
            self._transition_rollback_candidate_pinned = False
            self._transition_rollback_candidate_snapshot = None
            self._transition_rollback_candidate_pinned = False
            self._transition_options_payload = {}
            self._transition_result = None
            return self.async_create_entry(data=payload)
        return await self.async_step_strategy_transition_failed()

    async def async_step_strategy_transition_repair(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Finish the degraded transition from a COLD start (Batch 8 repair).

        The endpoint is already external (the restore was confirmed) and there
        may be no live session at all (HA was restarted). The coordinator-
        independent repair orchestrator bootstraps the management session back
        (Phase A) and then proves callback recovery (Phase B), committing the
        strategy and clearing the recovery state only on success. Driven as a
        progress task so it works even without a loaded coordinator.
        """

        del user_input
        self._transition_result = None
        self._transition_task = None
        self._transition_error = ""
        self._transition_activation_incomplete = False
        return await self.async_step_strategy_transition_repair_progress()

    async def async_step_strategy_transition_repair_progress(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if self._transition_task is None:
            self._transition_task = self.hass.async_create_task(
                self._async_run_degraded_repair_task()
            )
        if not self._transition_task.done():
            return self.async_show_progress(
                step_id="strategy_transition_repair_progress",
                progress_action="strategy_transition_running",
                progress_task=self._transition_task,
            )
        return self.async_show_progress_done(
            next_step_id="strategy_transition_repair_result"
        )

    async def _async_run_degraded_repair_task(self) -> None:
        """The repair progress task: bootstrap + proof, single-owner path."""

        from homeassistant.config_entries import ConfigEntryState

        from ...collector.callback_bootstrap import CallbackBootstrapChannel
        from ...connection.recovery.terminal import merge_recovery_contract
        from ...connection.strategy_transition_recovery import (
            RECOVERY_PHASE_PENDING,
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
            StrategyTransitionRecoveryState,
        )
        from ...connection.strategy_transition_repair import (
            REPAIR_STATE_INVALID,
            async_run_degraded_recovery_repair,
        )
        from ...passive_discovery import (
            get_callback_session_registry,
            get_passive_callback_discovery,
        )
        from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY

        state = StrategyTransitionRecoveryState.from_record(
            self._config_entry.data.get(CONF_STRATEGY_TRANSITION_STATE)
        )
        if state is None:
            self._transition_error = REPAIR_STATE_INVALID
            return
        registry = get_callback_session_registry(self.hass)
        discovery = get_passive_callback_discovery(self.hass)
        if registry is None or discovery is None:
            self._transition_error = "transition_runtime_unavailable"
            return

        # Pending is write-ahead intent, not a confirmed cloud restore. Resolve
        # only the exact durable user choice; malformed input fails closed.
        pending_restore_endpoint = None
        persist_restore_confirmed = None
        if state.phase == RECOVERY_PHASE_PENDING:
            durable_original = (
                self._config_entry.data[CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT]
                if CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT
                in self._config_entry.data
                else self._config_entry.options.get(
                    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT, ""
                )
            )
            pending_restore_endpoint = resolve_cloud_rollback_endpoint(
                explicit_user_endpoint="",
                durable_original_endpoint=durable_original,
                registry_endpoint="",
                registry_pn="",
                entry_pn=self._config_entry.data.get(CONF_COLLECTOR_PN),
                observed_current_endpoint="",
                confirmed_ha_endpoint=TransitionEndpointCandidate.none(),
            )

            def _persist_restore_confirmed(
                confirmed_state: StrategyTransitionRecoveryState,
            ) -> None:
                if (
                    type(confirmed_state) is not StrategyTransitionRecoveryState
                    or confirmed_state.phase != RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN
                    or not pn_is_same_identity(
                        confirmed_state.collector_pn,
                        state.collector_pn,
                    )
                ):
                    raise ValueError("transition_recovery_state_invalid")
                data = dict(self._config_entry.data)
                data[CONF_ENDPOINT_CONTROL_POLICY] = ENDPOINT_CONTROL_EXTERNAL
                data[CONF_STRATEGY_TRANSITION_STATE] = confirmed_state.to_record()
                data.pop(CONF_ENDPOINT_WRITTEN_VALUE, None)
                data.pop(CONF_ENDPOINT_WRITTEN_AT, None)
                self.hass.config_entries.async_update_entry(self._config_entry, data=data)

            persist_restore_confirmed = _persist_restore_confirmed

        entry_id = self._config_entry.entry_id

        # The same orchestrator repairs cold and loaded entries; only lifecycle
        # persistence/activation differs:
        #
        #  * UNLOADED (cold): the flow persists the terminal state directly (an
        #    unloaded entry has no update listener, so the write triggers no
        #    reload) and activates with a single awaited ``async_setup``;
        #  * LOADED (degraded, a live runtime already owns the shared listener but
        #    has no proven callback session): a RUNNING runtime would poll and
        #    adopt the very collector session Phase A/B needs, and its
        #    metadata-drift reload would tear the runtime down mid-proof. So the
        #    flow first SUSPENDS that runtime (unloads only the entry's
        #    coordinator; the domain registry + passive discovery are domain
        #    singletons and the shared listener is pinned by the ensure lease
        #    below, so every repair dependency survives), then runs the SAME cold
        #    repair with exclusive use of the collector session, and activates with
        #    the SAME single awaited ``async_setup``.
        #
        # Exactly one activation runs while the observed-listener lease is held.
        was_loaded = self._config_entry.state is ConfigEntryState.LOADED

        # ONE lifecycle ledger: every exit path (success, typed failure, error,
        # cancellation) resolves to a DEFINED final entry state through the single
        # finalization boundary below -- no fire-and-forget restore/activation.
        lifecycle: dict[str, Any] = {
            "suspend_attempted": False,  # set BEFORE async_unload (cancel-safe)
            "suspended_by_flow": False,  # the flow cleanly unloaded the entry
            "durable_commit_completed": False,  # the proof was persisted
            "activation_completed": False,
            "observed_token": None,  # released ONLY inside finalization
        }

        async def _commit(updates, terminal) -> str:
            # PERSISTENCE ONLY. At commit time the entry is UNLOADED (cold, or the
            # LOADED runtime was suspended below), so this public
            # ``async_update_entry`` fires no update-listener reload. Activation is
            # a SEPARATE awaited step in finalization.
            data = dict(self._config_entry.data)
            data.update(updates)
            data.pop(CONF_STRATEGY_TRANSITION_STATE, None)
            route_host, route_port, route_refusal = earned_advertised_route(
                committed_strategy=updates.get(CONF_CONNECTION_STRATEGY),
                terminal=terminal,
                attempted_host=state.advertised_host,
                attempted_port=state.advertised_port,
            )
            if route_refusal:
                return route_refusal
            if route_host:
                data[CONF_ADVERTISED_SERVER_IP] = route_host
                data[CONF_ADVERTISED_TCP_PORT] = route_port
            refusal = merge_recovery_contract(data, terminal)
            if refusal:
                return refusal
            # Home Assistant requires ``options`` to be a mapping whenever the
            # keyword is present; passing ``None`` can apply ``data`` first and
            # then raise from MappingProxyType(None), turning a completed proof
            # into a false UI failure. Commit the complete, byte-equivalent
            # options mapping even when there are no stale route keys to drop.
            options = dict(self._config_entry.options)
            if route_host:
                options.pop(CONF_ADVERTISED_SERVER_IP, None)
                options.pop(CONF_ADVERTISED_TCP_PORT, None)
            self.hass.config_entries.async_update_entry(
                self._config_entry,
                data=data,
                options=options,
            )
            lifecycle["durable_commit_completed"] = True
            return ""

        # The ONE public cold-bootstrap boundary owns every listener / wire /
        # trigger / registry-projection concern; the flow holds none of them.
        channel = CallbackBootstrapChannel(
            registry=registry,
            host=state.listener_bind_host,
            port=state.local_listener_port,
            entry_data=self._config_entry.data,
            entry_options=self._config_entry.options,
            entry_pn=state.collector_pn,
            trigger_timeout=DEFAULT_ONBOARDING_TIMEOUT_POLICY.discovery_timeout,
        )

        # ---- SUSPEND + REPAIR + DETERMINISTIC, CANCELLATION-SAFE FINALIZATION -
        # The cancellation-safe boundary ENCOMPASSES the suspend itself: a cancel
        # that lands mid-``async_unload`` (after the entry actually left LOADED)
        # still routes the mandatory restore through the one finalization boundary
        # before the ``CancelledError`` propagates. A running runtime would
        # poll/adopt the very collector session Phase A/B needs and reload
        # mid-proof, so a LOADED entry is SUSPENDED first; if it cannot be cleanly
        # suspended the flow refuses with a typed reason and runs NOTHING
        # downstream (zero listener ensure / UDP / orchestrator / commit).
        try:
            if was_loaded:
                refusal = await self._suspend_runtime_for_repair(entry_id, lifecycle)
                if refusal:
                    self._transition_error = refusal
                    return  # the finally still restores a partial unload
            # The ensure lives INSIDE the finalized boundary so its token is always
            # released through the one cleanup path, even if the ensure or the
            # orchestrator raises. The LISTENER bind host (not the UDP trigger
            # bind) shares the refcounted TCP listener; no private injection.
            lifecycle[
                "observed_token"
            ] = await discovery.async_ensure_observed_listener(
                state.listener_bind_host, state.local_listener_port
            )
            result = await async_run_degraded_recovery_repair(
                registry=registry,
                owner_id=entry_id,
                state=state,
                channel=channel,
                commit=_commit,
                clock=lambda: datetime.now(timezone.utc).isoformat(),
                pending_restore_endpoint=pending_restore_endpoint,
                persist_restore_confirmed=persist_restore_confirmed,
            )
            self._transition_result = result
            if not result.success:
                self._transition_error = str(
                    result.failure_reason or "transition_failed"
                )
        except asyncio.CancelledError:
            raise  # finalize in the finally FIRST, then propagate
        except Exception as exc:  # typed reasons travel as messages
            logger.info("Degraded repair task failed: %s", exc)
            self._transition_error = str(exc) or "transition_failed"
        finally:
            # ONE boundary resolves restore-or-activate + token release, run to
            # completion even under cancellation (never fire-and-forget), so a
            # cancel/error NEVER leaves the entry silently unloaded or the proof
            # half-applied.
            await self._await_critical(
                self._finalize_repair_lifecycle(
                    entry_id,
                    was_loaded=was_loaded,
                    lifecycle=lifecycle,
                    discovery=discovery,
                    registry=registry,
                )
            )

    @staticmethod
    async def _await_critical(coro):
        """Run a critical finalization coroutine to COMPLETION under cancellation.

        The finalization (restore of the suspended entry, or activation of the
        proven one) MUST finish before a ``CancelledError`` propagates, so a
        mid-flight abort never leaves the entry unloaded or the proof half-applied.
        It runs as a shielded task; re-delivered cancellations are absorbed until
        it completes. If the caller was cancelled at ANY point, the
        ``CancelledError`` is re-raised AFTER the work finishes -- a successful
        finalization NEVER turns a cancelled task into a normal completion, and
        repeated cancels never interrupt the cleanup.
        """

        task = asyncio.ensure_future(coro)
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
                # Keep the shielded critical work alive; wait for it to finish.
        if cancelled:
            # Honor the caller's cancellation AFTER cleanup completed.
            raise asyncio.CancelledError
        error = task.exception()
        if error is not None:
            raise error
        return task.result()

    async def _suspend_runtime_for_repair(
        self, entry_id: str, lifecycle: dict[str, Any]
    ) -> str:
        """Fail-closed suspend of the competing runtime before a LOADED repair.

        The attempt is marked BEFORE the unload so a cancellation that lands
        mid-``async_unload`` (after the entry left LOADED) still routes the
        mandatory restore through the shared finalization boundary -- never here.
        Returns "" on a CLEAN suspend (entry NOT_LOADED, ``suspended_by_flow`` set)
        or a typed refusal; a partial / failed unload is restored by finalization.
        """

        from homeassistant.config_entries import ConfigEntryState

        # Mark BEFORE the await: even a cancel delivered inside async_unload leaves
        # this recorded, so finalization knows the flow owns the restore.
        lifecycle["suspend_attempted"] = True
        try:
            unloaded = bool(await self.hass.config_entries.async_unload(entry_id))
        except asyncio.CancelledError:
            raise  # finalization (via the task's finally) restores + propagates
        except Exception as exc:
            logger.info("Suspend unload of %s raised: %s", entry_id, exc)
            unloaded = False
        if unloaded and self._config_entry.state is ConfigEntryState.NOT_LOADED:
            lifecycle["suspended_by_flow"] = True
            return ""
        # NOT cleanly suspended -> typed refusal. A partial unload (entry left
        # non-LOADED) is restored by the ONE finalization boundary, not here.
        return "transition_suspend_failed"

    async def _finalize_repair_lifecycle(
        self,
        entry_id: str,
        *,
        was_loaded: bool,
        lifecycle: dict[str, Any],
        discovery: Any,
        registry: Any,
    ) -> None:
        """The ONE deterministic exit boundary for the degraded-repair lifecycle.

        Post-commit: the proof is durable and is NEVER rolled back -- the proven
        runtime is activated EXACTLY once (a cancel during activation completes
        here, never leaving the entry unloaded). Pre-commit after a suspend: the
        ORIGINAL configuration is restored with exactly one awaited setup, and a
        restore failure is an HONEST typed reason (never suppressed). The held
        observed-listener token is released only AFTER the restore/activation
        attempt, through this same boundary.
        """

        from homeassistant.config_entries import ConfigEntryState

        try:
            result = self._transition_result
            if lifecycle["durable_commit_completed"]:
                if self._config_entry.state is not ConfigEntryState.LOADED:
                    try:
                        lifecycle["activation_completed"] = bool(
                            await self.hass.config_entries.async_setup(entry_id)
                        )
                    except Exception as exc:  # never CancelledError (shielded)
                        # HA recorded the honest post-setup state; the proof stays
                        # committed. A raising activation is NOT the proof's
                        # failure path -- it surfaces as activation-incomplete.
                        logger.info(
                            "Activation of proven entry did not complete: %s", exc
                        )
                        lifecycle["activation_completed"] = False
                else:
                    lifecycle["activation_completed"] = True
                # Postcondition via the PUBLIC registry: the certified owned
                # session must still be accepted, and HA must honestly report the
                # entry LOADED, before we call the activation complete.
                session_accepted = bool(
                    result is not None
                    and getattr(result, "outcome", None) is not None
                    and registry.reverify_permanent_owned_session(
                        getattr(result.outcome, "owner_certification", None)
                    )
                )
                runtime_activated = False
                if (
                    lifecycle["activation_completed"]
                    and self._config_entry.state is ConfigEntryState.LOADED
                    and session_accepted
                ):
                    coordinator = getattr(self._config_entry, "runtime_data", None)
                    activate = getattr(
                        coordinator,
                        "async_activate_proven_callback_session",
                        None,
                    )
                    if callable(activate):
                        try:
                            runtime_activated = bool(
                                await activate(
                                    getattr(
                                        result.outcome,
                                        "owner_certification",
                                        None,
                                    )
                                )
                            )
                        except Exception as exc:
                            logger.info(
                                "Activation of the proven callback session did "
                                "not complete: %s",
                                exc,
                            )
                if (
                    lifecycle["activation_completed"]
                    and self._config_entry.state is ConfigEntryState.LOADED
                    and session_accepted
                    and runtime_activated
                ):
                    self._transition_error = ""
                    self._transition_activation_incomplete = False
                else:
                    # Durable proof STANDS; only HA's load did not complete. The
                    # verification and collector reconfiguration are not repeated.
                    self._transition_activation_incomplete = True
                    self._transition_error = "transition_activation_incomplete"
            elif (
                was_loaded
                and lifecycle["suspend_attempted"]
                and self._config_entry.state is not ConfigEntryState.LOADED
            ):
                # The flow suspended (or began suspending) the entry and nothing
                # was committed -- restore the ORIGINAL configuration. This covers
                # a clean-suspend-then-failure AND a cancel delivered INSIDE the
                # unload after the entry already left LOADED.
                restored = False
                try:
                    restored = bool(
                        await self.hass.config_entries.async_setup(entry_id)
                    )
                except Exception as exc:  # never CancelledError (shielded)
                    logger.warning(
                        "Restore of suspended entry %s after aborted repair failed: %s",
                        entry_id,
                        exc,
                    )
                    restored = False
                if (
                    not restored
                    and self._config_entry.state is not ConfigEntryState.LOADED
                ):
                    # HONEST: the previously-working entry did not come back.
                    self._transition_error = "transition_restore_failed"
        finally:
            token = lifecycle.get("observed_token")
            if token is not None:
                lifecycle["observed_token"] = None
                await discovery.async_release_observed_listener(token)

    def _callback_proven_but_not_loaded(self) -> bool:
        """Whether the entry is a PROVEN callback config that just needs loading.

        True iff the connection strategy is callback_on_demand, a valid
        (callback-verified) RecoveryContract is present, there is NO recovery
        marker (the physical repair is done, not pending), and the entry is not
        LOADED. Such an entry needs a LOAD-ONLY activation retry -- never a fresh
        Phase A/B, UDP trigger, or proof change.
        """

        from homeassistant.config_entries import ConfigEntryState

        # Cheap gates first (a LOADED entry, wrong strategy, or a pending recovery
        # marker) short-circuit BEFORE the heavier RecoveryContract parse.
        if getattr(self._config_entry, "state", None) is ConfigEntryState.LOADED:
            return False
        data = self._config_entry.data
        if (
            str(data.get(CONF_CONNECTION_STRATEGY) or "")
            != CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        ):
            return False
        if str(data.get(CONF_STRATEGY_TRANSITION_STATE) or "").strip():
            return False  # a recovery marker means a physical repair is pending
        from ...connection.recovery_contract import RecoveryContract

        contract = RecoveryContract.from_entry_data(data)
        return contract is not None and contract.callback_verified

    @_with_translation_bundle
    async def async_step_strategy_transition_repair_result(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._transition_task = None
        if not self._transition_error and self._transition_result is not None:
            # Success: the repair committed the strategy + contract, cleared
            # the recovery state and scheduled the single reload.
            self._transition_result = None
            return self.async_create_entry(data=dict(self._config_entry.options))
        if self._transition_activation_incomplete:
            # The repair is PROVEN and committed; only HA's activation did not
            # complete. This is NOT a repair failure -- never offer the full
            # physical retry here.
            return await self.async_step_strategy_transition_activation_incomplete()
        return await self.async_step_strategy_transition_failed()

    @_with_translation_bundle
    async def async_step_strategy_transition_activation_incomplete(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """The proof committed durably but HA's activation did not complete.

        The strategy, the RecoveryContract and the cleared recovery state are all
        persisted -- the verification and the collector reconfiguration are done
        and MUST NOT be repeated. Home Assistant only failed to load the entry
        (setup retry / error). The sole remaining action is to retry LOADING the
        already-proven callback configuration for the SAME entry -- no fresh proof
        and no collector reconfiguration (Home Assistant's own same-PN callback
        reconnect on load is a normal runtime step, not part of the repair).
        """

        del user_input
        return self.async_show_menu(
            step_id="strategy_transition_activation_incomplete",
            menu_options=[
                "strategy_transition_activation_retry",
                "strategy_transition_cancel",
            ],
            description_placeholders={
                "failure_explanation": _shared_recovery_failure_explanation(
                    self._tr, self._transition_error
                ),
            },
        )

    async def async_step_strategy_transition_activation_retry(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Retry ONLY loading the already-proven entry.

        A plain reload of the SAME entry -- Home Assistant re-loads the
        already-proven callback configuration. The verification and the collector
        reconfiguration are NEVER repeated; the degraded-repair orchestrator is
        not invoked and the RecoveryContract / recovery-state fields are not
        touched. The proof stays committed either way; if the reload lands the
        entry LOADED we close the flow, otherwise we show the same
        activation-only menu again.
        """

        del user_input
        from homeassistant.config_entries import ConfigEntryState

        # Reachable either from the in-flow activation-incomplete menu (reason
        # already set) OR fresh from the options menu on a reopened proven entry;
        # make sure the fallback menu has an honest reason in both cases.
        if not self._transition_error:
            self._transition_error = "transition_activation_incomplete"

        entry_id = self._config_entry.entry_id
        try:
            await self.hass.config_entries.async_reload(entry_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("Activation reload of proven entry did not complete: %s", exc)
        runtime_activated = False
        if self._config_entry.state is ConfigEntryState.LOADED:
            try:
                coordinator = getattr(self._config_entry, "runtime_data", None)
                ensure_ready = getattr(
                    coordinator,
                    "async_ensure_callback_runtime_ready",
                    None,
                )
                runtime_activated = bool(
                    callable(ensure_ready)
                    and await ensure_ready(
                        timeout=(
                            DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait
                        )
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.info(
                    "Activation retry loaded the entry but did not activate "
                    "its proven callback session: %s",
                    exc,
                )
        if self._config_entry.state is ConfigEntryState.LOADED and runtime_activated:
            self._transition_activation_incomplete = False
            self._transition_result = None
            self._transition_error = ""
            return self.async_create_entry(data=dict(self._config_entry.options))
        # Still not loaded -- the proof remains committed; offer the same
        # load-only remedy again (no verification, no collector reconfiguration).
        return await self.async_step_strategy_transition_activation_incomplete()

    @_with_translation_bundle
    async def async_step_strategy_transition_failed(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Typed transition failure with the explicit next actions."""

        del user_input
        return self.async_show_menu(
            step_id="strategy_transition_failed",
            menu_options=[
                "strategy_transition",
                "strategy_transition_keep_settings",
                "strategy_transition_cancel",
            ],
            description_placeholders={
                "failure_explanation": _shared_recovery_failure_explanation(
                    self._tr, self._transition_error
                ),
            },
        )

    async def async_step_strategy_transition_keep_settings(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Save the submitted runtime settings WITHOUT changing the strategy."""

        del user_input
        options = dict(self._transition_options_payload)
        self._transition_target_strategy = ""
        self._transition_rollback_selection = None
        self._transition_rollback_candidate_snapshot = None
        self._transition_rollback_candidate_pinned = False
        self._transition_rollback_candidate_snapshot = None
        self._transition_rollback_candidate_pinned = False
        self._transition_options_payload = {}
        self._transition_result = None
        self._transition_error = ""
        self.hass.config_entries.async_update_entry(self._config_entry, options=options)
        return self.async_create_entry(data=options)

    async def async_step_strategy_transition_cancel(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Leave everything untouched (options included)."""

        del user_input
        self._transition_target_strategy = ""
        self._transition_rollback_selection = None
        self._transition_rollback_candidate_snapshot = None
        self._transition_rollback_candidate_pinned = False
        self._transition_rollback_candidate_snapshot = None
        self._transition_rollback_candidate_pinned = False
        self._transition_options_payload = {}
        self._transition_result = None
        self._transition_error = ""
        return self.async_create_entry(data=dict(self._config_entry.options))
