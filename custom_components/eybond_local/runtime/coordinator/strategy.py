"""Connection-strategy transition lifecycle for the runtime coordinator."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any

from ...const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY,
    CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_STRATEGY,
    CONF_CONTROL_MODE,
    CONF_ENDPOINT_CONTROL_POLICY,
    CONF_ENDPOINT_WRITTEN_AT,
    CONF_ENDPOINT_WRITTEN_VALUE,
    CONF_STRATEGY_TRANSITION_STATE,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    CONTROL_MODE_AUTO,
    CONTROL_MODE_FULL,
    CONTROL_MODE_READ_ONLY,
    ENDPOINT_CONTROL_EXTERNAL,
    ENDPOINT_CONTROL_INTEGRATION_MANAGED,
)
from ...support.collector_registry import remember_collector_original_endpoint
from .endpoint_projection import (
    normalize_preserved_collector_server_endpoint as _normalize_preserved_collector_server_endpoint,
)

logger = logging.getLogger(__name__)


class CoordinatorStrategyTransitionMixin:
    """Run the existing strategy authority through one coordinator facade."""

    async def _async_prepare_strategy_transition_management_session(
        self,
        *,
        registry: Any,
        entry_id: str,
        target_strategy: str,
        timeout: float,
    ) -> str:
        """Pin the exact live socket needed by one strategy transaction.

        A callback-mode entry is allowed to be idle between polls.  Switching
        it to inbound therefore has a mandatory preflight: ask the EXISTING
        runtime connection path for one callback session, then pin only the
        registry's strongly-observed current socket for this durable owner.
        The runtime path owns set>server causality; this method creates no
        parallel trigger/matcher and accepts no peer-IP evidence.

        Other transition directions retain their existing fail-closed
        semantics: they must already have a usable management session and this
        preflight never sends a callback trigger for them.
        """

        if registry is None:
            return ""

        previous_session_id = registry.claimed_session_id(entry_id)
        pinned_session_id = registry.pin_owner_claim_to_current_observed_session(
            entry_id
        )
        if pinned_session_id:
            logger.info(
                "Strategy transition management session selected "
                "previous_session=%s selected_session=%s",
                previous_session_id or "none",
                pinned_session_id,
            )
            return pinned_session_id

        if not (
            self.connection_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
            and target_strategy == CONNECTION_STRATEGY_INBOUND
        ):
            return ""

        ensure_session = getattr(
            self._runtime,
            "async_ensure_collector_management_session",
            None,
        )
        if not callable(ensure_session):
            return ""

        try:
            connected = await ensure_session(timeout=timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info(
                "Strategy transition callback session bootstrap failed: %s",
                exc,
            )
            return ""
        if connected is not True:
            return ""

        pinned_session_id = registry.pin_owner_claim_to_current_observed_session(
            entry_id
        )
        if not pinned_session_id:
            logger.info(
                "Strategy transition callback bootstrap produced no trusted "
                "owned session"
            )
            return ""
        logger.info(
            "Strategy transition callback management session established "
            "selected_session=%s",
            pinned_session_id,
        )
        return pinned_session_id

    def _apply_transition_commit(
        self,
        updates: dict[str, Any],
        terminal: Any,
        option_updates: Any,
        *,
        advertised_host: str,
        advertised_port: int,
    ) -> str:
        """Apply ONE verified strategy-transition commit (the facade's commit).

        Persists the strategy, the EARNED advertised route (Batch 1 CP1b) and the
        RecoveryContract in a SINGLE ``async_update_entry`` and schedules exactly
        ONE reload. The route is persisted ONLY on a true strategy commit, from
        the PROVEN route -- callback: the callback proof's advertised endpoint,
        which MUST equal this attempt's advertised route (mismatch/absent => typed
        refusal, no write); inbound: the confirmed HA endpoint the verified
        reconnect used. ``entry.data`` becomes the SINGLE canonical owner: stale
        ``options`` shadow copies are dropped in this same commit. A route or
        contract refusal aborts BEFORE any write.
        """

        from ...connection.recovery.terminal import merge_recovery_contract
        from ...connection.strategy_transition_context import earned_advertised_route

        data = dict(self.config_entry.data)
        data.update(updates)
        # Only a TRUE strategy commit clears the recovery state: an
        # ``inbound_recovered_after_restore`` merge (no strategy in ``updates``)
        # leaves the state, because a single autonomous reconnect on an already-
        # external endpoint does not prove a durable inbound future.
        if CONF_CONNECTION_STRATEGY in updates:
            data.pop(CONF_STRATEGY_TRANSITION_STATE, None)
        route_host, route_port, route_refusal = earned_advertised_route(
            committed_strategy=updates.get(CONF_CONNECTION_STRATEGY),
            terminal=terminal,
            attempted_host=advertised_host,
            attempted_port=advertised_port,
        )
        if route_refusal:
            return route_refusal
        if route_host:
            data[CONF_ADVERTISED_SERVER_IP] = route_host
            data[CONF_ADVERTISED_TCP_PORT] = route_port
        refusal = merge_recovery_contract(data, terminal)
        if refusal:
            return refusal
        # ``async_update_entry`` accepts an options MAPPING, not ``None``. Keep
        # the full existing mapping when no option delta exists so the atomic
        # data commit cannot succeed and then raise while wrapping options.
        options = dict(self.config_entry.options)
        # Pass options even with an empty option_payload when stale advertised
        # keys must be dropped, so they can never survive the commit and re-mask
        # the new canonical data route.
        if option_updates:
            options.update(option_updates)
        if route_host:
            options.pop(CONF_ADVERTISED_SERVER_IP, None)
            options.pop(CONF_ADVERTISED_TCP_PORT, None)
        self._async_update_entry_without_reload(data=data, options=options)
        # Exactly ONE reload, and only after the whole terminal state (axes +
        # contract) is consistent.
        self.hass.config_entries.async_schedule_reload(self.config_entry.entry_id)
        return ""

    def _durable_transition_collector_pn(
        self, *, identity_registry=None, owner_id: str = ""
    ) -> str:
        """Return the strongly-proven PN owned by this entry, or ``""``.

        A live/snapshot PN is transient and cannot key durable rollback facts.
        The entry PN must be exact-normalized and backed by its own validated
        RecoveryContract (whose parser enforces a strong identity source).
        """

        from ...connection.recovery_contract import RecoveryContract
        from ...collector_identity import pn_is_same_identity

        raw_pn = self.config_entry.data.get(CONF_COLLECTOR_PN)
        if (
            type(raw_pn) is not str
            or not raw_pn
            or raw_pn != raw_pn.strip()
        ):
            return ""
        contract = RecoveryContract.from_entry_data(self.config_entry.data)
        if contract is not None and pn_is_same_identity(
            raw_pn, contract.collector_pn
        ):
            return raw_pn

        # Older entries intentionally received no synthetic RecoveryContract
        # during migration.  They may still earn the same boundary from the
        # exact currently-owned socket: strict SessionHandle, observed,
        # conflict-free, same PN and an authoritative FC2/DTUPN source.
        if identity_registry is None or type(owner_id) is not str or not owner_id:
            return ""
        from ...connection.session_handle import SessionHandle
        from ...collector_identity import identity_source_is_strong
        from ...connection.session_registry import CallbackSessionRegistry

        if type(identity_registry) is not CallbackSessionRegistry:
            return ""
        handle = identity_registry.session_handle_for_claimed_session(owner_id)
        if (
            type(handle) is not SessionHandle
            or not handle.observed
            or handle.conflict
            or not pn_is_same_identity(raw_pn, handle.collector_pn)
            or not any(identity_source_is_strong(s) for s in handle.identity_sources)
        ):
            return ""
        return raw_pn

    async def _async_persist_cloud_rollback_selection(
        self,
        selection,
        *,
        collector_pn: str | None = None,
        identity_registry=None,
        owner_id: str = "",
    ) -> str:
        """PERSIST-BEFORE-WRITE the user's typed cloud rollback selection.

        Durably saves the chosen endpoint as the original-endpoint WHOLE RECORD in
        ``entry.data`` (canonical; the stale ``options`` copies are dropped in the
        SAME local update so they can never shadow) AND in the PN-bound collector
        registry, BEFORE any endpoint write/apply/reboot/UDP. Reuses the existing
        original-endpoint fields and registry (no second rollback record).

        Returns ``""`` on success or a typed refusal reason. On any failure NO
        endpoint write follows; a safely-written local entry intent may remain for
        retry but is never presented as a wire-confirmed endpoint (the policy,
        strategy and endpoint-written provenance are untouched here).
        """

        from ...connection.strategy_transition import (
            TRANSITION_ROLLBACK_PERSIST_FAILED,
            TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED,
        )

        # Registry is mandatory + PN-bound. A missing PN (or a virtual bridge with
        # no durable registry identity) is refused BEFORE any write.
        durable_pn = self._durable_transition_collector_pn(
            identity_registry=identity_registry, owner_id=owner_id
        )
        if (
            not durable_pn
            or (collector_pn is not None and collector_pn != durable_pn)
            or self.collector_capabilities.virtual_bridge
        ):
            return TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED

        observed_at = datetime.now(timezone.utc).isoformat()
        record = {
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: selection.endpoint_value,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: selection.persistence_source,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: selection.catalog_profile_key,
            CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT: observed_at,
        }
        # 1. entry.data whole-record (canonical owner); drop stale options copies.
        try:
            data = dict(self.config_entry.data)
            data.update(record)
            options = dict(self.config_entry.options)
            for key in record:
                options.pop(key, None)
            self._async_update_entry_without_reload(data=data, options=options)
            self._remembered_collector_server_endpoint = selection.endpoint_value
        except Exception as exc:  # pragma: no cover - defensive
            logger.info("Rollback selection entry persist failed: %s", exc)
            return TRANSITION_ROLLBACK_PERSIST_FAILED

        # 2. PN-bound registry.  The durable-PN boundary above requires either a
        # strong RecoveryContract or the exact strongly identified owned socket;
        # the registry's syntax check is not treated as identity evidence.
        # ``force`` is additionally source-gated by the registry, so only these
        # explicit user-selection source tokens can replace an older fact.
        collector = getattr(self.data, "collector", None)
        last_seen_ip = str(getattr(collector, "remote_ip", "") or "").strip()
        try:
            config_dir = Path(self.hass.config.config_dir)
            await self.hass.async_add_executor_job(
                lambda: remember_collector_original_endpoint(
                    config_dir=config_dir,
                    collector_pn=durable_pn,
                    original_endpoint_raw=selection.endpoint_value,
                    cloud_profile_key=selection.catalog_profile_key,
                    source=selection.persistence_source,
                    observed_at=observed_at,
                    last_seen_ip=last_seen_ip,
                    force=True,
                )
            )
        except Exception as exc:
            logger.info("Rollback selection registry persist failed: %s", exc)
            return TRANSITION_ROLLBACK_PERSIST_FAILED
        return ""

    async def _async_persist_inbound_rollback_endpoint(
        self,
        endpoint: str,
        *,
        collector_pn: str,
        identity_registry=None,
        owner_id: str = "",
    ) -> str:
        """Preserve a known external endpoint before an inbound overwrite.

        Registry persistence happens first.  Its existing non-force semantics
        preserve an older durable original; the exact returned whole record is
        then mirrored into canonical entry data.  A failure prevents the wire
        write, so background snapshot timing is never a correctness condition.
        """

        from ...connection.strategy_transition import (
            TRANSITION_INBOUND_ROLLBACK_PERSIST_FAILED,
            TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED,
        )

        durable_pn = self._durable_transition_collector_pn(
            identity_registry=identity_registry, owner_id=owner_id
        )
        if not durable_pn or collector_pn != durable_pn:
            return TRANSITION_ROLLBACK_REGISTRY_PN_REQUIRED
        if self.collector_capabilities.virtual_bridge:
            # A local ESP bridge has no vendor-cloud endpoint to preserve.  Its
            # server endpoint is the Home Assistant route itself, so moving it
            # to another confirmed HA address must not be blocked by the cloud
            # rollback registry.  The transition still requires the strong
            # durable PN above and still performs the normal endpoint write,
            # controlled restart and same-PN reconnect proof.
            return ""
        normalized = ""
        if endpoint:
            try:
                normalized = _normalize_preserved_collector_server_endpoint(endpoint)
            except (TypeError, ValueError):
                return TRANSITION_INBOUND_ROLLBACK_PERSIST_FAILED
        if not normalized or self._endpoint_looks_like_local_collector_callback(
            normalized
        ):
            # No external endpoint can be learned from this live snapshot.  The
            # overwrite is safe only when an existing PN-bound durable original
            # already resolves through the normal read model.
            from ...connection.strategy_transition_context import (
                CloudRollbackEndpoint,
            )

            existing = await self.collector_cloud_rollback_context()
            if type(existing) is not CloudRollbackEndpoint or not existing.known:
                return TRANSITION_INBOUND_ROLLBACK_PERSIST_FAILED
            return ""

        observed_at = datetime.now(timezone.utc).isoformat()
        collector = getattr(self.data, "collector", None)
        last_seen_ip = str(getattr(collector, "remote_ip", "") or "").strip()
        try:
            config_dir = Path(self.hass.config.config_dir)
            saved = await self.hass.async_add_executor_job(
                lambda: remember_collector_original_endpoint(
                    config_dir=config_dir,
                    collector_pn=durable_pn,
                    original_endpoint_raw=normalized,
                    source="runtime_observed_before_inbound_transition",
                    observed_at=observed_at,
                    last_seen_ip=last_seen_ip,
                )
            )
            record = {
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT: saved.original_endpoint_raw,
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_SOURCE: saved.source,
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_PROFILE_KEY: saved.cloud_profile_key,
                CONF_COLLECTOR_ORIGINAL_SERVER_ENDPOINT_OBSERVED_AT: saved.observed_at,
            }
            data = dict(self.config_entry.data)
            data.update(record)
            options = dict(self.config_entry.options)
            for key in record:
                options.pop(key, None)
            self._async_update_entry_without_reload(data=data, options=options)
            self._remembered_collector_server_endpoint = saved.original_endpoint_raw
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("Inbound rollback endpoint persist failed: %s", exc)
            return TRANSITION_INBOUND_ROLLBACK_PERSIST_FAILED
        return ""

    async def async_run_connection_strategy_transition(
        self,
        *,
        target_strategy: str,
        inbound_endpoint: str = "",
        callback_target_ip: str = "",
        advertised_host: str = "",
        advertised_port: int = 0,
        option_payload: dict[str, Any] | None = None,
        cloud_rollback_selection: Any = None,
    ):
        """THE verified strategy-transition facade entry point (Batch 8).

        Every high-level "switch how the collector connects" action funnels
        here. Since CP2A the sole user entry point is the options-flow strategy
        transition (with mandatory risk consent); the writable operation-mode
        select was removed. This method only ASSEMBLES the runtime-bound
        dependencies; the order, the proof requirements and the axis-write
        policy live in
        ``connection.strategy_transition.async_run_strategy_transition``.

        Addresses are caller-confirmed input: ``inbound_endpoint`` is the
        endpoint the user (or a previously-confirmed configuration) stated,
        and the callback route's advertised host/port default to the entry's
        configured advertised values — never to a peer IP and never to a
        local-vs-external guess.
        """

        from ...collector.silent_session_probe import SilentSessionIdentityProbeChannel
        from ...connection.callback_ledger import get_callback_trigger_ledger
        from ...connection.strategy_transition import (
            TRANSITION_ALREADY_RUNNING,
            StrategyTransitionResult,
            async_run_strategy_transition,
        )
        from ...connection.recovery.verification import CallbackRecoveryRoute
        from ...connection.recovery.verification import ObservedSessionRestartChannel
        from ...collector_endpoint import CollectorEndpointWriteShape
        from ...passive_discovery import get_callback_session_registry

        target = str(target_strategy or "").strip()
        endpoint_shape = self.collector_endpoint_write_shape
        if (
            target == CONNECTION_STRATEGY_INBOUND
            and type(endpoint_shape) is CollectorEndpointWriteShape
            and endpoint_shape.port_is_fixed
            and (
                type(advertised_host) is not str
                or not advertised_host
                or advertised_host != advertised_host.strip()
                or type(advertised_port) is not int
                or type(advertised_port) is bool
                or advertised_port != endpoint_shape.fixed_port
            )
        ):
            return StrategyTransitionResult(
                success=False,
                target_strategy=target,
                failure_reason="transition_advertised_route_invalid",
            )
        entry_id = self.config_entry.entry_id
        # Atomic per-entry exclusive lease: acquired SYNCHRONOUSLY before the
        # first await/side effect (no check-then-await-then-set window), and
        # always released in the finally below — cancellation included.
        from ...connection.strategy_transition import (
            STRATEGY_TRANSITION_LEASES,
        )

        # THE one production lease: acquired SYNCHRONOUSLY here, and the
        # try/finally opens IMMEDIATELY after a successful acquire so that
        # dependency assembly, probe construction/open, payload validation
        # and the transition all release the lease on any exception or
        # cancellation. A second concurrent call is typed-refused.
        if not STRATEGY_TRANSITION_LEASES.acquire(entry_id):
            # Busy: another endpoint operation owns this entry. A concurrent
            # strategy transition/repair keeps the historical
            # ``transition_already_running`` reason; any OTHER owner (proxy
            # capture, shadow learning, a manual write, bind/rollback) surfaces
            # the neutral typed busy reason with the active operation kind.
            from ...connection.collector_endpoint_operation import (
                COLLECTOR_ENDPOINT_OPERATION_AUTHORITY,
                COLLECTOR_ENDPOINT_OPERATION_BUSY,
                OPERATION_STRATEGY_REPAIR,
                OPERATION_STRATEGY_TRANSITION,
            )

            active = COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.active_operation(entry_id)
            reason = (
                TRANSITION_ALREADY_RUNNING
                if active in ("", OPERATION_STRATEGY_TRANSITION, OPERATION_STRATEGY_REPAIR)
                else COLLECTOR_ENDPOINT_OPERATION_BUSY
            )
            return StrategyTransitionResult(
                success=False,
                target_strategy=target,
                failure_reason=reason,
            )
        silent_probe = None
        runtime_operation_locked = False
        try:
            from ...connection.strategy_transition_recovery import (
                StrategyTransitionRecoveryState,
            )
            from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY

            # One strategy transition is one exclusive runtime transport
            # operation.  In particular, an ordinary poll may not race the
            # callback bootstrap, endpoint write or reboot.  The endpoint lease
            # above owns cross-feature mutation; this lock owns this entry's
            # live transport frames.
            await self._runtime_operation_lock.acquire()
            runtime_operation_locked = True

            registry = get_callback_session_registry(self.hass)
            spec = self._connection_spec
            inbound_listener_port = int(getattr(spec, "tcp_port", 0) or 0)
            inbound_expected_listener_port = 0
            callback_listener_port = int(getattr(spec, "tcp_port", 0) or 0)
            if (
                target == CONNECTION_STRATEGY_INBOUND
                and type(endpoint_shape) is CollectorEndpointWriteShape
                and endpoint_shape.port_is_fixed
            ):
                inbound_listener_port = endpoint_shape.fixed_port
                inbound_expected_listener_port = endpoint_shape.fixed_port
            probe_listener_port = (
                inbound_listener_port
                if target == CONNECTION_STRATEGY_INBOUND
                else callback_listener_port
            )

            # The durable PN claim can remember a proof socket which has since
            # closed while runtime has already adopted a newer routed socket.
            # Select the registry's canonical trusted handle first.  An idle
            # callback-mode entry switching to inbound gets exactly one normal
            # runtime callback bootstrap, then the SAME exact selection is run
            # again.  No peer-IP/session-order fallback exists here.
            await self._async_prepare_strategy_transition_management_session(
                registry=registry,
                entry_id=entry_id,
                target_strategy=target,
                timeout=(
                    DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait
                ),
            )

            def _resolve_owned_session_id() -> str:
                """Return only the claim's exact currently-trusted socket."""

                if registry is None:
                    return ""
                sid = str(registry.claimed_session_id(entry_id) or "")
                if not sid:
                    return ""
                handle = registry.session_handle_for_owned_session(entry_id, sid)
                if handle is None or not handle.observed or handle.conflict:
                    return ""
                return sid

            def _claimed_session_id() -> str:
                return _resolve_owned_session_id()

            # Build the callback route FIRST so the confirmed-restore hook can
            # snapshot the exact repair route into the persisted recovery state.
            callback_route = None
            if target == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
                # NAT-strict: the advertised host/port are EXPLICIT caller input
                # (the confirm form / the entry's explicit advertised_* config).
                # No silent fallback to a local interface address or local port.
                effective_host = str(advertised_host or "").strip()
                effective_port = int(advertised_port or 0)
                target_ip = str(callback_target_ip or "").strip() or str(
                    getattr(spec, "collector_ip", "") or ""
                ).strip()
                if target_ip and effective_host and effective_port:
                    callback_route = CallbackRecoveryRoute(
                        bind_ip=str(getattr(spec, "server_ip", "") or "0.0.0.0"),
                        trigger_target_ip=target_ip,
                        trigger_udp_port=int(getattr(spec, "udp_port", 0) or 0),
                        advertised_ha_host=effective_host,
                        advertised_ha_port=effective_port,
                        listener_port=callback_listener_port,
                    )

            # CP2B.2: assemble read-only validation/persistence capabilities for
            # the core authority.  This facade performs no early selection
            # persistence: the authority invokes it only after route/state/live-
            # session/management preflights and before write-ahead + wire I/O.
            restore_endpoint = ""
            if target == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
                needs_restore = (
                    self.endpoint_control_policy
                    == ENDPOINT_CONTROL_INTEGRATION_MANAGED
                )

            async def _validate_rollback_selection(selection) -> str:
                from ...collector.cloud_rollback_catalog import (
                    ROLLBACK_SELECTION_STALE,
                    validate_cloud_rollback_selection,
                )
                from ...connection.strategy_transition import (
                    TRANSITION_ROLLBACK_SELECTION_INVALID,
                    TRANSITION_ROLLBACK_SELECTION_STALE,
                )

                # Resolve at the exact authority call, not during facade
                # assembly: a candidate changed after the chooser render or
                # while the transition was queued is stale, never silently
                # substituted.
                current_rollback_candidate = (
                    await self.collector_cloud_rollback_context()
                )
                status = validate_cloud_rollback_selection(
                    selection,
                    confirmed_candidate=current_rollback_candidate,
                )
                if not status:
                    return ""
                if status == ROLLBACK_SELECTION_STALE:
                    return TRANSITION_ROLLBACK_SELECTION_STALE
                return TRANSITION_ROLLBACK_SELECTION_INVALID

            durable_entry_pn = self.config_entry.data.get(CONF_COLLECTOR_PN)

            async def _persist_rollback_selection(selection) -> str:
                return await self._async_persist_cloud_rollback_selection(
                    selection,
                    collector_pn=durable_entry_pn,
                    identity_registry=registry,
                    owner_id=entry_id,
                )

            # PRE-BUILD the TYPED recovery state (startable pending phase) from
            # the route + durable PN, BEFORE any physical side effect. It
            # carries only what the repair reads (route + PN); a value it cannot
            # validate yields None, and the authority then refuses the
            # transition instead of ever leaving "endpoint external, no recovery
            # state". The authority (not this facade) owns WHEN it is persisted.
            def _build_recovery_state():
                if callback_route is None:
                    return None
                try:
                    state = StrategyTransitionRecoveryState.create(
                        collector_pn=self.config_entry.data.get(CONF_COLLECTOR_PN),
                        now=datetime.now(timezone.utc).isoformat(),
                        trigger_target_host=callback_route.trigger_target_ip,
                        trigger_udp_port=callback_route.trigger_udp_port,
                        advertised_host=callback_route.advertised_ha_host,
                        advertised_port=callback_route.advertised_ha_port,
                        # TWO distinct transport binds: the UDP trigger bind is
                        # the route's bind_ip (effective server IP); the TCP
                        # listener bind is the runtime's ACTUAL listener host, so
                        # a cold repair borrows the very same shared listener the
                        # runtime binds (never a hardcoded host).
                        trigger_bind_host=callback_route.bind_ip,
                        listener_bind_host=str(
                            getattr(self._runtime, "listener_bind_host", "") or ""
                        ),
                        local_listener_port=callback_route.listener_port,
                    )
                except (ValueError, TypeError):
                    return None
                # Only accept a state whose record round-trips through the strict
                # parser -- what is persisted later is provably valid.
                if StrategyTransitionRecoveryState.from_record(state.to_record()) is None:
                    return None
                return state

            recovery_state = _build_recovery_state()

            raw_current_endpoint = self.data.values.get(
                "collector_server_endpoint", ""
            )
            current_endpoint = (
                raw_current_endpoint
                if type(raw_current_endpoint) is str
                and raw_current_endpoint == raw_current_endpoint.strip()
                else ""
            )
            endpoint_needs_write = bool(inbound_endpoint) and (
                self._endpoint_effective_parts(current_endpoint)
                != self._endpoint_effective_parts(inbound_endpoint)
            )
            persisted_advertised_host = self.config_entry.data.get(
                CONF_ADVERTISED_SERVER_IP
            )
            persisted_advertised_port = self.config_entry.data.get(
                CONF_ADVERTISED_TCP_PORT
            )
            route_metadata_needs_reverification = bool(
                target == CONNECTION_STRATEGY_INBOUND
                and self.connection_strategy == CONNECTION_STRATEGY_INBOUND
                and not endpoint_needs_write
                and type(endpoint_shape) is CollectorEndpointWriteShape
                and endpoint_shape.port_is_fixed
                and (
                    persisted_advertised_host != advertised_host
                    or persisted_advertised_port != advertised_port
                )
            )
            current_external_endpoint = ""
            if current_endpoint:
                try:
                    normalized_current_endpoint = (
                        _normalize_preserved_collector_server_endpoint(
                            current_endpoint
                        )
                    )
                except ValueError:
                    normalized_current_endpoint = ""
                if normalized_current_endpoint and not self._endpoint_looks_like_local_collector_callback(
                    normalized_current_endpoint
                ):
                    current_external_endpoint = normalized_current_endpoint

            async def _persist_inbound_rollback(endpoint: str) -> str:
                return await self._async_persist_inbound_rollback_endpoint(
                    endpoint,
                    collector_pn=durable_entry_pn,
                    identity_registry=registry,
                    owner_id=entry_id,
                )

            def _management_channel_for_session(
                expected_session_id: str,
            ) -> ObservedSessionRestartChannel:
                listener_port = 0
                if registry is not None:
                    for observed_session in registry.observed_sessions_per_socket():
                        if observed_session.session_id == expected_session_id:
                            listener_port = observed_session.listener_port
                            break
                # The session's listener is observed wire location, never an
                # endpoint/peer-IP guess.  A missing location remains invalid
                # and the channel fails closed before transport creation.
                return ObservedSessionRestartChannel(
                    host=str(
                        getattr(self._runtime, "listener_bind_host", "")
                        or "0.0.0.0"
                    ),
                    port=listener_port,
                    collector_pn=str(durable_entry_pn or ""),
                    session_id=expected_session_id,
                    session_id_provider=lambda: expected_session_id,
                    handle_provider=lambda: (
                        registry.session_handle_for_owned_session(
                            entry_id,
                            expected_session_id,
                        )
                        if registry is not None
                        else None
                    ),
                )

            def _on_written(value: str) -> None:
                self._persist_connection_axes(
                    {
                        CONF_ENDPOINT_CONTROL_POLICY: ENDPOINT_CONTROL_INTEGRATION_MANAGED,
                        CONF_ENDPOINT_WRITTEN_VALUE: value,
                        CONF_ENDPOINT_WRITTEN_AT: datetime.now(timezone.utc).isoformat(),
                    }
                )
                self._publish_snapshot_values(collector_server_endpoint=value)

            def _persist_pending(state) -> None:
                # WRITE-AHEAD durable intent (physical write only; the authority
                # owns the ORDER and calls this BEFORE the first side effect).
                # The old strategy, endpoint-control policy and endpoint-write
                # provenance are LEFT UNCHANGED -- only the pending recovery
                # marker is added, so a crash between here and the confirmed
                # restore is a repairable pending state, never a silent
                # strategy/policy flip.
                self._persist_connection_axes(
                    {CONF_STRATEGY_TRANSITION_STATE: state.to_record()}
                )

            def _persist_confirmed(state) -> None:
                # ONE durable write at the CONFIRMED restore (no crash window):
                # the policy becomes external, the endpoint-write provenance is
                # cleared, AND the recovery state advances to the
                # confirmed-unproven phase supplied by the authority. If HA
                # crashes right after this, the entry is never a silently-flat
                # "inbound + external": it holds the recoverable state.
                self._persist_connection_axes(
                    {
                        CONF_ENDPOINT_CONTROL_POLICY: ENDPOINT_CONTROL_EXTERNAL,
                        CONF_STRATEGY_TRANSITION_STATE: state.to_record(),
                    },
                    clear=(CONF_ENDPOINT_WRITTEN_VALUE, CONF_ENDPOINT_WRITTEN_AT),
                )

            def _on_restored(value: str) -> None:
                # UI-only: reflect the restored endpoint in the live snapshot.
                # The DURABLE entry-data write happened in ``_persist_confirmed``
                # (ONE write) at the same confirmed-restore boundary.
                self._publish_snapshot_values(collector_server_endpoint=value)

            async def _prepare_local_listener(port: int) -> None:
                # NAT split: prepare the LOCAL listener bind port — never the
                # advertised/forwarded port the collector is told about.
                ensure_listener = getattr(
                    self._runtime, "async_ensure_callback_listener", None
                )
                if ensure_listener is not None and int(port or 0) > 0:
                    await ensure_listener(int(port))

            async def _commit(
                updates: dict[str, Any], terminal, option_updates
            ) -> str:
                # The whole commit body lives in a testable method so the DI
                # boundary (route persistence + options cleanup + one update / one
                # reload) can be exercised on a real coordinator without the
                # facade preamble.
                return self._apply_transition_commit(
                    updates,
                    terminal,
                    option_updates,
                    advertised_host=advertised_host,
                    advertised_port=advertised_port,
                )

            silent_probe = SilentSessionIdentityProbeChannel(
                host=str(getattr(spec, "server_ip", "") or "0.0.0.0"),
                port=probe_listener_port,
            )
            await silent_probe.async_open()
            return await async_run_strategy_transition(
                target_strategy=target,
                current_strategy=self.connection_strategy,
                collector_pn=str(
                    self.config_entry.data.get(CONF_COLLECTOR_PN) or ""
                ),
                owner_id=entry_id,
                registry=registry,
                claimed_session_id=_claimed_session_id,
                clock=lambda: datetime.now(timezone.utc).isoformat(),
                commit=_commit,
                ledger=get_callback_trigger_ledger(),
                inbound_endpoint=inbound_endpoint,
                endpoint_needs_write=endpoint_needs_write,
                route_metadata_needs_reverification=route_metadata_needs_reverification,
                management_channel_factory=_management_channel_for_session,
                prepare_listener=_prepare_local_listener,
                local_listener_port=inbound_listener_port,
                expected_inbound_listener_port=inbound_expected_listener_port,
                on_endpoint_written=_on_written,
                current_external_endpoint=current_external_endpoint,
                persist_inbound_rollback_endpoint=_persist_inbound_rollback,
                callback_route=callback_route,
                endpoint_control_policy=self.endpoint_control_policy,
                restore_endpoint=restore_endpoint,
                cloud_rollback_selection=cloud_rollback_selection,
                validate_rollback_selection=_validate_rollback_selection,
                persist_rollback_selection=_persist_rollback_selection,
                management_action_available=self.collector_management_action_available,
                on_endpoint_restored=_on_restored,
                persist_pending=_persist_pending,
                persist_confirmed=_persist_confirmed,
                recovery_state=recovery_state,
                option_payload=option_payload,
                silent_session_probe=silent_probe,
            )
        finally:
            STRATEGY_TRANSITION_LEASES.release(entry_id)
            if runtime_operation_locked:
                self._runtime_operation_lock.release()
            if silent_probe is not None:
                await silent_probe.async_close()

    async def async_set_control_mode(self, mode: str) -> str:
        """Persist one integration control policy mode and reload the entry."""

        normalized_mode = str(mode or "").strip()
        if normalized_mode not in {CONTROL_MODE_AUTO, CONTROL_MODE_READ_ONLY, CONTROL_MODE_FULL}:
            raise ValueError("control_mode_invalid")
        if normalized_mode == self.control_mode:
            return normalized_mode

        data = dict(self.config_entry.data)
        options = dict(self.config_entry.options)
        data[CONF_CONTROL_MODE] = normalized_mode
        options[CONF_CONTROL_MODE] = normalized_mode
        self._async_update_entry_without_reload(
            data=data,
            options=options,
        )
        # The exposed capability-entity surface depends on the control mode
        # (untested capabilities exist only in full-control mode), and the
        # platforms materialize entities exactly once at setup — without a
        # reload, switching the mode changes nothing the user can see
        # (0.3.0-beta.1 field report: MUST PV1800 got no controls after
        # enabling full control). The suppressed update above plus one
        # explicit scheduled reload keeps this a single deterministic reload.
        self.hass.config_entries.async_schedule_reload(self.config_entry.entry_id)
        return normalized_mode



__all__ = ["CoordinatorStrategyTransitionMixin"]
