"""LinkWireAuthorityMixin ownership slice for the runtime link."""

from __future__ import annotations

from .common import (
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    ADAPTER_NONE,
    CollectorAtTransport,
    CollectorMetadataRouteSet,
    CollectorTransport,
    ConfirmedSessionProtocolEvidence,
    ConfirmedWireBinding,
    SessionHandle,
    _RUNTIME_SESSION_ENTRY_KEY,
    asyncio,
    build_collector_metadata_routes,
    logger,
    pn_is_same_identity,
    reconcile_pn,
)


class LinkWireAuthorityMixin:
    """Methods owned by LinkWireAuthorityMixin."""

    def _payload_transports(self) -> tuple[CollectorTransport, ...]:
        transports: list[CollectorTransport] = [self._transport]
        transports.extend(
            self._auxiliary_transports[port]
            for port in sorted(self._auxiliary_listener_ports)
            if port in self._auxiliary_transports
        )
        return tuple(transports)

    def _at_transports(self) -> tuple[CollectorAtTransport, ...]:
        transports: list[CollectorAtTransport] = [self._at_transport]
        transports.extend(
            self._auxiliary_at_transports[port]
            for port in sorted(self._auxiliary_listener_ports)
            if port in self._auxiliary_at_transports
        )
        return tuple(transports)

    def _selected_connected_remote_ip(self) -> tuple[str, bool]:
        if self._collector_pn:
            return "", False
        if self._collector_ip:
            return self._collector_ip, False

        payload_ips = {
            str(transport.collector_info.remote_ip or "").strip()
            for transport in self._payload_transports()
            if transport.connected and str(transport.collector_info.remote_ip or "").strip()
        }
        at_ips = {
            str(transport.collector_info.remote_ip or "").strip()
            for transport in self._at_transports()
            if transport.connected and str(transport.collector_info.remote_ip or "").strip()
        }

        if len(payload_ips) > 1 or len(at_ips) > 1:
            return "", True
        if payload_ips and at_ips:
            if payload_ips == at_ips:
                return next(iter(payload_ips)), False
            return "", True
        if payload_ips:
            return next(iter(payload_ips)), False
        if at_ips:
            return next(iter(at_ips)), False
        return "", False

    def _connected_payload_transport(self) -> CollectorTransport | None:
        selected_remote_ip, ambiguous = self._selected_connected_remote_ip()
        if ambiguous:
            return None

        connected: list[CollectorTransport] = []
        for transport in self._payload_transports():
            if not transport.connected:
                continue
            remote_ip = str(transport.collector_info.remote_ip or "").strip()
            if selected_remote_ip and remote_ip and remote_ip != selected_remote_ip:
                continue
            connected.append(transport)
            if transport.collector_info.heartbeat_fresh:
                return transport
        return connected[0] if connected else None

    def _connected_at_transport(self) -> CollectorAtTransport | None:
        selected_remote_ip, ambiguous = self._selected_connected_remote_ip()
        if ambiguous:
            return None

        for transport in self._at_transports():
            if not transport.connected:
                continue
            remote_ip = str(transport.collector_info.remote_ip or "").strip()
            if selected_remote_ip and remote_ip and remote_ip != selected_remote_ip:
                continue
            return transport
        return None

    def _apply_confirmed_session_protocol_to_transports(self) -> None:
        """Push the durable confirmed session protocol to every transport owner.

        This is the DURABLE probe-permission channel, DISTINCT from
        ``set_negotiated_wire`` (the live-wire activation). It dynamically
        (un)registers the confirmed listener protocol owner on the running
        primary AND auxiliary transports so a same-process SILENT reconnect can
        be identity-probed WITHOUT an HA restart and WITHOUT a listener rebuild.
        The value is the CONFIRMED protocol only (live handle or PN-validated
        confirmed binding); "" clears the owner (binding dropped on a durable-PN
        change). The inferred/expected cloud-family protocol is NEVER passed
        here. Called by the single binding writer ``_adopt_trusted_live_binding``
        on every adopt/clear so the transport owner always mirrors the binding.
        """

        confirmed_protocol = self._confirmed_session_protocol()
        for transport in (*self._payload_transports(), *self._at_transports()):
            if callable(getattr(transport, "set_confirmed_session_protocol", None)):
                transport.set_confirmed_session_protocol(confirmed_protocol)

    def _adopt_trusted_live_binding(self) -> None:
        """Adopt the current trusted live wire as the confirmed binding.

        This is the ONLY writer of ``_confirmed_wire_binding``. It is an explicit
        lifecycle step (called from the connect path and the owned-session
        monitor), never a side effect of a diagnostics/accessor read. A trusted
        observed handle of the same durable identity is adopted as an immutable
        ``ConfirmedWireBinding`` (durable wire facts only, no socket metadata).
        A conflict or an unobserved handle changes nothing. A stale binding for a
        now-different durable identity is dropped.
        """

        durable_pn = str(self._collector_pn or "").strip()
        handle = self._live_session_handle()
        live_pn = str(getattr(handle, "collector_pn", "") or "").strip()
        # Invariant: a confirmed binding requires a durable ENTRY PN AND a live
        # session PN of the SAME short/full identity. An unidentified live
        # session, an entry without a durable PN, or a foreign live identity can
        # never create or overwrite the binding. The stored PN is the preferred
        # (fuller) of the two, so a later short/full enrichment stays one
        # identity.
        if durable_pn and live_pn and pn_is_same_identity(durable_pn, live_pn):
            preferred_pn = reconcile_pn(durable_pn, live_pn)
            binding = ConfirmedWireBinding.from_handle(handle, collector_pn=preferred_pn)
            if binding is not None:
                self._confirmed_wire_binding = binding
                # A newly confirmed live wire is durable probe permission: push it
                # to the running transports so a later silent same-PN reconnect can
                # be identity-probed without a rebuild.
                self._apply_confirmed_session_protocol_to_transports()
                return
        # No positive evidence to adopt. Never overwrite an existing binding with
        # a foreign/absent identity; only drop one left over from a rebind to a
        # genuinely different durable identity.
        existing = getattr(self, "_confirmed_wire_binding", None)
        if existing is not None and durable_pn:
            if not existing.collector_pn or not pn_is_same_identity(
                durable_pn, existing.collector_pn
            ):
                self._confirmed_wire_binding = None
        # Whether the binding was just dropped (durable-PN change) or simply
        # unchanged, re-assert the confirmed owner on the transports so a cleared
        # binding also unregisters the listener protocol owner.
        self._apply_confirmed_session_protocol_to_transports()

    def _seed_confirmed_wire_binding_from_evidence(
        self,
        evidence: "ConfirmedSessionProtocolEvidence | None",
    ) -> None:
        """Seed the confirmed binding from confirmed-live evidence -- fail-closed.

        This is a TRUST BOUNDARY, not a "validated by construction" shortcut. The
        object is re-validated by ``ConfirmedSessionProtocolEvidence.coerce``,
        which rejects anything that is not a genuine evidence instance (a
        duck-typed ``SimpleNamespace`` never passes) AND re-checks every
        invariant against the entry PN -- ``source == live_session``, a known
        confirmed wire protocol, a non-empty durable PN, and the same short/full
        identity. A forged instance built via the raw dataclass constructor
        (bad source / unknown protocol / empty PN) therefore seeds nothing. Any
        untrusted input yields no binding (never an exception). A live
        SessionHandle still overrides whatever is seeded here.
        """

        validated = ConfirmedSessionProtocolEvidence.coerce(
            evidence, entry_pn=self._collector_pn
        )
        if validated is None:
            return
        # ``validated.collector_pn`` is already reconciled to the fuller identity.
        seeded = ConfirmedWireBinding.from_confirmed_protocol(
            collector_pn=validated.collector_pn,
            session_protocol=validated.protocol,
        )
        if seeded is not None:
            self._confirmed_wire_binding = seeded

    def _effective_wire_binding(self) -> ConfirmedWireBinding | None:
        """Return the confirmed wire binding for this collector (pure read).

        Never mutates runtime state. Returns ``None`` when nothing has been
        confirmed yet, or when the stored binding belongs to a now-different
        durable identity (defensively ignored without rewriting the field).
        """

        binding = getattr(self, "_confirmed_wire_binding", None)
        if binding is None:
            return None
        # A binding must carry a durable PN (the adoption invariant guarantees
        # this); a PN-less binding is never trusted.
        if not str(getattr(binding, "collector_pn", "") or "").strip():
            return None
        collector_pn = str(self._collector_pn or "").strip()
        if (
            collector_pn
            and binding.collector_pn
            and not pn_is_same_identity(collector_pn, binding.collector_pn)
        ):
            return None
        return binding

    def _confirmed_session_protocol(self) -> str:
        """Return the confirmed session protocol, else an empty string.

        Confirmed evidence only: a trusted live-observed wire (strongest) or the
        confirmed wire binding (live-derived this session, or persisted
        confirmed-live seeded for the same durable PN). This is what may register
        a listener protocol owner and seed a bootstrap adapter. No preliminary
        cloud-family protocol exists at this boundary.
        Construction-safe: falls back to the pure binding read if the live
        handle cannot be resolved yet (transports not built).
        """

        try:
            handle = self._live_session_handle()
        except Exception:
            handle = None
        if handle is not None and not handle.conflict and handle.observed:
            if handle.uses_framed_wire:
                return "eybond_framed"
            if handle.uses_at_text_wire:
                return "at_text"
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.session_protocol
        return ""

    def has_confirmed_wire_binding(self) -> bool:
        """Return whether a live wire has ever been confirmed for this collector.

        Once true, the live session is the transport authority: cloud-family /
        configuration metadata must not drive a
        steady-state destructive transport rebuild.
        """

        return self._effective_wire_binding() is not None

    @property
    def confirmed_wire_binding(self) -> ConfirmedWireBinding | None:
        """Return the confirmed wire binding (pure read), or None."""

        return self._effective_wire_binding()

    def _has_owned_pending_session(self) -> bool:
        """Return whether the registry currently sees an owned (pending/new) socket.

        Lifecycle evidence for a handover: a socket for THIS entry's identity is
        present but has not yet become a trusted live handle. A fully absent
        socket is offline, not a handover -- there is no timeout involved. The
        domain path uses the registry's owned-session location (present even for
        a parked/identified socket); the fallback path uses the claimed handle's
        session id.
        """

        if self._domain_ownership_active():
            return self._owned_domain_session() is not None
        handle = self._live_session_handle()
        return bool(str(getattr(handle, "session_id", "") or "").strip())

    def _handover_in_progress(self) -> bool:
        """Return whether an owned session handover is genuinely in progress.

        True only when ALL hold: a confirmed binding exists; the current live
        handle is not yet a trusted (observed) session and is not in conflict;
        and the registry actually sees an owned pending/new socket for this
        entry. A confirmed binding with NO owned socket is offline/idle, never an
        endless ``reconnecting``.
        """

        if self._effective_wire_binding() is None:
            return False
        live = self._live_session_handle()
        if live.observed or live.conflict:
            return False
        return self._has_owned_pending_session()

    def _raw_live_observed_protocol(self) -> str:
        """Return the CURRENTLY observed live protocol (no binding), else ""."""

        handle = self._live_session_handle()
        if handle.uses_framed_wire:
            return "eybond_framed"
        if handle.uses_at_text_wire:
            return "at_text"
        return ""

    def _inverter_forward_adapter(self) -> str:
        """Return the adapter that must carry inverter payloads.

        Authority order, all pure reads:
        - a live ``conflict`` fails closed to no adapter (contradictory wire);
        - a trusted ``observed`` live session uses its own adapter;
        - a transient gap uses the CONFIRMED wire binding (a same-collector
          handover never downgrades the wire);
        - with NO live and NO confirmed evidence the result is ADAPTER_NONE
          (fail-closed). The inferred/persisted EXPECTED protocol is NOT an
          adapter fallback -- there is no "unknown -> framed_fc4" default, so a
          connected socket whose wire has never been observed or confirmed is
          never forwarded.
        """

        handle = self._live_session_handle()
        if handle.conflict:
            return ADAPTER_NONE
        if handle.observed:
            return handle.inverter_forward_adapter
        binding = self._effective_wire_binding()
        if binding is not None:
            # Confirmed evidence: a live-derived confirmed wire (reconnect gap) or
            # a PN-validated persisted confirmed-live protocol.
            return binding.inverter_forward_adapter
        # No confirmed evidence. FAIL CLOSED. The inverter adapter is never
        # chosen from cloud-family, endpoint, driver, or unproven persisted data,
        # and there is no legacy "unknown -> framed_fc4" fallback: a connected
        # socket without an observed/confirmed wire is not safe to forward.
        return ADAPTER_NONE

    def _collector_management_selection(self) -> tuple[str, str]:
        """Return ``(adapter_id, provenance)`` -- the SINGLE management resolver.

        The one place adapter id and provenance are decided together, so they can
        never disagree in diagnostics. Authority order (collector-management role):

        * live ``conflict`` -> ``(none, "conflict")`` -- a contradictory wire fails
          closed and the stale confirmed binding is NOT reported as effective;
        * trusted ``observed`` live session -> ``(its adapter, "live")``;
        * transient gap with a CONFIRMED binding -> ``(binding adapter, "confirmed_binding")``;
        * no live and no confirmed evidence -> ``(none, "unavailable")``.

        The inferred/expected protocol never participates.
        """

        handle = self._live_session_handle()
        if handle.conflict:
            return ADAPTER_NONE, "conflict"
        if handle.observed:
            return handle.collector_management_adapter, "live"
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.collector_management_adapter, "confirmed_binding"
        return ADAPTER_NONE, "unavailable"

    def collector_management_adapter_id(self) -> str:
        """Return the negotiated collector-management adapter id (the single switch)."""

        return self._collector_management_selection()[0]

    def collector_management_adapter_provenance(self) -> str:
        """Return the management-adapter selection provenance (see the resolver)."""

        return self._collector_management_selection()[1]

    def _collector_bootstrap_claimable(self) -> bool:
        """Return whether a pre-heartbeat collector-only bootstrap read is allowed.

        A collector-only ESP produces no inverter heartbeat until it has an
        inverter, so its identity (FC=2 param 6) must be readable before a live
        payload wire is observed. That read is allowed ONLY on a socket the entry
        already OWNS through the registry -- i.e. a registry-claimed session id.
        The configured collector target is a connection address, NOT ownership
        evidence: it does not prove any pending socket belongs to this entry, and
        two PN-less entries behind one NAT/public target would resolve to the same
        socket, so it must never by itself yield a metadata route. The claimed
        session id is PN/identity-scoped by the registry, so a foreign strong PN
        is never claimed and an ambiguous PN-less collector yields no claim (and
        therefore no route).
        """

        if self.connected:
            return False
        return bool(str(self._claimed_session_id() or "").strip())

    def collector_metadata_routes(self) -> CollectorMetadataRouteSet:
        """Return the metadata channel routes for this entry's owned collector.

        Public route-authority facade for collector-metadata TELEMETRY. It is the
        ONE place framed/AT metadata channels are selected, built from trusted,
        owned session evidence -- the live observed ``SessionHandle`` (or the
        ``ConfirmedWireBinding`` during a handover gap), plus registry ownership
        for the collector-only bootstrap. It never routes by collector kind,
        cloud family, hostname, peer IP, driver key, or an inferred/persisted
        protocol without confirmed evidence.

        Dual-channel: a framed base metadata channel and an AT supplemental
        metadata channel can be routed simultaneously. The bootstrap channel is
        offered only when no framed metadata channel is available (a framed wire
        reads param 6 in its normal sweep).
        """

        generation = self._owned_session_generation
        session_id = self._claimed_session_id()
        handle = self._live_session_handle()
        if handle.conflict:
            # A contradictory live wire fails closed: no metadata channels, and
            # the stale confirmed binding is NOT reported as effective.
            return CollectorMetadataRouteSet(
                generation=generation,
                session_id=session_id,
                provenance="conflict",
            )

        if handle.observed:
            provenance = "live"
        elif self._effective_wire_binding() is not None:
            provenance = "confirmed_binding"
        else:
            provenance = "unavailable"

        # ``active_transport`` is the connected framed payload transport (None on
        # an at_text/raw wire, since the payload rides the AT session there);
        # ``active_collector_at_transport`` is the connected AT transport. Their
        # presence already encodes the negotiated wire via the fail-closed
        # inverter-forward adapter, so metadata channel selection follows the wire
        # without re-deriving it from any discriminator.
        framed_transport = self.active_transport
        at_transport = self.active_collector_at_transport

        if (
            framed_transport is None
            and at_transport is None
            and self._collector_bootstrap_claimable()
        ):
            # Pre-heartbeat collector-only bootstrap: route the AT/bootstrap read
            # to the claimable raw AT transport (it carries the registry-mediated
            # pending-socket claim internally).
            at_transport = self._at_transport
            if provenance == "unavailable":
                provenance = "bootstrap_claimable"

        return build_collector_metadata_routes(
            framed_transport=framed_transport,
            at_transport=at_transport,
            bootstrap_transport=at_transport,
            generation=generation,
            session_id=session_id,
            provenance=provenance,
            # Durable collector identity (PN) keys the service cache/health; a
            # short PN later enriched to the full PN is the SAME identity. Never
            # the peer IP.
            identity=str(self._collector_pn or "").strip(),
        )

    @property
    def session_handle(self) -> SessionHandle:
        """Return the negotiated live session handle for this entry's collector."""

        return self._live_session_handle()

    def _domain_ownership_active(self) -> bool:
        """Return whether the domain registry is the ownership authority here."""

        return (
            getattr(self, "_callback_ownership_registry", None) is not None
            and bool(getattr(self, "_callback_entry_id", ""))
        )

    def _owned_domain_session(self):
        """Return this entry's best owned live session from the DOMAIN registry.

        The domain registry observes every shared listener in the process, so
        this is what lets an entry whose primary tcp_port is e.g. 8899 find its
        own collector dialing the 18899 listener. Location (session_id +
        listener_port) is meaningful even for a parked/identified socket that is
        still waiting to be claimed; closed / route-identity-mismatch / foreign-
        owned sockets never qualify (registry-side filtering).
        """

        if not self._domain_ownership_active():
            return None
        try:
            exact_session_id = str(
                getattr(self, "_activation_session_id", "") or ""
            ).strip()
            if exact_session_id:
                registry = self._callback_ownership_registry
                if (
                    registry.claimed_session_id(self._callback_entry_id)
                    != exact_session_id
                    or registry.session_handle_for_owned_session(
                        self._callback_entry_id,
                        exact_session_id,
                    )
                    is None
                ):
                    return None
                return next(
                    (
                        session
                        for session in registry.observed_sessions_per_socket()
                        if session.session_id == exact_session_id
                    ),
                    None,
                )
            return self._callback_ownership_registry.owned_session_location(
                self._callback_entry_id
            )
        except Exception:
            logger.debug("Domain session location lookup failed", exc_info=True)
            return None

    def _claimed_session_id(self) -> str:
        """Return the registry-claimed session id for this entry's owned session.

        Domain-registry path: the exact session id of the entry-owned observed
        session -- including a parked/identified socket that has not been
        activated yet (activation is exactly what the claim is for). Fallback
        (no domain registry): only a trusted observed-wire session is returned;
        a route-identity mismatch / not-yet-routed session negotiates to an
        unknown wire and is never handed to the transport as the claim target.
        """

        if self._domain_ownership_active():
            session = self._owned_domain_session()
            return str(getattr(session, "session_id", "") or "") if session else ""
        handle = self._live_session_handle()
        return handle.session_id if handle.observed else ""

    def _effective_transport_wire(self) -> str:
        """Return the wire selector to push down: live if observed, else confirmed.

        A genuine live wire is applied in-place (this is how a real framed<->
        at_text change is adopted -- no destructive rebuild). During a transient
        gap the confirmed binding's wire is kept so the AT/framed activation
        stays ready for the reconnecting socket instead of being cleared.
        """

        handle = self._live_session_handle()
        if not handle.conflict and handle.observed:
            return handle.transport_wire
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.transport_wire
        return handle.transport_wire

    def _apply_live_wire_to_transports(self) -> None:
        """Push the effective wire + claim target down to the transports.

        This is an explicit lifecycle path (called every connect attempt), so it
        also ADOPTS a freshly-observed trusted session as the confirmed wire
        binding. It makes the runtime the single source of truth for (a) AT-vs-
        framed activation inside the transport (live if observed, else the
        confirmed binding) and (b) which inbound socket the transport claims (the
        registry-chosen session id; empty during a gap so no stale socket is
        claimed).
        """

        # ``_adopt_trusted_live_binding`` is the single writer of the confirmed
        # binding and now re-asserts the confirmed session-protocol owner on the
        # transports itself (operation (b) below), so it is applied here too.
        self._adopt_trusted_live_binding()
        wire = self._effective_transport_wire()
        for transport in self._at_transports():
            if callable(getattr(transport, "set_negotiated_wire", None)):
                transport.set_negotiated_wire(wire)
        # Two DISTINCT operations, applied to primary AND auxiliary transports:
        # (a) set_negotiated_wire(live wire) above -> AT/framed activation now;
        # (b) set_confirmed_session_protocol(durable probe permission) via
        #     ``_apply_confirmed_session_protocol_to_transports`` (invoked by the
        #     adopt step above) -> dynamically (un)register the confirmed listener
        #     protocol owner so a same-process silent reconnect can be safely
        #     identity-probed WITHOUT an HA restart and WITHOUT a listener rebuild.
        for transport in (*self._payload_transports(), *self._at_transports()):
            if callable(getattr(transport, "set_claimed_session_provider", None)):
                transport.set_claimed_session_provider(self._claimed_session_id)

    def _iter_observed_sessions(self) -> tuple[dict[str, object], ...]:
        """Return raw observed inbound sessions across this entry's own listeners.

        Uses the public ``observed_collector_sessions`` transport facade -- never
        the listener's private ``_session_inventory``. This is the only source
        the runtime session registry reads; ownership and short/full PN identity
        matching and untrusted-state exclusion all live in the registry.
        """

        sessions: list[dict[str, object]] = []
        seen_listeners: set[str] = set()
        for transport in self._payload_transports():
            provider = getattr(transport, "observed_collector_sessions", None)
            if not callable(provider):
                continue
            listener_key = str(getattr(transport, "listener_key", "") or "")
            dedup_key = listener_key or f"transport:{id(transport)}"
            if dedup_key in seen_listeners:
                continue
            seen_listeners.add(dedup_key)
            try:
                sessions.extend(provider())
            except Exception:
                continue
        return tuple(sessions)

    def _live_session_handle(self) -> SessionHandle:
        """Return the negotiated live SessionHandle for this entry's claimed session.

        Domain-registry path (production): the handle comes from the DOMAIN
        CallbackSessionRegistry under the REAL config entry id whose claim was
        registered at setup. That registry observes every shared listener in
        the process, so the handle follows the collector to whichever listener
        port it actually dialed. Fallback path (no domain registry injected --
        standalone hubs/unit tests): the runtime's own listener-scoped registry
        under a private key. The two paths are never active simultaneously, so
        there is exactly one ownership authority at any time. Neither path scans
        listener internals; ownership is durable-PN based (peer IP never a key);
        untrusted states negotiate to an unknown wire.
        """

        if self._domain_ownership_active():
            try:
                handle = self._callback_ownership_registry.session_handle_for_entry(
                    self._callback_entry_id
                )
            except Exception:
                logger.debug("Domain session handle lookup failed", exc_info=True)
                handle = None
            return handle or SessionHandle()

        registry = self._session_registry
        collector_pn = str(self._collector_pn or "").strip()
        # Keep the registry claim aligned with this entry's durable identity so
        # the handle represents the entry-owned session only. Re-claim only when
        # the durable PN changes (e.g. after a session-profile reconcile).
        if self._runtime_claim_pn != collector_pn:
            registry.release(_RUNTIME_SESSION_ENTRY_KEY)
            if collector_pn:
                try:
                    registry.claim(
                        _RUNTIME_SESSION_ENTRY_KEY,
                        collector_pn=collector_pn,
                    )
                except ValueError as exc:
                    # This should be unreachable for the normal runtime-scoped
                    # registry, but do not cache a failed claim as successful:
                    # keep the handle unknown and retry on the next observation
                    # cycle instead of freezing wire negotiation until PN changes.
                    logger.debug(
                        "Runtime callback-session claim rejected; will retry: %s",
                        exc,
                    )
                    self._runtime_claim_pn = ""
                    return SessionHandle()
            self._runtime_claim_pn = collector_pn
        if not collector_pn:
            return SessionHandle()
        return registry.session_handle_for_entry(_RUNTIME_SESSION_ENTRY_KEY) or SessionHandle()

    async def _async_wait_for_at_connection(self, *, timeout: float) -> bool:
        transports = self._at_transports()
        if len(transports) == 1:
            return await transports[0].wait_until_connected(timeout=timeout) and transports[0].connected

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if self.active_collector_at_transport is not None:
                return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            wait_timeout = min(0.1, remaining)
            for transport in transports:
                ok = await transport.wait_until_connected(timeout=wait_timeout)
                if ok and self._connected_at_transport() is not None:
                    return True

    async def _async_wait_for_payload_connection(self, *, timeout: float) -> bool:
        transports = self._payload_transports()
        if len(transports) == 1:
            return await transports[0].wait_until_connected(timeout=timeout) and transports[0].connected

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if self.active_transport is not None:
                return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False

            wait_timeout = min(0.1, remaining)
            for transport in transports:
                ok = await transport.wait_until_connected(timeout=wait_timeout)
                if ok and self._connected_payload_transport() is not None:
                    return True

    async def _async_wait_for_payload_heartbeat(self, *, timeout: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        first_pass = True
        while True:
            # A collector may replace its TCP socket while this wait is in
            # progress. Re-resolve the registry-owned session and connected
            # transport on every pass instead of pinning the socket that was
            # live at method entry. A transient no-socket window is therefore
            # part of handover, not an immediate heartbeat failure.
            await self._async_follow_owned_session_listener()
            self._apply_live_wire_to_transports()
            selected_remote_ip, ambiguous = self._selected_connected_remote_ip()
            if ambiguous:
                return False
            transports = tuple(
                transport
                for transport in self._payload_transports()
                if transport.connected
                and (
                    not selected_remote_ip
                    or not str(transport.collector_info.remote_ip or "").strip()
                    or str(transport.collector_info.remote_ip or "").strip()
                    == selected_remote_ip
                )
            )
            connected_wait_completed = False
            for transport in transports:
                if not transport.connected:
                    continue

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return False

                wait_timeout = timeout if first_pass else remaining
                ok = await transport.wait_until_heartbeat(timeout=wait_timeout)
                if ok:
                    return True
                # A connected transport returning False exhausted its heartbeat
                # wait normally. Only a socket that vanished during the wait is
                # handover evidence and warrants re-resolving a replacement.
                if transport.connected:
                    connected_wait_completed = True

            if connected_wait_completed:
                return False

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            first_pass = False
            await asyncio.sleep(min(0.05, remaining))
