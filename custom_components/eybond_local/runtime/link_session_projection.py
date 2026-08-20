"""LinkSessionProjectionMixin ownership slice for the runtime link."""

from __future__ import annotations

from .link_common import (
    ADAPTER_INVERTER_RAW_PASSTHROUGH,
    ADAPTER_NONE,
    Callable,
    CollectorAtTransport,
    CollectorInfo,
    CollectorTransport,
    _callback_identity_status_values,
    apply_collector_cloud_family_observation,
    asyncio,
    collector_cloud_family_observation_from_collector,
    logger,
    reconcile_pn,
    select_preferred_collector_cloud_family,
)


class LinkSessionProjectionMixin:
    """Methods owned by LinkSessionProjectionMixin."""

    def set_collector_connection_watcher(self, callback: Callable[[str], None] | None) -> None:
        """Notify ``callback(remote_ip)`` when this entry's collector dials in.

        Survives link rebuilds; used to trigger an immediate refresh instead
        of waiting out the poll backoff after the collector reconnects.
        """

        self._collector_connection_watcher = callback
        self._apply_collector_connection_watcher()

    def _apply_collector_connection_watcher(self) -> None:
        for transport in (
            self._transport,
            *self._auxiliary_transports.values(),
        ):
            set_watcher = getattr(transport, "set_connection_watcher", None)
            if callable(set_watcher):
                set_watcher(self._collector_connection_watcher)

    def _current_owned_session_fingerprint(self) -> tuple[str, int]:
        """Return the owned socket identity used to invalidate stale work."""

        session = self._owned_domain_session()
        if session is None:
            return ("", 0)
        return (
            str(getattr(session, "session_id", "") or ""),
            int(getattr(session, "listener_port", 0) or 0),
        )

    def _current_trusted_binding_observation_fingerprint(
        self,
    ) -> tuple[str, str, str]:
        """Return positive live-wire evidence for explicit binding adoption.

        The socket can move from ``waiting_for_route_identity`` to
        ``routed_framed``/``routed_at_text`` without changing session id or
        listener port.  Only the latter state is trusted wire evidence, so this
        fingerprint deliberately stays empty until the live SessionHandle is
        observed and non-conflicting.
        """

        handle = self._live_session_handle()
        if not handle.observed or handle.conflict:
            return ("", "", "")
        return (
            str(handle.session_id or "").strip(),
            str(handle.wire_framing or "").strip(),
            str(handle.collector_pn or "").strip(),
        )

    def _reconcile_owned_session_binding_observation(self) -> None:
        """Adopt a binding when the owned socket gains trusted wire evidence."""

        fingerprint = self._current_trusted_binding_observation_fingerprint()
        if fingerprint == getattr(
            self,
            "_owned_binding_observation_fingerprint",
            ("", "", ""),
        ):
            return
        self._owned_binding_observation_fingerprint = fingerprint
        self._adopt_trusted_live_binding()

    @property
    def owned_session_generation(self) -> int:
        """Return the generation of the currently owned inbound socket."""

        return self._owned_session_generation

    async def async_wait_for_owned_session_change(self, generation: int) -> None:
        """Wait until registry ownership moves to another live socket.

        This is used to cancel inverter detection that was started against a
        socket which has since disconnected or been replaced on another shared
        listener.  The background monitor is the event source; this method does
        not inspect listener internals.
        """

        while self._owned_session_generation == int(generation):
            self._owned_session_changed.clear()
            if self._owned_session_generation != int(generation):
                return
            await self._owned_session_changed.wait()

    def _start_owned_session_monitor(self) -> None:
        if self._owned_session_monitor_task is not None:
            return
        if not self._domain_ownership_active():
            return
        self._owned_session_fingerprint = self._current_owned_session_fingerprint()
        self._owned_session_monitor_task = asyncio.create_task(
            self._async_owned_session_monitor(),
            name=f"eybond_owned_session_{self._callback_entry_id}",
        )

    async def _stop_owned_session_monitor(self) -> None:
        task = self._owned_session_monitor_task
        self._owned_session_monitor_task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _async_owned_session_monitor(self) -> None:
        """Observe registry-owned socket replacement across shared listeners."""

        while True:
            await asyncio.sleep(0.2)
            # Routing/negotiation can complete on the same socket, so it is not
            # sufficient to react only to session_id/port replacement.
            self._reconcile_owned_session_binding_observation()
            fingerprint = self._current_owned_session_fingerprint()
            if fingerprint == self._owned_session_fingerprint:
                continue
            self._owned_session_fingerprint = fingerprint
            self._owned_session_generation += 1
            # A newly-observed owned socket is an explicit session event: adopt
            # its trusted wire as the confirmed binding here (not on any read).
            self._adopt_trusted_live_binding()
            self._owned_session_changed.set()
            if fingerprint[0] and self._collector_connection_watcher is not None:
                session = self._owned_domain_session()
                try:
                    self._collector_connection_watcher(
                        str(getattr(session, "peer_ip", "") or "")
                    )
                except Exception:
                    logger.debug(
                        "Owned-session connection watcher failed",
                        exc_info=True,
                    )

    def clear_discovery_reply(self) -> None:
        """Drop the remembered UDP discovery reply.

        ``collector_info`` rebuilds its snapshot from the announcer on every
        call, so stale-reply cleanup must clear the announcer source — not a
        returned copy.
        """

        self._announcer.last_reply = ""
        self._announcer.last_reply_from = ""

    @property
    def active_transport(self) -> CollectorTransport | None:
        """Return the connected payload transport selected for the active collector."""

        if self._inverter_forward_adapter() == ADAPTER_INVERTER_RAW_PASSTHROUGH:
            return None
        return self._connected_payload_transport()

    @property
    def active_collector_at_transport(self) -> CollectorAtTransport | None:
        """Return the connected AT transport selected for the active collector."""

        return self._connected_at_transport()

    @property
    def transport(self) -> CollectorTransport:
        """Return the active payload-capable transport."""

        adapter = self._inverter_forward_adapter()
        if adapter == ADAPTER_NONE:
            return self._unavailable_payload_transport
        if adapter == ADAPTER_INVERTER_RAW_PASSTHROUGH:
            return self.active_collector_at_transport or self._at_transport
        return self.active_transport or self._transport

    @property
    def collector_at_transport(self) -> CollectorAtTransport:
        """Return the collector AT transport sharing the same listener port."""

        return self.active_collector_at_transport or self._at_transport

    @property
    def connected(self) -> bool:
        """Return whether the physical link is currently connected (socket-level).

        Connectivity is independent of whether the payload WIRE is known yet:
        payload forwarding is separately fail-closed via the inverter adapter
        (``self.transport`` is the unavailable transport until the wire is
        observed/confirmed). So a connected-but-unobserved socket reports
        connected here but does NOT forward payloads -- reads go through the
        fail-closed ``transport`` and wait for observed/confirmed evidence.

        A contradictory live wire observation (``conflict``) is the one hard
        fail-closed state: it reports NOT connected so the runtime never treats a
        self-contradicting session as usable.
        """

        if self._live_session_handle().conflict:
            return False
        return (
            self.active_transport is not None
            or self.active_collector_at_transport is not None
        )

    @property
    def collector_info(self) -> CollectorInfo:
        """Return collector metadata merged with the latest UDP discovery reply."""

        _, ambiguous = self._selected_connected_remote_ip()
        if ambiguous:
            collector = CollectorInfo()
            at_collector = CollectorInfo()
        else:
            collector_transport = self.active_transport
            at_transport = self.active_collector_at_transport
            collector = collector_transport.collector_info if collector_transport is not None else self._transport.collector_info
            at_collector = at_transport.collector_info if at_transport is not None else self._at_transport.collector_info
        if not collector.remote_ip and at_collector.remote_ip:
            collector.remote_ip = at_collector.remote_ip
            collector.remote_port = at_collector.remote_port
        if at_collector.connection_count > collector.connection_count:
            collector.remote_port = at_collector.remote_port
            collector.connection_count = at_collector.connection_count
            collector.connection_replace_count = at_collector.connection_replace_count
            collector.disconnect_count = at_collector.disconnect_count
            collector.last_disconnect_reason = at_collector.last_disconnect_reason
            collector.pending_request_drop_count = at_collector.pending_request_drop_count
        # For at_text collectors all raw inverter traffic lives on the AT
        # connection; without this merge support bundles report zero raw
        # requests even while probes are actively timing out on the wire.
        if (
            at_collector.raw_request_count > collector.raw_request_count
            or at_collector.raw_unhandled_line_count > collector.raw_unhandled_line_count
        ):
            collector.raw_request_count = at_collector.raw_request_count
            collector.raw_response_count = at_collector.raw_response_count
            collector.raw_timeout_count = at_collector.raw_timeout_count
            collector.raw_unhandled_line_count = at_collector.raw_unhandled_line_count
            collector.raw_last_request_ascii = at_collector.raw_last_request_ascii
            collector.raw_last_request_hex = at_collector.raw_last_request_hex
            collector.raw_last_response_ascii = at_collector.raw_last_response_ascii
            collector.raw_last_response_hex = at_collector.raw_last_response_hex
            collector.raw_last_timeout_request_ascii = (
                at_collector.raw_last_timeout_request_ascii
            )
            collector.raw_last_parser = at_collector.raw_last_parser
            collector.raw_last_frame_format = at_collector.raw_last_frame_format
            collector.raw_last_spacing_wait_ms = at_collector.raw_last_spacing_wait_ms
            collector.raw_last_response_duration_ms = (
                at_collector.raw_last_response_duration_ms
            )
            collector.raw_last_total_duration_ms = (
                at_collector.raw_last_total_duration_ms
            )
        merged_pn = reconcile_pn(
            collector.collector_pn,
            at_collector.collector_pn,
        )
        if merged_pn and merged_pn != collector.collector_pn:
            collector.collector_pn = merged_pn
            collector.collector_pn_prefix = merged_pn[:1]
            collector.collector_pn_digits = merged_pn[1:]
        apply_collector_cloud_family_observation(
            collector,
            select_preferred_collector_cloud_family(
                collector_cloud_family_observation_from_collector(collector),
                collector_cloud_family_observation_from_collector(at_collector),
            ),
        )
        if not collector.smartess_collector_version and at_collector.smartess_collector_version:
            collector.smartess_collector_version = at_collector.smartess_collector_version
        collector.last_udp_reply = self._announcer.last_reply
        collector.last_udp_reply_from = self._announcer.last_reply_from
        collector.discovery_restart_count = self._discovery_restart_count
        collector.last_discovery_reason = self._last_discovery_reason
        return collector

    @property
    def effective_server_ip(self) -> str:
        """Return the current collector-facing IP used for discovery and advertising."""

        return self._effective_server_ip

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the advertised callback IP used by UDP bootstrap probes."""

        return self._configured_advertised_server_ip or self._effective_server_ip

    @property
    def effective_advertised_tcp_port(self) -> int:
        """Return the advertised callback TCP port used by UDP bootstrap probes."""

        return self._configured_advertised_tcp_port or self._tcp_port

    @property
    def listener_bind_host(self) -> str:
        """Return the local TCP bind host used by collector callback listeners."""

        return self._listener_bind_host

    @property
    def listener_status(self) -> str:
        """Return the listener lifecycle status for diagnostics."""

        return self._listener_status

    @property
    def listener_last_error(self) -> str:
        """Return the latest listener start error for diagnostics."""

        return self._listener_last_error

    def _current_live_session_state(self) -> str:
        """Return the current real session state for diagnostics (pure read).

        ``SessionHandle`` always describes the CURRENT socket; this collapses it
        to a coarse, honest label separate from the confirmed wire binding.
        """

        handle = self._live_session_handle()
        if handle.conflict:
            return "conflict"
        if handle.observed:
            return "active"
        if str(getattr(handle, "session_id", "") or "").strip():
            return "pending"
        return "absent"

    def listener_diagnostics(self) -> dict[str, object]:
        """Return listener bind and advertised endpoint diagnostics."""

        # Report the CURRENT live session and the CONFIRMED wire binding as two
        # separate facts. The effective wire/adapters describe how the runtime
        # routes RIGHT NOW: the live session when observed, otherwise the
        # confirmed binding (so a mid-handover support bundle shows framed_fc4,
        # not a momentary "unknown"). ``adapter_conflict`` reflects the CURRENT
        # live conflict only (fail-closed signal), never the binding.
        live_handle = self._live_session_handle()
        binding = self._effective_wire_binding()
        live_effective = live_handle.observed and not live_handle.conflict
        if live_effective:
            eff_wire = live_handle.wire_framing
            eff_sources = live_handle.identity_sources
            eff_forward = live_handle.inverter_forward_adapter
            eff_proxy = live_handle.proxy_adapter
        elif binding is not None:
            eff_wire = binding.wire_framing
            eff_sources = binding.identity_sources
            eff_forward = binding.inverter_forward_adapter
            eff_proxy = binding.proxy_adapter
        else:
            eff_wire = live_handle.wire_framing
            eff_sources = live_handle.identity_sources
            eff_forward = live_handle.inverter_forward_adapter
            eff_proxy = live_handle.proxy_adapter
        # Collector management is resolved by its OWN single resolver (conflict ->
        # none/"conflict"), NOT the shared wire/forward selection above.
        _mgmt_adapter_id, _mgmt_provenance = self._collector_management_selection()
        current_live_session = self._current_live_session_state()
        diagnostics: dict[str, object] = {
            "collector_listener_status": self._listener_status,
            "collector_listener_bind_host": self._listener_bind_host,
            "collector_listener_bind_endpoint": f"{self._listener_bind_host}:{self._tcp_port}",
            "collector_listener_effective_host": self._effective_server_ip,
            "collector_listener_advertised_endpoint": (
                f"{self.effective_advertised_server_ip}:{self.effective_advertised_tcp_port}"
            ),
            "collector_listener_rebind_count": self._listener_rebind_count,
            "collector_listener_last_error": self._listener_last_error,
            "collector_callback_observed_session_protocol": (
                self._owned_observed_session_protocol()
            ),
            # Configured, confirmed and live remain separate. There is no
            # cloud-derived preliminary/expected protocol tier.
            "collector_configured_session_protocol": (
                self._configured_collector_session_protocol
            ),
            "collector_confirmed_session_protocol": (
                binding.session_protocol if binding is not None else ""
            ),
            "collector_live_session_protocol": (
                ""
                if live_handle.conflict
                else (
                    "eybond_framed"
                    if live_handle.uses_framed_wire
                    else ("at_text" if live_handle.uses_at_text_wire else "")
                )
            ),
            # Current real session vs confirmed binding, reported separately.
            "collector_current_live_session": current_live_session,
            "collector_confirmed_wire_binding": (
                binding.wire_framing if binding is not None else "none"
            ),
            "collector_callback_wire_framing": eff_wire,
            "collector_callback_identity_sources": ", ".join(sorted(eff_sources)),
            # Collector-management adapter + provenance come from the ONE resolver,
            # so a live conflict reports (none, "conflict") -- the stale confirmed
            # binding is never shown as the effective management adapter, and the
            # id/provenance can never disagree.
            "collector_callback_collector_management_adapter": _mgmt_adapter_id,
            "collector_management_adapter_id": _mgmt_adapter_id,
            "collector_management_adapter_provenance": _mgmt_provenance,
            "collector_callback_inverter_forward_adapter": eff_forward,
            "collector_callback_proxy_adapter": eff_proxy,
            "collector_callback_adapter_conflict": live_handle.conflict,
            "collector_callback_identity_strategy": self._collector_identity_strategy,
            "collector_callback_raw_passthrough_bootstrap": (
                self._collector_raw_passthrough_bootstrap
            ),
            "collector_callback_raw_passthrough_frame_format": (
                self._collector_raw_passthrough_frame_format
            ),
            "collector_callback_raw_passthrough_min_interval_ms": (
                self._collector_raw_passthrough_min_interval_ms
            ),
        }
        diagnostics.update(self._session_ownership_diagnostics())
        diagnostics.update(self._session_inventory_diagnostics())
        diagnostics.update(self.callback_trigger_diagnostics())
        return diagnostics

    def _session_ownership_diagnostics(self) -> dict[str, object]:
        """Return domain transport-ownership diagnostics for the support bundle.

        Makes the end-to-end ownership chain auditable: which entry claim the
        domain registry resolved, the exact claimed session id, the listener
        port the collector actually dialed, the primary configured port, and the
        listener port of the transport currently carrying the connection.
        """

        domain_active = self._domain_ownership_active()
        session = self._owned_domain_session() if domain_active else None
        active_port = 0
        if getattr(self, "_transport", None) is not None and self._transport.connected:
            active_port = self._tcp_port
        else:
            for port in sorted(getattr(self, "_auxiliary_listener_ports", ()) or ()):
                transport = self._auxiliary_transports.get(port)
                if transport is not None and transport.connected:
                    active_port = port
                    break
            else:
                for port, transport in sorted(
                    (getattr(self, "_auxiliary_at_transports", {}) or {}).items()
                ):
                    if transport is not None and transport.connected:
                        active_port = port
                        break
        ownership_state = "no_domain_registry"
        if domain_active:
            if session is not None:
                ownership_state = str(getattr(session, "state", "") or "observed")
            else:
                ownership_state = "no_owned_session"
        return {
            "collector_session_ownership_authority": (
                "domain_registry" if domain_active else "runtime_registry"
            ),
            "collector_session_claim_entry_id": (
                self._callback_entry_id if domain_active else ""
            ),
            "collector_claimed_session_id": (
                str(getattr(session, "session_id", "") or "") if session else ""
            ),
            "collector_claimed_listener_port": (
                int(getattr(session, "listener_port", 0) or 0) if session else 0
            ),
            "collector_primary_tcp_port": self._tcp_port,
            "collector_active_listener_port": active_port,
            "collector_session_ownership_state": ownership_state,
        }

    def _owned_observed_session_protocol(self) -> str:
        """Return the effective observed session protocol for this entry (pure read).

        A trusted current live session reports its own protocol. During a
        transient gap (or a live conflict) the CONFIRMED wire binding is
        reported, so the coordinator never sees "" and never lets cloud-family
        bootstrap flip the profile to at_text mid-handover. Empty only before any
        live wire has ever been confirmed.
        """

        handle = self._live_session_handle()
        if not handle.conflict:
            if handle.uses_framed_wire:
                return "eybond_framed"
            if handle.uses_at_text_wire:
                return "at_text"
        binding = self._effective_wire_binding()
        if binding is not None:
            return binding.session_protocol
        return ""

    def _session_inventory_diagnostics(self) -> dict[str, object]:
        """Return passive callback-session inventory diagnostics."""

        summaries: list[dict[str, object]] = []
        seen_listeners: set[str] = set()
        for transport in self._payload_transports():
            listener_key = str(getattr(transport, "listener_key", "") or "")
            dedup_key = listener_key or f"transport:{id(transport)}"
            if dedup_key in seen_listeners:
                continue
            seen_listeners.add(dedup_key)
            diagnostics = transport.session_inventory_diagnostics()
            summaries.append(diagnostics)

        pending_count = sum(int(item.get("pending_session_count", 0) or 0) for item in summaries)
        recent_count = sum(int(item.get("recent_session_count", 0) or 0) for item in summaries)
        duplicate_peer_ips: set[str] = set()
        sessions: list[dict[str, object]] = []
        for item in summaries:
            for peer_ip in item.get("duplicate_peer_ips", []) or []:
                if isinstance(peer_ip, str) and peer_ip:
                    duplicate_peer_ips.add(peer_ip)
            for session in item.get("sessions", []) or []:
                if isinstance(session, dict):
                    sessions.append(dict(session))

        duplicate_peer_ip_count = len(duplicate_peer_ips)
        result: dict[str, object] = {
            "collector_callback_pending_session_count": pending_count,
            "collector_callback_recent_session_count": recent_count,
            "collector_callback_duplicate_peer_ip_count": duplicate_peer_ip_count,
            "collector_callback_duplicate_peer_ips": ", ".join(sorted(duplicate_peer_ips)),
            "collector_callback_session_inventory": sessions,
        }
        # A conflict is reported only on POSITIVE evidence (a
        # ``route_identity_mismatch`` state in the inventory). ``reconnecting`` is
        # reported only during a GENUINE handover -- a confirmed binding plus an
        # owned pending/new socket the registry can see (a fully offline
        # collector is idle, not endlessly reconnecting). A foreign identified
        # session on a shared listener is unresolved/unowned, never a conflict.
        binding = self._effective_wire_binding()
        live = self._live_session_handle()
        result.update(
            _callback_identity_status_values(
                pending_count=pending_count,
                recent_count=recent_count,
                duplicate_peer_ip_count=duplicate_peer_ip_count,
                sessions=sessions,
                expects_collector_identity=bool(str(self._collector_pn or "").strip()),
                owned_session_observed=bool(binding is not None or live.observed),
                handover_in_progress=self._handover_in_progress(),
            )
        )
        return result
