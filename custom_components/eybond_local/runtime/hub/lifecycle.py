"""HubLifecycleMixin ownership slice for the runtime hub."""

from __future__ import annotations

from .common import (
    Any,
    Callable,
    DetectedInverter,
    InverterDriver,
    RuntimeInverterCandidate,
    RuntimeSnapshot,
    ShadowWriteObservation,
    _PROVISIONAL_INVERTER_DETECTION_STATUSES,
    _SESSION_HANDOVER_CONNECT_TIMEOUT,
    _SESSION_HANDOVER_MAX_GENERATIONS,
    asyncio,
)


class HubLifecycleMixin:
    """Methods owned by HubLifecycleMixin."""

    @property
    def detected_inverter(self) -> DetectedInverter | None:
        """Return the currently bound detected inverter, if any.

        Exposed so the coordinator can hand the detected model to a driver's
        ``poll_policy_for`` once identity is known (a catalog driver may pick a
        model-specific policy). ``None`` before detection.
        """

        return self._inverter

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        """Return the rollback endpoint remembered during the active runtime session."""

        if self._collector_last_server_endpoint_before_change:
            return self._collector_last_server_endpoint_before_change
        return ""

    @property
    def effective_server_ip(self) -> str:
        """Return the collector-facing local host selected by the link manager."""

        return self._link_manager.effective_server_ip

    @property
    def effective_advertised_server_ip(self) -> str:
        """Return the callback host advertised to the collector."""

        return self._link_manager.effective_advertised_server_ip

    @property
    def listener_bind_host(self) -> str:
        """Return the ACTUAL local TCP bind host of the callback listener.

        A narrow read-only pass-through of the link's own public
        ``listener_bind_host`` -- so a cold repair can borrow the shared TCP
        listener on the exact host the runtime binds, never a guessed one.
        """

        return self._link_manager.listener_bind_host

    def diagnostic_link_transport(self):
        """Return the shared payload transport for read-only diagnostic command runs.

        Exposes the active collector link so the diagnostic command runner can
        reuse the existing connection instead of opening its own socket. Returns
        ``None`` when no link manager/transport is available.
        """

        link_manager = getattr(self, "_link_manager", None)
        if link_manager is None:
            return None
        return getattr(link_manager, "transport", None)

    async def async_start(self) -> None:
        """Start the underlying runtime link and discovery loop."""

        await self._link_manager.async_start()

    async def async_stop(self) -> None:
        """Stop discovery and the active runtime link."""

        self._snapshot_observer = None
        self._inverter_overlay_applier = None
        self._inverter_detection_observer = None
        self.set_collector_connection_watcher(None)
        await self._link_manager.async_stop()

    @property
    def inverter_protocol_candidates(self) -> tuple[RuntimeInverterCandidate, ...]:
        """Return ambiguity candidates observed on the current owned session."""

        if (
            self._inverter_protocol_candidates
            and self._inverter_protocol_candidate_generation
            != self._owned_session_generation()
        ):
            return ()
        return self._inverter_protocol_candidates

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        """Re-resolve listener network state after HA/network readiness changes."""

        return await self._link_manager.async_reconcile_network(reason=reason)

    async def async_reconcile_collector_session_profile(
        self,
        *,
        collector_session_protocol: str,
        collector_identity_strategy: str,
        collector_raw_passthrough_bootstrap: str = "",
        collector_raw_passthrough_frame_format: str = "",
        collector_raw_passthrough_min_interval_ms: int = 0,
        reason: str = "collector_session_profile_change",
    ) -> bool:
        """Rebuild link transports after a runtime-learned collector profile change."""

        return await self._link_manager.async_reconcile_collector_session_profile(
            collector_session_protocol=collector_session_protocol,
            collector_identity_strategy=collector_identity_strategy,
            collector_raw_passthrough_bootstrap=collector_raw_passthrough_bootstrap,
            collector_raw_passthrough_frame_format=collector_raw_passthrough_frame_format,
            collector_raw_passthrough_min_interval_ms=(
                collector_raw_passthrough_min_interval_ms
            ),
            reason=reason,
        )

    def listener_diagnostics(self) -> dict[str, object]:
        """Return active collector listener/session diagnostics."""

        diagnostics = getattr(self._link_manager, "listener_diagnostics", None)
        if callable(diagnostics):
            return dict(diagnostics())
        return {}

    def has_confirmed_wire_binding(self) -> bool:
        """Return whether the link has ever confirmed a live wire binding.

        Delegated so the coordinator's per-poll session-profile reconcile can be
        bootstrap-only: once a live wire is confirmed, the live session is the
        transport authority and the cloud-family/persisted profile must not drive
        a steady-state destructive rebuild.
        """

        probe = getattr(self._link_manager, "has_confirmed_wire_binding", None)
        if callable(probe):
            try:
                return bool(probe())
            except Exception:  # pragma: no cover - defensive
                return False
        return False

    def confirmed_session_protocol_evidence(self) -> tuple[str, str]:
        """Return ``(protocol, durable_pn)`` of the confirmed live wire, else ("","").

        Sourced ONLY from a trusted live SessionHandle (the link's confirmed wire
        binding). The coordinator persists this as durable ``live_session``
        provenance so a same-PN restart can bootstrap it. Never an inferred hint.
        """

        binding = getattr(self._link_manager, "confirmed_wire_binding", None)
        if binding is None:
            return "", ""
        protocol = str(getattr(binding, "session_protocol", "") or "").strip().lower()
        pn = str(getattr(binding, "collector_pn", "") or "").strip()
        if not protocol or not pn:
            return "", ""
        return protocol, pn

    def _owned_session_generation(self) -> int:
        """Return the registry-owned socket generation, if the link exposes it."""

        return int(getattr(self._link_manager, "owned_session_generation", 0) or 0)

    def _session_handover_active(self) -> bool:
        """Return whether registry lifecycle evidence shows a replacement."""

        return bool(
            self.has_confirmed_wire_binding()
            and self._owned_session_generation()
            != self._stable_owned_session_generation
        )

    async def _async_try_connect_for_session_lifecycle(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        """Connect once normally, or follow a bounded same-PN handover chain.

        Some collectors replace one long-lived socket with a short-lived first
        replacement and immediately dial again. Each registry-observed session
        generation gets the normal reconnect budget, capped to a small number
        of generations. A confirmed binding without a generation change is a
        normal offline collector and receives no repeated grace windows.
        """

        handover = self._session_handover_active()
        attempts = _SESSION_HANDOVER_MAX_GENERATIONS if handover else 1
        for attempt in range(attempts):
            generation = self._owned_session_generation()
            attempt_timeout = (
                max(float(timeout), _SESSION_HANDOVER_CONNECT_TIMEOUT)
                if handover
                else float(timeout)
            )
            if await self._link_manager.async_try_connect(
                timeout=attempt_timeout,
                require_heartbeat=require_heartbeat,
            ):
                return True

            # A handover may have started while the ordinary connect attempt
            # was already in flight. Promote into lifecycle recovery only on
            # positive generation evidence, never merely because a binding
            # exists.
            handover = self._session_handover_active()
            if not handover or attempt + 1 >= _SESSION_HANDOVER_MAX_GENERATIONS:
                return False

            if self._owned_session_generation() == generation:
                wait_for_change = getattr(
                    self._link_manager,
                    "async_wait_for_owned_session_change",
                    None,
                )
                if not callable(wait_for_change):
                    return False
                try:
                    await asyncio.wait_for(
                        wait_for_change(generation),
                        timeout=_SESSION_HANDOVER_CONNECT_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    return False
        return False

    async def async_ensure_collector_management_session(
        self,
        *,
        timeout: float,
    ) -> bool:
        """Ensure a live collector session without running inverter detection.

        This is the public runtime boundary for a management transaction which
        needs the entry's normal connection strategy first.  In callback mode
        it uses the SAME callback trigger, causality lease and exact ownership
        path as an ordinary runtime reconnect; in inbound mode it only waits for
        the already-configured autonomous session.  It never probes an inverter
        and never manufactures recovery evidence.
        """

        connected = await self._async_try_connect_for_session_lifecycle(
            timeout=timeout,
            require_heartbeat=False,
        )
        return connected is True

    def _mark_owned_session_stable(self) -> None:
        """Record the owned session generation that completed a driver poll."""

        self._stable_owned_session_generation = self._owned_session_generation()

    def set_initial_inverter_binding(
        self,
        driver: InverterDriver,
        inverter: DetectedInverter,
    ) -> None:
        """Seed runtime polling from persisted high-confidence inverter metadata."""

        if self._driver is not None or self._inverter is not None:
            return
        inverter.details.pop("probe_log", None)
        self._driver = driver
        self._inverter = inverter
        self._inverter_protocol_candidates = ()
        self._inverter_protocol_candidate_generation = -1
        self._accept_inverter_binding_identity()
        details = getattr(inverter, "details", {}) or {}
        self._inverter_binding_needs_live_detection_refresh = (
            str(details.get("runtime_detection_status") or "").strip()
            in _PROVISIONAL_INVERTER_DETECTION_STATUSES
        )
        self._reset_runtime_read_state()
        self._write_blockers.clear()

    def set_inverter_overlay_applier(
        self, applier: Callable[[DetectedInverter, Any], DetectedInverter] | None
    ) -> None:
        """Install a hook that post-processes the detected inverter.

        The coordinator uses this to merge activated device-scoped learned controls into
        the detected inverter (whose capabilities otherwise reflect only built-in
        detection), so the learned controls become entities and are writable.
        """

        self._inverter_overlay_applier = applier

    def set_inverter_detection_observer(
        self,
        observer: Callable[[InverterDriver, DetectedInverter], None] | None,
    ) -> None:
        """Install a best-effort observer for newly detected inverter identity.

        Runtime detection may succeed before the first runtime value read succeeds.
        The coordinator uses this hook to persist the confirmed identity before a
        later read timeout/reload can collapse the entry back to collector-only.
        """

        self._inverter_detection_observer = observer

    def set_runtime_snapshot_observer(
        self,
        observer: Callable[[RuntimeSnapshot], None] | None,
    ) -> None:
        """Install a best-effort observer for intermediate runtime snapshots."""

        self._snapshot_observer = observer

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        """Pass reverse-discovery policy changes through to the runtime link layer."""

        self._link_manager.set_reverse_discovery_enabled(enabled)

    def set_callback_ownership(self, registry: object, entry_id: str) -> None:
        """Pass the domain callback-session registry + entry id to the link layer.

        The link uses this as its production ownership authority for live
        session location, exact socket claim, negotiated wire, and
        claimed-by-other callback diagnostics.
        """

        self._collector_operation_entry_id = (
            entry_id
            if type(entry_id) is str
            and bool(entry_id)
            and entry_id == entry_id.strip()
            else ""
        )
        set_ownership = getattr(self._link_manager, "set_callback_ownership", None)
        if callable(set_ownership):
            set_ownership(registry, entry_id)

    def set_collector_connection_watcher(self, callback: Callable[[str], None] | None) -> None:
        """Notify ``callback(remote_ip)`` when this entry's collector dials in."""

        set_watcher = getattr(self._link_manager, "set_collector_connection_watcher", None)
        if callable(set_watcher):
            set_watcher(callback)

    async def async_ensure_callback_listener(self, port: int) -> None:
        """Ensure one auxiliary callback listener is available for collector redirects."""

        await self._link_manager.async_ensure_callback_listener(port)

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        """Send one explicit UDP bootstrap redirect through the runtime link layer."""

        return await self._link_manager.async_trigger_reverse_discovery(
            port=port,
            timeout=timeout,
        )

    async def async_start_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        proxy_wire_mode: str = "transparent",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        masked_endpoint: str = "",
        restore_trigger_path=None,
        async_open_output=None,
        async_close_output=None,
    ) -> None:
        """Start one in-process proxy capture route on the active runtime link."""

        route_kwargs = {
            "collector_ip": collector_ip,
            "collector_pn": collector_pn,
            "expected_session_protocol": expected_session_protocol,
            "proxy_wire_mode": proxy_wire_mode,
            "listen_port": listen_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "output_path": output_path,
            "masked_endpoint": masked_endpoint,
            "restore_trigger_path": restore_trigger_path,
        }
        if async_open_output is not None:
            route_kwargs["async_open_output"] = async_open_output
        if async_close_output is not None:
            route_kwargs["async_close_output"] = async_close_output
        if owner_id:
            route_kwargs["owner_id"] = owner_id
        if entry_id:
            route_kwargs["entry_id"] = entry_id
        await self._link_manager.async_start_proxy_capture_route(
            **route_kwargs,
        )

    async def async_stop_proxy_capture_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process proxy capture route."""

        if owner_id or force:
            await self._link_manager.async_stop_proxy_capture_route(
                owner_id=owner_id,
                force=force,
            )
        else:
            await self._link_manager.async_stop_proxy_capture_route()

    def proxy_capture_route_running(self) -> bool:
        """Return whether the runtime link currently owns one proxy route."""

        return self._link_manager.proxy_capture_route_running()

    async def async_start_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        entry_id: str = "",
        collector_ip: str,
        collector_pn: str = "",
        expected_session_protocol: str = "",
        listen_port: int,
        upstream_host: str,
        upstream_port: int,
        output_path,
        seed,
    ) -> None:
        """Start one in-process shadow-learning route on the active runtime link."""

        route_kwargs = {
            "collector_ip": collector_ip,
            "collector_pn": collector_pn,
            "expected_session_protocol": expected_session_protocol,
            "listen_port": listen_port,
            "upstream_host": upstream_host,
            "upstream_port": upstream_port,
            "output_path": output_path,
            "seed": seed,
        }
        if owner_id:
            route_kwargs["owner_id"] = owner_id
        if entry_id:
            route_kwargs["entry_id"] = entry_id
        await self._link_manager.async_start_shadow_learning_route(**route_kwargs)

    async def async_stop_shadow_learning_route(
        self,
        *,
        owner_id: str = "",
        force: bool = False,
    ) -> None:
        """Stop the active in-process shadow-learning route."""

        if owner_id or force:
            await self._link_manager.async_stop_shadow_learning_route(
                owner_id=owner_id,
                force=force,
            )
        else:
            await self._link_manager.async_stop_shadow_learning_route()

    def shadow_learning_route_running(self) -> bool:
        """Return whether the runtime link currently owns one shadow route."""

        return self._link_manager.shadow_learning_route_running()

    def shadow_learning_route_ready(self) -> bool:
        """Return whether the active shadow route is ready for cloud control learning."""

        return self._link_manager.shadow_learning_route_ready()

    def shadow_learning_route_status(self) -> dict[str, object]:
        """Return detailed status for the active shadow route."""

        return self._link_manager.shadow_learning_route_status()

    def shadow_learning_write_observations(
        self,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return observations through the runtime-manager public contract."""

        return self._link_manager.shadow_learning_write_observations()

    def shadow_learning_observation_cursor(self) -> int:
        """Return the active route's observation tail cursor."""

        return self._link_manager.shadow_learning_observation_cursor()

    def shadow_learning_observations_since(
        self,
        cursor: int,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Return active-route observations at or after one cursor."""

        return self._link_manager.shadow_learning_observations_since(cursor)

    async def async_wait_for_shadow_learning_observations_since(
        self,
        cursor: int,
        *,
        timeout_seconds: float,
    ) -> tuple[ShadowWriteObservation, ...]:
        """Wait for observations through the runtime-manager contract."""

        return await self._link_manager.async_wait_for_shadow_learning_observations_since(
            cursor,
            timeout_seconds=timeout_seconds,
        )

    def shadow_learning_read_map_snapshot(self) -> dict[str, object]:
        """Return a detached read-map snapshot through the runtime contract."""

        return self._link_manager.shadow_learning_read_map_snapshot()

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        """Drop active collector sockets without changing collector settings."""

        await self._link_manager.async_disconnect_collector_connections(reason=reason)
