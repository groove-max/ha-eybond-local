"""HubRefreshMixin ownership slice for the runtime hub."""

from __future__ import annotations

from .hub_common import (
    Any,
    DriverReadMode,
    RuntimeSnapshot,
    TypedTelemetryFrame,
    _INVERTER_BINDING_REFRESH_MAX_ATTEMPTS,
    _error_code,
    _inverter_identity_signature,
    _is_retryable_collector_error,
    _should_force_reconnect,
    _should_mark_snapshot_disconnected,
    asyncio,
    clear_unsupported_commands,
    coerce_driver_read_result,
    fold_driver_telemetry,
    logger,
    seed_unsupported_commands,
)


class HubRefreshMixin:
    """Methods owned by HubRefreshMixin."""

    async def async_refresh(self, *, poll_interval: float | None = None) -> RuntimeSnapshot:
        """Refresh the current runtime snapshot."""

        if not self._link_manager.connected:
            self._reset_runtime_read_state()
            ok = await self._async_try_connect_for_session_lifecycle(
                timeout=0.75,
            )
            if not ok:
                collector_values = await self._async_read_collector_runtime_values(
                    poll_interval=poll_interval,
                    force_liveness=True,
                )
                if (
                    self._driver is None
                    and self._inverter is None
                    and self._collector_runtime_read_fresh
                ):
                    self._collector_outage_caches_cleared = False
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error="inverter_heartbeat_missing",
                        connected=True,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
                self._clear_collector_value_caches_for_outage()
                self._reset_volatile_collector_link_fields()
                snapshot = self._build_snapshot(
                    extra_values=self._combined_collector_runtime_values(),
                    last_error="waiting_for_collector",
                    connected=False,
                )
                self._last_snapshot = snapshot
                return snapshot

        ok = await self._async_try_connect_for_session_lifecycle(
            timeout=1.5,
            require_heartbeat=True,
        )
        if not ok:
            self._reset_runtime_read_state()
            if self._link_manager.connected:
                if self._driver is None and self._inverter is None:
                    collector_values = await self._async_read_collector_runtime_values(
                        poll_interval=poll_interval,
                        force_liveness=True,
                    )
                    if self._collector_runtime_read_fresh:
                        self._collector_outage_caches_cleared = False
                        snapshot = self._build_snapshot(
                            extra_values=collector_values,
                            last_error="inverter_heartbeat_missing",
                            connected=True,
                        )
                        self._last_snapshot = snapshot
                        return snapshot

                logger.warning(
                    "Collector heartbeat timed out; resetting stale runtime connection"
                )
                try:
                    await self._async_recover_heartbeat_timeout(timeout=5.0)
                    ok = True
                except Exception as exc:
                    logger.warning("Collector heartbeat recovery failed: %s", exc)
                    self._record_recovery_failure(reason="collector_heartbeat_timeout")
                    self._clear_collector_value_caches_for_outage()
                    collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error="collector_heartbeat_timeout",
                        connected=False,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                self._clear_collector_value_caches_for_outage()
                collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
                self._reset_volatile_collector_link_fields()
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error="waiting_for_collector",
                    connected=False,
                )
                self._last_snapshot = snapshot
                return snapshot

        if not ok:
            self._clear_collector_value_caches_for_outage()
            collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
            snapshot = self._build_snapshot(
                extra_values=collector_values,
                last_error="collector_heartbeat_timeout",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        # Sub-phase timing for the bound path: the coordinator-level breakdown
        # repeatedly pointed at "runtime_refresh" as one opaque number.
        refresh_phase_started = asyncio.get_running_loop().time()
        refresh_phases: dict[str, int] = {}

        def _mark_refresh_phase(phase: str) -> None:
            nonlocal refresh_phase_started
            now_monotonic = asyncio.get_running_loop().time()
            refresh_phases[phase] = refresh_phases.get(phase, 0) + int(
                round((now_monotonic - refresh_phase_started) * 1000.0)
            )
            refresh_phase_started = now_monotonic

        collector_values = await self._async_read_collector_runtime_values(poll_interval=poll_interval)
        _mark_refresh_phase("collector_metadata")
        detect_error = ""
        if self._driver is None or self._inverter is None:
            self._publish_intermediate_snapshot(
                collector_values,
                status="detecting_inverter",
            )
            _mark_refresh_phase("intermediate_snapshot")

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            _mark_refresh_phase("driver_detection")
            if detect_error == "collector_session_changed":
                # The collector replaced its socket (possibly on another shared
                # listener) while the driver sweep was running.  Bind the new
                # registry-owned session immediately and restart detection once;
                # never publish the old sweep's offline/result state.
                self._reset_runtime_read_state()
                reconnected = await self._link_manager.async_try_connect(
                    timeout=1.5,
                    require_heartbeat=True,
                )
                _mark_refresh_phase("session_handover")
                if reconnected:
                    collector_values = await self._async_read_collector_runtime_values(
                        poll_interval=poll_interval,
                        force_liveness=True,
                    )
                    self._publish_intermediate_snapshot(
                        collector_values,
                        status="detecting_inverter",
                    )
                    detect_error = await self._async_detect_driver()
                    _mark_refresh_phase("driver_detection_after_handover")
                else:
                    detect_error = "waiting_for_collector"
            if self._driver is None or self._inverter is None:
                logger.warning("Driver detection failed: %s", detect_error)
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error=detect_error,
                    connected=self._link_manager.connected,
                )
                self._last_snapshot = snapshot
                return snapshot
        elif self._inverter_binding_needs_live_detection_refresh:
            # A startup-persisted (provisional) binding refreshes itself against
            # live detection. Bound the attempts so a permanently-silent inverter
            # cannot re-run detection on every poll: on success/conflict
            # _async_detect_driver clears the flag; on transient failure we stop
            # after a few tries and keep the provisional binding.
            self._inverter_binding_refresh_attempts += 1
            detect_error = await self._async_detect_driver()
            _mark_refresh_phase("driver_identity_refresh")
            if detect_error:
                if (
                    self._inverter_binding_refresh_attempts
                    >= _INVERTER_BINDING_REFRESH_MAX_ATTEMPTS
                ):
                    self._inverter_binding_needs_live_detection_refresh = False
                logger.debug(
                    "Deferred inverter identity refresh attempt %d failed: %s; keeping persisted binding",
                    self._inverter_binding_refresh_attempts,
                    detect_error,
                )

        remaining_backoff = self._recovery_backoff_remaining()
        if remaining_backoff > 0:
            logger.warning(
                "Runtime refresh backoff active after %s; skipping refresh for %.1fs",
                self._last_recovery_reason or "runtime_error",
                remaining_backoff,
            )
            snapshot = self._build_snapshot(
                extra_values=collector_values,
                last_error=self._last_recovery_reason or self._last_snapshot.last_error or "request_timeout",
                connected=False,
            )
            self._last_snapshot = snapshot
            return snapshot

        async def _async_read_driver_values() -> dict[str, object]:
            loop = asyncio.get_running_loop()
            started = loop.time()
            raw = await self._driver.async_read_values(
                self._link_manager.transport,
                self._inverter,
                runtime_state=self._runtime_read_state,
                poll_interval=poll_interval,
                now_monotonic=loop.time() if poll_interval is not None else None,
            )
            duration = max(0.0, loop.time() - started)
            # Typed contract: a bare dict means FULL; a DriverReadResult carries
            # its own FULL/DELTA mode. The measurement VALUES are folded into the
            # hub's last-good cache and applied in _build_snapshot; only driver
            # diagnostics + the poll duration flow through here.
            result = coerce_driver_read_result(
                raw, driver_key=getattr(self._inverter, "driver_key", "")
            )
            self._resolve_runtime_measurements(result)
            runtime_values: dict[str, object] = self._runtime_measurement_diagnostics()
            runtime_values["collector_poll_duration_ms"] = int(round(duration * 1000.0))
            return runtime_values

        try:
            runtime_values = await _async_read_driver_values()
            _mark_refresh_phase("driver_read")
        except Exception as exc:
            if _error_code(exc) == "request_timeout":
                # This timeout belongs to one inverter/UART payload request.
                # The collector TCP link and heartbeat may still be perfectly
                # healthy; tearing that link down turns one missed inverter
                # reply into a full callback/re-detection outage. Retry once on
                # the SAME transport, then keep last-known-good values if the
                # inverter remains silent for this cycle.
                logger.warning(
                    "Inverter payload request timed out; retrying without collector reconnect"
                )
                try:
                    runtime_values = await _async_read_driver_values()
                    _mark_refresh_phase("driver_read_retry_same_session")
                except Exception as retry_exc:
                    if _is_retryable_collector_error(retry_exc):
                        # The retry produced positive transport-failure
                        # evidence. Only now may connection recovery run.
                        logger.warning(
                            "Collector transport failed during payload retry: %s; reconnecting",
                            retry_exc,
                        )
                        try:
                            self._record_recovery_attempt(reason=_error_code(retry_exc))
                            await self._async_ensure_connected(
                                timeout=5.0,
                                require_heartbeat=True,
                            )
                            self._reset_runtime_read_state()
                            runtime_values = await _async_read_driver_values()
                        except Exception as reconnect_exc:
                            logger.warning(
                                "Runtime refresh failed after collector reconnect: %s",
                                reconnect_exc,
                            )
                            self._reset_runtime_read_state()
                            self._record_recovery_failure(
                                reason=_error_code(reconnect_exc)
                            )
                            snapshot = self._build_snapshot(
                                extra_values=collector_values,
                                last_error=str(reconnect_exc),
                                connected=(
                                    False
                                    if _should_mark_snapshot_disconnected(
                                        reconnect_exc
                                    )
                                    else None
                                ),
                            )
                            self._last_snapshot = snapshot
                            return snapshot
                        # Recovery succeeded; continue the normal snapshot path.
                    else:
                        logger.warning(
                            "Inverter payload retry failed without collector link failure: %s",
                            retry_exc,
                        )
                        retained_values = dict(
                            getattr(self._last_snapshot, "values", {}) or {}
                        )
                        retained_values.update(collector_values)
                        retained_values["runtime_payload_error"] = _error_code(
                            retry_exc
                        )
                        snapshot = self._build_snapshot(
                            extra_values=retained_values,
                            last_error=_error_code(retry_exc),
                            connected=self._link_manager.connected,
                        )
                        self._last_snapshot = snapshot
                        return snapshot
            elif _is_retryable_collector_error(exc):
                logger.warning("Runtime refresh failed: %s; retrying after collector reconnect", exc)
                try:
                    self._record_recovery_attempt(reason=_error_code(exc))
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    self._reset_runtime_read_state()
                    runtime_values = await _async_read_driver_values()
                except Exception as retry_exc:
                    logger.warning("Runtime refresh failed after retry: %s", retry_exc)
                    self._reset_runtime_read_state()
                    self._record_recovery_failure(reason=_error_code(retry_exc))
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            elif _should_force_reconnect(exc):
                logger.warning(
                    "Runtime refresh failed: %s; forcing collector reconnect and retry",
                    exc,
                )
                try:
                    self._record_recovery_attempt(reason=_error_code(exc))
                    await self._link_manager.async_reset_connection(reason=str(exc))
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    self._reset_runtime_read_state()
                    runtime_values = await _async_read_driver_values()
                except Exception as retry_exc:
                    logger.warning("Runtime refresh failed after forced reconnect: %s", retry_exc)
                    self._reset_runtime_read_state()
                    self._record_recovery_failure(reason=_error_code(retry_exc))
                    snapshot = self._build_snapshot(
                        extra_values=collector_values,
                        last_error=str(retry_exc),
                        connected=False if _should_mark_snapshot_disconnected(retry_exc) else None,
                    )
                    self._last_snapshot = snapshot
                    return snapshot
            else:
                logger.warning("Runtime refresh failed: %s", exc)
                self._reset_runtime_read_state()
                snapshot = self._build_snapshot(
                    extra_values=collector_values,
                    last_error=str(exc),
                    connected=False if _should_mark_snapshot_disconnected(exc) else None,
                )
                self._last_snapshot = snapshot
                return snapshot

        self._record_refresh_success()
        merged_values = {**collector_values, **runtime_values}
        snapshot = self._build_snapshot(
            extra_values=merged_values,
            last_error=detect_error or None,
        )
        self._mark_owned_session_stable()
        _mark_refresh_phase("snapshot_build")
        metadata_result = self._last_collector_metadata_result
        if metadata_result is not None:
            refresh_phases["collector_metadata_fc"] = metadata_result.framed_duration_ms
            refresh_phases["collector_metadata_at"] = metadata_result.at_duration_ms
        snapshot.values["runtime_refresh_phase_breakdown"] = ", ".join(
            f"{phase}={elapsed_ms}ms"
            for phase, elapsed_ms in sorted(
                refresh_phases.items(), key=lambda item: -item[1]
            )
        )
        self._last_snapshot = snapshot
        return snapshot

    async def async_activate_claimed_session(
        self,
        *,
        expected_session_id: str,
        timeout: float,
    ) -> bool:
        """Activate an already-certified callback socket without sending UDP.

        This is the post-setup half of a recovery handoff.  The recovery
        transaction has already proved and pinned the exact physical session;
        activation may only consume that claim, never start a fresh callback
        attempt or substitute another same-PN socket.
        """

        return await self._link_manager.async_activate_claimed_session(
            expected_session_id=expected_session_id,
            timeout=timeout,
        )

    async def _async_read_collector_runtime_values(
        self,
        *,
        poll_interval: float | None,
        force_liveness: bool = False,
    ) -> dict[str, object]:
        """Refresh collector-side metadata via the metadata service (thin delegate).

        The hub knows nothing about FC parameter numbers, AT command names,
        transport methods, channel selection, bootstrap encoding, or channel
        cadence/cache/dead-channel internals: it reads the negotiated metadata
        routes from the link (route authority) and hands them to the service,
        then consumes one normalized result. Sets ``_collector_runtime_read_fresh``
        when at least one channel returned live data this call.
        """

        routes = self._link_manager.collector_metadata_routes()
        result = await self._collector_metadata_service.async_refresh(
            routes,
            poll_interval=poll_interval,
            force_liveness=force_liveness,
        )
        self._last_collector_metadata_result = result
        self._collector_runtime_read_fresh = result.fresh
        return result.merged_values

    def _clear_collector_runtime_value_caches(self) -> None:
        self._collector_metadata_service.invalidate()

    def _reset_runtime_read_state(self) -> None:
        """Clear per-session read state, re-seeding the persisted facts.

        The unsupported-command set is an empirical device fact persisted in
        the config entry; a reconnect must not forget it and start burning
        timeouts on known-dead commands again.
        """

        self._runtime_read_state.clear()
        if self._persistent_unsupported_commands:
            seed_unsupported_commands(
                self._runtime_read_state,
                self._persistent_unsupported_commands,
            )

    def _record_inverter_detection_probe_log(
        self,
        entries: object,
        *,
        budget_exhausted: bool,
        generation: int,
    ) -> None:
        """Store one non-sensitive projection of the latest driver sweep."""

        allowed_outcomes = {
            "matched",
            "no_match",
            "probe_timeout",
            "inverter_link_down",
            "skipped_budget_exhausted",
        }
        sanitized: list[dict[str, object]] = []
        if isinstance(entries, (tuple, list)):
            for entry in entries:
                if type(entry) is not dict:
                    continue
                driver = entry.get("driver")
                if type(driver) is not str or not driver or driver != driver.strip():
                    driver = "unknown"
                elapsed_ms = entry.get("elapsed_ms")
                if type(elapsed_ms) is not int or elapsed_ms < 0:
                    elapsed_ms = 0
                outcome = entry.get("outcome")
                if type(outcome) is not str:
                    outcome = "unknown"
                elif outcome.startswith("error:"):
                    # Exception text can contain route details; diagnostics need
                    # the class of outcome, never the raw exception string.
                    outcome = "error"
                elif outcome not in allowed_outcomes:
                    outcome = "unknown"
                sanitized.append(
                    {
                        "driver": driver,
                        "elapsed_ms": elapsed_ms,
                        "outcome": outcome,
                        "saw_response": entry.get("saw_response") is True,
                    }
                )
        self._inverter_detection_probe_log = tuple(sanitized)
        self._inverter_detection_probe_budget_exhausted = (
            type(budget_exhausted) is bool and budget_exhausted
        )
        self._inverter_detection_probe_generation = generation

    def _reset_runtime_measurement_cache(self) -> None:
        """Drop last-good runtime measurements (a different device/driver)."""

        self._runtime_measurement_values = {}
        self._runtime_measurement_telemetry = TypedTelemetryFrame.empty()
        self._runtime_measurement_owned_keys = set()
        self._runtime_driver_diagnostics = {}
        self._runtime_driver_diagnostic_owned_keys = set()
        self._runtime_measurement_last_mode = ""
        self._runtime_measurement_fresh_count = 0
        self._runtime_measurement_reused_count = 0

    def _accept_inverter_binding_identity(self) -> None:
        """The single binding/cache lifecycle boundary.

        Invoked the MOMENT a new driver/inverter binding is accepted -- never
        lazily on the first successful read -- so a bind whose first read fails
        (or a snapshot built before any read) can never surface the previous
        device's measurements. If the durable identity (``driver|model|serial``)
        actually changed, the previous identity's owned keys are marked stale so
        the next snapshot also purges them from the carried ``_last_snapshot``,
        and the measurement cache is cleared. A reconnect or learned-overlay
        refresh of the SAME identity is a no-op, so last-good values survive.
        """

        token = _inverter_identity_signature(self._inverter)
        if token == self._runtime_measurement_identity:
            return
        self._stale_runtime_owned_keys |= self._runtime_measurement_owned_keys
        self._stale_runtime_driver_diagnostic_keys |= (
            self._runtime_driver_diagnostic_owned_keys
        )
        self._reset_runtime_measurement_cache()
        self._runtime_measurement_identity = token

    def _resolve_runtime_measurements(
        self, result: "DriverReadResult"
    ) -> dict[str, Any]:
        """Fold one typed driver read result into the last-good measurement cache.

        FULL replaces the cache (missing keys are dropped); DELTA overlays the
        given values and removes ``removed_keys`` while retaining every other
        last-good value. ``details`` is never consulted here. Identity lifecycle
        is owned by :meth:`_accept_inverter_binding_identity` (the bind boundary),
        never re-derived here.
        """

        fresh_keys = set(result.values)
        self._runtime_measurement_telemetry = fold_driver_telemetry(
            self._runtime_measurement_telemetry,
            driver_key=getattr(self._inverter, "driver_key", ""),
            values=result.values,
            replace=result.mode is DriverReadMode.FULL,
            removed_keys=result.removed_keys,
        )
        if result.mode is DriverReadMode.FULL:
            self._runtime_measurement_values = dict(result.values)
            reused = 0
        else:
            self._runtime_measurement_values.update(result.values)
            for key in result.removed_keys:
                self._runtime_measurement_values.pop(key, None)
            reused = len(set(self._runtime_measurement_values) - fresh_keys)
        self._runtime_measurement_owned_keys.update(fresh_keys)
        self._runtime_measurement_owned_keys.update(result.removed_keys)
        self._runtime_driver_diagnostics = dict(result.diagnostics)
        self._runtime_driver_diagnostic_owned_keys.update(result.diagnostics)
        self._runtime_measurement_last_mode = result.mode.value
        self._runtime_measurement_fresh_count = len(fresh_keys)
        self._runtime_measurement_reused_count = reused
        return dict(self._runtime_measurement_values)

    def _runtime_measurement_diagnostics(self) -> dict[str, object]:
        """Return neutral fresh/reused/mode diagnostics for the runtime snapshot."""

        return {
            "runtime_read_mode": self._runtime_measurement_last_mode,
            "runtime_measurement_fresh_count": self._runtime_measurement_fresh_count,
            "runtime_measurement_reused_count": self._runtime_measurement_reused_count,
            "runtime_measurement_value_count": len(self._runtime_measurement_values),
            "runtime_measurement_owned_key_count": len(
                self._runtime_measurement_owned_keys
            ),
            "runtime_typed_telemetry_count": len(
                self._runtime_measurement_telemetry.points
            ),
            "runtime_typed_telemetry_fresh_count": (
                self._runtime_measurement_telemetry.fresh_count
            ),
            "runtime_typed_telemetry_carried_count": (
                self._runtime_measurement_telemetry.carried_count
            ),
            "runtime_untyped_driver_value_count": max(
                0,
                len(self._runtime_measurement_values)
                - len(self._runtime_measurement_telemetry.points),
            ),
            "runtime_driver_diagnostic_count": len(
                self._runtime_driver_diagnostics
            ),
        }

    def set_persistent_unsupported_commands(self, commands: tuple[str, ...]) -> None:
        """Install the persisted unsupported-command set for this device.

        Any ``collector:``-namespaced metadata channel key is filtered out and
        never seeded into the DRIVER negative cache: metadata channel health is
        the metadata service's own state, persisted separately. This is
        belt-and-suspenders for a config entry not yet migrated -- the coordinator
        splits and migrates the persisted set, but a stray metadata key here is
        still kept out of the driver table.
        """

        self._persistent_unsupported_commands = tuple(
            command
            for command in (str(command or "").strip() for command in commands)
            if command and not command.startswith("collector:")
        )
        seed_unsupported_commands(
            self._runtime_read_state,
            self._persistent_unsupported_commands,
        )

    def set_persistent_metadata_dead_channels(self, channels: tuple[str, ...]) -> None:
        """Install the persisted metadata dead-channel set for this device."""

        self._collector_metadata_service.seed_dead_channels(
            tuple(
                channel
                for channel in (str(channel or "").strip() for channel in channels)
                if channel
            )
        )

    def collector_metadata_dead_channels(self) -> tuple[str, ...]:
        """Return the metadata dead-channel set for config-entry persistence."""

        return self._collector_metadata_service.dead_channels()

    def clear_unsupported_command_cache(self) -> None:
        """Forget both negative caches so the next cycles re-probe everything.

        The "Re-check supported commands" action revives inverter commands AND
        metadata channels; they are cleared through their SEPARATE stores.
        """

        self._persistent_unsupported_commands = ()
        clear_unsupported_commands(self._runtime_read_state)
        self._collector_metadata_service.clear_channel_health()

    def invalidate_collector_runtime_values(self) -> None:
        """Drop cached collector-side values so the next refresh reads them live."""

        self._clear_collector_runtime_value_caches()

    def _reset_volatile_collector_link_fields(self) -> None:
        """Drop link-scoped collector fields that must not survive an offline gap."""

        clear_reply = getattr(self._link_manager, "clear_discovery_reply", None)
        if callable(clear_reply):
            # The real link manager rebuilds collector_info from the announcer
            # on every access: the source must be cleared, not a snapshot.
            clear_reply()
        collector = self._link_manager.collector_info
        collector.last_udp_reply = ""
        collector.last_udp_reply_from = ""

    def _clear_collector_value_caches_for_outage(self) -> None:
        """Force one fresh collector read at the start of an outage.

        Consecutive failed cycles must not re-run the full (slow) AT metadata
        sweep every time: that inflates the failed-cycle duration, which the
        poll scheduler then mirrors into an equally long retry backoff.
        """

        if self._collector_outage_caches_cleared:
            return
        self._collector_outage_caches_cleared = True
        self._clear_collector_runtime_value_caches()

    def _combined_collector_runtime_values(self) -> dict[str, object]:
        return self._collector_metadata_service.merged_values()

    @property
    def _collector_runtime_values(self) -> dict[str, object]:
        return self._collector_metadata_service.framed_values

    @_collector_runtime_values.setter
    def _collector_runtime_values(self, value: dict[str, object]) -> None:
        self._collector_metadata_service.framed_values = value

    @property
    def _collector_at_runtime_values(self) -> dict[str, object]:
        return self._collector_metadata_service.at_values

    @_collector_at_runtime_values.setter
    def _collector_at_runtime_values(self, value: dict[str, object]) -> None:
        self._collector_metadata_service.at_values = value

    @property
    def _collector_runtime_values_dirty(self) -> bool:
        return self._collector_metadata_service.dirty

    @_collector_runtime_values_dirty.setter
    def _collector_runtime_values_dirty(self, value: bool) -> None:
        self._collector_metadata_service.dirty = value

    @property
    def _collector_runtime_last_refresh_monotonic(self) -> float:
        return self._collector_metadata_service.framed_last_refresh_monotonic

    @_collector_runtime_last_refresh_monotonic.setter
    def _collector_runtime_last_refresh_monotonic(self, value: float) -> None:
        self._collector_metadata_service.framed_last_refresh_monotonic = value

    @property
    def _collector_at_runtime_last_attempt_monotonic(self) -> float:
        return self._collector_metadata_service.at_last_attempt_monotonic

    @_collector_at_runtime_last_attempt_monotonic.setter
    def _collector_at_runtime_last_attempt_monotonic(self, value: float) -> None:
        self._collector_metadata_service.at_last_attempt_monotonic = value

    def _publish_intermediate_snapshot(
        self,
        collector_values: dict[str, object],
        *,
        status: str,
    ) -> None:
        """Publish known collector state before a potentially slow inverter probe."""

        if not str(status or "").strip():
            return
        if self._snapshot_observer is None:
            return
        snapshot = self._build_snapshot(
            extra_values={**collector_values, "runtime_detection_status": status},
        )
        self._last_snapshot = snapshot
        try:
            self._snapshot_observer(snapshot)
        except Exception:
            logger.debug("Runtime intermediate snapshot observer failed", exc_info=True)
