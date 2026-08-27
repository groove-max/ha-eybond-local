"""HubManagementMixin ownership slice for the runtime hub."""

from __future__ import annotations

from .common import (
    ADAPTER_COLLECTOR_AT_COMMANDS,
    CollectorEndpointWriteResult,
    CollectorManagementCapabilities,
    CollectorManagementError,
    CollectorSystemActionResult,
    _is_retryable_collector_error,
    _normalize_collector_server_endpoint,
    _should_confirm_write,
    _wall_time,
    _write_not_confirmed_error,
    _write_readback_matches,
    asyncio,
    logger,
    monotonic,
    select_collector_management_adapter,
)


class HubManagementMixin:
    """Methods owned by HubManagementMixin."""

    async def async_write_capability(
        self,
        capability_key: str,
        value: object,
    ) -> object:
        """Write one validated capability through the active driver."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                raise RuntimeError(detect_error or "no_supported_driver_matched")

        snapshot = await self.async_refresh()
        capability = self._inverter.get_capability(capability_key)
        runtime_values = snapshot.runtime_values()
        runtime_state = capability.runtime_state(runtime_values)
        if not runtime_state.editable:
            reasons = "; ".join(runtime_state.reasons) or "capability_not_editable"
            raise ValueError(f"capability_not_editable:{capability_key}:{reasons}")

        written_value: object | None = None
        confirmation_started_at = 0.0
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                confirmation_started_at = monotonic()
                written_value = await self._driver.async_write_capability(
                    self._link_manager.transport,
                    self._inverter,
                    capability_key,
                    value,
                    runtime_state=self._runtime_read_state,
                )
                self._write_blockers.pop(capability_key, None)
                break
            except Exception as exc:
                last_error = exc
                if attempt == 0 and _is_retryable_collector_error(exc):
                    logger.warning(
                        "Write %s failed: %s; retrying once after collector reconnect",
                        capability_key,
                        exc,
                    )
                    await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
                    continue
                classification = self._driver.classify_write_error(
                    capability,
                    exc,
                    operating_mode=runtime_values.get("operating_mode"),
                )
                if classification.user_error is not None:
                    raise classification.user_error from exc
                if classification.blocker is not None:
                    logger.warning(
                        "Blocking capability %s after write failure: %s (%s)",
                        capability_key,
                        classification.blocker.reason,
                        classification.blocker.code,
                    )
                    self._write_blockers[capability_key] = classification.blocker
                raise

        if written_value is None:
            raise last_error or RuntimeError(f"write_failed:{capability_key}")

        snapshot = await self.async_refresh()
        if snapshot.last_error in {"collector_disconnected", "collector_not_connected", "waiting_for_collector"}:
            logger.warning(
                "Refresh after write reported: %s; retrying once after collector reconnect",
                snapshot.last_error,
            )
            await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)
            snapshot = await self.async_refresh()
        if snapshot.last_error:
            logger.warning("Refresh after write reported: %s", snapshot.last_error)

        if _should_confirm_write(capability):
            # Some inverters acknowledge a setting before their configuration
            # block exposes it.  One old full read therefore is not terminal
            # evidence.  Allow exactly one more independent full refresh, but
            # never resend the write: a second old read remains an explicit
            # failure.  The refresh itself supplies the bounded settling window,
            # so this rule needs no model-specific sleep or timeout.
            for confirmation_attempt in range(2):
                readback_value = snapshot.runtime_value(capability.value_key)
                if _write_readback_matches(
                    capability,
                    requested_value=value,
                    written_value=written_value,
                    readback_value=readback_value,
                    confirmation_elapsed_seconds=max(
                        0.0,
                        monotonic() - confirmation_started_at,
                    ),
                ):
                    break
                if confirmation_attempt == 0:
                    logger.debug(
                        "Write %s was not visible in the first full readback; "
                        "refreshing once more without resending the write",
                        capability_key,
                    )
                    snapshot = await self.async_refresh()
                    continue
                logger.warning(
                    "Write %s was accepted but did not confirm by readback; expected=%r readback=%r refresh_error=%s",
                    capability_key,
                    written_value,
                    readback_value,
                    snapshot.last_error or "",
                )
                raise _write_not_confirmed_error(
                    capability,
                    written_value=written_value,
                    readback_value=readback_value,
                    refresh_error=snapshot.last_error,
                )
        return written_value

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        """Apply one declarative preset through sequential capability writes."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        if self._driver is None or self._inverter is None:
            detect_error = await self._async_detect_driver()
            if self._driver is None or self._inverter is None:
                raise RuntimeError(detect_error or "no_supported_driver_matched")

        snapshot = await self.async_refresh()
        preset = self._inverter.get_capability_preset(preset_key)
        runtime_state = preset.runtime_state(
            self._inverter,
            snapshot.runtime_values(),
        )
        if not runtime_state.visible:
            reasons = "; ".join(runtime_state.reasons) or "preset_not_visible"
            raise ValueError(f"preset_not_visible:{preset_key}:{reasons}")
        if not runtime_state.applicable:
            reasons = "; ".join(runtime_state.reasons or runtime_state.warnings) or "preset_not_applicable"
            raise ValueError(f"preset_not_applicable:{preset_key}:{reasons}")

        results: list[dict[str, object]] = []
        for item in sorted(preset.items, key=lambda item: (item.order, item.capability_key)):
            capability = self._inverter.get_capability(item.capability_key)
            current_value = snapshot.runtime_value(capability.value_key)
            target_label = capability.enum_value_map.get(item.value, item.value)
            if current_value == item.value or current_value == target_label:
                results.append(
                    {
                        "key": capability.key,
                        "status": "unchanged",
                        "current_value": current_value,
                        "target_value": target_label,
                    }
                )
                continue

            written_value = await self.async_write_capability(capability.key, item.value)
            snapshot = self._last_snapshot
            results.append(
                {
                    "key": capability.key,
                    "status": "written",
                    "current_value": current_value,
                    "target_value": target_label,
                    "written_value": written_value,
                }
            )

        return {
            "preset_key": preset.key,
            "title": preset.title,
            "results": results,
            "warnings": list(runtime_state.warnings),
        }

    def _collector_management_adapter(self, *, active_only: bool = False):
        """Build the negotiated collector-management adapter (single switch: link).

        The wire is chosen ONCE, in ``link.collector_management_adapter_id``
        (live trusted SessionHandle > confirmed binding > conflict/unknown ->
        none). This hub never guesses framed/AT: it just hands both transport
        providers to the factory, which resolves the live transport lazily so a
        reconnect/handover never leaves the adapter holding a stale socket.
        """

        return select_collector_management_adapter(
            self._link_manager.collector_management_adapter_id(),
            framed_transport_provider=lambda: (
                getattr(self._link_manager, "active_transport", None)
                if active_only
                else (
                    getattr(self._link_manager, "active_transport", None)
                    or self._link_manager.transport
                )
            ),
            at_transport_provider=lambda: (
                getattr(self._link_manager, "active_collector_at_transport", None)
                if active_only
                else (
                    getattr(self._link_manager, "active_collector_at_transport", None)
                    or getattr(self._link_manager, "collector_at_transport", None)
                )
            ),
        )

    def collector_management_capabilities(self) -> CollectorManagementCapabilities:
        """Return the CURRENT management capabilities (recomputed each call).

        Because the adapter is re-selected from the negotiated live wire on every
        call, capabilities reflect a live handover/adoption immediately without a
        config-entry reload.
        """

        return self._collector_management_adapter().capabilities

    def collector_management_diagnostics(self) -> dict[str, object]:
        """Return non-sensitive collector-management diagnostics.

        Never includes endpoint values, Wi-Fi credentials, or other secrets --
        only the selected adapter, its capabilities, and the last operation's
        status/error-class/duration/timestamp.
        """

        caps = self.collector_management_capabilities()
        provenance_getter = getattr(
            self._link_manager, "collector_management_adapter_provenance", None
        )
        diagnostics: dict[str, object] = {
            "collector_management_adapter_id": (
                self._link_manager.collector_management_adapter_id()
            ),
            "collector_management_adapter_provenance": (
                provenance_getter() if callable(provenance_getter) else ""
            ),
            "collector_management_capabilities": {
                "read_endpoint_state": caps.read_endpoint_state,
                "write_endpoint": caps.write_endpoint,
                "apply_changes": caps.apply_changes,
                "reboot": caps.reboot,
            },
        }
        if self._last_management_operation is not None:
            diagnostics["collector_management_last_operation"] = dict(
                self._last_management_operation
            )
        return diagnostics

    def collector_metadata_diagnostics(self) -> dict[str, object]:
        """Return non-sensitive collector-metadata TELEMETRY diagnostics.

        Delegates to the metadata service (routes / provenance / generation /
        per-channel outcome+duration / cache age+dirty / dead channels). Never
        includes endpoint values, Wi-Fi credentials, or raw AT payloads.
        """

        routes = None
        routes_getter = getattr(self._link_manager, "collector_metadata_routes", None)
        if callable(routes_getter):
            try:
                routes = routes_getter()
            except Exception:  # pragma: no cover - defensive during diagnostics
                routes = None
        return self._collector_metadata_service.diagnostics(routes)

    def _apply_collector_metadata_diagnostics(self, values: dict[str, object]) -> None:
        """Flatten metadata diagnostics into snapshot values for the support bundle.

        Safe, structured flat fields only -- counts / ages / typed error codes /
        per-channel failure counts / partial flags -- never endpoint values,
        credentials, raw AT payloads, or peer IP.
        """

        try:
            diagnostics = self.collector_metadata_diagnostics()
        except Exception:  # pragma: no cover - defensive during snapshot build
            return
        routes = [r for r in (diagnostics.get("routes") or []) if isinstance(r, dict)]
        channel_ids = [str(r.get("channel_id", "")) for r in routes if r.get("channel_id")]
        values["collector_metadata_route_channels"] = ", ".join(channel_ids)
        values["collector_metadata_route_provenance"] = str(
            diagnostics.get("route_provenance", "")
        )
        values["collector_metadata_session_generation"] = diagnostics.get(
            "session_generation", 0
        )
        values["collector_metadata_identity_known"] = bool(
            diagnostics.get("identity_known", False)
        )
        values["collector_metadata_identity_transitions"] = diagnostics.get(
            "identity_transitions", 0
        )

        def _join(pairs: list[str]) -> str:
            return ", ".join(pairs)

        statuses = [f"{r['channel_id']}={r.get('status', '')}" for r in routes if r.get("channel_id")]
        if statuses:
            values["collector_metadata_channel_status"] = _join(statuses)
        durations = [
            f"{r['channel_id']}={r.get('duration_ms', 0)}ms"
            for r in routes
            if r.get("channel_id") and r.get("duration_ms")
        ]
        if durations:
            values["collector_metadata_channel_duration_ms"] = _join(durations)
        errors = [
            f"{r['channel_id']}={r.get('error_code', '')}"
            for r in routes
            if r.get("channel_id") and r.get("error_code")
        ]
        if errors:
            values["collector_metadata_channel_errors"] = _join(errors)
        commands = [
            f"{r['channel_id']}={r.get('successful_commands', 0)}/{r.get('attempted_commands', 0)}"
            for r in routes
            if r.get("channel_id") and r.get("attempted_commands")
        ]
        if commands:
            values["collector_metadata_channel_commands"] = _join(commands)
        failures = [
            f"{r['channel_id']}={r.get('consecutive_failures', 0)}"
            for r in routes
            if r.get("channel_id") and r.get("consecutive_failures")
        ]
        if failures:
            values["collector_metadata_channel_failures"] = _join(failures)
        partial = [
            str(r["channel_id"]) for r in routes if r.get("channel_id") and r.get("partial")
        ]
        if partial:
            values["collector_metadata_partial_channels"] = _join(partial)
        effective_exclusions = [
            f"{r['channel_id']}="
            + "|".join(
                str(field)
                for field in (r.get("effective_excluded_semantic_fields") or [])
            )
            for r in routes
            if r.get("channel_id") and r.get("effective_excluded_semantic_fields")
        ]
        if effective_exclusions:
            values["collector_metadata_effective_exclusions"] = _join(
                effective_exclusions
            )
        unsupported_fields = [
            f"{r['channel_id']}="
            + "|".join(
                str(field) for field in (r.get("unsupported_semantic_fields") or [])
            )
            for r in routes
            if r.get("channel_id") and r.get("unsupported_semantic_fields")
        ]
        if unsupported_fields:
            values["collector_metadata_unsupported_fields"] = _join(
                unsupported_fields
            )

        semantic_ownership = diagnostics.get("semantic_ownership") or {}
        if isinstance(semantic_ownership, dict):
            binding_generation = semantic_ownership.get("binding_generation")
            if binding_generation is not None:
                values["collector_metadata_semantic_binding_generation"] = (
                    binding_generation
                )
            at_owned_fields = semantic_ownership.get("at_owned_fields") or []
            if at_owned_fields:
                values["collector_metadata_at_owned_fields"] = ", ".join(
                    str(field) for field in at_owned_fields
                )
            framed_unsupported_fields = (
                semantic_ownership.get("framed_unsupported_fields") or []
            )
            if framed_unsupported_fields:
                values["collector_metadata_framed_unsupported_fields"] = ", ".join(
                    str(field) for field in framed_unsupported_fields
                )

        refresh = diagnostics.get("refresh") or {}
        if isinstance(refresh, dict):
            values["collector_metadata_last_read_fresh"] = bool(
                refresh.get("last_read_fresh", False)
            )
        cache = diagnostics.get("cache") or {}
        if isinstance(cache, dict):
            values["collector_metadata_cache_dirty"] = bool(cache.get("dirty", False))
            values["collector_metadata_framed_cache_keys"] = cache.get("framed_cached_keys", 0)
            values["collector_metadata_at_cache_keys"] = cache.get("at_cached_keys", 0)
            framed_age = cache.get("framed_age_seconds")
            if framed_age is not None:
                values["collector_metadata_framed_age_seconds"] = framed_age
            at_age = cache.get("at_age_seconds")
            if at_age is not None:
                values["collector_metadata_at_age_seconds"] = at_age
        dead = [d for d in (diagnostics.get("dead_channels") or []) if isinstance(d, dict)]
        dead_ids = [str(d.get("channel_id", "")) for d in dead if d.get("channel_id")]
        if dead_ids:
            values["collector_metadata_dead_channels"] = ", ".join(dead_ids)
            values["collector_metadata_dead_channel_detail"] = ", ".join(
                f"{d['channel_id']}={d.get('consecutive_failures', 0)}/{d.get('threshold', 0)}"
                for d in dead
                if d.get("channel_id")
            )

    async def _run_management_operation(self, name: str, operation):
        """Execute one management operation, recording non-sensitive diagnostics.

        Records operation name, ok/error status, typed error class + short code,
        duration, and timestamp -- NEVER endpoint values or credentials -- so the
        per-action methods stay free of diagnostics bookkeeping.
        """

        started = asyncio.get_running_loop().time()
        record: dict[str, object] = {
            "operation": name,
            "status": "ok",
            "error_class": "",
            "error_code": "",
            "timestamp": _wall_time(),
        }
        try:
            return await operation()
        except CollectorManagementError as exc:
            record["status"] = "error"
            record["error_class"] = type(exc).__name__
            record["error_code"] = str(exc).split(":", 1)[0]
            raise
        except Exception as exc:  # noqa: BLE001 - recorded, then re-raised
            record["status"] = "error"
            record["error_class"] = type(exc).__name__
            raise
        finally:
            record["duration_ms"] = int(
                round((asyncio.get_running_loop().time() - started) * 1000.0)
            )
            self._last_management_operation = record

    def _collector_endpoint_write_result_to_dict(
        self, result: CollectorEndpointWriteResult
    ) -> dict[str, object]:
        """Map the normalized write result to the runtime/coordinator dict shape.

        ``status`` is HONEST: ``applied`` only when a requested apply was
        confirmed (``apply_performed``), otherwise ``staged`` (write done, no
        apply requested). A requested-but-unconfirmed apply never reaches here --
        the adapter raises. ``readback_endpoint`` is the real read (may be "").
        """

        out: dict[str, object] = {
            "status": "applied" if result.apply_performed else "staged",
            "requested_endpoint": result.requested_endpoint,
            "readback_endpoint": result.readback_endpoint,
            "apply_changes": result.apply_requested,
            "write_confirmed": result.write_confirmed,
            "apply_performed": result.apply_performed,
            "confirmation_source": result.confirmation_source,
        }
        if result.previous_endpoint:
            out["previous_endpoint"] = result.previous_endpoint
        if result.reboot_or_apply_required:
            out["reboot_required"] = result.reboot_or_apply_required
        if result.adapter_id == ADAPTER_COLLECTOR_AT_COMMANDS:
            out["management_protocol"] = "at_text"
        out.update(dict(result.extra or {}))
        if result.warnings:
            out["warning"] = result.warnings[0]
        return out

    async def async_set_collector_server_endpoint(
        self,
        endpoint: str,
        *,
        apply_changes: bool = True,
        timeout: float = 5.0,
        require_heartbeat: bool = True,
    ) -> dict[str, object]:
        """Stage or apply the collector's upstream endpoint via the management adapter."""

        await self._async_ensure_connected(
            timeout=max(0.0, float(timeout)),
            require_heartbeat=bool(require_heartbeat),
        )

        normalized_endpoint = _normalize_collector_server_endpoint(endpoint)
        adapter = self._collector_management_adapter()
        result = await self._run_management_operation(
            "write_endpoint",
            lambda: adapter.async_write_endpoint(
                normalized_endpoint, apply_changes=apply_changes
            ),
        )

        if result.previous_endpoint and result.previous_endpoint != normalized_endpoint:
            self._collector_last_server_endpoint_before_change = result.previous_endpoint

        # Overlay the EFFECTIVE endpoint (real readback if the collector echoed
        # it, else the requested value it just wrote) as authoritative action
        # state: the hub knows what it requested, so this is honest -- it never
        # fabricates the result's ``readback_endpoint``. The service marks the
        # framed cache fresh so the next cadence-gated sweep does not clobber it.
        overlay: dict[str, object] = {
            "collector_server_endpoint": result.readback_endpoint or result.requested_endpoint
        }
        if result.reboot_or_apply_required:
            overlay["collector_reboot_required"] = result.reboot_or_apply_required
        self._collector_metadata_service.apply_authoritative_values(overlay)
        return self._collector_endpoint_write_result_to_dict(result)

    async def async_apply_collector_changes(self) -> dict[str, object]:
        """Trigger collector apply on parameter 29 without changing parameter 21."""

        return await self._async_execute_collector_system_action(action="apply")

    async def async_reboot_collector(self) -> dict[str, object]:
        """Trigger collector reboot-intent on parameter 29."""

        return await self._async_execute_collector_system_action(action="reboot")

    async def async_rollback_collector_server_endpoint(
        self,
        *,
        apply_changes: bool = True,
    ) -> dict[str, object]:
        """Rollback parameter 21 to the cached endpoint remembered in this runtime session."""

        rollback_endpoint = self.collector_server_endpoint_rollback_target
        if not rollback_endpoint:
            raise RuntimeError("collector_rollback_endpoint_unavailable")

        result = await self.async_set_collector_server_endpoint(
            rollback_endpoint,
            apply_changes=apply_changes,
        )
        result["status"] = "rollback_applied" if apply_changes else "rollback_staged"
        result["rollback_source"] = "session_cached_previous_endpoint"
        result["rollback_endpoint"] = rollback_endpoint
        return result

    async def async_get_collector_server_endpoint_state(
        self,
        *,
        timeout: float = 5.0,
        require_heartbeat: bool = True,
    ) -> dict[str, object]:
        """Return the live collector endpoint and reboot-required flag from local management."""

        await self._async_ensure_connected(
            timeout=max(0.0, float(timeout)),
            require_heartbeat=bool(require_heartbeat),
        )

        adapter = self._collector_management_adapter()
        state = await self._run_management_operation(
            "read_endpoint_state", adapter.async_read_endpoint_state
        )

        overlay: dict[str, object] = {}
        if state.current_endpoint:
            overlay["collector_server_endpoint"] = state.current_endpoint
        if state.reboot_required:
            overlay["collector_reboot_required"] = state.reboot_required
        self._collector_metadata_service.apply_authoritative_values(overlay)
        return {
            "current_endpoint": state.current_endpoint,
            "reboot_required": state.reboot_required,
        }

    async def async_query_collector_parameters(
        self,
        parameters: tuple[int, ...],
    ) -> dict[int, str]:
        """Read collector parameters on the exact runtime-owned live session."""

        await self._async_ensure_connected(timeout=10.0, require_heartbeat=False)
        adapter = self._collector_management_adapter(active_only=True)
        return await adapter.async_query_parameters(parameters)

    async def async_set_collector_wifi_credentials(
        self,
        *,
        ssid: str,
        password: str,
        ssid_parameter: int,
        password_parameter: int,
    ) -> str:
        """Write Wi-Fi credentials through the exact runtime-owned session."""

        await self._async_ensure_connected(timeout=10.0, require_heartbeat=False)
        adapter = self._collector_management_adapter(active_only=True)
        return await adapter.async_set_wifi_credentials(
            ssid=ssid,
            password=password,
            ssid_parameter=ssid_parameter,
            password_parameter=password_parameter,
        )

    async def async_set_collector_uart_baudrate(self, baudrate: str) -> str:
        """Write UART speed through the exact runtime-owned live session."""

        await self._async_ensure_connected(timeout=10.0, require_heartbeat=False)
        adapter = self._collector_management_adapter(active_only=True)
        return await adapter.async_set_uart_baudrate(baudrate)

    async def _async_execute_collector_system_action(self, *, action: str) -> dict[str, object]:
        """Run a standalone collector apply/reboot via the management adapter."""

        await self._async_ensure_connected(timeout=5.0, require_heartbeat=True)

        adapter = self._collector_management_adapter()
        result: CollectorSystemActionResult = await self._run_management_operation(
            action,
            (
                adapter.async_apply_changes
                if action == "apply"
                else adapter.async_reboot
            ),
        )

        overlay: dict[str, object] = {"collector_reboot_required": "0"}
        if result.current_endpoint:
            overlay["collector_server_endpoint"] = result.current_endpoint
        self._collector_metadata_service.apply_authoritative_values(overlay)
        return {
            "status": "applied" if action == "apply" else "reboot_triggered",
            "action": action,
            "current_endpoint": result.current_endpoint,
            "reboot_required_before": result.reboot_required_before,
            "warning": (
                result.warnings[0]
                if result.warnings
                else "collector system action accepted; the current session may disconnect before the next refresh"
            ),
        }
