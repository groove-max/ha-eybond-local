"""HubSnapshotMixin ownership slice for the runtime hub."""

from __future__ import annotations

from .hub_common import (
    CONNECTION_TYPE_EYBOND,
    RUNTIME_COLLECTOR_STATE_IDENTIFIED,
    RUNTIME_COLLECTOR_STATE_UNKNOWN,
    RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE,
    RUNTIME_DRIVER_STATE_DRIVER_BOUND,
    RUNTIME_DRIVER_STATE_DRIVER_UNBOUND,
    RUNTIME_INVERTER_STATE_ABSENT,
    RUNTIME_INVERTER_STATE_AMBIGUOUS,
    RUNTIME_INVERTER_STATE_CONFLICT,
    RUNTIME_INVERTER_STATE_DETECTING,
    RUNTIME_INVERTER_STATE_LIVE_CONFIRMED,
    RUNTIME_INVERTER_STATE_PROVISIONAL,
    RUNTIME_POLL_STATE_DEGRADED,
    RUNTIME_POLL_STATE_DETECTING,
    RUNTIME_POLL_STATE_OFFLINE,
    RUNTIME_POLL_STATE_POLLING,
    RUNTIME_SESSION_STATE_OFFLINE,
    RUNTIME_SESSION_STATE_ONLINE,
    RuntimeSnapshot,
    _PROVISIONAL_INVERTER_DETECTION_STATUSES,
    _VOLATILE_COLLECTOR_VALUE_KEYS,
    _callback_owner_label,
    _collector_signal_quality,
    _collector_signal_source_label,
    _inverter_identity_is_present,
    _inverter_identity_signature,
    apply_canonical_measurements,
    apply_collector_cloud_family_observation,
    canonical_measurements_for_driver,
    collector_cloud_family_observation_from_collector,
    collector_cloud_family_observation_from_mapping,
    logger,
    monotonic,
    parse_esp_collector_hardware_token,
    project_canonical_telemetry,
    reconcile_durable_pn,
    select_preferred_collector_cloud_family,
)


class HubSnapshotMixin:
    """Methods owned by HubSnapshotMixin."""

    def _recovery_backoff_delay(self) -> float:
        base = max(2.0, float(self._connection.request_timeout))
        return min(60.0, base * (2 ** max(self._recovery_streak - 1, 0)))

    def _recovery_backoff_remaining(self) -> float:
        if self._recovery_backoff_until_monotonic <= 0.0:
            return 0.0
        return max(0.0, self._recovery_backoff_until_monotonic - monotonic())

    def _record_recovery_attempt(self, *, reason: str) -> None:
        self._reconnect_count += 1
        self._last_recovery_reason = reason

    def _record_recovery_failure(self, *, reason: str) -> None:
        self._recovery_streak += 1
        self._last_recovery_reason = reason
        self._recovery_backoff_until_monotonic = monotonic() + self._recovery_backoff_delay()

    def _record_refresh_success(self) -> None:
        self._last_success_monotonic = monotonic()
        self._collector_outage_caches_cleared = False
        self._recovery_streak = 0
        self._recovery_backoff_until_monotonic = 0.0
        self._last_recovery_reason = ""

    def _record_state_transition(self, tracks: tuple[str, ...]) -> None:
        """Append a composite state transition to the bounded diagnostic ring."""

        if tracks == self._last_composite_state:
            return
        self._last_composite_state = tracks
        self._state_transition_history.append("/".join(tracks))

    def _apply_runtime_state_values(
        self,
        values: dict[str, object],
        *,
        snapshot_connected: bool,
        last_error: str | None,
    ) -> None:
        """Derive the explicit runtime state-machine tracks onto the snapshot.

        These are diagnostic/auditable projections of the hub's implicit state;
        they never drive transport, ownership, or wire decisions. The tracks are
        independent on purpose -- the inverter track keeps its own lifecycle so a
        collector-only observation never erases a confirmed inverter identity.
        """

        collector = self._link_manager.collector_info
        collector_pn = str(getattr(collector, "collector_pn", "") or "").strip()
        durable_pn = str(getattr(self._connection, "collector_pn", "") or "").strip()
        inverter = self._inverter
        identity_present = _inverter_identity_is_present(inverter)
        detection_status = str(values.get("runtime_detection_status") or "").strip()
        recovering = (
            self._recovery_streak > 0
            or self._recovery_backoff_remaining() > 0.0
            or bool(last_error)
        )

        session_state = (
            RUNTIME_SESSION_STATE_ONLINE
            if snapshot_connected
            else RUNTIME_SESSION_STATE_OFFLINE
        )
        collector_state = (
            RUNTIME_COLLECTOR_STATE_IDENTIFIED
            if (collector_pn or durable_pn)
            else RUNTIME_COLLECTOR_STATE_UNKNOWN
        )

        if self._inverter_identity_conflict:
            inverter_state = RUNTIME_INVERTER_STATE_CONFLICT
        elif len(self.inverter_protocol_candidates) > 1:
            inverter_state = RUNTIME_INVERTER_STATE_AMBIGUOUS
        elif not identity_present:
            inverter_state = (
                RUNTIME_INVERTER_STATE_DETECTING
                if snapshot_connected
                else RUNTIME_INVERTER_STATE_ABSENT
            )
        elif (
            detection_status in _PROVISIONAL_INVERTER_DETECTION_STATUSES
            or self._inverter_binding_needs_live_detection_refresh
        ):
            inverter_state = RUNTIME_INVERTER_STATE_PROVISIONAL
        else:
            inverter_state = RUNTIME_INVERTER_STATE_LIVE_CONFIRMED

        # runtime_driver_state keeps its historical three values (backward compat).
        if not snapshot_connected:
            driver_state = RUNTIME_DRIVER_STATE_COLLECTOR_OFFLINE
        elif inverter is not None:
            driver_state = RUNTIME_DRIVER_STATE_DRIVER_BOUND
        else:
            driver_state = RUNTIME_DRIVER_STATE_DRIVER_UNBOUND

        if not snapshot_connected:
            poll_state = RUNTIME_POLL_STATE_OFFLINE
        elif inverter is None:
            poll_state = RUNTIME_POLL_STATE_DETECTING
        elif recovering:
            poll_state = RUNTIME_POLL_STATE_DEGRADED
        else:
            poll_state = RUNTIME_POLL_STATE_POLLING

        values["runtime_session_state"] = session_state
        values["runtime_collector_state"] = collector_state
        values["runtime_inverter_state"] = inverter_state
        values["runtime_driver_state"] = driver_state
        values["runtime_poll_state"] = poll_state

        candidates = self.inverter_protocol_candidates
        if len(candidates) > 1:
            values["runtime_inverter_candidate_count"] = len(candidates)
            values["runtime_inverter_candidate_drivers"] = ", ".join(
                candidate.driver_key for candidate in candidates
            )
        else:
            values.pop("runtime_inverter_candidate_count", None)
            values.pop("runtime_inverter_candidate_drivers", None)

        if self._inverter_identity_conflict:
            values["runtime_identity_conflict"] = self._inverter_identity_conflict
        elif values.get("collector_pn_identity_conflict"):
            values["runtime_identity_conflict"] = "collector_pn"
        else:
            values.pop("runtime_identity_conflict", None)

        # The last identity that reached driver_bound stays available across
        # offline/degraded snapshots so support can see the confirmed inverter
        # even while it is temporarily unreachable.
        if driver_state == RUNTIME_DRIVER_STATE_DRIVER_BOUND and identity_present:
            signature = _inverter_identity_signature(inverter)
            if signature:
                self._last_driver_bound_identity = signature
        if self._last_driver_bound_identity:
            values["runtime_last_driver_bound_identity"] = self._last_driver_bound_identity

        self._record_state_transition(
            (session_state, collector_state, inverter_state, driver_state, poll_state)
        )
        if self._state_transition_history:
            values["runtime_state_transitions"] = " | ".join(
                self._state_transition_history
            )

    def _build_snapshot(
        self,
        *,
        extra_values: dict[str, object] | None = None,
        last_error: str | None = None,
        connected: bool | None = None,
        preserve_inverter_values: bool = False,
    ) -> RuntimeSnapshot:
        generated_canonical_keys: set[str] = set()
        runtime_owned_keys: set[str] = set()
        if self._inverter is not None and not preserve_inverter_values:
            generated_canonical_keys = {
                description.key
                for description in canonical_measurements_for_driver(self._inverter.driver_key)
            }
        if not preserve_inverter_values:
            # ALL runtime-owned keys (current identity's + the previous identity's
            # stale keys) are dropped from the carried snapshot, then the current
            # runtime cache is re-applied below. This removes canonical AND
            # non-canonical / raw / optional driver values that a FULL snapshot,
            # a DELTA ``removed_keys``, or an identity change no longer includes --
            # generically, never via a driver-specific key list.
            runtime_owned_keys = (
                self._runtime_measurement_owned_keys | self._stale_runtime_owned_keys
            )

        stripped_keys = (
            generated_canonical_keys
            | runtime_owned_keys
            | self._runtime_driver_diagnostic_owned_keys
            | self._stale_runtime_driver_diagnostic_keys
        )
        values = {
            key: value
            for key, value in self._last_snapshot.values.items()
            if (
                not key.startswith("capability_block_")
                and key not in stripped_keys
                and key not in _VOLATILE_COLLECTOR_VALUE_KEYS
            )
        }
        # Older single-driver detection could copy the raw probe log from
        # ``DetectedInverter.details``.  Keep only the sanitized runtime view.
        values.pop("probe_log", None)
        for key in _VOLATILE_COLLECTOR_VALUE_KEYS:
            values.pop(key, None)
        collector = self._link_manager.collector_info

        # PN stability: the durable entry PN is authoritative. A short live
        # heartbeat PN must not downgrade it, and a different full PN is an
        # identity conflict rather than a normal update.
        durable_collector_pn = str(
            getattr(self._connection, "collector_pn", "") or ""
        ).strip()
        collector_pn_identity_conflict = False
        if durable_collector_pn:
            reconciled_pn, collector_pn_identity_conflict = reconcile_durable_pn(
                durable_collector_pn,
                collector.collector_pn,
            )
            if collector_pn_identity_conflict:
                logger.warning(
                    "Collector PN identity conflict: entry expects %s but live session reports %s; keeping the durable identity",
                    durable_collector_pn,
                    collector.collector_pn,
                )
            if reconciled_pn and reconciled_pn != collector.collector_pn:
                collector.collector_pn = reconciled_pn
                collector.collector_pn_prefix = reconciled_pn[:1]
                collector.collector_pn_digits = reconciled_pn[1:]

        collector_field_overrides = extra_values or {}
        if collector_field_overrides:
            merged_collector_pn, override_pn_conflict = reconcile_durable_pn(
                collector.collector_pn,
                collector_field_overrides.get("collector_pn"),
            )
            collector_pn_identity_conflict = (
                collector_pn_identity_conflict or override_pn_conflict
            )
            if merged_collector_pn and merged_collector_pn != collector.collector_pn:
                collector.collector_pn = merged_collector_pn
                collector.collector_pn_prefix = merged_collector_pn[:1]
                collector.collector_pn_digits = merged_collector_pn[1:]
            collector.smartess_collector_version = str(
                collector_field_overrides.get("smartess_collector_version", collector.smartess_collector_version) or ""
            )
            collector.smartess_protocol_raw_id = str(
                collector_field_overrides.get("smartess_protocol_raw_id", collector.smartess_protocol_raw_id) or ""
            )
            collector.smartess_protocol_asset_id = str(
                collector_field_overrides.get("smartess_protocol_asset_id", collector.smartess_protocol_asset_id) or ""
            )
            collector.smartess_protocol_asset_name = str(
                collector_field_overrides.get("smartess_protocol_asset_name", collector.smartess_protocol_asset_name) or ""
            )
            collector.smartess_protocol_suffix = str(
                collector_field_overrides.get("smartess_protocol_suffix", collector.smartess_protocol_suffix) or ""
            )
            collector.smartess_protocol_profile_key = str(
                collector_field_overrides.get("smartess_protocol_profile_key", collector.smartess_protocol_profile_key) or ""
            )
            collector.smartess_protocol_name = str(
                collector_field_overrides.get("smartess_protocol_name", collector.smartess_protocol_name) or ""
            )
            if collector_field_overrides.get("smartess_device_address") is not None:
                collector.smartess_device_address = int(collector_field_overrides["smartess_device_address"])
            hardware_token = parse_esp_collector_hardware_token(
                collector_field_overrides.get("collector_hardware_version")
            )
            if hardware_token.is_bridge:
                collector.collector_virtual_bridge = True
                collector.collector_bridge_kind = "esp-collector"
                collector.collector_bridge_version = hardware_token.version

        if collector.remote_ip:
            values["collector_remote_ip"] = collector.remote_ip
        values["collector_connection_count"] = collector.connection_count
        values["collector_connection_replace_count"] = collector.connection_replace_count
        values["collector_disconnect_count"] = collector.disconnect_count
        values["collector_pending_request_drop_count"] = collector.pending_request_drop_count
        values["collector_raw_request_count"] = collector.raw_request_count
        values["collector_raw_response_count"] = collector.raw_response_count
        values["collector_raw_timeout_count"] = collector.raw_timeout_count
        values["collector_raw_unhandled_line_count"] = collector.raw_unhandled_line_count
        values["collector_raw_last_spacing_wait_ms"] = (
            collector.raw_last_spacing_wait_ms
        )
        values["collector_raw_last_response_duration_ms"] = (
            collector.raw_last_response_duration_ms
        )
        values["collector_raw_last_total_duration_ms"] = (
            collector.raw_last_total_duration_ms
        )
        for key, value in (
            ("collector_raw_last_request_ascii", collector.raw_last_request_ascii),
            ("collector_raw_last_request_hex", collector.raw_last_request_hex),
            ("collector_raw_last_response_ascii", collector.raw_last_response_ascii),
            ("collector_raw_last_response_hex", collector.raw_last_response_hex),
            (
                "collector_raw_last_timeout_request_ascii",
                collector.raw_last_timeout_request_ascii,
            ),
            ("collector_raw_last_parser", collector.raw_last_parser),
            ("collector_raw_last_frame_format", collector.raw_last_frame_format),
        ):
            if value:
                values[key] = value
            else:
                values.pop(key, None)
        values["collector_discovery_restart_count"] = collector.discovery_restart_count
        if collector.collector_pn:
            values["collector_pn"] = collector.collector_pn
        if collector_pn_identity_conflict:
            values["collector_pn_identity_conflict"] = True
        if collector.profile_name:
            values["collector_profile"] = collector.profile_name
        if collector.profile_key:
            values["collector_profile_key"] = collector.profile_key
        if collector.last_disconnect_reason:
            values["collector_last_disconnect_reason"] = collector.last_disconnect_reason
        else:
            values.pop("collector_last_disconnect_reason", None)
        if collector.last_discovery_reason:
            values["collector_last_discovery_reason"] = collector.last_discovery_reason
        else:
            values.pop("collector_last_discovery_reason", None)
        if collector.heartbeat_devcode is not None:
            values["collector_heartbeat_devcode"] = f"0x{collector.heartbeat_devcode:04X}"
        if collector.heartbeat_payload_hex:
            values["collector_heartbeat_payload"] = collector.heartbeat_payload_hex
        if collector.heartbeat_age_seconds is not None:
            values["collector_heartbeat_age_seconds"] = round(collector.heartbeat_age_seconds, 1)
        else:
            values.pop("collector_heartbeat_age_seconds", None)
        if collector.heartbeat_ascii:
            values["collector_heartbeat_ascii"] = collector.heartbeat_ascii
        if collector.heartbeat_payload_len is not None:
            values["collector_heartbeat_payload_len"] = collector.heartbeat_payload_len
        if collector.heartbeat_format_key:
            values["collector_heartbeat_format"] = collector.heartbeat_format_key
        if collector.heartbeat_suffix_ascii:
            values["collector_heartbeat_suffix"] = collector.heartbeat_suffix_ascii
        if collector.heartbeat_suffix_kind:
            values["collector_heartbeat_suffix_kind"] = collector.heartbeat_suffix_kind
        if collector.heartbeat_suffix_uint is not None:
            values["collector_heartbeat_suffix_uint"] = collector.heartbeat_suffix_uint
        if collector.devcode_major is not None:
            values["collector_devcode_major"] = collector.devcode_major
        if collector.devcode_minor is not None:
            values["collector_devcode_minor"] = collector.devcode_minor
        if collector.collector_pn_prefix:
            values["collector_pn_prefix"] = collector.collector_pn_prefix
        if collector.collector_pn_digits:
            values["collector_pn_digits"] = collector.collector_pn_digits
        values["connection_type"] = CONNECTION_TYPE_EYBOND
        if self._connection_mode:
            values["connection_mode"] = self._connection_mode
        if self._connection.collector_ip:
            values["configured_collector_ip"] = self._connection.collector_ip
        listener_diagnostics = getattr(self._link_manager, "listener_diagnostics", None)
        if listener_diagnostics is not None:
            values.update(listener_diagnostics())
            if not values.get("collector_listener_last_error"):
                values.pop("collector_listener_last_error", None)
        # Non-sensitive collector-management diagnostics: selected adapter
        # capabilities + the last operation's status/error-class/duration/time
        # (NEVER endpoint values or credentials).
        try:
            caps = self.collector_management_capabilities()
        except Exception:  # pragma: no cover - defensive during snapshot build
            caps = None
        if caps is not None:
            values["collector_management_can_read_endpoint_state"] = caps.read_endpoint_state
            values["collector_management_can_write_endpoint"] = caps.write_endpoint
            values["collector_management_can_apply_changes"] = caps.apply_changes
            values["collector_management_can_reboot"] = caps.reboot
        if self._last_management_operation is not None:
            op = self._last_management_operation
            values["collector_management_last_operation"] = op.get("operation", "")
            values["collector_management_last_status"] = op.get("status", "")
            values["collector_management_last_error_class"] = op.get("error_class", "")
            values["collector_management_last_error_code"] = op.get("error_code", "")
            values["collector_management_last_duration_ms"] = op.get("duration_ms", 0)
            values["collector_management_last_timestamp"] = op.get("timestamp", 0.0)
        # Non-sensitive collector-metadata TELEMETRY diagnostics: channel routes /
        # provenance / generation / per-channel outcome+duration / cache dirty /
        # dead channels (NEVER endpoint values, credentials, or raw payloads).
        self._apply_collector_metadata_diagnostics(values)
        # Collector Devcode is STABLE identity: the heartbeat frame's devcode.
        # 0x0000 is a valid devcode, so gate on ``is not None`` and NEVER fall
        # back to the volatile last-frame devcode -- that alternates with every
        # data forward and made the sensor flip between 0x0994 / 0x0001 / 0x0000.
        # The last frame is exposed separately as an honestly-labelled frame
        # diagnostic so it can never be mistaken for the collector identity.
        if collector.heartbeat_devcode is not None:
            values["collector_devcode"] = f"0x{collector.heartbeat_devcode:04X}"
        else:
            values.pop("collector_devcode", None)
        if collector.last_devcode is not None:
            values["collector_last_frame_devcode"] = f"0x{collector.last_devcode:04X}"
        else:
            values.pop("collector_last_frame_devcode", None)
        if collector.last_udp_reply:
            values["collector_udp_reply"] = collector.last_udp_reply
        if collector.last_udp_reply_from:
            values["collector_udp_reply_from"] = collector.last_udp_reply_from
        if collector.smartess_collector_version:
            values["smartess_collector_version"] = collector.smartess_collector_version
        if collector.smartess_protocol_raw_id:
            values["smartess_protocol_raw_id"] = collector.smartess_protocol_raw_id
        if collector.smartess_protocol_asset_id:
            values["smartess_protocol_asset_id"] = collector.smartess_protocol_asset_id
        if collector.smartess_protocol_asset_name:
            values["smartess_protocol_asset_name"] = collector.smartess_protocol_asset_name
        if collector.smartess_protocol_suffix:
            values["smartess_protocol_suffix"] = collector.smartess_protocol_suffix
        if collector.smartess_protocol_profile_key:
            values["smartess_protocol_profile_key"] = collector.smartess_protocol_profile_key
        if collector.smartess_protocol_name:
            values["smartess_protocol_name"] = collector.smartess_protocol_name
        if collector.smartess_device_address is not None:
            values["smartess_device_address"] = collector.smartess_device_address
        if collector.collector_virtual_bridge:
            values["collector_virtual_bridge"] = True
            if collector.collector_bridge_kind:
                values["collector_bridge_kind"] = collector.collector_bridge_kind
            if collector.collector_bridge_version:
                values["collector_bridge_version"] = collector.collector_bridge_version

        if self._inverter is not None:
            values["driver_key"] = self._inverter.driver_key
            values["protocol_family"] = self._inverter.protocol_family
            if not (extra_values and "runtime_detection_status" in extra_values):
                values.pop("runtime_detection_status", None)
            if self._inverter.variant_key:
                values["variant_key"] = self._inverter.variant_key
            if self._inverter.profile_name:
                values["profile_name"] = self._inverter.profile_name
            if self._inverter.register_schema_name:
                values["register_schema_name"] = self._inverter.register_schema_name
            values["model_name"] = self._inverter.model_name
            values["serial_number"] = self._inverter.serial_number
            # Inverter payload route (probe target). This is the route the driver
            # reaches the inverter over -- distinct from the collector-management
            # route and from any single last frame. Every field gates on
            # ``is not None`` so devcode/addr 0x0000 stay visible.
            probe_target = getattr(self._inverter, "probe_target", None)
            if probe_target is not None:
                if getattr(probe_target, "devcode", None) is not None:
                    values["inverter_route_devcode"] = f"0x{probe_target.devcode:04X}"
                if getattr(probe_target, "collector_addr", None) is not None:
                    values["inverter_route_collector_addr"] = probe_target.collector_addr
                if getattr(probe_target, "device_addr", None) is not None:
                    values["inverter_route_device_addr"] = probe_target.device_addr
            if self._inverter.capabilities:
                values["write_capabilities"] = ", ".join(
                    capability.key for capability in self._inverter.capabilities
                )
            # Detection ``details`` are bootstrap/config ONLY. They seed values
            # before the first runtime poll, but they must never re-seed a key
            # the runtime measurement cache already owns -- that is exactly the
            # revert-to-detection bug (e.g. battery_voltage 27.3 -> 27.2). Once a
            # key has been measured for this identity it is owned by the cache.
            owned = self._runtime_measurement_owned_keys
            if owned:
                for key, value in self._inverter.details.items():
                    if key != "probe_log" and key not in owned:
                        values[key] = value
            else:
                values.update(
                    (key, value)
                    for key, value in self._inverter.details.items()
                    if key != "probe_log"
                )
            # Last-good runtime measurements are authoritative for their keys on
            # EVERY snapshot build (including error / last-known-good paths), so a
            # cycle that omitted a measurement keeps the previous live value.
            values.update(self._runtime_measurement_values)
            values.update(self._runtime_driver_diagnostics)

        if extra_values:
            safe_extra_values = dict(extra_values)
            if "collector_pn" in safe_extra_values:
                if collector.collector_pn:
                    safe_extra_values["collector_pn"] = collector.collector_pn
                else:
                    safe_extra_values.pop("collector_pn", None)
            values.update(safe_extra_values)

        if self._inverter_detection_probe_log:
            values["runtime_inverter_probe_log"] = [
                dict(entry) for entry in self._inverter_detection_probe_log
            ]
            values["runtime_inverter_probe_total_ms"] = sum(
                int(entry["elapsed_ms"])
                for entry in self._inverter_detection_probe_log
            )
            values["runtime_inverter_probe_budget_exhausted"] = (
                self._inverter_detection_probe_budget_exhausted
            )
            values["runtime_inverter_probe_current_session"] = (
                self._inverter_detection_probe_generation
                == self._owned_session_generation()
            )
        else:
            values.pop("runtime_inverter_probe_log", None)
            values.pop("runtime_inverter_probe_total_ms", None)
            values.pop("runtime_inverter_probe_budget_exhausted", None)
            values.pop("runtime_inverter_probe_current_session", None)

        callback_endpoint = values.get("collector_server_endpoint")
        if callback_endpoint:
            values["collector_callback_owner"] = _callback_owner_label(
                callback_endpoint,
                server_ip=self._connection.server_ip,
                advertised_server_ip=self._connection.effective_advertised_server_ip,
                advertised_tcp_port=self._connection.effective_advertised_tcp_port,
            )
        else:
            values.pop("collector_callback_owner", None)

        signal_strength = values.get("collector_signal_strength")
        if signal_strength is not None:
            values["collector_signal_quality"] = _collector_signal_quality(signal_strength)
        else:
            values.pop("collector_signal_quality", None)

        signal_source = values.get("collector_signal_strength_source")
        if signal_source:
            values["collector_signal_strength_source"] = _collector_signal_source_label(
                signal_source
            )
        else:
            values.pop("collector_signal_strength_source", None)

        if self._inverter is not None:
            apply_canonical_measurements(
                self._inverter.driver_key,
                values,
                variant_key=self._inverter.variant_key,
            )

        values["runtime_recovery_streak"] = self._recovery_streak
        values["runtime_reconnect_count"] = self._reconnect_count
        values["runtime_backoff_seconds"] = round(self._recovery_backoff_remaining(), 1)
        if self._last_success_monotonic is not None:
            values["runtime_last_success_age_seconds"] = round(
                max(0.0, monotonic() - self._last_success_monotonic),
                1,
            )
        else:
            values.pop("runtime_last_success_age_seconds", None)
        if self._last_recovery_reason:
            values["runtime_last_recovery_reason"] = self._last_recovery_reason
        else:
            values.pop("runtime_last_recovery_reason", None)

        operating_mode = values.get("operating_mode")
        if operating_mode != self._last_operating_mode:
            clearable = [
                capability_key
                for capability_key, blocker in self._write_blockers.items()
                if blocker.clear_on == "mode_change"
            ]
            if self._last_operating_mode is not None and clearable:
                logger.info(
                    "Operating mode changed from %r to %r; clearing %d capability write blockers",
                    self._last_operating_mode,
                    operating_mode,
                    len(clearable),
                )
            for capability_key in clearable:
                self._write_blockers.pop(capability_key, None)
            self._last_operating_mode = operating_mode

        for capability_key, blocker in sorted(self._write_blockers.items()):
            values[f"capability_block_reason_{capability_key}"] = blocker.reason
            values[f"capability_block_code_{capability_key}"] = blocker.code
            if blocker.suggested_action:
                values[f"capability_block_action_{capability_key}"] = blocker.suggested_action
            if blocker.exception_code is not None:
                values[f"capability_block_exception_{capability_key}"] = blocker.exception_code
        if self._write_blockers:
            values["blocked_write_capabilities"] = ", ".join(sorted(self._write_blockers))
            values["blocked_write_count"] = len(self._write_blockers)
            values["blocked_write_summary"] = "; ".join(
                f"{capability_key}: {blocker.code}"
                for capability_key, blocker in sorted(self._write_blockers.items())
            )
        else:
            values.pop("blocked_write_capabilities", None)
            values.pop("blocked_write_count", None)
            values.pop("blocked_write_summary", None)

        snapshot_connected = self._link_manager.connected if connected is None else connected
        self._apply_runtime_state_values(
            values,
            snapshot_connected=snapshot_connected,
            last_error=last_error,
        )

        if last_error:
            values["last_error"] = last_error
        else:
            values.pop("last_error", None)

        if self._inverter_overlay_applier is not None and self._inverter is not None:
            # Merge activated device-scoped learned controls into the inverter on every
            # snapshot. This is idempotent (it only appends not-yet-present learned
            # capabilities). It must run here, not only at detection: detection completes
            # before the collector identity is fully populated, so a detection-time scope
            # match can fail and never retry; here the collector is ready and the merge
            # converges, so the learned controls reliably become entities and are writable.
            self._inverter = self._inverter_overlay_applier(self._inverter, collector)

        typed_telemetry = self._runtime_measurement_telemetry
        if self._inverter is not None:
            typed_telemetry = project_canonical_telemetry(
                typed_telemetry,
                variant_key=self._inverter.variant_key or None,
            )
        if last_error or not snapshot_connected:
            typed_telemetry = typed_telemetry.as_carried()

        # ``values`` is the mutable assembly workspace: canonical projection,
        # write-blocker transitions, and runtime-state derivation intentionally
        # see the complete typed-first compatibility view while this snapshot is
        # being built.  The published broad mapping, however, owns only keys
        # that are not already represented by the immutable telemetry frame.
        # This removes the historical duplicate measurement authority without
        # changing the single internal snapshot assembly path.
        typed_keys = set(typed_telemetry.values())
        broad_values = {
            key: value for key, value in values.items() if key not in typed_keys
        }

        snapshot = RuntimeSnapshot(
            connected=snapshot_connected,
            collector=collector,
            inverter=self._inverter,
            values=broad_values,
            last_error=last_error,
            telemetry=typed_telemetry,
        )
        endpoint = ""
        if extra_values is not None:
            candidate = extra_values.get("collector_server_endpoint", "")
            if type(candidate) is str and candidate and candidate == candidate.strip():
                endpoint = candidate
        if not endpoint:
            endpoint = snapshot.collector_server_endpoint
        if endpoint:
            snapshot.set_collector_server_endpoint(endpoint)
        family_observation = select_preferred_collector_cloud_family(
            collector_cloud_family_observation_from_collector(snapshot.collector),
            collector_cloud_family_observation_from_mapping(extra_values),
        )
        if family_observation.known:
            apply_collector_cloud_family_observation(
                snapshot.collector,
                family_observation,
            )
            snapshot.values["collector_cloud_family"] = family_observation.family
            if family_observation.source:
                snapshot.values["collector_cloud_family_source"] = (
                    family_observation.source
                )
            else:
                snapshot.values.pop("collector_cloud_family_source", None)
            if family_observation.confidence:
                snapshot.values["collector_cloud_family_confidence"] = (
                    family_observation.confidence
                )
            else:
                snapshot.values.pop("collector_cloud_family_confidence", None)
        # An explicit fresh cloud-profile record wins as one value. The older
        # SmartESS-named protocol fields remain compatibility inputs only and
        # therefore cannot replace an already-explicit cloud profile.
        fresh_profile = None
        if extra_values is not None:
            explicit_key = extra_values.get("collector_cloud_profile_key", "")
            if type(explicit_key) is str and explicit_key:
                fresh_profile = RuntimeSnapshot(
                    values=dict(extra_values)
                ).collector_cloud_profile
        profile = (
            fresh_profile
            if fresh_profile is not None and fresh_profile.known
            else snapshot.collector_cloud_profile
        )
        if profile.known:
            snapshot.set_collector_cloud_profile(profile)
        return snapshot
