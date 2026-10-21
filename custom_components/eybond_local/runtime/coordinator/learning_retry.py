"""Explicit protected retries. No hold retirement or collector configuration writes."""

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from ...connection.collector_endpoint_operation import (
    COLLECTOR_ENDPOINT_OPERATION_AUTHORITY as AUTHORITY, OPERATION_SHADOW_LEARNING,
)
from ...connection.protected_learning_route import ProtectedLearningRoutePermit
from ...metadata.collector_cloud_profile_catalog_loader import resolve_collector_cloud_session_protocol
from ...passive_discovery import get_callback_session_registry
from ...support.memory_guard import read_available_memory_mib, shadow_learning_memory_blocker
from ...support.owned_worker import async_run_owned_worker
from ...support.shadow_learning.backend import (
    build_shadow_learning_seed, build_shadow_learning_preflight, build_shadow_learning_trace_path,
    learning_probe_target,
)
from ...support.shadow_learning.protection_store import CloudLearningProtectionStore
from ...support.shadow_learning.session import (
    build_shadow_learning_lease_deadline, shadow_learning_session_timestamp,
    replace_held_shadow_learning_session, load_shadow_learning_session_state,
)
from ...telemetry import TelemetryFreshness
from .endpoint_projection import resolve_collector_server_endpoint
from .learning_isolation import ProtectedLearningRun
from .learning_route import learning_listener_port, learning_target_endpoint


async def async_retry_context(c):
    """Read-only eligibility; never turn a corrupt/legacy/foreign hold into permission."""
    if await c._async_cloud_learning_hold_reason() != "cloud_learning_hold_pending":
        return None
    state = await c._async_active_shadow_learning_state(require_process=False)
    store = CloudLearningProtectionStore(Path(c.hass.config.config_dir))
    hold = await async_run_owned_worker(store.load, collector_pn=c.smartess_collector_pn)
    target = learning_target_endpoint(c)
    if (hold is None or state is None or state.status != "restore_failed"
            or state.protection_version != 1 or not state.cloud_commands_possible
            or hold.source not in {"smartess", "dessmonitor", "valuecloud"}
            or hold.entry_id != c.config_entry.entry_id or state.entry_id != hold.entry_id
            or state.collector_pn != hold.collector_pn or state.collector_pn != c.smartess_collector_pn
            or not state.route_owner_id.startswith("shadow_learning:")
            or c._endpoint_effective_parts(state.proxy_endpoint) != c._endpoint_effective_parts(target)
            or not state.upstream_endpoint or c._shadow_learning_process_running()
            or c._runtime.proxy_capture_route_running()
            or getattr(c, "_shutdown_complete", False)):
        return None
    return state, hold


async def _prove_local_route(c, state):
    """Revalidate the already-local route using reads, never reset or rewrite it."""
    registry = get_callback_session_registry(c.hass)
    entry = c.config_entry.entry_id
    if registry is None:
        raise RuntimeError("cloud_learning_retry_local_session_unavailable")
    # Durable by-PN runtime claims are intentionally unpinned after HA restart.
    # Bind only the registry's current trusted observed socket, then certify
    # that exact owner/session. Never trigger a callback or use peer IP here.
    sid = registry.pin_owner_claim_to_current_observed_session(entry)
    cert = registry.certify_permanent_owned_session(entry, sid)
    handle = registry.session_handle_for_owned_session(entry, sid)
    if (cert is None or cert.collector_pn != state.collector_pn or handle is None
            or handle.listener_port != learning_listener_port(c)):
        raise RuntimeError("cloud_learning_retry_local_identity_unverified")
    endpoint = await c._runtime.async_get_collector_server_endpoint_state(require_heartbeat=False)
    if (endpoint.get("reboot_required") not in (None, "", "0", False)
            or endpoint.get("apply_required") not in (None, "", "0", False)
            or c._endpoint_effective_parts(endpoint.get("current_endpoint", ""))
            != c._endpoint_effective_parts(state.proxy_endpoint)):
        raise RuntimeError("cloud_learning_retry_local_endpoint_unverified")
    snapshot = await c._runtime.async_refresh()
    if (not snapshot.connected or snapshot.last_error or snapshot.inverter is None
            or not any(p.freshness is TelemetryFreshness.FRESH and p.value is not None
                       for p in snapshot.telemetry.points)
            or not registry.reverify_permanent_owned_session(cert)):
        raise RuntimeError("cloud_learning_retry_local_payload_unverified")
    return registry, cert


async def async_start_protected_retry(c, *, confirm_cloud_outage, cloud_source, allow_ack_writes=False):
    """Own the preparation lock for the entire fresh, filtered route transaction."""
    if (confirm_cloud_outage is not True or allow_ack_writes is not False
            or cloud_source not in {"smartess", "dessmonitor", "valuecloud"}):
        raise RuntimeError("cloud_learning_hold_consent_required")
    async with c._async_cloud_tool_preparation():
        context = await async_retry_context(c)
        if context is None:
            raise RuntimeError("cloud_learning_protection_held")
        previous, hold = context
        if not c.collector_actions_enabled or not c.support_acquisition_readiness.active_control_learning.can_start:
            raise RuntimeError("shadow_learning_collector_control_disabled")
        memory = await async_run_owned_worker(read_available_memory_mib)
        if blocker := shadow_learning_memory_blocker(memory):
            raise RuntimeError(f"shadow_learning_preflight_blocked:{blocker}")
        token = AUTHORITY.adopt(previous.entry_id, OPERATION_SHADOW_LEARNING, previous.route_owner_id)
        if token is None:
            raise RuntimeError("collector_endpoint_operation_busy")
        registry, cert = await _prove_local_route(c, previous)
        raw = await c._runtime.async_capture_support_evidence()
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        session_id = f"{previous.entry_id}_{timestamp}"
        config_dir = Path(c.hass.config.config_dir)
        trace = await async_run_owned_worker(
            build_shadow_learning_trace_path, config_dir=config_dir, entry_id=previous.entry_id,
            collector_pn=previous.collector_pn, timestamp=timestamp,
        )
        seed, blockers = build_shadow_learning_seed(
            session_id=session_id, entry_id=previous.entry_id, collector_pn=previous.collector_pn,
            collector_cloud_family=c.collector_cloud_family,
            raw_passthrough_frame_format=c.collector_raw_passthrough_frame_format,
            collector_cloud_profile_key=c.collector_cloud_profile_key,
            collector_cloud_profile_label=c.collector_cloud_profile_label,
            collector_cloud_profile_source=c.collector_cloud_profile_source,
            collector_cloud_profile_confidence=c.collector_cloud_profile_confidence,
            collector_callback_endpoint=previous.proxy_endpoint,
            effective_metadata_snapshot=c.shadow_learning_effective_metadata,
            raw_capture=raw, write_response_mode="exception", allow_ack_writes=False,
            inverter_target=learning_probe_target(getattr(c, "identified_inverter", None)),
            identified_inverter=getattr(c, "identified_inverter", None),
            prepare_native_pi30=True,
            inspect_cloud_protocol=getattr(c, "collector_session_protocol", "") == "eybond_framed",
            inverter_device_addr=getattr(getattr(getattr(c, "identified_inverter", None), "probe_target", None), "device_addr", None),
        )
        blockers = blockers or build_shadow_learning_preflight(seed).blockers
        if blockers:
            raise RuntimeError("shadow_learning_preflight_blocked:" + ",".join(blockers))
        host, port, _ = resolve_collector_server_endpoint(previous.upstream_endpoint, cloud_family=c.collector_cloud_family)
        _, callback_port, _ = resolve_collector_server_endpoint(previous.proxy_endpoint, cloud_family=c.collector_cloud_family)
        await c._async_preflight_proxy_capture_network(
            target_host=c._effective_callback_server_host, target_port=callback_port,
            upstream_host=host, upstream_port=port,
        )
        if not registry.reverify_permanent_owned_session(cert) or not AUTHORITY.owns(token):
            raise RuntimeError("cloud_learning_retry_local_session_changed")
        now = shadow_learning_session_timestamp()
        state = replace(previous, route_owner_id="shadow_learning:" + session_id,
                        trace_path=str(trace), original_endpoint=previous.proxy_endpoint,
                        restore_required=False, started_at=now, updated_at=now,
                        expires_at=build_shadow_learning_lease_deadline(
                            lease_seconds=c.proxy_capture_configured_duration_minutes * 60),
                        restore_attempt_count=0, last_restore_attempt_at="", last_restore_error="",
                        status="preflight", cloud_source=cloud_source)
        store = CloudLearningProtectionStore(config_dir)
        c._protected_learning_run = None

        async def rollover():
            nonlocal token
            try:
                await async_run_owned_worker(replace_held_shadow_learning_session,
                    config_dir=config_dir, previous=previous, state=state, hold=hold)
            finally:
                # A directory fsync may fail AFTER atomic replacement. Adopt the
                # actual durable owner even on that failure; never leave a free gap.
                c._shadow_learning_session_state_loaded = False
                actual = await async_run_owned_worker(
                    load_shadow_learning_session_state, config_dir=config_dir,
                    entry_id=c.config_entry.entry_id,
                )
                if actual == state:
                    token = AUTHORITY.handoff_shadow_learning(token, state.route_owner_id)
                    c._cached_shadow_learning_session_state = state
                    c._shadow_learning_session_state_loaded = True

        try:
            _, cancellation = await c._run_finalization_shielded(rollover)
            if cancellation is not None:
                raise cancellation
            state = replace(state, status="starting")
            await c._async_save_shadow_learning_session_state(state)
            # A failed startup must export THIS run's trace, not an earlier
            # successful study whose artifacts are still visible in HA.
            c._publish_tooling_values(shadow_learning_session_status="starting",
                shadow_learning_session_ready=False, shadow_learning_trace_path=str(trace),
                shadow_learning_session_id=state.route_owner_id)
            run = ProtectedLearningRun(state.entry_id, state.route_owner_id, state.collector_pn,
                                       state.proxy_endpoint, learning_listener_port(c), cloud_source, hold)
            c._protected_learning_run = run

            async def validate_route():
                await async_run_owned_worker(store.require_exact_hold, hold=hold)
                actual = await c._async_active_shadow_learning_state(require_process=False)
                if (actual != state or c._protected_learning_run is not run
                        or getattr(c, "_shutdown_complete", False) or not AUTHORITY.owns(token)
                        or c.smartess_collector_pn != state.collector_pn
                        or not registry.reverify_permanent_owned_session(cert)):
                    raise RuntimeError("cloud_learning_retry_route_context_changed")

            permit = ProtectedLearningRoutePermit(state.entry_id, state.route_owner_id,
                state.collector_pn, run.local_port, host, port, validate_route)
            await c._runtime.async_start_shadow_learning_route(
                owner_id=state.route_owner_id, entry_id=state.entry_id,
                collector_ip=c._proxy_capture_collector_ip(), collector_pn=state.collector_pn,
                expected_session_protocol=resolve_collector_cloud_session_protocol(c.collector_cloud_family),
                listen_port=run.local_port, upstream_host=host, upstream_port=port,
                output_path=trace, seed=seed, protected_retry_permit=permit,
            )
            await c._async_disconnect_collector_for_cloud_tool_route(reason="shadow_learning_protected_retry")
            await c._async_wait_for_shadow_learning_ready(trace_path=trace, timeout_seconds=75.0,
                min_collector_connection_sequence=0)
            state = replace(state, status="ready", updated_at=shadow_learning_session_timestamp())
            await c._async_save_shadow_learning_session_state(state)
        except BaseException:
            async def cleanup():
                actual = await c._async_active_shadow_learning_state(require_process=False)
                if actual is not None and actual.route_owner_id == state.route_owner_id:
                    try:
                        await c._runtime.async_stop_shadow_learning_route(owner_id=state.route_owner_id)
                    finally:
                        c._protected_learning_run = None
                        await c._async_save_shadow_learning_session_state(replace(actual, status="restore_failed",
                            last_restore_error="cloud_learning_hold_pending", updated_at=shadow_learning_session_timestamp()))
                c._protected_learning_run = None
                c._publish_cloud_learning_hold("cloud_learning_hold_pending")
            await c._run_finalization_shielded(cleanup)
            raise
        c._publish_tooling_values(shadow_learning_session_status="ready", shadow_learning_session_ready=True,
            shadow_learning_trace_path=str(trace), shadow_learning_session_id=state.route_owner_id)
        return {"status": "ready", "session_id": state.route_owner_id, "trace_path": str(trace),
                "collector_callback_endpoint": state.proxy_endpoint, "upstream_endpoint": state.upstream_endpoint,
                "write_response_mode": "exception", "restore_required": False, "protected_retry": True}
