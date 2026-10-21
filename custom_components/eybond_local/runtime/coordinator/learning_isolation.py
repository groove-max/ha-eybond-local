"""Permanent local handoff prerequisite for a classic protected learning run.

Runs under the existing cloud-tool preparation and endpoint-operation lease.
Reuses the controlled-reset verifier; does not change the user's strategy or
reload the entry. No receipt from disk can resume cloud command dispatch.
"""

import logging
from dataclasses import dataclass

from ...connection.cloud_learning_protection import CloudLearningHold
from ...connection.recovery.verification import ObservedSessionRestartChannel
from ...connection.strategy_transition import async_run_strategy_transition
from ...const import CONNECTION_STRATEGY_INBOUND
from ...support.shadow_learning.session import shadow_learning_session_timestamp
from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY
from ...telemetry import TelemetryFreshness
from .learning_route import learning_listener_port

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProtectedLearningRun:
    """Process-local capability: never reconstruct this after restart."""

    entry_id: str
    route_owner_id: str
    collector_pn: str
    local_endpoint: str
    local_port: int
    source: str
    hold: CloudLearningHold | None = None


async def async_prepare_local_learning_route(coordinator, state) -> ProtectedLearningRun:
    """Prove reset/reconnect without UDP, endpoint readback and local runtime IO."""

    from ...collector.silent_session_probe import SilentSessionIdentityProbeChannel
    from ...passive_discovery import get_callback_session_registry

    c = coordinator
    registry = get_callback_session_registry(c.hass)
    entry = c.config_entry.entry_id
    port = learning_listener_port(c)
    if registry is None or state.entry_id != entry or state.collector_pn != c.smartess_collector_pn:
        raise RuntimeError("cloud_learning_hold_local_identity_unavailable")
    sid = await c._async_prepare_strategy_transition_management_session(
        registry=registry, entry_id=entry, target_strategy=CONNECTION_STRATEGY_INBOUND,
        timeout=DEFAULT_ONBOARDING_TIMEOUT_POLICY.callback_recovery_session_wait,
    )
    if not sid:
        raise RuntimeError("cloud_learning_hold_local_session_unavailable")
    initial = registry.certify_permanent_owned_session(entry, sid)
    if initial is None or initial.collector_pn != state.collector_pn:
        raise RuntimeError("cloud_learning_hold_local_identity_unavailable")

    def channel(session_id):
        handle = registry.session_handle_for_owned_session(entry, session_id)
        if handle is None:
            raise RuntimeError("cloud_learning_hold_local_session_changed")
        return ObservedSessionRestartChannel(
            host=c._runtime.listener_bind_host or "0.0.0.0", port=handle.listener_port,
            collector_pn=state.collector_pn, session_id=session_id,
            session_id_provider=lambda: session_id,
            handle_provider=lambda: registry.session_handle_for_owned_session(entry, session_id),
        )

    async def rollback(endpoint):
        # The caller persisted exact original + local target BEFORE entering.
        persisted = await c._async_active_shadow_learning_state(require_process=False)
        if persisted != state or endpoint != state.original_endpoint:
            return "cloud_learning_hold_local_record_changed"
        await c._async_require_cloud_learning_unheld()
        return ""

    async def commit(_updates, terminal, _options):
        # No strategy-axis write/reload: this is a temporary learning operation.
        proof = terminal.inbound_proof
        current = registry.claimed_session_id(entry)
        certification = registry.certify_permanent_owned_session(entry, current)
        if (
            proof is None or terminal.collector_pn != state.collector_pn
            or certification is None or certification.collector_pn != state.collector_pn
        ):
            return "cloud_learning_hold_local_proof_invalid"
        activated = await c._runtime.async_activate_claimed_session(
            expected_session_id=current, timeout=5.0,
        )
        if not activated or not registry.reverify_permanent_owned_session(certification):
            return "cloud_learning_hold_local_activation_failed"
        endpoint = await c._runtime.async_get_collector_server_endpoint_state(require_heartbeat=False)
        if (
            endpoint.get("reboot_required") not in (None, "", "0", False)
            or endpoint.get("apply_required") not in (None, "", "0", False)
            or c._endpoint_effective_parts(endpoint.get("current_endpoint", ""))
            != c._endpoint_effective_parts(state.proxy_endpoint)
        ):
            return "cloud_learning_hold_local_endpoint_unverified"
        snapshot = await c._runtime.async_refresh()
        fresh_payload = any(
            point.freshness is TelemetryFreshness.FRESH and point.value is not None
            for point in snapshot.telemetry.points
        )
        still_owned = registry.reverify_permanent_owned_session(certification)
        if (
            not snapshot.connected or snapshot.last_error or snapshot.inverter is None
            or not fresh_payload or not still_owned
        ):
            _LOGGER.warning(
                "Local learning payload check failed entry=%s connected=%s "
                "read_error=%s inverter_identified=%s fresh_payload=%s same_session=%s",
                entry, snapshot.connected, bool(snapshot.last_error),
                snapshot.inverter is not None, fresh_payload, still_owned,
            )
            return "cloud_learning_hold_local_payload_unverified"
        return ""

    probe = SilentSessionIdentityProbeChannel(host=c._runtime.listener_bind_host or "0.0.0.0", port=port)
    try:
        await probe.async_open()
        result = await async_run_strategy_transition(
            target_strategy=CONNECTION_STRATEGY_INBOUND, current_strategy=c.connection_strategy,
            collector_pn=state.collector_pn, owner_id=entry, registry=registry,
            claimed_session_id=lambda: sid, clock=shadow_learning_session_timestamp,
            commit=commit, inbound_endpoint=state.proxy_endpoint,
            endpoint_needs_write=state.restore_required, route_metadata_needs_reverification=True,
            management_channel_factory=channel, prepare_listener=c._runtime.async_ensure_callback_listener,
            local_listener_port=port, expected_inbound_listener_port=port,
            current_external_endpoint=state.original_endpoint,
            persist_inbound_rollback_endpoint=rollback, silent_session_probe=probe,
        )
        if not result.success:
            # This is the transition engine's local reason, not cloud HTTP
            # exception text (which may contain a signed request URL).
            _LOGGER.warning(
                "Local learning handoff failed entry=%s reason=%s",
                entry, result.failure_reason,
            )
            raise RuntimeError("cloud_learning_hold_local_unverified:" + result.failure_reason)
    finally:
        await probe.async_close()
    return ProtectedLearningRun(entry, state.route_owner_id, state.collector_pn,
                                state.proxy_endpoint, port, state.cloud_source)
