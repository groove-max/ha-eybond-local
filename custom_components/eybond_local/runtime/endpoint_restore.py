"""Bounded, read-first reconciliation of a saved collector endpoint.

The caller owns the endpoint transaction and excludes polling. All observations
come from live PN-owned runtime management, never a metadata cache. This proves
collector configuration, not the availability of the manufacturer's cloud.
"""

from __future__ import annotations

from ..collector.management import CollectorManagementTransportError
from ..collector_endpoint import resolve_collector_server_endpoint


_TRANSPORT_ERRORS = (CollectorManagementTransportError, TimeoutError, OSError, EOFError)


async def restore_collector_endpoint(
    runtime, endpoint: str, *, timeout: float, cloud_family: str = "",
) -> str:
    """Reconcile once; never blindly resend a possibly delivered endpoint write."""

    def resolve(value: str):
        return resolve_collector_server_endpoint(
            value, cloud_family=cloud_family,
            require_explicit_port=False, require_explicit_protocol=False,
        )

    expected = resolve(endpoint)

    async def reconnect():
        await runtime.async_disconnect_collector_connections(
            reason="collector_endpoint_restore_verification",
        )

    async def read():
        state = await runtime.async_get_collector_server_endpoint_state(
            timeout=timeout, require_heartbeat=False,
        )
        observed = state.get("current_endpoint") if type(state) is dict else None
        if type(observed) is not str or not observed or observed != observed.strip():
            raise RuntimeError("restore_live_endpoint_unavailable")
        route = resolve(observed)
        pending = state.get("reboot_required", "")
        if type(pending) is not str or pending not in {"", "0", "1"}:
            raise RuntimeError("restore_apply_state_invalid")
        return observed, route == expected, pending

    try:
        observed, matches, pending = await read()
    except _TRANSPORT_ERRORS:
        # One new session/read is safe; no setting has been sent at this point.
        await reconnect()
        observed, matches, pending = await read()

    if matches and pending == "0":
        return observed

    apply_confirmed = False
    try:
        if matches:
            # Already staged or no apply-status facility (e.g. AT): apply the
            # existing endpoint without repeating its write.
            result = await runtime.async_apply_collector_changes(
                timeout=timeout, require_heartbeat=False,
            )
            apply_confirmed = type(result) is dict and result.get("status") == "applied"
        else:
            result = await runtime.async_set_collector_server_endpoint(
                endpoint, apply_changes=True, timeout=timeout, require_heartbeat=False,
            )
            apply_confirmed = type(result) is dict and result.get("apply_performed") is True
    except _TRANSPORT_ERRORS:
        # A lost acknowledgement/readback is an unknown outcome, not permission
        # to resend. Independent readback may prove the operation completed.
        pass

    await reconnect()
    observed, matches, pending = await read()
    if not matches:
        raise RuntimeError("restore_live_endpoint_mismatch")
    if pending == "1" or (pending == "" and not apply_confirmed):
        raise RuntimeError("restore_apply_unconfirmed")
    return observed
