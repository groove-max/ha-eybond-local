"""Exact-session collector cleanup when a config entry is permanently removed.

An entry unload and an entry removal are different lifecycle events.  A normal
unload must release runtime ownership without touching the collector.  A
permanent removal, however, must clear a volatile callback route before the old
socket can be mistaken for a durable inbound connection.

This module consumes an in-memory ticket captured while the entry still owned
the exact physical session.  It never resolves by peer address or by a PN-wide
session search: the temporary removal owner is pinned back to that exact
``session_id`` and the live negotiated handle decides whether reboot is
supported.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Final
import uuid

from ..collector.management import CollectorManagementUnsupportedError
from .callback_ledger import get_callback_trigger_ledger
from .recovery.verification import (
    ObservedSessionRestartChannel,
    SessionUnavailableError,
)
from .session_registry import (
    CallbackSessionRegistry,
    identity_source_is_strong,
    pn_is_same_identity,
)


REMOVAL_RESTART_CONFIRMED: Final = "removal_restart_confirmed"
REMOVAL_SESSION_UNAVAILABLE: Final = "removal_session_unavailable"
REMOVAL_SESSION_CONFLICT: Final = "removal_session_conflict"
REMOVAL_RESTART_UNSUPPORTED: Final = "removal_restart_unsupported"
REMOVAL_RESTART_FAILED: Final = "removal_restart_failed"
REMOVAL_DISCONNECT_NOT_OBSERVED: Final = "removal_disconnect_not_observed"


def _strict_token(value: object, *, field: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field}_must_be_str")
    if not value or value != value.strip():
        raise ValueError(f"{field}_must_be_normalized")
    return value


@dataclass(frozen=True, slots=True)
class CollectorRemovalSessionTicket:
    """Non-persisted capability for one entry-owned physical collector socket."""

    collector_pn: str
    identity_source: str
    session_id: str
    listener_host: str
    listener_port: int

    def __post_init__(self) -> None:
        pn = _strict_token(self.collector_pn, field="collector_pn")
        source = _strict_token(self.identity_source, field="identity_source")
        _strict_token(self.session_id, field="session_id")
        _strict_token(self.listener_host, field="listener_host")
        if not identity_source_is_strong(source):
            raise ValueError("identity_source_not_strong")
        if type(self.listener_port) is not int:
            raise TypeError("listener_port_must_be_int")
        if not 1 <= self.listener_port <= 65535:
            raise ValueError("listener_port_out_of_range")
        if not pn:
            raise ValueError("collector_pn_required")

    @property
    def inventory_key(self) -> str:
        """Return the listener-scoped physical-session key used by discovery."""

        return f"{self.listener_port}:{self.session_id}"


@dataclass(frozen=True, slots=True)
class CollectorRemovalFinalizationResult:
    """Typed, non-persisted outcome of the best-effort removal finalizer."""

    status: str
    restarted: bool = False
    disconnect_observed: bool = False


def _session_is_live(registry: CallbackSessionRegistry, session_id: str) -> bool:
    for session in registry.observed_sessions_per_socket():
        if session.session_id != session_id:
            continue
        return not session.state.startswith("closed")
    return False


async def _wait_for_exact_disconnect(
    registry: CallbackSessionRegistry,
    session_id: str,
    *,
    timeout: float,
) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, float(timeout))
    while _session_is_live(registry, session_id):
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(0.05, remaining))
    return True


async def _close_channel_cancellation_safe(channel: object) -> None:
    close = getattr(channel, "async_close", None)
    if not callable(close):
        return
    task = asyncio.create_task(close())
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


async def async_finalize_collector_entry_removal(
    ticket: CollectorRemovalSessionTicket,
    registry: CallbackSessionRegistry,
    *,
    disconnect_timeout: float = 5.0,
) -> CollectorRemovalFinalizationResult:
    """Restart exactly the ticketed socket and confirm that it disconnected.

    Failure is typed and leaves entry deletion to its caller.  Registry claims
    and transports are always released, including cancellation paths.  The
    discovery layer independently keeps the old exact session quarantined.
    """

    if type(ticket) is not CollectorRemovalSessionTicket:
        raise TypeError("ticket_type_invalid")
    if type(registry) is not CallbackSessionRegistry:
        raise TypeError("registry_type_invalid")

    # The restart must not race an in-process set>server sender. Otherwise a
    # scan could write a fresh volatile callback target immediately after the
    # reset and recreate the exact stale-session condition removal is meant to
    # eliminate. This is the same process-wide silence gate used by inbound
    # recovery; it sends no trigger and creates no recovery proof.
    async with get_callback_trigger_ledger().inhibit_callback_triggers():
        return await _async_finalize_without_callback_triggers(
            ticket,
            registry,
            disconnect_timeout=disconnect_timeout,
        )


async def _async_finalize_without_callback_triggers(
    ticket: CollectorRemovalSessionTicket,
    registry: CallbackSessionRegistry,
    *,
    disconnect_timeout: float,
) -> CollectorRemovalFinalizationResult:
    """Run the exact-session finalizer inside the process-wide silence window."""

    owner = f"entry_removal:{uuid.uuid4()}"
    channel: ObservedSessionRestartChannel | None = None
    try:
        try:
            registry.claim_identity(owner, ticket.collector_pn)
            if not registry.pin_owner_claim_to_session(owner, ticket.session_id):
                return CollectorRemovalFinalizationResult(
                    status=REMOVAL_SESSION_UNAVAILABLE
                )
            certified_pn = registry.certify_owner_reconnected_session(
                owner, ticket.session_id
            )
            if not certified_pn or not pn_is_same_identity(
                certified_pn, ticket.collector_pn
            ):
                return CollectorRemovalFinalizationResult(
                    status=REMOVAL_SESSION_UNAVAILABLE
                )
            handle = registry.session_handle_for_owned_session(
                owner, ticket.session_id
            )
            if handle is None:
                return CollectorRemovalFinalizationResult(
                    status=REMOVAL_SESSION_UNAVAILABLE
                )
        except ValueError:
            return CollectorRemovalFinalizationResult(status=REMOVAL_SESSION_CONFLICT)

        channel = ObservedSessionRestartChannel(
            host=ticket.listener_host,
            port=ticket.listener_port,
            collector_pn=certified_pn,
            session_id=ticket.session_id,
            session_id_provider=lambda: registry.claimed_session_id(owner),
            handle_provider=lambda: registry.session_handle_for_owned_session(
                owner, ticket.session_id
            ),
        )
        try:
            await channel.async_send_restart()
        except CollectorManagementUnsupportedError:
            return CollectorRemovalFinalizationResult(
                status=REMOVAL_RESTART_UNSUPPORTED
            )
        except SessionUnavailableError:
            return CollectorRemovalFinalizationResult(
                status=REMOVAL_SESSION_UNAVAILABLE
            )
        except Exception:
            return CollectorRemovalFinalizationResult(status=REMOVAL_RESTART_FAILED)

        disconnected = await _wait_for_exact_disconnect(
            registry,
            ticket.session_id,
            timeout=disconnect_timeout,
        )
        if not disconnected:
            return CollectorRemovalFinalizationResult(
                status=REMOVAL_DISCONNECT_NOT_OBSERVED,
                restarted=True,
            )
        return CollectorRemovalFinalizationResult(
            status=REMOVAL_RESTART_CONFIRMED,
            restarted=True,
            disconnect_observed=True,
        )
    finally:
        try:
            if channel is not None:
                await _close_channel_cancellation_safe(channel)
        finally:
            registry.release(owner)


__all__ = [
    "CollectorRemovalFinalizationResult",
    "CollectorRemovalSessionTicket",
    "REMOVAL_DISCONNECT_NOT_OBSERVED",
    "REMOVAL_RESTART_CONFIRMED",
    "REMOVAL_RESTART_FAILED",
    "REMOVAL_RESTART_UNSUPPORTED",
    "REMOVAL_SESSION_CONFLICT",
    "REMOVAL_SESSION_UNAVAILABLE",
    "async_finalize_collector_entry_removal",
]
