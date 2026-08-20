"""Config-entry ownership and Home Assistant event registrations."""

from __future__ import annotations

import asyncio
import errno
import logging
from typing import TYPE_CHECKING

from .collector.transport import CollectorListenerBindError
from .const import CONF_COLLECTOR_PN
from .integration_common import (
    ConfigEntryError,
    ConfigEntryNotReady,
    EVENT_HOMEASSISTANT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
    _log_abandoned_shutdown_result,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

logger = logging.getLogger(__name__)

_STOP_SHUTDOWN_TIMEOUT = 15.0
_TRANSIENT_LISTENER_BIND_ERRNOS = {
    errno.EADDRNOTAVAIL,
    errno.ENETUNREACH,
    errno.EHOSTUNREACH,
    errno.ENODEV,
}

def _register_entry_stop_shutdown(hass: HomeAssistant, entry: ConfigEntry, coordinator) -> None:
    """Stop the runtime explicitly when Home Assistant is shutting down."""

    async def _async_shutdown_on_stop(_event) -> None:
        # Bounded: the process exits right after HA stop, so an unfinished
        # network teardown dies with it anyway — but an unbounded await here
        # keeps this task pending through every shutdown stage (observed
        # hanging >60s on a shielded listener release). asyncio.wait, not
        # wait_for: wait_for would await the cancelled task, and the shielded
        # cleanup inside re-awaits its future on cancel — the hang would
        # simply move here.
        task = asyncio.ensure_future(coordinator.async_shutdown())
        done, pending = await asyncio.wait({task}, timeout=_STOP_SHUTDOWN_TIMEOUT)
        if pending:
            # The abandoned task still needs its result retrieved, or a late
            # failure surfaces as a contextless "exception was never
            # retrieved" during interpreter teardown.
            task.add_done_callback(_log_abandoned_shutdown_result)
            logger.warning(
                "EyeBond runtime shutdown for entry %s did not finish within %.0fs on Home Assistant stop; abandoning cleanup",
                entry.entry_id,
                _STOP_SHUTDOWN_TIMEOUT,
            )
            return
        try:
            task.result()
        except Exception:
            logger.exception("Failed to shut down EyeBond runtime for entry %s on Home Assistant stop", entry.entry_id)

    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_shutdown_on_stop)
    )


def _register_entry_callback_session_claim(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Establish this entry's PERMANENT ownership before the runtime starts.

    Ownership is durable-PN based (peer IP is never used). It makes the registry
    the single authority for "which entry owns which inbound session", so passive
    discovery does not re-publish a collector an entry already owns. Contract:
    this either returns having recorded ownership (or having nothing to own -- no
    registry, or a PN-less legacy entry), or it RAISES a typed setup error so the
    coordinator/runtime never starts without ownership:

    * a conflict with an in-flight config-flow verification of the same PN raises
      ``ConfigEntryNotReady`` (retryable -- setup succeeds once the flow completes
      its handoff or releases its claim);
    * a conflict with a DIFFERENT permanent entry raises ``ConfigEntryError``
      (fail-closed -- a genuine duplicate is not resolved by retrying).
    """

    from .passive_discovery import (
        get_callback_session_registry,
        get_passive_callback_discovery,
    )

    def _release_callback_session() -> None:
        discovery = get_passive_callback_discovery(hass)
        if discovery is not None:
            discovery.retire_entry_sessions(entry.entry_id)
        released_registry = get_callback_session_registry(hass)
        if released_registry is not None:
            released_registry.release(entry.entry_id)

    # Register the release hook unconditionally so a failed setup (or unload)
    # always frees whatever this entry claimed below.
    entry.async_on_unload(_release_callback_session)

    registry = get_callback_session_registry(hass)
    if registry is None:
        return
    collector_pn = str(entry.data.get(CONF_COLLECTOR_PN, "") or "").strip()
    if not collector_pn:
        # A PN-less legacy entry has no durable identity to own; it stays
        # identity_binding_required (surfaced in diagnostics) and is repaired via
        # the reconfigure flow. This is a distinct, explicit state -- not a
        # silent "run without ownership".
        return
    session_protocol = str(
        entry.options.get(
            "collector_session_protocol",
            entry.data.get("collector_session_protocol", ""),
        )
        or ""
    ).strip()
    try:
        # First try to complete a committed config-flow handoff for this PN (the
        # verification flow prepared it under its own unique attempt owner); that
        # transfers the already-owned session with no unowned window. If there is
        # no committed handoff (an already-configured entry after an HA restart
        # lost the in-memory claim), claim the durable identity directly by PN so
        # the next same-PN inbound session binds to it. Never by peer IP.
        if not registry.complete_handoff(collector_pn, entry.entry_id):
            registry.claim(
                entry.entry_id,
                collector_pn=collector_pn,
                session_protocol=session_protocol,
            )
    except ValueError as exc:
        # Fail closed: the runtime must NOT start without ownership. Ask the
        # registry who actually holds the identity (avoids parsing the message).
        conflicting_owner = registry.owner_for_pn(collector_pn)
        if conflicting_owner.startswith(
            ("callback_verification:", "strategy_verification:")
        ):
            # An in-flight config-flow verification holds the identity; retry.
            raise ConfigEntryNotReady(
                f"EyeBond collector {collector_pn} is being verified in a config "
                "flow; ownership not yet available"
            ) from exc
        # A different permanent entry already owns this identity (a duplicate).
        raise ConfigEntryError(
            f"EyeBond collector {collector_pn} is already owned by another entry "
            f"({conflicting_owner or 'unknown'})"
        ) from exc


def _register_entry_network_reconcile(hass: HomeAssistant, entry: ConfigEntry, coordinator) -> None:
    """Ask the runtime to re-check listener network state after HA/network events."""

    async def _async_reconcile_on_event(event) -> None:
        reconcile = getattr(coordinator, "async_reconcile_network", None)
        if reconcile is None:
            return
        reason = str(getattr(event, "event_type", "") or "homeassistant_started")
        try:
            await reconcile(reason=reason)
        except Exception:
            logger.exception(
                "Failed to reconcile EyeBond listener network state for entry %s after %s",
                entry.entry_id,
                reason,
            )

    # A one-time listener auto-removes itself when it fires. Registering its
    # unsub on async_on_unload then double-removes it on unload (HA logs
    # "Unable to remove unknown job listener ... list.remove(x): x not in
    # list"). Track the fired state and skip the redundant removal.
    started = {"fired": False}

    async def _async_reconcile_on_started(event) -> None:
        started["fired"] = True
        await _async_reconcile_on_event(event)

    _unsub_started = hass.bus.async_listen_once(
        EVENT_HOMEASSISTANT_STARTED, _async_reconcile_on_started
    )

    def _unsub_started_if_pending() -> None:
        if not started["fired"]:
            _unsub_started()

    entry.async_on_unload(_unsub_started_if_pending)
    async_listen = getattr(hass.bus, "async_listen", None)
    if async_listen is not None:
        entry.async_on_unload(
            async_listen("core_config_updated", _async_reconcile_on_event)
        )


def _is_transient_listener_bind_error(exc: CollectorListenerBindError) -> bool:
    """Return whether one listener bind failure should be retried by HA."""

    if exc.errno in _TRANSIENT_LISTENER_BIND_ERRNOS:
        return True
    if exc.errno == errno.EADDRINUSE:
        return False
    message = str(exc.error).lower()
    if "address already in use" in message:
        return False
    return "could not bind" in message or "cannot assign requested address" in message
