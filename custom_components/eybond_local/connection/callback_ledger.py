"""Integration-wide ledger of outgoing collector callback triggers.

Every production path that asks a collector to dial back (the UDP
``set>server`` trigger) records the send here through the shared facade in
:mod:`..collector.discovery`. The ledger is intentionally process-global, like
the shared TCP listeners: a collector cannot tell WHICH config entry or flow
triggered it. Behavioral inbound verification therefore owns a process-wide
no-trigger window; the monotonic generation remains the fail-closed proof that
no uncoordinated/legacy sender changed that fact before reconnect.

This module is pure bookkeeping. It never sends anything, never inspects
addresses, and must not import transport/onboarding code.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from threading import Event, Lock
from time import monotonic
from typing import AsyncIterator, Iterator


class CallbackTriggerInhibitedError(RuntimeError):
    """A behavioral inbound verification currently owns trigger silence."""


def _initially_set_event() -> Event:
    event = Event()
    event.set()
    return event


@dataclass(slots=True)
class CallbackTriggerRecord:
    """One recorded callback-trigger send (diagnostic provenance only)."""

    generation: int
    target: str = ""
    source: str = ""
    monotonic_at: float = 0.0


@dataclass(slots=True)
class CallbackTriggerLedger:
    """Monotonic record of every callback trigger the integration sends."""

    _generation: int = 0
    _last: CallbackTriggerRecord | None = None
    _history: list[CallbackTriggerRecord] = field(default_factory=list)
    _history_limit: int = 20
    _state_lock: Lock = field(default_factory=Lock, repr=False)
    _trigger_sends_drained: Event = field(
        default_factory=_initially_set_event,
        repr=False,
    )
    _active_trigger_sends: int = 0
    _trigger_inhibitors: int = 0

    @contextmanager
    def callback_send_scope(self) -> Iterator[None]:
        """Own one physical callback-trigger send or refuse it atomically.

        Behavioral inbound verification must observe a reboot/reconnect window
        in which no ``set>server`` datagram can influence the collector.  The
        guard is process-wide for the same reason as the ledger itself: before
        durable PN evidence exists, no address, entry, or session heuristic can
        safely decide whether two attempts refer to the same collector.

        The scope covers the physical send, not the later identity/detection
        work.  A verifier waits for an already-started send to finish before it
        restarts the collector; new sends fail immediately while verification
        owns the silence window.
        """

        with self._state_lock:
            if self._trigger_inhibitors:
                raise CallbackTriggerInhibitedError(
                    "callback_trigger_inhibited_by_inbound_verification"
                )
            if not self._active_trigger_sends:
                self._trigger_sends_drained.clear()
            self._active_trigger_sends += 1
        try:
            yield
        finally:
            with self._state_lock:
                self._active_trigger_sends = max(
                    0, self._active_trigger_sends - 1
                )
                if not self._active_trigger_sends:
                    self._trigger_sends_drained.set()

    @asynccontextmanager
    async def inhibit_callback_triggers(self) -> AsyncIterator[None]:
        """Hold a process-wide no-callback window for inbound verification.

        Multiple inbound verifiers may coexist: none of them sends callback
        triggers, and each owns one inhibitor reference.  The first verifier
        blocks new sends immediately, then waits only for a physical send that
        had already begun.  Polling uses no loop-bound global asyncio primitive,
        keeping the process-global ledger safe across isolated test loops.
        """

        with self._state_lock:
            self._trigger_inhibitors += 1
        try:
            await asyncio.to_thread(self._trigger_sends_drained.wait)
            yield
        finally:
            with self._state_lock:
                self._trigger_inhibitors = max(0, self._trigger_inhibitors - 1)

    def record(self, *, target: str = "", source: str = "") -> int:
        """Record one outgoing callback trigger; returns the new generation."""

        self._generation += 1
        entry = CallbackTriggerRecord(
            generation=self._generation,
            target=str(target or ""),
            source=str(source or ""),
            monotonic_at=monotonic(),
        )
        self._last = entry
        self._history.append(entry)
        del self._history[: -self._history_limit]
        return self._generation

    def snapshot_generation(self) -> int:
        """Return the current generation (changes on every recorded trigger)."""

        return self._generation

    @property
    def last_record(self) -> CallbackTriggerRecord | None:
        return self._last

    def recent_records(self) -> tuple[CallbackTriggerRecord, ...]:
        return tuple(self._history)


_LEDGER = CallbackTriggerLedger()


def get_callback_trigger_ledger() -> CallbackTriggerLedger:
    """Return the process-wide callback trigger ledger."""

    return _LEDGER
