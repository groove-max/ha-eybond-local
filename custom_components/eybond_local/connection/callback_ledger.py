"""Integration-wide ledger of outgoing collector callback triggers.

Every production path that asks a collector to dial back (the UDP
``set>server`` trigger) records the send here through the shared facade in
:mod:`..collector.discovery`. The ledger is intentionally process-global, like
the shared TCP listeners: a collector cannot tell WHICH config entry or flow
triggered it, so any trigger anywhere invalidates a concurrent behavioral
inbound verification. A monotonic generation makes that check cheap and
race-free: sample the generation before the restart, and if it changed before
the new session appeared, the reconnect proves nothing.

This module is pure bookkeeping. It never sends anything, never inspects
addresses, and must not import transport/onboarding code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic


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
