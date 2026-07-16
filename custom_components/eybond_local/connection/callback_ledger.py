"""Integration-wide ledger of outgoing collector callback triggers.

Every production path that asks a collector to dial back (the UDP
``set>server`` trigger) records the send here through the shared facade in
:mod:`..collector.discovery`. The ledger is intentionally process-global, like
the shared TCP listeners: a collector cannot tell WHICH config entry or flow
triggered it. Behavioral inbound verification therefore owns a process-wide
no-trigger window; the monotonic generation remains the fail-closed proof that
no uncoordinated/legacy sender changed that fact before reconnect.

Scope of the guarantee -- honestly: PROCESS-LEVEL ONLY. Every in-process
sender funnels through ``callback_send_scope`` (the one choke point the
causality lease and the inhibitor gate), but a trigger sent from OUTSIDE this
Home Assistant process -- the ``collector_cloud_proxy`` support CLI, the
SmartESS app, another HA instance, an operator's script -- is invisible to
this ledger until its effect (a session) appears. What each layer can and
cannot exclude:

* the causality lease excludes only IN-PROCESS senders from the window;
* the session baseline excludes only sessions that ALREADY existed
  (old session ids can never be the answer to a later trigger);
* PN matching excludes only a DIFFERENT identity answering.

None of those layers can tell an EXTERNAL trigger aimed at the SAME collector
apart from ours: a session of the expected PN appearing after our baseline is
indistinguishable whether our datagram or an outside one caused it. That is
why a certified callback identity outcome proves the session<->PN binding and
nothing about trigger->session causality beyond this process -- and why it
must never be recorded as recovery evidence. No heuristic for detecting
external triggers belongs here; the honest boundary is the documentation.

This module is pure bookkeeping. It never sends anything, never inspects
addresses, and must not import transport/onboarding code.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Event, Lock
from time import monotonic
from typing import AsyncIterator, Iterator

# The attempt that owns the code currently running. Every send recorded while
# this is set is attributed to that attempt -- including sends from tasks the
# attempt spawns, because contextvars propagate into child tasks. A concurrent
# attempt runs in its own task with its own context, so it can never be credited
# with our trigger, nor we with its. This is what makes "was it MY trigger?"
# answerable without a global generation delta, which any other flow or the
# runtime could pollute.
_CURRENT_ATTEMPT: ContextVar[str] = ContextVar(
    "eybond_callback_attempt_id", default=""
)

# How often a queued attempt re-tries the lease. Short enough to be invisible
# next to a collector dial-in, long enough not to spin.
_LEASE_POLL_INTERVAL = 0.02


class CallbackTriggerInhibitedError(RuntimeError):
    """A callback trigger may not be sent right now.

    The base of every "refused to send" condition, so existing senders that
    already handle inhibition keep degrading exactly as they did.
    """


class CallbackTriggerLeaseHeldError(CallbackTriggerInhibitedError):
    """Another attempt holds the exclusive callback causality lease.

    The collector's wire carries NO correlation token: a ``set>server`` datagram
    and the TCP session it eventually causes share nothing an observer can match
    on, and peer IP/hostname/endpoint are never identity. Causality can therefore
    only be established by EXCLUSION -- while one attempt's window is open,
    nobody else may put a trigger on the wire, or a session appearing in that
    window stops being attributable to anyone.
    """


class CallbackCausalityBusyError(RuntimeError):
    """The exclusive causality lease could not be acquired in time."""


@dataclass(slots=True)
class CallbackAttempt:
    """Per-attempt trigger attribution for exactly one callback attempt.

    ``own_sends`` counts triggers this attempt caused; ``foreign_sends`` counts
    triggers anyone else recorded while it was open. The two are kept apart on
    purpose: "my trigger never went out" and "somebody else also triggered" are
    different failures with different honest names, and the old global
    generation delta could not tell them apart.
    """

    attempt_id: str
    own_sends: int = 0
    foreign_sends: int = 0

    @property
    def observed_sends(self) -> int:
        """Total triggers recorded during this attempt, from any source."""

        return self.own_sends + self.foreign_sends


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
    # Which attempt caused this send ("" = no attempt owned it: runtime, a
    # legacy/uncoordinated sender, or an operator tool).
    attempt_id: str = ""


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
    _attempts: dict[str, CallbackAttempt] = field(default_factory=dict)
    # THE exclusive causality lease. The owner string IS the lease -- there is no
    # separate lock to fall out of step with it. Both live under _state_lock, the
    # same mutex callback_send_scope reads them with, so "lease acquired" and
    # "owner published" are ONE atomic transition and no sender can ever observe
    # a held-but-unowned lease. A plain threading mutex on purpose: this module is
    # process-global and outlives isolated test loops, so no loop-bound asyncio
    # primitive may appear here.
    _causality_owner: str = ""

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
            # THE choke point. Every production sender reaches the wire through
            # this scope, so refusing here is what makes the lease exclusive
            # without every sender having to know the lease exists: manual,
            # pending, reconfigure, onboarding detection, management probes and
            # the runtime one-shot are all covered by one gate. A sender inside
            # the owning attempt's context passes; anyone else is refused now and
            # retries later, rather than silently poisoning the open window.
            owner = self._causality_owner
            if owner and _CURRENT_ATTEMPT.get() != owner:
                raise CallbackTriggerLeaseHeldError(
                    f"callback_trigger_blocked_by_attempt:{owner}"
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

    def _try_acquire_causality(self, attempt_id: str) -> CallbackAttempt | None:
        """Take the lease if it is free. Never blocks; atomic with publication.

        Returns the attempt on success, ``None`` if somebody else holds it. The
        critical section is a handful of instructions under the same mutex the
        send gate uses, so running it on the event loop cannot stall it -- and
        crucially there is no window between taking the lease and being visible
        as its owner.

        A duplicate id is checked FIRST and fails fast. Ordering matters: behind
        the "is it free?" gate the duplicate check was unreachable in the case
        that actually matters -- id A re-entering while A still holds the lease --
        and the caller sat waiting for a lease it could never be granted until it
        timed out with a misleading "busy".
        """

        with self._state_lock:
            if attempt_id in self._attempts:
                raise ValueError(f"callback_attempt_duplicate:{attempt_id}")
            if self._causality_owner:
                return None
            attempt = CallbackAttempt(attempt_id=attempt_id)
            self._causality_owner = attempt_id
            self._attempts[attempt_id] = attempt
            return attempt

    def _release_causality(self, attempt_id: str) -> None:
        """Drop the lease. Clearing the owner IS the release -- one transition."""

        with self._state_lock:
            if self._causality_owner == attempt_id:
                self._causality_owner = ""
            self._attempts.pop(attempt_id, None)

    @asynccontextmanager
    async def causality_lease(
        self,
        attempt_id: str,
        *,
        timeout: float = 30.0,
    ) -> AsyncIterator[CallbackAttempt]:
        """Hold the EXCLUSIVE right to establish callback causality.

        Acquire this BEFORE the baseline and hold it until the attempt reaches a
        terminal outcome (or its handoff is prepared). While it is held, nobody
        else may put a trigger on the wire -- see ``callback_send_scope``.

        Why exclusive rather than per-attempt accounting: counting our own sends
        cannot establish causality, because a trigger recorded just BEFORE our
        window opened can still cause a session that arrives INSIDE it. Nothing
        on the wire ties the datagram to the session it produces, so overlapping
        windows are fundamentally ambiguous no matter how carefully each side
        counts. Serializing removes the ambiguity instead of detecting it: two
        correct attempts now run one after another and BOTH succeed, where before
        both failed with interference.

        The exclusion is IN-PROCESS only (see the module docstring): a trigger
        from outside this Home Assistant process can still land a same-PN
        session inside the held window and is indistinguishable from ours.
        Holding this lease therefore supports an IDENTITY certification, never
        a recovery-causality claim.

        Waiting is done off the event loop so this stays loop-agnostic; the wait
        is bounded so a caller queues rather than hangs.
        """

        attempt_id = str(attempt_id or "").strip()
        if not attempt_id:
            raise ValueError("attempt_id_required")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout))
        attempt: CallbackAttempt | None = None
        try:
            while True:
                # Non-blocking try, then yield. There is deliberately NO worker
                # thread parked on a blocking acquire: one could win the lease
                # AFTER its waiter was cancelled and strand it forever. Here
                # cancellation can only land on the sleep below, where we hold
                # nothing at all, and between a successful try and `attempt`
                # being bound there is no await -- so the lease can never be held
                # by a coroutine that has already gone away.
                attempt = self._try_acquire_causality(attempt_id)
                if attempt is not None:
                    break
                if loop.time() >= deadline:
                    raise CallbackCausalityBusyError("callback_causality_lease_busy")
                await asyncio.sleep(_LEASE_POLL_INTERVAL)
            token = _CURRENT_ATTEMPT.set(attempt_id)
            try:
                yield attempt
            finally:
                _CURRENT_ATTEMPT.reset(token)
        finally:
            # Success, error, or cancellation inside the scope: the owner and the
            # lease go together, always.
            if attempt is not None:
                self._release_causality(attempt_id)

    def causality_owner(self) -> str:
        """Return the attempt id currently holding the lease ("" = free)."""

        with self._state_lock:
            return self._causality_owner

    def record(self, *, target: str = "", source: str = "", attempt_id: str | None = None) -> int:
        """Record one outgoing callback trigger; returns the new generation.

        The send is attributed to the attempt that is currently open in this
        context (or to ``attempt_id`` when a caller states it explicitly). Every
        OTHER open attempt is told a foreign trigger fired during its window.
        """

        owner = (
            _CURRENT_ATTEMPT.get() if attempt_id is None else str(attempt_id or "").strip()
        )
        with self._state_lock:
            self._generation += 1
            entry = CallbackTriggerRecord(
                generation=self._generation,
                target=str(target or ""),
                source=str(source or ""),
                monotonic_at=monotonic(),
                attempt_id=owner,
            )
            self._last = entry
            self._history.append(entry)
            del self._history[: -self._history_limit]
            for attempt_key, attempt in self._attempts.items():
                if owner and attempt_key == owner:
                    attempt.own_sends += 1
                else:
                    attempt.foreign_sends += 1
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
