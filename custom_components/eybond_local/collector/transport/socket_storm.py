"""Reset extra callback sockets for one identified collector.

Identity and survivor choice live here. The shared listener reports a strong
full PN, runs the check again when quiet-redial grace ends, and runs it again
when a framed or AT exchange is recorded during an open cycle. It still does
not rebind the listen socket and does not reset a live survivor merely to open
a quiet window.
"""

from __future__ import annotations

import logging
import socket
import struct
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

HEALTHY_SECONDS = 15.0
STALE_SECONDS = 45.0
GRACE_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class StormSocket:
    """One live pending, parked, or routed callback socket."""

    session_id: str
    peer_ip: str
    strong_pn: str = ""
    last_framed_exchange_at: float | None = None
    writer: object | None = None
    routed_payload: bool = False
    accepted_at: float = 0.0


@dataclass(frozen=True, slots=True)
class StormAction:
    """Sockets to abort, and the warning reason when this step logs.

    ``hold_quiet`` keeps ``session_ids`` empty. ``grace_deadline`` is the
    monotonic time when that quiet grace ends. Callers must not reset anyone
    for that reason.
    """

    session_ids: tuple[str, ...] = ()
    reason: str = ""
    grace_deadline: float | None = None


@dataclass
class _Cycle:
    pn: str
    admitted_id: str = ""
    admitted_at: float = 0.0
    quiet_grace: bool = False
    warned: set[str] = field(default_factory=set)


def reset_tcp_writer(writer: object) -> None:
    """Abort one accepted stream with TCP RST.

    ``StreamWriter.close()`` finishes with FIN. ``transport.abort()`` drops
    the socket only after ``SO_LINGER`` is set to zero, which is what makes
    that close a RST. The listen socket is never passed here.
    """

    get_extra_info = getattr(writer, "get_extra_info", None)
    sock = get_extra_info("socket") if callable(get_extra_info) else None
    if sock is not None:
        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )
    transport = getattr(writer, "transport", None)
    abort = getattr(transport, "abort", None)
    if callable(abort):
        abort()
        return
    close = getattr(writer, "close", None)
    if callable(close):
        close()


class CallbackSocketStorm:
    """Keep one survivor in a storm set; RST extras; replace only a stale socket."""

    def __init__(self) -> None:
        self._cycles: dict[str, _Cycle] = {}

    def clear(self) -> None:
        self._cycles.clear()

    def consider(
        self,
        sockets: list[StormSocket] | tuple[StormSocket, ...],
        focus_session_id: str,
        now: float,
    ) -> StormAction:
        """Decide from the current live sockets. ``now`` is monotonic time."""

        focus = next(
            (item for item in sockets if item.session_id == focus_session_id),
            None,
        )
        if focus is None:
            return StormAction()
        group = _storm_group(focus, sockets)
        if len(group) < 2:
            return StormAction()
        cycle = self._ensure_cycle(group)
        keep, reason = _pick_survivor(group, cycle, focus, now)
        if reason == "hold_quiet":
            # Grace starts when this same-PN overlap is first seen. A later
            # redial must not inherit an older admission clock, and a redial
            # while quiet grace is open does not extend it.
            newest_accept = max(
                (
                    item.accepted_at
                    for item in group
                    if item.session_id != keep.session_id
                ),
                default=0.0,
            )
            if cycle.quiet_grace:
                restart = now >= cycle.admitted_at + GRACE_SECONDS
            else:
                restart = cycle.admitted_at < newest_accept
            if cycle.admitted_id != keep.session_id or restart:
                cycle.admitted_id = keep.session_id
                cycle.admitted_at = now
                cycle.quiet_grace = True
                cycle.warned.clear()
            return StormAction((), "hold_quiet", cycle.admitted_at + GRACE_SECONDS)
        reset_ids = tuple(
            sorted(item.session_id for item in group if item.session_id != keep.session_id)
        )
        if not reset_ids:
            return StormAction()
        if reason == "replace_stale" or cycle.admitted_id != keep.session_id:
            cycle.admitted_id = keep.session_id
            cycle.admitted_at = now
            cycle.quiet_grace = False
            cycle.warned.clear()
        self._warn(cycle, focus, reset_ids, reason)
        return StormAction(reset_ids, reason)

    def _ensure_cycle(self, group: list[StormSocket]) -> _Cycle:
        pn = next(item.strong_pn for item in group if item.strong_pn)
        cycle = self._cycles.get(pn)
        if cycle is None:
            cycle = _Cycle(pn=pn)
            self._cycles[pn] = cycle
        cycle.pn = pn
        return cycle

    def _warn(
        self,
        cycle: _Cycle,
        focus: StormSocket,
        closed_ids: tuple[str, ...],
        reason: str,
    ) -> None:
        if reason == "keep_candidate" and reason in cycle.warned:
            return
        if reason == "keep_candidate":
            cycle.warned.add(reason)
        pn = focus.strong_pn or "pn-unknown"
        logger.warning(
            "Callback socket storm pn=%s peer=%s closed_sessions=%s reason=%s",
            pn,
            focus.peer_ip,
            ",".join(closed_ids),
            reason,
        )


def _pick_survivor(
    group: list[StormSocket],
    cycle: _Cycle,
    focus: StormSocket,
    now: float,
) -> tuple[StormSocket, str]:
    """Choose the socket that may keep polling. Extras are reset."""

    recent = _recent(group, now, HEALTHY_SECONDS)
    if recent:
        return max(recent, key=lambda item: item.last_framed_exchange_at or 0.0), "keep_healthy"

    quiet = _quiet_arrivals(group)
    admitted = next(
        (item for item in group if item.session_id == cycle.admitted_id),
        None,
    )
    if quiet:
        newest = max(item.accepted_at for item in quiet)
        grace_end = cycle.admitted_at + GRACE_SECONDS
        if cycle.quiet_grace:
            new_overlap = min(item.accepted_at for item in quiet) >= grace_end
        else:
            new_overlap = cycle.admitted_at < newest
        if admitted is None or now < grace_end or new_overlap:
            return _quiet_incumbent(group, quiet, admitted), "hold_quiet"
    if admitted is not None and now < cycle.admitted_at + GRACE_SECONDS:
        exchange = admitted.last_framed_exchange_at
        # Past the healthy window, a later framed exchange wins. An incumbent
        # exchange still inside the stale window, and at or after the other
        # sockets' accept times, stays keep_healthy. Quiet incumbents are
        # handled above. A different focus replaces only a socket with no
        # such exchange.
        if exchange is not None and now - exchange > HEALTHY_SECONDS:
            newer = [
                item
                for item in group
                if item.session_id != admitted.session_id
                and item.last_framed_exchange_at is not None
                and item.last_framed_exchange_at > exchange
            ]
            if newer:
                return (
                    max(newer, key=lambda item: item.last_framed_exchange_at or 0.0),
                    "keep_healthy",
                )
            if any(
                item.session_id == admitted.session_id
                for item in _recent(group, now, STALE_SECONDS)
            ):
                return admitted, "keep_healthy"
            if focus.session_id != admitted.session_id:
                return focus, "replace_stale"
        return admitted, "keep_candidate"

    usable = _recent(group, now, STALE_SECONDS)
    if usable:
        return max(usable, key=lambda item: item.last_framed_exchange_at or 0.0), "keep_healthy"

    extras = [
        item
        for item in group
        if admitted is None or item.session_id != admitted.session_id
    ]
    if admitted is not None and extras:
        # Neither socket has an exchange time. Keep the live routed session
        # (payload or AT) instead of the pending socket that happens to be
        # first. An exchange that predates the newcomer still replaces below.
        if all(item.last_framed_exchange_at is None for item in group):
            routed = [item for item in group if item.routed_payload]
            if routed:
                return routed[0], "keep_candidate"
        keep = focus if any(item.session_id == focus.session_id for item in extras) else extras[0]
        return keep, "replace_stale"

    routed = [item for item in group if item.routed_payload]
    if routed:
        return routed[0], "keep_candidate"
    identified = [item for item in group if item.strong_pn]
    if identified:
        return identified[0], "keep_candidate"
    return group[0], "keep_candidate"


def _recent(group: list[StormSocket], now: float, window: float) -> list[StormSocket]:
    """Sockets whose framed exchange is inside ``window`` and still current.

    An exchange older than another socket's accept did not happen after that
    socket arrived, so it cannot make the incumbent the healthy survivor.
    """

    return [
        item
        for item in group
        if item.last_framed_exchange_at is not None
        and now - item.last_framed_exchange_at <= window
        and _exchange_since_peers(item, group)
    ]


def _exchange_since_peers(item: StormSocket, group: list[StormSocket]) -> bool:
    exchanged_at = item.last_framed_exchange_at
    if exchanged_at is None:
        return False
    return all(
        exchanged_at >= other.accepted_at
        for other in group
        if other.session_id != item.session_id
    )


def _quiet_arrivals(group: list[StormSocket]) -> list[StormSocket]:
    """Sockets no peer has framed since, and that arrived into a live overlap.

    A missing exchange does not count. A later accept does, and so does a
    peer exchange that is already older than this socket's accept time.
    """

    arrivals: list[StormSocket] = []
    for item in group:
        peers = [other for other in group if other.session_id != item.session_id]
        if not peers or _exchange_since_peers_any(item, peers):
            continue
        arrived_later = any(item.accepted_at > peer.accepted_at for peer in peers)
        exchange_predates = any(
            peer.last_framed_exchange_at is not None
            and peer.last_framed_exchange_at < item.accepted_at
            for peer in peers
        )
        if arrived_later or exchange_predates:
            arrivals.append(item)
    return arrivals


def _exchange_since_peers_any(item: StormSocket, peers: list[StormSocket]) -> bool:
    return any(
        peer.last_framed_exchange_at is not None
        and peer.last_framed_exchange_at >= item.accepted_at
        for peer in peers
    )


def _quiet_incumbent(
    group: list[StormSocket],
    quiet: list[StormSocket],
    admitted: StormSocket | None,
) -> StormSocket:
    quiet_ids = {item.session_id for item in quiet}
    if admitted is not None and admitted.session_id not in quiet_ids:
        return admitted
    incumbents = [item for item in group if item.session_id not in quiet_ids]
    if not incumbents:
        return group[0]
    return max(
        incumbents,
        key=lambda item: (item.last_framed_exchange_at or 0.0, item.accepted_at),
    )


def _storm_group(focus: StormSocket, sockets: list[StormSocket] | tuple[StormSocket, ...]) -> list[StormSocket]:
    """Sockets in ``focus``'s storm set.

    The same strong full PN is one set. A shared peer IP is not ownership.
    A PN-less dial stays out until it presents its own strong PN, including
    a dial accepted after a live routed payload on that IP. A different
    strong PN is a different collector, even behind one NAT. Arrival order
    is not evidence either way.
    """

    if not focus.strong_pn:
        return [focus]
    return [item for item in sockets if item.strong_pn == focus.strong_pn]
