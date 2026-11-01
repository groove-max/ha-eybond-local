"""Reset extra callback sockets for one identified collector.

Identity and survivor choice live here. The shared listener only reports an
accept or a strong full PN and applies the returned resets. It does not rebind
its listen socket, and it does not reset a live survivor to open a quiet window.
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
    """Sockets to abort, and the warning reason when this step logs."""

    session_ids: tuple[str, ...] = ()
    reason: str = ""


@dataclass
class _Cycle:
    pn: str
    admitted_id: str = ""
    admitted_at: float = 0.0
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
        reset_ids = tuple(
            sorted(item.session_id for item in group if item.session_id != keep.session_id)
        )
        if not reset_ids:
            return StormAction()
        if reason == "replace_stale" or cycle.admitted_id != keep.session_id:
            cycle.admitted_id = keep.session_id
            cycle.admitted_at = now
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

    admitted = next(
        (item for item in group if item.session_id == cycle.admitted_id),
        None,
    )
    if admitted is not None and now < cycle.admitted_at + GRACE_SECONDS:
        exchange = admitted.last_framed_exchange_at
        # A dead admitted socket past the healthy window must not beat a
        # replacement that arrived during grace. Never-exchanged candidates
        # still keep the admitted socket.
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
    return [
        item
        for item in group
        if item.last_framed_exchange_at is not None
        and now - item.last_framed_exchange_at <= window
    ]


def _is_sibling(
    item: StormSocket,
    peers: set[str],
    routed_since: float | None,
) -> bool:
    """A later PN-less dial on the IP of a live routed payload connection.

    The routed connection itself is not a sibling of a later identification.
    A PN-less socket accepted before that routed payload is a different
    collector that has not identified yet, so it stays out of the storm set.
    """

    if not (
        not item.strong_pn
        and not item.routed_payload
        and bool(item.peer_ip)
        and item.peer_ip in peers
    ):
        return False
    if routed_since and item.accepted_at and item.accepted_at < routed_since:
        return False
    return True


def _routed_peers(sockets: list[StormSocket] | tuple[StormSocket, ...], pn: str) -> set[str]:
    return {
        item.peer_ip
        for item in sockets
        if item.strong_pn == pn and item.routed_payload and item.peer_ip
    }


def _routed_since(
    sockets: list[StormSocket] | tuple[StormSocket, ...],
    pn: str,
) -> dict[str, float]:
    """Earliest accept time of a live routed payload socket, by peer IP."""

    since: dict[str, float] = {}
    for item in sockets:
        if not (
            item.strong_pn == pn
            and item.routed_payload
            and item.peer_ip
            and item.accepted_at
        ):
            continue
        current = since.get(item.peer_ip)
        if current is None or item.accepted_at < current:
            since[item.peer_ip] = item.accepted_at
    return since


def _storm_group(focus: StormSocket, sockets: list[StormSocket] | tuple[StormSocket, ...]) -> list[StormSocket]:
    """Sockets in ``focus``'s storm set.

    The same strong PN is always one set. A PN-less sibling joins only when
    that IP already has a live routed payload connection for the PN, and the
    PN-less socket was not already accepted before that payload session.
    """

    if focus.strong_pn:
        peers = _routed_peers(sockets, focus.strong_pn)
        since = _routed_since(sockets, focus.strong_pn)
        return [
            item
            for item in sockets
            if item.strong_pn == focus.strong_pn
            or _is_sibling(item, peers, since.get(item.peer_ip))
        ]
    identified = [
        item
        for item in sockets
        if item.strong_pn and item.routed_payload and item.peer_ip == focus.peer_ip
    ]
    pns = {item.strong_pn for item in identified}
    if len(pns) != 1:
        return [focus]
    pn = next(iter(pns))
    peers = _routed_peers(sockets, pn)
    since = _routed_since(sockets, pn)
    return [
        item
        for item in sockets
        if item.strong_pn == pn or _is_sibling(item, peers, since.get(item.peer_ip))
    ]
