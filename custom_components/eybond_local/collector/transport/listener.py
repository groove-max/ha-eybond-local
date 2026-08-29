"""The single shared collector-listener and session-inventory authority."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from time import monotonic
from typing import Callable

from ...collector_identity import (
    identity_source_is_strong,
    pn_is_same_identity,
    prefer_identity_source,
    reconcile_pn,
    validated_collector_pn,
)
from ..identity_probe import (
    IdentityProbeRequest,
    build_identity_probe_request,
    parse_identity_probe_response,
)
from ..protocol import HEADER_SIZE, decode_header
from .common import (
    CollectorListenerBindError,
    _AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN,
    _classify_initial_protocol_shape,
    _close_writer_bounded,
    _collector_pn_from_initial_chunk,
    _finish_cleanup_on_cancel,
    _is_default_broadcast_alias_candidate,
    _is_hairpin_alias_candidate,
    _is_ipv4_broadcast_placeholder,
    _mask_identity_token,
    _seed_connection_collector_pn,
    _spawn_tracked_task,
)
from .connections import _CollectorAtConnection, _CollectorConnection

logger = logging.getLogger(__name__)


_TERMINAL_SESSION_STATES = frozenset(
    {
        "parked_evicted",
        "parked_expired",
        "parked_peer_closed",
        "parked_read_failed",
    }
)


def _session_state_is_terminal(state: str) -> bool:
    """Return whether one socket-scoped inventory state is irreversible."""

    normalized = str(state or "").strip()
    return normalized.startswith("closed") or normalized in _TERMINAL_SESSION_STATES

@dataclass(slots=True)
class _PendingCollectorSocket:
    remote_ip: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    session_id: str = ""
    remote_port: int | None = None
    sniff_task: asyncio.Task[None] | None = None
    initial_bytes: bytes = b""
    parked: bool = False


@dataclass(slots=True)
class _CollectorSessionInventoryEntry:
    session_id: str
    remote_ip: str
    remote_port: int | None
    state: str = "pending"
    protocol_shape: str = "unknown"
    first_bytes_len: int = 0
    first_bytes_prefix_hex: str = ""
    collector_pn: str = ""
    collector_identity_source: str = ""
    collector_identity_sources: set[str] = field(default_factory=set)
    observed_protocol_shapes: set[str] = field(default_factory=set)

    def diagnostics(self) -> dict[str, object]:
        result: dict[str, object] = {
            "session_id": self.session_id,
            "peer_ip": self.remote_ip,
            "state": self.state,
            "protocol_shape": self.protocol_shape,
            "first_bytes_len": self.first_bytes_len,
        }
        if self.remote_port is not None:
            result["peer_port"] = self.remote_port
        if self.first_bytes_prefix_hex:
            result["first_bytes_prefix_hex"] = self.first_bytes_prefix_hex
        if self.collector_pn:
            result["collector_identity_masked"] = _mask_identity_token(self.collector_pn)
        if self.collector_identity_source:
            result["collector_identity_source"] = self.collector_identity_source
        if self.collector_identity_sources:
            result["collector_identity_sources"] = sorted(
                self.collector_identity_sources
            )
        if self.observed_protocol_shapes:
            result["observed_protocol_shapes"] = sorted(
                self.observed_protocol_shapes
            )
        return result


@dataclass(frozen=True, slots=True)
class _ExclusiveCollectorRouteReservation:
    """One temporary route that must win over normal runtime activation."""

    collector_ip: str
    collector_pn: str
    baseline_session_ids: frozenset[str]
    transparent: bool
    expected_session_protocol: str


def _transparent_route_accepts_protocol_shape(
    expected_session_protocol: str,
    protocol_shape: str,
) -> bool:
    """Return whether one fresh socket may carry the expected cloud wire."""

    expected = str(expected_session_protocol or "").strip().lower()
    observed = str(protocol_shape or "").strip().lower() or "unknown"
    if expected == "at_text":
        # AT cloud sessions may be silent until the cloud sends its first
        # command, or start directly with raw inverter bytes.
        return observed in {"unknown", "at_text", "raw_tcp"}
    if expected == "eybond_framed":
        return observed in {"unknown", "eybond_framed"}
    return False


class _SharedEybondListener:
    _MAX_SESSION_INVENTORY = 20
    # Unclaimed collector callbacks are parked (held open passively) instead
    # of being closed: closing makes the collector firmware redial within
    # seconds, producing a permanent connect/close loop for collectors that
    # have no config entry. Parked sockets stay claimable by a later scan or
    # a newly added entry.
    _MAX_PARKED_SOCKETS = 8
    _PARKED_SOCKET_TTL_SECONDS = 900.0
    _PARKED_IDENTITY_BUFFER_LIMIT = 512

    def __init__(self, *, host: str, port: int) -> None:
        self._host = host
        self._port = int(port)
        self._server: asyncio.Server | None = None
        self._ref_count = 0
        self._connections: dict[str, _CollectorConnection] = {}
        self._at_connections: dict[str, _CollectorAtConnection] = {}
        self._connections_by_pn: dict[str, _CollectorConnection] = {}
        self._at_connections_by_pn: dict[str, _CollectorAtConnection] = {}
        self._session_payload_connections: dict[str, _CollectorConnection] = {}
        self._session_at_connections: dict[str, _CollectorAtConnection] = {}
        self._pending_sockets: dict[str, _PendingCollectorSocket] = {}
        self._last_connection_ip = ""
        self._last_at_connection_ip = ""
        self._last_pending_ip = ""
        self._payload_owner_counts: dict[str, int] = {}
        self._at_owner_counts: dict[str, int] = {}
        self._payload_pn_owner_counts: dict[str, int] = {}
        self._at_pn_owner_counts: dict[str, int] = {}
        # DURABLE runtime confirmed protocol owner (PN-validated live evidence).
        # This is the ONLY source of active protocol-owner authority: onboarding
        # never registers an owner from an inferred/expected hint.
        self._session_protocol_owner_counts: dict[str, int] = {}
        self._session_seq = 0
        self._session_inventory: dict[str, _CollectorSessionInventoryEntry] = {}
        self._pending_route_lock = asyncio.Lock()
        self._exclusive_route_seq = 0
        self._exclusive_routes: dict[int, _ExclusiveCollectorRouteReservation] = {}
        self._connection_watcher_seq = 0
        self._connection_watchers: dict[int, tuple[str, Callable[[str], None]]] = {}

    def add_connection_watcher(
        self,
        collector_ip: str,
        callback: Callable[[str], None],
    ) -> int:
        """Register a callback fired when a collector socket arrives.

        ``collector_ip`` scopes the watcher to one collector; an empty value
        matches any incoming connection. The callback runs on the event loop
        and must not block.
        """

        self._connection_watcher_seq += 1
        token = self._connection_watcher_seq
        self._connection_watchers[token] = (str(collector_ip or "").strip(), callback)
        return token

    def remove_connection_watcher(self, token: int) -> None:
        self._connection_watchers.pop(token, None)

    def _notify_connection_watchers(self, remote_ip: str) -> None:
        for watched_ip, callback in tuple(self._connection_watchers.values()):
            if watched_ip and watched_ip != remote_ip:
                continue
            try:
                callback(remote_ip)
            except Exception:
                logger.debug("Collector connection watcher failed", exc_info=True)

    async def acquire(self) -> None:
        self._ref_count += 1
        if self._server is None:
            try:
                self._server = await asyncio.start_server(
                    self._handle_connection,
                    self._host,
                    self._port,
                )
            except OSError as exc:
                self._ref_count = max(0, self._ref_count - 1)
                raise CollectorListenerBindError(self._host, self._port, exc) from exc
            except BaseException:
                # Cancelled (or any failure) mid-bind: NEVER leak the refcount the
                # increment above reserved -- the bind never became a live server.
                self._ref_count = max(0, self._ref_count - 1)
                raise
            logger.info("Shared EyeBond listener listening on %s:%d", self._host, self._port)

    async def release(self) -> bool:
        self._ref_count = max(0, self._ref_count - 1)
        if self._ref_count != 0:
            return False

        for pending in tuple(self._pending_sockets.values()):
            await self._close_pending_socket(pending)
        self._pending_sockets.clear()

        for connection in self._unique_connections():
            await connection.disconnect()
        self._connections.clear()
        self._connections_by_pn.clear()
        for connection in self._unique_at_connections():
            await connection.disconnect()
        self._at_connections.clear()
        self._at_connections_by_pn.clear()
        self._session_payload_connections.clear()
        self._session_at_connections.clear()
        self._last_connection_ip = ""
        self._last_at_connection_ip = ""
        self._last_pending_ip = ""
        self._payload_owner_counts.clear()
        self._at_owner_counts.clear()
        self._payload_pn_owner_counts.clear()
        self._at_pn_owner_counts.clear()
        self._session_protocol_owner_counts.clear()
        self._exclusive_routes.clear()
        self._session_inventory.clear()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        return True

    def register_payload_owner(self, collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        self._payload_owner_counts[owner] = self._payload_owner_counts.get(owner, 0) + 1

    def register_payload_pn_owner(self, collector_pn: str) -> None:
        owner = str(collector_pn or "").strip()
        if not owner:
            return
        self._payload_pn_owner_counts[owner] = self._payload_pn_owner_counts.get(owner, 0) + 1

    def unregister_payload_owner(self, collector_ip: str) -> None:
        self._decrement_owner_count(self._payload_owner_counts, collector_ip)

    def unregister_payload_pn_owner(self, collector_pn: str) -> None:
        self._decrement_owner_count(self._payload_pn_owner_counts, collector_pn)

    def register_at_owner(self, collector_ip: str) -> None:
        owner = str(collector_ip or "").strip()
        self._at_owner_counts[owner] = self._at_owner_counts.get(owner, 0) + 1

    def register_at_pn_owner(self, collector_pn: str) -> None:
        owner = str(collector_pn or "").strip()
        if not owner:
            return
        self._at_pn_owner_counts[owner] = self._at_pn_owner_counts.get(owner, 0) + 1

    def unregister_at_owner(self, collector_ip: str) -> None:
        self._decrement_owner_count(self._at_owner_counts, collector_ip)

    def unregister_at_pn_owner(self, collector_pn: str) -> None:
        self._decrement_owner_count(self._at_pn_owner_counts, collector_pn)

    def register_session_protocol_owner(self, session_protocol: str) -> None:
        owner = str(session_protocol or "").strip().lower()
        if not owner:
            return
        self._session_protocol_owner_counts[owner] = (
            self._session_protocol_owner_counts.get(owner, 0) + 1
        )

    def unregister_session_protocol_owner(self, session_protocol: str) -> None:
        self._decrement_owner_count(self._session_protocol_owner_counts, session_protocol)

    def register_exclusive_collector_route(
        self,
        *,
        collector_ip: str,
        collector_pn: str,
        transparent: bool = False,
        expected_session_protocol: str = "",
    ) -> int:
        """Reserve matching new callbacks for a temporary proxy route.

        Runtime transports remain registered so they can resume immediately
        after the tool stops, but they must not consume the reconnect that a
        proxy/shadow route is waiting for.
        """

        self._exclusive_route_seq += 1
        token = self._exclusive_route_seq
        self._exclusive_routes[token] = _ExclusiveCollectorRouteReservation(
            collector_ip=str(collector_ip or "").strip(),
            collector_pn=str(collector_pn or "").strip(),
            baseline_session_ids=frozenset(self._pending_sockets),
            transparent=bool(transparent),
            expected_session_protocol=str(
                expected_session_protocol or ""
            ).strip().lower(),
        )
        return token

    async def pop_pending_socket_for_transparent_route(
        self,
        token: int,
    ) -> _PendingCollectorSocket | None:
        """Claim one causally-new socket without injecting identity traffic.

        A cloud proxy must preserve the collector/cloud handshake byte-for-byte.
        Sending HA's FC=2/DTUPN probe before handing the socket to the proxy
        contaminates that handshake.  The temporary route therefore claims only
        one socket that appeared after its reservation baseline.  Identity is
        verified from the real cloud exchange before the proxy is considered
        ready.
        """

        async with self._pending_route_lock:
            reservation = self._exclusive_routes.get(token)
            if reservation is None or not reservation.transparent:
                return None

            candidates: list[_PendingCollectorSocket] = []
            for pending in self._pending_sockets.values():
                if pending.session_id in reservation.baseline_session_ids:
                    continue
                entry = self._session_inventory.get(pending.session_id)
                protocol_shape = str(
                    getattr(entry, "protocol_shape", "") or ""
                ).strip().lower()
                session_state = str(
                    getattr(entry, "state", "") or ""
                ).strip().lower()
                if not _transparent_route_accepts_protocol_shape(
                    reservation.expected_session_protocol,
                    protocol_shape,
                ):
                    continue
                if (
                    protocol_shape in {"", "unknown"}
                    and session_state != "exclusive_route_silent"
                ):
                    # Give the shared listener's existing passive read one full
                    # turn to classify a framed callback before an unknown
                    # socket may be provisionally selected as a silent cloud
                    # route. This is the listener's framing boundary, not a
                    # second onboarding/recovery timeout.
                    continue
                observed_pn = str(
                    getattr(entry, "collector_pn", "") or ""
                ).strip()
                observed_source = str(
                    getattr(entry, "collector_identity_source", "") or ""
                ).strip()
                if observed_pn:
                    if (
                        reservation.collector_pn
                        and self._collector_pn_matches(
                            reservation.collector_pn,
                            observed_pn,
                        )
                    ):
                        candidates.append(pending)
                    elif identity_source_is_strong(observed_source):
                        # Never route a strongly identified foreign collector.
                        continue
                    # A weak foreign-looking prefix is not sufficient authority
                    # to select or reject a socket behind shared NAT.
                    continue
                exact_route_hint = bool(
                    reservation.collector_ip
                    and self._callback_ip_matches_collector(
                        reservation.collector_ip,
                        pending.remote_ip,
                    )
                )
                transparent_candidates = tuple(
                    candidate
                    for candidate in self._exclusive_routes.values()
                    if candidate.transparent
                    and pending.session_id not in candidate.baseline_session_ids
                )
                if exact_route_hint or (
                    len(transparent_candidates) == 1
                    and transparent_candidates[0] is reservation
                ):
                    # A private hairpin gateway may rewrite 192.168.1.55 to
                    # 192.168.1.1, which is intentionally not an identity/IP
                    # match.  One causally-new socket plus exactly one active
                    # transparent reservation is still an unambiguous
                    # provisional route; the cloud handshake must prove PN
                    # before readiness.
                    candidates.append(pending)

            unique = {id(candidate): candidate for candidate in candidates}
            if len(unique) != 1:
                return None
            pending = next(iter(unique.values()))
            await self._pause_pending_sniff(pending)
            if not self._pending_socket_still_registered(pending):
                return None
            return self._claim_pending_socket(pending)

    def _reserved_for_transparent_route(
        self,
        pending: _PendingCollectorSocket,
    ) -> bool:
        """Return whether a fresh socket must remain byte-transparent."""

        candidates = tuple(
            reservation
            for reservation in self._exclusive_routes.values()
            if reservation.transparent
            and pending.session_id not in reservation.baseline_session_ids
            and _transparent_route_accepts_protocol_shape(
                reservation.expected_session_protocol,
                str(
                    getattr(
                        self._session_inventory.get(pending.session_id),
                        "protocol_shape",
                        "",
                    )
                    or ""
                ),
            )
        )
        if len(candidates) == 1:
            return True
        return any(
            reservation.collector_ip
            and self._callback_ip_matches_collector(
                reservation.collector_ip,
                pending.remote_ip,
            )
            for reservation in candidates
        )

    async def unregister_exclusive_collector_route(self, token: int) -> None:
        """Drop a reservation and return any unclaimed socket to normal routing."""

        if self._exclusive_routes.pop(token, None) is None:
            return

        for pending in tuple(self._pending_sockets.values()):
            entry = self._session_inventory.get(pending.session_id)
            if str(getattr(entry, "state", "") or "") != "waiting_for_exclusive_route":
                continue
            observed_pn = str(getattr(entry, "collector_pn", "") or "").strip()
            if self._matches_exclusive_collector_route(
                remote_ip=pending.remote_ip,
                observed_pn=observed_pn,
                protocol_shape=str(
                    getattr(entry, "protocol_shape", "") or ""
                ),
            ):
                continue
            await self._pause_pending_sniff(pending)
            if not self._pending_socket_still_registered(pending):
                continue
            pending.parked = False
            pending.sniff_task = _spawn_tracked_task(
                self._sniff_pending_socket(pending),
                name=f"collector_pending_sniff_{pending.remote_ip}",
            )

    def _matches_exclusive_collector_route(
        self,
        *,
        remote_ip: str,
        observed_pn: str,
        protocol_shape: str,
    ) -> bool:
        """Match by identity first; peer IP is only for unidentified sockets."""

        normalized_pn = str(observed_pn or "").strip()
        for reservation in self._exclusive_routes.values():
            if reservation.transparent and not _transparent_route_accepts_protocol_shape(
                reservation.expected_session_protocol,
                protocol_shape,
            ):
                continue
            if normalized_pn:
                if reservation.collector_pn and self._collector_pn_matches(
                    reservation.collector_pn,
                    normalized_pn,
                ):
                    return True
                # An observed foreign identity must never be captured merely
                # because several collectors share one NAT/peer address.
                continue
            if reservation.collector_ip and self._callback_ip_matches_collector(
                reservation.collector_ip,
                remote_ip,
            ):
                return True
        return False

    def _decrement_owner_count(self, owner_counts: dict[str, int], owner_value: str) -> None:
        owner = str(owner_value or "").strip()
        count = owner_counts.get(owner, 0)
        if count <= 1:
            owner_counts.pop(owner, None)
            return
        owner_counts[owner] = count - 1

    def ensure_connection(
        self,
        collector_ip: str,
        heartbeat_interval: float,
        write_timeout: float,
        collector_pn: str = "",
    ) -> _CollectorConnection | None:
        if collector_pn:
            normalized_pn = str(collector_pn or "").strip()
            connection = self._connection_by_collector_pn(
                normalized_pn,
                self._connections_by_pn,
            )
            if connection is None:
                connection = _CollectorConnection(
                    remote_ip_hint=collector_ip,
                    heartbeat_interval=heartbeat_interval,
                    write_timeout=write_timeout,
                )
                self._connections_by_pn[normalized_pn] = connection
            else:
                connection.set_heartbeat_interval(heartbeat_interval)
                connection.set_write_timeout(write_timeout)
            _seed_connection_collector_pn(connection, normalized_pn)
            return connection

        if collector_ip:
            connection = self._connections.get(collector_ip)
            if connection is None:
                connection = _CollectorConnection(
                    remote_ip_hint=collector_ip,
                    heartbeat_interval=heartbeat_interval,
                    write_timeout=write_timeout,
                )
                self._connections[collector_ip] = connection
            else:
                connection.set_heartbeat_interval(heartbeat_interval)
                connection.set_write_timeout(write_timeout)
            return connection

        connection = self.current_connection(
            heartbeat_interval=heartbeat_interval,
            write_timeout=write_timeout,
        )
        return connection

    @staticmethod
    def _connection_is_unbound_placeholder(
        connection: object,
        session_connections: dict[str, object],
    ) -> bool:
        """Return whether a facade is safe to bind to one new physical socket.

        A connection object is mutable transport state (reader, writer and
        session id), so two accepted sockets must never share it.  The only
        reusable object is an idle placeholder that has not yet been assigned
        to any socket-scoped session id.
        """

        return not bool(getattr(connection, "connected", False)) and not any(
            candidate is connection for candidate in session_connections.values()
        )

    def current_connection(self, *, heartbeat_interval: float, write_timeout: float) -> _CollectorConnection | None:
        connected = tuple(
            connection
            for connection in self._unique_connections()
            if connection.connected
        )
        if len(connected) != 1:
            return None

        connection = connected[0]
        connection.set_heartbeat_interval(heartbeat_interval)
        connection.set_write_timeout(write_timeout)
        return connection

    def payload_connection_for_session(
        self, session_id: str
    ) -> _CollectorConnection | None:
        """Return the ACTIVATED framed connection of one exact session id.

        A registry-claimed session id is the strongest route instruction there
        is: the caller owns exactly that observed socket, so neither peer IP nor
        a collector-PN index may substitute another one. A parked (not yet
        activated) socket resolves to ``None`` here -- claiming it is
        :meth:`pop_pending_socket_for_route`'s job, keyed by the same id.
        """

        sid = str(session_id or "").strip()
        if not sid:
            return None
        return self._session_payload_connections.get(sid)

    def at_connection_for_session(
        self, session_id: str
    ) -> _CollectorAtConnection | None:
        """Return the ACTIVATED AT connection of one exact session id."""

        sid = str(session_id or "").strip()
        if not sid:
            return None
        return self._session_at_connections.get(sid)

    def ensure_at_connection(
        self,
        collector_ip: str,
        write_timeout: float,
        collector_pn: str = "",
        raw_passthrough_bootstrap: str = "",
        raw_passthrough_frame_format: str = "",
        raw_passthrough_min_interval_ms: int = 0,
    ) -> _CollectorAtConnection | None:
        if collector_pn:
            normalized_pn = str(collector_pn or "").strip()
            connection = self._connection_by_collector_pn(
                normalized_pn,
                self._at_connections_by_pn,
            )
            if connection is None:
                connection = _CollectorAtConnection(
                    remote_ip_hint=collector_ip,
                    write_timeout=write_timeout,
                    raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                    raw_passthrough_frame_format=raw_passthrough_frame_format,
                    raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
                )
                self._at_connections_by_pn[normalized_pn] = connection
            else:
                connection.set_write_timeout(write_timeout)
                connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
                connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
                connection.set_raw_passthrough_min_interval_ms(
                    raw_passthrough_min_interval_ms
                )
            _seed_connection_collector_pn(connection, normalized_pn)
            return connection

        if collector_ip:
            connection = self._at_connections.get(collector_ip)
            if connection is None:
                connection = _CollectorAtConnection(
                    remote_ip_hint=collector_ip,
                    write_timeout=write_timeout,
                    raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                    raw_passthrough_frame_format=raw_passthrough_frame_format,
                    raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
                )
                self._at_connections[collector_ip] = connection
            else:
                connection.set_write_timeout(write_timeout)
                connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
                connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
                connection.set_raw_passthrough_min_interval_ms(
                    raw_passthrough_min_interval_ms
                )
            return connection

        connection = self.current_at_connection(write_timeout=write_timeout)
        if connection is not None:
            connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
            connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
            connection.set_raw_passthrough_min_interval_ms(raw_passthrough_min_interval_ms)
        return connection

    def _connection_by_collector_pn(
        self,
        collector_pn: str,
        connections_by_pn: dict[str, object],
    ) -> object | None:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return None
        exact = connections_by_pn.get(normalized_pn)
        if exact is not None and getattr(exact, "connected", False):
            return exact

        candidates: list[object] = []
        for known_pn, connection in connections_by_pn.items():
            if self._collector_pn_matches(normalized_pn, known_pn):
                candidates.append(connection)
        unique_candidates = {id(candidate): candidate for candidate in candidates}
        connected_candidates = {
            identity: candidate
            for identity, candidate in unique_candidates.items()
            if getattr(candidate, "connected", False)
        }
        if len(connected_candidates) == 1:
            return next(iter(connected_candidates.values()))
        if exact is not None:
            return exact
        if len(unique_candidates) != 1:
            return None
        return next(iter(unique_candidates.values()))

    def _single_registered_session_protocol(self) -> str:
        """Return the single DURABLE confirmed protocol owner (runtime), else "".

        The confirmed owner (from validated runtime confirmed evidence) is the
        ONLY thing that authorises an active identity probe. Onboarding registers
        no owner, so an inferred/expected/cloud-family hint can never drive a
        probe here. Ambiguous (>1 distinct) or absent fails closed ("").
        """

        protocols = tuple(
            protocol
            for protocol, count in self._session_protocol_owner_counts.items()
            if protocol and count > 0
        )
        if len(protocols) != 1:
            return ""
        return protocols[0]

    def current_at_connection(self, *, write_timeout: float) -> _CollectorAtConnection | None:
        connected = tuple(
            connection
            for connection in self._unique_at_connections()
            if connection.connected
        )
        if len(connected) != 1:
            return None

        connection = connected[0]
        connection.set_write_timeout(write_timeout)
        return connection

    def _unique_connections(self) -> tuple[_CollectorConnection, ...]:
        seen: set[int] = set()
        unique: list[_CollectorConnection] = []
        for mapping in (
            self._connections,
            self._connections_by_pn,
            self._session_payload_connections,
        ):
            for connection in mapping.values():
                identity = id(connection)
                if identity in seen:
                    continue
                seen.add(identity)
                unique.append(connection)
        return tuple(unique)

    def _unique_at_connections(self) -> tuple[_CollectorAtConnection, ...]:
        seen: set[int] = set()
        unique: list[_CollectorAtConnection] = []
        for mapping in (
            self._at_connections,
            self._at_connections_by_pn,
            self._session_at_connections,
        ):
            for connection in mapping.values():
                identity = id(connection)
                if identity in seen:
                    continue
                seen.add(identity)
                unique.append(connection)
        return tuple(unique)

    def session_inventory_diagnostics(self) -> dict[str, object]:
        entries = tuple(self._session_inventory.values())
        pending_ids = {pending.session_id for pending in self._pending_sockets.values()}
        peer_counts: dict[str, int] = {}
        for entry in entries:
            if not entry.remote_ip:
                continue
            peer_counts[entry.remote_ip] = peer_counts.get(entry.remote_ip, 0) + 1
        duplicate_peer_ips = sorted(
            peer_ip for peer_ip, count in peer_counts.items() if count > 1
        )
        return {
            "pending_session_count": len(pending_ids),
            "recent_session_count": len(entries),
            "duplicate_peer_ip_count": len(duplicate_peer_ips),
            "duplicate_peer_ips": duplicate_peer_ips,
            "sessions": [entry.diagnostics() for entry in entries],
        }

    def discovered_collector_sessions(self) -> tuple[dict[str, object], ...]:
        """Return raw collector identities observed by this listener.

        This is intentionally separate from ``session_inventory_diagnostics``:
        diagnostics mask collector PN values for support bundles, while onboarding
        needs the raw PN to materialize multiple collectors that call back from
        the same NAT peer IP.
        """

        sessions: list[dict[str, object]] = []
        for entry in self._session_inventory.values():
            collector_pn = str(entry.collector_pn or "").strip()
            remote_ip = str(entry.remote_ip or "").strip()
            session_id = str(entry.session_id or "").strip()
            has_live_backing = bool(session_id) and (
                any(
                    str(pending.session_id or "").strip() == session_id
                    for pending in self._pending_sockets.values()
                )
                or session_id in self._session_payload_connections
                or session_id in self._session_at_connections
            )
            if (
                not collector_pn
                or not remote_ip
                or str(entry.state or "").startswith("closed")
                or str(entry.state or "") == "parked_peer_closed"
                # Inventory is diagnostic history, not socket authority.  A
                # cancelled/released route can leave a non-terminal historical
                # state behind after its pending/activated backing is already
                # gone.  Publishing that record as live makes the domain
                # registry pin an entry to a ghost session; callback-on-demand
                # then refuses to send a new trigger forever because it believes
                # an exact owned socket is still available.
                or not has_live_backing
            ):
                continue
            sessions.append(
                {
                    "session_id": entry.session_id,
                    "peer_ip": remote_ip,
                    "peer_port": entry.remote_port,
                    "state": entry.state,
                    "protocol_shape": entry.protocol_shape,
                    "collector_pn": collector_pn,
                    "collector_identity_source": entry.collector_identity_source,
                    "collector_identity_sources": tuple(
                        sorted(entry.collector_identity_sources)
                    ),
                    "observed_protocol_shapes": tuple(
                        sorted(entry.observed_protocol_shapes)
                    ),
                }
            )
        return tuple(sessions)

    def _next_session_id(self) -> str:
        self._session_seq += 1
        return f"listener-{self._port}-{self._session_seq}"

    def _remember_session(
        self,
        *,
        session_id: str,
        remote_ip: str,
        remote_port: int | None,
    ) -> None:
        self._session_inventory[session_id] = _CollectorSessionInventoryEntry(
            session_id=session_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
        )
        while len(self._session_inventory) > self._MAX_SESSION_INVENTORY:
            oldest = next(iter(self._session_inventory))
            self._session_inventory.pop(oldest, None)

    def _mark_session_state(self, session_id: str, state: str) -> None:
        entry = self._session_inventory.get(session_id)
        if entry is not None:
            # Session ids describe physical TCP sockets.  Once that socket has
            # reached a terminal state, a slower route/identity coroutine must
            # never resurrect it as ``routed_*`` or ``route_identity_mismatch``.
            # E500 collectors can open several replacement sockets almost at
            # once, making this ordering observable during strategy recovery.
            if _session_state_is_terminal(entry.state) and not _session_state_is_terminal(
                state
            ):
                return
            entry.state = state

    def _mark_socket_session_closed(self, session_id: str, connection: object) -> None:
        """Close one physical socket session without unindexing its successor.

        A collector reconnect may reuse the same ``_CollectorConnection``
        facade. The old run then has a stale epoch and must not execute the
        facade-level disconnect callback, but its own session inventory record
        still has to become terminal. Session ids are per accepted socket, so
        this operation never identifies or owns a collector by peer IP.
        """

        normalized = str(session_id or "").strip()
        if not normalized:
            return
        for mapping in (
            self._session_payload_connections,
            self._session_at_connections,
        ):
            if mapping.get(normalized) is connection:
                mapping.pop(normalized, None)
        self._mark_session_state(normalized, "closed_disconnected")
        logger.info(
            "Collector socket session closed listener_port=%s session=%s",
            self._port,
            normalized,
        )

    def _mark_session_first_bytes(self, session_id: str, chunk: bytes) -> None:
        entry = self._session_inventory.get(session_id)
        if entry is None:
            return
        observed_shape = _classify_initial_protocol_shape(chunk)
        if observed_shape != "unknown":
            entry.observed_protocol_shapes.add(observed_shape)
        # ``protocol_shape`` is the PRIMARY activation observation.  A later
        # AT/FC identity response on the same hybrid socket is additional
        # capability evidence and must never rewrite those actual first bytes.
        if entry.protocol_shape in {"", "unknown"}:
            entry.first_bytes_len = len(chunk)
            entry.first_bytes_prefix_hex = chunk[:4].hex()
            if observed_shape != "unknown":
                entry.protocol_shape = observed_shape

    def _mark_session_identity(
        self,
        session_id: str,
        collector_pn: str,
        source: str,
    ) -> None:
        normalized_pn = validated_collector_pn(collector_pn)
        if not normalized_pn:
            return
        entry = self._session_inventory.get(session_id)
        if entry is not None:
            entry.collector_pn = reconcile_pn(
                entry.collector_pn,
                normalized_pn,
            )
            if source:
                entry.collector_identity_sources.add(source)
                entry.collector_identity_source = prefer_identity_source(
                    entry.collector_identity_source,
                    source,
                )

        payload_connection = self._session_payload_connections.get(session_id)
        if payload_connection is not None:
            self._connections_by_pn[normalized_pn] = payload_connection
        at_connection = self._session_at_connections.get(session_id)
        if at_connection is not None:
            self._at_connections_by_pn[normalized_pn] = at_connection

    def matching_callback_ips(self, collector_ip: str) -> tuple[str, ...]:
        if not collector_ip:
            return ()

        is_broadcast_placeholder = _is_ipv4_broadcast_placeholder(collector_ip)
        ordered: list[str] = []
        seen: set[str] = set()

        def _matches(remote_ip: str) -> bool:
            if not remote_ip:
                return False
            if is_broadcast_placeholder and remote_ip == collector_ip:
                return False
            if remote_ip == collector_ip:
                return True
            return bool(
                _is_hairpin_alias_candidate(collector_ip, remote_ip)
                or _is_default_broadcast_alias_candidate(collector_ip, remote_ip)
            )

        def _remember(remote_ip: str) -> None:
            if not _matches(remote_ip) or remote_ip in seen:
                return
            seen.add(remote_ip)
            ordered.append(remote_ip)

        _remember(self._last_connection_ip)
        _remember(self._last_pending_ip)
        _remember(self._last_at_connection_ip)

        for pending in self._pending_sockets.values():
            _remember(pending.remote_ip)

        for remote_ip, connection in self._connections.items():
            if connection.connected:
                _remember(remote_ip)

        for remote_ip, connection in self._at_connections.items():
            if connection.connected:
                _remember(remote_ip)

        return tuple(ordered)

    def _resolve_public_placeholder_alias(
        self,
        remote_ip: str,
        connections: dict[str, object] | None = None,
    ) -> object | None:
        connection_map = connections if connections is not None else self._connections
        if not remote_ip or remote_ip in connection_map:
            return connection_map.get(remote_ip)

        candidates: list[tuple[str, object]] = []
        for expected_ip, connection in connection_map.items():
            if getattr(connection, "connected", False):
                continue
            if not (
                _is_hairpin_alias_candidate(expected_ip, remote_ip)
                or _is_default_broadcast_alias_candidate(expected_ip, remote_ip)
            ):
                continue
            candidates.append((expected_ip, connection))

        unique_candidates: list[tuple[str, object]] = []
        seen: set[int] = set()
        for expected_ip, connection in candidates:
            identity = id(connection)
            if identity in seen:
                continue
            seen.add(identity)
            unique_candidates.append((expected_ip, connection))

        if len(unique_candidates) != 1:
            return None

        expected_ip, connection = unique_candidates[0]
        logger.info(
            "Aliasing collector callback from %s to pending unresolved target %s",
            remote_ip,
            expected_ip,
        )
        connection_map[remote_ip] = connection
        return connection

    def has_pending_socket(self, collector_ip: str = "") -> bool:
        return self._select_pending_socket(collector_ip) is not None

    def pop_pending_socket(self, collector_ip: str = "") -> _PendingCollectorSocket | None:
        pending = self._select_pending_socket(collector_ip)
        if pending is None:
            return None
        return self._claim_pending_socket(pending)

    async def pop_pending_socket_for_route(
        self,
        *,
        collector_ip: str = "",
        collector_pn: str = "",
        session_protocol: str = "",
        session_id: str = "",
    ) -> _PendingCollectorSocket | None:
        # Registry-mediated claim: this is the low-level socket-claim primitive,
        # NOT an independent ownership authority. Ownership ("which session
        # belongs to which entry/PN") is decided by
        # connection.session_registry.CallbackSessionRegistry (keyed by full PN,
        # never peer IP); the runtime passes the registry-chosen ``session_id``
        # so this claims exactly that socket. When no session_id is given the
        # durable ``collector_pn`` is authoritative; ``collector_ip`` is only a
        # narrowing hint for an unidentified silent collector and can never claim
        # or disturb a socket already identified as a different collector.
        async with self._pending_route_lock:
            normalized_session_id = str(session_id or "").strip()
            if normalized_session_id:
                # The registry told us exactly which observed session is ours.
                pending = self._select_pending_socket_by_session_id(normalized_session_id)
                if pending is None:
                    return None
                await self._pause_pending_sniff(pending)
                if not self._pending_socket_still_registered(pending):
                    return None
                return self._claim_pending_socket(pending)

            normalized_pn = str(collector_pn or "").strip()
            if not normalized_pn:
                pending = self._select_pending_socket(collector_ip)
                if pending is None:
                    return None
                await self._pause_pending_sniff(pending)
                if not self._pending_socket_still_registered(pending):
                    return None
                return self._claim_pending_socket(pending)

            matched = self._select_pending_socket_by_collector_pn(normalized_pn)
            if matched is not None:
                return self._claim_pending_socket(matched)

            candidates = self._route_identity_candidates(collector_ip)
            if not candidates:
                return None

            for pending in candidates:
                if not self._pending_socket_still_registered(pending):
                    continue
                # Ownership safety: when a PN is present, never probe or claim a
                # socket already identified as a DIFFERENT collector (e.g. a
                # second collector behind the same NAT/public IP). Only our-PN or
                # not-yet-identified (silent) sockets are eligible. Keep the
                # skipped socket watched so its true owner can still claim it and
                # it is not left as an unwatched phantom.
                existing_entry = self._session_inventory.get(pending.session_id)
                existing_pn = str(getattr(existing_entry, "collector_pn", "") or "").strip()
                existing_source = str(
                    getattr(existing_entry, "collector_identity_source", "") or ""
                ).strip()
                if (
                    existing_pn
                    and identity_source_is_strong(existing_source)
                    and not self._collector_pn_matches(normalized_pn, existing_pn)
                ):
                    self._resume_pending_watch(pending)
                    continue
                await self._pause_pending_sniff(pending)
                if not self._pending_socket_still_registered(pending):
                    continue
                pending_pn = await self._identify_pending_socket_for_route(
                    pending,
                    session_protocol=session_protocol,
                )
                if not self._pending_socket_still_registered(pending):
                    continue
                if self._collector_pn_matches(normalized_pn, pending_pn):
                    return self._claim_pending_socket(pending)
                if pending_pn:
                    self._mark_session_state(pending.session_id, "route_identity_mismatch")
                else:
                    self._mark_session_state(pending.session_id, "waiting_for_route_identity")
                # The pause above cancelled the sniff task; the socket stays
                # registered for another claimant, so it needs a watcher again.
                self._resume_pending_watch(pending)
            return None

    def silent_pending_collector_sessions(self) -> tuple[dict[str, object], ...]:
        """Live pending sockets that have volunteered NO identity yet.

        The onboarding bootstrap view: ``discovered_collector_sessions``
        deliberately hides PN-less sockets (they are not discovery
        candidates), but a callback identity attempt must be able to tell "a
        TCP session arrived and stayed silent" apart from "nothing arrived".
        Only session ids and lifecycle states are exposed -- no bytes, no
        wire guess, no peer-IP identity.
        """

        sessions: list[dict[str, object]] = []
        for pending in self._pending_sockets.values():
            session_id = str(pending.session_id or "").strip()
            if not session_id:
                continue
            entry = self._session_inventory.get(session_id)
            if str(getattr(entry, "collector_pn", "") or "").strip():
                continue
            state = str(getattr(entry, "state", "") or "").strip()
            if state.startswith("closed"):
                continue
            sessions.append({"session_id": session_id, "state": state})
        return tuple(sessions)

    async def async_identify_pending_session(
        self,
        session_id: str,
        *,
        session_protocol: str,
        identity_probe_kind: str = "",
    ) -> str:
        """ONE read-only identity probe of one exact silent pending socket.

        The onboarding-bootstrap entry point into the SAME probe algorithm the
        confirmed-owner route activation uses
        (``_identify_pending_socket_for_route``): pause the sniffer, send the
        single identity query of the given wire (framed FC=2 parameter 2 /
        ``AT+DTUPN``), record a strong identity on a valid reply, and keep the
        socket watched either way. Returns the PN or ``""`` -- never guesses,
        never retries, never falls back to another protocol.
        """

        async with self._pending_route_lock:
            pending = self._select_pending_socket_by_session_id(session_id)
            if pending is None:
                return ""
            pending_pn = ""
            try:
                await self._pause_pending_sniff(pending)
                if not self._pending_socket_still_registered(pending):
                    return ""
                pending_pn = await self._identify_pending_socket_for_route(
                    pending,
                    session_protocol=session_protocol,
                    identity_probe_kind=identity_probe_kind,
                )
                return pending_pn
            finally:
                # The probe temporarily owns the reader.  On success, failure,
                # or cancellation the still-pending socket must return to one
                # settled/watchable state.  Otherwise its peer close is never
                # observed and the registry keeps advertising a dead exact
                # session indefinitely.
                if self._pending_socket_still_registered(pending):
                    entry = self._session_inventory.get(pending.session_id)
                    observed_pn = str(
                        getattr(entry, "collector_pn", "") or ""
                    ).strip()
                    observed_source = str(
                        getattr(entry, "collector_identity_source", "") or ""
                    ).strip()
                    if observed_pn and identity_source_is_strong(observed_source):
                        self._mark_session_state(
                            pending.session_id,
                            "parked_identified_strong",
                        )
                    else:
                        self._mark_session_state(
                            pending.session_id,
                            "waiting_for_route_identity",
                        )
                    self._resume_pending_watch(pending)

    async def async_retire_pending_session(self, session_id: str) -> bool:
        """Close and retire exactly one still-pending socket by session id.

        This is used only after an unknown-wire identity challenge received no
        correlated response.  Retiring that physical socket prevents a later
        attempt from sending a different protocol dialect over the same stream.
        Selection is session-id only; peer IP, PN prefix and arrival order never
        participate.
        """

        async with self._pending_route_lock:
            pending = self._select_pending_socket_by_session_id(session_id)
            if pending is None:
                return False
            await self._pause_pending_sniff(pending)
            if not self._pending_socket_still_registered(pending):
                return False
            self._remove_pending_socket(pending)
            if self._last_pending_ip == pending.remote_ip:
                self._last_pending_ip = ""
            self._mark_session_state(
                pending.session_id,
                "closed_identity_negotiation_retry",
            )
            await _close_writer_bounded(pending.writer)
            return True

    def _select_pending_socket_by_session_id(
        self,
        session_id: str,
    ) -> _PendingCollectorSocket | None:
        sid = str(session_id or "").strip()
        if not sid:
            return None
        for pending in self._pending_sockets.values():
            if pending.session_id == sid:
                return pending
        return None

    def _claim_pending_socket(self, pending: _PendingCollectorSocket) -> _PendingCollectorSocket:
        self._remove_pending_socket(pending)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is not None:
            sniff_task.cancel()
        self._mark_session_state(pending.session_id, "claimed")
        return pending

    def _pending_socket_key(self, pending: _PendingCollectorSocket) -> str:
        for key, candidate in self._pending_sockets.items():
            if candidate is pending:
                return key
        return pending.session_id or pending.remote_ip

    def _remove_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        self._pending_sockets.pop(self._pending_socket_key(pending), None)

    def _pending_socket_still_registered(self, pending: _PendingCollectorSocket) -> bool:
        return any(candidate is pending for candidate in self._pending_sockets.values())

    def _pending_sockets_for_remote_ip(self, remote_ip: str) -> tuple[_PendingCollectorSocket, ...]:
        return tuple(
            pending
            for pending in self._pending_sockets.values()
            if pending.remote_ip == remote_ip
        )

    def _select_pending_socket_by_collector_pn(
        self,
        collector_pn: str,
    ) -> _PendingCollectorSocket | None:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return None
        candidates: list[_PendingCollectorSocket] = []
        for pending in self._pending_sockets.values():
            entry = self._session_inventory.get(pending.session_id)
            pending_pn = str(getattr(entry, "collector_pn", "") or "").strip()
            if self._collector_pn_matches(normalized_pn, pending_pn):
                candidates.append(pending)
        unique_candidates = {id(candidate): candidate for candidate in candidates}
        if len(unique_candidates) != 1:
            return None
        return next(iter(unique_candidates.values()))

    def _route_identity_candidates(self, collector_ip: str) -> tuple[_PendingCollectorSocket, ...]:
        if not collector_ip:
            return tuple(self._pending_sockets.values())

        exact = self._pending_sockets_for_remote_ip(collector_ip)
        if exact:
            return exact

        return tuple(
            pending
            for pending in self._pending_sockets.values()
            if _is_hairpin_alias_candidate(collector_ip, pending.remote_ip)
            or _is_default_broadcast_alias_candidate(collector_ip, pending.remote_ip)
        )

    async def _pause_pending_sniff(self, pending: _PendingCollectorSocket) -> None:
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is None or sniff_task.done():
            return
        sniff_task.cancel()
        # ``return_exceptions`` turns the deliberate child cancellation into a
        # normal drain result.  A CancelledError raised by shield therefore
        # belongs to this caller and must be propagated after mandatory cleanup.
        drain_task = asyncio.gather(sniff_task, return_exceptions=True)
        parent_cancelled = False
        while not drain_task.done():
            try:
                await asyncio.shield(drain_task)
            except asyncio.CancelledError:
                # The drain task converts the deliberate child cancellation
                # into normal completion, so every CancelledError observed here
                # belongs to this caller.  Finish draining the owned task, then
                # propagate cancellation across the public probe boundary.
                parent_cancelled = True
                # Repeated parent cancellation cannot interrupt mandatory
                # child cleanup; shield and wait again.
                continue
        await drain_task
        if parent_cancelled:
            raise asyncio.CancelledError

    def _collector_pn_matches(self, expected_pn: str, observed_pn: str) -> bool:
        return pn_is_same_identity(expected_pn, observed_pn)

    def _select_pending_socket(self, collector_ip: str) -> _PendingCollectorSocket | None:
        if collector_ip:
            exact = self._pending_sockets_for_remote_ip(collector_ip)
            if len({id(pending) for pending in exact}) == 1:
                return exact[0]
            if len(exact) > 1:
                return None

            candidates: list[_PendingCollectorSocket] = []
            for candidate in self._pending_sockets.values():
                remote_ip = candidate.remote_ip
                if not (
                    _is_hairpin_alias_candidate(collector_ip, remote_ip)
                    or _is_default_broadcast_alias_candidate(collector_ip, remote_ip)
                ):
                    continue
                candidates.append(candidate)

            unique_candidates = {id(candidate): candidate for candidate in candidates}
            if len(unique_candidates) == 1:
                return next(iter(unique_candidates.values()))
            # A broadcast address identifies a trigger route, never a physical
            # collector.  With multiple fresh sockets there is no safe winner:
            # arrival order / ``_last_pending_ip`` is timing, not causality or
            # identity.  Exact-session admission may resolve them individually;
            # the generic route selector must fail closed.
            if unique_candidates and _is_ipv4_broadcast_placeholder(collector_ip):
                return None
            return None

        unique_candidates = tuple({id(pending): pending for pending in self._pending_sockets.values()}.values())
        if len(unique_candidates) != 1:
            return None
        return unique_candidates[0]

    async def _close_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        sniff_task = pending.sniff_task
        pending.sniff_task = None
        if sniff_task is not None:
            sniff_task.cancel()
            try:
                await sniff_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        # Closing a pending socket is a physical terminal event.  Keep the
        # bounded inventory record for diagnostics, but never leave its last
        # probe/park state looking live to discovery and the ownership registry.
        self._mark_session_state(pending.session_id, "closed_disconnected")
        try:
            await _close_writer_bounded(pending.writer)
        except asyncio.CancelledError:
            pass

    def _evict_parked_sockets(self, new_pending: _PendingCollectorSocket) -> None:
        """Bound parked sockets by count; never collapse sessions by peer IP.

        Multiple collectors behind one NAT legitimately share a peer IP. Their
        accepted sockets remain independent by ``session_id`` until strong PN
        evidence and registry ownership resolve them. Resource safety comes from
        the total parked-socket cap plus each socket's TTL, not from treating an
        address as collector identity.
        """

        def _close(parked: _PendingCollectorSocket, state: str) -> None:
            self._remove_pending_socket(parked)
            task = parked.sniff_task
            parked.sniff_task = None
            if task is not None and task is not asyncio.current_task():
                task.cancel()
            self._mark_session_state(parked.session_id, state)
            parked.writer.close()

        parked_sockets = [
            candidate
            for candidate in self._pending_sockets.values()
            if candidate.parked and candidate is not new_pending
        ]
        while len(parked_sockets) >= self._MAX_PARKED_SOCKETS:
            _close(parked_sockets.pop(0), "parked_evicted")

    async def _park_unclaimed_pending_socket(
        self,
        pending: _PendingCollectorSocket,
        chunk: bytes,
        *,
        session_state: str,
    ) -> None:
        """Hold an ownerless collector callback open instead of dropping it.

        The already-sniffed bytes stay buffered so a later claim replays them;
        the watch loop keeps a bounded identity buffer, notices a peer close,
        and closes the socket after the TTL as a natural refresh point.
        """

        pending.initial_bytes = chunk + pending.initial_bytes
        pending.parked = True
        self._evict_parked_sockets(pending)
        if pending.session_id:
            self._pending_sockets[pending.session_id] = pending
        self._last_pending_ip = pending.remote_ip
        self._mark_session_state(pending.session_id, session_state)
        logger.debug(
            "Parked unclaimed collector callback from %s (%s)",
            pending.remote_ip,
            session_state,
        )
        await self._watch_parked_pending_socket(pending)

    def _resume_pending_watch(self, pending: _PendingCollectorSocket) -> None:
        """Re-arm the park watch after a paused sniff left the socket registered.

        A route-identity attempt pauses (cancels) the sniff task; when the
        socket turns out to belong to another collector it stays registered —
        without a watcher it would never notice a peer close, and a dead
        socket blocks same-IP routing as a phantom duplicate.
        """

        if not self._pending_socket_still_registered(pending):
            return
        if pending.sniff_task is not None and not pending.sniff_task.done():
            return
        pending.parked = True
        pending.sniff_task = _spawn_tracked_task(
            self._watch_parked_pending_socket(pending),
            name=f"collector_parked_watch_{pending.remote_ip}",
        )

    async def _watch_parked_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._PARKED_SOCKET_TTL_SECONDS
        close_state = "parked_expired"
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                data = await asyncio.wait_for(
                    pending.reader.read(256),
                    timeout=min(30.0, remaining),
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                # Claimed or listener shutdown: the socket is not ours to close.
                raise
            except Exception:
                close_state = "parked_read_failed"
                break
            if not data:
                close_state = "parked_peer_closed"
                break
            if len(pending.initial_bytes) < self._PARKED_IDENTITY_BUFFER_LIMIT:
                pending.initial_bytes = (
                    pending.initial_bytes + data
                )[: self._PARKED_IDENTITY_BUFFER_LIMIT]
                if _classify_initial_protocol_shape(pending.initial_bytes) != "unknown":
                    pending.parked = False
                    await self._sniff_pending_socket(pending)
                    return

        if not self._pending_socket_still_registered(pending):
            return
        self._remove_pending_socket(pending)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""
        self._mark_session_state(pending.session_id, close_state)
        await _close_writer_bounded(pending.writer)

    def _callback_ip_matches_collector(self, collector_ip: str, remote_ip: str) -> bool:
        if not collector_ip or not remote_ip:
            return False
        if remote_ip == collector_ip:
            return True
        return bool(
            _is_hairpin_alias_candidate(collector_ip, remote_ip)
            or _is_default_broadcast_alias_candidate(collector_ip, remote_ip)
        )

    def _has_owner_for_remote_ip(self, owner_counts: dict[str, int], remote_ip: str) -> bool:
        for collector_ip, count in owner_counts.items():
            if count <= 0:
                continue
            if not collector_ip:
                return True
            if self._callback_ip_matches_collector(collector_ip, remote_ip):
                return True
        return False

    def _has_owner_for_collector_pn(
        self,
        owner_counts: dict[str, int],
        collector_pn: str,
    ) -> bool:
        normalized_pn = str(collector_pn or "").strip()
        if not normalized_pn:
            return False
        for owner_pn, count in owner_counts.items():
            if count <= 0:
                continue
            if self._collector_pn_matches(normalized_pn, owner_pn):
                return True
        return False

    def _drop_connection_indexes_for_connection(self, connection: object) -> None:
        """Remove every listener index that still points at a disconnected connection."""

        selected_id = id(connection)
        payload_removed = False
        at_removed = False
        closed_session_ids: set[str] = set()
        for mapping, is_payload in (
            (self._connections, True),
            (self._connections_by_pn, True),
            (self._session_payload_connections, True),
            (self._at_connections, False),
            (self._at_connections_by_pn, False),
            (self._session_at_connections, False),
        ):
            for key, candidate in tuple(mapping.items()):
                if id(candidate) == selected_id:
                    if (
                        mapping is self._session_payload_connections
                        or mapping is self._session_at_connections
                    ):
                        closed_session_ids.add(str(key))
                    mapping.pop(key, None)
                    if is_payload:
                        payload_removed = True
                    else:
                        at_removed = True

        for session_id in closed_session_ids:
            self._mark_session_state(session_id, "closed_disconnected")

        if payload_removed and not any(
            id(candidate) == selected_id for candidate in self._connections.values()
        ):
            self._last_connection_ip = ""
        if at_removed and not any(
            id(candidate) == selected_id for candidate in self._at_connections.values()
        ):
            self._last_at_connection_ip = ""

    def _connection_keys_for_collector(
        self,
        collector_ip: str,
        connections: dict[str, object],
    ) -> tuple[str, ...]:
        if not collector_ip:
            return ()

        selected_ids: set[int] = set()
        for remote_ip, connection in connections.items():
            if self._callback_ip_matches_collector(collector_ip, remote_ip):
                selected_ids.add(id(connection))

        if not selected_ids:
            return ()

        return tuple(
            remote_ip
            for remote_ip, connection in connections.items()
            if id(connection) in selected_ids
        )

    def _connection_keys_for_collector_pn(
        self,
        collector_pn: str,
        connections_by_pn: dict[str, object],
        connections: dict[str, object],
    ) -> tuple[str, ...]:
        connection = self._connection_by_collector_pn(collector_pn, connections_by_pn)
        if connection is None:
            return ()
        selected_id = id(connection)
        return tuple(
            key
            for key, candidate in connections.items()
            if id(candidate) == selected_id
        )

    def _drop_connection_pn_indexes(
        self,
        connections_by_pn: dict[str, object],
        selected_connections: list[object],
    ) -> None:
        if not selected_connections:
            return
        selected_ids = {id(connection) for connection in selected_connections}
        for collector_pn, connection in tuple(connections_by_pn.items()):
            if id(connection) in selected_ids:
                connections_by_pn.pop(collector_pn, None)

    def _drop_connection_session_indexes(
        self,
        session_connections: dict[str, object],
        selected_connections: list[object],
    ) -> None:
        if not selected_connections:
            return
        selected_ids = {id(connection) for connection in selected_connections}
        for session_id, connection in tuple(session_connections.items()):
            if id(connection) in selected_ids:
                session_connections.pop(session_id, None)

    async def _disconnect_connection_keys(
        self,
        connections: dict[str, object],
        keys: tuple[str, ...],
    ) -> None:
        if not keys:
            return

        selected_connections: list[object] = []
        seen: set[int] = set()
        for key in keys:
            connection = connections.pop(key, None)
            if connection is None:
                continue
            identity = id(connection)
            if identity in seen:
                continue
            seen.add(identity)
            selected_connections.append(connection)

        for connection in selected_connections:
            disconnect = getattr(connection, "disconnect", None)
            if callable(disconnect):
                await disconnect()
        self._drop_connection_pn_indexes(self._connections_by_pn, selected_connections)
        self._drop_connection_pn_indexes(self._at_connections_by_pn, selected_connections)
        self._drop_connection_session_indexes(
            self._session_payload_connections,
            selected_connections,
        )
        self._drop_connection_session_indexes(
            self._session_at_connections,
            selected_connections,
        )

    async def _disconnect_matching_session_connections(
        self,
        session_connections: dict[str, object],
        *,
        collector_ip: str,
        collector_pn: str,
        preserve_session_id: str,
    ) -> None:
        """Close every exact physical session belonging to one released route."""

        selected: list[object] = []
        seen: set[int] = set()
        for session_id, connection in tuple(session_connections.items()):
            if preserve_session_id and session_id == preserve_session_id:
                continue
            entry = self._session_inventory.get(session_id)
            if entry is None:
                continue
            matches = False
            if collector_pn:
                matches = self._collector_pn_matches(
                    collector_pn,
                    str(entry.collector_pn or "").strip(),
                )
            elif collector_ip:
                matches = self._callback_ip_matches_collector(
                    collector_ip,
                    str(entry.remote_ip or "").strip(),
                )
            if not matches or id(connection) in seen:
                continue
            seen.add(id(connection))
            selected.append(connection)

        for connection in selected:
            disconnect = getattr(connection, "disconnect", None)
            if callable(disconnect):
                await disconnect()
            # ``run()`` normally invokes this through its disconnect callback;
            # repeat it idempotently so cancellation/teardown ordering cannot
            # leave a now-unowned exact-session index behind.
            self._drop_connection_indexes_for_connection(connection)

    async def release_collector_connections(
        self,
        collector_ip: str,
        collector_pn: str = "",
        *,
        close_payload: bool = False,
        close_at: bool = False,
        close_pending: bool = False,
        preserve_session_id: str = "",
    ) -> None:
        if not collector_ip and not collector_pn:
            return

        if (
            close_payload
            and not collector_pn
            and self._has_owner_for_remote_ip(self._payload_owner_counts, collector_ip)
        ):
            close_payload = False
            close_pending = False
        if close_payload and self._has_owner_for_collector_pn(
            self._payload_pn_owner_counts,
            collector_pn,
        ):
            close_payload = False
            close_pending = False
        if (
            close_at
            and not collector_pn
            and self._has_owner_for_remote_ip(self._at_owner_counts, collector_ip)
        ):
            close_at = False
        if close_at and self._has_owner_for_collector_pn(
            self._at_pn_owner_counts,
            collector_pn,
        ):
            close_at = False

        preserve_session_id = str(preserve_session_id or "").strip()

        if close_pending and collector_ip:
            for pending in tuple(self._pending_sockets.values()):
                if preserve_session_id and pending.session_id == preserve_session_id:
                    continue
                remote_ip = pending.remote_ip
                if not self._callback_ip_matches_collector(collector_ip, remote_ip):
                    continue
                self._remove_pending_socket(pending)
                await self._close_pending_socket(pending)

        if close_payload:
            await self._disconnect_matching_session_connections(
                self._session_payload_connections,
                collector_ip=collector_ip,
                collector_pn=collector_pn,
                preserve_session_id=preserve_session_id,
            )
            payload_keys = set()
            if collector_pn:
                payload_keys.update(
                    self._connection_keys_for_collector_pn(
                        collector_pn,
                        self._connections_by_pn,
                        self._connections,
                    )
                )
            if not payload_keys:
                payload_keys.update(
                    self._connection_keys_for_collector(collector_ip, self._connections)
                )
            if preserve_session_id:
                preserved = self._session_payload_connections.get(preserve_session_id)
                if preserved is not None:
                    payload_keys = {
                        key
                        for key in payload_keys
                        if self._connections.get(key) is not preserved
                    }
            await self._disconnect_connection_keys(
                self._connections,
                tuple(payload_keys),
            )
            if collector_ip and self._callback_ip_matches_collector(collector_ip, self._last_connection_ip):
                self._last_connection_ip = ""

        if close_at:
            await self._disconnect_matching_session_connections(
                self._session_at_connections,
                collector_ip=collector_ip,
                collector_pn=collector_pn,
                preserve_session_id=preserve_session_id,
            )
            at_keys = set()
            if collector_pn:
                at_keys.update(
                    self._connection_keys_for_collector_pn(
                        collector_pn,
                        self._at_connections_by_pn,
                        self._at_connections,
                    )
                )
            if not at_keys:
                at_keys.update(
                    self._connection_keys_for_collector(collector_ip, self._at_connections)
                )
            await self._disconnect_connection_keys(
                self._at_connections,
                tuple(at_keys),
            )
            if collector_ip and self._callback_ip_matches_collector(collector_ip, self._last_at_connection_ip):
                self._last_at_connection_ip = ""

        if close_pending and collector_ip and self._callback_ip_matches_collector(collector_ip, self._last_pending_ip):
            self._last_pending_ip = ""

    async def activate_pending_at_connection(
        self,
        pending: _PendingCollectorSocket,
        *,
        collector_ip: str,
        collector_pn: str = "",
        write_timeout: float,
        raw_passthrough_bootstrap: str = "",
        raw_passthrough_frame_format: str = "",
        raw_passthrough_min_interval_ms: int = 0,
    ) -> _CollectorAtConnection:
        remote_ip = pending.remote_ip
        normalized_pn = str(collector_pn or "").strip()
        if normalized_pn:
            connection = self._connection_by_collector_pn(
                normalized_pn,
                self._at_connections_by_pn,
            )
        else:
            connection = self._at_connections.get(remote_ip)
        if connection is None and not normalized_pn:
            connection = self._resolve_public_placeholder_alias(
                remote_ip,
                connections=self._at_connections,
            )
        if connection is None or not self._connection_is_unbound_placeholder(
            connection,
            self._session_at_connections,
        ):
            connection = _CollectorAtConnection(
                remote_ip_hint=remote_ip,
                write_timeout=write_timeout,
                raw_passthrough_bootstrap=raw_passthrough_bootstrap,
                raw_passthrough_frame_format=raw_passthrough_frame_format,
                raw_passthrough_min_interval_ms=raw_passthrough_min_interval_ms,
            )
        else:
            connection.set_write_timeout(write_timeout)
            connection.set_raw_passthrough_bootstrap(raw_passthrough_bootstrap)
            connection.set_raw_passthrough_frame_format(raw_passthrough_frame_format)
            connection.set_raw_passthrough_min_interval_ms(raw_passthrough_min_interval_ms)
        _seed_connection_collector_pn(connection, normalized_pn)

        if normalized_pn:
            self._at_connections_by_pn[normalized_pn] = connection
        else:
            self._at_connections[remote_ip] = connection
        if not normalized_pn and collector_ip and collector_ip not in self._at_connections:
            self._at_connections[collector_ip] = connection
        if pending.session_id:
            self._session_at_connections[pending.session_id] = connection
        self._last_at_connection_ip = remote_ip
        self._mark_session_state(pending.session_id, "routed_at_text")
        initial_bytes = pending.initial_bytes
        pending.initial_bytes = b""
        _spawn_tracked_task(
            connection.run(
                pending.reader,
                pending.writer,
                initial_bytes=initial_bytes,
                session_id=pending.session_id,
                session_identity_callback=self._mark_session_identity,
                session_closed_callback=self._mark_socket_session_closed,
                disconnect_callback=self._drop_connection_indexes_for_connection,
            ),
            name=f"collector_at_{remote_ip}",
        )
        await connection.wait_until_connected(timeout=0.1)
        return connection

    async def activate_pending_connection(
        self,
        pending: _PendingCollectorSocket,
        *,
        collector_ip: str,
        collector_pn: str = "",
        heartbeat_interval: float,
        write_timeout: float,
    ) -> _CollectorConnection:
        remote_ip = pending.remote_ip
        normalized_pn = str(collector_pn or "").strip()
        if normalized_pn:
            connection = self._connection_by_collector_pn(
                normalized_pn,
                self._connections_by_pn,
            )
        else:
            connection = self._connections.get(remote_ip)
        if connection is None and not normalized_pn:
            connection = self._resolve_public_placeholder_alias(remote_ip)
        if connection is None or not self._connection_is_unbound_placeholder(
            connection,
            self._session_payload_connections,
        ):
            connection = _CollectorConnection(
                remote_ip_hint=remote_ip,
                heartbeat_interval=heartbeat_interval,
                write_timeout=write_timeout,
            )
        else:
            connection.set_heartbeat_interval(heartbeat_interval)
            connection.set_write_timeout(write_timeout)
        _seed_connection_collector_pn(connection, normalized_pn)

        if normalized_pn:
            self._connections_by_pn[normalized_pn] = connection
        else:
            self._connections[remote_ip] = connection
        if not normalized_pn and collector_ip and collector_ip not in self._connections:
            self._connections[collector_ip] = connection
        if pending.session_id:
            self._session_payload_connections[pending.session_id] = connection
        self._last_connection_ip = remote_ip
        self._mark_session_state(pending.session_id, "routed_framed")
        initial_bytes = pending.initial_bytes
        pending.initial_bytes = b""
        _spawn_tracked_task(
            connection.run(
                pending.reader,
                pending.writer,
                initial_bytes=initial_bytes,
                session_id=pending.session_id,
                session_identity_callback=self._mark_session_identity,
                session_closed_callback=self._mark_socket_session_closed,
                disconnect_callback=self._drop_connection_indexes_for_connection,
            ),
            name=f"collector_framed_{remote_ip}",
        )
        await connection.wait_until_connected(timeout=0.1)
        return connection

    async def _read_pending_initial_chunk(
        self,
        pending: _PendingCollectorSocket,
    ) -> tuple[bytes, bool]:
        """Return enough bytes to identify the first bounded framed record.

        TCP can split the EyeBond payload after a complete header.  If routing
        sees only that header, a heartbeat has no readable PN and the socket is
        parked ownerless until the next heartbeat (commonly 60 seconds).  Once
        a valid header is available, accumulate the rest of the first frame
        before identity/routing decisions.  This is generic stream framing and
        does not depend on collector type, endpoint, or peer IP.
        """

        if pending.initial_bytes:
            chunk = pending.initial_bytes
            pending.initial_bytes = b""
        else:
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        pending.reader.read(64),
                        timeout=0.25,
                    )
                    break
                except asyncio.TimeoutError:
                    if self._reserved_for_transparent_route(pending):
                        # Keep passively observing every ambiguous fresh socket.
                        # A framed HA callback will normally identify its wire
                        # with a heartbeat; the still-silent cloud socket can
                        # then become the only eligible AT route.  Returning
                        # here would park both as permanently ``unknown`` and
                        # deadlock the selector.
                        self._mark_session_state(
                            pending.session_id,
                            "exclusive_route_silent",
                        )
                        continue
                    chunk = await self._async_probe_pending_identity(pending)
                    return chunk, False
                except Exception:
                    return b"", True
        if not chunk:
            return chunk, True

        if len(chunk) >= HEADER_SIZE:
            try:
                header = decode_header(chunk[:HEADER_SIZE])
            except Exception:
                header = None
            if header is not None:
                frame_len = HEADER_SIZE + max(int(header.payload_len), 0)
                if (
                    HEADER_SIZE <= frame_len <= HEADER_SIZE + _AT_TEXT_MAX_MIXED_FRAME_PAYLOAD_LEN
                    and len(chunk) < frame_len
                ):
                    try:
                        chunk += await asyncio.wait_for(
                            pending.reader.readexactly(frame_len - len(chunk)),
                            timeout=0.5,
                        )
                    except asyncio.IncompleteReadError as exc:
                        chunk += exc.partial
                        return chunk, True
                    except asyncio.TimeoutError:
                        return chunk, False
                    except (ConnectionResetError, OSError):
                        return chunk, True
        return chunk, False

    async def _async_probe_pending_identity(self, pending: _PendingCollectorSocket) -> bytes:
        session_protocol = self._single_registered_session_protocol()
        request = build_identity_probe_request(session_protocol)
        if request is None:
            self._mark_session_state(pending.session_id, "waiting_for_identity")
            return b""

        self._mark_session_state(pending.session_id, f"probing_identity_{session_protocol}")
        try:
            pending.writer.write(request.payload)
            await asyncio.wait_for(pending.writer.drain(), timeout=1.5)
            return await asyncio.wait_for(pending.reader.read(64), timeout=1.5)
        except asyncio.TimeoutError:
            self._mark_session_state(pending.session_id, "identity_probe_timeout")
            return b""
        except Exception:
            self._mark_session_state(pending.session_id, "identity_probe_failed")
            return b""

    async def _identify_pending_socket_for_route(
        self,
        pending: _PendingCollectorSocket,
        *,
        session_protocol: str = "",
        identity_probe_kind: str = "",
    ) -> str:
        if not self._pending_socket_still_registered(pending):
            return ""

        entry = self._session_inventory.get(pending.session_id)
        known_pn = str(getattr(entry, "collector_pn", "") or "").strip()
        known_source = str(
            getattr(entry, "collector_identity_source", "") or ""
        ).strip()
        if known_pn and identity_source_is_strong(known_source):
            return known_pn

        try:
            chunk = await asyncio.wait_for(pending.reader.read(64), timeout=0.25)
        except asyncio.TimeoutError:
            chunk = b""
        except Exception:
            self._mark_session_state(pending.session_id, "route_identity_read_failed")
            return ""

        # A WEAK identity read from the first bytes (e.g. a framed heartbeat's
        # SHORT PN) is recorded but does NOT end the read: it is UPGRADED to the
        # strong/full PN by the identity query below. An already-STRONG chunk
        # (fc2_parameter_2 / at_dtupn) is authoritative and returns immediately.
        weak_pn = ""
        if chunk:
            pending.initial_bytes += chunk
            self._mark_session_first_bytes(pending.session_id, chunk)
            collector_pn, source = _collector_pn_from_initial_chunk(chunk)
            if collector_pn:
                self._mark_session_identity(pending.session_id, collector_pn, source)
                if identity_source_is_strong(source):
                    return collector_pn
                weak_pn = collector_pn

        protocol = str(session_protocol or "").strip().lower()
        if not protocol:
            protocol = self._single_registered_session_protocol()
        request = build_identity_probe_request(
            protocol,
            probe_kind=identity_probe_kind,
        )
        if request is None:
            # No wire to upgrade with: keep whatever weak identity we already read.
            return weak_pn

        self._mark_session_state(
            pending.session_id,
            f"probing_route_identity_{request.probe_kind}",
        )
        try:
            pending.writer.write(request.payload)
            await asyncio.wait_for(pending.writer.drain(), timeout=1.5)
            response, collector_pn, source = await self._read_identity_probe_response(
                pending,
                request,
                timeout=1.5,
            )
        except asyncio.TimeoutError:
            self._mark_session_state(pending.session_id, "route_identity_probe_timeout")
            return weak_pn
        except Exception:
            self._mark_session_state(pending.session_id, "route_identity_probe_failed")
            return weak_pn

        if collector_pn:
            self._mark_session_first_bytes(pending.session_id, response)
            self._mark_session_identity(pending.session_id, collector_pn, source)
            return collector_pn
        # The upgrade produced nothing usable: never LOSE the weak identity.
        return weak_pn

    async def _read_identity_probe_response(
        self,
        pending: _PendingCollectorSocket,
        request: IdentityProbeRequest,
        *,
        timeout: float,
    ) -> tuple[bytes, str, str]:
        """Read one correlated response while preserving useful observations.

        A framed collector can emit an unsolicited heartbeat immediately before
        answering our request.  Record such a frame, but continue until the
        request's exact TID/function response arrives or the shared deadline
        expires.  AT remains line-oriented and uses its existing bounded read.
        """

        if request.session_protocol == "at_text":
            response = await asyncio.wait_for(
                pending.reader.read(128),
                timeout=max(0.0, float(timeout)),
            )
            pn, source = parse_identity_probe_response(request, response)
            return response, pn, source

        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(timeout))
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            header_bytes = await asyncio.wait_for(
                pending.reader.readexactly(HEADER_SIZE),
                timeout=remaining,
            )
            try:
                header = decode_header(header_bytes)
            except Exception:
                return header_bytes, "", ""
            if header.total_len < HEADER_SIZE or header.total_len > 4096:
                return header_bytes, "", ""
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            payload = await asyncio.wait_for(
                pending.reader.readexactly(header.payload_len),
                timeout=remaining,
            )
            frame = header_bytes + payload
            self._mark_session_first_bytes(pending.session_id, frame)

            # Preserve volunteered strong/weak observations even when this is
            # not the response correlated to our request.
            observed_pn, observed_source = _collector_pn_from_initial_chunk(frame)
            if observed_pn:
                self._mark_session_identity(
                    pending.session_id,
                    observed_pn,
                    observed_source,
                )
                if identity_source_is_strong(observed_source):
                    return frame, observed_pn, observed_source

            pn, source = parse_identity_probe_response(request, frame)
            if pn:
                return frame, pn, source

    async def _sniff_pending_socket(self, pending: _PendingCollectorSocket) -> None:
        chunk, exhausted = await self._read_pending_initial_chunk(pending)

        if not self._pending_socket_still_registered(pending):
            return

        if not chunk:
            if not exhausted:
                # No identity yet, but the socket must stay WATCHED: an
                # unwatched registered socket never notices a peer close, and
                # a dead entry blocks same-IP routing as a phantom duplicate.
                await self._park_unclaimed_pending_socket(
                    pending,
                    b"",
                    session_state=(
                        "waiting_for_exclusive_route"
                        if self._reserved_for_transparent_route(pending)
                        else "parked_waiting_for_identity"
                    ),
                )
                return
            self._remove_pending_socket(pending)
            if self._last_pending_ip == pending.remote_ip:
                self._last_pending_ip = ""
            self._mark_session_state(pending.session_id, "closed_no_payload")
            await _close_writer_bounded(pending.writer)
            return

        self._mark_session_first_bytes(pending.session_id, chunk)
        protocol_shape = _classify_initial_protocol_shape(chunk)
        if protocol_shape == "unknown" and 0 < len(chunk) < HEADER_SIZE:
            self._mark_session_state(
                pending.session_id,
                "waiting_for_more_initial_bytes",
            )
            await self._park_unclaimed_pending_socket(
                pending,
                chunk,
                session_state="waiting_for_more_initial_bytes",
            )
            return

        self._remove_pending_socket(pending)
        if self._last_pending_ip == pending.remote_ip:
            self._last_pending_ip = ""

        initial_pn, initial_pn_source = _collector_pn_from_initial_chunk(chunk)
        if initial_pn:
            self._mark_session_identity(
                pending.session_id,
                initial_pn,
                initial_pn_source,
            )
        identity_can_route = bool(
            initial_pn and identity_source_is_strong(initial_pn_source)
        )
        if self._matches_exclusive_collector_route(
            remote_ip=pending.remote_ip,
            observed_pn=initial_pn,
            protocol_shape=protocol_shape,
        ):
            await self._park_unclaimed_pending_socket(
                pending,
                chunk,
                session_state="waiting_for_exclusive_route",
            )
            return

        route_at = protocol_shape == "at_text"
        # Byte-shape is the only safe authority for explicit framed-vs-AT wire
        # observations. Owners/PN/entry metadata decide who may claim a socket,
        # not what wire the socket carries. Raw bytes are intentionally
        # ambiguous: a raw-passthrough AT stream can start with inverter bytes
        # rather than AT+, so for raw_tcp we allow a single registered owner to
        # choose the activation facade. A plausible EyeBond frame is never
        # downgraded to routed_at_text by an AT owner.
        framed_owner = (
            identity_can_route
            and self._has_owner_for_collector_pn(
                self._payload_pn_owner_counts, initial_pn
            )
        ) or (
            not initial_pn
            and self._has_owner_for_remote_ip(
                self._payload_owner_counts, pending.remote_ip
            )
        )
        at_owner = (
            identity_can_route
            and self._has_owner_for_collector_pn(
                self._at_pn_owner_counts, initial_pn
            )
        ) or (
            not initial_pn
            and self._has_owner_for_remote_ip(self._at_owner_counts, pending.remote_ip)
        )
        if protocol_shape == "eybond_framed":
            route_at = False
        elif protocol_shape == "raw_tcp":
            if at_owner and not framed_owner:
                route_at = True
            elif framed_owner and not at_owner:
                route_at = False
            elif at_owner:
                route_at = True

        if route_at:
            connection = None
            if initial_pn and identity_can_route:
                connection = self._connection_by_collector_pn(
                    initial_pn,
                    self._at_connections_by_pn,
                )
            if connection is None and not initial_pn:
                connection = self._at_connections.get(pending.remote_ip)
            if connection is None and not initial_pn:
                connection = self._resolve_public_placeholder_alias(
                    pending.remote_ip,
                    connections=self._at_connections,
                )
            if connection is None or not self._connection_is_unbound_placeholder(
                connection,
                self._session_at_connections,
            ):
                has_ip_owner = bool(
                    not initial_pn
                    and self._has_owner_for_remote_ip(
                        self._at_owner_counts,
                        pending.remote_ip,
                    )
                )
                has_pn_owner = bool(
                    identity_can_route
                    and self._has_owner_for_collector_pn(
                        self._at_pn_owner_counts,
                        initial_pn,
                    )
                )
                if not has_ip_owner and not has_pn_owner:
                    await self._park_unclaimed_pending_socket(
                        pending,
                        chunk,
                        session_state="parked_no_at_owner",
                    )
                    return
                connection = _CollectorAtConnection(
                    remote_ip_hint=pending.remote_ip,
                    write_timeout=1.5,
                )
            else:
                connection.set_write_timeout(1.5)
            self._at_connections[pending.remote_ip] = connection
            if pending.session_id:
                self._session_at_connections[pending.session_id] = connection
                if initial_pn:
                    self._mark_session_identity(
                        pending.session_id,
                        initial_pn,
                        initial_pn_source,
                    )
            self._last_at_connection_ip = pending.remote_ip
            self._mark_session_state(pending.session_id, "routed_at_text")
            await connection.run(
                pending.reader,
                pending.writer,
                initial_bytes=chunk,
                session_id=pending.session_id,
                session_identity_callback=self._mark_session_identity,
                session_closed_callback=self._mark_socket_session_closed,
                disconnect_callback=self._drop_connection_indexes_for_connection,
            )
            return

        connection = None
        if initial_pn and identity_can_route:
            connection = self._connection_by_collector_pn(
                initial_pn,
                self._connections_by_pn,
            )
        if connection is None and not initial_pn:
            connection = self._connections.get(pending.remote_ip)
        if connection is None and not initial_pn:
            connection = self._resolve_public_placeholder_alias(pending.remote_ip)
        if connection is None or not self._connection_is_unbound_placeholder(
            connection,
            self._session_payload_connections,
        ):
            has_ip_owner = bool(
                not initial_pn
                and self._has_owner_for_remote_ip(
                    self._payload_owner_counts,
                    pending.remote_ip,
                )
            )
            has_pn_owner = bool(
                identity_can_route
                and self._has_owner_for_collector_pn(
                    self._payload_pn_owner_counts,
                    initial_pn,
                )
            )
            if not has_ip_owner and not has_pn_owner:
                await self._park_unclaimed_pending_socket(
                    pending,
                    chunk,
                    session_state="parked_no_payload_owner",
                )
                return
            connection = _CollectorConnection(
                remote_ip_hint=pending.remote_ip,
                heartbeat_interval=60.0,
                write_timeout=1.5,
            )
        else:
            connection.set_heartbeat_interval(60.0)
            connection.set_write_timeout(1.5)
        self._connections[pending.remote_ip] = connection
        if pending.session_id:
            self._session_payload_connections[pending.session_id] = connection
            if initial_pn:
                self._mark_session_identity(
                    pending.session_id,
                    initial_pn,
                    initial_pn_source,
                )
        self._last_connection_ip = pending.remote_ip
        self._mark_session_state(pending.session_id, "routed_framed")
        await connection.run(
            pending.reader,
            pending.writer,
            initial_bytes=chunk,
            session_id=pending.session_id,
            session_identity_callback=self._mark_session_identity,
            session_closed_callback=self._mark_socket_session_closed,
            disconnect_callback=self._drop_connection_indexes_for_connection,
        )

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername") or ("", None)
        remote_ip = peer[0] or ""
        remote_port = peer[1]
        if not remote_ip:
            await _close_writer_bounded(writer)
            return

        session_id = self._next_session_id()
        self._remember_session(
            session_id=session_id,
            remote_ip=remote_ip,
            remote_port=remote_port if isinstance(remote_port, int) else None,
        )
        pending = _PendingCollectorSocket(
            session_id=session_id,
            remote_ip=remote_ip,
            remote_port=remote_port if isinstance(remote_port, int) else None,
            reader=reader,
            writer=writer,
        )
        self._pending_sockets[session_id] = pending
        self._last_pending_ip = remote_ip
        pending.sniff_task = _spawn_tracked_task(
            self._sniff_pending_socket(pending),
            name=f"collector_pending_sniff_{remote_ip}",
        )
        self._notify_connection_watchers(remote_ip)


_LISTENERS: dict[tuple[str, int], _SharedEybondListener] = {}
_LISTENERS_LOCK = asyncio.Lock()
_WILDCARD_BIND_HOSTS = ("0.0.0.0", "")


def _resolve_registered_listener(host: str, port: int) -> _SharedEybondListener | None:
    """Return a registered listener that already serves ``host:port``.

    A wildcard listener (bound on 0.0.0.0) accepts connections for every local
    address, so a request for a specific host on the same port must REUSE it:
    binding the specific address while the wildcard socket holds the port fails
    with EADDRINUSE. The runtime binds its callback listener on 0.0.0.0 while
    options-flow helpers historically ask for the entry's server IP — without
    this fallback those helpers cannot run while the runtime is up (the
    collector Wi-Fi change regression).
    """

    listener = _LISTENERS.get((host, int(port)))
    if listener is not None:
        return listener
    if host not in _WILDCARD_BIND_HOSTS:
        for wildcard in _WILDCARD_BIND_HOSTS:
            listener = _LISTENERS.get((wildcard, int(port)))
            if listener is not None:
                return listener
    return None


async def _acquire_listener_locked(host: str, port: int) -> _SharedEybondListener:
    """Get-or-create + acquire one shared listener. Caller holds _LISTENERS_LOCK."""

    listener = _resolve_registered_listener(host, port)
    if listener is None:
        listener = _SharedEybondListener(host=host, port=port)
        _LISTENERS[(host, int(port))] = listener
    try:
        await listener.acquire()
    except BaseException:
        # Cancellation included: drop a never-bound, unreferenced listener so a
        # cancelled acquire leaks neither a refcount nor a registry entry.
        if listener._server is None and listener._ref_count == 0:
            _LISTENERS.pop((listener._host, listener._port), None)
        raise
    return listener


async def _acquire_shared_listener(host: str, port: int) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        return await _acquire_listener_locked(host, port)


async def _acquire_shared_payload_listener(
    host: str,
    port: int,
    collector_ip: str,
    collector_pn: str = "",
    collector_session_protocol: str = "",
) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        listener = await _acquire_listener_locked(host, port)
        listener.register_payload_owner(collector_ip)
        listener.register_payload_pn_owner(collector_pn)
        listener.register_session_protocol_owner(collector_session_protocol)
        return listener


async def _acquire_shared_at_listener(
    host: str,
    port: int,
    collector_ip: str,
    collector_pn: str = "",
    collector_session_protocol: str = "",
) -> _SharedEybondListener:
    async with _LISTENERS_LOCK:
        listener = await _acquire_listener_locked(host, port)
        listener.register_at_owner(collector_ip)
        listener.register_at_pn_owner(collector_pn)
        listener.register_session_protocol_owner(collector_session_protocol)
        return listener


async def _release_shared_listener(
    listener: _SharedEybondListener,
    *,
    collector_ip: str = "",
    collector_pn: str = "",
    collector_session_protocol: str = "",
    close_payload: bool = False,
    close_at: bool = False,
    close_pending: bool = False,
    unregister_payload_owner: bool = False,
    unregister_payload_pn_owner: bool = False,
    unregister_at_owner: bool = False,
    unregister_at_pn_owner: bool = False,
    unregister_session_protocol_owner: bool = False,
    preserve_session_id: str = "",
) -> None:
    async def _release() -> None:
        async with _LISTENERS_LOCK:
            key = (listener._host, listener._port)
            if unregister_payload_owner:
                listener.unregister_payload_owner(collector_ip)
            if unregister_payload_pn_owner:
                listener.unregister_payload_pn_owner(collector_pn)
            if unregister_at_owner:
                listener.unregister_at_owner(collector_ip)
            if unregister_at_pn_owner:
                listener.unregister_at_pn_owner(collector_pn)
            if unregister_session_protocol_owner:
                listener.unregister_session_protocol_owner(collector_session_protocol)
            await listener.release_collector_connections(
                collector_ip,
                collector_pn,
                close_payload=close_payload,
                close_at=close_at,
                close_pending=close_pending,
                preserve_session_id=preserve_session_id,
            )
            closed = await listener.release()
            if closed:
                _LISTENERS.pop(key, None)

    await _finish_cleanup_on_cancel(_release())
