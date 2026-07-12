"""Runtime session handle + adapter negotiation for a claimed inbound session.

Phase 1 gave every config entry explicit connection axes and an ownership
ledger. Phase 2 adds the missing runtime abstraction: once an inbound collector
socket is accepted and owned by an entry, the runtime must talk to it through
the adapter the *live* session actually supports -- not the adapter implied by a
stale persisted ``collector_session_protocol`` hint.

A :class:`SessionHandle` describes one claimed inbound session and the adapters
negotiated for it from **safe, non-mutating observation only** (the byte-shape
the listener already sniffed, the routed state, and the framed/AT identity
source). It never writes the collector endpoint or UART settings, and it never
infers transport from hostname, endpoint, cloud family, peer IP, or collector
type. ESP-collector identity (FC=2 param 6 = ``esp-collector/...``) is a
capability marker, never the transport switch -- the switch is the observed wire.

Adapters (what the payload/driver can ride on this session):

- ``framed_forward`` -- forward device payloads through the framed EyeBond tunnel
  (FC forward). Used by SMG/modbus-like drivers and PI30 ASCII behind an ESP
  bridge.
- ``framed_collector_commands`` -- framed collector FC queries (PN / endpoint).
- ``at_commands`` -- SmartESS AT-text collector commands.
- ``raw_passthrough`` -- raw serial passthrough over the AT-text session, used by
  G-ASCII / ValueCloud style drivers.

The negotiated *wire* is the one thing runtime transport selection turns on:
``framed`` (use the framed transport) vs ``at_text`` (use the AT transport) vs
``""`` (nothing observed yet -> fall back to the persisted hint).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

# Adapter identifiers. Names are stable strings so drivers/tests can reference
# them without importing the transport layer. The role prefix is intentional:
# identity, collector management, inverter forwarding, and proxying are separate
# capabilities even when they share one TCP socket.
ADAPTER_COLLECTOR_FRAMED_COMMANDS = "framed_collector_commands"
ADAPTER_COLLECTOR_AT_COMMANDS = "at_commands"
ADAPTER_INVERTER_FRAMED_FC4 = "framed_fc4"
ADAPTER_INVERTER_RAW_PASSTHROUGH = "raw_passthrough"
ADAPTER_INVERTER_NATIVE_MODBUS_TCP = "native_modbus_tcp"
ADAPTER_PROXY_FRAMED_CLOUD = "framed_cloud_proxy"
ADAPTER_PROXY_RAW_TCP = "raw_tcp_proxy"
ADAPTER_NONE = "none"

# Backwards-compatible aliases used by older tests/callers. Keep them as aliases
# of the new explicit adapter roles, not as separate concepts.
ADAPTER_FRAMED_FORWARD = ADAPTER_INVERTER_FRAMED_FC4
ADAPTER_FRAMED_COLLECTOR_COMMANDS = ADAPTER_COLLECTOR_FRAMED_COMMANDS
ADAPTER_AT_COMMANDS = ADAPTER_COLLECTOR_AT_COMMANDS
ADAPTER_RAW_PASSTHROUGH = ADAPTER_INVERTER_RAW_PASSTHROUGH

# Negotiated live wire values.
WIRE_FRAMED = "eybond_framed"
WIRE_AT_TEXT = "at_text"
WIRE_RAW_TCP = "raw_tcp"
WIRE_UNKNOWN = "unknown"

# Observed signals that mean the live session is a framed EyeBond tunnel.
_FRAMED_STATES = frozenset({"routed_framed"})
_FRAMED_SHAPES = frozenset({"eybond_framed_or_binary", "eybond_framed"})
# Observed signals that mean the live session is a SmartESS AT-text session.
_AT_STATES = frozenset({"routed_at_text"})
_AT_SHAPES = frozenset({"at_text"})
_RAW_TCP_SHAPES = frozenset({"raw_tcp"})

# Listener states that are NOT trustworthy as wire truth: a route-identity
# mismatch or a socket still awaiting identity confirmation has not established
# an owned wire, so its sniffed byte shape must never drive runtime transport
# selection. Such a session negotiates to WIRE_UNKNOWN regardless of its shape.
_UNTRUSTED_STATES = frozenset(
    {
        "route_identity_mismatch",
        "waiting_for_route_identity",
        "parked_waiting_for_identity",
        "closed_no_payload",
    }
)

# Adapter sets per negotiated wire. A framed wire carries framed forward + framed
# collector commands. An AT-text wire carries AT commands + raw passthrough, and
# can also carry a single framed collector-command probe (FC over AT) which the
# transport uses to read the ESP bridge identity without switching the wire.
_TRANSPORT_WIRE_BY_WIRE_FRAMING: dict[str, str] = {
    WIRE_FRAMED: WIRE_FRAMED,
    WIRE_AT_TEXT: WIRE_AT_TEXT,
    # ``raw_tcp`` means inverter payload bytes are raw on the stream, but the
    # current shared runtime still claims that stream through the AT/raw facade.
    # Keep this mapping explicit so raw TCP does not look like framed.
    WIRE_RAW_TCP: WIRE_AT_TEXT,
    WIRE_UNKNOWN: "",
}


@dataclass(frozen=True, slots=True)
class SessionHandle:
    """One entry's claimed inbound session and its negotiated live adapters."""

    session_id: str = ""
    collector_pn: str = ""
    peer_ip: str = ""  # diagnostic / display only, never identity
    listener_port: int = 0
    wire_framing: str = WIRE_UNKNOWN
    identity_sources: frozenset[str] = field(default_factory=frozenset)
    collector_management_adapter: str = ADAPTER_NONE
    inverter_forward_adapter: str = ADAPTER_NONE
    proxy_adapter: str = ADAPTER_NONE
    conflict: str = ""
    state: str = ""

    @property
    def wire(self) -> str:
        """Backward-compatible negotiated wire value."""

        return self.wire_framing

    @property
    def payload_wire(self) -> str:
        """Return the live wire the payload must ride."""

        return self.wire_framing

    @property
    def transport_wire(self) -> str:
        """Return the legacy transport selector for the current implementation.

        The high-level model exposes ``wire_framing``. The existing shared
        transport still takes a coarse selector ("eybond_framed"/"at_text"/"").
        This adapter keeps that conversion explicit and localized.
        """

        return _TRANSPORT_WIRE_BY_WIRE_FRAMING.get(self.wire_framing, "")

    @property
    def identity_source(self) -> str:
        """Backward-compatible primary identity source."""

        return next(iter(sorted(self.identity_sources)), "")

    @property
    def available_adapters(self) -> frozenset[str]:
        """Return all non-empty negotiated adapters."""

        return frozenset(
            adapter
            for adapter in (
                self.collector_management_adapter,
                self.inverter_forward_adapter,
                self.proxy_adapter,
            )
            if adapter and adapter != ADAPTER_NONE
        )

    @property
    def observed(self) -> bool:
        """Return whether a live wire has actually been observed for this session."""

        return self.wire_framing in (WIRE_FRAMED, WIRE_AT_TEXT, WIRE_RAW_TCP)

    @property
    def uses_at_text_wire(self) -> bool:
        return self.wire_framing == WIRE_AT_TEXT

    @property
    def uses_framed_wire(self) -> bool:
        return self.wire_framing == WIRE_FRAMED

    @property
    def uses_raw_tcp_wire(self) -> bool:
        return self.wire_framing == WIRE_RAW_TCP

    def supports(self, adapter: str) -> bool:
        """Return whether the live session supports one adapter."""

        return adapter in self.available_adapters


@dataclass(frozen=True, slots=True)
class ConfirmedWireBinding:
    """The last confirmed live wire for a collector, decoupled from any socket.

    A ``SessionHandle`` always describes a CURRENT real session (its transient
    ``session_id`` / ``peer_ip`` / ``listener_port`` / live ``state`` /
    ``observed`` flag). Storing a whole SessionHandle as the "confirmed binding"
    would carry stale socket metadata across a reconnect. This immutable model
    carries ONLY the durable wire facts that a same-collector session handover
    must preserve -- never a socket identity or a live-observation flag. It is
    adopted from a trusted observed handle by an explicit lifecycle step, never
    as a side effect of reading diagnostics.
    """

    collector_pn: str
    wire_framing: str
    collector_management_adapter: str = ADAPTER_NONE
    inverter_forward_adapter: str = ADAPTER_NONE
    proxy_adapter: str = ADAPTER_NONE
    identity_sources: frozenset[str] = field(default_factory=frozenset)

    @property
    def uses_framed_wire(self) -> bool:
        return self.wire_framing == WIRE_FRAMED

    @property
    def uses_at_text_wire(self) -> bool:
        return self.wire_framing == WIRE_AT_TEXT

    @property
    def uses_raw_tcp_wire(self) -> bool:
        return self.wire_framing == WIRE_RAW_TCP

    @property
    def transport_wire(self) -> str:
        """Return the coarse transport selector for the current implementation."""

        return _TRANSPORT_WIRE_BY_WIRE_FRAMING.get(self.wire_framing, "")

    @property
    def session_protocol(self) -> str:
        """Return the legacy session-protocol string for this wire."""

        if self.wire_framing == WIRE_FRAMED:
            return "eybond_framed"
        if self.wire_framing == WIRE_AT_TEXT:
            return "at_text"
        return ""

    @classmethod
    def from_handle(
        cls,
        handle: "SessionHandle",
        *,
        collector_pn: str,
    ) -> "ConfirmedWireBinding | None":
        """Build a confirmed binding from a TRUSTED observed handle + durable PN.

        Returns ``None`` unless ALL hold:
        - a durable ``collector_pn`` is supplied (the entry's identity -- an
          entry with no durable PN can never confirm a binding);
        - the handle is observed, non-conflicting, with a real negotiated wire;
        - the handle itself carries a collector PN (an unidentified live session
          can never confirm a binding).

        The stored PN is the caller-supplied durable/preferred (fuller) PN, so a
        later short/full enrichment keeps one stable identity. The identity match
        between the durable PN and the handle's PN is enforced by the caller
        (which owns the short/full reconciliation function).
        """

        durable = str(collector_pn or "").strip()
        if not durable:
            return None
        if handle is None or not handle.observed or handle.conflict:
            return None
        if not handle.wire_framing or handle.wire_framing == WIRE_UNKNOWN:
            return None
        if not str(getattr(handle, "collector_pn", "") or "").strip():
            return None
        return cls(
            collector_pn=durable,
            wire_framing=handle.wire_framing,
            collector_management_adapter=handle.collector_management_adapter,
            inverter_forward_adapter=handle.inverter_forward_adapter,
            proxy_adapter=handle.proxy_adapter,
            identity_sources=handle.identity_sources,
        )


def _normalize_wire_signal(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _FRAMED_STATES or normalized in _FRAMED_SHAPES:
        return WIRE_FRAMED
    if normalized in _AT_STATES or normalized in _AT_SHAPES:
        return WIRE_AT_TEXT
    if normalized in _RAW_TCP_SHAPES:
        return WIRE_RAW_TCP
    if normalized == "framed":
        return WIRE_FRAMED
    return WIRE_UNKNOWN


def _adapters_for_wire(wire: str) -> tuple[str, str, str]:
    if wire == WIRE_FRAMED:
        return (
            ADAPTER_COLLECTOR_FRAMED_COMMANDS,
            ADAPTER_INVERTER_FRAMED_FC4,
            ADAPTER_PROXY_FRAMED_CLOUD,
        )
    if wire == WIRE_AT_TEXT:
        return (
            ADAPTER_COLLECTOR_AT_COMMANDS,
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
            ADAPTER_PROXY_RAW_TCP,
        )
    if wire == WIRE_RAW_TCP:
        return (
            ADAPTER_NONE,
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
            ADAPTER_PROXY_RAW_TCP,
        )
    return (ADAPTER_NONE, ADAPTER_NONE, ADAPTER_NONE)


def negotiate_wire_result(
    *,
    state: object = "",
    protocol_shape: object = "",
    session_protocol: object = "",
) -> tuple[str, str]:
    """Negotiate the live wire + conflict from safely-observed session signals.

    Precedence: routed state (authoritative -- the socket is already carrying
    that framing) > confirmed session protocol > sniffed byte shape. If routed
    state and observed shape contradict each other, return fail-closed
    ``WIRE_UNKNOWN`` with a conflict instead of silently downgrading.
    """

    normalized_state = str(state or "").strip().lower()
    if normalized_state in _UNTRUSTED_STATES:
        # Identity mismatch / not-yet-routed: no owned wire established. Do not
        # trust the sniffed shape as runtime wire truth.
        return WIRE_UNKNOWN, ""

    state_wire = _normalize_wire_signal(normalized_state)
    shape_wire = _normalize_wire_signal(protocol_shape)
    protocol_wire = _normalize_wire_signal(session_protocol)

    if (
        state_wire != WIRE_UNKNOWN
        and shape_wire != WIRE_UNKNOWN
        and state_wire != shape_wire
        # Raw bytes may be routed through the AT/raw passthrough facade. A
        # framed shape routed as AT is the dangerous impossible state we must
        # fail closed; raw_tcp routed_at_text is a valid raw-passthrough stream.
        and not (state_wire == WIRE_AT_TEXT and shape_wire == WIRE_RAW_TCP)
    ):
        return WIRE_UNKNOWN, f"wire_conflict:state={normalized_state}:shape={shape_wire}"

    if state_wire != WIRE_UNKNOWN:
        return state_wire, ""

    normalized_protocol = str(session_protocol or "").strip().lower()
    if protocol_wire != WIRE_UNKNOWN:
        if shape_wire != WIRE_UNKNOWN and protocol_wire != shape_wire:
            return shape_wire, ""
        return protocol_wire, ""

    return shape_wire, ""


def negotiate_wire(
    *,
    state: object = "",
    protocol_shape: object = "",
    session_protocol: object = "",
) -> str:
    """Backward-compatible wire negotiation helper."""

    wire, _conflict = negotiate_wire_result(
        state=state,
        protocol_shape=protocol_shape,
        session_protocol=session_protocol,
    )
    return wire


def negotiate_session_adapters(observed: Mapping[str, object] | None) -> SessionHandle:
    """Build a :class:`SessionHandle` from one observed session mapping.

    ``observed`` is the listener inventory shape (``discovered_collector_sessions``
    entry) or the registry ``CallbackSession.raw`` mapping. Detection is purely
    observational -- no probes are sent here.
    """

    if not observed:
        return SessionHandle()
    wire, conflict = negotiate_wire_result(
        state=observed.get("state"),
        protocol_shape=observed.get("protocol_shape"),
        session_protocol=observed.get("session_protocol"),
    )
    collector_adapter, inverter_adapter, proxy_adapter = _adapters_for_wire(wire)
    identity_source = str(observed.get("collector_identity_source") or "").strip()
    return SessionHandle(
        session_id=str(observed.get("session_id") or "").strip(),
        collector_pn=str(observed.get("collector_pn") or "").strip(),
        peer_ip=str(observed.get("peer_ip") or "").strip(),
        listener_port=int(observed.get("listener_port") or 0),
        wire_framing=wire,
        identity_sources=frozenset({identity_source} if identity_source else ()),
        collector_management_adapter=collector_adapter,
        inverter_forward_adapter=inverter_adapter,
        proxy_adapter=proxy_adapter,
        conflict=conflict,
        state=str(observed.get("state") or "").strip(),
    )
