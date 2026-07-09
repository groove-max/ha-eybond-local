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
# them without importing the transport layer.
ADAPTER_FRAMED_FORWARD = "framed_forward"
ADAPTER_FRAMED_COLLECTOR_COMMANDS = "framed_collector_commands"
ADAPTER_AT_COMMANDS = "at_commands"
ADAPTER_RAW_PASSTHROUGH = "raw_passthrough"

# Negotiated live wire values.
WIRE_FRAMED = "framed"
WIRE_AT_TEXT = "at_text"
WIRE_UNKNOWN = ""

# Observed signals that mean the live session is a framed EyeBond tunnel.
_FRAMED_STATES = frozenset({"routed_framed"})
_FRAMED_SHAPES = frozenset({"eybond_framed_or_binary", "eybond_framed"})
# Observed signals that mean the live session is a SmartESS AT-text session.
_AT_STATES = frozenset({"routed_at_text"})
_AT_SHAPES = frozenset({"at_text"})

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
_ADAPTERS_BY_WIRE: dict[str, frozenset[str]] = {
    WIRE_FRAMED: frozenset(
        {ADAPTER_FRAMED_FORWARD, ADAPTER_FRAMED_COLLECTOR_COMMANDS}
    ),
    WIRE_AT_TEXT: frozenset(
        {
            ADAPTER_AT_COMMANDS,
            ADAPTER_RAW_PASSTHROUGH,
            ADAPTER_FRAMED_COLLECTOR_COMMANDS,
        }
    ),
    WIRE_UNKNOWN: frozenset(),
}


@dataclass(frozen=True, slots=True)
class SessionHandle:
    """One entry's claimed inbound session and its negotiated live adapters."""

    session_id: str = ""
    collector_pn: str = ""
    peer_ip: str = ""  # diagnostic / display only, never identity
    listener_port: int = 0
    wire: str = WIRE_UNKNOWN
    available_adapters: frozenset[str] = field(default_factory=frozenset)
    identity_source: str = ""
    state: str = ""

    @property
    def payload_wire(self) -> str:
        """Return the live wire the payload must ride (``""`` when unknown)."""

        return self.wire

    @property
    def observed(self) -> bool:
        """Return whether a live wire has actually been observed for this session."""

        return self.wire in (WIRE_FRAMED, WIRE_AT_TEXT)

    @property
    def uses_at_text_wire(self) -> bool:
        return self.wire == WIRE_AT_TEXT

    @property
    def uses_framed_wire(self) -> bool:
        return self.wire == WIRE_FRAMED

    def supports(self, adapter: str) -> bool:
        """Return whether the live session supports one adapter."""

        return adapter in self.available_adapters


def negotiate_wire(
    *,
    state: object = "",
    protocol_shape: object = "",
    session_protocol: object = "",
) -> str:
    """Negotiate the live wire from safely-observed session signals.

    Precedence: routed state (authoritative -- the socket is already carrying
    that framing) > confirmed session protocol > sniffed byte shape. Returns
    ``""`` when nothing has been observed yet, so callers fall back to the
    persisted hint rather than guessing.
    """

    normalized_state = str(state or "").strip().lower()
    if normalized_state in _UNTRUSTED_STATES:
        # Identity mismatch / not-yet-routed: no owned wire established. Do not
        # trust the sniffed shape as runtime wire truth.
        return WIRE_UNKNOWN
    if normalized_state in _FRAMED_STATES:
        return WIRE_FRAMED
    if normalized_state in _AT_STATES:
        return WIRE_AT_TEXT

    normalized_protocol = str(session_protocol or "").strip().lower()
    if normalized_protocol == "eybond_framed":
        return WIRE_FRAMED
    if normalized_protocol == "at_text":
        return WIRE_AT_TEXT

    normalized_shape = str(protocol_shape or "").strip().lower()
    if normalized_shape in _FRAMED_SHAPES:
        return WIRE_FRAMED
    if normalized_shape in _AT_SHAPES:
        return WIRE_AT_TEXT
    return WIRE_UNKNOWN


def negotiate_session_adapters(observed: Mapping[str, object] | None) -> SessionHandle:
    """Build a :class:`SessionHandle` from one observed session mapping.

    ``observed`` is the listener inventory shape (``discovered_collector_sessions``
    entry) or the registry ``CallbackSession.raw`` mapping. Detection is purely
    observational -- no probes are sent here.
    """

    if not observed:
        return SessionHandle()
    wire = negotiate_wire(
        state=observed.get("state"),
        protocol_shape=observed.get("protocol_shape"),
        session_protocol=observed.get("session_protocol"),
    )
    return SessionHandle(
        session_id=str(observed.get("session_id") or "").strip(),
        collector_pn=str(observed.get("collector_pn") or "").strip(),
        peer_ip=str(observed.get("peer_ip") or "").strip(),
        listener_port=int(observed.get("listener_port") or 0),
        wire=wire,
        available_adapters=_ADAPTERS_BY_WIRE.get(wire, frozenset()),
        identity_source=str(observed.get("collector_identity_source") or "").strip(),
        state=str(observed.get("state") or "").strip(),
    )
