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
- ``at_mixed_forward`` -- exact-session data-plane negotiation on an AT-primary
  stream: a correlated reply selects raw passthrough or framed FC4.
- ``raw_passthrough`` -- a positively selected raw serial data plane.

The negotiated *wire* remains the one thing runtime payload routing turns on:
``framed`` (use the framed transport) vs ``at_text`` (use the AT transport) vs
``""`` (nothing observed yet -> fail closed).  Independent exact-session
capabilities record additional management dialects without changing that
primary parser/forwarding route.
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
ADAPTER_INVERTER_AT_MIXED = "at_mixed_forward"
ADAPTER_INVERTER_RAW_PASSTHROUGH = "raw_passthrough"
ADAPTER_INVERTER_NATIVE_MODBUS_TCP = "native_modbus_tcp"
ADAPTER_PROXY_FRAMED_CLOUD = "framed_cloud_proxy"
ADAPTER_PROXY_RAW_TCP = "raw_tcp_proxy"
ADAPTER_NONE = "none"

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
# collector commands. An AT-text wire carries AT commands plus an exact-session
# data-plane negotiator, and can also carry a framed collector-command probe
# (FC over AT) without switching the primary wire.
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
class SessionCapabilities:
    """Independently observed roles supported by one physical TCP session.

    ``wire_framing`` remains the single primary parser/activation route.  These
    sets deliberately answer a different question: which role-specific
    operations have positive evidence on that exact session.  A hybrid
    collector can therefore keep a framed primary route while also exposing an
    AT collector-management capability; that does not turn inverter forwarding
    into raw passthrough and does not create a second session owner.
    """

    collector_management_adapters: frozenset[str] = field(default_factory=frozenset)
    inverter_forward_adapters: frozenset[str] = field(default_factory=frozenset)
    proxy_adapters: frozenset[str] = field(default_factory=frozenset)

    @property
    def available_adapters(self) -> frozenset[str]:
        """Return the union of every role-specific observed adapter."""

        return frozenset(
            self.collector_management_adapters
            | self.inverter_forward_adapters
            | self.proxy_adapters
        )

    def supports(self, adapter: str) -> bool:
        """Return whether positive session evidence supports ``adapter``."""

        return adapter in self.available_adapters


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
    capabilities: SessionCapabilities = field(default_factory=SessionCapabilities)
    conflict: str = ""
    state: str = ""

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
    def available_adapters(self) -> frozenset[str]:
        """Return all non-empty negotiated adapters."""

        # Scalar adapters remain the compatibility projection consumed by the
        # existing runtime route selectors.  Include them for direct
        # SessionHandle constructors as well as the richer capability sets.
        projected = frozenset(
            adapter
            for adapter in (
                self.collector_management_adapter,
                self.inverter_forward_adapter,
                self.proxy_adapter,
            )
            if adapter and adapter != ADAPTER_NONE
        )
        return frozenset(self.capabilities.available_adapters | projected)

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
    carries ONLY the durable primary-wire facts that a same-collector session
    handover must preserve -- never a socket identity, a live-observation flag,
    or supplemental capabilities learned on the old physical socket. A new
    socket must prove hybrid AT/framed support again. The binding is adopted
    from a trusted observed handle by an explicit lifecycle step, never as a
    side effect of reading diagnostics.
    """

    collector_pn: str
    wire_framing: str
    collector_management_adapter: str = ADAPTER_NONE
    inverter_forward_adapter: str = ADAPTER_NONE
    proxy_adapter: str = ADAPTER_NONE
    capabilities: SessionCapabilities = field(default_factory=SessionCapabilities)
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
        durable_capabilities = _capabilities_for_observation(
            wire=handle.wire_framing,
            observed_wires=frozenset({handle.wire_framing}),
            identity_sources=frozenset(),
        )
        return cls(
            collector_pn=durable,
            wire_framing=handle.wire_framing,
            collector_management_adapter=handle.collector_management_adapter,
            inverter_forward_adapter=handle.inverter_forward_adapter,
            proxy_adapter=handle.proxy_adapter,
            capabilities=durable_capabilities,
            identity_sources=handle.identity_sources,
        )

    @classmethod
    def from_confirmed_protocol(
        cls,
        *,
        collector_pn: str,
        session_protocol: str,
    ) -> "ConfirmedWireBinding | None":
        """Build a confirmed binding from a durable PN + a CONFIRMED protocol.

        Used to seed a same-PN reconnect/startup bootstrap from persisted
        confirmed-live evidence. Returns ``None`` unless a durable PN is supplied
        and ``session_protocol`` is a known confirmed wire (``eybond_framed`` /
        ``at_text``). The caller is responsible for having validated the
        provenance (must be ``live_session``) and that the PN matches the entry.
        """

        durable = str(collector_pn or "").strip()
        protocol = str(session_protocol or "").strip().lower()
        if not durable:
            return None
        if protocol == "eybond_framed":
            wire = WIRE_FRAMED
        elif protocol == "at_text":
            wire = WIRE_AT_TEXT
        else:
            return None
        collector_adapter, inverter_adapter, proxy_adapter = _adapters_for_wire(wire)
        capabilities = _capabilities_for_observation(
            wire=wire,
            observed_wires=frozenset({wire}),
            identity_sources=frozenset(),
        )
        return cls(
            collector_pn=durable,
            wire_framing=wire,
            collector_management_adapter=collector_adapter,
            inverter_forward_adapter=inverter_adapter,
            proxy_adapter=proxy_adapter,
            capabilities=capabilities,
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
            ADAPTER_INVERTER_AT_MIXED,
            ADAPTER_PROXY_RAW_TCP,
        )
    if wire == WIRE_RAW_TCP:
        return (
            ADAPTER_NONE,
            ADAPTER_INVERTER_RAW_PASSTHROUGH,
            ADAPTER_PROXY_RAW_TCP,
        )
    return (ADAPTER_NONE, ADAPTER_NONE, ADAPTER_NONE)


def _strict_observed_strings(value: object) -> frozenset[str]:
    """Return exact normalized observation strings; malformed containers fail closed."""

    if type(value) not in (tuple, list, set, frozenset):
        return frozenset()
    return frozenset(
        item
        for item in value
        if type(item) is str and item and item == item.strip()
    )


def _capabilities_for_observation(
    *,
    wire: str,
    observed_wires: frozenset[str],
    identity_sources: frozenset[str],
) -> SessionCapabilities:
    """Build role capabilities from exact-session wire/identity evidence.

    Identity sources prove collector-management dialects only.  They do not
    prove how inverter payloads or cloud proxy bytes are forwarded.  Those
    roles require an observed/routed wire shape.  This is the load-bearing
    distinction for hybrid E500 collectors.
    """

    management: set[str] = set()
    inverter: set[str] = set()
    proxy: set[str] = set()

    # Any exact-session framed/AT observation proves only the corresponding
    # collector-management dialect.  Inverter forwarding and proxying stay
    # pinned to the ONE primary route selected for this socket.
    all_management_wires = set(observed_wires)
    if wire != WIRE_UNKNOWN:
        all_management_wires.add(wire)

    if WIRE_FRAMED in all_management_wires:
        management.add(ADAPTER_COLLECTOR_FRAMED_COMMANDS)
    if WIRE_AT_TEXT in all_management_wires:
        management.add(ADAPTER_COLLECTOR_AT_COMMANDS)

    if wire == WIRE_FRAMED:
        inverter.add(ADAPTER_INVERTER_FRAMED_FC4)
        proxy.add(ADAPTER_PROXY_FRAMED_CLOUD)
    if wire == WIRE_AT_TEXT:
        inverter.add(ADAPTER_INVERTER_AT_MIXED)
        proxy.add(ADAPTER_PROXY_RAW_TCP)
    if wire == WIRE_RAW_TCP:
        inverter.add(ADAPTER_INVERTER_RAW_PASSTHROUGH)
        proxy.add(ADAPTER_PROXY_RAW_TCP)

    if "at_dtupn" in identity_sources:
        management.add(ADAPTER_COLLECTOR_AT_COMMANDS)
    if identity_sources & {
        "fc1_identity_challenge",
        "fc2_parameter_2",
        "framed_heartbeat",
    }:
        management.add(ADAPTER_COLLECTOR_FRAMED_COMMANDS)

    return SessionCapabilities(
        collector_management_adapters=frozenset(management),
        inverter_forward_adapters=frozenset(inverter),
        proxy_adapters=frozenset(proxy),
    )


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
    raw_identity_source = observed.get("collector_identity_source")
    identity_source = (
        raw_identity_source
        if type(raw_identity_source) is str
        and raw_identity_source
        and raw_identity_source == raw_identity_source.strip()
        else ""
    )
    identity_sources = set(
        _strict_observed_strings(observed.get("collector_identity_sources"))
    )
    if identity_source:
        identity_sources.add(identity_source)
    observed_shapes = _strict_observed_strings(
        observed.get("observed_protocol_shapes")
    )
    observed_wires = frozenset(
        normalized
        for shape in observed_shapes
        if (normalized := _normalize_wire_signal(shape)) != WIRE_UNKNOWN
    )
    capabilities = _capabilities_for_observation(
        wire=wire,
        observed_wires=observed_wires,
        identity_sources=frozenset(identity_sources),
    )
    return SessionHandle(
        session_id=str(observed.get("session_id") or "").strip(),
        collector_pn=str(observed.get("collector_pn") or "").strip(),
        peer_ip=str(observed.get("peer_ip") or "").strip(),
        listener_port=int(observed.get("listener_port") or 0),
        wire_framing=wire,
        identity_sources=frozenset(identity_sources),
        collector_management_adapter=collector_adapter,
        inverter_forward_adapter=inverter_adapter,
        proxy_adapter=proxy_adapter,
        capabilities=capabilities,
        conflict=conflict,
        state=str(observed.get("state") or "").strip(),
    )
