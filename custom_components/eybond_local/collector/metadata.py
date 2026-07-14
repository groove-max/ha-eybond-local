"""Provider-neutral collector-metadata TELEMETRY channels (read-only).

This is the WIRE layer for collector-side TELEMETRY (identity / version /
endpoint / signal), NOT the collector-management ACTION surface. Endpoint
write / apply / reboot are a different contract carried by the negotiated
:class:`CollectorManagementAdapter`; nothing in this module writes a collector
setting or triggers an apply/reboot -- the readers are strictly read-only.

Three channels exist, each bound by the route-authority layer (link/session) to
owned/claimed session evidence -- never to peer IP, collector kind, cloud
family, hostname, or driver key:

* ``framed_metadata``           -- FC=2 read-only sweep over the neutral wire.
* ``at_metadata``               -- read-only SmartESS AT-text sweep (supplemental).
* ``framed_hardware_bootstrap`` -- one FC=2 param-6 identity probe over an
  AT-shaped ESP bridge session (collector-only bootstrap before an inverter
  heartbeat exists).

"Dual-channel" means framed base metadata and AT supplemental metadata can BOTH
be live at once. It is NOT a single global adapter selection: the framed and AT
readers are independent channels that the runtime service merges.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .at_runtime import read_runtime_collector_at_values
from .collector_wire import (
    CollectorWireError,
    CollectorWireManagementSession,
    QUERY_HARDWARE_VERSION,
    parse_query_collector_response,
)
from .metadata_result import (
    OUTCOME_COMMAND_ERROR,
    OUTCOME_EMPTY,
    OUTCOME_SUCCESS,
    OUTCOME_TRANSPORT_ERROR,
    CollectorMetadataChannelReadResult,
)
from .parameter_registry import (
    COLLECTOR_PARAMETER_DEFINITION_BY_ID,
    read_runtime_collector_values,
)

_BOOTSTRAP_TRANSPORT_FAILURES = (
    TimeoutError,
    OSError,
    ConnectionError,
    EOFError,
)

# Metadata channel ids. Namespaced under ``collector:`` so a metadata
# dead-channel verdict can NEVER collide with an inverter driver command key:
# metadata channel health and driver unsupported-command state are separate
# facts and are persisted in separate config-entry options.
FRAMED_METADATA_CHANNEL = "collector:fc_metadata"
FRAMED_HARDWARE_BOOTSTRAP_CHANNEL = "collector:fc_metadata_bootstrap"
AT_METADATA_CHANNEL = "collector:at_metadata"

METADATA_CHANNEL_IDS: tuple[str, ...] = (
    FRAMED_METADATA_CHANNEL,
    FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
    AT_METADATA_CHANNEL,
)

# Bound read contract: a zero-argument coroutine the service awaits. It is bound
# to the OWNED transport by the route authority so the service never selects a
# transport itself. Each reader returns a structured channel-read result.
MetadataReader = Callable[[], Awaitable[CollectorMetadataChannelReadResult]]


async def async_read_framed_metadata(transport: object) -> CollectorMetadataChannelReadResult:
    """Read the FC=2 read-only collector metadata sweep over the neutral wire.

    ``parameter_registry`` remains the owner of the FC parameter definitions and
    their decoders; this reader only wraps the transport in the NEUTRAL
    collector-wire session. No SmartESS catalog resolution happens here.
    """

    return await read_runtime_collector_values(CollectorWireManagementSession(transport))


async def async_read_at_metadata(transport: object) -> CollectorMetadataChannelReadResult:
    """Read the read-only SmartESS AT-text collector metadata sweep."""

    return await read_runtime_collector_at_values(transport)


async def async_read_framed_hardware_bootstrap(
    at_transport: object,
) -> CollectorMetadataChannelReadResult:
    """Read FC=2 param-6 identity through an AT-shaped ESP bridge bootstrap probe.

    The param-6 wire encoding lives with the transport's narrow bridge probe
    (``async_query_bridge_hardware_version``); this decodes exactly that one
    identity parameter using the ``parameter_registry`` decoder. Outcome codes
    mirror the sweep readers: a delivery failure is ``transport_error``, a
    malformed frame is ``command_error``, no param-6 metadata is ``empty``.
    """

    try:
        _header, payload = await at_transport.async_query_bridge_hardware_version()
    except asyncio.CancelledError:
        raise
    except _BOOTSTRAP_TRANSPORT_FAILURES as exc:  # noqa: BLE001 - typed code only
        return CollectorMetadataChannelReadResult.transport_error(
            type(exc).__name__, attempted=1
        )
    except Exception as exc:  # noqa: BLE001 - typed code only
        return CollectorMetadataChannelReadResult.transport_error(
            type(exc).__name__, attempted=1
        )
    try:
        response = parse_query_collector_response(payload)
    except CollectorWireError as exc:
        return CollectorMetadataChannelReadResult(
            outcome=OUTCOME_COMMAND_ERROR,
            safe_error_code=str(exc).split(":", 1)[0],
            attempted_commands=1,
            failed_commands=1,
        )
    if response.code != 0 or response.parameter != QUERY_HARDWARE_VERSION:
        return CollectorMetadataChannelReadResult(
            outcome=OUTCOME_EMPTY, attempted_commands=1
        )
    definition = COLLECTOR_PARAMETER_DEFINITION_BY_ID.get(QUERY_HARDWARE_VERSION)
    decoder = getattr(definition, "decode", None)
    values = decoder(response) if decoder is not None else {}
    if not any(str(value).strip() != "" for value in values.values()):
        return CollectorMetadataChannelReadResult(
            outcome=OUTCOME_EMPTY, attempted_commands=1
        )
    return CollectorMetadataChannelReadResult(
        values=values,
        outcome=OUTCOME_SUCCESS,
        attempted_commands=1,
        successful_commands=1,
    )


@dataclass(frozen=True)
class CollectorMetadataRoute:
    """One trusted, owned metadata channel bound to its read contract.

    ``reader`` is bound to the OWNED transport by the route authority (the link/
    session layer). Availability, provenance, session identity, and generation
    are all decided from trusted session evidence -- never from peer IP,
    collector kind, cloud family, hostname, or driver key. A route that carries
    a ``reader`` is, by construction, one the entry owns or can safely claim.
    """

    channel_id: str
    reader: MetadataReader
    provenance: str = "unavailable"
    session_id: str = ""
    generation: int = 0
    supports_liveness: bool = False
    is_bootstrap: bool = False


@dataclass(frozen=True)
class CollectorMetadataRouteSet:
    """The metadata channels available for one entry's owned collector session.

    Dual-channel: ``framed`` base metadata and ``at`` supplemental metadata can
    both be present at once. ``bootstrap`` is the collector-only FC=2 param-6
    identity probe used before an inverter heartbeat exists; it is only ever
    populated when no framed metadata channel is available (a framed wire reads
    param 6 in the normal sweep).
    """

    generation: int = 0
    session_id: str = ""
    provenance: str = "unavailable"
    # Durable collector identity (full/short PN) this route set belongs to. The
    # service keys its cache/health on this PN, NEVER on peer IP. Empty means a
    # provisional/PN-less session that must not overwrite a durable-identity cache.
    identity: str = ""
    framed: CollectorMetadataRoute | None = None
    at: CollectorMetadataRoute | None = None
    bootstrap: CollectorMetadataRoute | None = None

    @property
    def channels(self) -> tuple[CollectorMetadataRoute, ...]:
        """Return the present routes in a stable order for diagnostics."""

        return tuple(
            route
            for route in (self.framed, self.at, self.bootstrap)
            if route is not None
        )

    @property
    def has_any_channel(self) -> bool:
        return bool(self.channels)


def build_collector_metadata_routes(
    *,
    framed_transport: object | None = None,
    at_transport: object | None = None,
    bootstrap_transport: object | None = None,
    generation: int = 0,
    session_id: str = "",
    provenance: str = "unavailable",
    identity: str = "",
    framed_provenance: str = "",
    at_provenance: str = "",
    bootstrap_provenance: str = "",
) -> CollectorMetadataRouteSet:
    """Bind owned transports to metadata channels.

    The caller (route authority) decides WHICH transports are owned/claimable and
    passes only those; this helper never inspects a transport to decide the wire.
    ``bootstrap_transport`` is honoured only when no ``framed_transport`` is given
    (a framed wire already reads param 6 in the normal sweep).
    """

    framed_route: CollectorMetadataRoute | None = None
    at_route: CollectorMetadataRoute | None = None
    bootstrap_route: CollectorMetadataRoute | None = None

    if framed_transport is not None:
        framed_route = CollectorMetadataRoute(
            channel_id=FRAMED_METADATA_CHANNEL,
            reader=lambda t=framed_transport: async_read_framed_metadata(t),
            provenance=framed_provenance or provenance,
            session_id=session_id,
            generation=generation,
            supports_liveness=True,
        )

    if at_transport is not None:
        at_route = CollectorMetadataRoute(
            channel_id=AT_METADATA_CHANNEL,
            reader=lambda t=at_transport: async_read_at_metadata(t),
            provenance=at_provenance or provenance,
            session_id=session_id,
            generation=generation,
            supports_liveness=True,
        )

    if framed_transport is None and bootstrap_transport is not None:
        bootstrap_route = CollectorMetadataRoute(
            channel_id=FRAMED_HARDWARE_BOOTSTRAP_CHANNEL,
            reader=lambda t=bootstrap_transport: async_read_framed_hardware_bootstrap(t),
            provenance=bootstrap_provenance or provenance,
            session_id=session_id,
            generation=generation,
            is_bootstrap=True,
        )

    return CollectorMetadataRouteSet(
        generation=generation,
        session_id=session_id,
        provenance=provenance,
        identity=identity,
        framed=framed_route,
        at=at_route,
        bootstrap=bootstrap_route,
    )
