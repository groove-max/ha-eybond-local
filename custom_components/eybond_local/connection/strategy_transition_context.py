"""Neutral typed context for a connection-strategy transition.

REQUEST/CONTEXT only. Nothing here is a ``RecoveryProof`` or a ``RecoveryContract``
and nothing here mints recovery evidence. A ``ConfirmedStrategyTransitionPlan`` is
produced by the EXISTING transition authority ONLY after a successful verified run
(never before proof, and never as a second authority); this module carries the
pre-run request/context the confirmation form presents and the resolved default
HA advertised-endpoint candidate.

The resolved endpoint is an editable SUGGESTION the user confirms -- never an
automatically-applied route. Route sources are strictly separated (see the
provenance vocabulary); a bare peer IP, an L2/post-NAT destination, a hostname
shape or a cloud family is NEVER an authority for the advertised HA endpoint.

Neutral by construction: depends only on ``const`` and lower connection
primitives; imports nothing from ``config_flow``/``onboarding``/``runtime``/UI.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..collector_endpoint import inspect_collector_server_endpoint
from ..const import (
    CONNECTION_STRATEGIES,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
)

# Bind-all / unspecified hosts are never an advertisable endpoint.
_WILDCARD_HOSTS = frozenset({"0.0.0.0", "::", "0:0:0:0:0:0:0:0", "*"})

# ---- provenance vocabularies (closed) ----------------------------------------
# Advertised HA endpoint candidate provenance, in resolution priority order.
PROVENANCE_EXPLICIT_ADVERTISED = "explicit_advertised"
PROVENANCE_CALLBACK_PROOF = "callback_proof"
PROVENANCE_CONFIRMED_HA_ENDPOINT = "confirmed_ha_endpoint"
PROVENANCE_EFFECTIVE_RUNTIME_ROUTE = "effective_runtime_route"
PROVENANCE_NONE = "none"

_ENDPOINT_PROVENANCE = frozenset(
    {
        PROVENANCE_EXPLICIT_ADVERTISED,
        PROVENANCE_CALLBACK_PROOF,
        PROVENANCE_CONFIRMED_HA_ENDPOINT,
        PROVENANCE_EFFECTIVE_RUNTIME_ROUTE,
        PROVENANCE_NONE,
    }
)

# Cloud rollback endpoint provenance.
CLOUD_PROVENANCE_ORIGINAL = "original_cloud_endpoint"
CLOUD_PROVENANCE_OBSERVED_CURRENT = "observed_current_external_endpoint"
CLOUD_PROVENANCE_NONE = "none"

_CLOUD_PROVENANCE = frozenset(
    {
        CLOUD_PROVENANCE_ORIGINAL,
        CLOUD_PROVENANCE_OBSERVED_CURRENT,
        CLOUD_PROVENANCE_NONE,
    }
)


def _normalized_str(value: object) -> str | None:
    """Return an exact, already-normalized ``str`` or ``None`` (never coerce)."""

    if type(value) is not str:
        return None
    if value != value.strip():
        return None
    return value


def _valid_port(value: object) -> bool:
    return type(value) is int and type(value) is not bool and 1 <= value <= 65535


def _advertisable_host(value: object) -> str | None:
    """An exact, normalized, non-wildcard host, or ``None``."""

    host = _normalized_str(value)
    if not host or host in _WILDCARD_HOSTS:
        return None
    return host


@dataclass(frozen=True, slots=True)
class TransitionEndpointCandidate:
    """A resolved default advertised-HA-endpoint suggestion for the form.

    ``host``/``port`` is an editable suggestion, NOT an applied route.
    ``provenance`` is from the closed endpoint vocabulary. An absent candidate is
    ``provenance == "none"`` with empty host and port ``0``.
    """

    host: str
    port: int
    provenance: str

    def __post_init__(self) -> None:
        if type(self.provenance) is not str or self.provenance not in _ENDPOINT_PROVENANCE:
            raise ValueError("transition_endpoint_provenance_invalid")
        if type(self.port) is not int or type(self.port) is bool:
            raise TypeError("transition_endpoint_port_type_invalid")
        if self.provenance == PROVENANCE_NONE:
            # ``none`` is EXACTLY ("", 0, "none") -- nothing else.
            if type(self.host) is not str or self.host != "" or self.port != 0:
                raise ValueError("transition_endpoint_none_must_be_empty")
            return
        # Any real candidate must be an advertisable (exact, normalized,
        # non-wildcard) host and a valid port -- the direct constructor is a trust
        # boundary too, so a wildcard / padded / non-str host is unconstructible.
        if _advertisable_host(self.host) != self.host:
            raise ValueError("transition_endpoint_host_invalid")
        if not _valid_port(self.port):
            raise ValueError("transition_endpoint_incomplete")

    @classmethod
    def none(cls) -> "TransitionEndpointCandidate":
        return cls(host="", port=0, provenance=PROVENANCE_NONE)

    @property
    def has_candidate(self) -> bool:
        return self.provenance != PROVENANCE_NONE


@dataclass(frozen=True, slots=True)
class CloudRollbackEndpoint:
    """The remembered/observed cloud endpoint to restore SmartESS access.

    ``endpoint`` is the stored ``host,port[,proto]`` form. A KNOWN endpoint is
    syntactically validated by the existing provider-neutral parser (it may be
    written to the collector); the cloud FAMILY is never classified here. Absent =
    ``provenance == "none"``.
    """

    endpoint: str
    provenance: str

    def __post_init__(self) -> None:
        if type(self.provenance) is not str or self.provenance not in _CLOUD_PROVENANCE:
            raise ValueError("transition_cloud_provenance_invalid")
        if _normalized_str(self.endpoint) is None:
            raise ValueError("transition_cloud_endpoint_invalid")
        if self.provenance == CLOUD_PROVENANCE_NONE:
            if self.endpoint:
                raise ValueError("transition_cloud_none_must_be_empty")
            return
        if not self.endpoint:
            raise ValueError("transition_cloud_endpoint_missing")
        # A known rollback endpoint may be WRITTEN to the collector: it must be a
        # syntactically valid host,port[,proto] via the existing provider-neutral
        # parser. ``require_explicit_port`` keeps cloud-family resolution out of
        # the validation (the cloud family never participates here).
        try:
            parts = inspect_collector_server_endpoint(
                self.endpoint,
                require_explicit_port=True,
                require_explicit_protocol=False,
            )
        except ValueError as exc:
            raise ValueError("transition_cloud_endpoint_syntax_invalid") from exc
        # A wildcard bind is never a safe rollback target to write to a collector.
        if parts.host in _WILDCARD_HOSTS:
            raise ValueError("transition_cloud_endpoint_wildcard")

    @classmethod
    def none(cls) -> "CloudRollbackEndpoint":
        return cls(endpoint="", provenance=CLOUD_PROVENANCE_NONE)

    @property
    def known(self) -> bool:
        return self.provenance != CLOUD_PROVENANCE_NONE


@dataclass(frozen=True, slots=True)
class StrategyTransitionContext:
    """Pre-run request/context for one strategy transition (NOT a proof/plan).

    Separates every address role so none is confused for another: the advertised
    HA endpoint (resolved suggestion), the collector trigger target (callback
    only, editable), and the remembered cloud endpoint for rollback. Direction
    risk is derived from current->target, never from any address.
    """

    current_strategy: str
    target_strategy: str
    ha_endpoint: TransitionEndpointCandidate
    collector_trigger_target: str
    cloud_rollback: CloudRollbackEndpoint

    def __post_init__(self) -> None:
        for name in ("current_strategy", "target_strategy"):
            value = getattr(self, name)
            if type(value) is not str or value not in CONNECTION_STRATEGIES:
                raise ValueError(f"transition_{name}_invalid")
        if self.current_strategy == self.target_strategy:
            raise ValueError("transition_strategy_unchanged")
        if type(self.ha_endpoint) is not TransitionEndpointCandidate:
            raise TypeError("transition_ha_endpoint_type_required")
        if type(self.cloud_rollback) is not CloudRollbackEndpoint:
            raise TypeError("transition_cloud_rollback_type_required")
        if _normalized_str(self.collector_trigger_target) is None:
            raise ValueError("transition_collector_trigger_target_invalid")
        # A collector trigger target is meaningful ONLY for callback: inbound must
        # not carry one (nothing is triggered), and callback must have one.
        if self.target_strategy == CONNECTION_STRATEGY_INBOUND:
            if self.collector_trigger_target:
                raise ValueError("transition_inbound_forbids_trigger_target")
        elif self.target_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
            # A callback trigger target must be a real, non-wildcard address
            # (an already-normalized hostname is fine -- no IP-only heuristic).
            if not self.collector_trigger_target:
                raise ValueError("transition_callback_requires_trigger_target")
            if self.collector_trigger_target in _WILDCARD_HOSTS:
                raise ValueError("transition_callback_trigger_target_wildcard")

    @property
    def to_inbound(self) -> bool:
        return self.target_strategy == CONNECTION_STRATEGY_INBOUND


def _split_host_port(endpoint: str) -> tuple[str, int] | None:
    """Split an opaque ``host:port`` snapshot; ``None`` when unusable.

    Splits on the LAST colon so an IPv4 ``host:port`` is parsed. The host is NOT
    silently normalized: ``host :18899`` (a non-normalized host) is rejected, as
    is an empty/wildcard host or a port outside ``1..65535``. Never guessed.
    """

    host, sep, port_str = endpoint.rpartition(":")
    if not sep:
        return None
    advertisable = _advertisable_host(host)
    if advertisable is None:
        return None
    if port_str != port_str.strip():
        return None
    try:
        port = int(port_str)
    except (TypeError, ValueError):
        return None
    if not _valid_port(port):
        return None
    return advertisable, port


def _parse_present_proof_endpoint(value: object) -> tuple[str, int] | None:
    """Parse a PRESENT callback-proof endpoint strictly; ``None`` if malformed.

    A present proof must be an exact, un-padded ``str`` that parses as a
    normalized, non-wildcard ``host:port``. The empty-string absence signal is
    handled by the caller (never reaches here).
    """

    if type(value) is not str or value != value.strip():
        return None
    return _split_host_port(value)


def resolve_default_ha_endpoint(
    *,
    explicit_advertised_host: object,
    explicit_advertised_port: object,
    callback_proof_endpoint: object,
    confirmed_ha_endpoint: "TransitionEndpointCandidate | None",
    current_strategy: object,
    server_ip: object,
    tcp_port: object,
) -> TransitionEndpointCandidate:
    """Resolve the default advertised-HA-endpoint suggestion (strict priority).

    Priority (higher wins), each a STRICTLY SEPARATE source:

    1. an explicit ``advertised_*`` route the entry already stores;
    2. the advertised endpoint of a VALIDATED callback proof (proof > local
       runtime fallback -- a NAT public endpoint must win over the local bind);
    3. a confirmed HA endpoint the CALLER already role-classified (never derived
       here from peer IP / L2 / hostname / cloud family);
    4. the effective runtime route (``server_ip:tcp_port``), an editable LOCAL
       HINT only -- behind NAT it may be wrong, and it is NEVER a proof -- offered
       only when the entry currently runs ``callback_on_demand``;
    5. none -- the form then honestly asks for input (never a synthetic port).

    Fail-closed at every source boundary: a PRESENT but malformed explicit route
    or callback proof returns ``none`` -- it never falls through to a
    lower-priority source (so a bad address cannot be silently replaced by the
    local runtime hint).
    """

    # 1. explicit advertised. ABSENT is EXACTLY ("" str, 0 int) -- anything else
    #    present (partial, wrong type, padded, wildcard, bool port, out-of-range)
    #    is malformed and fails closed WITHOUT trying a lower-priority source.
    strictly_absent = (
        type(explicit_advertised_host) is str
        and explicit_advertised_host == ""
        and type(explicit_advertised_port) is int
        and type(explicit_advertised_port) is not bool
        and explicit_advertised_port == 0
    )
    if not strictly_absent:
        host = _advertisable_host(explicit_advertised_host)
        if host is not None and _valid_port(explicit_advertised_port):
            return TransitionEndpointCandidate(
                host=host,
                port=int(explicit_advertised_port),
                provenance=PROVENANCE_EXPLICIT_ADVERTISED,
            )
        return TransitionEndpointCandidate.none()

    # 2. validated callback proof. A STRICT empty string means "no proof" and may
    #    try the next source; anything else present but malformed fails closed.
    if not (type(callback_proof_endpoint) is str and callback_proof_endpoint == ""):
        parsed = _parse_present_proof_endpoint(callback_proof_endpoint)
        if parsed is not None:
            return TransitionEndpointCandidate(
                host=parsed[0], port=parsed[1], provenance=PROVENANCE_CALLBACK_PROOF
            )
        return TransitionEndpointCandidate.none()

    # 3. caller-provided, role-proven confirmed HA endpoint
    if (
        confirmed_ha_endpoint is not None
        and type(confirmed_ha_endpoint) is TransitionEndpointCandidate
        and confirmed_ha_endpoint.provenance == PROVENANCE_CONFIRMED_HA_ENDPOINT
    ):
        return confirmed_ha_endpoint

    # 4. effective runtime route (callback_on_demand only). server_ip:tcp_port is
    #    ONLY an editable LOCAL hint -- it is not necessarily the address the
    #    collector already dials (especially behind NAT), so it is presented for
    #    the user to confirm/correct, never as a proven route. A wildcard bind
    #    (0.0.0.0/::) is not advertisable and is skipped.
    if current_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
        bind_host = _advertisable_host(server_ip)
        if bind_host is not None and _valid_port(tcp_port):
            return TransitionEndpointCandidate(
                host=bind_host,
                port=int(tcp_port),
                provenance=PROVENANCE_EFFECTIVE_RUNTIME_ROUTE,
            )

    # 5. none
    return TransitionEndpointCandidate.none()


__all__ = [
    "CLOUD_PROVENANCE_NONE",
    "CLOUD_PROVENANCE_OBSERVED_CURRENT",
    "CLOUD_PROVENANCE_ORIGINAL",
    "CloudRollbackEndpoint",
    "PROVENANCE_CALLBACK_PROOF",
    "PROVENANCE_CONFIRMED_HA_ENDPOINT",
    "PROVENANCE_EFFECTIVE_RUNTIME_ROUTE",
    "PROVENANCE_EXPLICIT_ADVERTISED",
    "PROVENANCE_NONE",
    "StrategyTransitionContext",
    "TransitionEndpointCandidate",
    "resolve_default_ha_endpoint",
]
