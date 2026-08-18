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

# Cloud rollback endpoint provenance (closed vocabulary, in resolution priority
# order). ``explicit_user_endpoint`` is RESERVED for CP2B.2 (a user catalog /
# manual choice): the resolver accepts it at the top of the priority order, but
# CP2B.1 never produces it (no user choice exists yet).
CLOUD_PROVENANCE_EXPLICIT_USER = "explicit_user_endpoint"
CLOUD_PROVENANCE_ORIGINAL = "original_cloud_endpoint"
CLOUD_PROVENANCE_REGISTRY = "collector_registry"
CLOUD_PROVENANCE_OBSERVED_CURRENT = "observed_current_external_endpoint"
CLOUD_PROVENANCE_NONE = "none"

_CLOUD_PROVENANCE = frozenset(
    {
        CLOUD_PROVENANCE_EXPLICIT_USER,
        CLOUD_PROVENANCE_ORIGINAL,
        CLOUD_PROVENANCE_REGISTRY,
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


def normalized_advertised_host(value: object) -> str | None:
    """Public: an exact, normalized, non-wildcard advertised host, or ``None``."""

    return _advertisable_host(value)


def parse_advertised_port(value: object) -> int | None:
    """Safely parse a submitted advertised port; ``None`` when unusable.

    Accepts an exact ``int`` (never ``bool``), an integer-valued ``float`` (as a
    NumberSelector may yield), or a normalized decimal ``str``. Range
    ``1..65535``. NEVER raises -- a malformed value returns ``None`` so the caller
    can surface a form error instead of a 500.
    """

    if type(value) is bool:
        return None
    if type(value) is int:
        port = value
    elif type(value) is float and value.is_integer():
        port = int(value)
    elif type(value) is str and value == value.strip() and value.isdigit():
        port = int(value)
    else:
        return None
    return port if 1 <= port <= 65535 else None


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
        # Preserve every endpoint shape the existing collector layer supports,
        # including factory host-only values.  A direct model is normalized by
        # construction; persisted/raw inputs are normalized by the resolver
        # before they reach this boundary.
        normalized = _valid_writable_cloud_endpoint(self.endpoint)
        if normalized is None:
            raise ValueError("transition_cloud_endpoint_syntax_invalid")
        if normalized != self.endpoint:
            raise ValueError("transition_cloud_endpoint_not_normalized")

    @classmethod
    def none(cls) -> "CloudRollbackEndpoint":
        return cls(endpoint="", provenance=CLOUD_PROVENANCE_NONE)

    @property
    def known(self) -> bool:
        return self.provenance != CLOUD_PROVENANCE_NONE


# ---- cloud rollback SELECTION (CP2B.2) ---------------------------------------
# How the user chose the rollback endpoint (closed vocabulary).
ROLLBACK_SELECTION_CONFIRMED_CANDIDATE = "confirmed_candidate"
ROLLBACK_SELECTION_CATALOG = "catalog"
ROLLBACK_SELECTION_MANUAL = "manual"

_ROLLBACK_SELECTION_KINDS = frozenset(
    {
        ROLLBACK_SELECTION_CONFIRMED_CANDIDATE,
        ROLLBACK_SELECTION_CATALOG,
        ROLLBACK_SELECTION_MANUAL,
    }
)

# A confirmed candidate must carry one of the CP2B.1 resolver provenances (never
# the reserved explicit-user slot -- that belongs to catalog/manual).
_CONFIRMED_CANDIDATE_PROVENANCES = frozenset(
    {
        CLOUD_PROVENANCE_ORIGINAL,
        CLOUD_PROVENANCE_REGISTRY,
        CLOUD_PROVENANCE_OBSERVED_CURRENT,
    }
)

# Honest, closed persistence-source tokens (written to the original-endpoint
# whole-record). A catalog/manual endpoint is NEVER labelled observed/factory.
ROLLBACK_SOURCE_USER_CONFIRMED_EXISTING = "user_confirmed_existing"
ROLLBACK_SOURCE_USER_SELECTED_CATALOG = "user_selected_catalog"
ROLLBACK_SOURCE_USER_ENTERED_MANUAL = "user_entered_manual"

_ROLLBACK_SELECTION_SOURCE = {
    ROLLBACK_SELECTION_CONFIRMED_CANDIDATE: ROLLBACK_SOURCE_USER_CONFIRMED_EXISTING,
    ROLLBACK_SELECTION_CATALOG: ROLLBACK_SOURCE_USER_SELECTED_CATALOG,
    ROLLBACK_SELECTION_MANUAL: ROLLBACK_SOURCE_USER_ENTERED_MANUAL,
}


@dataclass(frozen=True, slots=True)
class CloudRollbackSelection:
    """The user's typed, immutable choice of cloud rollback endpoint (CP2B.2).

    The ONE authority object for a callback restore: the config flow BUILDS it,
    and the coordinator/transition authority exact-type validate it before the
    first mutation. It is NOT a proof and it does NOT change ``connection_strategy``.

    A confirmed candidate re-uses a CP2B.1 resolver endpoint verbatim (its
    ``candidate_provenance`` must equal the endpoint's own provenance). A catalog
    or manual choice is an explicit user endpoint -- it can never masquerade as an
    observed/factory-confirmed one. Every endpoint is already validated by the
    provider-neutral parser inside ``CloudRollbackEndpoint`` (host, host+port and
    host+port+protocol shapes preserved; wildcard rejected).
    """

    endpoint: CloudRollbackEndpoint
    selection_kind: str
    candidate_provenance: str = ""
    catalog_profile_key: str = ""
    user_confirmed: bool = False

    def __post_init__(self) -> None:
        if type(self.endpoint) is not CloudRollbackEndpoint:
            raise TypeError("rollback_selection_endpoint_type_required")
        if not self.endpoint.known:
            raise ValueError("rollback_selection_endpoint_required")
        if (
            type(self.selection_kind) is not str
            or self.selection_kind not in _ROLLBACK_SELECTION_KINDS
        ):
            raise ValueError("rollback_selection_kind_invalid")
        # user_confirmed must be the EXACT bool True (never 1 / "true" / duck).
        if self.user_confirmed is not True:
            raise ValueError("rollback_selection_unconfirmed")
        if type(self.candidate_provenance) is not str:
            raise TypeError("rollback_selection_candidate_provenance_type")
        if type(self.catalog_profile_key) is not str:
            raise TypeError("rollback_selection_catalog_key_type")
        if self.candidate_provenance != self.candidate_provenance.strip():
            raise ValueError("rollback_selection_candidate_provenance_not_normalized")
        if self.catalog_profile_key != self.catalog_profile_key.strip():
            raise ValueError("rollback_selection_catalog_key_not_normalized")
        if self.selection_kind == ROLLBACK_SELECTION_CONFIRMED_CANDIDATE:
            if self.candidate_provenance not in _CONFIRMED_CANDIDATE_PROVENANCES:
                raise ValueError("rollback_selection_candidate_provenance_invalid")
            if self.endpoint.provenance != self.candidate_provenance:
                raise ValueError("rollback_selection_candidate_provenance_mismatch")
            if self.catalog_profile_key:
                raise ValueError("rollback_selection_confirmed_forbids_catalog_key")
        elif self.selection_kind == ROLLBACK_SELECTION_CATALOG:
            if self.endpoint.provenance != CLOUD_PROVENANCE_EXPLICIT_USER:
                raise ValueError("rollback_selection_catalog_provenance_invalid")
            if self.candidate_provenance:
                raise ValueError("rollback_selection_catalog_forbids_candidate_provenance")
            if not self.catalog_profile_key:
                raise ValueError("rollback_selection_catalog_key_required")
        else:  # ROLLBACK_SELECTION_MANUAL
            if self.endpoint.provenance != CLOUD_PROVENANCE_EXPLICIT_USER:
                raise ValueError("rollback_selection_manual_provenance_invalid")
            if self.candidate_provenance:
                raise ValueError("rollback_selection_manual_forbids_candidate_provenance")
            if self.catalog_profile_key:
                raise ValueError("rollback_selection_manual_forbids_catalog_key")

    @property
    def endpoint_value(self) -> str:
        """The exact, shape-preserved endpoint string to write to the collector."""

        return self.endpoint.endpoint

    @property
    def persistence_source(self) -> str:
        """The honest whole-record source token for this selection kind."""

        return _ROLLBACK_SELECTION_SOURCE[self.selection_kind]


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


def _valid_writable_cloud_endpoint(value: object) -> str | None:
    """Return one normalized, non-wildcard collector endpoint, or ``None``.

    All shapes already supported by the existing endpoint layer remain valid:
    ``host``, ``host,port`` and ``host,port,protocol``.  The input must first be
    an exact string; normalization then goes through the existing parser and its
    shape-preserving renderer -- never through a second parser or ``str()``
    coercion.
    """

    endpoint = _normalized_str(value)
    if endpoint is None or endpoint == "":
        return None
    try:
        parts = inspect_collector_server_endpoint(
            endpoint,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return None
    if parts.host in _WILDCARD_HOSTS:
        return None
    return parts.render(preserve_shape=True)


def resolve_confirmed_ha_endpoint(
    *,
    current_strategy: object,
    entry_pn: object,
    advertised_host: object,
    advertised_port: object,
    recovery_contract: object,
) -> TransitionEndpointCandidate:
    """Resolve a PN-bound, proof-backed HA endpoint without runtime fallbacks.

    The persisted advertised pair is an earned fact only when it is tied to the
    valid ``RecoveryContract`` written by the same atomic strategy commit.  For
    callback strategy, the proof's NAT-visible ``advertised_ha_endpoint`` is the
    authority and must agree with a persisted pair when one is present.  For
    inbound strategy, the atomic pair is accepted only alongside an inbound
    proof.  A local bind/server address is deliberately not an input.
    """

    from .recovery_contract import RecoveryContract
    from ..collector_identity import pn_is_same_identity

    if type(recovery_contract) is not RecoveryContract:
        return TransitionEndpointCandidate.none()
    if (
        type(entry_pn) is not str
        or not entry_pn
        or entry_pn != entry_pn.strip()
        or not pn_is_same_identity(entry_pn, recovery_contract.collector_pn)
    ):
        return TransitionEndpointCandidate.none()

    pair_present = not (
        type(advertised_host) is str
        and advertised_host == ""
        and type(advertised_port) is int
        and type(advertised_port) is not bool
        and advertised_port == 0
    )
    pair_host = _advertisable_host(advertised_host)
    pair_valid = pair_host is not None and _valid_port(advertised_port)

    if current_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
        proof = recovery_contract.callback_proof
        if proof is None:
            return TransitionEndpointCandidate.none()
        parsed = _parse_present_proof_endpoint(proof.advertised_ha_endpoint)
        if parsed is None:
            return TransitionEndpointCandidate.none()
        if pair_present and (not pair_valid or parsed != (pair_host, advertised_port)):
            return TransitionEndpointCandidate.none()
        return TransitionEndpointCandidate(
            host=parsed[0],
            port=parsed[1],
            provenance=PROVENANCE_CONFIRMED_HA_ENDPOINT,
        )

    if current_strategy == CONNECTION_STRATEGY_INBOUND:
        if recovery_contract.inbound_proof is None or not pair_valid:
            return TransitionEndpointCandidate.none()
        return TransitionEndpointCandidate(
            host=pair_host,
            port=advertised_port,
            provenance=PROVENANCE_CONFIRMED_HA_ENDPOINT,
        )

    return TransitionEndpointCandidate.none()


def _observed_endpoint_is_distinct_from_ha(
    endpoint: str,
    confirmed_ha_endpoint: TransitionEndpointCandidate,
) -> bool:
    """Return whether a current endpoint is provably distinct from HA.

    DNS host comparison is case-insensitive; port and protocol participate in
    equivalence.  A compact host-only value on the same host is ambiguous when
    it does not resolve to the confirmed port, so it fails closed instead of
    being promoted using a cloud-family guess.
    """

    try:
        parts = inspect_collector_server_endpoint(
            endpoint,
            require_explicit_port=False,
            require_explicit_protocol=False,
        )
    except ValueError:
        return False
    if parts.host.casefold() != confirmed_ha_endpoint.host.casefold():
        return True
    if not parts.has_explicit_port:
        return False
    return not (
        parts.port == confirmed_ha_endpoint.port
        and parts.protocol.casefold() == "tcp"
    )


def resolve_cloud_rollback_endpoint(
    *,
    explicit_user_endpoint: object,
    durable_original_endpoint: object,
    registry_endpoint: object,
    registry_pn: object,
    entry_pn: object,
    observed_current_endpoint: object,
    confirmed_ha_endpoint: object,
) -> CloudRollbackEndpoint:
    """Resolve the cloud rollback endpoint from PRE-GATHERED facts (pure/read-only).

    NO I/O, NO persistence, NO proof, NO strategy authority. Every input is a
    fact the read-only boundary already gathered through existing APIs.

    Priority (higher wins), each a STRICTLY SEPARATE source:

    1. ``explicit_user_endpoint`` -- a user catalog/manual choice (CP2B.2; the
       CP2B.1 boundary always passes ``""``);
    2. ``durable_original_endpoint`` -- the saved original cloud endpoint (read
       as ONE whole record by the boundary, data-over-options; the boundary
       passes ``""`` only when NO original record exists at all);
    3. ``registry_endpoint`` -- a PN-bound collector-registry fact, accepted only
       when ``pn_is_same_identity(entry_pn, registry_pn)`` (short/full identity);
    4. ``observed_current_endpoint`` -- the confirmed current endpoint, a
       rollback candidate ONLY when a proof-backed full HA endpoint is known and
       the complete endpoint semantics are provably distinct;
    5. ``none``.

    Fail-closed at every boundary: a PRESENT-but-malformed higher-priority source
    returns ``none`` and NEVER falls through to a lower source (so corruption is
    never silently masked). Absence is EXACTLY the empty string ``""``.
    """

    # 1. explicit user choice (CP2B.2). Reserved: absent in CP2B.1.
    if not (type(explicit_user_endpoint) is str and explicit_user_endpoint == ""):
        endpoint = _valid_writable_cloud_endpoint(explicit_user_endpoint)
        if endpoint is not None:
            return CloudRollbackEndpoint(
                endpoint=endpoint, provenance=CLOUD_PROVENANCE_EXPLICIT_USER
            )
        return CloudRollbackEndpoint.none()

    # 2. durable saved original cloud endpoint (whole record).
    if not (type(durable_original_endpoint) is str and durable_original_endpoint == ""):
        endpoint = _valid_writable_cloud_endpoint(durable_original_endpoint)
        if endpoint is not None:
            return CloudRollbackEndpoint(
                endpoint=endpoint, provenance=CLOUD_PROVENANCE_ORIGINAL
            )
        return CloudRollbackEndpoint.none()

    # 3. PN-bound collector-registry endpoint.
    if not (type(registry_endpoint) is str and registry_endpoint == ""):
        endpoint = _valid_writable_cloud_endpoint(registry_endpoint)
        # Lazy import keeps this module's top-level imports minimal; the PN
        # identity rule is the ONE short/full reconciliation authority.
        from ..collector_identity import pn_is_same_identity

        if endpoint is not None and pn_is_same_identity(entry_pn, registry_pn):
            return CloudRollbackEndpoint(
                endpoint=endpoint, provenance=CLOUD_PROVENANCE_REGISTRY
            )
        return CloudRollbackEndpoint.none()

    # 4. confirmed current endpoint, only if proven not to be the HA endpoint.
    if not (type(observed_current_endpoint) is str and observed_current_endpoint == ""):
        endpoint = _valid_writable_cloud_endpoint(observed_current_endpoint)
        if (
            endpoint is not None
            and type(confirmed_ha_endpoint) is TransitionEndpointCandidate
            and confirmed_ha_endpoint.provenance
            == PROVENANCE_CONFIRMED_HA_ENDPOINT
            and _observed_endpoint_is_distinct_from_ha(
                endpoint, confirmed_ha_endpoint
            )
        ):
            return CloudRollbackEndpoint(
                endpoint=endpoint,
                provenance=CLOUD_PROVENANCE_OBSERVED_CURRENT,
            )
        return CloudRollbackEndpoint.none()

    # 5. none
    return CloudRollbackEndpoint.none()


def earned_advertised_route(
    *,
    committed_strategy: object,
    terminal: object,
    attempted_host: object,
    attempted_port: object,
) -> tuple[str, int, str]:
    """The EARNED advertised route to persist on a verified strategy commit.

    Returns ``(host, port, refusal)``, fully fail-closed with EXACT types:

    * ``committed_strategy is None`` is a true non-strategy merge -> ``("", 0, "")``
      (nothing persisted); an exact ``inbound``/``callback_on_demand`` is a
      strategy commit; ANY other value (duck / non-string / unknown string) is a
      ``transition_committed_strategy_invalid`` refusal, never a harmless merge;
    * the attempted route must be an advertisable (exact, normalized,
      non-wildcard) host and a real ``int`` port ``1..65535`` -- anything else is
      ``transition_advertised_route_invalid`` (never a coerced ``str``/``int`` and
      never an empty "success");
    * ``terminal`` must be the exact ``RecoveryTerminalInput`` and carry the
      matching typed proof: a callback commit requires a ``CallbackRecoveryProof``
      whose ``advertised_ha_endpoint`` EXACTLY equals the attempted ``host:port``
      (absent -> ``transition_callback_route_unproven``, differing ->
      ``transition_callback_route_mismatch``); an inbound commit requires an
      ``InboundRecoveryProof`` (absent -> ``transition_inbound_route_unproven``).

    Neutral: no peer IP / L2 / hostname / cloud-family input, no coercion.
    """

    from .recovery.terminal import RecoveryTerminalInput
    from .recovery_contract import CallbackRecoveryProof, InboundRecoveryProof

    # ``None`` is the only true non-strategy merge (nothing to persist). An exact
    # ``inbound``/``callback_on_demand`` is a strategy commit; ANY other value
    # (duck / non-string / unknown string) is a typed refusal -- it must never be
    # treated as a harmless merge that lets a bogus strategy commit with no route.
    if committed_strategy is None:
        return "", 0, ""
    if type(committed_strategy) is not str or committed_strategy not in (
        CONNECTION_STRATEGY_INBOUND,
        CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    ):
        return "", 0, "transition_committed_strategy_invalid"
    host = _advertisable_host(attempted_host)
    if host is None or not _valid_port(attempted_port):
        return "", 0, "transition_advertised_route_invalid"
    if type(terminal) is not RecoveryTerminalInput:
        return "", 0, "transition_terminal_proof_required"
    if committed_strategy == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
        if type(terminal.callback_proof) is not CallbackRecoveryProof:
            return "", 0, "transition_callback_route_unproven"
        if terminal.callback_proof.advertised_ha_endpoint != f"{host}:{attempted_port}":
            return "", 0, "transition_callback_route_mismatch"
        return host, attempted_port, ""
    if type(terminal.inbound_proof) is not InboundRecoveryProof:
        return "", 0, "transition_inbound_route_unproven"
    return host, attempted_port, ""


__all__ = [
    "CLOUD_PROVENANCE_EXPLICIT_USER",
    "CLOUD_PROVENANCE_NONE",
    "CLOUD_PROVENANCE_OBSERVED_CURRENT",
    "CLOUD_PROVENANCE_ORIGINAL",
    "CLOUD_PROVENANCE_REGISTRY",
    "CloudRollbackEndpoint",
    "CloudRollbackSelection",
    "ROLLBACK_SELECTION_CATALOG",
    "ROLLBACK_SELECTION_CONFIRMED_CANDIDATE",
    "ROLLBACK_SELECTION_MANUAL",
    "ROLLBACK_SOURCE_USER_CONFIRMED_EXISTING",
    "ROLLBACK_SOURCE_USER_ENTERED_MANUAL",
    "ROLLBACK_SOURCE_USER_SELECTED_CATALOG",
    "PROVENANCE_CALLBACK_PROOF",
    "PROVENANCE_CONFIRMED_HA_ENDPOINT",
    "PROVENANCE_EFFECTIVE_RUNTIME_ROUTE",
    "PROVENANCE_EXPLICIT_ADVERTISED",
    "PROVENANCE_NONE",
    "StrategyTransitionContext",
    "TransitionEndpointCandidate",
    "earned_advertised_route",
    "normalized_advertised_host",
    "parse_advertised_port",
    "resolve_cloud_rollback_endpoint",
    "resolve_confirmed_ha_endpoint",
    "resolve_default_ha_endpoint",
]
