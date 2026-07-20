"""Typed RecoveryContract: PROVEN ways to re-establish contact with a collector.

Three independent concepts, deliberately kept apart:

* ``connection_strategy`` -- the USER'S INTENT (inbound / callback_on_demand);
* ``endpoint_control_policy`` -- whether the integration MAY manage the
  collector endpoint (external / integration_managed);
* ``RecoveryContract`` (this module) -- the recorded results of REAL recovery
  proofs: verified ways to regain contact AFTER the current session is lost.

The contract holds at most two independent, typed proofs:

* an INBOUND recovery proof -- after a controlled restart/reboot, the collector
  opened a NEW session of the same full PN on its own, while callback triggers
  were provably inhibited (in-process);
* a CALLBACK recovery proof -- after a clean baseline reset and a full silent
  inbound window, an addressed ``set>server`` along one concrete route was
  followed by a NEW session of the same full PN, with every OTHER in-process
  trigger excluded for the whole window. The exclusivity is PROCESS-LOCAL: an
  external sender hitting the same collector inside the window cannot be
  detected or excluded (see ``CallbackRecoveryProof``), so this records the
  strongest causal statement one process can make, not metaphysical certainty.

What can NEVER become a proof here (fail-closed by construction):

* a currently-live session (liveness is not recovery);
* a certified callback identity outcome (identity is not recovery -- see
  ``connection.callback_identity``);
* the legacy ``callback_trigger`` strategy evidence (historical bookkeeping);
* the legacy ``user_confirmed_session`` evidence (a user binding, not a reboot
  proof);
* the legacy ``reboot_reconnect`` evidence (no <=v4 schema stored a
  verification timestamp or a strong identity source, so there is nothing to
  backfill -- and deliberately no "legacy" proof method to backfill into);
* a WEAKLY identified collector: the contract and every proof carry their own
  ``identity_source`` and exist only when
  ``session_registry.identity_source_is_strong`` says the PN was read
  authoritatively (FC=2 parameter 2 / AT DTUPN) -- a short heartbeat PN can
  never bind a contract;
* peer IP, endpoint hostname, collector kind, cloud family, driver/model, or
  any classification derived from them.

The callback proof's ``trigger_target`` / ``advertised_ha_endpoint`` are OPAQUE
snapshots of the values used during the proof. This module never interprets
them -- not as local/public/cloud, not as belonging to Home Assistant. They
exist only so a later caller can check "the configuration I am about to rely on
is the one that was actually proven".

Purity rules: immutable dataclasses; parsers never raise on malformed persisted
data (they drop exactly the untrusted part); no ``now()`` anywhere -- every
timestamp is supplied by the caller; no Home Assistant, transport, coordinator,
config-flow, driver or provider imports. Identity reconciliation delegates to
the session registry's single implementation (pure).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from .session_registry import (
    identity_source_is_strong,
    normalize_pn,
    pn_is_same_identity,
    prefer_full_pn,
)

# The ONE canonical persisted location: ``ConfigEntry.data[RECOVERY_CONTRACT_KEY]``.
# Never options, never a second store, never the collector registry.
RECOVERY_CONTRACT_KEY = "recovery_contract"

# Version of the persisted record shape (independent of the entry schema
# version). An unknown version is untrusted in its entirety.
RECOVERY_CONTRACT_SCHEMA_VERSION = 1

# Typed proof methods. A proof whose method is not in the matching set is
# malformed and does not exist.
INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER = "reboot_reconnect_no_trigger"
CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT = "reset_unicast_reconnect_same_pn"

# There is deliberately NO legacy method: <=v4 entry schemas stored neither a
# verification timestamp nor a strong identity source for their evidence, so
# nothing from before the contract era can ever be represented as a proof. The
# v4->v5 migration is a pure version bump, pinned by the migration tests.
INBOUND_RECOVERY_METHODS = frozenset(
    {INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER}
)
CALLBACK_RECOVERY_METHODS = frozenset({CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT})

# Record field names (kept as module constants so the serializer and the parser
# cannot drift apart).
_FIELD_VERSION = "schema_version"
_FIELD_PN = "collector_pn"
_FIELD_CONTRACT_IDENTITY_SOURCE = "collector_identity_source"
_FIELD_IDENTITY_SOURCE = "identity_source"
_FIELD_UPDATED_AT = "updated_at"
_FIELD_INBOUND = "inbound"
_FIELD_CALLBACK = "callback"
_FIELD_METHOD = "method"
_FIELD_VERIFIED_AT = "verified_at"
_FIELD_SESSION_PROTOCOL = "session_protocol"
_FIELD_TRIGGER_TARGET = "trigger_target"
_FIELD_ADVERTISED_ENDPOINT = "advertised_ha_endpoint"
_FIELD_LISTENER_PORT = "listener_port"


def _valid_timestamp(value: object) -> bool:
    """A proof timestamp must be a TIMEZONE-AWARE ISO-8601 datetime string.

    Deterministic: no clock is consulted; the value is judged on its own.
    Rejected: non-strings, blanks, malformed text, date-only values and
    timezone-NAIVE datetimes -- a proof time that cannot be placed on the
    global timeline (``tzinfo``/``utcoffset`` absent) proves nothing durable.
    """

    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    try:
        parsed = datetime.fromisoformat(text)
    except (ValueError, TypeError):
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _clean_str(value: object) -> str:
    return str(value).strip() if isinstance(value, str) else ""


def _strict_str(value: object) -> bool:
    """A value the record may SERIALIZE: a real ``str``, already normalized.

    ``type() is str`` on purpose (no subclasses, no ducks whose ``__str__``
    happens to look right -- ``identity_source_is_strong``/set membership
    stringify or compare by value, so only a strict type check stops them),
    and no surrounding whitespace: the constructor must never hold a value the
    parser would normalize into something else, or serialize -> parse ->
    serialize stops being byte-identical.
    """

    return type(value) is str and value == value.strip()


def _clean_pn(value: object) -> str:
    """A durable PN from persisted/caller data: strings only, never coerced.

    ``normalize_pn`` stringifies anything truthy (fine for live observations);
    a persisted record with a non-string PN is malformed and must be untrusted.
    """

    return normalize_pn(value) if isinstance(value, str) else ""


def _clean_port(value: object) -> int:
    """A listener port is part of the proven route: 1..65535 or nothing."""

    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value if 0 < value < 65536 else 0


@dataclass(frozen=True, slots=True)
class InboundRecoveryProof:
    """One verified 'the collector dials back in on its own' proof.

    ``identity_source`` is the authoritative source the proof's PN was read
    with (``fc2_parameter_2`` / ``at_dtupn``); a weak source (heartbeat, none)
    can never carry a proof. ``session_protocol`` is informative only (the
    confirmed live wire at proof time, when one existed); it is never a basis
    of the proof and never validated as one.
    """

    method: str
    collector_pn: str
    identity_source: str
    verified_at: str
    session_protocol: str = ""


@dataclass(frozen=True, slots=True)
class CallbackRecoveryProof:
    """One controlled 'an addressed set>server regains this collector' experiment.

    ``trigger_target`` and ``advertised_ha_endpoint`` are opaque snapshots of
    the route the proof actually used; ``listener_port`` is the minimal extra
    parameter a caller needs to check that today's configuration is the proven
    one. All three are REQUIRED: a partial route snapshot is no proof.

    HONEST CAUSALITY BOUNDARY: the experiment ran under PROCESS-LOCAL
    exclusivity (the callback ledger's documented guarantee). It excluded
    every other in-process trigger, every pre-existing session, foreign PNs
    and wrong-route arrivals -- it cannot exclude an EXTERNAL sender (a
    SmartESS app, a script, another HA instance) unicasting the same
    collector inside the verification window; such a datagram is physically
    indistinguishable on the receiving side. The proof therefore certifies
    the strongest causal statement available to one process, not an absolute
    'our datagram produced this session'.
    """

    method: str
    collector_pn: str
    identity_source: str
    verified_at: str
    trigger_target: str
    advertised_ha_endpoint: str
    listener_port: int


def _valid_inbound_proof(proof: InboundRecoveryProof, contract_pn: str) -> bool:
    if type(proof) is not InboundRecoveryProof:
        # Strict type identity: an identity outcome (or any other duck) with
        # similar attributes must never pass as a recovery proof.
        return False
    # Every serialized string field must be a strict, normalized str BEFORE any
    # value check -- a duck whose __str__ mimics a strong source, a padded
    # timestamp, or an arbitrary session_protocol object must never survive
    # into to_record().
    if not _strict_str(proof.method) or proof.method not in INBOUND_RECOVERY_METHODS:
        return False
    if not _strict_str(proof.identity_source) or not identity_source_is_strong(
        proof.identity_source
    ):
        return False
    if not _strict_str(proof.verified_at) or not _valid_timestamp(proof.verified_at):
        return False
    if not _strict_str(proof.session_protocol):
        return False
    if not _strict_str(proof.collector_pn):
        return False
    pn = _clean_pn(proof.collector_pn)
    if not pn or not pn_is_same_identity(contract_pn, pn):
        return False
    return True


def _valid_callback_proof(proof: CallbackRecoveryProof, contract_pn: str) -> bool:
    if type(proof) is not CallbackRecoveryProof:
        return False
    if not _strict_str(proof.method) or proof.method not in CALLBACK_RECOVERY_METHODS:
        return False
    if not _strict_str(proof.identity_source) or not identity_source_is_strong(
        proof.identity_source
    ):
        return False
    if not _strict_str(proof.verified_at) or not _valid_timestamp(proof.verified_at):
        return False
    if not _strict_str(proof.collector_pn):
        return False
    pn = _clean_pn(proof.collector_pn)
    if not pn or not pn_is_same_identity(contract_pn, pn):
        return False
    # The COMPLETE route snapshot, or nothing -- strict strings, a real port,
    # and the port stored exactly as validated.
    if not _strict_str(proof.trigger_target) or not proof.trigger_target:
        return False
    if not _strict_str(proof.advertised_ha_endpoint) or not proof.advertised_ha_endpoint:
        return False
    if _clean_port(proof.listener_port) != proof.listener_port or not proof.listener_port:
        return False
    return True


def _parse_inbound_proof(
    record: object, contract_pn: str
) -> InboundRecoveryProof | None:
    """Parse one persisted inbound branch; anything untrusted -> None."""

    if not isinstance(record, Mapping):
        return None
    proof = InboundRecoveryProof(
        method=_clean_str(record.get(_FIELD_METHOD)),
        collector_pn=_clean_pn(record.get(_FIELD_PN)),
        identity_source=_clean_str(record.get(_FIELD_IDENTITY_SOURCE)),
        verified_at=_clean_str(record.get(_FIELD_VERIFIED_AT)),
        session_protocol=_clean_str(record.get(_FIELD_SESSION_PROTOCOL)),
    )
    if not _valid_inbound_proof(proof, contract_pn):
        return None
    if len(contract_pn) > len(proof.collector_pn):
        # Normalize the proof to the fuller spelling of the SAME identity.
        # (Reconciliation only ever happens between strongly identified
        # records: both the contract and this proof passed the strong-source
        # gate above.)
        proof = InboundRecoveryProof(
            method=proof.method,
            collector_pn=prefer_full_pn(proof.collector_pn, contract_pn),
            identity_source=proof.identity_source,
            verified_at=proof.verified_at,
            session_protocol=proof.session_protocol,
        )
    return proof


def _parse_callback_proof(
    record: object, contract_pn: str
) -> CallbackRecoveryProof | None:
    """Parse one persisted callback branch; anything untrusted -> None."""

    if not isinstance(record, Mapping):
        return None
    proof = CallbackRecoveryProof(
        method=_clean_str(record.get(_FIELD_METHOD)),
        collector_pn=_clean_pn(record.get(_FIELD_PN)),
        identity_source=_clean_str(record.get(_FIELD_IDENTITY_SOURCE)),
        verified_at=_clean_str(record.get(_FIELD_VERIFIED_AT)),
        trigger_target=_clean_str(record.get(_FIELD_TRIGGER_TARGET)),
        advertised_ha_endpoint=_clean_str(record.get(_FIELD_ADVERTISED_ENDPOINT)),
        listener_port=_clean_port(record.get(_FIELD_LISTENER_PORT)),
    )
    if not _valid_callback_proof(proof, contract_pn):
        return None
    if len(contract_pn) > len(proof.collector_pn):
        proof = CallbackRecoveryProof(
            method=proof.method,
            collector_pn=prefer_full_pn(proof.collector_pn, contract_pn),
            identity_source=proof.identity_source,
            verified_at=proof.verified_at,
            trigger_target=proof.trigger_target,
            advertised_ha_endpoint=proof.advertised_ha_endpoint,
            listener_port=proof.listener_port,
        )
    return proof


@dataclass(frozen=True, slots=True)
class RecoveryContract:
    """The per-entry, per-durable-PN record of verified recovery methods.

    It never decides the connection strategy, never touches the endpoint, and
    never evaluates addresses. It answers exactly one pair of questions: has
    inbound recovery been PROVEN for this collector, and has callback recovery
    been PROVEN for this collector -- each with its method, time, and (for
    callback) the opaque route it was proven with.

    Construction is STRICT everywhere, including the direct constructor:
    ``__post_init__`` rejects an unknown schema version, a missing/unnormalized
    PN, a weak/missing identity source, a foreign/malformed proof and an
    invalid non-empty ``updated_at`` -- so an object that exists is an object
    ``to_record()`` may serialize. The parsers never raise: they sanitize fully
    and only then construct.
    """

    schema_version: int
    collector_pn: str
    collector_identity_source: str
    inbound_proof: InboundRecoveryProof | None
    callback_proof: CallbackRecoveryProof | None
    updated_at: str = ""

    def __post_init__(self) -> None:
        # ``type() is int`` on purpose: bool is an int subclass and 1.0 == 1,
        # so an equality check alone lets a float/bool masquerade as the known
        # version -- constructible here, rejected by the parser after
        # to_record(). The constructor must be exactly as strict as the parser.
        if (
            type(self.schema_version) is not int
            or self.schema_version != RECOVERY_CONTRACT_SCHEMA_VERSION
        ):
            raise ValueError(f"recovery_contract_schema_unknown:{self.schema_version!r}")
        if (
            not _strict_str(self.collector_pn)
            or not self.collector_pn
            or _clean_pn(self.collector_pn) != self.collector_pn
        ):
            raise ValueError("recovery_contract_requires_durable_pn")
        if not _strict_str(self.collector_identity_source) or not identity_source_is_strong(
            self.collector_identity_source
        ):
            # A short heartbeat / unknown observation is not durable identity:
            # without an authoritative source there is no contract at all. The
            # strict type check also stops a duck whose __str__ mimics a strong
            # source -- it would serialize as a non-JSON-safe object.
            raise ValueError(
                f"recovery_contract_identity_source_weak:{self.collector_identity_source!r}"
            )
        if not _strict_str(self.updated_at) or (
            self.updated_at and not _valid_timestamp(self.updated_at)
        ):
            raise ValueError(f"recovery_contract_updated_at_invalid:{self.updated_at!r}")
        if self.inbound_proof is not None:
            if type(self.inbound_proof) is not InboundRecoveryProof:
                raise TypeError("inbound_proof_type_required")
            if not _valid_inbound_proof(self.inbound_proof, self.collector_pn):
                raise ValueError("inbound_proof_invalid")
        if self.callback_proof is not None:
            if type(self.callback_proof) is not CallbackRecoveryProof:
                raise TypeError("callback_proof_type_required")
            if not _valid_callback_proof(self.callback_proof, self.collector_pn):
                raise ValueError("callback_proof_invalid")

    # --- state ---------------------------------------------------------------

    @property
    def pn_bound(self) -> bool:
        return bool(self.collector_pn)

    @property
    def inbound_verified(self) -> bool:
        return self.inbound_proof is not None

    @property
    def callback_verified(self) -> bool:
        return self.callback_proof is not None

    @property
    def is_empty(self) -> bool:
        return self.inbound_proof is None and self.callback_proof is None

    # --- construction ----------------------------------------------------------

    @classmethod
    def empty_for_pn(
        cls,
        collector_pn: object,
        *,
        identity_source: str,
        updated_at: str = "",
    ) -> "RecoveryContract":
        """A contract bound to one STRONGLY identified durable PN, no proofs.

        Builder-side API: a missing/blank PN, a weak/missing identity source,
        or an invalid non-empty ``updated_at`` is a programmer error and
        raises. The empty seed may carry ``updated_at=""`` (nothing recorded
        yet) or a valid timezone-aware timestamp -- nothing else. (The parsers
        below never raise -- they fail closed instead.)
        """

        pn = _clean_pn(collector_pn)
        if not pn:
            raise ValueError("recovery_contract_requires_durable_pn")
        return cls(
            schema_version=RECOVERY_CONTRACT_SCHEMA_VERSION,
            collector_pn=pn,
            collector_identity_source=_clean_str(identity_source),
            inbound_proof=None,
            callback_proof=None,
            updated_at=updated_at,
        )

    @classmethod
    def from_record(cls, record: object) -> "RecoveryContract | None":
        """Parse one persisted record. Malformed/unknown -> fail closed.

        * not a mapping / unknown ``schema_version`` / no durable PN -> ``None``
          (no trusted contract at all);
        * a malformed proof branch -> that branch is ``None`` while the other,
          independently valid branch is preserved;
        * a missing/weak contract identity source -> ``None`` (a short
          heartbeat or unknown observation can never bind a contract);
        * a missing/weak PROOF identity source -> only that branch is ``None``;
        * short/full spellings of the SAME identity normalize to the fuller PN
          -- reconciliation only ever happens between strongly identified
          records (both sides passed the strong-source gates);
        * an invalid ``updated_at`` is CLEARED to ``""`` (chosen deliberately:
          the proofs carry their own validated timestamps, so a broken record
          mtime must not destroy them).

        Never raises on persisted data.
        """

        if not isinstance(record, Mapping):
            return None
        version = record.get(_FIELD_VERSION)
        if isinstance(version, bool) or not isinstance(version, int):
            return None
        if version != RECOVERY_CONTRACT_SCHEMA_VERSION:
            # Unknown shape (newer or older): nothing in it may be trusted.
            return None
        contract_pn = _clean_pn(record.get(_FIELD_PN))
        if not contract_pn:
            return None
        contract_source = _clean_str(record.get(_FIELD_CONTRACT_IDENTITY_SOURCE))
        if not identity_source_is_strong(contract_source):
            # The contract's own identity is not authoritatively grounded:
            # nothing under it may be trusted.
            return None

        inbound = _parse_inbound_proof(record.get(_FIELD_INBOUND), contract_pn)
        callback = _parse_callback_proof(record.get(_FIELD_CALLBACK), contract_pn)
        # Enrichment only ever goes short -> full of the same identity: a proof
        # carrying the fuller spelling upgrades the contract PN; a shorter
        # spelling never downgrades it (prefer_full_pn is one-directional).
        enriched_pn = contract_pn
        for proof in (inbound, callback):
            if proof is not None:
                enriched_pn = prefer_full_pn(enriched_pn, proof.collector_pn)
        updated_at = record.get(_FIELD_UPDATED_AT)
        return cls(
            schema_version=version,
            collector_pn=enriched_pn,
            collector_identity_source=contract_source,
            inbound_proof=inbound,
            callback_proof=callback,
            updated_at=_clean_str(updated_at) if _valid_timestamp(updated_at) else "",
        )

    def write_to(self, data: "dict[str, object]") -> None:
        """Persist this (validated) contract into one entry-data dict.

        THE single production writer of the canonical key: every layer that
        wants to persist a contract goes through this method, so the store
        cannot fork into loose fields, an options copy, or a second location.
        The object is valid by construction (``__post_init__``), so what lands
        in ``data`` is exactly what the parser will accept back.
        """

        data[RECOVERY_CONTRACT_KEY] = self.to_record()

    @classmethod
    def from_entry_data(cls, data: Mapping[str, object] | None) -> "RecoveryContract | None":
        """Read the ONE canonical key from ``ConfigEntry.data``.

        Only ``data[RECOVERY_CONTRACT_KEY]`` is consulted -- never options,
        never individual loose fields, never a second store.
        """

        if not isinstance(data, Mapping):
            return None
        return cls.from_record(data.get(RECOVERY_CONTRACT_KEY))

    # --- serialization ---------------------------------------------------------

    def to_record(self) -> dict[str, object]:
        """Serialize to the persisted shape (plain JSON-safe types, deterministic)."""

        record: dict[str, object] = {
            _FIELD_VERSION: self.schema_version,
            _FIELD_PN: self.collector_pn,
            _FIELD_CONTRACT_IDENTITY_SOURCE: self.collector_identity_source,
            _FIELD_UPDATED_AT: self.updated_at,
        }
        if self.inbound_proof is not None:
            record[_FIELD_INBOUND] = {
                _FIELD_METHOD: self.inbound_proof.method,
                _FIELD_PN: self.inbound_proof.collector_pn,
                _FIELD_IDENTITY_SOURCE: self.inbound_proof.identity_source,
                _FIELD_VERIFIED_AT: self.inbound_proof.verified_at,
                _FIELD_SESSION_PROTOCOL: self.inbound_proof.session_protocol,
            }
        if self.callback_proof is not None:
            record[_FIELD_CALLBACK] = {
                _FIELD_METHOD: self.callback_proof.method,
                _FIELD_PN: self.callback_proof.collector_pn,
                _FIELD_IDENTITY_SOURCE: self.callback_proof.identity_source,
                _FIELD_VERIFIED_AT: self.callback_proof.verified_at,
                _FIELD_TRIGGER_TARGET: self.callback_proof.trigger_target,
                _FIELD_ADVERTISED_ENDPOINT: self.callback_proof.advertised_ha_endpoint,
                _FIELD_LISTENER_PORT: self.callback_proof.listener_port,
            }
        return record

    # --- immutable updates ------------------------------------------------------

    def _enriched(self, proof_pn: str) -> str:
        pn = _clean_pn(proof_pn)
        if not pn_is_same_identity(self.collector_pn, pn):
            raise ValueError(
                f"recovery_proof_foreign_pn:{self.collector_pn}:{pn}"
            )
        return prefer_full_pn(self.collector_pn, pn)

    def with_inbound_proof(
        self, proof: InboundRecoveryProof, *, updated_at: str
    ) -> "RecoveryContract":
        """Return a new contract holding this inbound proof (all-or-nothing).

        Builder-side: violations raise (a caller constructing an invalid proof
        is a bug, not persisted data). A foreign PN is rejected without any
        partial application; a fuller spelling of the same identity enriches
        the contract PN; a shorter one never downgrades it.
        """

        if type(proof) is not InboundRecoveryProof:
            raise TypeError("inbound_proof_type_required")
        if not _valid_timestamp(updated_at):
            raise ValueError("recovery_contract_updated_at_required")
        contract_pn = self._enriched(proof.collector_pn)
        if not _valid_inbound_proof(proof, contract_pn):
            raise ValueError("inbound_proof_invalid")
        stored = InboundRecoveryProof(
            method=proof.method,
            collector_pn=contract_pn,
            identity_source=_clean_str(proof.identity_source),
            verified_at=proof.verified_at.strip(),
            session_protocol=_clean_str(proof.session_protocol),
        )
        return RecoveryContract(
            schema_version=self.schema_version,
            collector_pn=contract_pn,
            collector_identity_source=self.collector_identity_source,
            inbound_proof=stored,
            callback_proof=self.callback_proof,
            updated_at=updated_at.strip(),
        )

    def with_callback_proof(
        self, proof: CallbackRecoveryProof, *, updated_at: str
    ) -> "RecoveryContract":
        """Return a new contract holding this callback proof (all-or-nothing)."""

        if type(proof) is not CallbackRecoveryProof:
            raise TypeError("callback_proof_type_required")
        if not _valid_timestamp(updated_at):
            raise ValueError("recovery_contract_updated_at_required")
        contract_pn = self._enriched(proof.collector_pn)
        if not _valid_callback_proof(proof, contract_pn):
            raise ValueError("callback_proof_invalid")
        stored = CallbackRecoveryProof(
            method=proof.method,
            collector_pn=contract_pn,
            identity_source=_clean_str(proof.identity_source),
            verified_at=proof.verified_at.strip(),
            trigger_target=proof.trigger_target.strip(),
            advertised_ha_endpoint=proof.advertised_ha_endpoint.strip(),
            listener_port=int(proof.listener_port),
        )
        return RecoveryContract(
            schema_version=self.schema_version,
            collector_pn=contract_pn,
            collector_identity_source=self.collector_identity_source,
            inbound_proof=self.inbound_proof,
            callback_proof=stored,
            updated_at=updated_at.strip(),
        )

    def without_inbound_proof(self, *, updated_at: str) -> "RecoveryContract":
        if not _valid_timestamp(updated_at):
            raise ValueError("recovery_contract_updated_at_required")
        return RecoveryContract(
            schema_version=self.schema_version,
            collector_pn=self.collector_pn,
            collector_identity_source=self.collector_identity_source,
            inbound_proof=None,
            callback_proof=self.callback_proof,
            updated_at=updated_at.strip(),
        )

    def without_callback_proof(self, *, updated_at: str) -> "RecoveryContract":
        if not _valid_timestamp(updated_at):
            raise ValueError("recovery_contract_updated_at_required")
        return RecoveryContract(
            schema_version=self.schema_version,
            collector_pn=self.collector_pn,
            collector_identity_source=self.collector_identity_source,
            inbound_proof=self.inbound_proof,
            callback_proof=None,
            updated_at=updated_at.strip(),
        )


# --- diagnostics ----------------------------------------------------------------


def recovery_contract_diagnostics(data: Mapping[str, object] | None) -> dict[str, object]:
    """Support-bundle view: proof STRUCTURE only, never network values.

    Exposes booleans/methods/timestamps. Deliberately absent: the raw
    ``trigger_target``, the raw ``advertised_ha_endpoint``, any session id,
    any peer IP, any credential. ``callback_route_bound`` /
    ``advertised_endpoint_bound`` say a snapshot exists without revealing it.
    """

    contract = RecoveryContract.from_entry_data(data)
    if contract is None:
        return {
            "recovery_contract_version": 0,
            "recovery_contract_valid": False,
            "recovery_contract_identity_strong": False,
            "recovery_contract_pn_bound": False,
            "inbound_recovery_verified": False,
            "inbound_recovery_method": "",
            "inbound_recovery_verified_at": "",
            "callback_recovery_verified": False,
            "callback_recovery_method": "",
            "callback_recovery_verified_at": "",
            "callback_route_bound": False,
            "advertised_endpoint_bound": False,
        }
    inbound = contract.inbound_proof
    callback = contract.callback_proof
    return {
        "recovery_contract_version": contract.schema_version,
        "recovery_contract_valid": True,
        "recovery_contract_identity_strong": identity_source_is_strong(
            contract.collector_identity_source
        ),
        "recovery_contract_pn_bound": contract.pn_bound,
        "inbound_recovery_verified": contract.inbound_verified,
        "inbound_recovery_method": inbound.method if inbound else "",
        "inbound_recovery_verified_at": inbound.verified_at if inbound else "",
        "callback_recovery_verified": contract.callback_verified,
        "callback_recovery_method": callback.method if callback else "",
        "callback_recovery_verified_at": callback.verified_at if callback else "",
        "callback_route_bound": bool(callback and callback.trigger_target),
        "advertised_endpoint_bound": bool(callback and callback.advertised_ha_endpoint),
    }


__all__ = [
    "CALLBACK_RECOVERY_METHODS",
    "CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT",
    "CallbackRecoveryProof",
    "INBOUND_RECOVERY_METHODS",
    "INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER",
    "InboundRecoveryProof",
    "RECOVERY_CONTRACT_KEY",
    "RECOVERY_CONTRACT_SCHEMA_VERSION",
    "RecoveryContract",
    "recovery_contract_diagnostics",
]
