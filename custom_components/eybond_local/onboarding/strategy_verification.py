"""The typed INBOUND RECOVERY transaction (reboot -> autonomous reconnect).

A collector observed on the passive callback listener is NOT proof of a
permanent inbound configuration: a factory EyeBond collector may only be
connected because an earlier UDP callback trigger made it dial Home Assistant
temporarily, and that link disappears on the collector's next restart. Recovery
must never be inferred from the endpoint/hostname, the cloud family, the
collector type, the peer IP, private/public address shape, or the mere
presence of a TCP session.

This module proves inbound RECOVERY through observable device behavior:

    observed_session
      -> waiting_for_strong_identity
      -> restart_requested            (via the negotiated management adapter)
      -> waiting_for_disconnect       (the collector itself drops the socket)
      -> waiting_for_inbound_reconnect
           -> inbound_verified        (typed InboundRecoveryProof)
           -> inbound_not_verified    (typed failure; the config flow continues
                                       on its existing manual callback step)

The verifier returns a typed :class:`InboundRecoveryOutcome`. It deliberately
does NOT return or decide a ``connection_strategy`` -- strategy is the user's
intent, recorded by the flow; this transaction only proves (or fails to prove)
that the collector re-establishes contact on its own after a controlled
reboot. The proof is a :class:`connection.recovery_contract.InboundRecoveryProof`
and exists ONLY on full success.

CAUSALITY IS OWNED HERE, not by the caller: ``async_verify`` itself acquires
the exclusive callback causality lease (before the baseline) and holds the
callback-trigger inhibitor for the whole window, so the verifier cannot be
invoked "unwrapped". The ledger generation is still sampled before/after as
defense in depth -- if anything slipped a trigger through, no proof is created.

The reboot goes through the ONE management-adapter switch
(:func:`collector.management.select_collector_management_adapter`), keyed only
by the live negotiated ``SessionHandle.collector_management_adapter`` -- never
by collector kind, virtual bridge, hostname/endpoint, cloud family, peer IP,
persisted protocol or driver/model. An AT-text session whose adapter honestly
cannot reboot yields the typed ``restart_not_supported``.

Identity rules follow the session registry: the full collector PN is durable
identity; strong/weak is the registry's centralized verdict
(``identity_source_is_strong``); short/full reconciliation is the registry's
single implementation. Peer IP is never consulted here at all.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Protocol

from ..collector.management import (
    CollectorManagementUnsupportedError,
    select_collector_management_adapter,
)
from ..connection.callback_ledger import (
    CallbackCausalityBusyError,
    get_callback_trigger_ledger,
)
from ..connection.recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    CallbackRecoveryProof,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    InboundRecoveryProof,
    RecoveryContract,
)
from ..connection.session_handle import negotiate_session_adapters
from ..connection.session_registry import (
    identity_source_is_strong,
    pn_is_same_identity,
)
from ..const import (
    CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION,
)
from .timeouts import DEFAULT_ONBOARDING_TIMEOUT_POLICY, OnboardingTimeoutPolicy

logger = logging.getLogger(__name__)

# --- states -------------------------------------------------------------------
STATE_OBSERVED_SESSION = "observed_session"
STATE_WAITING_FOR_STRONG_IDENTITY = "waiting_for_strong_identity"
STATE_RESTART_REQUESTED = "restart_requested"
STATE_WAITING_FOR_DISCONNECT = "waiting_for_disconnect"
STATE_WAITING_FOR_INBOUND_RECONNECT = "waiting_for_inbound_reconnect"
STATE_INBOUND_VERIFIED = "inbound_verified"
STATE_INBOUND_NOT_VERIFIED = "inbound_not_verified"
# Callback-recovery extension states (the SAME reset machine, second phase).
STATE_INBOUND_RECOVERED = "inbound_recovered"
STATE_CALLBACK_TRIGGER_REQUESTED = "callback_trigger_requested"
STATE_WAITING_FOR_CALLBACK_SESSION = "waiting_for_callback_session"
STATE_CALLBACK_VERIFIED = "callback_verified"

# The user explicitly bound an observed, unclaimed strong-PN session. Honest
# provenance for the pending options flow's binding action -- kept ONLY for
# that path and the legacy compatibility reader. This verifier records no
# strategy evidence at all: its product is the typed InboundRecoveryProof.
EVIDENCE_USER_CONFIRMED_SESSION = CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION

# --- typed failure reasons ------------------------------------------------------
FAILURE_STRONG_IDENTITY_TIMEOUT = "strong_identity_timeout"
FAILURE_RESTART_NOT_SUPPORTED = "restart_not_supported"
FAILURE_RESTART_NOT_CONFIRMED = "restart_not_confirmed"
FAILURE_DISCONNECT_NOT_OBSERVED = "disconnect_not_observed"
FAILURE_RECONNECT_TIMEOUT = "inbound_reconnect_timeout"
FAILURE_UDP_TRIGGER_OBSERVED = "udp_trigger_during_verification"
FAILURE_SESSION_UNAVAILABLE = "collector_session_unavailable"
FAILURE_SESSION_CLAIMED = "session_claimed_by_other_owner"
FAILURE_CAUSALITY_BUSY = "verification_causality_busy"
FAILURE_RECONNECTED_SESSION_UNTRUSTED = "reconnected_session_untrusted"
FAILURE_INBOUND_PROOF_INVALID = "inbound_proof_invalid"
# Callback-recovery typed failures. The identity-transaction vocabulary is
# reused where the meaning is identical (one typed language for callbacks).
FAILURE_TRIGGER_NOT_SENT = "callback_trigger_not_sent"
FAILURE_CALLBACK_TIMEOUT = "callback_recovery_timeout"
FAILURE_CALLBACK_INTERFERENCE = "callback_trigger_interference"
FAILURE_ROUTE_INVALID = "callback_route_invalid"
FAILURE_CALLBACK_PROOF_INVALID = "callback_proof_invalid"
# The transaction has no capability to move/commit registry ownership (a
# retarget or prepare-handoff hook is missing): a proof-producing success is
# structurally impossible, so the engine refuses BEFORE touching the
# collector. A wiring bug must never cost the user a pointless reboot.
FAILURE_OWNERSHIP_UNAVAILABLE = "recovery_ownership_unavailable"

# FULLY-SILENT reconnect diagnoses. A TCP socket demonstrably arrived (it is
# in the listener's silent-pending view), so collapsing these into a plain
# "timeout" is dishonest -- each names a different observable cause.
FAILURE_SILENT_SESSION_AMBIGUOUS = "recovery_silent_session_ambiguous"
FAILURE_RECOVERY_IDENTITY_MISMATCH = "recovery_identity_mismatch"
FAILURE_SILENT_PROBE_FAILED = "recovery_silent_probe_failed"
FAILURE_SILENT_PROBE_UNAVAILABLE = "recovery_silent_probe_unavailable"

# Typed observations of ONE silent-candidate probe poll (never exceptions as
# control flow). ``_SILENT_OBS_TO_FAILURE`` maps the terminal ones to the
# typed failures above; ``no_candidate`` / ``same_identity_observed`` are not
# failures (the wait keeps going / the finder succeeds).
_SILENT_OBS_NONE = "no_candidate"
_SILENT_OBS_AMBIGUOUS = "ambiguous"
_SILENT_OBS_PROBE_FAILED = "probe_failed"
_SILENT_OBS_FOREIGN = "foreign_identity"
_SILENT_OBS_SAME = "same_identity_observed"
_SILENT_OBS_UNAVAILABLE = "probe_unavailable"

_SILENT_OBS_TO_FAILURE = {
    _SILENT_OBS_AMBIGUOUS: FAILURE_SILENT_SESSION_AMBIGUOUS,
    _SILENT_OBS_PROBE_FAILED: FAILURE_SILENT_PROBE_FAILED,
    _SILENT_OBS_FOREIGN: FAILURE_RECOVERY_IDENTITY_MISMATCH,
    _SILENT_OBS_UNAVAILABLE: FAILURE_SILENT_PROBE_UNAVAILABLE,
}


@dataclass(frozen=True, slots=True)
class _ReconnectWaitResult:
    """Outcome of one bounded same-PN reconnect wait.

    ``session_id`` is non-empty on success (the finder resolved a new
    same-identity socket). ``silent_failure`` is a typed failure reason ONLY
    when the wait ended without a session AND a silent socket was observed
    that could not become that session (ambiguous / foreign / probe failed /
    channel unavailable). Empty ``silent_failure`` means no silent socket was
    ever observed -- the caller then uses its plain reconnect/callback timeout.
    """

    session_id: str = ""
    silent_failure: str = ""


class SessionUnavailableError(RuntimeError):
    """The observed session could not be claimed/activated for the restart."""


_DEFAULT_POLL_INTERVAL_SECONDS = 0.5

# Listener inventory states that mean the session is gone for good.
_CLOSED_STATE_PREFIXES = ("closed",)
# Inventory states that must never confirm a new inbound session.
_UNTRUSTED_SESSION_STATES = frozenset({"route_identity_mismatch"})


class RestartChannel(Protocol):
    """Transport-side contract for restarting the observed collector session."""

    async def async_send_restart(self) -> None:
        """Reboot via the negotiated management adapter; raise on failure."""

    async def async_probe_identity(self) -> str:
        """Read the authoritative collector PN over the claimed session."""

    def is_connected(self) -> bool:
        """Return whether the observed (old) session is still connected."""

    async def async_close(self) -> None:
        """Release any transport resources (idempotent)."""


@dataclass(slots=True)
class InboundRecoveryOutcome:
    """Typed outcome of ONE inbound recovery verification.

    Carries NO connection strategy and NO legacy evidence: strategy is user
    intent (the flow's concern), and the only thing this transaction can add
    to an entry is the typed ``proof`` -- which exists ONLY on full success.
    """

    status: str = STATE_OBSERVED_SESSION
    failure_reason: str = ""
    collector_pn: str = ""
    new_session_id: str = ""
    proof: InboundRecoveryProof | None = None
    transitions: tuple[str, ...] = ()

    @property
    def inbound_verified(self) -> bool:
        return self.status == STATE_INBOUND_VERIFIED and self.proof is not None


@dataclass(frozen=True, slots=True)
class CallbackRecoveryRoute:
    """The EXPLICIT unicast route one callback recovery attempt is proven on.

    Every field is caller-supplied and opaque to the verifier: nothing here is
    derived from internal/external HA URLs, peer IPs, private/public address
    shape, hostname semantics, cloud provider/family, collector kind or
    endpoint ownership -- and the verifier never classifies the values.

    The critical split (NAT-capable by construction):

    * ``bind_ip`` -- the LOCAL address the trigger's UDP socket binds on. It is
      a transport concern and never appears in the proof.
    * ``advertised_ha_host`` / ``advertised_ha_port`` -- what goes INTO the
      ``set>server`` payload verbatim: behind NAT this is the external
      address/hostname and forwarded port, NOT where we bind and NOT the
      internal listener port.
    * ``trigger_target_ip`` / ``trigger_udp_port`` -- where the single unicast
      datagram is aimed.
    * ``listener_port`` -- the LOCAL listener port the new session must
      actually arrive on; a session on any other port does not match this
      route and can never prove it.
    """

    bind_ip: str
    trigger_target_ip: str
    trigger_udp_port: int
    advertised_ha_host: str
    advertised_ha_port: int
    listener_port: int

    def invalid_reason(self) -> str:
        """Return why this route cannot be proven, or "" when complete.

        Purely structural validation (present, sane ports): no address
        classification of any kind.
        """

        for label, value in (
            ("bind_ip", self.bind_ip),
            ("trigger_target_ip", self.trigger_target_ip),
            ("advertised_ha_host", self.advertised_ha_host),
        ):
            if type(value) is not str or not value.strip() or value != value.strip():
                return f"route_field_invalid:{label}"
        for label, value in (
            ("trigger_udp_port", self.trigger_udp_port),
            ("advertised_ha_port", self.advertised_ha_port),
            ("listener_port", self.listener_port),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 65536:
                return f"route_field_invalid:{label}"
        return ""

    @property
    def trigger_target(self) -> str:
        """Opaque snapshot of where the trigger was aimed (for the proof)."""

        return f"{self.trigger_target_ip}:{self.trigger_udp_port}"

    @property
    def advertised_ha_endpoint(self) -> str:
        """Opaque snapshot of the advertised endpoint (for the proof)."""

        return f"{self.advertised_ha_host}:{self.advertised_ha_port}"


@dataclass(frozen=True, slots=True)
class RecoveryWireProbeAuthority:
    """Immutable wire authority for probing SILENT recovery reconnects.

    Captured ONLY from the trusted, observed, non-conflicting live
    ``SessionHandle`` of the very session the engine is about to reboot --
    never from an expected protocol, cloud family, endpoint, collector kind,
    peer IP or persisted hint. It permits the engine's single session-pinned
    identity query per causally-new silent candidate on the previously
    observed wire; it is not evidence, never persisted, and dies with the
    verification attempt.
    """

    collector_pn: str
    session_protocol: str
    old_session_id: str

    def __post_init__(self) -> None:
        for value in (self.collector_pn, self.session_protocol, self.old_session_id):
            if type(value) is not str or not value or value != value.strip():
                raise ValueError("recovery_wire_authority_invalid")
        if self.session_protocol not in ("eybond_framed", "at_text"):
            raise ValueError("recovery_wire_authority_protocol_invalid")


def _required_token(value: object) -> bool:
    """A mandatory outcome token: a real non-empty, already-normalized str.

    ``type() is str`` on purpose (no bytes, ints or ducks) and no surrounding
    whitespace -- the constructor never coerces; a padded value is the
    caller's bug, not something to silently clean.
    """

    return type(value) is str and bool(value) and value == value.strip()


@dataclass(frozen=True, slots=True)
class RecoveryVerificationOutcome:
    """Typed IMMUTABLE outcome of ONE combined recovery verification.

    Carries NO connection strategy and NO legacy evidence. Construction is a
    TRUST BOUNDARY enforcing the full status/proof/ownership matrix, so a
    malformed outcome cannot exist:

    * ``callback_verified`` -- exactly a strict-typed ``CallbackRecoveryProof``
      of the SAME identity, plus the NEW session id and the prepared
      ``handoff_owner``;
    * ``inbound_recovered`` -- exactly a strict-typed ``InboundRecoveryProof``
      of the SAME identity, plus the NEW session id and the prepared
      ``handoff_owner``;
    * ``inbound_verified`` (inbound-only mode) -- exactly a strict-typed
      ``InboundRecoveryProof`` and the NEW session id; NO handoff owner (that
      lifecycle belongs to the callback transaction);
    * any failure -- no proof and no handoff owner, ever.

    Every success proof is additionally re-validated through the strict
    ``RecoveryContract`` builders: an outcome may not exist holding a proof
    the persistence model would refuse to store (duck proofs, foreign PNs,
    weak identity sources, naive timestamps, partial route snapshots all
    raise here). Same-identity is judged ONLY by the registry's centralized
    ``pn_is_same_identity`` (short/full reconciliation, never string compare).

    ``handoff_owner`` is the EXACT registry owner token whose claim was
    committed by ``prepare_handoff``: the next batch consumes this capability
    directly instead of reconstructing ownership by PN lookup.
    """

    status: str = STATE_OBSERVED_SESSION
    failure_reason: str = ""
    collector_pn: str = ""
    new_session_id: str = ""
    inbound_proof: InboundRecoveryProof | None = None
    callback_proof: CallbackRecoveryProof | None = None
    handoff_owner: str = ""
    # The SEPARATE permanent-owner ownership mode (Batch 8): a recovery run
    # under an existing permanent owner carries a registry-issued
    # ``PermanentOwnedSessionCertification`` here INSTEAD of a prepared
    # onboarding ``handoff_owner``. The two are mutually exclusive.
    owner_certification: Any = None
    transitions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.inbound_proof is not None and self.callback_proof is not None:
            raise ValueError("recovery_outcome_carries_two_proofs")
        if self.status == STATE_CALLBACK_VERIFIED:
            self._validate_success(
                proof=self.callback_proof,
                proof_type=CallbackRecoveryProof,
                ownership_required=True,
            )
        elif self.status == STATE_INBOUND_RECOVERED:
            self._validate_success(
                proof=self.inbound_proof,
                proof_type=InboundRecoveryProof,
                ownership_required=True,
            )
        elif self.status == STATE_INBOUND_VERIFIED:
            self._validate_success(
                proof=self.inbound_proof,
                proof_type=InboundRecoveryProof,
                ownership_required=False,
            )
        else:
            if self.inbound_proof is not None or self.callback_proof is not None:
                raise ValueError("proof_requires_success_status")
            if self.handoff_owner:
                raise ValueError("handoff_owner_requires_success_status")
            if self.owner_certification is not None:
                raise ValueError("owner_certification_requires_success_status")

    def _validate_success(
        self,
        *,
        proof: Any,
        proof_type: type,
        ownership_required: bool,
    ) -> None:
        """Fail fast on any success shape the trust boundary must refuse.

        No coercion anywhere: a duck/wrong-type proof is a TypeError, a bad
        value a ValueError -- both are caller bugs, never data to clean up.
        """

        from ..connection.session_registry import (
            PermanentOwnedSessionCertification,
        )

        if type(proof) is not proof_type:
            raise TypeError("recovery_outcome_proof_type_invalid")
        if not _required_token(self.collector_pn):
            raise ValueError("recovery_outcome_requires_normalized_pn")
        if not _required_token(self.new_session_id):
            raise ValueError("recovery_outcome_requires_new_session_id")
        has_handoff = bool(_required_token(self.handoff_owner))
        has_certification = self.owner_certification is not None
        if has_certification and (
            type(self.owner_certification) is not PermanentOwnedSessionCertification
        ):
            raise TypeError("owner_certification_type_invalid")
        if ownership_required:
            # EXACTLY ONE ownership mode: the onboarding prepared handoff OR
            # the permanent-owner certification, never both, never neither.
            if has_handoff and has_certification:
                raise ValueError("recovery_outcome_two_ownership_modes")
            if not has_handoff and not has_certification:
                raise ValueError("recovery_outcome_requires_ownership")
        else:
            if has_handoff:
                raise ValueError("handoff_owner_requires_callback_transaction")
            if has_certification:
                raise ValueError("owner_certification_requires_callback_transaction")
        if self.failure_reason:
            raise ValueError("success_outcome_carries_failure_reason")
        if not pn_is_same_identity(self.collector_pn, proof.collector_pn):
            # The ONE centralized short/full reconciliation rule; a foreign
            # identity can never ride out under this outcome's PN.
            raise ValueError("recovery_outcome_proof_identity_mismatch")
        # The persistence gate: re-validate the proof through the strict
        # RecoveryContract builders (raises TypeError/ValueError on duck
        # fields, weak sources, naive timestamps, partial routes, foreign
        # PNs). An outcome must never carry a proof that cannot be stored.
        contract = RecoveryContract.empty_for_pn(
            self.collector_pn, identity_source=proof.identity_source
        )
        if proof_type is CallbackRecoveryProof:
            contract.with_callback_proof(proof, updated_at=proof.verified_at)
        else:
            contract.with_inbound_proof(proof, updated_at=proof.verified_at)

    @property
    def inbound_recovered(self) -> bool:
        return (
            self.status in (STATE_INBOUND_RECOVERED, STATE_INBOUND_VERIFIED)
            and self.inbound_proof is not None
        )

    @property
    def callback_verified(self) -> bool:
        return self.status == STATE_CALLBACK_VERIFIED and self.callback_proof is not None


class CallbackRecoveryTriggerSender(Protocol):
    """Sends exactly ONE logical unicast set>server sequence for a route."""

    async def async_send(self, route: CallbackRecoveryRoute) -> None: ...


class _ProductionRecoveryTriggerSender:
    """The one production sender: the shared ledger-recorded trigger facade.

    Same choke point as every other callback sender in the integration
    (``collector.discovery.async_send_callback_trigger``): compatible
    ``set>server`` payload variants form ONE logical sequence recorded as ONE
    ledger send. The advertised values go into the payload VERBATIM -- the
    bind address never leaks into it.
    """

    def __init__(self, *, timeout: float) -> None:
        self._timeout = float(timeout)

    async def async_send(self, route: CallbackRecoveryRoute) -> None:
        from ..collector.discovery import async_send_callback_trigger

        await async_send_callback_trigger(
            bind_ip=route.bind_ip,
            advertised_server_ip=route.advertised_ha_host,
            advertised_server_port=int(route.advertised_ha_port),
            target_ip=route.trigger_target_ip,
            udp_port=int(route.trigger_udp_port),
            timeout=self._timeout,
            source="callback_recovery_transaction",
        )


def _session_state(session: Mapping[str, Any]) -> str:
    return str(session.get("state") or "").strip().lower()


def _session_is_closed(session: Mapping[str, Any]) -> bool:
    state = _session_state(session)
    return any(state.startswith(prefix) for prefix in _CLOSED_STATE_PREFIXES)


def _session_has_strong_identity(session: Mapping[str, Any]) -> bool:
    """Return the registry-provided strong-identity verdict for one session dict.

    The strong/weak decision is centralized in the CallbackSessionRegistry
    (``CallbackSession.has_strong_identity``); sessions_source projections carry
    it as a plain bool. No local identity-source allowlist, no length heuristics.
    """

    return bool(session.get("has_strong_identity"))


class _ControlledResetRecoveryEngine:
    """THE controlled-reset recovery state machine (one implementation).

    Shared by both public verifiers -- there is deliberately no second
    algorithm:

    * ``InboundRecoveryVerifier`` runs it with ``callback_route=None``
      (inbound-only: the Batch-3A behavior, byte for byte);
    * ``CallbackRecoveryVerifier`` supplies a route, turning the inbound
      window's expiry into the callback phase instead of a failure.

    Phase structure and the ONE causal window::

        causality_lease (held from BEFORE baseline to the terminal outcome)
        |-- inhibit_callback_triggers
        |     strong identity -> promote -> baseline -> adapter reboot
        |     -> old socket closed -> bounded inbound reconnect wait
        |__ (inhibitor exits HERE; the lease does NOT)
        callback phase (route mode only):
              post-reset baseline -> exactly ONE own unicast sequence
              -> bounded callback session wait -> authoritative identity
              -> route/listener check -> contract pre-validation
              -> final claim retarget -> callback proof

    The inhibitor guarantees a silent reset window; exiting it while STILL
    holding the exclusive lease is what lets this attempt send its OWN trigger
    (the ledger's send gate admits the lease owner and refuses everyone else),
    so no other in-process attempt can slip a trigger between the reset and
    the proof. If an autonomous inbound reconnect appears, the callback phase
    never runs: no trigger is sent and the already-proven inbound proof is
    returned as ``inbound_recovered``.

    HONEST CAUSALITY BOUNDARY: the exclusivity above is PROCESS-LOCAL -- the
    ledger's own documented guarantee. A callback proof is the result of a
    controlled recovery experiment under in-process exclusivity: it excludes
    other in-process triggers, every baseline session, a different PN and a
    different listener route. It CANNOT physically exclude an external sender
    (a SmartESS app, a script, another HA instance) unicasting ``set>server``
    to the same collector inside the window -- such a datagram is
    indistinguishable on our side. No heuristic tries to detect it; the
    engine stays fail-closed within the evidence it can actually observe.

    All IO is injected: ``restart_channel`` owns the management-adapter path,
    ``trigger_sender`` the ledger-recorded unicast facade, ``sessions_source``
    the public registry projection. The clock is injected; the proofs are
    PRE-VALIDATED through the strict RecoveryContract builder before any final
    retarget.
    """

    def __init__(
        self,
        *,
        collector_pn: str,
        session_id: str,
        restart_channel: RestartChannel,
        sessions_source: Callable[[], Iterable[Mapping[str, Any]]],
        clock: Callable[[], str],
        policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        callback_route: CallbackRecoveryRoute | None = None,
        trigger_sender: CallbackRecoveryTriggerSender | None = None,
        callback_trigger_generation: Callable[[], int] | None = None,
        promote_claim: Callable[[str], None] | None = None,
        retarget_claim: Callable[[str], bool] | None = None,
        prepare_handoff: Callable[[str], str] | None = None,
        owner_certifier: Callable[[str], Any] | None = None,
        probe_reconnected_identity: Callable[[str], Any] | None = None,
        silent_session_probe: Any = None,
        ledger: Any = None,
        poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._collector_pn = str(collector_pn or "").strip()
        self._session_id = str(session_id or "").strip()
        self._restart_channel = restart_channel
        self._sessions_source = sessions_source
        # The injected time source for ``verified_at`` -- the engine never
        # calls now() itself, and the recovery-contract model validates the
        # value (timezone-aware ISO) when the proof is attached to a contract.
        self._clock = clock
        self._policy = policy
        self._callback_route = callback_route
        self._trigger_sender = trigger_sender
        self._callback_trigger_generation = callback_trigger_generation or (lambda: 0)
        # Ownership promotion hook: called with the strong FULL PN right after
        # the identity phase, BEFORE baseline/restart, so the transient
        # session-id claim becomes the durable full-PN claim in the registry.
        # Raising ValueError means another owner holds the identity.
        self._promote_claim = promote_claim
        # Ownership retarget hook: MUST move the registry claim from the closed
        # old socket to ``new_session_id`` (idempotent when already there).
        # Success without a retargeted claim is not success: the entry's
        # ownership handoff must carry the NEW socket. MANDATORY for any
        # proof-producing run -- ``async_verify`` refuses upfront when absent.
        self._retarget_claim = retarget_claim
        # Handoff commit hook: called with the certified full PN AFTER the
        # final retarget; must commit the registry claim as a prepared handoff
        # and return the EXACT owner token (empty string = refusal). Mandatory
        # whenever a callback route is armed: those successes are consumed as
        # ready-to-handoff capabilities, never re-discovered by PN.
        self._prepare_handoff = prepare_handoff
        # The SEPARATE permanent-owner ownership mode (mutually exclusive with
        # prepare_handoff): called with the certified full PN after the final
        # retarget; must return a registry-issued
        # ``PermanentOwnedSessionCertification`` or None (None = refusal). Used
        # by a recovery run under an existing permanent owner, so the outcome
        # carries a real certification instead of a fake prepared handoff.
        if prepare_handoff is not None and owner_certifier is not None:
            raise ValueError("recovery_engine_two_ownership_modes")
        self._owner_certifier = owner_certifier
        self._probe_reconnected_identity = probe_reconnected_identity
        # The narrow public transport boundary for FULLY SILENT reconnects
        # (see collector.silent_session_probe). Optional: without it the
        # engine sees only sessions that volunteer identity, as before.
        self._silent_session_probe = silent_session_probe
        # Captured from the TRUSTED live handle right before the reboot; the
        # only wire authority a silent-reconnect probe may use.
        self._wire_authority: RecoveryWireProbeAuthority | None = None
        self._pending_baseline: frozenset[str] = frozenset()
        self._ledger = ledger
        self._disconnect_timeout = max(
            0.0, float(policy.inbound_restart_disconnect_timeout)
        )
        self._reconnect_timeout = max(0.0, float(policy.inbound_reconnect_timeout))
        self._identity_timeout = max(0.0, float(policy.inbound_strong_identity_timeout))
        self._callback_wait_timeout = max(
            0.0, float(policy.callback_recovery_session_wait)
        )
        self._poll_interval = max(0.01, float(poll_interval))
        self._baseline_session_ids: frozenset[str] = frozenset()
        self._transitions: list[str] = [STATE_OBSERVED_SESSION]
        # The lease's per-attempt trigger accounting (own vs foreign sends);
        # bound while the causality lease is held.
        self._attempt: Any = None

    def _enter(self, state: str) -> None:
        self._transitions.append(state)

    def _result(
        self,
        *,
        failure_reason: str = "",
        new_session_id: str = "",
        inbound_proof: InboundRecoveryProof | None = None,
        callback_proof: CallbackRecoveryProof | None = None,
        handoff_owner: str = "",
        owner_certification: Any = None,
    ) -> RecoveryVerificationOutcome:
        return RecoveryVerificationOutcome(
            status=self._transitions[-1],
            failure_reason=failure_reason,
            collector_pn=self._collector_pn,
            new_session_id=new_session_id,
            inbound_proof=inbound_proof,
            callback_proof=callback_proof,
            handoff_owner=handoff_owner,
            owner_certification=owner_certification,
            transitions=tuple(self._transitions),
        )

    def _fail(self, reason: str) -> RecoveryVerificationOutcome:
        self._enter(STATE_INBOUND_NOT_VERIFIED)
        return self._result(failure_reason=reason)

    def _sessions(self) -> tuple[Mapping[str, Any], ...]:
        try:
            return tuple(self._sessions_source() or ())
        except Exception:
            logger.debug("Recovery verification sessions source failed", exc_info=True)
            return ()

    def _old_session_live(self) -> bool:
        for session in self._sessions():
            if str(session.get("session_id") or "").strip() != self._session_id:
                continue
            return not _session_is_closed(session)
        return False

    def _session_entry(self, session_id: str) -> Mapping[str, Any] | None:
        for session in self._sessions():
            if str(session.get("session_id") or "").strip() == session_id:
                return session
        return None

    def _observed_session_entry(self) -> Mapping[str, Any] | None:
        return self._session_entry(self._session_id)

    def _capture_wire_authority(self) -> None:
        """Freeze the silent-reconnect probe authority from the trusted handle.

        Called AFTER strong identity, BEFORE the reboot: the old session is
        still live and its negotiated wire is positive observed evidence. A
        channel that cannot vouch for a wire yields no authority -- silent
        candidates then simply cannot be probed (fail-closed, as before).
        """

        self._wire_authority = None
        observed_wire = getattr(self._restart_channel, "observed_wire", None)
        if not callable(observed_wire):
            return
        try:
            wire = str(observed_wire() or "").strip()
        except Exception:
            wire = ""
        if not wire:
            return
        try:
            self._wire_authority = RecoveryWireProbeAuthority(
                collector_pn=self._collector_pn,
                session_protocol=wire,
                old_session_id=self._session_id,
            )
        except ValueError:
            self._wire_authority = None

    def _snapshot_pending_baseline(self) -> None:
        probe = self._silent_session_probe
        if probe is None:
            self._pending_baseline = frozenset()
            return
        try:
            self._pending_baseline = frozenset(probe.snapshot_silent_session_ids())
        except Exception:
            self._pending_baseline = frozenset()

    def _capture_baseline(self) -> None:
        """Record EVERY session id visible right now (plus the old socket).

        A collector can hold several parallel sessions of the same durable PN.
        Only a session whose id was absent from the WHOLE baseline can prove a
        dial-in; comparing against the single selected old session id is not
        enough. Called once before the reboot (inbound phase) and AGAIN after
        the inbound window expires (post-reset baseline for the callback
        phase).
        """

        self._baseline_session_ids = frozenset(
            str(session.get("session_id") or "").strip()
            for session in self._sessions()
            if str(session.get("session_id") or "").strip()
        ) | {self._session_id}

    def _find_new_inbound_session(self) -> str:
        """Return the session_id of a NEW live session of the same full PN, or ""."""

        for session in self._sessions():
            session_id = str(session.get("session_id") or "").strip()
            if not session_id or session_id in self._baseline_session_ids:
                # Any baseline socket (or its re-listing) can never confirm a
                # dial-in -- including parallel baseline sessions of the same PN.
                continue
            if _session_is_closed(session):
                continue
            if _session_state(session) in _UNTRUSTED_SESSION_STATES:
                continue
            if not _session_has_strong_identity(session):
                # Only a strong (registry-certified) identity can prove the
                # dial-in; weak observations keep waiting.
                continue
            if str(session.get("collector_pn") or "").strip() != self._collector_pn:
                # After strong promotion the durable identity is FINAL: only the
                # exact full PN confirms (stricter than -- and consistent with --
                # the registry's short/full reconciliation, which is what made
                # the durable PN full in the first place). A different collector
                # behind the same peer IP never matches.
                continue
            return session_id
        return ""

    def _find_new_weak_identity_candidate(self) -> str:
        """Return one new trusted socket needing authoritative PN enrichment."""

        for session in self._sessions():
            session_id = str(session.get("session_id") or "").strip()
            if not session_id or session_id in self._baseline_session_ids:
                continue
            if _session_is_closed(session) or _session_state(session) in _UNTRUSTED_SESSION_STATES:
                continue
            if _session_has_strong_identity(session):
                continue
            session_pn = str(session.get("collector_pn") or "").strip()
            if session_pn and pn_is_same_identity(self._collector_pn, session_pn):
                return session_id
        return ""

    async def _async_wait_for_new_same_pn_session(
        self, deadline: float
    ) -> _ReconnectWaitResult:
        """One shared bounded wait for a NEW strong same-PN session.

        Used by BOTH phases (inbound reconnect and callback session): weak
        same-identity candidates get exactly ONE claim-retarget + authoritative
        enrichment attempt each; a duplicate probe is structurally impossible.

        Returns a typed :class:`_ReconnectWaitResult`. A demonstrably-arrived
        FULLY-SILENT socket that cannot become the session is diagnosed
        honestly instead of collapsing into a plain timeout: a foreign strong
        PN is definitive and returns at once (no retarget/claim); ambiguity, a
        query that ran without a strong PN, and an unavailable probe channel
        keep waiting for passive evidence and, only at the deadline, surface
        their own typed failure. No silent socket ever observed -> empty
        ``silent_failure`` and the caller's plain timeout.
        """

        loop = asyncio.get_running_loop()
        identity_probe_attempted: set[str] = set()
        silent_probe_attempted: set[str] = set()
        last_silent_failure = ""
        while True:
            new_session_id = self._find_new_inbound_session()
            if new_session_id:
                return _ReconnectWaitResult(session_id=new_session_id)
            observation = await self._async_probe_single_silent_candidate(
                silent_probe_attempted
            )
            if observation == _SILENT_OBS_FOREIGN:
                # A demonstrably foreign strong PN answered on the exact
                # bound socket: definitive, and never retargeted or claimed.
                return _ReconnectWaitResult(
                    silent_failure=FAILURE_RECOVERY_IDENTITY_MISMATCH
                )
            failure = _SILENT_OBS_TO_FAILURE.get(observation)
            if failure:
                # Sticky diagnosis: keep waiting (passive evidence may still
                # resolve to a valid same-PN session), but remember the most
                # recent honest cause for the deadline.
                last_silent_failure = failure
            weak_session_id = self._find_new_weak_identity_candidate()
            if (
                weak_session_id
                and weak_session_id not in identity_probe_attempted
                and self._probe_reconnected_identity is not None
            ):
                # EXACTLY ONE authoritative enrichment attempt per candidate
                # socket. The claim must be retargeted to the candidate FIRST,
                # because the reader is pinned to the registry-claimed session.
                identity_probe_attempted.add(weak_session_id)
                # The hook is guaranteed by the upfront ownership gate.
                if self._retarget_claim(weak_session_id):
                    try:
                        result = self._probe_reconnected_identity(weak_session_id)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as exc:
                        logger.info(
                            "Recovery verification: identity probe failed for %s: %s",
                            weak_session_id,
                            exc,
                        )
            if loop.time() >= deadline:
                return _ReconnectWaitResult(silent_failure=last_silent_failure)
            await asyncio.sleep(self._poll_interval)

    async def _async_probe_single_silent_candidate(self, attempted: set[str]) -> str:
        """One identity query per causally-new FULLY SILENT candidate.

        Returns a typed observation (never raises for flow control):

        * ``no_candidate`` -- no causally-new silent socket (or no silent
          probing wired at all);
        * ``probe_unavailable`` -- a probe channel exists but could not open;
        * ``ambiguous`` -- two or more causally-new silent sockets (never
          probed -- the answer cannot be attributed to one);
        * ``same_identity_observed`` -- the exact socket answered with the
          same durable identity (recorded in the inventory; the normal
          same-PN finder takes over next poll);
        * ``foreign_identity`` -- it answered with a DIFFERENT strong PN;
        * ``probe_failed`` -- the query ran and produced no strong PN.

        The query is session-pinned to that one id on the PREVIOUSLY OBSERVED
        wire -- no second protocol, no retry.
        """

        probe = self._silent_session_probe
        authority = self._wire_authority
        if probe is None or authority is None:
            # No silent-probe capability wired / no trusted wire authority:
            # the wire-authority contract is unchanged, this simply means the
            # silent path is inert here.
            return _SILENT_OBS_NONE
        if not getattr(probe, "available", True):
            return _SILENT_OBS_UNAVAILABLE
        try:
            live_silent = frozenset(probe.snapshot_silent_session_ids())
        except Exception:
            return _SILENT_OBS_NONE
        candidates = frozenset(
            session_id
            for session_id in live_silent
            if session_id not in self._pending_baseline
            and session_id not in self._baseline_session_ids
            and session_id != self._session_id
            and session_id not in attempted
        )
        if not candidates:
            return _SILENT_OBS_NONE
        if len(candidates) != 1:
            return _SILENT_OBS_AMBIGUOUS
        candidate = next(iter(candidates))
        attempted.add(candidate)
        try:
            probed_pn = await probe.async_identify_exact_session(
                candidate,
                session_protocol=authority.session_protocol,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("Silent reconnect probe failed on %s: %s", candidate, exc)
            return _SILENT_OBS_PROBE_FAILED
        probed_pn = str(probed_pn or "").strip()
        if not probed_pn:
            return _SILENT_OBS_PROBE_FAILED
        if not pn_is_same_identity(self._collector_pn, probed_pn):
            logger.info(
                "Silent reconnect candidate %s answered foreign identity", candidate
            )
            return _SILENT_OBS_FOREIGN
        logger.debug(
            "Silent reconnect candidate %s identified as same collector via %s",
            candidate,
            authority.session_protocol,
        )
        return _SILENT_OBS_SAME

    def _new_session_proof_fields(
        self, new_session_id: str
    ) -> tuple[str, str] | RecoveryVerificationOutcome:
        """Return (identity_source, session_protocol) for the proof, or a typed failure.

        The proof's fields come from the REGISTRY's view of the new socket: the
        strong identity source it certified and the live negotiated wire
        (informative). An untrusted/conflicting observation cannot carry a
        proof even though it matched the wait predicate.
        """

        new_entry = self._session_entry(new_session_id)
        if new_entry is None:
            return self._fail(FAILURE_RECONNECT_TIMEOUT)
        identity_source = str(
            new_entry.get("collector_identity_source") or ""
        ).strip()
        if not identity_source_is_strong(identity_source):
            return self._fail(FAILURE_RECONNECTED_SESSION_UNTRUSTED)
        handle = negotiate_session_adapters(new_entry.get("raw"))
        if handle.conflict:
            return self._fail(FAILURE_RECONNECTED_SESSION_UNTRUSTED)
        return identity_source, handle.wire_framing if handle.observed else ""

    def _session_listener_port(self, session_id: str) -> int:
        entry = self._session_entry(session_id)
        if entry is None:
            return 0
        raw = entry.get("raw")
        candidates = (
            entry.get("listener_port"),
            raw.get("listener_port") if isinstance(raw, Mapping) else None,
        )
        for value in candidates:
            if isinstance(value, bool):
                continue
            if isinstance(value, int) and 0 < value < 65536:
                return value
        return 0

    def _safe_clock(self) -> str:
        try:
            return str(self._clock() or "").strip()
        except Exception:
            return ""

    async def async_verify(self) -> RecoveryVerificationOutcome:
        """Run the WHOLE transaction inside its ONE causal window.

        The exclusive causality lease is acquired BEFORE the baseline and held
        until the terminal outcome -- across BOTH phases. The trigger inhibitor
        covers only the reset/inbound window; exiting it does NOT release the
        lease. On any failure or cancellation: no proof, the channel is
        closed, lease and inhibitor are released by their context managers.
        """

        if not self._collector_pn or not self._session_id:
            return self._fail(FAILURE_SESSION_UNAVAILABLE)
        has_ownership_commit = (
            self._prepare_handoff is not None or self._owner_certifier is not None
        )
        if self._retarget_claim is None or (
            self._callback_route is not None and not has_ownership_commit
        ):
            # Every proof-bearing success ends in a claim retarget (and, for
            # the callback transaction, a committed ownership capability --
            # either a prepared onboarding handoff or a permanent-owner
            # certification). Without those no success can exist -- refuse
            # BEFORE the lease and BEFORE the collector is ever rebooted.
            return self._fail(FAILURE_OWNERSHIP_UNAVAILABLE)
        if self._callback_route is not None:
            invalid = self._callback_route.invalid_reason()
            if invalid:
                # A route that cannot be proven fails BEFORE any reset or
                # trigger -- the collector is never touched.
                logger.info("Callback recovery route invalid: %s", invalid)
                return self._fail(FAILURE_ROUTE_INVALID)

        ledger = self._ledger if self._ledger is not None else get_callback_trigger_ledger()
        try:
            async with ledger.causality_lease(
                f"recovery_verification:{uuid.uuid4().hex}",
                timeout=self._policy.callback_causality_lease_wait,
            ) as attempt:
                self._attempt = attempt
                try:
                    # Phase 1 -- the silent reset window: reboot + autonomous
                    # inbound wait under the trigger inhibitor.
                    async with ledger.inhibit_callback_triggers():
                        reset_outcome = await self._async_reset_phase()
                    if reset_outcome is not None:
                        return reset_outcome
                    # Phase 2 -- the callback phase. The inhibitor is gone (we
                    # must send our OWN trigger) but the lease is STILL OURS:
                    # the send gate admits only this attempt, so nobody else
                    # can re-open causal ambiguity between reset and proof.
                    return await self._async_callback_phase()
                finally:
                    self._attempt = None
                    # Idempotent: most paths already closed the channel; a
                    # cancellation mid-phase must not leak the claimed socket.
                    await self._close_channel()
        except CallbackCausalityBusyError:
            # Another callback attempt owns causality: honest typed refusal --
            # we never touched the collector.
            return self._fail(FAILURE_CAUSALITY_BUSY)

    async def _async_reset_phase(self) -> RecoveryVerificationOutcome | None:
        """Identity -> promote -> baseline -> reboot -> disconnect -> inbound wait.

        Returns a terminal outcome, or ``None`` exactly when the inbound window
        expired AND a callback route is armed (the expected transition to the
        callback phase -- not a failure).
        """

        loop = asyncio.get_running_loop()
        generation_before = self._safe_trigger_generation()

        # observed_session -> waiting_for_strong_identity: never restart a
        # collector whose durable identity is not yet strong. Two matching short
        # PNs do not prove identity; the registry is the strong/weak authority.
        self._enter(STATE_WAITING_FOR_STRONG_IDENTITY)
        observed = self._observed_session_entry()
        if observed is None or not _session_has_strong_identity(observed):
            # A passive heartbeat commonly carries only a short PN. Once the
            # user consented, issue one safe read-only identity query over the
            # exact transient-claimed socket (negotiated wire; FC=2 or DTUPN).
            # Its response is recorded in the registry as strong evidence
            # before this coroutine resumes.
            probe_identity = getattr(self._restart_channel, "async_probe_identity", None)
            if callable(probe_identity):
                try:
                    await probe_identity()
                except Exception as exc:
                    logger.info(
                        "Recovery verification: collector identity probe did not complete: %s",
                        exc,
                    )
            observed = self._observed_session_entry()
        deadline = loop.time() + self._identity_timeout
        while True:
            if observed is not None and _session_has_strong_identity(observed):
                strong_pn = str(observed.get("collector_pn") or "").strip()
                if strong_pn and (
                    not self._collector_pn
                    or pn_is_same_identity(self._collector_pn, strong_pn)
                ):
                    if len(strong_pn) >= len(self._collector_pn):
                        # Adopt the enriched full PN as the durable identity.
                        self._collector_pn = strong_pn
                    break
            if loop.time() >= deadline:
                await self._close_channel()
                return self._fail(FAILURE_STRONG_IDENTITY_TIMEOUT)
            await asyncio.sleep(self._poll_interval)
            observed = self._observed_session_entry()

        # Promote the transient session-id claim to the now-final full durable
        # PN BEFORE baseline/restart. A conflict means another owner holds the
        # identity: stop without touching the collector.
        if self._promote_claim is not None:
            try:
                self._promote_claim(self._collector_pn)
            except ValueError as exc:
                logger.info(
                    "Recovery verification: identity %s already claimed during promotion: %s",
                    self._collector_pn,
                    exc,
                )
                await self._close_channel()
                return self._fail(FAILURE_SESSION_CLAIMED)

        # Baseline of ALL currently-visible sessions, captured before restart
        # (and, thanks to the lease, after every other attempt's causal window
        # has fully closed). The silent-pending snapshot and the trusted-wire
        # probe authority freeze at the same causal point: the old session is
        # still live, so its negotiated wire is positive observed evidence.
        self._capture_wire_authority()
        self._snapshot_pending_baseline()
        self._capture_baseline()

        # waiting_for_strong_identity -> restart_requested. The reboot goes
        # through the negotiated management adapter inside the channel; an
        # adapter that honestly cannot reboot (AT text today) surfaces as the
        # typed unsupported failure without any wire write.
        self._enter(STATE_RESTART_REQUESTED)
        try:
            await self._restart_channel.async_send_restart()
        except CollectorManagementUnsupportedError as exc:
            logger.info(
                "Recovery verification: collector %s management unsupported: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_RESTART_NOT_SUPPORTED)
        except SessionUnavailableError as exc:
            logger.info(
                "Recovery verification: collector %s session unavailable: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_SESSION_UNAVAILABLE)
        except Exception as exc:
            logger.info(
                "Recovery verification: collector %s restart not confirmed: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_RESTART_NOT_CONFIRMED)

        try:
            # restart_requested -> waiting_for_disconnect: the collector itself
            # must drop the old TCP session (we never close it ourselves before
            # observing the disconnect, so the EOF is genuine device behavior).
            self._enter(STATE_WAITING_FOR_DISCONNECT)
            deadline = loop.time() + self._disconnect_timeout
            # The registry's physical session_id is the sole disconnect truth.
            # ``RestartChannel`` wraps a reusable transport facade which may
            # immediately attach to a successor socket and remain connected;
            # consulting it here would turn a successful reboot/reconnect into
            # a false ``disconnect_not_observed`` result.
            while self._old_session_live():
                if loop.time() >= deadline:
                    return self._fail(FAILURE_DISCONNECT_NOT_OBSERVED)
                await asyncio.sleep(self._poll_interval)
        finally:
            # Release the (now dead or failed) claimed socket before watching
            # for the collector's fresh dial-in.
            await self._close_channel()

        # waiting_for_disconnect -> waiting_for_inbound_reconnect. The FULL
        # bounded inbound window always runs first: autonomous recovery must be
        # excluded before any callback may be proven.
        self._enter(STATE_WAITING_FOR_INBOUND_RECONNECT)
        wait = await self._async_wait_for_new_same_pn_session(
            loop.time() + self._reconnect_timeout
        )
        new_session_id = wait.session_id
        if not new_session_id:
            if self._callback_route is not None:
                # The expected transition to the callback phase, not a failure.
                # A diagnostic from THIS autonomous inbound window (silent
                # ambiguity, a foreign answer, a failed/unavailable probe)
                # must NOT pre-empt a provable callback success -- the
                # callback phase runs its own post-reset baseline/window.
                return None
            # Inbound-only: surface the honest silent diagnosis when a socket
            # demonstrably arrived; otherwise the plain reconnect timeout.
            return self._fail(wait.silent_failure or FAILURE_RECONNECT_TIMEOUT)

        # Defense in depth behind the lease+inhibitor: if ANY callback trigger
        # was recorded anywhere in the integration meanwhile, the reconnect
        # proves nothing -- conservatively refuse to certify inbound. A false
        # refusal is safe (manual callback follows); a false inbound is not.
        if self._safe_trigger_generation() != generation_before:
            return self._fail(FAILURE_UDP_TRIGGER_OBSERVED)

        fields = self._new_session_proof_fields(new_session_id)
        if isinstance(fields, RecoveryVerificationOutcome):
            return fields
        identity_source, session_protocol = fields

        # Build the proof and PRE-VALIDATE it through the strict
        # RecoveryContract builder BEFORE the final retarget: this engine must
        # never return a success whose proof the contract model would refuse
        # to persist. A naive/empty/invalid clock value or any other malformed
        # field is a typed failure -- no proof, no final retarget, and the
        # normal channel/lease/inhibitor cleanup applies.
        verified_at = self._safe_clock()
        proof = InboundRecoveryProof(
            method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
            collector_pn=self._collector_pn,
            identity_source=identity_source,
            verified_at=verified_at,
            session_protocol=session_protocol,
        )
        try:
            RecoveryContract.empty_for_pn(
                self._collector_pn, identity_source=identity_source
            ).with_inbound_proof(proof, updated_at=verified_at)
        except (TypeError, ValueError) as exc:
            logger.info(
                "Recovery verification: inbound proof for %s failed contract validation: %s",
                self._collector_pn,
                exc,
            )
            return self._fail(FAILURE_INBOUND_PROOF_INVALID)

        # SUCCESS leaves the registry claim bound to the NEW socket, never the
        # closed baseline one -- the entry's ownership handoff must carry the
        # session the collector actually opened. Idempotent when the weak-path
        # enrichment already retargeted. The hook is guaranteed by the upfront
        # ownership gate: success without a retargeted claim cannot exist.
        if not self._retarget_claim(new_session_id):
            return self._fail(FAILURE_SESSION_CLAIMED)

        # The collector recovered ON ITS OWN: in route mode no trigger is ever
        # sent and the honest outcome is inbound_recovered (with the inbound
        # proof already earned by this very reset -- no second reboot). The
        # callback transaction's successes are consumed as ready-to-handoff
        # capabilities, so the claim is COMMITTED here and the exact owner
        # token travels in the outcome; a refused commit is a typed failure
        # (the wrapper then releases everything).
        handoff_owner = ""
        owner_certification: Any = None
        if self._callback_route is not None:
            handoff_owner, owner_certification = self._commit_ownership()
            if not handoff_owner and owner_certification is None:
                return self._fail(FAILURE_SESSION_CLAIMED)
        self._enter(
            STATE_INBOUND_VERIFIED
            if self._callback_route is None
            else STATE_INBOUND_RECOVERED
        )
        return self._result(
            new_session_id=new_session_id,
            inbound_proof=proof,
            handoff_owner=handoff_owner,
            owner_certification=owner_certification,
        )

    async def _async_callback_phase(self) -> RecoveryVerificationOutcome:
        """One unicast sequence on the explicit route, inside the SAME lease.

        The resulting proof records a controlled recovery experiment under
        PROCESS-LOCAL exclusivity (the ledger's documented boundary): it rules
        out other in-process triggers, all baseline sessions, foreign PNs and
        wrong-route arrivals -- it cannot rule out an external sender
        unicasting the same collector inside the window, and no heuristic
        pretends otherwise.
        """

        loop = asyncio.get_running_loop()
        route = self._callback_route
        assert route is not None  # dispatch guarantee

        # POST-RESET baseline: the inbound window's sockets (none matched) and
        # anything else visible now can never be the callback answer -- the
        # silent-pending snapshot moves to the same causal point.
        self._snapshot_pending_baseline()
        self._capture_baseline()

        attempt = self._attempt
        if attempt is not None and attempt.foreign_sends:
            # Something recorded a foreign send during our window (an
            # uncoordinated in-process caller): causality is already spoiled.
            return self._fail(FAILURE_CALLBACK_INTERFERENCE)

        self._enter(STATE_CALLBACK_TRIGGER_REQUESTED)
        sender = self._trigger_sender or _ProductionRecoveryTriggerSender(
            timeout=self._policy.discovery_timeout
        )
        try:
            await sender.async_send(route)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.info("Callback recovery trigger could not be sent: %s", exc)
            return self._fail(FAILURE_TRIGGER_NOT_SENT)
        if attempt is not None and attempt.own_sends != 1:
            # Exactly ONE own logical sequence, confirmed by the ledger --
            # fewer means our datagram never went out; more means the sender
            # violated the one-sequence contract.
            logger.info(
                "Callback recovery sent %d own sequences (expected 1)",
                attempt.own_sends,
            )
            return self._fail(FAILURE_TRIGGER_NOT_SENT)

        self._enter(STATE_WAITING_FOR_CALLBACK_SESSION)
        wait = await self._async_wait_for_new_same_pn_session(
            loop.time() + self._callback_wait_timeout
        )
        new_session_id = wait.session_id
        if not new_session_id:
            # Only THIS phase's post-reset baseline/window fed the wait, so a
            # silent diagnosis here is attributable to the callback attempt.
            return self._fail(wait.silent_failure or FAILURE_CALLBACK_TIMEOUT)
        if attempt is not None and attempt.foreign_sends:
            # A foreign trigger fired inside OUR window: the new session is not
            # attributable to our sequence.
            return self._fail(FAILURE_CALLBACK_INTERFERENCE)

        fields = self._new_session_proof_fields(new_session_id)
        if isinstance(fields, RecoveryVerificationOutcome):
            return fields
        identity_source, session_protocol = fields

        # The session must have arrived on the ROUTE's listener port: a socket
        # on any other port does not match the advertised endpoint under proof.
        arrived_port = self._session_listener_port(new_session_id)
        if arrived_port != route.listener_port:
            logger.info(
                "Callback recovery session arrived on port %s, route expects %s",
                arrived_port,
                route.listener_port,
            )
            return self._fail(FAILURE_ROUTE_INVALID)

        # Candidate proof -> contract pre-validation -> ONLY THEN the final
        # retarget/ownership step. A malformed clock/route/proof must never
        # move the claim for a success that cannot exist.
        verified_at = self._safe_clock()
        proof = CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=self._collector_pn,
            identity_source=identity_source,
            verified_at=verified_at,
            trigger_target=route.trigger_target,
            advertised_ha_endpoint=route.advertised_ha_endpoint,
            listener_port=route.listener_port,
        )
        try:
            RecoveryContract.empty_for_pn(
                self._collector_pn, identity_source=identity_source
            ).with_callback_proof(proof, updated_at=verified_at)
        except (TypeError, ValueError) as exc:
            logger.info(
                "Callback recovery proof for %s failed contract validation: %s",
                self._collector_pn,
                exc,
            )
            return self._fail(FAILURE_CALLBACK_PROOF_INVALID)

        if not self._retarget_claim(new_session_id):
            return self._fail(FAILURE_SESSION_CLAIMED)
        # Retarget succeeded on the NEW socket: commit the ownership capability
        # under this transaction's owner (onboarding prepared handoff OR
        # permanent-owner certification). The exact capability travels in the
        # outcome -- the consumer holds it, not a PN to search.
        handoff_owner, owner_certification = self._commit_ownership()
        if not handoff_owner and owner_certification is None:
            return self._fail(FAILURE_SESSION_CLAIMED)

        self._enter(STATE_CALLBACK_VERIFIED)
        return self._result(
            new_session_id=new_session_id,
            callback_proof=proof,
            handoff_owner=handoff_owner,
            owner_certification=owner_certification,
        )

    def _commit_ownership(self) -> tuple[str, Any]:
        """Commit ownership via whichever mode is configured; refusal -> empty.

        Returns ``(handoff_owner, owner_certification)`` with exactly one side
        populated (or both empty on refusal). Onboarding runs use
        ``prepare_handoff``; a permanent-owner recovery run uses
        ``owner_certifier`` and never fabricates a prepared handoff.
        """

        if self._owner_certifier is not None:
            try:
                certification = self._owner_certifier(self._collector_pn)
            except ValueError as exc:
                logger.info(
                    "Recovery verification: permanent-owner certification for "
                    "%s refused: %s",
                    self._collector_pn,
                    exc,
                )
                return "", None
            return "", certification
        return self._commit_prepared_handoff(), None

    def _commit_prepared_handoff(self) -> str:
        """Commit the claim via the prepare-handoff hook; "" means refusal.

        Called ONLY after the final retarget succeeded. The hook must pin the
        registry claim to the certified full PN and flip it into the prepared
        (committed) handoff state, returning the exact owner token. Refusal
        (empty return or a ValueError from the registry -- another owner holds
        the PN, or the claim stands for a different identity) yields "" and
        the caller turns that into a typed failure without a proof.
        """

        if self._prepare_handoff is None:
            return ""
        try:
            return str(self._prepare_handoff(self._collector_pn) or "").strip()
        except ValueError as exc:
            logger.info(
                "Recovery verification: handoff for %s not preparable: %s",
                self._collector_pn,
                exc,
            )
            return ""

    def _safe_trigger_generation(self) -> int:
        try:
            return int(self._callback_trigger_generation() or 0)
        except Exception:
            return 0

    async def _close_channel(self) -> None:
        with suppress(Exception):
            await self._restart_channel.async_close()


class InboundRecoveryVerifier:
    """Facade: the shared reset machine in INBOUND-ONLY mode.

    Keeps the Batch-3A public API and outcome type; the algorithm lives once,
    in :class:`_ControlledResetRecoveryEngine` (``callback_route=None``).
    """

    def __init__(self, **kwargs: Any) -> None:
        self._engine = _ControlledResetRecoveryEngine(**kwargs)

    async def async_verify(self) -> InboundRecoveryOutcome:
        outcome = await self._engine.async_verify()
        return InboundRecoveryOutcome(
            status=outcome.status,
            failure_reason=outcome.failure_reason,
            collector_pn=outcome.collector_pn,
            new_session_id=outcome.new_session_id,
            proof=outcome.inbound_proof,
            transitions=outcome.transitions,
        )


class CallbackRecoveryVerifier:
    """Facade: the shared reset machine with an armed callback route.

    Same machine, second phase enabled: an autonomous inbound reconnect ends
    as ``inbound_recovered`` (zero callback sends, inbound proof kept); only a
    genuinely silent collector proceeds to the exactly-one-unicast callback
    proof on the caller's explicit route.
    """

    def __init__(
        self,
        *,
        route: CallbackRecoveryRoute,
        trigger_sender: CallbackRecoveryTriggerSender | None = None,
        **kwargs: Any,
    ) -> None:
        self._engine = _ControlledResetRecoveryEngine(
            callback_route=route,
            trigger_sender=trigger_sender,
            **kwargs,
        )

    async def async_verify(self) -> RecoveryVerificationOutcome:
        return await self._engine.async_verify()


def _registry_sessions_projection(
    registry: Any,
) -> Callable[[], tuple[dict[str, Any], ...]]:
    """Project the registry's OWN per-socket truth into the engine's shape.

    The one authority: every field the engine trusts -- the strong/weak
    verdict, the certified identity source, the listener port, the raw
    negotiated wire -- comes from the same ``CallbackSessionRegistry`` that
    owns the claim. The strong/weak verdict is the registry's DERIVED
    ``CallbackSession.has_strong_identity`` (a function of the identity
    source), so even a lying ``has_strong_identity`` flag in the underlying
    listener inventory cannot survive this projection. There is deliberately
    no way to hand the public transaction a second, forged version of the
    session truth.
    """

    def _sessions() -> tuple[dict[str, Any], ...]:
        try:
            observed = registry.observed_sessions_per_socket()
        except Exception:
            logger.debug(
                "Callback recovery: registry sessions projection failed",
                exc_info=True,
            )
            return ()
        return tuple(
            {
                "session_id": session.session_id,
                "collector_pn": session.collector_pn,
                "state": session.state,
                "has_strong_identity": session.has_strong_identity,
                "collector_identity_source": session.identity_source,
                "listener_port": session.listener_port,
                "raw": dict(session.raw),
            }
            for session in observed
        )

    return _sessions


async def async_run_callback_recovery_transaction(
    *,
    registry: Any,
    collector_pn: str,
    session_id: str,
    route: CallbackRecoveryRoute,
    clock: Callable[[], str],
    policy: OnboardingTimeoutPolicy = DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    listener_host: str = "0.0.0.0",
    trigger_sender: CallbackRecoveryTriggerSender | None = None,
    ledger: Any = None,
    poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    permanent_owner_id: str | None = None,
    owner_certifier: Callable[[str], Any] | None = None,
) -> RecoveryVerificationOutcome:
    """THE public callback-recovery transaction: owns the whole claim lifecycle.

    Permanent-owner mode (Batch 8 repair): when ``permanent_owner_id`` +
    ``owner_certifier`` are given, the transaction runs UNDER that existing
    permanent owner (no transient onboarding owner, no prepared handoff), and
    a failure does NOT release the permanent claim. The outcome then carries a
    typed ``owner_certification`` instead of a ``handoff_owner``.

    Safety is structural, not contractual: this wrapper -- not a caller --
    creates the transient registry claim, wires the ownership hooks, the
    registry-only sessions projection and the negotiated-handle channel, runs
    the shared reset machine, and guarantees cleanup. There is NO caller-
    supplied sessions source: the engine observes sessions exclusively
    through the SAME registry that holds the claim, so identity strength,
    identity source, listener port and negotiated wire cannot be forged past
    the registry's own certification.

    On success the claim is already COMMITTED as a prepared handoff on the
    NEW session and the outcome carries the exact ``handoff_owner`` token
    (verify with ``registry.prepared_handoff_identity(owner, pn)``); the next
    batch's terminal flow consumes that capability directly -- persisting the
    proof into an entry is deliberately NOT done here. On any failure or
    cancellation the temporary claim is released (which also destroys a
    prepared-but-unconsumed handoff), the channel and its transports are
    stopped, and the lease/inhibitor are gone.
    """

    permanent_mode = bool(permanent_owner_id) and owner_certifier is not None
    owner = (
        str(permanent_owner_id).strip()
        if permanent_mode
        else f"callback_recovery:{uuid.uuid4().hex}"
    )
    try:
        # In permanent mode the durable claim already exists; this pins it to
        # the (already-bootstrapped) session id idempotently. In onboarding
        # mode it creates the transient claim.
        registry.claim_session(owner, session_id=session_id)
    except ValueError as exc:
        logger.info(
            "Callback recovery: session %s already claimed: %s", session_id, exc
        )
        outcome = RecoveryVerificationOutcome(
            status=STATE_INBOUND_NOT_VERIFIED,
            failure_reason=FAILURE_SESSION_CLAIMED,
            collector_pn=str(collector_pn or "").strip(),
            transitions=(STATE_OBSERVED_SESSION, STATE_INBOUND_NOT_VERIFIED),
        )
        return outcome

    def _claimed_session_id() -> str:
        try:
            return str(registry.claimed_session_id(owner) or "").strip()
        except Exception:
            return ""

    def _promote(full_pn: str) -> None:
        registry.promote_claim_to_full_pn(owner, full_pn)

    def _retarget(new_session_id: str) -> bool:
        try:
            if registry.claimed_session_id(owner) == new_session_id:
                return True
            return bool(
                registry.retarget_claim_to_reconnected_session(owner, new_session_id)
            )
        except ValueError:
            return False

    def _prepare(full_pn: str) -> str:
        # Commit THIS transaction's claim as a prepared handoff; the returned
        # token is the capability the outcome hands to the next batch.
        try:
            return owner if registry.prepare_handoff(owner, full_pn) else ""
        except ValueError as exc:
            logger.info(
                "Callback recovery: handoff for %s not preparable: %s",
                full_pn,
                exc,
            )
            return ""

    channel = ObservedSessionRestartChannel(
        host=listener_host,
        port=route.listener_port,
        collector_pn="",
        session_id=session_id,
        session_id_provider=_claimed_session_id,
        handle_provider=lambda: registry.session_handle_for_claimed_session(owner),
    )
    from ..collector.silent_session_probe import SilentSessionIdentityProbeChannel

    silent_probe = SilentSessionIdentityProbeChannel(
        host=listener_host, port=route.listener_port
    )
    await silent_probe.async_open()

    async def _probe_reconnected_identity(_new_session_id: str) -> str:
        # The engine already retargeted the claim onto the candidate; the
        # channel's providers resolve exactly that socket on its live wire.
        return await channel.async_probe_identity()

    engine = _ControlledResetRecoveryEngine(
        collector_pn=collector_pn,
        session_id=session_id,
        restart_channel=channel,
        sessions_source=_registry_sessions_projection(registry),
        clock=clock,
        policy=policy,
        callback_route=route,
        trigger_sender=trigger_sender,
        promote_claim=_promote,
        retarget_claim=_retarget,
        prepare_handoff=None if permanent_mode else _prepare,
        owner_certifier=owner_certifier if permanent_mode else None,
        probe_reconnected_identity=_probe_reconnected_identity,
        silent_session_probe=silent_probe,
        ledger=ledger,
        poll_interval=poll_interval,
    )
    succeeded = False
    try:
        outcome = await engine.async_verify()
        succeeded = outcome.callback_verified or outcome.inbound_recovered
        return outcome
    finally:
        with suppress(Exception):
            await silent_probe.async_close()
        with suppress(Exception):
            await channel.async_close()
        # A permanent owner keeps its durable claim across a failed repair;
        # only a transient onboarding owner is released here.
        if not succeeded and not permanent_mode:
            with suppress(Exception):
                registry.release(owner)

class ObservedSessionRestartChannel:
    """Restart channel over an already-observed passive listener session.

    Claims exactly the registry-owned ``session_id`` through the shared
    transports' claimed-session mechanism (never peer IP, never a PN index) and
    performs BOTH management operations through neutral, wire-negotiated seams:

    * the REBOOT goes through :func:`select_collector_management_adapter`,
      keyed ONLY by the live ``SessionHandle.collector_management_adapter``
      resolved from ``handle_provider`` -- a framed session reboots via the
      framed adapter (FC parameter details live inside it); an AT session whose
      adapter honestly reports ``reboot=False`` raises the typed unsupported
      error without touching the wire;
    * the IDENTITY probe goes through the shared
      :class:`collector.session_identity_reader.SessionPinnedIdentityReader`
      on the negotiated wire.

    No SmartESS session class, no raw reboot wire helper, no FC/AT literals.
    Sends NO UDP.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        collector_pn: str,
        session_id: str,
        session_id_provider: Callable[[], str] | None = None,
        handle_provider: Callable[[], Any] | None = None,
        request_timeout: float = 5.0,
        heartbeat_interval: float = 60.0,
        claim_timeout: float = 5.0,
    ) -> None:
        self._host = str(host or "0.0.0.0")
        self._port = int(port)
        self._collector_pn = str(collector_pn or "").strip()
        self._session_id = str(session_id or "").strip()
        # Registry-owned claims are the ownership authority: when a provider is
        # given (the config flow's registry claim resolver), it decides which
        # session id the transports may claim.
        self._session_id_provider = session_id_provider
        # The live negotiated SessionHandle for the claimed session -- the ONLY
        # source of the management-adapter decision.
        self._handle_provider = handle_provider
        self._request_timeout = float(request_timeout)
        self._heartbeat_interval = float(heartbeat_interval)
        self._claim_timeout = float(claim_timeout)
        self._framed_transport: Any = None
        self._at_transport: Any = None

    def _resolve_session_id(self) -> str:
        """Resolve the registry-owned session id, with NO ownership fallback.

        The provider (the registry claim resolver) is the ONLY ownership
        source: without one, or with an empty result, this is an ERROR. The
        statically observed ``session_id`` is display/bookkeeping context and
        must never act as ownership -- the transport may not fall back to it,
        nor be allowed to pick some other socket by PN/IP.
        """

        provider = self._session_id_provider
        if provider is None:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        try:
            resolved = str(provider() or "").strip()
        except Exception as exc:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE) from exc
        if not resolved:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return resolved

    def observed_wire(self) -> str:
        """The live negotiated wire of the trusted handle, or "" fail-closed.

        The ONLY legitimate source of a silent-reconnect probe authority: the
        REAL observed, non-conflicting SessionHandle of the session this
        channel claims. No fallback of any kind.
        """

        try:
            handle = self._resolve_trusted_handle()
        except SessionUnavailableError:
            return ""
        if handle.uses_framed_wire:
            return "eybond_framed"
        if handle.uses_at_text_wire:
            return "at_text"
        return ""

    def _resolve_trusted_handle(self) -> Any:
        """Resolve the REAL, trusted, live negotiated SessionHandle -- or fail.

        Fail-closed BEFORE any transport exists, with no default of any kind:

        * no ``handle_provider`` -> error (nothing is "assumed framed");
        * provider error / ``None`` (e.g. the claimed socket is closed and the
          registry refuses to negotiate it) -> error;
        * a forged/duck object -> error (strict ``type() is SessionHandle`` --
          attribute look-alikes must never pick a wire);
        * ``observed`` False or a non-empty ``conflict`` -> error.
        """

        from ..connection.session_handle import SessionHandle

        provider = self._handle_provider
        if provider is None:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        try:
            handle = provider()
        except Exception as exc:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE) from exc
        if type(handle) is not SessionHandle:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        if not handle.observed or handle.conflict:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return handle

    async def _async_ensure_framed_transport(self):
        """Activate and return the exact registry-owned framed session."""

        if self._framed_transport is not None:
            return self._framed_transport

        from ..collector.transport import SharedEybondTransport

        # Resolve strictly BEFORE touching any socket; a missing registry claim
        # aborts here and no transport is created at all.
        resolved_session_id = self._resolve_session_id()
        transport = SharedEybondTransport(
            host=self._host,
            port=self._port,
            request_timeout=self._request_timeout,
            heartbeat_interval=self._heartbeat_interval,
            collector_ip="",
            collector_pn="",
        )
        transport.set_claimed_session_provider(lambda: resolved_session_id)
        await transport.start()
        self._framed_transport = transport
        connected = await transport.wait_until_connected(timeout=self._claim_timeout)
        if not connected:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return transport

    async def _async_ensure_at_transport(self):
        """Activate and return the exact registry-owned AT session."""

        if self._at_transport is not None:
            return self._at_transport

        from ..collector.transport import SharedCollectorAtTransport
        from ..connection.session_handle import WIRE_AT_TEXT

        resolved_session_id = self._resolve_session_id()
        transport = SharedCollectorAtTransport(
            host=self._host,
            port=self._port,
            request_timeout=self._request_timeout,
            collector_ip="",
            collector_pn="",
            collector_session_protocol=WIRE_AT_TEXT,
        )
        transport.set_claimed_session_provider(lambda: resolved_session_id)
        await transport.start()
        self._at_transport = transport
        connected = await transport.wait_until_connected(timeout=self._claim_timeout)
        if not connected:
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        return transport

    async def async_probe_identity(self) -> str:
        """Read full collector identity over the claimed session's live wire."""

        from ..collector.session_identity_reader import SessionPinnedIdentityReader
        from ..connection.session_handle import WIRE_AT_TEXT, WIRE_FRAMED

        # The trusted live handle is the ONLY wire source: no provider, a
        # forged handle, an unobserved/conflicting one -- all fail typed here,
        # before any transport exists. Nothing is ever "assumed framed".
        handle = self._resolve_trusted_handle()
        wire = str(handle.wire_framing or "")
        if wire not in (WIRE_FRAMED, WIRE_AT_TEXT):
            raise SessionUnavailableError(FAILURE_SESSION_UNAVAILABLE)
        resolved_session_id = self._resolve_session_id()
        reader = SessionPinnedIdentityReader(
            host=self._host, request_timeout=self._request_timeout
        )
        full_pn, _source = await reader.async_read_full_pn(
            session_id=resolved_session_id,
            session_protocol=wire,
            listener_port=self._port,
        )
        return full_pn

    async def async_send_restart(self) -> None:
        """Reboot through the ONE management-adapter switch (negotiated wire)."""

        from ..connection.session_handle import (
            ADAPTER_COLLECTOR_AT_COMMANDS,
            ADAPTER_COLLECTOR_FRAMED_COMMANDS,
        )

        # The trusted live handle is the ONLY adapter source (see
        # _resolve_trusted_handle): fail-closed before any transport exists.
        handle = self._resolve_trusted_handle()
        adapter_id = str(handle.collector_management_adapter or "")

        # Capability gate NEXT: an adapter that honestly cannot reboot must
        # fail typed before any socket is claimed or byte written.
        probe_adapter = select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: None,
            at_transport_provider=lambda: None,
        )
        if not probe_adapter.capabilities.reboot:
            raise CollectorManagementUnsupportedError(
                "collector_reboot_unsupported_on_negotiated_wire"
            )

        if adapter_id == ADAPTER_COLLECTOR_FRAMED_COMMANDS:
            transport = await self._async_ensure_framed_transport()
        elif adapter_id == ADAPTER_COLLECTOR_AT_COMMANDS:
            transport = await self._async_ensure_at_transport()
        else:  # pragma: no cover - the capability gate above already refused
            raise CollectorManagementUnsupportedError(
                "collector_reboot_unsupported_on_negotiated_wire"
            )
        adapter = select_collector_management_adapter(
            adapter_id,
            framed_transport_provider=lambda: transport,
            at_transport_provider=lambda: transport,
        )
        await adapter.async_reboot()

    def is_connected(self) -> bool:
        for transport in (self._framed_transport, self._at_transport):
            if transport is None:
                continue
            try:
                if bool(transport.connected):
                    return True
            except Exception:
                continue
        return False

    async def async_close(self) -> None:
        transports = (self._framed_transport, self._at_transport)
        self._framed_transport = None
        self._at_transport = None
        for transport in transports:
            if transport is None:
                continue
            with suppress(Exception):
                await transport.stop()


__all__ = [
    "CallbackRecoveryRoute",
    "CallbackRecoveryTriggerSender",
    "CallbackRecoveryVerifier",
    "EVIDENCE_USER_CONFIRMED_SESSION",
    "FAILURE_CALLBACK_INTERFERENCE",
    "FAILURE_RECOVERY_IDENTITY_MISMATCH",
    "FAILURE_SILENT_PROBE_FAILED",
    "FAILURE_SILENT_PROBE_UNAVAILABLE",
    "FAILURE_SILENT_SESSION_AMBIGUOUS",
    "FAILURE_CALLBACK_PROOF_INVALID",
    "FAILURE_CALLBACK_TIMEOUT",
    "FAILURE_OWNERSHIP_UNAVAILABLE",
    "FAILURE_ROUTE_INVALID",
    "FAILURE_TRIGGER_NOT_SENT",
    "RecoveryVerificationOutcome",
    "STATE_CALLBACK_TRIGGER_REQUESTED",
    "STATE_CALLBACK_VERIFIED",
    "STATE_INBOUND_RECOVERED",
    "STATE_WAITING_FOR_CALLBACK_SESSION",
    "async_run_callback_recovery_transaction",
    "FAILURE_CAUSALITY_BUSY",
    "FAILURE_DISCONNECT_NOT_OBSERVED",
    "FAILURE_INBOUND_PROOF_INVALID",
    "FAILURE_RECONNECT_TIMEOUT",
    "FAILURE_RECONNECTED_SESSION_UNTRUSTED",
    "FAILURE_RESTART_NOT_CONFIRMED",
    "FAILURE_RESTART_NOT_SUPPORTED",
    "FAILURE_SESSION_CLAIMED",
    "FAILURE_SESSION_UNAVAILABLE",
    "FAILURE_STRONG_IDENTITY_TIMEOUT",
    "FAILURE_UDP_TRIGGER_OBSERVED",
    "InboundRecoveryOutcome",
    "InboundRecoveryVerifier",
    "ObservedSessionRestartChannel",
    "RestartChannel",
    "SessionUnavailableError",
    "STATE_INBOUND_NOT_VERIFIED",
    "STATE_INBOUND_VERIFIED",
    "STATE_OBSERVED_SESSION",
    "STATE_RESTART_REQUESTED",
    "STATE_WAITING_FOR_DISCONNECT",
    "STATE_WAITING_FOR_INBOUND_RECONNECT",
    "STATE_WAITING_FOR_STRONG_IDENTITY",
]
