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

from ...collector.management import (
    CollectorManagementUnsupportedError,
    select_collector_management_adapter,
)
from ..callback_ledger import (
    CallbackCausalityBusyError,
    get_callback_trigger_ledger,
)
from ..recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    CallbackRecoveryProof,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    InboundRecoveryProof,
    RecoveryContract,
)
from ..session_handle import negotiate_session_adapters
from ...collector_identity import (
    identity_source_is_strong,
    pn_is_same_identity,
)
from ...const import (
    CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION,
)
from ...timeout_policy import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    OnboardingTimeoutPolicy,
)

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

    async def async_send_restart(self) -> Any:
        """Run the negotiated reset action; raise on failure."""

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

        from ..session_registry import (
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

    def __init__(
        self,
        *,
        timeout: float,
        retry_window: float = 0.0,
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        self._timeout = float(timeout)
        self._retry_window = max(0.0, float(retry_window))
        self._stop_requested = stop_requested

    async def async_send(self, route: CallbackRecoveryRoute) -> None:
        from ...collector.discovery import async_send_callback_trigger

        await async_send_callback_trigger(
            bind_ip=route.bind_ip,
            advertised_server_ip=route.advertised_ha_host,
            advertised_server_port=int(route.advertised_ha_port),
            target_ip=route.trigger_target_ip,
            udp_port=int(route.trigger_udp_port),
            timeout=self._timeout,
            source="callback_recovery_transaction",
            retry_window=self._retry_window,
            stop_requested=self._stop_requested,
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
