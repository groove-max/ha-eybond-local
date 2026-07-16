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


class InboundRecoveryVerifier:
    """One-shot behavioral verifier: reboot the collector, watch it come back.

    All IO is injected: ``restart_channel`` owns the management-adapter path
    used to reboot over the already-observed session, and ``sessions_source``
    is the public registry facade (session dicts with ``session_id``,
    ``collector_pn``, ``state``, ``has_strong_identity``,
    ``collector_identity_source`` and the raw observation for wire
    negotiation). The verifier itself has no way to send UDP; it OWNS the
    causality lease + trigger inhibitor for its whole window, and still samples
    ``udp_trigger_count`` before/after as defense in depth.
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
        callback_trigger_generation: Callable[[], int] | None = None,
        promote_claim: Callable[[str], None] | None = None,
        retarget_claim: Callable[[str], bool] | None = None,
        probe_reconnected_identity: Callable[[str], Any] | None = None,
        ledger: Any = None,
        poll_interval: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._collector_pn = str(collector_pn or "").strip()
        self._session_id = str(session_id or "").strip()
        self._restart_channel = restart_channel
        self._sessions_source = sessions_source
        # The injected time source for ``verified_at`` -- the verifier never
        # calls now() itself, and the recovery-contract model validates the
        # value (timezone-aware ISO) when the proof is attached to a contract.
        self._clock = clock
        self._policy = policy
        self._callback_trigger_generation = callback_trigger_generation or (lambda: 0)
        # Ownership promotion hook: called with the strong FULL PN right after
        # the identity phase, BEFORE baseline/restart, so the transient
        # session-id claim becomes the durable full-PN claim in the registry.
        # Raising ValueError means another owner holds the identity.
        self._promote_claim = promote_claim
        # Ownership retarget hook: MUST move the registry claim from the closed
        # old socket to ``new_session_id`` (idempotent when already there).
        # Success without a retargeted claim is not success: the entry's
        # ownership handoff must carry the NEW socket.
        self._retarget_claim = retarget_claim
        self._probe_reconnected_identity = probe_reconnected_identity
        self._ledger = ledger
        self._disconnect_timeout = max(
            0.0, float(policy.inbound_restart_disconnect_timeout)
        )
        self._reconnect_timeout = max(0.0, float(policy.inbound_reconnect_timeout))
        self._identity_timeout = max(0.0, float(policy.inbound_strong_identity_timeout))
        self._poll_interval = max(0.01, float(poll_interval))
        self._baseline_session_ids: frozenset[str] = frozenset()
        self._transitions: list[str] = [STATE_OBSERVED_SESSION]

    def _enter(self, state: str) -> None:
        self._transitions.append(state)

    def _result(
        self,
        *,
        failure_reason: str = "",
        new_session_id: str = "",
        proof: InboundRecoveryProof | None = None,
    ) -> InboundRecoveryOutcome:
        return InboundRecoveryOutcome(
            status=self._transitions[-1],
            failure_reason=failure_reason,
            collector_pn=self._collector_pn,
            new_session_id=new_session_id,
            proof=proof,
            transitions=tuple(self._transitions),
        )

    def _fail(self, reason: str) -> InboundRecoveryOutcome:
        self._enter(STATE_INBOUND_NOT_VERIFIED)
        return self._result(failure_reason=reason)

    def _sessions(self) -> tuple[Mapping[str, Any], ...]:
        try:
            return tuple(self._sessions_source() or ())
        except Exception:
            logger.debug("Inbound recovery sessions source failed", exc_info=True)
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

    def _capture_baseline(self) -> None:
        """Record EVERY session id visible before the restart.

        A collector can hold several parallel sessions of the same durable PN.
        Only a session whose id was absent from the WHOLE pre-restart baseline
        can prove the post-reboot dial-in; comparing against the single selected
        old session id is not enough.
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
                # Any pre-restart socket (or its re-listing) can never confirm
                # inbound -- including parallel baseline sessions of the same PN.
                continue
            if _session_is_closed(session):
                continue
            if _session_state(session) in _UNTRUSTED_SESSION_STATES:
                continue
            if not _session_has_strong_identity(session):
                # Only a strong (registry-certified) identity can prove the
                # post-reboot dial-in; weak observations keep waiting.
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

    async def async_verify(self) -> InboundRecoveryOutcome:
        """Run the WHOLE transaction inside its own causal window.

        The exclusive causality lease is acquired BEFORE the baseline and the
        callback-trigger inhibitor is held for the entire verification, so no
        caller can invoke this verifier "unwrapped". On any failure or
        cancellation: no proof, the channel is closed, lease and inhibitor are
        released by their context managers.
        """

        if not self._collector_pn or not self._session_id:
            return self._fail(FAILURE_SESSION_UNAVAILABLE)

        ledger = self._ledger if self._ledger is not None else get_callback_trigger_ledger()
        try:
            async with ledger.causality_lease(
                f"inbound_verification:{uuid.uuid4().hex}",
                timeout=self._policy.callback_causality_lease_wait,
            ):
                async with ledger.inhibit_callback_triggers():
                    try:
                        return await self._async_verify_inside_causality()
                    finally:
                        # Idempotent: most paths already closed the channel; a
                        # cancellation mid-phase must not leak the claimed socket.
                        await self._close_channel()
        except CallbackCausalityBusyError:
            # Another callback attempt owns causality: honest typed refusal --
            # we never touched the collector.
            return self._fail(FAILURE_CAUSALITY_BUSY)

    async def _async_verify_inside_causality(self) -> InboundRecoveryOutcome:
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
                        "Inbound recovery: collector identity probe did not complete: %s",
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
                    "Inbound recovery: identity %s already claimed during promotion: %s",
                    self._collector_pn,
                    exc,
                )
                await self._close_channel()
                return self._fail(FAILURE_SESSION_CLAIMED)

        # Baseline of ALL currently-visible sessions, captured before restart
        # (and, thanks to the lease, after every other attempt's causal window
        # has fully closed).
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
                "Inbound recovery: collector %s management unsupported: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_RESTART_NOT_SUPPORTED)
        except SessionUnavailableError as exc:
            logger.info(
                "Inbound recovery: collector %s session unavailable: %s",
                self._collector_pn,
                exc,
            )
            await self._close_channel()
            return self._fail(FAILURE_SESSION_UNAVAILABLE)
        except Exception as exc:
            logger.info(
                "Inbound recovery: collector %s restart not confirmed: %s",
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

        # waiting_for_disconnect -> waiting_for_inbound_reconnect
        self._enter(STATE_WAITING_FOR_INBOUND_RECONNECT)
        deadline = loop.time() + self._reconnect_timeout
        identity_probe_attempted: set[str] = set()
        while True:
            new_session_id = self._find_new_inbound_session()
            if new_session_id:
                break
            weak_session_id = self._find_new_weak_identity_candidate()
            if (
                weak_session_id
                and weak_session_id not in identity_probe_attempted
                and self._probe_reconnected_identity is not None
            ):
                # EXACTLY ONE authoritative enrichment attempt per candidate
                # socket -- the attempted-set makes a duplicate invocation
                # structurally impossible. The claim must be retargeted to the
                # candidate FIRST (the hook below), because the reader is
                # pinned to the registry-claimed session id.
                identity_probe_attempted.add(weak_session_id)
                if self._retarget_claim is None or self._retarget_claim(weak_session_id):
                    try:
                        result = self._probe_reconnected_identity(weak_session_id)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as exc:
                        logger.info(
                            "Inbound recovery: reconnect identity probe failed for %s: %s",
                            weak_session_id,
                            exc,
                        )
            if loop.time() >= deadline:
                return self._fail(FAILURE_RECONNECT_TIMEOUT)
            await asyncio.sleep(self._poll_interval)

        # Defense in depth behind the lease+inhibitor: if ANY callback trigger
        # was recorded anywhere in the integration meanwhile, the reconnect
        # proves nothing -- conservatively refuse to certify inbound. A false
        # refusal is safe (manual callback follows); a false inbound is not.
        if self._safe_trigger_generation() != generation_before:
            return self._fail(FAILURE_UDP_TRIGGER_OBSERVED)

        # The proof's fields come from the REGISTRY's view of the new socket:
        # the strong identity source it certified and the live negotiated wire
        # (informative). An untrusted/conflicting observation cannot carry a
        # proof even though it matched above.
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
        session_protocol = handle.wire_framing if handle.observed else ""

        # Build the proof and PRE-VALIDATE it through the strict
        # RecoveryContract builder BEFORE the final retarget: this verifier
        # must never return a success whose proof the contract model would
        # refuse to persist. A naive/empty/invalid clock value or any other
        # malformed field is a typed failure -- proof=None, no final retarget,
        # and the normal channel/lease/inhibitor cleanup applies.
        try:
            verified_at = str(self._clock() or "").strip()
        except Exception:
            verified_at = ""
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
                "Inbound recovery: proof for %s failed contract validation: %s",
                self._collector_pn,
                exc,
            )
            return self._fail(FAILURE_INBOUND_PROOF_INVALID)

        # SUCCESS leaves the registry claim bound to the NEW socket, never the
        # closed baseline one -- the entry's ownership handoff must carry the
        # session the collector actually opened. Idempotent when the weak-path
        # enrichment already retargeted.
        if self._retarget_claim is not None and not self._retarget_claim(new_session_id):
            return self._fail(FAILURE_SESSION_CLAIMED)

        self._enter(STATE_INBOUND_VERIFIED)
        return self._result(new_session_id=new_session_id, proof=proof)

    def _safe_trigger_generation(self) -> int:
        try:
            return int(self._callback_trigger_generation() or 0)
        except Exception:
            return 0

    async def _close_channel(self) -> None:
        with suppress(Exception):
            await self._restart_channel.async_close()


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
    "EVIDENCE_USER_CONFIRMED_SESSION",
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
