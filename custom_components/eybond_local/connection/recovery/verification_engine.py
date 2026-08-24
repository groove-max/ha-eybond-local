"""The single controlled-reset recovery verification engine."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
from typing import Any, Callable, Iterable, Mapping
import uuid

from ...collector.management import CollectorManagementUnsupportedError
from ...collector_identity import identity_source_is_strong, pn_is_same_identity
from ...timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY, OnboardingTimeoutPolicy
from ..callback_ledger import CallbackCausalityBusyError, get_callback_trigger_ledger
from ..recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    CallbackRecoveryProof,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    InboundRecoveryProof,
    RecoveryContract,
)
from ..session_handle import negotiate_session_adapters
from .verification_models import (
    CallbackRecoveryRoute,
    CallbackRecoveryTriggerSender,
    FAILURE_CALLBACK_INTERFERENCE,
    FAILURE_CALLBACK_PROOF_INVALID,
    FAILURE_CALLBACK_TIMEOUT,
    FAILURE_CAUSALITY_BUSY,
    FAILURE_DISCONNECT_NOT_OBSERVED,
    FAILURE_INBOUND_PROOF_INVALID,
    FAILURE_OWNERSHIP_UNAVAILABLE,
    FAILURE_RECONNECTED_SESSION_UNTRUSTED,
    FAILURE_RECONNECT_TIMEOUT,
    FAILURE_RECOVERY_IDENTITY_MISMATCH,
    FAILURE_RESTART_NOT_CONFIRMED,
    FAILURE_RESTART_NOT_SUPPORTED,
    FAILURE_ROUTE_INVALID,
    FAILURE_SESSION_CLAIMED,
    FAILURE_SESSION_UNAVAILABLE,
    FAILURE_STRONG_IDENTITY_TIMEOUT,
    FAILURE_TRIGGER_NOT_SENT,
    FAILURE_UDP_TRIGGER_OBSERVED,
    InboundRecoveryOutcome,
    RecoveryVerificationOutcome,
    RecoveryWireProbeAuthority,
    RestartChannel,
    STATE_CALLBACK_TRIGGER_REQUESTED,
    STATE_CALLBACK_VERIFIED,
    STATE_INBOUND_NOT_VERIFIED,
    STATE_INBOUND_RECOVERED,
    STATE_INBOUND_VERIFIED,
    STATE_OBSERVED_SESSION,
    STATE_RESTART_REQUESTED,
    STATE_WAITING_FOR_CALLBACK_SESSION,
    STATE_WAITING_FOR_DISCONNECT,
    STATE_WAITING_FOR_INBOUND_RECONNECT,
    STATE_WAITING_FOR_STRONG_IDENTITY,
    SessionUnavailableError,
    _DEFAULT_POLL_INTERVAL_SECONDS,
    _ProductionRecoveryTriggerSender,
    _ReconnectWaitResult,
    _SILENT_OBS_AMBIGUOUS,
    _SILENT_OBS_FOREIGN,
    _SILENT_OBS_NONE,
    _SILENT_OBS_PROBE_FAILED,
    _SILENT_OBS_SAME,
    _SILENT_OBS_TO_FAILURE,
    _SILENT_OBS_UNAVAILABLE,
    _UNTRUSTED_SESSION_STATES,
    _session_has_strong_identity,
    _session_is_closed,
    _session_state,
)

logger = logging.getLogger(__name__)

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
        |     -> baseline-cohort reset activity -> bounded reconnect wait
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
        expected_inbound_listener_port: int = 0,
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
        # Ownership retarget hook: MUST move the registry claim from the
        # baseline management socket to ``new_session_id`` (idempotent when
        # already there). The old socket may still overlap physically; the
        # complete baseline + new-id/strong-PN gates establish recovery, not a
        # single old-socket EOF assumption.
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
        if (
            type(expected_inbound_listener_port) is not int
            or type(expected_inbound_listener_port) is bool
            or not 0 <= expected_inbound_listener_port <= 65535
        ):
            raise ValueError("recovery_expected_listener_port_invalid")
        self._expected_inbound_listener_port = expected_inbound_listener_port
        # Captured from the TRUSTED live handle right before the reboot; the
        # only wire authority a silent-reconnect probe may use.
        self._wire_authority: RecoveryWireProbeAuthority | None = None
        self._pending_baseline: frozenset[str] = frozenset()
        self._ledger = ledger
        self._disconnect_timeout = max(
            0.0, float(policy.inbound_restart_disconnect_timeout)
        )
        self._reconnect_timeout = max(
            0.0, float(policy.inbound_reconnect_timeout)
        )
        self._identity_timeout = max(0.0, float(policy.inbound_strong_identity_timeout))
        self._callback_wait_timeout = max(
            0.0, float(policy.callback_recovery_session_wait)
        )
        self._poll_interval = max(0.01, float(poll_interval))
        self._baseline_session_ids: frozenset[str] = frozenset()
        # The subset of the baseline that is positively attributable to this
        # collector.  A real collector may keep several overlapping same-PN
        # sockets alive, and the management command can be carried by one while
        # a sibling is the first socket to close.  Reset causality therefore
        # cannot hinge on one arbitrarily selected ``old_session_id``.
        self._reset_target_session_ids: frozenset[str] = frozenset()
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

        sessions = self._sessions()
        self._baseline_session_ids = frozenset(
            str(session.get("session_id") or "").strip()
            for session in sessions
            if str(session.get("session_id") or "").strip()
        ) | {self._session_id}
        target_ids = {self._session_id}
        for session in sessions:
            session_id = str(session.get("session_id") or "").strip()
            session_pn = str(session.get("collector_pn") or "").strip()
            if (
                session_id
                and not _session_is_closed(session)
                and session_pn
                and pn_is_same_identity(self._collector_pn, session_pn)
            ):
                target_ids.add(session_id)
        self._reset_target_session_ids = frozenset(target_ids)

    def _reset_activity_observed(self) -> tuple[bool, str]:
        """Return whether the controlled reset changed the target cohort.

        This is deliberately an *activity* gate, not a recovery proof.  It
        accepts either disappearance/closure of any baseline socket positively
        tied to the collector, or appearance of a new same-identity/fully-silent
        socket outside the complete baseline.  The later reconnect path still
        requires a NEW strong exact-PN session, the expected listener, and a
        successful ownership retarget before it can mint a proof.

        Counting a new silent socket here is necessary for collectors that open
        their successor before an overlapping management socket has closed.  A
        foreign/noisy socket can at most advance to the fail-closed identity
        wait; it can never certify success.
        """

        sessions = self._sessions()
        by_id = {
            str(session.get("session_id") or "").strip(): session
            for session in sessions
            if str(session.get("session_id") or "").strip()
        }
        for session_id in self._reset_target_session_ids:
            session = by_id.get(session_id)
            if session is None or _session_is_closed(session):
                return True, f"baseline_closed:{session_id}"

        for session_id, session in by_id.items():
            if session_id in self._baseline_session_ids or _session_is_closed(session):
                continue
            session_pn = str(session.get("collector_pn") or "").strip()
            if session_pn and pn_is_same_identity(self._collector_pn, session_pn):
                return True, f"new_same_identity:{session_id}"

        probe = self._silent_session_probe
        if probe is not None:
            try:
                for session_id in probe.snapshot_silent_session_ids():
                    if (
                        session_id not in self._pending_baseline
                        and session_id not in self._baseline_session_ids
                    ):
                        return True, f"new_silent:{session_id}"
            except Exception:
                pass
        return False, ""

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
            if not self._session_matches_expected_inbound_port(session_id):
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
            if not self._session_matches_expected_inbound_port(session_id):
                continue
            if _session_has_strong_identity(session):
                continue
            session_pn = str(session.get("collector_pn") or "").strip()
            if session_pn and pn_is_same_identity(self._collector_pn, session_pn):
                return session_id
        return ""

    def _session_matches_expected_inbound_port(self, session_id: str) -> bool:
        """Whether a candidate arrived on this transition's pinned listener."""

        expected = self._expected_inbound_listener_port
        return expected == 0 or self._session_listener_port(session_id) == expected

    def _fresh_callback_socket_observed(self) -> bool:
        """Whether retransmitting the same callback route is no longer useful.

        This is deliberately weaker than proof: any fresh live socket on the
        pinned listener stops UDP retransmission, but the normal strong-PN,
        baseline, route and ownership gates below still decide whether that
        socket can certify success.  A false positive can only yield a safe
        timeout; it can never mint a proof for the wrong collector.
        """

        for session in self._sessions():
            session_id = str(session.get("session_id") or "").strip()
            if not session_id or session_id in self._baseline_session_ids:
                continue
            if _session_is_closed(session):
                continue
            if self._session_listener_port(session_id) == self._callback_route.listener_port:
                return True
        probe = self._silent_session_probe
        if probe is None:
            return False
        try:
            return any(
                session_id not in self._pending_baseline
                and session_id not in self._baseline_session_ids
                for session_id in probe.snapshot_silent_session_ids()
            )
        except Exception:
            return False

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
            # A probe above may have synchronously enriched the exact session.
            # Observe that result before applying the deadline so a zero-wait
            # caller still gets one complete snapshot+probe pass without sleep.
            new_session_id = self._find_new_inbound_session()
            if new_session_id:
                return _ReconnectWaitResult(session_id=new_session_id)
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
            and self._session_matches_expected_inbound_port(session_id)
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
        """Identity -> promote -> baseline -> reboot -> reset activity -> reconnect.

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
        # through the negotiated management adapter inside the channel. A wire
        # whose adapter cannot reboot surfaces as a typed unsupported failure
        # without any write; an accepted restart is still not a recovery proof
        # until the reset activity and same-PN reconnect below are observed.
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

        logger.info(
            "Recovery verification: restart confirmed collector=%s old_session=%s",
            self._collector_pn,
            self._session_id,
        )

        try:
            # restart_requested -> waiting_for_disconnect (compat state name):
            # observe a change in the WHOLE pre-reset target cohort.  Some
            # collectors overlap same-PN sockets, so requiring the one selected
            # management socket to close is neither necessary nor correct.
            self._enter(STATE_WAITING_FOR_DISCONNECT)
            deadline = loop.time() + self._disconnect_timeout
            while True:
                reset_observed, reset_reason = self._reset_activity_observed()
                if reset_observed:
                    logger.info(
                        "Recovery verification: reset activity observed "
                        "collector=%s management_session=%s reason=%s",
                        self._collector_pn,
                        self._session_id,
                        reset_reason,
                    )
                    break
                if loop.time() >= deadline:
                    logger.info(
                        "Recovery verification: baseline cohort did not change "
                        "collector=%s management_session=%s target_sessions=%s",
                        self._collector_pn,
                        self._session_id,
                        sorted(self._reset_target_session_ids),
                    )
                    return self._fail(FAILURE_DISCONNECT_NOT_OBSERVED)
                await asyncio.sleep(self._poll_interval)
        finally:
            # Release the exact management facade after the reset activity
            # barrier; proof collection uses the registry/silent probe views.
            await self._close_channel()

        # waiting_for_disconnect -> waiting_for_inbound_reconnect. Only an
        # inbound-only verification waits for autonomous recovery. A transaction
        # that already carries a typed callback route performs exactly one
        # non-waiting pass of the SAME reconnect mechanism. That pass may enrich
        # an already-present silent/weak exact session once; it never sleeps for
        # a future arrival. Otherwise the transaction proceeds directly to its
        # one addressed callback sequence. No arbitrary grace period pretends
        # to prove that a later autonomous reconnect is impossible.
        self._enter(STATE_WAITING_FOR_INBOUND_RECONNECT)
        reconnect_deadline = loop.time() + (
            0.0 if self._callback_route is not None else self._reconnect_timeout
        )
        wait = await self._async_wait_for_new_same_pn_session(
            reconnect_deadline
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
        # baseline management one -- the entry's ownership handoff must carry the
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
        callback_deadline = loop.time() + self._callback_wait_timeout
        sender = self._trigger_sender or _ProductionRecoveryTriggerSender(
            timeout=self._policy.discovery_timeout,
            retry_window=max(0.0, callback_deadline - loop.time()),
            stop_requested=self._fresh_callback_socket_observed,
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
        wait = await self._async_wait_for_new_same_pn_session(callback_deadline)
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
