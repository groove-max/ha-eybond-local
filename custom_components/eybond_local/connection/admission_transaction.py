"""The ONE neutral collector-admission transaction.

Owns the whole observed-session -> controlled restart -> autonomous reconnect ->
``InboundRecoveryProof`` lifecycle that used to live inline in ``config_flow``:
the immutable :class:`CollectorAdmissionRequest`, the current working PN
(including weak->strong enrichment), exact current-session resolution, the
temporary registry owner/claim, the restart channel, the silent-session probe,
the :class:`InboundRecoveryVerifier`, retry, cleanup on failure/cancel, holding a
successful claim until the terminal handoff, and the prepare/verify/rollback of
that handoff.

Neutral by construction: it depends only on lower-layer connection/recovery
primitives and a registry/policy the caller injects. It imports nothing from
``config_flow``, Home Assistant flow APIs, ``onboarding``, ``runtime`` or
drivers/provider/cloud. The config flow keeps only a reference to it, the HA
progress task, the UI continuation and result/error display.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
import logging
import uuid
from typing import Any, Callable

from .admission import CollectorAdmissionRequest
from .callback_continuation import (
    CallbackContinuation,
    CallbackIdentityContext,
    TerminalDecision,
)
from .callback_identity import (
    CallbackIdentityOutcome,
    CallbackIdentityRequest,
    ObservedSessionWireProbeIntent,
    SilentSessionBootstrapOffer,
    async_run_callback_identity_transaction,
)
from .callback_ledger import get_callback_trigger_ledger
from .recovery.terminal import RecoveryTerminalInput, verify_prepared_handoff
from .recovery.verification import (
    CallbackRecoveryRoute,
    FAILURE_OWNERSHIP_UNAVAILABLE,
    FAILURE_SESSION_CLAIMED,
    InboundRecoveryOutcome,
    InboundRecoveryVerifier,
    ObservedSessionRestartChannel,
    RecoveryVerificationOutcome,
    async_run_callback_recovery_transaction,
    registry_sessions_projection,
)
from .session_registry import identity_source_is_strong, pn_is_same_identity
from ..const import CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
from ..timeout_policy import DEFAULT_ONBOARDING_TIMEOUT_POLICY, OnboardingTimeoutPolicy

logger = logging.getLogger(__name__)

# Typed prepare_handoff outcomes (mapped to abort reasons by the caller).
HANDOFF_OK = ""
HANDOFF_ALREADY_CONFIGURED = "already_configured"
HANDOFF_NOT_PREPARED = "not_prepared"

# PRIVATE, non-persisted lifecycle. It never leaves the transaction and is never
# written to a config entry -- it only gates the public methods so a handoff can
# never happen before a real proof and cleanup can never leak an owner:
#
#   READY -> RUNNING -> VERIFIED -> HANDED_OFF -> (rollback) CLOSED
#                    \-> FAILED  -> (reset) READY
#                    \-> FAILED  -> (begin_callback) CALLBACK_READY
#   any (except RUNNING/HANDED_OFF) -> CLOSED via release()/async_close()
#
# The callback continuation (2D.2), reached ONLY from FAILED via
# begin_callback_continuation(), reuses the SAME single owner slot:
#   CALLBACK_READY -> IDENTITY_RUNNING -> IDENTITY_CERTIFIED
#                                      \-> (fail) CALLBACK_READY
#   IDENTITY_CERTIFIED -> RECOVERY_RUNNING -> RECOVERY_HELD
#                                          \-> (fail/cancel) CALLBACK_READY
#   RECOVERY_HELD -> (consume) RECOVERY_CONSUMED -> (adopt) RECOVERY_ADOPTED
#   RECOVERY_ADOPTED -> HANDED_OFF -> (rollback) CLOSED
# Between phases the owner slot may be EMPTY (inbound owner released before an
# identity owner; identity owner released before a recovery owner); never two.
_STATE_READY = "ready"
_STATE_RUNNING = "running"
_STATE_VERIFIED = "verified"
_STATE_FAILED = "failed"
_STATE_HANDED_OFF = "handed_off"
_STATE_CLOSED = "closed"
_STATE_CALLBACK_READY = "callback_ready"
_STATE_IDENTITY_RUNNING = "identity_running"
_STATE_IDENTITY_CERTIFIED = "identity_certified"
_STATE_RECOVERY_RUNNING = "recovery_running"
_STATE_RECOVERY_HELD = "recovery_held"
_STATE_RECOVERY_CONSUMED = "recovery_consumed"
_STATE_RECOVERY_ADOPTED = "recovery_adopted"

# Terminal owner mode: which owner the terminal coordinator is committing, so
# commit/rollback undo EXACTLY that owner (inbound commits at prepare; a callback
# recovery owner commits AFTER the terminal returns).
_TERMINAL_NONE = ""
_TERMINAL_INBOUND = "inbound"
_TERMINAL_RECOVERY_OWNER = "recovery_owner"


def _aware_clock() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strict_normalized_string(value: object, *, field: str) -> str:
    """Validate a request token without coercing duck-typed authority."""

    if type(value) is not str:
        raise TypeError(f"{field}_must_be_str")
    if value != value.strip():
        raise ValueError(f"{field}_not_normalized")
    return value


class CollectorAdmissionTransaction(CallbackContinuation):
    """Attempt-scoped owner of one observed-session admission.

    In 2D.2 this ONE transaction owns the whole admission-origin attempt -- inbound
    verification AND, after an inbound failure the user explicitly continues by
    callback, the callback identity + recovery + terminal handoff -- by
    implementing the neutral :class:`CallbackContinuation` contract. The config
    flow selects it as ``self._callback_continuation`` at the source boundary, so
    the shared orchestration never branches on the transaction. The single owner
    slot (``_owner``/``_registry``) is reused across every phase; at most one owner
    is ever held.
    """

    def __init__(
        self,
        request: CollectorAdmissionRequest,
        *,
        registry_provider: Callable[[], Any],
        listener_host: str,
        policy_provider: Callable[[], OnboardingTimeoutPolicy] | None = None,
        hass_provider: Callable[[], Any] | None = None,
    ) -> None:
        # STRICT input: only the typed request authorizes admission -- no loose
        # dict / getattr source authority, and ``origin`` never steers anything.
        if type(request) is not CollectorAdmissionRequest:
            raise TypeError("collector_admission_request_required")
        self._request = request
        self._registry_provider = registry_provider
        self._listener_host = str(listener_host or "").strip() or "0.0.0.0"
        self._policy_provider = policy_provider or (
            lambda: DEFAULT_ONBOARDING_TIMEOUT_POLICY
        )
        # Opaque HA handle, threaded (never imported) into the callback identity
        # authority, which needs it. ``None`` for inbound-only construction.
        self._hass_provider = hass_provider
        # The CURRENT working identity: the observation PN on the first attempt,
        # the weak->strong enriched FULL PN afterwards. The observation stays
        # immutable.
        self._expected_pn = request.observed_session.collector_pn
        self._old_session_id = request.observed_session.session_id
        self._outcome: InboundRecoveryOutcome | None = None
        # Held claim (only after a successful claim_session).
        self._registry: Any = None
        self._owner = ""
        # Live channels for cancellation-safe close.
        self._channel: Any = None
        self._silent_probe: Any = None
        # Set once the claim is committed to a handoff at the terminal.
        self._handed_off = False
        # The PN a committed handoff was prepared for (idempotence guard).
        self._handoff_pn = ""
        # Non-persisted lifecycle gate (see the _STATE_* comment above).
        self._state = _STATE_READY
        # Synchronous flow removal cannot await an in-flight identity/recovery
        # authority. It marks this flag; the running coroutine then performs the
        # exact delayed-owner cleanup and terminates CLOSED instead of resurrecting
        # this transaction after its flow disappeared.
        self._close_requested = False
        # ---- callback continuation (2D.2), all transient/private -------------
        # The exact certified identity of the current callback attempt.
        self._certified_pn = ""
        self._certified_session_id = ""
        self._identity_previous_pn = ""
        self._identity_enrichment_pending = False
        self._silent_offer: Any = None
        # The held (not-yet-consumed) recovery outcome AND the exact object
        # identity used to prove adopt/release only ever act on OUR own outcome.
        self._recovery_outcome: RecoveryVerificationOutcome | None = None
        self._produced_recovery_outcome: RecoveryVerificationOutcome | None = None
        # The adopted callback recovery terminal input + which owner the terminal
        # is committing.
        self._callback_terminal_input = RecoveryTerminalInput.none()
        self._terminal_owner_mode = _TERMINAL_NONE

    # ---- read-only surface -------------------------------------------------
    @property
    def request(self) -> CollectorAdmissionRequest:
        return self._request

    @property
    def expected_pn(self) -> str:
        return self._expected_pn

    @property
    def old_session_id(self) -> str:
        return self._old_session_id

    @property
    def peer_hint(self) -> str:
        """Editable display hint only -- never identity/claim/route."""

        return self._request.observed_session.peer_hint

    @property
    def outcome(self) -> InboundRecoveryOutcome | None:
        return self._outcome

    @property
    def failure_reason(self) -> str:
        return str(getattr(self._outcome, "failure_reason", "") or "")

    @property
    def verified(self) -> bool:
        return bool(
            self._outcome is not None
            and getattr(self._outcome, "inbound_verified", False)
        )

    @property
    def terminal_input(self) -> RecoveryTerminalInput:
        """The canonical terminal carrier -- never a second persisted model.

        A callback recovery owner adopted by this attempt takes precedence (the
        callback continuation is past the inbound phase); otherwise the verified
        inbound proof, or ``none()``.
        """

        if self._callback_terminal_input.prepared_handoff_owner:
            return self._callback_terminal_input
        if self.verified and self._outcome is not None:
            return RecoveryTerminalInput.from_inbound_outcome(self._outcome)
        return RecoveryTerminalInput.none()

    @property
    def holds_claim(self) -> bool:
        return self._registry is not None and bool(self._owner)

    @property
    def handed_off(self) -> bool:
        return self._handed_off

    @property
    def state(self) -> str:
        """The private lifecycle state (introspection/tests only; never persisted)."""

        return self._state

    # ---- lifecycle ---------------------------------------------------------
    async def async_run(self) -> None:
        """Claim the session, then run the ONE inbound verifier.

        The temporary registry claim is created BEFORE the restart and held for
        the whole verification (including the reconnect wait), so passive
        discovery cannot republish the session and no other entry/flow can claim
        the same durable identity meanwhile. On success the claim is HELD for the
        terminal handoff; every other path (failure/cancel/error) closes the
        channels and releases the claim. A SINGLE cancellation-safe cleanup
        boundary opens the instant the claim is held, so no exception after the
        claim (channel/probe construction, ``async_open``, the policy provider,
        the sessions projection, verifier construction or the verify itself) can
        leak the owner.
        """

        # State-safe: never run concurrently, or after a hand-off / close.
        if self._state != _STATE_READY:
            raise RuntimeError("admission_transaction_not_runnable")
        self._state = _STATE_RUNNING

        observed = self._request.observed_session
        # CURRENT expected identity (enriched on retry), never the stale
        # observation PN -- reading the observation PN would make a retry
        # re-resolve by the original SHORT PN and claim the old session.
        collector_pn = str(self._expected_pn or "").strip() or observed.collector_pn
        session_id = observed.session_id
        port = observed.listener_port

        owner = f"strategy_verification:{uuid.uuid4().hex}"
        registry: Any = None
        try:
            registry = self._registry_provider()
            if registry is None:
                self._outcome = InboundRecoveryOutcome(
                    failure_reason=FAILURE_OWNERSHIP_UNAVAILABLE,
                    collector_pn=collector_pn,
                )
                self._state = _STATE_FAILED
                return
            # Re-resolve the CURRENT socket immediately before claiming it.
            # ``require_exact`` follows the observation's identity STRENGTH: a
            # weak identity may only follow an EXACT PN -- which, after
            # enrichment, is the exact FULL PN, so a replacement full-PN
            # session is found while foreign and merely same-prefix sessions
            # are excluded; strong AT/FC2 evidence may use the registry's
            # centralized short/full reconciliation. Identity authority comes
            # ONLY from the typed observation.
            identity_source = observed.identity_source
            current_session = registry.current_session_for_pn(
                collector_pn,
                require_exact=not identity_source_is_strong(identity_source),
            )
            if current_session is not None:
                session_id = current_session.session_id
                self._old_session_id = session_id
        except Exception:
            logger.exception("Collector admission registry lookup failed")
            self._outcome = InboundRecoveryOutcome(
                failure_reason="verification_error",
                collector_pn=collector_pn,
            )
            self._state = _STATE_FAILED
            return

        try:
            # TRANSIENT claim strictly by session_id. The owner token is unique,
            # so releasing it after any failed claim is safe and also covers a
            # registry implementation that raised after a partial mutation.
            registry.claim_session(owner, session_id=session_id)
        except ValueError as exc:
            # The session/identity is already owned by a config entry or another
            # verification: never hijack it. Nothing was acquired -> nothing to
            # clean up.
            logger.info(
                "Strategy verification: session for %s already claimed: %s",
                collector_pn,
                exc,
            )
            self._outcome = InboundRecoveryOutcome(
                failure_reason=FAILURE_SESSION_CLAIMED,
                collector_pn=collector_pn,
            )
            with suppress(Exception):
                registry.release(owner)
            self._state = _STATE_FAILED
            return
        except Exception:
            logger.exception("Collector admission session claim failed")
            with suppress(Exception):
                registry.release(owner)
            self._outcome = InboundRecoveryOutcome(
                failure_reason="verification_error",
                collector_pn=collector_pn,
            )
            self._state = _STATE_FAILED
            return
        self._registry = registry
        self._owner = owner

        def _claimed_session_id() -> str:
            if registry is None:
                return ""
            try:
                return str(registry.claimed_session_id(owner) or "").strip()
            except Exception:
                return ""

        def _promote_claim(full_pn: str) -> None:
            if registry is not None:
                registry.promote_claim_to_full_pn(owner, full_pn)

        def _retarget_claim(new_session_id: str) -> bool:
            if registry is None:
                return True
            try:
                if registry.claimed_session_id(owner) == new_session_id:
                    return True
                return bool(
                    registry.retarget_claim_to_reconnected_session(
                        owner, new_session_id
                    )
                )
            except ValueError:
                return False

        def _session_handle_for_claim():
            if registry is None:
                return None
            return registry.session_handle_for_claimed_session(owner)

        # ---- ONE cleanup boundary, open now that the claim is HELD. ----
        cancelled = False
        try:
            channel = ObservedSessionRestartChannel(
                host=self._listener_host,
                port=port,
                collector_pn=collector_pn,
                session_id=session_id,
                session_id_provider=(
                    _claimed_session_id if registry is not None else None
                ),
                handle_provider=(
                    _session_handle_for_claim if registry is not None else None
                ),
            )
            self._channel = channel

            async def _probe_reconnected_identity(new_session_id: str) -> str:
                del new_session_id
                return await channel.async_probe_identity()

            from ..collector.silent_session_probe import (
                SilentSessionIdentityProbeChannel,
            )

            silent_probe = SilentSessionIdentityProbeChannel(
                host=self._listener_host, port=port
            )
            self._silent_probe = silent_probe
            await silent_probe.async_open()
            verifier = InboundRecoveryVerifier(
                collector_pn=collector_pn,
                session_id=session_id,
                restart_channel=channel,
                sessions_source=registry_sessions_projection(registry),
                silent_session_probe=silent_probe,
                clock=_aware_clock,
                policy=self._policy_provider(),
                callback_trigger_generation=(
                    get_callback_trigger_ledger().snapshot_generation
                ),
                promote_claim=_promote_claim if registry is not None else None,
                retarget_claim=_retarget_claim if registry is not None else None,
                probe_reconnected_identity=(
                    _probe_reconnected_identity if registry is not None else None
                ),
            )
            self._outcome = await verifier.async_verify()
        except asyncio.CancelledError:
            # Defer propagation until the mandatory finalizer has closed both
            # channels and released this attempt's owner.
            cancelled = True
        except Exception:
            # Any construction/open/verify failure -> typed verification_error;
            # close channels, then release our own owner.
            logger.exception("Passive discovery strategy verification failed")
            self._outcome = InboundRecoveryOutcome(
                failure_reason="verification_error",
                collector_pn=collector_pn,
            )

        # ONE mandatory finalization boundary for success, failure and cancel.
        # Re-delivered cancellations are absorbed until both channels finish
        # closing; only then is the owner retained/released and cancellation
        # propagated. No await follows the state/owner decision, so there is no
        # post-cleanup cancellation window that can strand RUNNING state.
        cancelled = await self._async_cleanup_channels_critical() or cancelled
        self._adopt_enriched_pn_from_outcome()
        if cancelled or self._close_requested:
            # A verifier may have returned a success immediately before the
            # cancellation/removal landed in channel cleanup. Do not expose that
            # unfinalized proof as a verified transaction, and never resurrect a
            # transaction whose flow already requested closure.
            self._outcome = None
            self._release_owner()
            self._state = _STATE_CLOSED if self._close_requested else _STATE_FAILED
            raise asyncio.CancelledError
        if self.verified:
            # SUCCESS: keep the strong full-PN claim held for the terminal.
            self._state = _STATE_VERIFIED
        else:
            self._release_owner()
            self._state = _STATE_FAILED

    async def _async_cleanup_channels(self) -> None:
        """Close the live probe/channel (idempotent; safe to re-invoke)."""

        probe, self._silent_probe = self._silent_probe, None
        channel, self._channel = self._channel, None
        if probe is not None:
            # A channel-specific cancellation must not prevent the other
            # resource from closing. The caller still propagates task
            # cancellation after the complete cleanup boundary.
            with suppress(asyncio.CancelledError, Exception):
                await probe.async_close()
        if channel is not None:
            with suppress(asyncio.CancelledError, Exception):
                await channel.async_close()

    async def _async_cleanup_channels_critical(self) -> bool:
        """Run channel cleanup to completion despite repeated caller cancels.

        Returns whether cancellation was delivered while waiting. The cleanup
        itself runs in a shielded child task and is never left in the
        background: this method waits until it is done before returning.
        """

        task = asyncio.ensure_future(self._async_cleanup_channels())
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        # _async_cleanup_channels is deliberately exception-contained, but
        # retrieve the result so no task exception can go unobserved if that
        # invariant changes later.
        try:
            task.result()
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            logger.exception("Collector admission channel cleanup failed")
        return cancelled

    def _release_owner(self) -> None:
        """Release exactly THIS transaction's own registry owner (idempotent)."""

        registry, owner = self._registry, self._owner
        self._registry = None
        self._owner = ""
        if registry is None or not owner:
            return
        with suppress(Exception):
            registry.release(owner)

    def _adopt_enriched_pn_from_outcome(self) -> None:
        enriched = str(getattr(self._outcome, "collector_pn", "") or "").strip()
        if enriched and enriched != self._expected_pn:
            self._expected_pn = enriched

    def reset_for_retry(self) -> None:
        """Release the failed claim and clear the outcome; keep the enriched PN.

        Allowed ONLY from a not-yet-run or FAILED transaction (never while
        RUNNING, nor after VERIFIED / HANDED_OFF). The old failed claim is fully
        released BEFORE the next run so a retry never contends with its own stale
        owner; the weak->strong enriched ``expected_pn`` is preserved so the
        retry resolves the replacement full-PN session.
        """

        if self._state not in (_STATE_READY, _STATE_FAILED):
            raise RuntimeError("admission_transaction_not_resettable")
        if self._close_requested:
            raise RuntimeError("admission_transaction_closed")
        self._release_owner()
        self._outcome = None
        self._state = _STATE_READY

    def release(self) -> None:
        """Release this transaction's OWN registry owner (state-safe, idempotent).

        While an authority is running, records a close request and lets that
        coroutine release any owner produced after cancellation. It is a TRUE
        no-op once the claim was committed to a handoff (entry setup completes it,
        and the owner/registry refs must survive for ``rollback_handoff``). Never
        touches another owner.
        """

        if self._state == _STATE_HANDED_OFF:
            return
        if self._state in (
            _STATE_RUNNING,
            _STATE_IDENTITY_RUNNING,
            _STATE_RECOVERY_RUNNING,
        ):
            # The authority may already have created an owner we cannot observe
            # until it returns. Mark closure now; its coroutine performs the
            # mandatory delayed cleanup and may not publish a result afterwards.
            self._close_requested = True
            return
        self._close_requested = True
        self._release_callback_recovery_capability()
        self._release_owner()
        self._state = _STATE_CLOSED

    async def async_close(self) -> None:
        """Close live channels THEN release the claim (state-safe, idempotent).

        Used on explicit cancellation: channels are always closed before the claim
        is released, and re-closing is a no-op.
        """

        cancelled = await self._async_cleanup_channels_critical()
        self.release()
        if cancelled:
            raise asyncio.CancelledError

    # ---- terminal handoff --------------------------------------------------
    def prepare_handoff(self, collector_pn: str) -> str:
        """Commit this attempt's VERIFIED claim to a handoff for entry setup.

        A handoff is allowed ONLY when the transaction is VERIFIED, its outcome
        is a real verified ``InboundRecoveryOutcome`` carrying a real
        ``InboundRecoveryProof``, it holds its OWN claim, and the requested PN is
        the same durable identity as both the proof and the expected PN (checked
        via the centralized ``pn_is_same_identity`` BEFORE any registry
        mutation). Before the run, while RUNNING, and after FAILED/CLOSED it
        returns :data:`HANDOFF_NOT_PREPARED` with ZERO registry mutation; a
        foreign PN fails closed the same way. Re-preparing the SAME
        already-handed-off identity is idempotent and never calls
        ``registry.prepare_handoff`` a second time.
        """

        pn = str(collector_pn or "").strip()
        # Idempotent re-prepare of the SAME committed identity: no second registry
        # call, no mutation. A foreign PN against a committed handoff fails closed.
        if self._state == _STATE_HANDED_OFF:
            if self._handoff_pn and pn and pn_is_same_identity(pn, self._handoff_pn):
                return HANDOFF_OK
            return HANDOFF_NOT_PREPARED
        # Fail closed on every non-VERIFIED state (pre-run / RUNNING / FAILED /
        # CLOSED) and on a missing proof / claim -- BEFORE touching the registry.
        if self._state != _STATE_VERIFIED or not self.verified:
            return HANDOFF_NOT_PREPARED
        proof = self.terminal_input.inbound_proof
        if proof is None or not self.holds_claim:
            return HANDOFF_NOT_PREPARED
        if not (
            pn
            and pn_is_same_identity(pn, proof.collector_pn)
            and pn_is_same_identity(pn, self._expected_pn)
        ):
            # Foreign PN after verification: refuse before any registry mutation;
            # the original claim is NOT rebound.
            return HANDOFF_NOT_PREPARED
        registry, owner = self._registry, self._owner
        try:
            prepared = bool(registry.prepare_handoff(owner, pn))
        except ValueError as exc:
            logger.info("Callback handoff refused for %s: %s", pn, exc)
            self._release_owner()
            self._state = _STATE_CLOSED
            return HANDOFF_ALREADY_CONFIGURED
        if not prepared:
            logger.debug(
                "No verification claim to hand off for %s (owner %s)", pn, owner
            )
            self._release_owner()
            self._state = _STATE_CLOSED
            return HANDOFF_NOT_PREPARED
        certified = ""
        with suppress(Exception):
            certified = str(
                registry.prepared_handoff_identity(owner, pn) or ""
            ).strip()
        if not certified:
            logger.info(
                "Prepared handoff for %s is not certifiable; refusing terminal", pn
            )
            self._release_owner()
            self._state = _STATE_CLOSED
            return HANDOFF_ALREADY_CONFIGURED
        self._handed_off = True
        self._handoff_pn = pn
        self._state = _STATE_HANDED_OFF
        return HANDOFF_OK

    def rollback_handoff(self) -> None:
        """Undo a committed handoff after the terminal helper threw.

        Releases EXACTLY this attempt's prepared owner (even if ``release`` was
        called meanwhile -- a handed-off ``release`` is a no-op that preserves the
        owner refs). The original exception is re-raised by the caller.
        """

        if self._state != _STATE_HANDED_OFF:
            return
        self._handed_off = False
        self._handoff_pn = ""
        self._release_owner()
        self._state = _STATE_CLOSED

    # ======================================================================
    # Callback continuation (2D.2): the SAME transaction owns the callback
    # identity -> recovery -> terminal lifecycle after an inbound failure the
    # user explicitly continues by callback. Implements CallbackContinuation.
    # ======================================================================

    # ---- callback-phase transition ----------------------------------------
    _CALLBACK_RESETTABLE = (
        _STATE_CALLBACK_READY,
        _STATE_IDENTITY_CERTIFIED,
        _STATE_RECOVERY_HELD,
        _STATE_RECOVERY_CONSUMED,
    )

    def begin_callback_continuation(self) -> None:
        """Move a FAILED inbound attempt into a callback-ready continuation.

        Allowed ONLY from FAILED, and ONLY once the inbound owner is released and
        the channels are closed (both true after ``async_run``'s finalizer on any
        failure path). Any other state -- or a lingering claim -- fails closed
        BEFORE mutation. The completed inbound attempt's outcome/proof is cleared
        so no inbound proof leaks into the callback phase; the (possibly enriched)
        expected PN + old session context are kept as the identity context.
        """

        if self._state != _STATE_FAILED:
            raise RuntimeError("admission_transaction_not_callback_ready")
        if self._close_requested:
            raise RuntimeError("admission_transaction_closed")
        if self.holds_claim:
            raise RuntimeError("admission_transaction_inbound_owner_not_released")
        if self._channel is not None or self._silent_probe is not None:
            raise RuntimeError("admission_transaction_inbound_channel_not_closed")
        self._outcome = None
        self._reset_callback_attempt()
        self._state = _STATE_CALLBACK_READY

    def begin_observed_callback_continuation(self) -> None:
        """Start selected-route verification from one exact observed session.

        This first pass sends no callback request. It claims and authoritatively
        reads the exact socket selected by the scan; therefore a weak heartbeat
        observation may be upgraded by FC=2/DTUPN, but can never certify itself.
        Recovery starts only after that strong read succeeds. If recovery fails,
        a retry starts from ``CALLBACK_READY`` and must obtain a new same-PN
        session through the selected route; it cannot reuse this observation.
        """

        observed = self._request.observed_session
        if self._state != _STATE_READY:
            raise RuntimeError("admission_transaction_not_callback_ready")
        if self._close_requested:
            raise RuntimeError("admission_transaction_closed")
        if type(self._request.callback_route) is not CallbackRecoveryRoute:
            raise RuntimeError("admission_transaction_callback_route_missing")
        # Construct now so an unsupported/malformed observed wire fails before
        # any state mutation or registry ownership change.
        self.observed_wire_probe_intent(_state_override=_STATE_READY)
        self._reset_callback_attempt()
        # The initial attempt adopts the exact observed socket. A later retry
        # runs from CALLBACK_READY and therefore performs a normal addressed
        # callback identity attempt instead of calling this method again.
        self._old_session_id = ""
        self._state = _STATE_CALLBACK_READY

    def observed_wire_probe_intent(
        self, *, _state_override: str = ""
    ) -> ObservedSessionWireProbeIntent:
        """Return the exact zero-send identity capability for this observation."""

        if (_state_override or self._state) not in (
            _STATE_READY,
            _STATE_CALLBACK_READY,
        ):
            raise RuntimeError("admission_transaction_observed_probe_unavailable")
        observed = self._request.observed_session
        return ObservedSessionWireProbeIntent(
            protocol=observed.protocol_shape,
            session_id=observed.session_id,
            collector_pn=observed.collector_pn,
            identity_source=observed.identity_source,
        )

    def _reset_callback_attempt(self) -> None:
        """Release the current callback owner + clear all held callback proof.

        A retry is a FULL new attempt: the current owner (identity or recovery)
        and any held-but-unadopted recovery outcome are released, and no PN /
        session / silent offer / outcome / proof of the previous attempt survives.
        The weak->strong enriched ``_expected_pn`` is deliberately kept.
        """

        self._release_callback_recovery_capability()
        self._release_owner()
        self._certified_pn = ""
        self._certified_session_id = ""
        self._identity_previous_pn = ""
        self._identity_enrichment_pending = False
        self._silent_offer = None
        self._callback_terminal_input = RecoveryTerminalInput.none()
        self._terminal_owner_mode = _TERMINAL_NONE

    # ---- identity phase ---------------------------------------------------
    @property
    def identity_context(self) -> CallbackIdentityContext:
        return CallbackIdentityContext(
            expected_pn=self._expected_pn,
            old_session_id=self._old_session_id,
        )

    def identity_context_for_attempt(
        self, declared_expected_pn: str
    ) -> CallbackIdentityContext:
        declared = _strict_normalized_string(
            declared_expected_pn, field="callback_declared_expected_pn"
        )
        if not pn_is_same_identity(declared, self._expected_pn):
            raise ValueError("callback_declared_expected_pn_mismatch")
        # Keep a stronger PN learned by this transaction; unlike the legacy
        # adapter, retry never downgrades its own strong same-identity evidence.
        return self.identity_context

    def adopt_certified_pn(self, collector_pn: str) -> str:
        pn = _strict_normalized_string(
            collector_pn, field="callback_certified_pn"
        )
        if (
            not pn
            or self._state != _STATE_IDENTITY_CERTIFIED
            or pn != self._certified_pn
            or not pn_is_same_identity(pn, self._expected_pn)
        ):
            raise ValueError("callback_certified_pn_mismatch")
        if not self._identity_enrichment_pending:
            return pn
        previous = self._identity_previous_pn
        self._identity_enrichment_pending = False
        return previous

    def adopt_passive_inbound_identity(
        self, collector_pn: str, session_id: str
    ) -> bool:
        _strict_normalized_string(
            collector_pn, field="passive_inbound_collector_pn"
        )
        _strict_normalized_string(session_id, field="passive_inbound_session_id")
        # This transaction reached the callback form only because controlled
        # autonomous reconnect verification failed. A currently visible (possibly
        # stale) socket cannot bypass that result and become a normal inbound entry.
        return False

    async def async_run_identity(
        self, request: CallbackIdentityRequest
    ) -> CallbackIdentityOutcome:
        # Exact-type + context-match gate BEFORE any mutation: a duck request,
        # a foreign PN/session, or a non-callback strategy can never authorize an
        # attempt (route fields stay user-provided and are NOT identity here).
        if type(request) is not CallbackIdentityRequest:
            raise TypeError("callback_identity_request_required")
        strategy = _strict_normalized_string(
            request.strategy, field="callback_identity_request_strategy"
        )
        expected_pn = _strict_normalized_string(
            request.expected_pn, field="callback_identity_request_expected_pn"
        )
        old_session_id = _strict_normalized_string(
            request.old_session_id,
            field="callback_identity_request_old_session_id",
        )
        if strategy != CONNECTION_STRATEGY_CALLBACK_ON_DEMAND:
            raise ValueError("callback_identity_request_strategy_invalid")
        if not pn_is_same_identity(expected_pn, self._expected_pn):
            raise ValueError("callback_identity_request_pn_mismatch")
        if old_session_id != self._old_session_id:
            raise ValueError("callback_identity_request_session_mismatch")
        if self._state not in self._CALLBACK_RESETTABLE:
            raise RuntimeError("admission_transaction_identity_not_runnable")
        if self._close_requested:
            raise RuntimeError("admission_transaction_closed")
        # Reset the previous callback attempt, then run the ONE identity authority.
        self._reset_callback_attempt()
        self._state = _STATE_IDENTITY_RUNNING
        hass = self._hass_provider() if self._hass_provider is not None else None
        try:
            outcome = await async_run_callback_identity_transaction(hass, request)
        except asyncio.CancelledError:
            self._reset_callback_attempt()
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )
            raise
        except Exception:
            self._reset_callback_attempt()
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )
            raise
        if type(outcome) is not CallbackIdentityOutcome:
            self._reset_callback_attempt()
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )
            raise TypeError("callback_identity_outcome_required")
        if self._close_requested:
            self._release_identity_outcome_owner(outcome)
            self._reset_callback_attempt()
            self._state = _STATE_CLOSED
            raise asyncio.CancelledError
        if not outcome.identity_certified:
            if (
                outcome.silent_bootstrap_offer is not None
                and type(outcome.silent_bootstrap_offer)
                is not SilentSessionBootstrapOffer
            ):
                self._reset_callback_attempt()
                self._state = _STATE_CALLBACK_READY
                raise TypeError("silent_bootstrap_offer_required")
            self._silent_offer = outcome.silent_bootstrap_offer
            self._state = _STATE_CALLBACK_READY
            return outcome
        # Adopt the certified proof as THE single owner (the inbound owner was
        # released on the failure path; no two owners coexist). The identity
        # transaction already prepared a handoff under ``handoff_owner``.
        collector_pn = _strict_normalized_string(
            outcome.collector_pn, field="callback_identity_outcome_collector_pn"
        )
        session_id = _strict_normalized_string(
            outcome.session_id, field="callback_identity_outcome_session_id"
        )
        owner = _strict_normalized_string(
            outcome.handoff_owner, field="callback_identity_outcome_handoff_owner"
        )
        if (
            not collector_pn
            or not session_id
            or not owner
            or not pn_is_same_identity(collector_pn, self._expected_pn)
        ):
            self._release_identity_outcome_owner(outcome)
            self._reset_callback_attempt()
            self._state = _STATE_CALLBACK_READY
            raise ValueError("callback_identity_outcome_invalid")
        self._registry = self._registry_provider()
        self._owner = owner
        self._certified_pn = collector_pn
        self._certified_session_id = session_id
        # weak->strong enrichment stays INSIDE the transaction.
        self._identity_previous_pn = self._expected_pn
        self._identity_enrichment_pending = collector_pn != self._expected_pn
        self._expected_pn = collector_pn
        self._state = _STATE_IDENTITY_CERTIFIED
        return outcome

    def _release_identity_outcome_owner(
        self, outcome: CallbackIdentityOutcome
    ) -> None:
        """Release a delayed identity owner only when its exact proof certifies."""

        if type(outcome) is not CallbackIdentityOutcome:
            return
        owner = outcome.handoff_owner
        collector_pn = outcome.collector_pn
        session_id = outcome.session_id
        if not (
            type(owner) is str
            and owner
            and owner == owner.strip()
            and type(collector_pn) is str
            and collector_pn
            and collector_pn == collector_pn.strip()
            and type(session_id) is str
            and session_id
            and session_id == session_id.strip()
        ):
            return
        registry = self._registry_provider()
        if registry is None:
            return
        try:
            certified = registry.prepared_handoff_identity(owner, collector_pn)
            claimed_session = registry.claimed_session_id(owner)
        except Exception:
            return
        if certified and pn_is_same_identity(certified, collector_pn) and (
            claimed_session == session_id
        ):
            with suppress(Exception):
                registry.release(owner)

    @property
    def certified_pn(self) -> str:
        return self._certified_pn

    @property
    def silent_bootstrap_offer(self) -> Any:
        return self._silent_offer

    # ---- recovery phase ---------------------------------------------------
    async def async_run_recovery(
        self, route: CallbackRecoveryRoute
    ) -> RecoveryVerificationOutcome | None:
        if type(route) is not CallbackRecoveryRoute:
            raise TypeError("callback_recovery_route_required")
        if self._state != _STATE_IDENTITY_CERTIFIED:
            raise RuntimeError("admission_transaction_recovery_not_runnable")
        if self._close_requested:
            raise RuntimeError("admission_transaction_closed")
        collector_pn = str(self._certified_pn or "").strip()
        session_id = str(self._certified_session_id or "").strip()
        registry = self._registry_provider()
        if not collector_pn or not session_id or registry is None:
            return None
        # Hand the session over: identity owner OUT, recovery owner IN.
        self._release_owner()
        self._state = _STATE_RECOVERY_RUNNING
        try:
            outcome = await async_run_callback_recovery_transaction(
                registry=registry,
                collector_pn=collector_pn,
                session_id=session_id,
                route=route,
                clock=_aware_clock,
                policy=self._policy_provider(),
                listener_host=self._listener_host,
            )
        except asyncio.CancelledError:
            # The recovery wrapper's own finally released its claim; the identity
            # owner was already released above. Reset before propagating cancel.
            self._reset_callback_attempt()
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )
            raise
        except Exception:
            self._reset_callback_attempt()
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )
            raise
        if type(outcome) is not RecoveryVerificationOutcome:
            self._reset_callback_attempt()
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )
            raise TypeError("recovery_verification_outcome_required")
        self._recovery_outcome = outcome
        self._produced_recovery_outcome = outcome
        if self._close_requested:
            self._release_callback_recovery_capability()
            self._state = _STATE_CLOSED
            raise asyncio.CancelledError
        self._state = _STATE_RECOVERY_HELD
        return outcome

    @property
    def recovery_outcome(self) -> RecoveryVerificationOutcome | None:
        return self._recovery_outcome

    def consume_recovery_outcome(self) -> RecoveryVerificationOutcome | None:
        if self._state != _STATE_RECOVERY_HELD:
            return None
        outcome = self._recovery_outcome
        self._recovery_outcome = None
        if outcome is not None:
            # Keep the produced-outcome identity so adopt/release can prove the
            # consumed outcome is the exact one we produced (clarification 3).
            self._state = _STATE_RECOVERY_CONSUMED
        return outcome

    def adopt_recovery(self, outcome: RecoveryVerificationOutcome) -> bool:
        # Exact-type + exact-object gate BEFORE any mutation: only the outcome
        # THIS transaction produced and handed out can be adopted -- never a
        # foreign / forged / separately-constructed one, and never by PN/string.
        if type(outcome) is not RecoveryVerificationOutcome:
            raise TypeError("recovery_verification_outcome_required")
        if outcome is not self._produced_recovery_outcome:
            raise ValueError("recovery_outcome_not_produced_by_transaction")
        if self._state != _STATE_RECOVERY_CONSUMED:
            raise RuntimeError("admission_transaction_adopt_not_allowed")
        terminal_input = RecoveryTerminalInput.from_callback_transaction(outcome)
        registry = self._registry_provider()
        certified = verify_prepared_handoff(registry, terminal_input)
        if not certified:
            logger.info(
                "Callback recovery outcome for %s is not certifiable; not adopting",
                terminal_input.collector_pn,
            )
            return False
        # Adopt the recovery owner as THE single owner (identity owner already
        # released before the recovery run; no two owners coexist).
        self._registry = registry
        self._owner = terminal_input.prepared_handoff_owner
        self._callback_terminal_input = terminal_input
        self._terminal_owner_mode = _TERMINAL_RECOVERY_OWNER
        self._state = _STATE_RECOVERY_ADOPTED
        return True

    def release_unadopted_recovery(self) -> None:
        self._release_callback_recovery_capability()
        if self._state in (_STATE_RECOVERY_HELD, _STATE_RECOVERY_CONSUMED):
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )

    def release_exact_recovery_owner(
        self, outcome: RecoveryVerificationOutcome
    ) -> None:
        # Exact-type + exact-object gate: a foreign / forged / separately built
        # outcome (even a valid one carrying a real token) can never release.
        if type(outcome) is not RecoveryVerificationOutcome:
            raise TypeError("recovery_verification_outcome_required")
        if outcome is not self._produced_recovery_outcome:
            raise ValueError("recovery_outcome_not_produced_by_transaction")
        self._release_produced_owner(outcome)
        self._recovery_outcome = None
        self._produced_recovery_outcome = None
        if self._state in (_STATE_RECOVERY_HELD, _STATE_RECOVERY_CONSUMED):
            self._state = (
                _STATE_CLOSED if self._close_requested else _STATE_CALLBACK_READY
            )

    def _release_callback_recovery_capability(self) -> None:
        """Release a held/consumed produced outcome before forgetting it."""

        outcome = self._recovery_outcome or self._produced_recovery_outcome
        self._recovery_outcome = None
        if outcome is not None:
            self._release_produced_owner(outcome)
        self._produced_recovery_outcome = None

    def _release_produced_owner(
        self, outcome: RecoveryVerificationOutcome
    ) -> None:
        """Release exactly ``outcome``'s prepared owner -- never the adopted one.

        A no-op once the owner was ADOPTED (it is now the single owner slot,
        governed by ``release``/terminal rollback) and for a failure outcome (no
        owner). Never resolves an owner by PN.
        """

        owner = str(outcome.handoff_owner or "").strip()
        if not owner or owner == self._owner:
            return
        registry = self._registry_provider()
        if registry is not None:
            with suppress(Exception):
                registry.release(owner)

    # ---- unified terminal ownership ---------------------------------------
    def prepare_terminal(
        self, collector_pn: str, recovery: RecoveryTerminalInput
    ) -> TerminalDecision:
        if type(recovery) is not RecoveryTerminalInput:
            raise TypeError("recovery_terminal_input_required")
        # CALLBACK RECOVERY owner: an already-prepared capability this attempt
        # adopted. Bound to the exact adopted state -- never a bare/forged input,
        # never reconstructed by PN. Commit is deferred until AFTER the terminal.
        if recovery.prepared_handoff_owner:
            if (
                self._state != _STATE_RECOVERY_ADOPTED
                or recovery is not self._callback_terminal_input
                or self._registry is None
                or recovery.prepared_handoff_owner != self._owner
            ):
                logger.info(
                    "Callback-recovery owner for %s was never adopted by this transaction; refusing terminal",
                    collector_pn,
                )
                return TerminalDecision(abort_reason="recovery_ownership_unavailable")
            certified = verify_prepared_handoff(self._registry, recovery)
            if not certified or not pn_is_same_identity(certified, collector_pn):
                logger.info(
                    "Prepared callback-recovery handoff for %s is not certifiable; refusing terminal",
                    collector_pn,
                )
                return TerminalDecision(abort_reason="recovery_ownership_unavailable")
            self._terminal_owner_mode = _TERMINAL_RECOVERY_OWNER
            return TerminalDecision(owns=True)
        # INBOUND verified claim: commit at prepare via the existing handoff.
        if self._state == _STATE_VERIFIED and self.holds_claim:
            status = self.prepare_handoff(collector_pn)
            if status == HANDOFF_ALREADY_CONFIGURED:
                return TerminalDecision(abort_reason="already_configured")
            if status == HANDOFF_NOT_PREPARED:
                if recovery.has_proof:
                    return TerminalDecision(
                        abort_reason="recovery_ownership_unavailable"
                    )
                return TerminalDecision()
            self._terminal_owner_mode = _TERMINAL_INBOUND
            return TerminalDecision(owns=True)
        # No owned claim: a proof-bearing terminal without ownership fails closed;
        # anything else is a plain pass-through (no ownership bookkeeping).
        if recovery.has_proof:
            return TerminalDecision(abort_reason="recovery_ownership_unavailable")
        return TerminalDecision()

    def commit_terminal(self) -> None:
        # RECOVERY_OWNER commits AFTER the terminal returns; INBOUND was already
        # committed at prepare_handoff (state HANDED_OFF).
        if self._terminal_owner_mode == _TERMINAL_RECOVERY_OWNER:
            self._handed_off = True
            self._handoff_pn = str(self._certified_pn or self._expected_pn or "")
            self._state = _STATE_HANDED_OFF

    def rollback_terminal(self) -> None:
        if self._terminal_owner_mode == _TERMINAL_RECOVERY_OWNER:
            # handed_off was never set; release exactly the adopted recovery owner.
            self._release_owner()
            self._state = _STATE_CLOSED
        elif self._terminal_owner_mode == _TERMINAL_INBOUND:
            self.rollback_handoff()

    def release_terminal_owner(self) -> None:
        # Cancel/removal: release the uncommitted current owner (a no-op once the
        # claim is committed to a handoff -- entry setup completes it).
        self.release()


__all__ = [
    "CollectorAdmissionTransaction",
    "HANDOFF_ALREADY_CONFIGURED",
    "HANDOFF_NOT_PREPARED",
    "HANDOFF_OK",
]
