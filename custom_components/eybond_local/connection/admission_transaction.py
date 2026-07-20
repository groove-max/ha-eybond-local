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
from .callback_ledger import get_callback_trigger_ledger
from .recovery.terminal import RecoveryTerminalInput
from .recovery.verification import (
    FAILURE_OWNERSHIP_UNAVAILABLE,
    FAILURE_SESSION_CLAIMED,
    InboundRecoveryOutcome,
    InboundRecoveryVerifier,
    ObservedSessionRestartChannel,
    registry_sessions_projection,
)
from .session_registry import identity_source_is_strong, pn_is_same_identity
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
#   any (except RUNNING/HANDED_OFF) -> CLOSED via release()/async_close()
_STATE_READY = "ready"
_STATE_RUNNING = "running"
_STATE_VERIFIED = "verified"
_STATE_FAILED = "failed"
_STATE_HANDED_OFF = "handed_off"
_STATE_CLOSED = "closed"


def _aware_clock() -> str:
    return datetime.now(timezone.utc).isoformat()


class CollectorAdmissionTransaction:
    """Attempt-scoped owner of one observed-session inbound admission."""

    def __init__(
        self,
        request: CollectorAdmissionRequest,
        *,
        registry_provider: Callable[[], Any],
        listener_host: str,
        policy_provider: Callable[[], OnboardingTimeoutPolicy] | None = None,
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
        """The canonical terminal carrier -- never a second persisted model."""

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
        if cancelled:
            # A verifier may have returned a success immediately before the
            # cancellation landed in channel cleanup. Do not expose that
            # unfinalized proof as a verified transaction.
            self._outcome = None
            self._release_owner()
            self._state = _STATE_FAILED
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
        self._release_owner()
        self._outcome = None
        self._state = _STATE_READY

    def release(self) -> None:
        """Release this transaction's OWN registry owner (state-safe, idempotent).

        A no-op while RUNNING (never drop a live verification claim) and a TRUE
        no-op once the claim was committed to a handoff (entry setup completes
        it, and the owner/registry refs must survive for ``rollback_handoff``).
        Never touches another owner.
        """

        if self._state in (_STATE_RUNNING, _STATE_HANDED_OFF):
            return
        self._release_owner()
        self._state = _STATE_CLOSED

    async def async_close(self) -> None:
        """Close live channels THEN release the claim (state-safe, idempotent).

        Used on explicit cancel / manual-callback bridge: channels are always
        closed before the claim is released, and re-closing is a no-op.
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


__all__ = [
    "CollectorAdmissionTransaction",
    "HANDOFF_ALREADY_CONFIGURED",
    "HANDOFF_NOT_PREPARED",
    "HANDOFF_OK",
]
