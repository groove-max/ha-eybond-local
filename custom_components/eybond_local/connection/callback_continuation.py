"""The typed ownership boundary for ONE callback continuation.

A callback continuation is the identity -> recovery -> adopted-terminal-owner
lifecycle a manual callback attempt runs after the user explicitly asks for it:
it proves WHO is on the wire (the identity transaction), then proves the callback
route can regain that collector (the recovery transaction), then hands the exact
certified owner to the terminal. Historically that lifecycle was orchestrated
inline in the config flow over eight mutable ``_verification_*``/``_manual_*``
fields; this contract makes the whole lifecycle -- reset, run, the typed
snapshots downstream steps need, and terminal ownership -- explicit and typed so
the shared orchestration depends on a boundary, not on those fields.

This is the NEUTRAL contract only. It:

* speaks exclusively in the existing typed identity/recovery/terminal models
  (``CallbackIdentityRequest``/``CallbackIdentityOutcome``,
  ``CallbackRecoveryRoute``/``RecoveryVerificationOutcome``,
  ``RecoveryTerminalInput``, ``SilentSessionBootstrapOffer``) plus one small
  ``TerminalDecision`` verdict -- it never duplicates a proof, a matcher, a
  recovery engine or a handoff algorithm, carries no ``dict``/``SimpleNamespace``
  /getattr/loose-string authority, and never returns a ConfigFlowResult;
* owns the identity and recovery owners for one attempt (reset-on-new-attempt,
  release-on-abort, adopt-on-commit) and the terminal handoff decision
  (prepare-vs-verify, commit, rollback, release);
* imports nothing from ``config_flow``/``onboarding``/``runtime``. The current
  legacy implementation is a flow-backed adapter that lives beside the config
  flow because it needs flow-owned fields; a future admission-transaction
  implementation will satisfy the same contract without those fields.

Every method/property here has a production caller.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from .callback_identity import (
    CallbackIdentityOutcome,
    CallbackIdentityRequest,
    SilentSessionBootstrapOffer,
)
from .recovery.terminal import RecoveryTerminalInput
from .recovery.verification import CallbackRecoveryRoute, RecoveryVerificationOutcome


_TERMINAL_ABORT_REASONS = ("", "already_configured", "recovery_ownership_unavailable")


@dataclass(frozen=True, slots=True)
class TerminalDecision:
    """The seam's typed verdict for a callback-continuation terminal handoff.

    * ``abort_reason`` non-empty -> config_flow aborts with that EXISTING reason
      (``already_configured`` / ``recovery_ownership_unavailable``); the terminal
      does not run;
    * ``owns`` True -> the seam holds a claim that must be committed after the
      terminal returns and rolled back exactly if it raises;
    * both empty/False -> no callback-continuation claim; the terminal runs with
      no ownership bookkeeping.

    Strictly constructed: ``abort_reason`` is an exact normalized string from the
    closed set above, ``owns`` is an exact bool, and a decision may NEVER both
    abort and own -- an abort never runs the terminal, so there is nothing to own.
    """

    abort_reason: str = ""
    owns: bool = False

    def __post_init__(self) -> None:
        if (
            type(self.abort_reason) is not str
            or self.abort_reason != self.abort_reason.strip()
        ):
            raise ValueError("terminal_decision_abort_reason_invalid")
        if self.abort_reason not in _TERMINAL_ABORT_REASONS:
            raise ValueError("terminal_decision_abort_reason_unknown")
        if type(self.owns) is not bool:
            raise ValueError("terminal_decision_owns_invalid")
        if self.abort_reason and self.owns:
            raise ValueError("terminal_decision_abort_and_owns")


@dataclass(frozen=True, slots=True)
class CallbackIdentityContext:
    """The durable expectation ONE callback identity attempt is built against.

    ``expected_pn`` is the strong PN a prior discovery / stored entry declared (or
    ``""`` when none); ``old_session_id`` is the session an admission failure is
    superseding (or ``""``). Both are exact normalized strings -- the seam hands
    this to the shared attempt so it constructs its ``CallbackIdentityRequest``
    without reading any legacy flow field.
    """

    expected_pn: str
    old_session_id: str

    def __post_init__(self) -> None:
        for name in ("expected_pn", "old_session_id"):
            value = getattr(self, name)
            if type(value) is not str:
                raise TypeError(f"callback_identity_context_{name}_type_invalid")
            if value != value.strip():
                raise ValueError(f"callback_identity_context_{name}_invalid")


class CallbackContinuation(ABC):
    """Own ONE callback attempt's identity+recovery+terminal ownership lifecycle.

    An implementation runs the two existing authorities, holds their prepared
    owners privately, and decides/commits/rolls back the terminal handoff of the
    owner it holds. Callers see only the typed outcomes, the read-only snapshots,
    and the explicit operations -- there are deliberately no field-level
    getters/setters for the legacy state.
    """

    # -- run phases ------------------------------------------------------- #

    @abstractmethod
    async def async_run_identity(
        self, request: CallbackIdentityRequest
    ) -> CallbackIdentityOutcome:
        """Reset the previous attempt, then run the ONE identity authority.

        The implementation first clears any prior identity/recovery proof and
        releases any owner/held recovery outcome of the previous attempt (the
        caller no longer clears those fields). On a certified outcome it adopts
        the transaction's prepared identity owner as this attempt's held claim
        and retains the certified full PN / session id; on a non-certified
        outcome it retains only the silent bootstrap offer, if any. The exact
        typed ``CallbackIdentityOutcome`` is returned for presentation.
        """

    @abstractmethod
    async def async_run_recovery(
        self, route: CallbackRecoveryRoute
    ) -> RecoveryVerificationOutcome | None:
        """Validate the certified session, then run the ONE recovery authority.

        The implementation obtains THIS attempt's certified PN, session id and
        registry internally and returns ``None`` (session unavailable) without
        touching the wire when any is missing. Otherwise it releases the identity
        owner (its session<->PN job is done), invokes the recovery authority with
        the exact certified PN/session (never re-found by peer IP or PN), retains
        the typed outcome and returns it. ``CancelledError`` propagates unchanged.
        """

    # -- typed read-only snapshots ---------------------------------------- #

    @property
    @abstractmethod
    def identity_context(self) -> CallbackIdentityContext:
        """The durable expectation the NEXT identity request is built against."""

    @abstractmethod
    def identity_context_for_attempt(
        self, declared_expected_pn: str
    ) -> CallbackIdentityContext:
        """Return the context for a new attempt after applying its declaration.

        The legacy implementation restores its durable pre-attempt declaration;
        a transaction-backed implementation keeps a stronger same-identity PN it
        learned itself.  This is the one source-neutral replacement for a caller
        writing a legacy ``_verification_expected_pn`` field.
        """

    @abstractmethod
    def adopt_certified_pn(self, collector_pn: str) -> str:
        """Accept this attempt's certified PN and return its previous spelling.

        The caller uses the previous spelling only to project short->full
        enrichment through its presentation models.  Identity ownership remains
        entirely inside the implementation.
        """

    @abstractmethod
    def adopt_passive_inbound_identity(
        self, collector_pn: str, session_id: str
    ) -> bool:
        """Try to own an explicitly selected passive inbound identity.

        Ordinary manual onboarding retains its historical behavior.  A
        transaction whose controlled inbound verification already failed must
        refuse this shortcut so a stale session cannot become a normal entry.
        """

    @property
    @abstractmethod
    def certified_pn(self) -> str:
        """The certified full PN of the current attempt, or ``""``."""

    @property
    @abstractmethod
    def silent_bootstrap_offer(self) -> SilentSessionBootstrapOffer | None:
        """The typed silent bootstrap offer from a non-certified identity, if any."""

    @property
    @abstractmethod
    def recovery_outcome(self) -> RecoveryVerificationOutcome | None:
        """The held (not-yet-consumed) recovery outcome, if any."""

    @property
    @abstractmethod
    def terminal_input(self) -> RecoveryTerminalInput:
        """The recovery terminal input carried into the terminal handoff."""

    @abstractmethod
    def consume_recovery_outcome(self) -> RecoveryVerificationOutcome | None:
        """Return and clear the held recovery outcome (for finalize/adoption)."""

    # -- owner operations ------------------------------------------------- #

    @abstractmethod
    def adopt_recovery(self, outcome: RecoveryVerificationOutcome) -> bool:
        """Adopt the exact prepared owner a successful recovery outcome carries.

        Returns whether the owner certified and was adopted as this attempt's
        held terminal owner. Ownership is never rebuilt by PN.
        """

    @abstractmethod
    def release_unadopted_recovery(self) -> None:
        """Release a held-but-not-yet-adopted recovery outcome's prepared owner."""

    @abstractmethod
    def release_exact_recovery_owner(
        self, outcome: RecoveryVerificationOutcome
    ) -> None:
        """Release exactly ``outcome``'s prepared owner -- never another's."""

    # -- terminal handoff ownership --------------------------------------- #

    @abstractmethod
    def prepare_terminal(
        self, collector_pn: str, recovery: RecoveryTerminalInput
    ) -> TerminalDecision:
        """Decide the terminal handoff for the owner this attempt holds.

        Distinguishes an already-prepared recovery owner (verified via
        ``prepared_handoff_identity`` for the exact owner, committed AFTER the
        terminal) from a normal identity claim (committed via ``prepare_handoff``
        now). Returns a typed :class:`TerminalDecision`; performs no create/update
        and returns no ConfigFlowResult.
        """

    @abstractmethod
    def commit_terminal(self) -> None:
        """Commit the prepared terminal ownership after the terminal returns."""

    @abstractmethod
    def rollback_terminal(self) -> None:
        """Roll back exactly this attempt's owner after the terminal raised."""

    @abstractmethod
    def release_terminal_owner(self) -> None:
        """Release this attempt's uncommitted claim on cancel/removal."""


__all__ = ["CallbackContinuation", "CallbackIdentityContext", "TerminalDecision"]
