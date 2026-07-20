"""ONE terminal boundary for recovery outcomes: validate -> merge -> handoff.

Every config-flow terminal (create/update) funnels its recovery evidence
through this module:

* :class:`RecoveryTerminalInput` -- the ONLY accepted, typed carrier of a
  recovery outcome into a terminal. It is constructed exclusively from the
  real verified outcome types (``InboundRecoveryOutcome`` with a proof, or a
  successful ``RecoveryVerificationOutcome`` from the callback recovery
  transaction) or as the explicit :meth:`RecoveryTerminalInput.none`. A
  ``CallbackIdentityOutcome`` -- or any duck -- is a ``TypeError``: identity
  certification is never recovery evidence. It is a small immutable carrier,
  NOT a second persisted model: the one persisted shape stays
  ``RecoveryContract`` under ``entry.data[RECOVERY_CONTRACT_KEY]``.

* :func:`merge_recovery_contract` -- THE single production config-flow writer
  of the recovery contract. It merges the terminal proof into the entry's
  staged ``data`` dict branch-by-branch (a new inbound proof replaces only
  the inbound branch and preserves the callback branch verbatim, and vice
  versa), enriches short -> full PN only through the registry's one
  reconciliation rule, refuses foreign identities and malformed existing
  records fail-closed (staged data untouched), and writes ONLY through
  ``RecoveryContract.write_to``. ``updated_at`` is the new proof's own
  ``verified_at`` -- no clock is ever consulted and no timestamp heuristics
  compare proofs.

* :func:`verify_prepared_handoff` -- the acceptance boundary for the callback
  transaction's ALREADY-PREPARED ownership capability: the exact
  ``handoff_owner`` token is presented to
  ``registry.prepared_handoff_identity`` and only its certified answer
  admits the terminal. The owner is never re-prepared, never reconstructed
  by PN (``owner_for_pn``), and never guessed from peer IPs.

Deliberately NOT here: connection_strategy (user intent, stamped by the flow
from the user's choice -- never inferred from a proof type), endpoint
control/provenance, operation mode, transports.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, MutableMapping

from ..recovery_contract import (
    CallbackRecoveryProof,
    InboundRecoveryProof,
    RECOVERY_CONTRACT_KEY,
    RecoveryContract,
)
from ..session_registry import pn_is_same_identity, prefer_full_pn
from ...const import CONF_COLLECTOR_PN

logger = logging.getLogger(__name__)

# Typed refusal reasons returned by merge_recovery_contract. "" = success.
MERGE_REFUSED_MALFORMED_CONTRACT = "recovery_contract_malformed"
MERGE_REFUSED_ENTRY_IDENTITY = "recovery_terminal_entry_identity_mismatch"
MERGE_REFUSED_PROOF_REJECTED = "recovery_contract_proof_rejected"


def _required_token(value: object) -> bool:
    return type(value) is str and bool(value) and value == value.strip()


@dataclass(frozen=True, slots=True)
class RecoveryTerminalInput:
    """The typed recovery evidence one terminal create/update may consume.

    At most ONE proof; ``prepared_handoff_owner`` is non-empty ONLY for the
    callback recovery transaction's outcomes (whose claim was already
    retargeted and prepared inside the transaction). An empty owner means the
    flow holds its own verification claim and the terminal coordinator
    prepares it exactly once itself.

    Direct construction is validated the same as the classmethods -- there is
    no loose-dict/getattr path into a terminal.
    """

    collector_pn: str = ""
    inbound_proof: InboundRecoveryProof | None = None
    callback_proof: CallbackRecoveryProof | None = None
    prepared_handoff_owner: str = ""

    def __post_init__(self) -> None:
        if self.inbound_proof is not None and self.callback_proof is not None:
            raise ValueError("recovery_terminal_carries_two_proofs")
        if self.inbound_proof is not None and type(self.inbound_proof) is not InboundRecoveryProof:
            raise TypeError("inbound_proof_type_required")
        if self.callback_proof is not None and type(self.callback_proof) is not CallbackRecoveryProof:
            raise TypeError("callback_proof_type_required")
        proof = self.proof
        if proof is None:
            if self.collector_pn or self.prepared_handoff_owner:
                raise ValueError("recovery_terminal_without_proof_carries_state")
            return
        if not _required_token(self.collector_pn):
            raise ValueError("recovery_terminal_requires_normalized_pn")
        if not pn_is_same_identity(self.collector_pn, proof.collector_pn):
            raise ValueError("recovery_terminal_proof_identity_mismatch")
        if self.prepared_handoff_owner and not _required_token(
            self.prepared_handoff_owner
        ):
            raise ValueError("recovery_terminal_handoff_owner_invalid")

    @property
    def proof(self) -> InboundRecoveryProof | CallbackRecoveryProof | None:
        return self.inbound_proof if self.inbound_proof is not None else self.callback_proof

    @property
    def has_proof(self) -> bool:
        return self.proof is not None

    @classmethod
    def none(cls) -> "RecoveryTerminalInput":
        """The explicit 'no recovery outcome' terminal input."""

        return cls()

    @classmethod
    def from_inbound_outcome(cls, outcome: Any) -> "RecoveryTerminalInput":
        """From the inbound-only verifier's outcome (flow-owned claim).

        Fail-closed: exactly ``InboundRecoveryOutcome`` (an identity outcome
        or any duck is a TypeError), and only a verified one with its typed
        proof. The flow keeps holding the verification claim; the terminal
        coordinator prepares the handoff exactly once later.
        """

        from .verification import InboundRecoveryOutcome

        if type(outcome) is not InboundRecoveryOutcome:
            raise TypeError("inbound_recovery_outcome_required")
        if not outcome.inbound_verified:
            raise ValueError("inbound_recovery_outcome_not_verified")
        proof = outcome.proof
        if type(proof) is not InboundRecoveryProof:
            raise TypeError("inbound_proof_type_required")
        return cls(
            # The durable spelling: enrich short -> full through the ONE
            # registry rule; never downgrade.
            collector_pn=prefer_full_pn(outcome.collector_pn, proof.collector_pn),
            inbound_proof=proof,
        )

    @classmethod
    def from_callback_transaction(cls, outcome: Any) -> "RecoveryTerminalInput":
        """From the callback recovery transaction's SUCCESS outcome.

        Both successes are accepted: ``callback_verified`` carries the
        callback proof; ``inbound_recovered`` carries the inbound proof
        earned by the same reset. Which branch of the RecoveryContract gets
        written follows the PROOF -- the entry's connection_strategy stays
        the user's separate intent and is never decided here. The outcome's
        exact ``handoff_owner`` travels along as the already-prepared
        ownership capability.
        """

        from .verification import RecoveryVerificationOutcome

        if type(outcome) is not RecoveryVerificationOutcome:
            raise TypeError("recovery_verification_outcome_required")
        if outcome.callback_verified:
            proof: Any = outcome.callback_proof
            kwargs = {"callback_proof": proof}
        elif outcome.inbound_recovered:
            proof = outcome.inbound_proof
            kwargs = {"inbound_proof": proof}
        else:
            raise ValueError("recovery_verification_outcome_not_verified")
        return cls(
            collector_pn=prefer_full_pn(outcome.collector_pn, proof.collector_pn),
            prepared_handoff_owner=outcome.handoff_owner,
            **kwargs,
        )

    @classmethod
    def from_permanent_owner_transaction(cls, outcome: Any) -> "RecoveryTerminalInput":
        """From a recovery run UNDER an existing permanent owner (Batch 8).

        Same strict outcome/proof validation as
        :meth:`from_callback_transaction`, but the terminal deliberately
        carries NO ``prepared_handoff_owner``: a permanent owner's recovered
        session is certified by the registry's
        ``certify_owner_reconnected_session`` capability, not by an onboarding
        prepare/complete handoff — pretending otherwise would fake an
        ownership-transfer capability the registry never issued.
        """

        from ..session_registry import (
            PermanentOwnedSessionCertification,
        )
        from .verification import RecoveryVerificationOutcome

        if type(outcome) is not RecoveryVerificationOutcome:
            raise TypeError("recovery_verification_outcome_required")
        # A permanent-owner outcome MUST carry the typed certification and NO
        # prepared onboarding handoff -- the two ownership modes never mix.
        if outcome.handoff_owner:
            raise ValueError("permanent_owner_outcome_carries_handoff")
        if type(outcome.owner_certification) is not PermanentOwnedSessionCertification:
            raise TypeError("permanent_owner_certification_required")
        if outcome.callback_verified:
            proof: Any = outcome.callback_proof
            kwargs = {"callback_proof": proof}
        elif outcome.inbound_recovered:
            proof = outcome.inbound_proof
            kwargs = {"inbound_proof": proof}
        else:
            raise ValueError("recovery_verification_outcome_not_verified")
        return cls(
            collector_pn=prefer_full_pn(outcome.collector_pn, proof.collector_pn),
            **kwargs,
        )


def merge_recovery_contract(
    data: MutableMapping[str, Any],
    terminal: RecoveryTerminalInput,
) -> str:
    """Merge the terminal's proof into ``data[RECOVERY_CONTRACT_KEY]``.

    Returns ``""`` on success (``data`` updated via the model's single-writer
    ``write_to``) or a typed refusal reason -- in which case ``data`` is left
    byte-for-byte untouched. No-proof input is a no-op success: it neither
    adds nor removes anything.

    Merge rules (the ONE algorithm for both branches):

    * canonical store only: ``entry.data[RECOVERY_CONTRACT_KEY]``, never
      options;
    * an existing valid same-identity contract is loaded via
      ``RecoveryContract.from_entry_data`` and the new proof replaces ONLY
      its own branch -- the opposite branch is preserved verbatim;
    * a non-empty malformed existing record is never silently clobbered:
      typed refusal, data untouched;
    * ``updated_at`` is the new proof's ``verified_at`` -- never now();
    * short/full PN only enriches through the contract's registry-rule
      reconciliation; a foreign PN is refused without partial changes.
    """

    if type(terminal) is not RecoveryTerminalInput:
        raise TypeError("recovery_terminal_input_required")
    proof = terminal.proof
    if proof is None:
        return ""
    entry_pn = str(data.get(CONF_COLLECTOR_PN) or "").strip()
    if entry_pn and not pn_is_same_identity(entry_pn, terminal.collector_pn):
        return MERGE_REFUSED_ENTRY_IDENTITY
    raw = data.get(RECOVERY_CONTRACT_KEY)
    existing: RecoveryContract | None = None
    if raw is not None and raw != {} and raw != "":
        existing = RecoveryContract.from_entry_data(data)
        if existing is None:
            # A non-empty record the strict parser refuses: replacing it
            # silently would destroy evidence we cannot even read. Refuse and
            # keep the original data for inspection.
            logger.info(
                "Recovery terminal: existing recovery_contract is malformed; refusing merge"
            )
            return MERGE_REFUSED_MALFORMED_CONTRACT
    try:
        base = (
            existing
            if existing is not None
            else RecoveryContract.empty_for_pn(
                terminal.collector_pn, identity_source=proof.identity_source
            )
        )
        if terminal.callback_proof is not None:
            merged = base.with_callback_proof(
                terminal.callback_proof,
                updated_at=terminal.callback_proof.verified_at,
            )
        else:
            merged = base.with_inbound_proof(
                terminal.inbound_proof,
                updated_at=terminal.inbound_proof.verified_at,
            )
    except (TypeError, ValueError) as exc:
        logger.info("Recovery terminal: contract refused the proof: %s", exc)
        return MERGE_REFUSED_PROOF_REJECTED
    merged.write_to(data)
    return ""


def verify_prepared_handoff(registry: Any, terminal: RecoveryTerminalInput) -> str:
    """Acceptance boundary for an ALREADY-PREPARED handoff capability.

    Returns the registry-certified full PN for the terminal's exact
    ``prepared_handoff_owner``, or ``""`` fail-closed (empty, foreign or
    stale owner; registry unavailable). ``prepared_handoff_identity`` is the
    ONLY question asked -- the owner is never re-prepared here and never
    reconstructed by PN lookup.
    """

    if type(terminal) is not RecoveryTerminalInput:
        raise TypeError("recovery_terminal_input_required")
    owner = terminal.prepared_handoff_owner
    if not owner or registry is None:
        return ""
    try:
        return str(
            registry.prepared_handoff_identity(owner, terminal.collector_pn) or ""
        ).strip()
    except Exception:
        logger.debug("Recovery terminal: prepared handoff check failed", exc_info=True)
        return ""


__all__ = [
    "MERGE_REFUSED_ENTRY_IDENTITY",
    "MERGE_REFUSED_MALFORMED_CONTRACT",
    "MERGE_REFUSED_PROOF_REJECTED",
    "RecoveryTerminalInput",
    "merge_recovery_contract",
    "verify_prepared_handoff",
]
