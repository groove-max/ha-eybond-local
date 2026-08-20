"""Callback recovery: the SAME reset machine, second phase (typed proof).

The engine proves FUTURE recoverability over one explicit unicast route --
never mere identity of a live session. These tests pin the phase transition
(immediate autonomous snapshot before callback), the single causal lease across both phases, the
exactly-one-sequence trigger accounting, the route/NAT model, the proof gate,
ownership/cleanup, and the architecture guards.
"""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import replace
import inspect
from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.callback_ledger import (
    CallbackTriggerLedger,
)
from custom_components.eybond_local.connection.recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    RecoveryContract,
)
from custom_components.eybond_local.connection.session_registry import (
    CallbackSessionRegistry,
)
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
    CallbackRecoveryVerifier,
    FAILURE_CALLBACK_INTERFERENCE,
    FAILURE_CALLBACK_PROOF_INVALID,
    FAILURE_CALLBACK_TIMEOUT,
    FAILURE_RECOVERY_IDENTITY_MISMATCH,
    FAILURE_SILENT_PROBE_FAILED,
    FAILURE_SILENT_PROBE_UNAVAILABLE,
    FAILURE_SILENT_SESSION_AMBIGUOUS,
    FAILURE_CAUSALITY_BUSY,
    FAILURE_OWNERSHIP_UNAVAILABLE,
    FAILURE_RECONNECTED_SESSION_UNTRUSTED,
    FAILURE_RESTART_NOT_SUPPORTED,
    FAILURE_ROUTE_INVALID,
    FAILURE_SESSION_CLAIMED,
    FAILURE_STRONG_IDENTITY_TIMEOUT,
    FAILURE_TRIGGER_NOT_SENT,
    RecoveryVerificationOutcome,
    STATE_CALLBACK_VERIFIED,
    STATE_INBOUND_NOT_VERIFIED,
    STATE_INBOUND_RECOVERED,
    STATE_INBOUND_VERIFIED,
    STATE_WAITING_FOR_CALLBACK_SESSION,
    async_run_callback_recovery_transaction,
)
from custom_components.eybond_local.onboarding.timeouts import (
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)

# Synthetic identities only.
FULL_PN = "V001020SYN62344022"
SHORT_PN = "V001020SYN6234"
OTHER_FULL_PN = "V000405SYN94677058"
OLD_SESSION = "listener-18899-1"
NEW_SESSION = "listener-18899-2"
TS = "2026-07-16T10:00:00+00:00"

# Deliberately NAT-shaped: the advertised endpoint differs from the bind
# address AND from the internal listener port.
ROUTE = CallbackRecoveryRoute(
    bind_ip="192.168.1.50",
    trigger_target_ip="192.168.1.60",
    trigger_udp_port=58899,
    advertised_ha_host="198.51.100.7",
    advertised_ha_port=48899,
    listener_port=18899,
)

FAST_POLICY = replace(
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    inbound_strong_identity_timeout=0.2,
    inbound_restart_disconnect_timeout=0.2,
    inbound_reconnect_timeout=0.1,
    callback_recovery_session_wait=0.2,
    callback_causality_lease_wait=1.0,
)


class _FakeChannel:
    def __init__(self, *, restart_error: Exception | None = None, drops_on_restart: bool = True) -> None:
        self.restart_calls = 0
        self.close_calls = 0
        self._restart_error = restart_error
        self._drops = drops_on_restart

    async def async_send_restart(self) -> None:
        self.restart_calls += 1
        if self._restart_error is not None:
            raise self._restart_error

    def is_connected(self) -> bool:
        return False

    async def async_close(self) -> None:
        self.close_calls += 1


def _session(
    session_id: str,
    pn: str,
    state: str = "identified_strong",
    *,
    strong: bool = True,
    identity_source: str | None = None,
    listener_port: int = ROUTE.listener_port,
) -> dict[str, object]:
    source = (
        identity_source
        if identity_source is not None
        else ("fc2_parameter_2" if strong else "framed_heartbeat")
    )
    return {
        "session_id": session_id,
        "collector_pn": pn,
        "state": state,
        "has_strong_identity": strong,
        "collector_identity_source": source,
        "listener_port": listener_port,
        "raw": {
            "session_id": session_id,
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "listener_port": listener_port,
        },
    }


def _strong_old() -> dict[str, object]:
    return _session(OLD_SESSION, FULL_PN)


class _Inventory:
    """Mutable inventory driven by the scripted channel/sender."""

    def __init__(self, *sessions) -> None:
        self.sessions = list(sessions)

    def __call__(self):
        return tuple(self.sessions)


class _Sender:
    """Scripted trigger sender: records like the production facade does."""

    def __init__(self, *, ledger, sends=1, on_send=None, error=None):
        self._ledger = ledger
        self._sends = sends
        self._on_send = on_send
        self._error = error
        self.routes: list = []

    async def async_send(self, route) -> None:
        self.routes.append(route)
        if self._error is not None:
            raise self._error
        for _ in range(self._sends):
            self._ledger.record(target=route.trigger_target_ip, source="test_recovery")
        if self._on_send is not None:
            self._on_send()


# The owner token the default test prepare-handoff hook commits under.
PREPARED_OWNER = "callback_recovery:prepared-test-owner"


def _verifier(channel, inventory, *, ledger, sender=None, route=ROUTE, **kwargs):
    defaults = {
        "collector_pn": FULL_PN,
        "session_id": OLD_SESSION,
        "restart_channel": channel,
        "sessions_source": inventory,
        "clock": lambda: TS,
        "policy": FAST_POLICY,
        "ledger": ledger,
        # Ownership capabilities are MANDATORY for proof-producing successes;
        # tests exercising their absence pass ``None`` explicitly.
        "retarget_claim": lambda _sid: True,
        "prepare_handoff": lambda _pn: PREPARED_OWNER,
        "poll_interval": 0.01,
    }
    defaults.update(kwargs)
    return CallbackRecoveryVerifier(route=route, trigger_sender=sender, **defaults)


def _reset_drops_old(inventory):
    """Channel whose confirmed reboot removes the old socket (no reconnect)."""

    class _Channel(_FakeChannel):
        async def async_send_restart(self) -> None:
            await super().async_send_restart()
            inventory.sessions = [
                session
                for session in inventory.sessions
                if session["session_id"] != OLD_SESSION
            ]

    return _Channel()


class CallbackRecoverySuccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_callback_route_does_not_wait_the_inbound_budget(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(
                _session(NEW_SESSION, FULL_PN)
            ),
        )
        policy = replace(
            FAST_POLICY,
            inbound_reconnect_timeout=30.0,
            callback_recovery_session_wait=0.2,
        )

        outcome = await asyncio.wait_for(
            _verifier(
                _reset_drops_old(inventory),
                inventory,
                ledger=ledger,
                sender=sender,
                policy=policy,
            ).async_verify(),
            timeout=1.0,
        )

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(len(sender.routes), 1)

    async def test_callback_after_reset_yields_validated_proof(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        retargets: list[str] = []
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(_session(NEW_SESSION, FULL_PN)),
        )

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            retarget_claim=lambda sid: retargets.append(sid) or True,
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(outcome.status, STATE_CALLBACK_VERIFIED)
        self.assertIsNone(outcome.inbound_proof)
        # Success is atomically handoff-ready: the outcome hands the consumer
        # the EXACT prepared owner token, never a PN to search by.
        self.assertEqual(outcome.handoff_owner, PREPARED_OWNER)
        proof = outcome.callback_proof
        self.assertEqual(proof.method, CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT)
        self.assertEqual(proof.collector_pn, FULL_PN)
        self.assertEqual(proof.identity_source, "fc2_parameter_2")
        self.assertEqual(proof.verified_at, TS)
        # Opaque route snapshots, verbatim from the model.
        self.assertEqual(proof.trigger_target, "192.168.1.60:58899")
        self.assertEqual(proof.advertised_ha_endpoint, "198.51.100.7:48899")
        self.assertEqual(proof.listener_port, 18899)
        # The strict contract accepts the proof (the engine pre-validated it).
        contract = RecoveryContract.empty_for_pn(
            proof.collector_pn, identity_source=proof.identity_source
        ).with_callback_proof(proof, updated_at=proof.verified_at)
        self.assertTrue(contract.callback_verified)
        # SUCCESS retargeted the claim onto the NEW callback session.
        self.assertEqual(retargets[-1], NEW_SESSION)
        # The immediate autonomous snapshot ran first; no timed inbound window.
        self.assertIn(STATE_WAITING_FOR_CALLBACK_SESSION, outcome.transitions)
        self.assertIn("waiting_for_inbound_reconnect", outcome.transitions)

    async def test_sender_receives_advertised_values_verbatim(self) -> None:
        # NAT model: bind != advertised host; advertised port != listener_port.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(_session(NEW_SESSION, FULL_PN)),
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertTrue(outcome.callback_verified)
        (sent_route,) = sender.routes
        self.assertEqual(sent_route.bind_ip, "192.168.1.50")
        self.assertEqual(sent_route.advertised_ha_host, "198.51.100.7")
        self.assertEqual(sent_route.advertised_ha_port, 48899)
        self.assertEqual(sent_route.trigger_target_ip, "192.168.1.60")
        self.assertNotEqual(sent_route.advertised_ha_host, sent_route.bind_ip)
        self.assertNotEqual(sent_route.advertised_ha_port, sent_route.listener_port)

    async def test_autonomous_inbound_reconnect_short_circuits_callback(self) -> None:
        # The collector came back ON ITS OWN: inbound_recovered, the inbound
        # proof from the SAME reset is kept, and ZERO callback sends happened.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())

        class _Channel(_FakeChannel):
            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory.sessions = [_session(NEW_SESSION, FULL_PN)]

        sender = _Sender(ledger=ledger)
        outcome = await _verifier(
            _Channel(),
            inventory,
            ledger=ledger,
            sender=sender,
            policy=replace(FAST_POLICY, inbound_reconnect_timeout=1.0),
        ).async_verify()

        self.assertTrue(outcome.inbound_recovered, outcome.failure_reason)
        self.assertEqual(outcome.status, STATE_INBOUND_RECOVERED)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(
            outcome.inbound_proof.method,
            INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        )
        # inbound_recovered is a SUCCESS of the callback transaction: it is
        # handoff-ready exactly like callback_verified.
        self.assertEqual(outcome.handoff_owner, PREPARED_OWNER)
        # No trigger, no sender invocation, ledger untouched.
        self.assertEqual(sender.routes, [])
        self.assertEqual(ledger.snapshot_generation(), 0)

    async def test_at_most_one_proof_is_structurally_enforced(self) -> None:
        inbound, callback = _sample_proofs()
        with self.assertRaises(ValueError):
            RecoveryVerificationOutcome(
                inbound_proof=inbound, callback_proof=callback
            )


def _sample_proofs():
    from custom_components.eybond_local.connection.recovery_contract import (
        CallbackRecoveryProof,
        InboundRecoveryProof,
    )

    inbound = InboundRecoveryProof(
        method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        collector_pn=FULL_PN,
        identity_source="fc2_parameter_2",
        verified_at=TS,
    )
    callback = CallbackRecoveryProof(
        method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        collector_pn=FULL_PN,
        identity_source="fc2_parameter_2",
        verified_at=TS,
        trigger_target="192.168.1.60:58899",
        advertised_ha_endpoint="198.51.100.7:48899",
        listener_port=18899,
    )
    return inbound, callback


class RecoveryOutcomeMatrixTests(unittest.TestCase):
    """The combined outcome is IMMUTABLE and status/proof/owner-consistent."""

    def _valid_callback_outcome(self) -> RecoveryVerificationOutcome:
        _, callback = _sample_proofs()
        return RecoveryVerificationOutcome(
            status=STATE_CALLBACK_VERIFIED,
            collector_pn=FULL_PN,
            new_session_id=NEW_SESSION,
            callback_proof=callback,
            handoff_owner=PREPARED_OWNER,
        )

    def test_outcome_is_frozen(self) -> None:
        outcome = self._valid_callback_outcome()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            outcome.status = STATE_INBOUND_NOT_VERIFIED  # type: ignore[misc]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            outcome.callback_proof = None  # type: ignore[misc]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            outcome.handoff_owner = "other"  # type: ignore[misc]

    def test_callback_verified_requires_proof_session_and_owner(self) -> None:
        _, callback = _sample_proofs()
        base = dict(
            status=STATE_CALLBACK_VERIFIED,
            collector_pn=FULL_PN,
            new_session_id=NEW_SESSION,
            callback_proof=callback,
            handoff_owner=PREPARED_OWNER,
        )
        RecoveryVerificationOutcome(**base)  # the valid shape constructs
        for broken in (
            {**base, "callback_proof": None},
            {**base, "new_session_id": ""},
            {**base, "handoff_owner": ""},
            {**base, "failure_reason": "leftover"},
        ):
            with self.subTest(
                broken=sorted(k for k in broken if broken.get(k) != base.get(k))
            ):
                with self.assertRaises((TypeError, ValueError)):
                    RecoveryVerificationOutcome(**broken)

    def test_inbound_recovered_requires_proof_session_and_owner(self) -> None:
        inbound, _ = _sample_proofs()
        base = dict(
            status=STATE_INBOUND_RECOVERED,
            collector_pn=FULL_PN,
            new_session_id=NEW_SESSION,
            inbound_proof=inbound,
            handoff_owner=PREPARED_OWNER,
        )
        RecoveryVerificationOutcome(**base)
        for broken in (
            {**base, "inbound_proof": None},
            {**base, "new_session_id": ""},
            {**base, "handoff_owner": ""},
        ):
            with self.subTest(
                broken=sorted(k for k in broken if broken.get(k) != base.get(k))
            ):
                with self.assertRaises((TypeError, ValueError)):
                    RecoveryVerificationOutcome(**broken)

    def test_inbound_verified_never_carries_a_handoff_owner(self) -> None:
        # The inbound-only facade mode has no handoff lifecycle: the owner
        # token exists ONLY on the callback transaction's two successes.
        inbound, _ = _sample_proofs()
        RecoveryVerificationOutcome(
            status=STATE_INBOUND_VERIFIED,
            collector_pn=FULL_PN,
            new_session_id=NEW_SESSION,
            inbound_proof=inbound,
        )
        with self.assertRaises(ValueError):
            RecoveryVerificationOutcome(
                status=STATE_INBOUND_VERIFIED,
                collector_pn=FULL_PN,
                new_session_id=NEW_SESSION,
                inbound_proof=inbound,
                handoff_owner=PREPARED_OWNER,
            )

    def test_failure_carries_no_proof_and_no_owner(self) -> None:
        inbound, callback = _sample_proofs()
        RecoveryVerificationOutcome(
            status=STATE_INBOUND_NOT_VERIFIED,
            failure_reason=FAILURE_CALLBACK_TIMEOUT,
            collector_pn=FULL_PN,
        )
        for broken in (
            dict(status=STATE_INBOUND_NOT_VERIFIED, inbound_proof=inbound),
            dict(status=STATE_INBOUND_NOT_VERIFIED, callback_proof=callback),
            dict(status=STATE_INBOUND_NOT_VERIFIED, handoff_owner=PREPARED_OWNER),
            # A default (non-terminal) status can smuggle nothing either.
            dict(callback_proof=callback),
            dict(inbound_proof=inbound),
            dict(handoff_owner=PREPARED_OWNER),
        ):
            with self.subTest(broken=sorted(broken)):
                with self.assertRaises(ValueError):
                    RecoveryVerificationOutcome(collector_pn=FULL_PN, **broken)

    # ------ the trust boundary: duck proofs, foreign PNs, padded tokens ------

    def _success_kwargs(self, status: str, proof_field: str, proof) -> dict:
        kwargs = dict(
            status=status,
            collector_pn=FULL_PN,
            new_session_id=NEW_SESSION,
        )
        kwargs[proof_field] = proof
        if status != STATE_INBOUND_VERIFIED:
            kwargs["handoff_owner"] = PREPARED_OWNER
        return kwargs

    def test_duck_and_cross_type_proofs_are_rejected(self) -> None:
        from types import SimpleNamespace

        inbound, callback = _sample_proofs()
        duck = SimpleNamespace(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=FULL_PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.60:58899",
            advertised_ha_endpoint="198.51.100.7:48899",
            listener_port=18899,
        )
        cases = (
            (STATE_CALLBACK_VERIFIED, "callback_proof", object()),
            (STATE_CALLBACK_VERIFIED, "callback_proof", duck),
            (STATE_CALLBACK_VERIFIED, "callback_proof", inbound),  # cross-type
            (STATE_INBOUND_RECOVERED, "inbound_proof", object()),
            (STATE_INBOUND_RECOVERED, "inbound_proof", SimpleNamespace()),
            (STATE_INBOUND_RECOVERED, "inbound_proof", callback),  # cross-type
            (STATE_INBOUND_VERIFIED, "inbound_proof", duck),
        )
        for status, field_name, proof in cases:
            with self.subTest(status=status, proof=type(proof).__name__):
                with self.assertRaises(TypeError):
                    RecoveryVerificationOutcome(
                        **self._success_kwargs(status, field_name, proof)
                    )

    def test_foreign_pn_proofs_are_rejected(self) -> None:
        from custom_components.eybond_local.connection.recovery_contract import (
            CallbackRecoveryProof,
            InboundRecoveryProof,
        )

        foreign_callback = CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=OTHER_FULL_PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.60:58899",
            advertised_ha_endpoint="198.51.100.7:48899",
            listener_port=18899,
        )
        foreign_inbound = InboundRecoveryProof(
            method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
            collector_pn=OTHER_FULL_PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
        )
        with self.assertRaises(ValueError):
            RecoveryVerificationOutcome(
                **self._success_kwargs(
                    STATE_CALLBACK_VERIFIED, "callback_proof", foreign_callback
                )
            )
        with self.assertRaises(ValueError):
            RecoveryVerificationOutcome(
                **self._success_kwargs(
                    STATE_INBOUND_RECOVERED, "inbound_proof", foreign_inbound
                )
            )

    def test_empty_or_padded_outcome_pn_is_rejected(self) -> None:
        _, callback = _sample_proofs()
        for bad_pn in ("", f" {FULL_PN} ", 123, None):
            with self.subTest(pn=bad_pn):
                kwargs = self._success_kwargs(
                    STATE_CALLBACK_VERIFIED, "callback_proof", callback
                )
                kwargs["collector_pn"] = bad_pn
                with self.assertRaises((TypeError, ValueError)):
                    RecoveryVerificationOutcome(**kwargs)

    def test_padded_or_nonstring_tokens_are_rejected(self) -> None:
        _, callback = _sample_proofs()
        for field_name, bad in (
            ("new_session_id", f" {NEW_SESSION} "),
            ("new_session_id", b"listener-18899-2"),
            ("new_session_id", 42),
            ("handoff_owner", f" {PREPARED_OWNER} "),
            ("handoff_owner", 42),
        ):
            with self.subTest(field=field_name, value=bad):
                kwargs = self._success_kwargs(
                    STATE_CALLBACK_VERIFIED, "callback_proof", callback
                )
                kwargs[field_name] = bad
                with self.assertRaises((TypeError, ValueError)):
                    RecoveryVerificationOutcome(**kwargs)

    def test_malformed_proof_fields_are_rejected_by_the_contract_gate(self) -> None:
        # The outcome may not exist holding a proof the strict RecoveryContract
        # model would refuse to persist.
        from custom_components.eybond_local.connection.recovery_contract import (
            CallbackRecoveryProof,
            InboundRecoveryProof,
        )

        valid = dict(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=FULL_PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.60:58899",
            advertised_ha_endpoint="198.51.100.7:48899",
            listener_port=18899,
        )
        for broken in (
            {**valid, "method": "bogus_method"},
            {**valid, "identity_source": "framed_heartbeat"},  # weak source
            {**valid, "verified_at": "2026-07-16T10:00:00"},  # naive
            {**valid, "verified_at": ""},
            {**valid, "trigger_target": ""},  # partial route snapshot
            {**valid, "advertised_ha_endpoint": " padded "},
            {**valid, "listener_port": 0},
        ):
            with self.subTest(
                broken=sorted(k for k in broken if broken[k] != valid[k])
            ):
                with self.assertRaises((TypeError, ValueError)):
                    RecoveryVerificationOutcome(
                        **self._success_kwargs(
                            STATE_CALLBACK_VERIFIED,
                            "callback_proof",
                            CallbackRecoveryProof(**broken),
                        )
                    )
        with self.assertRaises((TypeError, ValueError)):
            RecoveryVerificationOutcome(
                **self._success_kwargs(
                    STATE_INBOUND_RECOVERED,
                    "inbound_proof",
                    InboundRecoveryProof(
                        method="bogus_method",
                        collector_pn=FULL_PN,
                        identity_source="fc2_parameter_2",
                        verified_at=TS,
                    ),
                )
            )

    def test_short_full_reconciliation_uses_the_central_rule_only(self) -> None:
        # Same identity in either direction constructs; the judgement is the
        # registry's pn_is_same_identity, never a string compare.
        from custom_components.eybond_local.connection.recovery_contract import (
            CallbackRecoveryProof,
        )

        short_proof = CallbackRecoveryProof(
            method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
            collector_pn=SHORT_PN,
            identity_source="fc2_parameter_2",
            verified_at=TS,
            trigger_target="192.168.1.60:58899",
            advertised_ha_endpoint="198.51.100.7:48899",
            listener_port=18899,
        )
        _, full_proof = _sample_proofs()

        # Full outcome PN + short-prefix proof PN: same identity, constructs.
        outcome = RecoveryVerificationOutcome(
            **self._success_kwargs(
                STATE_CALLBACK_VERIFIED, "callback_proof", short_proof
            )
        )
        self.assertTrue(outcome.callback_verified)

        # Short outcome PN + full proof PN: same identity, constructs.
        kwargs = self._success_kwargs(
            STATE_CALLBACK_VERIFIED, "callback_proof", full_proof
        )
        kwargs["collector_pn"] = SHORT_PN
        self.assertTrue(
            RecoveryVerificationOutcome(**kwargs).callback_verified
        )


class CallbackRecoveryFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_pre_reset_session_cannot_confirm_callback(self) -> None:
        # The only "answer" is a socket that existed BEFORE the reset: it is in
        # both baselines and can never confirm anything.
        ledger = CallbackTriggerLedger()
        parallel = _session("listener-18899-9", FULL_PN)
        inventory = _Inventory(_strong_old(), parallel)
        channel = _reset_drops_old(inventory)
        sender = _Sender(ledger=ledger)  # sends, but nothing NEW appears

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)

    async def test_post_reset_baseline_session_cannot_confirm_callback(self) -> None:
        # A socket that appeared during the INBOUND window (weak, never
        # confirmed) is part of the post-reset baseline: the callback answer
        # must be newer than the trigger.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())

        class _Channel(_FakeChannel):
            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory.sessions = [
                    _session(
                        "listener-18899-7",
                        OTHER_FULL_PN,
                        strong=True,
                    )
                ]

        sender = _Sender(ledger=ledger)  # nothing new appears after the send
        outcome = await _verifier(
            _Channel(), inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)

    async def test_trigger_sent_but_no_session_is_callback_timeout(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(ledger=ledger)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)
        self.assertIsNone(outcome.callback_proof)

    async def test_sender_failure_is_trigger_not_sent(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(ledger=ledger, error=OSError("network unreachable"))

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_TRIGGER_NOT_SENT)

    async def test_zero_recorded_sends_is_trigger_not_sent(self) -> None:
        # The sender "succeeded" but the ledger saw no own sequence: the
        # datagram never demonstrably left.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(ledger=ledger, sends=0)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_TRIGGER_NOT_SENT)

    async def test_double_recorded_sends_is_trigger_not_sent(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(ledger=ledger, sends=2)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_TRIGGER_NOT_SENT)

    async def test_foreign_send_in_window_is_interference(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)

        def _answer_and_foreign():
            # An uncoordinated in-process caller records a foreign send while
            # our window is open, then a matching session appears.
            ledger.record(target="other", source="runtime", attempt_id="")
            inventory.sessions.append(_session(NEW_SESSION, FULL_PN))

        sender = _Sender(ledger=ledger, on_send=_answer_and_foreign)
        retargets: list[str] = []

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            retarget_claim=lambda sid: retargets.append(sid) or True,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_INTERFERENCE)
        # No FINAL retarget for an unattributable answer (the weak-path probe
        # never ran either: the session arrived strong).
        self.assertEqual(retargets, [])

    async def test_wrong_pn_session_never_confirms(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(
                _session(NEW_SESSION, OTHER_FULL_PN)
            ),
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)

    async def test_weak_heartbeat_pn_without_authoritative_read_never_confirms(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(
                _session(NEW_SESSION, SHORT_PN, strong=False)
            ),
        )

        # No probe hook installed: the weak candidate can never strengthen.
        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)

    async def test_strong_flag_with_weak_source_is_untrusted(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(
                _session(
                    NEW_SESSION,
                    FULL_PN,
                    strong=True,
                    identity_source="framed_heartbeat",
                )
            ),
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertEqual(
            outcome.failure_reason, FAILURE_RECONNECTED_SESSION_UNTRUSTED
        )

    async def test_listener_port_mismatch_is_route_invalid(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(
                _session(NEW_SESSION, FULL_PN, listener_port=28899)
            ),
        )
        retargets: list[str] = []

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            retarget_claim=lambda sid: retargets.append(sid) or True,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_ROUTE_INVALID)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(retargets, [])  # no final retarget

    async def test_invalid_route_fails_before_reset_or_trigger(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _FakeChannel()
        sender = _Sender(ledger=ledger)

        for bad_route in (
            replace(ROUTE, trigger_target_ip=""),
            replace(ROUTE, trigger_target_ip="  padded "),
            replace(ROUTE, advertised_ha_host=""),
            replace(ROUTE, bind_ip=""),
            replace(ROUTE, trigger_udp_port=0),
            replace(ROUTE, advertised_ha_port=65536),
            replace(ROUTE, listener_port=-1),
            replace(ROUTE, listener_port=True),
        ):
            with self.subTest(route=bad_route):
                outcome = await _verifier(
                    channel, inventory, ledger=ledger, sender=sender, route=bad_route
                ).async_verify()
                self.assertEqual(outcome.failure_reason, FAILURE_ROUTE_INVALID)
                # Neither the collector nor the wire was ever touched.
                self.assertEqual(channel.restart_calls, 0)
                self.assertEqual(sender.routes, [])

    async def test_invalid_clock_is_proof_invalid_without_final_retarget(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(_session(NEW_SESSION, FULL_PN)),
        )
        retargets: list[str] = []

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            clock=lambda: "2026-07-16T10:00:00",  # naive
            retarget_claim=lambda sid: retargets.append(sid) or True,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_PROOF_INVALID)
        self.assertEqual(retargets, [])

    async def test_at_unsupported_reboot_means_no_trigger(self) -> None:
        from custom_components.eybond_local.collector.management import (
            CollectorManagementUnsupportedError,
        )

        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _FakeChannel(
            restart_error=CollectorManagementUnsupportedError(
                "collector_reboot_unsupported_on_at_wire"
            )
        )
        sender = _Sender(ledger=ledger)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_RESTART_NOT_SUPPORTED)
        self.assertIsNone(outcome.callback_proof)
        # No trigger was ever sent for an unresettable collector.
        self.assertEqual(sender.routes, [])
        self.assertEqual(ledger.snapshot_generation(), 0)


class CallbackRecoveryOwnershipTests(unittest.IsolatedAsyncioTestCase):
    """Proof-producing success is impossible without retarget + prepared handoff."""

    def _happy_stack(self, ledger):
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(_session(NEW_SESSION, FULL_PN)),
        )
        return channel, inventory, sender

    async def test_missing_retarget_hook_refuses_before_reset_and_trigger(self) -> None:
        ledger = CallbackTriggerLedger()
        channel, inventory, sender = self._happy_stack(ledger)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender, retarget_claim=None
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertIsNone(outcome.callback_proof)
        self.assertIsNone(outcome.inbound_proof)
        self.assertEqual(outcome.failure_reason, FAILURE_OWNERSHIP_UNAVAILABLE)
        # The collector was never touched and no datagram ever left.
        self.assertEqual(channel.restart_calls, 0)
        self.assertEqual(sender.routes, [])
        self.assertEqual(ledger.causality_owner(), "")

    async def test_missing_prepare_handoff_hook_refuses_before_reset(self) -> None:
        ledger = CallbackTriggerLedger()
        channel, inventory, sender = self._happy_stack(ledger)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender, prepare_handoff=None
        ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_OWNERSHIP_UNAVAILABLE)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(channel.restart_calls, 0)
        self.assertEqual(sender.routes, [])

    async def test_refused_retarget_yields_no_proof(self) -> None:
        ledger = CallbackTriggerLedger()
        channel, inventory, sender = self._happy_stack(ledger)
        prepared: list[str] = []

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            retarget_claim=lambda _sid: False,
            prepare_handoff=lambda pn: prepared.append(pn) or PREPARED_OWNER,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(outcome.failure_reason, FAILURE_SESSION_CLAIMED)
        # prepare_handoff runs only AFTER a successful final retarget.
        self.assertEqual(prepared, [])

    async def test_refused_prepare_handoff_yields_no_proof(self) -> None:
        ledger = CallbackTriggerLedger()
        channel, inventory, sender = self._happy_stack(ledger)
        retargets: list[str] = []

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            retarget_claim=lambda sid: retargets.append(sid) or True,
            prepare_handoff=lambda _pn: "",
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(outcome.failure_reason, FAILURE_SESSION_CLAIMED)
        self.assertEqual(outcome.handoff_owner, "")
        # The retarget DID run first; the refused commit is what failed.
        self.assertEqual(retargets[-1], NEW_SESSION)

    async def test_autonomous_success_also_requires_prepared_handoff(self) -> None:
        # inbound_recovered is a success of the SAME transaction: a refused
        # handoff commit demotes it to a typed failure with no proof.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())

        class _Channel(_FakeChannel):
            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory.sessions = [_session(NEW_SESSION, FULL_PN)]

        outcome = await _verifier(
            _Channel(),
            inventory,
            ledger=ledger,
            sender=_Sender(ledger=ledger),
            policy=replace(FAST_POLICY, inbound_reconnect_timeout=1.0),
            prepare_handoff=lambda _pn: "",
        ).async_verify()

        self.assertFalse(outcome.inbound_recovered)
        self.assertIsNone(outcome.inbound_proof)
        self.assertEqual(outcome.failure_reason, FAILURE_SESSION_CLAIMED)

    async def test_real_registry_success_is_prepared_and_certifiable(self) -> None:
        # The engine driven by REAL CallbackSessionRegistry hooks (exactly the
        # wrapper's wiring): success leaves a committed handoff whose identity
        # the registry itself certifies via prepared_handoff_identity.
        from custom_components.eybond_local.connection.recovery.verification import (
            registry_sessions_projection,
        )

        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_session(OLD_SESSION, SHORT_PN, strong=False))
        registry = CallbackSessionRegistry(sessions_source=inventory)
        owner = "callback_recovery:real-registry-test"
        registry.claim_session(owner, session_id=OLD_SESSION)

        def _strengthen_old() -> None:
            inventory.sessions = [_session(OLD_SESSION, FULL_PN)]

        class _Channel(_FakeChannel):
            async def async_probe_identity(self) -> str:
                _strengthen_old()
                return FULL_PN

            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory.sessions = [
                    session
                    for session in inventory.sessions
                    if session["session_id"] != OLD_SESSION
                ]

        channel = _Channel()
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: inventory.sessions.append(_session(NEW_SESSION, FULL_PN)),
        )

        def _retarget(new_sid: str) -> bool:
            try:
                if registry.claimed_session_id(owner) == new_sid:
                    return True
                return bool(
                    registry.retarget_claim_to_reconnected_session(owner, new_sid)
                )
            except ValueError:
                return False

        def _prepare(full_pn: str) -> str:
            try:
                return owner if registry.prepare_handoff(owner, full_pn) else ""
            except ValueError:
                return ""

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            collector_pn=SHORT_PN,  # discovery knew only the prefix
            sessions_source=registry_sessions_projection(registry),
            promote_claim=lambda pn: registry.promote_claim_to_full_pn(owner, pn),
            retarget_claim=_retarget,
            prepare_handoff=_prepare,
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(outcome.handoff_owner, owner)
        # The claim sits COMMITTED on the new socket under the exact owner
        # token from the outcome -- and the registry certifies the identity
        # even when the caller only holds the short prefix.
        self.assertEqual(registry.claimed_session_id(owner), NEW_SESSION)
        self.assertEqual(
            registry.prepared_handoff_identity(outcome.handoff_owner, SHORT_PN),
            FULL_PN,
        )
        self.assertEqual(
            registry.prepared_handoff_identity(outcome.handoff_owner, FULL_PN),
            FULL_PN,
        )
        # A foreign owner token certifies nothing.
        self.assertEqual(
            registry.prepared_handoff_identity("callback_recovery:other", FULL_PN),
            "",
        )


class CallbackRecoveryCausalityTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_lease_spans_reset_and_callback_phases(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        owners: dict[str, str] = {}

        class _Channel(_FakeChannel):
            async def async_send_restart(self) -> None:
                owners["during_reset"] = ledger.causality_owner()
                await super().async_send_restart()
                inventory.sessions = []

        def _on_send():
            owners["during_send"] = ledger.causality_owner()
            inventory.sessions.append(_session(NEW_SESSION, FULL_PN))

        sender = _Sender(ledger=ledger, on_send=_on_send)
        outcome = await _verifier(
            _Channel(), inventory, ledger=ledger, sender=sender
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        # ONE unbroken lease: the same owner across reset AND callback send.
        self.assertTrue(owners["during_reset"].startswith("recovery_verification:"))
        self.assertEqual(owners["during_reset"], owners["during_send"])
        # Released only after the terminal outcome.
        self.assertEqual(ledger.causality_owner(), "")

    async def test_own_send_passes_gate_after_inhibitor_release(self) -> None:
        # The REAL boundary: the inhibitor covers the reset window; our own
        # send then passes the ledger gate (we own the lease) while a foreign
        # sender is still refused.
        from custom_components.eybond_local.connection.callback_ledger import (
            CallbackTriggerInhibitedError,
        )

        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)
        gate: dict[str, object] = {}

        class _GateProbingSender:
            routes: list = []

            async def async_send(self, route) -> None:
                type(self).routes.append(route)
                # Our own send goes through the production-style gate.
                with ledger.callback_send_scope():
                    ledger.record(target=route.trigger_target_ip, source="own")
                gate["own_send_allowed"] = True

                # A foreign sender in another causality context is refused.
                import contextvars

                def _foreign():
                    try:
                        with ledger.callback_send_scope():
                            return "allowed"
                    except CallbackTriggerInhibitedError:
                        return "refused"

                gate["foreign"] = contextvars.Context().run(_foreign)
                inventory.sessions.append(_session(NEW_SESSION, FULL_PN))

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=_GateProbingSender()
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertTrue(gate["own_send_allowed"])
        self.assertEqual(gate["foreign"], "refused")

    async def test_two_simultaneous_attempts_serialize_on_one_ledger(self) -> None:
        ledger = CallbackTriggerLedger()

        def _make_attempt():
            inventory = _Inventory(_strong_old())
            channel = _reset_drops_old(inventory)
            sender = _Sender(
                ledger=ledger,
                on_send=lambda: inventory.sessions.append(
                    _session(NEW_SESSION, FULL_PN)
                ),
            )
            return _verifier(channel, inventory, ledger=ledger, sender=sender)

        first, second = await asyncio.wait_for(
            asyncio.gather(
                _make_attempt().async_verify(), _make_attempt().async_verify()
            ),
            timeout=5.0,
        )

        # Both succeed BECAUSE they serialized: neither saw the other's
        # trigger inside its own window.
        self.assertTrue(first.callback_verified, first.failure_reason)
        self.assertTrue(second.callback_verified, second.failure_reason)
        self.assertEqual(ledger.causality_owner(), "")

    async def test_busy_causality_is_typed_and_touches_nothing(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _FakeChannel()
        sender = _Sender(ledger=ledger)

        async with ledger.causality_lease("callback_identity:other", timeout=1.0):
            outcome = await _verifier(
                channel,
                inventory,
                ledger=ledger,
                sender=sender,
                policy=replace(FAST_POLICY, callback_causality_lease_wait=0.05),
            ).async_verify()

        self.assertEqual(outcome.failure_reason, FAILURE_CAUSALITY_BUSY)
        self.assertEqual(channel.restart_calls, 0)
        self.assertEqual(sender.routes, [])

    async def test_cancellation_at_every_phase_cleans_lease_and_channel(self) -> None:
        # Phases: identity wait, reboot, disconnect wait, inbound wait, trigger
        # send, callback wait, identity read (weak probe), retarget. Cancel in
        # each: lease released, channel closed, send gate usable again.
        async def _cancel_case(*, build):
            ledger = CallbackTriggerLedger()
            channel, inventory, sender, kwargs = build(ledger)
            merged: dict = {
                # Park-everywhere defaults; a phase builder may override its
                # own policy to reach later phases quickly.
                "policy": replace(
                    FAST_POLICY,
                    inbound_strong_identity_timeout=30.0,
                    inbound_restart_disconnect_timeout=30.0,
                    inbound_reconnect_timeout=30.0,
                    callback_recovery_session_wait=30.0,
                ),
            }
            merged.update(kwargs)
            verifier = _verifier(
                channel, inventory, ledger=ledger, sender=sender, **merged
            )
            task = asyncio.create_task(verifier.async_verify())
            await asyncio.sleep(0.15)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertGreaterEqual(channel.close_calls, 1)
            self.assertEqual(ledger.causality_owner(), "")
            with ledger.callback_send_scope():
                pass

        def _identity_phase(ledger):
            inventory = _Inventory(
                _session(OLD_SESSION, SHORT_PN, strong=False)
            )
            return _FakeChannel(), inventory, _Sender(ledger=ledger), {
                "collector_pn": SHORT_PN
            }

        def _reboot_phase(ledger):
            inventory = _Inventory(_strong_old())

            class _Hang(_FakeChannel):
                async def async_send_restart(self) -> None:
                    self.restart_calls += 1
                    await asyncio.sleep(60)

            return _Hang(), inventory, _Sender(ledger=ledger), {}

        def _disconnect_phase(ledger):
            inventory = _Inventory(_strong_old())  # old never closes
            return _FakeChannel(), inventory, _Sender(ledger=ledger), {}

        def _inbound_phase(ledger):
            inventory = _Inventory(_strong_old())
            return _reset_drops_old(inventory), inventory, _Sender(ledger=ledger), {}

        def _trigger_phase(ledger):
            inventory = _Inventory(_strong_old())
            channel = _reset_drops_old(inventory)

            class _HangSender(_Sender):
                async def async_send(self, route) -> None:
                    self.routes.append(route)
                    await asyncio.sleep(60)

            return channel, inventory, _HangSender(ledger=ledger), {
                "policy": replace(FAST_POLICY, inbound_reconnect_timeout=0.02)
            }

        def _callback_wait_phase(ledger):
            inventory = _Inventory(_strong_old())
            channel = _reset_drops_old(inventory)
            return channel, inventory, _Sender(ledger=ledger), {
                "policy": replace(
                    FAST_POLICY,
                    inbound_reconnect_timeout=0.02,
                    callback_recovery_session_wait=30.0,
                )
            }

        def _identity_read_phase(ledger):
            inventory = _Inventory(_strong_old())
            channel = _reset_drops_old(inventory)

            async def _hanging_probe(_sid):
                await asyncio.sleep(60)

            sender = _Sender(
                ledger=ledger,
                on_send=lambda: inventory.sessions.append(
                    _session(NEW_SESSION, SHORT_PN, strong=False)
                ),
            )
            return channel, inventory, sender, {
                "policy": replace(
                    FAST_POLICY,
                    inbound_reconnect_timeout=0.02,
                    callback_recovery_session_wait=30.0,
                ),
                "probe_reconnected_identity": _hanging_probe,
                "retarget_claim": lambda _sid: True,
            }

        def _retarget_phase(ledger):
            inventory = _Inventory(_strong_old())
            channel = _reset_drops_old(inventory)

            def _hang_forever(_sid):
                # Simulate a stuck retarget via a probe that never finishes
                # after retarget succeeded once; final retarget is sync, so the
                # cancellable point is the surrounding wait loop.
                return True

            sender = _Sender(
                ledger=ledger,
                on_send=lambda: inventory.sessions.append(
                    _session(NEW_SESSION, SHORT_PN, strong=False)
                ),
            )

            async def _never_strengthens(_sid):
                return ""

            return channel, inventory, sender, {
                "policy": replace(
                    FAST_POLICY,
                    inbound_reconnect_timeout=0.02,
                    callback_recovery_session_wait=30.0,
                ),
                "probe_reconnected_identity": _never_strengthens,
                "retarget_claim": _hang_forever,
            }

        for label, build in (
            ("identity", _identity_phase),
            ("reboot", _reboot_phase),
            ("disconnect", _disconnect_phase),
            ("inbound_wait", _inbound_phase),
            ("trigger_send", _trigger_phase),
            ("callback_wait", _callback_wait_phase),
            ("identity_read", _identity_read_phase),
            ("retarget_loop", _retarget_phase),
        ):
            with self.subTest(phase=label):
                await _cancel_case(build=build)


class CallbackRecoveryTransactionWrapperTests(unittest.IsolatedAsyncioTestCase):
    """The public wrapper owns the WHOLE temporary-claim lifecycle."""

    def _registry(self, inventory) -> CallbackSessionRegistry:
        return CallbackSessionRegistry(sessions_source=inventory)

    async def test_failure_releases_the_temporary_claim(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        registry = self._registry(inventory)

        outcome = await async_run_callback_recovery_transaction(
            registry=registry,
            collector_pn=FULL_PN,
            session_id=OLD_SESSION,
            route=replace(ROUTE, trigger_target_ip=""),  # fails before reset
            clock=lambda: TS,
            policy=FAST_POLICY,
            ledger=ledger,
        )

        self.assertEqual(outcome.failure_reason, FAILURE_ROUTE_INVALID)
        self.assertEqual(registry.diagnostics()["claim_count"], 0)
        self.assertEqual(ledger.causality_owner(), "")

    async def test_already_claimed_session_is_typed_and_untouched(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        registry = self._registry(inventory)
        registry.claim_session("entry-other", session_id=OLD_SESSION)

        outcome = await async_run_callback_recovery_transaction(
            registry=registry,
            collector_pn=FULL_PN,
            session_id=OLD_SESSION,
            route=ROUTE,
            clock=lambda: TS,
            policy=FAST_POLICY,
            ledger=ledger,
        )

        self.assertEqual(outcome.failure_reason, FAILURE_SESSION_CLAIMED)
        self.assertEqual(registry.claimed_session_id("entry-other"), OLD_SESSION)

    async def test_cancellation_releases_claim_and_lease(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_session(OLD_SESSION, SHORT_PN, strong=False))
        registry = self._registry(inventory)

        task = asyncio.create_task(
            async_run_callback_recovery_transaction(
                registry=registry,
                collector_pn=SHORT_PN,
                session_id=OLD_SESSION,
                route=ROUTE,
                clock=lambda: TS,
                policy=replace(FAST_POLICY, inbound_strong_identity_timeout=30.0),
                ledger=ledger,
            )
        )
        await asyncio.sleep(0.15)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(registry.diagnostics()["claim_count"], 0)
        self.assertEqual(ledger.causality_owner(), "")

    def test_public_transaction_accepts_no_second_sessions_authority(self) -> None:
        # There is deliberately NO parameter through which a caller could hand
        # the transaction an alternative version of the session truth.
        parameters = inspect.signature(
            async_run_callback_recovery_transaction
        ).parameters
        self.assertNotIn("sessions_source", parameters)

    async def test_forged_projection_cannot_upgrade_a_weak_session(self) -> None:
        # The REAL registry knows the session only weakly (heartbeat prefix);
        # a forger prepares a projection that presents the same session as
        # strong with a certified source. There must be no API to inject it --
        # and the transaction, observing through the registry alone, must not
        # produce anything.
        ledger = CallbackTriggerLedger()
        weak_but_lying = dict(
            _session(OLD_SESSION, FULL_PN, identity_source="framed_heartbeat")
        )
        # Even a lying pre-computed flag inside the raw inventory mapping dies
        # in the registry's projection: has_strong_identity is DERIVED from
        # the identity source, never read from the inventory.
        weak_but_lying["has_strong_identity"] = True
        inventory = _Inventory(weak_but_lying)
        registry = self._registry(inventory)

        (projected,) = registry.observed_sessions_per_socket()
        self.assertFalse(projected.has_strong_identity)

        forged_calls: list[int] = []

        def _forged_projection():
            forged_calls.append(1)
            return (_session(OLD_SESSION, FULL_PN),)  # strong, certified

        with self.assertRaises(TypeError):
            await async_run_callback_recovery_transaction(  # type: ignore[call-arg]
                registry=registry,
                collector_pn=FULL_PN,
                session_id=OLD_SESSION,
                route=ROUTE,
                sessions_source=_forged_projection,
                clock=lambda: TS,
                policy=FAST_POLICY,
                ledger=ledger,
            )

        outcome = await async_run_callback_recovery_transaction(
            registry=registry,
            collector_pn=FULL_PN,
            session_id=OLD_SESSION,
            route=ROUTE,
            clock=lambda: TS,
            policy=FAST_POLICY,
            ledger=ledger,
        )

        self.assertFalse(outcome.callback_verified)
        self.assertFalse(outcome.inbound_recovered)
        self.assertIsNone(outcome.callback_proof)
        self.assertIsNone(outcome.inbound_proof)
        self.assertEqual(outcome.handoff_owner, "")
        self.assertEqual(outcome.failure_reason, FAILURE_STRONG_IDENTITY_TIMEOUT)
        self.assertEqual(forged_calls, [])
        self.assertEqual(registry.diagnostics()["claim_count"], 0)

    async def test_prepare_handoff_refusal_releases_everything(self) -> None:
        # A registry that refuses the handoff commit (claim vanished between
        # retarget and prepare) demotes success to a typed failure and the
        # wrapper releases the claim -- no proof, no prepared handoff, no
        # capability leak.
        class _RefusingRegistry(CallbackSessionRegistry):
            def prepare_handoff(self, attempt_owner: str, full_pn: object) -> bool:
                return False

        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_session(OLD_SESSION, SHORT_PN, strong=False))
        registry = _RefusingRegistry(sessions_source=inventory)
        # Drive the engine through real registry hooks but scripted IO (the
        # wrapper's real channel needs live wire; ownership wiring is what is
        # under test here, so mirror the wrapper hooks 1:1).
        owner = "callback_recovery:refused-prepare"
        registry.claim_session(owner, session_id=OLD_SESSION)

        def _strengthen() -> None:
            inventory.sessions = [_session(OLD_SESSION, FULL_PN)]

        class _Channel(_FakeChannel):
            async def async_probe_identity(self) -> str:
                _strengthen()
                return FULL_PN

            async def async_send_restart(self) -> None:
                await super().async_send_restart()
                inventory.sessions = [
                    session
                    for session in inventory.sessions
                    if session["session_id"] != OLD_SESSION
                ]

        from custom_components.eybond_local.connection.recovery.verification import (
            registry_sessions_projection,
        )

        def _retarget(new_sid: str) -> bool:
            try:
                if registry.claimed_session_id(owner) == new_sid:
                    return True
                return bool(
                    registry.retarget_claim_to_reconnected_session(owner, new_sid)
                )
            except ValueError:
                return False

        def _prepare(full_pn: str) -> str:
            try:
                return owner if registry.prepare_handoff(owner, full_pn) else ""
            except ValueError:
                return ""

        outcome = await _verifier(
            _Channel(),
            inventory,
            ledger=ledger,
            sender=_Sender(
                ledger=ledger,
                on_send=lambda: inventory.sessions.append(
                    _session(NEW_SESSION, FULL_PN)
                ),
            ),
            collector_pn=SHORT_PN,
            sessions_source=registry_sessions_projection(registry),
            promote_claim=lambda pn: registry.promote_claim_to_full_pn(owner, pn),
            retarget_claim=_retarget,
            prepare_handoff=_prepare,
        ).async_verify()
        # Mirror the wrapper's structural cleanup contract for non-successes.
        if not (outcome.callback_verified or outcome.inbound_recovered):
            registry.release(owner)

        self.assertEqual(outcome.failure_reason, FAILURE_SESSION_CLAIMED)
        self.assertIsNone(outcome.callback_proof)
        self.assertEqual(outcome.handoff_owner, "")
        self.assertEqual(registry.diagnostics()["claim_count"], 0)
        self.assertEqual(
            registry.prepared_handoff_identity(owner, FULL_PN), ""
        )


class CallbackRecoveryArchitectureGuardTests(unittest.TestCase):
    """One sender, one reader, one claim path -- and no proof coercion."""

    _MODULE_DIR = (
        REPO_ROOT
        / "custom_components"
        / "eybond_local"
        / "connection"
        / "recovery"
    )
    _MODULES = tuple(sorted(_MODULE_DIR.glob("verification*.py")))

    def _source(self) -> str:
        return "\n".join(path.read_text(encoding="utf-8") for path in self._MODULES)

    def _names(self) -> set[str]:
        import ast

        source = self._source()
        names: set[str] = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    names.update(alias.name.split("."))
            elif isinstance(node, ast.ImportFrom):
                names.update((node.module or "").split("."))
                names.update(alias.name for alias in node.names)
        return names

    def test_single_production_sender_and_reader(self) -> None:
        names = self._names()
        # The one ledger-recorded trigger facade and the one pinned reader.
        self.assertIn("async_send_callback_trigger", names)
        self.assertIn("SessionPinnedIdentityReader", names)
        # No raw probe utility, no second matcher, no provider session.
        for banned in (
            "async_probe_target",
            "async_probe_target_replies",
            "match_callback_answer",
            "SmartEssLocalSession",
            "smartess_local",
            "async_send_collector_reboot_or_apply",
            "collector_kind",
            "cloud_family",
            "hostname",
            "peer_ip",
            "driver_key",
        ):
            self.assertNotIn(banned, names, msg=f"banned: {banned}")

    def test_no_module_level_magic_timeouts(self) -> None:
        source = self._source()
        for gone in (
            "CALLBACK_RECOVERY_WAIT_SECONDS",
            "INBOUND_RECONNECT_TIMEOUT_SECONDS",
            "RESTART_DISCONNECT_TIMEOUT_SECONDS",
            "STRONG_IDENTITY_TIMEOUT_SECONDS",
        ):
            self.assertNotIn(gone, source)

    def test_dead_identity_mismatch_taxonomy_stays_deleted(self) -> None:
        # A foreign-PN inbound session proves nothing about OUR trigger; the
        # honest terminal outcome is a callback timeout. The once-declared
        # never-emitted failure constant must not come back.
        source = self._source()
        self.assertNotIn("FAILURE_IDENTITY_MISMATCH", source)

    def test_identity_outcome_cannot_become_a_recovery_proof(self) -> None:
        from custom_components.eybond_local.connection.callback_identity import (
            CallbackIdentityOutcome,
        )

        outcome = CallbackIdentityOutcome(
            result="",
            collector_pn=FULL_PN,
            session_id="s-live",
            session_protocol="eybond_framed",
            identity_source="fc2_parameter_2",
            handoff_owner="callback_verification:x",
        )
        contract = RecoveryContract.empty_for_pn(
            FULL_PN, identity_source="fc2_parameter_2"
        )
        with self.assertRaises(TypeError):
            contract.with_callback_proof(outcome, updated_at=TS)  # type: ignore[arg-type]
        # And CallbackIdentityOutcome still carries no proof-shaped fields.
        for forbidden in ("trigger_target", "advertised_ha_endpoint", "verified_at"):
            self.assertFalse(hasattr(outcome, forbidden))

    def test_no_callback_proof_writer_outside_the_contract_model(self) -> None:
        # The single-store guard lives in test_recovery_contract; here we pin
        # the batch boundary: callback proofs are persisted ONLY by the
        # terminal boundary (connection.recovery.terminal).
        # The verifier may name with_callback_proof solely for PRE-VALIDATION
        # and must never persist anything itself.
        import ast

        package_root = REPO_ROOT / "custom_components" / "eybond_local"
        for path in sorted(package_root.rglob("*.py")):
            if path.name == "recovery_contract.py":
                continue
            source = path.read_text(encoding="utf-8")
            if "with_callback_proof" not in source:
                continue
            self.assertIn(
                path.name,
                ("verification_engine.py", "verification_models.py", "terminal.py"),
                msg=f"unexpected callback-proof producer: {path.name}",
            )
            if path.name not in {"verification_engine.py", "verification_models.py"}:
                continue
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Attribute)
                    and node.attr == "write_to"
                ):
                    self.fail(
                        "strategy_verification must not persist contracts "
                        "(the terminal boundary owns persistence)"
                    )


if __name__ == "__main__":
    unittest.main()


class SilentReconnectProbeTests(unittest.IsolatedAsyncioTestCase):
    """FULLY silent recovery reconnects: trusted-wire authority, one probe."""

    class _FakeSilentProbe:
        def __init__(self, inventory, *, silent=(), identify_pn=""):
            self._inventory = inventory
            self.silent = list(silent)
            self.identify_calls: list[tuple[str, str]] = []
            self._identify_pn = identify_pn
            self.available = True

        def snapshot_silent_session_ids(self):
            return frozenset(self.silent)

        async def async_identify_exact_session(self, session_id, *, session_protocol):
            self.identify_calls.append((session_id, session_protocol))
            if not self._identify_pn:
                return ""
            self.silent = [sid for sid in self.silent if sid != session_id]
            self._inventory.sessions.append(_session(session_id, self._identify_pn))
            return self._identify_pn

    class _WireChannel(_FakeChannel):
        """A channel that vouches the observed wire and drops the old socket."""

        def __init__(self, inventory, **kwargs) -> None:
            super().__init__(**kwargs)
            self._inventory = inventory

        def observed_wire(self) -> str:
            return "eybond_framed"

        async def async_send_restart(self) -> None:
            await super().async_send_restart()
            self._inventory.sessions = [
                s for s in self._inventory.sessions if s["session_id"] != OLD_SESSION
            ]

    async def test_silent_autonomous_reconnect_is_probed_and_recovered(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)

        def _reboot_makes_silent_socket():
            probe.silent.append(NEW_SESSION)

        original_restart = channel.async_send_restart

        async def _restart():
            await original_restart()
            _reboot_makes_silent_socket()

        channel.async_send_restart = _restart  # type: ignore[method-assign]
        sender = _Sender(ledger=ledger)

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            silent_session_probe=probe,
            policy=replace(FAST_POLICY, inbound_reconnect_timeout=1.0),
        ).async_verify()

        self.assertTrue(outcome.inbound_recovered, outcome.failure_reason)
        # Exactly ONE session-pinned probe, on the PRE-REBOOT observed wire.
        self.assertEqual(probe.identify_calls, [(NEW_SESSION, "eybond_framed")])
        # Zero UDP: the autonomous phase never triggers.
        self.assertEqual(sender.routes, [])
        self.assertEqual(ledger.snapshot_generation(), 0)

    async def test_silent_callback_answer_is_probed_after_one_trigger(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)
        sender = _Sender(
            ledger=ledger, on_send=lambda: probe.silent.append(NEW_SESSION)
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(probe.identify_calls, [(NEW_SESSION, "eybond_framed")])
        self.assertEqual(len(sender.routes), 1)  # exactly one sequence
        self.assertEqual(ledger.snapshot_generation(), 1)

    async def test_two_silent_candidates_are_ambiguous_not_timeout(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)
        sender = _Sender(
            ledger=ledger,
            on_send=lambda: probe.silent.extend(["s-x", "s-y"]),
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        # Two sockets DID arrive -- honest ambiguity, never a plain timeout.
        self.assertEqual(
            outcome.failure_reason, FAILURE_SILENT_SESSION_AMBIGUOUS
        )
        self.assertEqual(probe.identify_calls, [])  # ambiguity: no guessing

    async def test_ambiguity_resolving_to_one_candidate_succeeds(self) -> None:
        # Two silent sockets at first; one closes, leaving a single valid
        # candidate that then identifies as our collector -> success.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)

        def _two_then_one():
            probe.silent.extend(["s-decoy", NEW_SESSION])

            async def _drop_decoy():
                await asyncio.sleep(0.03)
                if "s-decoy" in probe.silent:
                    probe.silent.remove("s-decoy")

            asyncio.get_running_loop().create_task(_drop_decoy())

        sender = _Sender(ledger=ledger, on_send=_two_then_one)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
            policy=replace(FAST_POLICY, callback_recovery_session_wait=2.0),
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(probe.identify_calls, [(NEW_SESSION, "eybond_framed")])

    async def test_foreign_probed_pn_is_definitive_identity_mismatch(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=OTHER_FULL_PN)
        retargets: list[str] = []
        sender = _Sender(
            ledger=ledger, on_send=lambda: probe.silent.append(NEW_SESSION)
        )

        outcome = await _verifier(
            channel,
            inventory,
            ledger=ledger,
            sender=sender,
            silent_session_probe=probe,
            retarget_claim=lambda sid: retargets.append(sid) or True,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        # A foreign strong PN answered: definitive, distinct from timeout.
        self.assertEqual(
            outcome.failure_reason, FAILURE_RECOVERY_IDENTITY_MISMATCH
        )
        # Probed exactly once; the foreign identity was never retargeted.
        self.assertEqual(probe.identify_calls, [(NEW_SESSION, "eybond_framed")])
        self.assertEqual(retargets, [])

    async def test_probe_query_without_pn_is_probe_failed(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn="")  # no answer
        sender = _Sender(
            ledger=ledger, on_send=lambda: probe.silent.append(NEW_SESSION)
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_SILENT_PROBE_FAILED)
        self.assertEqual(probe.identify_calls, [(NEW_SESSION, "eybond_framed")])

    async def test_unavailable_probe_channel_is_typed(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)
        probe.available = False  # the channel could not open its listener
        sender = _Sender(
            ledger=ledger, on_send=lambda: probe.silent.append(NEW_SESSION)
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(
            outcome.failure_reason, FAILURE_SILENT_PROBE_UNAVAILABLE
        )
        self.assertEqual(probe.identify_calls, [])  # never queried

    async def test_no_socket_at_all_stays_plain_timeout(self) -> None:
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)  # stays empty
        sender = _Sender(ledger=ledger)  # nothing ever dials in

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        # NOTHING arrived: the honest reason is the plain callback timeout.
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)
        self.assertEqual(probe.identify_calls, [])

    async def test_inbound_silent_diagnosis_does_not_block_callback_success(
        self,
    ) -> None:
        # The autonomous inbound window sees an AMBIGUOUS pair (diagnostic),
        # then the callback phase's own window gets a clean single silent
        # answer -> callback success. The inbound diagnosis never leaks.
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)

        # During the inbound window (no trigger yet) two decoys are silent.
        probe.silent.extend(["inbound-a", "inbound-b"])

        def _callback_answer():
            # The callback trigger clears the inbound decoys and produces the
            # single real answer of THIS phase's baseline.
            probe.silent.clear()
            probe.silent.append(NEW_SESSION)

        sender = _Sender(ledger=ledger, on_send=_callback_answer)

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
            policy=replace(FAST_POLICY, inbound_reconnect_timeout=0.2),
        ).async_verify()

        self.assertTrue(outcome.callback_verified, outcome.failure_reason)
        self.assertEqual(probe.identify_calls, [(NEW_SESSION, "eybond_framed")])

    async def test_inbound_only_silent_diagnosis_is_surfaced(self) -> None:
        # InboundRecoveryVerifier (no callback route): a silent probe failure
        # in the autonomous window is surfaced, not collapsed to timeout.
        from custom_components.eybond_local.connection.recovery.verification import (
            InboundRecoveryVerifier,
        )

        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = self._WireChannel(inventory)
        probe = self._FakeSilentProbe(inventory, identify_pn="")

        original_restart = channel.async_send_restart

        async def _restart():
            await original_restart()
            probe.silent.append(NEW_SESSION)

        channel.async_send_restart = _restart  # type: ignore[method-assign]

        outcome = await InboundRecoveryVerifier(
            collector_pn=FULL_PN,
            session_id=OLD_SESSION,
            restart_channel=channel,
            sessions_source=inventory,
            clock=lambda: TS,
            policy=replace(FAST_POLICY, inbound_reconnect_timeout=1.0),
            ledger=ledger,
            retarget_claim=lambda _sid: True,
            silent_session_probe=probe,
        ).async_verify()

        self.assertFalse(outcome.inbound_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_SILENT_PROBE_FAILED)

    async def test_no_authority_means_no_probe(self) -> None:
        # The channel cannot vouch for a wire (no observed_wire): silent
        # candidates stay unprobed -- fail-closed, exactly as before (a plain
        # timeout; the wire-authority contract is unchanged).
        ledger = CallbackTriggerLedger()
        inventory = _Inventory(_strong_old())
        channel = _reset_drops_old(inventory)  # no observed_wire method
        probe = self._FakeSilentProbe(inventory, identify_pn=FULL_PN)
        sender = _Sender(
            ledger=ledger, on_send=lambda: probe.silent.append(NEW_SESSION)
        )

        outcome = await _verifier(
            channel, inventory, ledger=ledger, sender=sender,
            silent_session_probe=probe,
        ).async_verify()

        self.assertFalse(outcome.callback_verified)
        self.assertEqual(outcome.failure_reason, FAILURE_CALLBACK_TIMEOUT)
        self.assertEqual(probe.identify_calls, [])
