"""RecoveryContract: pure typed model, fail-closed parsing, guards.

The contract stores RESULTS of real recovery proofs and nothing else. These
tests pin, in order: the model's constructors/updates (including the STRICT
direct constructor), the strong-identity boundary, the timezone-aware
timestamp rules, the parser's fail-closed behavior on every malformed shape,
the diagnostics' non-disclosure, and the architectural guards that keep the
module pure and the persisted store single.
"""

from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.recovery_contract import (
    CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
    CallbackRecoveryProof,
    INBOUND_RECOVERY_METHODS,
    INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    InboundRecoveryProof,
    RECOVERY_CONTRACT_KEY,
    RECOVERY_CONTRACT_SCHEMA_VERSION,
    RecoveryContract,
    recovery_contract_diagnostics,
)

# Synthetic identities only.
FULL_PN = "V001020SYN62344022"
SHORT_PN = "V001020SYN6234"  # a strict prefix of FULL_PN
OTHER_FULL_PN = "V000405SYN94677058"
TS = "2026-07-16T10:00:00+00:00"
TS_LATER = "2026-07-16T11:30:00+00:00"

STRONG_FC2 = "fc2_parameter_2"
STRONG_DTUPN = "at_dtupn"

# Deliberately network-looking opaque snapshots: the non-disclosure tests below
# prove these exact strings never surface in diagnostics or bundles.
TRIGGER_TARGET = "203.0.113.55:58899"
ADVERTISED = "192.0.2.10:18899"
LISTENER_PORT = 18899


def _inbound(
    pn=FULL_PN,
    *,
    method=INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
    identity_source=STRONG_FC2,
    verified_at=TS,
    session_protocol="eybond_framed",
):
    return InboundRecoveryProof(
        method=method,
        collector_pn=pn,
        identity_source=identity_source,
        verified_at=verified_at,
        session_protocol=session_protocol,
    )


def _callback(
    pn=FULL_PN,
    *,
    identity_source=STRONG_DTUPN,
    verified_at=TS,
    trigger_target=TRIGGER_TARGET,
    advertised=ADVERTISED,
    listener_port=LISTENER_PORT,
):
    return CallbackRecoveryProof(
        method=CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        collector_pn=pn,
        identity_source=identity_source,
        verified_at=verified_at,
        trigger_target=trigger_target,
        advertised_ha_endpoint=advertised,
        listener_port=listener_port,
    )


def _empty(pn=FULL_PN, *, identity_source=STRONG_FC2, updated_at=""):
    return RecoveryContract.empty_for_pn(
        pn, identity_source=identity_source, updated_at=updated_at
    )


def _both_record() -> dict:
    contract = (
        _empty()
        .with_inbound_proof(_inbound(), updated_at=TS)
        .with_callback_proof(_callback(), updated_at=TS_LATER)
    )
    return contract.to_record()


class RecoveryContractModelTests(unittest.TestCase):
    def test_empty_contract(self) -> None:
        contract = _empty()
        self.assertTrue(contract.pn_bound)
        self.assertTrue(contract.is_empty)
        self.assertFalse(contract.inbound_verified)
        self.assertFalse(contract.callback_verified)
        self.assertEqual(contract.schema_version, RECOVERY_CONTRACT_SCHEMA_VERSION)
        self.assertEqual(contract.collector_identity_source, STRONG_FC2)

    def test_contract_requires_durable_pn(self) -> None:
        for missing in ("", "   ", None):
            with self.subTest(pn=missing):
                with self.assertRaises(ValueError):
                    _empty(missing)

    def test_contract_requires_strong_identity_source(self) -> None:
        # A short heartbeat PN / unknown observation can never bind a contract.
        for weak in ("", "   ", "framed_heartbeat", "heartbeat", "manual", None):
            with self.subTest(source=weak):
                with self.assertRaises(ValueError):
                    _empty(identity_source=weak)

    def test_both_strong_sources_are_accepted(self) -> None:
        # FC=2 parameter 2 and AT DTUPN are the two authoritative reads.
        self.assertEqual(_empty(identity_source=STRONG_FC2).collector_identity_source, STRONG_FC2)
        self.assertEqual(
            _empty(identity_source=STRONG_DTUPN).collector_identity_source, STRONG_DTUPN
        )

    def test_empty_seed_updated_at_is_blank_or_aware_timestamp(self) -> None:
        # Documented explicitly: "" (nothing recorded yet) or a valid aware
        # timestamp -- nothing else.
        self.assertEqual(_empty(updated_at="").updated_at, "")
        self.assertEqual(_empty(updated_at=TS).updated_at, TS)
        for bad in ("2026-07-16", "2026-07-16T10:00:00", "soon", 5):
            with self.subTest(updated_at=bad):
                with self.assertRaises(ValueError):
                    _empty(updated_at=bad)

    def test_inbound_only(self) -> None:
        contract = _empty().with_inbound_proof(_inbound(), updated_at=TS)
        self.assertTrue(contract.inbound_verified)
        self.assertFalse(contract.callback_verified)
        self.assertEqual(contract.inbound_proof.verified_at, TS)
        self.assertEqual(contract.inbound_proof.identity_source, STRONG_FC2)
        # The confirmed live wire is carried as information only.
        self.assertEqual(contract.inbound_proof.session_protocol, "eybond_framed")

    def test_callback_only(self) -> None:
        contract = _empty().with_callback_proof(_callback(), updated_at=TS)
        self.assertFalse(contract.inbound_verified)
        self.assertTrue(contract.callback_verified)
        self.assertEqual(contract.callback_proof.trigger_target, TRIGGER_TARGET)
        self.assertEqual(contract.callback_proof.identity_source, STRONG_DTUPN)
        self.assertEqual(contract.callback_proof.listener_port, LISTENER_PORT)

    def test_both_proofs_roundtrip_serialization(self) -> None:
        record = _both_record()
        parsed = RecoveryContract.from_record(record)
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed.inbound_verified)
        self.assertTrue(parsed.callback_verified)
        self.assertEqual(parsed.collector_pn, FULL_PN)
        self.assertEqual(parsed.collector_identity_source, STRONG_FC2)
        # Identity sources survive the roundtrip verbatim.
        self.assertEqual(parsed.inbound_proof.identity_source, STRONG_FC2)
        self.assertEqual(parsed.callback_proof.identity_source, STRONG_DTUPN)
        self.assertEqual(parsed.updated_at, TS_LATER)
        # Deterministic: serialize -> parse -> serialize is byte-identical.
        self.assertEqual(parsed.to_record(), record)

    def test_from_entry_data_reads_only_the_canonical_key(self) -> None:
        record = _both_record()
        contract = RecoveryContract.from_entry_data({RECOVERY_CONTRACT_KEY: record})
        self.assertIsNotNone(contract)
        self.assertTrue(contract.callback_verified)
        # No loose-field fallback: the same record spread flat is invisible.
        self.assertIsNone(RecoveryContract.from_entry_data(dict(record)))
        self.assertIsNone(RecoveryContract.from_entry_data(None))
        self.assertIsNone(RecoveryContract.from_entry_data({}))

    # --- fail-closed parsing ---------------------------------------------------

    def test_unknown_schema_version_is_untrusted(self) -> None:
        record = _both_record()
        for version in (RECOVERY_CONTRACT_SCHEMA_VERSION + 1, 0, -1, "1", None, True):
            with self.subTest(version=version):
                bad = dict(record)
                bad["schema_version"] = version
                self.assertIsNone(RecoveryContract.from_record(bad))

    def test_malformed_top_level_record_fails_closed(self) -> None:
        for malformed in (None, "", 7, [], "recovery", object()):
            with self.subTest(record=malformed):
                self.assertIsNone(RecoveryContract.from_record(malformed))

    def test_missing_pn_fails_closed(self) -> None:
        record = _both_record()
        for pn in ("", "  ", None, 5):
            with self.subTest(pn=pn):
                bad = dict(record)
                bad["collector_pn"] = pn
                self.assertIsNone(RecoveryContract.from_record(bad))

    def test_weak_or_missing_contract_identity_source_untrusts_everything(self) -> None:
        # A contract whose OWN identity was never read authoritatively cannot
        # vouch for anything under it -- including otherwise-valid proofs.
        record = _both_record()
        for weak in ("", "framed_heartbeat", "heartbeat", "expected", None, 3):
            with self.subTest(source=weak):
                bad = dict(record)
                bad["collector_identity_source"] = weak
                self.assertIsNone(RecoveryContract.from_record(bad))
        removed = dict(record)
        del removed["collector_identity_source"]
        self.assertIsNone(RecoveryContract.from_record(removed))

    def test_short_pn_with_weak_source_never_binds_a_contract(self) -> None:
        # The classic trap: a 14-char heartbeat prefix. Neither the short PN
        # nor the weak source may produce a valid/pn_bound contract.
        record = _both_record()
        record["collector_pn"] = SHORT_PN
        record["collector_identity_source"] = "framed_heartbeat"
        self.assertIsNone(RecoveryContract.from_record(record))
        self.assertFalse(
            recovery_contract_diagnostics({RECOVERY_CONTRACT_KEY: record})[
                "recovery_contract_pn_bound"
            ]
        )

    def test_weak_or_missing_proof_identity_source_drops_only_that_branch(self) -> None:
        for weak in ("", "framed_heartbeat", "heartbeat", None):
            with self.subTest(source=weak):
                record = _both_record()
                record["inbound"] = dict(record["inbound"], identity_source=weak)
                parsed = RecoveryContract.from_record(record)
                self.assertIsNotNone(parsed)
                self.assertIsNone(parsed.inbound_proof)
                # The independently strong callback branch survives.
                self.assertTrue(parsed.callback_verified)

    def test_malformed_branch_is_dropped_not_fatal(self) -> None:
        record = _both_record()
        for broken in (None, "text", 3, ["x"], {}):
            with self.subTest(branch=broken):
                bad = dict(record)
                bad["callback"] = broken
                parsed = RecoveryContract.from_record(bad)
                self.assertIsNotNone(parsed)
                self.assertIsNone(parsed.callback_proof)
                self.assertTrue(parsed.inbound_verified)

    def test_unknown_proof_method_invalidates_only_that_branch(self) -> None:
        record = _both_record()
        record["inbound"] = dict(record["inbound"], method="wishful_thinking")
        parsed = RecoveryContract.from_record(record)
        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed.inbound_proof)
        self.assertTrue(parsed.callback_verified)

    def test_foreign_pn_branch_is_rejected_without_partial_application(self) -> None:
        # Even a STRONGLY identified foreign collector cannot lend its proof.
        record = _both_record()
        record["callback"] = dict(record["callback"], collector_pn=OTHER_FULL_PN)
        parsed = RecoveryContract.from_record(record)
        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed.callback_proof)
        self.assertTrue(parsed.inbound_verified)
        self.assertEqual(parsed.collector_pn, FULL_PN)

    def test_incomplete_callback_route_means_no_proof(self) -> None:
        for field, value in (
            ("trigger_target", ""),
            ("trigger_target", "   "),
            ("advertised_ha_endpoint", ""),
            ("listener_port", 0),
            ("listener_port", -1),
            ("listener_port", 65536),
            ("listener_port", "18899"),
            ("listener_port", True),
        ):
            with self.subTest(field=field, value=value):
                record = _both_record()
                record["callback"] = dict(record["callback"], **{field: value})
                parsed = RecoveryContract.from_record(record)
                self.assertIsNotNone(parsed)
                self.assertIsNone(parsed.callback_proof)
                self.assertTrue(parsed.inbound_verified)

    # --- timezone-aware timestamps ----------------------------------------------

    def test_proof_timestamp_must_be_timezone_aware(self) -> None:
        for bad_ts in (
            "",  # empty
            "  ",
            "yesterday",  # malformed
            "2026-13-77",
            "2026-07-16",  # date-only
            "2026-07-16T10:00:00",  # timezone-naive datetime
            1234,  # non-string
            None,
        ):
            with self.subTest(ts=bad_ts):
                record = _both_record()
                record["inbound"] = dict(record["inbound"], verified_at=bad_ts)
                parsed = RecoveryContract.from_record(record)
                self.assertIsNotNone(parsed)
                self.assertIsNone(parsed.inbound_proof)

    def test_aware_timestamps_with_offsets_are_accepted(self) -> None:
        for good_ts in ("2026-07-16T10:00:00+00:00", "2026-07-16T13:00:00+03:00"):
            with self.subTest(ts=good_ts):
                record = _both_record()
                record["inbound"] = dict(record["inbound"], verified_at=good_ts)
                parsed = RecoveryContract.from_record(record)
                self.assertIsNotNone(parsed.inbound_proof)
                self.assertEqual(parsed.inbound_proof.verified_at, good_ts)

    def test_invalid_updated_at_is_cleared_while_proofs_survive(self) -> None:
        # Chosen and pinned: the proofs carry their own validated timestamps,
        # so a broken record mtime clears to "" instead of killing the record.
        for bad in ("not-a-time", "2026-07-16", "2026-07-16T10:00:00", 7, None):
            with self.subTest(updated_at=bad):
                record = _both_record()
                record["updated_at"] = bad
                parsed = RecoveryContract.from_record(record)
                self.assertIsNotNone(parsed)
                self.assertEqual(parsed.updated_at, "")
                self.assertTrue(parsed.inbound_verified)
                self.assertTrue(parsed.callback_verified)

    # --- identity spelling ------------------------------------------------------

    def test_short_contract_pn_is_enriched_by_full_proof_pn(self) -> None:
        # Reconciliation between STRONGLY identified records only: both sides
        # carry strong sources here.
        record = _both_record()
        record["collector_pn"] = SHORT_PN
        parsed = RecoveryContract.from_record(record)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.collector_pn, FULL_PN)
        self.assertEqual(parsed.inbound_proof.collector_pn, FULL_PN)

    def test_short_proof_pn_never_downgrades_full_contract_pn(self) -> None:
        record = _both_record()
        record["inbound"] = dict(record["inbound"], collector_pn=SHORT_PN)
        parsed = RecoveryContract.from_record(record)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.collector_pn, FULL_PN)
        self.assertEqual(parsed.inbound_proof.collector_pn, FULL_PN)

    def test_builder_enriches_short_to_full_of_same_identity(self) -> None:
        contract = _empty(SHORT_PN).with_inbound_proof(
            _inbound(FULL_PN), updated_at=TS
        )
        self.assertEqual(contract.collector_pn, FULL_PN)

    def test_builder_rejects_foreign_pn_without_partial_application(self) -> None:
        contract = _empty()
        with self.assertRaises(ValueError):
            contract.with_inbound_proof(_inbound(OTHER_FULL_PN), updated_at=TS)
        with self.assertRaises(ValueError):
            contract.with_callback_proof(_callback(OTHER_FULL_PN), updated_at=TS)
        self.assertTrue(contract.is_empty)  # untouched

    # --- immutability / update helpers -------------------------------------------

    def test_contract_and_proofs_are_immutable(self) -> None:
        contract = _empty()
        with self.assertRaises(Exception):
            contract.collector_pn = OTHER_FULL_PN  # type: ignore[misc]
        proof = _inbound()
        with self.assertRaises(Exception):
            proof.verified_at = TS_LATER  # type: ignore[misc]

    def test_update_helpers_return_new_objects_and_preserve_the_other_branch(self) -> None:
        base = _empty().with_inbound_proof(_inbound(), updated_at=TS)
        with_both = base.with_callback_proof(_callback(), updated_at=TS_LATER)
        self.assertIsNot(with_both, base)
        self.assertIsNone(base.callback_proof)  # original unchanged
        self.assertTrue(with_both.inbound_verified)

        dropped = with_both.without_inbound_proof(updated_at=TS_LATER)
        self.assertIsNone(dropped.inbound_proof)
        self.assertTrue(dropped.callback_verified)
        self.assertTrue(with_both.inbound_verified)  # original unchanged

        cleared = dropped.without_callback_proof(updated_at=TS_LATER)
        self.assertTrue(cleared.is_empty)
        self.assertEqual(cleared.collector_pn, FULL_PN)

    def test_update_helpers_require_a_valid_aware_updated_at(self) -> None:
        contract = _empty().with_inbound_proof(_inbound(), updated_at=TS)
        for bad in ("", "2026-07-16", "2026-07-16T10:00:00", "later"):
            with self.subTest(updated_at=bad):
                with self.assertRaises(ValueError):
                    contract.with_callback_proof(_callback(), updated_at=bad)
                with self.assertRaises(ValueError):
                    contract.without_inbound_proof(updated_at=bad)
                with self.assertRaises(ValueError):
                    contract.without_callback_proof(updated_at=bad)

    # --- ducks -------------------------------------------------------------------

    def test_identity_outcome_is_never_accepted_as_a_proof_duck(self) -> None:
        from custom_components.eybond_local.onboarding.callback_identity import (
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
        contract = _empty()
        with self.assertRaises(TypeError):
            contract.with_inbound_proof(outcome, updated_at=TS)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            contract.with_callback_proof(outcome, updated_at=TS)  # type: ignore[arg-type]

        # And an attribute-compatible duck is refused just the same: proof
        # objects must BE the typed proof classes, not merely look like them.
        class _Duck:
            method = INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER
            collector_pn = FULL_PN
            identity_source = STRONG_FC2
            verified_at = TS
            session_protocol = ""

        with self.assertRaises(TypeError):
            contract.with_inbound_proof(_Duck(), updated_at=TS)  # type: ignore[arg-type]

    def test_builder_rejects_invalid_method_source_timestamp_and_partial_route(self) -> None:
        contract = _empty()
        with self.assertRaises(ValueError):
            contract.with_inbound_proof(_inbound(method="not_a_method"), updated_at=TS)
        with self.assertRaises(ValueError):
            contract.with_inbound_proof(
                _inbound(identity_source="framed_heartbeat"), updated_at=TS
            )
        with self.assertRaises(ValueError):
            contract.with_inbound_proof(_inbound(verified_at="nope"), updated_at=TS)
        with self.assertRaises(ValueError):
            contract.with_inbound_proof(
                _inbound(verified_at="2026-07-16T10:00:00"), updated_at=TS  # naive
            )
        with self.assertRaises(ValueError):
            contract.with_callback_proof(_callback(trigger_target=""), updated_at=TS)
        with self.assertRaises(ValueError):
            contract.with_callback_proof(_callback(listener_port=0), updated_at=TS)
        with self.assertRaises(ValueError):
            contract.with_callback_proof(
                _callback(identity_source=""), updated_at=TS
            )


class DirectConstructorValidationTests(unittest.TestCase):
    """The direct constructor is as strict as the builders.

    An object that exists is an object ``to_record()`` may serialize: nothing
    invalid can be constructed, so nothing invalid can be persisted.
    """

    def _kwargs(self, **overrides):
        kwargs = dict(
            schema_version=RECOVERY_CONTRACT_SCHEMA_VERSION,
            collector_pn=FULL_PN,
            collector_identity_source=STRONG_FC2,
            inbound_proof=None,
            callback_proof=None,
            updated_at="",
        )
        kwargs.update(overrides)
        return kwargs

    def test_valid_direct_construction_serializes(self) -> None:
        contract = RecoveryContract(**self._kwargs(inbound_proof=_inbound()))
        record = contract.to_record()
        self.assertEqual(record["collector_identity_source"], STRONG_FC2)
        self.assertEqual(RecoveryContract.from_record(record).to_record(), record)

    def test_unknown_schema_version_is_unconstructible(self) -> None:
        for version in (0, 2, -1, True):
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    RecoveryContract(**self._kwargs(schema_version=version))

    def test_float_schema_version_is_unconstructible(self) -> None:
        # 1.0 == 1, so an equality-only check would construct an object whose
        # to_record() the parser then refuses. type() is int closes it.
        for version in (1.0, 1.5, "1"):
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    RecoveryContract(**self._kwargs(schema_version=version))

    def test_duck_identity_source_is_unconstructible(self) -> None:
        # identity_source_is_strong stringifies its input, so an object whose
        # __str__ mimics a strong source would pass a value-only check and put
        # a non-JSON-safe object into to_record().
        class _StrongLooking:
            def __str__(self) -> str:
                return STRONG_FC2

        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(collector_identity_source=_StrongLooking())
            )
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(
                    inbound_proof=_inbound(identity_source=_StrongLooking())
                )
            )

    def test_non_string_proof_fields_are_unconstructible(self) -> None:
        # Every serialized proof string must be a strict str: an arbitrary
        # object smuggled through an unvalidated field (session_protocol was
        # the hole) must never reach to_record().
        for bad in (object(), 5, b"eybond_framed", None):
            with self.subTest(session_protocol=bad):
                with self.assertRaises(ValueError):
                    RecoveryContract(
                        **self._kwargs(inbound_proof=_inbound(session_protocol=bad))
                    )
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(callback_proof=_callback(trigger_target=object()))
            )
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(callback_proof=_callback(advertised=12345))
            )

    def test_whitespace_padded_fields_are_unconstructible(self) -> None:
        # The parser would strip these on read: holding them would break the
        # serialize -> parse -> serialize byte-equality invariant.
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(collector_identity_source=" " + STRONG_FC2)
            )
        with self.assertRaises(ValueError):
            RecoveryContract(**self._kwargs(updated_at=" " + TS + " "))
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(inbound_proof=_inbound(verified_at=" " + TS))
            )
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(
                    callback_proof=_callback(trigger_target=" " + TRIGGER_TARGET)
                )
            )

    def test_constructible_implies_json_safe_and_byte_stable(self) -> None:
        # THE invariant: for ANY successfully constructed contract,
        # json.dumps(to_record()) succeeds and parse -> serialize is
        # byte-identical.
        import json

        shapes = (
            RecoveryContract(**self._kwargs()),
            RecoveryContract(**self._kwargs(inbound_proof=_inbound())),
            RecoveryContract(**self._kwargs(callback_proof=_callback(), updated_at=TS)),
            RecoveryContract(
                **self._kwargs(
                    inbound_proof=_inbound(),
                    callback_proof=_callback(),
                    updated_at=TS_LATER,
                )
            ),
            _empty(identity_source=STRONG_DTUPN),
            _empty()
            .with_inbound_proof(_inbound(), updated_at=TS)
            .with_callback_proof(_callback(), updated_at=TS_LATER),
        )
        for contract in shapes:
            with self.subTest(contract=contract):
                record = contract.to_record()
                json.dumps(record)  # must not raise
                reparsed = RecoveryContract.from_record(record)
                self.assertIsNotNone(reparsed)
                self.assertEqual(reparsed.to_record(), record)

    def test_empty_or_unnormalized_pn_is_unconstructible(self) -> None:
        for pn in ("", "  ", "  " + FULL_PN):
            with self.subTest(pn=pn):
                with self.assertRaises(ValueError):
                    RecoveryContract(**self._kwargs(collector_pn=pn))

    def test_weak_identity_source_is_unconstructible(self) -> None:
        for weak in ("", "framed_heartbeat", "heartbeat", "manual"):
            with self.subTest(source=weak):
                with self.assertRaises(ValueError):
                    RecoveryContract(**self._kwargs(collector_identity_source=weak))

    def test_foreign_or_malformed_proof_is_unconstructible(self) -> None:
        with self.assertRaises(ValueError):
            RecoveryContract(**self._kwargs(inbound_proof=_inbound(OTHER_FULL_PN)))
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(inbound_proof=_inbound(verified_at="2026-07-16"))
            )
        with self.assertRaises(ValueError):
            RecoveryContract(
                **self._kwargs(
                    inbound_proof=_inbound(identity_source="framed_heartbeat")
                )
            )
        with self.assertRaises(ValueError):
            RecoveryContract(**self._kwargs(callback_proof=_callback(trigger_target="")))

    def test_duck_proof_is_unconstructible(self) -> None:
        class _Duck:
            method = INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER
            collector_pn = FULL_PN
            identity_source = STRONG_FC2
            verified_at = TS
            session_protocol = ""

        with self.assertRaises(TypeError):
            RecoveryContract(**self._kwargs(inbound_proof=_Duck()))

    def test_invalid_updated_at_is_unconstructible(self) -> None:
        for bad in ("2026-07-16", "2026-07-16T10:00:00", "soon", 5, None):
            with self.subTest(updated_at=bad):
                with self.assertRaises(ValueError):
                    RecoveryContract(**self._kwargs(updated_at=bad))


class NoLegacyProofArchitectureTests(unittest.TestCase):
    """<=v4 evidence has no timestamp and no strong source: no legacy method exists."""

    def test_inbound_methods_contain_no_legacy_entry(self) -> None:
        self.assertEqual(
            INBOUND_RECOVERY_METHODS,
            frozenset({INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER}),
        )

    def test_module_exports_no_legacy_helper_or_method(self) -> None:
        import custom_components.eybond_local.connection.recovery_contract as module

        for gone in (
            "legacy_evidence_recovery_record",
            "INBOUND_RECOVERY_LEGACY_REBOOT_RECONNECT",
        ):
            self.assertFalse(hasattr(module, gone), msg=f"{gone} must not exist")
            self.assertNotIn(gone, module.__all__)

    def test_legacy_method_value_is_rejected_by_the_parser(self) -> None:
        # Even a hand-crafted record claiming the old value is a foreign shape.
        record = _both_record()
        record["inbound"] = dict(record["inbound"], method="legacy_reboot_reconnect")
        parsed = RecoveryContract.from_record(record)
        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed.inbound_proof)


class RecoveryDiagnosticsTests(unittest.TestCase):
    _ALLOWED_KEYS = {
        "recovery_contract_version",
        "recovery_contract_valid",
        "recovery_contract_identity_strong",
        "recovery_contract_pn_bound",
        "inbound_recovery_verified",
        "inbound_recovery_method",
        "inbound_recovery_verified_at",
        "callback_recovery_verified",
        "callback_recovery_method",
        "callback_recovery_verified_at",
        "callback_route_bound",
        "advertised_endpoint_bound",
    }

    def test_absent_or_malformed_contract_reports_invalid(self) -> None:
        for data in (None, {}, {RECOVERY_CONTRACT_KEY: "garbage"}, {RECOVERY_CONTRACT_KEY: {"schema_version": 99}}):
            with self.subTest(data=data):
                diagnostics = recovery_contract_diagnostics(data)
                self.assertEqual(set(diagnostics), self._ALLOWED_KEYS)
                self.assertFalse(diagnostics["recovery_contract_valid"])
                self.assertFalse(diagnostics["recovery_contract_identity_strong"])
                self.assertFalse(diagnostics["inbound_recovery_verified"])
                self.assertFalse(diagnostics["callback_recovery_verified"])
                self.assertFalse(diagnostics["callback_route_bound"])

    def test_full_contract_reports_structure_only(self) -> None:
        diagnostics = recovery_contract_diagnostics(
            {RECOVERY_CONTRACT_KEY: _both_record()}
        )
        self.assertEqual(set(diagnostics), self._ALLOWED_KEYS)
        self.assertTrue(diagnostics["recovery_contract_valid"])
        self.assertTrue(diagnostics["recovery_contract_identity_strong"])
        self.assertTrue(diagnostics["recovery_contract_pn_bound"])
        self.assertTrue(diagnostics["inbound_recovery_verified"])
        self.assertEqual(
            diagnostics["inbound_recovery_method"],
            INBOUND_RECOVERY_REBOOT_RECONNECT_NO_TRIGGER,
        )
        self.assertEqual(diagnostics["inbound_recovery_verified_at"], TS)
        self.assertTrue(diagnostics["callback_recovery_verified"])
        self.assertEqual(
            diagnostics["callback_recovery_method"],
            CALLBACK_RECOVERY_RESET_UNICAST_RECONNECT,
        )
        self.assertTrue(diagnostics["callback_route_bound"])
        self.assertTrue(diagnostics["advertised_endpoint_bound"])

    def test_no_raw_network_or_identity_values_leak(self) -> None:
        diagnostics = recovery_contract_diagnostics(
            {RECOVERY_CONTRACT_KEY: _both_record()}
        )
        serialized = repr(sorted(diagnostics.items()))
        for secret in (TRIGGER_TARGET, ADVERTISED, "203.0.113.55", "192.0.2.10"):
            self.assertNotIn(secret, serialized)
        # Neither the PN nor the raw identity-source values are exposed --
        # only booleans/methods/timestamps.
        self.assertNotIn(FULL_PN, serialized)
        self.assertNotIn(STRONG_FC2, serialized)
        self.assertNotIn(STRONG_DTUPN, serialized)

    def test_entry_axis_diagnostics_carries_recovery_structure(self) -> None:
        from custom_components.eybond_local.connection.connection_policy import (
            entry_axis_diagnostics,
        )

        data = {
            "connection_strategy": "callback_on_demand",
            "collector_pn": FULL_PN,
            RECOVERY_CONTRACT_KEY: _both_record(),
        }
        axes = entry_axis_diagnostics(data, {})
        self.assertTrue(axes["recovery_contract_valid"])
        self.assertTrue(axes["recovery_contract_identity_strong"])
        self.assertTrue(axes["callback_recovery_verified"])
        serialized = repr(sorted(axes.items()))
        self.assertNotIn(TRIGGER_TARGET, serialized)
        self.assertNotIn(ADVERTISED, serialized)


# --- architectural guards ---------------------------------------------------------

_MODULE_PATH = (
    REPO_ROOT
    / "custom_components"
    / "eybond_local"
    / "connection"
    / "recovery_contract.py"
)
_PACKAGE_ROOT = REPO_ROOT / "custom_components" / "eybond_local"


def _module_source() -> str:
    return _MODULE_PATH.read_text(encoding="utf-8")


def _identifiers(source: str) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.keyword) and node.arg:
            names.add(node.arg)
    return names


def _imports(source: str) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.update(alias.name.split("."))
        elif isinstance(node, ast.ImportFrom):
            modules.update((node.module or "").split("."))
            modules.update(alias.name for alias in node.names)
    return {part for part in modules if part}


def _string_literals(source: str) -> set[str]:
    tree = ast.parse(source)
    docstrings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc:
                docstrings.add(doc)
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value not in docstrings
    }


class RecoveryContractArchitectureGuardTests(unittest.TestCase):
    """The contract stays pure, underivable, and single-stored."""

    def test_module_imports_are_a_closed_allowlist(self) -> None:
        # Stdlib + the pure identity helpers. Anything else -- Home Assistant,
        # config_flow, coordinator, runtime, transport, collector, onboarding,
        # drivers, providers, support, even const -- is structural coupling the
        # model must never grow.
        allowed = {
            # stdlib
            "annotations", "__future__", "collections", "abc", "Mapping",
            "dataclasses", "dataclass", "datetime",
            # package-pure identity helpers (single reconciliation authority)
            "session_registry",
            "identity_source_is_strong",
            "normalize_pn", "pn_is_same_identity", "prefer_full_pn",
        }
        self.assertLessEqual(
            _imports(_module_source()),
            allowed,
            msg="recovery_contract grew an import outside its purity allowlist",
        )

    def test_module_never_names_forbidden_sources(self) -> None:
        source = _module_source()
        code = _identifiers(source) | _imports(source)
        literals = _string_literals(source)
        for banned in (
            # HA / heavy layers
            "homeassistant", "config_flow", "coordinator", "runtime",
            "transport", "drivers", "providers", "onboarding",
            # never-a-proof sources
            "CallbackIdentityOutcome", "identity_certified",
            "CONF_CONNECTION_STRATEGY", "CONF_ENDPOINT_CONTROL_POLICY",
            "CONF_CONNECTION_STRATEGY_EVIDENCE",
            "CONNECTION_STRATEGY_EVIDENCE_CALLBACK_TRIGGER",
            "CONNECTION_STRATEGY_EVIDENCE_REBOOT_RECONNECT",
            "CONNECTION_STRATEGY_EVIDENCE_USER_CONFIRMED_SESSION",
            # never-consulted classifications
            "peer_ip", "hostname", "collector_kind", "cloud_family",
            "session_id",
            # never a second store
            "options",
            # the dead legacy architecture must not come back
            "legacy_reboot_reconnect", "legacy_evidence_recovery_record",
        ):
            self.assertNotIn(banned, code, msg=f"banned identifier: {banned}")
            self.assertNotIn(banned, literals, msg=f"banned literal: {banned}")

    def test_single_canonical_store(self) -> None:
        # The exact persisted key literal exists in ONE production module: the
        # model itself. Everyone else goes through its API, so a second
        # recovery store (options, registry, loose fields) cannot appear
        # silently.
        offenders: list[str] = []
        for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
            if path.name == "recovery_contract.py":
                continue
            if RECOVERY_CONTRACT_KEY in _string_literals(
                path.read_text(encoding="utf-8")
            ):
                offenders.append(str(path.relative_to(_PACKAGE_ROOT)))
        self.assertEqual(offenders, [], msg="second recovery store detected")

    def test_collector_registry_gets_no_recovery_fields(self) -> None:
        source = (_PACKAGE_ROOT / "support" / "collector_registry.py").read_text(
            encoding="utf-8"
        )
        code = _identifiers(source) | _imports(source) | _string_literals(source)
        for banned in ("recovery_contract", "RecoveryContract", "recovery"):
            self.assertNotIn(
                banned,
                code,
                msg="collector_registry must keep original-endpoint duty only",
            )

    def test_no_production_writer_of_the_key_outside_the_model(self) -> None:
        # Defense in depth for the same invariant: nobody subscript-assigns
        # data["recovery_contract"] anywhere (the literal ban above already
        # implies it; this documents the intent).
        for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
            if path.name == "recovery_contract.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Assign):
                    continue
                for target in node.targets:
                    if (
                        isinstance(target, ast.Subscript)
                        and isinstance(target.slice, ast.Constant)
                        and target.slice.value == RECOVERY_CONTRACT_KEY
                    ):
                        self.fail(f"{path.name} writes the recovery store directly")


if __name__ == "__main__":
    unittest.main()
