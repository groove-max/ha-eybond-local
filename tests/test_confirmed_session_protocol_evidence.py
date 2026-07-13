"""Atomic, validated confirmed-session-protocol evidence value object.

The confirmed collector wire is persisted as four related fields read as ONE
mapping (protocol, durable PN, provenance source, observed-at). The value object
can only be constructed through its validator, so downstream code never has to
trust a "caller validated" comment. These tests lock the fail-closed rules:
only a COMPLETE ``live_session`` record for the same durable PN produces
evidence, a record is never assembled from a MIX of ``data`` and ``options``, and
data/options precedence is at the whole-record level.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.confirmed_session_protocol import (
    ConfirmedSessionProtocolEvidence,
)
from custom_components.eybond_local.const import (
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
)


FULL_PN = "PNALPHA-FULL-0001"
SHORT_PN = "PNALPHA-FU"  # a 10-char prefix of FULL_PN
OTHER_FULL_PN = "PNBETA-FULL-0002"


def _record(
    *,
    protocol="eybond_framed",
    pn=FULL_PN,
    source="live_session",
    observed_at="2026-07-13T00:00:00+00:00",
):
    record = {}
    if protocol is not None:
        record[CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL] = protocol
    if pn is not None:
        record[CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN] = pn
    if source is not None:
        record[CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE] = source
    if observed_at is not None:
        record[CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT] = observed_at
    return record


class ConfirmedSessionProtocolEvidenceTests(unittest.TestCase):
    def test_complete_live_record_from_one_mapping_validates(self) -> None:
        evidence = ConfirmedSessionProtocolEvidence.from_record(
            _record(), entry_pn=FULL_PN
        )
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.protocol, "eybond_framed")
        self.assertEqual(evidence.collector_pn, FULL_PN)
        self.assertEqual(evidence.source, "live_session")
        self.assertEqual(evidence.observed_at, "2026-07-13T00:00:00+00:00")

    def test_at_text_is_a_valid_confirmed_wire(self) -> None:
        evidence = ConfirmedSessionProtocolEvidence.from_record(
            _record(protocol="at_text"), entry_pn=FULL_PN
        )
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.protocol, "at_text")

    def test_non_live_source_is_rejected(self) -> None:
        for source in ("", "cloud_family", "inferred", "unknown", "persisted"):
            self.assertIsNone(
                ConfirmedSessionProtocolEvidence.from_record(
                    _record(source=source), entry_pn=FULL_PN
                ),
                msg=f"source={source!r}",
            )

    def test_unknown_protocol_is_rejected(self) -> None:
        for protocol in ("", "smartess_at", "framed", "at", "pi30", "modbus"):
            self.assertIsNone(
                ConfirmedSessionProtocolEvidence.from_record(
                    _record(protocol=protocol), entry_pn=FULL_PN
                ),
                msg=f"protocol={protocol!r}",
            )

    def test_absent_confirmed_pn_is_rejected(self) -> None:
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_record(
                _record(pn=""), entry_pn=FULL_PN
            )
        )

    def test_absent_entry_pn_is_rejected(self) -> None:
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_record(_record(), entry_pn="")
        )

    def test_mismatched_pn_is_fail_closed(self) -> None:
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_record(
                _record(pn=OTHER_FULL_PN), entry_pn=FULL_PN
            )
        )

    def test_short_and_full_pn_reconcile_to_the_fuller_identity(self) -> None:
        # Entry full, confirmed short -> keep the full PN.
        e1 = ConfirmedSessionProtocolEvidence.from_record(
            _record(pn=SHORT_PN), entry_pn=FULL_PN
        )
        self.assertIsNotNone(e1)
        self.assertEqual(e1.collector_pn, FULL_PN)
        # Entry short, confirmed full -> upgrade to the full PN.
        e2 = ConfirmedSessionProtocolEvidence.from_record(
            _record(pn=FULL_PN), entry_pn=SHORT_PN
        )
        self.assertIsNotNone(e2)
        self.assertEqual(e2.collector_pn, FULL_PN)

    def test_none_and_non_mapping_records_are_rejected(self) -> None:
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_record(None, entry_pn=FULL_PN)
        )
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_record(
                "not-a-mapping", entry_pn=FULL_PN  # type: ignore[arg-type]
            )
        )

    def test_missing_observed_at_defaults_empty(self) -> None:
        evidence = ConfirmedSessionProtocolEvidence.from_record(
            _record(observed_at=None), entry_pn=FULL_PN
        )
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.observed_at, "")

    def test_evidence_is_immutable(self) -> None:
        evidence = ConfirmedSessionProtocolEvidence.from_record(
            _record(), entry_pn=FULL_PN
        )
        with self.assertRaises(Exception):
            evidence.protocol = "at_text"  # type: ignore[misc]


class ConfirmedEvidenceWholeRecordPrecedenceTests(unittest.TestCase):
    def test_options_complete_record_wins_over_data(self) -> None:
        # options has a complete live framed record; data has a complete live AT
        # record. Whole-record precedence: options wins as a UNIT.
        options = _record(protocol="eybond_framed")
        data = _record(protocol="at_text")
        evidence = ConfirmedSessionProtocolEvidence.from_entry(
            data, options, entry_pn=FULL_PN
        )
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.protocol, "eybond_framed")

    def test_falls_back_to_data_when_options_incomplete(self) -> None:
        # options is missing the source field -> not a complete record -> the
        # complete data record is used instead (never merged).
        options = _record(protocol="eybond_framed", source=None)
        data = _record(protocol="at_text")
        evidence = ConfirmedSessionProtocolEvidence.from_entry(
            data, options, entry_pn=FULL_PN
        )
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.protocol, "at_text")

    def test_partial_options_and_partial_data_are_never_merged(self) -> None:
        # options carries ONLY the live source; data carries ONLY the protocol +
        # PN. Neither is a complete record on its own. A correct atomic reader
        # must NOT stitch the live source from options onto the protocol from
        # data -- that cross-mapping mix is exactly the bug the value object
        # prevents. Result: no evidence.
        options = {
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: "live_session",
        }
        data = {
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: "eybond_framed",
            CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: FULL_PN,
        }
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_entry(
                data, options, entry_pn=FULL_PN
            )
        )

    def test_both_mappings_empty_yields_no_evidence(self) -> None:
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.from_entry({}, {}, entry_pn=FULL_PN)
        )


class ConfirmedEvidenceCoerceTrustBoundaryTests(unittest.TestCase):
    """`coerce` is the fail-closed trust boundary the seed path relies on.

    The type alone is NOT proof of validity: the raw frozen-dataclass constructor
    can forge an instance with any source/protocol/PN. `coerce` rejects non-
    instances and re-validates every provenance invariant against the entry PN.
    """

    def test_duck_typed_namespace_is_rejected(self) -> None:
        forged = SimpleNamespace(
            protocol="eybond_framed", collector_pn=FULL_PN, source="live_session"
        )
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.coerce(forged, entry_pn=FULL_PN)
        )

    def test_forged_instance_with_cloud_family_source_is_rejected(self) -> None:
        # A genuine instance, but built via the raw constructor with a bad source.
        forged = ConfirmedSessionProtocolEvidence(
            protocol="eybond_framed", collector_pn=FULL_PN, source="cloud_family"
        )
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.coerce(forged, entry_pn=FULL_PN)
        )

    def test_forged_instance_with_unknown_protocol_is_rejected(self) -> None:
        forged = ConfirmedSessionProtocolEvidence(
            protocol="pi30", collector_pn=FULL_PN, source="live_session"
        )
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.coerce(forged, entry_pn=FULL_PN)
        )

    def test_forged_instance_with_empty_pn_is_rejected(self) -> None:
        forged = ConfirmedSessionProtocolEvidence(
            protocol="eybond_framed", collector_pn="", source="live_session"
        )
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.coerce(forged, entry_pn=FULL_PN)
        )

    def test_forged_instance_with_foreign_pn_is_rejected(self) -> None:
        forged = ConfirmedSessionProtocolEvidence(
            protocol="eybond_framed", collector_pn=OTHER_FULL_PN, source="live_session"
        )
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.coerce(forged, entry_pn=FULL_PN)
        )

    def test_none_is_rejected(self) -> None:
        self.assertIsNone(
            ConfirmedSessionProtocolEvidence.coerce(None, entry_pn=FULL_PN)
        )

    def test_valid_instance_survives_and_reconciles_pn(self) -> None:
        valid = ConfirmedSessionProtocolEvidence.from_record(
            _record(pn=SHORT_PN), entry_pn=FULL_PN
        )
        coerced = ConfirmedSessionProtocolEvidence.coerce(valid, entry_pn=FULL_PN)
        self.assertIsNotNone(coerced)
        self.assertEqual(coerced.protocol, "eybond_framed")
        self.assertEqual(coerced.source, "live_session")
        # Short/full reconciliation preserved through the trust boundary.
        self.assertEqual(coerced.collector_pn, FULL_PN)

    def test_coerce_never_raises_on_untrusted_input(self) -> None:
        for candidate in (None, "str", 123, object(), {"protocol": "eybond_framed"}):
            # No exception -- untrusted input is None, not a raised error.
            self.assertIsNone(
                ConfirmedSessionProtocolEvidence.coerce(candidate, entry_pn=FULL_PN)
            )


if __name__ == "__main__":
    unittest.main()
