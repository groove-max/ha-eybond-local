"""The typed persisted strategy-transition recovery-state model (Batch 8A).

Strict trust boundary: exact-type direct constructor (no bool/subclass/coercion),
fail-closed non-raising parser, normalized PN, sane ports, aware byte-stable
timestamps, opaque route fields, JSON-safe byte-stable roundtrip, privacy-aware
diagnostics, and NO decorative/sensitive fields kept "for the future".
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    normalize_pn,
)
from custom_components.eybond_local.connection.strategy_transition_recovery import (  # noqa: E402
    RECOVERY_KIND_CALLBACK_TRANSITION_UNPROVEN,
    RECOVERY_PHASE_PENDING,
    RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    StrategyTransitionRecoveryState,
)

FULL_PN = normalize_pn("V001020SYN62344022")
TS = "2026-07-17T10:00:00+00:00"


def _state(**overrides):
    base = dict(
        collector_pn=FULL_PN,
        now=TS,
        trigger_target_host="192.168.88.72",
        trigger_udp_port=58899,
        advertised_host="public.example",
        advertised_port=18899,
        trigger_bind_host="127.0.0.1",
        listener_bind_host="127.0.0.1",
        local_listener_port=8899,
    )
    base.update(overrides)
    return StrategyTransitionRecoveryState.create(**base)


class RecoveryStateStrictConstructorTests(unittest.TestCase):
    """Blocker 1: the direct constructor is a strict, no-coercion boundary."""

    def test_schema_version_true_float_string_rejected(self) -> None:
        rec = _state().to_record()
        for bad in ("1", 1.0, True, "01", None, [1]):
            with self.subTest(bad=bad):
                self.assertIsNone(
                    StrategyTransitionRecoveryState.from_record(
                        {**rec, "schema_version": bad}
                    )
                )
        self.assertIsNotNone(
            StrategyTransitionRecoveryState.from_record({**rec, "schema_version": 1})
        )

    def test_padded_strings_rejected(self) -> None:
        rec = _state().to_record()
        for field in (
            "collector_pn",
            "created_at",
            "trigger_target_host",
            "advertised_host",
            "trigger_bind_host",
            "listener_bind_host",
            "kind",
        ):
            with self.subTest(field=field):
                self.assertIsNone(
                    StrategyTransitionRecoveryState.from_record(
                        {**rec, field: f" {rec[field]} "}
                    )
                )

    def test_str_subclass_and_duck_rejected(self) -> None:
        class _StrSub(str):
            pass

        class _Duck:
            def __str__(self) -> str:
                return "public.example"

        rec = _state().to_record()
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "advertised_host": _StrSub("public.example")}
            )
        )
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "trigger_target_host": _Duck()}
            )
        )

    def test_non_string_host_endpoint_timestamp_pn_rejected(self) -> None:
        rec = _state().to_record()
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "trigger_target_host": 123}
            )
        )
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "advertised_host": b"public.example"}
            )
        )
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "created_at": 1_752_744_000}
            )
        )
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "collector_pn": object()}
            )
        )

    def test_bool_port_and_out_of_range_rejected(self) -> None:
        rec = _state().to_record()
        for field in ("trigger_udp_port", "advertised_port", "local_listener_port"):
            for bad in (True, 0, -1, 65536, 8899.0, "8899"):
                with self.subTest(field=field, bad=bad):
                    self.assertIsNone(
                        StrategyTransitionRecoveryState.from_record(
                            {**rec, field: bad}
                        )
                    )

    def test_naive_timestamp_and_non_normalized_pn_rejected(self) -> None:
        rec = _state().to_record()
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "created_at": "2026-07-17T10:00:00"}
            )
        )
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "collector_pn": " " + rec["collector_pn"]}
            )
        )

    def test_kind_and_target_locked(self) -> None:
        rec = _state().to_record()
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record({**rec, "kind": "other"})
        )
        self.assertIsNone(
            StrategyTransitionRecoveryState.from_record(
                {**rec, "target_strategy": "inbound"}
            )
        )


class RecoveryStateDirectConstructorTests(unittest.TestCase):
    """Blocker 1: the LITERAL direct constructor is a strict exact-type boundary.

    These call ``StrategyTransitionRecoveryState(**record)`` directly (not via
    ``from_record``) so the constructor -- not only the parser -- is the wall.
    """

    def _kw(self, **overrides):
        rec = _state().to_record()
        rec.update(overrides)
        return rec

    def test_empty_created_or_updated_rejected(self) -> None:
        for field in ("created_at", "updated_at"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    StrategyTransitionRecoveryState(**self._kw(**{field: ""}))

    def test_naive_and_non_string_timestamp_rejected(self) -> None:
        with self.assertRaises(ValueError):
            StrategyTransitionRecoveryState(
                **self._kw(created_at="2026-07-17T10:00:00")
            )
        with self.assertRaises(ValueError):
            StrategyTransitionRecoveryState(**self._kw(updated_at=1_752_744_000))

    def test_bool_float_string_ports_rejected(self) -> None:
        for field in ("trigger_udp_port", "advertised_port", "local_listener_port"):
            for bad in (True, 1.0, "18899"):
                with self.subTest(field=field, bad=bad):
                    with self.assertRaises(ValueError):
                        StrategyTransitionRecoveryState(**self._kw(**{field: bad}))

    def test_object_bytes_str_subclass_hosts_rejected(self) -> None:
        class _StrSub(str):
            pass

        for bad in (object(), b"host", _StrSub("host")):
            with self.subTest(bad=type(bad).__name__):
                with self.assertRaises(ValueError):
                    StrategyTransitionRecoveryState(
                        **self._kw(advertised_host=bad)
                    )

    def test_phase_and_valid_construction(self) -> None:
        from custom_components.eybond_local.connection.strategy_transition_recovery import (
            RECOVERY_PHASE_PENDING,
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        )

        # An invalid phase is rejected.
        with self.assertRaises(ValueError):
            StrategyTransitionRecoveryState(**self._kw(phase="bogus"))
        # Both valid phases construct.
        for phase in (
            RECOVERY_PHASE_PENDING,
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
        ):
            s = StrategyTransitionRecoveryState(**self._kw(phase=phase))
            self.assertEqual(s.phase, phase)

    def test_create_path_rejects_non_str_and_non_int(self) -> None:
        good = dict(
            collector_pn=FULL_PN,
            now=TS,
            trigger_target_host="192.168.88.72",
            trigger_udp_port=58899,
            advertised_host="public.example",
            advertised_port=18899,
            trigger_bind_host="127.0.0.1",
            listener_bind_host="127.0.0.1",
            local_listener_port=8899,
        )
        # Valid create path works.
        self.assertIsNotNone(StrategyTransitionRecoveryState.create(**good))
        # create() never str()/int()-coerces arbitrary objects.
        for bad in (
            {"collector_pn": object()},
            {"now": 12345},
            {"advertised_host": b"public.example"},
            {"trigger_udp_port": True},
            {"advertised_port": 1.0},
            {"local_listener_port": "8899"},
        ):
            with self.subTest(bad=bad):
                with self.assertRaises((TypeError, ValueError)):
                    StrategyTransitionRecoveryState.create(**{**good, **bad})


class RecoveryStateParserTests(unittest.TestCase):
    def test_parser_never_raises_and_is_fail_closed(self) -> None:
        for junk in (None, "garbage", 42, [], {}, {"kind": "x"}, b"{}", object()):
            self.assertIsNone(StrategyTransitionRecoveryState.from_record(junk))

    def test_missing_any_key_is_fail_closed(self) -> None:
        # ``to_record`` always writes all 12 keys, so a record missing ANY key
        # is malformed and fail-closes (from_record never fills defaults for an
        # absent persisted key -- that would be a semantic guess).
        rec = _state().to_record()
        for field in rec:
            partial = {k: v for k, v in rec.items() if k != field}
            with self.subTest(missing=field):
                self.assertIsNone(
                    StrategyTransitionRecoveryState.from_record(partial)
                )


class RecoveryStateRoundtripTests(unittest.TestCase):
    def test_constructible_is_json_safe_and_byte_stable(self) -> None:
        s = _state()
        rec = s.to_record()
        self.assertEqual(json.loads(json.dumps(rec)), rec)
        self.assertEqual(
            StrategyTransitionRecoveryState.from_record(rec).to_record(), rec
        )
        self.assertEqual(StrategyTransitionRecoveryState.from_record(rec), s)

    def test_removed_decorative_fields_absent(self) -> None:
        rec = _state().to_record()
        self.assertNotIn("restored_endpoint", rec)
        self.assertNotIn("identity_source", rec)
        # A stray persisted extra key does not leak into the model.
        parsed = StrategyTransitionRecoveryState.from_record(
            {**rec, "identity_source": "fc2_parameter_2"}
        )
        self.assertIsNotNone(parsed)
        self.assertNotIn("identity_source", parsed.to_record())


class RecoveryStateDiagnosticsTests(unittest.TestCase):
    def test_diagnostics_never_leak_raw_addresses(self) -> None:
        s = _state()
        blob = json.dumps(s.diagnostics())
        for secret in ("public.example", "192.168.88.72", "127.0.0.1"):
            self.assertNotIn(secret, blob)
        diag = s.diagnostics()
        self.assertEqual(diag["kind"], RECOVERY_KIND_CALLBACK_TRANSITION_UNPROVEN)
        self.assertTrue(diag["route_complete"])
        self.assertNotIn("advertised_port", diag)
        self.assertNotIn("identity_source_present", diag)
        with_ports = s.diagnostics(include_ports=True)
        self.assertEqual(with_ports["advertised_port"], 18899)
        self.assertNotIn("public.example", json.dumps(with_ports))

    def test_callback_route_matches_snapshot(self) -> None:
        route = _state().callback_route()
        self.assertEqual(route.advertised_ha_host, "public.example")
        self.assertEqual(route.advertised_ha_port, 18899)
        self.assertEqual(route.trigger_target_ip, "192.168.88.72")
        self.assertEqual(route.trigger_udp_port, 58899)
        self.assertEqual(route.bind_ip, "127.0.0.1")
        self.assertEqual(route.listener_port, 8899)

    def test_trigger_and_listener_bind_are_distinct(self) -> None:
        # Batch 8B.2A: the UDP trigger bind and the TCP listener bind are two
        # DIFFERENT transport concerns and must never be conflated.
        s = _state(trigger_bind_host="127.0.0.1", listener_bind_host="0.0.0.0")
        # callback_route().bind_ip is the UDP TRIGGER bind -- NOT the listener.
        self.assertEqual(s.callback_route().bind_ip, "127.0.0.1")
        self.assertEqual(s.trigger_bind_host, "127.0.0.1")
        self.assertEqual(s.listener_bind_host, "0.0.0.0")
        # Both survive the persistence roundtrip distinctly.
        rec = s.to_record()
        self.assertEqual(rec["trigger_bind_host"], "127.0.0.1")
        self.assertEqual(rec["listener_bind_host"], "0.0.0.0")
        parsed = StrategyTransitionRecoveryState.from_record(rec)
        self.assertEqual(parsed.trigger_bind_host, "127.0.0.1")
        self.assertEqual(parsed.listener_bind_host, "0.0.0.0")
        # Each is a REQUIRED persisted field: a missing one fails closed.
        for missing in ("trigger_bind_host", "listener_bind_host"):
            dropped = s.to_record()
            dropped.pop(missing)
            self.assertIsNone(
                StrategyTransitionRecoveryState.from_record(dropped)
            )
        # Neither raw bind host leaks into diagnostics.
        blob = json.dumps(s.diagnostics(include_ports=True))
        self.assertNotIn("0.0.0.0", blob)
        self.assertNotIn("127.0.0.1", blob)


TS2 = "2026-07-17T11:30:00+00:00"


class RecoveryStatePhasePersistenceTests(unittest.TestCase):
    """Batch 8A.1: ``phase`` is a first-class PERSISTED, load-bearing field."""

    def test_both_phases_roundtrip_is_load_bearing(self) -> None:
        # For EACH valid phase: state -> record -> JSON -> from_record must
        # preserve the EXACT phase (never silently default to pending).
        pending = _state()
        confirmed = pending.with_phase(
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=TS2
        )
        for original in (pending, confirmed):
            with self.subTest(phase=original.phase):
                record = original.to_record()
                # The phase is actually WRITTEN into the record...
                self.assertEqual(record["phase"], original.phase)
                # ...survives a JSON round-trip byte-for-byte...
                json_record = json.loads(json.dumps(record))
                parsed = StrategyTransitionRecoveryState.from_record(json_record)
                self.assertIsNotNone(parsed)
                # ...and re-parses to the SAME phase and the SAME record.
                self.assertEqual(parsed.phase, original.phase)
                self.assertEqual(parsed.to_record(), record)
        # The two phases are genuinely distinct end to end (no collapse).
        self.assertNotEqual(pending.to_record()["phase"], confirmed.to_record()["phase"])

    def test_confirmed_phase_survives_when_pending_default_would_hide_a_bug(
        self,
    ) -> None:
        # A regression guard: parsing a persisted CONFIRMED record must NOT come
        # back as the dataclass-default pending phase.
        confirmed = _state().with_phase(
            RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=TS2
        )
        parsed = StrategyTransitionRecoveryState.from_record(confirmed.to_record())
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.phase, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN)
        self.assertNotEqual(parsed.phase, RECOVERY_PHASE_PENDING)

    def test_missing_phase_fails_closed(self) -> None:
        # A persisted record with NO phase key must fail-close, never default.
        record = _state().to_record()
        record.pop("phase")
        self.assertIsNone(StrategyTransitionRecoveryState.from_record(record))

    def test_unknown_non_string_and_padded_phase_fail_closed(self) -> None:
        base = _state().to_record()
        for bad in ("bogus", "", " transition_pending ", 123, 1.0, True, None, ["x"]):
            with self.subTest(bad=bad):
                self.assertIsNone(
                    StrategyTransitionRecoveryState.from_record(
                        {**base, "phase": bad}
                    )
                )

    def test_with_phase_strict_now_raises_and_yields_no_object(self) -> None:
        s = _state()
        # Non-string now -> TypeError; empty/naive/padded/non-ISO -> ValueError.
        with self.assertRaises(TypeError):
            s.with_phase(RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=123)
        with self.assertRaises(TypeError):
            s.with_phase(RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=None)
        for bad in ("", "2026-07-17T10:00:00", " 2026-07-17T10:00:00+00:00 "):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    s.with_phase(
                        RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=bad
                    )
        # A valid now DOES advance updated_at (proves the strict path is live).
        moved = s.with_phase(RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN, now=TS2)
        self.assertEqual(moved.updated_at, TS2)
        self.assertEqual(moved.phase, RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN)

    def test_touched_strict_now_raises(self) -> None:
        s = _state()
        for bad in (123, None):
            with self.subTest(bad=bad):
                with self.assertRaises(TypeError):
                    s.touched(now=bad)
        for bad in ("", "2026-07-17T10:00:00"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    s.touched(now=bad)
        self.assertEqual(s.touched(now=TS2).updated_at, TS2)


if __name__ == "__main__":
    unittest.main()
