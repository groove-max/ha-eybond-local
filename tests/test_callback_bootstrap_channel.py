"""The public cold-bootstrap boundary's WIRE authority (Batch 8B.1, group C).

Proves that the exact-session identity read rides ONLY a wire chosen by the
two-source authority -- (A) the live negotiated SessionHandle of the exact
observed session, or (B) a validated PN-bound ConfirmedSessionProtocolEvidence
-- and that a conflicting / unobserved / hint-only / evidence-less session
performs ZERO identity IO. Cloud family, peer IP, hostname, an expected identity
and a bare persisted hint never select a wire.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.callback_bootstrap import (  # noqa: E402
    CallbackBootstrapChannel,
)
from custom_components.eybond_local.const import (  # noqa: E402
    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
)

FULL_PN = "V001020SYN62344022"
TS = "2026-07-17T10:00:00+00:00"


class _FakeProbe:
    """Records every exact-session read so IO / no-IO is observable."""

    def __init__(self, result: str = FULL_PN, *, silent=()) -> None:
        self.reads: list[tuple[str, str]] = []
        self._result = result
        self._silent = frozenset(silent)

    async def async_open(self) -> None:
        return None

    async def async_close(self) -> None:
        return None

    @property
    def available(self) -> bool:
        return True

    def snapshot_silent_session_ids(self) -> frozenset[str]:
        return self._silent

    async def async_identify_exact_session(self, session_id, *, session_protocol):
        self.reads.append((session_id, session_protocol))
        return self._result


class _Reg:
    def observed_sessions_per_socket(self):
        return ()


def _channel(*, entry_data=None, probe=None):
    channel = CallbackBootstrapChannel(
        registry=_Reg(),
        host="127.0.0.1",
        port=8899,
        entry_data=entry_data or {},
        entry_options={},
        entry_pn=FULL_PN,
    )
    channel._probe = probe if probe is not None else _FakeProbe()
    return channel


def _session(**raw):
    return {"session_id": "sock-1", "raw": raw}


_CONFIRMED_AT = {
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: "at_text",
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: FULL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: (
        COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE
    ),
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT: TS,
}


class WireAuthorityResolutionTests(unittest.TestCase):
    def test_observed_framed_and_at_resolve_from_the_handle(self) -> None:
        ch = _channel()
        self.assertEqual(
            ch.resolve_wire(_session(state="routed_framed", protocol_shape="eybond_framed")),
            "eybond_framed",
        )
        self.assertEqual(
            ch.resolve_wire(_session(state="routed_at_text", protocol_shape="at_text")),
            "at_text",
        )

    def test_conflict_unobserved_and_hints_resolve_to_nothing(self) -> None:
        ch = _channel()
        # A routed/sniffed contradiction fails closed.
        self.assertEqual(
            ch.resolve_wire(_session(state="routed_framed", protocol_shape="at_text")),
            "",
        )
        # No observable byte shape.
        self.assertEqual(ch.resolve_wire(_session()), "")
        # Cloud family / peer IP / hostname are never a wire authority.
        self.assertEqual(
            ch.resolve_wire(
                _session(cloud_family="smartess", peer_ip="203.0.113.9", host="x")
            ),
            "",
        )

    def test_confirmed_evidence_selects_wire_when_unobserved(self) -> None:
        ch = _channel(entry_data=_CONFIRMED_AT)
        # Silent socket (no observation) but a validated PN-bound evidence exists.
        self.assertEqual(ch.resolve_wire(_session()), "at_text")

    def test_confirmed_evidence_ignored_for_foreign_pn(self) -> None:
        foreign = dict(_CONFIRMED_AT)
        foreign[CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN] = "V000405SYN94677058"
        ch = _channel(entry_data=foreign)  # entry_pn=FULL_PN != evidence PN
        self.assertEqual(ch.resolve_wire(_session()), "")


class ExactReadIoTests(unittest.IsolatedAsyncioTestCase):
    async def test_observed_framed_reads_on_framed(self) -> None:
        probe = _FakeProbe()
        ch = _channel(probe=probe)
        read = await ch.async_read_exact_session_identity(
            _session(state="routed_framed", protocol_shape="eybond_framed")
        )
        self.assertTrue(read.wire_available)
        self.assertEqual(read.session_protocol, "eybond_framed")
        self.assertEqual(read.collector_pn, FULL_PN)
        self.assertEqual(probe.reads, [("sock-1", "eybond_framed")])

    async def test_observed_at_reads_on_at(self) -> None:
        probe = _FakeProbe()
        ch = _channel(probe=probe)
        read = await ch.async_read_exact_session_identity(
            _session(state="routed_at_text", protocol_shape="at_text")
        )
        self.assertTrue(read.wire_available)
        self.assertEqual(probe.reads, [("sock-1", "at_text")])

    async def test_conflict_and_unobserved_do_zero_io(self) -> None:
        for raw in (
            _session(state="routed_framed", protocol_shape="at_text"),  # conflict
            _session(),  # unobserved
            _session(cloud_family="smartess", peer_ip="203.0.113.9"),  # hints only
        ):
            with self.subTest(raw=raw["raw"]):
                probe = _FakeProbe()
                ch = _channel(probe=probe)
                read = await ch.async_read_exact_session_identity(raw)
                self.assertFalse(read.wire_available)
                self.assertEqual(probe.reads, [])  # ZERO identity IO

    async def test_silent_with_confirmed_evidence_reads_once(self) -> None:
        probe = _FakeProbe()
        ch = _channel(entry_data=_CONFIRMED_AT, probe=probe)
        read = await ch.async_read_exact_session_identity(_session())
        self.assertTrue(read.wire_available)
        self.assertEqual(probe.reads, [("sock-1", "at_text")])  # exactly one read

    async def test_silent_without_evidence_is_unavailable_zero_io(self) -> None:
        probe = _FakeProbe()
        ch = _channel(probe=probe)  # no confirmed evidence
        read = await ch.async_read_exact_session_identity(_session())
        self.assertFalse(read.wire_available)
        self.assertEqual(probe.reads, [])


class ProjectionMergeTests(unittest.TestCase):
    """BLOCKER 1: silent PN-less sockets are merged into the projection."""

    def _channel(self, *, inventory, silent):
        from custom_components.eybond_local.connection.session_registry import (
            CallbackSessionRegistry,
        )

        registry = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
        channel = CallbackBootstrapChannel(
            registry=registry, host="127.0.0.1", port=8899, entry_pn=FULL_PN
        )
        channel._probe = _FakeProbe(silent=silent)
        return channel

    def _observed(self, sid, pn):
        return {
            "session_id": sid,
            "peer_ip": "203.0.113.9",
            "listener_port": 8899,
            "collector_pn": pn,
            "state": "routed_framed",
            "protocol_shape": "eybond_framed",
            "collector_identity_source": "fc2_parameter_2",
        }

    def test_silent_socket_appears_with_no_identity_or_wire_hint(self) -> None:
        ch = self._channel(inventory=[], silent={"silent-1"})
        rows = {r["session_id"]: r for r in ch.sessions()}
        self.assertIn("silent-1", rows)
        row = rows["silent-1"]
        self.assertEqual(row["collector_pn"], "")
        self.assertFalse(row["has_strong_identity"])
        self.assertEqual(row["listener_port"], 8899)  # from the channel
        # No wire/protocol/identity/peer hint in the synthetic raw.
        self.assertEqual(set(row["raw"]), {"session_id", "listener_port"})

    def test_registry_observation_wins_dedup(self) -> None:
        # A socket that is BOTH in the registry (strong) and reported silent
        # appears exactly once, as the registry observation.
        ch = self._channel(
            inventory=[self._observed("dual", FULL_PN)], silent={"dual"}
        )
        rows = [r for r in ch.sessions() if r["session_id"] == "dual"]
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["has_strong_identity"])
        self.assertEqual(rows[0]["collector_pn"], FULL_PN)


if __name__ == "__main__":
    unittest.main()
