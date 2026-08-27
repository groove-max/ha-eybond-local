from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.collector.identity_probe import (
    PROBE_AT_DTUPN,
    PROBE_FRAMED_FC1,
    PROBE_FRAMED_FC2,
    build_identity_probe_request,
    parse_identity_probe_response,
)
from custom_components.eybond_local.collector.protocol import decode_header, encode_header
from custom_components.eybond_local.collector.session_identity_negotiator import (
    ExactSessionIdentityNegotiator,
    NEGOTIATION_AMBIGUOUS,
    NEGOTIATION_FOREIGN_IDENTITY,
    NEGOTIATION_PROBE_FAILED,
)
from custom_components.eybond_local.collector.silent_session_probe import (
    SessionObservation,
)


FULL_PN = "E50000253745448949"
FOREIGN_PN = "E50000253745449999"


class _Channel:
    def __init__(
        self,
        observations,
        replies=None,
        *,
        block=False,
        block_retire=False,
    ):
        self.observations = list(observations)
        self.replies = list(replies or [])
        self.block = block
        self.block_retire = block_retire
        self.probes = []
        self.retired = []
        self.probe_started = asyncio.Event()
        self.retire_finished = asyncio.Event()
        self.retire_started = asyncio.Event()
        self.release_retire = asyncio.Event()

    def snapshot_session_observations(self):
        return tuple(self.observations)

    async def async_identify_exact_session(
        self,
        session_id,
        *,
        session_protocol,
        identity_probe_kind="",
    ):
        self.probes.append((session_id, session_protocol, identity_probe_kind))
        self.probe_started.set()
        if self.block:
            await asyncio.Event().wait()
        reply = self.replies.pop(0) if self.replies else ""
        if reply:
            source = (
                "at_dtupn"
                if session_protocol == "at_text"
                else "fc1_identity_challenge"
            )
            shape = (
                "at_text"
                if session_protocol == "at_text"
                else "eybond_framed"
            )
            self.observations = [
                SessionObservation(
                    session_id=session_id,
                    collector_pn=reply,
                    identity_source=source,
                    protocol_shape=shape,
                    state="waiting_for_route_identity",
                )
            ]
        return reply

    async def async_retire_exact_session(self, session_id):
        self.retire_started.set()
        if self.block_retire:
            await self.release_retire.wait()
        await asyncio.sleep(0)
        self.retired.append(session_id)
        self.retire_finished.set()
        return True


def _deadline(seconds=1.0):
    return asyncio.get_running_loop().time() + seconds


class IdentityProbeWireTests(unittest.TestCase):
    def test_fc1_requires_exact_tid_function_and_returns_full_pn(self):
        request = build_identity_probe_request(
            "eybond_framed", probe_kind=PROBE_FRAMED_FC1
        )
        self.assertIsNotNone(request)
        assert request is not None
        self.assertEqual(len(request.payload), 16)
        request_header = decode_header(request.payload[:8])
        self.assertEqual(
            (
                request_header.tid,
                request_header.devcode,
                request_header.devaddr,
                request_header.fcode,
                request.payload[-2:],
            ),
            (1, 0, 1, 1, b"\x00\x3c"),
        )
        response = (
            encode_header(1, 0x0102, 8 + len(FULL_PN), 0xFF, 1)
            + FULL_PN.encode("ascii")
        )
        self.assertEqual(
            parse_identity_probe_response(request, response),
            (FULL_PN, "fc1_identity_challenge"),
        )
        wrong_tid = (
            encode_header(2, 0x0102, 8 + len(FULL_PN), 0xFF, 1)
            + FULL_PN.encode("ascii")
        )
        self.assertEqual(parse_identity_probe_response(request, wrong_tid), ("", ""))

    def test_default_framed_probe_remains_fc2(self):
        request = build_identity_probe_request("eybond_framed")
        self.assertIsNotNone(request)
        assert request is not None
        self.assertEqual(request.probe_kind, PROBE_FRAMED_FC2)


class ExactSessionIdentityNegotiatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_uses_fc1_then_at_on_next_attempt_only(self):
        negotiator = ExactSessionIdentityNegotiator()
        first = _Channel([SessionObservation(session_id="s1")])
        result = await negotiator.async_negotiate(
            channel=first,
            expected_pn=FULL_PN,
            baseline_session_ids=frozenset({"s1"}),
            deadline=_deadline(),
        )
        self.assertEqual(result.status, NEGOTIATION_PROBE_FAILED)
        self.assertEqual(first.probes, [("s1", "eybond_framed", PROBE_FRAMED_FC1)])
        self.assertEqual(first.retired, ["s1"])

        second = _Channel(
            [SessionObservation(session_id="s2")],
            replies=[FULL_PN],
        )
        result = await negotiator.async_negotiate(
            channel=second,
            expected_pn=FULL_PN,
            baseline_session_ids=frozenset({"s2"}),
            deadline=_deadline(),
        )
        self.assertTrue(result.identified)
        self.assertEqual(second.probes, [("s2", "at_text", PROBE_AT_DTUPN)])
        self.assertEqual(second.retired, [])

    async def test_at_metadata_only_reorders_unknown_first_attempt(self):
        channel = _Channel(
            [SessionObservation(session_id="s1")], replies=[FULL_PN]
        )
        result = await ExactSessionIdentityNegotiator().async_negotiate(
            channel=channel,
            expected_pn=FULL_PN,
            baseline_session_ids=frozenset({"s1"}),
            deadline=_deadline(),
            preferred_protocol="at_text",
        )
        self.assertTrue(result.identified)
        self.assertEqual(channel.probes, [("s1", "at_text", PROBE_AT_DTUPN)])

    async def test_weak_framed_observation_uses_fc2_not_fc1(self):
        channel = _Channel(
            [
                SessionObservation(
                    session_id="s1",
                    collector_pn=FULL_PN[:14],
                    identity_source="framed_heartbeat",
                    protocol_shape="eybond_framed",
                )
            ],
            replies=[FULL_PN],
        )
        result = await ExactSessionIdentityNegotiator().async_negotiate(
            channel=channel,
            expected_pn=FULL_PN,
            baseline_session_ids=frozenset({"s1"}),
            deadline=_deadline(),
        )
        self.assertTrue(result.identified)
        self.assertEqual(
            channel.probes,
            [("s1", "eybond_framed", PROBE_FRAMED_FC2)],
        )

    async def test_two_fresh_sessions_are_ambiguous_and_send_nothing(self):
        channel = _Channel(
            [
                SessionObservation(session_id="s1"),
                SessionObservation(session_id="s2"),
            ]
        )
        result = await ExactSessionIdentityNegotiator().async_negotiate(
            channel=channel,
            expected_pn=FULL_PN,
            baseline_session_ids=frozenset(),
            deadline=_deadline(),
        )
        self.assertEqual(result.status, NEGOTIATION_AMBIGUOUS)
        self.assertEqual(channel.probes, [])

    async def test_foreign_strong_identity_sends_nothing(self):
        channel = _Channel(
            [
                SessionObservation(
                    session_id="s1",
                    collector_pn=FOREIGN_PN,
                    identity_source="at_dtupn",
                    protocol_shape="at_text",
                )
            ]
        )
        result = await ExactSessionIdentityNegotiator().async_negotiate(
            channel=channel,
            expected_pn=FULL_PN,
            baseline_session_ids=frozenset(),
            deadline=_deadline(),
        )
        self.assertEqual(result.status, NEGOTIATION_FOREIGN_IDENTITY)
        self.assertEqual(channel.probes, [])

    async def test_cancelled_unknown_probe_retires_before_cancel_propagates(self):
        channel = _Channel([SessionObservation(session_id="s1")], block=True)
        task = asyncio.create_task(
            ExactSessionIdentityNegotiator().async_negotiate(
                channel=channel,
                expected_pn=FULL_PN,
                baseline_session_ids=frozenset({"s1"}),
                deadline=_deadline(10),
            )
        )
        await asyncio.wait_for(channel.probe_started.wait(), timeout=1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(channel.retire_finished.is_set())
        self.assertEqual(channel.retired, ["s1"])

    async def test_repeated_cancel_during_retirement_does_not_detach_cleanup(self):
        channel = _Channel(
            [SessionObservation(session_id="s1")],
            block_retire=True,
        )
        task = asyncio.create_task(
            ExactSessionIdentityNegotiator().async_negotiate(
                channel=channel,
                expected_pn=FULL_PN,
                baseline_session_ids=frozenset({"s1"}),
                deadline=_deadline(10),
            )
        )
        await asyncio.wait_for(channel.retire_started.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        channel.release_retire.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(channel.retire_finished.is_set())
        self.assertEqual(channel.retired, ["s1"])


class SessionIdentityNegotiationArchitectureTests(unittest.TestCase):
    def test_negotiator_owns_no_callback_or_socket_routing_primitive(self):
        source = (
            REPO_ROOT
            / "custom_components/eybond_local/collector/session_identity_negotiator.py"
        ).read_text(encoding="utf-8")
        for forbidden in (
            "peer_ip",
            "async_send_callback_trigger",
            ".reader",
            ".writer",
        ):
            self.assertNotIn(forbidden, source)

    def test_runtime_delegates_dialects_to_the_one_negotiator(self):
        source = (
            REPO_ROOT / "custom_components/eybond_local/runtime/link/connection.py"
        ).read_text(encoding="utf-8")
        self.assertIn("_session_identity_negotiator.async_negotiate", source)
        self.assertNotIn("PROBE_FRAMED_FC1", source)
        self.assertNotIn("PROBE_FRAMED_FC2", source)
        self.assertNotIn("PROBE_AT_DTUPN", source)


if __name__ == "__main__":
    unittest.main()
