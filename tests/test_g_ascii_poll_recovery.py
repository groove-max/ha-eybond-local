"""Bound silent G-ASCII polls without confusing optional commands with outages."""

from __future__ import annotations

import asyncio
from copy import deepcopy
import unittest

from custom_components.eybond_local.drivers.eybond_g_ascii import (
    _async_collect_eybond_g_ascii_values,
)
from custom_components.eybond_local.payload.ascii_line import AsciiLineError


CORE = ("GMOD", "GPDAT0", "GPV")
CORE_REPLIES = {
    "GMOD": "B",
    "GPDAT0": (
        "0 5 4003 0 00 219.7 50.01 000.0 00.00 216.4 50.00 "
        "04.11 26.4 015.8 025 0904 0904 100 172.8 015.8 0844 048.0"
    ),
    "GPV": "183.1 027.6 00.43 05.31 00972 03 1",
}


class ScriptedSession:
    def __init__(self, reply):
        self.reply = reply
        self.connected = True
        self.commands = []

    async def request(self, command):
        self.commands.append(command)
        result = self.reply(command, len(self.commands))
        if isinstance(result, BaseException):
            raise result
        return result

    def last_transport_timing(self):
        return {"transport_total_duration_ms": 5}


def timeout():
    return AsciiLineError("request_timeout")


class GAsciiPollRecoveryTests(unittest.TestCase):
    def read(self, session, state=None, **kwargs):
        return asyncio.run(_async_collect_eybond_g_ascii_values(
            session, probe=False, runtime_state=state, **kwargs,
        ))

    def test_whole_outage_stops_after_core_without_poisoning_cache(self):
        session = ScriptedSession(lambda command, count: timeout())
        state = {"driver_unsupported_commands": {"GBAT": 2}}
        before = deepcopy(state)
        for _ in range(5):
            session.commands.clear()
            with self.assertRaisesRegex(AsciiLineError, "^request_timeout$"):
                self.read(session, state)
            self.assertEqual(session.commands, list(CORE))
            self.assertEqual(state, before)

    def test_no_core_payload_is_not_a_successful_refresh(self):
        for response in ("", "ACK", "garbled", "nan", AsciiLineError("nak")):
            with self.subTest(response=response):
                session = ScriptedSession(lambda command, count: response)
                with self.assertRaisesRegex(AsciiLineError, "g_ascii_core_unavailable"):
                    self.read(session, {})
                self.assertEqual(session.commands, list(CORE))

    def test_single_core_timeout_preserves_other_measurements(self):
        session = ScriptedSession(lambda command, count: (
            timeout() if command == "GMOD" else CORE_REPLIES.get(command, "")
        ))
        values = self.read(session, {})
        self.assertEqual(values["battery_voltage"], 26.4)
        self.assertEqual(values["pv_power"], 972)
        self.assertIn("GBMS", session.commands)

    def test_optional_timeouts_recheck_known_core_and_continue(self):
        session = ScriptedSession(lambda command, count: (
            timeout() if command in {"F", "SVFW", "GTMP"}
            else CORE_REPLIES.get(command, "")
        ))
        values = self.read(session, {})
        self.assertEqual(session.commands[:8], [*CORE, "F", "SVFW", "GTMP", "GPV", "GLINE"])
        self.assertEqual(values["pv_power"], 972)
        self.assertIn("GBMS", session.commands)
        checks = [row for row in values["eybond_g_ascii_runtime_command_timings"] if row["command"] == "GPV"]
        self.assertEqual(len(checks), 2)
        self.assertTrue(all(row["status"] == "ok" for row in checks))

    def test_mid_cycle_silence_aborts_after_one_core_recheck(self):
        session = ScriptedSession(lambda command, count: (
            CORE_REPLIES[command] if count <= 3 else timeout()
        ))
        state = {"driver_unsupported_commands": {"GTMP": 2}}
        before = deepcopy(state)
        with self.assertRaisesRegex(AsciiLineError, "^request_timeout$"):
            self.read(session, state)
        self.assertEqual(session.commands, [*CORE, "F", "SVFW", "GTMP", "GPV"])
        self.assertEqual(state, before)

    def test_negative_replies_are_not_silence(self):
        session = ScriptedSession(lambda command, count: (
            CORE_REPLIES.get(command, AsciiLineError("nak"))
        ))
        self.read(session, {})
        self.assertEqual(session.commands.count("GPV"), 1)
        self.assertIn("GBMS", session.commands)

    def test_readback_timeout_evidence_is_rolled_back_on_later_silence(self):
        from custom_components.eybond_local.metadata.profile_loader import load_driver_profile

        profile = load_driver_profile("eybond_g_ascii/models/lvyuan_ty_sic_3_6kbe_w1.json")
        silent = False

        def reply(command, count):
            nonlocal silent
            silent |= command == "CHGC???"
            return timeout() if silent else CORE_REPLIES.get(command, "")

        state = {"eybond_g_ascii_readback_timeout_counts": {"CHGC???": 2}}
        before = deepcopy(state)
        session = ScriptedSession(reply)
        with self.assertRaisesRegex(AsciiLineError, "^request_timeout$"):
            self.read(session, state, capabilities=profile.capabilities)
        self.assertEqual(state, before)
        self.assertEqual(session.commands[-1], "GPV")

    def test_only_an_answered_core_command_is_used_for_recheck(self):
        session = ScriptedSession(lambda command, count: (
            "B" if command == "GMOD" else timeout()
        ))
        values = self.read(session, {})
        self.assertEqual(values["operating_mode"], "Battery")
        self.assertGreater(session.commands.count("GMOD"), 1)
        self.assertEqual(session.commands.count("GPV"), 1)
        self.assertEqual(session.commands.count("GPDAT0"), 1)

    def test_probe_keeps_its_partial_detection_contract(self):
        session = ScriptedSession(lambda command, count: timeout())
        values = asyncio.run(_async_collect_eybond_g_ascii_values(session, probe=True))
        self.assertEqual(session.commands, list(CORE))
        self.assertEqual(values["eybond_g_ascii_runtime_command_count"], 3)

    def test_disconnected_transport_stops_without_further_requests(self):
        session = ScriptedSession(lambda command, count: "B")
        session.connected = False
        with self.assertRaisesRegex(ConnectionError, "collector_not_connected"):
            self.read(session, {})
        self.assertEqual(session.commands, [])

    def test_disconnect_during_timeout_is_transport_failure(self):
        def reply(command, count):
            if count == 4:
                session.connected = False
                return timeout()
            return CORE_REPLIES.get(command, "")

        session = ScriptedSession(reply)
        state = {}
        with self.assertRaisesRegex(ConnectionError, "collector_disconnected"):
            self.read(session, state)
        self.assertEqual(session.commands, [*CORE, "F"])
        self.assertEqual(state, {})

    def test_cancelled_cycle_leaves_no_pending_evidence(self):
        session = ScriptedSession(lambda command, count: (
            asyncio.CancelledError() if command == "GBAT"
            else CORE_REPLIES.get(command, AsciiLineError("nak"))
        ))
        state = {"driver_unsupported_commands": {"GTMP": 1}}
        before = deepcopy(state)
        with self.assertRaises(asyncio.CancelledError):
            self.read(session, state)
        self.assertEqual(state, before)

    def test_next_healthy_cycle_recovers_without_blacklisting_commands(self):
        state = {}
        with self.assertRaises(AsciiLineError):
            self.read(ScriptedSession(lambda command, count: timeout()), state)
        session = ScriptedSession(lambda command, count: CORE_REPLIES.get(command, ""))
        values = self.read(session, state)
        self.assertEqual(values["battery_voltage"], 26.4)
        self.assertEqual(state.get("driver_unsupported_commands"), {})
        self.assertNotIn("driver_unsupported_pending_failures", state)
        self.assertNotIn("driver_cycle_had_success", state)


if __name__ == "__main__":
    unittest.main()
