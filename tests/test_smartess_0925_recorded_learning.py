"""Replay the real 24V bench study without hardware, credentials or internet.

The fixture whitelists register data (excluding the serial range), cloud field
definitions, all 146 write payloads and API-returned response frames. The full
private archive retains timing, collector identity, PI30 data and raw diagnostics.
This replay validates our backend and overlay generation, not vendor retry policy
or a live network/collector. It must never be used as a cloud hold release receipt.
"""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from custom_components.eybond_local.collector.protocol import (
    HEADER_SIZE, build_collector_request, decode_header,
)
from custom_components.eybond_local.payload.modbus import build_read_request, crc16_modbus
from custom_components.eybond_local.smartess_cloud import SessionCredentials, build_device_control_action
from custom_components.eybond_local.support.shadow_learning.backend import (
    InProcessShadowLearningHandler, ShadowLearningSeed,
)
from custom_components.eybond_local.support.shadow_learning.orchestrator import (
    async_orchestrate_shadow_learning_settings, summarize_shadow_learning_attempts,
)
from custom_components.eybond_local.support.shadow_learning.overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)
from custom_components.eybond_local.support.shadow_learning.protocol_evidence import (
    CloudProtocolEvidence,
)

FIXTURE = Path(__file__).parent / "fixtures/smartess_0925_recorded_learning.json"


class SmartEss0925RecordedLearningTests(unittest.IsolatedAsyncioTestCase):
    async def test_recorded_study_replays_and_generates_controls_offline(self):
        data = json.loads(FIXTURE.read_text())
        bank = {block["start"] + i: value
                for block in data["register_ranges"]
                for i, value in enumerate(block["values"])}
        # The actual cloud reads include ranges outside the standard dump.
        for read in data["reads"]:
            bank.update({read["address"] + i: v for i, v in enumerate(read["values"])})
        seed = ShadowLearningSeed(
            session_id="recorded-0925", entry_id="offline-entry",
            collector_pn="Q0000000000001", collector_cloud_profile_key="eybond_ascii_pn_v1",
            collector_cloud_profile_label="Recorded ASCII PN v1",
            collector_cloud_profile_source="fixture", collector_cloud_profile_confidence="high",
            collector_callback_endpoint="127.0.0.1",
            effective_metadata_snapshot={"effective_owner_key": "smartess_local",
                "profile_name": data["source_profile"], "register_schema_name": data["source_schema"]},
            command_responses={}, register_bank=bank, collector_cloud_family="legacy_binary",
        )
        with tempfile.TemporaryDirectory() as tmp:
            backend = InProcessShadowLearningHandler(seed=seed, output_path=Path(tmp) / "trace.jsonl")
            await backend.start()
            try:
                for read in data["reads"]:
                    request = build_collector_request(read["tid"],
                        build_read_request(data["unit"], read["address"], read["count"]),
                        devcode=read["devcode"], collector_addr=read["devaddr"], fcode=4)
                    evidence = CloudProtocolEvidence.from_frame(request)
                    self.assertEqual(set(evidence.protocols), {"modbus_rtu"})
                    response = await backend._handle_frame(request, remote="offline")
                    payload = response[HEADER_SIZE:]
                    self.assertEqual(crc16_modbus(payload[:-2]), int.from_bytes(payload[-2:], "little"))
                    self.assertEqual([int.from_bytes(payload[i:i+2], "big")
                                      for i in range(3, len(payload)-2, 2)], read["values"])

                attempts = []
                tids = set()
                for index, row in enumerate(data["attempts"]):
                    item = dict(zip(data["attempt_columns"], row, strict=True))
                    expected = bytes.fromhex(item.pop("api_response_frame_hex"))
                    header = decode_header(expected)
                    self.assertEqual(header.total_len, len(expected))
                    self.assertEqual((header.devcode, header.devaddr, header.fcode), (1, 5, 4))
                    self.assertNotIn(header.tid, tids)
                    tids.add(header.tid)
                    self.assertEqual(item.pop("api_status"), 0)
                    request = build_collector_request(header.tid, bytes.fromhex(item.pop("write_payload_hex")),
                        devcode=header.devcode, collector_addr=header.devaddr, fcode=header.fcode)
                    response = await backend._handle_frame(request, remote="offline")
                    # Exact API echo of our refusal, not just an HTTP success/count.
                    self.assertEqual(response, expected)
                    self.assertEqual(response[HEADER_SIZE:HEADER_SIZE+3], bytes((5, 0x86, 1)))
                    self.assertEqual(backend._register_bank, bank)
                    if item.get("numeric_evidence") is None:
                        item.pop("numeric_evidence", None)
                    attempts.append({**item, "sequence_index": index,
                        "status": "captured_not_applied", "match_mode": "post_attempt_cursor",
                        "observation": backend.write_observations[-1].to_json_dict()})
                self.assertEqual(len(tids), data["expected"]["matched"])
                correlation = summarize_shadow_learning_attempts(
                    attempts=attempts, all_observations=backend.write_observations)
                self.assertEqual(correlation["matched_count"], 146)
                self.assertEqual(correlation["unmatched_write_count"], 0)

                # Also replay the async orchestrator: substitute only the saved
                # plan and remote provider, not correlation or the shadow backend.
                plan = [{**a, "value": a["requested_value"], "title": a["field_name"]}
                        for a in attempts]
                replay_rows = iter(data["attempts"])

                async def dispatch(_fetch, *, action, **_kwargs):
                    item = dict(zip(data["attempt_columns"], next(replay_rows), strict=True))
                    self.assertEqual(action, build_device_control_action(
                        pn=seed.collector_pn, sn="offline", devcode=1, devaddr=5,
                        field_id=item["field_id"], value=item["requested_value"]))
                    expected = bytes.fromhex(item["api_response_frame_hex"])
                    header = decode_header(expected)
                    frame = build_collector_request(header.tid, bytes.fromhex(item["write_payload_hex"]),
                        devcode=1, collector_addr=5, fcode=4)
                    response = await backend._handle_frame(frame, remote="offline")
                    self.assertEqual(response, expected)
                    return SimpleNamespace(err=0, desc="ERR_NONE",
                                           dat={"dat": expected.hex(" "), "status": 0})

                no_http = Mock(side_effect=AssertionError("Offline replay must not call HTTP"))
                with patch("custom_components.eybond_local.support.shadow_learning.orchestrator.build_learn_settings_plan",
                           return_value=plan):
                    result = await async_orchestrate_shadow_learning_settings(
                        settings_dat={"field": [{"id": a["field_id"]} for a in attempts]},
                        session=SessionCredentials(token="offline", secret="offline"),
                        pn=seed.collector_pn, sn="offline", devcode=1, devaddr=5,
                        dry_run=False, confirm_cloud_write=True, shadow_session_state="ready",
                        field_ids=[], include_numeric=True, dispatch_action=dispatch, fetch_action=no_http,
                        observation_cursor=backend.observation_cursor,
                        current_observations_since=backend.observations_since,
                        wait_for_observations_since=backend.wait_for_observations_since)
                no_http.assert_not_called()
                self.assertEqual(result["captured_not_applied_count"], 146)
                self.assertEqual(result["leaked_count"], 0)
                self.assertEqual(result["unknown_field_count"], 0)
                self.assertEqual(backend._register_bank, bank)
                correlation = result["correlation"]
                self.assertEqual(correlation["matched_count"], 146)
                self.assertEqual(correlation["unmatched_write_count"], 0)
                draft = generate_shadow_learning_overlay_drafts(
                    config_dir=Path(tmp), source_profile_name=data["source_profile"],
                    source_schema_name=data["source_schema"], correlation=correlation,
                    session_manifest={"session_id": seed.session_id, "collector_pn": seed.collector_pn,
                                      "devcode": 1, "devaddr": 5, "write_response_mode": "exception"})
                self.assertEqual(draft.generated_capability_count, data["expected"]["new_controls"])
                self.assertEqual(draft.skipped_duplicate_count, data["expected"]["existing_controls"])
            finally:
                await backend.stop()
