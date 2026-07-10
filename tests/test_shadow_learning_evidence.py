from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.payload.modbus import (
    crc16_modbus,
    build_read_holding_request,
    build_write_multiple_request,
    decode_read_request,
    decode_write_request,
)
from custom_components.eybond_local.support.shadow_learning import (
    ShadowCorrelationInput,
    ShadowEventRecord,
    ShadowSessionManifest,
    ShadowWriteObservation,
    correlation_to_jsonl_line,
    deterministic_evidence_hash,
    event_to_jsonl_line,
    parse_jsonl_line,
    write_observation_from_modbus_request,
    write_observation_to_jsonl_line,
)


class ShadowLearningEvidenceTests(unittest.TestCase):
    def test_models_round_trip_to_stable_json(self) -> None:
        manifest = ShadowSessionManifest(
            session_id="smg6200-session-1",
            collector_pn="E50000200000000001",
            cloud_pn="E50000200000000001",
            cloud_sn="E50000200000000001000001",
            devcode=2376,
            devaddr=1,
            write_response_mode="exception",
            created_at="2026-06-05T12:00:00+00:00",
        )
        event = ShadowEventRecord(
            timestamp="2026-06-05T12:00:01+00:00",
            kind="modbus_write_request",
            direction="cloud_to_shadow",
            payload={"register": 305, "values": [0]},
        )
        observation = ShadowWriteObservation(
            timestamp="2026-06-05T12:00:01+00:00",
            source="shadow_cloud",
            unit=1,
            function_code=16,
            register=305,
            values=(0,),
            devcode=2376,
            devaddr=1,
            raw_payload_hex="0110013100010200000000",
        )
        correlation = ShadowCorrelationInput(
            sequence_index=11,
            field_id="sys_eybond_ctrl_53",
            field_name="Backlight Control",
            requested_value="0",
            write_observation=observation,
        )

        self.assertEqual(
            ShadowSessionManifest.from_json_dict(manifest.to_json_dict()).to_json_dict(),
            manifest.to_json_dict(),
        )
        self.assertEqual(
            ShadowEventRecord.from_json_dict(event.to_json_dict()).to_json_dict(),
            event.to_json_dict(),
        )
        self.assertEqual(
            ShadowWriteObservation.from_json_dict(observation.to_json_dict()).to_json_dict(),
            observation.to_json_dict(),
        )
        self.assertEqual(
            ShadowCorrelationInput.from_json_dict(correlation.to_json_dict()).to_json_dict(),
            correlation.to_json_dict(),
        )

    def test_compact_jsonl_helpers_round_trip(self) -> None:
        event = ShadowEventRecord(
            timestamp="2026-06-05T12:00:01+00:00",
            kind="modbus_write_request",
            direction="cloud_to_shadow",
            payload={"register": 305, "values": [0]},
        )
        observation = ShadowWriteObservation(
            timestamp="2026-06-05T12:00:01+00:00",
            source="shadow_cloud",
            unit=1,
            function_code=16,
            register=305,
            values=(0,),
            devcode=2376,
            devaddr=1,
            raw_payload_hex="0110013100010200000000",
        )
        correlation = ShadowCorrelationInput(
            sequence_index=11,
            field_id="sys_eybond_ctrl_53",
            field_name="Backlight Control",
            requested_value="0",
            write_observation=observation,
        )

        parsed_event = parse_jsonl_line(event_to_jsonl_line(event))
        parsed_observation = parse_jsonl_line(write_observation_to_jsonl_line(observation))
        parsed_correlation = parse_jsonl_line(correlation_to_jsonl_line(correlation))

        self.assertEqual(parsed_event["payload"]["register"], 305)
        self.assertEqual(parsed_observation["function_code"], 16)
        self.assertEqual(parsed_correlation["field_id"], "sys_eybond_ctrl_53")

    def test_evidence_hash_excludes_volatile_timestamps(self) -> None:
        base = {
            "field_id": "sys_eybond_ctrl_53",
            "register": 305,
            "values": [0],
            "timestamp": "2026-06-05T12:00:01+00:00",
            "created_at": "2026-06-05T12:00:02+00:00",
        }
        changed = {
            "field_id": "sys_eybond_ctrl_53",
            "register": 305,
            "values": [0],
            "timestamp": "2026-06-05T12:01:01+00:00",
            "created_at": "2026-06-05T12:01:02+00:00",
        }

        self.assertEqual(deterministic_evidence_hash(base), deterministic_evidence_hash(changed))
        self.assertNotEqual(
            deterministic_evidence_hash(base, exclude_keys=frozenset()),
            deterministic_evidence_hash(changed, exclude_keys=frozenset()),
        )

    def test_modbus_decode_helpers_decode_read_and_write_requests(self) -> None:
        read_frame = build_read_holding_request(1, 100, 3)
        read = decode_read_request(read_frame)
        assert read is not None
        self.assertEqual(read.function_code, 0x03)
        self.assertEqual(read.address, 100)
        self.assertEqual(read.count, 3)

        write_frame = build_write_multiple_request(1, 305, [0])
        write = decode_write_request(write_frame)
        assert write is not None
        self.assertEqual(write.function_code, 0x10)
        self.assertEqual(write.address, 305)
        self.assertEqual(write.values, (0,))

        write_single_no_crc = bytes([1, 0x06, 0x01, 0x2C, 0x00, 0x01])
        write_single = write_single_no_crc + crc16_modbus(write_single_no_crc).to_bytes(2, "little")
        single = decode_write_request(write_single)
        assert single is not None
        self.assertEqual(single.function_code, 0x06)
        self.assertEqual(single.address, 300)
        self.assertEqual(single.values, (1,))

    def test_write_observation_from_modbus_request_captures_required_fields(self) -> None:
        frame = build_write_multiple_request(1, 305, [0])
        observation = write_observation_from_modbus_request(
            frame=frame,
            devcode=2376,
            devaddr=1,
            timestamp="2026-06-05T12:00:01+00:00",
        )
        self.assertIsNotNone(observation)
        assert observation is not None

        self.assertEqual(observation.register, 305)
        self.assertEqual(observation.values, (0,))
        self.assertEqual(observation.function_code, 0x10)
        self.assertEqual(observation.devcode, 2376)
        self.assertEqual(observation.devaddr, 1)
        self.assertEqual(observation.raw_payload_hex, frame.hex())


if __name__ == "__main__":
    unittest.main()
