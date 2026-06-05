from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.shadow_learning_overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)


def _sample_session_manifest() -> dict[str, object]:
    return {
        "session_id": "smg-shadow-session-01",
        "collector_pn": "E5000025388419",
        "cloud_pn": "E50000253884199645",
        "cloud_sn": "E50000253884199645094801",
        "devcode": 2376,
        "devaddr": 1,
        "write_response_mode": "exception",
    }


def _sample_correlation_payload() -> dict[str, object]:
    return {
        "matched_count": 4,
        "unmatched_attempt_count": 1,
        "unmatched_write_count": 0,
        "matched": [
            {
                "sequence_index": 0,
                "field_id": "sys_eybond_ctrl_53",
                "field_name": "Backlight Control",
                "requested_value": "0",
                "requested_at": "2026-06-05T12:00:01+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:01.100000+00:00",
                    "source": "shadow_learning",
                    "unit": 1,
                    "function_code": 16,
                    "register": 705,
                    "values": [0],
                    "devcode": 2376,
                    "devaddr": 1,
                    "raw_payload_hex": "0110013100010200000000",
                },
            },
            {
                "sequence_index": 1,
                "field_id": "sys_eybond_ctrl_53",
                "field_name": "Backlight Control",
                "requested_value": "1",
                "requested_at": "2026-06-05T12:00:02+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:02.100000+00:00",
                    "source": "shadow_learning",
                    "unit": 1,
                    "function_code": 16,
                    "register": 705,
                    "values": [1],
                    "devcode": 2376,
                    "devaddr": 1,
                    "raw_payload_hex": "0110013100010200010000",
                },
            },
            {
                "sequence_index": 2,
                "field_id": "bat_eybond_ctrl_76",
                "field_name": "Maximum charging current",
                "requested_value": "20",
                "requested_at": "2026-06-05T12:00:03+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:03.100000+00:00",
                    "source": "shadow_learning",
                    "unit": 1,
                    "function_code": 16,
                    "register": 331,
                    "values": [20],
                    "devcode": 2376,
                    "devaddr": 1,
                    "raw_payload_hex": "0110014b00010200140000",
                },
            },
            {
                "sequence_index": 3,
                "field_id": "sys_eybond_ctrl_500",
                "field_name": "Reset user parameters",
                "requested_value": "1",
                "requested_at": "2026-06-05T12:00:04+00:00",
                "observation": {
                    "timestamp": "2026-06-05T12:00:04.100000+00:00",
                    "source": "shadow_learning",
                    "unit": 1,
                    "function_code": 6,
                    "register": 690,
                    "values": [1],
                    "devcode": 2376,
                    "devaddr": 1,
                    "raw_payload_hex": "010602b200010000",
                },
            },
        ],
    }


def _find_capability(raw: dict[str, object], key_suffix: str) -> dict[str, object]:
    capabilities = raw.get("capabilities")
    if not isinstance(capabilities, list):
        raise KeyError(key_suffix)
    for item in capabilities:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "")
        if key.endswith(key_suffix):
            return item
    raise KeyError(key_suffix)


class ShadowLearningOverlayGeneratorTests(unittest.TestCase):
    def test_generates_inactive_profile_and_schema_drafts_with_manifest_scope(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
            )

            self.assertIn("/learned/shadow_learning/", str(result.profile_path))
            self.assertIn("/learned/shadow_learning/", str(result.schema_path))
            self.assertEqual(result.generated_capability_count, 2)
            self.assertEqual(result.skipped_duplicate_count, 1)

            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            schema_raw = json.loads(result.schema_path.read_text(encoding="utf-8"))

            self.assertTrue(bool(profile_raw.get("experimental")))
            self.assertTrue(bool(schema_raw.get("experimental")))
            self.assertEqual(
                str(profile_raw.get("draft_of") or ""),
                "smg_modbus.json",
            )
            self.assertEqual(
                str(schema_raw.get("draft_of") or ""),
                "modbus_smg/models/smg_6200.json",
            )

            overlay = profile_raw.get("shadow_learning_overlay")
            self.assertIsInstance(overlay, dict)
            assert isinstance(overlay, dict)
            self.assertEqual(str(overlay.get("scope") or ""), "device")
            session = overlay.get("session")
            self.assertIsInstance(session, dict)
            assert isinstance(session, dict)
            self.assertEqual(str(session.get("cloud_sn") or ""), "E50000253884199645094801")
            self.assertEqual(int(session.get("devcode", 0)), 2376)
            self.assertEqual(int(session.get("devaddr", 0)), 1)

            reset_capability = _find_capability(profile_raw, "_690")
            self.assertEqual(str(reset_capability.get("value_kind") or ""), "action")
            self.assertTrue(bool(reset_capability.get("requires_confirm")))
            self.assertTrue(bool(reset_capability.get("unsafe_while_running")))
            provenance = reset_capability.get("learned_provenance")
            self.assertIsInstance(provenance, dict)
            assert isinstance(provenance, dict)
            self.assertEqual(str(provenance.get("source") or ""), "smartess_shadow_learning")
            self.assertEqual(str(provenance.get("scope") or ""), "device")
            self.assertEqual(str(provenance.get("safety_class") or ""), "destructive_action")
            self.assertTrue(bool(str(provenance.get("evidence_hash") or "")))

    def test_deduplicates_learned_capabilities_against_existing_builtin_registers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
            )

            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            generated_registers = {
                int(item.get("register", 0))
                for item in list(profile_raw.get("capabilities") or [])
                if isinstance(item, dict)
            }
            self.assertNotIn(331, generated_registers)
            overlay = profile_raw.get("shadow_learning_overlay")
            self.assertIsInstance(overlay, dict)
            assert isinstance(overlay, dict)
            skipped = overlay.get("skipped_duplicates")
            self.assertIsInstance(skipped, list)
            assert isinstance(skipped, list)
            self.assertTrue(
                any(
                    int(item.get("register", -1)) == 331
                    and str(item.get("reason") or "") == "register_already_mapped"
                    for item in skipped
                    if isinstance(item, dict)
                )
            )

    def test_uses_explicit_matched_count_when_matched_payload_is_malformed(self) -> None:
        correlation = _sample_correlation_payload()
        correlation["matched_count"] = 99
        correlation["matched"] = None

        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=correlation,
            )

            self.assertEqual(result.generated_capability_count, 0)
            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            overlay = profile_raw.get("shadow_learning_overlay")
            self.assertIsInstance(overlay, dict)
            assert isinstance(overlay, dict)
            summary = overlay.get("correlation_summary")
            self.assertIsInstance(summary, dict)
            assert isinstance(summary, dict)
            self.assertEqual(int(summary.get("matched_count", -1)), 99)

    def test_same_field_id_with_different_registers_generates_distinct_capabilities(self) -> None:
        session = _sample_session_manifest()
        correlation = {
            "matched_count": 2,
            "unmatched_attempt_count": 0,
            "unmatched_write_count": 0,
            "matched": [
                {
                    "sequence_index": 0,
                    "field_id": "sys_eybond_ctrl_multi",
                    "field_name": "Multi Register Control",
                    "requested_value": "1",
                    "requested_at": "2026-06-05T13:00:01+00:00",
                    "observation": {
                        "timestamp": "2026-06-05T13:00:01.100000+00:00",
                        "function_code": 16,
                        "register": 25001,
                        "values": [1],
                        "devcode": 2376,
                        "devaddr": 1,
                    },
                },
                {
                    "sequence_index": 1,
                    "field_id": "sys_eybond_ctrl_multi",
                    "field_name": "Multi Register Control",
                    "requested_value": "2",
                    "requested_at": "2026-06-05T13:00:02+00:00",
                    "observation": {
                        "timestamp": "2026-06-05T13:00:02.100000+00:00",
                        "function_code": 16,
                        "register": 25002,
                        "values": [2],
                        "devcode": 2376,
                        "devaddr": 1,
                    },
                },
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=session,
                correlation=correlation,
            )

            self.assertEqual(result.generated_capability_count, 2)
            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            capabilities = [
                item
                for item in list(profile_raw.get("capabilities") or [])
                if isinstance(item, dict)
            ]
            generated_registers = {int(item.get("register", -1)) for item in capabilities}
            self.assertEqual(generated_registers, {25001, 25002})


if __name__ == "__main__":
    unittest.main()
