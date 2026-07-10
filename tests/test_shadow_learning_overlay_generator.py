from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.metadata.local_metadata import (
    local_profiles_root,
    local_register_schemas_root,
)
from custom_components.eybond_local.metadata.profile_loader import (
    builtin_base_profile_name,
    clear_profile_loader_cache,
    set_external_profile_roots,
)
from custom_components.eybond_local.metadata.register_schema_loader import (
    builtin_base_schema_name,
    clear_register_schema_loader_cache,
    set_external_register_schema_roots,
)
from custom_components.eybond_local.support.shadow_learning_overlay_generator import (
    _build_learned_read_overlay,
    _classify_learned_control,
    generate_shadow_learning_overlay_drafts,
)
from custom_components.eybond_local.support.shadow_learning_review_model import (
    RISK_HIGH,
    RISK_NORMAL,
    RISK_UNCERTAIN,
    build_learned_control_review_model,
    classify_learned_control_risk,
    default_learned_control_label,
)


def _sample_session_manifest() -> dict[str, object]:
    return {
        "session_id": "smg-shadow-session-01",
        "collector_pn": "E5000020000000",
        "cloud_pn": "E50000200000000001",
        "cloud_sn": "E50000200000000001000001",
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
    def setUp(self) -> None:
        # The duplicate-emission validation toggle may be left True in the working tree; force it
        # off so these dedup-count assertions test the real (deduplicated) behaviour.
        patcher = mock.patch(
            "custom_components.eybond_local.support.shadow_learning_overlay_generator."
            "_EMIT_BUILTIN_DUPLICATE_CONTROLS",
            False,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_manifest_embeds_normalized_session_read_map(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
                read_map={
                    "read_blocks": [[200, 22, 79], [641, 5, 79]],
                    "registers": {"205": [2305], "643": [6200]},
                    "read_event_count": 158,
                    "value_source": "seed_bank",
                },
                read_bindings={
                    "bindings": [
                        {
                            "cloud_id": "bt_eybond_read_404",
                            "title": "Aux Learned Voltage",
                            "status": "unique",
                            "candidates": [{"register": 404, "divisor": 10}],
                        }
                    ],
                    "unique_count": 1,
                },
            )

        read_map = result.manifest["read_map"]
        self.assertEqual(read_map["read_blocks"], [[200, 22, 79], [641, 5, 79]])
        self.assertEqual(read_map["registers"]["205"], [2305])
        self.assertEqual(read_map["read_event_count"], 158)
        self.assertEqual(read_map["value_source"], "seed_bank")

        read_bindings = result.manifest["read_bindings"]
        self.assertEqual(read_bindings["unique_count"], 1)
        self.assertEqual(read_bindings["bindings"][0]["title"], "Aux Learned Voltage")
        review_model = result.manifest["review_model"]
        self.assertEqual(review_model["counts"]["learned_read_all"], 1)
        self.assertEqual(
            review_model["learned_read_all"][0]["key"], "learned_read_404"
        )
        self.assertEqual(
            review_model["read_enabled_by_default"], ["learned_read_404"]
        )

    def test_manifest_read_map_empty_when_session_had_no_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
                read_map=None,
            )

        self.assertEqual(result.manifest["read_map"], {})
        self.assertEqual(result.manifest["read_bindings"], {})

    def test_generates_command_based_capability_for_eybond_g_ascii_learning(self) -> None:
        correlation = {
            "matched_count": 2,
            "unmatched_attempt_count": 0,
            "unmatched_write_count": 0,
            "matched": [
                {
                    "sequence_index": 0,
                    "field_id": "cltd_lcd_backlight",
                    "field_name": "LCD Backlight",
                    "requested_value": "0",
                    "value_label": "Off",
                    "value_source": "choice",
                    "requested_at": "2026-06-29T12:00:00+00:00",
                    "observation": {
                        "timestamp": "2026-06-29T12:00:00.100000+00:00",
                        "source": "shadow_learning",
                        "unit": 0,
                        "function_code": 0,
                        "register": -1,
                        "values": [],
                        "protocol": "eybond_g_ascii",
                        "command": "PBL",
                        "value": "0",
                        "raw_payload_hex": "50424c300d",
                    },
                },
                {
                    "sequence_index": 1,
                    "field_id": "cltd_lcd_backlight",
                    "field_name": "LCD Backlight",
                    "requested_value": "1",
                    "value_label": "On",
                    "value_source": "choice",
                    "requested_at": "2026-06-29T12:00:01+00:00",
                    "observation": {
                        "timestamp": "2026-06-29T12:00:01.100000+00:00",
                        "source": "shadow_learning",
                        "unit": 0,
                        "function_code": 0,
                        "register": -1,
                        "values": [],
                        "protocol": "eybond_g_ascii",
                        "command": "PBL",
                        "value": "1",
                        "raw_payload_hex": "50424c310d",
                    },
                },
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="eybond_g_ascii/base.json",
                source_schema_name="eybond_g_ascii/base.json",
                session_manifest=_sample_session_manifest(),
                correlation=correlation,
            )
            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            schema_raw = json.loads(result.schema_path.read_text(encoding="utf-8"))

        capability = profile_raw["capabilities"][0]
        self.assertEqual(result.generated_capability_count, 1)
        self.assertEqual(capability["register"], -1)
        self.assertEqual(capability["command"], "PBL")
        self.assertEqual(capability["value_kind"], "bool")
        self.assertEqual(capability["enum_map"], {"0": "Off", "1": "On"})
        self.assertEqual(result.manifest["learned_capabilities"][0]["command"], "PBL")
        self.assertEqual(schema_raw["learned_write_commands"], ["PBL"])

    def test_g_ascii_learning_groups_one_field_with_different_command_lines(self) -> None:
        correlation = {
            "matched_count": 4,
            "unmatched_attempt_count": 0,
            "unmatched_write_count": 0,
            "matched": [
                {
                    "sequence_index": 0,
                    "field_id": "cltd_energy_saving_mode",
                    "field_name": "Battery energy-saving mode",
                    "requested_value": "68",
                    "value_label": "disable",
                    "value_source": "choice",
                    "requested_at": "2026-06-29T12:00:00+00:00",
                    "observation": {
                        "timestamp": "2026-06-29T12:00:00.100000+00:00",
                        "source": "shadow_learning",
                        "register": -1,
                        "protocol": "eybond_g_ascii",
                        "command": "TDI",
                        "raw_payload_hex": "5444490d",
                    },
                },
                {
                    "sequence_index": 1,
                    "field_id": "cltd_energy_saving_mode",
                    "field_name": "Battery energy-saving mode",
                    "requested_value": "69",
                    "value_label": "encode",
                    "value_source": "choice",
                    "requested_at": "2026-06-29T12:00:01+00:00",
                    "observation": {
                        "timestamp": "2026-06-29T12:00:01.100000+00:00",
                        "source": "shadow_learning",
                        "register": -1,
                        "protocol": "eybond_g_ascii",
                        "command": "TEI",
                        "raw_payload_hex": "5445490d",
                    },
                },
                {
                    "sequence_index": 2,
                    "field_id": "cltd_set_output_priority",
                    "field_name": "Output priority",
                    "requested_value": "12336",
                    "value_label": "Mains output is preferred",
                    "value_source": "choice",
                    "read_key": "g_ascii_setting_output_priority",
                    "requested_at": "2026-06-29T12:00:02+00:00",
                    "observation": {
                        "timestamp": "2026-06-29T12:00:02.100000+00:00",
                        "source": "shadow_learning",
                        "register": -1,
                        "protocol": "eybond_g_ascii",
                        "command": "OPR",
                        "value": "00",
                        "raw_payload_hex": "4f505230300d",
                    },
                },
                {
                    "sequence_index": 3,
                    "field_id": "cltd_set_output_priority",
                    "field_name": "Output priority",
                    "requested_value": "12337",
                    "value_label": "Photovoltaic output is preferred",
                    "value_source": "choice",
                    "read_key": "g_ascii_setting_output_priority",
                    "requested_at": "2026-06-29T12:00:03+00:00",
                    "observation": {
                        "timestamp": "2026-06-29T12:00:03.100000+00:00",
                        "source": "shadow_learning",
                        "register": -1,
                        "protocol": "eybond_g_ascii",
                        "command": "OPR",
                        "value": "01",
                        "raw_payload_hex": "4f505230310d",
                    },
                },
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="eybond_g_ascii/base.json",
                source_schema_name="eybond_g_ascii/base.json",
                session_manifest=_sample_session_manifest(),
                correlation=correlation,
            )
            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            schema_raw = json.loads(result.schema_path.read_text(encoding="utf-8"))

        self.assertEqual(result.generated_capability_count, 2)
        by_field = {
            capability["learned_provenance"]["cloud_field_id"]: capability
            for capability in profile_raw["capabilities"]
        }
        energy = by_field["cltd_energy_saving_mode"]
        self.assertEqual(energy["value_kind"], "bool")
        self.assertEqual(energy["enum_map"], {"0": "disable", "1": "encode"})
        self.assertEqual(energy["command_map"], {"0": "TDI", "1": "TEI"})
        self.assertEqual(energy["read_key"], "g_ascii_setting_energy_saving_mode")

        output_priority = by_field["cltd_set_output_priority"]
        self.assertEqual(output_priority["command"], "OPR")
        self.assertEqual(output_priority["read_key"], "g_ascii_setting_output_priority")
        self.assertEqual(
            output_priority["command_map"],
            {"12336": "OPR00", "12337": "OPR01"},
        )
        self.assertEqual(
            schema_raw["learned_write_commands"],
            ["OPR00", "OPR01", "TDI", "TEI"],
        )

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
            self.assertEqual(str(session.get("cloud_sn") or ""), "E50000200000000001000001")
            self.assertEqual(int(session.get("devcode", 0)), 2376)
            self.assertEqual(int(session.get("devaddr", 0)), 1)

            reset_capability = _find_capability(profile_raw, "_690")
            self.assertEqual(str(reset_capability.get("value_kind") or ""), "action")
            self.assertTrue(bool(reset_capability.get("requires_confirm")))
            self.assertTrue(bool(reset_capability.get("unsafe_while_running")))
            provenance = reset_capability.get("learned_provenance")
            self.assertIsInstance(provenance, dict)
            assert isinstance(provenance, dict)
            self.assertEqual(str(provenance.get("source") or ""), "cloud_shadow_learning")
            self.assertEqual(str(provenance.get("scope") or ""), "device")
            self.assertEqual(str(provenance.get("safety_class") or ""), "destructive_action")
            self.assertTrue(bool(str(provenance.get("evidence_hash") or "")))

    def test_rerun_after_activation_rebases_to_builtin_base(self) -> None:
        # Regression: once a learned overlay is activated, the runtime reports it as
        # the effective profile/schema name. Re-running discovery fed those learned
        # names back as the source, and the generator re-wrapped the schema name in
        # ``builtin:`` -> a non-existent install-dir path -> FileNotFoundError, which
        # surfaced as "0 controls found" and a failed support export. The generator
        # must rebase the source names to the built-in base the overlay derives from.
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            first = generate_shadow_learning_overlay_drafts(
                config_dir=config_dir,
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
            )
            learned_profile_name = str(first.manifest["output"]["profile_name"])
            learned_schema_name = str(first.manifest["output"]["schema_name"])
            self.assertIn("learned/shadow_learning/", learned_schema_name)

            # Mirror the deployed runtime: the freshly written overlay files become
            # resolvable via the external config-dir roots.
            set_external_profile_roots((local_profiles_root(config_dir),))
            set_external_register_schema_roots((local_register_schemas_root(config_dir),))
            clear_profile_loader_cache()
            clear_register_schema_loader_cache()
            try:
                # Loader-level rebase resolves a learned overlay back to its base.
                self.assertEqual(
                    builtin_base_schema_name(learned_schema_name),
                    "modbus_smg/models/smg_6200.json",
                )
                self.assertEqual(
                    builtin_base_profile_name(learned_profile_name),
                    "smg_modbus.json",
                )

                second = generate_shadow_learning_overlay_drafts(
                    config_dir=config_dir,
                    source_profile_name=learned_profile_name,
                    source_schema_name=learned_schema_name,
                    session_manifest=dict(
                        _sample_session_manifest(),
                        session_id="smg-shadow-session-02",
                    ),
                    correlation=_sample_correlation_payload(),
                )
            finally:
                set_external_profile_roots(())
                set_external_register_schema_roots(())
                clear_profile_loader_cache()
                clear_register_schema_loader_cache()

            second_profile_raw = json.loads(second.profile_path.read_text(encoding="utf-8"))
            second_schema_raw = json.loads(second.schema_path.read_text(encoding="utf-8"))

            # The re-run overlay extends the built-in base, never the prior overlay.
            self.assertEqual(
                str(second_schema_raw.get("extends") or ""),
                "builtin:modbus_smg/models/smg_6200.json",
            )
            self.assertEqual(
                str(second_schema_raw.get("draft_of") or ""),
                "modbus_smg/models/smg_6200.json",
            )
            self.assertEqual(
                str(second_profile_raw.get("extends") or ""),
                "smg_modbus.json",
            )
            self.assertEqual(
                str(second_profile_raw.get("draft_of") or ""),
                "smg_modbus.json",
            )
            # Output names keep clean built-in stems without accumulating session tokens.
            self.assertEqual(second.schema_path.name, "smg_6200_smg_shadow_session_02.json")
            self.assertEqual(second.profile_path.name, "smg_modbus_smg_shadow_session_02.json")
            self.assertEqual(second.generated_capability_count, 2)

    def test_generated_overlay_defines_every_capability_group(self) -> None:
        # Regression: learned capabilities use the "config" group, but the
        # overlay did not define it (base SMG profiles use output/charging/
        # battery/system). Activating the overlay then failed profile validation
        # (unknown_group_for_capability) and bricked the integration on every
        # coordinator update. The overlay must define every group it references.
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
            )
            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            group_keys = {str(group.get("key")) for group in profile_raw.get("groups", [])}
            self.assertIn("config", group_keys)
            capabilities = profile_raw.get("capabilities", [])
            self.assertTrue(capabilities)
            for capability in capabilities:
                self.assertIn(
                    str(capability.get("group")),
                    group_keys,
                    f"capability {capability.get('key')} references an undefined group",
                )

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


def _capability(**overrides: object) -> dict[str, object]:
    capability: dict[str, object] = {
        "key": "learned_sample_700",
        "title": "Sample setting",
        "register": 700,
        "value_kind": "bool",
        "learned_provenance": {
            "cloud_field_id": "sys_eybond_ctrl_700",
            "confidence": "high",
            "safety_class": "setting",
            "evidence_hash": "deadbeef",
        },
    }
    provenance_overrides = overrides.pop("learned_provenance", None)
    capability.update(overrides)
    if isinstance(provenance_overrides, dict):
        provenance = dict(capability["learned_provenance"])  # type: ignore[arg-type]
        provenance.update(provenance_overrides)
        capability["learned_provenance"] = provenance
    return capability


class LearnedControlRiskClassifierTests(unittest.TestCase):
    def test_normal_bool_control_is_enabled_by_default(self) -> None:
        result = classify_learned_control_risk(
            _capability(title="Backlight Control", value_kind="bool")
        )
        self.assertEqual(result["risk_level"], RISK_NORMAL)
        self.assertEqual(result["reasons"], [])
        self.assertTrue(result["enabled_by_default"])

    def test_bounded_numeric_control_is_normal(self) -> None:
        result = classify_learned_control_risk(
            _capability(value_kind="u16", minimum=10, maximum=60)
        )
        self.assertEqual(result["risk_level"], RISK_NORMAL)
        self.assertTrue(result["enabled_by_default"])

    def test_enum_with_meaningful_labels_is_normal(self) -> None:
        result = classify_learned_control_risk(
            _capability(value_kind="enum", enum_map={2: "Line", 5: "Battery"})
        )
        self.assertEqual(result["risk_level"], RISK_NORMAL)
        self.assertTrue(result["enabled_by_default"])

    def test_name_keyword_alone_no_longer_disables_control(self) -> None:
        # Only destructive ACTIONS are high-risk now; a scary name on an ordinary (non-action)
        # control no longer disables it -- the user opted to pre-check everything but clear/reset
        # actions.
        for title in ("Reset user parameters", "Factory settings", "Reboot device"):
            with self.subTest(title=title):
                result = classify_learned_control_risk(
                    _capability(title=title, value_kind="bool")
                )
                self.assertNotEqual(result["risk_level"], RISK_HIGH)
                self.assertTrue(result["enabled_by_default"])

    def test_non_destructive_action_is_enabled_by_default(self) -> None:
        # A one-shot action that is NOT destructive (no clear/reset/...) is no longer auto
        # high-risk -- e.g. "Forced EQ Charging", "Exit Fault Mode" -- so it is pre-checked.
        result = classify_learned_control_risk(
            _capability(title="Sync clock", value_kind="action")
        )
        self.assertNotEqual(result["risk_level"], RISK_HIGH)
        self.assertTrue(result["enabled_by_default"])

    def test_destructive_action_safety_class_is_high_risk(self) -> None:
        result = classify_learned_control_risk(
            _capability(
                title="Reset user settings",
                value_kind="action",
                learned_provenance={"safety_class": "destructive_action"},
            )
        )
        self.assertEqual(result["risk_level"], RISK_HIGH)
        self.assertIn("destructive_action", result["reasons"])
        self.assertFalse(result["enabled_by_default"])

    def test_unknown_value_kind_is_uncertain_but_enabled(self) -> None:
        # Uncertain controls are still surfaced as uncertain, but now pre-checked (only
        # high-risk destructive actions are left unchecked).
        result = classify_learned_control_risk(
            _capability(value_kind="mystery_kind")
        )
        self.assertEqual(result["risk_level"], RISK_UNCERTAIN)
        self.assertIn("unknown_value_kind", result["reasons"])
        self.assertTrue(result["enabled_by_default"])

    def test_numeric_without_bounded_range_is_uncertain(self) -> None:
        result = classify_learned_control_risk(
            _capability(value_kind="u16", minimum=20, maximum=20)
        )
        self.assertEqual(result["risk_level"], RISK_UNCERTAIN)
        self.assertIn("numeric_without_bounded_range", result["reasons"])

    def test_enum_without_meaningful_labels_is_uncertain(self) -> None:
        result = classify_learned_control_risk(
            _capability(value_kind="enum", enum_map={2: "2", 5: "5"})
        )
        self.assertEqual(result["risk_level"], RISK_UNCERTAIN)
        self.assertIn("enum_without_labels", result["reasons"])

    def test_weak_or_missing_correlation_is_uncertain(self) -> None:
        for confidence in ("low", "", "none"):
            with self.subTest(confidence=confidence):
                result = classify_learned_control_risk(
                    _capability(
                        value_kind="bool",
                        learned_provenance={"confidence": confidence},
                    )
                )
                self.assertEqual(result["risk_level"], RISK_UNCERTAIN)
                self.assertIn("weak_correlation", result["reasons"])

    def test_classifier_is_deterministic(self) -> None:
        capability = _capability(title="Factory reset", value_kind="action")
        first = classify_learned_control_risk(capability)
        second = classify_learned_control_risk(capability)
        self.assertEqual(first, second)
        self.assertEqual(first["reasons"], sorted(first["reasons"]))

    def test_malformed_capability_is_treated_as_uncertain(self) -> None:
        result = classify_learned_control_risk(None)  # type: ignore[arg-type]
        self.assertEqual(result["risk_level"], RISK_UNCERTAIN)
        self.assertFalse(result["enabled_by_default"])


class LearnedControlClassificationFromFieldDefTests(unittest.TestCase):
    """Classify learned controls from the SmartESS field definition (labels + option count)."""

    @staticmethod
    def _group(field_name: str, options: list[tuple[str, int]]) -> dict:
        # options: (smartess label, first observed register value), in sweep order.
        samples = [
            {
                "sequence_index": index,
                "requested_value": str(index),
                "value_label": label,
                "value_source": "choice",
                "observation": {"values": [observed]},
            }
            for index, (label, observed) in enumerate(options)
        ]
        return {"field_name": field_name, "field_id": "", "samples": samples}

    def test_multi_option_choice_is_select_with_real_labels(self) -> None:
        # Output Voltage: SmartESS enum 220/230/240 -> select keyed by the register value, NOT a
        # numeric field; the read-back value (e.g. 2300) maps to "230Vac".
        result = self._group(
            "Output Voltage", [("220Vac", 2200), ("230Vac", 2300), ("240Vac", 2400)]
        )
        classified = _classify_learned_control(result)
        self.assertEqual(classified["value_kind"], "enum")
        self.assertEqual(
            classified["enum_map"], {2200: "220Vac", 2300: "230Vac", 2400: "240Vac"}
        )

    def test_two_non_on_off_options_are_a_select_not_numeric(self) -> None:
        # Output Frequency: 50Hz/60Hz are two options but not on/off -> select, not a number.
        classified = _classify_learned_control(
            self._group("Output Frequency", [("50Hz", 5000), ("60Hz", 6000)])
        )
        self.assertEqual(classified["value_kind"], "enum")
        self.assertEqual(classified["enum_map"], {5000: "50Hz", 6000: "60Hz"})

    def test_on_off_pair_is_a_switch_with_labels(self) -> None:
        classified = _classify_learned_control(
            self._group("Beeps", [("Beeps OFF", 0), ("Beeps ON", 1)])
        )
        self.assertEqual(classified["value_kind"], "bool")
        self.assertEqual(classified["enum_map"], {0: "Beeps OFF", 1: "Beeps ON"})

    def test_single_option_choice_is_a_button(self) -> None:
        # Forced EQ Charging / Exit Fault Mode: one momentary option -> action (button), not an
        # input field. action_value is the value to write.
        classified = _classify_learned_control(
            self._group("Forced EQ Charging", [("Forced EQ Charging Once", 1)])
        )
        self.assertEqual(classified["value_kind"], "action")
        self.assertEqual(classified["action_value"], 1)

    @staticmethod
    def _numeric_group(field_name: str, written: str, observed: int) -> dict:
        return {
            "field_name": field_name,
            "field_id": "",
            "samples": [
                {
                    "sequence_index": 0,
                    "requested_value": written,
                    "value_label": "",
                    "value_source": "numeric",
                    "observation": {"values": [observed]},
                }
            ],
        }

    def test_numeric_field_derives_scaled_divisor_from_observed_write(self) -> None:
        # Wrote displayed 56.0 -> observed register 560 -> divisor 10 (scaled number).
        classified = _classify_learned_control(
            self._numeric_group("Bulk Charge Voltage", "56.0", 560)
        )
        self.assertEqual(classified["value_kind"], "scaled_u16")
        self.assertEqual(classified["divisor"], 10)

    def test_integer_numeric_field_is_unscaled_u16(self) -> None:
        classified = _classify_learned_control(
            self._numeric_group("Max Charge Current", "1", 1)
        )
        self.assertEqual(classified["value_kind"], "u16")
        self.assertNotIn("divisor", classified)


class LearnedControlReviewModelTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch(
            "custom_components.eybond_local.support.shadow_learning_overlay_generator."
            "_EMIT_BUILTIN_DUPLICATE_CONTROLS",
            False,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_default_label_prefers_smartess_title(self) -> None:
        self.assertEqual(
            default_learned_control_label(field_name="  Backlight   Control "),
            "Backlight Control",
        )
        self.assertEqual(
            default_learned_control_label(field_id="sys_eybond_ctrl_53"),
            "sys eybond ctrl 53",
        )
        self.assertEqual(
            default_learned_control_label(register=705),
            "Discovered control 705",
        )

    def test_review_model_partitions_enabled_and_excluded(self) -> None:
        capabilities = [
            _capability(
                key="learned_backlight_705",
                title="Backlight Control",
                register=705,
                value_kind="bool",
            ),
            _capability(
                key="learned_reset_690",
                title="Reset user parameters",
                register=690,
                value_kind="action",
                learned_provenance={"safety_class": "destructive_action"},
            ),
        ]
        model = build_learned_control_review_model(capabilities)

        self.assertEqual(model["kind"], "learned_control_review_model")
        self.assertEqual(model["counts"]["learned_all"], 2)
        self.assertEqual(model["counts"]["enabled_by_default"], 1)
        self.assertEqual(model["counts"]["excluded_by_policy"], 1)
        self.assertEqual(model["enabled_by_default"], ["learned_backlight_705"])

        excluded = model["excluded_by_policy"]
        self.assertEqual(len(excluded), 1)
        self.assertEqual(excluded[0]["key"], "learned_reset_690")
        self.assertEqual(excluded[0]["risk_level"], RISK_HIGH)
        self.assertIn("destructive_action", excluded[0]["reasons"])

        labels = {entry["key"]: entry["default_label"] for entry in model["learned_all"]}
        self.assertEqual(labels["learned_backlight_705"], "Backlight Control")
        self.assertEqual(labels["learned_reset_690"], "Reset user parameters")

    def test_review_model_preserves_every_discovered_control(self) -> None:
        capabilities = [
            _capability(key=f"learned_ctrl_{index}", register=700 + index)
            for index in range(5)
        ]
        model = build_learned_control_review_model(capabilities)
        self.assertEqual(
            [entry["key"] for entry in model["learned_all"]],
            [f"learned_ctrl_{index}" for index in range(5)],
        )

    def test_overlay_manifest_embeds_review_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = generate_shadow_learning_overlay_drafts(
                config_dir=Path(temp_dir),
                source_profile_name="smg_modbus.json",
                source_schema_name="modbus_smg/models/smg_6200.json",
                session_manifest=_sample_session_manifest(),
                correlation=_sample_correlation_payload(),
            )

            profile_raw = json.loads(result.profile_path.read_text(encoding="utf-8"))
            overlay = profile_raw.get("shadow_learning_overlay")
            assert isinstance(overlay, dict)
            review_model = overlay.get("review_model")
            self.assertIsInstance(review_model, dict)
            assert isinstance(review_model, dict)

            # Backlight (bool) is enabled; Reset (destructive action) is excluded.
            self.assertEqual(review_model["counts"]["learned_all"], 2)
            self.assertEqual(review_model["counts"]["enabled_by_default"], 1)
            self.assertEqual(review_model["counts"]["excluded_by_policy"], 1)

            learned_all = review_model["learned_all"]
            by_register = {int(entry["register"]): entry for entry in learned_all}
            self.assertIn(705, by_register)
            self.assertIn(690, by_register)

            backlight = by_register[705]
            self.assertEqual(backlight["risk_level"], RISK_NORMAL)
            self.assertTrue(backlight["enabled_by_default"])
            self.assertEqual(backlight["default_label"], "Backlight Control")

            reset = by_register[690]
            self.assertEqual(reset["risk_level"], RISK_HIGH)
            self.assertFalse(reset["enabled_by_default"])
            self.assertEqual(reset["default_label"], "Reset user parameters")
            self.assertTrue(reset["exclusion_reasons"])
            self.assertIn("destructive_action", reset["exclusion_reasons"])




def _fake_schema(*, specs=None, measurements=None, enum_tables=None):
    spec_sets = {}
    for set_name, registers in (specs or {}).items():
        spec_sets[set_name] = tuple(
            SimpleNamespace(register=reg, key=f"{set_name}_{reg}") for reg in registers
        )
    return SimpleNamespace(
        blocks=(
            SimpleNamespace(key="status", start=100, count=10),
            SimpleNamespace(key="live", start=201, count=34),
            SimpleNamespace(key="config", start=300, count=44),
        ),
        spec_sets=spec_sets,
        measurement_descriptions=tuple(
            SimpleNamespace(key=key, name=name) for key, name in (measurements or {}).items()
        ),
        enum_tables=enum_tables or {},
    )


def _numeric_binding(register, title, *, unit="V", divisor=10, decimals=1, signed=False):
    return {
        "title": title,
        "unit": unit,
        "status": "unique",
        "decimals": decimals,
        "candidates": [{"register": register, "divisor": divisor, "signed": signed}],
    }


class LearnedReadOverlayTests(unittest.TestCase):
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)

    def test_out_of_block_register_routes_to_aux_config_with_metadata(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": [201]}),
            read_bindings={"bindings": [_numeric_binding(404, "Some Aux Voltage")]},
            read_enum_bindings={"bindings": []},
        )

        self.assertEqual(result["generated"][0]["register"], 404)
        self.assertEqual(result["generated"][0]["spec_set"], "aux_config")
        fragment = result["schema_fragment"]
        spec = fragment["spec_sets"]["aux_config"][0]
        self.assertEqual(spec, {"key": "learned_read_404", "register": 404, "divisor": 10, "decimals": 1})
        measurement = fragment["measurement_descriptions"][0]
        self.assertEqual(measurement["unit"], "V")
        self.assertEqual(measurement["device_class"], "voltage")
        self.assertEqual(measurement["suggested_display_precision"], 1)
        self.assertEqual(measurement["state_class"], "measurement")
        self.assertEqual(fragment["learned_read_registers"], [404])

    def test_in_block_register_routes_to_its_polled_spec_set(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}),
            read_bindings={"bindings": [_numeric_binding(214, "Aux Live Reading")]},
            read_enum_bindings={"bindings": []},
        )

        self.assertEqual(result["generated"][0]["spec_set"], "live")
        self.assertIn("live", result["schema_fragment"]["spec_sets"])

    def test_already_decoded_register_is_skipped(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": [215]}),
            read_bindings={"bindings": [_numeric_binding(215, "Battery Voltage")]},
            read_enum_bindings={"bindings": []},
        )

        self.assertEqual(result["generated"], [])
        self.assertEqual(result["skipped"][0]["reason"], "register_already_decoded")

    def test_already_titled_measurement_is_skipped(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}, measurements={"pv_voltage": "PV Voltage"}),
            read_bindings={"bindings": [_numeric_binding(404, "PV Voltage")]},
            read_enum_bindings={"bindings": []},
        )

        self.assertEqual(result["skipped"][0]["reason"], "title_already_mapped")

    def test_signed_and_unscaled_specs_render_minimally(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}),
            read_bindings={
                "bindings": [
                    _numeric_binding(404, "Battery Current", unit="A", divisor=10, decimals=1, signed=True),
                    _numeric_binding(405, "Load Percent", unit="%", divisor=1, decimals=0),
                ]
            },
            read_enum_bindings={"bindings": []},
        )

        specs = {spec["register"]: spec for spec in result["schema_fragment"]["spec_sets"]["aux_config"]}
        self.assertTrue(specs[404]["signed"])
        self.assertEqual(specs[404]["divisor"], 10)
        self.assertNotIn("divisor", specs[405])  # divisor 1 omitted
        measurements = {m["key"]: m for m in result["schema_fragment"]["measurement_descriptions"]}
        self.assertEqual(measurements["learned_read_405"]["unit"], "%")
        self.assertNotIn("device_class", measurements["learned_read_405"])  # % has no device class

    def test_unique_enum_binding_emits_enum_spec_and_text_measurement(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}),
            read_bindings={"bindings": []},
            read_enum_bindings={
                "bindings": [
                    {
                        "title": "Working State",
                        "status": "unique",
                        "candidates": [{"register": 460, "enum_table": "mode_names"}],
                    }
                ]
            },
        )

        spec = result["schema_fragment"]["spec_sets"]["aux_config"][0]
        self.assertEqual(spec, {"key": "learned_read_460", "register": 460, "enum_table": "mode_names"})
        measurement = result["schema_fragment"]["measurement_descriptions"][0]
        self.assertEqual(measurement["name"], "Working State")
        self.assertNotIn("unit", measurement)

    def test_known_title_gets_canonical_presentation_even_without_cloud_unit(self) -> None:
        # An out-of-block register labeled "Battery Voltage" but with NO cloud unit:
        # the semantic catalog still supplies device_class/unit/translation_key.
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}),
            read_bindings={
                "bindings": [
                    {
                        "title": "Battery Voltage",
                        "unit": "",
                        "status": "unique",
                        "decimals": 1,
                        "candidates": [{"register": 460, "divisor": 10, "signed": False}],
                    }
                ]
            },
            read_enum_bindings={"bindings": []},
        )

        measurement = result["schema_fragment"]["measurement_descriptions"][0]
        self.assertEqual(measurement["device_class"], "voltage")
        self.assertEqual(measurement["unit"], "V")
        self.assertEqual(measurement["translation_key"], "battery_voltage")

    def test_unknown_title_falls_back_to_unit_device_class(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}),
            read_bindings={
                "bindings": [
                    {
                        "title": "Mystery Reading",
                        "unit": "A",
                        "status": "unique",
                        "decimals": 1,
                        "candidates": [{"register": 460, "divisor": 10, "signed": False}],
                    }
                ]
            },
            read_enum_bindings={"bindings": []},
        )

        measurement = result["schema_fragment"]["measurement_descriptions"][0]
        self.assertEqual(measurement["device_class"], "current")
        self.assertNotIn("translation_key", measurement)

    def test_force_unsupported_disables_read_dedup(self) -> None:
        # With the validation toggle on, a read whose register is already decoded
        # by the builtin schema is still materialized (no dedup), so the learning
        # flow can be exercised end to end on a supported device.
        binding = {
            "title": "Battery Voltage",
            "unit": "V",
            "status": "unique",
            "decimals": 1,
            "candidates": [{"register": 215, "divisor": 10, "signed": False}],
        }
        schema = _fake_schema(specs={"live": [215]})  # 215 already decoded

        deduped = _build_learned_read_overlay(
            schema=schema,
            read_bindings={"bindings": [binding]},
            read_enum_bindings={"bindings": []},
        )
        self.assertEqual(deduped["generated"], [])  # normally skipped

        with mock.patch(
            "custom_components.eybond_local.support.shadow_learning_overlay_generator."
            "force_unsupported_models",
            return_value=True,
        ):
            forced = _build_learned_read_overlay(
                schema=schema,
                read_bindings={"bindings": [binding]},
                read_enum_bindings={"bindings": []},
            )
        self.assertEqual(len(forced["generated"]), 1)
        self.assertEqual(forced["generated"][0]["register"], 215)

    def test_non_unique_bindings_are_not_emitted(self) -> None:
        result = _build_learned_read_overlay(
            schema=_fake_schema(specs={"live": []}),
            read_bindings={
                "bindings": [
                    {"title": "X", "status": "ambiguous", "candidates": [{"register": 404, "divisor": 1}]},
                    {"title": "Y", "status": "no_match", "candidates": []},
                    {"title": "Z", "status": "skipped_zero", "candidates": []},
                ]
            },
            read_enum_bindings={"bindings": []},
        )

        self.assertEqual(result["generated"], [])
        self.assertEqual(result["schema_fragment"], {})


if __name__ == "__main__":
    unittest.main()
