from __future__ import annotations

import ast
import asyncio
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.metadata.effective_metadata_snapshot import (
    EffectiveMetadataSnapshot,
)
from custom_components.eybond_local.payload.modbus import (
    build_read_holding_request,
    build_write_multiple_request,
)
from custom_components.eybond_local.runtime.link import EybondRuntimeLinkManager
from custom_components.eybond_local.support.shadow_learning_backend import (
    InProcessShadowLearningHandler,
    ShadowLearningSeed,
    build_shadow_learning_preflight,
    build_shadow_learning_seed,
)
from custom_components.eybond_local.support.shadow_learning_protocol import (
    EybondGAsciiShadowLearningAdapter,
    ModbusRtuShadowLearningAdapter,
    resolve_shadow_learning_protocol_adapter,
)


def _sample_snapshot() -> EffectiveMetadataSnapshot:
    return EffectiveMetadataSnapshot(
        effective_owner_key="modbus_smg",
        effective_owner_name="Modbus SMG",
        variant_key="default",
        profile_name="modbus_smg/default.json",
        register_schema_name="modbus_smg/default.json",
        confidence="high",
        generation=1,
        generated_at="2026-06-05T12:00:00+00:00",
    )


def _sample_raw_capture() -> dict[str, object]:
    return {
        "capture_kind": "generic_register_dump",
        "responses": {"WFSS": "1"},
        "captures": [
            {
                "driver_key": "modbus_smg",
                "fixture_ranges": [
                    {"start": 300, "count": 2, "values": [1, 2]},
                    {"start": 305, "count": 1, "values": [10]},
                ],
                "range_failures": [],
            }
        ],
    }


class ShadowLearningBackendTests(unittest.TestCase):
    def test_coordinator_exposes_shadow_learning_lifecycle_methods(self) -> None:
        coordinator_path = REPO_ROOT / "custom_components/eybond_local/runtime/coordinator.py"
        tree = ast.parse(coordinator_path.read_text(encoding="utf-8"))

        class_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondLocalCoordinator"
        )
        method_names = {
            node.name
            for node in class_node.body
            if isinstance(node, ast.AsyncFunctionDef)
        }

        self.assertIn("async_start_shadow_learning", method_names)
        self.assertIn("async_stop_shadow_learning", method_names)

    def test_shadow_trace_start_paths_do_not_block_event_loop_with_file_open(self) -> None:
        for relative_path, class_name in (
            ("support/shadow_learning_backend.py", "InProcessShadowLearningHandler"),
            ("support/shadow_learning_proxy.py", "InProcessFailClosedShadowProxyHandler"),
        ):
            module_path = REPO_ROOT / "custom_components/eybond_local" / relative_path
            tree = ast.parse(module_path.read_text(encoding="utf-8"))
            class_node = next(
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            )
            start_node = next(
                node
                for node in class_node.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == "start"
            )
            blocking_calls = [
                node
                for node in ast.walk(start_node)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in {"mkdir", "open"}
            ]
            self.assertEqual(blocking_calls, [], relative_path)

    def test_json_line_writer_does_not_write_or_flush_directly_in_async_write(self) -> None:
        module_path = REPO_ROOT / "custom_components/eybond_local/support/collector_cloud_proxy.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        class_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "JsonLineWriter"
        )
        write_node = next(
            node
            for node in class_node.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "write"
        )
        blocking_calls = [
            node
            for node in ast.walk(write_node)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"write", "flush"}
        ]
        self.assertEqual(blocking_calls, [])

    def test_seed_builder_uses_raw_capture_and_synthesizes_required_at_responses(self) -> None:
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.command_responses["CLDSRVHOST1"], "192.168.1.50,18899,TCP")
        self.assertEqual(seed.command_responses["QID"], "E5000020000000")
        self.assertEqual(seed.command_responses["WFSS"], "1")
        self.assertEqual(seed.register_bank[300], 1)
        self.assertEqual(seed.register_bank[301], 2)
        self.assertEqual(seed.register_bank[305], 10)

    def test_seed_builder_reports_explicit_register_blocker_when_seed_is_missing(self) -> None:
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=None,
            write_response_mode="exception",
        )

        self.assertEqual(seed.register_bank, {})
        self.assertIn("missing_register_seed", blockers)
        self.assertEqual(build_shadow_learning_preflight(seed).blockers, blockers)

    def test_partial_tier_snapshot_without_profile_is_not_blocked(self) -> None:
        # Partial / unidentified devices (the family-default tier learning
        # targets) bind a base register schema but no controls profile. The
        # preflight must NOT raise missing_effective_metadata_snapshot for them.
        family_snapshot = EffectiveMetadataSnapshot(
            effective_owner_key="modbus_smg",
            effective_owner_name="Modbus SMG",
            variant_key="default",
            profile_name="",
            register_schema_name="modbus_smg/base.json",
            confidence="medium",
            generation=1,
            generated_at="2026-06-05T12:00:00+00:00",
        )
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=family_snapshot,
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertNotIn("missing_effective_metadata_snapshot", blockers)
        self.assertEqual(blockers, ())
        self.assertTrue(build_shadow_learning_preflight(seed).can_start)

    def test_empty_persisted_snapshot_is_blocked(self) -> None:
        # The raw PERSISTED snapshot is empty for a partial-tier device (it never
        # persists one). Passing it straight through is the bug that blocked the
        # learning start: the coordinator must fall back to live metadata first
        # (coordinator.shadow_learning_effective_metadata) before seeding.
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=EffectiveMetadataSnapshot(),
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertIn("missing_effective_metadata_snapshot", blockers)

    def test_missing_cloud_profile_does_not_block(self) -> None:
        # The collector cloud profile is manifest metadata only. A collector that
        # reports no protocol asset (e.g. firmware 8.50.18.3) returns empty
        # key/label, but the device is still fully learnable, so the scan must NOT
        # be blocked on it.
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260615T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="",
            collector_cloud_profile_label="",
            collector_cloud_profile_source="",
            collector_cloud_profile_confidence="",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertNotIn("missing_collector_cloud_profile", blockers)
        self.assertEqual(blockers, ())
        self.assertTrue(build_shadow_learning_preflight(seed).can_start)

    def test_explicit_non_modbus_protocol_is_blocked_fail_closed(self) -> None:
        snapshot = {
            "effective_owner_key": "pi30_ascii",
            "effective_owner_name": "PI30 ASCII",
            "variant_key": "default",
            "profile_name": "pi30_ascii/base.json",
            "register_schema_name": "pi30_ascii/base.json",
            "protocol_family": "pi30_ascii",
            "confidence": "high",
        }

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260615T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=snapshot,
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertEqual(seed.protocol_adapter_key, "unsupported")
        self.assertIn("unsupported_shadow_learning_protocol:pi30_ascii", blockers)
        self.assertFalse(build_shadow_learning_preflight(seed).can_start)

    def test_modbus_protocol_resolves_learning_adapter(self) -> None:
        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260615T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.protocol_adapter_key, "modbus_rtu")

    def test_eybond_g_ascii_protocol_resolves_learning_adapter_without_register_seed(self) -> None:
        snapshot = {
            "effective_owner_key": "eybond_g_ascii",
            "effective_owner_name": "EyeBond G-ASCII",
            "variant_key": "default",
            "profile_name": "eybond_g_ascii/base.json",
            "register_schema_name": "eybond_g_ascii/base.json",
            "protocol_family": "eybond_g_ascii",
            "driver_key": "eybond_g_ascii",
            "confidence": "high",
        }

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260625T120000Z",
            entry_id="entry-1",
            collector_pn="A0000000000001",
            collector_cloud_family="valuecloud_at",
            collector_cloud_profile_key="valuecloud_at",
            collector_cloud_profile_label="ValueCloud AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=snapshot,
            raw_capture={
                "runtime": {
                    "values": {
                        "eybond_g_ascii_gdat0_fields": "B 0 5 4003 0 00 219.7 50.01",
                        "eybond_g_ascii_gpv_fields": "183.1 027.6 00.43 05.31 00972",
                    }
                }
            },
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.protocol_adapter_key, "eybond_g_ascii")
        self.assertEqual(seed.register_bank, {})
        self.assertIn("GPDAT0", seed.command_responses)
        self.assertTrue(build_shadow_learning_preflight(seed).can_start)

    def test_valuecloud_cloud_family_alone_does_not_select_g_ascii_learning(self) -> None:
        adapter = resolve_shadow_learning_protocol_adapter(
            {
                "effective_owner_key": "modbus_smg",
                "protocol_family": "modbus_smg",
                "driver_key": "modbus_smg",
                "register_schema_name": "modbus_smg/default.json",
            },
            collector_cloud_family="valuecloud_at",
        )

        self.assertIsInstance(adapter, ModbusRtuShadowLearningAdapter)

    def test_plain_line_g_ascii_runtime_evidence_selects_g_ascii_learning(self) -> None:
        adapter = resolve_shadow_learning_protocol_adapter(
            {
                "register_schema_name": "eybond_g_ascii/base.json",
            },
            collector_cloud_family="some_future_ascii_provider",
            raw_passthrough_frame_format="plain_line",
        )

        self.assertIsInstance(adapter, EybondGAsciiShadowLearningAdapter)

    def test_eybond_g_ascii_learning_read_commands_follow_command_schema(self) -> None:
        adapter = EybondGAsciiShadowLearningAdapter()

        for command in ("GBMS", "GPPV", "I", "CFG", "Q1", "FAN???", "GPID9"):
            with self.subTest(command=command):
                frame = f"{command}\r".encode("ascii")
                read_request = adapter.decode_read_request(frame)
                self.assertIsNotNone(read_request)
                self.assertEqual(read_request.command, command)
                self.assertIsNone(adapter.decode_write_request(frame))
                self.assertIsNone(
                    adapter.write_observation(
                        frame=frame,
                        devcode=None,
                        devaddr=None,
                        timestamp="2026-06-25T12:00:00+00:00",
                    )
                )

    def test_eybond_g_ascii_learning_still_classifies_unknown_control_as_write(self) -> None:
        adapter = EybondGAsciiShadowLearningAdapter()

        frame = b"SW230\r"

        self.assertIsNone(adapter.decode_read_request(frame))
        write_request = adapter.decode_write_request(frame)
        self.assertIsNotNone(write_request)
        self.assertEqual(write_request.command, "SW")
        self.assertEqual(write_request.value, "230")

    def test_eybond_g_ascii_seed_keeps_support_capture_command_responses(self) -> None:
        snapshot = {
            "register_schema_name": "eybond_g_ascii/base.json",
            "protocol_family": "eybond_g_ascii",
            "driver_key": "eybond_g_ascii",
        }

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260625T120000Z",
            entry_id="entry-1",
            collector_pn="A0000000000001",
            collector_cloud_family="valuecloud_at",
            collector_cloud_profile_key="valuecloud_at",
            collector_cloud_profile_label="ValueCloud AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=snapshot,
            raw_capture={
                "driver_key": "eybond_g_ascii",
                "responses": {
                    "GPDAT0": "(0 5 4003 0 00 219.7 49.95\r",
                    "GBMS": "(00000 65535 65535\r",
                    "GPPV": "(1 016.0 000.0\r",
                    "CFG": "NAK\r",
                },
                "failures": {},
            },
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.protocol_adapter_key, "eybond_g_ascii")
        self.assertEqual(seed.command_responses["GBMS"], "(00000 65535 65535\r")
        self.assertEqual(seed.command_responses["GPPV"], "(1 016.0 000.0\r")
        self.assertEqual(seed.command_responses["CFG"], "NAK\r")
        self.assertTrue(build_shadow_learning_preflight(seed).can_start)

    def test_must_register_protocol_resolves_modbus_learning_adapter(self) -> None:
        snapshot = {
            "effective_owner_key": "must_pv_ph18",
            "effective_owner_name": "MUST PV/PH18",
            "variant_key": "default",
            "profile_name": "must_pv_ph18/base.json",
            "register_schema_name": "must_pv_ph18/base.json",
            "protocol_family": "must_pv_ph18",
            "confidence": "high",
        }

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260615T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=snapshot,
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.protocol_adapter_key, "modbus_rtu")

    def test_legacy_binary_pi30_proxy_resolves_modbus_learning_adapter(self) -> None:
        # A PI30 4200 collector proxy dump from 2026-06-19 showed the legacy
        # Eybond cloud data plane carrying Modbus RTU-like FC_FORWARD_TO_DEVICE
        # payloads (reads around 4501/4546/5001/6030 and writes at 5004/6030).
        # Shadow learning observes the cloud-to-collector dialect, not the
        # local HA-to-inverter protocol, so the cloud family must override the
        # local pi30_ascii metadata for adapter selection.
        snapshot = {
            "effective_owner_key": "pi30",
            "effective_owner_name": "PI30-family runtime",
            "variant_key": "pi30_max",
            "profile_name": "pi30_ascii/models/pi30_max.json",
            "register_schema_name": "pi30_ascii/models/pi30_max.json",
            "protocol_family": "pi30",
            "confidence": "high",
        }

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260619T210946Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_family="legacy_binary",
            collector_cloud_profile_key="02ff_legacy_binary",
            collector_cloud_profile_label="02FF legacy binary",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=snapshot,
            raw_capture=_sample_raw_capture(),
            write_response_mode="exception",
        )

        self.assertEqual(blockers, ())
        self.assertEqual(seed.protocol_adapter_key, "modbus_rtu")

    def test_exception_mode_logs_write_without_mutating_register_bank(self) -> None:
        seed = ShadowLearningSeed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot().as_dict(),
            command_responses={"CLDSRVHOST1": "192.168.1.50,18899,TCP", "QID": "E5000020000000"},
            register_bank={300: 1, 301: 2},
            latest_support_evidence=_sample_raw_capture(),
            write_response_mode="exception",
            allow_ack_writes=False,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = InProcessShadowLearningHandler(
                seed=seed,
                output_path=Path(temp_dir) / "shadow.jsonl",
            )

            async def _run() -> None:
                await handler.start()
                frame = build_collector_request(
                    1,
                    build_write_multiple_request(1, 300, [7]),
                    devcode=2376,
                    collector_addr=1,
                    fcode=4,
                )
                response = await handler._handle_frame(frame, remote="192.168.1.15:50000")
                self.assertIsNotNone(response)
                assert response is not None
                self.assertEqual(handler.register_bank_snapshot[300], 1)
                self.assertEqual(handler.register_bank_snapshot[301], 2)
                await handler.stop()

            asyncio.run(_run())

            log_text = (Path(temp_dir) / "shadow.jsonl").read_text(encoding="utf-8")
            self.assertIn("shadow_modbus_write_observation", log_text)
            self.assertIn("shadow_modbus_write_response", log_text)
            self.assertIn('"protocol_adapter_key": "modbus_rtu"', log_text)

    def test_read_requests_accumulate_into_session_read_map(self) -> None:
        seed = ShadowLearningSeed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot().as_dict(),
            command_responses={"CLDSRVHOST1": "192.168.1.50,18899,TCP"},
            register_bank={300: 1, 301: 2, 302: 560},
            latest_support_evidence=_sample_raw_capture(),
            write_response_mode="exception",
            allow_ack_writes=False,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = InProcessShadowLearningHandler(
                seed=seed,
                output_path=Path(temp_dir) / "shadow.jsonl",
            )

            async def _run() -> None:
                await handler.start()
                frame = build_collector_request(
                    1,
                    build_read_holding_request(1, 300, 3),
                    devcode=2376,
                    collector_addr=1,
                    fcode=4,
                )
                # The same block polled twice: occurrences accumulate, distinct
                # samples do not duplicate.
                await handler._handle_frame(frame, remote="192.168.1.15:50000")
                await handler._handle_frame(frame, remote="192.168.1.15:50000")
                await handler.stop()

            asyncio.run(_run())

            read_map = handler.read_map
            self.assertEqual(read_map["read_blocks"], [[300, 3, 2]])
            self.assertEqual(read_map["registers"]["300"], [1])
            self.assertEqual(read_map["registers"]["301"], [2])
            self.assertEqual(read_map["registers"]["302"], [560])
            self.assertEqual(read_map["read_event_count"], 2)
            self.assertEqual(read_map["value_source"], "seed_bank")

    def test_eybond_g_ascii_reads_and_writes_are_handled_fail_closed(self) -> None:
        seed = ShadowLearningSeed(
            session_id="entry-1_20260625T120000Z",
            entry_id="entry-1",
            collector_pn="A0000000000001",
            collector_cloud_profile_key="valuecloud_at",
            collector_cloud_profile_label="ValueCloud AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot={
                "profile_name": "eybond_g_ascii/base.json",
                "register_schema_name": "eybond_g_ascii/base.json",
                "protocol_family": "eybond_g_ascii",
                "driver_key": "eybond_g_ascii",
            },
            command_responses={
                "CLDSRVHOST1": "192.168.1.50,18899,TCP",
                "GPDAT0": "B 0 5 4003 0 00 219.7 50.01",
                "GPV": "(183.1 027.6 00.43 05.31 00972\r",
            },
            register_bank={},
            latest_support_evidence=None,
            collector_cloud_family="valuecloud_at",
            protocol_adapter_key="eybond_g_ascii",
            write_response_mode="exception",
            allow_ack_writes=False,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = InProcessShadowLearningHandler(
                seed=seed,
                output_path=Path(temp_dir) / "shadow.jsonl",
            )

            async def _run() -> None:
                await handler.start()
                read_response = await handler._handle_ascii_frame(
                    b"GPDAT0\r",
                    remote="iot.eybond.com:18899",
                )
                write_response = await handler._handle_ascii_frame(
                    b"OPR01\r",
                    remote="iot.eybond.com:18899",
                )
                await handler.stop()
                self.assertEqual(read_response, b"(B 0 5 4003 0 00 219.7 50.01\r")
                self.assertEqual(write_response, b"NAK\r")

            asyncio.run(_run())

            observations = handler.write_observations
            self.assertEqual(len(observations), 1)
            self.assertEqual(observations[0].protocol, "eybond_g_ascii")
            self.assertEqual(observations[0].command, "OPR")
            self.assertEqual(observations[0].value, "01")
            read_map = handler.read_map
            self.assertEqual(read_map["ascii_commands"], [["GPDAT0", 1]])
            self.assertEqual(read_map["ascii_fields"]["GPDAT0"], ["B 0 5 4003 0 00 219.7 50.01"])
            self.assertEqual(read_map["value_source"], "seed_command_responses")

    def test_ack_mode_mutates_register_bank_and_returns_ack(self) -> None:
        seed = ShadowLearningSeed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot().as_dict(),
            command_responses={"CLDSRVHOST1": "192.168.1.50,18899,TCP", "QID": "E5000020000000"},
            register_bank={300: 1, 301: 2},
            latest_support_evidence=_sample_raw_capture(),
            write_response_mode="ack",
            allow_ack_writes=True,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = InProcessShadowLearningHandler(
                seed=seed,
                output_path=Path(temp_dir) / "shadow.jsonl",
            )

            async def _run() -> None:
                await handler.start()
                frame = build_collector_request(
                    1,
                    build_write_multiple_request(1, 300, [7]),
                    devcode=2376,
                    collector_addr=1,
                    fcode=4,
                )
                response = await handler._handle_frame(frame, remote="192.168.1.15:50000")
                self.assertIsNotNone(response)
                assert response is not None
                self.assertEqual(handler.register_bank_snapshot[300], 7)
                await handler.stop()

            asyncio.run(_run())

    def test_link_manager_shadow_route_lifecycle_isolated_from_proxy_capture(self) -> None:
        manager = EybondRuntimeLinkManager(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.15",
            tcp_port=18899,
            udp_port=18898,
            discovery_target="192.168.1.15",
            discovery_interval=30,
            heartbeat_interval=10,
        )

        events: list[tuple[str, object]] = []

        class _Handler:
            def __init__(self, *, upstream_host, upstream_port, seed, output_path) -> None:
                events.append(("handler_init", (upstream_host, upstream_port, output_path)))
                self.running = False
                self.ready = False

            async def start(self) -> None:
                self.running = True
                events.append(("handler_start", None))

            async def stop(self) -> None:
                self.running = False
                events.append(("handler_stop", None))

            async def handle_client(self, reader, writer) -> None:
                pass

            def status(self) -> dict[str, object]:
                return {
                    "running": self.running,
                    "collector_connected": False,
                    "upstream_connected": False,
                    "ready": self.ready,
                    "upstream_error": "",
                }

        class _Route:
            def __init__(self, **kwargs) -> None:
                events.append(("route_init", kwargs))

            async def start(self) -> None:
                events.append(("route_start", None))

            async def stop(self) -> None:
                events.append(("route_stop", None))

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
        )
        self.assertFalse(blockers)

        async def _run() -> None:
            with patch("custom_components.eybond_local.runtime.link.InProcessFailClosedShadowProxyHandler", _Handler), patch(
                "custom_components.eybond_local.runtime.link.SharedProxyCaptureRoute",
                _Route,
            ):
                await manager.async_start_shadow_learning_route(
                    collector_ip="192.168.1.15",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/shadow.jsonl"),
                    seed=seed,
                )
                self.assertTrue(manager.shadow_learning_route_running())
                await manager.async_stop_shadow_learning_route()
                self.assertFalse(manager.shadow_learning_route_running())

        asyncio.run(_run())

        self.assertEqual(
            [event for event, _ in events],
            ["handler_init", "handler_start", "route_init", "route_start", "route_stop", "handler_stop"],
        )
        route_kwargs = dict(events[2][1])
        self.assertEqual(route_kwargs["host"], "0.0.0.0")
        self.assertEqual(route_kwargs["port"], 502)
        self.assertEqual(route_kwargs["collector_ip"], "192.168.1.15")

    def test_shadow_start_blocks_when_proxy_capture_is_running(self) -> None:
        manager = EybondRuntimeLinkManager(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.15",
            tcp_port=18899,
            udp_port=18898,
            discovery_target="192.168.1.15",
            discovery_interval=30,
            heartbeat_interval=10,
        )
        stop_calls: list[str] = []

        class _ExistingProxyHandler:
            running = True

            async def stop(self) -> None:
                stop_calls.append("handler_stop")

        class _ExistingProxyRoute:
            async def stop(self) -> None:
                stop_calls.append("route_stop")

        manager._proxy_capture_handler = _ExistingProxyHandler()  # type: ignore[attr-defined]
        manager._proxy_capture_route = _ExistingProxyRoute()  # type: ignore[attr-defined]

        seed, blockers = build_shadow_learning_seed(
            session_id="entry-1_20260605T120000Z",
            entry_id="entry-1",
            collector_pn="E5000020000000",
            collector_cloud_profile_key="smartess_at",
            collector_cloud_profile_label="SmartESS AT",
            collector_cloud_profile_source="runtime_observed",
            collector_cloud_profile_confidence="high",
            collector_callback_endpoint="192.168.1.50,18899,TCP",
            effective_metadata_snapshot=_sample_snapshot(),
            raw_capture=_sample_raw_capture(),
        )
        self.assertFalse(blockers)

        async def _run() -> None:
            with self.assertRaisesRegex(RuntimeError, "proxy_capture_route_running"):
                await manager.async_start_shadow_learning_route(
                    collector_ip="192.168.1.15",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/shadow.jsonl"),
                    seed=seed,
                )

        asyncio.run(_run())
        self.assertEqual(stop_calls, [])

    def test_proxy_capture_start_blocks_when_shadow_learning_is_running(self) -> None:
        manager = EybondRuntimeLinkManager(
            server_ip="192.168.1.50",
            collector_ip="192.168.1.15",
            tcp_port=18899,
            udp_port=18898,
            discovery_target="192.168.1.15",
            discovery_interval=30,
            heartbeat_interval=10,
        )
        stop_calls: list[str] = []

        class _ExistingShadowHandler:
            running = True

            async def stop(self) -> None:
                stop_calls.append("handler_stop")

        class _ExistingShadowRoute:
            async def stop(self) -> None:
                stop_calls.append("route_stop")

        manager._shadow_learning_handler = _ExistingShadowHandler()  # type: ignore[attr-defined]
        manager._shadow_learning_route = _ExistingShadowRoute()  # type: ignore[attr-defined]

        async def _run() -> None:
            with self.assertRaisesRegex(RuntimeError, "shadow_learning_route_running"):
                await manager.async_start_proxy_capture_route(
                    collector_ip="192.168.1.15",
                    listen_port=502,
                    upstream_host="cloud.example",
                    upstream_port=18899,
                    output_path=Path("/tmp/proxy.jsonl"),
                    masked_endpoint="cloud.example,18899,TCP",
                    restore_trigger_path=Path("/tmp/proxy.restore"),
                )

        asyncio.run(_run())
        self.assertEqual(stop_calls, [])


if __name__ == "__main__":
    unittest.main()
