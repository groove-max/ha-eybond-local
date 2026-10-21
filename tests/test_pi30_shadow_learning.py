"""Synthetic native PI30 evidence: no cloud account or hardware is required."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.protocol import HEADER_SIZE, build_collector_request
from custom_components.eybond_local.drivers.pi30 import Pi30Driver, _build_write_command
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.metadata.profile_loader import _parse_capability
from custom_components.eybond_local.payload.modbus import build_read_request
from custom_components.eybond_local.payload.pi30 import build_request, parse_response
from custom_components.eybond_local.support.shadow_learning.backend import (
    build_shadow_learning_seed, InProcessShadowLearningHandler, pi30_learning_target,
)
from custom_components.eybond_local.support.shadow_learning.pi30_protocol import (
    Pi30ShadowLearningAdapter, decode_pi30_learning_command,
)
from custom_components.eybond_local.support.shadow_learning.protocol import (
    resolve_shadow_learning_wire_adapter,
)
from custom_components.eybond_local.support.shadow_learning.overlay_generator import (
    generate_shadow_learning_overlay_drafts,
)
from custom_components.eybond_local.support.shadow_learning.proxy import (
    InProcessFailClosedShadowProxyHandler,
    route_status_indicates_control_ready, route_status_indicates_control_write_ready,
)
from custom_components.eybond_local.support.shadow_learning.runtime import ShadowLearningRouteStatus
from custom_components.eybond_local.support.shadow_learning.retry_evidence import qualify_retry_evidence


class CollectorMetadataQueryGrammarTests(unittest.TestCase):
    def test_only_bounded_non_sensitive_collector_reads_are_forwarded(self):
        from custom_components.eybond_local.support.shadow_learning.proxy import _is_collector_metadata_query

        def frame(payload, **kw):
            return build_collector_request(1, payload, **{
                "devcode": 1, "collector_addr": 255, "fcode": 2, **kw,
            })

        self.assertTrue(_is_collector_metadata_query(frame(bytes((1, 2, 4, 5, 12, 25, 26, 34, 65)))))
        for rejected in (
            frame(b""), frame(bytes(range(1, 34))), frame(b"\x02\x02"),
            frame(b"\x02\x2b"), frame(b"\x1d"), frame(b"\0"),
            frame(b"\x02", fcode=3), frame(b"\x02", devcode=0x994),
            frame(b"\x02", collector_addr=7), frame(b"\x02") + b"extra",
        ):
            with self.subTest(frame=rejected.hex()):
                self.assertFalse(_is_collector_metadata_query(rejected))


def seed(**changes):
    args = dict(
        session_id="native-pi30-test", entry_id="entry-native",
        collector_pn="SYNTHETIC-PN", collector_cloud_family="legacy_binary",
        collector_cloud_profile_key="", collector_cloud_profile_label="",
        collector_cloud_profile_source="endpoint_host",
        collector_cloud_profile_confidence="low", collector_callback_endpoint="",
        effective_metadata_snapshot={
            "effective_owner_key": "pi30", "protocol_family": "pi30",
            "profile_name": "pi30_ascii/models/pi30_max.json",
            "register_schema_name": "pi30_ascii/models/pi30_max.json",
        },
        raw_capture={"capture_kind": "pi30_ascii_dump", "responses": {"QFLAG": "EaDx"}},
        observed_cloud_request=build_request("QFLAG"),
        inverter_target=(0x0994, 0xFF),
    )
    args.update(changes)
    return build_shadow_learning_seed(**args)


def correlation(commands=("PDa", "PEa"), values=("0", "1"), labels=("Off", "On")):
    adapter = Pi30ShadowLearningAdapter()
    return {"matched": [
        {
            "sequence_index": index, "field_id": "native_test_option",
            "field_name": "Native learned option", "requested_value": value,
            "value_label": label, "value_source": "choice",
            "observation": adapter.write_observation(
                frame=build_request(command), devcode=0x0994, devaddr=0xFF,
            ).to_json_dict(),
        }
        for index, (command, value, label) in enumerate(zip(commands, values, labels))
    ]}


def overlay(captured, **changes):
    with tempfile.TemporaryDirectory() as directory:
        args = dict(
            config_dir=Path(directory),
            source_profile_name="pi30_ascii/models/pi30_max.json",
            source_schema_name="pi30_ascii/models/pi30_max.json",
            session_manifest={
                "session_id": "native-pi30-test", "collector_pn": "SYNTHETIC-PN",
                "devcode": 0x0994, "devaddr": 0xFF,
            },
            correlation=captured,
        )
        args.update(changes)
        result = generate_shadow_learning_overlay_drafts(**args)
        return json.loads(result.profile_path.read_text()), result


class Pi30LearningCodecTests(unittest.TestCase):
    def test_protected_retry_qualifies_exact_native_variants_not_command_as_target(self):
        captured = correlation()
        result = qualify_retry_evidence({"correlation": captured})
        self.assertEqual(result["correlation"]["matched_count"], 2)
        _, draft = overlay(result["correlation"])
        self.assertEqual(draft.generated_capability_count, 1)
        for change in (
            lambda data: data["matched"][0]["observation"].update(raw_payload_hex="00"),
            lambda data: data["matched"][0]["observation"].update(command="PEA"),
            lambda data: data["matched"][0]["observation"].update(devaddr=1),
            lambda data: data["matched"][0]["observation"].update(devcode=True),
            lambda data: data.update(unmatched_writes=[{"unexpected": True}]),
            lambda data: data.update(matched=[data["matched"][0]]),
        ):
            candidate = deepcopy(captured)
            change(candidate)
            self.assertEqual(qualify_retry_evidence(
                {"correlation": candidate})["correlation"]["matched_count"], 0)
        same_command = correlation(commands=("PEa", "PEa"))
        self.assertEqual(qualify_retry_evidence(
            {"correlation": same_command})["correlation"]["matched_count"], 0)
        result = qualify_retry_evidence({
            "correlation": captured,
            "results": [{"field_id": "native_test_option", "observation_count": 2}],
        })
        self.assertEqual(result["correlation"]["matched_count"], 0)

    def test_preparation_requires_real_local_data_and_bound_pi30_target(self):
        candidate, blockers = seed(observed_cloud_request=b"", prepare_native_pi30=True)
        self.assertFalse(blockers)
        self.assertTrue(candidate.prepare_native_pi30)
        self.assertEqual(candidate.protocol_adapter_key, "pi30_pending_cloud_proof")
        self.assertNotIn("QID", candidate.command_responses)
        for change in (
            {"inverter_target": None}, {"inverter_target": (1, True)},
            {"raw_capture": {"capture_kind": "pi30_ascii_dump", "responses": {"QFLAG": "NAK"}}},
            {"raw_capture": {"responses": {"QFLAG": "EaDx"}}},
            {"effective_metadata_snapshot": {"protocol_family": "modbus_smg"}},
        ):
            with self.subTest(change=change):
                rejected, blockers = seed(observed_cloud_request=b"", prepare_native_pi30=True, **change)
                self.assertFalse(rejected.prepare_native_pi30)
                self.assertTrue(blockers)
        missing_pn, blockers = seed(observed_cloud_request=b"", prepare_native_pi30=True, collector_pn="")
        self.assertIn("missing_collector_pn", blockers)
        self.assertTrue(missing_pn.prepare_native_pi30)
        inverter = DetectedInverter("pi30", "pi30", "synthetic", "", ProbeTarget(0x994, 255, 1))
        self.assertEqual(pi30_learning_target(inverter), (0x994, 255))
        self.assertIsNone(pi30_learning_target(None))

    def test_preparation_readiness_survives_typed_boundary(self):
        status = dict(running=True, collector_connected=True, collector_protocol_ingress=True,
                      route_protocol_activity=True, upstream_connected=True, ready=True,
                      protocol_preparation_pending=True)
        for candidate in (status, ShadowLearningRouteStatus.from_mapping(status).to_mapping()):
            self.assertFalse(route_status_indicates_control_ready(candidate))
            self.assertFalse(route_status_indicates_control_write_ready(candidate))
        status["protocol_preparation_pending"] = False
        self.assertTrue(route_status_indicates_control_write_ready(status))
        status["protocol_preparation_error"] = "cloud_payload_modbus_requires_register_seed"
        self.assertFalse(route_status_indicates_control_write_ready(
            ShadowLearningRouteStatus.from_mapping(status).to_mapping()))
        for malformed in ("false", None, 0):
            candidate = {**status, "protocol_preparation_error": "",
                         "protocol_preparation_pending": malformed}
            self.assertFalse(route_status_indicates_control_write_ready(
                ShadowLearningRouteStatus.from_mapping(candidate).to_mapping()))

    def test_only_known_crc_valid_read_for_exact_target_can_prepare(self):
        candidate, _ = seed(observed_cloud_request=b"", prepare_native_pi30=True)
        with tempfile.TemporaryDirectory() as directory:
            backend = InProcessShadowLearningHandler(seed=candidate, output_path=Path(directory) / "trace")
            for payload, devcode, addr, fc in (
                (build_request("PEa"), 0x994, 255, 4),
                (build_request("QUNKNOWN"), 0x994, 255, 4),
                (build_request("QID"), 0x994, 255, 4),
                (build_request("QFLAG"), 0x995, 255, 4),
                (build_request("QFLAG"), 0x994, 1, 4),
                (build_request("QFLAG"), 0x994, 255, 2),
                (b"QFLAGbad\r", 0x994, 255, 4),
            ):
                frame = build_collector_request(1, payload, devcode=devcode,
                                                collector_addr=addr, fcode=fc)
                self.assertEqual(backend.confirm_pi30_cloud_read(frame), "")
            modbus = build_collector_request(2, build_read_request(1, 400, 1),
                                             devcode=0x994, collector_addr=255, fcode=4)
            self.assertEqual(backend.confirm_pi30_cloud_read(modbus),
                             "cloud_payload_modbus_requires_register_seed")
            # Real dual-protocol devices may use a different cloud wrapper
            # and slave address. This proves a mismatch, not PI30 readiness.
            modbus = bytes.fromhex("16a70001000aff04050311d30002314a")
            self.assertEqual(backend.confirm_pi30_cloud_read(modbus),
                             "cloud_payload_modbus_requires_register_seed")
            self.assertEqual(backend.cloud_modbus_read.device_addr, 5)
            self.assertTrue(backend._seed.prepare_native_pi30)
            self.assertEqual(backend._seed.protocol_adapter_key, "pi30_pending_cloud_proof")
            frame = build_collector_request(3, build_request("QFLAG"), devcode=0x994,
                                            collector_addr=255, fcode=4)
            self.assertEqual(backend.confirm_pi30_cloud_read(frame), "ready")
            self.assertEqual(backend._protocol_adapter.key, "pi30_ascii")
            self.assertEqual(backend.write_observations, ())

    def test_exact_wire_commands_and_synthetic_replies(self):
        adapter = Pi30ShadowLearningAdapter()
        for command in ("PEa", "PEA", "PDa", "POP02", "PCVV27.9", "MUCHGC002"):
            frame = build_request(command)
            self.assertEqual(decode_pi30_learning_command(frame), command)
            request = adapter.decode_write_request(frame)
            self.assertEqual(request.command, command)
            self.assertEqual(parse_response(adapter.build_write_ack_response(request)), "ACK")
            self.assertEqual(parse_response(adapter.build_write_exception_response(
                request, exception_code=1)), "NAK")
            observation = adapter.write_observation(frame=frame, devcode=1, devaddr=0)
            self.assertEqual(observation.raw_payload_hex, frame.hex())
            self.assertEqual(observation.command, command)
            self.assertEqual(observation.values, ())
            self.assertEqual(observation.register, -1)

    def test_bad_crc_truncation_responses_and_collector_commands_rejected(self):
        valid = build_request("PEa")
        bad = [b"", b"PEa\r", valid + b"\n", build_request("(ACK"),
               build_request("AT+RST"), build_request("P" * 65),
               build_request("PEa\nPDa"), build_request(" PEa")]
        bad.extend(valid[:index] for index in range(len(valid)))
        bad.extend(valid[:index] + bytes([byte ^ 1]) + valid[index + 1:]
                   for index, byte in enumerate(valid))
        for frame in bad:
            with self.subTest(frame=frame):
                self.assertIsNone(decode_pi30_learning_command(frame))
                self.assertIsNone(Pi30ShadowLearningAdapter().decode_write_request(frame))

    def test_seeded_query_never_invents_values(self):
        adapter = Pi30ShadowLearningAdapter()
        request = adapter.decode_read_request(build_request("QFLAG"))
        self.assertIsNone(adapter.decode_write_request(build_request("QFLAG")))
        self.assertEqual(parse_response(adapter.build_seeded_read_response(
            request, [], {"QFLAG": "EaDx"})), "EaDx")
        for responses in ({}, {"QFLAG": "bad\nvalue"}, {"QFLAG": "é"}):
            self.assertEqual(parse_response(adapter.build_seeded_read_response(
                request, [], responses)), "NAK")
        self.assertFalse(adapter.has_command_seed({"QID": "SYNTHETIC-PN"}))
        self.assertFalse(adapter.has_command_seed({"QFLAG": "NAK"}))

    def test_wire_evidence_resolves_dialect_without_changing_legacy_fallback(self):
        self.assertEqual(resolve_shadow_learning_wire_adapter(build_request("QFLAG")).key,
                         "pi30_ascii")
        self.assertEqual(resolve_shadow_learning_wire_adapter(build_read_request(1, 400, 1)).key,
                         "modbus_rtu")
        native, blockers = seed()
        self.assertFalse(blockers)
        self.assertEqual(native.protocol_adapter_key, "pi30_ascii")
        self.assertEqual(native.register_bank, {})
        _, blockers = seed(inverter_target=None)
        self.assertEqual(blockers, ("missing_inverter_target",))
        _, blockers = seed(observed_cloud_request=b"")
        self.assertEqual(blockers, ("missing_register_seed",))
        _, blockers = seed(observed_cloud_request=b"corrupted\r")
        self.assertIn("unsupported_shadow_learning_protocol:unproven_cloud_payload", blockers)
        _, blockers = seed(raw_capture={"responses": {"QID": "SYNTHETIC-PN"}})
        self.assertEqual(blockers, ("missing_command_seed",))

    def test_framed_backend_records_exact_commands_without_device_io(self):
        async def run():
            native, blockers = seed()
            self.assertFalse(blockers)
            with tempfile.TemporaryDirectory() as directory:
                handler = InProcessShadowLearningHandler(
                    seed=native, output_path=Path(directory) / "trace.jsonl",
                )
                for command, expected in (("QFLAG", "EaDx"), ("QUNKNOWN", "NAK"),
                                          ("QID", "NAK"),
                                          ("PEa", "NAK")):
                    request = build_collector_request(
                        7, build_request(command), devcode=0x0994, collector_addr=0xFF,
                        fcode=4,
                    )
                    reply = await handler._handle_frame(request, remote="synthetic")
                    self.assertEqual(parse_response(reply[HEADER_SIZE:]), expected)
                observed, = handler.observations_since(0)
                self.assertEqual((observed.command, observed.devcode, observed.devaddr),
                                 ("PEa", 0x0994, 0xFF))
                request = build_collector_request(
                    8, build_request("PDa"), devcode=0x0994, collector_addr=0xFF, fcode=2,
                )
                self.assertIsNone(await handler._handle_frame(request, remote="synthetic"))
                self.assertEqual(len(handler.observations_since(0)), 1)
                wrong_target = build_collector_request(
                    9, build_request("PDa"), devcode=0x0994, collector_addr=1, fcode=4,
                )
                self.assertIsNone(await handler._handle_frame(wrong_target, remote="synthetic"))
                self.assertEqual(len(handler.observations_since(0)), 1)
                self.assertEqual(handler.register_bank_snapshot, {})
        asyncio.run(run())


class Pi30LearnedControlsTests(unittest.TestCase):
    def test_case_survives_overlay_profile_and_runtime_command_building(self):
        raw, result = overlay(correlation())
        self.assertEqual(result.generated_capability_count, 1)
        capability = _parse_capability(raw["capabilities"][0], {}, {}, {})
        self.assertEqual(capability.command_map, {0: "PDa", 1: "PEa"})
        self.assertEqual(_build_write_command(capability, 1), "PEa")
        self.assertEqual(_build_write_command(capability, 0), "PDa")
        self.assertTrue(capability.is_device_scoped_experimental)
        self.assertFalse(capability.tested)
        with self.assertRaises(ValueError):
            _build_write_command(capability, 2)

    def test_provider_codes_remain_enum_and_padding_is_preserved(self):
        raw, _ = overlay(correlation(("POP00", "POP02"), ("48", "49")))
        capability = _parse_capability(raw["capabilities"][0], {}, {}, {})
        self.assertEqual(capability.value_kind, "enum")
        self.assertEqual(_build_write_command(capability, 49), "POP02")

    def test_numeric_decimal_format_cloud_limits_and_unsampled_value(self):
        captured = correlation(("PCVV27.9", "PCVV28.9"), ("27.9", "28.9"))
        for sample in captured["matched"]:
            sample.update(value_source="numeric_probe", value_label="",
                          numeric_evidence={"kind": "number", "unit": "V"})
        raw, result = overlay(captured)
        self.assertEqual(result.generated_capability_count, 1)
        record = raw["capabilities"][0]
        self.assertNotIn("minimum", record)
        self.assertNotIn("maximum", record)
        self.assertNotIn("command_map", record)
        self.assertEqual((record["command"], record["command_precision"],
                          record["command_width"], record["divisor"]),
                         ("PCVV", 1, 4, 10))
        capability = _parse_capability(record, {}, {}, {})
        self.assertEqual(_build_write_command(capability, 285), "PCVV28.5")
        for sample in captured["matched"]:
            sample["numeric_evidence"].update(minimum="24.0", maximum="30.0",
                                               range_source="provider_hint")
        raw, _ = overlay(captured)
        self.assertEqual(raw["capabilities"][0]["minimum"], 240)
        self.assertEqual(raw["capabilities"][0]["maximum"], 300)

    def test_numeric_needs_consistent_encoding_and_multiple_values(self):
        for commands, values in (
            (("PCVV27.9", "PCVV28.9"), ("27.9", "29.9")),
            (("PCVV27.9", "PCVV027.9"), ("27.9", "27.9")),
            (("PCVV27.9", "PCVV27.9"), ("27.9", "27.9")),
        ):
            with self.subTest(commands=commands, values=values):
                captured = correlation(commands, values)
                for sample in captured["matched"]:
                    sample.update(value_source="numeric_probe",
                                  numeric_evidence={"kind": "number", "unit": "V"})
                _, result = overlay(captured)
                self.assertEqual(result.generated_capability_count, 0)

    def test_driver_executes_exact_learned_commands_and_does_not_retry_rejection(self):
        raw, _ = overlay(correlation())
        capability = _parse_capability(raw["capabilities"][0], {}, {}, {})
        inverter = DetectedInverter(
            "pi30", "pi30", "Synthetic", "SYNTHETIC-SN",
            ProbeTarget(0x0994, 0xFF, 0), capabilities=(capability,),
        )

        class Transport:
            def __init__(self, reply):
                self.reply = reply
                self.sent = []

            async def async_send_payload(self, payload, *, route):
                self.sent.append((payload, route))
                return build_request("(" + self.reply)

        async def run():
            transport = Transport("ACK")
            result = await Pi30Driver().async_write_capability(
                transport, inverter, capability.key, True,
            )
            self.assertTrue(result)
            self.assertEqual(transport.sent, [
                (build_request("PEa"), inverter.probe_target.link_route),
            ])
            transport = Transport("NAK")
            with self.assertRaises(Exception) as caught:
                await Pi30Driver().async_write_capability(
                    transport, inverter, capability.key, False,
                )
            self.assertIn("nak", str(caught.exception))
            self.assertEqual(len(transport.sent), 1)
        asyncio.run(run())

    def test_bad_route_crc_conflicting_mapping_and_numeric_are_not_activated(self):
        mutations = [
            ("pi30_command_route_mismatch", lambda s: s["observation"].update(devaddr=1)),
            ("pi30_command_evidence_invalid", lambda s: s["observation"].update(
                raw_payload_hex=build_request("PEA").hex())),
            ("pi30_command_evidence_invalid", lambda s: s["observation"].update(
                raw_payload_hex="not-hex")),
            ("pi30_numeric_encoding_not_proven", lambda s: s.update(value_source="numeric_probe")),
            ("mixed_command_protocols", lambda s: s["observation"].update(protocol="eybond_g_ascii")),
        ]
        for reason, mutate in mutations:
            with self.subTest(reason=reason):
                captured = deepcopy(correlation())
                mutate(captured["matched"][0])
                _, result = overlay(captured)
                self.assertEqual(result.generated_capability_count, 0)
                self.assertEqual(result.manifest["skipped_controls"][0]["reason"], reason)
        _, result = overlay(correlation(values=("0", "0")))
        self.assertEqual(result.generated_capability_count, 0)
        self.assertEqual(result.manifest["skipped_controls"][0]["reason"],
                         "pi30_command_mapping_conflict")


class Pi30ProxyLoopbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_automatic_preparation_gates_writes_and_reproves_each_socket(self):
        cloud_connections = asyncio.Queue()
        release_cloud = asyncio.Event()

        async def cloud(reader, writer):
            await cloud_connections.put((reader, writer))
            try:
                await release_cloud.wait()
            finally:
                writer.close()
                await writer.wait_closed()

        cloud_server = await asyncio.start_server(cloud, "127.0.0.1", 0)
        with tempfile.TemporaryDirectory() as directory:
            candidate, _ = seed(observed_cloud_request=b"", prepare_native_pi30=True)
            handler = InProcessFailClosedShadowProxyHandler(
                upstream_host="127.0.0.1", upstream_port=cloud_server.sockets[0].getsockname()[1],
                seed=candidate, output_path=Path(directory) / "trace",
                expected_session_protocol="eybond_framed",
            )
            await handler.start()
            server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            writers = []
            try:
                # Socket PN challenge is tested independently. Here exercise the
                # real pumps/parser with a verified connection and fragmented data.
                with patch("custom_components.eybond_local.support.shadow_learning.proxy.async_verify_shadow_socket",
                           new=AsyncMock(return_value=("synthetic_verified_pn", b""))):
                    for attempt in range(2):
                        reader, writer = await asyncio.open_connection(
                            "127.0.0.1", server.sockets[0].getsockname()[1])
                        writers.append(writer)
                        cloud_reader, cloud_writer = await asyncio.wait_for(cloud_connections.get(), 2)
                        # Real legacy clouds initiate registration with FC1.
                        # Permit only its exact clock/interval grammar, while
                        # keeping PI30 preparation and writes gated.
                        clock_query = build_collector_request(
                            10, bytes((26, 10, 20, 12, 30, 0, 0, 35)),
                            devcode=1, collector_addr=255, fcode=1,
                        )
                        cloud_writer.write(clock_query)
                        await cloud_writer.drain()
                        self.assertEqual(await asyncio.wait_for(reader.readexactly(len(clock_query)), 1), clock_query)
                        self.assertFalse(route_status_indicates_control_ready(handler.status()))
                        metadata_query = build_collector_request(
                            13, bytes((1, 2, 4, 5, 12, 25, 26, 34, 65)),
                            devcode=1, collector_addr=255, fcode=2,
                        )
                        cloud_writer.write(metadata_query)
                        await cloud_writer.drain()
                        self.assertEqual(await asyncio.wait_for(reader.readexactly(len(metadata_query)), 1), metadata_query)
                        self.assertFalse(route_status_indicates_control_ready(handler.status()))
                        heartbeat = build_collector_request(1, b"ping", devcode=0x994,
                                                            collector_addr=255, fcode=1)
                        writer.write(heartbeat)
                        await writer.drain()
                        self.assertEqual(await cloud_reader.readexactly(len(heartbeat)), heartbeat)
                        self.assertFalse(route_status_indicates_control_ready(handler.status()))
                        count = len(handler.backend.write_observations)
                        premature_write = build_collector_request(
                            2, build_request("PEa"), devcode=0x994, collector_addr=255, fcode=4)
                        malformed_clock = build_collector_request(
                            11, bytes((26, 13, 20, 12, 30, 0, 0, 35)),
                            devcode=1, collector_addr=255, fcode=1,
                        )
                        endpoint_write = build_collector_request(
                            12, b"\x15attacker.invalid", devcode=1, collector_addr=255, fcode=3,
                        )
                        private_query = build_collector_request(
                            14, bytes((2, 43)), devcode=1, collector_addr=255, fcode=2,
                        )
                        cloud_writer.write(malformed_clock + endpoint_write + private_query)
                        await cloud_writer.drain()
                        query = build_collector_request(
                            3, build_request("QFLAG"), devcode=0x994, collector_addr=255, fcode=4)
                        wrong_target = build_collector_request(
                            4, build_request("QFLAG"), devcode=0x995, collector_addr=255, fcode=4)
                        for byte in premature_write + wrong_target + query:
                            cloud_writer.write(bytes([byte]))
                            await cloud_writer.drain()
                            await asyncio.sleep(0)
                        reply = await asyncio.wait_for(
                            cloud_reader.readexactly(HEADER_SIZE + len(build_request("(EaDx"))), 2)
                        self.assertEqual(parse_response(reply[HEADER_SIZE:]), "EaDx")
                        self.assertTrue(route_status_indicates_control_write_ready(handler.status()))
                        self.assertEqual(len(handler.backend.write_observations), count)
                        cloud_writer.write(premature_write)
                        await cloud_writer.drain()
                        reply = await asyncio.wait_for(
                            cloud_reader.readexactly(HEADER_SIZE + len(build_request("(NAK"))), 2)
                        self.assertEqual(parse_response(reply[HEADER_SIZE:]), "NAK")
                        self.assertEqual(len(handler.backend.write_observations), count + 1)
                        cloud_writer.close()
                        await cloud_writer.wait_closed()
                        self.assertEqual(await asyncio.wait_for(reader.read(100), 2), b"")
                        writer.close()
                        await writer.wait_closed()
                        # Wait for owned cleanup, not just remote TCP EOF.
                        async with asyncio.timeout(2):
                            while handler._tasks:
                                await asyncio.sleep(0)
                        self.assertFalse(handler.ready)
            finally:
                for writer in writers:
                    writer.close()
                    await writer.wait_closed()
                release_cloud.set()
                server.close()
                await server.wait_closed()
                await handler.stop()
                cloud_server.close()
                await cloud_server.wait_closed()

    async def test_fragmented_native_cloud_writes_never_reach_collector(self):
        for framed in (False, True):
            with self.subTest(framed=framed):
                done = asyncio.get_running_loop().create_future()
                heartbeat = build_collector_request(1, b"ping", devcode=0x0994,
                                                    collector_addr=0xFF, fcode=1)
                request = build_request("PEa")
                if framed:
                    request = build_collector_request(2, request, devcode=0x0994,
                                                      collector_addr=0xFF, fcode=4)

                async def cloud(reader, writer):
                    try:
                        received = await reader.readexactly(len(heartbeat))
                        for byte in request:
                            writer.write(bytes([byte]))
                            await writer.drain()
                            await asyncio.sleep(0)
                        reply = await reader.readexactly(
                            len(build_request("(NAK")) + (HEADER_SIZE if framed else 0))
                        done.set_result((received, reply))
                    except Exception as exc:
                        if not done.done():
                            done.set_exception(exc)
                    finally:
                        writer.close()
                        await writer.wait_closed()

                cloud_server = await asyncio.start_server(cloud, "127.0.0.1", 0)
                with tempfile.TemporaryDirectory() as directory:
                    native, _ = seed()
                    handler = InProcessFailClosedShadowProxyHandler(
                        upstream_host="127.0.0.1",
                        upstream_port=cloud_server.sockets[0].getsockname()[1],
                        seed=native, output_path=Path(directory) / "capture.jsonl",
                    )
                    await handler.start()
                    server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
                    reader, writer = await asyncio.open_connection(
                        "127.0.0.1", server.sockets[0].getsockname()[1])
                    try:
                        writer.write(heartbeat)
                        await writer.drain()
                        received, reply = await asyncio.wait_for(done, 2)
                        self.assertEqual(received, heartbeat)
                        self.assertEqual(parse_response(reply[HEADER_SIZE:] if framed else reply),
                                         "NAK")
                        self.assertEqual(await asyncio.wait_for(reader.read(100), 1), b"")
                        observation, = handler._backend.observations_since(0)
                        self.assertEqual(observation.command, "PEa")
                    finally:
                        writer.close()
                        await writer.wait_closed()
                        server.close()
                        await server.wait_closed()
                        await handler.stop()
                        cloud_server.close()
                        await cloud_server.wait_closed()


if __name__ == "__main__":
    unittest.main()
