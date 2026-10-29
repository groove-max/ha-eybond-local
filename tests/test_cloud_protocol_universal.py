"""Protocol-independent detection, read-only probes and intercepted preparation."""

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.drivers.registry import iter_drivers
from custom_components.eybond_local.link_models import EybondLinkRoute
from custom_components.eybond_local.models import DetectedInverter
from custom_components.eybond_local.payload.modbus import build_read_request, build_write_single_request
from custom_components.eybond_local.payload.pi30 import build_request as pi30
from custom_components.eybond_local.payload.pi18 import build_request as pi18
from custom_components.eybond_local.runtime.protocol_check import (
    ReadOnlyProtocolTransport, check_protocol_profiles, selected_probe_target,
)
from custom_components.eybond_local.support.shadow_learning.backend import (
    ShadowLearningSeed, build_shadow_learning_preflight, InProcessShadowLearningHandler,
)
from custom_components.eybond_local.support.shadow_learning.protocol_evidence import (
    CloudProtocolEvidence, read_protocols, protocol_sample,
)
from custom_components.eybond_local.support.shadow_learning.proxy import (
    InProcessFailClosedShadowProxyHandler, route_status_indicates_control_write_ready,
)


def frame(payload):
    return build_collector_request(15, payload, devcode=1, collector_addr=255, fcode=4)


def seed(driver="modbus_smg"):
    return ShadowLearningSeed(
        session_id="protocol-inspection", entry_id="synthetic", collector_pn="E5000020000000",
        collector_cloud_profile_key="", collector_cloud_profile_label="", collector_cloud_profile_source="",
        collector_cloud_profile_confidence="", collector_callback_endpoint="127.0.0.1,8899,TCP",
        effective_metadata_snapshot={"driver_key": driver, "profile_name": "synthetic.json",
                                     "register_schema_name": "synthetic.json"},
        command_responses={}, register_bank={300: 10}, inverter_target=(1, 255), inspect_cloud_protocol=True,
        inverter_device_addr=5,
    )


class ProtocolEvidenceTests(unittest.TestCase):
    def test_export_masking_preserves_validated_read_evidence(self):
        from custom_components.eybond_local.support.masking import mask_numeric_identifiers

        evidence = CloudProtocolEvidence(1, 255, build_read_request(5, 4501, 45))
        exported = mask_numeric_identifiers({"cloud_protocol_evidence": evidence.to_mapping()})
        self.assertEqual(CloudProtocolEvidence.from_mapping(exported["cloud_protocol_evidence"]), evidence)
        # This is wire-byte handling, not an exemption for encoded identifiers.
        identifier = b"92632500000001"
        masked = mask_numeric_identifiers({"request_hex": identifier.hex()})
        self.assertNotIn(identifier, bytes.fromhex(masked["request_hex"]))

    def test_all_registered_drivers_declare_wire_contracts(self):
        for driver in iter_drivers("auto"):
            self.assertTrue(driver.cloud_probe_codecs, driver.key)
            self.assertIs(type(driver.cloud_learning_supported), bool)

    def test_read_codecs_are_distinct_and_ambiguous_commands_are_not_guessed(self):
        cases = ((build_read_request(5, 4501, 14), "modbus_rtu"), (pi30("QPIGS"), "pi30_ascii"),
                 (pi18("^P005GS"), "pi18_ascii"), (b"GBAT\r", "eybond_g_ascii"),
                 (b"MD\x01\r", "eybond_short_ascii"), (b"PV1\r", "eybond_09c1"))
        for payload, codec in cases:
            self.assertEqual(set(read_protocols(payload)), {codec})
            evidence = CloudProtocolEvidence.from_frame(frame(payload))
            self.assertEqual(CloudProtocolEvidence.from_mapping(evidence.to_mapping()), evidence)
        self.assertGreater(len(read_protocols(b"F\r")), 1)
        for payload in (build_write_single_request(5, 300, 1), pi30("POP02"), b"AT+PASSWORD=secret\r",
                        pi30("QPIGS")[:-2] + b"x\r", b"x" * 513):
            self.assertFalse(read_protocols(payload))
            self.assertIsNone(CloudProtocolEvidence.from_frame(frame(payload)))

    def test_unknown_payload_is_bounded_redacted_diagnostic_not_probe_authority(self):
        sample = protocol_sample(b"password=not-for-support", direction="cloud_to_collector", carrier="frame")
        self.assertEqual(sample["status"], "unknown")
        self.assertNotIn("request_hex", sample)
        self.assertNotIn("not-for-support", json.dumps(sample))
        self.assertIsNone(CloudProtocolEvidence.from_mapping({**sample, "protocols": ["pi30_ascii"]}))

    def test_inspection_is_not_learning_admission_for_an_unsupported_profile(self):
        candidate = replace(seed("pi18"), register_bank={})
        self.assertTrue(build_shadow_learning_preflight(candidate).can_start)
        backend = InProcessShadowLearningHandler(seed=candidate, output_path=Path("unused"))
        self.assertEqual(backend.confirm_cloud_protocol(frame(pi18("^P005GS"))), "cloud_protocol_learning_unsupported")
        self.assertIsNotNone(backend.cloud_protocol_evidence)
        self.assertFalse(backend.observations_since(0))

    def test_same_codec_different_unit_cannot_reuse_seed(self):
        backend = InProcessShadowLearningHandler(seed=seed(), output_path=Path("unused"))
        self.assertEqual(backend.confirm_cloud_protocol(frame(build_read_request(6, 300, 1))),
                         "cloud_protocol_route_mismatch")

    def test_raw_cloud_read_requires_matching_codec_unit_and_existing_seed(self):
        for candidate, payload, expected in (
            (seed(), build_read_request(5, 300, 1), "ready"),
            (seed(), build_read_request(6, 300, 1), "cloud_protocol_route_mismatch"),
            (replace(seed(), inverter_device_addr=None), build_read_request(5, 300, 1), "cloud_protocol_route_mismatch"),
            (replace(seed(), register_bank={}), build_read_request(5, 300, 1), "cloud_protocol_seed_missing"),
            (seed(), pi30("QPIGS"), "cloud_protocol_mismatch"),
            (seed(), build_write_single_request(5, 300, 1), "cloud_protocol_unrecognized"),
        ):
            with self.subTest(expected=expected):
                backend = InProcessShadowLearningHandler(seed=candidate, output_path=Path("unused"))
                self.assertEqual(backend.confirm_raw_cloud_protocol(payload), expected)
                self.assertIsNone(backend.cloud_protocol_evidence)  # no invented wrapper


class GenericProfileTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_driver_is_discovered_from_contract_without_checker_allowlist(self):
        evidence = CloudProtocolEvidence(1, 255, pi30("QPIGS"))
        target = evidence.target("pi30_ascii")
        inverter = DetectedInverter("new_driver", "future_family", "Synthetic", "", target,
            profile_name="synthetic", register_schema_name="synthetic", details={"battery_voltage": 24.5})
        driver = SimpleNamespace(key="new_driver", cloud_probe_codecs=("pi30_ascii",), cloud_learning_supported=False,
            probe_read_plans=lambda target: (), async_probe=AsyncMock(return_value=inverter), support_marker=lambda **kw: None)
        with patch("custom_components.eybond_local.runtime.protocol_check.iter_drivers", return_value=(driver,)):
            result = await check_protocol_profiles(object(), evidence, {"battery_voltage": 24.5})
        self.assertEqual(result["candidates"][0]["driver_key"], "new_driver")
        self.assertFalse(result["candidates"][0]["learning_supported"])

    async def test_pi30_guard_blocks_writes_other_codecs_and_routes(self):
        evidence = CloudProtocolEvidence(1, 255, pi30("QPIGS"))
        transport = SimpleNamespace(async_send_payload=AsyncMock(return_value=b"reply"))
        guarded = ReadOnlyProtocolTransport(transport, evidence.target("pi30_ascii"), codec="pi30_ascii")
        route = EybondLinkRoute(devcode=1, collector_addr=255)
        await guarded.async_send_payload(pi30("QPIGS"), route=route)
        for payload, candidate_route in ((pi30("POP02"), route), (build_read_request(1, 300, 1), route),
                (pi30("QPIGS"), EybondLinkRoute(devcode=2, collector_addr=255))):
            with self.assertRaises(ValueError):
                await guarded.async_send_payload(payload, route=candidate_route)
        self.assertEqual(transport.async_send_payload.await_count, 1)
        selected = {"driver_key": "pi30", "codec": "pi30_ascii", "devcode": 1, "collector_addr": 255, "device_addr": 0}
        self.assertEqual(selected_probe_target(selected, "pi30"), evidence.target("pi30_ascii"))
        self.assertIsNone(selected_probe_target(selected, "smartess_local"))


class GenericPreparationPumpTests(unittest.IsolatedAsyncioTestCase):
    async def test_mismatch_unknown_and_ambiguous_frames_stay_off_device(self):
        for payload, expected in ((pi30("QPIGS"), "cloud_protocol_mismatch"),
                                  (b"password=not-for-support", "cloud_protocol_unrecognized"),
                                  (b"F\r", "cloud_protocol_ambiguous")):
            with self.subTest(reason=expected):
                await self._exchange(payload, expected)

    async def test_matching_modbus_read_qualifies_without_physical_forward(self):
        await self._exchange(build_read_request(5, 300, 1), "")

    async def test_raw_modbus_read_qualifies_without_physical_forward(self):
        await self._exchange(build_read_request(5, 300, 1), "", raw=True)
        await self._exchange(build_read_request(6, 300, 1), "cloud_protocol_route_mismatch", raw=True)

    async def test_raw_unit_change_after_preparation_is_not_seed_authority(self):
        await self._exchange(build_read_request(5, 300, 1), "", raw=True,
                             next_read=build_read_request(6, 300, 1))

    async def test_evidence_sample_is_not_completed_protocol_preparation(self):
        await self._exchange(b"F\r", "cloud_protocol_ambiguous", pause_evidence=True)
        await self._exchange(build_read_request(5, 300, 1), "", pause_evidence=True)

    async def _exchange(self, payload, error, raw=False, next_read=None, pause_evidence=False):
        connections = asyncio.Queue()
        release = asyncio.Event()
        evidence_started = asyncio.Event()
        evidence_release = asyncio.Event()

        async def cloud(reader, writer):
            await connections.put((reader, writer))
            await release.wait()
            writer.close()
            await writer.wait_closed()

        cloud_server = await asyncio.start_server(cloud, "127.0.0.1", 0)
        with tempfile.TemporaryDirectory() as directory:
            handler = InProcessFailClosedShadowProxyHandler(seed=seed(), upstream_host="127.0.0.1",
                upstream_port=cloud_server.sockets[0].getsockname()[1], output_path=Path(directory) / "trace.jsonl",
                expected_session_protocol="eybond_framed")
            if pause_evidence:
                append_event = handler._append_event

                async def delayed_evidence(event, direction, details):
                    if event == "shadow_protocol_evidence":
                        evidence_started.set()
                        await evidence_release.wait()
                    await append_event(event, direction, details)

                handler._append_event = delayed_evidence
            await handler.start()
            server = await asyncio.start_server(handler.handle_client, "127.0.0.1", 0)
            try:
                with patch("custom_components.eybond_local.support.shadow_learning.proxy.async_verify_shadow_socket",
                           new=AsyncMock(return_value=("synthetic_verified_pn", b""))):
                    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
                    cloud_reader, cloud_writer = await asyncio.wait_for(connections.get(), 1)
                    packet = payload if raw else frame(payload)
                    cloud_writer.write(packet[:5])
                    await cloud_writer.drain()
                    self.assertFalse(route_status_indicates_control_write_ready(handler.status()))
                    cloud_writer.write(packet[5:])
                    await cloud_writer.drain()
                    if pause_evidence:
                        await asyncio.wait_for(evidence_started.wait(), 1)
                        self.assertTrue(handler.status()["protocol_diagnostics"]["samples"])
                        self.assertTrue(handler.status()["protocol_preparation_pending"])
                        self.assertFalse(route_status_indicates_control_write_ready(handler.status()))
                        evidence_release.set()
                    # Recording a sample yields before protocol classification.
                    # Wait for its outcome, not the earlier diagnostics update.
                    async with asyncio.timeout(2):
                        while True:
                            status = handler.status()
                            if status["protocol_preparation_error"] or not status["protocol_preparation_pending"]:
                                break
                            await asyncio.sleep(.005)
                    self.assertTrue(handler.status()["protocol_diagnostics"]["samples"])
                    self.assertEqual(handler.status()["protocol_preparation_error"], error)
                    self.assertEqual(route_status_indicates_control_write_ready(handler.status()), not bool(error))
                    self.assertNotIn("not-for-support", json.dumps(handler.status()))
                    with self.assertRaises(TimeoutError):
                        await asyncio.wait_for(reader.read(1), .03)
                    if not error:
                        self.assertTrue(await asyncio.wait_for(cloud_reader.read(512), 1))
                    if next_read is not None:
                        cloud_writer.write(next_read)
                        await cloud_writer.drain()
                        async with asyncio.timeout(1):
                            while not handler.status()["protocol_preparation_error"]:
                                await asyncio.sleep(.005)
                        self.assertEqual(handler.status()["protocol_preparation_error"], "cloud_protocol_route_mismatch")
                        self.assertFalse(route_status_indicates_control_write_ready(handler.status()))
                        with self.assertRaises(TimeoutError):
                            await asyncio.wait_for(reader.read(1), .03)
                    writer.close()
                    await writer.wait_closed()
            finally:
                evidence_release.set()
                release.set()
                server.close()
                cloud_server.close()
                await server.wait_closed()
                await cloud_server.wait_closed()
                await handler.stop()
