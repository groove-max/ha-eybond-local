"""Protocol mismatch evidence and non-mutating alternate-profile checks."""

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from custom_components.eybond_local.collector.protocol import build_collector_request
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
from custom_components.eybond_local.payload.modbus import (
    build_read_request, build_write_single_request, crc16_modbus, decode_read_request,
)
from custom_components.eybond_local.drivers.smartess_local import SmartEssLocalDriver
from custom_components.eybond_local.support.shadow_learning.protocol import resolve_shadow_learning_protocol_adapter
from custom_components.eybond_local.runtime.protocol_check import (
    ReadOnlyModbusTransport, check_modbus_profiles, selected_probe_target,
)
from custom_components.eybond_local.runtime.driver_detection import async_detect_inverter
from custom_components.eybond_local.support.shadow_learning.protocol_mismatch import CloudModbusRead


READ = CloudModbusRead(1, 255, 5, 3, 4563, 2)


class CloudReadTests(unittest.TestCase):
    def test_exact_wire_and_closed_mapping(self):
        frame = bytes.fromhex("16a70001000aff04050311d30002314a")
        self.assertEqual(CloudModbusRead.from_frame(frame), READ)
        self.assertEqual(CloudModbusRead.from_mapping(READ.to_mapping()), READ)
        for data in ({}, {**READ.to_mapping(), "count": True},
                     {**READ.to_mapping(), "count": 126},
                     {**READ.to_mapping(), "device_addr": 0},
                     {**READ.to_mapping(), "address": 65535}):
            self.assertIsNone(CloudModbusRead.from_mapping(data))
        for bad in (frame[:-1], frame + b"x", frame[:-1] + b"\x00"):
            self.assertIsNone(CloudModbusRead.from_frame(bad))
        for payload in (build_write_single_request(5, 4563, 1), build_read_request(0, 4563, 2),
                        build_read_request(5, 4563, 0)):
            self.assertIsNone(CloudModbusRead.from_frame(build_collector_request(
                1, payload, devcode=1, collector_addr=255, fcode=4,
            )))

    def test_route_selection_is_driver_scoped_not_automatic(self):
        raw = {**READ.to_mapping(), "driver_key": "modbus_smg"}
        self.assertEqual(selected_probe_target(raw, "modbus_smg"), READ.target)
        for hint in ("auto", "pi30", "srne_modbus", "unknown"):
            self.assertIsNone(selected_probe_target(raw, hint))
        with self.assertRaises(ValueError):
            selected_probe_target({**raw, "device_addr": "5"}, "modbus_smg")


class ProfileCheckTests(unittest.IsolatedAsyncioTestCase):
    async def test_reload_detection_uses_selected_route_even_for_signature(self):
        inverter = DetectedInverter("modbus_smg", "modbus_smg", "Synthetic", "", READ.target)
        driver = SimpleNamespace(
            key="modbus_smg", probe_targets=(ProbeTarget(1, 255, 1),), signature_timeout=1,
            probe_timeout=1, async_probe_signature=AsyncMock(return_value=True),
            async_probe=AsyncMock(return_value=inverter),
        )
        with patch("custom_components.eybond_local.runtime.driver_detection.iter_drivers", return_value=(driver,)):
            context = await async_detect_inverter(object(), driver_hint="modbus_smg", probe_target=READ.target)
            self.assertEqual(context.inverter.probe_target, READ.target)
            self.assertEqual(driver.async_probe_signature.call_args.args[1], READ.target)
            self.assertEqual(driver.async_probe.call_args.args[1], READ.target)
            driver.async_probe.reset_mock()
            with self.assertRaises(ValueError):
                await async_detect_inverter(object(), driver_hint="auto", probe_target=READ.target)
            driver.async_probe.assert_not_called()

    async def test_guard_blocks_writes_other_routes_and_raw_transport(self):
        transport = SimpleNamespace(async_send_payload=AsyncMock(return_value=b"reply"))
        guard = ReadOnlyModbusTransport(transport, READ.target)
        route = EybondLinkRoute(devcode=1, collector_addr=255)
        await guard.async_send_payload(build_read_request(5, 4563, 2), route=route)
        self.assertEqual(transport.async_send_payload.await_count, 1)
        for payload, candidate_route in (
            (build_write_single_request(5, 4563, 2), route),
            (build_read_request(1, 4563, 2), route),
            (build_read_request(5, 4563, 2), EybondLinkRoute(devcode=2, collector_addr=255)),
            (build_read_request(5, 4563, 2), RawSerialLinkRoute()),
            (build_read_request(5, 65535, 2), route),
        ):
            with self.assertRaises(ValueError):
                await guard.async_send_payload(payload, route=candidate_route)
        self.assertEqual(transport.async_send_payload.await_count, 1)

    async def test_catalog_probe_and_fresh_comparable_values_required(self):
        inverter = DetectedInverter("modbus_smg", "modbus_smg", "Synthetic", "", READ.target,
                                    profile_name="test", register_schema_name="test")
        driver = SimpleNamespace(key="modbus_smg", cloud_probe_codecs=("modbus_rtu",), cloud_learning_supported=True,
                                 probe_read_plans=lambda target: (),
                                 async_probe=AsyncMock(return_value=inverter),
                                 async_read_onboarding_values=AsyncMock(return_value={"battery_voltage": 27.2}),
                                 support_marker=lambda **kw: None)
        with patch("custom_components.eybond_local.runtime.protocol_check.iter_drivers", return_value=(driver,)):
            result = await check_modbus_profiles(object(), READ, {"battery_voltage": 27.1})
            self.assertEqual(result["candidates"][0]["route"]["device_addr"], 5)
            self.assertEqual(result["candidates"][0]["comparisons"], [
                {"key": "battery_voltage", "local": 27.1, "modbus": 27.2},
            ])
            driver.async_read_onboarding_values.return_value = {"battery_voltage": float("nan")}
            self.assertFalse((await check_modbus_profiles(object(), READ, {"battery_voltage": 27.1}))["candidates"])
            driver.async_probe.return_value = None
            self.assertFalse((await check_modbus_profiles(object(), READ, {}))["candidates"])

    async def test_cancellation_not_converted_to_no_match(self):
        driver = SimpleNamespace(key="modbus_smg", cloud_probe_codecs=("modbus_rtu",),
                                 probe_read_plans=lambda target: (),
                                 async_probe=AsyncMock(side_effect=asyncio.CancelledError))
        with patch("custom_components.eybond_local.runtime.protocol_check.iter_drivers", return_value=(driver,)):
            with self.assertRaises(asyncio.CancelledError):
                await check_modbus_profiles(object(), READ, {})

    async def test_0925_catalog_probe_with_captured_words_and_split_routes(self):
        # Raw wire words from the read-only bench run; data words are byte
        # swapped by the real 0925 driver, configuration words are not.
        words = {
            (255, 4501, 14): [1024, 777, 62465, 0, 768, 5377, 25600, 1024, 0, 777, 62465, 0, 0, 0],
            (255, 4517, 12): [56320, 22045, 0, 256, 26640, 26640, 58880, 4608, 61440, 58880, 62465, 4608],
            (5, 5004, 1): [0],
        }
        calls = []

        async def send(payload, *, route, request_timeout=None):
            request = decode_read_request(payload)
            self.assertIsNotNone(request)
            self.assertEqual((route.devcode, request.slave_id, request.function_code), (1, 5, 3))
            key = (route.collector_addr, request.address, request.count)
            calls.append(key)
            values = words[key]
            body = bytes((5, 3, len(values) * 2)) + b"".join(value.to_bytes(2, "big") for value in values)
            return body + crc16_modbus(body).to_bytes(2, "little")

        with patch("custom_components.eybond_local.runtime.protocol_check.iter_drivers", return_value=(SmartEssLocalDriver(),)):
            result = await check_modbus_profiles(SimpleNamespace(async_send_payload=send), READ, {"battery_voltage": 27.7})
        self.assertEqual(len(result["candidates"]), 1)
        candidate = result["candidates"][0]
        self.assertEqual(candidate["driver_key"], "smartess_local")
        self.assertEqual(candidate["profile_name"], "smartess_local/models/0925.json")
        self.assertEqual(candidate["comparisons"], [{"key": "battery_voltage", "local": 27.7, "modbus": 27.7}])
        self.assertEqual(selected_probe_target(candidate["route"], "smartess_local"), READ.target)
        self.assertEqual(set(calls), set(words))
        self.assertEqual(len(calls), 3)  # No full settings poll after a successful identity probe.
        adapter = resolve_shadow_learning_protocol_adapter({"driver_key": "smartess_local"})
        self.assertTrue(adapter.supported)
        self.assertEqual(adapter.key, "modbus_rtu")

    async def test_split_route_allowance_does_not_authorize_other_config_reads_or_writes(self):
        transport = SimpleNamespace(async_send_payload=AsyncMock(return_value=b"reply"))
        guard = ReadOnlyModbusTransport(transport, READ.target,
            extra_reads=SmartEssLocalDriver().probe_read_plans(READ.target))
        config_route = EybondLinkRoute(devcode=1, collector_addr=5)
        await guard.async_send_payload(build_read_request(5, 5004, 1), route=config_route)
        for payload, route in (
            (build_write_single_request(5, 5004, 1), config_route),
            (build_read_request(5, 5005, 1), config_route),
            (build_read_request(5, 5004, 2), config_route),
            (build_read_request(6, 5004, 1), config_route),
            (build_read_request(5, 5004, 1), EybondLinkRoute(devcode=2, collector_addr=5)),
            (build_read_request(5, 5004, 1), EybondLinkRoute(devcode=1, collector_addr=6)),
        ):
            with self.assertRaises(ValueError):
                await guard.async_send_payload(payload, route=route)
        self.assertEqual(transport.async_send_payload.await_count, 1)
