from __future__ import annotations

import asyncio
import socket
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
HELPERS_DIR = REPO_ROOT / "tests" / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))


from custom_components.eybond_local.collector.at import parse_at_response
from custom_components.eybond_local.collector.protocol import (
    FC_HEARTBEAT,
    HEADER_SIZE,
    decode_header,
    parse_heartbeat_pn,
)
from custom_components.eybond_local.collector.smartess_local import parse_query_collector_response
from custom_components.eybond_local.collector.transport import SharedEybondTransport
from custom_components.eybond_local.onboarding.eybond import DiscoveryTarget, OnboardingDetector
from custom_components.eybond_local.payload.modbus import (
    ModbusError,
    build_read_holding_request,
    parse_read_holding_response,
)
from custom_components.eybond_local.payload.pi30 import build_request, parse_response
from fake_collector import FakeCollectorService
from fake_collector_lib import (
    CollectorProfile,
    FC4_MODE_TIMEOUT,
    PRESET_COLLECTOR_ONLY,
    PRESET_MODBUS_SMG_READONLY,
    PRESET_SMARTESS_HINT,
    QUERY_MODE_TIMEOUT,
    build_at_reply,
    build_forward_response,
    build_query_collector_response,
    parse_discovery_redirect,
    resolve_scenario,
)


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _free_udp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class FakeCollectorLibTests(unittest.TestCase):
    def test_parse_discovery_redirect_accepts_newline_variant(self) -> None:
        redirect = parse_discovery_redirect(b"set>server=192.168.1.50:8899;\r\n")

        self.assertEqual(redirect.server_ip, "192.168.1.50")
        self.assertEqual(redirect.server_port, 8899)
        self.assertEqual(redirect.raw, "set>server=192.168.1.50:8899;")

    def test_query_collector_response_obeys_preset_and_timeout_knobs(self) -> None:
        collector_only = parse_query_collector_response(
            build_query_collector_response(
                5,
                resolve_scenario(
                    preset=PRESET_COLLECTOR_ONLY,
                    profile=CollectorProfile(mode=PRESET_COLLECTOR_ONLY),
                ),
            )
        )
        smartess_hint = parse_query_collector_response(
            build_query_collector_response(
                14,
                resolve_scenario(
                    preset=PRESET_SMARTESS_HINT,
                    profile=CollectorProfile(mode=PRESET_SMARTESS_HINT, protocol_descriptor="0942#fake"),
                ),
            )
        )
        dropped = build_query_collector_response(
            14,
            resolve_scenario(
                preset=PRESET_SMARTESS_HINT,
                profile=CollectorProfile(mode=PRESET_SMARTESS_HINT),
                query_14_mode=QUERY_MODE_TIMEOUT,
            ),
        )

        self.assertEqual(collector_only.code, 1)
        self.assertEqual(collector_only.parameter, 5)
        self.assertEqual(smartess_hint.code, 0)
        self.assertEqual(smartess_hint.parameter, 14)
        self.assertEqual(smartess_hint.text, "0942#fake")
        self.assertIsNone(dropped)

    def test_modbus_smg_readonly_forward_success_returns_register_data(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(
                mode=PRESET_MODBUS_SMG_READONLY,
                serial_number="SMG11K240001",
                rated_power=6200,
                protocol_number=1,
            ),
        )
        request = build_read_holding_request(slave_id=1, address=186, count=2)
        response = build_forward_response(request, scenario)

        assert response is not None
        values = parse_read_holding_response(response, slave_id=1, count=2)
        self.assertEqual(values[0], int.from_bytes(b"SM", "big"))
        self.assertEqual(values[1], int.from_bytes(b"G1", "big"))

    def test_negative_forward_payload_returns_modbus_exception_without_retryable_shape(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_COLLECTOR_ONLY,
            profile=CollectorProfile(mode=PRESET_COLLECTOR_ONLY),
        )
        request = build_read_holding_request(slave_id=1, address=100, count=3)
        response = build_forward_response(request, scenario)

        assert response is not None
        with self.assertRaisesRegex(ModbusError, "exception_code:1"):
            parse_read_holding_response(response, slave_id=1, count=3)

    def test_fc4_timeout_mode_drops_forward_response(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(mode=PRESET_MODBUS_SMG_READONLY),
            fc4_mode=FC4_MODE_TIMEOUT,
        )

        self.assertIsNone(build_forward_response(build_read_holding_request(1, 100, 1), scenario))

    def test_negative_forward_payload_returns_pi30_nak(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_COLLECTOR_ONLY,
            profile=CollectorProfile(mode=PRESET_COLLECTOR_ONLY),
        )
        request = build_request("QPI")
        response = build_forward_response(request, scenario)

        assert response is not None
        self.assertEqual(parse_response(response), "NAK")

    def test_build_at_reply_reflects_cloud_endpoint(self) -> None:
        response = build_at_reply(
            "CLDSRVHOST1",
            profile=CollectorProfile(),
            cloud_endpoint="192.168.1.50,18899,TCP",
        )

        parsed = parse_at_response(response)
        self.assertEqual(parsed.command, "CLDSRVHOST1")
        self.assertEqual(parsed.value, "192.168.1.50,18899,TCP")

    def test_build_at_reply_vdtu_is_empty_for_default_factory_profile(self) -> None:
        response = build_at_reply(
            "VDTU",
            profile=CollectorProfile(),
            cloud_endpoint="",
        )

        parsed = parse_at_response(response)
        self.assertEqual(parsed.command, "VDTU")
        self.assertEqual(parsed.value, "")

    def test_build_at_reply_vdtu_reflects_bridge_profile(self) -> None:
        bridge_reply = (
            "esp-collector,0.4.0;features=local_only,no_cloud,wifi_params;"
            "uart=2400,8,1,NONE;spacing_ms=100;queue=4"
        )
        response = build_at_reply(
            "VDTU",
            profile=CollectorProfile(vdtu=bridge_reply),
            cloud_endpoint="",
        )

        parsed = parse_at_response(response)
        self.assertEqual(parsed.command, "VDTU")
        self.assertEqual(parsed.value, bridge_reply)

    def test_modbus_smg_preset_uses_matching_transport_defaults(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(mode=PRESET_MODBUS_SMG_READONLY),
        )

        self.assertEqual(scenario.heartbeat_devcode, 0x0001)
        self.assertEqual(scenario.collector_addr, 0xFF)


class FakeCollectorServiceScenarioTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _force_patch = patch(
            "custom_components.eybond_local.metadata.device_catalog_loader."
            "FORCE_UNSUPPORTED_MODELS",
            False,
        )
        _force_patch.start()
        self.addCleanup(_force_patch.stop)

    async def _detect_with_scenario(
        self,
        scenario,
        *,
        request_timeout: float = 0.15,
        connect_timeout: float = 0.75,
        heartbeat_timeout: float = 0.2,
    ):
        tcp_port = _free_tcp_port()
        udp_port = _free_udp_port()
        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=udp_port,
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=0.05,
            connect_timeout=1.0,
            udp_reply="rsp>server=2;",
            scenario=scenario,
        )
        detector = OnboardingDetector(
            server_ip="127.0.0.1",
            tcp_port=tcp_port,
            udp_port=udp_port,
            request_timeout=request_timeout,
        )

        await service.start()
        try:
            return await detector._async_detect_target(
                DiscoveryTarget(ip="127.0.0.1", source="known_ip"),
                discovery_timeout=0.2,
                connect_timeout=connect_timeout,
                heartbeat_timeout=heartbeat_timeout,
            )
        finally:
            await service.stop()

    async def test_modbus_smg_readonly_preset_produces_positive_detection_match(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(
                mode=PRESET_MODBUS_SMG_READONLY,
                pn="E5000099990002",
                serial_number="SMG11K240001",
                model_name="SMG II 6200",
                rated_power=6200,
                protocol_number=1,
            ),
        )

        result = await self._detect_with_scenario(scenario)

        self.assertIsNotNone(result.match)
        self.assertEqual(result.next_action, "create_entry")
        self.assertEqual(result.match.driver_key, "modbus_smg")
        self.assertEqual(result.match.model_name, "SMG 6200")
        self.assertEqual(result.match.serial_number, "SMG11K240001")
        self.assertTrue(result.collector.connected)
        self.assertEqual(result.collector.collector.smartess_collector_version, "8.50.12.3")
        self.assertEqual(result.collector.collector.smartess_protocol_asset_id, "0942")

    async def test_first_heartbeat_delay_can_reproduce_warning_without_breaking_match(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(mode=PRESET_MODBUS_SMG_READONLY),
            first_heartbeat_delay=0.25,
        )

        result = await self._detect_with_scenario(
            scenario,
            request_timeout=0.15,
            connect_timeout=0.75,
            heartbeat_timeout=0.05,
        )

        self.assertIsNotNone(result.match)
        self.assertIn("collector_heartbeat_not_observed", result.warnings)

    async def test_reverse_connect_delay_can_hold_result_at_not_connected(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(mode=PRESET_MODBUS_SMG_READONLY),
            reverse_connect_delay=0.25,
        )

        result = await self._detect_with_scenario(
            scenario,
            request_timeout=0.05,
            connect_timeout=0.05,
            heartbeat_timeout=0.05,
        )

        self.assertIsNone(result.match)
        self.assertEqual(result.last_error, "collector_not_connected")
        self.assertEqual(result.next_action, "manual_input")

    async def test_fc4_timeout_mode_keeps_collector_connected_but_unmatched(self) -> None:
        scenario = resolve_scenario(
            preset=PRESET_MODBUS_SMG_READONLY,
            profile=CollectorProfile(mode=PRESET_MODBUS_SMG_READONLY),
            fc4_mode=FC4_MODE_TIMEOUT,
        )

        result = await self._detect_with_scenario(
            scenario,
            request_timeout=0.05,
            connect_timeout=0.75,
            heartbeat_timeout=0.2,
        )

        self.assertIsNone(result.match)
        self.assertTrue(result.collector.connected)
        self.assertEqual(result.next_action, "manual_driver_selection")

    async def test_nat_peer_mode_creates_multiple_sessions_from_same_source_ip(self) -> None:
        tcp_port = _free_tcp_port()
        udp_port = _free_udp_port()
        sessions: list[tuple[str, str]] = []

        async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                header_bytes = await asyncio.wait_for(
                    reader.readexactly(HEADER_SIZE),
                    timeout=1.0,
                )
                header = decode_header(header_bytes)
                payload = await asyncio.wait_for(
                    reader.readexactly(header.payload_len),
                    timeout=1.0,
                )
                if header.fcode == FC_HEARTBEAT:
                    peer = writer.get_extra_info("peername") or ("", 0)
                    sessions.append((str(peer[0]), parse_heartbeat_pn(payload)))
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(_handle_client, "127.0.0.1", tcp_port)
        primary_pn = "E5000099990001"
        peer_pn = "E5000099990002"
        primary = resolve_scenario(
            preset=PRESET_COLLECTOR_ONLY,
            profile=CollectorProfile(pn=primary_pn, mode=PRESET_COLLECTOR_ONLY),
        )
        peer = resolve_scenario(
            preset=PRESET_COLLECTOR_ONLY,
            profile=CollectorProfile(pn=peer_pn, mode=PRESET_COLLECTOR_ONLY),
        )
        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=udp_port,
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=0.5,
            connect_timeout=1.0,
            udp_reply="rsp>server=2;",
            scenario=primary,
            nat_peer_scenarios=(peer,),
        )

        await service.start()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(
                    f"set>server=127.0.0.1:{tcp_port};\n".encode("ascii"),
                    ("127.0.0.1", udp_port),
                )

            deadline = asyncio.get_running_loop().time() + 2.0
            while len(sessions) < 2 and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.02)

            self.assertEqual(len(sessions), 2)
            self.assertEqual({ip for ip, _pn in sessions}, {"127.0.0.1"})
            self.assertEqual({pn for _ip, pn in sessions}, {primary_pn, peer_pn})
        finally:
            await service.stop()
            server.close()
            await server.wait_closed()

    async def test_nat_peer_mode_routes_shared_transports_by_pn(self) -> None:
        tcp_port = _free_tcp_port()
        udp_port = _free_udp_port()
        primary_pn = "E5000099990001"
        peer_pn = "E5000099990002"
        primary = resolve_scenario(
            preset=PRESET_COLLECTOR_ONLY,
            profile=CollectorProfile(pn=primary_pn, mode=PRESET_COLLECTOR_ONLY),
        )
        peer = resolve_scenario(
            preset=PRESET_COLLECTOR_ONLY,
            profile=CollectorProfile(pn=peer_pn, mode=PRESET_COLLECTOR_ONLY),
        )
        service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=udp_port,
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=0.5,
            connect_timeout=1.0,
            udp_reply="rsp>server=2;",
            scenario=primary,
            nat_peer_scenarios=(peer,),
        )
        first = SharedEybondTransport(
            host="127.0.0.1",
            port=tcp_port,
            request_timeout=0.5,
            heartbeat_interval=1.0,
            collector_ip="",
            collector_pn=primary_pn,
        )
        second = SharedEybondTransport(
            host="127.0.0.1",
            port=tcp_port,
            request_timeout=0.5,
            heartbeat_interval=1.0,
            collector_ip="",
            collector_pn=peer_pn,
        )

        await first.start()
        await second.start()
        await service.start()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(
                    f"set>server=127.0.0.1:{tcp_port};\n".encode("ascii"),
                    ("127.0.0.1", udp_port),
                )

            self.assertTrue(await first.wait_until_connected(timeout=2.0))
            self.assertTrue(await second.wait_until_connected(timeout=2.0))
            self.assertEqual(first.collector_info.collector_pn, primary_pn)
            self.assertEqual(second.collector_info.collector_pn, peer_pn)
            diagnostics = first.session_inventory_diagnostics()
            self.assertEqual(diagnostics["duplicate_peer_ip_count"], 1)
            self.assertEqual(set(diagnostics["duplicate_peer_ips"]), {"127.0.0.1"})
        finally:
            await service.stop()
            await second.stop()
            await first.stop()

if __name__ == "__main__":
    unittest.main()
