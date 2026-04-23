from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.onboarding.detection import (
    DiscoveryTarget,
    DetectedDriverContext,
    OnboardingDetector,
    build_unicast_fallback_targets,
)
from custom_components.eybond_local.onboarding.driver_detection import _build_driver_match
from custom_components.eybond_local.models import DetectedInverter
from custom_components.eybond_local.models import (
    CollectorCandidate,
    DriverMatch,
    CollectorInfo,
    OnboardingResult,
    ProbeTarget,
)
from custom_components.eybond_local.drivers.pi30 import Pi30Driver
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.collector.protocol import EybondHeader
from custom_components.eybond_local.collector.discovery import DiscoveryProbeResult


class DetectionTests(unittest.IsolatedAsyncioTestCase):
    def test_build_driver_match_keeps_family_fallback_at_medium_confidence(self) -> None:
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG Family (Unverified Variant)",
            serial_number="SMG11K240001",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01),
            variant_key="family_fallback",
            details={"rated_power": 11000},
        )

        match = _build_driver_match(SmgModbusDriver(), inverter)

        self.assertEqual(match.variant_key, "family_fallback")
        self.assertEqual(match.confidence, "medium")
        self.assertIn("family_fallback_variant", match.reasons)

    def test_build_driver_match_keeps_non_fallback_read_only_smg_profile_at_medium_confidence(self) -> None:
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG Protocol 1 Candidate",
            serial_number="SMG11K240123",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01),
            variant_key="doc_backed_variant",
            profile_name="modbus_smg/family_fallback.json",
            capabilities=(),
            details={"rated_power": 4200},
        )

        match = _build_driver_match(SmgModbusDriver(), inverter)

        self.assertEqual(match.variant_key, "doc_backed_variant")
        self.assertEqual(match.confidence, "medium")
        self.assertIn("read_only_profile", match.reasons)

    def test_build_driver_match_keeps_anenji_4200_protocol_1_at_medium_confidence(self) -> None:
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji 4200 (Protocol 1)",
            serial_number="99432409105281",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01),
            variant_key="anenji_4200_protocol_1",
            profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            capabilities=(),
            details={"rated_power": 4200},
        )

        match = _build_driver_match(SmgModbusDriver(), inverter)

        self.assertEqual(match.variant_key, "anenji_4200_protocol_1")
        self.assertEqual(match.confidence, "medium")
        self.assertIn("unverified_variant", match.reasons)

    def test_build_unicast_fallback_targets_scans_local_24_without_server_ip(self) -> None:
        targets = build_unicast_fallback_targets(server_ip="192.168.1.50")

        self.assertEqual(len(targets), 253)
        self.assertEqual(targets[0], DiscoveryTarget(ip="192.168.1.1", source="subnet_unicast"))
        self.assertEqual(targets[-1], DiscoveryTarget(ip="192.168.1.254", source="subnet_unicast"))
        self.assertNotIn(DiscoveryTarget(ip="192.168.1.50", source="subnet_unicast"), targets)

    def test_build_unicast_fallback_targets_respects_selected_network(self) -> None:
        targets = build_unicast_fallback_targets(
            server_ip="192.168.1.50",
            network_cidr="192.168.0.0/16",
        )

        self.assertEqual(len(targets), 65533)
        self.assertEqual(targets[0], DiscoveryTarget(ip="192.168.0.1", source="subnet_unicast"))
        self.assertEqual(targets[-1], DiscoveryTarget(ip="192.168.255.254", source="subnet_unicast"))
        self.assertNotIn(DiscoveryTarget(ip="192.168.1.50", source="subnet_unicast"), targets)

    async def test_auto_detect_keeps_broadcast_results_without_unicast_fallback(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        broadcast_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
            ),
            connection_mode="broadcast",
        )

        with (
            patch.object(
                detector,
                "async_detect_targets",
                new=AsyncMock(return_value=(broadcast_result,)),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(),
            ),
        ):
            results = await detector.async_auto_detect(discovery_target="192.168.1.255", attempts=1)

        self.assertEqual(detect_targets.await_count, 1)
        self.assertEqual(
            {result.collector.ip for result in results if result.collector is not None},
            {"192.168.1.55"},
        )

    async def test_deep_detect_appends_unicast_fallback_results_after_broadcast_match(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        broadcast_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632511100118",
                probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
            ),
            connection_mode="broadcast",
        )
        fallback_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="subnet_unicast",
                ip="192.168.1.14",
                connected=True,
            ),
            connection_mode="subnet_unicast",
        )

        with (
            patch.object(
                detector,
                "async_detect_targets",
                new=AsyncMock(side_effect=[(broadcast_result,), (fallback_result,)]),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(return_value=(DiscoveryTarget(ip="192.168.1.14", source="subnet_unicast"),)),
            ),
        ):
            results = await detector.async_deep_detect(
                discovery_target="192.168.1.255",
                unicast_network_cidr="192.168.0.0/16",
                attempts=1,
            )

        self.assertEqual(detect_targets.await_count, 2)
        self.assertEqual(
            {result.collector.ip for result in results if result.collector is not None},
            {"192.168.1.55", "192.168.1.14"},
        )

    async def test_detect_target_shortens_connect_wait_when_udp_reply_is_missing(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            instances: list["FakeTransport"] = []

            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.collector_info = CollectorInfo(remote_ip="")
                self.connected_timeout: float | None = None
                FakeTransport.instances.append(self)

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                return None

            async def wait_until_connected(self, timeout: float) -> bool:
                self.connected_timeout = timeout
                return False

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return False

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_target",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="",
                        reply_from="",
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=1.5,
                connect_timeout=5.0,
                heartbeat_timeout=2.0,
            )

        self.assertEqual(FakeTransport.instances[0].connected_timeout, 0.75)
        self.assertEqual(result.last_error, "collector_not_connected")

    async def test_detect_target_uses_shared_transport_and_routes_to_reply_ip(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            instances: list["FakeTransport"] = []

            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.host = host
                self.port = port
                self.collector_ip = collector_ip
                self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
                self.connected = True
                self.started = False
                FakeTransport.instances.append(self)

            async def start(self) -> None:
                self.started = True

            async def stop(self) -> None:
                self.started = False

            def set_collector_ip(self, collector_ip: str) -> None:
                self.collector_ip = collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                return True

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return True

        detected = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            serial_number="553555355535552",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
            details={"protocol_id": "PI30"},
        )

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_target",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="rsp>server=2;",
                        reply_from="192.168.1.14:58899",
                    )
                ),
            ),
            patch.object(
                detector,
                "_async_detect_driver_with_retries",
                new=AsyncMock(
                    return_value=DetectedDriverContext(
                        driver=Pi30Driver(),
                        inverter=detected,
                        match=DriverMatch(
                            driver_key="pi30",
                            protocol_family="pi30",
                            model_name="PowMr 4.2kW",
                            serial_number="553555355535552",
                            probe_target=detected.probe_target,
                        ),
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        self.assertEqual(result.connection_mode, "broadcast")
        self.assertEqual(result.next_action, "create_entry")
        self.assertIsNotNone(result.match)
        self.assertEqual(result.collector.ip, "192.168.1.14")
        self.assertEqual(FakeTransport.instances[0].collector_ip, "192.168.1.14")

    async def test_detect_target_reports_missing_heartbeat_warning(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.collector_ip = collector_ip
                self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
                self.connected = True

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                self.collector_ip = collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                return True

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return False

        detected = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            serial_number="553555355535552",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
            details={"protocol_id": "PI30"},
        )

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_target",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="rsp>server=2;",
                        reply_from="192.168.1.14:58899",
                    )
                ),
            ),
            patch.object(
                detector,
                "_async_detect_driver_with_retries",
                new=AsyncMock(
                    return_value=DetectedDriverContext(
                        driver=Pi30Driver(),
                        inverter=detected,
                        match=DriverMatch(
                            driver_key="pi30",
                            protocol_family="pi30",
                            model_name="PowMr 4.2kW",
                            serial_number="553555355535552",
                            probe_target=detected.probe_target,
                        ),
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        self.assertIn("collector_heartbeat_not_observed", result.warnings)

    async def test_detect_target_collects_smartess_metadata_on_successful_match(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.collector_ip = collector_ip
                self.collector_info = CollectorInfo(remote_ip="192.168.1.14")

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                self.collector_ip = collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                return True

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return True

            async def async_send_collector(
                self,
                *,
                fcode: int,
                payload: bytes = b"",
                devcode: int = 0,
                collector_addr: int = 1,
            ) -> tuple[EybondHeader, bytes]:
                responses = {
                    (2, b"\x05"): b"\x00\x051.2.3",
                    (2, b"\x0e"): b"\x00\x0e0925#SD-HYM-4862HWP",
                }
                response = responses[(fcode, payload)]
                return (
                    EybondHeader(
                        tid=1,
                        devcode=devcode,
                        wire_len=len(response) + 2,
                        devaddr=collector_addr,
                        fcode=fcode,
                    ),
                    response,
                )

        detected = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            serial_number="553555355535552",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
            details={},
        )

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_target",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="rsp>server=2;",
                        reply_from="192.168.1.14:58899",
                    )
                ),
            ),
            patch.object(
                detector,
                "_async_detect_driver_with_retries",
                new=AsyncMock(
                    return_value=DetectedDriverContext(
                        driver=Pi30Driver(),
                        inverter=detected,
                        match=DriverMatch(
                            driver_key="pi30",
                            protocol_family="pi30",
                            model_name="PowMr 4.2kW",
                            serial_number="553555355535552",
                            probe_target=detected.probe_target,
                            details={},
                        ),
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        assert result.collector is not None
        assert result.collector.collector is not None
        assert result.match is not None
        self.assertEqual(result.collector.collector.smartess_collector_version, "1.2.3")
        self.assertEqual(result.collector.collector.smartess_protocol_asset_id, "0925")
        self.assertEqual(result.collector.collector.smartess_protocol_profile_key, "smartess_0925")
        self.assertEqual(result.collector.collector.smartess_device_address, 5)
        self.assertEqual(result.match.details["smartess_collector_version"], "1.2.3")
        self.assertEqual(result.match.details["smartess_protocol_asset_id"], "0925")
        self.assertEqual(result.match.details["smartess_profile_key"], "smartess_0925")
        self.assertEqual(result.match.details["smartess_device_address"], 5)

    async def test_detect_target_keeps_smartess_metadata_when_no_driver_matches(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.collector_ip = collector_ip
                self.collector_info = CollectorInfo(remote_ip="192.168.1.14")

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                self.collector_ip = collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                return True

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return True

            async def async_send_collector(
                self,
                *,
                fcode: int,
                payload: bytes = b"",
                devcode: int = 0,
                collector_addr: int = 1,
            ) -> tuple[EybondHeader, bytes]:
                responses = {
                    (2, b"\x05"): b"\x00\x052.0.1",
                    (2, b"\x0e"): b"\x00\x0e0911#PVInverter",
                }
                response = responses[(fcode, payload)]
                return (
                    EybondHeader(
                        tid=1,
                        devcode=devcode,
                        wire_len=len(response) + 2,
                        devaddr=collector_addr,
                        fcode=fcode,
                    ),
                    response,
                )

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_target",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="rsp>server=2;",
                        reply_from="192.168.1.14:58899",
                    )
                ),
            ),
            patch.object(
                detector,
                "_async_detect_driver_with_retries",
                new=AsyncMock(side_effect=RuntimeError("no_supported_driver_matched")),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        self.assertEqual(result.next_action, "manual_driver_selection")
        self.assertEqual(result.last_error, "no_supported_driver_matched")
        assert result.collector is not None
        assert result.collector.collector is not None
        self.assertEqual(result.collector.collector.smartess_collector_version, "2.0.1")
        self.assertEqual(result.collector.collector.smartess_protocol_asset_id, "0911")
        self.assertEqual(result.collector.collector.smartess_protocol_profile_key, "smartess_0911")
        self.assertEqual(result.collector.collector.smartess_device_address, 3)


if __name__ == "__main__":
    unittest.main()
