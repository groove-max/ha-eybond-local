from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.smartess_ble import (
    BleakSmartEssBleScanner,
    BleakSmartEssBleLink,
    PROVISION_LAYOUT,
    PROXY_LAYOUT,
    SmartEssBleProvisionBranch,
    SmartEssBleProvisionOutcome,
    SmartEssBleProvisionResult,
    SmartEssBleProvisioner,
    SmartEssBleProvisioningInfo,
    VENDOR_LAYOUT,
    SmartEssBleCandidate,
    SmartEssBleError,
    SmartEssBleScanRecord,
    SmartEssBleSession,
    SmartEssBleWifiNetwork,
    _default_bleak_connect_client,
    async_probe_ble_host_capability,
    build_ble_candidate,
    build_ble_text_payload,
    choose_ble_uuid_layout,
    compare_ble_versions,
    decode_ble_text_payload,
    is_smartess_ble_pn,
    parse_intpara48_provision_result,
    parse_link_provision_result,
    parse_wifi_scan_response,
    normalize_discovered_candidate,
    parse_ble_scan_record,
    select_ble_provision_branch,
)


class _FakeBleLink:
    def __init__(self, service_uuids: list[str], *, read_results: list[bytes] | None = None) -> None:
        self._service_uuids = list(service_uuids)
        self.connected = False
        self.notify_uuid = ""
        self.stopped_notify_uuid = ""
        self.write_calls: list[tuple[str, bytes, bool]] = []
        self.read_calls: list[str] = []
        self._notify_callback = None
        self._read_results = list(read_results or [])

    async def connect(self) -> list[str]:
        self.connected = True
        return list(self._service_uuids)

    async def disconnect(self) -> None:
        self.connected = False

    async def start_notify(self, characteristic_uuid: str, callback) -> None:
        self.notify_uuid = characteristic_uuid
        self._notify_callback = callback

    async def stop_notify(self, characteristic_uuid: str) -> None:
        self.stopped_notify_uuid = characteristic_uuid

    async def write(self, characteristic_uuid: str, data: bytes, *, response: bool = False) -> None:
        self.write_calls.append((characteristic_uuid, bytes(data), bool(response)))

    async def read(self, characteristic_uuid: str) -> bytes:
        self.read_calls.append(characteristic_uuid)
        if not self._read_results:
            return b""
        return self._read_results.pop(0)

    def push_notification(self, payload: bytes) -> None:
        if self._notify_callback is None:
            raise AssertionError("notify callback not registered")
        self._notify_callback(payload)


class _FakeBleakService:
    def __init__(self, uuid: str) -> None:
        self.uuid = uuid


class _FakeBleakServices:
    def __init__(self, uuids: list[str]) -> None:
        self.services = {uuid: _FakeBleakService(uuid) for uuid in uuids}


class _FakeBleakClient:
    def __init__(self, address: str, uuids: list[str]) -> None:
        self.address = address
        self.services = _FakeBleakServices(uuids)
        self.connected = False
        self.notify_uuid = ""
        self.stopped_notify_uuid = ""
        self.write_calls: list[tuple[str, bytes, bool]] = []
        self.read_calls: list[str] = []
        self._notify_callback = None
        self._read_results: list[bytes] = []

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def start_notify(self, characteristic_uuid: str, callback) -> None:
        self.notify_uuid = characteristic_uuid
        self._notify_callback = callback

    async def stop_notify(self, characteristic_uuid: str) -> None:
        self.stopped_notify_uuid = characteristic_uuid

    async def write_gatt_char(self, characteristic_uuid: str, data: bytes, response: bool = False) -> None:
        self.write_calls.append((characteristic_uuid, bytes(data), bool(response)))

    async def read_gatt_char(self, characteristic_uuid: str) -> bytes:
        self.read_calls.append(characteristic_uuid)
        if not self._read_results:
            return b""
        return self._read_results.pop(0)

    def emit_notification(self, payload: bytes) -> None:
        if self._notify_callback is None:
            raise AssertionError("notify callback not registered")
        self._notify_callback(1, payload)

    def queue_read_result(self, payload: bytes) -> None:
        self._read_results.append(payload)


class _FakeBleakDevice:
    def __init__(self, address: str, name: str = "", metadata: dict[str, object] | None = None) -> None:
        self.address = address
        self.name = name
        self.metadata = metadata or {}


class _FakeAdvertisement:
    def __init__(
        self,
        *,
        local_name: str = "",
        service_uuids: list[str] | None = None,
        manufacturer_data: dict[int, bytes] | None = None,
    ) -> None:
        self.local_name = local_name
        self.service_uuids = service_uuids or []
        self.manufacturer_data = manufacturer_data or {}


class _FakeTextSession:
    def __init__(self, responses: dict[str, list[str | BaseException]]) -> None:
        self._responses = {key: list(value) for key, value in responses.items()}
        self.commands: list[str] = []
        self.calls: list[tuple[str, float, bool, bool]] = []

    async def exchange_text(
        self,
        command: str,
        *,
        timeout: float = 3.0,
        append_crlf: bool = True,
        response: bool = False,
        drain_before_send: bool = True,
    ) -> str:
        self.commands.append(command)
        self.calls.append((command, timeout, append_crlf, response))
        if command not in self._responses or not self._responses[command]:
            raise AssertionError(f"unexpected command: {command}")
        value = self._responses[command].pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


class SmartEssBleHelperTests(unittest.TestCase):
    def test_compare_ble_versions_and_branch_selection(self) -> None:
        self.assertLess(compare_ble_versions("1.10", "1.11"), 0)
        self.assertEqual(compare_ble_versions("1.11", "1.11.0"), 0)
        self.assertEqual(select_ble_provision_branch("1.11"), SmartEssBleProvisionBranch.WFLKAP)
        self.assertEqual(select_ble_provision_branch("1.10"), SmartEssBleProvisionBranch.INTPARA)

    def test_is_smartess_ble_pn_accepts_14_and_18_char_formats(self) -> None:
        self.assertTrue(is_smartess_ble_pn("E5000020000000"))
        self.assertTrue(is_smartess_ble_pn("E50000200000000001"))
        self.assertFalse(is_smartess_ble_pn("5000020000000"))

    def test_parse_wifi_scan_response_extracts_networks(self) -> None:
        self.assertEqual(
            parse_wifi_scan_response("AT+INTPARA:49,[Home WiFi,-48],[Office,Guest,-70]"),
            (
                SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-48),
                SmartEssBleWifiNetwork(ssid="Office,Guest", signal=-70),
            ),
        )

    def test_parse_wifi_scan_response_accepts_raw_49_payload(self) -> None:
        self.assertEqual(
            parse_wifi_scan_response(
                "49,[HomeNet,98],[WiFi OptoLAN,88],[optolan,88],[OBLIVION,32],"
                "[TP-Link_DE28,32],[Odessa WIFI,26],[WiFi OptoLAN (541),20],"
                "[TP-LINK56,16],[Archer,12],"
            ),
            (
                SmartEssBleWifiNetwork(ssid="HomeNet", signal=98),
                SmartEssBleWifiNetwork(ssid="WiFi OptoLAN", signal=88),
                SmartEssBleWifiNetwork(ssid="optolan", signal=88),
                SmartEssBleWifiNetwork(ssid="OBLIVION", signal=32),
                SmartEssBleWifiNetwork(ssid="TP-Link_DE28", signal=32),
                SmartEssBleWifiNetwork(ssid="Odessa WIFI", signal=26),
                SmartEssBleWifiNetwork(ssid="WiFi OptoLAN (541)", signal=20),
                SmartEssBleWifiNetwork(ssid="TP-LINK56", signal=16),
                SmartEssBleWifiNetwork(ssid="Archer", signal=12),
            ),
        )

    def test_parse_link_and_intpara48_results_normalize_outcomes(self) -> None:
        self.assertEqual(
            parse_link_provision_result("AT+LINK:W302"),
            (SmartEssBleProvisionOutcome.DEGRADED, "W302"),
        )
        self.assertEqual(
            parse_intpara48_provision_result("AT+INTPARA:48,0,1,1"),
            (SmartEssBleProvisionOutcome.FAILURE, "W008", ("0", "1", "1")),
        )
        self.assertEqual(
            parse_intpara48_provision_result("AT+INTPARA:48,0,0,1"),
            (SmartEssBleProvisionOutcome.DEGRADED, "W051", ("0", "0", "1")),
        )

    def test_parse_ble_scan_record_extracts_name_and_pn(self) -> None:
        payload = (
            bytes((0x02, 0x01, 0x06))
            + bytes((0x08, 0x09))
            + b"DTU-Box"
            + bytes((0x13, 0xFF))
            + b"E50000200000000001"
        )

        parsed = parse_ble_scan_record(payload)

        self.assertEqual(
            parsed,
            SmartEssBleScanRecord(local_name="DTU-Box", local_pn="E50000200000000001", flags=0x06),
        )

    def test_build_ble_candidate_requires_valid_pn(self) -> None:
        candidate = build_ble_candidate(
            address="AA:BB:CC:DD:EE:FF",
            device_name="",
            local_name="Collector Box",
            local_pn="E50000200000000001",
            service_uuids=[PROVISION_LAYOUT.service_uuid],
        )

        self.assertEqual(
            candidate,
            SmartEssBleCandidate(
                address="AA:BB:CC:DD:EE:FF",
                local_pn="E50000200000000001",
                local_name="Collector Box",
                device_name="",
                service_uuids=(PROVISION_LAYOUT.service_uuid,),
            ),
        )
        self.assertIsNone(
            build_ble_candidate(
                address="AA:BB:CC:DD:EE:11",
                local_name="Unknown",
                local_pn="",
            )
        )

    def test_normalize_discovered_candidate_accepts_device_name_pn(self) -> None:
        candidate = normalize_discovered_candidate(
            address="AA:BB:CC:DD:EE:FF",
            device_name="E5000020000000",
            advertisement_local_name="\u200bCollector\ufeff",
            service_uuids=[VENDOR_LAYOUT.service_uuid],
        )

        assert candidate is not None
        self.assertEqual(candidate.local_pn, "E5000020000000")
        self.assertEqual(candidate.local_name, "Collector")
        self.assertEqual(candidate.preferred_name, "Collector")

    def test_normalize_discovered_candidate_rebuilds_pn_from_manufacturer_id_prefix(self) -> None:
        candidate = normalize_discovered_candidate(
            address="AA:BB:CC:DD:EE:47",
            device_name="AA:BB:CC:DD:EE:47",
            manufacturer_data={0x3545: b"0000200000000001"},
        )

        assert candidate is not None
        self.assertEqual(candidate.local_pn, "E50000200000000001")
        self.assertEqual(candidate.local_name, "AA:BB:CC:DD:EE:47")

    def test_choose_ble_uuid_layout_prefers_known_service_layouts(self) -> None:
        self.assertEqual(
            choose_ble_uuid_layout(["00001827-0000-1000-8000-00805F9B34FB"]),
            PROVISION_LAYOUT,
        )
        self.assertEqual(
            choose_ble_uuid_layout(["00001828-0000-1000-8000-00805F9B34FB"]),
            PROXY_LAYOUT,
        )
        self.assertEqual(
            choose_ble_uuid_layout(["12345678-1234-5678-1234-567812345678"]),
            VENDOR_LAYOUT,
        )

    def test_build_and_decode_ble_text_payload(self) -> None:
        self.assertEqual(build_ble_text_payload("AT+FWVER?"), b"AT+FWVER?\r\n")
        self.assertEqual(
            decode_ble_text_payload(b"AT+FWVER:8.50.12.3\r\n"),
            "AT+FWVER:8.50.12.3",
        )

    def test_build_ble_text_payload_rejects_non_ascii(self) -> None:
        with self.assertRaisesRegex(SmartEssBleError, "ble_command_invalid"):
            build_ble_text_payload("AT+SSID=мережа")

    def test_default_capability_probe_reports_missing_backend(self) -> None:
        async def _run() -> None:
            with patch(
                "custom_components.eybond_local.collector.smartess_ble.importlib.util.find_spec",
                return_value=None,
            ):
                result = await async_probe_ble_host_capability()

            self.assertFalse(result.available)
            self.assertEqual(result.backend, "bleak")
            self.assertEqual(result.reason, "backend_missing")

        asyncio.run(_run())

    def test_custom_capability_probe_reports_host_failure(self) -> None:
        async def _probe() -> None:
            raise PermissionError("bluetooth access denied")

        async def _run() -> None:
            result = await async_probe_ble_host_capability(probe=_probe)
            self.assertFalse(result.available)
            self.assertEqual(result.backend, "probe")
            self.assertEqual(result.reason, "permission_denied")
            self.assertIn("access denied", result.detail)

        asyncio.run(_run())

    def test_scanner_discovers_only_smartess_candidates(self) -> None:
        discovered_device = _FakeBleakDevice("AA:BB:CC:DD:EE:01", "")

        async def _discover(_timeout: float) -> object:
            return {
                "AA:BB:CC:DD:EE:01": (
                    discovered_device,
                    _FakeAdvertisement(
                        local_name="Collector A",
                        service_uuids=[PROVISION_LAYOUT.service_uuid],
                        manufacturer_data={1: b"E50000200000000001"},
                    ),
                ),
                "AA:BB:CC:DD:EE:02": (
                    _FakeBleakDevice("AA:BB:CC:DD:EE:02", "Random Device"),
                    _FakeAdvertisement(local_name="Random Device"),
                ),
            }

        async def _run() -> None:
            scanner = BleakSmartEssBleScanner(discover=_discover)

            candidates = await scanner.discover_candidates(timeout=4.0)

            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0].address, "AA:BB:CC:DD:EE:01")
            self.assertEqual(candidates[0].local_name, "Collector A")
            self.assertEqual(candidates[0].local_pn, "E50000200000000001")
            self.assertEqual(candidates[0].service_uuids, (PROVISION_LAYOUT.service_uuid,))
            self.assertIs(candidates[0].device, discovered_device)

        asyncio.run(_run())

    def test_provisioner_reads_versions_and_scans_wifi(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+FWVER?": ["AT+FWVER:8.50.8.18"],
                    "AT+ATVER?": ["AT+ATVER:1.11"],
                    "AT+INTPARA49?": ["AT+INTPARA:49,[Home WiFi,-48],[Office,Guest,-70]"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0)

            info = await provisioner.query_device_info()
            networks = await provisioner.scan_wifi_networks()

            self.assertEqual(
                info,
                SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.11",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )
            self.assertEqual(
                networks,
                (
                    SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-48),
                    SmartEssBleWifiNetwork(ssid="Office,Guest", signal=-70),
                ),
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+FWVER?", 4.0, False, True),
                    ("AT+ATVER?", 4.0, False, True),
                    ("AT+INTPARA49?", 20.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_scan_wifi_uses_long_command_timeout(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+FWVER?": ["AT+FWVER:8.50.8.18"],
                    "AT+INTPARA49?": ["AT+INTPARA:49,[Home WiFi,-48]"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, wifi_scan_preflight_delay=0.0)

            await provisioner.scan_wifi_networks()

            self.assertEqual(
                session.calls,
                [
                    ("AT+FWVER?", 4.0, False, True),
                    ("AT+INTPARA49?", 20.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_scan_wifi_continues_when_preflight_times_out(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+FWVER?": [SmartEssBleError("ble_notification_timeout")],
                    "AT+INTPARA49?": ["AT+INTPARA:49,[Home WiFi,-48]"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, wifi_scan_preflight_delay=0.0)

            networks = await provisioner.scan_wifi_networks()

            self.assertEqual(networks, (SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-48),))
            self.assertEqual(
                session.calls,
                [
                    ("AT+FWVER?", 4.0, False, True),
                    ("AT+INTPARA49?", 20.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_query_device_info_prefers_wflkap_for_newer_fw_when_at_probe_times_out(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+FWVER?": ["AT+FWVER:8.50.8.18"],
                    "AT+ATVER?": [SmartEssBleError("ble_notification_timeout")],
                }
            )
            provisioner = SmartEssBleProvisioner(session)

            info = await provisioner.query_device_info()

            self.assertEqual(
                info,
                SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.10",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+FWVER?", 4.0, False, True),
                    ("AT+ATVER?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_query_device_info_prefers_wflkap_when_version_probes_time_out(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+FWVER?": [SmartEssBleError("ble_notification_timeout")],
                    "AT+ATVER?": [SmartEssBleError("ble_notification_timeout")],
                }
            )
            provisioner = SmartEssBleProvisioner(session)

            info = await provisioner.query_device_info()

            self.assertEqual(
                info,
                SmartEssBleProvisioningInfo(
                    fw_version="7.5.1.1",
                    at_version="1.10",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )
            self.assertEqual(provisioner.last_firmware_version, "")
            self.assertEqual(
                session.calls,
                [
                    ("AT+FWVER?", 4.0, False, True),
                    ("AT+ATVER?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_query_device_info_uses_known_firmware_without_requerying_fwver(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+ATVER?": ["AT+ATVER:1.11"],
                }
            )
            provisioner = SmartEssBleProvisioner(session)

            info = await provisioner.query_device_info(known_fw_version="8.50.8.18")

            self.assertEqual(
                info,
                SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.11",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+ATVER?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_scan_wifi_propagates_notification_timeout_after_long_command_window(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+FWVER?": ["AT+FWVER:8.50.8.18"],
                    "AT+INTPARA49?": [
                        SmartEssBleError("ble_notification_timeout"),
                    ],
                }
            )
            provisioner = SmartEssBleProvisioner(session, wifi_scan_preflight_delay=0.0)

            with self.assertRaisesRegex(SmartEssBleError, "ble_notification_timeout"):
                await provisioner.scan_wifi_networks()

            self.assertEqual(
                session.calls,
                [
                    ("AT+FWVER?", 4.0, False, True),
                    ("AT+INTPARA49?", 20.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_runs_wflkap_branch_until_success(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+WFLKAP=Home,AES,WPA2_PSK,Secret123": ["AT+WFLKAP:W000"],
                    "AT+LINK?": ["AT+LINK:W052", "AT+LINK:W000"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=3)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.11",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    outcome=SmartEssBleProvisionOutcome.SUCCESS,
                    status_code="W000",
                    raw_response="AT+LINK:W000",
                    details=None,
                ),
            )
            self.assertEqual(
                session.commands,
                [
                    "AT+WFLKAP=Home,AES,WPA2_PSK,Secret123",
                    "AT+LINK?",
                    "AT+LINK?",
                ],
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+WFLKAP=Home,AES,WPA2_PSK,Secret123", 4.0, False, True),
                    ("AT+LINK?", 4.0, False, True),
                    ("AT+LINK?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_wflkap_timeout_can_still_succeed_via_link_poll(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+WFLKAP=Home,AES,WPA2_PSK,Secret123": [SmartEssBleError("ble_notification_timeout")],
                    "AT+LINK?": ["AT+LINK:W000"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=3)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.11",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    outcome=SmartEssBleProvisionOutcome.SUCCESS,
                    status_code="W000",
                    raw_response="AT+LINK:W000",
                    details=None,
                ),
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+WFLKAP=Home,AES,WPA2_PSK,Secret123", 4.0, False, True),
                    ("AT+LINK?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_wflkap_missing_final_status_is_degraded_not_failure(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+WFLKAP=Home,AES,WPA2_PSK,Secret123": ["AT+WFLKAP:W000"],
                    "AT+LINK?": [
                        SmartEssBleError("ble_notification_timeout"),
                        SmartEssBleError("ble_notification_timeout"),
                    ],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=2)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.11",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    outcome=SmartEssBleProvisionOutcome.DEGRADED,
                    status_code="Timeout",
                    raw_response="",
                    details=None,
                ),
            )

        asyncio.run(_run())

    def test_provisioner_wflkap_transport_loss_during_final_status_is_degraded_not_failure(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+WFLKAP=Home,AES,WPA2_PSK,Secret123": ["AT+WFLKAP:W000"],
                    "AT+LINK?": [RuntimeError("Bluetooth GATT Error address=AA:BB:CC:DD:EE:FF handle=18 error=133 description=Error")],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=2)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="8.50.8.18",
                    at_version="1.11",
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.WFLKAP,
                    outcome=SmartEssBleProvisionOutcome.DEGRADED,
                    status_code="TransportLost",
                    raw_response="",
                    details=None,
                ),
            )

        asyncio.run(_run())

    def test_provisioner_runs_intpara_branch_with_restart(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+INTPARA=41,Home": ["AT+INTPARA:W000"],
                    "AT+INTPARA=43,Secret123": ["AT+INTPARA:W000"],
                    "AT+INTPARA=29,1": ["AT+INTPARA:W000"],
                    "AT+INTPARA48?": ["AT+INTPARA:48,0,0,1", "AT+INTPARA:48,2,0,0"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=3)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="7.5.1.1",
                    at_version="1.10",
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    requires_restart=True,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    outcome=SmartEssBleProvisionOutcome.SUCCESS,
                    status_code="W000",
                    raw_response="AT+INTPARA:48,2,0,0",
                    details=("2", "0", "0"),
                ),
            )
            self.assertEqual(
                session.commands,
                [
                    "AT+INTPARA=41,Home",
                    "AT+INTPARA=43,Secret123",
                    "AT+INTPARA=29,1",
                    "AT+INTPARA48?",
                    "AT+INTPARA48?",
                ],
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+INTPARA=41,Home", 4.0, False, True),
                    ("AT+INTPARA=43,Secret123", 4.0, True, True),
                    ("AT+INTPARA=29,1", 4.0, False, True),
                    ("AT+INTPARA48?", 4.0, False, True),
                    ("AT+INTPARA48?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_intpara_timeouts_can_progress_to_status_poll(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+INTPARA=41,Home": [SmartEssBleError("ble_notification_timeout")],
                    "AT+INTPARA=43,Secret123": [SmartEssBleError("ble_notification_timeout")],
                    "AT+INTPARA48?": ["AT+INTPARA:48,2,0,0"],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=2)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="7.5.1.1",
                    at_version="1.10",
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    outcome=SmartEssBleProvisionOutcome.SUCCESS,
                    status_code="W000",
                    raw_response="AT+INTPARA:48,2,0,0",
                    details=("2", "0", "0"),
                ),
            )
            self.assertEqual(
                session.calls,
                [
                    ("AT+INTPARA=41,Home", 4.0, False, True),
                    ("AT+INTPARA=43,Secret123", 4.0, True, True),
                    ("AT+INTPARA48?", 4.0, False, True),
                ],
            )

        asyncio.run(_run())

    def test_provisioner_intpara_missing_final_status_is_degraded_not_failure(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+INTPARA=41,Home": ["AT+INTPARA:W000"],
                    "AT+INTPARA=43,Secret123": ["AT+INTPARA:W000"],
                    "AT+INTPARA48?": [
                        SmartEssBleError("ble_notification_timeout"),
                        SmartEssBleError("ble_notification_timeout"),
                    ],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=2)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="7.5.1.1",
                    at_version="1.10",
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    outcome=SmartEssBleProvisionOutcome.DEGRADED,
                    status_code="Timeout",
                    raw_response="",
                    details=None,
                ),
            )

        asyncio.run(_run())

    def test_provisioner_intpara_transport_loss_during_final_status_is_degraded_not_failure(self) -> None:
        async def _run() -> None:
            session = _FakeTextSession(
                {
                    "AT+INTPARA=41,Home": ["AT+INTPARA:W000"],
                    "AT+INTPARA=43,Secret123": ["AT+INTPARA:W000"],
                    "AT+INTPARA48?": [RuntimeError("Bluetooth GATT Error address=AA:BB:CC:DD:EE:FF handle=18 error=133 description=Error")],
                }
            )
            provisioner = SmartEssBleProvisioner(session, status_poll_interval=0.0, max_status_polls=2)

            result = await provisioner.provision_wifi(
                ssid="Home",
                password="Secret123",
                info=SmartEssBleProvisioningInfo(
                    fw_version="7.5.1.1",
                    at_version="1.10",
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    requires_restart=False,
                ),
            )

            self.assertEqual(
                result,
                SmartEssBleProvisionResult(
                    branch=SmartEssBleProvisionBranch.INTPARA,
                    outcome=SmartEssBleProvisionOutcome.DEGRADED,
                    status_code="TransportLost",
                    raw_response="",
                    details=None,
                ),
            )

        asyncio.run(_run())


class SmartEssBleSessionTests(unittest.TestCase):
    def test_default_connect_client_falls_back_to_address_only_when_establish_connection_times_out(self) -> None:
        async def _run() -> None:
            address_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
            resolved_device = types.SimpleNamespace(name="Collector BLE", address="AA:BB:CC:DD:EE:FF")
            created_clients: list[str] = []

            async def _establish_connection(*_args, **_kwargs):
                await asyncio.Event().wait()

            bleak_module = types.ModuleType("bleak")
            bleak_module.BleakClient = lambda value: (created_clients.append(str(value)), address_client)[1]
            bleak_module.BleakScanner = types.SimpleNamespace(find_device_by_address=None)

            retry_module = types.ModuleType("bleak_retry_connector")
            retry_module.BleakClientWithServiceCache = object
            retry_module.establish_connection = _establish_connection

            with patch.dict(
                sys.modules,
                {
                    "bleak": bleak_module,
                    "bleak_retry_connector": retry_module,
                },
            ), patch(
                "custom_components.eybond_local.collector.smartess_ble.importlib.util.find_spec",
                side_effect=lambda name: object() if name in {"bleak", "bleak_retry_connector"} else None,
            ), patch(
                "custom_components.eybond_local.collector.smartess_ble._DEFAULT_ESTABLISH_CONNECTION_TIMEOUT",
                0.001,
            ):
                client = await _default_bleak_connect_client(
                    "AA:BB:CC:DD:EE:FF",
                    device=resolved_device,
                )

            self.assertIs(client, address_client)
            self.assertTrue(address_client.connected)
            self.assertEqual(created_clients, ["AA:BB:CC:DD:EE:FF"])

        asyncio.run(_run())

    def test_default_connect_client_uses_fresh_scanner_device_for_fallback_connect(self) -> None:
        async def _run() -> None:
            address_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
            resolved_device = types.SimpleNamespace(name="Collector BLE", address="AA:BB:CC:DD:EE:FF")
            fresh_device = types.SimpleNamespace(name="Fresh Collector", address="AA:BB:CC:DD:EE:FF")
            created_targets: list[object] = []

            async def _establish_connection(*_args, **_kwargs):
                await asyncio.Event().wait()

            async def _find_device_by_address(_address: str, timeout: float = 8.0):
                del timeout
                return fresh_device

            bleak_module = types.ModuleType("bleak")
            bleak_module.BleakClient = lambda value: (created_targets.append(value), address_client)[1]
            bleak_module.BleakScanner = types.SimpleNamespace(find_device_by_address=_find_device_by_address)

            retry_module = types.ModuleType("bleak_retry_connector")
            retry_module.BleakClientWithServiceCache = object
            retry_module.establish_connection = _establish_connection

            with patch.dict(
                sys.modules,
                {
                    "bleak": bleak_module,
                    "bleak_retry_connector": retry_module,
                },
            ), patch(
                "custom_components.eybond_local.collector.smartess_ble.importlib.util.find_spec",
                side_effect=lambda name: object() if name in {"bleak", "bleak_retry_connector"} else None,
            ), patch(
                "custom_components.eybond_local.collector.smartess_ble._DEFAULT_ESTABLISH_CONNECTION_TIMEOUT",
                0.001,
            ):
                client = await _default_bleak_connect_client(
                    "AA:BB:CC:DD:EE:FF",
                    device=resolved_device,
                )

            self.assertIs(client, address_client)
            self.assertTrue(address_client.connected)
            self.assertEqual(created_targets, [fresh_device])

        asyncio.run(_run())

    def test_default_connect_client_refreshes_provided_device_before_establish_connection(self) -> None:
        async def _run() -> None:
            connected_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
            resolved_device = types.SimpleNamespace(name="Collector BLE", address="AA:BB:CC:DD:EE:FF")
            fresh_device = types.SimpleNamespace(name="Fresh Collector", address="AA:BB:CC:DD:EE:FF")
            establish_devices: list[object] = []

            async def _establish_connection(_client_cls, device, **_kwargs):
                establish_devices.append(device)
                connected_client.connected = True
                return connected_client

            async def _find_device_by_address(_address: str, timeout: float = 8.0):
                del timeout
                return fresh_device

            bleak_module = types.ModuleType("bleak")
            bleak_module.BleakClient = lambda value: connected_client
            bleak_module.BleakScanner = types.SimpleNamespace(find_device_by_address=_find_device_by_address)

            retry_module = types.ModuleType("bleak_retry_connector")
            retry_module.BleakClientWithServiceCache = object
            retry_module.establish_connection = _establish_connection

            with patch.dict(
                sys.modules,
                {
                    "bleak": bleak_module,
                    "bleak_retry_connector": retry_module,
                },
            ), patch(
                "custom_components.eybond_local.collector.smartess_ble.importlib.util.find_spec",
                side_effect=lambda name: object() if name in {"bleak", "bleak_retry_connector"} else None,
            ), patch(
                "custom_components.eybond_local.collector.smartess_ble._DEFAULT_REFRESH_DEVICE_LOOKUP_TIMEOUT",
                0.001,
            ):
                client = await _default_bleak_connect_client(
                    "AA:BB:CC:DD:EE:FF",
                    device=resolved_device,
                )

            self.assertIs(client, connected_client)
            self.assertEqual(establish_devices, [fresh_device])

        asyncio.run(_run())

    def test_bleak_link_connect_extracts_service_uuids(self) -> None:
        async def _run() -> None:
            fake_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [PROXY_LAYOUT.service_uuid])
            link = BleakSmartEssBleLink(
                "AA:BB:CC:DD:EE:FF",
                client_factory=lambda _address: fake_client,
            )

            service_uuids = await link.connect()

            self.assertTrue(fake_client.connected)
            self.assertEqual(service_uuids, (PROXY_LAYOUT.service_uuid,))

        asyncio.run(_run())

    def test_bleak_link_can_use_async_connect_client(self) -> None:
        async def _run() -> None:
            fake_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [PROVISION_LAYOUT.service_uuid])

            async def _connect_client(_address: str):
                fake_client.connected = True
                return fake_client

            link = BleakSmartEssBleLink(
                "AA:BB:CC:DD:EE:FF",
                connect_client=_connect_client,
            )

            service_uuids = await link.connect()

            self.assertTrue(fake_client.connected)
            self.assertEqual(service_uuids, (PROVISION_LAYOUT.service_uuid,))

        asyncio.run(_run())

    def test_bleak_link_bridges_notify_and_write_calls(self) -> None:
        async def _run() -> None:
            fake_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
            link = BleakSmartEssBleLink(
                "AA:BB:CC:DD:EE:FF",
                client_factory=lambda _address: fake_client,
            )
            await link.connect()
            received: list[bytes] = []

            await link.start_notify(VENDOR_LAYOUT.notify_uuid, received.append)
            fake_client.emit_notification(b"AT+FWVER:8.50.12.3\r\n")
            await link.write(VENDOR_LAYOUT.write_uuid, b"AT+FWVER?\r\n")
            await link.stop_notify(VENDOR_LAYOUT.notify_uuid)

            self.assertEqual(received, [b"AT+FWVER:8.50.12.3\r\n"])
            self.assertEqual(
                fake_client.write_calls,
                [(VENDOR_LAYOUT.write_uuid, b"AT+FWVER?\r\n", False)],
            )
            self.assertEqual(fake_client.notify_uuid, VENDOR_LAYOUT.notify_uuid)
            self.assertEqual(fake_client.stopped_notify_uuid, VENDOR_LAYOUT.notify_uuid)

        asyncio.run(_run())

    def test_bleak_link_bridges_characteristic_reads(self) -> None:
        async def _run() -> None:
            fake_client = _FakeBleakClient("AA:BB:CC:DD:EE:FF", [VENDOR_LAYOUT.service_uuid])
            fake_client.queue_read_result(b"AT+FWVER:8.50.12.3\r\n")
            link = BleakSmartEssBleLink(
                "AA:BB:CC:DD:EE:FF",
                client_factory=lambda _address: fake_client,
            )
            await link.connect()

            payload = await link.read(VENDOR_LAYOUT.write_uuid)

            self.assertEqual(payload, b"AT+FWVER:8.50.12.3\r\n")
            self.assertEqual(fake_client.read_calls, [VENDOR_LAYOUT.write_uuid])

        asyncio.run(_run())

    def test_session_connect_selects_layout_and_subscribes(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink([PROVISION_LAYOUT.service_uuid])
            session = SmartEssBleSession(link)

            layout = await session.connect()

            self.assertTrue(session.connected)
            self.assertEqual(layout, PROVISION_LAYOUT)
            self.assertEqual(link.notify_uuid, PROVISION_LAYOUT.notify_uuid)

        asyncio.run(_run())

    def test_session_send_bytes_uses_selected_write_uuid(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink([PROXY_LAYOUT.service_uuid])
            session = SmartEssBleSession(link)
            await session.connect()

            await session.send_bytes(b"AT+INTPARA49?\r\n")

            self.assertEqual(
                link.write_calls,
                [(PROXY_LAYOUT.write_uuid, b"AT+INTPARA49?\r\n", False)],
            )

        asyncio.run(_run())

    def test_session_exchange_text_returns_next_notify_payload(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
            session = SmartEssBleSession(link)
            await session.connect()

            task = asyncio.create_task(session.exchange_text("AT+FWVER?"))
            await asyncio.sleep(0)
            link.push_notification(b"AT+FWVER:8.50.12.3\r\n")

            response = await task

            self.assertEqual(response, "AT+FWVER:8.50.12.3")
            self.assertEqual(
                link.write_calls,
                [(VENDOR_LAYOUT.write_uuid, b"AT+FWVER?\r\n", False)],
            )

        asyncio.run(_run())

    def test_session_exchange_text_falls_back_to_vendor_characteristic_read(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink(
                [VENDOR_LAYOUT.service_uuid],
                read_results=[b"AT+FWVER:8.50.12.3\r\n"],
            )
            session = SmartEssBleSession(link)
            await session.connect()

            response = await session.exchange_text("AT+FWVER?", timeout=0.05)

            self.assertEqual(response, "AT+FWVER:8.50.12.3")
            self.assertEqual(link.read_calls, [VENDOR_LAYOUT.write_uuid])

        asyncio.run(_run())

    def test_session_exchange_text_collects_fragmented_vendor_wifi_scan_reads(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink(
                [VENDOR_LAYOUT.service_uuid],
                read_results=[
                    b"AT+INTPARA:49,[Home WiFi,-48]",
                    b"AT+INTPARA:49,[Office,-70]",
                    b"------",
                ],
            )
            session = SmartEssBleSession(link)
            await session.connect()

            response = await session.exchange_text("AT+INTPARA49?", timeout=0.4)

            self.assertEqual(
                parse_wifi_scan_response(response),
                (
                    SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-48),
                    SmartEssBleWifiNetwork(ssid="Office", signal=-70),
                ),
            )
            self.assertEqual(
                link.read_calls,
                [VENDOR_LAYOUT.write_uuid, VENDOR_LAYOUT.write_uuid, VENDOR_LAYOUT.write_uuid],
            )

        asyncio.run(_run())

    def test_session_exchange_text_ignores_vendor_placeholders_until_wifi_scan_payload_arrives(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink(
                [VENDOR_LAYOUT.service_uuid],
                read_results=[
                    b"------------------",
                    b"------------------",
                    b"AT+INTPARA:49,[Home WiFi,-48]",
                    b"------------------",
                ],
            )
            session = SmartEssBleSession(link)
            await session.connect()

            response = await session.exchange_text("AT+INTPARA49?", timeout=1.0)

            self.assertEqual(
                parse_wifi_scan_response(response),
                (SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-48),),
            )
            self.assertEqual(
                link.read_calls,
                [
                    VENDOR_LAYOUT.write_uuid,
                    VENDOR_LAYOUT.write_uuid,
                    VENDOR_LAYOUT.write_uuid,
                    VENDOR_LAYOUT.write_uuid,
                ],
            )

        asyncio.run(_run())

    def test_session_exchange_text_ignores_unrelated_vendor_payloads_until_wifi_scan_payload_arrives(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink(
                [VENDOR_LAYOUT.service_uuid],
                read_results=[
                    b"111111118",
                    b"AT+INTPARA:49,[Home WiFi,-48]",
                    b"------------------",
                ],
            )
            session = SmartEssBleSession(link)
            await session.connect()

            response = await session.exchange_text("AT+INTPARA49?", timeout=1.0)

            self.assertEqual(
                parse_wifi_scan_response(response),
                (SmartEssBleWifiNetwork(ssid="Home WiFi", signal=-48),),
            )
            self.assertEqual(
                link.read_calls,
                [
                    VENDOR_LAYOUT.write_uuid,
                    VENDOR_LAYOUT.write_uuid,
                    VENDOR_LAYOUT.write_uuid,
                ],
            )

        asyncio.run(_run())

    def test_session_exchange_text_can_preserve_pending_notification_before_send(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
            session = SmartEssBleSession(link)
            await session.connect()
            link.push_notification(b"AT+WFLKAP:W000\r\n")

            response = await session.exchange_text(
                "AT+LINK?",
                timeout=0.05,
                append_crlf=False,
                response=True,
                drain_before_send=False,
            )

            self.assertEqual(response, "AT+WFLKAP:W000")
            self.assertEqual(
                link.write_calls,
                [(VENDOR_LAYOUT.write_uuid, b"AT+LINK?", True)],
            )

        asyncio.run(_run())

    def test_session_disconnect_unsubscribes_and_clears_state(self) -> None:
        async def _run() -> None:
            link = _FakeBleLink([VENDOR_LAYOUT.service_uuid])
            session = SmartEssBleSession(link)
            await session.connect()
            link.push_notification(b"AT+PING:W000\r\n")

            await session.disconnect()

            self.assertFalse(session.connected)
            self.assertEqual(link.stopped_notify_uuid, VENDOR_LAYOUT.notify_uuid)
            with self.assertRaisesRegex(SmartEssBleError, "ble_layout_not_selected"):
                _ = session.layout

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()