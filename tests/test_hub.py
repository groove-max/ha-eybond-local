from __future__ import annotations

from pathlib import Path
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.collector.at import CollectorAtResponse
from custom_components.eybond_local.models import (
    CollectorInfo,
    DetectedInverter,
    ProbeTarget,
    RuntimeSnapshot,
)
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile


class _FakeLinkManager:
    def __init__(self, *, heartbeat_result: bool = True) -> None:
        self.connected = True
        self.reset_calls = 0
        self.heartbeat_result = heartbeat_result
        self.collector_info = CollectorInfo(
            remote_ip="192.168.1.14",
            last_udp_reply="collector-reply",
            last_udp_reply_from="192.168.1.14",
        )
        self.transport = object()
        self.collector_at_transport = None

    async def async_try_connect(self, *, timeout: float, require_heartbeat: bool = False) -> bool:
        if require_heartbeat and not self.heartbeat_result:
            return False
        self.connected = True
        return self.connected

    async def async_ensure_connected(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> None:
        ok = await self.async_try_connect(timeout=timeout, require_heartbeat=require_heartbeat)
        if not ok:
            if require_heartbeat and self.connected:
                raise ConnectionError("collector_heartbeat_timeout")
            raise ConnectionError("collector_not_connected")

    async def async_reset_connection(self, *, reason: str = "") -> None:
        self.reset_calls += 1
        self.connected = False

    def listener_diagnostics(self) -> dict[str, object]:
        return {
            "collector_callback_session_protocol": "at_text",
            "collector_callback_identity_strategy": "at_dtupn",
        }


class _StaleHeartbeatThenRecoveredLinkManager(_FakeLinkManager):
    def __init__(self) -> None:
        super().__init__()
        self.heartbeat_attempts = 0

    async def async_try_connect(self, *, timeout: float, require_heartbeat: bool = False) -> bool:
        if require_heartbeat:
            self.heartbeat_attempts += 1
            if self.heartbeat_attempts == 1:
                self.connected = True
                return False
        self.connected = True
        return True


class _ProxyRouteLinkManager(_FakeLinkManager):
    def __init__(self) -> None:
        super().__init__()
        self.reverse_discovery_flags: list[bool] = []
        self.reverse_discovery_calls: list[dict[str, float | int]] = []
        self.callback_listener_ports: list[int] = []
        self.proxy_route_start_calls: list[dict[str, object]] = []
        self.proxy_route_stop_calls = 0
        self.proxy_route_running_value = False
        self.disconnect_reasons: list[str] = []

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        self.reverse_discovery_flags.append(bool(enabled))

    async def async_ensure_callback_listener(self, port: int) -> None:
        self.callback_listener_ports.append(int(port))

    async def async_trigger_reverse_discovery(
        self,
        *,
        port: int = 0,
        timeout: float = 0.75,
    ) -> dict[str, object]:
        self.reverse_discovery_calls.append({"port": int(port), "timeout": float(timeout)})
        return {"status": "probe_sent"}

    async def async_start_proxy_capture_route(self, **kwargs) -> None:
        self.proxy_route_start_calls.append(dict(kwargs))
        self.proxy_route_running_value = True

    async def async_stop_proxy_capture_route(self) -> None:
        self.proxy_route_stop_calls += 1
        self.proxy_route_running_value = False

    def proxy_capture_route_running(self) -> bool:
        return self.proxy_route_running_value

    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        self.disconnect_reasons.append(str(reason))


class _TimeoutDriver:
    def __init__(self) -> None:
        self.calls = 0

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.calls += 1
        raise ModbusError("request_timeout")


class _DisconnectedDriver:
    def __init__(self) -> None:
        self.calls = 0

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.calls += 1
        raise ConnectionError("collector_not_connected")


class _TimeoutThenSuccessDriver:
    def __init__(self) -> None:
        self.calls = 0

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.calls += 1
        if self.calls == 1:
            raise ModbusError("request_timeout")
        return {
            "output_power": 420,
            "battery_average_power": -180,
        }


class _IllegalDataValueDriver:
    def __init__(self) -> None:
        self.read_calls = 0
        self.write_calls = 0

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.read_calls += 1
        return {
            "battery_connected": True,
            "utility_charging_allowed": True,
            "charging_active": False,
            "charging_inactive": True,
            "operating_mode": "Off-Grid",
            "max_ac_charge_current": 20,
        }

    async def async_write_capability(
        self,
        transport,
        inverter,
        capability_key,
        value,
    ):
        self.write_calls += 1
        raise ModbusError("exception_code:3")


class _WriteConfirmedDriver:
    def __init__(self) -> None:
        self.read_calls = 0
        self.write_calls = 0
        self._current_value = 20

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.read_calls += 1
        return {
            "battery_connected": True,
            "utility_charging_allowed": True,
            "charging_active": False,
            "charging_inactive": True,
            "operating_mode": "Off-Grid",
            "max_ac_charge_current": self._current_value,
        }

    async def async_write_capability(
        self,
        transport,
        inverter,
        capability_key,
        value,
    ):
        self.write_calls += 1
        self._current_value = value
        return value


class _WriteUnconfirmedDriver:
    def __init__(self) -> None:
        self.read_calls = 0
        self.write_calls = 0

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.read_calls += 1
        return {
            "battery_connected": True,
            "utility_charging_allowed": True,
            "charging_active": False,
            "charging_inactive": True,
            "operating_mode": "Off-Grid",
            "max_ac_charge_current": 20,
        }

    async def async_write_capability(
        self,
        transport,
        inverter,
        capability_key,
        value,
    ):
        self.write_calls += 1
        return value


class _WriteConfirmedWhileChargingDriver:
    def __init__(self) -> None:
        self.read_calls = 0
        self.write_calls = 0
        self._current_value = 20

    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        self.read_calls += 1
        return {
            "battery_connected": True,
            "utility_charging_allowed": True,
            "charging_active": True,
            "charging_inactive": False,
            "operating_mode": "Off-Grid",
            "max_ac_charge_current": self._current_value,
        }

    async def async_write_capability(
        self,
        transport,
        inverter,
        capability_key,
        value,
    ):
        self.write_calls += 1
        self._current_value = value
        return value


class _CollectorQueryTransport:
    def __init__(self, responses: dict[tuple[int, bytes], bytes]) -> None:
        self._responses = dict(responses)
        self.requests: list[tuple[int, bytes]] = []

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ):
        self.requests.append((fcode, payload))
        return (None, self._responses[(fcode, payload)])


class _CollectorManagementTransport:
    def __init__(self) -> None:
        self.endpoint = "47.91.67.66,18899,TCP"
        self.reboot_required = "0"
        self.requests: list[tuple[int, bytes]] = []

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ):
        self.requests.append((fcode, payload))
        if fcode == 2:
            parameter = payload[0]
            if parameter == 21:
                return (None, bytes((0, 21)) + self.endpoint.encode("ascii"))
            if parameter == 30:
                return (None, bytes((0, 30)) + self.reboot_required.encode("ascii"))
            raise KeyError((fcode, payload))
        if fcode == 3:
            parameter = payload[0]
            value = payload[1:].decode("ascii")
            if parameter == 21:
                self.endpoint = value
                self.reboot_required = "1"
                return (None, bytes((0, 21)))
            if parameter == 29:
                self.reboot_required = "0"
                return (None, bytes((0, 29)))
            raise KeyError((fcode, payload))
        raise KeyError((fcode, payload))


class _CollectorAtQueryTransport:
    def __init__(self, responses: dict[str, str], *, connected: bool = True) -> None:
        self._responses = dict(responses)
        self.connected = connected
        self.queries: list[str] = []
        self.writes: list[tuple[str, str]] = []

    async def async_query(self, command: str) -> CollectorAtResponse:
        self.queries.append(command)
        value = self._responses[command]
        return CollectorAtResponse(command=command, value=value, raw=f"AT+{command}:{value}")

    async def async_write(self, command: str, value: str) -> CollectorAtResponse:
        self.writes.append((command, value))
        self._responses[command] = value
        return CollectorAtResponse(command=command, value="W000", raw=f"AT+{command}:W000")


class _CollectorOnlyLinkManager(_FakeLinkManager):
    def __init__(self, at_transport: _CollectorAtQueryTransport) -> None:
        super().__init__()
        self.connected = False
        self.transport = object()
        self.collector_at_transport = at_transport

    async def async_try_connect(self, *, timeout: float, require_heartbeat: bool = False) -> bool:
        return False


class _AmbiguousActiveLinkManager(_FakeLinkManager):
    def __init__(
        self,
        transport: _CollectorQueryTransport,
        at_transport: _CollectorAtQueryTransport,
    ) -> None:
        super().__init__()
        self.connected = False
        self.transport = transport
        self.collector_at_transport = at_transport
        self.active_transport = None
        self.active_collector_at_transport = None

    async def async_try_connect(self, *, timeout: float, require_heartbeat: bool = False) -> bool:
        return False


class _InactiveActiveLinkManager(_FakeLinkManager):
    def __init__(
        self,
        transport: _CollectorQueryTransport,
        at_transport: _CollectorAtQueryTransport | None = None,
    ) -> None:
        super().__init__()
        self.connected = False
        self.transport = transport
        self.collector_at_transport = at_transport
        self.active_transport = None
        self.active_collector_at_transport = None

    async def async_try_connect(self, *, timeout: float, require_heartbeat: bool = False) -> bool:
        return False


class _RuntimeValuesDriver:
    async def async_read_values(
        self,
        transport,
        inverter,
        *,
        runtime_state=None,
        poll_interval=None,
        now_monotonic=None,
    ):
        return {"output_power": 420}


class HubSnapshotTests(unittest.TestCase):
    def test_listener_diagnostics_delegate_to_link_manager(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()

        diagnostics = hub.listener_diagnostics()

        self.assertEqual(diagnostics["collector_callback_session_protocol"], "at_text")
        self.assertEqual(diagnostics["collector_callback_identity_strategy"], "at_dtupn")

    def test_build_snapshot_includes_effective_profile_and_schema_names(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._inverter = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            variant_key="vmii_nxpw5kw",
            serial_number="553555355535552",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
            profile_name="pi30_ascii/models/vmii_nxpw5kw.json",
            register_schema_name="pi30_ascii/models/vmii_nxpw5kw.json",
        )

        snapshot = hub._build_snapshot()

        self.assertEqual(snapshot.values["driver_key"], "pi30")
        self.assertEqual(snapshot.values["runtime_driver_state"], "driver_bound")
        self.assertEqual(snapshot.values["variant_key"], "vmii_nxpw5kw")
        self.assertEqual(snapshot.values["profile_name"], "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(
            snapshot.values["register_schema_name"],
            "pi30_ascii/models/vmii_nxpw5kw.json",
        )

    def test_build_snapshot_does_not_reuse_stale_collector_identity_values(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._last_snapshot = RuntimeSnapshot(
            values={
                "smartess_collector_version": "8.50.12.3",
                "collector_type": "Wi-Fi.DTU",
                "collector_server_endpoint": "dtu_ess.eybond.com,18899,TCP",
                "collector_signal_quality": "excellent",
                "collector_virtual_bridge": True,
                "collector_bridge_version": "0.4.0",
                "collector_reboot_required": "1",
                "collector_upload_mode": "ON",
                "collector_system_time": "20250120120000",
                "collector_serial_baudrate": "2400,8,1,NONE",
                "smartess_protocol_asset_id": "0925",
            }
        )

        snapshot = hub._build_snapshot()

        self.assertNotIn("smartess_collector_version", snapshot.values)
        self.assertNotIn("collector_type", snapshot.values)
        self.assertNotIn("collector_server_endpoint", snapshot.values)
        self.assertNotIn("collector_signal_quality", snapshot.values)
        self.assertNotIn("collector_virtual_bridge", snapshot.values)
        self.assertNotIn("collector_bridge_version", snapshot.values)
        self.assertNotIn("collector_reboot_required", snapshot.values)
        self.assertNotIn("collector_upload_mode", snapshot.values)
        self.assertNotIn("collector_system_time", snapshot.values)
        self.assertNotIn("collector_serial_baudrate", snapshot.values)
        self.assertNotIn("smartess_protocol_asset_id", snapshot.values)

    def test_collector_phase_publish_preserves_previous_inverter_values(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._inverter = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            serial_number="553555355535552",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
        )
        hub._last_snapshot = RuntimeSnapshot(
            connected=True,
            values={
                "grid_voltage": 230.0,
                "collector_serial_baudrate": "2400,8,1,NONE",
            },
        )
        observed: list[RuntimeSnapshot] = []
        hub.set_runtime_snapshot_observer(observed.append)

        # Once an inverter is detected there is no detection status, and the
        # collector-phase publish is skipped entirely: no mid-cycle snapshot
        # may overwrite the previously published inverter values.
        hub._publish_intermediate_snapshot(
            {"collector_serial_baudrate": "9600,8,1,NONE"},
            status="",
        )

        self.assertEqual(observed, [])
        self.assertEqual(hub._last_snapshot.values["grid_voltage"], 230.0)
        self.assertEqual(
            hub._last_snapshot.values["collector_serial_baudrate"],
            "2400,8,1,NONE",
        )

        # While detection is still running the publish happens and carries
        # the detection status alongside the fresh collector values.
        hub._publish_intermediate_snapshot(
            {"collector_serial_baudrate": "9600,8,1,NONE"},
            status="detecting_inverter",
        )

        self.assertEqual(len(observed), 1)
        self.assertEqual(
            observed[0].values["runtime_detection_status"],
            "detecting_inverter",
        )
        self.assertEqual(
            observed[0].values["collector_serial_baudrate"],
            "9600,8,1,NONE",
        )

    def test_build_snapshot_adds_canonical_common_values_for_pi30(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._inverter = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PowMr 4.2kW",
            variant_key="vmii_nxpw5kw",
            serial_number="553555355535552",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
        )

        snapshot = hub._build_snapshot(
            extra_values={
                "input_voltage": 230.0,
                "input_frequency": 50.0,
                "output_active_power": 1400,
                "pv_input_voltage": 118.0,
                "pv_input_current": 8.5,
                "pv_input_power": 1003,
                "battery_voltage": 51.2,
                "battery_charge_current": 12.0,
                "battery_discharge_current": 0.0,
            }
        )

        self.assertEqual(snapshot.values["grid_voltage"], 230.0)
        self.assertEqual(snapshot.values["grid_frequency"], 50.0)
        self.assertEqual(snapshot.values["output_power"], 1400)
        self.assertEqual(snapshot.values["pv_voltage"], 118.0)
        self.assertEqual(snapshot.values["pv_current"], 8.5)
        self.assertEqual(snapshot.values["pv_power"], 1003)
        self.assertEqual(snapshot.values["battery_power"], 614.4)

    def test_build_snapshot_includes_collector_churn_markers(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._link_manager.collector_info.connection_count = 3
        hub._link_manager.collector_info.connection_replace_count = 1
        hub._link_manager.collector_info.disconnect_count = 2
        hub._link_manager.collector_info.pending_request_drop_count = 4
        hub._link_manager.collector_info.last_disconnect_reason = "collector_connection_reset"
        hub._link_manager.collector_info.discovery_restart_count = 5
        hub._link_manager.collector_info.last_discovery_reason = "heartbeat_timeout"

        snapshot = hub._build_snapshot()

        self.assertEqual(snapshot.values["collector_connection_count"], 3)
        self.assertEqual(snapshot.values["collector_connection_replace_count"], 1)
        self.assertEqual(snapshot.values["collector_disconnect_count"], 2)
        self.assertEqual(snapshot.values["collector_pending_request_drop_count"], 4)
        self.assertEqual(
            snapshot.values["collector_last_disconnect_reason"],
            "collector_connection_reset",
        )
        self.assertEqual(snapshot.values["collector_discovery_restart_count"], 5)
        self.assertEqual(
            snapshot.values["collector_last_discovery_reason"],
            "heartbeat_timeout",
        )

    def test_build_snapshot_prefers_more_complete_runtime_collector_pn(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._link_manager.collector_info.collector_pn = "E5000020000000"

        snapshot = hub._build_snapshot(
            extra_values={"collector_pn": "E50000200000000001"}
        )

        self.assertEqual(snapshot.collector.collector_pn, "E50000200000000001")
        self.assertEqual(snapshot.collector.collector_pn_prefix, "E")
        self.assertEqual(snapshot.collector.collector_pn_digits, "50000200000000001")
        self.assertEqual(snapshot.values["collector_pn"], "E50000200000000001")

    def test_support_evidence_skips_generic_scan_for_bridge_probe_timeout(self) -> None:
        async def _run() -> dict[str, object]:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._link_manager.collector_info.collector_virtual_bridge = True
            hub._driver = None
            hub._inverter = None

            async def _detect_driver() -> str:
                return "smartess_local:probe_timeout"

            async def _generic_support_evidence(_detect_error: str) -> dict[str, object]:
                raise AssertionError("generic register scan must be skipped")

            hub._async_detect_driver = _detect_driver
            hub._async_capture_generic_support_evidence = _generic_support_evidence
            return await hub.async_capture_support_evidence()

        evidence = asyncio.run(_run())

        self.assertEqual(evidence["capture_kind"], "collector_only")
        self.assertEqual(evidence["detection_error"], "smartess_local:probe_timeout")
        self.assertEqual(evidence["captures"], [])

    def test_build_snapshot_recomputes_smg_canonical_battery_power(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="92632500000001",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
        )
        hub._last_snapshot = hub._build_snapshot(
            extra_values={
                "battery_average_power": -216.0,
            }
        )

        snapshot = hub._build_snapshot(
            extra_values={
                "battery_average_power": -144.0,
            }
        )

        self.assertEqual(snapshot.values["battery_average_power"], -144.0)
        self.assertEqual(snapshot.values["battery_power"], -144.0)

    def test_async_refresh_marks_snapshot_disconnected_on_request_timeout(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _TimeoutDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
            )
            hub._last_snapshot = hub._build_snapshot(
                extra_values={
                    "output_power": 50,
                    "battery_average_power": -71,
                }
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(snapshot.connected)
            self.assertEqual(snapshot.last_error, "request_timeout")
            self.assertEqual(snapshot.values["output_power"], 50)
            self.assertEqual(snapshot.values["battery_power"], -71)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 1)
            self.assertGreater(snapshot.values["runtime_backoff_seconds"], 0)
            self.assertEqual(hub._link_manager.reset_calls, 1)
            self.assertEqual(hub._driver.calls, 2)

        asyncio.run(_run())

    def test_async_refresh_merges_safe_collector_runtime_queries(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            link_manager.transport = _CollectorQueryTransport(
                {
                    (2, b"\x04"): b"\x00\x041.11",
                    (2, b"\x05"): b"\x00\x058.50.12.3",
                    (2, b"\x06"): b"\x00\x061.0",
                    (2, b"\x0e"): b"\x00\x0e0925#Hybrid",
                    (2, b"\x10"): b"\x00\x10192.168.1.55",
                    (2, b"\x15"): b"\x00\x15192.168.1.193,18899,TCP",
                    (2, b"\x1e"): b"\x00\x1e1",
                    (2, b"\x20"): b"\x00\x20RTU",
                    (2, b"\x22"): b"\x00\x229600,8,1,NONE",
                    (2, b"\x30"): b"\x00\x30STA:-67",
                    (2, b"\x37"): b"\x00\x37-67",
                }
            )
            hub._link_manager = link_manager
            hub._driver = _RuntimeValuesDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="INV123",
                probe_target=ProbeTarget(devcode=1, collector_addr=1, device_addr=1),
                profile_name="builtin:profiles/modbus_smg/default.json",
                register_schema_name="builtin:register_schemas/modbus_smg/models/smg_6200.json",
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertEqual(snapshot.values["smartess_collector_version"], "8.50.12.3")
            self.assertEqual(snapshot.values["collector_protocol_version"], "1.11")
            self.assertEqual(snapshot.values["collector_hardware_version"], "1.0")
            self.assertEqual(snapshot.values["collector_local_ip_address"], "192.168.1.55")
            self.assertEqual(snapshot.values["collector_server_endpoint"], "192.168.1.193,18899,TCP")
            self.assertEqual(snapshot.values["collector_reboot_required"], "1")
            self.assertEqual(snapshot.values["collector_transmission_mode"], "RTU")
            self.assertEqual(snapshot.values["collector_serial_baudrate"], "9600,8,1,NONE")
            self.assertEqual(snapshot.values["collector_network_diagnostics"], "STA:-67")
            self.assertEqual(snapshot.values["collector_signal_strength"], -67)
            self.assertEqual(snapshot.values["collector_signal_strength_raw"], "-67")
            self.assertEqual(snapshot.values["collector_signal_strength_source"], "Wi-Fi RSSI")
            self.assertEqual(snapshot.values["collector_signal_quality"], "excellent")
            self.assertEqual(snapshot.values["collector_callback_owner"], "Custom endpoint")
            self.assertEqual(snapshot.values["smartess_protocol_asset_id"], "0925")
            self.assertEqual(snapshot.values["smartess_protocol_profile_key"], "smartess_0925")

        asyncio.run(_run())

    def test_async_refresh_prefers_at_signal_queries_over_fc_values(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            link_manager.transport = _CollectorQueryTransport(
                {
                    (2, b"\x15"): b"\x00\x15fc.example,18899,TCP",
                    (2, b"\x29"): b"\x00\x29MyWiFi",
                    (2, b"\x30"): b"\x00\x301",
                    (2, b"\x37"): b"\x00\x371",
                }
            )
            link_manager.collector_at_transport = _CollectorAtQueryTransport(
                {
                    "ATVER": "2.05",
                    "CLDSRVHOST1": "at.example,18899,TCP",
                    "DTUPN": "E1234567890",
                    "DTUTYPE": "Wi-Fi.DTU",
                    "ENUPMODE": "ON",
                    "FWVER": "8.50.12.3",
                    "HTBT": "60",
                    "INTPARA49": "ssid1,-55;ssid2,-71",
                    "LINK": "STA,CONNECTED",
                    "SYST": "20250120120000",
                    "UART": "9600,8,1,NONE",
                    "WFSS": "-55",
                }
            )
            hub._link_manager = link_manager
            hub._driver = _RuntimeValuesDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="INV123",
                probe_target=ProbeTarget(devcode=1, collector_addr=1, device_addr=1),
                profile_name="builtin:profiles/modbus_smg/default.json",
                register_schema_name="builtin:register_schemas/modbus_smg/models/smg_6200.json",
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertEqual(snapshot.values["collector_server_endpoint"], "at.example,18899,TCP")
            self.assertEqual(snapshot.values["collector_signal_strength"], -55)
            self.assertEqual(snapshot.values["collector_signal_strength_raw"], "-55")
            self.assertEqual(snapshot.values["collector_signal_strength_source"], "Wi-Fi RSSI")
            self.assertEqual(snapshot.values["collector_signal_quality"], "excellent")
            self.assertEqual(snapshot.values["collector_type"], "Wi-Fi.DTU")
            self.assertEqual(snapshot.values["collector_upload_mode"], "ON")
            self.assertEqual(snapshot.values["collector_system_time"], "20250120120000")
            self.assertEqual(snapshot.values["collector_cloud_heartbeat_value"], "60")
            self.assertEqual(snapshot.values["collector_ssid"], "MyWiFi")
            self.assertEqual(snapshot.values["collector_link_status"], "STA,CONNECTED")
            self.assertEqual(snapshot.values["collector_wifi_scan_list"], "ssid1,-55;ssid2,-71")

        asyncio.run(_run())

    def test_async_refresh_returns_live_collector_at_snapshot_when_framed_link_is_missing(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            at_transport = _CollectorAtQueryTransport(
                {
                    "ATVER": "2.05",
                    "CLDSRVHOST1": "at.example,18899,TCP",
                    "DTUPN": "E1234567890",
                    "DTUTYPE": "Wi-Fi.DTU",
                    "ENUPMODE": "ON",
                    "FWVER": "8.50.12.3",
                    "HTBT": "60",
                    "INTPARA49": "ssid1,-55;ssid2,-71",
                    "LINK": "STA,CONNECTED",
                    "SYST": "20250120120000",
                    "UART": "9600,8,1,NONE",
                    "WFSS": "-55",
                },
                connected=False,
            )
            hub._link_manager = _CollectorOnlyLinkManager(at_transport)

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(snapshot.connected)
            self.assertEqual(snapshot.last_error, "inverter_heartbeat_missing")
            self.assertEqual(snapshot.values["runtime_driver_state"], "driver_unbound")
            self.assertEqual(snapshot.values["collector_protocol_version"], "2.05")
            self.assertEqual(snapshot.values["collector_server_endpoint"], "at.example,18899,TCP")
            self.assertEqual(snapshot.values["collector_signal_strength"], -55)
            self.assertEqual(snapshot.values["collector_signal_quality"], "excellent")
            self.assertEqual(snapshot.values["collector_type"], "Wi-Fi.DTU")
            self.assertEqual(snapshot.values["collector_upload_mode"], "ON")
            self.assertEqual(snapshot.values["collector_cloud_heartbeat_value"], "60")
            self.assertNotIn("collector_ssid", snapshot.values)
            self.assertEqual(snapshot.values["collector_link_status"], "STA,CONNECTED")
            self.assertEqual(snapshot.values["collector_wifi_scan_list"], "ssid1,-55;ssid2,-71")

        asyncio.run(_run())

    def test_async_refresh_does_not_reuse_stale_collector_runtime_cache_when_offline(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._collector_runtime_values = {
                "collector_server_endpoint": "dtu_ess.eybond.com,18899,TCP",
                "collector_reboot_required": "1",
                "collector_upload_mode": "ON",
                "collector_system_time": "20250120120000",
            }
            hub._collector_at_runtime_values = {
                "smartess_collector_version": "8.50.12.3",
                "collector_type": "Wi-Fi.DTU",
            }
            hub._link_manager = _CollectorOnlyLinkManager(
                _CollectorAtQueryTransport({}, connected=False)
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(snapshot.connected)
            self.assertEqual(snapshot.last_error, "waiting_for_collector")
            self.assertNotIn("collector_server_endpoint", snapshot.values)
            self.assertNotIn("collector_reboot_required", snapshot.values)
            self.assertNotIn("collector_upload_mode", snapshot.values)
            self.assertNotIn("collector_system_time", snapshot.values)
            self.assertNotIn("smartess_collector_version", snapshot.values)
            self.assertNotIn("collector_type", snapshot.values)

        asyncio.run(_run())

    def test_invalidate_collector_runtime_values_clears_cached_uart(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._collector_runtime_values = {
            "collector_serial_baudrate": "2400,8,1,NONE",
        }
        hub._collector_at_runtime_values = {
            "collector_link_status": "STA,CONNECTED",
        }
        hub._collector_runtime_values_dirty = False

        hub.invalidate_collector_runtime_values()

        self.assertEqual(hub._collector_runtime_values, {})
        self.assertEqual(hub._collector_at_runtime_values, {})
        self.assertTrue(hub._collector_runtime_values_dirty)

    def test_async_refresh_skips_runtime_collector_queries_when_active_transports_are_ambiguous(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            transport = _CollectorQueryTransport(
                {
                    (2, b"\x15"): b"\x00\x15wrong.example,18899,TCP",
                }
            )
            at_transport = _CollectorAtQueryTransport(
                {
                    "ATVER": "2.05",
                }
            )
            hub._link_manager = _AmbiguousActiveLinkManager(transport, at_transport)

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(snapshot.connected)
            self.assertEqual(snapshot.last_error, "waiting_for_collector")
            self.assertEqual(transport.requests, [])
            self.assertEqual(at_transport.queries, [])
            self.assertNotIn("collector_protocol_version", snapshot.values)
            self.assertNotIn("collector_server_endpoint", snapshot.values)

        asyncio.run(_run())

    def test_async_refresh_bootstraps_virtual_bridge_metadata_without_heartbeat(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            transport = _CollectorQueryTransport(
                {
                    (2, b"\x06"): b"\x00\x06esp-collector/0.1.5/ESP32",
                }
            )
            hub._link_manager = _InactiveActiveLinkManager(transport)

            snapshot = await hub.async_refresh(poll_interval=10.0)

            self.assertTrue(snapshot.connected)
            self.assertEqual(snapshot.last_error, "inverter_heartbeat_missing")
            self.assertEqual(snapshot.values["runtime_driver_state"], "driver_unbound")
            self.assertIn((2, b"\x06"), transport.requests)
            self.assertTrue(snapshot.collector.collector_virtual_bridge)
            self.assertEqual(snapshot.collector.collector_bridge_kind, "esp-collector")
            self.assertEqual(snapshot.collector.collector_bridge_version, "0.1.5")
            self.assertTrue(snapshot.values["collector_virtual_bridge"])
            self.assertEqual(snapshot.values["collector_bridge_kind"], "esp-collector")
            self.assertEqual(snapshot.values["collector_bridge_version"], "0.1.5")

        asyncio.run(_run())

    def test_build_snapshot_normalizes_signal_quality_for_gprs_csq(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_signal_strength": -111,
                "collector_signal_strength_source": "gprs_csq",
                "collector_signal_strength_raw": "1",
            }
        )

        self.assertEqual(snapshot.values["collector_signal_strength"], -111)
        self.assertEqual(snapshot.values["collector_signal_strength_source"], "GPRS CSQ")
        self.assertEqual(snapshot.values["collector_signal_quality"], "weak")

    def test_build_snapshot_marks_proxy_callback_on_home_assistant_as_home_assistant(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_server_endpoint": "192.168.1.10,18899,TCP",
            }
        )

        self.assertEqual(snapshot.values["collector_callback_owner"], "Home Assistant")

    def test_proxy_capture_route_methods_delegate_to_link_manager(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _ProxyRouteLinkManager()
            hub._link_manager = link_manager

            hub.set_reverse_discovery_enabled(False)
            await hub.async_ensure_callback_listener(18899)
            await hub.async_trigger_reverse_discovery(timeout=1.25)
            await hub.async_start_proxy_capture_route(
                collector_ip="192.168.1.14",
                listen_port=18899,
                upstream_host="dtu_ess.eybond.com",
                upstream_port=18899,
                output_path=Path("/tmp/proxy-capture.jsonl"),
                masked_endpoint="dtu_ess.eybond.com,18899,TCP",
                restore_trigger_path=Path("/tmp/proxy-capture.restore"),
            )
            await hub.async_disconnect_collector_connections(reason="proxy_capture_start")

            self.assertEqual(link_manager.reverse_discovery_flags, [False])
            self.assertEqual(link_manager.callback_listener_ports, [18899])
            self.assertEqual(
                link_manager.reverse_discovery_calls,
                [{"port": 0, "timeout": 1.25}],
            )
            self.assertTrue(hub.proxy_capture_route_running())
            self.assertEqual(
                link_manager.proxy_route_start_calls,
                [
                    {
                        "collector_ip": "192.168.1.14",
                        "collector_pn": "",
                        "collector_session_protocol": "",
                        "listen_port": 18899,
                        "upstream_host": "dtu_ess.eybond.com",
                        "upstream_port": 18899,
                        "output_path": Path("/tmp/proxy-capture.jsonl"),
                        "masked_endpoint": "dtu_ess.eybond.com,18899,TCP",
                        "restore_trigger_path": Path("/tmp/proxy-capture.restore"),
                    }
                ],
            )
            self.assertEqual(link_manager.disconnect_reasons, ["proxy_capture_start"])

            await hub.async_stop_proxy_capture_route()

            self.assertEqual(link_manager.proxy_route_stop_calls, 1)
            self.assertFalse(hub.proxy_capture_route_running())

        asyncio.run(_run())

    def test_async_set_collector_server_endpoint_stages_and_applies_parameter_21(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            link_manager.transport = transport
            hub._link_manager = link_manager

            result = await hub.async_set_collector_server_endpoint(
                "192.168.1.193,18899,TCP",
                apply_changes=True,
            )

            self.assertEqual(result["status"], "applied")
            self.assertEqual(result["previous_endpoint"], "47.91.67.66,18899,TCP")
            self.assertEqual(result["requested_endpoint"], "192.168.1.193,18899,TCP")
            self.assertEqual(result["readback_endpoint"], "192.168.1.193,18899,TCP")
            self.assertEqual(hub._collector_runtime_values["collector_server_endpoint"], "192.168.1.193,18899,TCP")
            self.assertEqual(hub._collector_runtime_values["collector_reboot_required"], "1")
            self.assertEqual(
                transport.requests,
                [
                    (2, b"\x15"),
                    (3, b"\x15192.168.1.193,18899,TCP"),
                    (2, b"\x15"),
                    (2, b"\x1e"),
                    (3, b"\x1d1"),
                ],
            )

        asyncio.run(_run())

    def test_async_set_collector_server_endpoint_uses_at_management_when_fc_path_is_missing(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            link_manager.transport = object()
            at_transport = _CollectorAtQueryTransport(
                {"CLDSRVHOST1": "iot.eybond.com,18899,TCP"}
            )
            link_manager.collector_at_transport = at_transport
            hub._link_manager = link_manager

            result = await hub.async_set_collector_server_endpoint(
                "192.168.8.113,18899,TCP",
                apply_changes=True,
            )

            self.assertEqual(result["status"], "applied")
            self.assertEqual(result["management_protocol"], "at_text")
            self.assertEqual(result["at_apply_response"], "W000")
            self.assertEqual(result["previous_endpoint"], "iot.eybond.com,18899,TCP")
            self.assertEqual(result["requested_endpoint"], "192.168.8.113,18899,TCP")
            self.assertEqual(result["readback_endpoint"], "192.168.8.113,18899,TCP")
            self.assertEqual(
                hub._collector_runtime_values["collector_server_endpoint"],
                "192.168.8.113,18899,TCP",
            )
            self.assertEqual(at_transport.queries, ["CLDSRVHOST1", "CLDSRVHOST1"])
            self.assertEqual(
                at_transport.writes,
                [
                    ("CLDSRVHOST1", "192.168.8.113,18899,TCP"),
                    ("INTPARA", "29,1"),
                ],
            )

        asyncio.run(_run())

    def test_async_apply_collector_changes_triggers_parameter_29_without_endpoint_change(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            transport.reboot_required = "1"
            link_manager.transport = transport
            hub._link_manager = link_manager

            result = await hub.async_apply_collector_changes()

            self.assertEqual(result["status"], "applied")
            self.assertEqual(result["action"], "apply")
            self.assertEqual(result["current_endpoint"], "47.91.67.66,18899,TCP")
            self.assertEqual(result["reboot_required_before"], "1")
            self.assertEqual(hub._collector_runtime_values["collector_reboot_required"], "0")
            self.assertEqual(
                transport.requests,
                [
                    (2, b"\x15"),
                    (2, b"\x1e"),
                    (3, b"\x1d1"),
                ],
            )

        asyncio.run(_run())

    def test_async_reboot_collector_allows_virtual_bridge_without_reboot_feature(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            link_manager.transport = transport
            link_manager.collector_info.collector_virtual_bridge = True
            link_manager.collector_info.collector_bridge_kind = "esp-collector"
            hub._link_manager = link_manager

            result = await hub.async_reboot_collector()

            self.assertEqual(result["status"], "reboot_triggered")
            self.assertEqual(result["action"], "reboot")
            self.assertEqual(
                transport.requests,
                [
                    (2, b"\x15"),
                    (2, b"\x1e"),
                    (3, b"\x1d1"),
                ],
            )

        asyncio.run(_run())

    def test_async_reboot_collector_allows_virtual_bridge_with_reboot_feature(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            link_manager.transport = transport
            link_manager.collector_info.collector_virtual_bridge = True
            link_manager.collector_info.collector_bridge_kind = "esp-collector"
            hub._link_manager = link_manager

            result = await hub.async_reboot_collector()

            self.assertEqual(result["status"], "reboot_triggered")
            self.assertEqual(result["action"], "reboot")
            self.assertEqual(
                transport.requests,
                [
                    (2, b"\x15"),
                    (2, b"\x1e"),
                    (3, b"\x1d1"),
                ],
            )

        asyncio.run(_run())

    def test_async_rollback_collector_server_endpoint_uses_session_cached_previous_value(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            link_manager.transport = transport
            hub._link_manager = link_manager

            await hub.async_set_collector_server_endpoint(
                "192.168.1.193,18899,TCP",
                apply_changes=False,
            )
            result = await hub.async_rollback_collector_server_endpoint(apply_changes=False)

            self.assertEqual(result["status"], "rollback_staged")
            self.assertEqual(result["rollback_source"], "session_cached_previous_endpoint")
            self.assertEqual(result["rollback_endpoint"], "47.91.67.66,18899,TCP")
            self.assertEqual(result["readback_endpoint"], "47.91.67.66,18899,TCP")
            self.assertEqual(hub._collector_runtime_values["collector_server_endpoint"], "47.91.67.66,18899,TCP")

        asyncio.run(_run())

    def test_async_rollback_collector_server_endpoint_preserves_host_only_previous_value(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            transport.endpoint = "ess.eybond.com"
            link_manager.transport = transport
            hub._link_manager = link_manager

            await hub.async_set_collector_server_endpoint(
                "192.168.1.193,18899,TCP",
                apply_changes=False,
            )
            result = await hub.async_rollback_collector_server_endpoint(apply_changes=False)

            self.assertEqual(result["rollback_source"], "session_cached_previous_endpoint")
            self.assertEqual(result["rollback_endpoint"], "ess.eybond.com")
            self.assertEqual(result["readback_endpoint"], "ess.eybond.com")
            self.assertEqual(hub._collector_runtime_values["collector_server_endpoint"], "ess.eybond.com")

        asyncio.run(_run())

    def test_async_rollback_collector_server_endpoint_requires_cached_previous_value(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link_manager = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            transport.server_endpoint = "192.168.1.10,18899,TCP"
            link_manager.transport = transport
            hub._link_manager = link_manager
            hub._collector_runtime_values["collector_server_endpoint"] = "192.168.1.10,18899,TCP"

            with self.assertRaisesRegex(RuntimeError, "collector_rollback_endpoint_unavailable"):
                await hub.async_rollback_collector_server_endpoint(apply_changes=False)

        asyncio.run(_run())


class HubWriteBlockerTests(unittest.TestCase):
    def test_exception_code_3_returns_friendly_error_without_persistent_blocker(self) -> None:
        async def _run() -> None:
            profile = load_driver_profile("smg_modbus.json")
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _IllegalDataValueDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
                capabilities=profile.capabilities,
                capability_groups=profile.groups,
                capability_presets=profile.presets,
            )

            with self.assertRaisesRegex(
                ValueError,
                r"illegal_data_value:max_ac_charge_current:.*Allowed profile range:",
            ):
                await hub.async_write_capability("max_ac_charge_current", 0)

            self.assertEqual(hub._write_blockers, {})
            self.assertEqual(hub._driver.write_calls, 1)

        asyncio.run(_run())

    def test_async_write_capability_returns_when_readback_confirms_value(self) -> None:
        async def _run() -> None:
            profile = load_driver_profile("smg_modbus.json")
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _WriteConfirmedDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
                capabilities=profile.capabilities,
                capability_groups=profile.groups,
                capability_presets=profile.presets,
            )

            written = await hub.async_write_capability("max_ac_charge_current", 30)

            self.assertEqual(written, 30)
            self.assertEqual(hub._driver.write_calls, 1)
            self.assertEqual(hub._driver.read_calls, 2)

        asyncio.run(_run())

    def test_async_write_capability_raises_when_readback_stays_old(self) -> None:
        async def _run() -> None:
            profile = load_driver_profile("smg_modbus.json")
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _WriteUnconfirmedDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
                capabilities=profile.capabilities,
                capability_groups=profile.groups,
                capability_presets=profile.presets,
            )

            with self.assertRaisesRegex(
                RuntimeError,
                r"write_not_confirmed:max_ac_charge_current:Command accepted, but 'Max AC Charge Current' did not confirm by readback.",
            ):
                await hub.async_write_capability("max_ac_charge_current", 30)

            self.assertEqual(hub._driver.write_calls, 1)
            self.assertEqual(hub._driver.read_calls, 2)

        asyncio.run(_run())

    def test_async_write_capability_allows_write_attempt_while_soft_gate_is_active(self) -> None:
        async def _run() -> None:
            profile = load_driver_profile("smg_modbus.json")
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _WriteConfirmedWhileChargingDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
                capabilities=profile.capabilities,
                capability_groups=profile.groups,
                capability_presets=profile.presets,
            )

            written = await hub.async_write_capability("max_ac_charge_current", 30)

            self.assertEqual(written, 30)
            self.assertEqual(hub._driver.write_calls, 1)
            self.assertEqual(hub._driver.read_calls, 2)

        asyncio.run(_run())

    def test_async_refresh_skips_repeated_timeout_during_backoff(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _TimeoutDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
            )

            first = await hub.async_refresh(poll_interval=3.0)
            second = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(first.connected)
            self.assertFalse(second.connected)
            self.assertEqual(second.last_error, "request_timeout")
            self.assertEqual(hub._driver.calls, 2)
            self.assertEqual(hub._link_manager.reset_calls, 1)
            self.assertEqual(second.values["runtime_recovery_streak"], 1)
            self.assertGreater(second.values["runtime_backoff_seconds"], 0)

        asyncio.run(_run())

    def test_async_refresh_marks_snapshot_disconnected_on_collector_disconnect(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _DisconnectedDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(snapshot.connected)
            self.assertEqual(snapshot.last_error, "collector_not_connected")
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 1)
            self.assertEqual(hub._driver.calls, 2)

        asyncio.run(_run())

    def test_async_refresh_marks_snapshot_disconnected_on_heartbeat_timeout(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager(heartbeat_result=False)

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(snapshot.connected)
            self.assertEqual(snapshot.last_error, "collector_heartbeat_timeout")
            self.assertEqual(hub._link_manager.reset_calls, 1)
            self.assertEqual(snapshot.values["runtime_reconnect_count"], 1)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 1)

        asyncio.run(_run())

    def test_async_refresh_keeps_collector_live_when_unbound_heartbeat_is_missing(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            link = _FakeLinkManager(heartbeat_result=False)
            link.transport = _CollectorQueryTransport(
                {
                    (2, b"\x06"): b"\x00\x06esp-collector/0.1.5/ESP32",
                }
            )
            hub._link_manager = link

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(snapshot.connected)
            self.assertEqual(snapshot.last_error, "inverter_heartbeat_missing")
            self.assertEqual(snapshot.values["runtime_driver_state"], "driver_unbound")
            self.assertEqual(hub._link_manager.reset_calls, 0)
            self.assertTrue(snapshot.values["collector_virtual_bridge"])

        asyncio.run(_run())

    def test_outage_cache_clear_runs_once_per_outage(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        clears: list[int] = []
        original = hub._clear_collector_runtime_value_caches
        hub._clear_collector_runtime_value_caches = lambda: (clears.append(1), original())[1]

        hub._clear_collector_value_caches_for_outage()
        hub._clear_collector_value_caches_for_outage()
        self.assertEqual(len(clears), 1)

        hub._record_refresh_success()
        hub._clear_collector_value_caches_for_outage()
        self.assertEqual(len(clears), 2)

    def test_empty_at_metadata_result_respects_attempt_cadence(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )

            class _DeadAtTransport:
                connected = True

                def __init__(self) -> None:
                    self.queries = 0

                async def async_query(self, command: str):
                    self.queries += 1
                    raise asyncio.TimeoutError()

            at_transport = _DeadAtTransport()
            link = _FakeLinkManager()
            link.collector_at_transport = at_transport
            hub._link_manager = link

            await hub._async_read_collector_runtime_values(poll_interval=10.0)
            first_attempt_queries = at_transport.queries
            self.assertGreaterEqual(first_attempt_queries, 1)

            # Second read within the refresh interval: the dead AT link is
            # NOT re-swept just because the previous sweep yielded nothing.
            await hub._async_read_collector_runtime_values(poll_interval=10.0)
            self.assertEqual(at_transport.queries, first_attempt_queries)

        asyncio.run(_run())

    def test_dead_at_metadata_channel_is_learned_and_skipped(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )

            class _DeadAtTransport:
                connected = True

                def __init__(self) -> None:
                    self.queries = 0

                async def async_query(self, command: str):
                    self.queries += 1
                    raise asyncio.TimeoutError()

            from custom_components.eybond_local.drivers.command_support import (
                UNSUPPORTED_COMMAND_STRIKES,
                commit_cycle_failures,
                record_command_success,
            )

            at_transport = _DeadAtTransport()
            link = _FakeLinkManager()
            link.collector_at_transport = at_transport
            link.transport = _CollectorQueryTransport(
                {(2, b"\x06"): b"\x00\x06esp-collector/0.1.5/ESP32"}
            )
            hub._link_manager = link

            for _ in range(UNSUPPORTED_COMMAND_STRIKES):
                # Force each attempt through the cadence gate, emulate a
                # cycle where the framed side answered, and commit.
                hub._collector_at_runtime_last_attempt_monotonic = -1000.0
                hub._collector_runtime_last_refresh_monotonic = -1000.0
                await hub._async_read_collector_runtime_values(poll_interval=10.0)
                record_command_success(hub._runtime_read_state, "collector:fc_metadata")
                commit_cycle_failures(hub._runtime_read_state)

            learned_queries = at_transport.queries
            self.assertGreaterEqual(learned_queries, UNSUPPORTED_COMMAND_STRIKES)

            # Once the strike threshold is reached, the channel verdict
            # blocks the sweep entirely, even with the cadence forced open.
            hub._collector_at_runtime_last_attempt_monotonic = -1000.0
            hub._collector_runtime_last_refresh_monotonic = -1000.0
            await hub._async_read_collector_runtime_values(poll_interval=10.0)
            self.assertEqual(at_transport.queries, learned_queries)

            # The re-check path clears the verdict and probes again.
            hub.clear_unsupported_command_cache()
            hub._collector_at_runtime_last_attempt_monotonic = -1000.0
            hub._collector_runtime_last_refresh_monotonic = -1000.0
            await hub._async_read_collector_runtime_values(poll_interval=10.0)
            self.assertEqual(at_transport.queries, learned_queries + 1)

        asyncio.run(_run())

    def test_persistent_unsupported_commands_survive_runtime_state_reset(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        from custom_components.eybond_local.drivers.command_support import (
            command_skipped_as_unsupported,
        )

        hub.set_persistent_unsupported_commands(("QPIWS", "QET"))
        self.assertTrue(
            command_skipped_as_unsupported(hub._runtime_read_state, "QPIWS")
        )

        # A reconnect clears the session state but must re-seed device facts.
        hub._reset_runtime_read_state()
        self.assertTrue(
            command_skipped_as_unsupported(hub._runtime_read_state, "QET")
        )

        hub.clear_unsupported_command_cache()
        hub._reset_runtime_read_state()
        self.assertFalse(
            command_skipped_as_unsupported(hub._runtime_read_state, "QPIWS")
        )

    def test_async_refresh_keeps_bound_inverter_offline_when_framed_link_is_missing(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            at_transport = _CollectorAtQueryTransport({"ATVER": "2.05"}, connected=False)
            link = _CollectorOnlyLinkManager(at_transport)
            hub._link_manager = link
            hub._inverter = DetectedInverter(
                driver_key="pi30",
                protocol_family="pi30",
                model_name="PowMr 4.2kW",
                variant_key="vmii_nxpw5kw",
                serial_number="553555355535552",
                probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
                profile_name="pi30_ascii/models/vmii_nxpw5kw.json",
                register_schema_name="pi30_ascii/models/vmii_nxpw5kw.json",
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(snapshot.connected)
            self.assertEqual(snapshot.last_error, "waiting_for_collector")
            self.assertEqual(snapshot.values["runtime_driver_state"], "collector_offline")
            self.assertNotIn("collector_udp_reply", snapshot.values)
            self.assertNotIn("collector_udp_reply_from", snapshot.values)
            self.assertEqual(link.collector_info.last_udp_reply_from, "")

        asyncio.run(_run())

    def test_async_refresh_recovers_after_stale_heartbeat_reset(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _StaleHeartbeatThenRecoveredLinkManager()
            hub._driver = _RuntimeValuesDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(snapshot.connected)
            self.assertIsNone(snapshot.last_error)
            self.assertEqual(snapshot.values["output_power"], 420)
            self.assertEqual(snapshot.values["runtime_reconnect_count"], 1)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 0)

        asyncio.run(_run())

    def test_async_refresh_recovers_after_request_timeout_reconnect(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=8899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )
            hub._link_manager = _FakeLinkManager()
            hub._driver = _TimeoutThenSuccessDriver()
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(snapshot.connected)
            self.assertIsNone(snapshot.last_error)
            self.assertEqual(snapshot.values["output_power"], 420)
            self.assertEqual(snapshot.values["battery_power"], -180)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 0)
            self.assertEqual(snapshot.values["runtime_reconnect_count"], 1)
            self.assertEqual(hub._link_manager.reset_calls, 1)

        asyncio.run(_run())


class HubAtTextAsciiProbeTests(unittest.TestCase):
    @staticmethod
    def _build_hub(session_protocol: str) -> EybondHub:
        return EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.98",
                collector_ip="192.168.2.209",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
                collector_session_protocol=session_protocol,
            ),
        )

    def test_at_text_ascii_probe_records_raw_attempts(self) -> None:
        from custom_components.eybond_local.link_models import RawSerialLinkRoute
        from custom_components.eybond_local.payload.pi30 import crc16_xmodem

        class _AtTransport:
            def select_payload_route(self, route, *, payload_family=""):
                return RawSerialLinkRoute(protocol=payload_family)

            async def async_send_payload(self, payload, *, route, request_timeout=None):
                assert isinstance(route, RawSerialLinkRoute)
                if payload.startswith(b"QPIRI") or payload.startswith(b"QPIGS"):
                    raise asyncio.TimeoutError
                if payload.startswith(b"QPI"):
                    body = b"(PI30"
                    crc = crc16_xmodem(body)
                    return body + bytes(((crc >> 8) & 0xFF, crc & 0xFF)) + b"\r"
                raise asyncio.TimeoutError

        async def _run() -> None:
            hub = self._build_hub("at_text")
            link = _FakeLinkManager()
            link.transport = _AtTransport()
            hub._link_manager = link

            probe = await hub._async_capture_at_text_ascii_probe()

            assert probe is not None
            self.assertEqual(probe["session_protocol"], "at_text")
            attempts = {item["command"]: item for item in probe["attempts"]}
            self.assertIn("QPI", attempts)
            self.assertIn("QPIRI", attempts)
            self.assertIn("GPV", attempts)
            self.assertEqual(attempts["QPI"]["payload_family"], "pi30_ascii")
            self.assertTrue(attempts["QPI"]["response_ascii"].startswith("(PI30"))
            self.assertEqual(attempts["QPIRI"]["error"], "request_timeout")
            self.assertTrue(attempts["QPI"]["request_hex"])

        asyncio.run(_run())

    def test_at_text_ascii_probe_skipped_for_framed_sessions(self) -> None:
        async def _run() -> None:
            hub = self._build_hub("eybond_framed")
            hub._link_manager = _FakeLinkManager()

            probe = await hub._async_capture_at_text_ascii_probe()

            self.assertIsNone(probe)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
