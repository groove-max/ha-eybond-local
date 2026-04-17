from __future__ import annotations

from pathlib import Path
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.models import CollectorInfo, DetectedInverter, ProbeTarget
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


class HubSnapshotTests(unittest.TestCase):
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
        self.assertEqual(snapshot.values["variant_key"], "vmii_nxpw5kw")
        self.assertEqual(snapshot.values["profile_name"], "pi30_ascii/models/vmii_nxpw5kw.json")
        self.assertEqual(
            snapshot.values["register_schema_name"],
            "pi30_ascii/models/vmii_nxpw5kw.json",
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
            serial_number="92632511100118",
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
                serial_number="92632511100118",
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
                serial_number="92632511100118",
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
                serial_number="92632511100118",
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
                serial_number="92632511100118",
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
                serial_number="92632511100118",
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


if __name__ == "__main__":
    unittest.main()
