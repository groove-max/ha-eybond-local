from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.connection.collector_endpoint_operation import (
    COLLECTOR_ENDPOINT_OPERATION_AUTHORITY,
    OPERATION_COLLECTOR_SYSTEM_ACTION,
    OPERATION_RUNTIME_LINK_BAUD_SWEEP,
)
from custom_components.eybond_local.collector.at import CollectorAtResponse
from custom_components.eybond_local.collector.management import (
    CollectorManagementUnsupportedError,
)
from custom_components.eybond_local.models import (
    CollectorInfo,
    DetectedInverter,
    DriverMatch,
    ProbeTarget,
    RuntimeSnapshot,
)
from custom_components.eybond_local.runtime.driver_detection import (
    DetectedDriverContext,
    DriverCandidateScan,
    DriverSweepNoMatch,
)
from custom_components.eybond_local.drivers.modbus_write_error import ModbusWriteErrorMixin
from custom_components.eybond_local.drivers.local_register_evidence import (
    LocalRegisterBlockObservation,
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
)
from custom_components.eybond_local.payload.modbus import ModbusError
from custom_components.eybond_local.runtime.hub import EybondHub
from custom_components.eybond_local.runtime.hub.common import _write_readback_matches
from custom_components.eybond_local.metadata.profile_loader import load_driver_profile
from custom_components.eybond_local.const import DRIVER_DETECTION_FULL_SCAN


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
        # Stand-in for the configured collector target the real link uses to gate
        # a disconnected collector-only bootstrap read. Non-empty by default (the
        # base fake is used connected, so it is irrelevant there); the ambiguous
        # link fake blanks it to model an unconfigured collector.
        self.configured_collector_ip = "192.168.1.14"

    async def async_try_connect(self, *, timeout: float, require_heartbeat: bool = False) -> bool:
        if require_heartbeat and not self.heartbeat_result:
            return False
        self.connected = True
        return self.connected

    def collector_metadata_routes(self):
        """Reproduce the pre-service transport selection for characterization.

        The real link decides metadata routes from trusted session evidence; this
        test fake preserves the historical hasattr/collector_ip gating so the hub
        characterization tests still exercise the exact same channel selection.
        """

        from custom_components.eybond_local.collector.metadata import (
            build_collector_metadata_routes,
        )

        missing = object()
        collector_ip = str(getattr(self, "configured_collector_ip", "") or "").strip()
        active_transport = getattr(self, "active_transport", missing)
        if active_transport is missing:
            transport = self.transport if self.connected else None
        else:
            transport = active_transport
            if transport is None and not self.connected and collector_ip:
                transport = getattr(self, "transport", None)
        active_at = getattr(self, "active_collector_at_transport", missing)
        if active_at is missing:
            at_transport = getattr(self, "collector_at_transport", None)
        else:
            at_transport = active_at
            if at_transport is None and not self.connected and collector_ip:
                at_transport = getattr(self, "collector_at_transport", None)
        allow_disconnected = (
            at_transport is not None and not self.connected and bool(collector_ip)
        )
        fc_ok = transport is not None and hasattr(transport, "async_send_collector")
        at_usable = at_transport is not None and (
            getattr(at_transport, "connected", False) or allow_disconnected
        )
        framed = transport if fc_ok else None
        at = at_transport if at_usable else None
        bootstrap = (
            at_transport
            if (
                not fc_ok
                and at_usable
                and hasattr(at_transport, "async_query_bridge_hardware_version")
            )
            else None
        )
        return build_collector_metadata_routes(
            framed_transport=framed,
            at_transport=at,
            bootstrap_transport=bootstrap,
            generation=int(getattr(self, "owned_session_generation", 0) or 0),
            provenance="live" if self.connected else "bootstrap_claimable",
            # This test double exposes an AT transport only when its scripted
            # session is intended to have confirmed that management dialect.
            at_capability_confirmed=at is not None,
        )

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
            "collector_configured_session_protocol": "at_text",
            "collector_callback_identity_strategy": "at_dtupn",
        }

    def collector_management_adapter_id(self) -> str:
        from custom_components.eybond_local.connection.session_handle import (
            ADAPTER_COLLECTOR_AT_COMMANDS,
            ADAPTER_COLLECTOR_FRAMED_COMMANDS,
            ADAPTER_NONE,
        )

        if hasattr(self.transport, "async_send_collector"):
            return ADAPTER_COLLECTOR_FRAMED_COMMANDS
        if hasattr(self.collector_at_transport, "async_query"):
            return ADAPTER_COLLECTOR_AT_COMMANDS
        return ADAPTER_NONE


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


class _OwnedSessionHandoverLinkManager(_FakeLinkManager):
    """Expose a registry generation change before reconnecting the same collector."""

    def __init__(self) -> None:
        super().__init__()
        self.owned_session_generation = 1
        self.connect_timeouts: list[float] = []

    def has_confirmed_wire_binding(self) -> bool:
        return True

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        self.connect_timeouts.append(float(timeout))
        if not self.connected and timeout < 5.0:
            return False
        self.connected = True
        return True


class _DoubleReplacementLinkManager(_OwnedSessionHandoverLinkManager):
    """First replacement disappears; the next generation becomes usable."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_first_handover_generation = True

    async def async_try_connect(
        self,
        *,
        timeout: float,
        require_heartbeat: bool = False,
    ) -> bool:
        self.connect_timeouts.append(float(timeout))
        if (
            not self.connected
            and self.owned_session_generation == 2
            and self.fail_first_handover_generation
        ):
            self.fail_first_handover_generation = False
            self.owned_session_generation = 3
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


class _TimeoutThenDisconnectedDriver:
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
        raise ConnectionError("collector_not_connected")


class _IllegalDataValueDriver(ModbusWriteErrorMixin):
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
        *,
        runtime_state=None,
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
        *,
        runtime_state=None,
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
        *,
        runtime_state=None,
    ):
        self.write_calls += 1
        return value


class _WriteDelayedConfirmationDriver:
    def __init__(self) -> None:
        self.read_calls = 0
        self.write_calls = 0
        self._written_value = 20

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
        # Read 1 is the editability snapshot. Read 2 is the first post-write
        # full poll and still exposes the old value. Read 3 converges.
        readback = self._written_value if self.read_calls >= 3 else 20
        return {
            "battery_connected": True,
            "utility_charging_allowed": True,
            "charging_active": False,
            "charging_inactive": True,
            "operating_mode": "Off-Grid",
            "max_ac_charge_current": readback,
        }

    async def async_write_capability(
        self,
        transport,
        inverter,
        capability_key,
        value,
        *,
        runtime_state=None,
    ):
        self.write_calls += 1
        self._written_value = value
        return value


class _AdvancingClockWriteDriver:
    def __init__(self, *, readback: str) -> None:
        self.read_calls = 0
        self.write_calls = 0
        self._written = False
        self._readback = readback

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
            "inverter_time": self._readback if self._written else "19:04:03",
        }

    async def async_write_capability(
        self,
        transport,
        inverter,
        capability_key,
        value,
        *,
        runtime_state=None,
    ):
        self.write_calls += 1
        self._written = True
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
        *,
        runtime_state=None,
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
        self.uart = "2400,8,1,NONE"
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
            if parameter == 34:
                return (None, bytes((0, 34)) + self.uart.encode("ascii"))
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
            if parameter == 34:
                self.uart = f"{value},8,1,NONE"
                return (None, bytes((0, 34)))
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
        # No configured collector target: the disconnected collector-only read is
        # not allowed, so an ambiguous inactive session yields no metadata reads.
        self.configured_collector_ip = ""

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


class _SeedDriver:
    pass


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

        self.assertEqual(diagnostics["collector_configured_session_protocol"], "at_text")
        self.assertEqual(diagnostics["collector_callback_identity_strategy"], "at_dtupn")

    def test_initial_inverter_binding_seeds_runtime_driver_state(self) -> None:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="",
                collector_pn="V001020SYN62344022",
                tcp_port=18899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
                collector_configured_session_protocol="at_text",
            ),
        )
        hub._link_manager = _FakeLinkManager()
        driver = _SeedDriver()
        inverter = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="PI30 3500",
            variant_key="default",
            serial_number="55355535553555",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
            profile_name="pi30_ascii/models/smartess_0925_compat.json",
            register_schema_name="pi30_ascii/models/smartess_0925_compat.json",
        )

        hub.set_initial_inverter_binding(driver, inverter)  # type: ignore[arg-type]
        snapshot = hub._build_snapshot()

        self.assertIs(hub._driver, driver)
        self.assertIs(hub._inverter, inverter)
        self.assertEqual(snapshot.values["runtime_driver_state"], "driver_bound")
        self.assertEqual(snapshot.values["driver_key"], "pi30")

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

    def test_build_snapshot_synchronizes_fresh_endpoint_into_collector_info(self) -> None:
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
        hub._link_manager.collector_info.collector_server_endpoint = (
            "old.example,18899,TCP"
        )

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_server_endpoint": "fresh.example,18899,TCP",
            }
        )

        self.assertEqual(
            snapshot.collector.collector_server_endpoint,
            "fresh.example,18899,TCP",
        )
        self.assertEqual(
            snapshot.values["collector_server_endpoint"],
            "fresh.example,18899,TCP",
        )

    def test_build_snapshot_refuses_foreign_collector_pn_override(self) -> None:
        durable_pn = "V001020SYN62344022"
        foreign_pn = "V001020ABC99999999"
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                collector_pn=durable_pn,
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub._link_manager.collector_info.collector_pn = durable_pn

        snapshot = hub._build_snapshot(
            extra_values={"collector_pn": foreign_pn}
        )

        self.assertEqual(snapshot.collector.collector_pn, durable_pn)
        self.assertEqual(snapshot.values["collector_pn"], durable_pn)
        self.assertTrue(snapshot.values["collector_pn_identity_conflict"])

    def test_build_snapshot_synchronizes_fresh_cloud_profile_as_one_value(self) -> None:
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
        hub._link_manager.collector_info.collector_cloud_profile_key = "stale"
        hub._link_manager.collector_info.collector_cloud_profile_label = "Stale"
        hub._link_manager.collector_info.collector_cloud_profile_source = "entry_persisted"
        hub._link_manager.collector_info.collector_cloud_profile_confidence = "low"

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_cloud_profile_key": "valuecloud_at",
                "collector_cloud_profile_label": "ValueCloud AT",
                "collector_cloud_profile_source": "transport_sniff",
                "collector_cloud_profile_confidence": "high",
            }
        )

        self.assertEqual(snapshot.collector_cloud_profile.key, "valuecloud_at")
        self.assertEqual(snapshot.collector_cloud_profile.label, "ValueCloud AT")
        self.assertEqual(snapshot.collector_cloud_profile.source, "transport_sniff")
        self.assertEqual(snapshot.collector_cloud_profile.confidence, "high")
        self.assertEqual(
            snapshot.collector.collector_cloud_profile_key,
            "valuecloud_at",
        )
        self.assertEqual(
            snapshot.values["collector_cloud_profile_key"],
            "valuecloud_at",
        )

    def test_build_snapshot_synchronizes_stronger_cloud_family_provenance(self) -> None:
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
        collector = hub._link_manager.collector_info
        collector.collector_cloud_family = "smartess_at"
        collector.collector_cloud_family_source = "endpoint_host"
        collector.collector_cloud_family_confidence = "low"

        snapshot = hub._build_snapshot(
            extra_values={
                "collector_cloud_family": "valuecloud_at",
                "collector_cloud_family_source": "transport_sniff",
                "collector_cloud_family_confidence": "high",
            }
        )

        self.assertEqual(snapshot.collector.collector_cloud_family, "valuecloud_at")
        self.assertEqual(
            snapshot.collector.collector_cloud_family_source,
            "transport_sniff",
        )
        self.assertEqual(snapshot.values["collector_cloud_family"], "valuecloud_at")
        self.assertEqual(
            snapshot.values["collector_cloud_family_confidence"],
            "high",
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

    def test_bound_collector_phase_does_not_publish_stale_inverter_values(self) -> None:
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

    def test_detection_phase_publish_reports_collector_state(self) -> None:
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
            connected=True,
            values={
                "collector_serial_baudrate": "2400,8,1,NONE",
            },
        )
        observed: list[RuntimeSnapshot] = []
        hub.set_runtime_snapshot_observer(observed.append)

        hub._publish_intermediate_snapshot(
            {"collector_serial_baudrate": "9600,8,1,NONE"},
            status="detecting_inverter",
        )

        self.assertEqual(len(observed), 1)
        self.assertEqual(
            observed[0].values["collector_serial_baudrate"],
            "9600,8,1,NONE",
        )
        self.assertEqual(
            observed[0].values["runtime_detection_status"],
            "detecting_inverter",
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

    def test_local_register_snapshot_uses_live_identity_and_exact_driver_result(self) -> None:
        async def _run() -> tuple[LocalRegisterSnapshot, object]:
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
            hub._link_manager.collector_info.collector_pn = "E50000200000000001"
            hub._inverter = DetectedInverter(
                driver_key="smg",
                protocol_family="modbus",
                model_name="SMG",
                serial_number="serial",
                probe_target=ProbeTarget(
                    devcode=2376,
                    collector_addr=1,
                    device_addr=1,
                ),
            )
            snapshot = LocalRegisterSnapshot(
                collector_pn="E50000200000000001",
                driver_key="smg",
                started_at="2026-08-22T10:00:00+00:00",
                completed_at="2026-08-22T10:00:02+00:00",
                planned_block_count=1,
                failed_block_count=0,
                blocks=(
                    LocalRegisterBlockObservation(
                        plan=LocalRegisterReadPlan(
                            devcode=2376,
                            collector_addr=1,
                            device_addr=1,
                            function=3,
                            start=300,
                            count=1,
                        ),
                        observed_at="2026-08-22T10:00:01+00:00",
                        values=(2305,),
                    ),
                ),
            )
            driver = SimpleNamespace(
                async_capture_local_register_snapshot=AsyncMock(
                    return_value=snapshot
                )
            )
            hub._driver = driver

            captured = await hub.async_capture_local_register_snapshot()
            return captured, driver

        captured, driver = asyncio.run(_run())

        self.assertEqual(captured.collector_pn, "E50000200000000001")
        driver.async_capture_local_register_snapshot.assert_awaited_once()
        self.assertEqual(
            driver.async_capture_local_register_snapshot.await_args.kwargs,
            {"collector_pn": "E50000200000000001"},
        )

    def test_local_register_snapshot_rejects_duck_driver_result(self) -> None:
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
            hub._link_manager.collector_info.collector_pn = "E50000200000000001"
            hub._inverter = DetectedInverter(
                driver_key="smg",
                protocol_family="modbus",
                model_name="SMG",
                serial_number="serial",
                probe_target=ProbeTarget(2376, 1, 1),
            )
            hub._driver = SimpleNamespace(
                async_capture_local_register_snapshot=AsyncMock(
                    return_value={"authority": "live_local_wire_observation"}
                )
            )

            with self.assertRaisesRegex(
                TypeError,
                "driver_local_register_snapshot_invalid",
            ):
                await hub.async_capture_local_register_snapshot()

        asyncio.run(_run())

    def test_generic_support_evidence_keeps_compiled_identity_point_reads(self) -> None:
        async def _run() -> tuple[dict[str, object], list[tuple[int, int]]]:
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
                driver_hint="modbus_smg",
            )
            hub._link_manager = _FakeLinkManager()
            reads: list[tuple[int, int]] = []

            class _RecordingSession:
                def __init__(self, *_args, **_kwargs) -> None:
                    pass

                async def read_holding(self, start: int, count: int) -> list[int]:
                    reads.append((start, count))
                    return [0] * count

            with (
                patch(
                    "custom_components.eybond_local.runtime.hub.support.ModbusSession",
                    _RecordingSession,
                ),
                patch.object(
                    hub,
                    "_async_capture_at_text_ascii_probe",
                    AsyncMock(return_value=None),
                ),
            ):
                evidence = await hub._async_capture_generic_support_evidence(
                    "modbus_smg:no_match"
                )
            return evidence, reads

        evidence, reads = asyncio.run(_run())

        captures = evidence["captures"]
        self.assertEqual(len(captures), 1)
        planned = [
            (item["start"], item["count"])
            for item in captures[0]["planned_ranges"]
        ]
        self.assertEqual(planned[:2], [(171, 1), (184, 1)])
        self.assertNotIn((171, 14), planned)
        self.assertIn((643, 2), planned)
        self.assertIn((643, 1), planned)
        self.assertEqual(reads, planned)

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

    def test_async_refresh_keeps_collector_connected_on_inverter_request_timeout(self) -> None:
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

            self.assertTrue(snapshot.connected)
            self.assertEqual(snapshot.last_error, "request_timeout")
            self.assertEqual(snapshot.values["output_power"], 50)
            self.assertEqual(snapshot.values["battery_power"], -71)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 0)
            self.assertEqual(snapshot.values["runtime_backoff_seconds"], 0)
            self.assertEqual(snapshot.values["runtime_payload_error"], "request_timeout")
            self.assertEqual(hub._link_manager.reset_calls, 0)
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

    def test_async_refresh_uses_framed_owner_and_at_only_for_supplemental_fields(self) -> None:
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
                    "INTPARA41": "MyWiFi",
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

            self.assertEqual(snapshot.values["collector_server_endpoint"], "fc.example,18899,TCP")
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
                    "INTPARA41": "MyWiFi",
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
            self.assertEqual(snapshot.values["collector_ssid"], "MyWiFi")
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
                expected_session_protocol="at_text",
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
                        "expected_session_protocol": "at_text",
                        "proxy_wire_mode": "transparent",
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


class WriteReadbackConfirmationTests(unittest.TestCase):
    def setUp(self) -> None:
        profile = load_driver_profile(
            "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"
        )
        self.time_capability = profile.get_capability("inverter_time_write")
        self.scalar_capability = profile.get_capability("max_ac_charge_current")

    def test_running_clock_confirms_within_measured_operation_time(self) -> None:
        self.assertTrue(
            _write_readback_matches(
                self.time_capability,
                requested_value="19:04:04",
                written_value="19:04:04",
                readback_value="19:04:08",
                confirmation_elapsed_seconds=3.0,
            )
        )

    def test_running_clock_rejects_values_outside_measured_operation_time(self) -> None:
        self.assertFalse(
            _write_readback_matches(
                self.time_capability,
                requested_value="19:04:04",
                written_value="19:04:04",
                readback_value="19:04:09",
                confirmation_elapsed_seconds=3.0,
            )
        )
        self.assertFalse(
            _write_readback_matches(
                self.time_capability,
                requested_value="19:04:04",
                written_value="19:04:04",
                readback_value="19:04:03",
                confirmation_elapsed_seconds=3.0,
            )
        )

    def test_running_clock_confirmation_handles_midnight_rollover(self) -> None:
        self.assertTrue(
            _write_readback_matches(
                self.time_capability,
                requested_value="23:59:59",
                written_value="23:59:59",
                readback_value="00:00:03",
                confirmation_elapsed_seconds=3.0,
            )
        )

    def test_running_clock_confirmation_fails_closed_on_malformed_inputs(self) -> None:
        for malformed in ("19:04", " 19:04:04", "24:00:00", object(), None):
            with self.subTest(malformed=malformed):
                self.assertFalse(
                    _write_readback_matches(
                        self.time_capability,
                        requested_value="19:04:04",
                        written_value="19:04:04",
                        readback_value=malformed,
                        confirmation_elapsed_seconds=3.0,
                    )
                )
        for malformed_elapsed in (True, "3", float("nan"), float("inf")):
            with self.subTest(malformed_elapsed=malformed_elapsed):
                self.assertFalse(
                    _write_readback_matches(
                        self.time_capability,
                        requested_value="19:04:04",
                        written_value="19:04:04",
                        readback_value="19:04:08",
                        confirmation_elapsed_seconds=malformed_elapsed,
                    )
                )

    def test_elapsed_confirmation_does_not_relax_scalar_writes(self) -> None:
        self.assertFalse(
            _write_readback_matches(
                self.scalar_capability,
                requested_value=30,
                written_value=30,
                readback_value=31,
                confirmation_elapsed_seconds=300.0,
            )
        )

    def test_hub_accepts_clock_that_advanced_during_real_readback_path(self) -> None:
        async def _run() -> None:
            profile = load_driver_profile(
                "modbus_smg/models/anenji_anj_11kw_48v_wifi_p.json"
            )
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
            hub._driver = _AdvancingClockWriteDriver(readback="19:04:08")
            hub._inverter = DetectedInverter(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="Anenji ANJ-11KW-48V-WIFI-P",
                serial_number="92632500000001",
                probe_target=ProbeTarget(
                    devcode=0x0001,
                    collector_addr=0x02,
                    device_addr=0x01,
                ),
                capabilities=profile.capabilities,
                capability_groups=profile.groups,
                capability_presets=profile.presets,
            )

            with patch(
                "custom_components.eybond_local.runtime.hub.management.monotonic",
                side_effect=(100.0, 104.0),
            ):
                written = await hub.async_write_capability(
                    "inverter_time_write",
                    "19:04:04",
                )

            self.assertEqual(written, "19:04:04")
            self.assertEqual(hub._driver.write_calls, 1)
            self.assertEqual(hub._driver.read_calls, 2)

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

    def test_async_write_capability_confirms_delayed_readback_without_resending(self) -> None:
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
            hub._driver = _WriteDelayedConfirmationDriver()
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
            self.assertEqual(hub._driver.read_calls, 3)

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
            self.assertEqual(hub._driver.read_calls, 3)

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

    def test_async_refresh_repeated_payload_timeout_does_not_enter_backoff(self) -> None:
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

            self.assertTrue(first.connected)
            self.assertTrue(second.connected)
            self.assertEqual(second.last_error, "request_timeout")
            self.assertEqual(hub._driver.calls, 4)
            self.assertEqual(hub._link_manager.reset_calls, 0)
            self.assertEqual(second.values["runtime_recovery_streak"], 0)
            self.assertEqual(second.values["runtime_backoff_seconds"], 0)

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

    def test_at_text_metadata_bootstrap_reads_bridge_hardware_version(self) -> None:
        async def _run() -> None:
            hub = EybondHub(
                connection=EybondConnectionSpec(
                    server_ip="192.168.1.10",
                    collector_ip="192.168.1.14",
                    tcp_port=18899,
                    udp_port=58899,
                    discovery_target="192.168.1.255",
                    discovery_interval=30,
                    heartbeat_interval=60,
                    request_timeout=5.0,
                ),
            )

            class _AtTextFcBootstrapTransport:
                connected = True

                def __init__(self) -> None:
                    self.fc_requests: list[tuple[int, bytes]] = []

                async def async_query_bridge_hardware_version(self):
                    self.fc_requests.append((2, b"\x06"))
                    return (None, b"\x00\x06esp-collector/0.1.8/ESP8266")

                async def async_query(self, command: str):
                    raise asyncio.TimeoutError()

            at_transport = _AtTextFcBootstrapTransport()
            link = _FakeLinkManager()
            link.transport = object()
            link.collector_at_transport = at_transport
            hub._link_manager = link

            values = await hub._async_read_collector_runtime_values(poll_interval=10.0)

            self.assertEqual(at_transport.fc_requests, [(2, b"\x06")])
            self.assertEqual(
                values["collector_hardware_version"],
                "esp-collector/0.1.8/ESP8266",
            )
            snapshot = hub._build_snapshot(extra_values=values)
            self.assertTrue(snapshot.collector.collector_virtual_bridge)
            self.assertEqual(snapshot.collector.collector_bridge_version, "0.1.8")

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

            # Dead-channel truth (Phase-4 semantics): a channel becomes dead when
            # it DELIVERS commands over a live link yet answers with no metadata
            # (blank values), NOT when it times out (that is a transport error).
            class _EmptyAnsweringAtTransport:
                connected = True

                def __init__(self) -> None:
                    self.sweeps = 0
                    self._pending = False

                async def async_query(self, command: str):
                    if not self._pending:
                        self.sweeps += 1
                        self._pending = True
                    if command == "INTPARA49":  # last non-overlapping command
                        self._pending = False
                    return CollectorAtResponse(command=command, value="", raw=f"AT+{command}:")

            at_transport = _EmptyAnsweringAtTransport()
            link = _FakeLinkManager()
            link.collector_at_transport = at_transport
            link.transport = _CollectorQueryTransport(
                {(2, b"\x06"): b"\x00\x06esp-collector/0.1.5/ESP32"}
            )
            hub._link_manager = link

            for _ in range(4):
                # Force each attempt through the cadence gate.
                hub._collector_at_runtime_last_attempt_monotonic = -1000.0
                hub._collector_runtime_last_refresh_monotonic = -1000.0
                await hub._async_read_collector_runtime_values(poll_interval=10.0)

            learned_sweeps = at_transport.sweeps
            self.assertGreaterEqual(learned_sweeps, 4)

            # Dead: the channel verdict blocks the sweep entirely, even with the
            # cadence forced open.
            hub._collector_at_runtime_last_attempt_monotonic = -1000.0
            hub._collector_runtime_last_refresh_monotonic = -1000.0
            await hub._async_read_collector_runtime_values(poll_interval=10.0)
            self.assertEqual(at_transport.sweeps, learned_sweeps)

            # The re-check path clears the verdict and probes again.
            hub.clear_unsupported_command_cache()
            hub._collector_at_runtime_last_attempt_monotonic = -1000.0
            hub._collector_runtime_last_refresh_monotonic = -1000.0
            await hub._async_read_collector_runtime_values(poll_interval=10.0)
            self.assertEqual(at_transport.sweeps, learned_sweeps + 1)

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

    def _metadata_hub(self) -> "EybondHub":
        return EybondHub(
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

    def test_persistent_unsupported_commands_filter_out_metadata_channels(self) -> None:
        from custom_components.eybond_local.drivers.command_support import (
            command_skipped_as_unsupported,
        )

        hub = self._metadata_hub()
        hub.set_persistent_unsupported_commands(("QPIWS", "collector:at_metadata"))
        # The driver negative cache gets the real command but NOT the metadata key.
        self.assertTrue(command_skipped_as_unsupported(hub._runtime_read_state, "QPIWS"))
        self.assertFalse(
            command_skipped_as_unsupported(hub._runtime_read_state, "collector:at_metadata")
        )

    def test_metadata_dead_channels_seed_and_revive(self) -> None:
        hub = self._metadata_hub()
        hub.set_persistent_metadata_dead_channels(("collector:at_metadata",))
        self.assertEqual(
            hub.collector_metadata_dead_channels(), ("collector:at_metadata",)
        )
        self.assertTrue(hub._collector_metadata_service.at_channel_disabled())
        # The re-check action revives the metadata channel (separate store).
        hub.clear_unsupported_command_cache()
        self.assertEqual(hub.collector_metadata_dead_channels(), ())
        self.assertFalse(hub._collector_metadata_service.at_channel_disabled())

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
            self.assertEqual(snapshot.runtime_value("output_power"), 420)
            self.assertNotIn("output_power", snapshot.values)
            self.assertEqual(snapshot.values["runtime_reconnect_count"], 1)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 0)

        asyncio.run(_run())

    def test_async_refresh_retries_request_timeout_without_reconnect(self) -> None:
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
            self.assertEqual(snapshot.runtime_value("output_power"), 420)
            self.assertEqual(snapshot.runtime_value("battery_power"), -180)
            self.assertNotIn("output_power", snapshot.values)
            self.assertNotIn("battery_power", snapshot.values)
            self.assertEqual(snapshot.values["runtime_recovery_streak"], 0)
            self.assertEqual(snapshot.values["runtime_reconnect_count"], 0)
            self.assertEqual(hub._link_manager.reset_calls, 0)

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
                collector_configured_session_protocol=session_protocol,
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


class _SuccessDriver:
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
        return {"output_power": 100, "battery_average_power": -50}


class RuntimeStateMachineTests(unittest.TestCase):
    """Runtime state-machine hardening: explicit states + sticky inverter identity."""

    _MODEL = "SMG 6200"
    # Pure-digit synthetic serials (no leading letter -> not PN-shaped identifiers).
    _SERIAL = "92632500000001"
    _OTHER_SERIAL = "92632599999999"

    def _hub(self, *, full_scan: bool = False) -> EybondHub:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                collector_pn="V001020SYN62344022",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
            driver_detection_strategy=(
                DRIVER_DETECTION_FULL_SCAN if full_scan else "first_match"
            ),
        )
        hub._link_manager = _FakeLinkManager()
        hub.set_callback_ownership(None, "entry-runtime-state-machine")
        return hub

    def _inverter(
        self,
        *,
        serial: str | None = None,
        model: str | None = None,
        driver_key: str = "modbus_smg",
        detection_status: str = "",
    ) -> DetectedInverter:
        details: dict[str, object] = {}
        if detection_status:
            details["runtime_detection_status"] = detection_status
        return DetectedInverter(
            driver_key=driver_key,
            protocol_family=driver_key,
            model_name=model or self._MODEL,
            serial_number=serial or self._SERIAL,
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0x02, device_addr=0x01),
            details=details,
        )

    def _fake_detection(self, inverter: DetectedInverter, driver: object):
        async def _detect(_transport, *, driver_hint=""):
            return SimpleNamespace(
                driver=driver,
                inverter=inverter,
                match=SimpleNamespace(confidence="high"),
            )

        return _detect

    def _candidate_context(
        self,
        *,
        driver_key: str,
        protocol_family: str,
        model: str,
    ) -> DetectedDriverContext:
        inverter = self._inverter(
            driver_key=driver_key,
            model=model,
            serial=self._SERIAL,
        )
        inverter.protocol_family = protocol_family
        return DetectedDriverContext(
            driver=SimpleNamespace(key=driver_key),
            inverter=inverter,
            match=DriverMatch(
                driver_key=driver_key,
                protocol_family=protocol_family,
                model_name=model,
                serial_number=self._SERIAL,
                probe_target=inverter.probe_target,
            ),
        )

    def test_default_detection_stops_at_first_confirmed_match(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            context = self._candidate_context(
                driver_key="smartess_local",
                protocol_family="0925",
                model="Hybrid 5K",
            )
            with (
                patch(
                    "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter",
                    return_value=context,
                ) as first_match,
                patch(
                    "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter_candidates",
                    side_effect=AssertionError("full scan must not run"),
                ),
            ):
                result = await hub._async_detect_driver()

            self.assertEqual(result, "")
            first_match.assert_awaited_once()
            self.assertIs(hub._driver, context.driver)
            self.assertEqual(hub.inverter_protocol_candidates, ())

        asyncio.run(_run())

    def test_auto_detection_keeps_multi_protocol_result_unbound(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            scan_calls = 0
            scan = DriverCandidateScan(
                candidates=(
                    self._candidate_context(
                        driver_key="smartess_local",
                        protocol_family="0925",
                        model="Hybrid 5K",
                    ),
                    self._candidate_context(
                        driver_key="pi30",
                        protocol_family="pi30",
                        model="Hybrid 5K",
                    ),
                ),
                probe_log=(
                    {
                        "driver": "smartess_local",
                        "elapsed_ms": 800,
                        "outcome": "matched",
                        "saw_response": True,
                    },
                    {
                        "driver": "pi30",
                        "elapsed_ms": 900,
                        "outcome": "matched",
                        "saw_response": True,
                    },
                ),
            )

            async def _scan(*_args, **_kwargs):
                nonlocal scan_calls
                scan_calls += 1
                return scan

            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter_candidates",
                side_effect=_scan,
            ):
                first = await hub._async_detect_driver()
                second = await hub._async_detect_driver()

            self.assertEqual(first, "inverter_protocol_ambiguous")
            self.assertEqual(second, "inverter_protocol_ambiguous")
            self.assertEqual(scan_calls, 1)
            self.assertIsNone(hub._driver)
            self.assertIsNone(hub._inverter)
            self.assertEqual(
                [item.driver_key for item in hub.inverter_protocol_candidates],
                ["smartess_local", "pi30"],
            )
            snapshot = hub._build_snapshot(last_error=first)
            self.assertEqual(snapshot.values["runtime_inverter_state"], "ambiguous")
            self.assertEqual(snapshot.values["runtime_inverter_candidate_count"], 2)
            self.assertEqual(snapshot.values["runtime_inverter_probe_total_ms"], 1700)

        asyncio.run(_run())

    def test_auto_detection_resolves_declared_exact_catalog_overlap(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            smg = self._candidate_context(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model="False-positive SMG surface",
            )
            target = smg.inverter.probe_target
            inverter = DetectedInverter(
                driver_key="modbus_catalog",
                protocol_family="modbus_catalog",
                model_name="Deye-Compatible Three-Phase Hybrid 8 kW (Modbus)",
                serial_number="",
                probe_target=target,
                variant_key="deye_3ph_high_80kw",
                profile_name="modbus_catalog/deye_3ph_high_80kw.json",
                register_schema_name="deye_3ph_high_80kw/base.json",
                details={
                    "catalog_detection": {
                        "resolution": "exact",
                        "surface_key": "deye_3ph_high_80kw_untested",
                        "confidence": "high",
                    }
                },
            )
            catalog = DetectedDriverContext(
                driver=SimpleNamespace(key="modbus_catalog"),
                inverter=inverter,
                match=DriverMatch(
                    driver_key="modbus_catalog",
                    protocol_family="modbus_catalog",
                    model_name=inverter.model_name,
                    serial_number="",
                    probe_target=target,
                    variant_key=inverter.variant_key,
                ),
            )

            with patch(
                "custom_components.eybond_local.runtime.hub.detection."
                "async_detect_inverter_candidates",
                return_value=DriverCandidateScan(candidates=(smg, catalog)),
            ):
                result = await hub._async_detect_driver()

            self.assertEqual(result, "")
            self.assertIs(hub._driver, catalog.driver)
            self.assertIs(hub._inverter, catalog.inverter)
            self.assertEqual(hub.inverter_protocol_candidates, ())
            self.assertEqual(
                hub._inverter.details["driver_candidate_selection"],
                {
                    "kind": "catalog_protocol_precedence",
                    "catalog_entry_key": "deye_3ph_high_80kw",
                    "superseded_protocols": ["modbus_smg"],
                },
            )

        asyncio.run(_run())

    def test_auto_detection_binds_the_only_runtime_candidate(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            context = self._candidate_context(
                driver_key="pi30",
                protocol_family="pi30",
                model="Hybrid 5K",
            )
            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter_candidates",
                return_value=DriverCandidateScan(candidates=(context,)),
            ):
                result = await hub._async_detect_driver()

            self.assertEqual(result, "")
            self.assertIs(hub._driver, context.driver)
            self.assertIs(hub._inverter, context.inverter)
            self.assertEqual(hub.inverter_protocol_candidates, ())

        asyncio.run(_run())

    def test_runtime_probe_log_is_sanitized_and_published_for_support(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            context = self._candidate_context(
                driver_key="pi30",
                protocol_family="pi30",
                model="Hybrid 5K",
            )
            # A raw details log must never bypass the runtime diagnostics
            # projection, even if a driver supplied it.
            context.inverter.details["probe_log"] = [
                {"outcome": "error:route 192.0.2.10"}
            ]
            scan = DriverCandidateScan(
                candidates=(context,),
                budget_exhausted=True,
                probe_log=(
                    {
                        "driver": "modbus_smg",
                        "elapsed_ms": 45000,
                        "outcome": "error:route 192.0.2.10 refused",
                        "saw_response": False,
                    },
                    {
                        "driver": "pi30",
                        "elapsed_ms": 1234,
                        "outcome": "matched",
                        "saw_response": True,
                        "routes": [
                            {
                                "family": "eybond",
                                "devcode": 0x0994,
                                "collector_addr": 1,
                                "attempts": 4,
                                "responses": 4,
                                "endpoint": "192.0.2.10",
                            },
                            {
                                "family": "eybond",
                                "devcode": True,
                                "collector_addr": 1,
                                "attempts": 1,
                                "responses": 1,
                            },
                        ],
                    },
                ),
            )

            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter_candidates",
                return_value=scan,
            ):
                self.assertEqual(await hub._async_detect_driver(), "")

            snapshot = hub._build_snapshot()
            self.assertNotIn("probe_log", context.inverter.details)
            self.assertNotIn("probe_log", snapshot.values)
            self.assertEqual(
                snapshot.values["runtime_inverter_probe_log"],
                [
                    {
                        "driver": "modbus_smg",
                        "elapsed_ms": 45000,
                        "outcome": "error",
                        "saw_response": False,
                    },
                    {
                        "driver": "pi30",
                        "elapsed_ms": 1234,
                        "outcome": "matched",
                        "saw_response": True,
                        "routes": [
                            {
                                "family": "eybond",
                                "devcode": 0x0994,
                                "collector_addr": 1,
                                "attempts": 4,
                                "responses": 4,
                            }
                        ],
                    },
                ],
            )
            self.assertEqual(
                snapshot.values["runtime_inverter_probe_total_ms"], 46234
            )
            self.assertTrue(
                snapshot.values["runtime_inverter_probe_budget_exhausted"]
            )
            self.assertTrue(
                snapshot.values["runtime_inverter_probe_current_session"]
            )
            self.assertNotIn("192.0.2.10", str(snapshot.values))

            hub._link_manager.owned_session_generation = 8
            self.assertFalse(
                hub._build_snapshot().values[
                    "runtime_inverter_probe_current_session"
                ]
            )

        asyncio.run(_run())

    def test_failed_runtime_sweep_keeps_probe_log_in_snapshot(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            failure = DriverSweepNoMatch(
                "pi30:probe_timeout",
                silent=False,
                probe_log=(
                    {
                        "driver": "modbus_smg",
                        "elapsed_ms": 45000,
                        "outcome": "no_match",
                        "saw_response": True,
                        "diagnostic": {
                            "kind": "catalog_identity",
                            "status": "partial_identity",
                            "protocol": "modbus_smg",
                            "model_code": 0x4321,
                            "executed_actions": ["modbus_smg.identity.171"],
                            "failed_actions": ["modbus_smg.identity.184"],
                            "action_failures": [
                                {
                                    "action": "modbus_smg.identity.184",
                                    "reason": "modbus_exception",
                                    "exception_code": 2,
                                }
                            ],
                            "endpoint": "must-not-survive",
                        },
                    },
                    {
                        "driver": "pi30",
                        "elapsed_ms": 45000,
                        "outcome": "probe_timeout",
                        "saw_response": False,
                    },
                ),
            )
            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter_candidates",
                side_effect=failure,
            ):
                result = await hub._async_detect_driver()

            self.assertEqual(result, "pi30:probe_timeout")
            snapshot = hub._build_snapshot(last_error=result)
            self.assertEqual(
                snapshot.values["runtime_inverter_probe_total_ms"], 90000
            )
            self.assertEqual(
                [
                    entry["outcome"]
                    for entry in snapshot.values["runtime_inverter_probe_log"]
                ],
                ["no_match", "probe_timeout"],
            )
            diagnostic = snapshot.values["runtime_inverter_probe_log"][0][
                "diagnostic"
            ]
            self.assertEqual(diagnostic["model_code"], 0x4321)
            self.assertNotIn("endpoint", diagnostic)

            hub._link_manager.owned_session_generation = 1
            stale_snapshot = hub._build_snapshot()
            self.assertFalse(
                stale_snapshot.values["runtime_inverter_probe_current_session"]
            )
            self.assertEqual(
                stale_snapshot.values["runtime_inverter_probe_log"][0][
                    "diagnostic"
                ]["model_code"],
                0x4321,
            )

        asyncio.run(_run())

    def test_full_scan_silence_adopts_runtime_uart_sweep_match(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            context = self._candidate_context(
                driver_key="pi30",
                protocol_family="pi30",
                model="Hybrid 5K",
            )
            failure = DriverSweepNoMatch(
                "pi30:probe_timeout",
                silent=True,
                probe_log=(
                    {
                        "driver": "pi30",
                        "elapsed_ms": 45000,
                        "outcome": "probe_timeout",
                        "saw_response": False,
                    },
                ),
            )
            recovered = DriverCandidateScan(candidates=(context,))

            with (
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "async_detect_inverter_candidates",
                    side_effect=failure,
                ),
                patch.object(
                    hub,
                    "_async_attempt_runtime_link_baud_sweep",
                    new=AsyncMock(return_value=recovered),
                ) as baud_sweep,
            ):
                result = await hub._async_detect_driver()

            self.assertEqual(result, "")
            baud_sweep.assert_awaited_once_with(detection_generation=7)
            self.assertIs(hub._driver, context.driver)
            self.assertIs(hub._inverter, context.inverter)

        asyncio.run(_run())

    def test_first_match_silence_never_changes_runtime_uart(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=False)
            failure = DriverSweepNoMatch(
                "pi30:probe_timeout",
                silent=True,
            )
            with (
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "async_detect_inverter",
                    side_effect=failure,
                ),
                patch.object(
                    hub,
                    "_async_attempt_runtime_link_baud_sweep",
                    new=AsyncMock(
                        side_effect=AssertionError(
                            "first-match detection must not change UART"
                        )
                    ),
                ),
            ):
                result = await hub._async_detect_driver()

            self.assertEqual(result, "pi30:probe_timeout")

        asyncio.run(_run())

    def test_runtime_uart_sweep_is_once_per_owned_esp_session(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            hub._link_manager.transport = SimpleNamespace(
                async_send_collector=AsyncMock()
            )
            hub._collector_metadata_service.framed_values = {
                "collector_hardware_version": "esp-collector/0.1.5/ESP32"
            }
            context = self._candidate_context(
                driver_key="pi30",
                protocol_family="pi30",
                model="Hybrid 5K",
            )
            recovered = DriverCandidateScan(candidates=(context,))
            channel = AsyncMock()
            channel.async_read_current_baud.return_value = 2400
            channel.async_set_baud.return_value = True

            with (
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "RuntimeLinkBaudChannel",
                    return_value=channel,
                ),
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "catalog_link_baud_hints",
                    return_value=(2400, 9600),
                ),
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "driver_keys_for_link_baud",
                    return_value=("pi30",),
                ),
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "async_detect_inverter_candidates",
                    return_value=recovered,
                ) as detect,
            ):
                first = await hub._async_attempt_runtime_link_baud_sweep(
                    detection_generation=7
                )
                second = await hub._async_attempt_runtime_link_baud_sweep(
                    detection_generation=7
                )

            self.assertIs(first, recovered)
            self.assertIsNone(second)
            channel.async_set_baud.assert_awaited_once_with(9600)
            detect.assert_awaited_once()

        asyncio.run(_run())

    def test_runtime_uart_sweep_skips_busy_authority_without_consuming_session(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            hub._link_manager.transport = SimpleNamespace(
                async_send_collector=AsyncMock()
            )
            hub._collector_metadata_service.framed_values = {
                "collector_hardware_version": "esp-collector/0.1.5/ESP32"
            }
            held = COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.acquire(
                "entry-runtime-state-machine",
                OPERATION_COLLECTOR_SYSTEM_ACTION,
            )
            self.assertTrue(held.acquired)
            try:
                with patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "RuntimeLinkBaudChannel",
                ) as channel_type:
                    result = await hub._async_attempt_runtime_link_baud_sweep(
                        detection_generation=7
                    )
                self.assertIsNone(result)
                self.assertEqual(hub._link_baud_sweep_generation, -1)
                channel_type.assert_not_called()
            finally:
                self.assertTrue(
                    COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.release(
                        "entry-runtime-state-machine",
                        held.token,
                    )
                )

        asyncio.run(_run())

    def test_runtime_uart_sweep_holds_and_releases_shared_authority(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            hub._link_manager.transport = SimpleNamespace(
                async_send_collector=AsyncMock()
            )
            hub._collector_metadata_service.framed_values = {
                "collector_hardware_version": "esp-collector/0.1.5/ESP32"
            }
            entered = asyncio.Event()
            finish = asyncio.Event()

            async def blocked_sweep(**_kwargs):
                entered.set()
                await finish.wait()
                return SimpleNamespace(matched=False)

            with (
                patch.object(
                    hub,
                    "_collector_management_adapter",
                    return_value=object(),
                ),
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "async_run_link_baud_sweep",
                    side_effect=blocked_sweep,
                ),
            ):
                task = asyncio.create_task(
                    hub._async_attempt_runtime_link_baud_sweep(
                        detection_generation=7
                    )
                )
                await entered.wait()
                self.assertEqual(
                    COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.active_operation(
                        "entry-runtime-state-machine"
                    ),
                    OPERATION_RUNTIME_LINK_BAUD_SWEEP,
                )
                competing = COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.acquire(
                    "entry-runtime-state-machine",
                    OPERATION_COLLECTOR_SYSTEM_ACTION,
                )
                self.assertFalse(competing.acquired)
                self.assertEqual(
                    competing.busy_operation,
                    OPERATION_RUNTIME_LINK_BAUD_SWEEP,
                )
                finish.set()
                self.assertIsNone(await task)

            self.assertEqual(
                COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.active_operation(
                    "entry-runtime-state-machine"
                ),
                "",
            )

        asyncio.run(_run())

    def test_runtime_uart_sweep_releases_authority_when_cancelled(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)
            hub._link_manager.owned_session_generation = 7
            hub._link_manager.transport = SimpleNamespace(
                async_send_collector=AsyncMock()
            )
            hub._collector_metadata_service.framed_values = {
                "collector_hardware_version": "esp-collector/0.1.5/ESP32"
            }
            with (
                patch.object(
                    hub,
                    "_collector_management_adapter",
                    return_value=object(),
                ),
                patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "async_run_link_baud_sweep",
                    new=AsyncMock(side_effect=asyncio.CancelledError),
                ),
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await hub._async_attempt_runtime_link_baud_sweep(
                        detection_generation=7
                    )

            self.assertEqual(
                COLLECTOR_ENDPOINT_OPERATION_AUTHORITY.active_operation(
                    "entry-runtime-state-machine"
                ),
                "",
            )

        asyncio.run(_run())

    def test_runtime_uart_sweep_never_writes_factory_collector(self) -> None:
        asyncio.run(
            self._assert_runtime_uart_hardware_is_read_only(
                "Eybond Wi-Fi DTU V2.4"
            )
        )

    def test_runtime_uart_sweep_never_writes_bk72xx_bridge(self) -> None:
        asyncio.run(
            self._assert_runtime_uart_hardware_is_read_only(
                "esp-collector/0.1.5/BK72xx/RTL87xx"
            )
        )

    async def _assert_runtime_uart_hardware_is_read_only(
        self,
        hardware_text: str,
    ) -> None:
        hub = self._hub(full_scan=True)
        hub._link_manager.owned_session_generation = 7
        hub._link_manager.transport = SimpleNamespace(
            async_send_collector=AsyncMock()
        )
        hub._collector_metadata_service.framed_values = {
            "collector_hardware_version": hardware_text
        }

        with (
            patch(
                "custom_components.eybond_local.runtime.hub.detection."
                "RuntimeLinkBaudChannel",
            ) as channel_type,
            patch(
                "custom_components.eybond_local.runtime.hub.detection."
                "async_detect_inverter_candidates",
                new=AsyncMock(
                    side_effect=AssertionError(
                        "unsupported collector must not enter UART re-sweep"
                    )
                ),
            ),
        ):
            result = await hub._async_attempt_runtime_link_baud_sweep(
                detection_generation=7
            )

        self.assertIsNone(result)
        channel_type.assert_not_called()

    # 1. detected inverter + first poll timeout keeps inverter identity.
    def test_first_poll_timeout_after_detection_keeps_inverter_identity(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            hub._driver = _TimeoutDriver()
            hub._inverter = self._inverter()
            # A first bound+connected snapshot records the driver-bound identity.
            hub._last_snapshot = hub._build_snapshot()
            self.assertEqual(
                hub._last_snapshot.values["runtime_driver_state"], "driver_bound"
            )

            snapshot = await hub.async_refresh(poll_interval=3.0)

            # Identity survives the first-poll timeout; the entry is not collapsed.
            self.assertIsNotNone(snapshot.inverter)
            self.assertEqual(snapshot.inverter.serial_number, self._SERIAL)
            self.assertEqual(snapshot.inverter.model_name, self._MODEL)
            self.assertTrue(snapshot.connected)
            self.assertNotEqual(snapshot.values["runtime_poll_state"], "offline")
            self.assertEqual(
                snapshot.values["runtime_last_driver_bound_identity"],
                "modbus_smg|SMG 6200|92632500000001",
            )

        asyncio.run(_run())

    # 2. collector-only build after driver-bound does not erase the inverter.
    def test_collector_only_build_keeps_confirmed_inverter(self) -> None:
        hub = self._hub()
        hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())
        hub._last_snapshot = hub._build_snapshot()

        # A subsequent snapshot (e.g. a collector-metadata-only refresh) keeps the
        # confirmed inverter; the inverter track is independent of the collector.
        snapshot = hub._build_snapshot(extra_values={"collector_signal_strength": -55})

        self.assertIsNotNone(snapshot.inverter)
        self.assertEqual(snapshot.inverter.serial_number, self._SERIAL)
        self.assertEqual(snapshot.values["runtime_inverter_state"], "live_confirmed")
        self.assertEqual(snapshot.values["runtime_driver_state"], "driver_bound")

    # 3. startup persisted identity is provisional; live detection promotes it.
    def test_startup_persisted_identity_is_provisional_then_promoted(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            hub.set_initial_inverter_binding(
                _SuccessDriver(),
                self._inverter(detection_status="startup_persisted_identity"),
            )
            provisional = hub._build_snapshot()
            self.assertEqual(
                provisional.values["runtime_inverter_state"], "provisional"
            )
            self.assertTrue(hub._inverter_binding_needs_live_detection_refresh)

            # Live detection confirms the SAME identity -> promote to live-confirmed.
            live_inverter = self._inverter()
            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter",
                new=self._fake_detection(live_inverter, _SuccessDriver()),
            ):
                snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertFalse(hub._inverter_binding_needs_live_detection_refresh)
            self.assertEqual(snapshot.values["runtime_inverter_state"], "live_confirmed")
            self.assertNotIn("runtime_identity_conflict", snapshot.values)

        asyncio.run(_run())

    def test_catalog_alias_rename_promotes_live_identity_without_conflict(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            probe_target = ProbeTarget(
                devcode=0x0001,
                collector_addr=0x02,
                device_addr=0x01,
            )
            durable = DetectedInverter(
                driver_key="modbus_catalog",
                protocol_family="modbus_catalog",
                model_name="Deye-Compatible Three-Phase Hybrid 80 kW (Modbus)",
                serial_number="",
                probe_target=probe_target,
                details={"runtime_detection_status": "startup_persisted_identity"},
            )
            live = DetectedInverter(
                driver_key="modbus_catalog",
                protocol_family="modbus_catalog",
                model_name="Deye-Compatible Three-Phase Hybrid 8 kW (Modbus)",
                serial_number="",
                probe_target=probe_target,
                details={},
            )
            hub.set_initial_inverter_binding(_SuccessDriver(), durable)

            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter",
                new=self._fake_detection(live, _SuccessDriver()),
            ):
                snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertEqual(snapshot.inverter.model_name, live.model_name)
            self.assertEqual(snapshot.values["runtime_inverter_state"], "live_confirmed")
            self.assertNotIn("runtime_identity_conflict", snapshot.values)
            self.assertFalse(hub._inverter_binding_needs_live_detection_refresh)

        asyncio.run(_run())

    def test_catalog_restored_identity_keeps_binding_and_probe_error(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            hub.set_initial_inverter_binding(
                _SuccessDriver(),
                self._inverter(
                    detection_status="persisted_model_probe_degraded"
                ),
            )

            async def _probe_timeout(_transport, *, driver_hint=""):
                raise RuntimeError("modbus_catalog:probe_timeout")

            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter",
                new=_probe_timeout,
            ):
                snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertIsNotNone(snapshot.inverter)
            self.assertEqual(snapshot.values["runtime_driver_state"], "driver_bound")
            self.assertEqual(snapshot.values["runtime_inverter_state"], "provisional")
            self.assertEqual(snapshot.last_error, "modbus_catalog:probe_timeout")

        asyncio.run(_run())

    # 4. startup persisted identity + different live full identity reports conflict.
    def test_startup_persisted_identity_conflict_keeps_durable(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            hub.set_initial_inverter_binding(
                _SuccessDriver(),
                self._inverter(detection_status="startup_persisted_identity"),
            )

            # Live detection reports a DIFFERENT serial (different physical inverter).
            other = self._inverter(serial=self._OTHER_SERIAL)
            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter",
                new=self._fake_detection(other, _SuccessDriver()),
            ):
                snapshot = await hub.async_refresh(poll_interval=3.0)

            # Durable identity kept, not silently swapped; conflict published.
            self.assertEqual(snapshot.inverter.serial_number, self._SERIAL)
            self.assertEqual(snapshot.values["runtime_inverter_state"], "conflict")
            self.assertIn("runtime_identity_conflict", snapshot.values)
            self.assertIn(self._SERIAL, snapshot.values["runtime_identity_conflict"])
            self.assertIn(self._OTHER_SERIAL, snapshot.values["runtime_identity_conflict"])
            self.assertFalse(hub._inverter_binding_needs_live_detection_refresh)

        asyncio.run(_run())

    # 5. reconnect after offline preserves driver and resumes polling.
    def test_reconnect_after_offline_resumes_driver_bound_polling(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())
            hub._last_snapshot = hub._build_snapshot()

            # Simulate an offline gap; the fake link reconnects on the next attempt.
            hub._link_manager.connected = False

            snapshot = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(snapshot.connected)
            self.assertIsNotNone(snapshot.inverter)
            self.assertEqual(snapshot.inverter.serial_number, self._SERIAL)
            self.assertEqual(snapshot.values["runtime_driver_state"], "driver_bound")
            self.assertEqual(snapshot.values["runtime_poll_state"], "polling")

        asyncio.run(_run())

    def test_owned_session_replacement_gets_one_bounded_handover_grace(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            link = _OwnedSessionHandoverLinkManager()
            hub._link_manager = link
            hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())

            # Establish the generation that completed a normal driver poll.
            initial = await hub.async_refresh(poll_interval=3.0)
            self.assertTrue(initial.connected)
            self.assertEqual(hub._stable_owned_session_generation, 1)

            # The same PN replaces its TCP socket. The regular 0.75s connect
            # budget is insufficient in this fake; lifecycle evidence grants
            # exactly one bounded 5s handover attempt for generation 2.
            link.connected = False
            link.owned_session_generation = 2
            recovered = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(recovered.connected)
            self.assertEqual(recovered.values["runtime_driver_state"], "driver_bound")
            self.assertNotEqual(recovered.values["runtime_poll_state"], "offline")
            self.assertIn(5.0, link.connect_timeouts)
            self.assertEqual(hub._stable_owned_session_generation, 2)

        asyncio.run(_run())

    def test_double_replacement_recovers_per_owned_session_generation(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            link = _DoubleReplacementLinkManager()
            hub._link_manager = link
            hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())
            await hub.async_refresh(poll_interval=3.0)

            link.connected = False
            link.owned_session_generation = 2
            recovered = await hub.async_refresh(poll_interval=3.0)

            self.assertTrue(recovered.connected)
            self.assertEqual(recovered.values["runtime_driver_state"], "driver_bound")
            self.assertEqual(hub._stable_owned_session_generation, 3)
            self.assertGreaterEqual(link.connect_timeouts.count(5.0), 2)

        asyncio.run(_run())

    # 6. short PN / partial metadata does not downgrade durable identity.
    def test_short_collector_pn_does_not_downgrade_durable_identity(self) -> None:
        hub = self._hub()
        hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())
        # Live session reports only the short heartbeat PN prefix.
        hub._link_manager.collector_info.collector_pn = "V001020SYN6234"

        snapshot = hub._build_snapshot()

        self.assertEqual(snapshot.collector.collector_pn, "V001020SYN62344022")
        self.assertEqual(snapshot.values["collector_pn"], "V001020SYN62344022")
        self.assertEqual(snapshot.values["runtime_collector_state"], "identified")
        self.assertNotIn("runtime_identity_conflict", snapshot.values)

    # 7. no infinite live-detection refresh loop.
    def test_provisional_refresh_is_bounded_when_detection_keeps_failing(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            hub.set_initial_inverter_binding(
                _SuccessDriver(),
                self._inverter(detection_status="startup_persisted_identity"),
            )

            detect_calls = 0

            async def _always_fail(_transport, *, driver_hint=""):
                nonlocal detect_calls
                detect_calls += 1
                raise RuntimeError("probe_timeout")

            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter",
                new=_always_fail,
            ):
                for _ in range(6):
                    await hub.async_refresh(poll_interval=3.0)

            # Bounded: detection stops re-running after the max attempts, and the
            # provisional identity is kept rather than lost.
            self.assertEqual(detect_calls, 3)
            self.assertFalse(hub._inverter_binding_needs_live_detection_refresh)
            self.assertIsNotNone(hub._inverter)
            self.assertEqual(hub._inverter.serial_number, self._SERIAL)

        asyncio.run(_run())

    # 8. support diagnostics include the explicit runtime state fields.
    def test_snapshot_exposes_all_runtime_state_fields(self) -> None:
        hub = self._hub()
        hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())

        snapshot = hub._build_snapshot()

        for key in (
            "runtime_session_state",
            "runtime_collector_state",
            "runtime_inverter_state",
            "runtime_driver_state",
            "runtime_poll_state",
            "runtime_last_driver_bound_identity",
            "runtime_state_transitions",
        ):
            self.assertIn(key, snapshot.values, key)
        self.assertEqual(snapshot.values["runtime_session_state"], "online")
        self.assertEqual(snapshot.values["runtime_collector_state"], "identified")
        self.assertEqual(snapshot.values["runtime_inverter_state"], "live_confirmed")

    def test_state_transition_history_is_bounded(self) -> None:
        hub = self._hub()
        hub.set_initial_inverter_binding(_SuccessDriver(), self._inverter())
        # Force many distinct composite states so the ring would overflow.
        for index in range(40):
            hub._link_manager.connected = bool(index % 2)
            hub._build_snapshot()

        self.assertLessEqual(len(hub._state_transition_history), 20)

    def test_driver_detection_is_cancelled_when_owned_session_changes(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=True)

            class _SessionChangingLink(_FakeLinkManager):
                def __init__(self) -> None:
                    super().__init__()
                    self.owned_session_generation = 1
                    self.changed = asyncio.Event()

                async def async_wait_for_owned_session_change(self, generation: int) -> None:
                    while self.owned_session_generation == generation:
                        await self.changed.wait()

            link = _SessionChangingLink()
            hub._link_manager = link
            started = asyncio.Event()
            cancelled = asyncio.Event()

            async def _slow_detection(*_args, **_kwargs):
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            with patch(
                "custom_components.eybond_local.runtime.hub.detection.async_detect_inverter_candidates",
                side_effect=_slow_detection,
            ):
                detection = asyncio.create_task(hub._async_detect_driver())
                await asyncio.wait_for(started.wait(), timeout=1.0)
                link.owned_session_generation += 1
                link.changed.set()
                result = await asyncio.wait_for(detection, timeout=1.0)

            self.assertEqual(result, "collector_session_changed")
            self.assertTrue(cancelled.is_set())

        asyncio.run(_run())

    def test_cancelled_refresh_drains_late_driver_detection_failure(self) -> None:
        async def _run() -> None:
            hub = self._hub(full_scan=False)
            started = asyncio.Event()
            child_cancelled = asyncio.Event()
            finish_child = asyncio.Event()
            loop_errors: list[dict[str, object]] = []
            loop = asyncio.get_running_loop()
            previous_handler = loop.get_exception_handler()
            loop.set_exception_handler(
                lambda _loop, context: loop_errors.append(context)
            )

            async def _late_failure(*_args, **_kwargs):
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    child_cancelled.set()
                    await finish_child.wait()
                    raise DriverSweepNoMatch(
                        "no_supported_driver_matched",
                        silent=True,
                    )

            try:
                with patch(
                    "custom_components.eybond_local.runtime.hub.detection."
                    "async_detect_inverter",
                    side_effect=_late_failure,
                ):
                    detection = asyncio.create_task(hub._async_detect_driver())
                    await asyncio.wait_for(started.wait(), timeout=1.0)
                    detection.cancel()
                    await asyncio.wait_for(child_cancelled.wait(), timeout=1.0)
                    self.assertFalse(detection.done())
                    detection.cancel()
                    await asyncio.sleep(0)
                    self.assertFalse(detection.done())
                    finish_child.set()
                    try:
                        result = await asyncio.wait_for(detection, timeout=1.0)
                    except asyncio.CancelledError:
                        pass
                    else:
                        self.fail(f"cancelled detection returned {result!r}")

                await asyncio.sleep(0)
                self.assertTrue(detection.cancelled())
                self.assertEqual(loop_errors, [])
            finally:
                loop.set_exception_handler(previous_handler)

        asyncio.run(_run())


class CollectorDevcodeDiagnosticsTests(unittest.TestCase):
    """Diagnostics/devcode split: stable identity vs volatile frame vs route."""

    def _hub(self) -> EybondHub:
        hub = EybondHub(
            connection=EybondConnectionSpec(
                server_ip="192.168.1.10",
                collector_ip="192.168.1.14",
                collector_pn="V001020SYN62344022",
                tcp_port=8899,
                udp_port=58899,
                discovery_target="192.168.1.255",
                discovery_interval=30,
                heartbeat_interval=60,
                request_timeout=5.0,
            ),
        )
        hub._link_manager = _FakeLinkManager()
        return hub

    def test_zero_heartbeat_devcode_is_preserved_as_0x0000(self) -> None:
        hub = self._hub()
        hub._link_manager.collector_info.heartbeat_devcode = 0x0000
        hub._link_manager.collector_info.last_devcode = 0x0994  # volatile last frame

        snapshot = hub._build_snapshot()

        # 0x0000 is a valid identity, not "no data": collector_devcode stays 0x0000
        # and never falls through to the volatile last-frame devcode.
        self.assertEqual(snapshot.values["collector_devcode"], "0x0000")
        self.assertEqual(snapshot.values["collector_heartbeat_devcode"], "0x0000")
        self.assertEqual(snapshot.values["collector_last_frame_devcode"], "0x0994")

    def test_last_frame_devcode_changes_without_changing_identity(self) -> None:
        hub = self._hub()
        hub._link_manager.collector_info.heartbeat_devcode = 0x0994  # stable identity

        hub._link_manager.collector_info.last_devcode = 0x0001
        first = hub._build_snapshot()
        hub._last_snapshot = first
        hub._link_manager.collector_info.last_devcode = 0x0002
        second = hub._build_snapshot()

        # Stable identity is unchanged across frames; only the frame diagnostic moves.
        self.assertEqual(first.values["collector_devcode"], "0x0994")
        self.assertEqual(second.values["collector_devcode"], "0x0994")
        self.assertEqual(first.values["collector_last_frame_devcode"], "0x0001")
        self.assertEqual(second.values["collector_last_frame_devcode"], "0x0002")

    def test_inverter_route_devcode_distinct_from_collector_devcode(self) -> None:
        hub = self._hub()
        hub._link_manager.collector_info.heartbeat_devcode = 0x0000  # collector identity
        hub._link_manager.collector_info.smartess_device_address = 4  # mgmt addr
        hub._inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="92632500000001",
            probe_target=ProbeTarget(devcode=0x0994, collector_addr=1, device_addr=0),
        )

        snapshot = hub._build_snapshot()

        # The inverter payload route (probe target) is distinct from the collector
        # identity/management devcode.
        self.assertEqual(snapshot.values["inverter_route_devcode"], "0x0994")
        self.assertEqual(snapshot.values["collector_devcode"], "0x0000")
        self.assertNotEqual(
            snapshot.values["inverter_route_devcode"],
            snapshot.values["collector_devcode"],
        )
        self.assertEqual(snapshot.values["smartess_device_address"], 4)

    def test_snapshot_exposes_separate_heartbeat_frame_route_diagnostics(self) -> None:
        hub = self._hub()
        hub._link_manager.collector_info.heartbeat_devcode = 0x0001
        hub._link_manager.collector_info.last_devcode = 0x0994
        hub._inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG 6200",
            serial_number="92632500000001",
            probe_target=ProbeTarget(devcode=0x0002, collector_addr=1, device_addr=0),
        )

        snapshot = hub._build_snapshot()

        self.assertEqual(snapshot.values["collector_heartbeat_devcode"], "0x0001")
        self.assertEqual(snapshot.values["collector_last_frame_devcode"], "0x0994")
        self.assertEqual(snapshot.values["inverter_route_devcode"], "0x0002")
        # 0 collector/device addr are preserved (is-not-None, not falsy).
        self.assertEqual(snapshot.values["inverter_route_collector_addr"], 1)
        self.assertEqual(snapshot.values["inverter_route_device_addr"], 0)

    def test_stable_devcode_does_not_mask_last_frame_devcode(self) -> None:
        hub = self._hub()
        hub._link_manager.collector_info.heartbeat_devcode = 0x0994
        hub._link_manager.collector_info.last_devcode = 0x0001

        snapshot = hub._build_snapshot()

        # The stable collector_devcode and the volatile last-frame field coexist
        # as distinct, clearer fields -- neither masks the other.
        self.assertIn("collector_devcode", snapshot.values)
        self.assertIn("collector_last_frame_devcode", snapshot.values)
        self.assertNotEqual(
            snapshot.values["collector_devcode"],
            snapshot.values["collector_last_frame_devcode"],
        )

    def test_metadata_semantic_ownership_is_flattened_for_support(self) -> None:
        hub = self._hub()
        hub.collector_metadata_diagnostics = lambda: {
            "routes": [
                {
                    "channel_id": "collector:fc_metadata",
                    "effective_excluded_semantic_fields": ["collector_ssid"],
                    "unsupported_semantic_fields": ["collector_ssid"],
                },
                {
                    "channel_id": "collector:at_metadata",
                    "effective_excluded_semantic_fields": [
                        "collector_wifi_gateway",
                        "collector_wifi_ip",
                    ],
                    "unsupported_semantic_fields": [],
                },
            ],
            "semantic_ownership": {
                "binding_generation": 4,
                "at_owned_fields": [
                    "collector_signal_strength",
                    "collector_signal_strength_raw",
                ],
                "framed_unsupported_fields": ["collector_ssid"],
            },
        }
        values: dict[str, object] = {}

        hub._apply_collector_metadata_diagnostics(values)

        self.assertEqual(
            values["collector_metadata_effective_exclusions"],
            "collector:fc_metadata=collector_ssid, "
            "collector:at_metadata=collector_wifi_gateway|collector_wifi_ip",
        )
        self.assertEqual(
            values["collector_metadata_unsupported_fields"],
            "collector:fc_metadata=collector_ssid",
        )
        self.assertEqual(values["collector_metadata_semantic_binding_generation"], 4)
        self.assertEqual(
            values["collector_metadata_at_owned_fields"],
            "collector_signal_strength, collector_signal_strength_raw",
        )
        self.assertEqual(
            values["collector_metadata_framed_unsupported_fields"],
            "collector_ssid",
        )


class HubCollectorManagementTests(unittest.TestCase):
    """Hub delegation of collector-management ACTIONS to the negotiated adapter."""

    def _hub(self) -> EybondHub:
        return EybondHub(
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

    def _framed_hub(self):
        hub = self._hub()
        link = _FakeLinkManager()
        link.transport = _CollectorManagementTransport()
        hub._link_manager = link
        return hub, link

    def _at_hub(self):
        hub = self._hub()
        link = _FakeLinkManager()
        link.transport = object()  # no async_send_collector -> not framed
        link.collector_at_transport = _CollectorAtQueryTransport(
            {"CLDSRVHOST1": "iot.eybond.com,18899,TCP"}
        )
        hub._link_manager = link
        return hub, link

    def test_framed_capabilities_all_true(self) -> None:
        hub, _ = self._framed_hub()
        caps = hub.collector_management_capabilities()
        self.assertTrue(caps.read_endpoint_state)
        self.assertTrue(caps.write_endpoint)
        self.assertTrue(caps.apply_changes)
        self.assertTrue(caps.reboot)

    def test_wifi_management_uses_the_exact_runtime_owned_framed_session(
        self,
    ) -> None:
        class _WifiTransport:
            def __init__(self) -> None:
                self.values = {7: "Old SSID", 8: "old-password"}
                self.requests: list[tuple[int, bytes]] = []

            async def async_send_collector(
                self,
                *,
                fcode: int,
                payload: bytes = b"",
                devcode: int = 0,
                collector_addr: int = 1,
            ):
                del devcode, collector_addr
                self.requests.append((fcode, payload))
                parameter = payload[0]
                if fcode == 2:
                    return (
                        None,
                        bytes((0, parameter))
                        + self.values.get(parameter, "").encode("ascii"),
                    )
                if fcode == 3:
                    value = payload[1:].decode("ascii")
                    if parameter != 29:
                        self.values[parameter] = value
                    return (None, bytes((0, parameter)))
                raise KeyError((fcode, payload))

        async def _run() -> None:
            hub = self._hub()
            link = _FakeLinkManager()
            transport = _WifiTransport()
            link.transport = transport
            link.active_transport = transport
            hub._link_manager = link

            before = await hub.async_query_collector_parameters((7, 8))
            applied_ssid = await hub.async_set_collector_wifi_credentials(
                ssid="New SSID",
                password="new-password",
                ssid_parameter=7,
                password_parameter=8,
            )

            self.assertEqual(before, {7: "Old SSID", 8: "old-password"})
            self.assertEqual(applied_ssid, "New SSID")
            self.assertEqual(transport.values[7], "New SSID")
            self.assertEqual(transport.values[8], "new-password")
            self.assertIn((3, bytes((29,)) + b"1"), transport.requests)

        asyncio.run(_run())

    def test_uart_management_uses_the_exact_runtime_owned_framed_session(self) -> None:
        async def _run() -> None:
            hub = self._hub()
            link = _FakeLinkManager()
            transport = _CollectorManagementTransport()
            link.transport = transport
            link.active_transport = transport
            hub._link_manager = link

            before = await hub.async_query_collector_parameters((34,))
            readback = await hub.async_set_collector_uart_baudrate("9600")

            self.assertEqual(before, {34: "2400,8,1,NONE"})
            self.assertEqual(readback, "9600,8,1,NONE")
            self.assertEqual(transport.uart, "9600,8,1,NONE")

        asyncio.run(_run())

    def test_wifi_management_never_uses_a_stale_fallback_transport(self) -> None:
        async def _run() -> None:
            hub, link = self._framed_hub()
            link.active_transport = None

            with self.assertRaisesRegex(
                CollectorManagementUnsupportedError,
                "collector_local_management_not_supported",
            ):
                await hub.async_query_collector_parameters((7,))

            self.assertEqual(link.transport.requests, [])

        asyncio.run(_run())

    def test_at_capabilities_include_vendor_restart(self) -> None:
        hub, _ = self._at_hub()
        caps = hub.collector_management_capabilities()
        self.assertTrue(caps.read_endpoint_state)
        self.assertTrue(caps.write_endpoint)
        self.assertTrue(caps.apply_changes)
        self.assertTrue(caps.reboot)

    def test_capabilities_follow_live_handover_without_reload(self) -> None:
        # Start framed, then the SAME hub sees the wire hand over to AT:
        # capabilities remain live and update without a config-entry reload.
        hub, link = self._framed_hub()
        self.assertTrue(hub.collector_management_capabilities().reboot)
        link.transport = object()
        link.collector_at_transport = _CollectorAtQueryTransport(
            {"CLDSRVHOST1": "iot.eybond.com,18899,TCP"}
        )
        self.assertTrue(hub.collector_management_capabilities().reboot)
        self.assertTrue(hub.collector_management_capabilities().write_endpoint)

    def test_unavailable_when_adapter_is_none(self) -> None:
        hub = self._hub()
        link = _FakeLinkManager()
        link.transport = object()  # neither framed nor AT
        hub._link_manager = link
        caps = hub.collector_management_capabilities()
        self.assertFalse(
            any((caps.read_endpoint_state, caps.write_endpoint, caps.apply_changes, caps.reboot))
        )

    def test_live_conflict_with_binding_is_none_capabilities_false_provenance_conflict(self) -> None:
        # Even with an existing (framed) confirmed binding, a live conflict yields
        # none adapter, all-false capabilities, provenance "conflict", and the
        # diagnostics never show framed/AT as the effective management adapter.
        hub = self._hub()
        link = _FakeLinkManager()
        link.transport = _CollectorManagementTransport()  # framed transport present

        def _adapter_id() -> str:
            from custom_components.eybond_local.connection.session_handle import ADAPTER_NONE

            return ADAPTER_NONE

        link.collector_management_adapter_id = _adapter_id
        link.collector_management_adapter_provenance = lambda: "conflict"
        hub._link_manager = link

        caps = hub.collector_management_capabilities()
        self.assertFalse(
            any((caps.read_endpoint_state, caps.write_endpoint, caps.apply_changes, caps.reboot))
        )
        diag = hub.collector_management_diagnostics()
        self.assertEqual(diag["collector_management_adapter_id"], "none")
        self.assertEqual(diag["collector_management_adapter_provenance"], "conflict")
        self.assertNotIn("framed_collector_commands", str(diag))
        self.assertNotIn("at_commands", str(diag))

    def test_at_write_uses_cldsrvhost1_and_intpara(self) -> None:
        async def _run() -> None:
            hub, link = self._at_hub()
            result = await hub.async_set_collector_server_endpoint(
                "192.168.8.113,18899,TCP", apply_changes=True
            )
            at = link.collector_at_transport
            self.assertIn(("CLDSRVHOST1", "192.168.8.113,18899,TCP"), at.writes)
            self.assertIn(("INTPARA", "29,1"), at.writes)
            self.assertEqual(result["status"], "applied")
            self.assertEqual(result["management_protocol"], "at_text")
            self.assertTrue(result["apply_performed"])

        asyncio.run(_run())

    def test_at_read_endpoint_state(self) -> None:
        async def _run() -> None:
            hub, link = self._at_hub()
            state = await hub.async_get_collector_server_endpoint_state()
            self.assertEqual(state["current_endpoint"], "iot.eybond.com,18899,TCP")
            self.assertIn("CLDSRVHOST1", link.collector_at_transport.queries)

        asyncio.run(_run())

    def test_at_reboot_uses_intpara_and_returns_confirmed_result(self) -> None:
        async def _run() -> None:
            hub, link = self._at_hub()

            result = await hub.async_reboot_collector()

            self.assertEqual(result["status"], "reboot_triggered")
            self.assertEqual(result["action"], "reboot")
            self.assertIn("CLDSRVHOST1", link.collector_at_transport.queries)
            self.assertIn(
                ("INTPARA", "29,1"), link.collector_at_transport.writes
            )

        asyncio.run(_run())

    def test_framed_staged_when_apply_not_requested(self) -> None:
        async def _run() -> None:
            hub, _ = self._framed_hub()
            result = await hub.async_set_collector_server_endpoint(
                "192.168.1.193,18899,TCP", apply_changes=False
            )
            self.assertEqual(result["status"], "staged")
            self.assertFalse(result["apply_performed"])
            self.assertTrue(result["write_confirmed"])

        asyncio.run(_run())

    def test_last_operation_recorded_on_success(self) -> None:
        async def _run() -> None:
            hub, _ = self._framed_hub()
            await hub.async_set_collector_server_endpoint(
                "192.168.1.193,18899,TCP", apply_changes=False
            )
            diag = hub.collector_management_diagnostics()
            op = diag["collector_management_last_operation"]
            self.assertEqual(op["operation"], "write_endpoint")
            self.assertEqual(op["status"], "ok")
            self.assertEqual(op["error_class"], "")
            self.assertIn("duration_ms", op)
            # No endpoint value leaked into diagnostics.
            self.assertNotIn("192.168.1.193", str(diag))

        asyncio.run(_run())

    def test_last_operation_records_at_reboot_success(self) -> None:
        async def _run() -> None:
            hub, _ = self._at_hub()
            await hub.async_reboot_collector()
            op = hub.collector_management_diagnostics()["collector_management_last_operation"]
            self.assertEqual(op["operation"], "reboot")
            self.assertEqual(op["status"], "ok")
            self.assertEqual(op["error_class"], "")

        asyncio.run(_run())

    def test_diagnostics_have_no_endpoint_or_credentials(self) -> None:
        hub, _ = self._framed_hub()
        diag = hub.collector_management_diagnostics()
        self.assertIn("collector_management_adapter_id", diag)
        self.assertIn("collector_management_capabilities", diag)
        blob = str(diag)
        for secret in ("password", "ssid", "18899", "eybond.com"):
            self.assertNotIn(secret, blob)


if __name__ == "__main__":
    unittest.main()
