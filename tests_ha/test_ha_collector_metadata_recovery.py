"""Real HA coordinator/archive with the production metadata reader and service."""

import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.collector.metadata import build_collector_metadata_routes
from custom_components.eybond_local.collector.parameter_registry import FramedMetadataReadCursor
from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.models import CollectorInfo
from custom_components.eybond_local.runtime.hub import EybondHub
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("partial", [False, True])
async def test_metadata_loss_is_bounded_and_recovers_without_setting_writes(
    hass, fake_runtime, monkeypatch, partial,
):
    """Only device replies/time are synthetic; reader, hub, HA and ZIP are real."""
    from conftest import FakeRuntimeManager

    calls = []
    clock = [100.0]
    recovered = False
    cursor = FramedMetadataReadCursor()

    class Transport:
        async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
            assert fcode == 2, "metadata must not write or reboot the collector"
            parameter = payload[0]
            calls.append(parameter)
            if parameter == 2 and (partial or recovered):
                return None, b"\x00\x02" + SYNTHETIC_COLLECTOR_PN.encode()
            if not recovered:
                raise TimeoutError("synthetic silence")
            if parameter == 6:
                return None, b"\x00\x066.0"
            return None, bytes((1, parameter))

    transport = Transport()
    hub = EybondHub(connection=EybondConnectionSpec(
        server_ip=SYNTHETIC_SERVER_IP, collector_ip=SYNTHETIC_COLLECTOR_IP,
        tcp_port=8899, udp_port=58899, discovery_target=SYNTHETIC_COLLECTOR_IP,
        discovery_interval=30, heartbeat_interval=60, request_timeout=5,
    ))
    hub._link_manager = SimpleNamespace(
        connected=True, owned_session_generation=1,
        collector_info=CollectorInfo(
            remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN,
        ),
        collector_metadata_routes=lambda: build_collector_metadata_routes(
            framed_transport=transport, framed_read_cursor=cursor, generation=1,
            identity=SYNTHETIC_COLLECTOR_PN, session_id="synthetic-metadata",
        ),
    )
    hub._collector_metadata_service._now = lambda: clock[0]

    async def refresh(self, *, poll_interval=None):
        values = await hub._async_read_collector_runtime_values(poll_interval=10)
        snapshot = hub._build_snapshot(extra_values=values)
        hub._last_snapshot = snapshot
        return snapshot

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="Synthetic metadata recovery",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN, "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "control_mode": "read_only",
            "connection_strategy": "callback_on_demand", "endpoint_control_policy": "external",
            "proxy_enabled": False,
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    assert calls == ([2, 4, 5] if partial else [2, 4])
    key = "collector_metadata_channel_deferred_commands"
    assert coordinator.data.values[key] == f"collector:fc_metadata={10 if partial else 11}"
    assert hub._collector_runtime_read_fresh is partial
    path = Path(await coordinator.async_export_support_package())

    def read_archive():
        with zipfile.ZipFile(path) as archive:
            return json.loads(archive.read("support_bundle.json"))

    bundle = await hass.async_add_executor_job(read_archive)
    assert bundle["roles"]["diagnostics"]["collector_metadata"][key] == coordinator.data.values[key]
    assert not hub._collector_metadata_service.dead_channels()

    before = list(calls)
    clock[0] = 120
    await coordinator.async_refresh()
    assert calls == before
    assert hub._collector_runtime_read_fresh is False

    recovered = True
    clock[0] = 131
    await coordinator.async_refresh()
    assert calls[len(before)] == (6 if partial else 5)
    assert coordinator.data.values["collector_hardware_version"] == "6.0"
    assert hub._collector_runtime_read_fresh is True
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
