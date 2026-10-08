"""Real coordinator export and signed HA download retain management diagnostics."""
from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.collector.management import (
    CollectorManagementTransportError, FramedCollectorManagementAdapter,
)
from custom_components.eybond_local.connection.models import EybondConnectionSpec
from custom_components.eybond_local.connection.session_handle import ADAPTER_COLLECTOR_FRAMED_COMMANDS
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.models import CollectorInfo
from custom_components.eybond_local.runtime.hub import EybondHub
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("parameter", [21, 30])
@pytest.mark.parametrize("identity_changed", [False, True])
async def test_failed_collector_read_context_reaches_downloaded_archive(
    hass, hass_client_no_auth, fake_runtime, monkeypatch, parameter, identity_changed,
):
    from conftest import FakeRuntimeManager

    calls = []
    link = SimpleNamespace(
        connected=True, owned_session_generation=17,
        owned_session_identity=("listener-synthetic-old", 8899),
        collector_info=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP,
                                     collector_pn=SYNTHETIC_COLLECTOR_PN),
        collector_management_adapter_id=lambda: ADAPTER_COLLECTOR_FRAMED_COMMANDS,
    )

    class Transport:
        async def async_send_collector(self, *, fcode, payload, devcode, collector_addr):
            calls.append((fcode, payload))
            assert fcode == 2
            if payload == bytes((parameter,)):
                link.owned_session_generation += 1
                link.connected = not identity_changed
                if identity_changed:
                    link.owned_session_identity = ("", 0)
                raise TimeoutError("private socket response")
            assert payload == b"\x15"
            return None, b"\x00\x15private-endpoint.example.test,18899,TCP"

    link.transport = Transport()
    hub = EybondHub(connection=EybondConnectionSpec(
        server_ip=SYNTHETIC_SERVER_IP, collector_ip=SYNTHETIC_COLLECTOR_IP,
        tcp_port=8899, udp_port=58899, discovery_target=SYNTHETIC_COLLECTOR_IP,
        discovery_interval=30, heartbeat_interval=60, request_timeout=5,
    ))
    hub._link_manager = link
    adapter = FramedCollectorManagementAdapter(lambda: link.transport)
    with pytest.raises(CollectorManagementTransportError):
        await hub._run_management_operation("read_endpoint_state", adapter.async_read_endpoint_state)

    async def refresh(self, *, poll_interval=None):
        snapshot = hub._build_snapshot()
        hub._last_snapshot = snapshot
        return snapshot

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="Collector management archive",
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
    path = Path(await coordinator.async_export_support_package())
    assert path.is_relative_to(Path(hass.config.config_dir) / "eybond_local/support_packages")
    assert not (Path(hass.config.config_dir) / "www/eybond_local/support_packages").exists()
    url = coordinator.data.values["support_package_download_url"]
    client = await hass_client_no_auth()
    assert (await client.get(url.split("?", 1)[0])).status == 401
    response = await client.get(url)
    assert response.status == 200
    downloaded = await response.read()
    assert downloaded == await hass.async_add_executor_job(path.read_bytes)
    with zipfile.ZipFile(BytesIO(downloaded)) as archive:
        text = archive.read("support_bundle.json").decode()
        bundle = json.loads(text)
    for secret in ("private socket response", "private-endpoint.example.test", SYNTHETIC_COLLECTOR_PN):
        assert secret not in text
    assert bundle["runtime"]["connected"] is not identity_changed
    for view in (
        bundle["runtime"]["values"], bundle["runtime"]["metadata"],
        bundle["roles"]["collector"]["values"],
        bundle["roles"]["diagnostics"]["collector_management_route"],
    ):
        assert view["collector_management_last_failed_request"] == {
            "protocol": "eybond_framed", "function": 2, "parameter": parameter,
        }
        assert view["collector_management_last_error_code"] == "TimeoutError"
        assert view["collector_management_last_session_generation_start"] == 17
        assert view["collector_management_last_session_generation_end"] == 18
        assert view["collector_management_last_session_identity_changed"] is identity_changed
        assert view["collector_management_last_failure"]["session_identity_changed"] is identity_changed
    assert calls == [(2, b"\x15")] + ([(2, b"\x1e")] if parameter == 30 else [])
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
