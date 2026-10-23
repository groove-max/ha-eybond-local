"""Real HA UI/action, coordinator ownership and export for diagnostic overrides.

Only the device boundary is synthetic; physical stream framing has separate
unit tests. No sockets, user HA servers or inverter writes are involved.
"""

import asyncio
import json

import pytest

from custom_components.eybond_local.const import DOMAIN, SERVICE_RUN_DIAGNOSTIC_COMMANDS
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.link_models import RawSerialLinkRoute
from custom_components.eybond_local.models import ProbeTarget
from test_ha_config_flow import collector_entry


SCRIPT = "driver modbus\nwire raw_modbus\ndevice_addr 1\nwrite_single 354 2\nread 354"


class Device:
    connected = True

    def __init__(self, coordinator):
        self.coordinator = coordinator
        self.requests = []
        self.wait = None
        self.started = asyncio.Event()
        self.fixture = FixtureTransport(registers={354: 1}, command_responses=None,
                                        probe_target=ProbeTarget(0, 0, 1))

    def create_diagnostic_transport(self, wire, *, timeout):
        assert wire == "raw_modbus"
        assert timeout == 10
        return self

    def select_payload_route(self, route, *, payload_family):
        assert payload_family == "modbus_rtu"
        return RawSerialLinkRoute(protocol="modbus_rtu")

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        assert self.coordinator._diagnostic_active
        assert self.coordinator._runtime_operation_lock.locked()
        assert isinstance(route, RawSerialLinkRoute)
        self.requests.append(payload)
        self.started.set()
        if self.wait is not None:
            await self.wait.wait()
        return await self.fixture.async_send_forward(payload, devcode=0, collector_addr=0)


@pytest.mark.parametrize("through_ui", [False, True])
async def test_confirmed_override_runs_and_exports_without_changing_entry(
    hass, hass_client_no_auth, collector_entry, fake_runtime, monkeypatch, through_ui,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    device = Device(coordinator)
    monkeypatch.setattr(coordinator, "_diagnostic_link_transport", lambda: device)
    before = (dict(collector_entry.data), dict(collector_entry.options))
    flow_id = None
    try:
        if through_ui:
            options = hass.config_entries.options
            initial = await options.async_init(collector_entry.entry_id)
            flow_id = initial["flow_id"]
            flow = options._progress[flow_id]
            result = await flow.async_step_diagnostic_commands({
                "diagnostic_commands": SCRIPT, "diagnostic_confirm_write": False,
                "diagnostic_publish_download_copy": True,
            })
            assert not result["errors"]
            assert "confirm_write" in flow._diagnostic_commands_output
            assert device.requests == []
            result = await flow.async_step_diagnostic_commands({
                "diagnostic_commands": SCRIPT, "diagnostic_confirm_write": True,
                "diagnostic_publish_download_copy": True,
            })
            assert not result["errors"]
            assert "wire: raw_modbus" in flow._diagnostic_commands_output
            url = flow._diagnostic_commands_download_url
        else:
            response = await hass.services.async_call(
                DOMAIN, SERVICE_RUN_DIAGNOSTIC_COMMANDS,
                {"entry_id": collector_entry.entry_id, "commands": SCRIPT,
                 "confirm_write": True, "publish_download_copy": True},
                blocking=True, return_response=True,
            )
            assert response["success"]
            url = response["download_url"]
        assert [request[1] for request in device.requests] == [6, 3]
        assert not coordinator._diagnostic_active
        assert not coordinator._runtime_operation_lock.locked()
        assert (dict(collector_entry.data), dict(collector_entry.options)) == before
        client = await hass_client_no_auth()
        response = await client.get(url)
        assert response.status == 200
        artifact = json.loads(await response.text())
        assert artifact["success"]
        assert artifact["context"]["requested_wire"] == "raw_modbus"
        assert len(artifact["context"]["wire_events"]) == 2
        assert "entry_id" not in artifact["context"]
        assert artifact["results"][-1]["response"]["decimal"] == [2]
    finally:
        if flow_id:
            hass.config_entries.options.async_abort(flow_id)
        await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_unload_cancels_diagnostic_and_releases_runtime_lock(
    hass, collector_entry, fake_runtime, monkeypatch,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    device = Device(coordinator)
    device.wait = asyncio.Event()
    monkeypatch.setattr(coordinator, "_diagnostic_link_transport", lambda: device)
    pending = asyncio.create_task(coordinator.async_run_diagnostic_commands(commands=SCRIPT, confirm_write=True))
    await asyncio.wait_for(device.started.wait(), 2)
    await hass.config_entries.async_unload(collector_entry.entry_id)
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not coordinator._diagnostic_active
    assert not coordinator._runtime_operation_lock.locked()
