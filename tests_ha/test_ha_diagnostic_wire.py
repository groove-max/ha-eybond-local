"""Real HA UI/action, coordinator ownership and export for diagnostic overrides.

Only the device boundary is synthetic; physical stream framing has separate
unit tests. No sockets, user HA servers or inverter writes are involved.
"""

import asyncio
from dataclasses import replace
from io import BytesIO
import json
import logging
from pathlib import Path
from zipfile import ZipFile

import pytest

from custom_components.eybond_local.const import DOMAIN, SERVICE_RUN_DIAGNOSTIC_COMMANDS
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget
from test_ha_config_flow import collector_entry
from synthetic import SYNTHETIC_OTHER_COLLECTOR_PN


SCRIPT = "driver modbus\nwire raw_modbus\ndevice_addr 1\nwrite_single 354 2\nread 354"
PAYLOAD_SCRIPT = (
    "driver raw\nwire fc4\ndevcode 1\ncollector_addr 255\ndevice_addr 1\n"
    "payload_hex 43 4D 53 47 06 49 4E 56 2D 48 42 0D"
)


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


class PayloadDevice(Device):
    response = b"\xab\xcd\xef"

    def create_diagnostic_transport(self, wire, *, timeout):
        assert wire == "fc4"
        assert timeout == 10
        return self

    async def async_send_payload(self, payload, *, route, request_timeout=None):
        assert self.coordinator._diagnostic_active
        assert self.coordinator._runtime_operation_lock.locked()
        assert route == EybondLinkRoute(devcode=1, collector_addr=255)
        self.requests.append(payload)
        self.started.set()
        if self.wait is not None:
            await self.wait.wait()
        return self.response


@pytest.mark.parametrize("through_ui", [False, True])
@pytest.mark.parametrize("opaque", [False, True])
async def test_confirmed_override_runs_and_exports_without_changing_entry(
    hass, hass_client_no_auth, collector_entry, fake_runtime, monkeypatch, through_ui, opaque,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    device = PayloadDevice(coordinator) if opaque else Device(coordinator)
    script = PAYLOAD_SCRIPT if opaque else SCRIPT
    wire = "fc4" if opaque else "raw_modbus"
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
                "diagnostic_commands": script, "diagnostic_confirm_write": False,
                "diagnostic_publish_download_copy": True,
            })
            assert not result["errors"]
            assert "confirm_write" in flow._diagnostic_commands_output
            assert device.requests == []
            result = await flow.async_step_diagnostic_commands({
                "diagnostic_commands": script, "diagnostic_confirm_write": True,
                "diagnostic_publish_download_copy": True,
            })
            assert not result["errors"]
            assert f"wire: {wire}" in flow._diagnostic_commands_output
            url = flow._diagnostic_commands_download_url
        else:
            response = await hass.services.async_call(
                DOMAIN, SERVICE_RUN_DIAGNOSTIC_COMMANDS,
                {"entry_id": collector_entry.entry_id, "commands": script,
                 "confirm_write": True, "publish_download_copy": True},
                blocking=True, return_response=True,
            )
            assert response["success"]
            url = response["download_url"]
        if opaque:
            assert device.requests == [b"CMSG\x06INV-HB\r"]
        else:
            assert [request[1] for request in device.requests] == [6, 3]
        assert not coordinator._diagnostic_active
        assert not coordinator._runtime_operation_lock.locked()
        assert (dict(collector_entry.data), dict(collector_entry.options)) == before
        client = await hass_client_no_auth()
        response = await client.get(url)
        assert response.status == 200
        artifact = json.loads(await response.text())
        assert artifact["success"]
        assert artifact["context"]["requested_wire"] == wire
        assert len(artifact["context"]["wire_events"]) == (1 if opaque else 2)
        assert "entry_id" not in artifact["context"]
        if opaque:
            assert artifact["results"][-1]["response"] == {
                "raw_hex": "abcdef", "response_size": 3,
                "reply_status": "payload_received", "decode_status": "not_attempted",
            }
        else:
            assert artifact["results"][-1]["response"]["decimal"] == [2]
    finally:
        if flow_id:
            hass.config_entries.options.async_abort(flow_id)
        await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("opaque", [False, True])
async def test_unload_cancels_diagnostic_and_releases_runtime_lock(
    hass, collector_entry, fake_runtime, monkeypatch, opaque,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    device = PayloadDevice(coordinator) if opaque else Device(coordinator)
    device.wait = asyncio.Event()
    monkeypatch.setattr(coordinator, "_diagnostic_link_transport", lambda: device)
    pending = asyncio.create_task(coordinator.async_run_diagnostic_commands(
        commands=PAYLOAD_SCRIPT if opaque else SCRIPT, confirm_write=True,
    ))
    await asyncio.wait_for(device.started.wait(), 2)
    await hass.config_entries.async_unload(collector_entry.entry_id)
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not coordinator._diagnostic_active
    assert not coordinator._runtime_operation_lock.locked()


@pytest.mark.parametrize("code", ["099E", "099E_extended", "09D5"])
async def test_saved_candidate_in_signed_support_zip_after_reload(
    hass, hass_client_no_auth, collector_entry, fake_runtime, monkeypatch, code,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    device = PayloadDevice(coordinator)
    if code == "099E_extended":
        fixture = json.loads((Path(__file__).resolve().parents[1]
                              / "tests/fixtures/protocol_candidates/issue82_099e_status.json")
                             .read_text(encoding="utf-8"))
        device.response = bytes.fromhex(fixture["response_hex"])
        script = PAYLOAD_SCRIPT
    elif code == "099E":
        body = bytes([0xAB] * 66)
        device.response = b"\xa1\xb2\xc3\xd4\xe5" + body + bytes([sum(body) & 255])
        script = PAYLOAD_SCRIPT
    else:
        body = b"\xa1\xb2\xc3\xd4\xe5" + bytes([0xAB] * 48)
        device.response = body + (sum(body) & 65535).to_bytes(2, "big") + b"\xfe"
        script = PAYLOAD_SCRIPT.rsplit("payload_hex", 1)[0] + "payload_hex eb90000100017c7f"
    monkeypatch.setattr(coordinator, "_diagnostic_link_transport", lambda: device)
    response = await coordinator.async_run_diagnostic_commands(commands=script, confirm_write=True)
    assert response["success"]
    assert len(device.requests) == 1
    # Persistence, not an in-memory last-result pointer; the replacement
    # coordinator has no diagnostic transport and must not rerun the command.
    assert await hass.config_entries.async_unload(collector_entry.entry_id)
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    reloaded = collector_entry.runtime_data
    await reloaded.async_export_support_package()
    client = await hass_client_no_auth()
    download = await client.get(reloaded.data.values["support_package_download_url"])
    assert download.status == 200
    with ZipFile(BytesIO(await download.read())) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        status = manifest["archive_members"]["diagnostic_evidence"]
        assert status["status"] == "included"
        assert status["identity_level"] == "collector_only"
        payload = json.loads(archive.read("evidence/diagnostic_run.json"))
        report = json.loads(archive.read("evidence/protocol_candidates.json"))
        observation = report["observations"][0]
        assert observation["vendor_code"] == code.split("_")[0]
        if code == "099E_extended":
            assert observation["expected_response_size"] == 84
            assert observation["body_size"] == 78
            assert observation["vendor_parse_body_size"] == 66
            assert observation["extension_size"] == 12
            assert observation["layout_evidence_url"] == fixture["source_url"].split("#")[0]
        assert not report["runtime_support_confirmed"]
        assert report["additional_requests_sent"] == 0
        assert payload["context"]["wire_events"][0]["response_hex"] == device.response.hex()
        assert payload["context"]["integration_build"]
    assert len(device.requests) == 1
    await hass.config_entries.async_unload(collector_entry.entry_id)


async def test_identity_change_during_diagnostic_does_not_bind_archive_cache(
    hass, hass_client_no_auth, collector_entry, fake_runtime, monkeypatch,
):
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    device = PayloadDevice(coordinator)
    device.wait = asyncio.Event()
    monkeypatch.setattr(coordinator, "_diagnostic_link_transport", lambda: device)
    pending = asyncio.create_task(coordinator.async_run_diagnostic_commands(
        commands=PAYLOAD_SCRIPT, confirm_write=True, publish_download_copy=True,
    ))
    await asyncio.wait_for(device.started.wait(), 2)
    coordinator.data = replace(coordinator.data, collector=CollectorInfo(
        remote_ip="192.0.2.23", collector_pn=SYNTHETIC_OTHER_COLLECTOR_PN,
    ))
    device.wait.set()
    result = await pending
    client = await hass_client_no_auth()
    response = await client.get(result["download_url"])
    payload = json.loads(await response.text())
    assert payload["context"]["support_archive_cache_status"] == "identity_unavailable_or_changed"
    assert result["success"]  # The independent diagnostic result is preserved.
    await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("finish", ["complete", "cancel", "unload"])
async def test_diagnostic_waits_for_existing_poll_without_extra_requests(
    hass, hass_client_no_auth, collector_entry, fake_runtime, monkeypatch, caplog, finish,
):
    from custom_components.eybond_local.runtime.coordinator import diagnostics

    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    snapshot = coordinator.data
    device = PayloadDevice(coordinator)
    entered, release = asyncio.Event(), asyncio.Event()
    ticks = [100.0]
    caplog.set_level(logging.DEBUG, logger="custom_components.eybond_local.runtime.coordinator")
    monkeypatch.setattr(diagnostics, "monotonic", lambda: ticks[0])
    monkeypatch.setattr(coordinator, "_diagnostic_link_transport", lambda: device)

    async def slow_poll(**kwargs):
        entered.set()
        await release.wait()
        return snapshot

    monkeypatch.setattr(coordinator, "_async_update_data_with_runtime_lock", slow_poll)
    poll = asyncio.create_task(coordinator._async_update_data())
    pending = unload = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        pending = asyncio.create_task(coordinator.async_run_diagnostic_commands(
            commands=PAYLOAD_SCRIPT, confirm_write=True, publish_download_copy=True,
        ))
        for _ in range(20):
            await asyncio.sleep(0)
            if "waiting for runtime ownership" in caplog.text:
                break
        assert coordinator._diagnostic_active
        assert "waiting for runtime ownership" in caplog.text
        assert not pending.done()
        assert device.requests == []
        assert await coordinator._async_update_data() is snapshot
        ticks[0] += 3.0
        if finish == "unload":
            unload = asyncio.create_task(hass.config_entries.async_unload(collector_entry.entry_id))
        elif finish == "cancel":
            pending.cancel()
        if finish != "complete":
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(pending, 2)
            assert device.requests == []
            assert not coordinator._diagnostic_active
            # Cancelling queued diagnostics cannot steal the poll's lock.
            assert coordinator._runtime_operation_lock.locked()
            assert not poll.done()
            assert "cancelled phase=runtime_lock_wait" in caplog.text
            if unload is not None:
                assert not unload.done()
                assert coordinator._runtime.stopped == 0
        release.set()
        await asyncio.wait_for(poll, 2)
        if finish == "complete":
            response = await asyncio.wait_for(pending, 2)
            assert response["success"]
            assert len(device.requests) == 1
            timing = {"runtime_lock_wait_ms": 3000, "scenario_ms": 0}
            assert response["context"]["coordinator_timing"] == timing
            client = await hass_client_no_auth()
            download = await client.get(response["download_url"])
            assert download.status == 200
            artifact = json.loads(await download.text())
            assert artifact["context"]["coordinator_timing"] == timing
            assert "export wait finished completed=True" in caplog.text
        if unload is not None:
            assert await asyncio.wait_for(unload, 2)
            assert "shutdown=True owns_poll=False" in caplog.text
        assert not coordinator._runtime_operation_lock.locked()
    finally:
        release.set()
        tasks = [task for task in (poll, pending, unload) if task is not None]
        if pending is not None and not pending.done():
            pending.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if unload is None:
            await hass.config_entries.async_unload(collector_entry.entry_id)
