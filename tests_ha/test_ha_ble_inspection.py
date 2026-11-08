"""Real HA options lifecycle and exports; only Bluetooth hardware is fake."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest
from homeassistant.setup import async_setup_component

from custom_components.eybond_local.collector.smartess_ble import VENDOR_LAYOUT
from custom_components.eybond_local.runtime import ble_inspection as runtime
from custom_components.eybond_local.support.diagnostic_evidence import load_diagnostic_evidence
from test_ha_config_flow import collector_entry
from synthetic import SYNTHETIC_OTHER_COLLECTOR_PN


ADDRESS = "AA:BB:CC:DD:EE:FF"


class Link:
    def __init__(self, pn):
        self.connected = False
        self.commands = []
        self.pn = pn
        self.started = asyncio.Event()
        self.release = None
        self.disconnect_started = asyncio.Event()
        self.disconnect_release = None
        self.fail_disconnect = False
        self.callback = None
        self.reply_mode = "complete"

    async def connect(self):
        self.connected = True
        return [VENDOR_LAYOUT.service_uuid]

    async def disconnect(self):
        self.disconnect_started.set()
        if self.disconnect_release is not None:
            await self.disconnect_release.wait()
        self.connected = False
        if self.fail_disconnect:
            raise RuntimeError("synthetic close failure")

    async def start_notify(self, uuid, callback):
        self.callback = callback

    async def stop_notify(self, uuid):
        self.callback = None

    async def write(self, uuid, payload, *, response=False):
        assert uuid == VENDOR_LAYOUT.write_uuid
        assert response is True
        self.commands.append(payload)
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        command = payload.decode().strip()
        values = {"AT+DTUPN?": self.pn, "AT+FWVER?": "1.2.3",
                  "AT+ATVER?": "2.0", "AT+CLDSRVHOST1?": "example.invalid,18899,TCP"}
        reply = (command[:-1] + ":" + values[command]).encode()
        if self.reply_mode == "unrelated":
            self.callback(b"AT+PASS:private-password-in-notification\r\n")
        elif self.reply_mode == "split":
            reply += b"\r\n"
            self.callback(reply[:4])
            self.callback(reply[4:])
        else:
            self.callback(reply)

    async def read(self, uuid):
        return b""


@pytest.fixture
def ble(hass, collector_entry, monkeypatch):
    pn = collector_entry.data["collector_pn"]
    device = SimpleNamespace(name=pn, address=ADDRESS)
    info = SimpleNamespace(name=pn, address=ADDRESS, device=device,
                           service_uuids=[VENDOR_LAYOUT.service_uuid], manufacturer_data={})
    bluetooth = SimpleNamespace(
        async_discovered_service_info=Mock(return_value=[info]),
        async_ble_device_from_address=Mock(return_value=device),
    )
    monkeypatch.setattr(runtime, "_bluetooth", lambda: bluetooth)
    link = Link(pn)
    factory = Mock(return_value=link)
    monkeypatch.setattr(runtime, "BleakSmartEssBleLink", factory)
    return SimpleNamespace(link=link, device=device, info=info, bluetooth=bluetooth, factory=factory)


async def open_flow(hass, entry):
    options = hass.config_entries.options
    result = await options.async_init(entry.entry_id)
    flow_id = result["flow_id"]
    result = await options.async_configure(flow_id, {"next_step_id": "diagnostics"})
    assert "ble_inspection" in result["menu_options"]
    result = await options.async_configure(flow_id, {"next_step_id": "ble_inspection"})
    assert result["step_id"] == "ble_inspection"
    return options, flow_id


@pytest.mark.parametrize("source", ["local", "esphome"])
async def test_options_read_export_and_reopen_do_not_repeat_queries(
    hass, hass_client_no_auth, collector_entry, ble, source,
):
    # HA resolves the routing detail; no local scanner is constructed by this tool.
    ble.device.details = {"source": source}
    before = (dict(collector_entry.data), dict(collector_entry.options))
    options, flow_id = await open_flow(hass, collector_entry)
    try:
        assert not ble.link.commands
        result = await options.async_configure(flow_id, {"ble_inspection_action": "refresh"})
        assert result["step_id"] == "ble_inspection"
        assert not ble.link.commands
        result = await options.async_configure(flow_id, {
            "ble_inspection_action": "read", "ble_address": ADDRESS,
            "ble_line_ending": "none", "ble_download": True,
        })
        assert result["type"] == "progress"
        flow = options._progress[flow_id]
        await flow._ble_inspection_task
        await hass.async_block_till_done()
        result = await options.async_configure(flow_id)
        if result["type"] == "progress_done":
            result = await options.async_configure(flow_id)
        assert result["step_id"] == "ble_inspection_result"
        response = flow._ble_inspection_result
        assert response["success"], response
        assert response["cache_status"] == "available"
        ble.factory.assert_called_once_with(ADDRESS, device=ble.device, client_factory=ANY)
        ble.bluetooth.async_ble_device_from_address.assert_called_once_with(
            hass, ADDRESS, connectable=True,
        )
        assert ble.link.commands == [b"AT+DTUPN?", b"AT+FWVER?", b"AT+ATVER?", b"AT+CLDSRVHOST1?"]
        assert not ble.link.connected
        client = await hass_client_no_auth()
        download = await client.get(response["download_url"])
        assert download.status == 200
        text = await download.text()
        assert ADDRESS not in text
        assert collector_entry.data["collector_pn"] not in text
        artifact = json.loads(text)
        assert artifact["context"]["protocol_candidates"]["status"] == "not_applicable_ble"
        status, members = load_diagnostic_evidence(
            Path(hass.config.config_dir), collector_entry.entry_id, runtime.inspection_scope(collector_entry),
        )
        assert status["status"] == "included"
        assert members["evidence/diagnostic_run.json"]["success"]
        await options.async_configure(flow_id, {"ble_inspection_action": "read"})
        assert len(ble.link.commands) == 4  # Back to selection, no implicit rerun.
        assert (dict(collector_entry.data), dict(collector_entry.options)) == before
    finally:
        options.async_abort(flow_id)


async def test_removed_route_no_fallback_and_refresh_or_done_without_selection(hass, collector_entry, ble):
    options, flow_id = await open_flow(hass, collector_entry)
    try:
        ble.bluetooth.async_ble_device_from_address.return_value = None
        with pytest.raises(ValueError, match="unavailable"):
            await runtime.async_inspect_collector_ble(hass, collector_entry, address=ADDRESS, append_crlf=False)
        ble.factory.assert_not_called()
        result = await options.async_configure(flow_id, {"ble_inspection_action": "done"})
        assert result["step_id"] == "diagnostics"
    finally:
        options.async_abort(flow_id)


@pytest.mark.parametrize("mode", ["split", "unrelated"])
async def test_transport_evidence_survives_shareable_export_even_without_identity(
    hass, hass_client_no_auth, collector_entry, ble, monkeypatch, mode,
):
    from custom_components.eybond_local.support import ble_inspection

    monkeypatch.setattr(ble_inspection, "QUERY_TIMEOUT", .15)
    ble.link.reply_mode = mode
    before = (dict(collector_entry.data), dict(collector_entry.options))
    assert await async_setup_component(hass, "http", {})
    result = await runtime.async_inspect_collector_ble(
        hass, collector_entry, address=ADDRESS, append_crlf=False, publish_download=True,
    )
    assert result["success"] is (mode == "split")
    assert result["download_url"], result
    client = await hass_client_no_auth()  # Start serving after download views are registered.
    response = await client.get(result["download_url"])
    assert response.status == 200
    text = await response.text()
    assert "private-password" not in text
    assert ADDRESS not in text
    assert collector_entry.data["collector_pn"] not in text
    artifact = json.loads(text)
    assert artifact["context"]["connection"]["status"] == "ready"
    assert not artifact["context"]["settings_changed"]
    assert len(artifact["results"]) == 4
    for item in artifact["results"]:
        trace = item["transport"]
        assert trace["write_completed"]
        assert trace["notifications_received"] == (2 if mode == "split" else 1)
        assert trace["notifications_consumed"] == trace["notifications_received"]
        assert trace["phase"] == ("reply_complete" if mode == "split" else "receiving")
        assert (trace["matched_notify_bytes"] > 0) is (mode == "split")
    status, members = load_diagnostic_evidence(
        Path(hass.config.config_dir), collector_entry.entry_id, runtime.inspection_scope(collector_entry),
    )
    if mode == "split":
        assert status["status"] == "included"
    else:
        assert result["cache_status"] == "identity_unavailable_or_changed"
        assert members == {}  # Unbound identity must not attach to the collector.
    assert (dict(collector_entry.data), dict(collector_entry.options)) == before
    assert not ble.link.connected
    assert ble.link.commands == [b"AT+DTUPN?", b"AT+FWVER?", b"AT+ATVER?", b"AT+CLDSRVHOST1?"]


async def test_flow_abort_drains_cleanup_and_does_not_free_slot_early(hass, collector_entry, ble):
    ble.link.release = asyncio.Event()
    ble.link.disconnect_release = asyncio.Event()
    options, flow_id = await open_flow(hass, collector_entry)
    await options.async_configure(flow_id, {"ble_inspection_action": "read", "ble_address": ADDRESS})
    task = options._progress[flow_id]._ble_inspection_task
    await asyncio.wait_for(ble.link.started.wait(), 1)
    options.async_abort(flow_id)
    await asyncio.wait_for(ble.link.disconnect_started.wait(), 1)
    try:
        task.cancel()  # Repeated cancellation during mandatory finalization.
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(ValueError, match="busy"):
            await runtime.async_inspect_collector_ble(hass, collector_entry, address=ADDRESS, append_crlf=False)
    finally:
        ble.link.disconnect_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not ble.link.connected
    assert len(ble.link.commands) == 1
    assert not hass.data["eybond_local"][runtime._ACTIVE]
    ble.link.release.set()
    result = await runtime.async_inspect_collector_ble(hass, collector_entry, address=ADDRESS, append_crlf=False)
    assert result["success"]


@pytest.mark.parametrize("change", ["mismatch", "entry_changed", "cleanup_error"])
async def test_partial_evidence_does_not_claim_success_or_wrong_identity(hass, collector_entry, ble, change):
    if change == "mismatch":
        ble.link.pn = SYNTHETIC_OTHER_COLLECTOR_PN
    if change == "cleanup_error":
        ble.link.fail_disconnect = True
    ble.link.release = asyncio.Event()
    task = asyncio.create_task(runtime.async_inspect_collector_ble(
        hass, collector_entry, address=ADDRESS, append_crlf=False,
    ))
    await asyncio.wait_for(ble.link.started.wait(), 1)
    if change == "entry_changed":
        hass.config_entries.async_update_entry(collector_entry, data={
            **collector_entry.data, "collector_pn": SYNTHETIC_OTHER_COLLECTOR_PN,
        })
    ble.link.release.set()
    result = await task
    status, members = load_diagnostic_evidence(
        Path(hass.config.config_dir), collector_entry.entry_id, runtime.inspection_scope(collector_entry),
    )
    if change == "cleanup_error":
        assert not result["success"]
        assert result["error"] == "ble_cleanup_failed"
        assert status["status"] == "included"  # Failure evidence, not success.
    else:
        assert members == {}
        assert result["cache_status"] == "identity_unavailable_or_changed"


async def test_export_failure_retains_result_and_releases_slot(hass, collector_entry, ble, monkeypatch):
    monkeypatch.setattr(runtime, "export_diagnostic_run", Mock(side_effect=OSError("disk full")))
    result = await runtime.async_inspect_collector_ble(hass, collector_entry, address=ADDRESS, append_crlf=True)
    assert result["success"]  # Wire result remains distinct from export failure.
    assert result["export_error"] == "ble_inspection_export_failed"
    assert not ble.link.connected
    assert not hass.data["eybond_local"][runtime._ACTIVE]
    assert all(command.endswith(b"\r\n") for command in ble.link.commands)


async def test_failed_ha_device_connection_never_falls_back_to_address_scanning(
    hass, collector_entry, ble, monkeypatch,
):
    import bleak
    from custom_components.eybond_local.collector import smartess_ble

    monkeypatch.setattr(runtime, "BleakSmartEssBleLink", smartess_ble.BleakSmartEssBleLink)
    client = AsyncMock()
    client.connect.side_effect = RuntimeError("proxy unavailable")
    constructor = Mock(return_value=client)
    scanner = AsyncMock(side_effect=AssertionError("must not scan by address"))
    fallback = AsyncMock(side_effect=AssertionError("must not use standalone fallback"))
    monkeypatch.setattr(bleak, "BleakClient", constructor)
    monkeypatch.setattr(bleak.BleakScanner, "find_device_by_address", scanner)
    monkeypatch.setattr(smartess_ble, "_default_bleak_connect_client", fallback)
    result = await runtime.async_inspect_collector_ble(hass, collector_entry, address=ADDRESS, append_crlf=False)
    assert result["error"] == "ble_connection_failed"
    constructor.assert_called_once_with(ble.device)
    client.disconnect.assert_awaited_once()
    scanner.assert_not_called()
    fallback.assert_not_called()
    assert not hass.data["eybond_local"][runtime._ACTIVE]


async def test_conflicting_tcp_identity_cannot_bind_matching_ble_reply(hass, collector_entry, ble):
    collector_entry.runtime_data = SimpleNamespace(data=SimpleNamespace(
        collector={"collector_pn": SYNTHETIC_OTHER_COLLECTOR_PN}, inverter=None,
    ))
    result = await runtime.async_inspect_collector_ble(hass, collector_entry, address=ADDRESS, append_crlf=False)
    assert result["context"]["identity_status"] == "matched"
    assert result["error"] == "collector_identity_scope_changed"
    assert not result["success"]
    assert result["cache_status"] == "identity_unavailable_or_changed"
