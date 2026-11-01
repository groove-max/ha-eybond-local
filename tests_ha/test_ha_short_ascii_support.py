"""Real HA archive/download path with the real short-ASCII evidence collector."""

from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
import zipfile

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.eybond_short_ascii import EybondShortAsciiDriver
from custom_components.eybond_local.link_models import EybondLinkRoute
from custom_components.eybond_local.models import CollectorInfo, ProbeTarget, RuntimeSnapshot
from custom_components.eybond_local.payload.short_ascii import READ_COMMANDS, parse_q1, parse_rb
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


@pytest.mark.parametrize("outcome", ["ok", "partial", "invalid", "offline"])
async def test_support_archive_preserves_read_evidence_through_ha_download(
    hass, hass_client_no_auth, fake_runtime, monkeypatch, outcome,
) -> None:
    from conftest import FakeRuntimeManager

    body = b"230.0 04 03 115.0 013 60.0 13.2 35.0 1000010" + bytes(3)
    rb = bytearray(37)
    rb[:2] = (520).to_bytes(2, "big")
    rb[2] = 80
    responses = {
        "MP": b"\x01" + bytes(range(36)) + b"\r",
        "Q1": b"\x01" + body + sum(body).to_bytes(2, "big") + b"\r",
        "MD": b"S2-8127-260101-V7.00  \x00\r",
        "RH": b"\x01" + bytes(8) + b"\x01" + bytes(18) + b"\x01\r",
        "F": b"#115.0 105 48.00 60.0\r",
        "RB": b"\x01" + rb + bytes([sum(rb) & 255]) + b"\r",
    }
    # Deliberately malformed wire evidence must survive export too. Do not
    # require successful semantic decoding just to retain a diagnostic sample.
    if outcome == "invalid":
        responses["RB"] = responses["RB"][:-2] + b"\xff\r"

    class Transport:
        connected = True
        capturing = False
        capture_commands = []

        async def async_send_payload(self, payload, *, route, request_timeout=None):
            assert type(route) is EybondLinkRoute
            assert (route.devcode, route.collector_addr) == (767, 255)
            command = payload[:-2].decode("ascii")
            assert command in READ_COMMANDS and payload[-2:] == b"\x01\r"
            if self.capturing:
                self.capture_commands.append(command)
                if outcome == "offline" or (outcome == "partial" and command == "F"):
                    raise ConnectionError("synthetic_connection_lost")
                if outcome == "partial" and command == "RB":
                    raise TimeoutError("synthetic_read_timeout")
            return bytes(responses[command])

    driver, transport = EybondShortAsciiDriver(), Transport()
    inverter = await driver.async_probe(transport, ProbeTarget(767, 255, 1))
    assert inverter is not None

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=outcome != "offline",
            collector=CollectorInfo(
                remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN,
            ),
            inverter=inverter,
            values=parse_q1(responses["Q1"]) if outcome != "offline" else {},
        )

    async def capture(self):
        transport.capturing = True
        try:
            return await driver.async_capture_support_evidence(transport, inverter)
        finally:
            transport.capturing = False

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    monkeypatch.setattr(FakeRuntimeManager, "async_capture_support_evidence", capture)
    entry = MockConfigEntry(
        domain=DOMAIN, version=3, title="Short-ASCII support test",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "collector_operation_mode": "home_assistant_only",
            "tcp_port": 8899, "udp_port": 58899, "driver_hint": "auto",
            "control_mode": "read_only", "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external", "proxy_enabled": False,
            "detected_driver": driver.key, "detected_model": inverter.model_name,
            "detected_serial": "", "detection_confidence": "high",
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()
    coordinator = entry.runtime_data
    path = Path(await coordinator.async_export_support_package())
    assert path.is_relative_to(Path(hass.config.config_dir) / "eybond_local/support_packages")
    assert not (Path(hass.config.config_dir) / "www/eybond_local/support_packages").exists()
    url = coordinator.data.values["support_package_download_url"]
    assert "authSig=" in url
    # A frontend link opens without an Authorization header; its signed URL
    # must work on its own, while the same unsigned private route must not.
    client = await hass_client_no_auth()
    denied = await client.get(url.split("?", 1)[0])
    assert denied.status == 401
    response = await client.get(url)
    assert response.status == 200
    downloaded = await response.read()
    assert downloaded == await hass.async_add_executor_job(path.read_bytes)

    with zipfile.ZipFile(BytesIO(downloaded)) as archive:
        assert archive.testzip() is None
        manifest = json.loads(archive.read("manifest.json"))
        bundle = json.loads(archive.read("support_bundle.json"))
        raw = json.loads(archive.read("raw_capture.json"))
        assert manifest["sharing_guidance"]["recommended_artifact"] == path.name
        assert SYNTHETIC_COLLECTOR_PN not in archive.read("support_bundle.json").decode()
    assert bundle["runtime"]["connected"] is (outcome != "offline")
    assert raw["capture_kind"] == "short_ascii_read_only"
    assert transport.capture_commands == list(READ_COMMANDS)
    failed = set(READ_COMMANDS) if outcome == "offline" else {"F", "RB"} if outcome == "partial" else set()
    assert set(raw["failures"]) == failed
    assert set(raw["responses_hex"]) == set(READ_COMMANDS) - failed
    for command, payload in raw["responses_hex"].items():
        assert bytes.fromhex(payload) == responses[command], command
    if outcome == "partial":
        assert raw["failures"] == {"F": "ConnectionError", "RB": "TimeoutError"}
    if outcome == "ok":
        assert parse_rb(bytes.fromhex(raw["responses_hex"]["RB"]))["battery_soc"] == 80
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
