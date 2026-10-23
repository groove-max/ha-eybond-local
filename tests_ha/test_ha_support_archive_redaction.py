"""Old cloud evidence is sanitized through the real coordinator and HA download."""

from copy import deepcopy
from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.drivers.probe_evidence import DriverProbeEvidence, ProbeActionFailure
from custom_components.eybond_local.models import CollectorInfo, RuntimeSnapshot
from custom_components.eybond_local.support.cloud_evidence import CloudEvidenceRecord
from synthetic import SYNTHETIC_COLLECTOR_IP, SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


async def test_saved_cloud_credentials_removed_without_losing_probe_evidence(
    hass, hass_client_no_auth, fake_runtime, monkeypatch,
):
    from conftest import FakeRuntimeManager

    # Synthetic failure, not a claim about the issue owner's actual failed read.
    action = "modbus_smg.runtime.read_holding.406.1"
    evidence = DriverProbeEvidence(
        kind="catalog_identity", status="runtime_validation_failed", protocol_key="modbus_smg",
        layout_code=1, model_code=0x3700, resolution="exact", candidate_keys=("anenji_6200",),
        failed_actions=(action,), action_failures=(ProbeActionFailure(action, "timeout"),),
    ).as_public_dict()

    async def refresh(self, *, poll_interval=None):
        return RuntimeSnapshot(
            connected=True,
            collector=CollectorInfo(remote_ip=SYNTHETIC_COLLECTOR_IP, collector_pn=SYNTHETIC_COLLECTOR_PN),
            inverter=None,
            values={"runtime_inverter_probe_log": [{
                "driver": "modbus_smg", "elapsed_ms": 5000, "outcome": "no_match",
                "saw_response": True, "diagnostic": deepcopy(evidence),
            }]},
        )

    monkeypatch.setattr(FakeRuntimeManager, "async_refresh", refresh)
    entry = MockConfigEntry(
        domain=DOMAIN, version=5, title="Support privacy regression",
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

    credentials = {key: f"synthetic-private-{key}" for key in ("token", "secret", "usr", "uid")}
    cloud = {"payload": {"session": credentials, "words": [14080, 1, 6200]}}
    source = Path(hass.config.config_dir) / "old_cloud_evidence.json"
    original = json.dumps(cloud)
    await hass.async_add_executor_job(source.write_text, original)
    coordinator._cached_smartess_cloud_evidence_record = CloudEvidenceRecord(path=source, payload=cloud)
    coordinator._cached_cloud_evidence_provider = coordinator.cloud_evidence_provider
    client = await hass_client_no_auth()

    # The second export also contains the first download URL in runtime values.
    previous_signature = None
    for _ in range(2):
        path = Path(await coordinator.async_export_support_package())
        url = coordinator.data.values["support_package_download_url"]
        assert "authSig=" in url
        assert (await client.get(url.split("?", 1)[0])).status == 401
        response = await client.get(url)
        assert response.status == 200
        downloaded = await response.read()
        assert downloaded == await hass.async_add_executor_job(path.read_bytes)
        with ZipFile(BytesIO(downloaded)) as archive:
            assert archive.testzip() is None
            for name in archive.namelist():
                text = archive.read(name).decode()
                for marker in credentials.values():
                    assert marker not in text, name
                if previous_signature:
                    assert previous_signature not in text, name
            exported = json.loads(archive.read("evidence/cloud_evidence.json"))
            assert exported == {"payload": {"session": {}, "words": [14080, 1, 6200]}}
            bundle = json.loads(archive.read("support_bundle.json"))
            assert bundle["runtime"]["values"]["runtime_inverter_probe_log"][0]["diagnostic"] == evidence
        previous_signature = url.split("authSig=", 1)[1]

    assert await hass.async_add_executor_job(source.read_text) == original
    assert cloud["payload"]["session"] == credentials
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
