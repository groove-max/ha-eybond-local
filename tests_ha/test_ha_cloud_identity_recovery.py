"""Cloud identity and endpoint recovery through the real HA options lifecycle."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from homeassistant.data_entry_flow import FlowResultType

from custom_components.eybond_local.models import CollectorInfo, RuntimeSnapshot
from custom_components.eybond_local.support.shadow_learning.session import (
    build_shadow_learning_session_state,
)
from test_ha_config_flow import collector_entry
from synthetic import SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP


def local_endpoint():
    return f"{SYNTHETIC_SERVER_IP},18899,TCP"


@pytest.mark.parametrize("family,host,provider,source", [
    ("valuecloud_at", "iot.eybond.com", "valuecloud", "valuecloud"),
    ("smartvalue_at", "m2m.eybond.com", "smartvalue", ""),
    ("smartess_at", "dtu_ess.eybond.com", "smartess", "smartess"),
])
async def test_local_redirect_does_not_change_cloud_api_picker(
    hass, collector_entry, fake_runtime, family, host, provider, source,
):
    hass.config_entries.async_update_entry(collector_entry, data={
        **collector_entry.data,
        "collector_cloud_family": "smartess_at",
        "control_mode": "full",
        "collector_original_server_endpoint": f"{host},18899,TCP",
        "collector_original_server_endpoint_profile_key": family,
        "collector_original_server_endpoint_source": "runtime_observed",
    })
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    coordinator.data = RuntimeSnapshot(
        collector=CollectorInfo(
            collector_pn=SYNTHETIC_COLLECTOR_PN,
            collector_server_endpoint=local_endpoint(),
            collector_cloud_family="smartess_at",
            collector_cloud_family_source="explicit_endpoint_port",
            collector_cloud_family_confidence="medium",
        ),
        values={
            "collector_server_endpoint": local_endpoint(),
            "collector_cloud_family": "smartess_at",
            "collector_cloud_family_source": "explicit_endpoint_port",
            "collector_cloud_family_confidence": "medium",
        },
    )
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    try:
        flow = hass.config_entries.options._progress[first["flow_id"]]
        assert coordinator.collector_cloud_family == family
        assert coordinator.cloud_evidence_provider == provider
        assert coordinator.collector_session_protocol == ""
        assert await coordinator.async_shadow_learning_start_blocker() == ""
        if source:
            result = await flow.async_step_shadow_learning({"learning_method": "active_correlation"})
            # Providers with a single source advance directly to consent.
            assert flow._control_discovery_default_learning_source(coordinator, "active_correlation") == source
            assert source in {item.source_id for item in flow._control_discovery_learning_sources(coordinator)}
            assert result["type"] is FlowResultType.FORM
            assert result["step_id"] in {"shadow_learning_source", "shadow_learning_consent"}
        else:
            # A preserved family without a registered active engine must not
            # acquire another provider's credentials/controls through its port.
            assert not any(method.method_id == "active_correlation" for method in flow._control_discovery_learning_methods(coordinator))
        await coordinator._async_remember_runtime_identity(coordinator.data)
        assert collector_entry.data["collector_cloud_family"] == family
        snapshot = await coordinator._async_prepare_runtime_snapshot_profile(coordinator.data)
        assert snapshot.collector.collector_cloud_family == family
        assert snapshot.values["collector_cloud_family"] == family
        assert snapshot.collector.collector_cloud_family_source == "endpoint_host"
        assert snapshot.values["collector_cloud_family_source"] == "endpoint_host"
        assert snapshot.collector.collector_cloud_family_confidence == "high"
        assert snapshot.values["collector_cloud_family_confidence"] == "high"
        assert coordinator.collector_session_protocol == ""
    finally:
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)


@pytest.mark.parametrize("status", ["restoring", "restore_failed"])
async def test_pending_restore_blocks_learning_and_allows_explicit_recovery(
    hass, collector_entry, fake_runtime, monkeypatch, status,
):
    hass.config_entries.async_update_entry(collector_entry, data={
        **collector_entry.data,
        "collector_cloud_family": "valuecloud_at", "control_mode": "full",
        "collector_original_server_endpoint": "iot.eybond.com,18899,TCP",
        "collector_original_server_endpoint_profile_key": "valuecloud_at",
    })
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    coordinator = collector_entry.runtime_data
    runtime = coordinator._runtime
    original = "iot.eybond.com,18899,TCP"
    state = build_shadow_learning_session_state(
        entry_id=collector_entry.entry_id,
        route_owner_id=f"shadow_learning:{collector_entry.entry_id}_recovery",
        collector_pn=SYNTHETIC_COLLECTOR_PN, trace_path="",
        original_endpoint=original, proxy_endpoint=local_endpoint(),
        upstream_endpoint=original, restore_required=True,
        started_at="2026-10-05T08:00:00+00:00",
        updated_at="2026-10-05T08:01:00+00:00", status=status,
    )
    await coordinator._async_save_shadow_learning_session_state(state)
    # Model a coordinator which has to load its recovery record from disk.
    coordinator._cached_shadow_learning_session_state = None
    coordinator._shadow_learning_session_state_loaded = False
    coordinator.data = replace(coordinator.data, values={})
    capture = AsyncMock()
    monkeypatch.setattr(runtime, "async_capture_support_evidence", capture)
    write = AsyncMock(side_effect=TimeoutError())
    monkeypatch.setattr(runtime, "async_set_collector_server_endpoint", write)
    monkeypatch.setattr(runtime, "async_get_collector_server_endpoint_state", AsyncMock(
        return_value={"current_endpoint": original},
    ))
    first = await hass.config_entries.options.async_init(collector_entry.entry_id)
    try:
        assert await coordinator.async_shadow_learning_start_blocker() == "shadow_learning_restore_pending"
        with pytest.raises(RuntimeError, match="^shadow_learning_restore_pending$"):
            await coordinator.async_start_shadow_learning()
        write.assert_not_awaited()
        capture.assert_not_awaited()
        flow = hass.config_entries.options._progress[first["flow_id"]]
        result = await flow.async_step_shadow_learning()
        assert result["step_id"] == "shadow_learning_result"
        actions = result["data_schema"].schema["result_action"].config["options"]
        assert [item["value"] for item in actions] == ["restore_connection", "create_support_package", "done"]
        result = await flow.async_step_shadow_learning_result({"result_action": "restore_connection"})
        assert result["errors"] == {}
        assert coordinator.shadow_learning_restore_pending
        assert coordinator._cached_shadow_learning_session_state.status == "restore_failed"
        assert "could not confirm" in result["description_placeholders"]["control_discovery_hint"]
        write.side_effect = None
        write.return_value = {"readback_endpoint": original}
        result = await flow.async_step_shadow_learning_result({"result_action": "restore_connection"})
        assert result["errors"] == {}
        assert not coordinator.shadow_learning_restore_pending
        assert coordinator._cached_shadow_learning_session_state is None
        assert await coordinator.async_shadow_learning_start_blocker() == ""
        assert "was restored" in result["description_placeholders"]["control_discovery_hint"]
        assert all(call.args == (original,) for call in write.await_args_list)
        capture.assert_not_awaited()
    finally:
        # Leave the genuine persisted transaction finalized even on assertion failure.
        write.side_effect = None
        write.return_value = {"readback_endpoint": original}
        hass.config_entries.options.async_abort(first["flow_id"])
        await hass.config_entries.async_unload(collector_entry.entry_id)
