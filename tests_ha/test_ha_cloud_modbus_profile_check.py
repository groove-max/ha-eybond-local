"""Real HA flow and coordinator boundaries for explicit protocol switching."""

import asyncio
from time import monotonic
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.data_entry_flow import FlowResultType

from test_ha_config_flow import collector_entry
from test_ha_cloud_learning_dispatch import dispatch_context
from custom_components.eybond_local.options_flow import EybondLocalOptionsFlow
from custom_components.eybond_local.support.shadow_learning.protocol_mismatch import (
    CloudModbusRead, CloudProtocolMismatchError,
)


READ = CloudModbusRead(1, 255, 5, 3, 4563, 2)
CANDIDATE = {
    "driver_key": "modbus_smg", "model_name": "Synthetic",
    "profile_name": "synthetic", "register_schema_name": "synthetic",
    "route": {"driver_key": "modbus_smg", "devcode": 1, "collector_addr": 255, "device_addr": 5},
    "comparisons": [{"key": "battery_voltage", "local": 27.1, "modbus": 27.2}],
}


def test_startup_cannot_bind_selected_driver_to_old_protocol_schema():
    from custom_components.eybond_local.runtime.coordinator.startup import CoordinatorStartupIdentityMixin
    from custom_components.eybond_local.metadata.effective_metadata_snapshot import build_effective_metadata_snapshot

    class Startup(CoordinatorStartupIdentityMixin):
        pass

    startup = Startup()
    startup.config_entry = SimpleNamespace(
        data={"detected_model": "PI30 4200", "detected_driver": "pi30"},
        options={"driver_hint": "smartess_local"},
    )
    # Also reject a mixed snapshot already stamped with the new owner by an
    # earlier version: profile/schema ownership, not just the label, matters.
    for owner in ("pi30", "smartess_local"):
        startup.effective_metadata_snapshot = build_effective_metadata_snapshot(
            effective_owner_key=owner, confidence="high",
            profile_name="pi30_ascii/models/pi30_max.json",
            register_schema_name="pi30_ascii/models/pi30_max.json",
        )
        assert startup._prime_startup_inverter_from_persisted_metadata() is None


@pytest.fixture
def review_flow(hass, collector_entry):
    flow = EybondLocalOptionsFlow(collector_entry)
    flow.hass = hass
    coordinator = SimpleNamespace(
        identified_inverter=SimpleNamespace(driver_key="pi30"),
        async_shadow_learning_start_blocker=AsyncMock(return_value=""),
        async_check_cloud_modbus_profile=AsyncMock(return_value={"candidates": [CANDIDATE]}),
    )
    flow._coordinator = lambda: coordinator
    flow._cloud_modbus_check_owner = coordinator
    flow._cloud_modbus_check_inverter = coordinator.identified_inverter
    flow._cloud_modbus_checked_at = monotonic()
    flow._shadow_learning_state = {
        "cloud_modbus_read": READ.to_mapping(),
        "discovery": {"status": "error", "reason": "control_discovery_cloud_register_seed_required"},
        "modbus_profile_check": {"candidates": [CANDIDATE]},
    }
    flow.async_step_shadow_learning_result = AsyncMock(return_value={"step_id": "shadow_learning_result"})
    return flow, coordinator


async def test_review_needs_explicit_choice_and_readings_confirmation(review_flow, collector_entry):
    flow, _ = review_flow
    before = (dict(collector_entry.data), dict(collector_entry.options))
    result = await flow.async_step_shadow_modbus_review()
    assert result["type"] == FlowResultType.FORM
    assert result["data_schema"]({})["driver"] == "keep"
    result = await flow.async_step_shadow_modbus_review({"driver": "modbus_smg"})
    assert result["errors"] == {"confirm_readings": "invalid_selection"}
    result = await flow.async_step_shadow_modbus_review({"driver": "srne_modbus", "confirm_readings": True})
    assert result["errors"] == {"driver": "invalid_selection"}
    assert (dict(collector_entry.data), dict(collector_entry.options)) == before
    result = await flow.async_step_shadow_modbus_review({"driver": "modbus_smg", "confirm_readings": True})
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert collector_entry.options["driver_hint"] == "modbus_smg"
    assert collector_entry.options["inverter_probe_route"] == CANDIDATE["route"]
    assert collector_entry.data["detected_driver"] == ""
    assert collector_entry.data["control_mode"] == "read_only"


@pytest.mark.parametrize("risk", ["uncertain", "high"])
def test_review_warning_matches_actual_default_selection(review_flow, risk):
    flow, _ = review_flow
    for enabled in (False, True):
        note = flow._control_discovery_status_note({"risk_level": risk, "enabled_by_default": enabled})
        assert ("selected by default" if enabled else "off by default") in note


async def test_0925_review_preserves_observed_route_and_requires_confirmation(review_flow, collector_entry):
    flow, _ = review_flow
    flow.hass.config_entries.async_update_entry(collector_entry, options={
        **collector_entry.options, "effective_metadata_snapshot": {"profile_name": "pi30_ascii/models/pi30_max.json"},
    })
    candidate = {**CANDIDATE, "driver_key": "smartess_local",
                 "profile_name": "smartess_local/models/0925.json",
                 "route": {**CANDIDATE["route"], "driver_key": "smartess_local"}}
    flow._shadow_learning_state["modbus_profile_check"]["candidates"] = [candidate]
    result = await flow.async_step_shadow_modbus_review({"driver": "smartess_local"})
    assert result["errors"] == {"confirm_readings": "invalid_selection"}
    assert "inverter_probe_route" not in collector_entry.options
    result = await flow.async_step_shadow_modbus_review({"driver": "smartess_local", "confirm_readings": True})
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert collector_entry.options["inverter_probe_route"] == candidate["route"]
    assert "effective_metadata_snapshot" not in collector_entry.options
    assert collector_entry.data["control_mode"] == "read_only"


async def test_non_pi30_owner_can_review_but_rebound_inverter_cannot_confirm(review_flow, collector_entry):
    flow, coordinator = review_flow
    coordinator.identified_inverter.driver_key = "must_pv_ph18"
    result = await flow.async_step_shadow_modbus_review()
    assert result["type"] == FlowResultType.FORM
    assert "PI30" not in result["description_placeholders"]["result"]
    coordinator.identified_inverter = SimpleNamespace(driver_key="must_pv_ph18")
    result = await flow.async_step_shadow_modbus_review({"driver": "modbus_smg", "confirm_readings": True})
    assert result["step_id"] == "shadow_learning_result"
    assert "inverter_probe_route" not in collector_entry.options


@pytest.mark.parametrize("blocker", ["cloud_learning_protection_held", "shadow_learning_restore_pending"])
async def test_recovery_and_protection_block_profile_switch(review_flow, collector_entry, blocker):
    flow, coordinator = review_flow
    coordinator.async_shadow_learning_start_blocker.return_value = blocker
    before = dict(collector_entry.options)
    result = await flow.async_step_shadow_modbus_review({"driver": "modbus_smg", "confirm_readings": True})
    assert result["step_id"] == "shadow_learning_result"
    assert dict(collector_entry.options) == before


async def test_stale_or_missing_compatibility_never_switches(review_flow, collector_entry):
    flow, _ = review_flow
    flow._cloud_modbus_checked_at = monotonic() - 301
    result = await flow.async_step_shadow_modbus_review({"driver": "modbus_smg", "confirm_readings": True})
    assert result["errors"] == {"driver": "invalid_selection"}
    assert "inverter_probe_route" not in collector_entry.options


async def test_mismatch_remains_actionable_after_restoration(review_flow):
    flow, coordinator = review_flow
    coordinator.async_stop_shadow_learning = AsyncMock(return_value={"restore_confirmed": True})
    assert await flow._async_control_discovery_retry_restore(coordinator)
    assert flow._cloud_modbus_check_available(restore_pending=False, protection_held=False)
    assert not flow._cloud_modbus_check_available(restore_pending=True, protection_held=False)
    assert "Modbus" in flow._control_discovery_error_detail()


async def test_typed_mismatch_survives_readiness_boundary(dispatch_context, monkeypatch, tmp_path):
    ctx = dispatch_context
    ctx.status.update(protocol_preparation_pending=True,
                      protocol_preparation_error="cloud_payload_modbus_requires_register_seed",
                      collector_identity_verified=True, cloud_modbus_read=READ.to_mapping())
    monkeypatch.setattr(ctx.coordinator, "_shadow_learning_process_running", lambda: True)
    with pytest.raises(CloudProtocolMismatchError) as caught:
        await ctx.coordinator._async_wait_for_shadow_learning_ready(trace_path=tmp_path, timeout_seconds=1)
    assert caught.value.cloud_read == READ


async def test_unknown_protocol_diagnostics_survive_readiness_failure(dispatch_context, monkeypatch, tmp_path):
    ctx = dispatch_context
    diagnostics = {"samples": [{"status": "unknown", "length": 12, "sha256": "synthetic"}]}
    ctx.status.update(protocol_preparation_pending=True, protocol_preparation_error="cloud_protocol_unrecognized",
                      collector_identity_verified=True, protocol_diagnostics=diagnostics)
    monkeypatch.setattr(ctx.coordinator, "_shadow_learning_process_running", lambda: True)
    with pytest.raises(CloudProtocolMismatchError) as caught:
        await ctx.coordinator._async_wait_for_shadow_learning_ready(trace_path=tmp_path, timeout_seconds=1)
    assert caught.value.cloud_read is None
    assert caught.value.evidence is None
    assert caught.value.diagnostics == diagnostics


async def test_unknown_protocol_and_probe_results_are_published_for_support(review_flow):
    flow, coordinator = review_flow
    diagnostics = {"samples": [{"status": "unknown", "length": 12, "sha256": "synthetic"}]}
    flow._shadow_learning_state["protocol_diagnostics"] = diagnostics
    coordinator.publish_shadow_learning_artifacts = Mock(return_value={})
    flow._publish_shadow_learning_artifacts(coordinator)
    published = coordinator.publish_shadow_learning_artifacts.call_args.kwargs["orchestration"]
    assert published["protocol_diagnostics"] == diagnostics
    assert "modbus_profile_check" in published


async def test_no_match_is_read_only_and_cancellation_propagates(review_flow):
    flow, coordinator = review_flow
    coordinator.async_check_cloud_modbus_profile.return_value = {"candidates": [], "attempts": []}
    await flow._async_run_cloud_modbus_check(coordinator)
    result = await flow.async_step_shadow_modbus_review()
    assert "current profile is unchanged" in result["description_placeholders"]["result"]
    coordinator.async_check_cloud_modbus_profile.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await flow._async_run_cloud_modbus_check(coordinator)


async def test_real_flow_manager_serves_progress_and_explicit_review(hass, collector_entry, review_flow, monkeypatch):
    seed_flow, coordinator = review_flow
    async def start_at_compatibility(self, user_input=None):
        return self.async_show_menu(step_id="init", menu_options=["shadow_modbus_check", "runtime"])
    monkeypatch.setattr(EybondLocalOptionsFlow, "async_step_init", start_at_compatibility)
    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert result["type"] == FlowResultType.MENU, result
    flow = hass.config_entries.options._progress[result["flow_id"]]
    flow._coordinator = lambda: coordinator
    flow._shadow_learning_state = dict(seed_flow._shadow_learning_state)
    assert await flow._async_cloud_modbus_check_allowed(coordinator)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"next_step_id": "shadow_modbus_check"},
    )
    # Real HA can consume an already completed task immediately; either path
    # must reach the review without a placeholder/unknown handler.
    if result["type"] == FlowResultType.SHOW_PROGRESS:
        await hass.async_block_till_done()
        result = await hass.config_entries.options.async_configure(result["flow_id"])
    if result["type"] == FlowResultType.SHOW_PROGRESS_DONE:
        result = await hass.config_entries.options.async_configure(result["flow_id"])
    assert result["step_id"] == "shadow_modbus_review"
    assert "inverter_probe_route" not in collector_entry.options
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"driver": "modbus_smg", "confirm_readings": True},
    )
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert collector_entry.options["inverter_probe_route"] == CANDIDATE["route"]


async def test_check_serializes_polling_and_cancellation_releases_guard(monkeypatch):
    from custom_components.eybond_local.runtime.coordinator.diagnostics import CoordinatorDiagnosticsMixin
    from custom_components.eybond_local.support.diagnostic_runner import DiagnosticSingleFlight
    from custom_components.eybond_local.runtime import protocol_check

    coordinator = CoordinatorDiagnosticsMixin()
    coordinator._runtime_operation_lock = asyncio.Lock()
    coordinator._diagnostic_flight = DiagnosticSingleFlight()
    coordinator._diagnostic_active = False
    coordinator._shutdown_complete = False
    coordinator._cloud_tool_preparation_task = None
    coordinator.async_shadow_learning_start_blocker = AsyncMock(return_value="")
    coordinator._shadow_learning_process_running = lambda: False
    coordinator._proxy_capture_process_running = lambda: False
    coordinator.identified_inverter = SimpleNamespace(driver_key="pi30")
    coordinator._diagnostic_link_transport = lambda: SimpleNamespace(connected=True)
    coordinator.data = SimpleNamespace(runtime_values=lambda: {"battery_voltage": 27.1})
    async def check(*args):
        assert coordinator._runtime_operation_lock.locked()
        assert coordinator._diagnostic_active
        raise asyncio.CancelledError
    monkeypatch.setattr(protocol_check, "check_protocol_profiles", check)
    with pytest.raises(asyncio.CancelledError):
        await coordinator.async_check_cloud_modbus_profile(READ.to_mapping())
    assert not coordinator._runtime_operation_lock.locked()
    assert not coordinator._diagnostic_active
    assert not coordinator._diagnostic_flight.running


@pytest.mark.parametrize("hint", ["modbus_smg", "pi30", "auto"])
async def test_runtime_edits_preserve_route_only_for_same_driver(review_flow, collector_entry, hint):
    flow, _ = review_flow
    flow.hass.config_entries.async_update_entry(collector_entry, options={
        **collector_entry.options, "driver_hint": "modbus_smg",
        "inverter_probe_route": CANDIDATE["route"],
    })
    flow._build_runtime_options_from_flat_input = lambda _: {"driver_hint": hint, "poll_interval": 30}
    result = flow._async_commit_runtime_options({})
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert ("inverter_probe_route" in collector_entry.options) is (hint == "modbus_smg")
