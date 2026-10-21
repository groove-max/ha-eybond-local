"""PI30 preparation through real HA flow/readiness/dispatch boundaries.

Network/device evidence is synthetic; these are not hardware qualification.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from test_ha_cloud_learning_dispatch import dispatch_context, dispatch
from test_ha_config_flow import collector_entry


@pytest.mark.parametrize("status_change", [
    {"protocol_preparation_pending": True},
    {"protocol_preparation_error": "cloud_payload_modbus_requires_register_seed"},
    {"protocol_preparation_pending": None},
    {"protocol_preparation_error": None},
])
async def test_pi30_unprepared_route_cannot_dispatch_cloud_controls(dispatch_context, status_change):
    ctx = dispatch_context
    ctx.status.update(status_change)
    action = Mock()
    with pytest.raises(RuntimeError):
        await dispatch(ctx, action)
    action.assert_not_called()
    status = ctx.coordinator.shadow_learning_runtime.view.route_status
    assert status.protocol_preparation_pending or status.protocol_preparation_error


async def test_preparation_mismatch_is_not_reported_as_network_failure(dispatch_context, monkeypatch):
    ctx = dispatch_context
    ctx.status["protocol_preparation_pending"] = True
    ctx.status["protocol_preparation_error"] = "cloud_payload_modbus_requires_register_seed"
    monkeypatch.setattr(ctx.coordinator, "_shadow_learning_process_running", lambda: True)
    with pytest.raises(RuntimeError, match="shadow_learning_protocol_preparation_failed:cloud_payload_modbus"):
        await ctx.coordinator._async_wait_for_shadow_learning_ready(
            trace_path=Path("synthetic"), timeout_seconds=1,
        )


async def test_pi30_flow_preview_offers_protected_preparation_from_real_local_seed(hass, monkeypatch):
    from custom_components.eybond_local.flows.options.shadow_runtime import ShadowLearningRuntimeMixin
    from custom_components.eybond_local.flows.options import shadow_runtime
    from custom_components.eybond_local.models import DetectedInverter, ProbeTarget
    from custom_components.eybond_local.support.shadow_learning.runtime import ShadowLearningRouteStatus

    flow = ShadowLearningRuntimeMixin()
    flow.hass = hass
    flow._config_entry = SimpleNamespace(entry_id="synthetic")
    flow._shadow_learning_state = {}
    capture = AsyncMock(return_value={
        "capture_kind": "pi30_ascii_dump", "responses": {"QFLAG": "EaDx"},
    })
    flow._shadow_learning_runtime = lambda _: SimpleNamespace(async_capture_support_evidence=capture)
    flow._shadow_learning_route_status = lambda _: ShadowLearningRouteStatus()
    monkeypatch.setattr(shadow_runtime, "read_available_memory_mib", lambda: 4096)
    coordinator = SimpleNamespace(
        data=SimpleNamespace(connected=True, values={}),
        smartess_collector_pn="SYNTHETIC-PN", collector_cloud_family="legacy_binary",
        collector_cloud_profile_key="", collector_cloud_profile_label="",
        collector_cloud_profile_source="endpoint_host", collector_cloud_profile_confidence="low",
        collector_callback_target_endpoint="", effective_profile_name="pi30_ascii/models/pi30_max.json",
        effective_register_schema_name="pi30_ascii/models/pi30_max.json",
        shadow_learning_effective_metadata={
            "effective_owner_key": "pi30", "protocol_family": "pi30",
            "profile_name": "pi30_ascii/models/pi30_max.json",
            "register_schema_name": "pi30_ascii/models/pi30_max.json",
        },
        identified_inverter=DetectedInverter("pi30", "pi30", "synthetic", "", ProbeTarget(0x994, 255, 1)),
    )
    result = await flow._build_shadow_learning_preflight_snapshot(coordinator)
    assert result["can_start"]
    assert result["protocol_adapter_key"] == "pi30_pending_cloud_proof"
    assert not result["shadow_session_ready"]
    coordinator.identified_inverter = None
    result = await flow._build_shadow_learning_preflight_snapshot(coordinator)
    assert not result["can_start"]
    assert "missing_register_seed" in result["blockers"]


@pytest.mark.parametrize(("internal", "public"), [
    ("shadow_learning_cloud_protocol_not_confirmed", "control_discovery_cloud_protocol_unconfirmed"),
    ("shadow_learning_protocol_preparation_failed:cloud_payload_modbus_requires_register_seed",
     "control_discovery_cloud_register_seed_required"),
])
async def test_preparation_failure_has_closed_user_facing_reason(internal, public):
    from custom_components.eybond_local.flows.options.shadow_run import ShadowLearningRunMixin

    assert ShadowLearningRunMixin._control_discovery_failure_reason(RuntimeError(internal)) == public


def test_learning_failure_location_does_not_expose_exception_or_locals():
    from custom_components.eybond_local.flows.options.shadow_run import _learning_failure_location

    secret = "https://provider.invalid/?password=synthetic-secret"
    code = compile("raise RuntimeError(secret)", "/private/custom_components/eybond_local/synthetic.py", "exec")
    try:
        exec(code, {"secret": secret})
    except RuntimeError as exc:
        assert _learning_failure_location(exc) == "synthetic.py:<module>:1"
    assert _learning_failure_location(RuntimeError(secret)) == "unknown"


def test_learning_management_failure_preserves_request_not_secret():
    from custom_components.eybond_local.collector.management import CollectorManagementTransportError
    from custom_components.eybond_local.flows.options.shadow_run import _learning_management_failure

    secret = "signed-url-password-must-not-be-persisted"
    error = CollectorManagementTransportError(secret, query_parameter=21)
    error.__cause__ = ConnectionError(secret)
    result = _learning_management_failure(error)
    assert secret not in str(result)
    assert result == {"management_failure": {
        "error_code": "ConnectionError",
        "failed_request": {"protocol": "eybond_framed", "function": 2, "parameter": 21},
        "call_path": [],
    }}
    assert _learning_management_failure(RuntimeError(secret)) == {}
