"""Native HA setup carries explicit routes without accepting UDP as identity."""
from unittest.mock import AsyncMock, patch

from homeassistant.data_entry_flow import FlowResultType

from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.onboarding.detection import DiscoveryTarget
from synthetic import SYNTHETIC_BROADCAST


async def _advanced_setup(hass):
    from custom_components.eybond_local.flows.config.scan import CollectorScanFlowMixin
    initial = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    flow_id = initial["flow_id"]
    flow = hass.config_entries.flow._progress[flow_id]
    with patch.object(CollectorScanFlowMixin, "_async_do_scan", new=AsyncMock()):
        await hass.config_entries.flow.async_configure(flow_id, {"next_step_id": "auto"})
        await flow._scan_task
        result = await hass.config_entries.flow.async_configure(flow_id)
    assert result["step_id"] == "scan_results"
    menu = await hass.config_entries.flow.async_configure(flow_id, {"result_key": "action:advanced_setup"})
    assert "scan_targets" in menu["menu_options"]
    return flow_id, flow


async def test_known_targets_form_and_scan_use_same_inventory_path(hass):
    flow_id, flow = await _advanced_setup(hass)
    form = await hass.config_entries.flow.async_configure(flow_id, {"next_step_id": "scan_targets"})
    assert form["step_id"] == "scan_targets"
    invalid = await hass.config_entries.flow.async_configure(flow_id, {"known_collector_ips": "198.51.100.0/24"})
    assert invalid["errors"] == {"known_collector_ips": "invalid_collector_targets"}
    assert flow._scan_task is None or flow._scan_task.done()
    detector = AsyncMock()
    detector.async_scan.return_value = ()
    with patch("custom_components.eybond_local.flows.config.scan.create_onboarding_manager", return_value=detector), \
         patch.object(flow, "_async_passive_scan_results", new=AsyncMock(return_value=())), \
         patch.object(flow, "_shared_registry_scan_results", return_value=()):
        progress = await hass.config_entries.flow.async_configure(flow_id, {
            "known_collector_ips": "198.51.100.7,203.0.113.9,198.51.100.7",
        })
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        await flow._scan_task
    assert detector.async_scan.call_args.kwargs["discovery_targets"] == (
        DiscoveryTarget(SYNTHETIC_BROADCAST, "broadcast"),
        DiscoveryTarget("198.51.100.7", "known_ip"),
        DiscoveryTarget("203.0.113.9", "known_ip"),
    )
    assert flow._scan_known_collector_ips == ("198.51.100.7", "203.0.113.9")
    assert not hass.config_entries.async_entries(DOMAIN)
    assert not flow._autodetect_results
    hass.config_entries.flow.async_abort(flow_id)


async def test_manual_flow_keeps_local_listener_separate_from_advertised_callback(hass):
    from custom_components.eybond_local.connection.callback_identity import CallbackIdentityOutcome
    from synthetic import SYNTHETIC_SERVER_IP

    flow_id, flow = await _advanced_setup(hass)
    form = await hass.config_entries.flow.async_configure(flow_id, {"next_step_id": "manual"})
    assert form["step_id"] == "manual"
    identity = AsyncMock(return_value=CallbackIdentityOutcome(result="callback_timeout"))
    with patch("custom_components.eybond_local.connection.admission_transaction.async_run_callback_identity_transaction",
               new=identity):
        result = await hass.config_entries.flow.async_configure(flow_id, {
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": "198.51.100.7",
            "driver_hint": "auto",
            "connection_strategy": "callback_on_demand",
            "advanced_connection": {
                "tcp_port": 8899,
                "udp_port": 58899,
                "advertised_server_ip": "203.0.113.10",
                "advertised_tcp_port": "18899",
                "discovery_target": SYNTHETIC_BROADCAST,
                "discovery_interval": 3,
                "heartbeat_interval": 60,
            },
        })
    assert result["type"] is not FlowResultType.CREATE_ENTRY
    request = identity.call_args.args[1]
    assert request.server_ip == SYNTHETIC_SERVER_IP
    assert request.tcp_port == 8899
    assert request.advertised_server_ip == "203.0.113.10"
    assert request.advertised_tcp_port == 18899
    assert request.target_ip == "198.51.100.7"
    hass.config_entries.flow.async_abort(flow_id)
