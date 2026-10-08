"""PN-less repair through real HA forms/menus, registry handoff and reload.

Only the device identity result is scripted; no fake Home Assistant modules.
The transaction/transport's real-socket coverage lives in the callback suites.
"""
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.connection.callback_identity import (
    CallbackIdentityOutcome, SilentSessionBootstrapOffer,
)
from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.passive_discovery import get_callback_session_registry
from synthetic import (
    SYNTHETIC_BROADCAST, SYNTHETIC_COLLECTOR_IP,
    SYNTHETIC_COLLECTOR_PN, SYNTHETIC_SERVER_IP,
)

TX = "custom_components.eybond_local.connection.admission_transaction.async_run_callback_identity_transaction"
SESSION = "listener-synthetic-repair"


def settings():
    return {
        "server_ip": SYNTHETIC_SERVER_IP,
        "collector_ip": SYNTHETIC_COLLECTOR_IP,
        "driver_hint": "auto",
        "advanced_connection": {
            "tcp_port": 502, "udp_port": 58899,
            "discovery_target": SYNTHETIC_BROADCAST,
            "discovery_interval": 3, "heartbeat_interval": 60,
        },
    }


def silent():
    return CallbackIdentityOutcome(
        result="callback_session_silent",
        silent_bootstrap_offer=SilentSessionBootstrapOffer(SESSION),
    )


@pytest.fixture
async def pending_entry(hass, fake_runtime):
    entry = MockConfigEntry(
        domain=DOMAIN, title="EyeBond Setup Pending", version=3,
        unique_id="synthetic-legacy-entry",
        data={
            "connection_type": "eybond", "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": "", "tcp_port": 8899, "udp_port": 58899,
            "driver_hint": "auto", "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external", "control_mode": "read_only",
        },
        options={"poll_interval": 30, "tcp_port": 18899},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    yield entry
    await hass.async_block_till_done()
    if hass.config_entries.async_get_entry(entry.entry_id):
        await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def start_repair(hass, entry):
    first = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "reconfigure", "entry_id": entry.entry_id},
    )
    return await hass.config_entries.flow.async_configure(first["flow_id"], settings())


@pytest.mark.parametrize("interface_present", [True, False])
async def test_repair_preserves_available_saved_interface(hass, interface_present):
    """Repair must not silently advertise the host's default-route interface."""
    saved_ip = "198.51.100.10"
    interfaces = [{"name": "eth0", "ip": SYNTHETIC_SERVER_IP, "label": "Default interface",
                   "network": "192.0.2.0/24", "broadcast": SYNTHETIC_BROADCAST}]
    if interface_present:
        interfaces.append({"name": "eth1", "ip": saved_ip, "label": "Collector interface",
                           "network": "198.51.100.0/24", "broadcast": "198.51.100.255"})
    pending_entry = MockConfigEntry(
        domain=DOMAIN, title="EyeBond Setup Pending", version=5,
        data={"connection_type": "eybond", "connection_mode": "manual",
              "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
              "collector_pn": "", "tcp_port": 8899, "udp_port": 58899},
        options={"server_ip": saved_ip},
    )
    pending_entry.add_to_hass(hass)
    with patch("custom_components.eybond_local.network_interfaces.get_ipv4_interfaces",
               return_value=interfaces):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "reconfigure", "entry_id": pending_entry.entry_id},
        )
    expected_ip = saved_ip if interface_present else SYNTHETIC_SERVER_IP
    defaults = {str(key): key.default() for key in result["data_schema"].schema
                if str(key) == "server_ip"}
    assert defaults["server_ip"] == expected_ip
    submitted = settings()
    submitted["server_ip"] = defaults["server_ip"]
    identity = AsyncMock(return_value=CallbackIdentityOutcome(result="callback_timeout"))
    with patch(TX, identity):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)
    assert identity.await_args.args[1].server_ip == expected_ip
    assert identity.await_args.args[1].target_ip == SYNTHETIC_COLLECTOR_IP
    if interface_present:
        result = await choose(hass, result, "manual_edit_settings")
        submitted["server_ip"] = SYNTHETIC_SERVER_IP
        with patch(TX, identity):
            result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)
        assert identity.await_args.args[1].server_ip == SYNTHETIC_SERVER_IP
        result = await choose(hass, result, "manual_edit_settings")
        defaults = {str(key): key.default() for key in result["data_schema"].schema
                    if str(key) == "server_ip"}
        assert defaults["server_ip"] == SYNTHETIC_SERVER_IP
    hass.config_entries.flow.async_abort(result["flow_id"])


async def choose(hass, result, action):
    return await hass.config_entries.flow.async_configure(
        result["flow_id"], {"next_step_id": action},
    )


def certified(hass, protocol):
    registry = get_callback_session_registry(hass)
    source = "fc2_parameter_2" if protocol == "eybond_framed" else "at_dtupn"
    registry.sessions_source = lambda: ({
        "session_id": SESSION, "collector_pn": SYNTHETIC_COLLECTOR_PN,
        "state": "routed_framed" if protocol == "eybond_framed" else "routed_at",
        "collector_identity_source": source, "listener_port": 502,
        "protocol_shape": protocol,
        "raw": {"session_id": SESSION, "protocol_shape": protocol},
    },)
    owner = "callback_verification:synthetic-repair"
    registry.claim_session(owner, session_id=SESSION)
    assert registry.promote_claim_to_full_pn(owner, SYNTHETIC_COLLECTOR_PN)
    assert registry.prepare_handoff(owner, SYNTHETIC_COLLECTOR_PN)
    return CallbackIdentityOutcome(
        result="", collector_pn=SYNTHETIC_COLLECTOR_PN, session_id=SESSION,
        session_protocol=protocol, identity_source=source, handoff_owner=owner,
    )


@pytest.mark.parametrize("protocol,action", [
    ("eybond_framed", "manual_bootstrap_framed"),
    ("at_text", "manual_bootstrap_at"),
])
async def test_silent_repair_updates_same_entry_and_reloads(hass, pending_entry, protocol, action):
    calls = []
    original_entries = {entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN)}

    async def identity(_hass, request):
        calls.append(request)
        return silent() if len(calls) == 1 else certified(hass, protocol)

    with patch(TX, side_effect=identity):
        result = await start_repair(hass, pending_entry)
        assert result["step_id"] == "reconfigure_confirm"
        assert action in result["menu_options"]
        assert "manual_save" not in result["menu_options"]
        assert "manual_recovery_verify" not in result["menu_options"]
        assert "connected" in result["description_placeholders"]["failure_explanation"].lower()
        result = await choose(hass, result, action)
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    await hass.async_block_till_done()
    assert {entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN)} == original_entries
    assert pending_entry.data["collector_pn"] == SYNTHETIC_COLLECTOR_PN
    assert pending_entry.data["connection_strategy"] == "callback_on_demand"
    assert pending_entry.data["control_mode"] == "read_only"
    assert "connection_strategy_evidence" not in pending_entry.data
    assert "recovery_contract" not in pending_entry.data
    assert pending_entry.data["tcp_port"] == pending_entry.options["tcp_port"] == 502
    assert pending_entry.options["poll_interval"] == 30
    assert calls[0].bootstrap_probe is None
    assert calls[1].bootstrap_probe.protocol == protocol
    assert calls[1].bootstrap_probe.session_id == SESSION
    assert calls[1].bootstrap_probe.source == "explicit_user_selection"


async def test_failed_silent_repair_does_not_guess_another_wire(hass, pending_entry):
    original = dict(pending_entry.data)
    identity = AsyncMock(side_effect=[silent(), CallbackIdentityOutcome(
        result="callback_silent_session_unavailable",
    ), silent()])
    with patch(TX, identity):
        result = await start_repair(hass, pending_entry)
        result = await choose(hass, result, "manual_bootstrap_framed")
        assert result["step_id"] == "reconfigure_confirm"
        assert "manual_bootstrap_at" not in result["menu_options"]
        assert identity.await_count == 2
        result = await choose(hass, result, "manual_probe_again")
        assert "manual_bootstrap_at" in result["menu_options"]
        assert identity.await_args.args[1].bootstrap_probe is None
    assert pending_entry.data == original
    hass.config_entries.flow.async_abort(result["flow_id"])


@pytest.mark.parametrize("reason", ["callback_session_silent", "callback_silent_session_unavailable", "callback_timeout"])
async def test_closed_or_missing_silent_session_keeps_repair_retry_menu(hass, pending_entry, reason):
    """No live offer means no protocol query, not a dead-end raw-error form."""
    original = dict(pending_entry.data)
    identity = AsyncMock(side_effect=[CallbackIdentityOutcome(result=reason), silent()])
    with patch(TX, identity):
        result = await start_repair(hass, pending_entry)
        assert result["type"] is FlowResultType.MENU
        assert result["step_id"] == "reconfigure_confirm"
        assert result["menu_options"] == ["manual_probe_again", "manual_edit_settings"]
        result = await choose(hass, result, "manual_probe_again")
        assert "manual_bootstrap_framed" in result["menu_options"]
        assert "manual_bootstrap_at" in result["menu_options"]
        assert identity.await_count == 2
        assert identity.await_args.args[1].bootstrap_probe is None
    assert pending_entry.data == original
    hass.config_entries.flow.async_abort(result["flow_id"])


async def test_edit_and_cancel_discard_silent_offer(hass, pending_entry):
    identity = AsyncMock(return_value=silent())
    with patch(TX, identity):
        result = await start_repair(hass, pending_entry)
        flow = hass.config_entries.flow._progress[result["flow_id"]]
        result = await choose(hass, result, "manual_edit_settings")
        assert result["step_id"] == "reconfigure"
        assert flow._callback_continuation.silent_bootstrap_offer is None
        assert identity.await_count == 1
        hass.config_entries.flow.async_abort(result["flow_id"])
    assert pending_entry.data["collector_pn"] == ""


@pytest.mark.parametrize("deleted", [False, True])
async def test_stale_repair_does_not_touch_a_changed_entry(hass, pending_entry, deleted):
    identity = AsyncMock(return_value=silent())
    with patch(TX, identity):
        result = await start_repair(hass, pending_entry)
        if deleted:
            await hass.config_entries.async_remove(pending_entry.entry_id)
        else:
            hass.config_entries.async_update_entry(
                pending_entry, data={**pending_entry.data, "collector_pn": SYNTHETIC_COLLECTOR_PN},
            )
        result = await choose(hass, result, "manual_bootstrap_at")
        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == (
            "reconfigure_entry_missing" if deleted else "reconfigure_not_required"
        )
        assert identity.await_count == 1


async def test_repair_collision_releases_certified_owner(hass, pending_entry):
    original = dict(pending_entry.data)
    other = MockConfigEntry(
        domain=DOMAIN, title="Existing collector", version=5,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data={"collector_pn": SYNTHETIC_COLLECTOR_PN},
    )
    other.add_to_hass(hass)
    calls = 0

    async def identity(_hass, request):
        nonlocal calls
        calls += 1
        return silent() if calls == 1 else certified(hass, "at_text")

    with patch(TX, side_effect=identity):
        result = await start_repair(hass, pending_entry)
        result = await choose(hass, result, "manual_bootstrap_at")
    assert result["reason"] == "already_configured"
    assert pending_entry.data == original
    registry = get_callback_session_registry(hass)
    assert registry.claimed_session_id("callback_verification:synthetic-repair") == ""


async def test_entry_changed_during_probe_is_not_overwritten(hass, pending_entry):
    calls = 0

    async def identity(_hass, request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return silent()
        outcome = certified(hass, "at_text")
        hass.config_entries.async_update_entry(
            pending_entry, data={**pending_entry.data, "collector_pn": SYNTHETIC_COLLECTOR_PN},
        )
        return outcome

    with patch(TX, side_effect=identity):
        result = await start_repair(hass, pending_entry)
        result = await choose(hass, result, "manual_bootstrap_at")
    assert result["reason"] == "reconfigure_not_required"
    assert pending_entry.data["tcp_port"] == 8899
    assert pending_entry.options["tcp_port"] == 18899
    registry = get_callback_session_registry(hass)
    assert registry.claimed_session_id("callback_verification:synthetic-repair") == ""
