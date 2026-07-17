"""The silent-callback acceptance path through the REAL Home Assistant lifecycle.

Everything Home Assistant owns is genuine here: the flow manager starts the
config flow, ``async_setup`` runs the real integration setup (which completes
the ownership handoff itself), the real coordinator builds the real runtime,
confirmed live evidence is persisted through the real config-entry update
path, and unload/reload go through ``hass.config_entries``. Only external
boundaries are faked: the collector is the scripted fake device over REAL
loopback sockets (``socket_enabled``), entity platform forwarding is trimmed,
and the host-network/local-IP probes are stubbed like the rest of this suite.

Deliberately NOT called anywhere in this test:
``_register_entry_callback_session_claim``,
``_persist_confirmed_session_protocol_from_runtime``,
``create_runtime_manager`` -- they all run only where production runs them.
"""

from __future__ import annotations

import asyncio
import dataclasses
import socket
from dataclasses import replace
from pathlib import Path
import sys

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[0]
for _path in (str(REPO_ROOT), str(HERE), str(REPO_ROOT / "tests" / "helpers")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from custom_components.eybond_local.const import DOMAIN  # noqa: E402

from synthetic import SYNTHETIC_SERVER_IP  # noqa: E402

# Synthetic identity only (18 chars; heartbeat carries the 14-char prefix).
FULL_PN = "V001020SYN62344022"

_LIFECYCLE_TIMEOUT = 120.0


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.mark.timeout(180)
async def test_silent_callback_full_ha_lifecycle(
    hass: HomeAssistant, socket_enabled
) -> None:
    from unittest.mock import patch

    from fake_collector import FakeCollectorService
    from fake_collector_lib import CollectorProfile, resolve_scenario

    import custom_components.eybond_local as integration
    from custom_components.eybond_local import config_flow as config_flow_module
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.onboarding.timeouts import (
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
    )

    tcp_port = _free_tcp_port()
    service = FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=0,
        tcp_bind_ip="127.0.0.1",
        heartbeat_interval=1.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            set_29_mode="reboot_silent",
            first_heartbeat_delay=3600.0,  # FULLY silent from first contact
            pi30_mode="success",
        ),
    )
    await service.start()
    udp_port = int(service._udp_transport.get_extra_info("sockname")[1])

    fast_policy = replace(
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        inbound_strong_identity_timeout=5.0,
        inbound_restart_disconnect_timeout=5.0,
        inbound_reconnect_timeout=1.5,
        callback_recovery_session_wait=8.0,
        callback_causality_lease_wait=3.0,
        callback_identity_session_wait=6.0,
    )

    entry = None
    boot = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond boot",
        unique_id="collector:E5000099990003",
        data={
            "connection_type": "eybond",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": "192.0.2.55",
            "collector_pn": "E5000099990003",
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
        },
        options={},
    )
    injected_listener = None
    try:
        loopback_interfaces = [
            {
                "name": "lo",
                "ip": "127.0.0.1",
                "label": "lo - 127.0.0.1",
                "network": "127.0.0.0/8",
                "broadcast": "127.255.255.255",
            }
        ]
        async def _empty_scan(self) -> None:
            # The network auto-scan is an external boundary (like the fake
            # device): stub the WORK, not the transitions. Every step still
            # routes through the real flow manager; auto-scan simply finds
            # nothing, so the flow lands on scan_results -> advanced -> manual.
            self._autodetect_results = {}

        with patch.object(integration, "PLATFORMS", ()), patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="127.0.0.1",
        ), patch(
            "custom_components.eybond_local.config_flow._get_ipv4_interfaces",
            return_value=loopback_interfaces,
        ), patch(
            "custom_components.eybond_local.config_flow._get_local_ip",
            return_value="127.0.0.1",
        ), patch.object(
            config_flow_module.EybondLocalConfigFlow, "_async_do_scan", _empty_scan
        ), patch.object(
            config_flow_module, "_ONBOARDING_TIMEOUT_POLICY", fast_policy
        ), patch(
            "custom_components.eybond_local.onboarding.callback_identity."
            "DEFAULT_ONBOARDING_TIMEOUT_POLICY",
            fast_policy,
        ):
            # Boot the domain services (ownership registry + passive
            # discovery service). The passive listener binds deployment ports
            # in production; the OS/network boundary here is the TEST port, so
            # the service observes the same shared listener the flow and the
            # runtime use.
            boot.add_to_hass(hass)
            assert await hass.config_entries.async_setup(boot.entry_id)
            await hass.async_block_till_done()
            from custom_components.eybond_local.collector.transport import (
                _acquire_shared_listener,
            )
            from custom_components.eybond_local.passive_discovery import (
                get_passive_callback_discovery,
            )

            discovery_service = get_passive_callback_discovery(hass)
            assert discovery_service is not None
            injected_listener = await _acquire_shared_listener("0.0.0.0", tcp_port)
            discovery_service._listeners[tcp_port] = injected_listener

            # ---- 1-2. EVERY transition through the real flow manager -----
            flows = hass.config_entries.flow
            # The manual form groups advanced fields under a collapsible
            # section, exactly as the frontend submits them.
            manual_input = {
                "server_ip": "127.0.0.1",
                "collector_ip": "127.0.0.1",
                "driver_hint": "pi30",
                "connection_strategy": "callback_on_demand",
                "advanced_connection": {
                    "tcp_port": tcp_port,
                    "udp_port": udp_port,
                    "discovery_target": "127.0.0.1",
                    "discovery_interval": 3,
                    "heartbeat_interval": 60,
                },
            }

            async def _menu(result, option):
                assert result["type"] is FlowResultType.MENU, result
                assert option in result["menu_options"], result["menu_options"]
                return await flows.async_configure(
                    result["flow_id"], {"next_step_id": option}
                )

            async def _drain_progress(result, expected_next=None):
                # Re-enter a show_progress step until it resolves, exactly like
                # the frontend polls it -- never touching the flow object. The
                # manager collapses progress_done into the next step, so keep
                # configuring until the type is no longer a progress result.
                while result["type"] in (
                    FlowResultType.SHOW_PROGRESS,
                    FlowResultType.SHOW_PROGRESS_DONE,
                ):
                    await hass.async_block_till_done()
                    result = await flows.async_configure(result["flow_id"])
                if expected_next is not None:
                    assert result["step_id"] == expected_next, result
                return result

            result = await flows.async_init(DOMAIN, context={"source": "user"})
            # user -> collector_network -> auto (empty scan) -> scan_results
            # -> advanced_setup -> manual, all as manager transitions.
            for _ in range(12):
                if result["type"] in (
                    FlowResultType.SHOW_PROGRESS,
                    FlowResultType.SHOW_PROGRESS_DONE,
                ):
                    result = await _drain_progress(result)
                    continue
                if result["type"] is FlowResultType.MENU:
                    step = result["step_id"]
                    if step == "collector_network":
                        result = await _menu(result, "auto")
                    elif step == "advanced_setup":
                        result = await _menu(result, "manual")
                    else:
                        result = await _menu(result, result["menu_options"][0])
                    continue
                if result["type"] is FlowResultType.FORM and result["step_id"] == "scan_results":
                    # Empty auto-scan: pick the advanced-setup action key.
                    from custom_components.eybond_local.config_flow import (
                        _SCAN_RESULTS_ACTION_ADVANCED,
                    )

                    result = await flows.async_configure(
                        result["flow_id"],
                        {"result_key": _SCAN_RESULTS_ACTION_ADVANCED},
                    )
                    continue
                break
            # The manual form.
            assert result["type"] is FlowResultType.FORM, result
            assert result["step_id"] == "manual", result
            result = await flows.async_configure(result["flow_id"], manual_input)

            # Fully silent first socket -> honest taxonomy -> explicit framed.
            assert result["type"] is FlowResultType.MENU, result
            assert result["step_id"] == "manual_confirm", result
            assert "manual_bootstrap_framed" in result["menu_options"]
            result = await _menu(result, "manual_bootstrap_framed")

            # Recovery consent -> progress -> result, all manager-driven.
            assert result["step_id"] == "manual_recovery_confirm", result
            result = await _menu(result, "manual_recovery_verify")
            result = await _drain_progress(result)

            # The FLOW MANAGER created the entry.
            assert result["type"] is FlowResultType.CREATE_ENTRY, result
            data = dict(result["data"])
            assert data["connection_strategy"] == "callback_on_demand"
            contract = RecoveryContract.from_entry_data(data)
            assert contract is not None and contract.callback_verified

            registry = get_callback_session_registry(hass)
            assert registry is not None

            # ---- 3. the FLOW MANAGER created the entry AND set it up. Real
            # setup completes the exact handoff -- the prepared
            # callback_recovery owner becomes the durable entry owner with no
            # unowned window and no manual claim call.
            entry = next(
                e
                for e in hass.config_entries.async_entries(DOMAIN)
                if e.unique_id == f"collector:{FULL_PN}"
            )
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED
            assert registry.owner_for_pn(FULL_PN) == entry.entry_id

            # ---- 4-5. real coordinator/runtime; evidence via entry update -
            coordinator = entry.runtime_data
            deadline = asyncio.get_running_loop().time() + 45.0
            while True:
                await coordinator.async_refresh()
                snapshot = coordinator.data
                if (
                    snapshot is not None
                    and snapshot.connected
                    and "battery_voltage" in (snapshot.values or {})
                    and entry.data.get("collector_confirmed_session_protocol")
                ):
                    break
                assert (
                    asyncio.get_running_loop().time() < deadline
                ), f"runtime never settled: {getattr(snapshot, 'last_error', None)}"
                await asyncio.sleep(0.5)

            assert (
                entry.data["collector_confirmed_session_protocol"] == "eybond_framed"
            )
            assert (
                entry.data["collector_confirmed_session_protocol_source"]
                == "live_session"
            )

            # ---- 6. unload + reload through the REAL HA lifecycle --------
            assert await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.NOT_LOADED

            # The collector genuinely lost its link while HA was down (the
            # pcap sequence); the next attempt gets a fresh FULLY SILENT
            # socket the persisted-evidence FC=2 probe must identify.
            await service._close_tcp_only()
            service._last_discovery = None
            service.pre_rx_heartbeats = 0

            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED

            # ---- 7-8. silent socket -> active FC=2 -> connected + PI30 ---
            coordinator = entry.runtime_data
            deadline = asyncio.get_running_loop().time() + 45.0
            while True:
                await coordinator.async_refresh()
                snapshot = coordinator.data
                if (
                    snapshot is not None
                    and snapshot.connected
                    and "battery_voltage" in (snapshot.values or {})
                ):
                    break
                assert (
                    asyncio.get_running_loop().time() < deadline
                ), f"silent reconnect never settled: {getattr(snapshot, 'last_error', None)}"
                await asyncio.sleep(0.5)

            # Zero unsolicited bytes before the reconnect probe, and the live
            # values really came from a valid PI30 poll.
            assert getattr(service, "pre_rx_heartbeats", 0) == 0
            assert snapshot.values["battery_voltage"] > 0
    finally:
        if entry is not None and entry.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()
        if boot.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(boot.entry_id)
            await hass.async_block_till_done()
        if injected_listener is not None:
            from custom_components.eybond_local.collector.transport import (
                _release_shared_listener,
            )

            discovery_service = None
            try:
                from custom_components.eybond_local.passive_discovery import (
                    get_passive_callback_discovery,
                )

                discovery_service = get_passive_callback_discovery(hass)
            except Exception:
                discovery_service = None
            if discovery_service is not None:
                discovery_service._listeners.pop(tcp_port, None)
            await _release_shared_listener(
                injected_listener,
                close_pending=True,
                close_payload=True,
                close_at=True,
            )
        await service.stop()
