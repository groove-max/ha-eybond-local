"""Passive-discovery inbound acceptance through the REAL Home Assistant.

A collector that already dials into the passive callback listener is offered
as an integration_discovery flow. The user confirms the controlled restart;
the inbound verifier reboots the exact claimed session, sends ZERO UDP, waits
for the autonomous reconnect, and -- once the same-PN session returns -- the
FLOW MANAGER creates a normal inbound entry. Real ``async_setup`` completes the
exact ownership handoff, and unload/reload stays green.

Every flow transition goes through ``hass.config_entries.flow.async_init /
async_configure`` -- no ``async_step_*`` is called directly, and the target
entry is created by the flow manager, never by a hand-built MockConfigEntry.
Only external boundaries are faked: the scripted collector over REAL loopback
sockets, entity platform forwarding, and the host-network probes.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import socket
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

FULL_PN = "V001020SYN62344022"


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.mark.timeout(180)
async def test_discovery_inbound_full_ha_lifecycle(
    hass: HomeAssistant, socket_enabled
) -> None:
    from unittest.mock import patch

    from fake_collector import FakeCollectorService
    from fake_collector_lib import CollectorProfile, resolve_scenario

    import custom_components.eybond_local as integration
    from custom_components.eybond_local import config_flow as config_flow_module
    from custom_components.eybond_local.collector.transport import (
        _acquire_shared_listener,
        _release_shared_listener,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.onboarding.timeouts import (
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
        get_passive_callback_discovery,
    )

    import dataclasses

    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )

    tcp_port = _free_tcp_port()
    service = FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=0,
        tcp_bind_ip="127.0.0.1",
        # A very long interval so the FIRST-contact connection announces itself
        # with exactly ONE heartbeat (the realistic short-PN discovery edge)
        # and never volunteers another unsolicited byte. Runtime liveness comes
        # from the collector's FC=1 replies, not from unsolicited heartbeats.
        heartbeat_interval=3600.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            # The collector reconnects on its OWN after the reboot -- the
            # autonomous inbound recovery this flow proves. No UDP is involved.
            set_29_mode="reboot",
            reboot_reconnect_delay=0.3,
            # First contact heartbeats immediately (so discovery sees it); the
            # scenario is switched to fully-silent before verification below.
            first_heartbeat_delay=0.0,
            pi30_mode="success",
        ),
    )
    await service.start()

    fast_policy = replace(
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        inbound_strong_identity_timeout=5.0,
        inbound_restart_disconnect_timeout=5.0,
        inbound_reconnect_timeout=6.0,
        callback_causality_lease_wait=3.0,
        callback_identity_session_wait=6.0,
    )

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
    entry = None
    try:
        loopback = [
            {
                "name": "lo",
                "ip": "127.0.0.1",
                "label": "lo - 127.0.0.1",
                "network": "127.0.0.0/8",
                "broadcast": "127.255.255.255",
            }
        ]
        with patch.object(integration, "PLATFORMS", ()), patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="127.0.0.1",
        ), patch(
            "custom_components.eybond_local.config_flow._get_ipv4_interfaces",
            return_value=loopback,
        ), patch(
            "custom_components.eybond_local.config_flow._get_local_ip",
            return_value="127.0.0.1",
        ), patch.object(
            config_flow_module, "_ONBOARDING_TIMEOUT_POLICY", fast_policy
        ):
            boot.add_to_hass(hass)
            assert await hass.config_entries.async_setup(boot.entry_id)
            await hass.async_block_till_done()
            discovery_service = get_passive_callback_discovery(hass)
            assert discovery_service is not None
            injected_listener = await _acquire_shared_listener("0.0.0.0", tcp_port)
            discovery_service._listeners[tcp_port] = injected_listener
            registry = get_callback_session_registry(hass)
            assert registry is not None

            # The collector dials in (the passive-discovery topology) and
            # announces itself with ONE short-PN heartbeat.
            redirect = f"set>server=127.0.0.1:{tcp_port};".encode("ascii")
            await service.handle_discovery(redirect, ("127.0.0.1", 0))

            async def _observed_session():
                # Passive discovery observes the collector with the SHORT PN
                # from the heartbeat and a WEAK (framed_heartbeat) identity
                # source -- the real first-contact edge, not a preset full PN.
                short_pn = FULL_PN[:14]
                deadline = asyncio.get_running_loop().time() + 6.0
                while True:
                    for session in registry.observed_sessions_per_socket():
                        if (
                            session.collector_pn.startswith(short_pn)
                            and not session.state.startswith("closed")
                        ):
                            return session
                    if asyncio.get_running_loop().time() >= deadline:
                        raise AssertionError("collector never dialed in")
                    await asyncio.sleep(0.05)

            observed = await _observed_session()
            observed_session_id = observed.session_id
            observed_pn = observed.collector_pn
            observed_source = observed.identity_source
            observed_shape = str(observed.raw.get("protocol_shape") or "")
            # Faithful first-contact evidence: short PN + weak heartbeat source.
            assert observed_pn == FULL_PN[:14], observed_pn
            assert observed_source == "framed_heartbeat", observed_source

            # ---- baseline: the fake goes FULLY SILENT before verification ---
            # Every socket the collector opens from here on volunteers ZERO
            # unsolicited bytes; identity can only come from an active FC=2 on
            # the trusted old wire.
            service._scenario = dataclasses.replace(
                service._scenario, first_heartbeat_delay=3600.0
            )
            service.pre_rx_heartbeats = 0
            ledger_generation_before = get_callback_trigger_ledger().snapshot_generation()
            discovery_rx_before = service.discovery_rx_count
            old_session_id = observed_session_id

            flows = hass.config_entries.flow

            async def _drain(result):
                while result["type"] in (
                    FlowResultType.SHOW_PROGRESS,
                    FlowResultType.SHOW_PROGRESS_DONE,
                ):
                    await hass.async_block_till_done()
                    result = await flows.async_configure(result["flow_id"])
                return result

            # ---- 1. the REAL flow manager starts the discovery flow --------
            # Only REALLY-observed values reach the payload: the short PN, the
            # weak framed_heartbeat source, the actual protocol shape and the
            # exact session id. No full PN / fc2_parameter_2 is preset.
            result = await flows.async_init(
                DOMAIN,
                context={
                    "source": "integration_discovery",
                    "eybond_discovery": {
                        "collector_pn": observed_pn,
                        "tcp_port": tcp_port,
                        "peer_ip": "127.0.0.1",
                        "session_id": observed_session_id,
                        "collector_identity_source": observed_source,
                    },
                },
                data={
                    "connection_type": "eybond",
                    "tcp_port": tcp_port,
                    "collector_pn": observed_pn,
                    "peer_ip": "127.0.0.1",
                    "session_id": observed_session_id,
                    "protocol_shape": observed_shape,
                    "collector_session_protocol": "",
                    "collector_identity_source": observed_source,
                },
            )
            discovery_flow_id = result["flow_id"]
            # Consent form: verifying restarts the collector.
            assert result["type"] is FlowResultType.FORM, result
            assert result["step_id"] == "verify_connection", result

            # ---- 2. consent -> progress -> inbound success -----------------
            result = await flows.async_configure(result["flow_id"], {})
            result = await _drain(result)

            # ---- 3. drive the identified inbound candidate to the entry ----
            for _ in range(8):
                if result["type"] in (
                    FlowResultType.SHOW_PROGRESS,
                    FlowResultType.SHOW_PROGRESS_DONE,
                ):
                    result = await _drain(result)
                    continue
                if result["type"] is FlowResultType.CREATE_ENTRY:
                    break
                if result["type"] is FlowResultType.MENU:
                    step = result["step_id"]
                    option = (
                        "confirm"
                        if "confirm" in result["menu_options"]
                        else result["menu_options"][0]
                    )
                    result = await flows.async_configure(
                        result["flow_id"], {"next_step_id": option}
                    )
                    continue
                if result["type"] is FlowResultType.FORM:
                    step = result["step_id"]
                    if step == "confirm":
                        result = await flows.async_configure(
                            result["flow_id"], {"poll_mode": "auto"}
                        )
                    else:
                        result = await flows.async_configure(result["flow_id"], {})
                    continue
                break

            assert result["type"] is FlowResultType.CREATE_ENTRY, result
            # Short->full enrichment happened INSIDE the same flow: the flow
            # manager never issued a new flow_id for it.
            assert result["flow_id"] == discovery_flow_id
            data = dict(result["data"])
            # The verifier enriched the short heartbeat PN to the FULL PN.
            assert data["collector_pn"] == FULL_PN
            assert result["result"].unique_id == f"collector:{FULL_PN}"
            # The discovery flow proved INBOUND -- the user's intent for a
            # passively-dialing collector -- and stored a real RecoveryContract.
            assert data["connection_strategy"] == "inbound"
            # Admission creates one collector-first entry regardless of source.
            # The scan-time heartbeat/probe never becomes inverter authority.
            assert data["driver_hint"] == "auto"
            assert data["detected_model"] == ""
            assert data["detected_serial"] == ""
            assert data["detection_confidence"] == "none"
            assert data["control_mode"] == "read_only"
            contract = RecoveryContract.from_entry_data(data)
            assert contract is not None and contract.inbound_verified
            # Inbound entries persist no unverified peer address as identity.
            assert data.get("collector_ip", "") == ""

            # ---- inbound-verification invariants: FULLY SILENT, ZERO UDP ---
            # The recovered socket volunteered no unsolicited application byte;
            # its identity came only from the active FC=2 on the trusted wire.
            assert getattr(service, "pre_rx_heartbeats", 0) == 0
            # Inbound sends ZERO UDP: neither the callback ledger nor the
            # collector's set>server/discovery receiver moved.
            assert (
                get_callback_trigger_ledger().snapshot_generation()
                == ledger_generation_before
            )
            assert service.discovery_rx_count == discovery_rx_before

            # ---- 4. real setup completes the EXACT ownership handoff -------
            entry = next(
                e
                for e in hass.config_entries.async_entries(DOMAIN)
                if e.unique_id == f"collector:{FULL_PN}"
            )
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED
            assert registry.owner_for_pn(FULL_PN) == entry.entry_id

            # The recovered session is a DIFFERENT socket than the old one, and
            # it was identified authoritatively (FC=2), never by heartbeat.
            recovered_session_id = registry.claimed_session_id(entry.entry_id)
            assert recovered_session_id
            assert recovered_session_id != old_session_id
            recovered = next(
                s
                for s in registry.observed_sessions_per_socket()
                if s.session_id == recovered_session_id
            )
            assert recovered.identity_source == "fc2_parameter_2"
            assert recovered.collector_pn == FULL_PN

            # No stale discovery flow for this PN remains.
            remaining = [
                flow
                for flow in flows.async_progress()
                if flow.get("handler") == DOMAIN
                and str(flow.get("context", {}).get("source") or "")
                == "integration_discovery"
            ]
            assert remaining == []

            # ---- 5. unload + reload through the REAL HA lifecycle ----------
            assert await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.NOT_LOADED
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED
            # A normal unload/reload never executes the removal ticket.
            assert registry.claimed_session_id(entry.entry_id) == recovered_session_id

            # ---- 6. permanent removal restarts the exact owned socket ------
            # Let the autonomous inbound reconnect announce itself normally so
            # the post-removal session becomes a genuine new discovery edge.
            service._scenario = dataclasses.replace(
                service._scenario, first_heartbeat_delay=0.0
            )
            discovery_rx_before_removal = service.discovery_rx_count
            remove_result = await hass.config_entries.async_remove(entry.entry_id)
            await hass.async_block_till_done()
            assert remove_result == {"require_restart": False}
            assert hass.config_entries.async_get_entry(entry.entry_id) is None
            assert registry.owner_for_pn(FULL_PN) == ""

            deadline = asyncio.get_running_loop().time() + 6.0
            replacement = None
            while asyncio.get_running_loop().time() < deadline:
                replacement = next(
                    (
                        session
                        for session in registry.observed_sessions_per_socket()
                        if session.session_id != recovered_session_id
                        and not session.state.startswith("closed")
                        and session.collector_pn.startswith(FULL_PN[:14])
                    ),
                    None,
                )
                if replacement is not None:
                    break
                await asyncio.sleep(0.05)
            assert replacement is not None
            assert replacement.session_id != recovered_session_id
            assert replacement.owner_entry_id == ""
            # Removal clears volatile callback state by FC=3 restart; it never
            # emits a set>server UDP trigger or matches the replacement by IP.
            assert service.discovery_rx_count == discovery_rx_before_removal
    finally:
        if entry is not None and entry.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()
        if boot.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(boot.entry_id)
            await hass.async_block_till_done()
        if injected_listener is not None:
            svc = get_passive_callback_discovery(hass)
            if svc is not None:
                svc._listeners.pop(tcp_port, None)
            await _release_shared_listener(
                injected_listener,
                close_pending=True,
                close_payload=True,
                close_at=True,
            )
        await service.stop()
