"""Batch 8 acceptance: the verified strategy transition through REAL HA options.

An inbound entry is live against the scripted fake collector over real
loopback sockets. The user switches the runtime selector to
``callback_on_demand``: the options flow must NOT write the strategy — it
routes through the dedicated confirmation/progress steps, the ONE transition
authority reboots the exact claimed session, the collector stays silent (its
endpoint no longer volunteers), exactly ONE ``set>server`` unicast brings a
new same-PN session, and only then the strategy + RecoveryContract land in
``entry.data`` with a single reload.

Every options-flow transition goes through
``hass.config_entries.options.async_init / async_configure`` — no
``async_step_*`` is called directly. The entry itself is the standard
tests_ha precondition fixture; the transition outcome is never written by
hand.
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

FULL_PN = "V001020SYN62344022"


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.mark.timeout(180)
async def test_options_strategy_transition_inbound_to_callback(
    hass: HomeAssistant, socket_enabled
) -> None:
    from unittest.mock import patch

    from fake_collector import FakeCollectorService
    from fake_collector_lib import CollectorProfile, resolve_scenario

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection import strategy_transition as st
    from custom_components.eybond_local.onboarding.timeouts import (
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    )

    tcp_port = _free_tcp_port()
    service = FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=0,
        tcp_bind_ip="127.0.0.1",
        # A periodic heartbeat: after the coordinator's first metadata reload
        # the NEW runtime re-binds to the still-open socket on the next
        # heartbeat (the production shape — collectors heartbeat every ~60 s).
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            # After the transition's controlled reboot the collector goes
            # FULLY silent and does NOT come back on its own — only the
            # single set>server proves the callback strategy.
            set_29_mode="reboot_silent",
            first_heartbeat_delay=0.0,
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
        callback_recovery_session_wait=10.0,
        callback_causality_lease_wait=3.0,
    )

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond collector",
        unique_id=f"collector:{FULL_PN}",
        data={
            "connection_type": "eybond",
            "server_ip": "127.0.0.1",
            "collector_ip": "127.0.0.1",
            "collector_pn": FULL_PN,
            "tcp_port": tcp_port,
            "udp_port": udp_port,
            "driver_hint": "pi30",
            "connection_strategy": "inbound",
            "discovery_target": "127.0.0.1",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        },
        options={"control_mode": "auto"},
    )
    try:
        with patch.object(integration, "PLATFORMS", ()), patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="127.0.0.1",
        ), patch(
            "custom_components.eybond_local.config_flow._get_ipv4_interfaces",
            return_value=[
                {
                    "name": "lo",
                    "ip": "127.0.0.1",
                    "label": "lo - 127.0.0.1",
                    "network": "127.0.0.0/8",
                    "broadcast": "127.255.255.255",
                }
            ],
        ), patch(
            "custom_components.eybond_local.config_flow._get_local_ip",
            return_value="127.0.0.1",
        ), patch.object(
            st, "DEFAULT_ONBOARDING_TIMEOUT_POLICY", fast_policy
        ):
            entry.add_to_hass(hass)
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()
            assert entry.state is ConfigEntryState.LOADED

            # The registry observes sessions through the passive-discovery
            # service's listeners; production binds deployment ports, the OS
            # boundary here is the TEST port — inject it exactly like the
            # other lifecycle tests do.
            from custom_components.eybond_local.collector.transport import (
                _acquire_shared_listener,
            )
            from custom_components.eybond_local.passive_discovery import (
                get_passive_callback_discovery,
            )

            discovery_service = get_passive_callback_discovery(hass)
            assert discovery_service is not None
            injected_listener = await _acquire_shared_listener("127.0.0.1", tcp_port)
            discovery_service._listeners[tcp_port] = injected_listener

            # Plumbing: the collector learns the listener endpoint and dials
            # in (the inbound topology's precondition, not a proof step).
            redirect = f"set>server=127.0.0.1:{tcp_port};".encode("ascii")
            await service.handle_discovery(redirect, ("127.0.0.1", 0))

            # The collector dials in and the runtime settles connected. A
            # REAL collector re-dials on its own whenever its TCP drops (the
            # coordinator's first-metadata reload closes the socket); model
            # that device behavior here.
            coordinator = entry.runtime_data
            deadline = asyncio.get_running_loop().time() + 45.0
            while True:
                coordinator = entry.runtime_data or coordinator
                await coordinator.async_refresh()
                snapshot = coordinator.data
                if snapshot is not None and snapshot.connected:
                    break
                if not service._connection_alive():
                    await service.handle_discovery(redirect, ("127.0.0.1", 0))
                assert (
                    asyncio.get_running_loop().time() < deadline
                ), f"runtime never connected: {getattr(snapshot, 'last_error', None)}"
                await asyncio.sleep(0.5)

            ledger_generation_before = (
                get_callback_trigger_ledger().snapshot_generation()
            )

            # ---- the OPTIONS flow, driven only through the flow manager ----
            options = hass.config_entries.options
            result = await options.async_init(entry.entry_id)
            assert result["type"] is FlowResultType.MENU, result
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "runtime"}
            )
            assert result["type"] is FlowResultType.FORM, result
            assert result["step_id"] == "runtime", result

            result = await options.async_configure(
                result["flow_id"],
                {
                    "poll_interval": 15,
                    "poll_mode": "auto",
                    "control_mode": "auto",
                    "connection_strategy": "callback_on_demand",
                    "connection": {
                        "server_ip": "127.0.0.1",
                        "collector_ip": "127.0.0.1",
                        "tcp_port": tcp_port,
                        "udp_port": udp_port,
                        "discovery_target": "127.0.0.1",
                        "discovery_interval": 3,
                        "heartbeat_interval": 60,
                        "driver_hint": "pi30",
                    },
                },
            )
            # The CHANGED strategy was NOT written: the dedicated
            # confirmation form appears and entry.data is untouched.
            assert result["type"] is FlowResultType.FORM, result
            assert result["step_id"] == "strategy_transition", result
            assert entry.data["connection_strategy"] == "inbound"

            # From here on the collector is FULLY silent after any reboot
            # (the pcap regression shape): the transition's silent-reconnect
            # probe must identify the new socket with an active FC=2.
            import dataclasses as _dc

            service._scenario = _dc.replace(
                service._scenario, first_heartbeat_delay=3600.0
            )

            result = await options.async_configure(
                result["flow_id"],
                {
                    "advertised_server_ip": "127.0.0.1",
                    "advertised_tcp_port": tcp_port,
                    "collector_ip": "127.0.0.1",
                },
            )
            while result["type"] in (
                FlowResultType.SHOW_PROGRESS,
                FlowResultType.SHOW_PROGRESS_DONE,
            ):
                await hass.async_block_till_done()
                result = await options.async_configure(result["flow_id"])
            assert result["type"] is FlowResultType.CREATE_ENTRY, result
            # Exactly ONE logical set>server sequence belongs to the PROOF.
            # (The reloaded callback-strategy runtime may already have sent
            # its own legitimate reconnect trigger by now — count sources.)
            transaction_records = [
                record
                for record in get_callback_trigger_ledger().recent_records()
                if record.source == "callback_recovery_transaction"
                and record.generation > ledger_generation_before
            ]
            assert len(transaction_records) == 1, transaction_records

            # ---- the authority committed: strategy + proof, one reload -----
            await hass.async_block_till_done()
            assert entry.data["connection_strategy"] == "callback_on_demand"
            contract = RecoveryContract.from_entry_data(entry.data)
            assert contract is not None and contract.callback_verified
            # The runtime options travelled with the same commit.
            assert entry.options.get("poll_interval") == 15
            # The entry reloaded and is healthy under the new strategy.
            assert entry.state is ConfigEntryState.LOADED
    finally:
        if entry.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()
        try:
            from custom_components.eybond_local.collector.transport import (
                _release_shared_listener,
            )
            from custom_components.eybond_local.passive_discovery import (
                get_passive_callback_discovery,
            )

            svc = get_passive_callback_discovery(hass)
            if svc is not None:
                svc._listeners.pop(tcp_port, None)
            if "injected_listener" in dir():
                await _release_shared_listener(
                    injected_listener,
                    close_pending=True,
                    close_payload=True,
                    close_at=True,
                )
        except Exception:
            pass
        await service.stop()
