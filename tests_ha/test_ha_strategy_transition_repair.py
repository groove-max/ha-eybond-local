"""Batch 8B.2A: a TRUE COLD degraded strategy-transition repair through REAL HA.

The target collector entry is left in the typed persisted recovery state and is
NOT loaded: no coordinator, no runtime, no registry claim, no live session. The
domain services (passive discovery + the ownership registry) are brought up by a
separate LISTENER-role bootstrap entry, NOT by setting up the target. The repair
runs entirely through ``hass.config_entries.options.async_init/async_configure``.

What makes this a real cold proof (none of the old shortcuts):

* the target entry is never set up before the repair;
* the test never borrows the shared listener itself or injects the discovery
  service's private listener map -- the repair makes the RANDOM custom callback
  port observable by the DOMAIN registry through the public
  ``async_ensure_observed_listener`` boundary;
* Phase A receives a FULLY-SILENT callback socket through production wiring and
  identifies it with one FC=2 read on the confirmed-evidence wire;
* Phase B runs the full callback recovery proof;
* the commit sets the strategy up and the cold entry is set up EXACTLY once.

``tests/test_cross_layer_architecture.py`` structurally bans the old shortcuts in
this file.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import socket
import sys

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[0]
for _path in (str(REPO_ROOT), str(HERE), str(REPO_ROOT / "tests" / "helpers")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from custom_components.eybond_local.const import (  # noqa: E402
    COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN,
    CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE,
    CONF_ENTRY_ROLE,
    CONF_STRATEGY_TRANSITION_STATE,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
)

FULL_PN = "V001020SYN62344022"
TS = "2026-07-17T10:00:00+00:00"


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _loopback_patches(integration, repair_mod=None, sv_mod=None, fast_policy=None):
    from unittest.mock import patch

    patches = [
        patch.object(integration, "PLATFORMS", ()),
        patch(
            "custom_components.eybond_local.runtime.link._default_local_ip",
            return_value="127.0.0.1",
        ),
        patch(
            "custom_components.eybond_local.config_flow._get_ipv4_interfaces",
            return_value=[{
                "name": "lo", "ip": "127.0.0.1", "label": "lo",
                "network": "127.0.0.0/8", "broadcast": "127.255.255.255",
            }],
        ),
        patch(
            "custom_components.eybond_local.config_flow._get_local_ip",
            return_value="127.0.0.1",
        ),
    ]
    if repair_mod is not None and fast_policy is not None:
        patches.append(
            patch.object(repair_mod, "DEFAULT_ONBOARDING_TIMEOUT_POLICY", fast_policy)
        )
    if sv_mod is not None and fast_policy is not None:
        patches.append(
            patch.object(sv_mod, "DEFAULT_ONBOARDING_TIMEOUT_POLICY", fast_policy)
        )
    return patches


def _confirmed_evidence() -> dict:
    return {
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL: "eybond_framed",
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_PN: FULL_PN,
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE: (
            COLLECTOR_CONFIRMED_SESSION_PROTOCOL_SOURCE_LIVE
        ),
        CONF_COLLECTOR_CONFIRMED_SESSION_PROTOCOL_OBSERVED_AT: TS,
    }


def _target_entry(
    *, tcp_port: int, udp_port: int, extra_data=None, extra_options=None,
    recovery_phase=None,
) -> MockConfigEntry:
    from custom_components.eybond_local.connection.strategy_transition_recovery import (
        RECOVERY_PHASE_PENDING,
        StrategyTransitionRecoveryState,
    )

    recovery_state = StrategyTransitionRecoveryState.create(
        collector_pn=FULL_PN,
        now=TS,
        trigger_target_host="127.0.0.1",
        trigger_udp_port=udp_port,
        advertised_host="127.0.0.1",
        advertised_port=tcp_port,
        # The PRODUCTION-GENERATED split (server_ip=127.0.0.1, non-default): the
        # UDP trigger binds the loopback server IP, while the shared TCP listener
        # binds the runtime's ACTUAL listener host (0.0.0.0). This is exactly what
        # the coordinator writes -- the repair's ensure then borrows the SAME
        # refcounted listener the runtime binds on the custom port.
        trigger_bind_host="127.0.0.1",
        listener_bind_host="0.0.0.0",
        local_listener_port=tcp_port,
        phase=recovery_phase or RECOVERY_PHASE_PENDING,
    )
    data = {
        "connection_type": "eybond",
        "server_ip": "127.0.0.1",
        "collector_ip": "127.0.0.1",
        "collector_pn": FULL_PN,
        "tcp_port": tcp_port,
        "udp_port": udp_port,
        "driver_hint": "pi30",
        # Degraded: canonical strategy still inbound, endpoint external.
        "connection_strategy": "inbound",
        "endpoint_control_policy": "external",
        "advertised_server_ip": "127.0.0.1",
        "advertised_tcp_port": tcp_port,
        "discovery_target": "127.0.0.1",
        "discovery_interval": 3,
        "heartbeat_interval": 60,
        CONF_STRATEGY_TRANSITION_STATE: recovery_state.to_record(),
        # A VALID PN-bound confirmed-evidence from a previous live session: the
        # fully-silent Phase A must pick the framed wire from THIS, never a
        # cloud/driver/expected hint.
        **_confirmed_evidence(),
    }
    data.update(extra_data or {})
    options = {"control_mode": "auto"}
    options.update(extra_options or {})
    return MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond collector",
        unique_id=f"collector:{FULL_PN}",
        data=data,
        options=options,
    )


async def _boot_domain_services(hass: HomeAssistant) -> MockConfigEntry:
    """Bring up passive discovery + the domain registry WITHOUT the target.

    A LISTENER-role entry keeps the integration loaded (its setup has no
    coordinator/runtime) so ``async_setup`` runs and starts the domain services.
    """

    boot = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond listener",
        unique_id="listener-boot",
        data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
        options={},
    )
    boot.add_to_hass(hass)
    assert await hass.config_entries.async_setup(boot.entry_id)
    await hass.async_block_till_done()
    return boot


async def _drain_options(options, result, hass):
    while result["type"] in (
        FlowResultType.SHOW_PROGRESS,
        FlowResultType.SHOW_PROGRESS_DONE,
    ):
        await hass.async_block_till_done()
        result = await options.async_configure(result["flow_id"])
    return result


@pytest.mark.timeout(90)
async def test_cold_degraded_repair_through_real_ha(
    hass: HomeAssistant, socket_enabled
) -> None:
    from contextlib import ExitStack
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
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
    )
    from custom_components.eybond_local.onboarding.timeouts import (
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    )
    from custom_components.eybond_local.connection.session_registry import (
        pn_is_same_identity,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
    )

    tcp_port = _free_tcp_port()
    service = FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=0,
        tcp_bind_ip="127.0.0.1",
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            # Phase A: the dial-in stays fully SILENT long enough for the FC=2
            # read; the Phase-B reconnect heartbeats (so the recovery engine can
            # observe its wire). One value serves both because the Phase-A read
            # is sub-second while the reconnect heartbeats well inside the wait.
            first_heartbeat_delay=1.5,
            # Phase B: reboot on FC=29 and stay down until the callback set>server.
            set_29_mode="reboot_silent",
            reboot_reconnect_delay=0.3,
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
        callback_recovery_session_wait=12.0,
        callback_causality_lease_wait=3.0,
    )

    boot = None
    # A large poll interval so the runtime's normal post-LOADED callback polling
    # does not fire inside the test window and pollute the repair's ledger delta.
    target = _target_entry(
        tcp_port=tcp_port,
        udp_port=udp_port,
        extra_data={"discovery_interval": 3600},
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, fast_policy):
                stack.enter_context(p)

            boot = await _boot_domain_services(hass)

            # ---- the target entry is genuinely COLD ------------------------
            target.add_to_hass(hass)
            assert target.state is ConfigEntryState.NOT_LOADED
            assert getattr(target, "runtime_data", None) is None
            registry = get_callback_session_registry(hass)
            assert registry is not None
            assert registry.owner_for_pn(FULL_PN) == ""
            assert registry.claimed_session_id(target.entry_id) == ""
            assert not any(
                s.collector_pn == FULL_PN
                for s in registry.observed_sessions_per_socket()
            )

            ledger = get_callback_trigger_ledger()

            # The ledger's _history is a process-global RING BUFFER; baseline the
            # generation so every count below is THIS repair's OWN contribution,
            # order-independent in the full suite (prior tests' sends are excluded).
            gen_before = ledger.snapshot_generation()

            def _by_source(source: str) -> int:
                return sum(
                    1 for r in ledger._history
                    if r.source == source and r.generation > gen_before
                )

            def _repair_sequences() -> int:
                # Phase A + Phase B both send through the channel's bootstrap
                # sender; the runtime's polling is a DIFFERENT source and is not
                # counted here.
                return _by_source("degraded_repair_bootstrap")

            # ---- instrument the REAL activation (load-bearing) --------------
            # A counter + controlled delay wraps the entry's async_setup. During
            # the delay the repair STILL holds the observed-listener token (the
            # fix awaits activation before releasing it), so the certified Phase-B
            # session must still be OBSERVED and OWNED. The old
            # create_task-before-release variant drops the token first, closing
            # the session, and this snapshot would be empty -> the test fails.
            setup_calls: list = []
            reload_calls: list = []
            snap: dict = {}
            orig_setup = hass.config_entries.async_setup
            orig_reload = hass.config_entries.async_reload

            async def _instrumented_reload(eid, *args, **kwargs):
                if eid == target.entry_id:
                    reload_calls.append(eid)  # the runtime's own metadata-drift reload
                return await orig_reload(eid, *args, **kwargs)

            stack.enter_context(
                patch.object(
                    hass.config_entries, "async_reload", _instrumented_reload
                )
            )

            async def _instrumented_setup(eid, *args, **kwargs):
                if eid != target.entry_id or setup_calls:
                    return await orig_setup(eid, *args, **kwargs)
                setup_calls.append(str(target.state))
                # BEFORE the runtime comes up (token STILL held by the repair):
                # the certified Phase-B session must already be present + owned.
                await asyncio.sleep(0.3)
                cert_sid = registry.claimed_session_id(target.entry_id)
                snap["before"] = {
                    "owner": registry.owner_for_pn(FULL_PN),
                    "sid": cert_sid,
                    "observed": bool(cert_sid) and any(
                        s.session_id == cert_sid
                        for s in registry.observed_sessions_per_socket()
                    ),
                }
                # Run the REAL setup while the observed-listener token is HELD.
                ok = await orig_setup(eid, *args, **kwargs)
                # AFTER setup returns (token STILL held): the runtime adopted the
                # SAME certified session, a trusted SessionHandle observes it, and
                # it never had to re-trigger the collector.
                handle = registry.session_handle_for_owned_session(
                    target.entry_id, cert_sid
                )
                snap["after"] = {
                    "setup_ok": bool(ok),
                    "sid": registry.claimed_session_id(target.entry_id),
                    "cert_sid": cert_sid,
                    "observed": any(
                        s.session_id == cert_sid
                        for s in registry.observed_sessions_per_socket()
                    ),
                    "handle_ok": (
                        handle is not None
                        and handle.observed
                        and handle.session_id == cert_sid
                    ),
                    "handle_pn_same": (
                        handle is not None
                        and pn_is_same_identity(handle.collector_pn, FULL_PN)
                    ),
                    "runtime_triggers": _by_source("runtime_callback_on_demand"),
                }
                return ok

            stack.enter_context(
                patch.object(
                    hass.config_entries, "async_setup", _instrumented_setup
                )
            )

            # ---- drive the repair ONLY through the HA options manager ------
            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            assert result["type"] is FlowResultType.MENU, result
            assert "strategy_transition_repair" in result["menu_options"], result

            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            assert result["type"] is FlowResultType.CREATE_ENTRY, result

            # ---- exactly two logical set>server sequences (A + B) ----------
            assert _repair_sequences() == 2, [
                (r.generation, r.source) for r in ledger._history
            ]
            # ---- LOAD-BEARING: continuous handoff proven POST-SETUP ---------
            # The config flow activates the entry with exactly ONE awaited
            # async_setup (the instrumented wrapper counts only that first,
            # config-flow-driven activation). Any later reload is the runtime's
            # OWN metadata-drift reload of a fresh cold entry -- a separate,
            # legitimate lifecycle, not a repair double-activation.
            assert len(setup_calls) == 1, (setup_calls, reload_calls)
            before, after = snap.get("before", {}), snap.get("after", {})
            # BEFORE the runtime came up (token held): the certified session is
            # present + owned. The old create_task-before-release variant releases
            # the token first, so the session is gone and ``observed`` is False.
            assert before.get("owner") == target.entry_id
            assert before.get("sid")
            assert before.get("observed") is True, "certified session gone before setup"
            # AFTER the REAL setup returned, WHILE the token is still held: the
            # runtime adopted the SAME certified session (not a new callback), a
            # trusted SessionHandle observes it, and it never re-triggered.
            assert after.get("setup_ok") is True
            assert after.get("sid") == after.get("cert_sid"), "runtime replaced session"
            assert after.get("observed") is True, "certified session gone after setup"
            assert after.get("handle_ok") is True, "no trusted handle for the session"
            assert after.get("handle_pn_same") is True
            assert after.get("runtime_triggers") == 0, "runtime re-triggered on setup"

            # ---- proven commit, cleared state, owned identity --------------
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data
            contract = RecoveryContract.from_entry_data(target.data)
            assert contract is not None and contract.callback_verified
            proof = contract.callback_proof
            assert proof.collector_pn == FULL_PN
            assert proof.identity_source == "fc2_parameter_2"
            assert f"127.0.0.1:{tcp_port}" in proof.advertised_ha_endpoint
            assert registry.owner_for_pn(FULL_PN) == target.entry_id

            # ---- the entry ends LOADED and owned, no extra repair sequences -
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert registry.owner_for_pn(FULL_PN) == target.entry_id
            assert _repair_sequences() == 2
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()
        await service.stop()


@pytest.mark.timeout(75)
async def test_cold_repair_cancellation_is_clean(
    hass: HomeAssistant, socket_enabled
) -> None:
    from contextlib import ExitStack
    from dataclasses import replace

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection.strategy_transition import (
        STRATEGY_TRANSITION_LEASES,
    )
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
    )
    from custom_components.eybond_local.onboarding.timeouts import (
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    )

    tcp_port = _free_tcp_port()
    udp_port = _free_tcp_port()  # nothing listens: the collector never answers
    fast_policy = replace(
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        callback_recovery_session_wait=4.0,
        callback_causality_lease_wait=2.0,
    )

    boot = None
    target = _target_entry(tcp_port=tcp_port, udp_port=udp_port)
    original_data = dict(target.data)
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, fast_policy):
                stack.enter_context(p)

            boot = await _boot_domain_services(hass)
            target.add_to_hass(hass)
            assert target.state is ConfigEntryState.NOT_LOADED

            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            assert "strategy_transition_repair" in result["menu_options"]
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            assert result["type"] is FlowResultType.SHOW_PROGRESS, result
            flow_id = result["flow_id"]
            # Let Phase A start and send its ONE trigger, then leave it MID-WAIT
            # (no collector answers). A short sleep (not block_till_done) keeps the
            # progress task running so the abort truly cancels it in flight.
            await asyncio.sleep(1.0)
            assert ledger.snapshot_generation() - gen_before == 1  # Phase A only

            # ---- cancel the IN-FLIGHT flow through the PUBLIC manager API ---
            options.async_abort(flow_id)

            # ---- the in-flight task unwinds; wait (bounded) for release -----
            async def _lease_freed() -> bool:
                if STRATEGY_TRANSITION_LEASES.acquire(target.entry_id):
                    STRATEGY_TRANSITION_LEASES.release(target.entry_id)
                    return True
                return False

            freed = False
            for _ in range(60):
                await hass.async_block_till_done()
                if await _lease_freed():
                    freed = True
                    break
                await asyncio.sleep(0.1)

            # ---- everything is released, nothing committed -----------------
            assert freed, "strategy lease never released after cancellation"
            assert ledger._causality_owner == ""
            assert target.state is ConfigEntryState.NOT_LOADED
            # Connection axes + recovery state byte-for-byte unchanged.
            assert target.data["connection_strategy"] == "inbound"
            assert CONF_STRATEGY_TRANSITION_STATE in target.data
            assert target.data == original_data
            assert RecoveryContract.from_entry_data(target.data) is None
            # A documented PN-only ownership intent may survive, but NO socket.
            from custom_components.eybond_local.passive_discovery import (
                get_callback_session_registry,
            )

            registry = get_callback_session_registry(hass)
            assert registry.claimed_session_id(target.entry_id) == ""

            # ---- the custom listener is actually CLOSED after cleanup -------
            # Black-box (no private _listeners access): with the ensured token
            # released and no runtime holding it, a bounded TCP connect to the
            # random port is refused.
            import contextlib as _ctx

            async def _listener_open() -> bool:
                try:
                    _r, _w = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", tcp_port), timeout=0.3
                    )
                except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
                    return False
                _w.close()
                with _ctx.suppress(Exception):
                    await _w.wait_closed()
                return True

            closed = False
            for _ in range(30):
                if not await _listener_open():
                    closed = True
                    break
                await asyncio.sleep(0.1)
            assert closed, "custom listener still open after cancellation cleanup"

            # At most Phase A's single trigger fired; cancellation adds none.
            assert ledger.snapshot_generation() - gen_before <= 1
            gen_after_cancel = ledger.snapshot_generation()

            # ---- a fresh retry can start (nothing is jammed) ---------------
            retry = await options.async_init(target.entry_id)
            assert "strategy_transition_repair" in retry["menu_options"]
            options.async_abort(retry["flow_id"])
            await hass.async_block_till_done()
            assert ledger.snapshot_generation() == gen_after_cancel  # no new UDP
    finally:
        if boot is not None and boot.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(boot.entry_id)
            await hass.async_block_till_done()


@pytest.mark.timeout(120)
async def test_normal_inbound_entry_has_no_repair_and_sends_no_udp(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Regression: a healthy inbound entry (no recovery state) never offers the
    repair action and never triggers a bootstrap set>server."""

    from contextlib import ExitStack

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )

    tcp_port = _free_tcp_port()
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
            "udp_port": 58899,
            "driver_hint": "pi30",
            "connection_strategy": "inbound",
            "discovery_target": "127.0.0.1",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
        },
        options={"control_mode": "auto"},
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration):
                stack.enter_context(p)
            entry.add_to_hass(hass)
            assert await hass.config_entries.async_setup(entry.entry_id)
            await hass.async_block_till_done()

            gen_before = get_callback_trigger_ledger().snapshot_generation()
            result = await hass.config_entries.options.async_init(entry.entry_id)
            assert result["type"] is FlowResultType.MENU
            assert "strategy_transition_repair" not in result["menu_options"]
            assert (
                get_callback_trigger_ledger().snapshot_generation() == gen_before
            )
    finally:
        if entry.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(entry.entry_id)
            await hass.async_block_till_done()


@pytest.mark.timeout(30)
async def test_coordinator_recovery_state_splits_trigger_and_listener_bind(
    socket_enabled,
) -> None:
    """Batch 8B.2A: with a NON-DEFAULT server_ip the coordinator writes the
    recovery state's trigger_bind_host (UDP = server_ip) and listener_bind_host
    (TCP = the runtime's actual listener host) as TWO distinct values -- never
    conflated, never hardcoded. Drives the REAL transition facade and captures
    what it hands the authority."""

    import socket as _sock
    import types
    from unittest.mock import patch

    from custom_components.eybond_local.connection.strategy_transition import (
        StrategyTransitionResult,
    )
    from custom_components.eybond_local.runtime.coordinator import (
        EybondLocalCoordinator,
    )

    probe = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    free_port = probe.getsockname()[1]
    probe.close()

    coordinator = object.__new__(EybondLocalCoordinator)
    coordinator.config_entry = types.SimpleNamespace(
        entry_id="entry-split-1",
        data={"collector_pn": FULL_PN, "connection_strategy": "inbound"},
        options={},
    )
    coordinator._connection_spec = types.SimpleNamespace(
        server_ip="127.0.0.1",  # NON-default: distinct from the listener host
        collector_ip="127.0.0.1",
        udp_port=58899,
        tcp_port=free_port,
    )
    coordinator._runtime = types.SimpleNamespace(listener_bind_host="0.0.0.0")
    coordinator.data = types.SimpleNamespace(values={})
    coordinator.hass = types.SimpleNamespace()

    captured: dict = {}

    async def _capture_authority(**kwargs):
        captured["recovery_state"] = kwargs.get("recovery_state")
        return StrategyTransitionResult(
            success=False,
            target_strategy="callback_on_demand",
            failure_reason="captured",
        )

    class _FakeProbe:
        def __init__(self, **kwargs):
            pass

        async def async_open(self):
            return None

        async def async_close(self):
            return None

    with patch(
        "custom_components.eybond_local.connection.strategy_transition."
        "async_run_strategy_transition",
        _capture_authority,
    ), patch(
        "custom_components.eybond_local.collector.silent_session_probe."
        "SilentSessionIdentityProbeChannel",
        _FakeProbe,
    ), patch(
        "custom_components.eybond_local.passive_discovery."
        "get_callback_session_registry",
        lambda _h: None,
    ):
        await coordinator.async_run_connection_strategy_transition(
            target_strategy="callback_on_demand",
            advertised_host="127.0.0.1",
            advertised_port=18899,
            callback_target_ip="127.0.0.1",
        )

    state = captured.get("recovery_state")
    assert state is not None
    assert state.trigger_bind_host == "127.0.0.1"  # server IP (UDP trigger bind)
    assert state.listener_bind_host == "0.0.0.0"  # runtime host (TCP listener bind)
    assert state.trigger_bind_host != state.listener_bind_host
    assert state.callback_route().bind_ip == "127.0.0.1"  # route bind = trigger bind


@pytest.mark.timeout(90)
@pytest.mark.parametrize("fail_mode", ["returns_false", "raises"])
async def test_activation_failure_after_proof_keeps_proof_and_offers_reload_only(
    hass: HomeAssistant, socket_enabled, fail_mode: str
) -> None:
    """Batch 8B.2A Task 4: the proof committed durably, but HA's activation did
    NOT complete (async_setup returns falsy / raises). The durable proof must
    survive untouched, the physical repair must NOT be repeated, and the UI must
    offer ONLY a plain reload -- never the full physical-repair retry."""

    from contextlib import ExitStack
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
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
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
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            first_heartbeat_delay=1.5,
            set_29_mode="reboot_silent",
            reboot_reconnect_delay=0.3,
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
        callback_recovery_session_wait=12.0,
        callback_causality_lease_wait=3.0,
    )

    boot = None
    target = _target_entry(
        tcp_port=tcp_port, udp_port=udp_port, extra_data={"discovery_interval": 3600}
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, fast_policy):
                stack.enter_context(p)

            boot = await _boot_domain_services(hass)
            target.add_to_hass(hass)
            assert target.state is ConfigEntryState.NOT_LOADED
            registry = get_callback_session_registry(hass)

            ledger = get_callback_trigger_ledger()

            def _repair_sequences() -> int:
                return sum(
                    1 for r in ledger._history if r.source == "degraded_repair_bootstrap"
                )

            # The trigger ledger is a process-global singleton; baseline it so the
            # count is this repair's OWN contribution, not leakage from prior tests.
            seq_before = _repair_sequences()

            # ---- the proof succeeds, but HA cannot bring the entry up ---------
            setup_calls: list = []
            reload_calls: list = []
            orig_setup = hass.config_entries.async_setup
            orig_reload = hass.config_entries.async_reload

            async def _instrumented_reload(eid, *args, **kwargs):
                if eid == target.entry_id:
                    reload_calls.append(eid)
                return await orig_reload(eid, *args, **kwargs)

            async def _failing_setup(eid, *args, **kwargs):
                if eid == target.entry_id and not setup_calls:
                    # The FIRST (config-flow-driven) activation fails AFTER the
                    # durable proof commit -- HA never loads the entry, so it
                    # stays NOT_LOADED (no coordinator, no runtime side effects).
                    setup_calls.append(str(target.state))
                    if fail_mode == "raises":
                        raise RuntimeError("simulated setup failure")
                    return False
                return await orig_setup(eid, *args, **kwargs)

            stack.enter_context(
                patch.object(hass.config_entries, "async_reload", _instrumented_reload)
            )
            stack.enter_context(
                patch.object(hass.config_entries, "async_setup", _failing_setup)
            )

            # ---- drive the repair through the HA options manager -------------
            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            assert "strategy_transition_repair" in result["menu_options"]
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)

            # ---- the UI is the activation-incomplete menu, NOT a full repair --
            assert result["type"] is FlowResultType.MENU, result
            opts = result["menu_options"]
            assert "strategy_transition_activation_retry" in opts, opts
            assert "strategy_transition_cancel" in opts, opts
            # The proof is durable: NEVER re-run the physical repair or resubmit.
            assert "strategy_transition" not in opts, opts
            assert "strategy_transition_keep_settings" not in opts, opts
            # An HONEST, localized reason -- not the raw code, not a repair-failed
            # sentence, and no false "the connection was not verified" claim.
            explanation = result["description_placeholders"]["failure_explanation"]
            assert "verified and saved" in explanation.lower(), explanation
            assert "transition_activation_incomplete" not in explanation

            # ---- the durable proof survives untouched ------------------------
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data  # NOT restored
            contract = RecoveryContract.from_entry_data(target.data)
            assert contract is not None and contract.callback_verified
            assert contract.callback_proof.collector_pn == FULL_PN
            assert registry.owner_for_pn(FULL_PN) == target.entry_id

            # ---- the physical repair ran EXACTLY once (A + B); no repeat ------
            assert _repair_sequences() - seq_before == 2, [
                (r.generation, r.source) for r in ledger._history
            ]
            assert len(setup_calls) == 1, setup_calls
            assert reload_calls == [], reload_calls  # no metadata-drift reload
            assert target.state is ConfigEntryState.NOT_LOADED

            # ---- item 6: press activation-retry -> the entry LOADS -----------
            # Home Assistant can now load the entry (only the FIRST setup failed).
            # The retry re-runs ONLY the load (a reload), never the repair: no new
            # Phase A/B, the RecoveryContract is untouched, the recovery marker
            # does not come back, and the entry ends LOADED and owned.
            retry = await options.async_configure(
                result["flow_id"],
                {"next_step_id": "strategy_transition_activation_retry"},
            )
            retry = await _drain_options(options, retry, hass)
            assert retry["type"] is FlowResultType.CREATE_ENTRY, retry
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data  # still gone
            contract_after = RecoveryContract.from_entry_data(target.data)
            assert contract_after is not None and contract_after.callback_verified
            assert contract_after.callback_proof.collector_pn == FULL_PN
            assert registry.owner_for_pn(FULL_PN) == target.entry_id
            # The retry loaded the proven config; it never re-ran Phase A/B.
            assert _repair_sequences() - seq_before == 2, [
                (r.generation, r.source) for r in ledger._history
            ]
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()
        await service.stop()


@pytest.mark.timeout(90)
async def test_activation_retry_failure_stays_on_activation_menu(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Batch 8B.2A item 7: when the activation retry ALSO fails to load the entry,
    the flow stays on the activation-only menu -- the proof/axes persist, no
    physical repair is retried, the action is repeatable, and cancel closes the
    flow without rolling anything back."""

    from contextlib import ExitStack
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
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
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
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            first_heartbeat_delay=1.5,
            set_29_mode="reboot_silent",
            reboot_reconnect_delay=0.3,
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
        callback_recovery_session_wait=12.0,
        callback_causality_lease_wait=3.0,
    )

    boot = None
    target = _target_entry(
        tcp_port=tcp_port, udp_port=udp_port, extra_data={"discovery_interval": 3600}
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, fast_policy):
                stack.enter_context(p)

            boot = await _boot_domain_services(hass)
            target.add_to_hass(hass)
            assert target.state is ConfigEntryState.NOT_LOADED
            registry = get_callback_session_registry(hass)
            ledger = get_callback_trigger_ledger()

            def _repair_sequences() -> int:
                return sum(
                    1 for r in ledger._history if r.source == "degraded_repair_bootstrap"
                )

            seq_before = _repair_sequences()

            # The TARGET entry can NEVER be loaded (every activation fails); the
            # boot listener entry loads normally.
            orig_setup = hass.config_entries.async_setup

            async def _always_failing_setup(eid, *args, **kwargs):
                if eid == target.entry_id:
                    return False
                return await orig_setup(eid, *args, **kwargs)

            stack.enter_context(
                patch.object(hass.config_entries, "async_setup", _always_failing_setup)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            # Proof succeeded, activation failed -> activation-only menu.
            assert result["type"] is FlowResultType.MENU, result
            assert "strategy_transition_activation_retry" in result["menu_options"]
            assert "strategy_transition" not in result["menu_options"]
            assert _repair_sequences() - seq_before == 2  # A + B once

            # ---- retry ALSO fails -> STAYS on the activation-only menu --------
            retry = await options.async_configure(
                result["flow_id"],
                {"next_step_id": "strategy_transition_activation_retry"},
            )
            retry = await _drain_options(options, retry, hass)
            assert retry["type"] is FlowResultType.MENU, retry
            assert "strategy_transition_activation_retry" in retry["menu_options"]
            # NEVER a physical-repair retry, NEVER a full-repair resubmit.
            assert "strategy_transition" not in retry["menu_options"]
            assert "strategy_transition_keep_settings" not in retry["menu_options"]
            # Proof + axes persist; no Phase A/B repeat.
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data
            assert RecoveryContract.from_entry_data(target.data) is not None
            assert _repair_sequences() - seq_before == 2

            # ---- the action is REPEATABLE (retry again -> still menu) --------
            retry2 = await options.async_configure(
                retry["flow_id"],
                {"next_step_id": "strategy_transition_activation_retry"},
            )
            retry2 = await _drain_options(options, retry2, hass)
            assert retry2["type"] is FlowResultType.MENU, retry2
            assert _repair_sequences() - seq_before == 2  # still no repeat

            # ---- cancel closes the flow, rolling back NOTHING ---------------
            cancel = await options.async_configure(
                retry2["flow_id"], {"next_step_id": "strategy_transition_cancel"}
            )
            assert cancel["type"] is FlowResultType.CREATE_ENTRY, cancel
            # The durable proof survives cancel: axes kept, marker not restored.
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data
            assert RecoveryContract.from_entry_data(target.data) is not None
            assert _repair_sequences() - seq_before == 2
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()
        await service.stop()


@pytest.mark.timeout(120)
async def test_loaded_degraded_entry_repair_through_real_ha(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Batch 8B.2A loaded-lifecycle: a LOADED degraded entry is a NORMAL real
    scenario, repaired by the ONE bootstrap+proof orchestrator over the running
    runtime.

    The entry carries the recovery state in ``callback_restore_confirmed_unproven``
    (canonical strategy still inbound, endpoint external), goes through a REAL
    ``async_setup`` and becomes LOADED with a coordinator but NO live callback
    session (the collector is not connected). The options menu offers the repair;
    the repair runs Phase A + Phase B once, the RUNNING runtime owns the persist +
    single-reload boundary, and the entry stays LOADED and durably owned with the
    recovery marker cleared -- exactly two ``degraded_repair_bootstrap`` sequences,
    exactly one repair activation reload, no LOADED refusal."""

    from contextlib import ExitStack
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
    from custom_components.eybond_local.connection.strategy_transition_recovery import (
        RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
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
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            first_heartbeat_delay=1.5,
            set_29_mode="reboot_silent",
            reboot_reconnect_delay=0.3,
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
        callback_recovery_session_wait=12.0,
        callback_causality_lease_wait=3.0,
    )

    target = _target_entry(
        tcp_port=tcp_port,
        udp_port=udp_port,
        extra_data={"discovery_interval": 3600},
        recovery_phase=RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, fast_policy):
                stack.enter_context(p)

            # ---- the entry sets up for REAL and becomes LOADED -------------
            # No trigger has been sent, so the collector is NOT connected: the
            # runtime comes up with a coordinator but NO live callback session.
            # This is the genuine degraded-LOADED scenario, not a misuse.
            target.add_to_hass(hass)
            assert await hass.config_entries.async_setup(target.entry_id)
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert target.runtime_data is not None  # coordinator/runtime exist
            registry = get_callback_session_registry(hass)
            assert registry is not None
            # A documented PN-ownership INTENT may exist (the runtime registers its
            # own PN), but there is NO live callback session: no claimed session id
            # and no observed socket for the PN.
            assert registry.claimed_session_id(target.entry_id) == ""
            assert not any(
                s.collector_pn == FULL_PN
                for s in registry.observed_sessions_per_socket()
            )
            assert target.data["connection_strategy"] == "inbound"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE in target.data

            ledger = get_callback_trigger_ledger()

            # The ledger's ``_history`` is a process-global RING BUFFER (bounded,
            # evicts oldest); across a full test file its early entries are gone.
            # Count by monotonic GENERATION instead -- the repair's own sends are
            # the newest and are never evicted within a single test.
            def _seqs_since(gen: int, source: str) -> int:
                return sum(
                    1 for r in ledger._history
                    if r.source == source and r.generation > gen
                )

            gen_before = ledger.snapshot_generation()

            # ---- instrument the REAL suspend (unload) + activation (setup) --
            # The repair suspends the competing runtime with ONE unload, then
            # activates the proven entry with ONE setup while holding its
            # observed-listener lease. Any LATER reload is the fresh runtime's OWN
            # metadata-drift snapshot -- a distinct legitimate lifecycle.
            unload_calls: list = []
            setup_events: list = []
            orig_unload = hass.config_entries.async_unload
            orig_setup = hass.config_entries.async_setup

            async def _instrumented_unload(eid, *args, **kwargs):
                if eid == target.entry_id:
                    unload_calls.append(eid)
                return await orig_unload(eid, *args, **kwargs)

            async def _instrumented_setup(eid, *args, **kwargs):
                if eid != target.entry_id:
                    return await orig_setup(eid, *args, **kwargs)
                cert_sid = registry.claimed_session_id(target.entry_id)
                before = {
                    "sid": cert_sid,
                    "owner": registry.owner_for_pn(FULL_PN),
                    "observed": bool(cert_sid) and any(
                        s.session_id == cert_sid
                        for s in registry.observed_sessions_per_socket()
                    ),
                    "cert_reverifies": (
                        registry.session_handle_for_owned_session(
                            target.entry_id, cert_sid
                        ) is not None
                    ) if cert_sid else False,
                }
                rt_gen = ledger.snapshot_generation()
                ok = await orig_setup(eid, *args, **kwargs)
                setup_events.append({
                    "before": before,
                    "after": {
                        "setup_ok": bool(ok),
                        "owner": registry.owner_for_pn(FULL_PN),
                        "live_sid": registry.claimed_session_id(target.entry_id),
                        # runtime callback triggers fired DURING this setup call
                        "rt_during": sum(
                            1 for r in ledger._history
                            if r.source == "runtime_callback_on_demand"
                            and r.generation > rt_gen
                        ),
                    },
                })
                return ok

            stack.enter_context(
                patch.object(hass.config_entries, "async_unload", _instrumented_unload)
            )
            stack.enter_context(
                patch.object(hass.config_entries, "async_setup", _instrumented_setup)
            )

            # ---- the LOADED entry OFFERS the repair (no refusal) ----------
            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            assert result["type"] is FlowResultType.MENU, result
            assert "strategy_transition_repair" in result["menu_options"], result

            # ---- drive the repair ONLY through the HA options manager -----
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            assert result["type"] is FlowResultType.CREATE_ENTRY, result

            # ---- exactly two set>server sequences (A + B); one proof ------
            assert _seqs_since(gen_before, "degraded_repair_bootstrap") == 2, [
                (r.generation, r.source) for r in ledger._history
            ]

            # ---- proven commit, cleared marker, owned identity ------------
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data  # marker gone
            contract = RecoveryContract.from_entry_data(target.data)
            assert contract is not None and contract.callback_verified
            proof = contract.callback_proof
            assert proof.collector_pn == FULL_PN
            assert proof.identity_source == "fc2_parameter_2"
            assert f"127.0.0.1:{tcp_port}" in proof.advertised_ha_endpoint

            # ---- the repair's lifecycle: ONE suspend + ONE activation -----
            # The repair suspends the competing runtime (the FIRST unload) and
            # activates the proven entry with the FIRST setup. The commit is a
            # plain persist on the UNLOADED entry -- no update listener exists to
            # fire, so there is NO auto-reload + separate second activation, hence
            # the FIRST setup IS the activation. Any second unload/setup is the
            # fresh runtime's OWN metadata-drift reload (a distinct lifecycle),
            # never a repeated repair -- so the churn is bounded, never runaway.
            assert unload_calls[0] == target.entry_id, unload_calls
            assert 1 <= len(unload_calls) <= 2, unload_calls
            activation = setup_events[0]
            assert activation["after"]["setup_ok"] is True, setup_events

            # ---- item 5H: EXACT-session handoff, explicit ------------------
            # Before the proven runtime came up (repair still holding the lease):
            # the certified session is present, observed, and re-verifies.
            before, after = activation["before"], activation["after"]
            assert before["sid"], activation                       # non-empty
            assert before["observed"] is True, activation
            assert before["cert_reverifies"] is True, activation
            # After activation: the runtime adopted the EXACT certified session,
            # the owner is unchanged, and the activation itself fired NO runtime
            # callback trigger (the exact session made a reconnect unnecessary).
            assert after["live_sid"] == before["sid"], activation  # exact handoff
            assert after["owner"] == before["owner"] == target.entry_id, activation
            assert after["rt_during"] == 0, activation

            # ---- entry LOADED, durably owned, no foreign adoption, no repeat
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert registry.owner_for_pn(FULL_PN) == target.entry_id  # durable owner
            assert _seqs_since(gen_before, "degraded_repair_bootstrap") == 2  # no 2nd proof

            # ---- item 5: the runtime's own reconnect is bounded + not the repair
            # A post-commit ``runtime_callback_on_demand`` is the runtime's NORMAL
            # lifecycle: at most ONE logical sequence per runtime startup (setup),
            # never part of the repair matcher (whose bootstrap count stays two).
            reconnect_delta = _seqs_since(gen_before, "runtime_callback_on_demand")
            assert reconnect_delta <= len(setup_events), (
                reconnect_delta, len(setup_events)
            )
    finally:
        if target.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(target.entry_id)
            await hass.async_block_till_done()
        await service.stop()


async def _boot_and_load_degraded_target(
    hass, stack, integration, *, tcp_port, udp_port, fast_policy=None,
    repair_mod=None, sv_mod=None,
):
    """Boot domain services + a LOADED degraded target (no collector connected)."""
    from custom_components.eybond_local.connection.strategy_transition_recovery import (
        RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )

    for p in _loopback_patches(integration, repair_mod, sv_mod, fast_policy):
        stack.enter_context(p)
    boot = await _boot_domain_services(hass)
    target = _target_entry(
        tcp_port=tcp_port,
        udp_port=udp_port,
        extra_data={"discovery_interval": 3600},
        recovery_phase=RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )
    target.add_to_hass(hass)
    assert await hass.config_entries.async_setup(target.entry_id)
    await hass.async_block_till_done()
    assert target.state is ConfigEntryState.LOADED
    return boot, target


@pytest.mark.timeout(60)
async def test_loaded_repair_unload_returns_false_runs_nothing(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5B: async_unload returns False -> fail-closed suspend runs NOTHING
    (0 ensure, 0 UDP, 0 orchestrator/commit); the entry stays LOADED, untouched."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_passive_callback_discovery,
    )

    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration,
                tcp_port=_free_tcp_port(), udp_port=_free_tcp_port(),
            )
            original_data = dict(target.data)
            original_options = dict(target.options)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()
            discovery = get_passive_callback_discovery(hass)

            ensure_calls: list = []
            orig_ensure = discovery.async_ensure_observed_listener

            async def _spy_ensure(host, port):
                ensure_calls.append((host, port))
                return await orig_ensure(host, port)

            async def _refuse_unload(eid, *args, **kwargs):
                return False  # entry stays LOADED, no unload

            stack.enter_context(
                patch.object(discovery, "async_ensure_observed_listener", _spy_ensure)
            )
            stack.enter_context(
                patch.object(hass.config_entries, "async_unload", _refuse_unload)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)

            # Typed suspend-failed; entry untouched; NOTHING downstream ran.
            assert result["type"] is FlowResultType.MENU, result
            explanation = result["description_placeholders"]["failure_explanation"]
            assert "could not pause" in explanation.lower(), explanation
            assert target.state is ConfigEntryState.LOADED
            assert ensure_calls == []                              # 0 listener ensure
            assert ledger.snapshot_generation() == gen_before      # 0 UDP
            assert target.data == original_data                    # 0 commit
            assert target.options == original_options
            assert RecoveryContract.from_entry_data(target.data) is None
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()


@pytest.mark.timeout(60)
async def test_loaded_repair_unload_raises_partial_is_restored(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5C: async_unload raises after PARTIALLY unloading -> the entry is
    restored to LOADED and the repair never starts (0 UDP/commit)."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_passive_callback_discovery,
    )

    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration,
                tcp_port=_free_tcp_port(), udp_port=_free_tcp_port(),
            )
            original_data = dict(target.data)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()
            discovery = get_passive_callback_discovery(hass)

            ensure_calls: list = []
            orig_ensure = discovery.async_ensure_observed_listener

            async def _spy_ensure(host, port):
                ensure_calls.append((host, port))
                return await orig_ensure(host, port)

            orig_unload = hass.config_entries.async_unload

            async def _partial_then_raise(eid, *args, **kwargs):
                if eid == target.entry_id:
                    # Genuinely unload the entry, THEN fail: a partial/aborted
                    # unload leaving the entry NOT_LOADED.
                    await orig_unload(eid, *args, **kwargs)
                    raise RuntimeError("simulated partial unload failure")
                return await orig_unload(eid, *args, **kwargs)

            stack.enter_context(
                patch.object(discovery, "async_ensure_observed_listener", _spy_ensure)
            )
            stack.enter_context(
                patch.object(hass.config_entries, "async_unload", _partial_then_raise)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            await hass.async_block_till_done()

            # Restored to LOADED; the repair never began.
            assert result["type"] is FlowResultType.MENU, result
            assert target.state is ConfigEntryState.LOADED           # restored
            assert ensure_calls == []                                # 0 ensure
            assert ledger.snapshot_generation() == gen_before        # 0 UDP
            assert target.data == original_data                      # 0 commit
            assert RecoveryContract.from_entry_data(target.data) is None
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()


@pytest.mark.timeout(60)
async def test_loaded_repair_ensure_raises_after_suspend_restores(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5D: the listener ensure raises AFTER a successful suspend -> the entry
    is restored to LOADED with 0 UDP / 0 commit."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_passive_callback_discovery,
    )

    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration,
                tcp_port=_free_tcp_port(), udp_port=_free_tcp_port(),
            )
            original_data = dict(target.data)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()
            discovery = get_passive_callback_discovery(hass)

            async def _boom_ensure(host, port):
                raise RuntimeError("simulated ensure failure")

            stack.enter_context(
                patch.object(discovery, "async_ensure_observed_listener", _boom_ensure)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            await hass.async_block_till_done()

            # Suspended, ensure blew up, entry restored -- no UDP, no commit.
            assert result["type"] is FlowResultType.MENU, result
            assert target.state is ConfigEntryState.LOADED           # restored
            assert ledger.snapshot_generation() == gen_before        # 0 UDP
            assert target.data == original_data                      # 0 commit
            assert RecoveryContract.from_entry_data(target.data) is None
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()


@pytest.mark.timeout(90)
async def test_loaded_repair_orchestrator_raises_before_commit_restores(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5E: the orchestrator raises before commit -> the entry is restored and
    the observed-listener token is released (listener freed, lease freed)."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection.strategy_transition import (
        STRATEGY_TRANSITION_LEASES,
    )

    tcp_port = _free_tcp_port()
    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration,
                tcp_port=tcp_port, udp_port=_free_tcp_port(),
            )
            original_data = dict(target.data)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()

            async def _boom_orchestrator(*args, **kwargs):
                raise RuntimeError("simulated orchestrator failure")

            stack.enter_context(
                patch(
                    "custom_components.eybond_local.connection."
                    "strategy_transition_repair.async_run_degraded_recovery_repair",
                    _boom_orchestrator,
                )
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            await hass.async_block_till_done()

            # Restored; nothing committed; lease free.
            assert result["type"] is FlowResultType.MENU, result
            assert target.state is ConfigEntryState.LOADED           # restored
            assert ledger.snapshot_generation() == gen_before        # 0 UDP
            assert target.data == original_data                      # 0 commit
            assert RecoveryContract.from_entry_data(target.data) is None
            assert STRATEGY_TRANSITION_LEASES.acquire(target.entry_id)
            STRATEGY_TRANSITION_LEASES.release(target.entry_id)

            # The observed-listener token was released (not leaked): after the
            # runtime is unloaded, a bounded connect to the shared port is refused.
            async def _listener_open() -> bool:
                try:
                    _r, _w = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", tcp_port), timeout=0.3
                    )
                except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
                    return False
                _w.close()
                return True

            await hass.config_entries.async_unload(target.entry_id)
            await hass.async_block_till_done()
            closed = False
            for _ in range(30):
                if not await _listener_open():
                    closed = True
                    break
                await asyncio.sleep(0.1)
            assert closed, "observed-listener token leaked after failed repair"
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()


@pytest.mark.timeout(90)
async def test_loaded_repair_cancelled_after_suspend_restores_entry(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5A: a LOADED repair cancelled AFTER the suspend restores the entry to
    LOADED with byte-for-byte original data/options, no proof, cleanup complete."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection.strategy_transition import (
        STRATEGY_TRANSITION_LEASES,
    )
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
    )

    tcp_port = _free_tcp_port()
    udp_port = _free_tcp_port()
    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration, tcp_port=tcp_port, udp_port=udp_port,
                repair_mod=repair_mod, sv_mod=sv_mod,
            )
            original_data = dict(target.data)
            original_options = dict(target.options)

            # Hold the repair OPEN right after the suspend + ensure (before Phase
            # A / commit) with a real orchestrator that blocks, so the cancel
            # lands DETERMINISTICALLY on a suspended, pre-commit entry.
            started = asyncio.Event()
            release = asyncio.Event()

            async def _blocking_orchestrator(**kwargs):
                started.set()
                await release.wait()  # the test cancels instead of releasing
                raise AssertionError("unreachable")

            stack.enter_context(
                patch(
                    "custom_components.eybond_local.connection."
                    "strategy_transition_repair.async_run_degraded_recovery_repair",
                    _blocking_orchestrator,
                )
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            assert result["type"] is FlowResultType.SHOW_PROGRESS, result
            flow_id = result["flow_id"]

            # The suspend (unload) + ensure already ran before the orchestrator
            # blocked; the entry is now SUSPENDED. (No block_till_done here -- the
            # orchestrator task is deliberately held open.)
            await asyncio.wait_for(started.wait(), timeout=10)
            assert target.state is ConfigEntryState.NOT_LOADED  # suspended

            # Cancel mid-flight through the PUBLIC manager API. Drive the loop with
            # sleeps (not block_till_done, which would wait on the held task) so the
            # cancellation + the shielded finalization restore run to completion.
            options.async_abort(flow_id)
            restored = False
            for _ in range(100):
                await asyncio.sleep(0.1)
                if target.state is ConfigEntryState.LOADED:
                    restored = True
                    break

            assert restored, "cancelled LOADED repair never restored the entry"
            await hass.async_block_till_done()
            assert target.data == original_data          # byte-for-byte
            assert target.options == original_options
            assert CONF_STRATEGY_TRANSITION_STATE in target.data
            assert RecoveryContract.from_entry_data(target.data) is None
            # No leaked transition lease.
            assert STRATEGY_TRANSITION_LEASES.acquire(target.entry_id)
            STRATEGY_TRANSITION_LEASES.release(target.entry_id)
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()


def _repair_collector_service(tcp_port):
    from fake_collector import FakeCollectorService
    from fake_collector_lib import CollectorProfile, resolve_scenario

    return FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=0,
        tcp_bind_ip="127.0.0.1",
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=FULL_PN),
            first_heartbeat_delay=1.5,
            set_29_mode="reboot_silent",
            reboot_reconnect_delay=0.3,
            pi30_mode="success",
        ),
    )


def _repair_fast_policy():
    from custom_components.eybond_local.onboarding.timeouts import (
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
    )

    return replace(
        DEFAULT_ONBOARDING_TIMEOUT_POLICY,
        inbound_strong_identity_timeout=5.0,
        inbound_restart_disconnect_timeout=5.0,
        inbound_reconnect_timeout=1.5,
        callback_recovery_session_wait=12.0,
        callback_causality_lease_wait=3.0,
    )


@pytest.mark.timeout(120)
async def test_loaded_repair_cancelled_during_post_commit_setup(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5F: cancelling the flow DURING the post-commit activation completes
    that activation (shielded) -- the proof + canonical axes are saved, the old
    inbound data is NOT restored, and the entry is NOT left silently unloaded."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
    )

    tcp_port = _free_tcp_port()
    service = _repair_collector_service(tcp_port)
    await service.start()
    udp_port = int(service._udp_transport.get_extra_info("sockname")[1])
    fast_policy = _repair_fast_policy()
    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration, tcp_port=tcp_port, udp_port=udp_port,
                fast_policy=fast_policy, repair_mod=repair_mod, sv_mod=sv_mod,
            )

            activation_started = asyncio.Event()
            activation_proceed = asyncio.Event()
            orig_setup = hass.config_entries.async_setup

            async def _blocking_activation_setup(eid, *args, **kwargs):
                # The POST-COMMIT activation is the setup that runs while the entry
                # data is already callback_on_demand (the commit cleared inbound).
                if (
                    eid == target.entry_id
                    and target.data.get("connection_strategy") == "callback_on_demand"
                    and not activation_started.is_set()
                ):
                    activation_started.set()
                    await activation_proceed.wait()
                return await orig_setup(eid, *args, **kwargs)

            stack.enter_context(
                patch.object(
                    hass.config_entries, "async_setup", _blocking_activation_setup
                )
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            assert result["type"] is FlowResultType.SHOW_PROGRESS, result
            flow_id = result["flow_id"]

            # Wait until the proof committed and the activation began (blocked).
            await asyncio.wait_for(activation_started.wait(), timeout=40)
            # Proof is durably committed BEFORE activation.
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data
            assert RecoveryContract.from_entry_data(target.data) is not None

            # Cancel DURING the post-commit activation, then let it finish. The
            # shielded finalization must complete the activation.
            options.async_abort(flow_id)
            activation_proceed.set()
            loaded = False
            for _ in range(100):
                await asyncio.sleep(0.1)
                if target.state is ConfigEntryState.LOADED:
                    loaded = True
                    break

            # Entry NOT silently unloaded; proof + axes intact; NOT rolled back.
            assert loaded, "post-commit activation did not complete after cancel"
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data
            contract = RecoveryContract.from_entry_data(target.data)
            assert contract is not None and contract.callback_verified
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()
        await service.stop()


@pytest.mark.timeout(120)
async def test_proven_but_unloaded_entry_offers_load_only_retry(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5G: a proven-but-unloaded entry (after the flow closed) reopens with a
    LOAD-ONLY activation retry -- no Phase A/B, no UDP, no proof change."""

    from contextlib import ExitStack

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
    )

    tcp_port = _free_tcp_port()
    service = _repair_collector_service(tcp_port)
    await service.start()
    udp_port = int(service._udp_transport.get_extra_info("sockname")[1])
    fast_policy = _repair_fast_policy()
    boot = None
    target = None
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration, tcp_port=tcp_port, udp_port=udp_port,
                fast_policy=fast_policy, repair_mod=repair_mod, sv_mod=sv_mod,
            )
            ledger = get_callback_trigger_ledger()

            # ---- run the real repair to PROVEN + LOADED --------------------
            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)
            assert result["type"] is FlowResultType.CREATE_ENTRY, result
            await hass.async_block_till_done()
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert RecoveryContract.from_entry_data(target.data) is not None
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data

            # ---- the flow closed; now UNLOAD the proven entry --------------
            await hass.config_entries.async_unload(target.entry_id)
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.NOT_LOADED
            gen_proven = ledger.snapshot_generation()

            # ---- reopen options: a DEDICATED activation-only menu ----------
            result = await options.async_init(target.entry_id)
            assert result["type"] is FlowResultType.MENU, result
            # EXACT set: load retry + cancel only -- no runtime / shadow / Wi-Fi /
            # diagnostics / physical repair.
            assert list(result["menu_options"]) == [
                "strategy_transition_activation_retry",
                "strategy_transition_cancel",
            ], result["menu_options"]

            # ---- pick it: the entry LOADS, no Phase A/B, no UDP ------------
            result = await options.async_configure(
                result["flow_id"],
                {"next_step_id": "strategy_transition_activation_retry"},
            )
            result = await _drain_options(options, result, hass)
            assert result["type"] is FlowResultType.CREATE_ENTRY, result
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            # The RecoveryContract is unchanged and the recovery marker stays gone.
            contract = RecoveryContract.from_entry_data(target.data)
            assert contract is not None and contract.callback_verified
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data
            # No repair bootstrap since the entry became proven -- load only.
            assert sum(
                1 for r in ledger._history
                if r.source == "degraded_repair_bootstrap"
                and r.generation > gen_proven
            ) == 0
    finally:
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()
        await service.stop()


@pytest.mark.timeout(90)
async def test_loaded_repair_cancelled_inside_unload_restores_entry(
    hass: HomeAssistant, socket_enabled
) -> None:
    """Item 5.1: a cancel delivered INSIDE async_unload (after the entry actually
    left LOADED, before the suspend returned) still restores the entry to LOADED
    with byte-for-byte data/options, runs NOTHING downstream (0 ensure / UDP /
    orchestrator / commit), and the progress task ends with CancelledError."""

    from contextlib import ExitStack
    from unittest.mock import patch

    import custom_components.eybond_local as integration
    from custom_components.eybond_local.connection.callback_ledger import (
        get_callback_trigger_ledger,
    )
    from custom_components.eybond_local.connection.recovery_contract import (
        RecoveryContract,
    )
    from custom_components.eybond_local.connection import (
        strategy_transition_repair as repair_mod,
    )
    from custom_components.eybond_local.onboarding import (
        strategy_verification as sv_mod,
    )
    from custom_components.eybond_local.passive_discovery import (
        get_passive_callback_discovery,
    )

    boot = None
    target = None
    inside_unload = asyncio.Event()
    release_unload = asyncio.Event()
    try:
        with ExitStack() as stack:
            boot, target = await _boot_and_load_degraded_target(
                hass, stack, integration,
                tcp_port=_free_tcp_port(), udp_port=_free_tcp_port(),
                repair_mod=repair_mod, sv_mod=sv_mod,
            )
            original_data = dict(target.data)
            original_options = dict(target.options)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()
            discovery = get_passive_callback_discovery(hass)

            ensure_calls: list = []
            orig_ensure = discovery.async_ensure_observed_listener

            async def _spy_ensure(host, port):
                ensure_calls.append((host, port))
                return await orig_ensure(host, port)

            repair_tasks: list = []
            orig_create_task = hass.async_create_task

            def _capture(coro, *args, **kwargs):
                t = orig_create_task(coro, *args, **kwargs)
                if getattr(
                    getattr(coro, "cr_code", None), "co_name", ""
                ) == "_async_run_degraded_repair_task":
                    repair_tasks.append(t)
                return t

            orig_unload = hass.config_entries.async_unload

            async def _unload_then_block(eid, *args, **kwargs):
                if eid == target.entry_id and not inside_unload.is_set():
                    # REAL unload -> entry leaves LOADED, THEN block so a cancel
                    # can land INSIDE async_unload.
                    result = await orig_unload(eid, *args, **kwargs)
                    inside_unload.set()
                    await release_unload.wait()  # a cancel unblocks this
                    return result
                return await orig_unload(eid, *args, **kwargs)

            stack.enter_context(
                patch.object(discovery, "async_ensure_observed_listener", _spy_ensure)
            )
            stack.enter_context(patch.object(hass, "async_create_task", _capture))
            stack.enter_context(
                patch.object(hass.config_entries, "async_unload", _unload_then_block)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            assert result["type"] is FlowResultType.SHOW_PROGRESS, result
            flow_id = result["flow_id"]

            # The entry is genuinely unloaded, and async_unload is now blocked.
            await asyncio.wait_for(inside_unload.wait(), timeout=10)
            assert target.state is ConfigEntryState.NOT_LOADED

            # Cancel INSIDE the unload window. The cancel unblocks the wait, so the
            # suspend never returns to run the ensure/orchestrator/commit.
            options.async_abort(flow_id)
            restored = False
            for _ in range(100):
                await asyncio.sleep(0.1)
                if target.state is ConfigEntryState.LOADED:
                    restored = True
                    break

            assert restored, "entry not restored after a cancel inside async_unload"
            await hass.async_block_till_done()
            # Byte-for-byte original; NOTHING downstream ran.
            assert target.data == original_data
            assert target.options == original_options
            assert CONF_STRATEGY_TRANSITION_STATE in target.data
            assert RecoveryContract.from_entry_data(target.data) is None
            assert ensure_calls == []                              # 0 ensure
            assert ledger.snapshot_generation() == gen_before      # 0 UDP / commit
            # The progress task ended with CancelledError (not a normal completion).
            assert repair_tasks, "repair progress task was not captured"
            assert repair_tasks[0].cancelled()
    finally:
        release_unload.set()  # safety: never leave the patched unload blocked
        for entry in (target, boot):
            if entry is not None and entry.state is ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)
                await hass.async_block_till_done()
