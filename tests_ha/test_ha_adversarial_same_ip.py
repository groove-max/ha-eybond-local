"""Batch 8B.2B.2 -- real-HA adversarial same-IP / foreign-PN acceptance.

Drives the ACTUAL Home Assistant flow manager (options.async_init /
async_configure -- never a direct async_step_* call) end to end with MORE THAN
ONE collector at the SAME loopback peer IP, and proves the RecoveryContract
persistence layer that 8B.2B.1 deliberately left to this batch:

* A -- a foreign collector B sits in the baseline; the target A's fresh, silent
  FC=2 reconnect is the ONLY thing certified, written to the RecoveryContract and
  adopted by the runtime; B never enters proof/identity/ownership;
* B -- only a foreign PN answers -> typed identity mismatch; no commit, no
  contract, entry byte-for-byte unchanged, the suspended runtime restored;
* C -- two strong PNs appear in one fresh window -> typed ambiguity; no commit,
  entry restored, no foreign/ambiguous claims.

Nothing here builds a second matcher / transaction / registry / listener, injects
a listener through private discovery internals, hand-writes a RecoveryContract,
or pre-stamps a session with a full PN or strong identity. The foreign collector
is a REAL FakeCollectorService dialing into the ONE production shared listener;
its strong identity is read over the REAL FC=2 wire. Every async rendezvous has a
hard local deadline via an Event/barrier so a hang fails in seconds.
"""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import socket
import sys
from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[0]
for _path in (str(REPO_ROOT), str(HERE), str(REPO_ROOT / "tests" / "helpers")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# Reuse the ONE real-HA harness -- no second harness, matcher or registry.
from test_ha_strategy_transition_repair import (  # noqa: E402
    FULL_PN,
    _drain_options,
    _free_tcp_port,
    _loopback_patches,
    _target_entry,
)

from custom_components.eybond_local.const import (  # noqa: E402
    CONF_STRATEGY_TRANSITION_STATE,
)

# The foreign collector -- a different durable PN at the SAME loopback IP.
FOREIGN_PN = "V000405SYN94677058"


def _send_set_server(udp_port: int, advertised_host: str, advertised_port: int) -> None:
    """Fire ONE ``set>server=host:port;`` datagram -- the production redirect a
    collector reacts to -- so a fake collector dials into the shared listener."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.sendto(
            f"set>server={advertised_host}:{advertised_port};".encode("ascii"),
            ("127.0.0.1", int(udp_port)),
        )
    finally:
        sock.close()


def _fast_policy():
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


def _collector(udp_port, pn, *, silent=True, pi30_mode="success", peers=()):
    from fake_collector import FakeCollectorService
    from fake_collector_lib import CollectorProfile, resolve_scenario

    return FakeCollectorService(
        listen_ip="127.0.0.1",
        udp_port=udp_port,
        tcp_bind_ip="127.0.0.1",
        heartbeat_interval=2.0,
        connect_timeout=2.0,
        udp_reply="",
        scenario=resolve_scenario(
            preset="collector_only",
            profile=CollectorProfile(pn=pn),
            first_heartbeat_delay=3600.0 if silent else 0.1,
            set_29_mode="reboot_silent",
            reboot_reconnect_delay=0.3,
            pi30_mode=pi30_mode,
        ),
        nat_peer_scenarios=peers,
    )


async def _wait_for(predicate, *, deadline=6.0, interval=0.05):
    """Await a predicate with a HARD deadline (a hang fails, never stalls)."""
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline
    while loop.time() < end:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.mark.timeout(120)
async def test_scenario_a_foreign_baseline_then_target_success(
    hass: HomeAssistant, socket_enabled
) -> None:
    """A: foreign B baseline, target A fresh -> ONLY A is certified + persisted."""

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
    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
        get_passive_callback_discovery,
    )

    tcp_port = _free_tcp_port()
    a = _collector(0, FULL_PN)                       # target A (silent framed)
    b = _collector(0, FOREIGN_PN, silent=False)      # foreign B (heartbeats -> identifies)
    await a.start()
    await b.start()
    a_udp = int(a._udp_transport.get_extra_info("sockname")[1])
    b_udp = int(b._udp_transport.get_extra_info("sockname")[1])

    target = _target_entry(
        tcp_port=tcp_port, udp_port=a_udp, extra_data={"discovery_interval": 3600},
        recovery_phase=RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, _fast_policy()):
                stack.enter_context(p)

            target.add_to_hass(hass)
            assert await hass.config_entries.async_setup(target.entry_id)
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            registry = get_callback_session_registry(hass)
            discovery = get_passive_callback_discovery(hass)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()

            def _seqs(source):
                return sum(
                    1 for r in ledger._history
                    if r.source == source and r.generation > gen_before
                )

            # ---- BARRIER: seat foreign B in the baseline right after the ensure,
            # BEFORE the bootstrap captures its baseline + triggers A. B dials into
            # the SAME just-ensured production listener; no private injection.
            orig_ensure = discovery.async_ensure_observed_listener
            orig_release = discovery.async_release_observed_listener
            b_seated = {"session_id": "", "token": ""}
            released_tokens: list[str] = []

            async def _ensure_then_seat_b(host, port):
                token = await orig_ensure(host, port)
                before_ids = {
                    s.session_id for s in registry.observed_sessions_per_socket()
                }
                _send_set_server(b_udp, "127.0.0.1", tcp_port)
                assert await _wait_for(
                    lambda: any(
                        s.session_id not in before_ids
                        for s in registry.observed_sessions_per_socket()
                    )
                ), "foreign collector did not enter the baseline inventory"
                b_seated["session_id"] = next(
                    s.session_id
                    for s in registry.observed_sessions_per_socket()
                    if s.session_id not in before_ids
                )
                b_seated["token"] = token
                return token

            async def _release_observed(token):
                released_tokens.append(token)
                await orig_release(token)

            stack.enter_context(
                patch.object(
                    discovery, "async_ensure_observed_listener", _ensure_then_seat_b
                )
            )
            stack.enter_context(
                patch.object(
                    discovery, "async_release_observed_listener", _release_observed
                )
            )

            # exact-session handoff capture across the REAL activation setup.
            setup_events: list = []
            orig_setup = hass.config_entries.async_setup

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
                }
                rt_gen = ledger.snapshot_generation()
                ok = await orig_setup(eid, *args, **kwargs)
                setup_events.append({
                    "before": before,
                    "after": {
                        "live_sid": registry.claimed_session_id(target.entry_id),
                        "owner": registry.owner_for_pn(FULL_PN),
                        "rt_during": sum(
                            1 for r in ledger._history
                            if r.source == "runtime_callback_on_demand"
                            and r.generation > rt_gen
                        ),
                    },
                })
                return ok

            stack.enter_context(
                patch.object(hass.config_entries, "async_setup", _instrumented_setup)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            assert "strategy_transition_repair" in result["menu_options"], result
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)

            # ---- ONLY the target A is certified + durably persisted ----------
            assert result["type"] is FlowResultType.CREATE_ENTRY, result
            assert target.data["connection_strategy"] == "callback_on_demand"
            assert target.data["endpoint_control_policy"] == "external"
            assert CONF_STRATEGY_TRANSITION_STATE not in target.data  # marker gone
            contract = RecoveryContract.from_entry_data(target.data)
            assert contract is not None and contract.callback_verified
            proof = contract.callback_proof
            assert proof.collector_pn == FULL_PN                 # proof is A
            assert proof.identity_source == "fc2_parameter_2"    # strong wire read
            assert f"127.0.0.1:{tcp_port}" in proof.advertised_ha_endpoint
            # The persisted contract round-trips through its own parser.
            from custom_components.eybond_local.connection.recovery_contract import (
                RECOVERY_CONTRACT_KEY,
            )
            record = target.data[RECOVERY_CONTRACT_KEY]
            parsed = RecoveryContract.from_record(record)
            assert parsed is not None
            assert parsed.to_record() == record
            # B never entered the proof / entry identity.
            assert FOREIGN_PN not in str(target.data)

            # ---- exact-session handoff: A owned before AND after setup -------
            activation = setup_events[0]
            assert activation["before"]["sid"]                    # A certified
            assert activation["before"]["observed"] is True
            # Exact-session handoff: the runtime adopted the SAME certified A
            # session; the owner never changed, so setup must not send another
            # callback trigger.
            assert activation["after"]["live_sid"] == activation["before"]["sid"]
            assert activation["before"]["owner"] == target.entry_id
            assert activation["after"]["owner"] == target.entry_id
            assert activation["after"]["rt_during"] == 0          # exact session: no trigger

            # ---- ownership: A owned by the entry; B independent/unowned ------
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert registry.owner_for_pn(FULL_PN) == target.entry_id
            self_owner_b = registry.owner_for_pn(FOREIGN_PN)
            assert self_owner_b != target.entry_id  # B never owned by entry A
            assert self_owner_b == ""               # B unowned by ANYONE
            # The adopted/certified session is A's FRESH socket -- provably NOT the
            # baseline B socket (the runtime never handed itself the foreign peer).
            assert activation["after"]["live_sid"]
            assert activation["after"]["live_sid"] != b_seated["session_id"]
            assert b_seated["token"]
            assert released_tokens == [b_seated["token"]]

            # ---- exactly two repair sequences (Phase A + Phase B) -----------
            assert _seqs("degraded_repair_bootstrap") == 2, [
                (r.generation, r.source) for r in ledger._history
            ]
    finally:
        if target.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(target.entry_id)
            await hass.async_block_till_done()
        await a.stop()
        await b.stop()


@pytest.mark.timeout(120)
async def test_scenario_b_only_foreign_pn_answers_is_typed_mismatch(
    hass: HomeAssistant, socket_enabled
) -> None:
    """B: only a foreign PN answers -> typed identity mismatch. No commit, no
    contract, entry byte-for-byte unchanged, the suspended runtime restored."""

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
    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
        get_passive_callback_discovery,
    )

    tcp_port = _free_tcp_port()
    # The ONLY collector that answers reports the FOREIGN PN (the entry targets A).
    collector = _collector(0, FOREIGN_PN)
    await collector.start()
    udp_port = int(collector._udp_transport.get_extra_info("sockname")[1])

    target = _target_entry(
        tcp_port=tcp_port, udp_port=udp_port, extra_data={"discovery_interval": 3600},
        recovery_phase=RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, _fast_policy()):
                stack.enter_context(p)

            target.add_to_hass(hass)
            assert await hass.config_entries.async_setup(target.entry_id)
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            registry = get_callback_session_registry(hass)
            discovery = get_passive_callback_discovery(hass)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()
            original_data = deepcopy(dict(target.data))
            original_options = deepcopy(dict(target.options))
            repair_results: list = []
            ensured_tokens: list[str] = []
            released_tokens: list[str] = []

            orig_repair = repair_mod.async_run_degraded_recovery_repair
            orig_ensure = discovery.async_ensure_observed_listener
            orig_release = discovery.async_release_observed_listener

            async def _capture_repair(**kwargs):
                result = await orig_repair(**kwargs)
                repair_results.append(result)
                return result

            async def _capture_ensure(host, port):
                token = await orig_ensure(host, port)
                ensured_tokens.append(token)
                return token

            async def _capture_release(token):
                released_tokens.append(token)
                await orig_release(token)

            stack.enter_context(
                patch.object(
                    repair_mod,
                    "async_run_degraded_recovery_repair",
                    _capture_repair,
                )
            )
            stack.enter_context(
                patch.object(discovery, "async_ensure_observed_listener", _capture_ensure)
            )
            stack.enter_context(
                patch.object(discovery, "async_release_observed_listener", _capture_release)
            )

            def _seqs(source):
                return sum(
                    1 for r in ledger._history
                    if r.source == source and r.generation > gen_before
                )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            assert "strategy_transition_repair" in result["menu_options"], result
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)

            # ---- typed failure, NOT a success/commit ------------------------
            assert result["type"] is FlowResultType.MENU, result
            assert "strategy_transition_activation_retry" not in result["menu_options"]
            assert len(repair_results) == 1
            assert repair_results[0].failure_reason == "identity_mismatch"
            assert repair_results[0].phase == "bootstrap"

            # ---- entry byte-for-byte unchanged; NO contract; marker kept ----
            await hass.async_block_till_done()
            assert target.data == original_data
            assert target.options == original_options
            assert target.data["connection_strategy"] == "inbound"
            assert CONF_STRATEGY_TRANSITION_STATE in target.data          # marker kept
            assert RecoveryContract.from_entry_data(target.data) is None

            # ---- the suspended runtime is restored to LOADED ----------------
            assert target.state is ConfigEntryState.LOADED

            # ---- foreign B never claimed / handed off to entry A ------------
            assert registry.owner_for_pn(FOREIGN_PN) != target.entry_id
            assert registry.claimed_session_id(target.entry_id) == ""

            # ---- causality/listener leases freed; exactly ONE Phase A (no B) --
            assert ledger.causality_owner() == ""
            assert _seqs("degraded_repair_bootstrap") == 1
            assert ensured_tokens and released_tokens == ensured_tokens
    finally:
        if target.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(target.entry_id)
            await hass.async_block_till_done()
        await collector.stop()


@pytest.mark.timeout(120)
async def test_scenario_c_two_strong_pns_one_window_is_ambiguous(
    hass: HomeAssistant, socket_enabled
) -> None:
    """C: two strong PNs reconnect in ONE causal window (co-located NAT peers on
    the SAME set>server) -> typed ambiguity. No commit/proof/handoff; the entry is
    restored; no foreign/ambiguous claims survive.

    The companion :func:`test_scenario_c_ambiguity_is_socket_order_independent`
    proves the *decision* is order-free at the ONE matcher (which never reads
    peer IP or socket order), so this end-to-end case fixes a single orientation
    and asserts the fail-closed restore through the REAL flow deterministically --
    the silent-callback reboot/reconnect lifecycle makes "which socket lands in
    the fresh window first" a wire race, never an identity input."""
    primary_pn, peer_pn = FULL_PN, FOREIGN_PN

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
    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
        get_passive_callback_discovery,
    )

    tcp_port = _free_tcp_port()
    # Two independent collectors share the same loopback peer IP. The trigger
    # wrapper below models one NAT-side fan-out of the production redirect to
    # both UDP endpoints while the ledger still records one logical sequence.
    primary = _collector(0, primary_pn, pi30_mode="nak")
    peer = _collector(0, peer_pn, pi30_mode="nak")
    await primary.start()
    await peer.start()
    udp_port = int(primary._udp_transport.get_extra_info("sockname")[1])
    peer_udp_port = int(peer._udp_transport.get_extra_info("sockname")[1])

    target = _target_entry(  # the entry always targets A (FULL_PN)
        tcp_port=tcp_port, udp_port=udp_port, extra_data={"discovery_interval": 3600},
        recovery_phase=RECOVERY_PHASE_RESTORE_CONFIRMED_UNPROVEN,
    )
    try:
        with ExitStack() as stack:
            for p in _loopback_patches(integration, repair_mod, sv_mod, _fast_policy()):
                stack.enter_context(p)

            target.add_to_hass(hass)
            assert await hass.config_entries.async_setup(target.entry_id)
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            registry = get_callback_session_registry(hass)
            discovery = get_passive_callback_discovery(hass)
            ledger = get_callback_trigger_ledger()
            gen_before = ledger.snapshot_generation()
            original_data = deepcopy(dict(target.data))
            original_options = deepcopy(dict(target.options))
            repair_results: list = []
            ensured_tokens: list[str] = []
            released_tokens: list[str] = []

            orig_repair = repair_mod.async_run_degraded_recovery_repair
            orig_ensure = discovery.async_ensure_observed_listener
            orig_release = discovery.async_release_observed_listener

            async def _capture_repair(**kwargs):
                result = await orig_repair(**kwargs)
                repair_results.append(result)
                # Isolate rollback persistence from unrelated runtime metadata
                # learning: once ambiguity is terminal, both adversarial sockets
                # disappear before the suspended inbound runtime is restored.
                # Any entry-data change after this point is therefore the repair
                # lifecycle itself, not a legitimate fresh metadata snapshot.
                if not result.success:
                    await primary.stop()
                    await peer.stop()
                return result

            async def _capture_ensure(host, port):
                token = await orig_ensure(host, port)
                ensured_tokens.append(token)
                return token

            async def _capture_release(token):
                released_tokens.append(token)
                await orig_release(token)

            stack.enter_context(
                patch.object(
                    repair_mod,
                    "async_run_degraded_recovery_repair",
                    _capture_repair,
                )
            )
            stack.enter_context(
                patch.object(discovery, "async_ensure_observed_listener", _capture_ensure)
            )
            stack.enter_context(
                patch.object(discovery, "async_release_observed_listener", _capture_release)
            )

            # ---- BARRIER: after the repair's set>server, do not let the read run
            # until BOTH co-located sockets are present -- so both are in the SAME
            # fresh window (removes the socket-arrival race, never a sleep-as-proof).
            from custom_components.eybond_local.collector import discovery as disc_mod
            orig_trigger = disc_mod.async_send_callback_trigger

            async def _trigger_then_await_both(**kwargs):
                before_ids = {
                    s.session_id for s in registry.observed_sessions_per_socket()
                }
                await orig_trigger(**kwargs)
                _send_set_server(peer_udp_port, "127.0.0.1", tcp_port)
                assert await _wait_for(
                    lambda: len(
                        {
                            s.session_id
                            for s in registry.observed_sessions_per_socket()
                            if s.session_id not in before_ids
                        }
                    )
                    >= 2,
                    deadline=8.0,
                ), "both co-located collector sessions did not enter the fresh window"

            stack.enter_context(
                patch.object(disc_mod, "async_send_callback_trigger", _trigger_then_await_both)
            )

            options = hass.config_entries.options
            result = await options.async_init(target.entry_id)
            result = await options.async_configure(
                result["flow_id"], {"next_step_id": "strategy_transition_repair"}
            )
            result = await _drain_options(options, result, hass)

            # ---- typed ambiguity, NOT a success/commit ----------------------
            assert result["type"] is FlowResultType.MENU, result
            assert len(repair_results) == 1
            assert repair_results[0].failure_reason == "ambiguous"
            assert repair_results[0].phase == "bootstrap"

            # ---- no durable commit / proof / handoff; entry restored --------
            await hass.async_block_till_done()
            assert target.state is ConfigEntryState.LOADED
            assert target.data == original_data
            assert target.options == original_options
            assert target.data["connection_strategy"] == "inbound"
            assert CONF_STRATEGY_TRANSITION_STATE in target.data
            assert RecoveryContract.from_entry_data(target.data) is None

            # ---- no foreign/ambiguous claim bound; lease freed --------------
            assert registry.claimed_session_id(target.entry_id) == ""
            assert registry.owner_for_pn(FOREIGN_PN) != target.entry_id
            assert ledger.causality_owner() == ""
            assert sum(
                1
                for r in ledger._history
                if r.source == "degraded_repair_bootstrap"
                and r.generation > gen_before
            ) == 1
            assert ensured_tokens and released_tokens == ensured_tokens
    finally:
        if target.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(target.entry_id)
            await hass.async_block_till_done()
        await primary.stop()
        await peer.stop()


def test_scenario_c_ambiguity_is_socket_order_independent() -> None:
    """C (order-freedom): the SAME production matcher that the real-HA repair
    uses decides two distinct strong PNs at one peer IP identically regardless of
    which arrived first -- typed ambiguity, empty selection, no first/last/peer-IP
    tiebreak. This is the deterministic complement to the wire-raced end-to-end
    ambiguity above: the decision function never reads socket order or peer IP, so
    the co-located reconnect order can never become identity evidence."""

    from custom_components.eybond_local.onboarding.callback_matching import (
        MATCH_IDENTITY_AMBIGUOUS,
        match_callback_answer,
    )

    def _strong(sid, pn):
        # A session in the matcher's shape at the shared loopback peer IP. The
        # matcher is handed peer_ip on purpose -- it must not tip the decision.
        return {
            "session_id": sid,
            "state": "identified_strong",
            "has_strong_identity": True,
            "collector_pn": pn,
            "peer_ip": "127.0.0.1",
        }

    a = _strong("sA", FULL_PN)
    b = _strong("sB", FOREIGN_PN)
    for order in ([a, b], [b, a]):
        m = match_callback_answer(
            order,
            baseline_session_ids=set(),
            result_pn=FULL_PN,
            expected_pn=FULL_PN,
        )
        assert m.result == MATCH_IDENTITY_AMBIGUOUS, (
            [s["session_id"] for s in order],
            m.result,
        )
        assert m.session_id == ""  # no socket is selected as the winner
        assert not m.confirmed


def test_repair_flow_decision_path_has_no_framed_only_branch() -> None:
    """AT coverage (architectural): the real-HA repair flow that scenarios A/B/C
    drive decides certify / mismatch / ambiguous / restore from IDENTITY alone --
    it has NO framed-vs-AT branch.

    The wire-specific work is confined to the ONE session-pinned identity reader
    (guarded in test_cross_layer_architecture: callback identity re-derives no wire
    rule of its own, delegates to the session-handle authority, uses the shared
    neutral reader). The real AT DTUPN wire itself is exercised end-to-end by
    8B.2B.1's production-wire test
    (test_callback_bootstrap_production_wire.AtTextAdversarialWireTests). So framed
    FC=2 and AT DTUPN traverse a byte-identical DECISION path here; there is no
    AT-specific flow boundary that would warrant duplicating the heavy HA lifecycle
    (and this proof fails the moment such a boundary is introduced)."""

    import ast
    import inspect

    from custom_components.eybond_local import config_flow
    from custom_components.eybond_local.connection import strategy_transition_repair
    from custom_components.eybond_local.onboarding import callback_matching

    def _named_funcs(source, names):
        """All FunctionDef/AsyncFunctionDef nodes (incl. methods) whose name is in
        ``names`` -- a list so a duplicate name is never silently dropped."""
        out = []
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
                out.append((node.name, node))
        return out

    def _string_consts(node):
        return {
            n.value
            for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
        }

    def _get_keys(node):
        keys = set()
        for n in ast.walk(node):
            if (
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Attribute)
                and n.func.attr == "get"
                and n.args
                and isinstance(n.args[0], ast.Constant)
                and isinstance(n.args[0].value, str)
            ):
                keys.add(n.args[0].value)
        return keys

    # -- Guard 1: the matcher's per-session decision inputs are EXACTLY the
    #    identity/lifecycle closed set. No protocol_shape, no
    #    collector_identity_source, no peer_ip, no listener wire tag can tip it.
    m_src = inspect.getsource(callback_matching)
    matcher = dict(_named_funcs(m_src, {"match_callback_answer"}))["match_callback_answer"]
    assert _get_keys(matcher) == {
        "session_id",
        "state",
        "has_strong_identity",
        "collector_pn",
    }, _get_keys(matcher)

    # -- Guard 2: no wire-KIND value is compared ANYWHERE on the decision path.
    #    A framed-vs-AT branch can only exist by comparing one of these values;
    #    lifecycle/route tags ("closed", "route_identity_mismatch",
    #    "listener_port") and the faithfully-recorded identity_source label are
    #    NOT wire branches and are intentionally absent from this set.
    WIRE_KIND_VALUES = {
        "routed_framed",
        "routed_at_text",
        "eybond_framed",
        "eybond_framed_or_binary",
        "raw_tcp",
        "at_dtupn",
        "at_text",
        "framed_fc4",
        "raw_passthrough",
        "framed_collector_commands",
        "at_commands",
    }
    WIRE_KIND_IDENTIFIERS = {
        "WIRE_FRAMED",
        "WIRE_AT_TEXT",
        "WIRE_RAW_TCP",
        "ADAPTER_INVERTER_FRAMED_FC4",
        "ADAPTER_INVERTER_RAW_PASSTHROUGH",
        "ADAPTER_MANAGEMENT_FRAMED",
        "ADAPTER_MANAGEMENT_AT",
        "wire_framing",
        "transport_wire",
        "protocol_shape",
        "inverter_forward_adapter",
        "collector_management_adapter",
        "uses_framed_wire",
        "uses_at_text_wire",
    }
    surfaces = [
        (callback_matching, {"match_callback_answer"}),
        (
            strategy_transition_repair,
            {
                "_is_new_socket",
                "_wait_and_read",
                "_cold_bootstrap",
                "_map_match_failure",
                "_existing_live_owner_certification",
                "async_run_callback_bootstrap_transaction",
                "async_run_degraded_recovery_repair",
            },
        ),
        (
            config_flow,
            {
                "async_step_strategy_transition_repair",
                "async_step_strategy_transition_repair_progress",
                "async_step_strategy_transition_repair_result",
                "async_step_strategy_transition_activation_retry",
            },
        ),
    ]
    for module, names in surfaces:
        found = _named_funcs(inspect.getsource(module), names)
        seen = {name for name, _ in found}
        assert names <= seen, (module.__name__, names - seen)  # guard sees them all
        for name, node in found:
            leaked = _string_consts(node) & WIRE_KIND_VALUES
            assert not leaked, f"{module.__name__}.{name} branches on wire kind: {leaked}"
            identifiers = {
                n.id
                for n in ast.walk(node)
                if isinstance(n, ast.Name)
            } | {
                n.attr
                for n in ast.walk(node)
                if isinstance(n, ast.Attribute)
            }
            leaked_ids = identifiers & WIRE_KIND_IDENTIFIERS
            assert not leaked_ids, (
                f"{module.__name__}.{name} references wire authority: {leaked_ids}"
            )
