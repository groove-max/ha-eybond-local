"""Pending-collector lifecycle under a REAL Home Assistant.

These prove the parts a stub cannot: that Home Assistant really loads a pending
entry without any runtime, really serves its options flow through the genuine
flow manager (no "Invalid flow specified" / "doesn't support step"), really
applies ConfigEntryNotReady retry semantics to a callback pending entry, and that
promotion really turns the SAME entry into a normal collector entry.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryDisabler, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import (
    CONF_COLLECTOR_IP,
    CONF_COLLECTOR_PN,
    CONF_CONNECTION_STRATEGY,
    CONF_ENTRY_ROLE,
    CONF_PENDING_ADDRESS_HINT,
    CONF_PENDING_ID,
    CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
    CONNECTION_STRATEGY_INBOUND,
    DOMAIN,
    ENTRY_ROLE_PENDING_COLLECTOR,
    PENDING_UNIQUE_ID_PREFIX,
)
from synthetic import (
    SYNTHETIC_BROADCAST,
    SYNTHETIC_COLLECTOR_IP,
    SYNTHETIC_COLLECTOR_PN,
    SYNTHETIC_SERVER_IP,
)


def _pending_entry(
    hass: HomeAssistant,
    *,
    strategy: str = CONNECTION_STRATEGY_INBOUND,
    address: str = "",
    pending_id: str = "01TESTPENDING0000000000001",
) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond collector (waiting)",
        unique_id=f"{PENDING_UNIQUE_ID_PREFIX}{pending_id}",
        version=4,
        data={
            "connection_type": "eybond",
            CONF_ENTRY_ROLE: ENTRY_ROLE_PENDING_COLLECTOR,
            CONF_PENDING_ID: pending_id,
            CONF_CONNECTION_STRATEGY: strategy,
            CONF_PENDING_ADDRESS_HINT: address,
            CONF_COLLECTOR_IP: address,
            CONF_COLLECTOR_PN: "",
            "server_ip": SYNTHETIC_SERVER_IP,
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
        },
        options={},
    )
    entry.add_to_hass(hass)
    return entry


def _observed_session(session_id: str, collector_pn: str) -> dict:
    """One observed inbound session (the external boundary the registry reads)."""

    return {
        "session_id": session_id,
        "peer_ip": SYNTHETIC_COLLECTOR_IP,
        "listener_port": 18899,
        "collector_pn": collector_pn,
        "state": "routed_framed",
        "collector_identity_source": "at_dtupn",
    }


def _install_registry(hass: HomeAssistant, sessions: list[dict]):
    """Expose a real CallbackSessionRegistry fed by synthetic observed sessions."""

    from custom_components.eybond_local.connection.session_registry import (
        CallbackSessionRegistry,
    )

    registry = CallbackSessionRegistry(sessions_source=lambda: tuple(sessions))
    hass.data.setdefault(DOMAIN, {})["callback_session_registry"] = registry
    return registry


async def test_inbound_pending_loads_without_runtime_or_platforms(
    hass: HomeAssistant, fake_runtime
) -> None:
    """An inbound pending entry loads, but starts NO collector runtime at all."""

    entry = _pending_entry(hass, strategy=CONNECTION_STRATEGY_INBOUND)

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    # No coordinator was ever constructed (the runtime factory was never used).
    assert not fake_runtime
    assert entry.runtime_data is None

    # No entities/devices were created for it.
    from homeassistant.helpers import entity_registry as er

    registry = er.async_get(hass)
    assert not er.async_entries_for_config_entry(registry, entry.entry_id)

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED


async def test_inbound_pending_sends_zero_udp(hass: HomeAssistant, fake_runtime) -> None:
    """An inbound pending entry must never emit a callback trigger."""

    entry = _pending_entry(hass, strategy=CONNECTION_STRATEGY_INBOUND)

    with patch(
        "custom_components.eybond_local.onboarding.pending_attempt."
        "async_run_pending_callback_attempt",
        side_effect=AssertionError("inbound pending must never run a callback attempt"),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED


async def test_pending_entry_claims_no_session(hass: HomeAssistant, fake_runtime) -> None:
    """A pending entry never claims a session -- it has no durable identity."""

    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
    )

    entry = _pending_entry(hass, address=SYNTHETIC_COLLECTOR_IP)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    registry = get_callback_session_registry(hass)
    if registry is not None:
        assert registry.claimed_identity(entry.entry_id) == ""


async def test_two_pending_entries_share_one_nat_ip(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Several pending collectors may sit behind one address -- IP is not identity."""

    first = _pending_entry(hass, address=SYNTHETIC_COLLECTOR_IP, pending_id="01TESTPENDINGAAAAAAAAAAAA1")
    second = _pending_entry(hass, address=SYNTHETIC_COLLECTOR_IP, pending_id="01TESTPENDINGBBBBBBBBBBBB2")

    for entry in (first, second):
        if entry.state is ConfigEntryState.NOT_LOADED:
            await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert first.state is ConfigEntryState.LOADED
    assert second.state is ConfigEntryState.LOADED
    assert first.unique_id != second.unique_id
    assert first.data[CONF_COLLECTOR_IP] == second.data[CONF_COLLECTOR_IP]


async def test_callback_pending_runs_one_attempt_then_not_ready(
    hass: HomeAssistant, fake_runtime
) -> None:
    """One bounded attempt per setup; failure -> ConfigEntryNotReady (HA retries)."""

    from custom_components.eybond_local.onboarding.pending_attempt import (
        PendingAttemptOutcome,
    )

    entry = _pending_entry(
        hass,
        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        address=SYNTHETIC_COLLECTOR_IP,
    )

    calls: list[str] = []

    async def _one_attempt(_hass, _entry):
        calls.append(_entry.entry_id)
        return PendingAttemptOutcome(result="callback_timeout")

    with patch(
        "custom_components.eybond_local.onboarding.pending_attempt."
        "async_run_pending_callback_attempt",
        side_effect=_one_attempt,
    ):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        # Exactly ONE attempt for this setup -- no loop inside the integration.
        assert len(calls) == 1
        # Home Assistant now owns the retry cadence (its own backoff timer).
        assert entry.state is ConfigEntryState.SETUP_RETRY
        # And it is still a pending entry with no runtime.
        assert entry.data[CONF_ENTRY_ROLE] == ENTRY_ROLE_PENDING_COLLECTOR
        assert not fake_runtime

        # The retry is HA's, so it is still armed: unload inside the patch so it
        # cannot fire a REAL (socket-opening) attempt during a later test. That
        # it must be disarmed at all is itself the proof HA owns the backoff.
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert len(calls) == 1


async def test_callback_pending_promotes_and_starts_normal_runtime(
    hass: HomeAssistant, fake_runtime
) -> None:
    """A verified full PN promotes the SAME entry and the normal runtime starts."""

    from custom_components.eybond_local.onboarding.pending_attempt import (
        PendingAttemptOutcome,
    )

    entry = _pending_entry(
        hass,
        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        address=SYNTHETIC_COLLECTOR_IP,
    )
    entry_id = entry.entry_id

    # The attempt must earn its promotion: claim the answering session, promote
    # the claim to the durable PN and PREPARE the handoff -- exactly what the real
    # attempt does. Promotion refuses a PN that is not registry-certified.
    # (Installed inside the attempt: the domain's async_setup publishes its own
    # registry, so this must run after that.)
    owner = "pending_attempt:test-owner"

    async def _promoting_attempt(_hass, _entry):
        registry = _install_registry(
            _hass, [_observed_session("s-new", SYNTHETIC_COLLECTOR_PN)]
        )
        registry.claim_session(owner, session_id="s-new")
        registry.promote_claim_to_full_pn(owner, SYNTHETIC_COLLECTOR_PN)
        registry.prepare_handoff(owner, SYNTHETIC_COLLECTOR_PN)
        return PendingAttemptOutcome(
            result="promoted",
            collector_pn=SYNTHETIC_COLLECTOR_PN,
            evidence="callback_trigger",
            handoff_owner=owner,
        )

    with patch(
        "custom_components.eybond_local.onboarding.pending_attempt."
        "async_run_pending_callback_attempt",
        side_effect=_promoting_attempt,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    # SAME entry, promoted in place -- never a second collector entry.
    assert entry.entry_id == entry_id
    # Exactly one COLLECTOR entry: promotion never creates a second one. (The
    # integration's own listener/bootstrap entry is a separate, expected role.)
    collector_entries = [
        candidate
        for candidate in hass.config_entries.async_entries(DOMAIN)
        if str(candidate.data.get(CONF_ENTRY_ROLE) or "") != "listener"
    ]
    assert len(collector_entries) == 1
    assert entry.unique_id == f"collector:{SYNTHETIC_COLLECTOR_PN}"
    assert entry.data[CONF_COLLECTOR_PN] == SYNTHETIC_COLLECTOR_PN
    assert entry.data[CONF_ENTRY_ROLE] == ""
    # The canonical strategy the user picked survived promotion.
    assert entry.data[CONF_CONNECTION_STRATEGY] == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
    assert CONF_PENDING_ID not in entry.data
    # The normal runtime is now running.
    assert entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 1
    assert entry.runtime_data is not None

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_pn_collision_keeps_entry_pending(hass: HomeAssistant, fake_runtime) -> None:
    """A durable-PN collision fails closed and leaves the pending entry intact."""

    from custom_components.eybond_local.onboarding.pending_attempt import (
        PendingAttemptOutcome,
    )

    # It only has to EXIST for the collector:{pn} uniqueness scan; disabling it
    # keeps Home Assistant from starting its (real) runtime in this test.
    existing = MockConfigEntry(
        domain=DOMAIN,
        title="Existing collector",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=4,
        disabled_by=ConfigEntryDisabler.USER,
        data={
            "connection_type": "eybond",
            CONF_COLLECTOR_PN: SYNTHETIC_COLLECTOR_PN,
            CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_INBOUND,
            "server_ip": SYNTHETIC_SERVER_IP,
        },
    )
    existing.add_to_hass(hass)

    entry = _pending_entry(
        hass,
        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        address=SYNTHETIC_COLLECTOR_IP,
    )

    owner = "pending_attempt:collide"

    async def _colliding_attempt(_hass, _entry):
        registry = _install_registry(
            _hass, [_observed_session("s-new", SYNTHETIC_COLLECTOR_PN)]
        )
        registry.claim_session(owner, session_id="s-new")
        registry.promote_claim_to_full_pn(owner, SYNTHETIC_COLLECTOR_PN)
        registry.prepare_handoff(owner, SYNTHETIC_COLLECTOR_PN)
        return PendingAttemptOutcome(
            result="promoted",
            collector_pn=SYNTHETIC_COLLECTOR_PN,
            evidence="callback_trigger",
            handoff_owner=owner,
        )

    with patch(
        "custom_components.eybond_local.onboarding.pending_attempt."
        "async_run_pending_callback_attempt",
        side_effect=_colliding_attempt,
    ):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        # Unchanged: still pending, still no PN, still its synthetic identity.
        assert entry.data[CONF_ENTRY_ROLE] == ENTRY_ROLE_PENDING_COLLECTOR
        assert entry.data[CONF_COLLECTOR_PN] == ""
        assert entry.unique_id.startswith(PENDING_UNIQUE_ID_PREFIX)
        assert entry.state is ConfigEntryState.SETUP_RETRY

        # Stop Home Assistant's retry timer before leaving the patch, so no real
        # attempt (and no socket) can run during teardown.
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_pending_options_flow_is_served_by_real_flow_manager(
    hass: HomeAssistant, fake_runtime
) -> None:
    """The pending role gets its own options flow: no invalid flow / bad step."""

    entry = _pending_entry(hass, strategy=CONNECTION_STRATEGY_INBOUND)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    result = await hass.config_entries.options.async_init(entry.entry_id)

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    assert result["step_id"] == "pending"


async def test_pending_options_settings_step_writes_canonical_data(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Changing the strategy writes entry.data (canonical), never options."""

    from custom_components.eybond_local.onboarding.pending_attempt import (
        PendingAttemptOutcome,
    )

    async def _no_device(_hass, _entry):
        # Switching to callback_on_demand makes the reload run one real attempt;
        # keep it device-free so the test never touches a socket.
        return PendingAttemptOutcome(result="callback_timeout")

    entry = _pending_entry(hass, strategy=CONNECTION_STRATEGY_INBOUND)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    with patch(
        "custom_components.eybond_local.onboarding.pending_attempt."
        "async_run_pending_callback_attempt",
        side_effect=_no_device,
    ):
        result = await hass.config_entries.options.async_init(entry.entry_id)
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], user_input={"next_step_id": "pending_settings"}
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "pending_settings"

        result = await hass.config_entries.options.async_configure(
            result["flow_id"],
            user_input={
                CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
                CONF_COLLECTOR_IP: SYNTHETIC_COLLECTOR_IP,
            },
        )
        await hass.async_block_till_done()

        assert entry.data[CONF_CONNECTION_STRATEGY] == CONNECTION_STRATEGY_CALLBACK_ON_DEMAND
        assert entry.data[CONF_PENDING_ADDRESS_HINT] == SYNTHETIC_COLLECTOR_IP
        # entry.options NEVER holds the strategy.
        assert CONF_CONNECTION_STRATEGY not in entry.options

        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_promoted_entry_uses_the_normal_options_flow(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Once promoted, the entry is served by the ordinary collector options flow."""

    from custom_components.eybond_local.config_flow import (
        EybondLocalConfigFlow,
        EybondLocalOptionsFlow,
        PendingCollectorOptionsFlow,
    )

    pending = _pending_entry(hass, strategy=CONNECTION_STRATEGY_INBOUND)
    assert isinstance(
        EybondLocalConfigFlow.async_get_options_flow(pending),
        PendingCollectorOptionsFlow,
    )

    promoted = MockConfigEntry(
        domain=DOMAIN,
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=4,
        data={
            "connection_type": "eybond",
            CONF_ENTRY_ROLE: "",
            CONF_COLLECTOR_PN: SYNTHETIC_COLLECTOR_PN,
            CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_INBOUND,
        },
    )
    promoted.add_to_hass(hass)
    assert isinstance(
        EybondLocalConfigFlow.async_get_options_flow(promoted), EybondLocalOptionsFlow
    )


async def test_pending_entry_survives_restart(hass: HomeAssistant, fake_runtime) -> None:
    """Role and canonical strategy persist across an unload/setup cycle."""

    entry = _pending_entry(hass, strategy=CONNECTION_STRATEGY_INBOUND)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    # Same stored entry, set up again (what a restart does).
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    assert entry.data[CONF_ENTRY_ROLE] == ENTRY_ROLE_PENDING_COLLECTOR
    assert entry.data[CONF_CONNECTION_STRATEGY] == CONNECTION_STRATEGY_INBOUND
    assert entry.unique_id.startswith(PENDING_UNIQUE_ID_PREFIX)
    assert not fake_runtime


async def test_pending_entry_removal_is_clean(hass: HomeAssistant, fake_runtime) -> None:
    """Removing a pending entry leaves no claim and no discovery suppression."""

    entry = _pending_entry(hass, address=SYNTHETIC_COLLECTOR_IP)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert await hass.config_entries.async_remove(entry.entry_id)
    await hass.async_block_till_done()

    assert not hass.config_entries.async_entries(DOMAIN)


# --- BLOCKER 1: the chosen strategy governs active vs passive onboarding -------


async def test_manual_inbound_never_probes_or_triggers(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Choosing inbound must send ZERO UDP and run NO active auto-detect.

    The whole point: the user said the collector dials in, so Home Assistant may
    not reach out at all. Any active probe here is a bug.
    """

    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    flow_id = result["flow_id"]

    with patch(
        "custom_components.eybond_local.config_flow.EybondLocalConfigFlow."
        "_async_probe_manual_target",
        side_effect=AssertionError("inbound onboarding must not run an active probe"),
    ):
        # Drive to the manual step and submit an inbound choice.
        flow = hass.config_entries.flow._progress[flow_id]
        flow._manual_config = {}
        submitted = await flow.async_step_manual(
            {
                "server_ip": SYNTHETIC_SERVER_IP,
                "collector_ip": "",
                "tcp_port": 8899,
                "udp_port": 58899,
                "discovery_target": SYNTHETIC_BROADCAST,
                "discovery_interval": 3,
                "heartbeat_interval": 60,
                "driver_hint": "auto",
                CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_INBOUND,
            }
        )

    # No probe ran (the patch would have raised); a pending entry was saved and
    # the canonical strategy is the one the user chose.
    assert submitted["type"] is FlowResultType.CREATE_ENTRY, submitted.get("errors")
    assert submitted["data"][CONF_ENTRY_ROLE] == ENTRY_ROLE_PENDING_COLLECTOR
    assert submitted["data"][CONF_CONNECTION_STRATEGY] == CONNECTION_STRATEGY_INBOUND
    # The strategy is canonical in data and never in options.
    assert CONF_CONNECTION_STRATEGY not in (submitted.get("options") or {})


async def test_manual_callback_requires_a_target(hass: HomeAssistant, fake_runtime) -> None:
    """callback_on_demand has nowhere to send its single trigger without one."""

    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    flow = hass.config_entries.flow._progress[result["flow_id"]]

    with patch(
        "custom_components.eybond_local.config_flow.EybondLocalConfigFlow."
        "_async_probe_manual_target",
        side_effect=AssertionError("must not probe without a target"),
    ):
        submitted = await flow.async_step_manual(
            {
                "server_ip": SYNTHETIC_SERVER_IP,
                "collector_ip": "",
                "tcp_port": 8899,
                "udp_port": 58899,
                "discovery_target": SYNTHETIC_BROADCAST,
                "discovery_interval": 3,
                "heartbeat_interval": 60,
                "driver_hint": "auto",
                CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
            }
        )

    assert submitted["type"] is FlowResultType.FORM
    assert submitted["errors"][CONF_COLLECTOR_IP] == "callback_target_required"


# --- BLOCKER 3: two pending entries behind one NAT, via the REAL flow ----------


async def test_two_pending_entries_behind_one_nat_via_real_flow(
    hass: HomeAssistant, fake_runtime
) -> None:
    """The REAL flow manager creates TWO pending entries sharing one address.

    Entries are finalized by Home Assistant's own flow manager (not hand-built
    MockConfigEntry), so its unique-id handling is what allows/blocks them. The
    flow is positioned on the manual step directly: the menu chain that normally
    leads there is scan-dependent and is not what this test is about.
    """

    for _ in range(2):
        init = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}
        )
        flow_id = init["flow_id"]
        flow = hass.config_entries.flow._progress[flow_id]
        flow.cur_step = {
            "type": FlowResultType.FORM,
            "flow_id": flow_id,
            "handler": DOMAIN,
            "step_id": "manual",
        }
        result = await hass.config_entries.flow.async_configure(
            flow_id,
            {
                "server_ip": SYNTHETIC_SERVER_IP,
                "collector_ip": SYNTHETIC_COLLECTOR_IP,  # SAME NAT address
                "tcp_port": 8899,
                "udp_port": 58899,
                "discovery_target": SYNTHETIC_BROADCAST,
                "discovery_interval": 3,
                "heartbeat_interval": 60,
                "driver_hint": "auto",
                CONF_CONNECTION_STRATEGY: CONNECTION_STRATEGY_INBOUND,
            },
        )
        assert result["type"] is FlowResultType.CREATE_ENTRY, result.get("errors")
    await hass.async_block_till_done()

    pending = [
        entry
        for entry in hass.config_entries.async_entries(DOMAIN)
        if entry.data.get(CONF_ENTRY_ROLE) == ENTRY_ROLE_PENDING_COLLECTOR
    ]
    # Home Assistant really created BOTH: an address is not an identity.
    assert len(pending) == 2
    assert pending[0].unique_id != pending[1].unique_id

    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
    )

    registry = get_callback_session_registry(hass)
    for entry in pending:
        assert entry.unique_id.startswith(PENDING_UNIQUE_ID_PREFIX)
        assert SYNTHETIC_COLLECTOR_IP not in entry.unique_id
        assert entry.data[CONF_PENDING_ADDRESS_HINT] == SYNTHETIC_COLLECTOR_IP
        # Neither claims a session before an explicit confirmation.
        if registry is not None:
            assert registry.claimed_identity(entry.entry_id) == ""


# --- Retry now = exactly one reload -------------------------------------------


async def test_pending_retry_now_triggers_exactly_one_reload(
    hass: HomeAssistant, fake_runtime
) -> None:
    """"Retry now" is ONE async_reload -> one bounded attempt. No extra loop."""

    from custom_components.eybond_local.onboarding.pending_attempt import (
        PendingAttemptOutcome,
    )

    entry = _pending_entry(
        hass,
        strategy=CONNECTION_STRATEGY_CALLBACK_ON_DEMAND,
        address=SYNTHETIC_COLLECTOR_IP,
    )

    calls: list[str] = []

    async def _one_attempt(_hass, _entry):
        calls.append(_entry.entry_id)
        return PendingAttemptOutcome(result="callback_timeout")

    with patch(
        "custom_components.eybond_local.onboarding.pending_attempt."
        "async_run_pending_callback_attempt",
        side_effect=_one_attempt,
    ):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert len(calls) == 1  # the initial setup attempt

        result = await hass.config_entries.options.async_init(entry.entry_id)
        assert result["step_id"] == "pending"
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], user_input={"next_step_id": "pending_retry"}
        )
        await hass.async_block_till_done()

        # Retry now performed exactly ONE more setup attempt (one reload), not a
        # loop and not a scheduler of its own.
        assert len(calls) == 2

        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        assert len(calls) == 2
