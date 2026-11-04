"""Config flow and options flow driven by the REAL Home Assistant flow manager.

These catch the class of bug the stub suite structurally cannot: unknown handler,
invalid flow, and `Handler ... doesn't support step ...`.
"""

from __future__ import annotations

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.eybond_local.const import (
    CONF_AUTOMATIC_PASSIVE_DISCOVERY,
    CONF_ENTRY_ROLE,
    DOMAIN,
    ENTRY_ROLE_LISTENER,
)
from custom_components.eybond_local.runtime.manager import RuntimeInverterCandidate

from synthetic import (
    SYNTHETIC_COLLECTOR_IP,
    SYNTHETIC_COLLECTOR_PN,
    SYNTHETIC_SERVER_IP,
)


async def test_user_config_flow_reaches_first_step(hass: HomeAssistant) -> None:
    """The real flow manager starts our user flow and returns a usable step.

    No network discovery is performed: the host-interface boundary is faked and
    the first step is a form/menu that asks the user something.
    """

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    # Real HA rejects an unknown handler before we get here; reaching a step also
    # proves the declared step exists on the handler.
    assert result["step_id"]
    assert result.get("errors") in (None, {})


async def test_user_flow_step_is_servable_twice(hass: HomeAssistant) -> None:
    """Re-entering the flow is stable (no handler/step registration drift)."""

    first = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    second = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )

    assert first["step_id"] == second["step_id"]
    for flow in hass.config_entries.flow.async_progress():
        hass.config_entries.flow.async_abort(flow["flow_id"])


async def test_unknown_step_is_not_silently_accepted(hass: HomeAssistant) -> None:
    """Guard the guard: the real flow manager rejects a nonexistent step."""

    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    with pytest.raises(Exception):
        await hass.config_entries.flow.async_configure(
            result["flow_id"], user_input=None, step_id="definitely_not_a_step"
        )


async def test_existing_background_discovery_has_specific_abort(
    hass: HomeAssistant,
) -> None:
    """The real HA unique-id guard must not replace the listener UX message."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Local — Discovery",
        unique_id=f"{DOMAIN}:listener",
        version=3,
        data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "listener"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "background_discovery_refreshed"


@pytest.fixture
def collector_entry(hass: HomeAssistant) -> MockConfigEntry:
    """A real, fully-formed collector entry with a synthetic durable identity."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Collector",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        version=3,
        data={
            "connection_type": "eybond",
            "connection_mode": "known_ip",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
            "control_mode": "read_only",
            "connection_strategy": "callback_on_demand",
            "endpoint_control_policy": "external",
            "proxy_enabled": False,
        },
        options={"poll_interval": 30, "poll_mode": "auto"},
    )
    entry.add_to_hass(hass)
    return entry


async def test_options_flow_reaches_first_step(
    hass: HomeAssistant, collector_entry: MockConfigEntry
) -> None:
    """The real flow manager serves our options flow's first step.

    This is what catches `Handler ... doesn't support step ...`: HA resolves the
    options handler from the entry and invokes its declared initial step.
    """

    result = await hass.config_entries.options.async_init(collector_entry.entry_id)

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    assert result["step_id"]


async def test_listener_entry_gets_its_own_options_flow(hass: HomeAssistant) -> None:
    """The listener role resolves to its dedicated options handler, for real."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Local — Discovery",
        unique_id=f"{DOMAIN}:listener",
        version=3,
        data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)

    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)
    assert result["step_id"]


async def test_listener_automatic_discovery_option_persists_in_real_ha(
    hass: HomeAssistant,
) -> None:
    """The listener option flows through Home Assistant's options manager."""

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond Local — Discovery",
        unique_id=f"{DOMAIN}:listener",
        version=3,
        data={CONF_ENTRY_ROLE: ENTRY_ROLE_LISTENER},
    )
    entry.add_to_hass(hass)

    menu = await hass.config_entries.options.async_init(entry.entry_id)
    assert menu["step_id"] == "listener"
    assert "automatic_discovery" in menu["menu_options"]

    form = await hass.config_entries.options.async_configure(
        menu["flow_id"], {"next_step_id": "automatic_discovery"}
    )
    assert form["type"] is FlowResultType.FORM
    assert form["step_id"] == "automatic_discovery"

    result = await hass.config_entries.options.async_configure(
        menu["flow_id"],
        user_input={CONF_AUTOMATIC_PASSIVE_DISCOVERY: False},
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_AUTOMATIC_PASSIVE_DISCOVERY] is False


async def test_options_flow_entry_is_not_loaded_requirement(
    hass: HomeAssistant, collector_entry: MockConfigEntry
) -> None:
    """Options flow init does not require a started runtime (no device needed)."""

    assert collector_entry.state is ConfigEntryState.NOT_LOADED
    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert result["type"] in (FlowResultType.FORM, FlowResultType.MENU)


@pytest.mark.parametrize("flag_case", ["profile", "lower", "upper"])
async def test_pi30_flag_case_survives_real_options_setup_and_reload_without_writes(
    hass, collector_entry, fake_runtime, monkeypatch, flag_case,
):
    from unittest.mock import AsyncMock
    from conftest import FakeRuntimeManager

    received = []
    original_init = FakeRuntimeManager.__init__

    def initialize(self, *args, **kwargs):
        received.append(kwargs["pi30_flag_command_case"])
        original_init(self, *args, **kwargs)

    write = AsyncMock(side_effect=AssertionError("Saving a dialect must not write inverter settings"))
    monkeypatch.setattr(FakeRuntimeManager, "__init__", initialize)
    monkeypatch.setattr(FakeRuntimeManager, "async_write_capability", write)
    hass.config_entries.async_update_entry(collector_entry, data={
        **collector_entry.data, "detected_driver": "pi30", "detected_model": "PI30 3000",
    })
    flow = await hass.config_entries.options.async_init(collector_entry.entry_id)
    flow = await hass.config_entries.options.async_configure(
        flow["flow_id"], user_input={"next_step_id": "runtime"},
    )
    assert flow["step_id"] == "runtime"
    assert "pi30_flag_command_case" in flow["data_schema"].schema
    flow = await hass.config_entries.options.async_configure(flow["flow_id"], user_input={
        "driver_hint": "auto", "driver_detection_strategy": "first_match",
        "control_mode": "read_only", "poll_mode": "auto",
        "pi30_flag_command_case": flag_case,
        "connection": {
            "server_ip": SYNTHETIC_SERVER_IP, "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "tcp_port": 8899, "udp_port": 58899, "discovery_target": SYNTHETIC_COLLECTOR_IP,
            "discovery_interval": 3, "heartbeat_interval": 60,
        },
    })
    assert flow["type"] is FlowResultType.CREATE_ENTRY
    assert collector_entry.options["pi30_flag_command_case"] == flag_case
    assert collector_entry.options["control_mode"] == "read_only"
    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    assert await hass.config_entries.async_reload(collector_entry.entry_id)
    await hass.async_block_till_done()
    assert received == [flag_case, flag_case]
    write.assert_not_awaited()


@pytest.mark.parametrize("provider", ["smartess", ""])
async def test_smartclient_read_only_analysis_completes_without_inverter(
    hass: HomeAssistant, collector_entry: MockConfigEntry, provider: str, monkeypatch,
) -> None:
    """Real HA completes collector-only analysis, not just source selection."""

    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from custom_components.eybond_local.models import RuntimeSnapshot
    from custom_components.eybond_local.smartclient_cloud import SmartClientEvidence, SmartClientIdentity
    from custom_components.eybond_local.support import smartclient_learning
    from custom_components.eybond_local.support.package import build_shadow_learning_runtime_values

    snapshot = RuntimeSnapshot(connected=True)

    def publish(**kwargs):
        values = build_shadow_learning_runtime_values(**kwargs)
        snapshot.values.update(values)
        return values["shadow_learning_artifacts"]

    bundle = SmartClientEvidence(
        identity=SmartClientIdentity(SYNTHETIC_COLLECTOR_PN, "SYNTHETIC", 567, 1),
        telemetry=({"field_id": "2", "title": "PV Voltage", "unit": "V", "value": "220.5"},),
        controls=({"field_id": "boot", "title": "Startup", "unit": "", "choices": []},),
        device_info={}, history={}, raw_packet={}, unavailable_actions=(), action_errors=(),
        fetched_at="2026-09-09T09:00:00+00:00",
    )
    monkeypatch.setattr(smartclient_learning, "fetch_read_only_evidence", lambda **_kwargs: bundle)
    start_route, stop_route = AsyncMock(), AsyncMock()

    collector_entry.runtime_data = SimpleNamespace(
        data=snapshot,
        cloud_evidence_provider=provider,
        smartess_collector_pn=SYNTHETIC_COLLECTOR_PN,
        publish_shadow_learning_artifacts=publish,
        async_start_shadow_learning=start_route,
        async_stop_shadow_learning=stop_route,
    )
    before_data = dict(collector_entry.data)
    before_options = dict(collector_entry.options)
    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    flow_id = result["flow_id"]
    result = await hass.config_entries.options.async_configure(
        flow_id, {"next_step_id": "cloud_tools"},
    )
    result = await hass.config_entries.options.async_configure(
        flow_id, {"next_step_id": "shadow_learning"},
    )
    if result["step_id"] == "shadow_learning":
        result = await hass.config_entries.options.async_configure(
            flow_id, {"learning_method": "read_only_evidence"},
        )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "shadow_learning_source"
    selector = next(iter(result["data_schema"].schema.values()))
    assert any(option["value"] == "smartclient" for option in selector.config["options"])
    result = await hass.config_entries.options.async_configure(
        flow_id, user_input={"learning_source": "smartclient"},
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "shadow_learning_credentials"
    assert result.get("errors") in (None, {})
    assert "SmartClient" in str(result["description_placeholders"])
    result = await hass.config_entries.options.async_configure(
        flow_id, user_input={"username": "synthetic-user", "password": "synthetic-secret"},
    )
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    await hass.async_block_till_done()
    result = await hass.config_entries.options.async_configure(flow_id)
    if result["type"] is FlowResultType.SHOW_PROGRESS_DONE:
        result = await hass.config_entries.options.async_configure(flow_id)
    assert result["step_id"] == "shadow_learning_review"
    orchestration = snapshot.values["shadow_learning_orchestration"]
    assert orchestration["source"] == "smartclient"
    assert orchestration["metadata_field_count"] == 2
    assert orchestration["planned_write_count"] == 0
    evidence = orchestration["metadata_evidence"]
    assert evidence["semantic_report"]["read_candidate_count"] > 0
    assert "local_coverage" not in evidence
    assert len(evidence["control_fields"]) == 1
    assert "synthetic-user" not in str(snapshot.values)
    assert "synthetic-secret" not in str(snapshot.values)
    start_route.assert_not_awaited()
    stop_route.assert_not_awaited()
    assert collector_entry.data == before_data
    assert collector_entry.options == before_options
    hass.config_entries.options.async_abort(flow_id)


async def test_runtime_options_commit_strategy_to_data_with_one_reload(
    hass: HomeAssistant, collector_entry: MockConfigEntry, fake_runtime
) -> None:
    """Polling stays a plain options update with exactly one reload.

    The dedicated connection/profile step is the only UX entry into a verified
    strategy transition. The runtime form does not carry that field at all.
    """

    from custom_components.eybond_local.connection.connection_policy import (
        resolve_connection_strategy,
    )

    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    assert collector_entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 1
    assert collector_entry.runtime_data._suppress_entry_reload_count == 0

    current_strategy = resolve_connection_strategy(
        collector_entry.data, collector_entry.options
    )

    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert result["type"] is FlowResultType.MENU
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"next_step_id": "runtime"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "runtime"

    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
            {
                "driver_hint": "auto",
                "poll_mode": "auto",
                "control_mode": "read_only",
                "connection": {
                "server_ip": SYNTHETIC_SERVER_IP,
                "collector_ip": SYNTHETIC_COLLECTOR_IP,
                "tcp_port": 8899,
                "udp_port": 58899,
                    "discovery_target": "192.0.2.255",
                    "discovery_interval": 3,
                    "heartbeat_interval": 60,
                },
            },
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()

    assert collector_entry.data["connection_strategy"] == current_strategy
    assert "connection_strategy" not in collector_entry.options
    assert collector_entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 2, "runtime options must schedule exactly one reload"

    await hass.config_entries.async_unload(collector_entry.entry_id)
    await hass.async_block_till_done()


async def test_runtime_ambiguity_is_resolved_post_entry_with_one_reload(
    hass: HomeAssistant, collector_entry: MockConfigEntry, fake_runtime
) -> None:
    """A real HA options flow stores intent; runtime facts remain unproven."""

    assert await hass.config_entries.async_setup(collector_entry.entry_id)
    await hass.async_block_till_done()
    fake_runtime[0].inverter_protocol_candidates = (
        RuntimeInverterCandidate(
            driver_key="smartess_local",
            protocol_family="0925",
            model_name="Hybrid 5K",
            serial_number="12345",
        ),
        RuntimeInverterCandidate(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="Hybrid 5K",
            serial_number="12345",
        ),
    )

    result = await hass.config_entries.options.async_init(collector_entry.entry_id)
    assert result["type"] is FlowResultType.MENU
    assert "inverter_protocol" in result["menu_options"]
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"next_step_id": "inverter_protocol"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "inverter_protocol"
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"driver_hint": "pi30"}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()

    assert collector_entry.options["driver_hint"] == "pi30"
    assert collector_entry.data["detected_driver"] == ""
    assert collector_entry.data["detected_model"] == ""
    assert collector_entry.data["detected_serial"] == ""
    assert collector_entry.data["detection_confidence"] == "none"
    assert collector_entry.state is ConfigEntryState.LOADED
    assert len(fake_runtime) == 2, "the protocol choice must cause exactly one reload"

    await hass.config_entries.async_unload(collector_entry.entry_id)
    await hass.async_block_till_done()


async def test_terminal_create_hands_prepared_claim_to_real_entry_setup(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Batch 5 terminal coordinator under REAL HA: flow claim -> prepare once ->
    CREATE_ENTRY -> the real ``async_setup_entry`` completes exactly this
    handoff (no unowned window, no PN re-lookup)."""

    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
    )

    # Boot the domain services (the ownership registry) with a first entry.
    boot = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond boot",
        unique_id="collector:E5000099990003",
        data={
            "connection_type": "eybond",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": "E5000099990003",
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
        },
        options={},
    )
    boot.add_to_hass(hass)
    assert await hass.config_entries.async_setup(boot.entry_id)
    await hass.async_block_till_done()
    registry = get_callback_session_registry(hass)
    assert registry is not None

    # A live observed session for the collector this flow verified (the REAL
    # registry instance; only its inventory source is synthetic).
    session_id = "listener-8899-77"
    registry.sessions_source = lambda: (
        {
            "session_id": session_id,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "state": "routed_framed",
            "collector_identity_source": "fc2_parameter_2",
            "listener_port": 8899,
            "raw": {"session_id": session_id, "protocol_shape": "eybond_framed"},
        },
    )

    # The REAL flow handler from the REAL manager, holding its verification
    # claim exactly like a successful inbound verification leaves it.
    init = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    flow = hass.config_entries.flow._progress[init["flow_id"]]
    assert flow._callback_continuation.adopt_passive_inbound_identity(
        SYNTHETIC_COLLECTOR_PN, session_id
    )
    owner = flow._callback_continuation.owner

    entry_data = {
        "connection_type": "eybond",
        "server_ip": SYNTHETIC_SERVER_IP,
        "collector_ip": "",
        "collector_pn": SYNTHETIC_COLLECTOR_PN,
        "tcp_port": 8899,
        "udp_port": 58899,
        "driver_hint": "auto",
        "connection_strategy": "inbound",
    }
    result = flow._create_entry_with_handoff(
        SYNTHETIC_COLLECTOR_PN,
        lambda: flow.async_create_entry(
            title="EyeBond collector", data=entry_data, options={}
        ),
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    # The prepared handoff survived the terminal, awaiting entry setup.
    assert (
        registry.prepared_handoff_identity(owner, SYNTHETIC_COLLECTOR_PN)
        == SYNTHETIC_COLLECTOR_PN
    )
    hass.config_entries.flow.async_abort(init["flow_id"])

    # Materialize the created entry and run the REAL setup: it must complete
    # exactly this committed handoff -- the identity is never re-claimed by PN
    # while the flow's owner still holds it. (The local-IP probe inside the
    # runtime link is an OS/network boundary -- faked like the interface scan.)
    from unittest.mock import patch

    entry = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond collector",
        unique_id=f"collector:{SYNTHETIC_COLLECTOR_PN}",
        data=dict(result["data"]),
        options={},
    )
    entry.add_to_hass(hass)
    with patch(
        "custom_components.eybond_local.runtime.link.common._default_local_ip",
        return_value=SYNTHETIC_SERVER_IP,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert registry.owner_for_pn(SYNTHETIC_COLLECTOR_PN) == entry.entry_id
    assert registry.claimed_session_id(entry.entry_id) == session_id

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.config_entries.async_unload(boot.entry_id)
    await hass.async_block_till_done()


async def test_terminal_exception_rolls_back_only_this_flows_claim(
    hass: HomeAssistant, fake_runtime
) -> None:
    """Batch 5 rollback under REAL HA: the terminal throws AFTER the handoff
    was committed -> exactly this flow's owner is released; no entry, no
    stranded prepared handoff, other owners untouched."""

    from custom_components.eybond_local.passive_discovery import (
        get_callback_session_registry,
    )

    boot = MockConfigEntry(
        domain=DOMAIN,
        title="EyeBond boot",
        unique_id="collector:E5000099990003",
        data={
            "connection_type": "eybond",
            "server_ip": SYNTHETIC_SERVER_IP,
            "collector_ip": SYNTHETIC_COLLECTOR_IP,
            "collector_pn": "E5000099990003",
            "tcp_port": 8899,
            "udp_port": 58899,
            "driver_hint": "auto",
        },
        options={},
    )
    boot.add_to_hass(hass)
    assert await hass.config_entries.async_setup(boot.entry_id)
    await hass.async_block_till_done()
    registry = get_callback_session_registry(hass)
    assert registry is not None

    session_id = "listener-8899-88"
    registry.sessions_source = lambda: (
        {
            "session_id": session_id,
            "collector_pn": SYNTHETIC_COLLECTOR_PN,
            "state": "routed_framed",
            "collector_identity_source": "fc2_parameter_2",
            "listener_port": 8899,
            "raw": {"session_id": session_id, "protocol_shape": "eybond_framed"},
        },
    )

    init = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}
    )
    flow = hass.config_entries.flow._progress[init["flow_id"]]
    assert flow._callback_continuation.adopt_passive_inbound_identity(
        SYNTHETIC_COLLECTOR_PN, session_id
    )
    owner = flow._callback_continuation.owner

    entries_before = len(hass.config_entries.async_entries(DOMAIN))

    def _terminal_boom():
        raise RuntimeError("late terminal failure")

    try:
        flow._create_entry_with_handoff(SYNTHETIC_COLLECTOR_PN, _terminal_boom)
    except RuntimeError:
        pass
    else:  # pragma: no cover - the raise is load-bearing
        raise AssertionError("terminal exception must propagate")

    # Rollback: this flow's owner is fully released -- no claim, no prepared
    # handoff, no entry; the unrelated boot entry's ownership is untouched.
    assert registry.owner_for_pn(SYNTHETIC_COLLECTOR_PN) == ""
    assert registry.prepared_handoff_identity(owner, SYNTHETIC_COLLECTOR_PN) == ""
    assert flow._callback_continuation.handed_off is False
    assert len(hass.config_entries.async_entries(DOMAIN)) == entries_before
    assert registry.claimed_identity(boot.entry_id) == "E5000099990003"

    hass.config_entries.flow.async_abort(init["flow_id"])
    await hass.config_entries.async_unload(boot.entry_id)
    await hass.async_block_till_done()
