"""Permanent route proof uses the real registry and controlled-reset engine.

Only device/network edges are scripted. No cloud commands or real sockets.
"""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from custom_components.eybond_local.runtime.coordinator import learning_isolation


@pytest.fixture
def isolation(hass, monkeypatch):
    from custom_components.eybond_local.runtime.coordinator import learning_isolation as mod
    from custom_components.eybond_local import passive_discovery
    from custom_components.eybond_local.collector import silent_session_probe
    from custom_components.eybond_local.connection.callback_ledger import CallbackTriggerLedger
    from custom_components.eybond_local.connection.session_registry import CallbackSessionRegistry
    from custom_components.eybond_local.support.shadow_learning.session import build_shadow_learning_session_state
    from custom_components.eybond_local.telemetry import TelemetryFreshness, TelemetryPoint, TypedTelemetryFrame

    pn = "V001020SYN62344022"
    def session(sid, pn=pn, port=8899):
        return dict(session_id=sid, collector_pn=pn, listener_port=port,
                    peer_ip="203.0.113.10", state="identified",
                    protocol_shape="eybond_framed", collector_identity_source="fc2_parameter_2")
    inventory = [session("old")]
    registry = CallbackSessionRegistry(sessions_source=lambda: tuple(inventory))
    registry.claim_session("entry", session_id="old")
    registry.promote_claim_to_full_pn("entry", pn)
    state = build_shadow_learning_session_state(
        entry_id="entry", collector_pn=pn, route_owner_id="shadow_learning:proof",
        trace_path="synthetic.jsonl", original_endpoint="cloud.invalid,18899,TCP",
        proxy_endpoint="198.51.100.2,502,TCP", upstream_endpoint="cloud.invalid,18899,TCP",
        restore_required=True, started_at="2026-10-15T12:00:00Z",
        updated_at="2026-10-15T12:00:00Z", status="starting",
        protection_version=1, cloud_source="smartess",
    )
    snap = SimpleNamespace(connected=True, last_error="", inverter=object(), telemetry=TypedTelemetryFrame(
        "modbus_smg", (TelemetryPoint("battery_voltage", 52.0, TelemetryFreshness.FRESH),),
    ))
    runtime = SimpleNamespace(
        listener_bind_host="192.0.2.1", async_activate_claimed_session=AsyncMock(return_value=True),
        async_get_collector_server_endpoint_state=AsyncMock(return_value={"current_endpoint": state.proxy_endpoint}),
        async_refresh=AsyncMock(return_value=snap), async_ensure_callback_listener=AsyncMock(),
    )
    c = SimpleNamespace(hass=object(), config_entry=SimpleNamespace(entry_id="entry"),
        _connection_spec=SimpleNamespace(tcp_port=8899), smartess_collector_pn=pn,
        connection_strategy="callback_on_demand", _runtime=runtime,
        _async_prepare_strategy_transition_management_session=AsyncMock(return_value="old"),
        _async_active_shadow_learning_state=AsyncMock(return_value=state),
        _async_require_cloud_learning_unheld=AsyncMock(),
        _endpoint_effective_parts=lambda value: tuple(value.split(",")),
    )
    ctx = SimpleNamespace(c=c, state=state, inventory=inventory, registry=registry,
                          session=session, events=[], new_pn=pn, new_port=8899,
                          reconnect=True, snapshot=snap)

    class Channel:
        def __init__(self, **kw):
            self.sid = kw["session_id"]
            assert kw["port"] == 8899  # NEVER the NAT advertised port 502
        async def async_probe_collector_pn(self):
            return pn
        def observed_wire(self):
            return "eybond_framed"
        async def async_write_endpoint(self, endpoint, *, apply_changes):
            # The write-ahead recovery record has already been checked.
            c._async_active_shadow_learning_state.assert_awaited()
            ctx.events.append(("write", endpoint))
            inventory.clear()
            if ctx.reconnect:
                inventory.append(session("new", ctx.new_pn, ctx.new_port))
            return dict(status="applied", readback_endpoint=endpoint, apply_changes=apply_changes,
                        management_session_id=self.sid)
        async def async_send_restart(self):
            raise AssertionError("an applied endpoint write already performs reset")
        def is_connected(self):
            return any(s["session_id"] == self.sid for s in inventory)
        async def async_close(self):
            pass

    monkeypatch.setattr(passive_discovery, "get_callback_session_registry", lambda _hass: registry)
    monkeypatch.setattr(silent_session_probe, "SilentSessionIdentityProbeChannel",
                        lambda **kw: SimpleNamespace(async_open=AsyncMock(), async_close=AsyncMock()))
    monkeypatch.setattr(mod, "ObservedSessionRestartChannel", Channel)
    actual_transition = mod.async_run_strategy_transition
    async def transition(**kw):
        return await actual_transition(**kw, policy=replace(
            mod.DEFAULT_ONBOARDING_TIMEOUT_POLICY, inbound_strong_identity_timeout=.15,
            inbound_restart_disconnect_timeout=.15, inbound_reconnect_timeout=.15,
            callback_causality_lease_wait=.15,
        ), ledger=CallbackTriggerLedger(), poll_interval=.01)
    monkeypatch.setattr(mod, "async_run_strategy_transition", transition)
    ctx.prepare = lambda: mod.async_prepare_local_learning_route(c, state)
    return ctx


async def test_permanent_route_proves_reset_identity_endpoint_and_fresh_local_io(isolation):
    ctx = isolation
    proof = await ctx.prepare()
    assert proof.local_port == 8899
    assert proof.local_endpoint == "198.51.100.2,502,TCP"
    assert proof.hold is None
    assert ctx.events == [("write", ctx.state.proxy_endpoint)]
    assert ctx.registry.claimed_session_id("entry") == "new"
    ctx.c._runtime.async_refresh.assert_awaited_once()


async def test_host_only_collector_proves_fixed_listener_not_callback_port(isolation):
    from custom_components.eybond_local.runtime.coordinator.learning_route import learning_target_endpoint

    ctx = isolation
    ctx.c.collector_cloud_family = "legacy_binary"
    ctx.c._effective_callback_server_host = "192.0.2.1"
    ctx.c._connection_spec.effective_advertised_tcp_port = 8899
    state = replace(ctx.state, proxy_endpoint=learning_target_endpoint(ctx.c))
    assert state.proxy_endpoint == "192.0.2.1"  # no comma/port in CLDSRVHOST1
    ctx.c._async_active_shadow_learning_state.return_value = state
    ctx.c._runtime.async_get_collector_server_endpoint_state.return_value = {"current_endpoint": state.proxy_endpoint}
    ctx.new_port = 502
    proof = await learning_isolation.async_prepare_local_learning_route(ctx.c, state)
    assert proof.local_port == 502
    assert ctx.events == [("write", "192.0.2.1")]
    ctx.c._runtime.async_ensure_callback_listener.assert_any_await(502)


def test_host_only_explicit_nat_port_mapping_is_preserved():
    from custom_components.eybond_local.runtime.coordinator.learning_route import learning_listener_port, learning_target_endpoint

    c = SimpleNamespace(collector_cloud_family="legacy_binary", _effective_callback_server_host="198.51.100.2",
                        _connection_spec=SimpleNamespace(tcp_port=8899, effective_advertised_tcp_port=502))
    assert learning_listener_port(c) == 8899
    assert learning_target_endpoint(c) == "198.51.100.2"


@pytest.mark.parametrize("flag", ["0", "", False])
async def test_local_proof_accepts_cleared_or_unsupported_apply_flag(isolation, flag):
    ctx = isolation
    ctx.c._runtime.async_get_collector_server_endpoint_state.return_value = {
        "current_endpoint": ctx.state.proxy_endpoint, "reboot_required": flag,
    }
    assert (await ctx.prepare()).local_port == 8899


@pytest.mark.parametrize("flag", ["1", True, "binary-garbage"])
async def test_local_proof_rejects_pending_or_unparsed_apply_flag(isolation, flag):
    ctx = isolation
    ctx.c._runtime.async_get_collector_server_endpoint_state.return_value = {
        "current_endpoint": ctx.state.proxy_endpoint, "reboot_required": flag,
    }
    with pytest.raises(RuntimeError, match="cloud_learning_hold_local_unverified"):
        await ctx.prepare()


@pytest.mark.parametrize("fault", ["foreign_pn", "wrong_port", "no_reconnect", "record_changed",
                                  "endpoint", "apply_pending", "stale", "empty", "disconnected", "claim_lost"])
async def test_local_proof_refuses_incomplete_or_unrelated_evidence(isolation, fault):
    from custom_components.eybond_local.telemetry import TypedTelemetryFrame
    ctx = isolation
    runtime = ctx.c._runtime
    if fault == "foreign_pn":
        ctx.new_pn = "V000405SYN94677058"
    elif fault == "wrong_port":
        ctx.new_port = 18899
    elif fault == "no_reconnect":
        ctx.reconnect = False
    elif fault == "record_changed":
        ctx.c._async_active_shadow_learning_state.return_value = None
    elif fault == "endpoint":
        runtime.async_get_collector_server_endpoint_state.return_value = {"current_endpoint": ctx.state.original_endpoint}
    elif fault == "apply_pending":
        runtime.async_get_collector_server_endpoint_state.return_value = {"current_endpoint": ctx.state.proxy_endpoint, "apply_required": True}
    elif fault == "stale":
        ctx.snapshot.telemetry = TypedTelemetryFrame("modbus_smg", tuple(p.as_carried() for p in ctx.snapshot.telemetry.points))
    elif fault == "empty":
        ctx.snapshot.telemetry = TypedTelemetryFrame("modbus_smg", ())
    elif fault == "disconnected":
        ctx.snapshot.connected = False
    else:
        async def refresh():
            ctx.inventory.clear()
            return ctx.snapshot
        runtime.async_refresh.side_effect = refresh
    with pytest.raises(RuntimeError, match="cloud_learning_hold_local_unverified"):
        await ctx.prepare()
    if fault == "record_changed":
        assert not ctx.events
