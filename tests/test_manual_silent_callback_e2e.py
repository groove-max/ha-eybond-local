"""The pcap-derived silent-callback acceptance test, end to end, on real wire.

The regression this pins (synthetic identities, no user pcap material):

1. the collector opens a framed callback TCP socket and sends ZERO application
   bytes until asked;
2. manual onboarding certifies identity with a REAL framed FC=2 PN query;
3. the user consents; the REAL callback recovery transaction reboots the
   collector, proves the silent inbound window, sends exactly ONE addressed
   ``set>server`` sequence and re-identifies the same full PN on the new
   silent socket;
4. the normal callback_on_demand entry is created with a valid callback
   RecoveryContract branch and the exact prepared owner handed to setup;
5. the REAL runtime binds the live session, confirms the wire, and the
   coordinator's validated ``live_session`` evidence is persisted;
6. after unload/reload the ConnectionSpec is rebuilt from persisted data
   alone; the NEXT callback socket is again silent -- and the persisted
   confirmed-protocol owner makes the listener actively FC=2-probe it instead
   of deadlocking in ``waiting_for_route_identity``;
7. the runtime reconnects, identifies the synthetic E500 as SMG, and at least
   one REAL Modbus FC=4 poll returns valid live values.

Load-bearing wiring: the flow steps call the PRODUCTION
``async_run_callback_recovery_transaction`` (removing that call breaks phase
3-4); phase 6-7 works ONLY because the persisted evidence seeds the confirmed
protocol owner (removing the bootstrap leaves the silent socket unprobed and
the bounded reconnect loop fails). Every rendezvous is deadline-bounded.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
HELPERS_DIR = REPO_ROOT / "tests" / "helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

# Installs the stub homeassistant modules BEFORE the integration imports.
import test_config_flow as flow_scaffold  # noqa: E402
from test_config_flow import _FakeHass, _FakeSetupEntry  # noqa: E402


def _ensure_stub_module(name: str):
    import types as _types

    module = sys.modules.get(name)
    if module is None:
        module = _types.ModuleType(name)
        sys.modules[name] = module
    return module


# The coordinator import needs a few more homeassistant surfaces than the
# config-flow stubs provide. ONLY absent stdlib-shaped stubs are added here --
# no real integration module is ever overwritten (the runtime phases below
# need the REAL drivers registry, connection models and transports).
_components = _ensure_stub_module("homeassistant.components")
_ensure_stub_module("homeassistant.components.network")
_network_util = _ensure_stub_module("homeassistant.components.network.util")
_persistent = _ensure_stub_module("homeassistant.components.persistent_notification")
if not hasattr(_persistent, "async_create"):
    _persistent.async_create = lambda *args, **kwargs: None
if not hasattr(_persistent, "async_dismiss"):
    _persistent.async_dismiss = lambda *args, **kwargs: None
_config_entries = _ensure_stub_module("homeassistant.config_entries")
if not hasattr(_config_entries, "ConfigEntry"):
    _config_entries.ConfigEntry = type("ConfigEntry", (), {})
_ha_const = _ensure_stub_module("homeassistant.const")
if not hasattr(_ha_const, "EVENT_COMPONENT_LOADED"):
    _ha_const.EVENT_COMPONENT_LOADED = "component_loaded"
if not hasattr(_ha_const, "EVENT_HOMEASSISTANT_STOP"):
    _ha_const.EVENT_HOMEASSISTANT_STOP = "homeassistant_stop"
_helpers = _ensure_stub_module("homeassistant.helpers")
_device_registry = _ensure_stub_module("homeassistant.helpers.device_registry")
if not hasattr(_device_registry, "DeviceInfo"):
    _device_registry.DeviceInfo = dict
_helpers.device_registry = _device_registry
_update_coordinator = _ensure_stub_module("homeassistant.helpers.update_coordinator")
if not hasattr(_update_coordinator, "DataUpdateCoordinator"):
    class _DataUpdateCoordinator:
        def __class_getitem__(cls, _item):
            return cls

        def __init__(self, *args, **kwargs):
            pass

    _update_coordinator.DataUpdateCoordinator = _DataUpdateCoordinator
_util = _ensure_stub_module("homeassistant.util")
_dt_util = _ensure_stub_module("homeassistant.util.dt")
_util.dt = _dt_util

from custom_components.eybond_local import (  # noqa: E402
    _register_entry_callback_session_claim,
)
import custom_components.eybond_local.flows.config.base as config_base_module  # noqa: E402
from custom_components.eybond_local.collector.transport import (  # noqa: E402
    _acquire_shared_listener,
    _release_shared_listener,
)
from custom_components.eybond_local.collector.transport_profile import (  # noqa: E402
    collector_session_protocol_from_inventory_state,
)
from custom_components.eybond_local.config_flow import EybondLocalConfigFlow  # noqa: E402
from custom_components.eybond_local.connection.callback_ledger import (  # noqa: E402
    get_callback_trigger_ledger,
)
from custom_components.eybond_local.connection.spec_factory import (  # noqa: E402
    build_connection_spec,
)
from custom_components.eybond_local.connection.recovery_contract import (  # noqa: E402
    RecoveryContract,
)
from custom_components.eybond_local.connection.session_registry import (  # noqa: E402
    CallbackSessionRegistry,
)
from custom_components.eybond_local.onboarding.timeouts import (  # noqa: E402
    DEFAULT_ONBOARDING_TIMEOUT_POLICY,
)
from custom_components.eybond_local.runtime.factory import (  # noqa: E402
    create_runtime_manager,
)
from fake_collector import FakeCollectorService  # noqa: E402
from fake_collector_lib import (  # noqa: E402
    PRESET_MODBUS_SMG_READONLY,
    CollectorProfile,
    resolve_scenario,
)

# Synthetic E500 identity only: heartbeat carries just the 14-char prefix.
FULL_PN = "E500002SYN62344022"

_E2E_TIMEOUT = 150.0  # the whole scenario, hard-bounded


def _free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class ManualSilentCallbackEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tcp_port = _free_tcp_port()
        # Bound like production: the shared listener key is (0.0.0.0, port) --
        # the flow channels AND the runtime link share this exact instance.
        self._listener = await _acquire_shared_listener("0.0.0.0", self._tcp_port)

        def _sessions():
            out = []
            for session in self._listener.discovered_collector_sessions():
                if not isinstance(session, dict):
                    continue
                enriched = dict(session)
                enriched.setdefault("listener_port", int(self._tcp_port))
                enriched.setdefault(
                    "session_protocol",
                    collector_session_protocol_from_inventory_state(
                        state=session.get("state"),
                        protocol_shape=session.get("protocol_shape"),
                    ),
                )
                out.append(enriched)
            return tuple(out)

        self._registry = CallbackSessionRegistry(sessions_source=_sessions)
        self._service = FakeCollectorService(
            listen_ip="127.0.0.1",
            udp_port=0,  # REAL UDP listener on an ephemeral port
            tcp_bind_ip="127.0.0.1",
            heartbeat_interval=1.0,
            connect_timeout=2.0,
            udp_reply="",
            scenario=resolve_scenario(
                preset=PRESET_MODBUS_SMG_READONLY,
                profile=CollectorProfile(
                    mode=PRESET_MODBUS_SMG_READONLY,
                    pn=FULL_PN,
                    serial_number="SMGSYN240001",
                    model_name="SMG II 6200",
                    rated_power=6200,
                    protocol_number=1,
                ),
                set_29_mode="reboot_silent",
                # EVERY socket in this scenario is FULLY silent: no unsolicited
                # byte ever fires (the delay exceeds the whole run). All
                # identification comes from explicit/authorized active probes;
                # link liveness comes from the runtime's own FC=1 requests,
                # which a real collector answers like any other query.
                first_heartbeat_delay=3600.0,
            ),
        )
        await self._service.start()

    async def asyncTearDown(self) -> None:
        await self._service.stop()
        await _release_shared_listener(
            self._listener, close_pending=True, close_payload=True, close_at=True
        )

    # ---- helpers ---------------------------------------------------------

    def _service_udp_port(self) -> int:
        transport = self._service._udp_transport
        assert transport is not None
        return int(transport.get_extra_info("sockname")[1])

    def _make_flow(self) -> EybondLocalConfigFlow:
        flow = EybondLocalConfigFlow()
        flow.hass = _FakeHass(None)
        flow.hass.data.setdefault("eybond_local", {})[
            "callback_session_registry"
        ] = self._registry
        flow.context = {}
        flow._local_ip = "127.0.0.1"
        flow._auto_config = {"server_ip": "127.0.0.1"}
        flow._interface_options = [
            {
                "name": "lo",
                "ip": "127.0.0.1",
                "label": "lo - 127.0.0.1",
                "network": "127.0.0.0/8",
                "broadcast": "127.255.255.255",
            }
        ]
        return flow

    def _manual_input(self) -> dict[str, object]:
        return {
            "server_ip": "127.0.0.1",
            "tcp_port": self._tcp_port,
            "udp_port": self._service_udp_port(),
            "collector_ip": "127.0.0.1",
            "discovery_target": "127.0.0.1",
            "discovery_interval": 3,
            "heartbeat_interval": 60,
            "driver_hint": "auto",
            "connection_strategy": "callback_on_demand",
        }

    def _session_view(self, session_id: str):
        for session in self._registry.observed_sessions_per_socket():
            if session.session_id == session_id:
                return session
        return None

    async def _bounded(self, predicate, *, timeout: float, message: str):
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            value = predicate()
            if value:
                return value
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(f"bounded rendezvous failed: {message}")
            await asyncio.sleep(0.05)

    # ---- THE tests -------------------------------------------------------

    async def test_silent_callback_regression_end_to_end(self) -> None:
        await asyncio.wait_for(self._run_scenario(), timeout=_E2E_TIMEOUT)

    async def test_first_ever_socket_fully_silent_needs_explicit_bootstrap(self) -> None:
        """The EXACT pcap shape: the FIRST onboarding socket says NOTHING.

        No heartbeat ever fires before the identity window closes, no
        persisted evidence exists yet, the PN is unknown -- the ONLY wire
        authority is the user's explicit bootstrap protocol choice, which
        permits exactly one read-only FC=2 identity query on the causally-new
        session. Load-bearing: this test FAILS on any build where the first
        fully-silent socket cannot be onboarded (e.g. when identification
        relies on an unsolicited heartbeat).
        """

        await asyncio.wait_for(
            self._run_silent_first_socket_bootstrap(), timeout=60.0
        )

    async def _run_silent_first_socket_bootstrap(self) -> None:
        # The scenario is already fully silent (delay 3600): no flips needed.
        flow = self._make_flow()

        fast_policy = replace(
            DEFAULT_ONBOARDING_TIMEOUT_POLICY,
            callback_identity_session_wait=6.0,
            callback_causality_lease_wait=3.0,
        )
        with patch.object(
            config_base_module, "_ONBOARDING_TIMEOUT_POLICY", fast_policy
        ), patch(
            "custom_components.eybond_local.connection.callback_identity."
            "DEFAULT_ONBOARDING_TIMEOUT_POLICY",
            fast_policy,
        ):
            # Attempt 1: no bootstrap intent. The session ARRIVES (TCP) but
            # stays silent -- the honest typed result is
            # callback_session_silent, never "did not call back".
            result = await flow.async_step_manual(self._manual_input())
            self.assertEqual(result["type"], "menu")
            self.assertEqual(
                flow._manual_result.last_error, "callback_session_silent"
            )
            self.assertEqual(getattr(self._service, "pre_rx_heartbeats", 0), 0)
            # The silent-session recovery actions are offered.
            self.assertIn("manual_bootstrap_framed", result["menu_options"])
            self.assertIn("manual_bootstrap_at", result["menu_options"])

            # Attempt 2: the user explicitly chooses the EyeBond framed
            # protocol. ONE read-only FC=2 query identifies the silent socket;
            # the full PN exists nowhere but in that reply.
            retried = await flow.async_step_manual_bootstrap_framed()

        self.assertEqual(
            retried.get("step_id"),
            "manual_recovery_confirm",
            getattr(flow._manual_result, "last_error", retried),
        )
        self.assertEqual(flow._callback_continuation.certified_pn, FULL_PN)
        self.assertEqual(getattr(self._service, "pre_rx_heartbeats", 0), 0)
        view = self._session_view(
            flow._callback_continuation.certified_session_id
        )
        self.assertIsNotNone(view)
        self.assertEqual(view.identity_source, "fc2_parameter_2")
        flow.async_remove()

    async def _run_scenario(self) -> None:
        flow = self._make_flow()
        ledger = get_callback_trigger_ledger()

        # ---- Phase A: manual identity over real wire ----------------------
        # The FIRST-EVER socket is fully silent: the plain attempt honestly
        # reports callback_session_silent, and only the user's explicit
        # framed bootstrap choice permits the one FC=2 identity query.
        identity_policy = replace(
            DEFAULT_ONBOARDING_TIMEOUT_POLICY,
            callback_identity_session_wait=6.0,
            callback_causality_lease_wait=3.0,
        )
        with patch(
            "custom_components.eybond_local.connection.callback_identity."
            "DEFAULT_ONBOARDING_TIMEOUT_POLICY",
            identity_policy,
        ):
            generation_start = ledger.snapshot_generation()
            result = await flow.async_step_manual(self._manual_input())
            self.assertEqual(result["type"], "menu")
            self.assertEqual(
                flow._manual_result.last_error, "callback_session_silent"
            )
            self.assertIn("manual_bootstrap_framed", result["menu_options"])
            # The first attempt sent exactly ONE logical trigger sequence.
            self.assertEqual(ledger.snapshot_generation(), generation_start + 1)
            rx_after_first = self._service.discovery_rx_count

            result = await flow.async_step_manual_bootstrap_framed()

            # The bootstrap CONTINUATION sent ZERO additional UDP: no new
            # ledger sequence, no new datagram on the collector's socket.
            self.assertEqual(ledger.snapshot_generation(), generation_start + 1)
            self.assertEqual(self._service.discovery_rx_count, rx_after_first)
        self.assertEqual(
            result.get("step_id"),
            "manual_recovery_confirm",
            result.get("errors") or result,
        )
        self.assertEqual(flow._callback_continuation.certified_pn, FULL_PN)
        identity_session = flow._callback_continuation.certified_session_id
        view = self._session_view(identity_session)
        self.assertIsNotNone(view)
        # ZERO unsolicited bytes before the probe; the FULL PN exists nowhere
        # but the real FC=2 reply on the user-selected wire.
        self.assertEqual(getattr(self._service, "pre_rx_heartbeats", 0), 0)
        self.assertEqual(view.identity_source, "fc2_parameter_2")
        self.assertEqual(view.collector_pn, FULL_PN)

        # ---- Phase B: consent -> REAL recovery transaction ----------------
        # The RECOVERY reconnect socket is FULLY silent too: the engine's
        # pre-reboot trusted-wire authority drives the single session-pinned
        # FC=2 probe of the causally-new silent socket.
        consent = await flow.async_step_manual_recovery_confirm()
        self.assertEqual(consent["type"], "menu")
        self.assertIn("manual_recovery_verify", consent["menu_options"])

        generation_before = ledger.snapshot_generation()
        fast_policy = replace(
            DEFAULT_ONBOARDING_TIMEOUT_POLICY,
            inbound_strong_identity_timeout=5.0,
            inbound_restart_disconnect_timeout=5.0,
            inbound_reconnect_timeout=1.5,  # a real (brief) silent window
            callback_recovery_session_wait=8.0,
            callback_causality_lease_wait=3.0,
        )
        with patch.object(
            config_base_module, "_ONBOARDING_TIMEOUT_POLICY", fast_policy
        ):
            progress = await flow.async_step_manual_recovery_verify()
            self.assertEqual(progress["type"], "progress")
            await flow._manual_recovery_task
            done = await flow.async_step_manual_recovery_verify()
            self.assertEqual(done["type"], "progress_done")

            async def _passthrough_enrich(_user_input, r):
                return r

            with patch.object(
                flow,
                "_async_enrich_manual_collector_profile",
                side_effect=_passthrough_enrich,
            ):
                created = await flow.async_step_manual_recovery_result()

        self.assertEqual(
            created.get("type"), "create_entry", flow._manual_recovery_error
        )
        # Exactly ONE logical set>server sequence for the whole recovery.
        self.assertEqual(ledger.snapshot_generation(), generation_before + 1)

        data = dict(created["data"])
        self.assertEqual(data["connection_strategy"], "callback_on_demand")
        self.assertNotIn("connection_strategy", created.get("options") or {})
        contract = RecoveryContract.from_entry_data(data)
        self.assertIsNotNone(contract)
        self.assertTrue(contract.callback_verified)
        self.assertEqual(contract.collector_pn, FULL_PN)

        # The exact prepared owner survived the terminal for entry setup.
        owner = self._registry.owner_for_pn(FULL_PN)
        self.assertTrue(owner.startswith("callback_recovery:"), owner)
        recovery_session = self._registry.claimed_session_id(owner)
        self.assertNotEqual(recovery_session, identity_session)
        # The recovery answer arrived on another SILENT socket: identified by
        # the active FC=2 read again, never by unsolicited bytes.
        self.assertEqual(
            self._session_view(recovery_session).identity_source, "fc2_parameter_2"
        )

        # ---- Phase C: production setup completes the exact handoff -------
        entry = _FakeSetupEntry("entry-e2e", data, dict(created.get("options") or {}))
        _register_entry_callback_session_claim(flow.hass, entry)
        self.assertEqual(self._registry.owner_for_pn(FULL_PN), "entry-e2e")
        self.assertEqual(
            self._registry.claimed_session_id("entry-e2e"), recovery_session
        )
        flow.async_remove()  # flow cleanup must not disturb the completed handoff
        self.assertEqual(self._registry.owner_for_pn(FULL_PN), "entry-e2e")

        # ---- Phase D: REAL runtime on the live session -------------------
        spec = build_connection_spec(entry.data, entry.options)
        if spec is None:
            self.fail(
                f"spec None: ct={entry.data.get('connection_type')!r} "
                f"keys={sorted(entry.data)}"
            )
        admission_evidence = spec.confirmed_session_protocol_evidence
        self.assertIsNotNone(
            admission_evidence,
            "the exact-session identity proof must seed first runtime setup",
        )
        self.assertEqual(admission_evidence.protocol, "eybond_framed")
        self.assertEqual(admission_evidence.collector_pn, FULL_PN)
        runtime1 = create_runtime_manager(spec, driver_hint="auto")
        await runtime1.async_start()
        try:
            snapshot = await self._bounded_refresh(runtime1, timeout=45.0)
            self.assertTrue(snapshot.connected)
            protocol, pn = runtime1.confirmed_session_protocol_evidence()
            self.assertEqual(protocol, "eybond_framed")
            self.assertEqual(pn, FULL_PN)

            # The REAL coordinator write path remains idempotent when admission
            # already persisted the same PN-bound live evidence.
            stub = type(
                "_CoordinatorStub",
                (),
                {
                    "hass": flow.hass,
                    "config_entry": entry,
                    "_runtime": runtime1,
                    "_persist_connection_axes": (
                        lambda self, updates=None: entry.data.update(updates or {})
                    ),
                },
            )()
            # Bind the real implementation owner directly. Entity tests may
            # install a lightweight coordinator facade, but the package remains
            # importable and must never change which mixin owns persistence.
            from custom_components.eybond_local.runtime.coordinator.persistence import (
                CoordinatorPersistenceMixin,
            )

            persist = (
                CoordinatorPersistenceMixin
                ._persist_confirmed_session_protocol_from_runtime
            )
            persist(stub)
            self.assertEqual(
                entry.data.get("collector_confirmed_session_protocol"),
                "eybond_framed",
            )
            self.assertEqual(
                entry.data.get("collector_confirmed_session_protocol_source"),
                "live_session",
            )
        finally:
            await runtime1.async_stop()

        # ---- Phase E: reload from persisted data alone --------------------
        # The NEXT callback socket keeps the pcap deadlock shape: fully
        # silent -- the active FC=2 probe seeded by the persisted evidence is
        # the only identification vector. The collector genuinely lost its
        # link while HA was down (the pcap sequence), so the next attempt
        # produces a fresh silent socket instead of resuming the old one.
        await self._service._close_tcp_only()
        self._service._last_discovery = None

        spec2 = build_connection_spec(entry.data, entry.options)
        evidence = spec2.confirmed_session_protocol_evidence
        self.assertIsNotNone(evidence, "persisted evidence bootstrap is load-bearing")
        self.assertEqual(evidence.protocol, "eybond_framed")
        self.assertEqual(evidence.source, "live_session")
        self.assertEqual(evidence.collector_pn, FULL_PN)

        runtime2 = create_runtime_manager(spec2, driver_hint="auto")
        await runtime2.async_start()
        try:
            rx_before = self._service.discovery_rx_count
            # The NEXT callback attempt is ENTIRELY the runtime's: its own
            # single trigger sequence makes the collector dial back in -- and
            # the new socket again says NOTHING until asked.
            snapshot2 = await self._bounded_refresh(runtime2, timeout=45.0)
            self.assertTrue(snapshot2.connected, snapshot2.last_error)
            # No waiting_for_route_identity deadlock: the persisted confirmed
            # owner made the listener actively FC=2-probe the silent socket.
            live = [
                s
                for s in self._registry.observed_sessions_per_socket()
                if not s.state.startswith("closed")
                and s.session_id not in (identity_session, recovery_session)
            ]
            self.assertTrue(live, "no new live session observed")
            self.assertEqual(live[0].identity_source, "fc2_parameter_2")
            self.assertEqual(live[0].collector_pn, FULL_PN)
            # Runtime, not config flow, identifies the inverter through the
            # real SMG Modbus driver and parses at least one live FC=4 poll.
            self.assertEqual(snapshot2.inverter.driver_key, "modbus_smg")
            self.assertEqual(snapshot2.inverter.model_name, "SMG 6200")
            self.assertTrue(snapshot2.telemetry.points, "no SMG telemetry polled")
            self.assertTrue(snapshot2.has_runtime_value("battery_voltage"))
            self.assertNotIn("battery_voltage", snapshot2.values)
            # And no unsolicited pre-probe bytes on this socket either.
            self.assertEqual(getattr(self._service, "pre_rx_heartbeats", 0), 0)
            # The runtime really triggered (the collector cannot dial on its
            # own after reboot_silent) -- and sent AT MOST one logical
            # sequence: no continuous 3-second trigger spam.
            rx_delta = self._service.discovery_rx_count - rx_before
            self.assertGreaterEqual(rx_delta, 1)
            self.assertLessEqual(rx_delta, 3)
            # The heartbeat workaround is GONE: across the WHOLE scenario the
            # collector never volunteered a single unsolicited byte.
            self.assertEqual(getattr(self._service, "pre_rx_heartbeats", 0), 0)
        finally:
            await runtime2.async_stop()

    async def _bounded_refresh(self, runtime, *, timeout: float):
        deadline = asyncio.get_running_loop().time() + timeout
        last = None
        trace: list[str] = []
        while True:
            last = await runtime.async_refresh()
            values = last.runtime_values()
            trace.append(
                f"{last.connected}/{last.last_error}/"
                f"{values.get('runtime_driver_state')}/"
                f"{values.get('runtime_session_state')}/"
                f"{values.get('collector_callback_state')}"
            )
            if (
                last.connected
                and last.has_runtime_value("battery_voltage")
            ):
                return last
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(
                    f"runtime never connected: last_error={last.last_error!r} "
                    f"trace={trace}"
                )
            await asyncio.sleep(0.4)


if __name__ == "__main__":
    unittest.main()
