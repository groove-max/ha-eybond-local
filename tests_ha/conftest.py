"""Fixtures for the REAL Home Assistant integration tests.

Everything here talks to the genuine `homeassistant` package via
pytest-homeassistant-custom-component. Nothing in `tests/` is importable from
this suite by design: those modules install fake `homeassistant.*` packages into
`sys.modules`, which is precisely what this job must not do.

Only EXTERNAL boundaries are faked -- the device/transport runtime and host
network enumeration. The Home Assistant lifecycle itself (config entries, flow
manager, platform forwarding, unload) always runs for real.
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[0]
for _path in (str(REPO_ROOT), str(HERE)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

pytest_plugins = ("pytest_homeassistant_custom_component",)

from synthetic import (  # noqa: E402  (needs the sys.path bootstrap above)
    SYNTHETIC_BROADCAST,
    SYNTHETIC_NETWORK,
    SYNTHETIC_SERVER_IP,
)


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Let Home Assistant load `custom_components/eybond_local` for real."""

    yield


@pytest.fixture(autouse=True)
def no_host_network_scan():
    """Fake the host network enumeration boundary (an OS/network read).

    The config flow asks the host for its IPv4 interfaces. That is an external
    boundary, not Home Assistant logic, and under pytest-socket it would be a
    blocked socket call. Returning a fixed synthetic interface keeps the REAL
    flow logic (defaulting, validation, step routing) running unchanged.
    """

    from unittest.mock import patch

    interfaces = [
        {
            "name": "eth0",
            "ip": SYNTHETIC_SERVER_IP,
            "label": f"eth0 - {SYNTHETIC_SERVER_IP}",
            "network": SYNTHETIC_NETWORK,
            "broadcast": SYNTHETIC_BROADCAST,
        }
    ]
    with (
        patch(
            "custom_components.eybond_local.network_interfaces.get_ipv4_interfaces",
            return_value=interfaces,
        ),
        patch(
            "custom_components.eybond_local.network_interfaces.get_local_ip",
            return_value=SYNTHETIC_SERVER_IP,
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def no_passive_discovery_listeners():
    """Fake the passive-discovery listener boundary (it binds real sockets).

    `async_setup` starts the domain-level passive callback discovery, which opens
    TCP/UDP listeners. Only the socket-binding start/stop is faked; the service
    object, its session registry, and every consumer of it stay real.
    """

    from unittest.mock import AsyncMock, patch

    with (
        patch(
            "custom_components.eybond_local.passive_discovery.PassiveCallbackDiscovery.async_start",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "custom_components.eybond_local.passive_discovery.PassiveCallbackDiscovery.async_stop",
            new=AsyncMock(return_value=None),
        ),
    ):
        yield


class FakeRuntimeManager:
    """A device-free stand-in for the transport/device runtime boundary.

    This implements the project's own `runtime.manager.RuntimeManager` protocol.
    It is injected at `create_runtime_manager` -- the factory seam the coordinator
    already uses -- so the REAL coordinator, the REAL `async_setup_entry`, the
    REAL platform forwarding and the REAL unload all still execute. Nothing about
    the Home Assistant lifecycle is mocked; only the thing that would open a
    socket to an inverter is.
    """

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.started = 0
        self.stopped = 0

    # --- addressing -------------------------------------------------------
    @property
    def effective_server_ip(self) -> str:
        return SYNTHETIC_SERVER_IP

    @property
    def effective_advertised_server_ip(self) -> str:
        return SYNTHETIC_SERVER_IP

    @property
    def collector_server_endpoint_rollback_target(self) -> str:
        return ""

    # --- lifecycle --------------------------------------------------------
    async def async_start(self) -> None:
        self.started += 1

    async def async_stop(self) -> None:
        self.stopped += 1

    async def async_reconcile_network(self, *, reason: str = "network_change") -> bool:
        return False

    async def async_activate_claimed_session(
        self,
        *,
        expected_session_id: str,
        timeout: float,
    ) -> bool:
        """Honor the runtime protocol used by a prepared config-flow handoff."""

        del expected_session_id, timeout
        return True

    async def async_refresh(self, *, poll_interval: float | None = None):
        from custom_components.eybond_local.models import RuntimeSnapshot

        # Offline-but-healthy: no collector session, no inverter, no values. This
        # is the honest shape for "no device present" and exercises the
        # coordinator's disconnected path without inventing fake telemetry.
        return RuntimeSnapshot(
            connected=False,
            collector=None,
            inverter=None,
            values={},
            last_error=None,
        )

    def invalidate_collector_runtime_values(self) -> None:
        return None

    def listener_diagnostics(self) -> dict[str, object]:
        return {}

    # --- capability / control surface (never exercised by these smoke tests) --
    async def async_write_capability(self, capability_key: str, value: Any) -> Any:
        raise AssertionError("HA smoke tests must not write to a device")

    async def async_apply_preset(self, preset_key: str) -> dict[str, object]:
        raise AssertionError("HA smoke tests must not apply presets")

    async def async_set_collector_server_endpoint(
        self, endpoint: str, *, apply_changes: bool = True
    ) -> dict[str, object]:
        raise AssertionError("HA smoke tests must not write a collector endpoint")

    def set_reverse_discovery_enabled(self, enabled: bool) -> None:
        return None

    async def async_ensure_callback_listener(self, port: int) -> None:
        return None

    async def async_trigger_reverse_discovery(
        self, *, port: int = 0, timeout: float = 0.75
    ) -> dict[str, object]:
        raise AssertionError("HA smoke tests must not emit a UDP trigger")

    # --- proxy / shadow routes -------------------------------------------
    async def async_start_proxy_capture_route(self, **_kwargs: Any) -> None:
        raise AssertionError("HA smoke tests must not start a proxy route")

    async def async_stop_proxy_capture_route(self, **_kwargs: Any) -> None:
        return None

    def proxy_capture_route_running(self) -> bool:
        return False

    async def async_start_shadow_learning_route(self, **_kwargs: Any) -> None:
        raise AssertionError("HA smoke tests must not start a shadow route")

    async def async_stop_shadow_learning_route(self, **_kwargs: Any) -> None:
        return None

    def shadow_learning_route_running(self) -> bool:
        return False

    def shadow_learning_route_ready(self) -> bool:
        return False

    def shadow_learning_route_status(self) -> dict[str, object]:
        return {}

    # --- collector management --------------------------------------------
    async def async_disconnect_collector_connections(self, *, reason: str = "") -> None:
        return None

    async def async_apply_collector_changes(self) -> dict[str, object]:
        return {}

    async def async_reboot_collector(self) -> dict[str, object]:
        raise AssertionError("HA smoke tests must not reboot a collector")

    async def async_rollback_collector_server_endpoint(
        self, *, apply_changes: bool = True
    ) -> dict[str, object]:
        return {}

    async def async_get_collector_server_endpoint_state(self) -> dict[str, object]:
        return {}

    def collector_management_capabilities(self) -> Any:
        from custom_components.eybond_local.collector.capabilities import (
            collector_capability_profile_from_runtime,
        )

        return collector_capability_profile_from_runtime(None)

    def collector_management_diagnostics(self) -> dict[str, object]:
        return {}

    def collector_metadata_diagnostics(self) -> dict[str, object]:
        return {}

    async def async_capture_support_evidence(self) -> dict[str, object]:
        return {}


@pytest.fixture
def fake_runtime():
    """Inject the device-free runtime at the coordinator's factory seam."""

    from unittest.mock import patch

    created: list[FakeRuntimeManager] = []

    def _create(*args: Any, **kwargs: Any) -> FakeRuntimeManager:
        manager = FakeRuntimeManager(*args, **kwargs)
        created.append(manager)
        return manager

    with patch(
        "custom_components.eybond_local.runtime.coordinator.create_runtime_manager",
        side_effect=_create,
    ):
        yield created
