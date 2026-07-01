from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_voluptuous_stub() -> None:
    """Provide the voluptuous module for local pure-unit runs without HA deps."""

    if "voluptuous" in sys.modules:
        return

    import types

    voluptuous_module = types.ModuleType("voluptuous")

    class Schema:
        def __init__(self, schema):
            self.schema = schema

        def __call__(self, value):
            return value

    def Required(key, default=None):
        return key

    def Optional(key, default=None):
        return key

    def All(*validators):
        return validators

    def Range(**kwargs):
        return kwargs

    def In(container):
        return container

    voluptuous_module.Schema = Schema
    voluptuous_module.Required = Required
    voluptuous_module.Optional = Optional
    voluptuous_module.All = All
    voluptuous_module.Range = Range
    voluptuous_module.In = In
    sys.modules["voluptuous"] = voluptuous_module


_install_voluptuous_stub()


from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.services import (
    _async_handle_apply_collector_changes,
    _async_handle_bind_collector_to_home_assistant,
    _async_handle_reboot_collector,
    _async_handle_rollback_collector_server_endpoint,
    _async_handle_set_collector_server_endpoint,
    _async_handle_start_proxy_capture,
    _async_handle_stop_proxy_capture,
)


class _FakeCoordinator:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def async_set_collector_server_endpoint(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"status": "applied", "readback_endpoint": "192.168.1.193,18899,TCP"}

    async def async_bind_collector_to_home_assistant(self, **kwargs):
        self.calls.append({"bind": dict(kwargs)})
        return {"status": "applied", "target_role": "home_assistant"}

    async def async_apply_collector_changes(self, **kwargs):
        self.calls.append({"apply": dict(kwargs)})
        return {"status": "applied", "action": "apply"}

    async def async_reboot_collector(self, **kwargs):
        self.calls.append({"reboot": dict(kwargs)})
        return {"status": "reboot_triggered", "action": "reboot"}

    async def async_rollback_collector_server_endpoint(self, **kwargs):
        self.calls.append({"rollback": dict(kwargs)})
        return {"status": "rollback_applied", "rollback_source": "session_cached_previous_endpoint"}

    async def async_start_proxy_capture(self, **kwargs):
        self.calls.append({"start_proxy_capture": dict(kwargs)})
        return {"status": "running", "trace_path": "/config/eybond_local/proxy_traces/session.jsonl"}

    async def async_stop_proxy_capture(self, **kwargs):
        self.calls.append({"stop_proxy_capture": dict(kwargs)})
        return {"status": "stopped", "manifest_path": "/config/eybond_local/proxy_traces/session.json"}


class _FakeEntry:
    def __init__(self, runtime_data) -> None:
        self.domain = DOMAIN
        self.runtime_data = runtime_data


class _FakeConfigEntries:
    def __init__(self, entry) -> None:
        self._entry = entry

    def async_get_entry(self, entry_id: str):
        if entry_id == "entry-1":
            return self._entry
        return None


class _FakeHass:
    def __init__(self, entry) -> None:
        self.config_entries = _FakeConfigEntries(entry)


class _FakeServiceCall:
    def __init__(self, data: dict[str, object]) -> None:
        self.data = data


class ServiceHandlerTests(unittest.TestCase):
    def test_set_collector_server_endpoint_forwards_guarded_arguments(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_set_collector_server_endpoint(
                hass,
                _FakeServiceCall(
                    {
                        "entry_id": "entry-1",
                        "server_host": "192.168.1.193",
                        "server_port": 18899,
                        "server_protocol": "tcp",
                        "apply_changes": True,
                        "confirm_redirect": True,
                    }
                ),
            )

            self.assertEqual(result["status"], "applied")
            self.assertEqual(
                coordinator.calls,
                [
                    {
                        "server_host": "192.168.1.193",
                        "server_port": 18899,
                        "server_protocol": "tcp",
                        "apply_changes": True,
                        "confirm_redirect": True,
                    }
                ],
            )

        asyncio.run(_run())

    def test_set_collector_server_endpoint_requires_matching_entry(self) -> None:
        async def _run() -> None:
            hass = _FakeHass(_FakeEntry(_FakeCoordinator()))

            with self.assertRaisesRegex(ValueError, "eybond_entry_not_found"):
                await _async_handle_set_collector_server_endpoint(
                    hass,
                    _FakeServiceCall(
                        {
                            "entry_id": "missing",
                            "server_host": "192.168.1.193",
                            "server_port": 18899,
                            "confirm_redirect": True,
                        }
                    ),
                )

        asyncio.run(_run())

    def test_apply_collector_changes_forwards_confirmation(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_apply_collector_changes(
                hass,
                _FakeServiceCall({"entry_id": "entry-1", "confirm_restart": True}),
            )

            self.assertEqual(result["action"], "apply")
            self.assertEqual(coordinator.calls[-1], {"apply": {"confirm_restart": True}})

        asyncio.run(_run())

    def test_bind_collector_to_home_assistant_forwards_confirmation(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_bind_collector_to_home_assistant(
                hass,
                _FakeServiceCall({"entry_id": "entry-1", "confirm_redirect": True}),
            )

            self.assertEqual(result["target_role"], "home_assistant")
            self.assertEqual(
                coordinator.calls[-1],
                {"bind": {"confirm_redirect": True}},
            )

        asyncio.run(_run())

    def test_reboot_collector_forwards_confirmation(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_reboot_collector(
                hass,
                _FakeServiceCall({"entry_id": "entry-1", "confirm_restart": True}),
            )

            self.assertEqual(result["action"], "reboot")
            self.assertEqual(coordinator.calls[-1], {"reboot": {"confirm_restart": True}})

        asyncio.run(_run())

    def test_rollback_collector_server_endpoint_forwards_flags(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_rollback_collector_server_endpoint(
                hass,
                _FakeServiceCall(
                    {
                        "entry_id": "entry-1",
                        "apply_changes": False,
                        "confirm_redirect": True,
                    }
                ),
            )

            self.assertEqual(result["status"], "rollback_applied")
            self.assertEqual(
                coordinator.calls[-1],
                {"rollback": {"apply_changes": False, "confirm_redirect": True}},
            )

        asyncio.run(_run())

    def test_start_proxy_capture_forwards_anonymized_flag(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_start_proxy_capture(
                hass,
                _FakeServiceCall({"entry_id": "entry-1", "anonymized": False, "confirm_redirect": True}),
            )

            self.assertEqual(result["status"], "running")
            self.assertEqual(
                coordinator.calls[-1],
                {"start_proxy_capture": {"anonymized": False, "confirm_redirect": True}},
            )

        asyncio.run(_run())

    def test_stop_proxy_capture_calls_coordinator(self) -> None:
        async def _run() -> None:
            coordinator = _FakeCoordinator()
            hass = _FakeHass(_FakeEntry(coordinator))

            result = await _async_handle_stop_proxy_capture(
                hass,
                _FakeServiceCall({"entry_id": "entry-1"}),
            )

            self.assertEqual(result["status"], "stopped")
            self.assertEqual(
                coordinator.calls[-1],
                {"stop_proxy_capture": {}},
            )

        asyncio.run(_run())
