from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.const import DOMAIN
from custom_components.eybond_local.services import _async_handle_run_diagnostic_commands


class _FakeCoordinator:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def async_run_diagnostic_commands(self, **kwargs) -> dict:
        self.calls.append(dict(kwargs))
        return {
            "success": True,
            "output": "driver: modbus_smg (active_runtime)\n",
            "results": [],
            "context": {"selected_driver_key": "modbus_smg"},
            "started_at": "2026-06-19T00:00:00+00:00",
            "finished_at": "2026-06-19T00:00:01+00:00",
            "result_path": "/config/eybond_local/diagnostic_runs/x.json",
            "download_url": "/local/eybond_local/diagnostic_runs/x.share.json",
        }


class _FakeEntry:
    def __init__(self, coordinator: _FakeCoordinator) -> None:
        self.domain = DOMAIN
        self.runtime_data = coordinator


class _FakeConfigEntries:
    def __init__(self, entry: _FakeEntry) -> None:
        self._entry = entry

    def async_get_entry(self, entry_id: str):
        return self._entry if entry_id == "entry-1" else None


class _FakeHass:
    def __init__(self, entry: _FakeEntry) -> None:
        self.config_entries = _FakeConfigEntries(entry)


class _FakeCall:
    def __init__(self, data: dict) -> None:
        self.data = data


class DiagnosticServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_action_response_passthrough_and_forwarded_args(self) -> None:
        coordinator = _FakeCoordinator()
        hass = _FakeHass(_FakeEntry(coordinator))
        call = _FakeCall(
            {
                "entry_id": "entry-1",
                "commands": "read 171 14\n",
                "stop_on_error": False,
                "operation_timeout": 2.5,
                "publish_download_copy": True,
            }
        )
        result = await _async_handle_run_diagnostic_commands(hass, call)

        self.assertTrue(result["success"])
        self.assertIn("output", result)
        self.assertIn("results", result)
        self.assertIn("context", result)
        self.assertIn("started_at", result)
        self.assertIn("finished_at", result)
        self.assertEqual(result["result_path"], "/config/eybond_local/diagnostic_runs/x.json")
        self.assertEqual(
            result["download_url"], "/local/eybond_local/diagnostic_runs/x.share.json"
        )

        forwarded = coordinator.calls[0]
        self.assertEqual(forwarded["commands"], "read 171 14\n")
        self.assertEqual(forwarded["stop_on_error"], False)
        self.assertEqual(forwarded["operation_timeout"], 2.5)
        self.assertEqual(forwarded["publish_download_copy"], True)

    async def test_operation_timeout_defaults_to_none(self) -> None:
        coordinator = _FakeCoordinator()
        hass = _FakeHass(_FakeEntry(coordinator))
        call = _FakeCall({"entry_id": "entry-1", "commands": "read 10\n"})
        await _async_handle_run_diagnostic_commands(hass, call)
        self.assertIsNone(coordinator.calls[0]["operation_timeout"])
        self.assertEqual(coordinator.calls[0]["stop_on_error"], True)
        self.assertEqual(coordinator.calls[0]["publish_download_copy"], False)

    async def test_unknown_entry_raises(self) -> None:
        coordinator = _FakeCoordinator()
        hass = _FakeHass(_FakeEntry(coordinator))
        call = _FakeCall({"entry_id": "missing", "commands": "read 10\n"})
        with self.assertRaises(ValueError):
            await _async_handle_run_diagnostic_commands(hass, call)


if __name__ == "__main__":
    unittest.main()
