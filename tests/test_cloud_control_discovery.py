"""Provider isolation for the cloud control-discovery runners.

Proves the SmartESS runner runs only SmartESS code and the ValueCloud runner only
ValueCloud code, that an unknown provider fails closed, and that the runners
carry no credentials in their result.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.support import cloud_control_discovery as ccd  # noqa: E402
from custom_components.eybond_local.support.cloud_control_discovery import (  # noqa: E402
    ControlDiscoveryOutcome,
)
from custom_components.eybond_local.support.cloud_evidence_providers import (  # noqa: E402
    resolve_cloud_evidence_provider,
)


def _runner(provider_id: str):
    return resolve_cloud_evidence_provider(provider_id).control_discovery_runner()


async def _executor(fn, *args):
    return fn(*args)


def _run(runner, **overrides):
    kwargs = dict(
        executor=_executor,
        collector_pn="E1",
        username="u",
        password="p",
        fallback_identity={"pn": "E1", "sn": "S1", "devcode": 1, "devaddr": 1},
        max_fields=8,
        progress=lambda *a, **k: None,
        orchestrator_callbacks={},
        on_identity=lambda identity: None,
        on_learning=lambda: None,
    )
    kwargs.update(overrides)
    return asyncio.run(runner.async_run(**kwargs))


_ORCHESTRATION = {
    "planned_write_count": 1,
    "executed_result_count": 1,
    "degraded_count": 0,
    "leaked_count": 0,
    "results": [],
    "correlation": {"matched_count": 1},
}


class RegistryTests(unittest.TestCase):
    def test_supported_and_resolution(self) -> None:
        self.assertTrue(resolve_cloud_evidence_provider("smartess").control_discovery_available)
        self.assertTrue(resolve_cloud_evidence_provider("valuecloud").control_discovery_available)
        self.assertFalse(resolve_cloud_evidence_provider("nope").control_discovery_available)
        self.assertEqual(_runner("smartess").provider_id, "smartess")

    def test_unknown_provider_fails_closed(self) -> None:
        runner = _runner("nope")
        with self.assertRaisesRegex(RuntimeError, "control_discovery_provider_not_supported"):
            _run(runner)


class ProviderIsolationTests(unittest.TestCase):
    def test_smartess_runner_runs_only_smartess_code(self) -> None:
        bundle = {
            "request": {"params": {"pn": "E1", "sn": "S1", "devcode": 1, "devaddr": 1}},
            "responses": {"device_settings": {"dat": {"fields": []}}},
        }
        seen: dict[str, int] = {}
        with patch.object(ccd, "login_with_password", return_value=(object(), object())), patch.object(
            ccd, "fetch_device_bundle_for_collector", return_value=bundle
        ) as smartess_fetch, patch.object(
            ccd, "async_orchestrate_shadow_learning_settings",
            side_effect=lambda **kw: dict(_ORCHESTRATION),
        ), patch.object(
            ccd.valuecloud_cloud_module, "login_with_password"
        ) as vc_login, patch.object(
            ccd.valuecloud_cloud_module, "fetch_device_bundle_for_collector_with_session"
        ) as vc_fetch:
            outcome = _run(_runner("smartess"))
        self.assertIsInstance(outcome, ControlDiscoveryOutcome)
        smartess_fetch.assert_called_once()
        vc_login.assert_not_called()
        vc_fetch.assert_not_called()

    def test_valuecloud_runner_runs_only_valuecloud_code(self) -> None:
        bundle = {
            "request": {"params": {"pn": "E1", "sn": "S1", "devcode": 1, "devaddr": 1}},
            "normalized": {"batch_control": {"fields": []}},
        }
        with patch.object(
            ccd.valuecloud_cloud_module, "login_with_password", return_value=(object(), object())
        ), patch.object(
            ccd.valuecloud_cloud_module,
            "fetch_device_bundle_for_collector_with_session",
            return_value=bundle,
        ) as vc_fetch, patch.object(
            ccd, "async_orchestrate_valuecloud_shadow_learning",
            side_effect=lambda **kw: dict(_ORCHESTRATION),
        ), patch.object(
            ccd, "login_with_password"
        ) as smartess_login, patch.object(
            ccd, "fetch_device_bundle_for_collector"
        ) as smartess_fetch:
            outcome = _run(_runner("valuecloud"))
        self.assertIsInstance(outcome, ControlDiscoveryOutcome)
        vc_fetch.assert_called_once()
        smartess_login.assert_not_called()
        smartess_fetch.assert_not_called()

    def test_outcome_carries_no_credentials(self) -> None:
        bundle = {
            "request": {"params": {"pn": "E1", "sn": "S1", "devcode": 1, "devaddr": 1}},
            "responses": {"device_settings": {"dat": {"fields": []}}},
        }
        with patch.object(ccd, "login_with_password", return_value=(object(), object())), patch.object(
            ccd, "fetch_device_bundle_for_collector", return_value=bundle
        ), patch.object(
            ccd, "async_orchestrate_shadow_learning_settings",
            side_effect=lambda **kw: dict(_ORCHESTRATION),
        ):
            outcome = _run(_runner("smartess"))
        blob = str(outcome).lower()
        self.assertNotIn("password", blob)
        self.assertNotIn("secret", blob)

    def test_identity_and_learning_hooks_fire(self) -> None:
        bundle = {
            "request": {"params": {"pn": "E1", "sn": "S1", "devcode": 1, "devaddr": 1}},
            "responses": {"device_settings": {"dat": {"fields": []}}},
        }
        events: list[str] = []
        with patch.object(ccd, "login_with_password", return_value=(object(), object())), patch.object(
            ccd, "fetch_device_bundle_for_collector", return_value=bundle
        ), patch.object(
            ccd, "async_orchestrate_shadow_learning_settings",
            side_effect=lambda **kw: dict(_ORCHESTRATION),
        ):
            _run(
                _runner("smartess"),
                on_identity=lambda identity: events.append("identity"),
                on_learning=lambda: events.append("learning"),
            )
        # identity resolved before the learning phase begins.
        self.assertEqual(events, ["identity", "learning"])


if __name__ == "__main__":
    unittest.main()
