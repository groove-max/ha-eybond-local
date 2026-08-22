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
    DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY,
    ControlDiscoveryOutcome,
    ControlDiscoveryTimeoutPolicy,
)
from custom_components.eybond_local.support.cloud_learning_engines import (  # noqa: E402
    resolve_cloud_learning_engine,
)


def _runner(source_id: str):
    return resolve_cloud_learning_engine(source_id).control_discovery_runner()


async def _executor(fn, *args):
    return fn(*args)


async def _start_shadow_route():
    return None


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
        start_shadow_route=_start_shadow_route,
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
    def test_timeout_policy_is_strict_and_separates_metadata_from_actions(self) -> None:
        policy = DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY
        self.assertGreater(policy.metadata_request, policy.action_request)
        for malformed in (True, "15", None, object()):
            with self.subTest(malformed=malformed):
                with self.assertRaises(TypeError):
                    ControlDiscoveryTimeoutPolicy(metadata_request=malformed)
        for malformed in (0, -1.0):
            with self.subTest(malformed=malformed):
                with self.assertRaises(ValueError):
                    ControlDiscoveryTimeoutPolicy(action_request=malformed)

    def test_supported_and_resolution(self) -> None:
        self.assertTrue(resolve_cloud_learning_engine("smartess").available)
        self.assertTrue(resolve_cloud_learning_engine("valuecloud").available)
        self.assertFalse(resolve_cloud_learning_engine("nope").available)
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
        control_session = object()
        with patch.object(
            ccd, "fetch_control_discovery_bundle_for_collector", return_value=bundle
        ) as bundle_fetch, patch.object(
            ccd, "login_for_control_discovery", return_value=(object(), control_session)
        ) as control_login, patch.object(
            ccd, "async_orchestrate_shadow_learning_settings",
            side_effect=lambda **kw: dict(_ORCHESTRATION),
        ) as smartess_orchestrate, patch.object(
            ccd.valuecloud_cloud_module, "login_with_password"
        ) as vc_login, patch.object(
            ccd.valuecloud_cloud_module, "fetch_device_bundle_for_collector_with_session"
        ) as vc_fetch:
            outcome = _run(_runner("smartess"))
        self.assertIsInstance(outcome, ControlDiscoveryOutcome)
        self.assertIs(
            smartess_orchestrate.call_args.kwargs["session"],
            control_session,
        )
        policy = DEFAULT_CONTROL_DISCOVERY_TIMEOUT_POLICY
        self.assertEqual(
            bundle_fetch.call_args.kwargs["timeout"],
            policy.metadata_request,
        )
        self.assertEqual(
            control_login.call_args.kwargs["timeout"],
            policy.action_request,
        )
        self.assertEqual(
            smartess_orchestrate.call_args.kwargs["timeout"],
            policy.action_request,
        )
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
            ccd, "login_for_control_discovery"
        ) as smartess_login, patch.object(
            ccd, "fetch_control_discovery_bundle_for_collector"
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
        with patch.object(ccd, "fetch_control_discovery_bundle_for_collector", return_value=bundle), patch.object(
            ccd, "login_for_control_discovery", return_value=(object(), object())
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
        progress_values: list[float] = []

        async def _start_route() -> None:
            events.append("route")

        def _fetch_bundle(**_kwargs):
            events.append("bundle")
            return bundle

        def _login(**_kwargs):
            events.append("control_login")
            return object(), "control_session"

        async def _orchestrate(**_kwargs):
            events.append("orchestrate")
            return dict(_ORCHESTRATION)

        with patch.object(ccd, "fetch_control_discovery_bundle_for_collector", side_effect=_fetch_bundle), patch.object(
            ccd, "login_for_control_discovery", side_effect=_login
        ), patch.object(
            ccd, "async_orchestrate_shadow_learning_settings",
            side_effect=_orchestrate,
        ):
            _run(
                _runner("smartess"),
                progress=lambda fraction, _stage: progress_values.append(fraction),
                on_identity=lambda identity: events.append("identity"),
                start_shadow_route=_start_route,
                on_learning=lambda: events.append("learning"),
            )
        # The route is active for the provider-owned metadata bundle. A fresh
        # login after that bundle creates the control-dispatch session.
        self.assertEqual(
            events,
            [
                "route",
                "bundle",
                "identity",
                "control_login",
                "learning",
                "orchestrate",
            ],
        )
        self.assertEqual(progress_values, [0.08, 0.18, 0.30])
        self.assertEqual(progress_values, sorted(progress_values))
if __name__ == "__main__":
    unittest.main()
