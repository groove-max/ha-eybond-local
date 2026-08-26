from __future__ import annotations

from dataclasses import replace
import ast
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.acquisition import (  # noqa: E402
    SUPPORT_BLOCKER_CLOUD_PROVIDER,
    SUPPORT_BLOCKER_COLLECTOR_IDENTITY,
    SUPPORT_BLOCKER_LOCAL_BRIDGE,
    SUPPORT_BLOCKER_OPERATING_PROFILE,
    SupportOperationReadiness,
    resolve_support_acquisition_readiness,
)


PN = "E50000200000000001"


class SupportAcquisitionReadinessTests(unittest.TestCase):
    def _resolve(self, **changes):
        values = {
            "collector_pn": PN,
            "inverter_identified": False,
            "virtual_bridge": False,
            "cloud_provider": "smartess",
            "cloud_provider_supported": True,
            "cloud_route_allowed": True,
        }
        values.update(changes)
        return resolve_support_acquisition_readiness(**values)

    def test_unbound_inverter_keeps_all_safe_collector_paths(self) -> None:
        readiness = self._resolve(inverter_identified=False)

        self.assertTrue(readiness.collector_identified)
        self.assertFalse(readiness.inverter_identified)
        self.assertTrue(readiness.cloud_metadata_read.can_start)
        self.assertTrue(readiness.proxy_capture.can_start)
        self.assertTrue(readiness.active_control_learning.can_start)

    def test_metadata_read_does_not_require_resolved_provider_or_cloud_route(self) -> None:
        readiness = self._resolve(
            cloud_provider="",
            cloud_provider_supported=False,
            cloud_route_allowed=False,
        )

        self.assertTrue(readiness.cloud_metadata_read.can_start)
        self.assertFalse(readiness.proxy_capture.visible)
        self.assertEqual(
            readiness.proxy_capture.blocker,
            SUPPORT_BLOCKER_CLOUD_PROVIDER,
        )

    def test_proxy_stays_visible_when_only_operating_profile_blocks_start(self) -> None:
        readiness = self._resolve(cloud_route_allowed=False)

        self.assertTrue(readiness.proxy_capture.visible)
        self.assertFalse(readiness.proxy_capture.can_start)
        self.assertEqual(
            readiness.proxy_capture.blocker,
            SUPPORT_BLOCKER_OPERATING_PROFILE,
        )
        self.assertTrue(readiness.cloud_metadata_read.can_start)

    def test_local_bridge_has_no_vendor_cloud_support_path(self) -> None:
        readiness = self._resolve(virtual_bridge=True)

        for operation in (
            readiness.cloud_metadata_read,
            readiness.proxy_capture,
            readiness.active_control_learning,
        ):
            self.assertFalse(operation.visible)
            self.assertEqual(operation.blocker, SUPPORT_BLOCKER_LOCAL_BRIDGE)

    def test_invalid_collector_identity_fails_closed(self) -> None:
        for pn in ("", " bad ", object(), "\x13\x03\x13"):
            with self.subTest(pn=pn):
                readiness = self._resolve(collector_pn=pn)
                self.assertFalse(readiness.collector_identified)
                self.assertFalse(readiness.cloud_metadata_read.can_start)
                self.assertEqual(
                    readiness.cloud_metadata_read.blocker,
                    SUPPORT_BLOCKER_COLLECTOR_IDENTITY,
                )

    def test_direct_models_reject_ducks_and_contradictions(self) -> None:
        valid = SupportOperationReadiness(visible=True, can_start=True, blocker="")
        for changes in (
            {"visible": 1},
            {"can_start": 1},
            {"can_start": False},
            {"visible": False},
            {"blocker": " unknown "},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    replace(valid, **changes)

    def test_resolver_rejects_non_exact_inputs(self) -> None:
        for field, value in (
            ("inverter_identified", 1),
            ("virtual_bridge", 0),
            ("cloud_provider_supported", 1),
            ("cloud_route_allowed", None),
            ("cloud_provider", object()),
            ("cloud_provider", " smartess"),
        ):
            with self.subTest(field=field):
                with self.assertRaises((TypeError, ValueError)):
                    self._resolve(**{field: value})

    def test_projection_has_no_flow_or_runtime_dependency(self) -> None:
        path = (
            REPO_ROOT
            / "custom_components"
            / "eybond_local"
            / "support"
            / "acquisition.py"
        )
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }

        self.assertFalse(
            any(
                token in imported
                for imported in imports
                for token in ("flows", "config_flow", "runtime")
            )
        )


if __name__ == "__main__":
    unittest.main()
