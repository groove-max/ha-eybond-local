from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import (
    CollectorCloudProfile,
    CollectorInfo,
    RuntimeSnapshot,
)


class RuntimeSnapshotCollectorMetadataTests(unittest.TestCase):
    @staticmethod
    def _coordinator_sources() -> tuple[str, ...]:
        runtime_dir = REPO_ROOT / "custom_components/eybond_local/runtime"
        return tuple(
            path.read_text(encoding="utf-8")
            for path in sorted(runtime_dir.glob("coordinator*.py"))
        )

    @classmethod
    def _coordinator_method(cls, method_name: str) -> ast.AST:
        for source in cls._coordinator_sources():
            for node in ast.walk(ast.parse(source)):
                if (
                    isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name == method_name
                ):
                    return node
        raise AssertionError(f"coordinator method not found: {method_name}")

    def test_cloud_profile_constructor_is_strict_and_key_owns_metadata(self) -> None:
        profile = CollectorCloudProfile(
            key="valuecloud_at",
            label="ValueCloud AT",
            source="runtime_observed",
            confidence="high",
        )

        self.assertTrue(profile.known)
        for invalid in (None, 1, b"profile"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(TypeError):
                    CollectorCloudProfile(key=invalid)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            CollectorCloudProfile(key=" padded ")
        with self.assertRaises(ValueError):
            CollectorCloudProfile(label="orphan")

    def test_typed_cloud_profile_wins_as_one_coherent_candidate(self) -> None:
        snapshot = RuntimeSnapshot(
            collector=CollectorInfo(
                collector_cloud_profile_key="valuecloud_at",
                collector_cloud_profile_label="ValueCloud AT",
                collector_cloud_profile_source="transport_sniff",
                collector_cloud_profile_confidence="high",
            ),
            values={
                "collector_cloud_profile_key": "stale_profile",
                "collector_cloud_profile_label": "Stale label",
                "collector_cloud_profile_source": "entry_persisted",
                "collector_cloud_profile_confidence": "low",
            },
        )

        self.assertEqual(
            snapshot.collector_cloud_profile,
            CollectorCloudProfile(
                key="valuecloud_at",
                label="ValueCloud AT",
                source="transport_sniff",
                confidence="high",
            ),
        )

    def test_legacy_protocol_profile_is_projected_without_provider_assumption(self) -> None:
        snapshot = RuntimeSnapshot(
            values={
                "smartess_protocol_profile_key": "smartvalue_at",
                "smartess_protocol_name": "SmartValue AT",
            },
        )

        self.assertEqual(
            snapshot.collector_cloud_profile,
            CollectorCloudProfile(
                key="smartvalue_at",
                label="SmartValue AT",
                source="runtime_observed",
                confidence="high",
            ),
        )

    def test_malformed_typed_cloud_profile_fails_closed(self) -> None:
        collector = CollectorInfo()
        collector.collector_cloud_profile_key = object()  # type: ignore[assignment]
        snapshot = RuntimeSnapshot(
            collector=collector,
            values={
                "collector_cloud_profile_key": "stale_profile",
                "collector_cloud_profile_source": "entry_persisted",
            },
        )

        self.assertEqual(snapshot.collector_cloud_profile, CollectorCloudProfile())

    def test_set_cloud_profile_updates_both_projections_atomically(self) -> None:
        collector = CollectorInfo()
        snapshot = RuntimeSnapshot(collector=collector)
        profile = CollectorCloudProfile(
            key="smartess_at",
            label="Cloud AT",
            source="runtime_observed",
            confidence="high",
        )

        snapshot.set_collector_cloud_profile(profile)

        self.assertEqual(snapshot.collector_cloud_profile, profile)
        self.assertEqual(collector.collector_cloud_profile_key, profile.key)
        self.assertEqual(
            snapshot.values["collector_cloud_profile_source"],
            profile.source,
        )
        with self.assertRaises(TypeError):
            snapshot.set_collector_cloud_profile(object())  # type: ignore[arg-type]
        self.assertEqual(snapshot.collector_cloud_profile, profile)

    def test_typed_collector_endpoint_wins_over_legacy_projection(self) -> None:
        snapshot = RuntimeSnapshot(
            collector=CollectorInfo(
                collector_server_endpoint="typed.example,18899,TCP",
            ),
            values={"collector_server_endpoint": "legacy.example,18899,TCP"},
        )

        self.assertEqual(
            snapshot.collector_server_endpoint,
            "typed.example,18899,TCP",
        )

    def test_legacy_endpoint_keeps_partial_snapshots_readable(self) -> None:
        snapshot = RuntimeSnapshot(
            values={"collector_server_endpoint": "legacy.example,18899,TCP"},
        )

        self.assertEqual(
            snapshot.collector_server_endpoint,
            "legacy.example,18899,TCP",
        )

    def test_runtime_endpoint_writers_use_the_snapshot_boundary(self) -> None:
        coordinator_source = "\n".join(self._coordinator_sources())
        self.assertNotIn(
            'snapshot.values["collector_server_endpoint"] =',
            coordinator_source,
        )
        self.assertNotIn(
            'snapshot.values.get("collector_server_endpoint")',
            coordinator_source,
        )
        self.assertNotIn(
            'self.data.values.get("collector_server_endpoint")',
            coordinator_source,
        )

        publisher = self._coordinator_method("_publish_snapshot_values")
        publisher_calls = {
            node.func.attr
            for node in ast.walk(publisher)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("set_collector_server_endpoint", publisher_calls)

        hub_path = REPO_ROOT / "custom_components/eybond_local/runtime/hub_snapshot.py"
        hub_tree = ast.parse(hub_path.read_text(encoding="utf-8"))
        hub_class = next(
            node
            for node in hub_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "HubSnapshotMixin"
        )
        snapshot_builder = next(
            node
            for node in hub_class.body
            if isinstance(node, ast.FunctionDef) and node.name == "_build_snapshot"
        )
        builder_calls = {
            node.func.attr
            for node in ast.walk(snapshot_builder)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("set_collector_server_endpoint", builder_calls)

    def test_cloud_profile_projection_never_splits_fields_in_coordinator(self) -> None:
        coordinator_source = "\n".join(self._coordinator_sources())
        for key in (
            "collector_cloud_profile_key",
            "collector_cloud_profile_label",
            "collector_cloud_profile_source",
            "collector_cloud_profile_confidence",
        ):
            self.assertNotIn(f'values.get("{key}")', coordinator_source)
            self.assertNotIn(f'snapshot.values["{key}"] =', coordinator_source)
        self.assertIn("snapshot.set_collector_cloud_profile", coordinator_source)
        self.assertIn("self.collector_cloud_profile", coordinator_source)

    def test_collector_endpoint_ui_never_reads_the_legacy_slot_directly(self) -> None:
        for relative_path in (
            "custom_components/eybond_local/button.py",
            "custom_components/eybond_local/text.py",
        ):
            with self.subTest(path=relative_path):
                source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
                self.assertNotIn(
                    'values.get("collector_server_endpoint")',
                    source,
                )
                self.assertIn("collector_server_endpoint", source)

    def test_set_endpoint_updates_typed_and_legacy_projections_atomically(self) -> None:
        collector = CollectorInfo()
        snapshot = RuntimeSnapshot(collector=collector)

        snapshot.set_collector_server_endpoint("cloud.example,18899,TCP")

        self.assertEqual(
            collector.collector_server_endpoint,
            "cloud.example,18899,TCP",
        )
        self.assertEqual(
            snapshot.values["collector_server_endpoint"],
            "cloud.example,18899,TCP",
        )

        snapshot.set_collector_server_endpoint("")

        self.assertEqual(collector.collector_server_endpoint, "")
        self.assertNotIn("collector_server_endpoint", snapshot.values)

    def test_invalid_endpoint_refuses_without_partial_mutation(self) -> None:
        collector = CollectorInfo(
            collector_server_endpoint="before.example,18899,TCP",
        )
        snapshot = RuntimeSnapshot(
            collector=collector,
            values={"collector_server_endpoint": "before.example,18899,TCP"},
        )

        for invalid in (None, 18899, b"endpoint"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(TypeError):
                    snapshot.set_collector_server_endpoint(invalid)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            snapshot.set_collector_server_endpoint(" padded.example,18899,TCP ")

        self.assertEqual(
            collector.collector_server_endpoint,
            "before.example,18899,TCP",
        )
        self.assertEqual(
            snapshot.values["collector_server_endpoint"],
            "before.example,18899,TCP",
        )

    def test_malformed_typed_value_falls_back_without_coercion(self) -> None:
        collector = CollectorInfo()
        collector.collector_server_endpoint = 18899  # type: ignore[assignment]
        snapshot = RuntimeSnapshot(
            collector=collector,
            values={"collector_server_endpoint": "legacy.example,18899,TCP"},
        )

        self.assertEqual(
            snapshot.collector_server_endpoint,
            "legacy.example,18899,TCP",
        )


if __name__ == "__main__":
    unittest.main()
