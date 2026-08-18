from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.models import CollectorInfo, RuntimeSnapshot


class RuntimeSnapshotCollectorMetadataTests(unittest.TestCase):
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
        coordinator_path = (
            REPO_ROOT
            / "custom_components/eybond_local/runtime/coordinator.py"
        )
        coordinator_source = coordinator_path.read_text(encoding="utf-8")
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

        coordinator_tree = ast.parse(coordinator_source)
        coordinator_class = next(
            node
            for node in coordinator_tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "EybondLocalCoordinator"
        )
        publisher = next(
            node
            for node in coordinator_class.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "_publish_snapshot_values"
        )
        publisher_calls = {
            node.func.attr
            for node in ast.walk(publisher)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("set_collector_server_endpoint", publisher_calls)

        hub_path = REPO_ROOT / "custom_components/eybond_local/runtime/hub.py"
        hub_tree = ast.parse(hub_path.read_text(encoding="utf-8"))
        hub_class = next(
            node
            for node in hub_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "EybondHub"
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
