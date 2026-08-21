from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.diagnostic_projection import (
    build_runtime_transport_debug,
)


class _Done:
    def __init__(self, value: bool) -> None:
        self._value = value

    def done(self) -> bool:
        return self._value


class _Writer:
    def is_closing(self) -> bool:
        return True


class DiagnosticProjectionTests(unittest.TestCase):
    def test_projects_collector_and_at_connection_debug_without_mutation(self) -> None:
        collector = SimpleNamespace(
            remote_ip="192.0.2.10",
            remote_port=18899,
            collector_pn="E50000200000000001",
            raw_request_count=3,
            raw_response_count=2,
            raw_timeout_count=1,
            raw_unhandled_line_count=4,
            raw_last_spacing_wait_ms=10,
            raw_last_response_duration_ms=20,
            raw_last_total_duration_ms=30,
            raw_last_request_ascii="QPI",
            raw_last_response_ascii="(PI30",
            raw_last_timeout_request_ascii="QPIGS",
            raw_last_parser="pi30",
            raw_last_frame_format="at_text",
        )
        connection = SimpleNamespace(
            connected=True,
            _reader_task=_Done(False),
            _writer=_Writer(),
            _pending_raw_response=_Done(True),
            _raw_passthrough_frame_format="at_text",
        )
        transport = SimpleNamespace(
            connected=True,
            collector_info=collector,
            _at_connection=lambda *, create_placeholder: (
                connection if create_placeholder is False else None
            ),
        )

        debug = build_runtime_transport_debug(transport)

        self.assertEqual(debug["transport_type"], "SimpleNamespace")
        self.assertTrue(debug["transport_connected"])
        self.assertEqual(debug["collector_remote_ip"], "192.0.2.10")
        self.assertTrue(debug["collector_pn_present"])
        self.assertEqual(debug["raw_last_request_ascii"], "QPI")
        self.assertFalse(debug["at_reader_task_done"])
        self.assertTrue(debug["at_writer_closing"])
        self.assertTrue(debug["at_pending_raw_done"])

    def test_missing_transport_and_broken_surfaces_fail_closed(self) -> None:
        self.assertEqual(
            build_runtime_transport_debug(None),
            {
                "transport_type": "",
                "transport_id": 0,
                "transport_connected": False,
            },
        )

        class BrokenTransport:
            connected = True

            @property
            def collector_info(self):
                raise RuntimeError("collector unavailable")

            def _at_connection(self, *, create_placeholder: bool):
                raise RuntimeError("connection unavailable")

        debug = build_runtime_transport_debug(BrokenTransport())

        self.assertEqual(debug["collector_info_error"], "collector unavailable")
        self.assertEqual(debug["connection_debug_error"], "connection unavailable")


class DiagnosticProjectionArchitectureTests(unittest.TestCase):
    def test_projection_is_neutral_and_coordinator_only_delegates(self) -> None:
        projection_path = (
            REPO_ROOT
            / "custom_components/eybond_local/support/diagnostic_projection.py"
        )
        projection_source = projection_path.read_text(encoding="utf-8")
        self.assertNotIn("homeassistant", projection_source)
        self.assertNotIn("runtime.coordinator", projection_source)
        self.assertNotIn("config_flow", projection_source)

        runtime_dir = (
            REPO_ROOT / "custom_components/eybond_local/runtime/coordinator"
        )
        coordinator_sources = [
            path.read_text(encoding="utf-8")
            for path in sorted(runtime_dir.glob("*.py"))
        ]
        method_names: set[str] = set()
        for source in coordinator_sources:
            for node in ast.parse(source).body:
                if not isinstance(node, ast.ClassDef):
                    continue
                method_names.update(
                    method.name
                    for method in node.body
                    if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                )
        self.assertNotIn("_diagnostic_runtime_debug", method_names)
        self.assertGreaterEqual(
            sum(
                source.count("build_runtime_transport_debug(")
                for source in coordinator_sources
            ),
            2,
        )


if __name__ == "__main__":
    unittest.main()
