from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.proxy_capture import build_proxy_capture_overview
from custom_components.eybond_local.support.proxy_trace import build_proxy_capture_session_state


class ProxyCapturePlannerTests(unittest.TestCase):
    def test_allows_redirect_when_collector_control_policy_allows_it(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=True,
            collector_connected=True,
            current_endpoint="collector-cloud.smartess.example,18899,TCP",
            upstream_endpoint="collector-cloud.smartess.example,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "ready")
        self.assertTrue(overview.can_start)
        self.assertTrue(overview.redirect_required)

    def test_blocks_redirect_when_collector_control_policy_blocks_it(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=False,
            collector_connected=True,
            current_endpoint="collector-cloud.smartess.example,18899,TCP",
            upstream_endpoint="collector-cloud.smartess.example,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "blocked")
        self.assertEqual(overview.blocking_reason, "collector_control_disabled")
        self.assertFalse(overview.can_start)
        self.assertTrue(overview.redirect_required)

    def test_blocks_when_collector_has_no_proxy_capture_capability(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_proxy_capture_allowed=False,
            collector_connected=True,
            current_endpoint="192.168.1.50,18899,TCP",
            upstream_endpoint="collector-cloud.smartess.example,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "blocked")
        self.assertEqual(overview.blocking_reason, "collector_proxy_capture_unavailable")
        self.assertFalse(overview.can_start)

    def test_ready_when_no_redirect_is_required(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_connected=True,
            current_endpoint="192.168.1.50,18899,TCP",
            upstream_endpoint="collector-cloud.smartess.example,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
            latest_trace_path="/config/eybond_local/proxy_traces/trace.jsonl",
            latest_manifest_path="/config/eybond_local/proxy_traces/trace.json",
        )

        self.assertEqual(overview.status, "ready")
        self.assertTrue(overview.can_start)
        self.assertFalse(overview.redirect_required)
        self.assertEqual(overview.latest_trace_path, "/config/eybond_local/proxy_traces/trace.jsonl")
        self.assertEqual(overview.latest_manifest_path, "/config/eybond_local/proxy_traces/trace.json")

    def test_running_state_blocks_start_and_can_stop(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            trace_path="/config/eybond_local/proxy_traces/current_session.jsonl",
            original_endpoint="collector-cloud.smartess.example,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            restore_required=True,
            anonymized=True,
            started_at="2026-04-28T12:00:00Z",
            expires_at="2026-04-28T12:05:00Z",
            status="running",
        )

        overview = build_proxy_capture_overview(
            control_mode="full",
            collector_connected=True,
            current_endpoint="192.168.1.50,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
            active_state=state,
        )

        self.assertEqual(overview.status, "running")
        self.assertFalse(overview.can_start)
        self.assertTrue(overview.can_stop)
        self.assertEqual(overview.masked_endpoint, "collector-cloud.smartess.example,18899,TCP")
        self.assertEqual(
            overview.latest_trace_path,
            "/config/eybond_local/proxy_traces/current_session.jsonl",
        )

    def test_running_state_hides_previous_manifest_and_prefers_active_trace(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            trace_path="/config/eybond_local/proxy_traces/current_session.jsonl",
            original_endpoint="collector-cloud.smartess.example,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            restore_required=True,
            anonymized=True,
            started_at="2026-04-28T12:00:00Z",
            expires_at="2026-04-28T12:05:00Z",
            status="running",
        )

        overview = build_proxy_capture_overview(
            control_mode="full",
            collector_connected=True,
            current_endpoint="192.168.1.50,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
            active_state=state,
            latest_trace_path="/config/eybond_local/proxy_traces/previous_session.jsonl",
            latest_manifest_path="/config/eybond_local/proxy_traces/previous_session.json",
        )

        self.assertEqual(
            overview.latest_trace_path,
            "/config/eybond_local/proxy_traces/current_session.jsonl",
        )
        self.assertEqual(overview.latest_manifest_path, "")

    def test_critical_phase_disables_stop(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
            original_endpoint="collector-cloud.smartess.example,18899,TCP",
            proxy_endpoint="192.168.1.50,18899,TCP",
            restore_required=True,
            anonymized=True,
            started_at="2026-04-28T12:00:00Z",
            expires_at="2026-04-28T12:05:00Z",
            status="restoring",
        )

        overview = build_proxy_capture_overview(
            control_mode="full",
            collector_connected=True,
            current_endpoint="192.168.1.50,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
            active_state=state,
        )

        self.assertEqual(overview.status_label, "Restoring")
        self.assertTrue(overview.critical_phase)
        self.assertFalse(overview.can_stop)


if __name__ == "__main__":
    unittest.main()
