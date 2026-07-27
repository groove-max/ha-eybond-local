from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.proxy_capture import (
    PROXY_WIRE_TRANSPARENT,
    build_proxy_capture_overview,
    resolve_proxy_wire_mode,
)
from custom_components.eybond_local.support.proxy_trace import build_proxy_capture_session_state


class ProxyCapturePlannerTests(unittest.TestCase):
    def test_uses_new_at_cloud_session_not_existing_framed_ha_session(
        self,
    ) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=True,
            collector_connected=True,
            cloud_tools_allowed=True,
            collector_cloud_family="smartess_at",
            collector_session_protocol="eybond_framed",
            cloud_session_protocol="at_text",
            current_endpoint="dtu_ess.eybond.com,18899,TCP",
            upstream_endpoint="dtu_ess.eybond.com,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "ready")
        self.assertEqual(overview.blocking_reason, "")
        self.assertTrue(overview.can_start)
        self.assertEqual(
            resolve_proxy_wire_mode("eybond_framed", "at_text"),
            PROXY_WIRE_TRANSPARENT,
        )

    def test_blocks_transparent_capture_when_only_one_wire_is_known(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=True,
            collector_connected=True,
            cloud_tools_allowed=True,
            collector_cloud_family="smartess_at",
            collector_session_protocol="eybond_framed",
            cloud_session_protocol="",
            current_endpoint="dtu_ess.eybond.com,18899,TCP",
            upstream_endpoint="dtu_ess.eybond.com,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "blocked")
        self.assertEqual(
            overview.blocking_reason,
            "transparent_wire_unavailable",
        )
        self.assertFalse(overview.can_start)

    def test_existing_at_ha_session_does_not_block_framed_cloud_session(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=True,
            collector_connected=True,
            cloud_tools_allowed=True,
            collector_cloud_family="legacy_binary",
            collector_session_protocol="at_text",
            cloud_session_protocol="eybond_framed",
            current_endpoint="ess.eybond.com,18899,TCP",
            upstream_endpoint="ess.eybond.com,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "ready")
        self.assertEqual(overview.blocking_reason, "")
        self.assertTrue(overview.can_start)

    def test_allows_transparent_capture_when_local_and_cloud_wires_match(
        self,
    ) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=True,
            collector_connected=True,
            cloud_tools_allowed=True,
            collector_cloud_family="legacy_binary",
            collector_session_protocol="eybond_framed",
            cloud_session_protocol="eybond_framed",
            current_endpoint="ess.eybond.com",
            upstream_endpoint="ess.eybond.com",
            target_endpoint="192.168.1.50,8899,TCP",
        )

        self.assertEqual(overview.status, "ready")
        self.assertTrue(overview.can_start)

    def test_allows_redirect_when_collector_control_policy_allows_it(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="auto",
            collector_control_allowed=True,
            collector_connected=True,
            cloud_tools_allowed=True,
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
            cloud_tools_allowed=True,
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
            cloud_tools_allowed=True,
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
            cloud_tools_allowed=True,
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
            cloud_tools_allowed=True,
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
            cloud_tools_allowed=True,
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
            cloud_tools_allowed=True,
            current_endpoint="192.168.1.50,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
            active_state=state,
        )

        self.assertEqual(overview.status_label, "Restoring")
        self.assertTrue(overview.critical_phase)
        self.assertFalse(overview.can_stop)

    def test_new_session_requires_cloud_and_home_assistant_profile(self) -> None:
        overview = build_proxy_capture_overview(
            control_mode="full",
            collector_connected=True,
            cloud_tools_allowed=False,
            current_endpoint="collector-cloud.smartess.example,18899,TCP",
            upstream_endpoint="collector-cloud.smartess.example,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
        )

        self.assertEqual(overview.status, "blocked")
        self.assertEqual(
            overview.blocking_reason,
            "operating_profile_requires_cloud_and_ha",
        )
        self.assertFalse(overview.can_start)
        self.assertFalse(overview.can_stop)

    def test_active_session_remains_stoppable_after_profile_drift(self) -> None:
        state = build_proxy_capture_session_state(
            entry_id="entry-1",
            collector_pn="E5000020000000",
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
            cloud_tools_allowed=False,
            current_endpoint="192.168.1.50,18899,TCP",
            target_endpoint="192.168.1.50,18899,TCP",
            active_state=state,
        )

        self.assertEqual(overview.status, "running")
        self.assertTrue(overview.can_stop)


if __name__ == "__main__":
    unittest.main()
