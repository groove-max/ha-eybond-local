from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.ui import EYBOND_CONNECTION_DISPLAY_METADATA
from custom_components.eybond_local.models import (
    CollectorCandidate,
    CollectorInfo,
    DriverMatch,
    OnboardingResult,
    ProbeTarget,
)
from custom_components.eybond_local.onboarding.presentation import (
    build_choose_placeholders,
    build_scan_result_line,
    build_scan_results_placeholders,
    confidence_label,
    default_control_summary,
    has_smartess_collector_hint,
    result_label,
    result_placeholders,
    scan_result_sort_key,
    scan_result_status_code,
    scan_result_status_label,
)


class OnboardingPresentationTests(unittest.TestCase):
    def _matched_result(self, *, confidence: str = "high") -> OnboardingResult:
        return OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="autodetect",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="PN123"),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus",
                model_name="SMG 6200",
                serial_number="ABC123",
                probe_target=ProbeTarget(devcode=1, collector_addr=2, device_addr=3),
                confidence=confidence,
            ),
        )

    def _collector_only_result(self, *, replied: bool = False) -> OnboardingResult:
        return OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.56",
                source="autodetect",
                ip="192.168.1.56",
                udp_reply="pong" if replied else "",
                connected=not replied,
                collector=CollectorInfo(collector_pn="PN456"),
            )
        )

    def _smartess_hint_result(self) -> OnboardingResult:
        return OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.57",
                source="autodetect",
                ip="192.168.1.57",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="PN789",
                    smartess_collector_version="8.50.12.3",
                    smartess_protocol_asset_id="0000",
                ),
            )
        )

    def test_status_code_and_label_follow_result_shape(self) -> None:
        self.assertEqual(scan_result_status_code(self._matched_result()), "ready")
        self.assertEqual(scan_result_status_code(self._matched_result(confidence="medium")), "review")
        self.assertTrue(has_smartess_collector_hint(self._smartess_hint_result()))
        self.assertEqual(scan_result_status_code(self._smartess_hint_result()), "smartess_hint")
        self.assertEqual(scan_result_status_code(self._collector_only_result()), "collector_only")
        self.assertEqual(scan_result_status_code(self._collector_only_result(replied=True)), "collector_replied")
        self.assertEqual(scan_result_status_label(self._smartess_hint_result()), "SmartESS hint")
        self.assertEqual(scan_result_status_label(self._matched_result()), "Ready")
        self.assertEqual(scan_result_status_label(self._matched_result(), already_added=True), "Already added")

    def test_sort_key_prioritizes_ready_before_collector_only(self) -> None:
        ready_key = scan_result_sort_key(self._matched_result())
        collector_only_key = scan_result_sort_key(self._collector_only_result())
        self.assertLess(ready_key, collector_only_key)

    def test_result_label_and_placeholders_are_branch_aware(self) -> None:
        result = self._matched_result(confidence="medium")
        label = result_label(result, display=EYBOND_CONNECTION_DISPLAY_METADATA)
        placeholders = result_placeholders(result, display=EYBOND_CONNECTION_DISPLAY_METADATA)

        self.assertIn("Review", label)
        self.assertIn("SMG 6200", label)
        self.assertIn("192.168.1.55", label)
        self.assertEqual(placeholders["collector_pn"], "PN123")
        self.assertEqual(placeholders["confidence"], "Medium confidence")
        self.assertEqual(placeholders["control_summary"], "The integration will start in **monitoring-only** mode.")

        smartess_label = result_label(self._smartess_hint_result(), display=EYBOND_CONNECTION_DISPLAY_METADATA)
        self.assertIn("SmartESS hint", smartess_label)
        self.assertIn("SmartESS metadata", smartess_label)

    def test_scan_results_placeholders_cover_empty_and_ready_states(self) -> None:
        empty = build_scan_results_placeholders(
            display=EYBOND_CONNECTION_DISPLAY_METADATA,
            selected_scan_interface="eth0 - 192.168.1.50",
            detected_count=0,
            available_count=0,
            already_added_count=0,
            ready_model_names=[],
        )
        ready = build_scan_results_placeholders(
            display=EYBOND_CONNECTION_DISPLAY_METADATA,
            selected_scan_interface="eth0 - 192.168.1.50",
            detected_count=3,
            available_count=2,
            already_added_count=1,
            ready_model_names=["SMG 6200", "SMG 6200", "PowMr 4.2kW"],
        )

        self.assertIn("No reachable collectors or inverters", empty["scan_summary"])
        self.assertIn("2", ready["scan_summary"])
        self.assertIn("SMG 6200, PowMr 4.2kW", ready["scan_summary"])
        self.assertIn("Choose **Add detected device**", ready["scan_next_hint"])

    def test_scan_results_placeholders_cover_pending_smartess_state(self) -> None:
        pending = build_scan_results_placeholders(
            display=EYBOND_CONNECTION_DISPLAY_METADATA,
            selected_scan_interface="eth0 - 192.168.1.50",
            detected_count=1,
            available_count=1,
            already_added_count=0,
            ready_model_names=[],
        )

        self.assertIn("local inverter matching is still pending", pending["scan_summary"])
        self.assertIn("save a pending entry", pending["scan_next_hint"])

    def test_scan_result_line_includes_existing_entry_hint(self) -> None:
        line = build_scan_result_line(
            1,
            self._collector_only_result(),
            display=EYBOND_CONNECTION_DISPLAY_METADATA,
            existing_entry_title="EyeBond Local (192.168.1.56)",
        )
        self.assertIn("Already added", line)
        self.assertIn('already added as "EyeBond Local (192.168.1.56)"', line)

        smartess_line = build_scan_result_line(
            2,
            self._smartess_hint_result(),
            display=EYBOND_CONNECTION_DISPLAY_METADATA,
        )
        self.assertIn("SmartESS hint", smartess_line)
        self.assertIn("SmartESS metadata", smartess_line)

    def test_simple_choose_and_confidence_helpers(self) -> None:
        placeholders = build_choose_placeholders(4)
        self.assertIn("4", placeholders["choose_summary"])
        self.assertEqual(confidence_label("high"), "High confidence")
        self.assertEqual(default_control_summary("high"), "Tested controls will be enabled automatically.")


if __name__ == "__main__":
    unittest.main()
