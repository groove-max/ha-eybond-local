from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.connection.ui import EYBOND_CONNECTION_DISPLAY_METADATA
from custom_components.eybond_local.connection.admission import ObservedCollectorSession
from custom_components.eybond_local.connection.recovery.verification import (
    CallbackRecoveryRoute,
)
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
                collector=(
                    None if replied else CollectorInfo(collector_pn="PN456")
                ),
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

    def test_status_code_and_label_follow_collector_first_addability(self) -> None:
        self.assertEqual(scan_result_status_code(self._matched_result()), "found")
        self.assertEqual(
            scan_result_status_code(self._matched_result(confidence="medium")),
            "found",
        )
        ambiguous = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.55",
                source="autodetect",
                ip="192.168.1.55",
                connected=True,
            ),
            match=DriverMatch(
                driver_key="pi30",
                protocol_family="pi30",
                model_name="PowMr 4.2kW",
                serial_number="ABC123",
                probe_target=ProbeTarget(devcode=0x0994, collector_addr=255, device_addr=0),
            ),
            alternative_matches=(
                DriverMatch(
                    driver_key="modbus_smg",
                    protocol_family="modbus_smg",
                    model_name="SMG-compatible",
                    serial_number="ABC123",
                    probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
                ),
            ),
        )
        self.assertEqual(scan_result_status_code(ambiguous), "found")
        self.assertEqual(scan_result_status_label(ambiguous), "Found")
        self.assertTrue(has_smartess_collector_hint(self._smartess_hint_result()))
        self.assertEqual(scan_result_status_code(self._smartess_hint_result()), "found")
        self.assertEqual(scan_result_status_code(self._collector_only_result()), "found")
        self.assertEqual(
            scan_result_status_code(self._collector_only_result(replied=True)),
            "address_found",
        )
        identified_reply = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.56",
                source="autodetect",
                ip="192.168.1.56",
                udp_reply="pong",
                connected=False,
                collector=CollectorInfo(collector_pn="PN456"),
            )
        )
        self.assertEqual(scan_result_status_code(identified_reply), "found")
        passive = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.50",
                source="callback_listener",
                ip="198.51.100.10",
                connected=True,
                collector=CollectorInfo(collector_pn="PN456"),
            ),
            observed_session=ObservedCollectorSession(
                collector_pn="PN456",
                identity_source="fc2_parameter_2",
                session_id="listener-1",
                listener_port=8899,
                peer_hint="198.51.100.10",
            ),
        )
        self.assertEqual(scan_result_status_code(passive), "address_required")
        self.assertEqual(scan_result_status_label(passive), "Needs confirmation")
        active = OnboardingResult(
            collector=passive.collector,
            observed_session=passive.observed_session,
            callback_route=CallbackRecoveryRoute(
                bind_ip="192.168.1.50",
                trigger_target_ip="192.168.1.55",
                trigger_udp_port=58899,
                advertised_ha_host="192.168.1.50",
                advertised_ha_port=8899,
                listener_port=8899,
            ),
        )
        self.assertEqual(scan_result_status_code(active), "found")
        self.assertEqual(scan_result_status_label(active), "Found")
        self.assertEqual(scan_result_status_label(self._smartess_hint_result()), "Found")
        self.assertEqual(scan_result_status_label(self._matched_result()), "Found")
        self.assertEqual(scan_result_status_label(self._matched_result(), already_added=True), "Already added")

    def test_sort_key_prioritizes_addable_before_address_only(self) -> None:
        found_key = scan_result_sort_key(self._matched_result())
        address_key = scan_result_sort_key(self._collector_only_result(replied=True))
        self.assertLess(found_key, address_key)

    def test_result_label_and_placeholders_are_branch_aware(self) -> None:
        result = self._matched_result(confidence="medium")
        label = result_label(result, display=EYBOND_CONNECTION_DISPLAY_METADATA)
        placeholders = result_placeholders(result, display=EYBOND_CONNECTION_DISPLAY_METADATA)

        self.assertIn("Found", label)
        self.assertIn("PN PN123", label)
        self.assertIn("192.168.1.55", label)
        self.assertEqual(placeholders["collector_pn"], "PN123")
        self.assertEqual(placeholders["confidence"], "Medium confidence")
        self.assertEqual(placeholders["control_summary"], "The integration will start in **monitoring-only** mode.")

        smartess_label = result_label(self._smartess_hint_result(), display=EYBOND_CONNECTION_DISPLAY_METADATA)
        self.assertIn("Found", smartess_label)
        self.assertIn("PN PN789", smartess_label)

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

        self.assertIn("No compatible devices", empty["scan_summary"])
        self.assertIn("2", ready["scan_summary"])
        self.assertNotIn("SMG 6200", ready["scan_summary"])
        self.assertIn("Choose a device or address", ready["scan_next_hint"])

    def test_scan_results_placeholders_ignore_runtime_inverter_preview(self) -> None:
        placeholders = build_scan_results_placeholders(
            display=EYBOND_CONNECTION_DISPLAY_METADATA,
            selected_scan_interface="eth0 - 192.168.1.50",
            detected_count=1,
            available_count=1,
            already_added_count=0,
            ready_model_names=[],
        )

        self.assertIn("Ready to set up", placeholders["scan_summary"])
        self.assertNotIn("inverter", placeholders["scan_summary"].lower())
        self.assertNotIn("pending", placeholders["scan_next_hint"].lower())

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
        self.assertIn("Found", smartess_line)
        # Scan presentation does not expose runtime-only SmartESS/driver hints.
        self.assertNotIn("SmartESS metadata", smartess_line)
        self.assertNotIn("connected", smartess_line)

    def test_simple_choose_and_confidence_helpers(self) -> None:
        placeholders = build_choose_placeholders(4)
        self.assertIn("4", placeholders["choose_summary"])
        self.assertEqual(confidence_label("high"), "High confidence")
        self.assertEqual(default_control_summary("high"), "Tested controls will be enabled automatically.")


class AlreadyConfiguredStatusTests(unittest.TestCase):
    def test_already_configured_result_maps_to_already_added_status(self) -> None:
        result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="subnet_unicast",
                ip="192.168.1.14",
            ),
            last_error="already_configured",
        )

        self.assertEqual(scan_result_status_code(result), "already_added")
        self.assertEqual(scan_result_status_label(result), "Already added")


if __name__ == "__main__":
    unittest.main()
