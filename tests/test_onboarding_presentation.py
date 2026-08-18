from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


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
    has_smartess_collector_hint,
    scan_result_sort_key,
    scan_result_status_code,
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

    def test_status_code_follows_collector_first_addability(self) -> None:
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
        self.assertEqual(
            scan_result_status_code(self._matched_result(), already_added=True),
            "already_added",
        )

    def test_sort_key_prioritizes_addable_before_address_only(self) -> None:
        found_key = scan_result_sort_key(self._matched_result())
        address_key = scan_result_sort_key(self._collector_only_result(replied=True))
        self.assertLess(found_key, address_key)

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


class PresentationArchitectureTests(unittest.TestCase):
    def test_module_has_no_parallel_unlocalized_ui_renderer(self) -> None:
        path = (
            REPO_ROOT
            / "custom_components/eybond_local/onboarding/presentation.py"
        )
        tree = ast.parse(path.read_text(encoding="utf-8"))
        public_functions = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

        self.assertEqual(
            public_functions,
            {
                "confidence_sort_score",
                "has_smartess_collector_hint",
                "scan_result_sort_key",
                "scan_result_status_code",
            },
        )


if __name__ == "__main__":
    unittest.main()
