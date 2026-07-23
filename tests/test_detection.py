from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.onboarding.detection import (
    DETECTION_DEPTH_DEEP,
    DETECTION_DEPTH_FAST,
    DiscoveryTarget,
    OnboardingDetector,
    async_probe_fallback_targets,
    build_unicast_fallback_targets,
)
from custom_components.eybond_local.runtime.driver_detection import _build_driver_match
from custom_components.eybond_local.onboarding.timeouts import OnboardingDeadline
from custom_components.eybond_local.models import DetectedInverter
from custom_components.eybond_local.models import (
    CollectorCandidate,
    DriverMatch,
    CollectorInfo,
    OnboardingResult,
    ProbeTarget,
)
from custom_components.eybond_local.drivers.smg import SmgModbusDriver
from custom_components.eybond_local.collector.discovery import DiscoveryProbeResult


class DetectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_auto_fallback_keeps_replies_for_previously_seen_weak_routes(
        self,
    ) -> None:
        """An earlier lease-busy placeholder must not erase later UDP evidence."""

        detector = OnboardingDetector(server_ip="192.168.1.50")
        weak_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="subnet_unicast",
                ip="192.168.1.51",
            ),
            connection_mode="subnet_unicast",
            last_error="callback_causality_lease_busy",
        )
        replies = tuple(
            DiscoveryTarget(
                ip=ip,
                source="subnet_unicast",
                observed_probe=DiscoveryProbeResult(
                    target_ip=ip,
                    message="set>server=192.168.1.50:8899;",
                    local_port=40000,
                    reply="rsp>server=2;",
                    reply_from=f"{ip}:58899",
                ),
            )
            for ip in ("192.168.1.51", "192.168.1.55")
        )

        with patch(
            "custom_components.eybond_local.onboarding.eybond."
            "async_probe_fallback_targets",
            new=AsyncMock(return_value=replies),
        ):
            fallback = await detector._async_auto_unicast_fallback_targets(
                resolved_targets=(
                    DiscoveryTarget(ip="192.168.1.255", source="broadcast"),
                    DiscoveryTarget(ip="192.168.1.51", source="broadcast"),
                ),
                results=(weak_result,),
                discovery_timeout=1.5,
                deadline=OnboardingDeadline.from_timeout(5.0),
            )

        self.assertEqual(
            {target.ip for target in fallback},
            {"192.168.1.51", "192.168.1.55"},
        )

    async def test_auto_fallback_suppresses_only_an_already_preserved_route_reply(
        self,
    ) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        preserved = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="subnet_unicast",
                ip="192.168.1.51",
                udp_reply="rsp>server=2;",
                udp_reply_from="192.168.1.51:58899",
            ),
            connection_mode="subnet_unicast",
        )
        replies = tuple(
            DiscoveryTarget(
                ip=ip,
                source="subnet_unicast",
                observed_probe=DiscoveryProbeResult(
                    target_ip=ip,
                    message="set>server=192.168.1.50:8899;",
                    local_port=40000,
                    reply="rsp>server=2;",
                    reply_from=f"{ip}:58899",
                ),
            )
            for ip in ("192.168.1.51", "192.168.1.55")
        )

        with patch(
            "custom_components.eybond_local.onboarding.eybond."
            "async_probe_fallback_targets",
            new=AsyncMock(return_value=replies),
        ):
            fallback = await detector._async_auto_unicast_fallback_targets(
                resolved_targets=(
                    DiscoveryTarget(ip="192.168.1.255", source="broadcast"),
                ),
                results=(preserved,),
                discovery_timeout=1.5,
                deadline=OnboardingDeadline.from_timeout(5.0),
            )

        self.assertEqual(tuple(target.ip for target in fallback), ("192.168.1.55",))

    def test_dedupe_prefers_later_route_reply_over_equal_weak_placeholder(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        weak = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="subnet_unicast",
                ip="192.168.1.51",
            ),
            connection_mode="subnet_unicast",
            last_error="callback_causality_lease_busy",
        )
        replied = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.51",
                source="subnet_unicast",
                ip="192.168.1.51",
                udp_reply="rsp>server=2;",
                udp_reply_from="192.168.1.51:58899",
            ),
            connection_mode="subnet_unicast",
        )

        deduped = detector._dedupe_results((weak, replied))

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].collector.udp_reply, "rsp>server=2;")

    async def test_fallback_probe_preserves_attempted_route_when_reply_is_rewritten(
        self,
    ) -> None:
        target = DiscoveryTarget(ip="192.168.1.55", source="subnet_unicast")
        probe = DiscoveryProbeResult(
            target_ip=target.ip,
            message="set>server=192.168.1.50:8899;",
            local_port=40000,
            reply="rsp>server=1;",
            reply_from="192.168.1.1:58899",
        )
        with patch(
            "custom_components.eybond_local.onboarding.eybond."
            "async_send_callback_trigger",
            new=AsyncMock(return_value=probe),
        ):
            found = await async_probe_fallback_targets(
                bind_ip="192.168.1.50",
                advertised_server_ip="192.168.1.50",
                advertised_server_port=8899,
                udp_port=58899,
                targets=(target,),
            )
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].ip, target.ip)
        self.assertEqual(found[0].source, target.source)
        self.assertIs(found[0].observed_probe, probe)
        self.assertNotEqual(found[0].ip, "192.168.1.1")

    async def test_every_fallback_reply_survives_a_later_target_timeout(self) -> None:
        from custom_components.eybond_local.onboarding.eybond import (
            _TargetDetectionState,
        )
        from custom_components.eybond_local.onboarding.presentation import (
            scan_result_status_code,
        )

        targets = (
            DiscoveryTarget(ip="192.168.1.51", source="subnet_unicast"),
            DiscoveryTarget(ip="192.168.1.55", source="subnet_unicast"),
        )

        async def _reply(**kwargs):
            target_ip = kwargs["target_ip"]
            return DiscoveryProbeResult(
                target_ip=target_ip,
                message="set>server=192.168.1.50:8899;",
                local_port=40000,
                reply=(
                    "rsp>server=2;"
                    if target_ip == "192.168.1.51"
                    else "rsp>server=1;"
                ),
                reply_from=f"{target_ip}:58899",
            )

        with patch(
            "custom_components.eybond_local.onboarding.eybond."
            "async_send_callback_trigger",
            side_effect=_reply,
        ):
            found = await async_probe_fallback_targets(
                bind_ip="192.168.1.50",
                advertised_server_ip="192.168.1.50",
                advertised_server_port=8899,
                udp_port=58899,
                targets=targets,
                concurrency=2,
            )

        detector = OnboardingDetector(server_ip="192.168.1.50")
        results = tuple(
            detector._timeout_result_for_state(_TargetDetectionState(target))
            for target in found
        )
        by_ip = {result.collector.ip: result for result in results}

        self.assertEqual(set(by_ip), {"192.168.1.51", "192.168.1.55"})
        self.assertEqual(by_ip["192.168.1.51"].collector.udp_reply, "rsp>server=2;")
        self.assertEqual(by_ip["192.168.1.55"].collector.udp_reply, "rsp>server=1;")
        self.assertEqual(
            {scan_result_status_code(result) for result in results},
            {"address_found"},
        )

    def test_build_driver_match_keeps_family_fallback_at_medium_confidence(self) -> None:
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG Family (Unverified Variant)",
            serial_number="SMG11K240001",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01),
            variant_key="family_fallback",
            details={"rated_power": 11000},
        )

        match = _build_driver_match(SmgModbusDriver(), inverter)

        self.assertEqual(match.variant_key, "family_fallback")
        self.assertEqual(match.confidence, "medium")
        self.assertIn("family_fallback_variant", match.reasons)

    def test_build_driver_match_keeps_non_fallback_read_only_smg_profile_at_medium_confidence(self) -> None:
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="SMG Protocol 1 Candidate",
            serial_number="SMG11K240123",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01),
            variant_key="doc_backed_variant",
            profile_name="modbus_smg/family_fallback.json",
            capabilities=(),
            details={"rated_power": 4200},
        )

        match = _build_driver_match(SmgModbusDriver(), inverter)

        self.assertEqual(match.variant_key, "doc_backed_variant")
        self.assertEqual(match.confidence, "medium")
        self.assertIn("read_only_profile", match.reasons)

    def test_build_driver_match_keeps_anenji_4200_protocol_1_at_medium_confidence(self) -> None:
        inverter = DetectedInverter(
            driver_key="modbus_smg",
            protocol_family="modbus_smg",
            model_name="Anenji 4200 (Protocol 1)",
            serial_number="99432409105281",
            probe_target=ProbeTarget(devcode=0x0001, collector_addr=0xFF, device_addr=0x01),
            variant_key="anenji_4200_protocol_1",
            profile_name="modbus_smg/models/anenji_4200_protocol_1.json",
            capabilities=(),
            details={"rated_power": 4200},
        )

        match = _build_driver_match(SmgModbusDriver(), inverter)

        self.assertEqual(match.variant_key, "anenji_4200_protocol_1")
        self.assertEqual(match.confidence, "medium")
        self.assertIn("unverified_variant", match.reasons)

    def test_build_unicast_fallback_targets_scans_local_24_without_server_ip(self) -> None:
        targets = build_unicast_fallback_targets(server_ip="192.168.1.50")

        self.assertEqual(len(targets), 253)
        self.assertEqual(targets[0], DiscoveryTarget(ip="192.168.1.1", source="subnet_unicast"))
        self.assertEqual(targets[-1], DiscoveryTarget(ip="192.168.1.254", source="subnet_unicast"))
        self.assertNotIn(DiscoveryTarget(ip="192.168.1.50", source="subnet_unicast"), targets)

    def test_build_unicast_fallback_targets_respects_selected_network(self) -> None:
        targets = build_unicast_fallback_targets(
            server_ip="192.168.1.50",
            network_cidr="192.168.0.0/16",
        )

        self.assertEqual(len(targets), 65533)
        self.assertEqual(targets[0], DiscoveryTarget(ip="192.168.0.1", source="subnet_unicast"))
        self.assertEqual(targets[-1], DiscoveryTarget(ip="192.168.255.254", source="subnet_unicast"))
        self.assertNotIn(DiscoveryTarget(ip="192.168.1.50", source="subnet_unicast"), targets)

    async def test_auto_detect_keeps_broadcast_results_without_unicast_fallback(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        broadcast_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E50000200000000001"),
            ),
            connection_mode="broadcast",
        )

        with (
            patch.object(
                detector,
                "_async_detect_targets",
                new=AsyncMock(return_value=(broadcast_result,)),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(return_value=()),
            ) as probe_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger_replies",
                new=AsyncMock(
                    return_value=(
                        DiscoveryProbeResult(
                            target_ip="192.168.1.255",
                            message="set>server=192.168.1.50:8899;",
                            local_port=40000,
                            reply="rsp>server=1;",
                            reply_from="192.168.1.55:40000",
                        ),
                    )
                ),
            ),
        ):
            results = await detector.async_auto_detect(discovery_target="192.168.1.255", attempts=1)

        self.assertEqual(detect_targets.await_count, 1)
        self.assertEqual(detect_targets.await_args.kwargs["depth"], DETECTION_DEPTH_FAST)
        self.assertTrue(detect_targets.await_args.kwargs["return_after_first_identity"])
        probe_targets.assert_not_awaited()
        self.assertEqual(
            {result.collector.ip for result in results if result.collector is not None},
            {"192.168.1.55"},
        )

    async def test_auto_detect_does_not_append_local_unicast_fallback_results_after_broadcast_reply(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        broadcast_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(collector_pn="E50000200000000001"),
            ),
            connection_mode="broadcast",
        )
        fallback_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="subnet_unicast",
                ip="192.168.1.14",
                connected=True,
            ),
            connection_mode="subnet_unicast",
        )

        with (
            patch.object(
                detector,
                "_async_detect_targets",
                new=AsyncMock(return_value=(broadcast_result,)),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(return_value=(DiscoveryTarget(ip="192.168.1.14", source="subnet_unicast"),)),
            ) as probe_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger_replies",
                new=AsyncMock(
                    return_value=(
                        DiscoveryProbeResult(
                            target_ip="192.168.1.255",
                            message="set>server=192.168.1.50:8899;",
                            local_port=40000,
                            reply="rsp>server=1;",
                            reply_from="192.168.1.55:40000",
                        ),
                    )
                ),
            ),
        ):
            results = await detector.async_auto_detect(
                discovery_target="192.168.1.255",
                attempts=1,
            )

        self.assertEqual(detect_targets.await_count, 1)
        probe_targets.assert_not_awaited()
        self.assertEqual(
            {result.collector.ip for result in results if result.collector is not None},
            {"192.168.1.55"},
        )

    async def test_auto_detect_fans_out_additional_broadcast_callbacks(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        primary_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="E50000200000000001",
                ),
            ),
            connection_mode="broadcast",
        )
        extra_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="broadcast",
                ip="192.168.1.14",
                connected=True,
            ),
            connection_mode="broadcast",
        )

        class FakeListener:
            def __init__(self) -> None:
                self.requested_collector_ips: list[str] = []

            def matching_callback_ips(self, collector_ip: str) -> tuple[str, ...]:
                self.requested_collector_ips.append(collector_ip)
                return ("192.168.1.55", "192.168.1.14")

        fake_listener = FakeListener()

        with (
            patch.object(
                detector,
                "_async_detect_targets",
                new=AsyncMock(return_value=(primary_result, extra_result)),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond._acquire_shared_listener",
                new=AsyncMock(return_value=fake_listener),
                create=True,
            ) as acquire_listener,
            patch(
                "custom_components.eybond_local.onboarding.eybond._release_shared_listener",
                new=AsyncMock(),
                create=True,
            ) as release_listener,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger_replies",
                new=AsyncMock(return_value=()),
            ),
        ):
            results = await detector.async_auto_detect(
                discovery_target="192.168.1.255",
                attempts=1,
            )

        acquire_listener.assert_awaited_once()
        release_listener.assert_awaited_once_with(fake_listener)
        self.assertEqual(fake_listener.requested_collector_ips[0], "192.168.1.255")
        self.assertEqual(detect_targets.await_count, 1)
        self.assertEqual(
            detect_targets.await_args.args[0],
            (
                DiscoveryTarget(ip="192.168.1.55", source="broadcast"),
                DiscoveryTarget(ip="192.168.1.14", source="broadcast"),
            ),
        )
        self.assertEqual(
            {result.collector.ip for result in results if result.collector is not None},
            {"192.168.1.55", "192.168.1.14"},
        )

    async def test_auto_detect_materializes_nat_peer_sessions_from_inventory(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        primary_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.193",
                connected=True,
                collector=CollectorInfo(
                    remote_ip="192.168.1.193",
                    collector_pn="E5000099990003",
                ),
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="SMG11K240001",
                probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
            ),
            connection_mode="broadcast",
        )

        class FakeListener:
            def matching_callback_ips(self, collector_ip: str) -> tuple[str, ...]:
                return ("192.168.1.193",)

            def discovered_collector_sessions(self) -> tuple[dict[str, object], ...]:
                return (
                    {
                        "session_id": "listener-8899-1",
                        "peer_ip": "192.168.1.193",
                        "peer_port": 51001,
                        "state": "routed_framed",
                        "collector_pn": "E5000099990001",
                        "collector_identity_source": "framed_heartbeat",
                    },
                    {
                        "session_id": "listener-8899-2",
                        "peer_ip": "192.168.1.193",
                        "peer_port": 51002,
                        "state": "routed_framed",
                        "collector_pn": "E5000099990002",
                        "collector_identity_source": "framed_heartbeat",
                    },
                    {
                        "session_id": "listener-8899-3",
                        "peer_ip": "192.168.1.193",
                        "peer_port": 51003,
                        "state": "closed_no_payload_owner",
                        "collector_pn": "E5000099990003",
                        "collector_identity_source": "framed_heartbeat",
                    },
                )

        fake_listener = FakeListener()

        with (
            patch.object(
                detector,
                "_async_detect_targets",
                new=AsyncMock(return_value=(primary_result,)),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond._acquire_shared_listener",
                new=AsyncMock(return_value=fake_listener),
                create=True,
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond._release_shared_listener",
                new=AsyncMock(),
                create=True,
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger_replies",
                new=AsyncMock(
                    return_value=(
                        DiscoveryProbeResult(
                            target_ip="192.168.1.255",
                            message="set>server=192.168.1.50:8899;",
                            local_port=40000,
                            reply="rsp>server=1;",
                            reply_from="192.168.1.193:40000",
                        ),
                    )
                ),
            ),
        ):
            results = await detector.async_auto_detect(
                discovery_target="192.168.1.255",
                attempts=1,
            )

        self.assertEqual(detect_targets.await_count, 1)
        self.assertEqual(
            {
                result.collector.collector.collector_pn
                for result in results
                if result.collector is not None and result.collector.collector is not None
            },
            {"E5000099990001", "E5000099990002", "E5000099990003"},
        )
        self.assertEqual(
            {
                result.next_action
                for result in results
                if (
                    result.collector is not None
                    and result.collector.collector is not None
                    and result.collector.collector.collector_pn
                    in {"E5000099990001", "E5000099990002"}
                )
            },
            {"manual_driver_selection"},
        )
        passive_by_pn = {
            result.collector.collector.collector_pn: result
            for result in results
            if (
                result.collector is not None
                and result.collector.collector is not None
                and result.collector.collector.collector_pn
                in {"E5000099990001", "E5000099990002"}
            )
        }
        self.assertEqual(
            passive_by_pn["E5000099990001"].detection.details["session_id"],
            "listener-8899-1",
        )
        self.assertEqual(
            passive_by_pn["E5000099990002"].detection.details[
                "collector_identity_source"
            ],
            "framed_heartbeat",
        )

    async def test_auto_detect_accepts_total_timeout_kwarg(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")

        with (
            patch.object(
                detector,
                "_async_detect_targets",
                new=AsyncMock(return_value=()),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(return_value=()),
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger_replies",
                new=AsyncMock(
                    return_value=(
                        DiscoveryProbeResult(
                            target_ip="192.168.1.255",
                            message="set>server=192.168.1.50:8899;",
                            local_port=40000,
                            reply="rsp>server=1;",
                            reply_from="192.168.1.55:40000",
                        ),
                    )
                ),
            ),
        ):
            results = await detector.async_auto_detect(
                discovery_target="192.168.1.255",
                attempts=1,
                total_timeout=9.0,
            )

        self.assertEqual(results, ())
        detect_targets.assert_awaited_once()

    async def test_deep_detect_appends_unicast_fallback_results_after_broadcast_match(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        broadcast_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.255",
                source="broadcast",
                ip="192.168.1.55",
                connected=True,
            ),
            match=DriverMatch(
                driver_key="modbus_smg",
                protocol_family="modbus_smg",
                model_name="SMG 6200",
                serial_number="92632500000001",
                probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
            ),
            connection_mode="broadcast",
        )
        fallback_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.14",
                source="subnet_unicast",
                ip="192.168.1.14",
                connected=True,
            ),
            connection_mode="subnet_unicast",
        )

        with (
            patch.object(
                detector,
                "_async_detect_targets",
                new=AsyncMock(side_effect=[(broadcast_result,), (fallback_result,)]),
            ) as detect_targets,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(return_value=(DiscoveryTarget(ip="192.168.1.14", source="subnet_unicast"),)),
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger_replies",
                new=AsyncMock(return_value=()),
            ),
        ):
            results = await detector.async_deep_detect(
                discovery_target="192.168.1.255",
                unicast_network_cidr="192.168.0.0/16",
                attempts=1,
            )

        self.assertEqual(detect_targets.await_count, 2)
        self.assertEqual(detect_targets.await_args_list[0].kwargs["depth"], DETECTION_DEPTH_DEEP)
        self.assertFalse(detect_targets.await_args_list[0].kwargs["return_after_first_identity"])
        self.assertEqual(detect_targets.await_args_list[1].kwargs["depth"], DETECTION_DEPTH_DEEP)
        self.assertFalse(detect_targets.await_args_list[1].kwargs["return_after_first_identity"])
        self.assertEqual(
            {result.collector.ip for result in results if result.collector is not None},
            {"192.168.1.55", "192.168.1.14"},
        )

    async def test_deep_detect_accepts_total_timeout_kwarg(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")

        with (
            patch.object(
                detector,
                "async_auto_detect",
                new=AsyncMock(return_value=()),
            ) as auto_detect,
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_probe_fallback_targets",
                new=AsyncMock(return_value=()),
            ) as probe_targets,
        ):
            results = await detector.async_deep_detect(
                discovery_target="192.168.1.255",
                unicast_network_cidr="192.168.0.0/16",
                attempts=1,
                total_timeout=9.0,
            )

        self.assertEqual(results, ())
        auto_detect.assert_awaited_once()
        probe_targets.assert_awaited_once()

    def test_public_scan_contract_is_collector_only(self) -> None:
        # Architectural guard: config-flow onboarding has no switch that can
        # resurrect pre-entry driver probing, and the dead BLE handoff detector
        # is not part of the manager surface.
        import inspect

        self.assertFalse(hasattr(OnboardingDetector, "async_handoff_detect"))
        self.assertFalse(hasattr(OnboardingDetector, "async_detect_targets"))
        self.assertFalse(
            hasattr(OnboardingDetector, "_async_detect_driver_with_retries")
        )
        self.assertFalse(hasattr(OnboardingDetector, "_async_attempt_link_baud_sweep"))
        self.assertNotIn(
            "driver_hint",
            inspect.signature(OnboardingDetector).parameters,
        )
        for method in (
            OnboardingDetector.async_auto_detect,
            OnboardingDetector.async_deep_detect,
        ):
            params = set(inspect.signature(method).parameters)
            self.assertNotIn("enrich_runtime_details", params)
            self.assertNotIn("identify_collector_only", params)

        integration_root = (
            REPO_ROOT / "custom_components" / "eybond_local"
        )
        onboarding_source = (
            integration_root / "onboarding" / "eybond.py"
        ).read_text(encoding="utf-8")
        self.assertFalse(
            (integration_root / "onboarding" / "driver_detection.py").exists()
        )
        self.assertNotIn("driver_detection", onboarding_source)
        self.assertNotIn("link_baud_sweep", onboarding_source)
        self.assertTrue(
            (integration_root / "runtime" / "driver_detection.py").exists()
        )
        self.assertTrue(
            (integration_root / "runtime" / "link_baud_sweep.py").exists()
        )

    async def test_detect_target_builds_transport_without_session_protocol(self) -> None:
        # _async_detect_target must NOT pass any session protocol / probe lease to
        # the onboarding transport, so no confirmed owner is registered from a
        # hint. The FakeTransport accepts only the neutral kwargs; passing a
        # session-protocol/lease kwarg would raise TypeError.
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(
            ip="192.168.1.14", source="known_ip", collector_pn="PN-1"
        )

        class FakeTransport:
            instances: list["FakeTransport"] = []

            def __init__(
                self,
                *,
                host: str,
                port: int,
                request_timeout: float,
                heartbeat_interval: float,
                collector_ip: str,
                collector_pn: str = "",
            ) -> None:
                self.init_kwargs = {
                    "collector_ip": collector_ip,
                    "collector_pn": collector_pn,
                }
                self.stop_calls = 0
                self.collector_info = CollectorInfo(remote_ip="")
                FakeTransport.instances.append(self)

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                self.stop_calls += 1

            def set_collector_ip(self, collector_ip: str) -> None:
                return None

            async def wait_until_connected(self, timeout: float) -> bool:
                return False

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return False

        with (
            patch(
                "custom_components.eybond_local.onboarding.eybond.SharedEybondTransport",
                FakeTransport,
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.14",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="",
                        reply_from="",
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=1.5,
                connect_timeout=5.0,
                heartbeat_timeout=2.0,
            )

        transport = FakeTransport.instances[0]
        # No session-protocol / lease kwarg was passed to the transport.
        self.assertEqual(
            set(transport.init_kwargs), {"collector_ip", "collector_pn"}
        )
        # The transport is still stopped when the attempt ends.
        self.assertEqual(transport.stop_calls, 1)
        # A silent, non-connecting collector yields an honest outcome.
        self.assertEqual(result.last_error, "collector_not_connected")

    async def test_detect_target_keeps_full_connect_wait_for_known_ip_without_udp_reply(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.14", source="known_ip")

        class FakeTransport:
            instances: list["FakeTransport"] = []

            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.collector_info = CollectorInfo(remote_ip="")
                self.collector_ip = collector_ip
                self.connected_timeout: float | None = None
                FakeTransport.instances.append(self)

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                return None

            async def wait_until_connected(self, timeout: float) -> bool:
                self.connected_timeout = timeout
                return False

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return False

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="",
                        reply_from="",
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=1.5,
                connect_timeout=5.0,
                heartbeat_timeout=2.0,
            )

        self.assertEqual(FakeTransport.instances[0].collector_ip, "192.168.1.14")
        self.assertEqual(FakeTransport.instances[0].connected_timeout, 5.0)
        self.assertEqual(result.last_error, "collector_not_connected")
        self.assertIsNotNone(result.detection)
        self.assertEqual(result.detection.depth, DETECTION_DEPTH_FAST)
        self.assertEqual(result.detection.status, "collector_not_connected")
        self.assertFalse(result.detection.budget_exhausted)

    async def test_detect_target_empty_retry_does_not_erase_fallback_reply(self) -> None:
        from custom_components.eybond_local.onboarding.presentation import (
            scan_result_status_code,
        )

        detector = OnboardingDetector(server_ip="192.168.1.50")
        initial_probe = DiscoveryProbeResult(
            target_ip="192.168.1.55",
            message="set>server=192.168.1.50:8899;",
            local_port=40000,
            reply="rsp>server=1;",
            reply_from="192.168.1.55:58899",
        )
        target = DiscoveryTarget(
            ip="192.168.1.55",
            source="subnet_unicast",
            observed_probe=initial_probe,
        )

        class FakeTransport:
            def __init__(self, **kwargs) -> None:
                del kwargs
                self.collector_info = CollectorInfo(remote_ip="")

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                del collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                del timeout
                return False

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                del timeout
                return False

        with (
            patch(
                "custom_components.eybond_local.onboarding.eybond."
                "SharedEybondTransport",
                FakeTransport,
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond."
                "async_send_callback_trigger",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip=target.ip,
                        message="set>server=192.168.1.50:8899;",
                        local_port=40001,
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        self.assertEqual(result.collector.udp_reply, "rsp>server=1;")
        self.assertEqual(
            result.collector.udp_reply_from, "192.168.1.55:58899"
        )
        self.assertEqual(scan_result_status_code(result), "address_found")

    async def test_detect_target_preserves_attempted_route_over_reply_source(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            instances: list["FakeTransport"] = []

            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.host = host
                self.port = port
                self.collector_ip = collector_ip
                self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
                self.connected = True
                self.started = False
                FakeTransport.instances.append(self)

            async def start(self) -> None:
                self.started = True

            async def stop(self) -> None:
                self.started = False

            def set_collector_ip(self, collector_ip: str) -> None:
                self.collector_ip = collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                return True

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return True

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="rsp>server=2;",
                        reply_from="192.168.1.14:58899",
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        self.assertEqual(result.connection_mode, "broadcast")
        self.assertEqual(result.next_action, "confirm_collector")
        self.assertIsNone(result.match)
        self.assertEqual(result.collector.ip, "192.168.1.255")
        self.assertEqual(FakeTransport.instances[0].collector_ip, "192.168.1.255")

    async def test_detect_target_reports_missing_heartbeat_warning(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.255", source="broadcast")

        class FakeTransport:
            def __init__(self, *, host: str, port: int, request_timeout: float, heartbeat_interval: float, collector_ip: str) -> None:
                self.collector_ip = collector_ip
                self.collector_info = CollectorInfo(remote_ip="192.168.1.14")
                self.connected = True

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def set_collector_ip(self, collector_ip: str) -> None:
                self.collector_ip = collector_ip

            async def wait_until_connected(self, timeout: float) -> bool:
                return True

            async def wait_until_heartbeat(self, timeout: float) -> bool:
                return False

        with (
            patch("custom_components.eybond_local.onboarding.eybond.SharedEybondTransport", FakeTransport),
            patch(
                "custom_components.eybond_local.onboarding.eybond.async_send_callback_trigger",
                new=AsyncMock(
                    return_value=DiscoveryProbeResult(
                        target_ip="192.168.1.255",
                        message="set>server=192.168.1.50:8899;",
                        local_port=40000,
                        reply="rsp>server=2;",
                        reply_from="192.168.1.14:58899",
                    )
                ),
            ),
        ):
            result = await detector._async_detect_target(
                target,
                discovery_timeout=0.1,
                connect_timeout=0.1,
                heartbeat_timeout=0.1,
            )

        self.assertIn("collector_heartbeat_not_observed", result.warnings)

    async def test_targets_cancelled_after_first_match_are_not_timeouts(self) -> None:
        from custom_components.eybond_local.onboarding.presentation import (
            scan_result_status_code,
        )

        detector = OnboardingDetector(server_ip="192.168.1.50")
        fast_target = DiscoveryTarget(ip="192.168.1.20", source="subnet_unicast")
        slow_target = DiscoveryTarget(ip="192.168.1.21", source="subnet_unicast")

        identified_result = OnboardingResult(
            collector=CollectorCandidate(
                target_ip="192.168.1.20",
                source="subnet_unicast",
                ip="192.168.1.20",
                connected=True,
                collector=CollectorInfo(
                    collector_pn="E50000200000000001",
                ),
            ),
            connection_mode="subnet_unicast",
        )

        async def _detect_target(target, **kwargs):
            state = kwargs.get("detection_state")
            if target.ip == fast_target.ip:
                return identified_result
            # The slow target has already produced a replied candidate when
            # it gets cancelled.
            if state is not None:
                state.candidate = CollectorCandidate(
                    target_ip=target.ip,
                    source=target.source,
                    ip=target.ip,
                    udp_reply="rsp>server=1;",
                )
            await asyncio.sleep(30)
            raise AssertionError("slow target must be cancelled")

        with patch.object(detector, "_async_detect_target", new=_detect_target):
            results = await detector._async_detect_targets(
                (fast_target, slow_target),
                return_after_first_identity=True,
                total_timeout=20.0,
            )

        by_ip = {result.collector.ip: result for result in results}
        self.assertEqual(
            by_ip["192.168.1.20"].collector.collector.collector_pn,
            "E50000200000000001",
        )

        cancelled = by_ip["192.168.1.21"]
        self.assertEqual(cancelled.last_error, "cancelled_first_match_found")
        self.assertEqual(cancelled.detection.status, "cancelled_first_match_found")
        self.assertFalse(cancelled.detection.budget_exhausted)
        # It must not present as a detection timeout.
        self.assertNotEqual(scan_result_status_code(cancelled), "detection_timeout")

    async def test_detect_targets_skips_probing_configured_collectors(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        targets = (
            DiscoveryTarget(ip="192.168.1.14", source="subnet_unicast"),
            DiscoveryTarget(ip="192.168.1.55", source="subnet_unicast"),
        )

        with patch.object(
            detector,
            "_async_detect_target",
            new=AsyncMock(side_effect=AssertionError("configured collector must not be probed")),
        ) as detect_target:
            results = await detector._async_detect_targets(
                targets,
                skip_probe_ips=frozenset({"192.168.1.14", "192.168.1.55"}),
            )

        detect_target.assert_not_awaited()
        self.assertEqual(len(results), 2)
        for result in results:
            self.assertEqual(result.last_error, "already_configured")
            self.assertEqual(result.detection.status, "already_configured")
            self.assertEqual(result.next_action, "")


    async def test_detect_targets_returns_connected_collector_when_target_deadline_expires(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        target = DiscoveryTarget(ip="192.168.1.14", source="known_ip")

        async def slow_detect(target, *, detection_state=None, **kwargs):
            candidate = CollectorCandidate(
                target_ip=target.ip,
                source=target.source,
                ip=target.ip,
                connected=True,
            )
            if detection_state is not None:
                detection_state.candidate = candidate
            await asyncio.sleep(1.0)
            return OnboardingResult(collector=candidate, connection_mode=target.source)

        with patch.object(detector, "_async_detect_target", new=AsyncMock(side_effect=slow_detect)):
            results = await detector._async_detect_targets(
                (target,),
                total_timeout=0.01,
            )

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result.last_error, "target_detection_timeout")
        self.assertEqual(result.next_action, "manual_driver_selection")
        self.assertIsNotNone(result.detection)
        self.assertEqual(result.detection.status, "target_timeout")
        self.assertTrue(result.detection.budget_exhausted)
        self.assertIsNotNone(result.collector)
        self.assertTrue(result.collector.connected)
        self.assertEqual(result.collector.ip, "192.168.1.14")

    async def test_detect_targets_detects_fast_collector_while_legacy_target_times_out(self) -> None:
        detector = OnboardingDetector(server_ip="192.168.1.50")
        targets = (
            DiscoveryTarget(ip="192.168.1.14", source="known_ip"),
            DiscoveryTarget(ip="192.168.1.55", source="known_ip"),
        )

        async def detect_target(target, *, detection_state=None, **kwargs):
            candidate = CollectorCandidate(
                target_ip=target.ip,
                source=target.source,
                ip=target.ip,
                connected=True,
            )
            if detection_state is not None:
                detection_state.candidate = candidate
            if target.ip == "192.168.1.14":
                await asyncio.sleep(1.0)
                return OnboardingResult(collector=candidate, connection_mode=target.source)
            await asyncio.sleep(0.01)
            return OnboardingResult(
                collector=candidate,
                match=DriverMatch(
                    driver_key="modbus_smg",
                    protocol_family="modbus_smg",
                    model_name="SMG 6200",
                    serial_number="92632500000001",
                    probe_target=ProbeTarget(devcode=1, collector_addr=255, device_addr=1),
                ),
                connection_mode=target.source,
            )

        with patch.object(detector, "_async_detect_target", new=AsyncMock(side_effect=detect_target)):
            results = await detector._async_detect_targets(
                targets,
                total_timeout=0.05,
                concurrency=2,
            )

        by_ip = {
            result.collector.ip: result
            for result in results
            if result.collector is not None
        }
        self.assertIsNotNone(by_ip["192.168.1.55"].match)
        self.assertEqual(by_ip["192.168.1.14"].last_error, "target_detection_timeout")
        self.assertEqual(by_ip["192.168.1.14"].next_action, "manual_driver_selection")


if __name__ == "__main__":
    unittest.main()
