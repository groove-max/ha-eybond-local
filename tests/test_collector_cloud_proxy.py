from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.protocol import HEADER_SIZE, decode_header
from custom_components.eybond_local.support.collector_cloud_proxy import (
    AtChunkProxyFilter,
    ObservedCollectorAddress,
    RestoreTarget,
    SessionObservation,
    _looks_like_at_traffic,
    build_restore_at_commands,
    build_restore_frames,
    parse_restore_target,
)


class CollectorCloudProxyTests(unittest.TestCase):
    def test_parse_restore_target_normalizes_protocol(self) -> None:
        target = parse_restore_target("collector-cloud.smartess.example,18899,tcp")

        self.assertEqual(target, RestoreTarget("collector-cloud.smartess.example", 18899, "TCP"))
        self.assertEqual(target.endpoint, "collector-cloud.smartess.example,18899,TCP")

    def test_parse_restore_target_preserves_legacy_host_only_shape(self) -> None:
        target = parse_restore_target("ess.eybond.com")

        self.assertEqual(target, RestoreTarget("ess.eybond.com", 502, "TCP"))
        self.assertEqual(target.endpoint, "ess.eybond.com")

    def test_build_restore_frames_preserves_host_only_restore_payload(self) -> None:
        target = parse_restore_target("ess.eybond.com")

        frames = build_restore_frames(
            target=target,
            devcode=0x0102,
            collector_addr=0x05,
        )

        endpoint_payload = frames[0][1][HEADER_SIZE:]
        self.assertEqual(endpoint_payload[1:].decode("ascii"), "ess.eybond.com")

    def test_build_restore_frames_writes_endpoint_then_apply(self) -> None:
        target = RestoreTarget("collector-cloud.smartess.example", 18899, "TCP")

        frames = build_restore_frames(
            target=target,
            devcode=0x0102,
            collector_addr=0x05,
        )

        self.assertEqual([label for label, _ in frames], ["restore_endpoint", "apply_restore"])

        endpoint_header = decode_header(frames[0][1][:HEADER_SIZE])
        endpoint_payload = frames[0][1][HEADER_SIZE:]
        self.assertEqual(endpoint_header.devcode, 0x0102)
        self.assertEqual(endpoint_header.devaddr, 0x05)
        self.assertEqual(endpoint_header.fcode, 3)
        self.assertEqual(endpoint_payload[0], 21)
        self.assertEqual(endpoint_payload[1:].decode("ascii"), target.endpoint)

        apply_header = decode_header(frames[1][1][:HEADER_SIZE])
        apply_payload = frames[1][1][HEADER_SIZE:]
        self.assertEqual(apply_header.devcode, 0x0102)
        self.assertEqual(apply_header.devaddr, 0x05)
        self.assertEqual(apply_header.fcode, 3)
        self.assertEqual(apply_payload[0], 29)
        self.assertEqual(apply_payload[1:].decode("ascii"), "1")

    def test_build_restore_at_commands_sets_cloud_endpoint(self) -> None:
        target = RestoreTarget("collector-cloud.smartess.example", 18899, "TCP")

        commands = build_restore_at_commands(target=target)

        self.assertEqual(commands, [("restore_endpoint_at", b"AT+CLDSRVHOST1=collector-cloud.smartess.example,18899,TCP\r\n")])

    def test_build_restore_at_commands_preserves_host_only_restore_endpoint(self) -> None:
        target = parse_restore_target("ess.eybond.com")

        commands = build_restore_at_commands(target=target)

        self.assertEqual(commands, [("restore_endpoint_at", b"AT+CLDSRVHOST1=ess.eybond.com\r\n")])

    def test_build_restore_at_commands_accepts_optional_followup(self) -> None:
        target = RestoreTarget("collector-cloud.smartess.example", 18899, "TCP")

        commands = build_restore_at_commands(
            target=target,
            followup_command="AT+INTPARA=29,1",
        )

        self.assertEqual(commands[1], ("restore_followup_at", b"AT+INTPARA=29,1\r\n"))

    def test_detects_plain_at_cloud_traffic(self) -> None:
        self.assertTrue(_looks_like_at_traffic(b"AT+WFSS?\r\n"))
        self.assertFalse(_looks_like_at_traffic(b"\x01\x03\x00d\x00\x03D\x14"))

    def test_session_observation_can_flag_at_session_without_frame_address(self) -> None:
        observed = SessionObservation(collector=ObservedCollectorAddress(), saw_at_traffic=True)

        self.assertTrue(observed.saw_at_traffic)
        self.assertIsNone(observed.collector.devcode)
        self.assertIsNone(observed.collector.collector_addr)

    def test_masks_cldsrvhost1_response_to_original_endpoint(self) -> None:
        observed = SessionObservation(collector=ObservedCollectorAddress())
        collector_filter = AtChunkProxyFilter(
            direction="collector_to_cloud",
            remote="192.168.1.55:40000",
            observed=observed,
            masked_endpoint="collector-cloud.smartess.example,18899,TCP",
        )
        cloud_filter = AtChunkProxyFilter(
            direction="cloud_to_collector",
            remote="192.168.1.55:40000",
            observed=observed,
            masked_endpoint="collector-cloud.smartess.example,18899,TCP",
        )

        forwarded_query, query_events = cloud_filter.feed(b"AT+CLDSRVHOST1?\r\n")
        forwarded_response, response_events = collector_filter.feed(
            b"AT+CLDSRVHOST1:192.168.1.50,18899,TCP\r\n"
        )

        self.assertEqual(forwarded_query, b"AT+CLDSRVHOST1?\r\n")
        self.assertEqual(query_events, [])
        self.assertEqual(
            forwarded_response,
            b"AT+CLDSRVHOST1:collector-cloud.smartess.example,18899,TCP\r\n",
        )
        self.assertEqual(len(response_events), 1)
        self.assertEqual(response_events[0]["kind"], "masked_endpoint_response")
        self.assertFalse(observed.pending_masked_endpoint_response)

    def test_masker_handles_split_at_chunks(self) -> None:
        observed = SessionObservation(collector=ObservedCollectorAddress())
        collector_filter = AtChunkProxyFilter(
            direction="collector_to_cloud",
            remote="192.168.1.55:40000",
            observed=observed,
            masked_endpoint="collector-cloud.smartess.example,18899,TCP",
        )
        cloud_filter = AtChunkProxyFilter(
            direction="cloud_to_collector",
            remote="192.168.1.55:40000",
            observed=observed,
            masked_endpoint="collector-cloud.smartess.example,18899,TCP",
        )

        part_one, _ = cloud_filter.feed(b"AT+CLDSR")
        part_two, _ = cloud_filter.feed(b"VHOST1?\r\n")
        response_one, _ = collector_filter.feed(b"AT+CLDSRVHOST1:192")
        response_two, response_events = collector_filter.feed(b".168.1.50,18899,TCP\r\n")

        self.assertEqual(part_one, b"")
        self.assertEqual(part_two, b"AT+CLDSRVHOST1?\r\n")
        self.assertEqual(response_one, b"")
        self.assertEqual(
            response_two,
            b"AT+CLDSRVHOST1:collector-cloud.smartess.example,18899,TCP\r\n",
        )
        self.assertEqual(len(response_events), 1)


if __name__ == "__main__":
    unittest.main()
