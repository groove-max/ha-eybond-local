"""Tests for ESP EyeBond Collector virtual-bridge detection and gating."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from unittest.mock import AsyncMock, patch  # noqa: E402

from custom_components.eybond_local.collector.at import CollectorAtResponse  # noqa: E402
from custom_components.eybond_local.collector.at_runtime import (  # noqa: E402
    CollectorVirtualBridgeInfo,
    parse_collector_vdtu,
    query_runtime_collector_at_values,
)
from custom_components.eybond_local.connection.models import EybondConnectionSpec  # noqa: E402
from custom_components.eybond_local.models import (  # noqa: E402
    CollectorInfo,
    DetectedInverter,
    DriverMatch,
    ProbeTarget,
)
from custom_components.eybond_local.onboarding.driver_detection import (  # noqa: E402
    DetectedDriverContext,
)
from custom_components.eybond_local.onboarding.eybond import OnboardingDetector  # noqa: E402
from custom_components.eybond_local.runtime.hub import EybondHub  # noqa: E402


# A real ESP EyeBond Collector v0.4.0 reply shape (synthetic version metadata).
_VALID_VDTU = (
    "esp-collector,0.4.0;features=local_only,no_cloud,wifi_params;"
    "uart=2400,8,1,NONE;spacing_ms=100;queue=4"
)


class _FakeLinkManager:
    """Minimal link manager that surfaces one fixed collector identity."""

    def __init__(self) -> None:
        self.connected = True
        self.collector_info = CollectorInfo(remote_ip="192.0.2.14")
        self.transport = object()
        self.collector_at_transport = None


def _make_hub() -> EybondHub:
    hub = EybondHub(
        connection=EybondConnectionSpec(
            server_ip="192.0.2.10",
            collector_ip="192.0.2.14",
            tcp_port=8899,
            udp_port=58899,
            discovery_target="192.0.2.255",
            discovery_interval=30,
            heartbeat_interval=60,
            request_timeout=5.0,
        ),
    )
    hub._link_manager = _FakeLinkManager()
    return hub


class ParseCollectorVdtuTests(unittest.TestCase):
    def test_parses_valid_esp_collector_reply(self) -> None:
        info = parse_collector_vdtu(_VALID_VDTU)

        self.assertTrue(info.is_virtual_bridge)
        self.assertEqual(info.kind, "esp-collector")
        self.assertEqual(info.version, "0.4.0")
        self.assertEqual(
            info.features,
            ("local_only", "no_cloud", "wifi_params"),
        )

    def test_empty_value_is_not_a_bridge(self) -> None:
        self.assertEqual(parse_collector_vdtu(""), CollectorVirtualBridgeInfo())
        self.assertEqual(parse_collector_vdtu(None), CollectorVirtualBridgeInfo())
        self.assertEqual(parse_collector_vdtu("   "), CollectorVirtualBridgeInfo())

    def test_factory_style_non_matching_value_is_not_a_bridge(self) -> None:
        # Factory collectors answer with an error/other value lacking the prefix.
        info = parse_collector_vdtu("ERROR")
        self.assertFalse(info.is_virtual_bridge)
        self.assertEqual(info, CollectorVirtualBridgeInfo())

        info2 = parse_collector_vdtu("Wi-Fi.DTU,1.0")
        self.assertFalse(info2.is_virtual_bridge)

    def test_future_version_with_unknown_features_does_not_raise(self) -> None:
        raw = (
            "esp-collector,9.9.9-rc1;features=local_only,quantum_link,teleport;"
            "uart=2400,8,1,NONE;spacing_ms=50;queue=8;future_key=whatever"
        )
        info = parse_collector_vdtu(raw)

        self.assertTrue(info.is_virtual_bridge)
        self.assertEqual(info.version, "9.9.9-rc1")
        # Unknown feature tokens are kept verbatim, never rejected.
        self.assertEqual(
            info.features,
            ("local_only", "quantum_link", "teleport"),
        )

    def test_truncated_reply_still_parses_prefix_and_version(self) -> None:
        # Reply cut off before the features segment.
        info = parse_collector_vdtu("esp-collector,0.4.0")
        self.assertTrue(info.is_virtual_bridge)
        self.assertEqual(info.version, "0.4.0")
        self.assertEqual(info.features, ())

        # Reply cut off mid-features list.
        info2 = parse_collector_vdtu("esp-collector,0.4.0;features=local_only,")
        self.assertTrue(info2.is_virtual_bridge)
        self.assertEqual(info2.features, ("local_only",))

    def test_prefix_only_reply_is_a_bridge_with_empty_version(self) -> None:
        info = parse_collector_vdtu("esp-collector,")
        self.assertTrue(info.is_virtual_bridge)
        self.assertEqual(info.version, "")
        self.assertEqual(info.features, ())


class QueryRuntimeVdtuTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_emits_collector_vdtu_raw(self) -> None:
        class _BridgeTransport:
            async def async_query(self, command: str) -> CollectorAtResponse:
                value = _VALID_VDTU if command == "VDTU" else ""
                return CollectorAtResponse(command=command, value=value, raw=value)

        values = await query_runtime_collector_at_values(_BridgeTransport())
        self.assertEqual(values["collector_vdtu_raw"], _VALID_VDTU)

    async def test_query_tolerates_vdtu_failure(self) -> None:
        class _FailingVdtuTransport:
            async def async_query(self, command: str) -> CollectorAtResponse:
                if command == "VDTU":
                    raise RuntimeError("collector_at_timeout")
                return CollectorAtResponse(command=command, value="", raw="")

        # A per-command failure must be swallowed (no VDTU key, no exception).
        values = await query_runtime_collector_at_values(_FailingVdtuTransport())
        self.assertNotIn("collector_vdtu_raw", values)


class SnapshotDetectionFlagTests(unittest.TestCase):
    def test_valid_vdtu_sets_virtual_bridge_flag(self) -> None:
        hub = _make_hub()

        snapshot = hub._build_snapshot(extra_values={"collector_vdtu_raw": _VALID_VDTU})

        self.assertTrue(snapshot.collector.collector_virtual_bridge)
        self.assertEqual(snapshot.collector.collector_bridge_kind, "esp-collector")
        self.assertEqual(snapshot.collector.collector_bridge_version, "0.4.0")
        self.assertEqual(
            snapshot.collector.collector_bridge_features,
            ("local_only", "no_cloud", "wifi_params"),
        )
        self.assertTrue(snapshot.values["collector_virtual_bridge"])
        self.assertEqual(snapshot.values["collector_bridge_version"], "0.4.0")
        self.assertEqual(
            snapshot.values["collector_bridge_features"],
            "local_only, no_cloud, wifi_params",
        )

    def test_factory_collector_does_not_set_flag(self) -> None:
        hub = _make_hub()

        snapshot = hub._build_snapshot(extra_values={"collector_vdtu_raw": ""})

        self.assertFalse(snapshot.collector.collector_virtual_bridge)
        self.assertNotIn("collector_virtual_bridge", snapshot.values)

    def test_missing_vdtu_leaves_flag_default(self) -> None:
        # No VDTU key at all (older firmware / missed query) => default behavior.
        hub = _make_hub()

        snapshot = hub._build_snapshot()

        self.assertFalse(snapshot.collector.collector_virtual_bridge)
        self.assertNotIn("collector_virtual_bridge", snapshot.values)


class OnboardingBridgeDetectionTests(unittest.IsolatedAsyncioTestCase):
    """Item 2: the onboarding AT+VDTU probe carries the bridge verdict to confirm."""

    def _make_context(self) -> DetectedDriverContext:
        probe_target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0)
        inverter = DetectedInverter(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="Bench Inverter",
            serial_number="000000000000001",
            probe_target=probe_target,
            details={},
        )
        match = DriverMatch(
            driver_key="pi30",
            protocol_family="pi30",
            model_name="Bench Inverter",
            serial_number="000000000000001",
            probe_target=probe_target,
            details={},
        )

        class _FakeDriver:
            async def async_read_values(self, transport, inverter, **kwargs):
                return {}

        return DetectedDriverContext(driver=_FakeDriver(), inverter=inverter, match=match)

    class _FakeAtTransport:
        def __init__(self, *, host, port, request_timeout, collector_ip) -> None:
            self.collector_ip = collector_ip

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    async def _run_enrich(self, at_values: dict[str, object]) -> DetectedDriverContext:
        detector = OnboardingDetector(server_ip="192.0.2.10")
        context = self._make_context()
        # No async_send_collector on the transport -> the FC query path is skipped,
        # leaving only the AT-value (VDTU) path under test.
        transport = object()
        with (
            patch(
                "custom_components.eybond_local.onboarding.eybond.SharedCollectorAtTransport",
                self._FakeAtTransport,
            ),
            patch(
                "custom_components.eybond_local.onboarding.eybond.query_runtime_collector_at_values",
                new=AsyncMock(return_value=at_values),
            ),
        ):
            await detector._async_enrich_onboarding_runtime_details(
                transport,
                context,
                collector_ip="192.0.2.14",
            )
        return context

    async def test_bridge_vdtu_carries_verdict_to_match_details(self) -> None:
        context = await self._run_enrich({"collector_vdtu_raw": _VALID_VDTU})

        self.assertTrue(context.match.details["collector_virtual_bridge"])
        self.assertEqual(context.match.details["collector_bridge_kind"], "esp-collector")
        self.assertEqual(context.match.details["collector_bridge_version"], "0.4.0")
        self.assertEqual(
            context.match.details["collector_bridge_features"],
            "local_only, no_cloud, wifi_params",
        )
        # The raw VDTU string is intentionally not carried into match details.
        self.assertNotIn("collector_vdtu_raw", context.match.details)

    async def test_factory_collector_carries_no_bridge_verdict(self) -> None:
        context = await self._run_enrich({"collector_vdtu_raw": "ERROR"})

        self.assertNotIn("collector_virtual_bridge", context.match.details)
        self.assertNotIn("collector_vdtu_raw", context.match.details)

    async def test_unanswered_vdtu_carries_no_bridge_verdict(self) -> None:
        # AT query answered, but no VDTU key at all (older firmware / missed probe).
        context = await self._run_enrich({"collector_signal_strength": -67})

        self.assertNotIn("collector_virtual_bridge", context.match.details)


if __name__ == "__main__":
    unittest.main()
