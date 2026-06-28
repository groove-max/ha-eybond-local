from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.link_models import EybondLinkRoute, RawSerialLinkRoute
from custom_components.eybond_local.link_transport import async_send_payload, select_payload_route
from custom_components.eybond_local.models import ProbeTarget


class _LegacyForwardTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, int, int]] = []

    async def async_send_forward(
        self,
        payload: bytes,
        *,
        devcode: int,
        collector_addr: int,
    ) -> bytes:
        self.calls.append((payload, devcode, collector_addr))
        return b"ok"


class _NativeRouteTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, str]] = []

    async def async_send_payload(self, payload: bytes, *, route) -> bytes:
        self.calls.append((payload, route.family))
        return b"native"


class _TimeoutAwareNativeRouteTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, str, float | None]] = []

    async def async_send_payload(
        self,
        payload: bytes,
        *,
        route,
        request_timeout: float | None = None,
    ) -> bytes:
        self.calls.append((payload, route.family, request_timeout))
        return b"timeout-aware"


class _SelectingTransport:
    def select_payload_route(self, route, *, payload_family: str = ""):
        return RawSerialLinkRoute(protocol=payload_family)


class LinkTransportTests(unittest.TestCase):
    def test_probe_target_exposes_link_route_and_payload_address(self) -> None:
        target = ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0x02)

        self.assertEqual(target.payload_address, 0x02)
        self.assertEqual(
            target.link_route,
            EybondLinkRoute(devcode=0x0994, collector_addr=0x01),
        )

    def test_async_send_payload_falls_back_to_legacy_forward_transport(self) -> None:
        transport = _LegacyForwardTransport()
        route = EybondLinkRoute(devcode=0x0994, collector_addr=0x01)

        response = asyncio.run(async_send_payload(transport, b"ping", route=route))

        self.assertEqual(response, b"ok")
        self.assertEqual(transport.calls, [(b"ping", 0x0994, 0x01)])

    def test_async_send_payload_prefers_native_route_transport(self) -> None:
        transport = _NativeRouteTransport()
        route = EybondLinkRoute(devcode=0x0994, collector_addr=0x01)

        response = asyncio.run(async_send_payload(transport, b"ping", route=route))

        self.assertEqual(response, b"native")
        self.assertEqual(transport.calls, [(b"ping", "eybond")])

    def test_async_send_payload_forwards_request_timeout_when_supported(self) -> None:
        transport = _TimeoutAwareNativeRouteTransport()
        route = EybondLinkRoute(devcode=0x0994, collector_addr=0x01)

        response = asyncio.run(
            async_send_payload(
                transport,
                b"ping",
                route=route,
                request_timeout=10.0,
            )
        )

        self.assertEqual(response, b"timeout-aware")
        self.assertEqual(transport.calls, [(b"ping", "eybond", 10.0)])

    def test_select_payload_route_uses_transport_selector(self) -> None:
        selected = select_payload_route(
            _SelectingTransport(),
            EybondLinkRoute(devcode=0x0994, collector_addr=0x01),
            payload_family="pi30_ascii",
        )

        self.assertEqual(selected, RawSerialLinkRoute(protocol="pi30_ascii"))

    def test_select_payload_route_keeps_default_without_selector(self) -> None:
        route = EybondLinkRoute(devcode=0x0994, collector_addr=0x01)

        self.assertIs(select_payload_route(object(), route, payload_family="pi30_ascii"), route)


if __name__ == "__main__":
    unittest.main()
