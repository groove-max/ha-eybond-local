from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.link_models import EybondLinkRoute
from custom_components.eybond_local.link_transport import async_send_payload
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


if __name__ == "__main__":
    unittest.main()
