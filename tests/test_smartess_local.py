from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.protocol import EybondHeader  # noqa: E402
from custom_components.eybond_local.collector.smartess_local import (  # noqa: E402
    QUERY_COLLECTOR_VERSION,
    QUERY_PROTOCOL_DESCRIPTOR,
    SET_TARGET_SSID,
    CollectorQueryResponse,
    SmartEssLocalError,
    SmartEssLocalSession,
    build_query_collector_payload,
    build_set_collector_payload,
    parse_query_collector_response,
    parse_set_collector_response,
    resolve_protocol_descriptor,
)


class _FakeCollectorTransport:
    def __init__(self, responses: dict[tuple[int, bytes], bytes]) -> None:
        self._responses = dict(responses)
        self.requests: list[dict[str, object]] = []

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
        self.requests.append(
            {
                "fcode": fcode,
                "payload": payload,
                "devcode": devcode,
                "collector_addr": collector_addr,
            }
        )
        response = self._responses[(fcode, payload)]
        return (
            EybondHeader(
                tid=1,
                devcode=devcode,
                wire_len=len(response) + 2,
                devaddr=collector_addr,
                fcode=fcode,
            ),
            response,
        )


class SmartEssLocalHelperTests(unittest.TestCase):
    def test_build_query_collector_payload_supports_multiple_parameters(self) -> None:
        self.assertEqual(build_query_collector_payload(5, 14), b"\x05\x0e")

    def test_build_set_collector_payload_requires_ascii(self) -> None:
        with self.assertRaisesRegex(SmartEssLocalError, "set_value_not_ascii"):
            build_set_collector_payload(41, "мережа")

    def test_parse_query_collector_response_decodes_ascii_tail(self) -> None:
        response = parse_query_collector_response(b"\x00\x0e0925#Hybrid")

        self.assertEqual(response.code, 0)
        self.assertEqual(response.parameter, 14)
        self.assertEqual(response.text, "0925#Hybrid")
        self.assertEqual(response.data, b"0925#Hybrid")

    def test_parse_query_collector_response_rejects_short_payload(self) -> None:
        with self.assertRaisesRegex(SmartEssLocalError, "query_response_too_short"):
            parse_query_collector_response(b"\x00")

    def test_parse_set_collector_response_requires_status_and_parameter(self) -> None:
        response = parse_set_collector_response(bytes((0, SET_TARGET_SSID)))

        self.assertEqual(response.status, 0)
        self.assertEqual(response.parameter, SET_TARGET_SSID)

    def test_resolve_protocol_descriptor_parses_normal_asset_id(self) -> None:
        descriptor = resolve_protocol_descriptor("0925#SD-HYM-4862HWP")

        self.assertEqual(descriptor.raw_id, "0925")
        self.assertEqual(descriptor.asset_id, "0925")
        self.assertEqual(descriptor.asset_name, "0925.json")
        self.assertEqual(descriptor.suffix, "SD-HYM-4862HWP")
        self.assertFalse(descriptor.uses_legacy_alias)

    def test_resolve_protocol_descriptor_applies_legacy_0230_alias(self) -> None:
        descriptor = resolve_protocol_descriptor("0230#legacy")

        self.assertEqual(descriptor.raw_id, "0230")
        self.assertEqual(descriptor.asset_id, "0942")
        self.assertEqual(descriptor.asset_name, "0942.json")
        self.assertTrue(descriptor.uses_legacy_alias)


class SmartEssLocalSessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_collector_version_uses_fc2(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x05"): b"\x00\x051.2.3",
            }
        )
        session = SmartEssLocalSession(transport)

        version = await session.query_collector_version()

        self.assertEqual(version, "1.2.3")
        self.assertEqual(
            transport.requests,
            [
                {
                    "fcode": 2,
                    "payload": b"\x05",
                    "devcode": 1,
                    "collector_addr": 1,
                }
            ],
        )

    async def test_query_protocol_descriptor_returns_parsed_asset(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x0e"): b"\x00\x0e0925#Hybrid",
            }
        )
        session = SmartEssLocalSession(transport)

        descriptor = await session.query_protocol_descriptor()

        self.assertEqual(descriptor.asset_id, "0925")
        self.assertEqual(descriptor.asset_name, "0925.json")
        self.assertEqual(descriptor.suffix, "Hybrid")

    async def test_query_known_protocol_resolves_catalog_entry(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x0e"): b"\x00\x0e0911#PVInverter",
            }
        )
        session = SmartEssLocalSession(transport)

        protocol = await session.query_known_protocol()

        assert protocol is not None
        self.assertEqual(protocol.asset_id, "0911")
        self.assertEqual(protocol.profile_key, "smartess_0911")
        self.assertEqual(protocol.device_addresses, (3,))

    async def test_query_known_protocol_returns_none_for_unknown_asset(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x0e"): b"\x00\x0e9999#Unknown",
            }
        )
        session = SmartEssLocalSession(transport)

        self.assertIsNone(await session.query_known_protocol())

    async def test_set_collector_sends_fc3_ascii_payload(self) -> None:
        payload = build_set_collector_payload(SET_TARGET_SSID, "MyWiFi")
        transport = _FakeCollectorTransport(
            {
                (3, payload): bytes((0, SET_TARGET_SSID)),
            }
        )
        session = SmartEssLocalSession(transport)

        response = await session.set_collector(SET_TARGET_SSID, "MyWiFi")

        self.assertEqual(response.status, 0)
        self.assertEqual(response.parameter, SET_TARGET_SSID)

    async def test_query_collector_raises_on_nonzero_status(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, bytes((QUERY_COLLECTOR_VERSION,))): bytes((1, QUERY_COLLECTOR_VERSION)),
            }
        )
        session = SmartEssLocalSession(transport)

        with self.assertRaisesRegex(SmartEssLocalError, "query_failed"):
            await session.query_collector_version()


if __name__ == "__main__":
    unittest.main()