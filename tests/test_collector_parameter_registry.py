from __future__ import annotations

from pathlib import Path
import asyncio
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.collector.parameter_registry import query_runtime_collector_values
from custom_components.eybond_local.collector.protocol import EybondHeader
from custom_components.eybond_local.collector.smartess_local import SmartEssLocalSession


class _FakeCollectorTransport:
    def __init__(self, responses: dict[tuple[int, bytes], bytes]) -> None:
        self._responses = dict(responses)

    async def async_send_collector(
        self,
        *,
        fcode: int,
        payload: bytes = b"",
        devcode: int = 0,
        collector_addr: int = 1,
    ) -> tuple[EybondHeader, bytes]:
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


class CollectorParameterRegistryTests(unittest.TestCase):
    def test_query_runtime_collector_values_decodes_safe_metadata_set(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x02"): b"\x00\x02Q0000000000001",
                (2, b"\x04"): b"\x00\x041.11",
                (2, b"\x05"): b"\x00\x058.50.12.3",
                (2, b"\x06"): b"\x00\x061.0",
                (2, b"\x0e"): b"\x00\x0e0925#Hybrid",
                (2, b"\x10"): b"\x00\x10192.168.1.55",
                (2, b"\x15"): b"\x00\x15192.168.1.193,18899,TCP",
                (2, b"\x1e"): b"\x00\x1e1",
                (2, b"\x20"): b"\x00\x20RTU",
                (2, b"\x22"): b"\x00\x229600,8,1,NONE",
                (2, b"\x29"): b"\x00\x29MyWiFi",
                (2, b"\x30"): b"\x00\x30STA:-67",
                (2, b"\x37"): b"\x00\x37-67",
            }
        )

        values = asyncio.run(query_runtime_collector_values(SmartEssLocalSession(transport)))

        self.assertEqual(values["collector_pn"], "Q0000000000001")
        self.assertEqual(values["collector_protocol_version"], "1.11")
        self.assertEqual(values["smartess_collector_version"], "8.50.12.3")
        self.assertEqual(values["collector_hardware_version"], "1.0")
        self.assertEqual(values["collector_local_ip_address"], "192.168.1.55")
        self.assertEqual(values["collector_server_endpoint"], "192.168.1.193,18899,TCP")
        self.assertEqual(values["collector_reboot_required"], "1")
        self.assertEqual(values["collector_transmission_mode"], "RTU")
        self.assertEqual(values["collector_serial_baudrate"], "9600,8,1,NONE")
        self.assertEqual(values["collector_ssid"], "MyWiFi")
        self.assertEqual(values["collector_network_diagnostics"], "STA:-67")
        self.assertEqual(values["collector_signal_strength"], -67)
        self.assertEqual(values["collector_signal_strength_source"], "wifi_rssi")
        self.assertEqual(values["collector_signal_strength_raw"], "-67")
        self.assertEqual(values["smartess_protocol_asset_id"], "0925")
        self.assertEqual(values["smartess_protocol_profile_key"], "smartess_0925")

    def test_query_runtime_collector_values_tolerates_missing_signal_query(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x04"): b"\x00\x041.11",
                (2, b"\x05"): b"\x00\x058.50.12.3",
                (2, b"\x06"): b"\x00\x061.0",
                (2, b"\x0e"): b"\x00\x0e0925#Hybrid",
                (2, b"\x10"): b"\x00\x10192.168.1.55",
                (2, b"\x15"): b"\x00\x15collector-cloud.smartess.example,18899,TCP",
                (2, b"\x1e"): b"\x00\x1e1",
                (2, b"\x20"): b"\x00\x20RTU",
                (2, b"\x22"): b"\x00\x229600,8,1,NONE",
                (2, b"\x30"): b"\x00\x30STA:-67",
            }
        )

        values = asyncio.run(query_runtime_collector_values(SmartEssLocalSession(transport)))

        self.assertEqual(values["collector_network_diagnostics"], "STA:-67")
        self.assertEqual(values["collector_signal_strength"], -67)
        self.assertEqual(values["collector_signal_strength_source"], "wifi_rssi")
        self.assertNotIn("collector_signal_strength_raw", values)

    def test_query_runtime_collector_values_normalizes_gprs_csq_when_rssi_missing(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x04"): b"\x00\x041.11",
                (2, b"\x05"): b"\x00\x058.50.12.3",
                (2, b"\x06"): b"\x00\x061.0",
                (2, b"\x0e"): b"\x00\x0e0925#Hybrid",
                (2, b"\x10"): b"\x00\x10192.168.1.55",
                (2, b"\x15"): b"\x00\x15collector-cloud.smartess.example,18899,TCP",
                (2, b"\x1e"): b"\x00\x1e1",
                (2, b"\x20"): b"\x00\x20RTU",
                (2, b"\x22"): b"\x00\x229600,8,1,NONE",
                (2, b"\x37"): b"\x00\x371",
            }
        )

        values = asyncio.run(query_runtime_collector_values(SmartEssLocalSession(transport)))

        self.assertEqual(values["collector_signal_strength"], -111)
        self.assertEqual(values["collector_signal_strength_source"], "gprs_csq")
        self.assertEqual(values["collector_signal_strength_raw"], "1")

    def test_query_runtime_collector_values_ignores_non_rssi_network_flags(self) -> None:
        transport = _FakeCollectorTransport(
            {
                (2, b"\x04"): b"\x00\x041.11",
                (2, b"\x05"): b"\x00\x058.50.12.3",
                (2, b"\x06"): b"\x00\x061.0",
                (2, b"\x0e"): b"\x00\x0e0925#Hybrid",
                (2, b"\x10"): b"\x00\x10192.168.1.55",
                (2, b"\x15"): b"\x00\x15collector-cloud.smartess.example,18899,TCP",
                (2, b"\x1e"): b"\x00\x1e1",
                (2, b"\x20"): b"\x00\x20RTU",
                (2, b"\x22"): b"\x00\x229600,8,1,NONE",
                (2, b"\x30"): b"\x00\x301,0,0",
                (2, b"\x37"): b"\x00\x37ON",
            }
        )

        values = asyncio.run(query_runtime_collector_values(SmartEssLocalSession(transport)))

        self.assertEqual(values["collector_network_diagnostics"], "1,0,0")
        self.assertEqual(values["collector_signal_strength_raw"], "ON")
        self.assertNotIn("collector_signal_strength", values)
        self.assertNotIn("collector_signal_strength_source", values)


class ProtocolDescriptorParsingTests(unittest.TestCase):
    def test_composite_config_descriptor_extracts_first_field_and_gates_asset_id(self) -> None:
        from custom_components.eybond_local.collector.smartess_local import (
            resolve_protocol_descriptor,
        )

        descriptor = resolve_protocol_descriptor("02FF,0,0,#0#")
        self.assertEqual(descriptor.raw_id, "02FF")
        # 02FF is not a catalog asset id: the parameter-registry decode must
        # not claim an asset id for it (the bound driver owns that value).
        from custom_components.eybond_local.collector.parameter_registry import (
            _decode_protocol_descriptor,
        )
        from custom_components.eybond_local.collector.smartess_local import (
            CollectorQueryResponse,
        )

        values = _decode_protocol_descriptor(
            CollectorQueryResponse(code=0, parameter=14, data=b"", text="02FF,0,0,#0#")
        )
        self.assertEqual(values["smartess_protocol_raw_id"], "02FF")
        self.assertNotIn("smartess_protocol_asset_id", values)

    def test_catalog_known_descriptor_still_resolves_asset_metadata(self) -> None:
        from custom_components.eybond_local.collector.parameter_registry import (
            _decode_protocol_descriptor,
        )
        from custom_components.eybond_local.collector.smartess_local import (
            CollectorQueryResponse,
        )

        values = _decode_protocol_descriptor(
            CollectorQueryResponse(code=0, parameter=14, data=b"", text="0925#SD-HYM-4862HWP")
        )
        self.assertEqual(values["smartess_protocol_asset_id"], "0925")
        self.assertEqual(values["smartess_protocol_profile_key"], "smartess_0925")


if __name__ == "__main__":
    unittest.main()