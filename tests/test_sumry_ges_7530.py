"""Offline qualification only: this schema must not auto-bind an inverter."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from custom_components.eybond_local.drivers.modbus_catalog import ModbusCatalogDriver
from custom_components.eybond_local.drivers.registry import prime_metadata_caches
from custom_components.eybond_local.fixtures.transport import FixtureTransport
from custom_components.eybond_local.metadata.compiled_detection_catalog import (
    load_compiled_detection_catalog,
)
from custom_components.eybond_local.metadata.register_schema_loader import load_register_schema
from custom_components.eybond_local.models import ProbeTarget
from custom_components.eybond_local.payload.modbus import (
    ModbusError,
    ModbusSession,
    build_read_holding_request,
    parse_read_holding_response,
)
from custom_components.eybond_local.payload.register_decode import (
    decode_block,
    read_spec_set_values,
)
from tools.model_catalog import load_models, load_sources


SCHEMA_NAME = "sumry_ges_7530/base.json"
TARGET = ProbeTarget(devcode=1, collector_addr=255, device_addr=1)
# Public telemetry only, issue #49 comment 5881411950. These are separate
# samples, not a simultaneous snapshot and not a model/class fingerprint.
CAPTURES = (
    (
        0x7530,
        "01 03 75 30 00 0A DF CE",
        "01 03 14 02 03 00 00 00 41 00 00 00 00 00 00 04 C9 04 A7 02 3E 01 11 D2 BB",
    ),
    (
        0x7548,
        "01 03 75 48 00 0A 5F D7",
        "01 03 14 04 CD 00 0E 17 6E 00 03 00 6C 00 B1 00 00 00 00 04 A7 00 1D 09 F1",
    ),
    (
        0x756A,
        "01 03 75 6A 00 0A FF DD",
        "01 03 14 04 CB 00 15 17 6E 04 A7 00 1D 04 A7 00 00 01 01 01 59 00 00 E4 C3",
    ),
)


def _captured_registers() -> dict[int, int]:
    registers = {}
    for start, _, response in CAPTURES:
        words = parse_read_holding_response(bytes.fromhex(response), slave_id=1, count=10)
        registers.update({start + index: word for index, word in enumerate(words)})
    return registers


class ReadOnlyTransport(FixtureTransport):
    """Make accidental writes in an offline replay fail loudly."""

    def __init__(self, registers: dict[int, int]) -> None:
        super().__init__(registers=registers, command_responses=None, probe_target=TARGET)
        self.requests: list[bytes] = []

    async def async_send_payload(self, payload, *, route):
        if payload[1] not in (3, 4):
            raise AssertionError("offline qualification must not write")
        self.requests.append(payload)
        return await super().async_send_payload(payload, route=route)


class SumryGesQualificationTests(unittest.TestCase):
    def test_owner_frames_validate_and_decode_only_the_retained_fields(self) -> None:
        schema = load_register_schema(SCHEMA_NAME)
        expected = (
            {"battery_voltage": 51.5, "battery_current": 0.0, "battery_percent": 65},
            {"output_voltage_phase_a": 122.9, "output_current_phase_a": 1.4,
             "output_frequency": 59.98},
            {"mains_voltage_phase_a": 122.7, "mains_current_phase_a": 2.1,
             "mains_frequency": 59.98},
        )
        for (start, request, response), wanted in zip(CAPTURES, expected, strict=True):
            with self.subTest(start=hex(start)):
                self.assertEqual(build_read_holding_request(1, start, 10), bytes.fromhex(request))
                words = parse_read_holding_response(bytes.fromhex(response), slave_id=1, count=10)
                specs = tuple(spec for spec in schema.spec_set("runtime") if spec.key in wanted)
                self.assertEqual(decode_block(start, words, specs), wanted)

    def test_current_signs_follow_documented_signed_words(self) -> None:
        # Synthetic checks: the actual battery sample is zero, not proof of
        # either direction. Positive battery current means discharge in PDF p7.
        schema = load_register_schema(SCHEMA_NAME)
        for key in ("battery_current", "output_current_phase_a", "mains_current_phase_a"):
            spec = next(spec for spec in schema.spec_set("runtime") if spec.key == key)
            for raw, expected in ((100, 10.0), (0xFF9C, -10.0), (0xFFFF, -0.1), (0, 0.0)):
                with self.subTest(key=key, raw=raw):
                    self.assertEqual(decode_block(spec.register, [raw], (spec,)), {key: expected})

    def test_read_plan_is_small_and_contains_no_identity_guess_or_power(self) -> None:
        schema = load_register_schema(SCHEMA_NAME)
        self.assertEqual([(block.start, block.count, block.function) for block in schema.blocks],
                         [(0x7530, 3, 3), (0x7548, 3, 3), (0x756A, 3, 3)])
        self.assertEqual(set(schema.spec_sets), {"runtime"})
        self.assertIsNone(schema.support_read_plan)
        self.assertEqual(schema.binary_sensor_descriptions, ())
        specs = schema.spec_set("runtime")
        self.assertEqual(len(specs), 9)
        self.assertEqual({spec.key for spec in specs},
                         {desc.key for desc in schema.measurement_descriptions})
        for spec in specs:
            with self.subTest(key=spec.key):
                self.assertFalse(any(part in spec.key for part in ("total", "power", "phase_b", "phase_c")))
                self.assertEqual(spec.word_count, 1)
                self.assertEqual(spec.function, 3)
        for key in ("output_voltage_phase_a", "output_current_phase_a",
                    "mains_voltage_phase_a", "mains_current_phase_a"):
            self.assertIn("Phase A", schema.measurement_description(key).name)
        self.assertEqual(schema.measurement_description("output_frequency").suggested_display_precision, 2)

    def test_catalog_records_research_without_runtime_binding(self) -> None:
        model = next(model for model in load_models() if model["model_key"] == "anenji_ges48120m250_500p")
        self.assertEqual(model["lifecycle"], "research")
        self.assertEqual(model["coverage"]["runtime_control_surface"], "none")
        self.assertEqual(model["validation"]["telemetry"], "partial")
        self.assertEqual(model["validation"]["controls"], "none")
        self.assertTrue(all(not variant["device_descriptor_keys"] for variant in model["variants"]))
        sources = {source["source_key"]: source for source in load_sources()}
        self.assertTrue(set(model["source_keys"]).issubset(sources))
        self.assertFalse(any("fingerprint" in sources[key]["assertions"] for key in model["source_keys"]))
        catalog = load_compiled_detection_catalog()
        self.assertFalse(any(surface.register_schema_name == SCHEMA_NAME for surface in catalog.surfaces.values()))

    def test_metadata_startup_does_not_implicitly_load_unbound_schema(self) -> None:
        with patch("custom_components.eybond_local.drivers.registry.load_register_schema",
                   wraps=load_register_schema) as loader:
            prime_metadata_caches()
        self.assertGreater(loader.call_count, 0)
        self.assertFalse(any(call.args[0].removeprefix("builtin:") == SCHEMA_NAME
                             for call in loader.call_args_list))
        for protocol in load_compiled_detection_catalog().protocols.values():
            for action in protocol.probe_actions:
                if action.register is None or action.count is None:
                    continue
                addresses = range(action.register, action.register + action.count)
                with self.subTest(protocol=protocol.key, action=action.key):
                    self.assertFalse(any(address in addresses for address in
                                         (0x7530, 0x7548, 0x756A, 0xC738, 0xC739, 0xC768)))

    def test_documented_diagnostic_frames_are_exact_read_only_requests(self) -> None:
        # Validation of the handoff commands only; these are not probe actions
        # and no responses/identity constants are invented for them.
        for start, count, request in (
            (0xC738, 2, "01 03 C7 38 00 02 78 B2"),
            (0xC768, 1, "01 03 C7 68 00 01 38 A2"),
            (0xC764, 5, "01 03 C7 64 00 05 F9 62"),
        ):
            with self.subTest(start=hex(start)):
                self.assertEqual(build_read_holding_request(1, start, count), bytes.fromhex(request))


class SumryGesReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_offline_subset_reads_only_three_short_fc03_blocks(self) -> None:
        transport = ReadOnlyTransport(_captured_registers())
        session = ModbusSession(transport, route=TARGET.link_route, slave_id=1)
        values = await read_spec_set_values(session, load_register_schema(SCHEMA_NAME))
        self.assertEqual(len(values), 9)
        self.assertEqual(values["battery_voltage"], 51.5)
        self.assertEqual(values["mains_frequency"], 59.98)
        self.assertEqual(transport.requests, [build_read_holding_request(1, start, 3)
                                              for start in (0x7530, 0x7548, 0x756A)])

    async def test_failed_block_is_absent_and_cannot_create_partial_totals(self) -> None:
        schema = load_register_schema(SCHEMA_NAME)
        for block in schema.blocks:
            with self.subTest(block=block.key):
                registers = _captured_registers()
                del registers[block.start]
                transport = ReadOnlyTransport(registers)
                session = ModbusSession(transport, route=TARGET.link_route, slave_id=1)
                values = await read_spec_set_values(session, schema)
                self.assertEqual(len(values), 6)
                missing_keys = {spec.key for spec in schema.spec_set("runtime")
                                if block.start <= spec.register < block.start + block.count}
                self.assertTrue(missing_keys.isdisjoint(values))
                self.assertFalse(any("power" in key or "total" in key for key in values))

    async def test_malformed_block_cannot_decode_as_zero(self) -> None:
        class TruncatedTransport(ReadOnlyTransport):
            async def async_send_payload(self, payload, *, route):
                response = await super().async_send_payload(payload, route=route)
                return response[:-1]

        session = ModbusSession(TruncatedTransport(_captured_registers()),
                                route=TARGET.link_route, slave_id=1)
        with self.assertRaises(ModbusError):
            await read_spec_set_values(session, load_register_schema(SCHEMA_NAME))

    async def test_telemetry_and_unqualified_class_values_do_not_auto_bind(self) -> None:
        # Class 0/10/20 occurs in the PDF, but none is proven for this GES.
        # A made-up nonzero model plus a class enum must not become an anchor.
        for product_class in (None, 0, 10, 20):
            with self.subTest(product_class=product_class):
                registers = _captured_registers()
                if product_class is not None:
                    registers.update({0xC738: 1234, 0xC739: product_class})
                transport = ReadOnlyTransport(registers)
                self.assertIsNone(await ModbusCatalogDriver().async_probe(transport, TARGET))
                # This change adds no automatic requests to the candidate map.
                self.assertFalse(any(0x7530 <= int.from_bytes(req[2:4], "big") <= 0x7596
                                     or 0xC738 <= int.from_bytes(req[2:4], "big") <= 0xC768
                                     for req in transport.requests))


if __name__ == "__main__":
    unittest.main()
