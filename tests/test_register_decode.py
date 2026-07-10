from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.fixtures.transport import FixtureTransport  # noqa: E402
from custom_components.eybond_local.models import ProbeTarget, RegisterValueSpec  # noqa: E402
from custom_components.eybond_local.payload.modbus import (  # noqa: E402
    ModbusSession,
    build_read_request,
    crc16_modbus,
    parse_read_registers_response,
)
from custom_components.eybond_local.payload.register_decode import (  # noqa: E402
    decode_ascii_word,
    decode_block,
    decode_raw_value,
)


def _spec(**kwargs) -> RegisterValueSpec:
    return RegisterValueSpec(key=kwargs.pop("key", "value"), register=kwargs.pop("register", 10), **kwargs)


class DecodeBlockTests(unittest.TestCase):
    def test_multiplier_scales_and_rounds(self) -> None:
        specs = (_spec(multiplier=-0.1, signed=True, decimals=1),)
        decoded = decode_block(10, [1200], specs)
        self.assertEqual(decoded["value"], -120.0)

    def test_multiplier_takes_precedence_over_divisor(self) -> None:
        specs = (_spec(multiplier=2.0, divisor=10),)
        decoded = decode_block(10, [21], specs)
        self.assertEqual(decoded["value"], 42.0)

    def test_divisor_still_applies_without_multiplier(self) -> None:
        specs = (_spec(divisor=10, decimals=1),)
        decoded = decode_block(10, [512], specs)
        self.assertEqual(decoded["value"], 51.2)

    def test_divisor_without_decimals_keeps_implied_precision(self) -> None:
        specs = (_spec(divisor=10),)
        decoded = decode_block(10, [512], specs)
        self.assertEqual(decoded["value"], 51.2)

    def test_explicit_zero_decimals_rounds_to_integer_despite_divisor(self) -> None:
        specs = (_spec(divisor=10, decimals=0),)
        decoded = decode_block(10, [512], specs)
        self.assertEqual(decoded["value"], 51)

    def test_offset_applies_after_the_all_ones_sentinel(self) -> None:
        specs = (_spec(offset=-1000, divisor=10, decimals=1),)
        # A live reading shifts by the offset before scaling...
        decoded = decode_block(10, [1385], specs)
        self.assertEqual(decoded["value"], 38.5)
        # ...but the all-ones sentinel is checked on the WIRE value, so the
        # offset must not unmask 0xFFFF into a plausible number.
        decoded = decode_block(10, [0xFFFF], specs, all_ones_unavailable=True)
        self.assertIsNone(decoded["value"])

    def test_signed_u32_combines_negative_value(self) -> None:
        specs = (_spec(word_count=2, signed=True),)
        decoded = decode_block(10, [0xFFFF, 0xFFF6], specs)
        self.assertEqual(decoded["value"], -10)

    def test_unsigned_u32_unchanged(self) -> None:
        specs = (_spec(word_count=2),)
        decoded = decode_block(10, [0x0001, 0x0002], specs)
        self.assertEqual(decoded["value"], 0x0001_0002)

    def test_ascii_styles_filter_differently(self) -> None:
        # 0x2A is "*" — printable but outside the model charset.
        self.assertEqual(decode_ascii_word(0x2A41, style="printable"), "*A")
        self.assertEqual(decode_ascii_word(0x2A41, style="model"), "A")

    def test_missing_register_defaults_to_zero(self) -> None:
        raw = decode_raw_value({}, _spec())
        self.assertEqual(raw, 0)


class ReadInputRegistersTests(unittest.IsolatedAsyncioTestCase):
    def test_build_read_request_function_04(self) -> None:
        frame = build_read_request(1, 64, 2, function=0x04)
        self.assertEqual(frame[1], 0x04)
        self.assertEqual(int.from_bytes(frame[-2:], "little"), crc16_modbus(frame[:-2]))

    def test_build_read_request_rejects_unknown_function(self) -> None:
        with self.assertRaises(ValueError):
            build_read_request(1, 64, 2, function=0x02)

    def test_parse_read_registers_response_function_04(self) -> None:
        body = bytearray([1, 0x04, 4, 0x00, 0x2A, 0x01, 0x00])
        body.extend(crc16_modbus(body).to_bytes(2, "little"))
        values = parse_read_registers_response(
            bytes(body), slave_id=1, count=2, function=0x04
        )
        self.assertEqual(values, [42, 256])

    def test_parse_maps_exception_to_function_specific_code(self) -> None:
        body = bytearray([1, 0x84, 0x02])
        body.extend(b"\x00\x00")
        body.extend(crc16_modbus(body).to_bytes(2, "little"))
        with self.assertRaises(Exception) as ctx:
            parse_read_registers_response(
                bytes(body[:5]), slave_id=1, count=1, function=0x04
            )
        self.assertIn("exception_code", str(ctx.exception))

    async def test_session_read_input_uses_input_register_space(self) -> None:
        target = ProbeTarget(devcode=2477, collector_addr=16, device_addr=1)
        transport = FixtureTransport(
            registers={100: 11},
            input_registers={100: 22},
            command_responses=None,
            probe_target=target,
        )
        session = ModbusSession(transport, route=target.link_route, slave_id=1)
        self.assertEqual(await session.read_holding(100, 1), [11])
        self.assertEqual(await session.read_input(100, 1), [22])


class SchemaFunctionValidationTests(unittest.TestCase):
    def test_spec_function_must_be_read_function(self) -> None:
        from custom_components.eybond_local.metadata.register_schema_loader import (
            _parse_read_function,
        )

        self.assertEqual(_parse_read_function(4), 4)
        with self.assertRaises(ValueError):
            _parse_read_function(5)

    def test_fixture_payload_splits_register_spaces(self) -> None:
        from custom_components.eybond_local.fixtures.transport import (
            load_fixture_payload,
        )

        transport, _raw = load_fixture_payload(
            {
                "fixture_version": 1,
                "probe_target": {"devcode": 1, "collector_addr": 255, "device_addr": 1},
                "ranges": [
                    {"start": 100, "values": [11]},
                    {"start": 100, "function": 4, "values": [22]},
                ],
            },
            name="split-spaces",
        )
        self.assertEqual(transport._registers[100], 11)
        self.assertEqual(transport._input_registers[100], 22)


class CatalogSchemaCoverageTests(unittest.TestCase):
    def test_uncovered_spec_fails_schema_load(self) -> None:
        import json
        import tempfile

        from custom_components.eybond_local.metadata.register_schema_loader import (
            clear_register_schema_loader_cache,
            load_register_schema,
            set_external_register_schema_roots,
        )

        schema = {
            "schema_key": "coverage_probe",
            "title": "Coverage Probe",
            "driver_key": "modbus_catalog",
            "protocol_family": "coverage_probe",
            "blocks": [{"key": "core", "start": 0, "count": 4, "function": 3}],
            "enum_tables": {},
            "spec_sets": {
                "runtime": [
                    # Same address range as the block but the INPUT space:
                    # no block covers it, so loading must fail.
                    {"key": "orphan", "register": 1, "function": 4}
                ]
            },
            "measurement_descriptions": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "coverage_probe").mkdir()
            (root / "coverage_probe" / "base.json").write_text(
                json.dumps(schema), encoding="utf-8"
            )
            set_external_register_schema_roots((root,))
            try:
                with self.assertRaises(ValueError) as ctx:
                    load_register_schema("coverage_probe/base.json")
                self.assertIn("uncovered_spec", str(ctx.exception))
            finally:
                set_external_register_schema_roots(())
                clear_register_schema_loader_cache()


if __name__ == "__main__":
    unittest.main()
