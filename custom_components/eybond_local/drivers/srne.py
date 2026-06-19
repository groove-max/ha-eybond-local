"""SRNE-compatible Modbus RTU read-only driver."""

from __future__ import annotations

from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget, RegisterValueSpec
from ..payload.modbus import ModbusSession, to_signed_16
from .base import InverterDriver


class SrneModbusDriver(InverterDriver):
    """Read-only driver for SRNE-compatible Modbus devices."""

    key = "srne_modbus"
    name = "SRNE / Modbus"

    @property
    def probe_timeout(self) -> float:
        return load_compiled_detection_catalog().protocols[self.key].probe_timeout

    @property
    def probe_targets(self) -> tuple[ProbeTarget, ...]:
        return tuple(
            ProbeTarget(
                devcode=devcode,
                collector_addr=collector_addr,
                device_addr=device_addr,
            )
            for devcode, collector_addr, device_addr
            in load_compiled_detection_catalog().protocols[self.key].probe_targets
        )

    @property
    def register_schema_name(self) -> str:
        return _srne_default_schema_name()

    @property
    def measurements(self):
        schema = self.register_schema_metadata
        return schema.measurement_descriptions if schema is not None else ()

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        schema_name = self.register_schema_name
        schema = load_register_schema(schema_name)
        session = self._session(transport, target)
        try:
            product_block = schema.block("serial")
            product_words = await session.read_holding(
                product_block.start,
                product_block.count,
            )
        except Exception:
            return None

        product_info = _decode_product_info(product_words)
        if not _looks_like_srne_product_info(product_info):
            return None

        surface = load_compiled_detection_catalog().surfaces["srne_modbus_read_only"]
        details = {
            "product_info": product_info,
            "protocol_id": "SRNE_MODBUS",
            "catalog_detection": {
                "resolution": "exact",
                "surface_key": surface.key,
                "evidence": {
                    "identity.product_info": product_info,
                    "protocol.protocol_id": "SRNE_MODBUS",
                },
            },
        }
        return DetectedInverter(
            driver_key=self.key,
            protocol_family="srne_modbus",
            model_name=f"SRNE {product_info}",
            serial_number="",
            probe_target=target,
            variant_key=surface.variant_key,
            details=details,
            profile_name=surface.profile_name,
            register_schema_name=surface.register_schema_name,
        )

    async def async_read_values(
        self,
        transport,
        inverter: DetectedInverter,
        *,
        runtime_state: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        now_monotonic: float | None = None,
    ) -> dict[str, Any]:
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        session = self._session(transport, inverter.probe_target)
        values: dict[str, Any] = {}
        for block in schema.blocks:
            try:
                words = await session.read_holding(block.start, block.count)
            except Exception:
                continue
            specs = tuple(
                spec
                for spec in schema.spec_set("runtime")
                if block.start <= spec.register
                and spec.register + spec.word_count <= block.start + block.count
            )
            values.update(_decode_block(block.start, words, specs))
        return values

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        raise ValueError(f"unsupported_capability:{self.key}:{capability_key}")

    async def async_capture_support_evidence(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        session = self._session(transport, inverter.probe_target)
        captured_ranges: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for block in schema.blocks:
            try:
                values = await session.read_holding(block.start, block.count)
            except Exception as exc:
                failures.append(
                    {
                        "start": block.start,
                        "count": block.count,
                        "error": str(exc),
                    }
                )
                continue
            captured_ranges.append(
                {
                    "start": block.start,
                    "count": block.count,
                    "words": list(values),
                }
            )
        return {
            "capture_kind": "srne_modbus_register_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "capture_notes": [
                "SRNE-compatible support is read-only and expects Modbus RTU at 9600 8N1, slave address 1."
            ],
            "planned_ranges": [
                {"start": block.start, "count": block.count}
                for block in schema.blocks
            ],
            "captured_ranges": captured_ranges,
            "range_failures": failures,
            "fixture_ranges": [
                {
                    "start": item["start"],
                    "count": item["count"],
                    "values": list(item["words"]),
                }
                for item in captured_ranges
            ],
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ModbusSession:
        return ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )


def _srne_default_schema_name() -> str:
    for surface in load_compiled_detection_catalog().surfaces.values():
        if surface.driver_key == SrneModbusDriver.key and surface.default_for_driver:
            return surface.register_schema_name
    return "srne_modbus/base.json"


def _looks_like_srne_product_info(product_info: str) -> bool:
    return len(product_info) >= 3 and "SR" in product_info.upper()


def _decode_product_info(words: list[int]) -> str:
    chars: list[str] = []
    for word in words:
        byte = int(word) & 0xFF
        if byte in (0x00, 0xFF):
            continue
        char = chr(byte)
        if char.isprintable():
            chars.append(char)
    return "".join(chars).strip()


def _decode_ascii_word(value: int) -> str:
    chars = []
    for byte in ((int(value) >> 8) & 0xFF, int(value) & 0xFF):
        if byte in (0x00, 0xFF):
            continue
        char = chr(byte)
        if char.isprintable():
            chars.append(char)
    return "".join(chars).strip()


def _decode_block(
    start_register: int,
    words: list[int],
    specs: tuple[RegisterValueSpec, ...],
) -> dict[str, Any]:
    registers = {start_register + index: int(value) for index, value in enumerate(words)}
    decoded: dict[str, Any] = {}
    for spec in specs:
        raw = _decode_raw_value(registers, spec)
        if spec.enum_map is not None:
            decoded[spec.key] = spec.enum_map.get(raw, f"Unknown ({raw})")
        elif spec.divisor and isinstance(raw, int):
            decoded[spec.key] = round(raw / spec.divisor, spec.decimals or 0)
        else:
            decoded[spec.key] = raw
    return decoded


def _decode_raw_value(registers: dict[int, int], spec: RegisterValueSpec) -> int | str:
    if spec.combine == "ascii_low_byte":
        return _decode_product_info(
            [registers.get(spec.register + offset, 0) for offset in range(spec.word_count)]
        )
    if spec.combine == "ascii":
        chars: list[str] = []
        for offset in range(spec.word_count):
            chars.append(_decode_ascii_word(registers.get(spec.register + offset, 0)))
        return "".join(chars).strip()
    if spec.word_count >= 2:
        high = registers.get(spec.register, 0)
        low = registers.get(spec.register + 1, 0)
        if spec.combine == "u32_low_first":
            raw = (low << 16) | high
        else:
            raw = (high << 16) | low
    else:
        raw = registers.get(spec.register, 0)
    if spec.signed and isinstance(raw, int) and spec.word_count == 1:
        return to_signed_16(raw)
    return raw
