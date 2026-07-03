"""SRNE-compatible Modbus RTU read-only driver."""

from __future__ import annotations

from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusSession
from ..payload.register_decode import decode_ascii_low_bytes, read_spec_set_values
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
        return await read_spec_set_values(session, schema, ascii_style="printable")

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
    return decode_ascii_low_bytes(words)
