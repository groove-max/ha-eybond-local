"""SRNE-compatible Modbus RTU read-only driver."""

from __future__ import annotations

from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusError, ModbusSession
from ..payload.register_decode import decode_ascii_low_bytes, read_spec_set_values
from ..poll_policy import PollPolicy
from .base import InverterDriver
from .local_register_evidence import (
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
    async_capture_modbus_snapshot,
)
from .read_result import DriverReadMode, DriverReadResult
from .support_diagnostics import capture_support_reads


# SRNE Modbus V2.07, P01 DC Data Area (addresses in that table are hexadecimal).
# Documented short reads after an explicit rejection of the 0x0100/18 block.
# In particular, do not cross the optional/new fields or reserved 0x010D again.
_DC_DIAGNOSTIC_RANGES = (
    (0x0100, 3, "battery"),
    (0x0107, 3, "pv1"),
    (0x010B, 1, "charge_state"),
    (0x010E, 1, "charge_power"),
    (0x010F, 3, "pv2"),
)
_DC_DIAGNOSTIC_TIMEOUT = 15.0


class SrneModbusDriver(InverterDriver):
    """Read-only driver for SRNE-compatible Modbus devices.

    This driver never issues capability writes (``async_write_capability`` always
    raises ``unsupported_capability``), so it must NOT opt into Modbus write-error
    classification -- it keeps the neutral base (empty) classification.
    """

    key = "srne_modbus"
    poll_policy = PollPolicy(min_auto_interval=5.0, max_auto_interval=90.0)
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

        catalog = load_compiled_detection_catalog()
        evidence = {
            "identity.product_info": product_info,
            "protocol.protocol_id": "SRNE_MODBUS",
        }
        resolution = catalog.resolve_family(protocol_key=self.key, evidence=evidence)
        if not resolution.surface_key:
            return None
        surface = catalog.surfaces[resolution.surface_key]
        details = {
            "product_info": product_info,
            "protocol_id": "SRNE_MODBUS",
            "catalog_detection": {
                "resolution": resolution.resolution,
                "surface_key": surface.key,
                "confidence": resolution.confidence,
                "candidate_keys": list(resolution.candidate_keys),
                "catalog_version": resolution.catalog_version,
                "descriptor_revisions": list(resolution.descriptor_revisions),
                "evidence_fingerprint": resolution.evidence_fingerprint,
                "evidence": evidence,
                "decision_path": list(resolution.decision_path),
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
    ) -> DriverReadResult:
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        session = self._session(transport, inverter.probe_target)
        values = await read_spec_set_values(
            session,
            schema,
            ascii_style="printable",
            illegal_address_fallbacks={
                (3, 0x0100, 18): tuple((start, count) for start, count, _ in _DC_DIAGNOSTIC_RANGES)
            },
        )
        return DriverReadResult(values=values, mode=DriverReadMode.FULL)

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
        *,
        runtime_state: dict[str, Any] | None = None,
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
        needs_dc_diagnostics = False
        for block in schema.blocks:
            try:
                values = await session.read_holding(block.start, block.count)
            except Exception as exc:
                if (block.start, block.count) == (0x0100, 18) and _is_address_rejection(exc):
                    needs_dc_diagnostics = True
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
        evidence = {
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
        # Finish the ordinary evidence first, so extra diagnostics cannot consume
        # its budget. Keep the rejected parent and successful subreads separate.
        if needs_dc_diagnostics:
            diagnostics = await _capture_dc_subranges(session)
            evidence["dc_subrange_diagnostics"] = diagnostics
            evidence["fixture_ranges"].extend(
                {
                    "start": item["start"],
                    "count": item["count"],
                    "values": list(item["words"]),
                }
                for item in diagnostics["captured_ranges"]
            )
        return evidence

    def local_register_read_plans(
        self, inverter: DetectedInverter
    ) -> tuple[LocalRegisterReadPlan, ...]:
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        # Bounded background evidence uses the same documented DC groups, not
        # the unsupported parent or zero-filled holes. Ordinary polling still
        # prefers the compact parent when the device accepts it.
        plans = []
        for block in schema.blocks:
            ranges = (
                tuple((start, count) for start, count, _ in _DC_DIAGNOSTIC_RANGES)
                if (block.start, block.count) == (0x0100, 18)
                else ((block.start, block.count),)
            )
            plans.extend(
                LocalRegisterReadPlan.for_target(
                    inverter.probe_target, function=3, start=start, count=count
                )
                for start, count in ranges
            )
        return tuple(plans)

    async def async_capture_local_register_snapshot(
        self, transport, inverter: DetectedInverter, *, collector_pn: str
    ) -> LocalRegisterSnapshot:
        return await async_capture_modbus_snapshot(
            collector_pn=collector_pn,
            driver_key=self.key,
            plans=self.local_register_read_plans(inverter),
            session_factory=lambda target: self._session(transport, target),
        )

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ModbusSession:
        return ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )


def _is_address_rejection(error: Exception) -> bool:
    return isinstance(error, ModbusError) and str(error) == "exception_code:2"


async def _capture_dc_subranges(session: ModbusSession) -> dict[str, Any]:
    """Try at most five documented DC groups with one shared deadline.

    Preserve normal Modbus retry semantics, but stop this diagnostic sequence on
    any error except an explicit illegal-address response. No recursive splitting,
    phase probing, capability writes or mutation of the runtime schema occurs.
    """

    result = await capture_support_reads(
        session,
        _DC_DIAGNOSTIC_RANGES,
        timeout_seconds=_DC_DIAGNOSTIC_TIMEOUT,
        source="SRNE Modbus V2.07, P01 DC Data Area",
        purpose="support_only_rejected_dc_block",
    )
    result["trigger_range"] = {"start": 0x0100, "count": 18}
    return result


def _srne_default_schema_name() -> str:
    for surface in load_compiled_detection_catalog().surfaces.values():
        if surface.driver_key == SrneModbusDriver.key and surface.default_for_driver:
            return surface.register_schema_name
    return "srne_modbus/base.json"


def _looks_like_srne_product_info(product_info: str) -> bool:
    return len(product_info) >= 3 and "SR" in product_info.upper()


def _decode_product_info(words: list[int]) -> str:
    return decode_ascii_low_bytes(words)
