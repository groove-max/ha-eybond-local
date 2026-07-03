"""Catalog-driven generic Modbus driver.

Unlike the bespoke Modbus drivers (SMG, SRNE, MUST) this driver carries no
device knowledge in Python: everything — identity probe actions, anchors,
register schema, and runtime surface — comes from declarative device packs in
the inverter catalog under the ``modbus_catalog`` protocol key.  Adding a new
Modbus inverter is a catalog + register-schema change, not a code change.

Identity anchors for packs without an explicit model register are
*plausibility* checks: several independent, family-wide range/enum conditions
(state enum in its known value set, SOC 0–100, battery voltage inside the
whole family envelope).  They must never encode electrical-variant specifics
(a 24 V vs 48 V split shares one map), so the decision tree resolves the pack
and runtime data resolves the variant.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..metadata.compiled_detection_catalog import (
    PROBE_ACTION_MODBUS_READ,
    load_compiled_detection_catalog,
)
from ..metadata.detection_decision_tree import evaluate_detection_decision_tree
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusSession
from ..payload.register_decode import (
    decode_ascii_low_bytes,
    decode_ascii_word,
    read_spec_set_values,
)
from .base import InverterDriver
from .catalog_probe import action_for_evidence_key

logger = logging.getLogger(__name__)

PROTOCOL_KEY = "modbus_catalog"


class ModbusCatalogDriver(InverterDriver):
    """Generic read-only driver executing catalog device packs over Modbus."""

    key = PROTOCOL_KEY
    name = "Modbus / Device Catalog"

    @property
    def probe_timeout(self) -> float:
        protocol = self._protocol()
        return protocol.probe_timeout if protocol is not None else 0.0

    @property
    def probe_targets(self) -> tuple[ProbeTarget, ...]:
        protocol = self._protocol()
        if protocol is None:
            return ()
        return tuple(
            ProbeTarget(
                devcode=devcode,
                collector_addr=collector_addr,
                device_addr=device_addr,
            )
            for devcode, collector_addr, device_addr in protocol.probe_targets
        )

    @property
    def register_schema_name(self) -> str:
        surface = self._default_surface()
        return surface.register_schema_name if surface is not None else ""

    @property
    def measurements(self):
        schema = self.register_schema_metadata
        return schema.measurement_descriptions if schema is not None else ()

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        catalog = load_compiled_detection_catalog()
        protocol = self._protocol()
        if protocol is None:
            return None
        tree = catalog.decision_trees.get(PROTOCOL_KEY)
        if tree is None:
            return None

        session = self._session(transport, target)
        evidence: dict[str, object] = {}
        raw_values: dict[str, object] = {}
        executed: list[str] = []
        failed: list[str] = []
        failed_evidence: set[str] = set()

        while True:
            evaluation = evaluate_detection_decision_tree(
                tree,
                evidence,
                unavailable_evidence_keys=frozenset(failed_evidence),
            )
            if evaluation.status != "missing_anchor":
                break
            action = action_for_evidence_key(
                protocol,
                evaluation.missing_anchor_key or "",
                excluded_action_keys=frozenset((*executed, *failed)),
            )
            if (
                action is None
                or action.kind != PROBE_ACTION_MODBUS_READ
                or action.register is None
                or action.count is None
            ):
                failed_evidence.add(evaluation.missing_anchor_key or "")
                continue
            words = await self._read_action(session, action)
            if words is None:
                failed.append(action.key)
                failed_evidence.update(field.key for field in action.evidence_fields)
                continue
            executed.append(action.key)
            registers = {
                action.register + index: int(value) for index, value in enumerate(words)
            }
            for field in action.evidence_fields:
                value = _decode_evidence_field(field, registers)
                if value is None:
                    continue
                raw_values[field.source_key] = value
                evidence[field.key] = value

        if evaluation.status != "resolved":
            return None
        resolution = catalog.resolution_for_candidates(
            protocol_key=PROTOCOL_KEY,
            candidate_keys=evaluation.candidate_keys,
            evidence=evidence,
            decision_path=tuple(
                f"{step.anchor_key}={step.value!r}" for step in evaluation.path
            ),
        )
        if not resolution.surface_key:
            return None
        surface = catalog.surfaces[resolution.surface_key]
        descriptor = next(
            (
                catalog.devices[key]
                for key in resolution.candidate_keys
                if key in catalog.devices
            ),
            None,
        )
        model_name = descriptor.model_name if descriptor is not None else PROTOCOL_KEY
        details: dict[str, Any] = {
            "protocol_id": PROTOCOL_KEY.upper(),
            "identity_evidence": dict(raw_values),
            "catalog_detection": {
                "resolution": resolution.resolution,
                "surface_key": surface.key,
                "confidence": resolution.confidence,
                "evidence": {key: evidence[key] for key in sorted(evidence)},
                "decision_path": list(resolution.decision_path),
            },
        }
        return DetectedInverter(
            driver_key=self.key,
            protocol_family=PROTOCOL_KEY,
            model_name=model_name,
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
                values = await session.read_registers(
                    block.start, block.count, function=block.function
                )
            except Exception as exc:  # pylint: disable=broad-except
                failures.append(
                    {
                        "start": block.start,
                        "count": block.count,
                        "function": block.function,
                        "error": str(exc),
                    }
                )
                continue
            captured_ranges.append(
                {
                    "start": block.start,
                    "count": block.count,
                    "function": block.function,
                    "words": list(values),
                }
            )
        return {
            "capture_kind": "modbus_catalog_register_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "capture_notes": [
                "Catalog-pack Modbus support is read-only; blocks list their"
                " function codes (3 = holding, 4 = input).",
            ],
            "planned_ranges": [
                {"start": block.start, "count": block.count, "function": block.function}
                for block in schema.blocks
            ],
            "captured_ranges": captured_ranges,
            "range_failures": failures,
            "fixture_ranges": [
                {
                    "start": item["start"],
                    "count": item["count"],
                    "function": item["function"],
                    "values": list(item["words"]),
                }
                for item in captured_ranges
            ],
        }

    @staticmethod
    def _protocol():
        return load_compiled_detection_catalog().protocols.get(PROTOCOL_KEY)

    def _default_surface(self):
        catalog = load_compiled_detection_catalog()
        for surface in catalog.surfaces.values():
            if surface.driver_key == self.key and surface.default_for_driver:
                return surface
        return None

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ModbusSession:
        return ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )

    @staticmethod
    async def _read_action(session: ModbusSession, action) -> list[int] | None:
        last_error: Exception | None = None
        for _attempt in range(action.retries + 1):
            try:
                request = session.read_registers(
                    action.register, action.count, function=action.function
                )
                return (
                    await asyncio.wait_for(request, timeout=action.timeout)
                    if action.timeout > 0
                    else await request
                )
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
        logger.debug(
            "modbus_catalog identity action failed action=%s error=%s",
            action.key,
            last_error,
        )
        return None


def _decode_evidence_field(field, registers: dict[int, int]) -> object | None:
    """Decode one compiled evidence field from raw register words."""

    words = [registers.get(field.register + offset) for offset in range(field.words)]
    if any(word is None for word in words):
        return None
    values = [int(word) for word in words if word is not None]
    if field.decoder == "ascii":
        return " ".join(
            part
            for part in (decode_ascii_word(value) for value in values)
            if part
        ).strip() or None
    if field.decoder == "ascii_low_byte":
        return decode_ascii_low_bytes(values) or None
    if field.decoder == "u32_high_first":
        if len(values) < 2:
            return None
        return (values[0] << 16) | values[1]
    # Default: single unsigned word.
    return values[0]
