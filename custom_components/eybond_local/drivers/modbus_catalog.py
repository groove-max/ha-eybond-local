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
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusSession
from ..payload.register_decode import (
    decode_block,
    decode_ascii_low_bytes,
    decode_ascii_word,
    read_spec_set_values,
)
from .base import InverterDriver
from .local_register_evidence import (
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
    async_capture_modbus_snapshot,
)
from .read_result import DriverReadMode, DriverReadResult
from .modbus_write_error import ModbusWriteErrorMixin
from .capability_codec import (
    CapabilityPostWriteReadError,
    CapabilityPreWriteReadError,
    decode_capability_value,
    encode_capability_words,
    find_capability,
    merge_capability_register_word,
)
from .catalog_probe import async_walk_detection_dag

logger = logging.getLogger(__name__)

PROTOCOL_KEY = "modbus_catalog"
_CONTROL_SPEC_SET = "controls"
_CONTROL_BLOCK_PREFIX = "control_"
_CONTROL_STATE_KEY = "modbus_catalog_controls"
_CONTROL_CACHE_KEY = "values"
_CONTROL_ROTATION_KEY = "modbus_catalog_control_rotation"


class ModbusCatalogDriver(ModbusWriteErrorMixin, InverterDriver):
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
    def profile_name(self) -> str:
        surface = self._default_surface()
        return surface.profile_name if surface is not None else ""

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

        async def _execute(action) -> str:
            if action.register is None or action.count is None:
                return "failed"
            words = await self._read_action(session, action)
            if words is None:
                return "failed"
            registers = {
                action.register + index: int(value) for index, value in enumerate(words)
            }
            for field in action.evidence_fields:
                value = _decode_evidence_field(field, registers)
                if value is None:
                    continue
                raw_values[field.source_key] = value
                evidence[field.key] = value
            return "executed"

        walk = await async_walk_detection_dag(
            protocol=protocol,
            tree=tree,
            evidence=evidence,
            execute_action=_execute,
            supported_kinds=frozenset({PROBE_ACTION_MODBUS_READ}),
        )
        evaluation = walk.evaluation

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
        # Live entity setup reads capabilities from the DetectedInverter, not
        # from the driver — and this driver is a multi-pack singleton whose
        # own profile is whatever the default surface says, so the pack's
        # profile must ride along with the detection result.
        profile = _profile_for_name(surface.profile_name)
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
            capability_groups=tuple(profile.groups) if profile is not None else (),
            capabilities=tuple(profile.capabilities) if profile is not None else (),
            capability_presets=tuple(profile.presets) if profile is not None else (),
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
        values = await read_spec_set_values(session, schema, ascii_style="printable")
        control_updates = await _read_control_settings(
            session,
            schema,
            runtime_state=runtime_state,
        )
        if runtime_state is None:
            values.update(control_updates)
        else:
            control_cache = _control_value_cache(runtime_state)
            control_cache.update(control_updates)
            values.update(control_cache)
        return DriverReadResult(values=values, mode=DriverReadMode.FULL)

    @property
    def capability_groups(self):
        profile = self.profile_metadata
        return profile.groups if profile is not None else ()

    @property
    def write_capabilities(self):
        profile = self.profile_metadata
        return profile.capabilities if profile is not None else ()

    @property
    def capability_presets(self):
        profile = self.profile_metadata
        return profile.presets if profile is not None else ()

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
        *,
        runtime_state: dict[str, Any] | None = None,
    ) -> Any:
        # A restored entry may carry a profile_name but no materialized
        # capabilities; the driver-level fallback is useless here because
        # this multi-pack singleton's own profile is the default surface's.
        capabilities = inverter.capabilities
        if not capabilities and inverter.profile_name:
            profile = _profile_for_name(inverter.profile_name)
            capabilities = tuple(profile.capabilities) if profile is not None else ()
        capability = find_capability(
            capability_key, capabilities or self.write_capabilities
        )
        raw_words = encode_capability_words(capability, value)
        session = self._session(transport, inverter.probe_target)
        requested_words = list(raw_words)
        if capability.write_function == 6:
            # Firmwares like Growatt SPF only accept single-register writes
            # for their holding config block.
            for offset, word in enumerate(raw_words):
                await session.write_single_holding(
                    capability.register + offset, int(word)
                )
        else:
            if capability.bitmask:
                try:
                    current = await session.read_holding(capability.register, 1)
                except Exception as exc:
                    raise CapabilityPreWriteReadError(
                        f"bitmask_pre_write_read_failed:{capability.key}:{exc}"
                    ) from exc
                if not current:
                    raise CapabilityPreWriteReadError(
                        f"bitmask_read_back_empty:{capability.key}"
                    )
                merged = merge_capability_register_word(
                    capability,
                    current_word=int(current[0]),
                    encoded_word=int(raw_words[0]),
                )
                await session.write_holding(capability.register, [merged])
            else:
                await session.write_holding(
                    capability.register,
                    [int(word) for word in raw_words],
                )

        try:
            observed_words = list(
                await session.read_holding(capability.register, capability.word_count)
            )
        except Exception as exc:
            raise CapabilityPostWriteReadError(
                f"capability_post_write_read_failed:{capability.key}:{exc}"
            ) from exc
        if len(observed_words) != capability.word_count:
            raise CapabilityPostWriteReadError(
                f"capability_post_write_read_empty:{capability.key}"
            )
        if capability.bitmask:
            shift = capability.bitmask_shift
            observed_field = (int(observed_words[0]) & capability.bitmask) >> shift
            comparable_words = [observed_field]
        else:
            comparable_words = observed_words
        if comparable_words != requested_words:
            raise RuntimeError(
                f"capability_write_readback_mismatch:{capability.key}:"
                f"requested={requested_words}:observed={comparable_words}"
            )

        native_value = decode_capability_value(capability, comparable_words)
        if runtime_state is not None:
            _control_value_cache(runtime_state)[capability.value_key] = native_value
        return native_value

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
                "Support capture only reads catalog blocks; it never invokes"
                " the driver's optional write capabilities. Blocks list their"
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

    async def async_capture_local_register_snapshot(
        self,
        transport,
        inverter: DetectedInverter,
        *,
        collector_pn: str,
    ) -> LocalRegisterSnapshot:
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        plans = tuple(
            LocalRegisterReadPlan.for_target(
                inverter.probe_target,
                function=block.function,
                start=block.start,
                count=block.count,
            )
            for block in schema.blocks
        )
        return await async_capture_modbus_snapshot(
            collector_pn=collector_pn,
            driver_key=self.key,
            plans=plans,
            session_factory=lambda target: self._session(transport, target),
        )

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


def _profile_for_name(profile_name: str):
    """Load a pack's controls profile, tolerating packs without one."""

    name = str(profile_name or "").strip()
    if not name:
        return None
    try:
        return load_driver_profile(name)
    except Exception:
        logger.warning("Failed to load catalog pack profile %s", name, exc_info=True)
        return None


async def _read_control_settings(
    session: ModbusSession,
    schema,
    *,
    runtime_state: dict[str, Any] | None,
) -> dict[str, Any]:
    """Read one rotating control block (all blocks for explicit one-shot reads)."""

    try:
        specs = schema.spec_set(_CONTROL_SPEC_SET)
    except KeyError:
        return {}
    if not specs:
        return {}
    blocks = tuple(
        block
        for block in schema.blocks
        if str(block.key).startswith(_CONTROL_BLOCK_PREFIX)
    )
    if not blocks:
        return {}

    if runtime_state is None:
        selected_blocks = blocks
    else:
        raw_index = runtime_state.get(_CONTROL_ROTATION_KEY, 0)
        index = raw_index if type(raw_index) is int and raw_index >= 0 else 0
        selected_blocks = (blocks[index % len(blocks)],)
        runtime_state[_CONTROL_ROTATION_KEY] = (index + 1) % len(blocks)

    values: dict[str, Any] = {}
    for block in selected_blocks:
        block_specs = tuple(
            spec
            for spec in specs
            if spec.function == block.function
            and block.start <= spec.register
            and spec.register + spec.word_count <= block.start + block.count
        )
        if not block_specs:
            continue
        try:
            words = await session.read_registers(
                block.start,
                block.count,
                function=block.function,
            )
        except Exception:  # pylint: disable=broad-except
            continue
        values.update(
            decode_block(
                block.start,
                words,
                block_specs,
                ascii_style="printable",
            )
        )
    return values


def _control_value_cache(runtime_state: dict[str, Any]) -> dict[str, Any]:
    """Return the non-persisted per-session settings cache.

    The cache belongs to the hub-provided driver runtime state. It must never
    be stored in ``DetectedInverter.details``: that mapping carries detection
    evidence and is projected into runtime diagnostics and Support Archives.
    """

    state = runtime_state.setdefault(_CONTROL_STATE_KEY, {})
    if type(state) is not dict:
        state = {}
        runtime_state[_CONTROL_STATE_KEY] = state
    cache = state.setdefault(_CONTROL_CACHE_KEY, {})
    if type(cache) is not dict:
        cache = {}
        state[_CONTROL_CACHE_KEY] = cache
    return cache


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
