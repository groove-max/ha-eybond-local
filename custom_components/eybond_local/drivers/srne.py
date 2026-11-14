"""SRNE Modbus driver; writes are restricted to the exact SMX-II identity."""

from __future__ import annotations

from dataclasses import replace
import logging
import math
from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.detection_decision_tree import evaluate_detection_decision_tree_static
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusError, ModbusSession
from ..payload.register_decode import decode_ascii_low_bytes, read_spec_set_values
from ..poll_policy import PollPolicy
from .base import InverterDriver
from .capability_codec import (
    CapabilityPostWriteReadError,
    CapabilityPreWriteReadError,
    decode_capability_value,
    encode_capability_words,
    find_capability,
    merge_capability_register_word,
)
from .local_register_evidence import (
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
    async_capture_modbus_snapshot,
)
from .read_result import DriverReadMode, DriverReadResult
from .modbus_write_error import ModbusWriteErrorMixin
from .support_diagnostics import capture_support_reads
from .write_confirmation import (
    load_write_confirmation,
    start_write_confirmation,
    store_write_confirmation,
    write_confirmation_diagnostics,
)


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

# The exact-unit SMX-II overlay includes settings, identity and lifetime counters
# which do not need the same cadence as electrical readings. Preserve the full
# cold-start snapshot; thereafter rotate slow blocks in per-link runtime state.
# Every selected fast block is read on every poll, including after read failures.
_SMX_SCHEMA_KEY = "srne_modbus_smx_ii_24v"
_SMX_PRODUCT_INFO = "SR-2206260036-300917"
_SMX_SURFACE_KEY = "srne_modbus_smx_ii"
_SMX_PROFILE = "srne_modbus/smx_ii.json"
_SMX_POLL_STATE_KEY = "srne_smx_ii_poll"
_SMX_WRITE_READBACK_KEY = "pending_write_readback"
_SMX_SLOW_SWEEP_TARGET_SECONDS = 600.0
_SMX_FAST_BLOCK_KEYS = frozenset({
    "config", "controller_dc", "pv1", "charge_state", "charge_summary",
    "status", "live", "energy_today", "energy_utility_today",
})
_LOGGER = logging.getLogger(__name__)


class SrneModbusDriver(ModbusWriteErrorMixin, InverterDriver):
    """Generic SRNE remains read-only; exact owner SMX-II may expose controls."""

    key = "srne_modbus"
    cloud_probe_codecs = ("modbus_rtu",)
    cloud_learning_supported = True
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
        # Resolve an exact catalog-backed identity before considering controls.
        # A family match never receives the SMX-II write profile.
        evaluation = evaluate_detection_decision_tree_static(
            catalog.decision_trees[self.key], evidence
        )
        resolution = catalog.resolution_for_candidates(
            protocol_key=self.key,
            candidate_keys=(
                evaluation.candidate_keys
                if evaluation.status == "resolved"
                else ()
            ),
            evidence=evidence,
        )
        if resolution.resolution != "exact":
            resolution = catalog.resolve_family(protocol_key=self.key, evidence=evidence)
        if not resolution.surface_key:
            return None
        surface = catalog.surfaces[resolution.surface_key]
        profile = (
            load_driver_profile(_SMX_PROFILE)
            if surface.key == _SMX_SURFACE_KEY
            and resolution.resolution == "exact"
            and product_info == _SMX_PRODUCT_INFO
            and surface.profile_name == _SMX_PROFILE
            else None
        )
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
        if schema.key == _SMX_SCHEMA_KEY and runtime_state is not None:
            return await _async_read_smx_ii_values(
                session, schema, inverter, runtime_state, poll_interval=poll_interval
            )
        values = await read_spec_set_values(
            session,
            schema,
            ascii_style="printable",
            illegal_address_fallbacks={
                (3, 0x0100, 18): tuple((start, count) for start, count, _ in _DC_DIAGNOSTIC_RANGES)
            },
        )
        return DriverReadResult(
            values=values,
            mode=DriverReadMode.FULL,
            diagnostics=_observe_smx_ii_write_poll(runtime_state, inverter, values),
        )

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
        *,
        runtime_state: dict[str, Any] | None = None,
    ) -> Any:
        if not _is_exact_smx_ii(inverter):
            raise ValueError(f"unsupported_capability:{self.key}:{capability_key}")

        # Resolve the allowlist from the reviewed profile, never solely from
        # caller-provided or persisted capability objects.
        profile = load_driver_profile(_SMX_PROFILE)
        capability = find_capability(capability_key, tuple(profile.capabilities))
        encoded_words = encode_capability_words(capability, value)
        if len(encoded_words) != 1:
            raise ValueError(f"smx_ii_multiword_write_not_supported:{capability.key}")
        session = self._session(transport, inverter.probe_target)
        write_word = int(encoded_words[0])

        if capability.bitmask:
            try:
                current = await session.read_holding(capability.register, 1)
            except Exception as exc:
                raise CapabilityPreWriteReadError(
                    f"bitmask_pre_write_read_failed:{capability.key}:{exc}"
                ) from exc
            if len(current) != 1:
                raise CapabilityPreWriteReadError(
                    f"bitmask_pre_write_read_empty:{capability.key}"
                )
            write_word = merge_capability_register_word(
                capability,
                current_word=int(current[0]),
                encoded_word=write_word,
            )

        if capability.write_function == 6:
            try:
                await session.write_single_holding(capability.register, write_word)
            except ModbusError as exc:
                if not _can_retry_with_default_password(capability, exc):
                    raise
                # Exact-model legacy behavior: only an explicit 0x0B denial
                # permits one user-password authorization attempt. Never touch
                # 0xE202 (the password CHANGE register).
                status = await session.read_holding(0x0211, 1)
                if status != [0]:
                    raise
                await session.write_holding(0xE203, [0])
                _LOGGER.info("SMX-II default-user authorization retried for 0x%04X", capability.register)
                await session.write_single_holding(capability.register, write_word)
        elif capability.write_function == 16:
            await session.write_holding(capability.register, [write_word])
        else:
            raise ValueError(f"unsupported_smx_ii_write_function:{capability.key}")

        native_value = decode_capability_value(capability, list(encoded_words))
        trace = start_write_confirmation(
            runtime_state,
            capability_key=capability.key,
            value_key=capability.value_key,
            requested_value=value,
            expected_value=native_value,
            requested_words=tuple(encoded_words),
        )
        try:
            readback = await session.read_holding(capability.register, 1)
            if len(readback) != 1:
                raise ValueError("empty_register_readback")
        except Exception as exc:
            store_write_confirmation(runtime_state, trace.with_immediate_unavailable(error=exc))
            raise CapabilityPostWriteReadError(
                f"smx_ii_post_write_read_failed:{capability.key}:{exc}"
            ) from exc

        observed = (
            (int(readback[0]) & capability.bitmask) >> capability.bitmask_shift
            if capability.bitmask
            else int(readback[0])
        )
        matched = observed == encoded_words[0]
        store_write_confirmation(
            runtime_state,
            trace.with_immediate_observation(
                value=decode_capability_value(capability, [observed]),
                words=(observed,),
                matched=matched,
            ),
        )
        if not matched:
            raise RuntimeError(
                f"smx_ii_write_readback_mismatch:{capability.key}:"
                f"requested={encoded_words[0]}:observed={observed}"
            )
        _schedule_smx_ii_write_readback(runtime_state, inverter, capability)
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
                "Generic SRNE is read-only; exact SMX-II writes require a model-qualified profile and immediate readback."
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


async def _async_read_smx_ii_values(
    session: ModbusSession,
    schema,
    inverter: DetectedInverter,
    runtime_state: dict[str, Any],
    *,
    poll_interval: float | None,
) -> DriverReadResult:
    """Keep live SMX-II readings fresh while rotating static register groups.

    The hub owns ``runtime_state`` per inverter binding and folds DELTA reads
    into its own last-good cache. We retain only a small rotation cursor here,
    never duplicate telemetry or persist readings across sessions. Explicit
    reads without runtime state still use the original full-sweep behavior.
    """

    identity = (schema.source_path, inverter.probe_target)
    state = runtime_state.get(_SMX_POLL_STATE_KEY)
    if type(state) is not dict or state.get("identity") != identity:
        # Populate all settings and counters once. Failed blocks remain absent
        # from the first FULL snapshot instead of becoming synthetic zeros.
        values = await read_spec_set_values(session, schema, ascii_style="printable")
        runtime_state[_SMX_POLL_STATE_KEY] = {"identity": identity, "index": 0}
        return DriverReadResult(
            values=values,
            mode=DriverReadMode.FULL,
            diagnostics=_observe_smx_ii_write_poll(runtime_state, inverter, values),
        )

    fast_blocks = tuple(
        block for block in schema.blocks if block.key in _SMX_FAST_BLOCK_KEYS
    )
    slow_blocks = tuple(
        block for block in schema.blocks if block.key not in _SMX_FAST_BLOCK_KEYS
    )
    if not slow_blocks:
        values = await read_spec_set_values(session, schema, ascii_style="printable")
        return DriverReadResult(
            values=values,
            mode=DriverReadMode.FULL,
            diagnostics=_observe_smx_ii_write_poll(runtime_state, inverter, values),
        )

    # One slow block per short poll; at longer configured intervals take enough
    # blocks to aim for a complete slow sweep in approximately ten minutes.
    # This is a cadence target, not a timer or an additional background task.
    interval = (
        float(poll_interval)
        if poll_interval is not None and math.isfinite(poll_interval) and poll_interval > 0
        else 5.0
    )
    count = min(
        len(slow_blocks),
        max(1, math.ceil(len(slow_blocks) * interval / _SMX_SLOW_SWEEP_TARGET_SECONDS)),
    )
    raw_index = state.get("index", 0)
    index = raw_index if type(raw_index) is int and raw_index >= 0 else 0
    selected_slow = tuple(
        slow_blocks[(index + offset) % len(slow_blocks)]
        for offset in range(count)
    )
    pending = state.get(_SMX_WRITE_READBACK_KEY)
    if type(pending) is dict:
        pending_block = next(
            (block for block in slow_blocks if block.key == pending.get("block_key")),
            None,
        )
        if pending_block is not None and pending_block not in selected_slow:
            # The post-write hub refresh must observe the *written* setting,
            # not a previous value retained by the DELTA snapshot cache.
            # Add one targeted slow block, keeping normal polling unchanged.
            selected_slow = (*selected_slow, pending_block)
    selected_blocks = (*fast_blocks, *selected_slow)
    values = await read_spec_set_values(
        session,
        replace(schema, blocks=selected_blocks),
        ascii_style="printable",
    )

    # DELTA retains omitted slow blocks, but failed *selected* reads must not
    # leave old live readings or settings masquerading as fresh observations.
    selected_specs = (
        spec
        for spec in schema.spec_set("runtime")
        if any(
            spec.function == block.function
            and block.start <= spec.register
            and spec.register + spec.word_count <= block.start + block.count
            for block in selected_blocks
        )
    )
    removed = frozenset(spec.key for spec in selected_specs if spec.key not in values)
    if type(pending) is dict:
        if pending.get("value_key") in values:
            state.pop(_SMX_WRITE_READBACK_KEY, None)
        else:
            remaining = pending.get("remaining", 0)
            if type(remaining) is not int or remaining <= 1:
                state.pop(_SMX_WRITE_READBACK_KEY, None)
            else:
                pending["remaining"] = remaining - 1
    state["index"] = (index + count) % len(slow_blocks)
    return DriverReadResult(
        values=values,
        mode=DriverReadMode.DELTA,
        removed_keys=removed,
        diagnostics=_observe_smx_ii_write_poll(runtime_state, inverter, values),
    )


def _is_exact_smx_ii(inverter: DetectedInverter) -> bool:
    """Recheck the catalog identity at the write boundary, not just the UI."""

    proof = inverter.details.get("catalog_detection")
    return (
        inverter.driver_key == SrneModbusDriver.key
        and inverter.variant_key == "smx_ii_24v"
        and inverter.profile_name == _SMX_PROFILE
        and inverter.register_schema_name == "srne_modbus/smx_ii_24v.json"
        and inverter.details.get("product_info") == _SMX_PRODUCT_INFO
        and type(proof) is dict
        and proof.get("resolution") == "exact"
        and proof.get("surface_key") == _SMX_SURFACE_KEY
    )


def _can_retry_with_default_password(capability, error: ModbusError) -> bool:
    """Preserve the old exact-device one-time login retry after Modbus 0x0B."""

    return (
        str(error) == "exception_code:11"
        and capability.value_kind != "action"
        and 0xE000 <= capability.register <= 0xE2FF
        and capability.word_count == 1
        and capability.write_function == 6
    )


def _schedule_smx_ii_write_readback(
    runtime_state: dict[str, Any] | None,
    inverter: DetectedInverter,
    capability,
) -> None:
    """Include the written setting's slow block in the next hub refresh.

    The exact-register read already proved the immediate value.  A separate
    normal poll must still confirm persistence, but a rotating DELTA poll can
    otherwise serve an old cached value for several minutes.  Never widen
    unrelated polls or issue another write.  A failed selected read gets at
    most one more targeted attempt.
    """

    if runtime_state is None:
        return
    state = runtime_state.get(_SMX_POLL_STATE_KEY)
    if type(state) is not dict:
        return  # The next first poll is already a complete FULL read.
    schema = load_register_schema(inverter.register_schema_name)
    spec = next(
        (item for item in schema.spec_set("runtime") if item.key == capability.value_key),
        None,
    )
    if spec is None:
        return  # No normal-poll readback exists for this capability.
    block = next(
        (
            block for block in schema.blocks
            if block.function == spec.function
            and block.start <= spec.register
            and spec.register + spec.word_count <= block.start + block.count
        ),
        None,
    )
    if block is None or block.key in _SMX_FAST_BLOCK_KEYS:
        return
    state[_SMX_WRITE_READBACK_KEY] = {
        "block_key": block.key,
        "value_key": capability.value_key,
        "remaining": 2,
    }


def _observe_smx_ii_write_poll(
    runtime_state: dict[str, Any] | None,
    inverter: DetectedInverter,
    values: dict[str, Any],
) -> dict[str, Any]:
    """Record actual slow-block readback without forcing extra Modbus polls."""

    trace = load_write_confirmation(runtime_state)
    if trace is None:
        return {}
    if trace.value_key not in values:
        # A slow setting was not selected in this DELTA poll. Missing is
        # different from an observed mismatch or failed read.
        return write_confirmation_diagnostics(runtime_state)
    capability = next(
        (item for item in inverter.capabilities if item.key == trace.capability_key),
        None,
    )
    matched = None
    if capability is not None:
        try:
            observed_value = values[trace.value_key]
            if capability.value_kind == "bool" and type(observed_value) is str:
                # Register-schema diagnostics intentionally show On/Off, while
                # capability writes use native bools.
                if observed_value in {"On", "Off"}:
                    observed_value = observed_value == "On"
            matched = tuple(encode_capability_words(capability, observed_value)) == trace.requested_words
        except (TypeError, ValueError):
            matched = False
    store_write_confirmation(
        runtime_state,
        trace.with_poll_observation(
            value=values[trace.value_key],
            matched=matched,
        ),
    )
    return write_confirmation_diagnostics(runtime_state)


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
