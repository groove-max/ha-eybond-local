"""MUST PV/PH18 Modbus RTU read-only driver."""

from __future__ import annotations

from typing import Any

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.device_catalog_loader import resolve_support_capture_policy
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusError, ModbusSession
from ..payload.register_decode import decode_ascii_word, read_spec_set_values
from .base import InverterDriver
from .capability_codec import (
    decode_capability_value,
    encode_capability_words,
    find_capability,
)


_MODEL_PREFIXES = ("PV", "PH", "EP")


class MustPvPh18Driver(InverterDriver):
    """Driver for MUST PV/PH18 Modbus devices."""

    key = "must_pv_ph18"
    name = "MUST PV/PH18"
    profile_name = "must_pv_ph18/base.json"

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
        return _must_default_schema_name()

    @property
    def measurements(self):
        schema = self.register_schema_metadata
        return schema.measurement_descriptions if schema is not None else ()

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        schema_name = self.register_schema_name
        schema = load_register_schema(schema_name)
        session = self._session(transport, target)
        model_name = await _async_probe_model_name(session, schema)
        if not model_name.startswith(_MODEL_PREFIXES):
            return None

        surface = load_compiled_detection_catalog().surfaces["must_pv_ph18_full"]
        details = {
            "model_number": model_name,
            "protocol_id": "MUST_PV_PH18",
            "catalog_detection": {
                "resolution": "exact",
                "surface_key": surface.key,
                "evidence": {
                    "identity.model_number": model_name,
                    "protocol.protocol_id": "MUST_PV_PH18",
                },
            },
        }
        # Entity setup reads capabilities from the DetectedInverter; carry
        # the profile's untested controls with the detection result.
        profile = self.profile_metadata if surface.profile_name else None
        return DetectedInverter(
            driver_key=self.key,
            protocol_family="must_pv_ph18",
            model_name=f"MUST {model_name}",
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
    ) -> dict[str, Any]:
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        session = self._session(transport, inverter.probe_target)
        values = await read_spec_set_values(session, schema, ascii_style="model")

        if "pv_generation_sum_high" in values and "pv_generation_sum_low" in values:
            values["pv_generation_sum"] = (
                int(values["pv_generation_sum_high"]) * 1000
                + int(values["pv_generation_sum_low"])
            )
        if "model_prefix" in values and "model_suffix" in values:
            values["model_number"] = f"{values['model_prefix']}{values['model_suffix']}"
        return values

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        capability = find_capability(
            capability_key, inverter.capabilities or self.write_capabilities
        )
        raw_words = encode_capability_words(capability, value)
        session = self._session(transport, inverter.probe_target)
        if capability.write_function == 6:
            # The PH protocol document does not state the write function
            # code; the profile pins single-register writes.
            for offset, word in enumerate(raw_words):
                await session.write_single_holding(
                    capability.register + offset, int(word)
                )
        else:
            await session.write_holding(capability.register, [int(w) for w in raw_words])
        native_value = decode_capability_value(capability, raw_words)
        inverter.details[capability.key] = native_value
        return native_value

    async def async_capture_support_evidence(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        session = self._session(transport, inverter.probe_target)
        ranges = _support_capture_ranges(
            inverter.register_schema_name or self.register_schema_name
        )
        captured_ranges: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for start, count in ranges:
            try:
                values = await session.read_holding(start, count)
            except Exception as exc:
                failures.append(
                    {
                        "start": start,
                        "count": count,
                        "error": str(exc),
                    }
                )
                continue
            captured_ranges.append(
                {
                    "start": start,
                    "count": count,
                    "words": list(values),
                }
            )
        return {
            "capture_kind": "must_pv_ph18_modbus_register_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "capture_notes": list(_support_capture_notes()),
            "planned_ranges": [
                {"start": start, "count": count}
                for start, count in ranges
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


def _must_default_schema_name() -> str:
    for surface in load_compiled_detection_catalog().surfaces.values():
        if surface.driver_key == MustPvPh18Driver.key and surface.default_for_driver:
            return surface.register_schema_name
    return "must_pv_ph18/base.json"


def _support_capture_ranges(schema_name: str) -> tuple[tuple[int, int], ...]:
    schema = load_register_schema(schema_name)
    planned = [(block.start, block.count) for block in schema.blocks]
    planned.extend(_support_capture_policy().ranges)
    return _merge_capture_ranges(planned)


def _support_capture_notes() -> tuple[str, ...]:
    policy_notes = _support_capture_policy().notes
    if policy_notes:
        return policy_notes
    return ("MUST PV/PH18 uses Modbus RTU at 19200 8N1 and slave address 4.",)


def _support_capture_policy():
    return resolve_support_capture_policy(
        driver_key=MustPvPh18Driver.key,
        variant_key="pv_ph18",
        profile_name="",
        register_schema_name=_must_default_schema_name(),
    )


def _merge_capture_ranges(ranges: list[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    normalized = sorted(
        (int(start), int(count))
        for start, count in ranges
        if int(count) > 0
    )
    merged: list[tuple[int, int]] = []
    for start, count in normalized:
        end = start + count
        if not merged:
            merged.append((start, count))
            continue
        last_start, last_count = merged[-1]
        last_end = last_start + last_count
        if start > last_end:
            merged.append((start, count))
            continue
        merged[-1] = (last_start, max(last_end, end) - last_start)
    return tuple(merged)


async def _async_probe_model_name(session: ModbusSession, schema) -> str:
    try:
        model_block = schema.block("serial")
        model_words = await session.read_holding(model_block.start, model_block.count)
    except Exception:
        model_words = []

    model_name = _decode_model_name(model_words)
    if model_name.startswith(_MODEL_PREFIXES):
        return model_name

    try:
        model_number_words = await session.read_holding(20001, 1)
    except Exception:
        return model_name
    return _decode_numeric_pv_model_name(model_number_words) or model_name


def _decode_model_name(words: list[int]) -> str:
    if len(words) < 2:
        return ""
    prefix = _decode_ascii_word(words[0])
    suffix = str(int(words[1])) if int(words[1]) > 0 else ""
    return f"{prefix}{suffix}".strip()


def _decode_numeric_pv_model_name(words: list[int]) -> str:
    if not words:
        return ""
    value = int(words[0])
    if not (1000 <= value <= 12000):
        return ""
    return f"PV{value}"


def _decode_ascii_word(value: int) -> str:
    return decode_ascii_word(value, style="model")
