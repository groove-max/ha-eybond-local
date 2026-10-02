"""MUST PV/PH18 telemetry and model-qualified local controls."""

from __future__ import annotations

from ..poll_policy import PollPolicy


from typing import Any
import time

from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.device_catalog_loader import resolve_support_capture_policy
from ..metadata.detection_decision_tree import evaluate_detection_decision_tree_static
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..models import DetectedInverter, ProbeTarget
from ..payload.modbus import ModbusError, ModbusSession
from ..payload.register_decode import decode_ascii_word, read_spec_set_values
from .base import InverterDriver
from .support_diagnostics import capture_support_reads
from .local_register_evidence import (
    LocalRegisterReadPlan,
    LocalRegisterSnapshot,
    async_capture_modbus_snapshot,
)
from .read_result import DriverReadMode, DriverReadResult
from .modbus_write_error import ModbusWriteErrorMixin
from .must_bms import async_read_bms
from .capability_codec import (
    decode_capability_value,
    encode_capability_words,
    find_capability,
)


_MODEL_PREFIXES = ("PV", "PH", "EP")
_BMS_DIAGNOSTIC_TIMEOUT_SECONDS = 3.0


class MustPvPh18Driver(ModbusWriteErrorMixin, InverterDriver):
    """Driver for MUST PV/PH18 Modbus devices."""

    key = "must_pv_ph18"
    poll_policy = PollPolicy(min_auto_interval=5.0, max_auto_interval=90.0)
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

        catalog = load_compiled_detection_catalog()
        evidence = {
            "identity.model_number": model_name,
            "protocol.protocol_id": "MUST_PV_PH18",
        }
        evaluation = evaluate_detection_decision_tree_static(catalog.decision_trees[self.key], evidence)
        resolution = catalog.resolution_for_candidates(
            protocol_key=self.key,
            candidate_keys=evaluation.candidate_keys if evaluation.status == "resolved" else (),
            evidence=evidence,
        )
        # Unrecognized PV/PH/EP models keep the common map. A documented model
        # override must be selected from identity, never guessed from live watts.
        surface = catalog.surfaces[resolution.surface_key or "must_pv_ph18_full"]
        details = {
            "model_number": model_name,
            "protocol_id": "MUST_PV_PH18",
            "catalog_detection": {
                "resolution": resolution.resolution if resolution.surface_key else "family",
                "surface_key": surface.key,
                "evidence": evidence,
                "candidate_keys": list(resolution.candidate_keys),
                "catalog_version": resolution.catalog_version,
                "descriptor_revisions": list(resolution.descriptor_revisions),
                "evidence_fingerprint": resolution.evidence_fingerprint,
            },
        }
        # Entity setup reads capabilities from the DetectedInverter; carry
        # the identity-selected profile, not the common MUST profile. Only
        # PV3300 has the four owner-confirmed controls from issue #46.
        profile = load_driver_profile(surface.profile_name) if surface.profile_name else None
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
    ) -> DriverReadResult:
        started = time.monotonic()
        now = started if now_monotonic is None else float(now_monotonic)
        schema = load_register_schema(
            inverter.register_schema_name or self.register_schema_name
        )
        session = self._session(transport, inverter.probe_target)
        values = await read_spec_set_values(session, schema, ascii_style="model")

        if "pv_generation_sum_high" in values and "pv_generation_sum_low" in values:
            # PH/PV Modbus 1.4.15: 15217 is in 1000 kWh, 15218 in 0.1 kWh.
            # Use a new entity key: the old Wh counter had incorrect scaling;
            # continuing its statistics would invent an energy-consumption jump.
            values["pv_energy_total"] = (
                int(values["pv_generation_sum_high"]) * 10000
                + int(values["pv_generation_sum_low"])
            ) / 10
        if "model_prefix" in values and "model_suffix" in values:
            values["model_number"] = f"{values['model_prefix']}{values['model_suffix']}"
        diagnostics = {}
        if schema.source_name == "must_pv_ph18/pv3300.json":
            bms_values, diagnostics = await async_read_bms(
                session, transport, inverter, schema,
                runtime_state if runtime_state is not None else {},
                lambda: now + max(0, time.monotonic() - started),
            )
            values.update(bms_values)
        return DriverReadResult(values=values, mode=DriverReadMode.FULL, diagnostics=diagnostics)

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
        *,
        runtime_state: dict[str, Any] | None = None,
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
        evidence = {
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
        if inverter.register_schema_name == "must_pv_ph18/pv3300.json":
            raw = {
                block["start"] + offset: word
                for block in captured_ranges
                for offset, word in enumerate(block["words"])
            }
            if all(raw.get(address) == 0 for address in (25210, 25211, 25212)):
                evidence["current_read_diagnostics"] = await capture_support_reads(
                    session,
                    ((25210, 1, "inverter_current"), (25211, 1, "grid_current"),
                     (25212, 1, "load_current")),
                    timeout_seconds=6.0,
                    source="PH/PV Modbus 1.4.3 and protocol 1916 current registers",
                    purpose="support_only_zero_bulk_current_single_read_comparison",
                )
            # The vendor's 6422/1916 map describes a separate optional BMS
            # window (voltage/current/temperature/reserved/SOC). Keep the raw
            # diagnostic read independently bounded, even during runtime backoff.
            current_status = evidence.get("current_read_diagnostics", {}).get("status", "completed")
            if current_status == "completed":
                evidence["bms_read_diagnostics"] = await capture_support_reads(
                    session,
                    ((109, 5, "bms_soc_candidate"),),
                    timeout_seconds=_BMS_DIAGNOSTIC_TIMEOUT_SECONDS,
                    source="Manufacturer protocol 6422 (1916), FC03 BMS registers 109-113",
                    purpose="support_only_optional_bms_soc_availability",
                )
            else:
                evidence["bms_read_diagnostics"] = {"status": "skipped_after_current_read_failure"}
        return evidence

    def local_register_read_plans(
        self, inverter: DetectedInverter
    ) -> tuple[LocalRegisterReadPlan, ...]:
        return tuple(
            LocalRegisterReadPlan.for_target(
                inverter.probe_target,
                function=3,
                start=start,
                count=count,
            )
            for start, count in _support_capture_ranges(
                inverter.register_schema_name or self.register_schema_name
            )
        )

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


def _must_default_schema_name() -> str:
    for surface in load_compiled_detection_catalog().surfaces.values():
        if surface.driver_key == MustPvPh18Driver.key and surface.default_for_driver:
            return surface.register_schema_name
    return "must_pv_ph18/base.json"


def _support_capture_ranges(schema_name: str) -> tuple[tuple[int, int], ...]:
    schema = load_register_schema(schema_name)
    # BMS evidence has its own deadline and failure record below; never merge
    # it into the mandatory/core capture or read it twice during one export.
    planned = [(block.start, block.count) for block in schema.blocks if block.key != "bms"]
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
