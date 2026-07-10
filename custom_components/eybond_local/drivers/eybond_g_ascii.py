"""EyeBond G-ASCII inverter/logger protocol driver."""

from __future__ import annotations

import asyncio
from functools import lru_cache
import json
from pathlib import Path
import time
from typing import Any

from ..eybond_g_ascii_settings import (
    G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD,
    GAsciiSettingDefinition,
)
from ..metadata.device_catalog_loader import resolve_catalog_surface_binding
from ..metadata.detection_decision_tree import evaluate_detection_decision_tree_static
from ..metadata.compiled_detection_catalog import (
    RESOLUTION_COMPATIBLE_GROUP,
    RESOLUTION_EXACT,
    RESOLUTION_FAMILY,
    load_compiled_detection_catalog,
)
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from ..models import (
    DetectedInverter,
    ProbeTarget,
    WriteCapability,
    decimals_for_divisor,
)
from ..payload.ascii_line import (
    AsciiLineError,
    AsciiLineSession,
    parse_ascii_line_response,
    parse_space_fields,
)
from .base import InverterDriver
from .command_support import (
    apply_unsupported_diagnostics,
    command_skipped_as_unsupported,
    commit_cycle_failures,
    record_command_failure,
    record_command_success,
)
from .catalog_probe import catalog_model_name


_EYBOND_G_ASCII_DRIVER_KEY = "eybond_g_ascii"
_EYBOND_G_ASCII_VARIANT_KEY = "g_ascii_family"
_EYBOND_G_ASCII_FALLBACK_PROFILE_NAME = "eybond_g_ascii/base.json"
_EYBOND_G_ASCII_FALLBACK_SCHEMA_NAME = "eybond_g_ascii/base.json"
_EYBOND_G_ASCII_EXACT_FINGERPRINT_KEYS: tuple[str, ...] = (
    "rating.output_voltage",
    "rating.output_current",
    "rating.battery_voltage",
    "rating.frequency",
    "firmware.software_version",
)
_EYBOND_G_ASCII_DETECTION_DETAIL_VALUE_KEYS: tuple[str, ...] = (
    "rated_output_voltage",
    "rated_output_current",
    "rated_battery_voltage",
    "rated_frequency",
    "eybond_g_ascii_software_version",
    "eybond_g_ascii_software_date",
    "gdat0_internal_code",
)

_EYBOND_G_ASCII_PROBE_TARGETS: tuple[ProbeTarget, ...] = (
    ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
)

_OPERATING_MODE_BY_CODE: dict[str, str] = {
    "P": "Power On",
    "S": "Standby",
    "L": "Line",
    "B": "Battery",
    "F": "Fault",
    "D": "Shutdown",
    "X": "Test",
}
_GPDAT_OPERATING_MODE_BY_CODE: dict[str, str] = {
    "0": "Power On",
    "1": "Shutdown",
    "2": "Fault",
    "3": "Standby",
    "4": "Line",
    "5": "Battery",
    "6": "Test",
}
_COMMAND_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "protocol_catalogs"
    / "command_schemas"
    / "eybond_g_ascii"
    / "base.json"
)
_G_ASCII_SETTINGS_BY_READ_KEY: dict[str, GAsciiSettingDefinition] = {
    definition.read_key: definition
    for definition in G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD.values()
    if definition.read_key
}


@lru_cache(maxsize=1)
def _eybond_g_ascii_catalog_binding() -> tuple[str, str, str]:
    binding = resolve_catalog_surface_binding(
        _EYBOND_G_ASCII_DRIVER_KEY,
        variant_key=_EYBOND_G_ASCII_VARIANT_KEY,
    )
    if binding is None:
        return (_EYBOND_G_ASCII_VARIANT_KEY, "", _EYBOND_G_ASCII_FALLBACK_SCHEMA_NAME)
    return (
        binding.variant_key,
        binding.profile_name,
        binding.register_schema_name or _EYBOND_G_ASCII_FALLBACK_SCHEMA_NAME,
    )


def _eybond_g_ascii_variant_key() -> str:
    return _eybond_g_ascii_catalog_binding()[0]


def _eybond_g_ascii_profile_name() -> str:
    return _eybond_g_ascii_catalog_binding()[1] or _EYBOND_G_ASCII_FALLBACK_PROFILE_NAME


def _eybond_g_ascii_register_schema_name() -> str:
    return _eybond_g_ascii_catalog_binding()[2]


@lru_cache(maxsize=1)
def _eybond_g_ascii_register_schema():
    return load_register_schema(_eybond_g_ascii_register_schema_name())


def _eybond_g_ascii_measurements():
    return _eybond_g_ascii_register_schema().measurement_descriptions


def _eybond_g_ascii_binary_sensors():
    return _eybond_g_ascii_register_schema().binary_sensor_descriptions


class EybondGAsciiDriver(InverterDriver):
    """Read-only driver for the EyeBond G-command ASCII protocol family."""

    key = _EYBOND_G_ASCII_DRIVER_KEY
    name = "EyeBond G-ASCII"
    probe_timeout = 12.0
    signature_timeout = 4.0
    probe_targets = _EYBOND_G_ASCII_PROBE_TARGETS
    measurements = _eybond_g_ascii_measurements()
    binary_sensors = _eybond_g_ascii_binary_sensors()
    capability_groups = ()
    write_capabilities = ()
    capability_presets = ()

    async def async_probe_signature(self, transport, target: ProbeTarget) -> bool:
        session = self._session(transport, target)
        for command in ("GPDAT0", "GPV"):
            try:
                payload = await session.request(command)
            except Exception:
                continue
            fields = parse_space_fields(payload)
            if len(fields) >= 10:
                return True
        return False

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        session = self._session(transport, target)
        try:
            values = await _async_collect_eybond_g_ascii_values(session, probe=True)
        except Exception:
            return None
        if not _looks_like_eybond_g_ascii(values):
            return None
        await _async_collect_eybond_g_ascii_offline_fingerprint_values(session, values)

        collector = getattr(transport, "collector_info", None)
        collector_pn = str(getattr(collector, "collector_pn", "") or "").strip()
        serial_number = collector_pn or str(values.get("eybond_g_ascii_serial_hint") or "").strip()
        if len(serial_number) < 6:
            return None

        evidence: dict[str, Any] = {
            "protocol.protocol_id": "EYBOND_G_ASCII",
            "collector.cloud_family": getattr(collector, "collector_cloud_family", ""),
        }
        gdat0_field_count = _space_field_count(
            values.get("eybond_g_ascii_gdat0_fields")
        )
        if gdat0_field_count:
            evidence["shape.gdat0_field_count"] = gdat0_field_count
        gpv_field_count = _space_field_count(values.get("eybond_g_ascii_gpv_fields"))
        if gpv_field_count:
            evidence["shape.gpv_field_count"] = gpv_field_count
        _extend_eybond_g_ascii_offline_fingerprint_evidence(values, evidence)

        catalog = load_compiled_detection_catalog()
        resolution = _resolve_eybond_g_ascii_catalog(catalog, evidence)
        surface = catalog.surfaces.get(resolution.surface_key or "")
        if (
            surface is None
            or resolution.resolution
            not in {RESOLUTION_EXACT, RESOLUTION_COMPATIBLE_GROUP, RESOLUTION_FAMILY}
        ):
            return None
        model_name = catalog_model_name(
            protocol_key=self.key,
            resolution=resolution,
            values=values,
        )
        profile_name = surface.profile_name or _EYBOND_G_ASCII_FALLBACK_PROFILE_NAME
        schema_name = surface.register_schema_name or _EYBOND_G_ASCII_FALLBACK_SCHEMA_NAME
        profile = load_driver_profile(profile_name)

        details = {
            key: values[key]
            for key in _EYBOND_G_ASCII_DETECTION_DETAIL_VALUE_KEYS
            if key in values
        } | {
            "protocol_id": "EYBOND_G_ASCII",
            "catalog_detection": {
                "protocol_key": resolution.protocol_key,
                "resolution": resolution.resolution,
                "candidate_keys": list(resolution.candidate_keys),
                "surface_key": resolution.surface_key,
                "confidence": resolution.confidence,
                "catalog_version": resolution.catalog_version,
                "descriptor_revisions": list(resolution.descriptor_revisions),
                "evidence_fingerprint": resolution.evidence_fingerprint,
                "collected_evidence_keys": list(resolution.collected_evidence_keys),
                "missing_evidence_keys": list(resolution.missing_evidence_keys),
                "failed_evidence_keys": list(resolution.failed_evidence_keys),
                "unsupported_evidence_keys": list(resolution.unsupported_evidence_keys),
                "contradicting_evidence_keys": list(resolution.contradicting_evidence_keys),
                "decision_path": list(resolution.decision_path),
                "evidence": evidence,
            },
        }

        return DetectedInverter(
            driver_key=self.key,
            protocol_family="eybond_g_ascii",
            model_name=model_name,
            variant_key=surface.variant_key,
            serial_number=serial_number,
            probe_target=target,
            details=details,
            profile_name=profile_name,
            register_schema_name=schema_name,
            capability_groups=profile.groups,
            capabilities=profile.capabilities,
            capability_presets=(),
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
        return await _async_collect_eybond_g_ascii_values(
            self._session(transport, inverter.probe_target),
            probe=False,
            capabilities=inverter.capabilities,
            runtime_state=runtime_state,
            poll_interval=poll_interval,
            now_monotonic=now_monotonic,
        )

    async def async_capture_support_evidence(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        """Capture an extended read-only G-ASCII command dump for support packages."""

        return await _async_capture_eybond_g_ascii_support_evidence(
            self._session(transport, inverter.probe_target),
            driver_key=self.key,
            model_name=inverter.model_name,
            serial_number=inverter.serial_number,
        )

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        capability = _find_capability(capability_key, inverter.capabilities or self.write_capabilities)
        command_prefix = str(capability.command or "").strip().upper()
        command_map = capability.command_map or {}
        if not command_prefix and not command_map:
            raise KeyError(capability_key)

        raw_value = _encode_capability_value(capability, value)
        command = command_map.get(raw_value)
        if not command:
            command = f"{command_prefix}{_format_command_value(capability, raw_value)}"
        response = await self._session(transport, inverter.probe_target).request(command)
        if response != "ACK":
            raise RuntimeError(f"unexpected_write_response:{capability.key}:{response}")

        written_value = _decode_capability_value(capability, raw_value)
        inverter.details[capability.value_key] = written_value
        if capability.key != capability.value_key:
            inverter.details[capability.key] = written_value
        return written_value

    def _session(self, transport, target: ProbeTarget) -> AsciiLineSession:
        return AsciiLineSession(
            transport,
            route=target.link_route,
            payload_family="eybond_g_ascii",
        )


async def _async_capture_eybond_g_ascii_support_evidence(
    session: AsciiLineSession,
    *,
    driver_key: str,
    model_name: str,
    serial_number: str,
) -> dict[str, Any]:
    responses: dict[str, str] = {}
    failures: dict[str, str] = {}
    command_results: list[dict[str, Any]] = []
    command_schema_key, command_specs = await asyncio.to_thread(_support_probe_plan)

    for spec in command_specs:
        command = str(spec.get("command") or "")
        source = str(spec.get("source") or "")
        description = str(spec.get("description") or "")
        known_fields = list(spec.get("fields") or [])
        started = time.monotonic()
        timing: dict[str, int] = {}
        try:
            raw_response = await session.request_raw(command)
        except Exception as exc:
            duration_ms = int(round((time.monotonic() - started) * 1000.0))
            failures[command] = str(exc)
            command_results.append(
                {
                    "command": command,
                    "source": source,
                    "description": description,
                    "status": "error",
                    "duration_ms": duration_ms,
                    "error": str(exc),
                }
            )
            continue
        duration_ms = int(round((time.monotonic() - started) * 1000.0))
        timing = session.last_transport_timing()

        raw_ascii = raw_response.decode("ascii", errors="replace")
        parsed_payload = ""
        parse_error = ""
        try:
            parsed_payload = parse_ascii_line_response(raw_response)
        except AsciiLineError as exc:
            parse_error = str(exc)

        fields = parse_space_fields(parsed_payload) if parsed_payload else []
        known_field_indexes = _known_field_indexes(known_fields)
        responses[command] = raw_ascii
        result: dict[str, Any] = {
            "command": command,
            "source": source,
            "description": description,
            "status": _support_response_status(parsed_payload),
            "duration_ms": duration_ms,
            "raw_response_ascii": raw_ascii,
            "raw_response_hex": raw_response.hex(),
            "parsed_payload": parsed_payload,
            "field_count": len(fields),
            "response_kind": _support_response_kind(parsed_payload),
            "known_fields": known_fields,
            "known_field_count": len(known_fields),
            "unknown_field_count": max(0, len(fields) - len(known_field_indexes & set(range(len(fields))))),
        }
        if timing:
            result["transport_timing"] = timing
        if parse_error:
            result["parse_error"] = parse_error
        command_results.append(result)

    return {
        "capture_kind": "eybond_g_ascii_protocol_probe",
        "driver_key": driver_key,
        "model_name": model_name,
        "serial_number": serial_number,
        "protocol_id": "EYBOND_G_ASCII",
        "capture_notes": [
            "Extended support probe sends read-only/documented G-ASCII query commands only.",
            "The command plan is loaded from protocol_catalogs/command_schemas/eybond_g_ascii/base.json.",
            "Unsupported commands may return NAK/NOA/ERCRC or timeout; these are preserved as evidence.",
            "Indexed sweeps are bounded to GPDAT0..GPDAT9 and GPID0..GPID9 to avoid long support-package captures.",
        ],
        "planned_commands": [
            {
                "command": spec.get("command"),
                "source": spec.get("source"),
                "description": spec.get("description"),
                "known_field_count": len(spec.get("fields") or []),
            }
            for spec in command_specs
        ],
        "responses": responses,
        "failures": failures,
        "protocol_probe": {
            "schema_version": 1,
            "command_schema_key": command_schema_key,
            "protocol_id": "EYBOND_G_ASCII",
            "command_count": len(command_results),
            "response_count": len(responses),
            "failure_count": len(failures),
            "negative_response_count": sum(
                1 for item in command_results if item.get("status") == "negative_response"
            ),
            "commands": command_results,
        },
    }


@lru_cache(maxsize=1)
def _load_command_schema() -> dict[str, Any]:
    return json.loads(_COMMAND_SCHEMA_PATH.read_text(encoding="utf-8"))


def _support_probe_plan() -> tuple[str, tuple[dict[str, Any], ...]]:
    schema = _load_command_schema()
    return str(schema.get("schema_key") or ""), _support_command_specs_from_schema(schema)


def _find_capability(
    capability_key: str,
    capabilities: tuple[WriteCapability, ...],
) -> WriteCapability:
    for capability in capabilities:
        if capability.key == capability_key:
            return capability
    raise KeyError(capability_key)


def _encode_capability_value(capability: WriteCapability, value: Any) -> int:
    kind = capability.value_kind
    if kind == "bool":
        return _encode_bool_value(value)
    if kind == "enum":
        return _encode_enum_value(capability, value)
    if kind == "action":
        return int(capability.action_value if capability.action_value is not None else 1)
    if kind == "scaled_u16":
        divisor = int(capability.divisor or 1)
        return int(round(float(value) * divisor))
    if kind in {"u16", "u32"}:
        return int(value)
    raise ValueError(f"unsupported_g_ascii_capability_kind:{capability.key}:{kind}")


def _decode_capability_value(capability: WriteCapability, raw_value: int) -> Any:
    if capability.value_kind == "bool":
        return bool(raw_value)
    if capability.value_kind == "enum":
        return capability.enum_value_map.get(raw_value, str(raw_value))
    if capability.value_kind == "scaled_u16" and capability.divisor:
        return round(raw_value / capability.divisor, decimals_for_divisor(capability.divisor))
    return raw_value


def _encode_bool_value(value: Any) -> int:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"on", "true", "yes", "enable", "enabled", "1"}:
            return 1
        if normalized in {"off", "false", "no", "disable", "disabled", "0"}:
            return 0
    return 1 if bool(value) else 0


def _encode_enum_value(capability: WriteCapability, value: Any) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        normalized = str(value or "").strip()
        reverse = {label: raw for raw, label in capability.enum_value_map.items()}
        if normalized in reverse:
            return int(reverse[normalized])
        raise ValueError(f"unsupported_enum_option:{capability.key}:{value}") from None
    if capability.enum_value_map and candidate not in capability.enum_value_map:
        raise ValueError(f"unsupported_enum_option:{capability.key}:{value}")
    return candidate


def _format_command_value(capability: WriteCapability, raw_value: int) -> str:
    if capability.value_kind in {"u16", "u32"}:
        text = str(int(raw_value))
        if capability.command_width:
            return text.zfill(capability.command_width)
        return text
    if capability.value_kind == "scaled_u16" and capability.divisor:
        precision = (
            capability.command_precision
            if capability.command_precision is not None
            else decimals_for_divisor(capability.divisor)
        )
        return f"{raw_value / capability.divisor:.{precision}f}"
    return str(int(raw_value))


def _support_command_specs_from_schema(schema: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    deduped: dict[str, dict[str, Any]] = {}
    for raw_item in schema.get("commands") or []:
        if not isinstance(raw_item, dict):
            continue
        if raw_item.get("support_probe_enabled") is False:
            continue
        if str(raw_item.get("access") or "").strip().lower() not in {"read", "query"}:
            continue
        command = str(raw_item.get("command") or "").strip()
        if not command:
            continue
        deduped.setdefault(
            command,
            {
                "command": command,
                "source": str(raw_item.get("source") or ""),
                "description": str(raw_item.get("description") or ""),
                "fields": list(raw_item.get("fields") or []),
            },
        )
    return tuple(deduped.values())


def _known_field_indexes(fields: list[Any]) -> set[int]:
    indexes: set[int] = set()
    for field in fields:
        if not isinstance(field, dict):
            continue
        raw_index = field.get("index")
        if isinstance(raw_index, int):
            indexes.add(raw_index)
            continue
        if isinstance(raw_index, list):
            for item in raw_index:
                if isinstance(item, int):
                    indexes.add(item)
    return indexes


def _support_response_status(parsed_payload: str) -> str:
    normalized = str(parsed_payload or "").strip().upper()
    if normalized in {"NAK", "NOA", "ERCRC"}:
        return "negative_response"
    return "ok"


def _support_response_kind(parsed_payload: str) -> str:
    text = str(parsed_payload or "").strip()
    upper = text.upper()
    if not text:
        return "empty"
    if upper in {"ACK", "NAK", "NOA", "ERCRC"}:
        return upper
    if text.startswith("#"):
        return "rated_info"
    if upper.startswith("BL"):
        return "battery_level"
    return "fields" if parse_space_fields(text) else "text"


async def _async_collect_eybond_g_ascii_values(
    session: AsciiLineSession,
    *,
    probe: bool,
    capabilities: tuple[WriteCapability, ...] = (),
    runtime_state: dict[str, Any] | None = None,
    poll_interval: float | None = None,
    now_monotonic: float | None = None,
) -> dict[str, Any]:
    del poll_interval
    values: dict[str, Any] = {}
    state = runtime_state if runtime_state is not None else {}
    now = time.monotonic() if now_monotonic is None else float(now_monotonic)

    await _async_collect_eybond_g_ascii_core_values(session, values)

    if probe:
        return values

    # The core commands answered (no exception): this cycle's optional-command
    # failures are command-specific evidence, not a link-wide outage.
    record_command_success(state, "__g_ascii_core__")

    await _async_collect_eybond_g_ascii_offline_fingerprint_values(session, values)
    await _async_collect_eybond_g_ascii_secondary_values(
        session,
        values,
        runtime_state=state,
    )
    await _async_collect_g_ascii_capability_readbacks(
        session,
        values,
        capabilities,
        runtime_state=state,
        now_monotonic=now,
        deferred=False,
    )
    await _async_collect_eybond_g_ascii_bms_values(
        session,
        values,
        runtime_state=state,
        now_monotonic=now,
    )
    await _async_collect_g_ascii_capability_readbacks(
        session,
        values,
        capabilities,
        runtime_state=state,
        now_monotonic=now,
        deferred=True,
    )
    values["eybond_g_ascii_runtime_polled_groups"] = (
        "core, fingerprint, secondary, readbacks"
        if capabilities
        else "core, fingerprint, secondary"
    )

    commit_cycle_failures(state)
    apply_unsupported_diagnostics(values, state)

    return values


async def _async_collect_eybond_g_ascii_secondary_values(
    session: AsciiLineSession,
    values: dict[str, Any],
    *,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    gtmp = await _optional_request(session, values, "GTMP", runtime_state=runtime_state)
    if gtmp:
        fields = parse_space_fields(gtmp)
        _set_float(values, "pv_side_temperature", fields, 0)
        _set_float(values, "charger_temperature", fields, 1)
        _set_float(values, "ambient_temperature", fields, 2)
        _set_float(values, "low_voltage_mppt_temperature_1", fields, 3)
        _set_float(values, "low_voltage_mppt_temperature_2", fields, 4)

    gline = await _optional_request(session, values, "GLINE", runtime_state=runtime_state)
    if gline:
        fields = parse_space_fields(gline)
        values["eybond_g_ascii_gline_fields"] = " ".join(fields)
        _set_float(values, "grid_voltage", fields, 0)
        _set_float(values, "grid_frequency", fields, 1)
        _set_float(values, "mains_input_voltage", fields, 0)
        _set_float(values, "mains_frequency", fields, 1)
        _set_float(values, "grid_loss_high_voltage", fields, 2)
        _set_float(values, "grid_loss_low_voltage", fields, 3)
        _set_float(values, "grid_restore_high_voltage", fields, 4)
        _set_float(values, "grid_restore_low_voltage", fields, 5)
        _set_float(values, "grid_loss_high_frequency", fields, 6)
        _set_float(values, "grid_loss_low_frequency", fields, 7)
        _set_float(values, "output_load_percentage", fields, 9)
        _set_scaled_float(values, "grid_energy_today", fields, 10, divisor=100.0)
        _set_combined_scaled_counter(values, "grid_energy_total", fields, 11, 12, divisor=100.0)

    gbat = await _optional_request(session, values, "GBAT", runtime_state=runtime_state)
    if gbat:
        fields = parse_space_fields(gbat)
        values["eybond_g_ascii_gbat_fields"] = " ".join(fields)
        _set_float(values, "battery_voltage", fields, 0)
        _set_float_preserve_existing_nonzero(values, "battery_current", fields, 1)
        _set_float(values, "battery_cell_count", fields, 2)
        _set_float(values, "battery_discharge_cutoff_voltage", fields, 3)
        _set_float(values, "battery_discharge_alarm_voltage", fields, 4)

    gbus = await _optional_request(session, values, "GBUS", runtime_state=runtime_state)
    if gbus:
        fields = parse_space_fields(gbus)
        _set_float(values, "bus_voltage", fields, 0)
        _set_float(values, "bus_reference_start_voltage", fields, 1)
        _set_float(values, "bus_reference_voltage", fields, 2)

    gchg = await _optional_request(session, values, "GCHG", runtime_state=runtime_state)
    if gchg:
        fields = parse_space_fields(gchg)
        values["eybond_g_ascii_gchg_fields"] = " ".join(fields)
        _set_float(values, "bus_voltage", fields, 0)
        _set_float(values, "charging_voltage", fields, 1)
        _set_float(values, "battery_cell_count", fields, 2)
        _set_float(values, "charging_current", fields, 3)
        _set_float(values, "constant_voltage_charging_voltage", fields, 6)
        _set_float(values, "float_charging_voltage", fields, 7)
        _set_float(values, "equalization_charging_voltage", fields, 8)
        _set_float(values, "max_charging_current", fields, 9)
        _set_float(values, "constant_voltage_charging_time", fields, 10)
        _set_float(values, "equalization_charging_time", fields, 11)
        _set_float(values, "equalization_timeout", fields, 12)
        _set_float(values, "equalization_interval", fields, 13)
        _set_bool_flag(values, "equalization_enabled", fields, 14)
        _set_str(values, "battery_type_code", fields, 15)
        _set_float(values, "low_power_discharge_time", fields, 16)
        _set_str(values, "charging_mode_code", fields, 17)

    gop = await _optional_request(session, values, "GOP", runtime_state=runtime_state)
    if gop:
        fields = parse_space_fields(gop)
        values["eybond_g_ascii_gop_fields"] = " ".join(fields)
        _set_float(values, "output_voltage", fields, 0)
        _set_float(values, "output_frequency", fields, 1)
        _set_float(values, "output_current", fields, 2)
        _set_float(values, "output_low_current", fields, 3)
        _set_float(values, "output_active_power", fields, 4)
        _set_float(values, "output_apparent_power", fields, 6)
        _set_float(values, "output_low_current_power", fields, 7)
        _set_float(values, "output_half_wave_apparent_power", fields, 8)
        _set_float(values, "output_load_percentage", fields, 9)
        _set_scaled_float(values, "output_energy_today", fields, 12, divisor=100.0)
        _set_combined_scaled_counter(values, "output_energy_total", fields, 13, 14, divisor=100.0)

    ginv = await _optional_request(session, values, "GINV", runtime_state=runtime_state)
    if ginv:
        fields = parse_space_fields(ginv)
        _set_float(values, "inverter_voltage", fields, 0)
        _set_float(values, "inverter_frequency", fields, 1)
        _set_float(values, "inverter_current", fields, 2)

    gws = await _optional_request(session, values, "GWS", runtime_state=runtime_state)
    if gws:
        fields = parse_space_fields(gws)
        values["eybond_g_ascii_gws_fields"] = " ".join(fields)
        _set_str(values, "fault_code", fields, 0)
        _set_str(values, "warning_status_1", fields, 1)
        _set_str(values, "warning_status_2", fields, 2)

    bl = await _optional_request(session, values, "BL", runtime_state=runtime_state)
    if bl:
        text = bl.strip()
        if text.startswith("BL"):
            text = text[2:]
        try:
            values["battery_capacity"] = float(text)
        except ValueError:
            pass

    fan = await _optional_request(session, values, "FAN???", runtime_state=runtime_state)
    if fan:
        fields = parse_space_fields(fan)
        values["eybond_g_ascii_fan_fields"] = " ".join(fields)
        _set_float(values, "fan_speed_percentage", fields, 0)
        _set_float(values, "fan1_speed_detected", fields, 1)
        _set_float(values, "fan2_speed_detected", fields, 2)
        _set_bool_flag(values, "fan1_stopped", fields, 3)
        _set_bool_flag(values, "fan2_stopped", fields, 4)

    tcqn = await _optional_request(session, values, "TCQN????", runtime_state=runtime_state)
    if tcqn:
        fields = parse_space_fields(tcqn)
        _set_float(values, "equalization_elapsed_hours", fields, 0)

    date = await _optional_request(session, values, "DATE??????", runtime_state=runtime_state)
    if date:
        fields = parse_space_fields(date)
        _set_offset_2000_date(values, "inverter_date", fields)

    time_value = await _optional_request(session, values, "TIME??????", runtime_state=runtime_state)
    if time_value:
        fields = parse_space_fields(time_value)
        _set_hms_time(values, "inverter_time", fields)

async def _async_collect_eybond_g_ascii_bms_values(
    session: AsciiLineSession,
    values: dict[str, Any],
    *,
    runtime_state: dict[str, Any],
    now_monotonic: float,
) -> None:
    """Collect optional BMS telemetry only when the BMS path appears available.

    G-ASCII payloads are bare ASCII lines without command echoes. A command that
    routinely times out can therefore delay the poll and may contaminate the
    next command if a late payload appears. BMS telemetry is optional and some
    ValueCloud devices without a BMS do not answer ``GBMS`` at all, so gate it
    on the BMS communication readback when available and back off after a real
    timeout when the state is unknown.
    """

    bms_state = _bms_communication_readback_state(values)
    if bms_state is False:
        values["g_ascii_bms_available"] = False
        values["eybond_g_ascii_gbms_skipped_reason"] = "bms_communication_disabled"
        return

    suppressed_until = _optional_command_suppressed_until(runtime_state, "GBMS")
    if suppressed_until > now_monotonic:
        values["g_ascii_bms_available"] = False
        values["eybond_g_ascii_gbms_skipped_reason"] = "recent_timeout"
        values["eybond_g_ascii_gbms_retry_after_s"] = int(
            max(0.0, suppressed_until - now_monotonic)
        )
        return

    gbms = await _optional_request(session, values, "GBMS", runtime_state=runtime_state)
    if _last_command_timing_is_timeout(values, "GBMS"):
        values["g_ascii_bms_available"] = False
        _suppress_optional_command(
            runtime_state,
            "GBMS",
            now_monotonic=now_monotonic,
        )
        return
    if not gbms:
        values["g_ascii_bms_available"] = False
        return
    if gbms:
        fields = parse_space_fields(gbms)
        if not _gbms_has_live_values(fields):
            values["g_ascii_bms_available"] = False
            values["eybond_g_ascii_gbms_skipped_reason"] = "no_live_bms_values"
            _suppress_optional_command(
                runtime_state,
                "GBMS",
                now_monotonic=now_monotonic,
            )
            return
        values["g_ascii_bms_available"] = True
        values["eybond_g_ascii_gbms_fields"] = " ".join(fields)
        _set_str(values, "bms_communication_status_code", fields, 0)
        _set_str(values, "bms_status_code", fields, 1)
        _set_scaled_float_unless_unavailable(
            values, "bms_voltage", fields, 2, divisor=10.0
        )
        _set_scaled_float_unless_unavailable(
            values, "bms_current", fields, 3, divisor=100.0
        )
        _set_scaled_float_unless_unavailable(
            values, "bms_temperature", fields, 4, divisor=10.0
        )
        _set_float_unless_unavailable(values, "bms_soc_raw", fields, 5)
        _set_scaled_float_unless_unavailable(
            values, "bms_remaining_capacity", fields, 6, divisor=10.0
        )
        _set_scaled_float_unless_unavailable(
            values, "bms_rated_capacity", fields, 7, divisor=10.0
        )
        _set_str_unless_unavailable(values, "bms_fault_code", fields, 8)
        _set_str_unless_unavailable(values, "bms_warning_code", fields, 9)
        _set_scaled_float_unless_unavailable(
            values, "bms_max_charging_current", fields, 10, divisor=100.0
        )
        _set_scaled_float_unless_unavailable(
            values, "bms_constant_voltage_point", fields, 11, divisor=10.0
        )


async def _async_collect_g_ascii_capability_readbacks(
    session: AsciiLineSession,
    values: dict[str, Any],
    capabilities: tuple[WriteCapability, ...],
    *,
    runtime_state: dict[str, Any],
    now_monotonic: float,
    deferred: bool = False,
) -> None:
    """Read current values for active learned G-ASCII setting capabilities."""

    by_read_key = {
        capability.read_key: capability
        for capability in capabilities
        if capability.read_key in _G_ASCII_SETTINGS_BY_READ_KEY
        and _definition_deferred_readback_enabled(
            _G_ASCII_SETTINGS_BY_READ_KEY[capability.read_key],
            values,
            deferred=deferred,
        )
    }
    if not by_read_key:
        return

    command_responses: dict[str, str] = {}
    needs_feature_flags = False
    needs_output_voltage = False
    for read_key in by_read_key:
        definition = _G_ASCII_SETTINGS_BY_READ_KEY[read_key]
        if definition.readback_kind in {"enum_command", "numeric_command"} and definition.read_command:
            command_responses.setdefault(
                definition.read_command,
                await _capability_readback_request(
                    session,
                    values,
                    definition.read_command,
                    runtime_state=runtime_state,
                    now_monotonic=now_monotonic,
                    affected_capabilities=_capabilities_for_readback_command(
                        by_read_key,
                        definition.read_command,
                    ),
                ),
            )
        elif definition.readback_kind == "feature_flag":
            needs_feature_flags = True
        elif definition.readback_kind == "output_voltage":
            needs_output_voltage = True

    if needs_output_voltage:
        output_voltage_caps = _capabilities_for_readback_kind(by_read_key, "output_voltage")
        voltage = await _capability_readback_request(
            session,
            values,
            "V???",
            runtime_state=runtime_state,
            now_monotonic=now_monotonic,
            affected_capabilities=output_voltage_caps,
        )
        hv_mode = await _capability_readback_request(
            session,
            values,
            "HV?",
            runtime_state=runtime_state,
            now_monotonic=now_monotonic,
            affected_capabilities=output_voltage_caps,
        )
        _set_output_voltage_setting_readback(values, by_read_key, voltage, hv_mode)

    if needs_feature_flags:
        feature_flag_caps = _capabilities_for_readback_kind(by_read_key, "feature_flag")
        enabled_flags = _compact_flag_response(
            await _capability_readback_request(
                session,
                values,
                "TE?",
                runtime_state=runtime_state,
                now_monotonic=now_monotonic,
                affected_capabilities=feature_flag_caps,
            )
        )
        disabled_flags = _compact_flag_response(
            await _capability_readback_request(
                session,
                values,
                "TD?",
                runtime_state=runtime_state,
                now_monotonic=now_monotonic,
                affected_capabilities=feature_flag_caps,
            )
        )
        _set_feature_flag_readbacks(values, by_read_key, enabled_flags, disabled_flags)

    for read_key, capability in by_read_key.items():
        definition = _G_ASCII_SETTINGS_BY_READ_KEY[read_key]
        if definition.readback_kind not in {"enum_command", "numeric_command"} or not definition.read_command:
            continue
        response = command_responses.get(definition.read_command, "")
        fields = parse_space_fields(response)
        if not fields:
            continue
        raw_code = fields[0].strip()
        if definition.readback_kind == "numeric_command":
            _set_capability_numeric_value(values, capability, raw_code)
            continue
        cloud_value = (definition.read_value_map or {}).get(raw_code)
        if cloud_value is None:
            continue
        _set_capability_label(values, capability, cloud_value)


_READBACK_TIMEOUT_SUPPRESSION_THRESHOLD = 3


async def _capability_readback_request(
    session: AsciiLineSession,
    values: dict[str, Any],
    command: str,
    *,
    runtime_state: dict[str, Any],
    now_monotonic: float,
    affected_capabilities: tuple[WriteCapability, ...],
) -> str:
    del now_monotonic
    if _readback_command_is_suppressed(runtime_state, command):
        _mark_readback_capabilities_hidden(values, affected_capabilities, command)
        _publish_suppressed_readback_commands(values, runtime_state)
        return ""

    payload = await _optional_request(session, values, command)
    if _last_command_timing_is_timeout(values, command) and _g_ascii_runtime_link_active(values):
        count = _increment_readback_timeout_count(runtime_state, command)
        values[f"eybond_g_ascii_readback_timeout_count_{_command_value_key(command)}"] = count
        if count >= _READBACK_TIMEOUT_SUPPRESSION_THRESHOLD:
            _suppress_readback_command(runtime_state, command)
            _mark_readback_capabilities_hidden(values, affected_capabilities, command)
            _publish_suppressed_readback_commands(values, runtime_state)
        return ""

    if payload:
        _clear_readback_timeout_state(runtime_state, command)
    _publish_suppressed_readback_commands(values, runtime_state)
    return payload


def _capabilities_for_readback_command(
    capabilities_by_read_key: dict[str, WriteCapability],
    command: str,
) -> tuple[WriteCapability, ...]:
    result: list[WriteCapability] = []
    for read_key, capability in capabilities_by_read_key.items():
        definition = _G_ASCII_SETTINGS_BY_READ_KEY.get(read_key)
        if definition is None or definition.read_command != command:
            continue
        result.append(capability)
    return tuple(result)


def _capabilities_for_readback_kind(
    capabilities_by_read_key: dict[str, WriteCapability],
    readback_kind: str,
) -> tuple[WriteCapability, ...]:
    result: list[WriteCapability] = []
    for read_key, capability in capabilities_by_read_key.items():
        definition = _G_ASCII_SETTINGS_BY_READ_KEY.get(read_key)
        if definition is None or definition.readback_kind != readback_kind:
            continue
        result.append(capability)
    return tuple(result)


def _readback_command_is_suppressed(runtime_state: dict[str, Any], command: str) -> bool:
    suppressed = runtime_state.get("eybond_g_ascii_readback_suppressed_commands")
    return isinstance(suppressed, dict) and bool(suppressed.get(command))


def _increment_readback_timeout_count(runtime_state: dict[str, Any], command: str) -> int:
    counts = runtime_state.setdefault("eybond_g_ascii_readback_timeout_counts", {})
    if not isinstance(counts, dict):
        counts = {}
        runtime_state["eybond_g_ascii_readback_timeout_counts"] = counts
    count = int(counts.get(command, 0) or 0) + 1
    counts[command] = count
    return count


def _clear_readback_timeout_state(runtime_state: dict[str, Any], command: str) -> None:
    counts = runtime_state.get("eybond_g_ascii_readback_timeout_counts")
    if isinstance(counts, dict):
        counts.pop(command, None)
    suppressed = runtime_state.get("eybond_g_ascii_readback_suppressed_commands")
    if isinstance(suppressed, dict):
        suppressed.pop(command, None)


def _suppress_readback_command(runtime_state: dict[str, Any], command: str) -> None:
    suppressed = runtime_state.setdefault("eybond_g_ascii_readback_suppressed_commands", {})
    if not isinstance(suppressed, dict):
        suppressed = {}
        runtime_state["eybond_g_ascii_readback_suppressed_commands"] = suppressed
    suppressed[command] = "consecutive_timeouts"


def _mark_readback_capabilities_hidden(
    values: dict[str, Any],
    capabilities: tuple[WriteCapability, ...],
    command: str,
) -> None:
    reason = (
        f"Readback command {command} timed out repeatedly. "
        "The control is hidden until the integration is reloaded or the device is rechecked."
    )
    for capability in capabilities:
        values[f"capability_hidden_reason_{capability.key}"] = reason
        values[f"g_ascii_readback_available_{capability.read_key}"] = False


def _publish_suppressed_readback_commands(
    values: dict[str, Any],
    runtime_state: dict[str, Any],
) -> None:
    suppressed = runtime_state.get("eybond_g_ascii_readback_suppressed_commands")
    if not isinstance(suppressed, dict) or not suppressed:
        return
    values["eybond_g_ascii_suppressed_readback_commands"] = ",".join(
        sorted(str(command) for command in suppressed)
    )


def _g_ascii_runtime_link_active(values: dict[str, Any]) -> bool:
    return bool(
        values.get("eybond_g_ascii_gdat0_fields")
        or values.get("eybond_g_ascii_gpv_fields")
        or values.get("operating_mode")
    )


def _command_value_key(command: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in command).strip("_")


def _definition_deferred_readback_enabled(
    definition: GAsciiSettingDefinition,
    values: dict[str, Any],
    *,
    deferred: bool,
) -> bool:
    requires_key = str(definition.requires_key or "").strip()
    if not requires_key:
        return not deferred
    if not deferred:
        return False
    return bool(values.get(requires_key))


async def _async_collect_eybond_g_ascii_offline_fingerprint_values(
    session: AsciiLineSession,
    values: dict[str, Any],
) -> None:
    """Collect cheap local-only fingerprint fields used for model resolution."""

    rated = await _optional_request(session, values, "F")
    if rated:
        fields = parse_space_fields(rated)
        _set_float(values, "rated_output_voltage", fields, 0)
        _set_float(values, "rated_output_current", fields, 1)
        _set_float(values, "rated_battery_voltage", fields, 2)
        _set_float(values, "rated_frequency", fields, 3)

    svfw = await _optional_request(session, values, "SVFW")
    if svfw:
        fields = parse_space_fields(svfw)
        _set_str(values, "eybond_g_ascii_software_version", fields, 0)
        _set_clean_date(values, "eybond_g_ascii_software_date", fields, 1)


def _extend_eybond_g_ascii_offline_fingerprint_evidence(
    values: dict[str, Any],
    evidence: dict[str, Any],
) -> None:
    """Publish local-only G-ASCII fingerprint evidence for catalog matching."""

    _copy_present_evidence(values, evidence, "rated_output_voltage", "rating.output_voltage")
    _copy_present_evidence(values, evidence, "rated_output_current", "rating.output_current")
    _copy_present_evidence(values, evidence, "rated_battery_voltage", "rating.battery_voltage")
    _copy_present_evidence(values, evidence, "rated_frequency", "rating.frequency")
    _copy_present_evidence(
        values,
        evidence,
        "eybond_g_ascii_software_version",
        "firmware.software_version",
    )
    _copy_present_evidence(
        values,
        evidence,
        "eybond_g_ascii_software_date",
        "firmware.software_date",
    )
    _copy_present_evidence(values, evidence, "gdat0_internal_code", "fingerprint.gdat0_internal_code")


def _resolve_eybond_g_ascii_catalog(catalog, evidence: dict[str, Any]):
    """Resolve exact G-ASCII model only when the local fingerprint is complete.

    Exact matching goes through the same compiled decision tree every other
    catalog driver uses (the value-based resolve() engine had divergent
    priority semantics and is gone). The tree is fail-closed on missing
    anchors, so exact matching is attempted only once the offline fingerprint
    keys required by the LVYUAN descriptor are present; anything else falls
    back to the explicit family-fallback operation, which is the degraded
    path for devices that do not answer F/SVFW.
    """

    if all(key in evidence for key in _EYBOND_G_ASCII_EXACT_FINGERPRINT_KEYS):
        tree = catalog.decision_trees.get(_EYBOND_G_ASCII_DRIVER_KEY)
        if tree is not None:
            evaluation = evaluate_detection_decision_tree_static(tree, evidence)
            if evaluation.status in {"resolved", "ambiguous"} and evaluation.candidate_keys:
                return catalog.resolution_for_candidates(
                    protocol_key=_EYBOND_G_ASCII_DRIVER_KEY,
                    candidate_keys=evaluation.candidate_keys,
                    evidence=evidence,
                    decision_path=tuple(
                        f"{step.anchor_key}={step.value!r}" for step in evaluation.path
                    ),
                )
    return catalog.resolve_family(protocol_key=_EYBOND_G_ASCII_DRIVER_KEY, evidence=evidence)


def _copy_present_evidence(
    values: dict[str, Any],
    evidence: dict[str, Any],
    value_key: str,
    evidence_key: str,
) -> None:
    value = values.get(value_key)
    if value not in (None, ""):
        evidence[evidence_key] = value


def _set_output_voltage_setting_readback(
    values: dict[str, Any],
    capabilities: dict[str, WriteCapability],
    voltage_response: str,
    hv_mode_response: str,
) -> None:
    capability = capabilities.get("g_ascii_setting_output_voltage")
    if capability is None:
        return
    fields = parse_space_fields(voltage_response)
    if not fields:
        return
    hv_fields = parse_space_fields(hv_mode_response)
    low_voltage_mode = bool(hv_fields and hv_fields[0].strip() == "1")
    raw_value = fields[1].strip() if low_voltage_mode and len(fields) >= 2 else fields[0].strip()
    if raw_value not in capability.enum_value_map and len(fields) >= 2:
        fallback = fields[1].strip()
        if fallback in capability.enum_value_map:
            raw_value = fallback
    _set_capability_label(values, capability, raw_value)


def _set_feature_flag_readbacks(
    values: dict[str, Any],
    capabilities: dict[str, WriteCapability],
    enabled_flags: str,
    disabled_flags: str,
) -> None:
    for read_key, capability in capabilities.items():
        definition = _G_ASCII_SETTINGS_BY_READ_KEY.get(read_key)
        if definition is None or definition.readback_kind != "feature_flag":
            continue
        flag = definition.feature_flag.upper()
        enabled_value = "1" if 1 in capability.enum_value_map else "69"
        disabled_value = "0" if 0 in capability.enum_value_map else "68"
        if flag and flag in enabled_flags:
            _set_capability_label(values, capability, enabled_value)
        elif flag and flag in disabled_flags:
            _set_capability_label(values, capability, disabled_value)


def _compact_flag_response(response: str) -> str:
    return "".join(parse_space_fields(response)).strip().upper()


def _set_capability_label(
    values: dict[str, Any],
    capability: WriteCapability,
    raw_value: str,
) -> None:
    try:
        raw_int = int(raw_value)
    except (TypeError, ValueError):
        return
    label = capability.enum_value_map.get(raw_int)
    if label is not None:
        values[capability.value_key] = label


def _set_capability_numeric_value(
    values: dict[str, Any],
    capability: WriteCapability,
    raw_value: str,
) -> None:
    try:
        parsed = float(str(raw_value).strip())
    except (TypeError, ValueError):
        return
    if capability.value_kind in {"u16", "u32"}:
        values[capability.value_key] = int(round(parsed))
    else:
        values[capability.value_key] = round(
            parsed,
            capability.command_precision
            if capability.command_precision is not None
            else decimals_for_divisor(capability.divisor or 1),
        )


async def _async_collect_eybond_g_ascii_core_values(
    session: AsciiLineSession,
    values: dict[str, Any],
) -> None:
    """Collect core fields used by probe and regular runtime reads."""

    gmod = await _optional_request(session, values, "GMOD")
    if gmod:
        mode_code = gmod.strip()
        values["eybond_g_ascii_operating_mode_code"] = mode_code
        values["operating_mode"] = _OPERATING_MODE_BY_CODE.get(
            mode_code.upper(),
            f"Unknown ({mode_code})",
        )

    gdat0 = await _optional_request(session, values, "GPDAT0")
    if gdat0:
        fields = parse_space_fields(gdat0)
        values["eybond_g_ascii_gdat0_fields"] = " ".join(fields)
        _set_str(values, "gdat0_communication_status_code", fields, 0)
        _set_str(values, "gdat0_internal_code", fields, 2)
        _set_mapped_str_if_absent(
            values,
            "operating_mode",
            fields,
            1,
            _GPDAT_OPERATING_MODE_BY_CODE,
        )
        _set_str(values, "gdat0_operating_mode_code", fields, 1)
        _set_float(values, "inverter_voltage", fields, 5)
        _set_float(values, "inverter_frequency", fields, 6)
        _set_float(values, "grid_voltage", fields, 7)
        _set_float(values, "grid_frequency", fields, 8)
        _set_float(values, "mains_input_voltage", fields, 7)
        _set_float(values, "mains_frequency", fields, 8)
        _set_float(values, "output_voltage", fields, 9)
        _set_float(values, "output_frequency", fields, 10)
        _set_float(values, "output_current", fields, 11)
        _set_float_if_absent(values, "battery_voltage", fields, 12)
        _set_float_if_absent(values, "battery_current", fields, 13)
        _set_float(values, "output_load_percentage", fields, 14)
        _set_float(values, "output_apparent_power", fields, 15)
        _set_float(values, "output_active_power", fields, 16)
        _set_float(values, "battery_capacity", fields, 17)
        _set_float_if_absent(values, "pv_input_voltage", fields, 18)
        _set_float_if_absent(values, "pv_charging_current", fields, 19)
        _set_float_if_absent(values, "pv_power", fields, 20)
        _set_float(values, "mainboard_temperature", fields, 21)

    gpv = await _optional_request(session, values, "GPV")
    if gpv:
        fields = parse_space_fields(gpv)
        values["eybond_g_ascii_gpv_fields"] = " ".join(fields)
        _set_float(values, "pv_input_voltage", fields, 0)
        _set_float_if_absent(values, "battery_voltage", fields, 1)
        _set_float(values, "pv_charging_current", fields, 2)
        _set_float(values, "pv_current", fields, 3)
        _set_float(values, "pv_power", fields, 4)
        _set_str(values, "pv_tracking_status", fields, 5)
        _set_str(values, "pv_chargeable_status", fields, 6)
        _set_scaled_float(values, "pv_energy_today", fields, 20, divisor=100.0)
        _set_combined_scaled_counter(values, "pv_energy_total", fields, 21, 22, divisor=100.0)
        _set_str(values, "warning_status_1", fields, 23)


async def _optional_request(
    session: AsciiLineSession,
    values: dict[str, Any],
    command: str,
    *,
    runtime_state: dict[str, Any] | None = None,
) -> str:
    # The shared unsupported-command cache applies only when the caller
    # passes runtime state; the capability-readback path keeps its own
    # dedicated suppression mechanism and calls without it.
    if runtime_state is not None and command_skipped_as_unsupported(runtime_state, command):
        return ""
    started = time.monotonic()
    status = "ok"
    error = ""
    payload = ""
    try:
        payload = await session.request(command)
        record_command_success(runtime_state, command)
        return payload
    except (AsciiLineError, KeyError, TimeoutError) as exc:
        status = "error"
        error = str(exc) or exc.__class__.__name__
        record_command_failure(runtime_state, command)
        return ""
    finally:
        _record_g_ascii_runtime_command_timing(
            values,
            command=command,
            status=status,
            duration_ms=int(round((time.monotonic() - started) * 1000.0)),
            payload=payload,
            error=error,
            transport_timing=_last_ascii_transport_timing(session),
        )


def _last_ascii_transport_timing(session: AsciiLineSession) -> dict[str, int]:
    getter = getattr(session, "last_transport_timing", None)
    if not callable(getter):
        return {}
    try:
        return dict(getter())
    except Exception:
        return {}


def _record_g_ascii_runtime_command_timing(
    values: dict[str, Any],
    *,
    command: str,
    status: str,
    duration_ms: int,
    payload: str,
    error: str,
    transport_timing: dict[str, int],
) -> None:
    timings = values.setdefault("eybond_g_ascii_runtime_command_timings", [])
    if not isinstance(timings, list):
        return
    record: dict[str, Any] = {
        "command": command,
        "status": status,
        "duration_ms": max(0, int(duration_ms)),
    }
    if error:
        record["error"] = error
    if transport_timing:
        record["transport"] = dict(transport_timing)
    if payload:
        fields = parse_space_fields(payload)
        record["field_count"] = len(fields)
        record["payload_preview"] = _short_ascii_preview(payload)
    timings.append(record)

    slowest = values.get("eybond_g_ascii_runtime_slowest_command")
    if not isinstance(slowest, dict) or record["duration_ms"] > int(
        slowest.get("duration_ms", 0) or 0
    ):
        values["eybond_g_ascii_runtime_slowest_command"] = dict(record)
    values["eybond_g_ascii_runtime_command_count"] = len(timings)


_OPTIONAL_COMMAND_TIMEOUT_BACKOFF_SECONDS = 600.0


def _bms_communication_readback_state(values: dict[str, Any]) -> bool | None:
    text = str(values.get("g_ascii_setting_bms_communication") or "").strip().lower()
    if not text:
        return None
    if "disabled" in text:
        return False
    if "enabled" in text:
        return True
    return None


def _optional_command_suppressed_until(
    runtime_state: dict[str, Any],
    command: str,
) -> float:
    suppressed = runtime_state.get("eybond_g_ascii_optional_command_suppressed_until")
    if not isinstance(suppressed, dict):
        return 0.0
    try:
        return float(suppressed.get(command, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _suppress_optional_command(
    runtime_state: dict[str, Any],
    command: str,
    *,
    now_monotonic: float,
) -> None:
    suppressed = runtime_state.setdefault(
        "eybond_g_ascii_optional_command_suppressed_until",
        {},
    )
    if not isinstance(suppressed, dict):
        suppressed = {}
        runtime_state["eybond_g_ascii_optional_command_suppressed_until"] = suppressed
    suppressed[command] = float(now_monotonic) + _OPTIONAL_COMMAND_TIMEOUT_BACKOFF_SECONDS


def _last_command_timing_is_timeout(values: dict[str, Any], command: str) -> bool:
    timings = values.get("eybond_g_ascii_runtime_command_timings")
    if not isinstance(timings, list) or not timings:
        return False
    record = timings[-1]
    if not isinstance(record, dict):
        return False
    if str(record.get("command") or "") != command:
        return False
    if str(record.get("status") or "") != "error":
        return False
    return str(record.get("error") or "").strip() == "request_timeout"


def _short_ascii_preview(payload: str, *, limit: int = 160) -> str:
    text = "".join(ch if ch.isprintable() else " " for ch in str(payload))
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _looks_like_eybond_g_ascii(values: dict[str, Any]) -> bool:
    return bool(
        values.get("eybond_g_ascii_gdat0_fields")
        or values.get("eybond_g_ascii_gpv_fields")
    )


def _space_field_count(value: object) -> int:
    if not isinstance(value, str) or not value.strip():
        return 0
    return len(parse_space_fields(value))


def _set_str(values: dict[str, Any], key: str, fields: list[str], index: int) -> None:
    try:
        raw = fields[index]
    except IndexError:
        return
    text = str(raw).strip()
    if text:
        values[key] = text


def _set_mapped_str_if_absent(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
    mapping: dict[str, str],
) -> None:
    if key in values:
        return
    try:
        raw = fields[index]
    except IndexError:
        return
    code = str(raw).strip().upper()
    if not code:
        return
    values[key] = mapping.get(code, f"Unknown ({code})")


def _set_str_unless_unavailable(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
) -> None:
    try:
        raw = fields[index]
    except IndexError:
        return
    if _is_unavailable_numeric_field(raw):
        return
    text = str(raw).strip()
    if text:
        values[key] = text


def _set_clean_date(values: dict[str, Any], key: str, fields: list[str], index: int) -> None:
    try:
        raw = fields[index]
    except IndexError:
        return
    text = str(raw).strip().lstrip("(").rstrip(".")
    if len(text) == 8 and text.isdigit():
        values[key] = f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    elif text:
        values[key] = text


def _set_float(values: dict[str, Any], key: str, fields: list[str], index: int) -> None:
    try:
        raw = _clean_numeric_field(fields[index])
    except IndexError:
        return
    try:
        values[key] = float(raw)
    except (TypeError, ValueError):
        return


def _set_float_unless_unavailable(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
) -> None:
    try:
        raw = fields[index]
    except IndexError:
        return
    if _is_unavailable_numeric_field(raw):
        return
    try:
        values[key] = float(_clean_numeric_field(raw))
    except (TypeError, ValueError):
        return


def _set_float_if_absent(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
) -> None:
    if key in values:
        return
    _set_float(values, key, fields, index)


def _set_float_preserve_existing_nonzero(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
) -> None:
    """Set a float value without replacing a previous live value by zero.

    Some EyeBond G-ASCII firmwares report ``GBAT[1]`` as ``0.00`` even while
    ``GPDAT0``/``GPV`` expose a real charge current.  Treat the later zero as a
    missing value in that specific case, but still allow non-zero GBAT values to
    override earlier telemetry.
    """

    try:
        raw = _clean_numeric_field(fields[index])
    except IndexError:
        return
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return
    existing = values.get(key)
    if value == 0.0 and isinstance(existing, (int, float)) and float(existing) != 0.0:
        return
    values[key] = value


def _set_bool_flag(values: dict[str, Any], key: str, fields: list[str], index: int) -> None:
    try:
        raw = str(fields[index]).strip()
    except IndexError:
        return
    if raw in {"0", "1"}:
        values[key] = raw == "1"


def _set_scaled_float(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
    *,
    divisor: float,
) -> None:
    try:
        raw = _clean_numeric_field(fields[index])
    except IndexError:
        return
    try:
        values[key] = float(raw) / float(divisor)
    except (TypeError, ValueError, ZeroDivisionError):
        return


def _set_scaled_float_unless_unavailable(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
    *,
    divisor: float,
) -> None:
    try:
        raw = fields[index]
    except IndexError:
        return
    if _is_unavailable_numeric_field(raw):
        return
    try:
        values[key] = float(_clean_numeric_field(raw)) / float(divisor)
    except (TypeError, ValueError, ZeroDivisionError):
        return


def _set_combined_scaled_counter(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    high_index: int,
    low_index: int,
    *,
    divisor: float,
) -> None:
    try:
        high = int(_clean_numeric_field(fields[high_index]))
        low = int(_clean_numeric_field(fields[low_index]))
    except (IndexError, TypeError, ValueError):
        return
    try:
        values[key] = float((high << 16) + low) / float(divisor)
    except ZeroDivisionError:
        return


def _set_offset_2000_date(values: dict[str, Any], key: str, fields: list[str]) -> None:
    if len(fields) < 3:
        return
    try:
        year = 2000 + int(_clean_numeric_field(fields[0]))
        month = int(_clean_numeric_field(fields[1]))
        day = int(_clean_numeric_field(fields[2]))
    except (TypeError, ValueError):
        return
    if 2000 <= year <= 2099 and 1 <= month <= 12 and 1 <= day <= 31:
        values[key] = f"{year:04d}-{month:02d}-{day:02d}"


def _set_hms_time(values: dict[str, Any], key: str, fields: list[str]) -> None:
    if len(fields) < 3:
        return
    try:
        hour = int(_clean_numeric_field(fields[0]))
        minute = int(_clean_numeric_field(fields[1]))
        second = int(_clean_numeric_field(fields[2]))
    except (TypeError, ValueError):
        return
    if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
        values[key] = f"{hour:02d}:{minute:02d}:{second:02d}"


def _clean_numeric_field(value: object) -> str:
    return str(value).strip().lstrip("#").rstrip(".")


def _is_unavailable_numeric_field(value: object) -> bool:
    text = _clean_numeric_field(value)
    if not text:
        return True
    try:
        return int(text) in {0xFFFF, 0xFFFFFFFF}
    except ValueError:
        return False


def _gbms_has_live_values(fields: list[str]) -> bool:
    """Return true when GBMS carries real BMS data, not no-BMS sentinels.

    Devices without a BMS may still answer ``GBMS`` with status-like zeros and
    ``65535`` placeholders.  Do not expose BMS entities until at least one
    measurement/configuration field that represents actual BMS data is present.
    """

    if len(fields) < 12:
        return False

    meaningful_indexes = (2, 3, 4, 5, 6, 7, 10, 11)
    for index in meaningful_indexes:
        try:
            raw = fields[index]
        except IndexError:
            continue
        if _is_unavailable_numeric_field(raw):
            continue
        try:
            if float(_clean_numeric_field(raw)) != 0.0:
                return True
        except (TypeError, ValueError):
            return True
    return False
