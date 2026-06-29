"""EyeBond G-ASCII inverter/logger protocol driver."""

from __future__ import annotations

import asyncio
from functools import lru_cache
import json
from pathlib import Path
import time
from typing import Any

from ..metadata.device_catalog_loader import resolve_catalog_surface_binding
from ..metadata.register_schema_loader import load_register_schema
from ..models import (
    DetectedInverter,
    ProbeTarget,
)
from ..payload.ascii_line import (
    AsciiLineError,
    AsciiLineSession,
    parse_ascii_line_response,
    parse_space_fields,
)
from .base import InverterDriver


_EYBOND_G_ASCII_DRIVER_KEY = "eybond_g_ascii"
_EYBOND_G_ASCII_VARIANT_KEY = "g_ascii_family"
_EYBOND_G_ASCII_FALLBACK_SCHEMA_NAME = "eybond_g_ascii/base.json"

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
    return _eybond_g_ascii_catalog_binding()[1]


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

        details = {
            **values,
            "protocol_id": "EYBOND_G_ASCII",
            "catalog_detection": {
                "resolution": "family",
                "surface_key": "eybond_g_ascii_read_only",
                "evidence": evidence,
            },
        }

        return DetectedInverter(
            driver_key=self.key,
            protocol_family="eybond_g_ascii",
            model_name="EyeBond G-ASCII inverter",
            variant_key=_eybond_g_ascii_variant_key(),
            serial_number=serial_number,
            probe_target=target,
            details=details,
            profile_name=_eybond_g_ascii_profile_name(),
            register_schema_name=_eybond_g_ascii_register_schema_name(),
            capability_groups=(),
            capabilities=(),
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
        del runtime_state, poll_interval, now_monotonic
        return await _async_collect_eybond_g_ascii_values(
            self._session(transport, inverter.probe_target),
            probe=False,
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
        del transport, inverter, value
        raise KeyError(capability_key)

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


def _support_command_specs() -> tuple[dict[str, Any], ...]:
    schema = _load_command_schema()
    return _support_command_specs_from_schema(schema)


def _support_probe_plan() -> tuple[str, tuple[dict[str, Any], ...]]:
    schema = _load_command_schema()
    return str(schema.get("schema_key") or ""), _support_command_specs_from_schema(schema)


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
) -> dict[str, Any]:
    values: dict[str, Any] = {}

    await _async_collect_eybond_g_ascii_core_values(session, values)

    if probe:
        return values

    if not probe:
        rated = await _optional_request(session, "F")
        if rated:
            fields = parse_space_fields(rated)
            _set_float(values, "rated_output_voltage", fields, 0)
            _set_float(values, "rated_output_current", fields, 1)
            _set_float(values, "rated_frequency", fields, 3)

        svfw = await _optional_request(session, "SVFW")
        if svfw:
            fields = parse_space_fields(svfw)
            _set_str(values, "eybond_g_ascii_software_version", fields, 0)
            _set_clean_date(values, "eybond_g_ascii_software_date", fields, 1)

        gtmp = await _optional_request(session, "GTMP")
        if gtmp:
            fields = parse_space_fields(gtmp)
            _set_float(values, "pv_side_temperature", fields, 0)
            _set_float(values, "charger_temperature", fields, 1)
            _set_float(values, "ambient_temperature", fields, 2)
            _set_float(values, "low_voltage_mppt_temperature_1", fields, 3)
            _set_float(values, "low_voltage_mppt_temperature_2", fields, 4)

        gline = await _optional_request(session, "GLINE")
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

        gbat = await _optional_request(session, "GBAT")
        if gbat:
            fields = parse_space_fields(gbat)
            values["eybond_g_ascii_gbat_fields"] = " ".join(fields)
            _set_float(values, "battery_voltage", fields, 0)
            _set_float_preserve_existing_nonzero(values, "battery_current", fields, 1)
            _set_float(values, "battery_cell_count", fields, 2)
            _set_float(values, "battery_discharge_cutoff_voltage", fields, 3)
            _set_float(values, "battery_discharge_alarm_voltage", fields, 4)

        gbus = await _optional_request(session, "GBUS")
        if gbus:
            fields = parse_space_fields(gbus)
            _set_float(values, "bus_voltage", fields, 0)
            _set_float(values, "bus_reference_start_voltage", fields, 1)
            _set_float(values, "bus_reference_voltage", fields, 2)

        gchg = await _optional_request(session, "GCHG")
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

        gop = await _optional_request(session, "GOP")
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

        ginv = await _optional_request(session, "GINV")
        if ginv:
            fields = parse_space_fields(ginv)
            _set_float(values, "inverter_voltage", fields, 0)
            _set_float(values, "inverter_frequency", fields, 1)
            _set_float(values, "inverter_current", fields, 2)

        gws = await _optional_request(session, "GWS")
        if gws:
            fields = parse_space_fields(gws)
            values["eybond_g_ascii_gws_fields"] = " ".join(fields)
            _set_str(values, "fault_code", fields, 0)
            _set_str(values, "warning_status_1", fields, 1)
            _set_str(values, "warning_status_2", fields, 2)

        bl = await _optional_request(session, "BL")
        if bl:
            text = bl.strip()
            if text.startswith("BL"):
                text = text[2:]
            try:
                values["battery_capacity"] = float(text)
            except ValueError:
                pass

        fan = await _optional_request(session, "FAN???")
        if fan:
            fields = parse_space_fields(fan)
            values["eybond_g_ascii_fan_fields"] = " ".join(fields)
            _set_float(values, "fan_speed_percentage", fields, 0)
            _set_float(values, "fan1_speed_detected", fields, 1)
            _set_float(values, "fan2_speed_detected", fields, 2)
            _set_bool_flag(values, "fan1_stopped", fields, 3)
            _set_bool_flag(values, "fan2_stopped", fields, 4)

        tcqn = await _optional_request(session, "TCQN????")
        if tcqn:
            fields = parse_space_fields(tcqn)
            _set_float(values, "equalization_elapsed_hours", fields, 0)

        date = await _optional_request(session, "DATE??????")
        if date:
            fields = parse_space_fields(date)
            _set_offset_2000_date(values, "inverter_date", fields)

        time = await _optional_request(session, "TIME??????")
        if time:
            fields = parse_space_fields(time)
            _set_hms_time(values, "inverter_time", fields)

        gbms = await _optional_request(session, "GBMS")
        if gbms:
            fields = parse_space_fields(gbms)
            if _gbms_has_live_values(fields):
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

    return values


async def _async_collect_eybond_g_ascii_core_values(
    session: AsciiLineSession,
    values: dict[str, Any],
) -> None:
    """Collect core fields used by probe and regular runtime reads."""

    gmod = await _optional_request(session, "GMOD")
    if gmod:
        mode_code = gmod.strip()
        values["eybond_g_ascii_operating_mode_code"] = mode_code
        values["operating_mode"] = _OPERATING_MODE_BY_CODE.get(
            mode_code.upper(),
            f"Unknown ({mode_code})",
        )

    gdat0 = await _optional_request(session, "GPDAT0")
    if gdat0:
        fields = parse_space_fields(gdat0)
        values["eybond_g_ascii_gdat0_fields"] = " ".join(fields)
        _set_str(values, "gdat0_communication_status_code", fields, 0)
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

    gpv = await _optional_request(session, "GPV")
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


async def _optional_request(session: AsciiLineSession, command: str) -> str:
    try:
        return await session.request(command)
    except (AsciiLineError, KeyError, TimeoutError):
        return ""


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
