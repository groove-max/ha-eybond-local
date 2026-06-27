"""EyeBond G-ASCII inverter/logger protocol driver."""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from ..models import DetectedInverter, MeasurementDescription, ProbeTarget
from ..payload.ascii_line import (
    AsciiLineError,
    AsciiLineSession,
    parse_ascii_line_response,
    parse_space_fields,
)
from .base import InverterDriver


_EYBOND_G_ASCII_PROBE_TARGETS: tuple[ProbeTarget, ...] = (
    ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
)

_DCDC_STATUS_BY_MODE: dict[str, str] = {
    "B": "Charge",
    "0": "Discharge soft start",
}
_COMMAND_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "protocol_catalogs"
    / "command_schemas"
    / "eybond_g_ascii"
    / "base.json"
)


_MEASUREMENTS: tuple[MeasurementDescription, ...] = (
    MeasurementDescription(
        key="eybond_g_ascii_operating_mode_code",
        name="EyeBond G-ASCII Operating Mode",
        icon="mdi:state-machine",
    ),
    MeasurementDescription(
        key="grid_voltage",
        name="Grid Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="grid_frequency",
        name="Grid Frequency",
        unit="Hz",
        device_class="frequency",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="output_voltage",
        name="Output Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="output_frequency",
        name="Output Frequency",
        unit="Hz",
        device_class="frequency",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="mains_input_voltage",
        name="Mains Input Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="mains_frequency",
        name="Mains Frequency",
        unit="Hz",
        device_class="frequency",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="output_current",
        name="Output Current",
        unit="A",
        device_class="current",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="inverter_voltage",
        name="Inverter Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="inverter_frequency",
        name="Inverter Frequency",
        unit="Hz",
        device_class="frequency",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="output_load_percentage",
        name="Output Load Percentage",
        unit="%",
        state_class="measurement",
        suggested_display_precision=0,
    ),
    MeasurementDescription(
        key="output_active_power",
        name="Output Active Power",
        unit="W",
        device_class="power",
        state_class="measurement",
        suggested_display_precision=0,
    ),
    MeasurementDescription(
        key="output_apparent_power",
        name="Output Apparent Power",
        unit="VA",
        device_class="apparent_power",
        state_class="measurement",
        suggested_display_precision=0,
    ),
    MeasurementDescription(
        key="battery_voltage",
        name="Battery Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="battery_current",
        name="Battery Current",
        unit="A",
        device_class="current",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="battery_capacity",
        name="Battery Capacity",
        unit="%",
        device_class="battery",
        state_class="measurement",
        suggested_display_precision=0,
    ),
    MeasurementDescription(
        key="battery_cell_count",
        name="Battery Cell Count",
        icon="mdi:battery-cog-outline",
        diagnostic=True,
    ),
    MeasurementDescription(
        key="battery_discharge_cutoff_voltage",
        name="Battery Discharge Cut-Off Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="battery_discharge_alarm_voltage",
        name="Battery Discharge Alarm Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="pv_input_voltage",
        name="PV Input Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="pv_charging_current",
        name="PV Charging Current",
        unit="A",
        device_class="current",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="pv_current",
        name="PV Current",
        unit="A",
        device_class="current",
        state_class="measurement",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="pv_power",
        name="PV Power",
        unit="W",
        device_class="power",
        state_class="measurement",
        suggested_display_precision=0,
    ),
    MeasurementDescription(
        key="pv_tracking_status",
        name="PV Tracking Status",
        icon="mdi:solar-power",
        diagnostic=True,
    ),
    MeasurementDescription(
        key="pv_chargeable_status",
        name="PV Chargeable Status",
        icon="mdi:battery-charging",
        diagnostic=True,
    ),
    MeasurementDescription(
        key="pv_energy_today",
        name="PV Energy Today",
        unit="kWh",
        device_class="energy",
        state_class="total_increasing",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="pv_energy_total",
        name="Total PV Energy",
        unit="kWh",
        device_class="energy",
        state_class="total_increasing",
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="inverter_temperature",
        name="Inverter Temperature",
        unit="°C",
        device_class="temperature",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="mainboard_temperature",
        name="Mainboard Temperature",
        unit="°C",
        device_class="temperature",
        state_class="measurement",
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="pv_side_temperature",
        name="PV Side Temperature",
        unit="°C",
        device_class="temperature",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="charger_temperature",
        name="Charger Temperature",
        unit="°C",
        device_class="temperature",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="ambient_temperature",
        name="Ambient Temperature",
        unit="°C",
        device_class="temperature",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="bus_voltage",
        name="Bus Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="charging_voltage",
        name="Charging Voltage",
        unit="V",
        device_class="voltage",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="charging_current",
        name="Charging Current",
        unit="A",
        device_class="current",
        state_class="measurement",
        diagnostic=True,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="grid_energy_today",
        name="Grid Energy Today",
        unit="kWh",
        device_class="energy",
        state_class="total_increasing",
        diagnostic=True,
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="grid_energy_total",
        name="Total Grid Energy",
        unit="kWh",
        device_class="energy",
        state_class="total_increasing",
        diagnostic=True,
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="output_energy_today",
        name="Output Energy Today",
        unit="kWh",
        device_class="energy",
        state_class="total_increasing",
        diagnostic=True,
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="output_energy_total",
        name="Total Output Energy",
        unit="kWh",
        device_class="energy",
        state_class="total_increasing",
        diagnostic=True,
        suggested_display_precision=2,
    ),
    MeasurementDescription(
        key="rated_output_voltage",
        name="Rated Output Voltage",
        unit="V",
        device_class="voltage",
        diagnostic=True,
        live=False,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="rated_output_current",
        name="Rated Output Current",
        unit="A",
        device_class="current",
        diagnostic=True,
        live=False,
        suggested_display_precision=0,
    ),
    MeasurementDescription(
        key="rated_frequency",
        name="Rated Frequency",
        unit="Hz",
        device_class="frequency",
        diagnostic=True,
        live=False,
        suggested_display_precision=1,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_software_version",
        name="EyeBond G-ASCII Software Version",
        icon="mdi:chip",
        diagnostic=True,
        live=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_software_date",
        name="EyeBond G-ASCII Software Date",
        icon="mdi:calendar",
        diagnostic=True,
        live=False,
    ),
    MeasurementDescription(
        key="fault_code",
        name="Fault Code",
        icon="mdi:alert-circle-outline",
        diagnostic=True,
    ),
    MeasurementDescription(
        key="warning_status_1",
        name="Warning Status 1",
        icon="mdi:alert-outline",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="warning_status_2",
        name="Warning Status 2",
        icon="mdi:alert-outline",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="dcdc_control_status",
        name="DCDC Control Status",
        icon="mdi:transfer",
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gdat0_fields",
        name="EyeBond GPDAT0 Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gpv_fields",
        name="EyeBond GPV Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gbat_fields",
        name="EyeBond GBAT Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gline_fields",
        name="EyeBond GLINE Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gop_fields",
        name="EyeBond GOP Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gchg_fields",
        name="EyeBond GCHG Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
    MeasurementDescription(
        key="eybond_g_ascii_gws_fields",
        name="EyeBond GWS Fields",
        icon="mdi:format-list-numbered",
        diagnostic=True,
        enabled_default=False,
    ),
)


class EybondGAsciiDriver(InverterDriver):
    """Read-only driver for the EyeBond G-command ASCII protocol family."""

    key = "eybond_g_ascii"
    name = "EyeBond G-ASCII"
    probe_timeout = 12.0
    signature_timeout = 4.0
    probe_targets = _EYBOND_G_ASCII_PROBE_TARGETS
    measurements = _MEASUREMENTS
    binary_sensors = ()
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
            variant_key="family_fallback",
            serial_number=serial_number,
            probe_target=target,
            details=details,
            profile_name="",
            register_schema_name="eybond_g_ascii/base.json",
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

    for spec in _support_command_specs():
        command = str(spec.get("command") or "")
        source = str(spec.get("source") or "")
        description = str(spec.get("description") or "")
        known_fields = list(spec.get("fields") or [])
        try:
            raw_response = await session.request_raw(command)
        except Exception as exc:
            failures[command] = str(exc)
            command_results.append(
                {
                    "command": command,
                    "source": source,
                    "description": description,
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue

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
            "raw_response_ascii": raw_ascii,
            "raw_response_hex": raw_response.hex(),
            "parsed_payload": parsed_payload,
            "field_count": len(fields),
            "response_kind": _support_response_kind(parsed_payload),
            "known_fields": known_fields,
            "known_field_count": len(known_fields),
            "unknown_field_count": max(0, len(fields) - len(known_field_indexes & set(range(len(fields))))),
        }
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
            for spec in _support_command_specs()
        ],
        "responses": responses,
        "failures": failures,
        "protocol_probe": {
            "schema_version": 1,
            "command_schema_key": str(_load_command_schema().get("schema_key") or ""),
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

    gmod = await _optional_request(session, "GMOD")
    if gmod:
        mode_code = gmod.strip()
        values["eybond_g_ascii_operating_mode_code"] = mode_code
        values["dcdc_control_status"] = _DCDC_STATUS_BY_MODE.get(mode_code, mode_code)

    gdat0 = await _optional_request(session, "GPDAT0")
    if gdat0:
        fields = parse_space_fields(gdat0)
        values["eybond_g_ascii_gdat0_fields"] = " ".join(fields)
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

        gline = await _optional_request(session, "GLINE")
        if gline:
            fields = parse_space_fields(gline)
            values["eybond_g_ascii_gline_fields"] = " ".join(fields)
            _set_float(values, "grid_voltage", fields, 0)
            _set_float(values, "grid_frequency", fields, 1)
            _set_float(values, "mains_input_voltage", fields, 0)
            _set_float(values, "mains_frequency", fields, 1)
            _set_float(values, "output_load_percentage", fields, 9)
            _set_scaled_float(values, "grid_energy_today", fields, 10, divisor=100.0)
            _set_combined_scaled_counter(values, "grid_energy_total", fields, 11, 12, divisor=100.0)

        gbat = await _optional_request(session, "GBAT")
        if gbat:
            fields = parse_space_fields(gbat)
            values["eybond_g_ascii_gbat_fields"] = " ".join(fields)
            _set_float(values, "battery_voltage", fields, 0)
            _set_float(values, "battery_current", fields, 1)
            _set_float(values, "battery_cell_count", fields, 2)
            _set_float(values, "battery_discharge_cutoff_voltage", fields, 3)
            _set_float(values, "battery_discharge_alarm_voltage", fields, 4)

        gbus = await _optional_request(session, "GBUS")
        if gbus:
            fields = parse_space_fields(gbus)
            _set_float(values, "bus_voltage", fields, 0)

        gchg = await _optional_request(session, "GCHG")
        if gchg:
            fields = parse_space_fields(gchg)
            values["eybond_g_ascii_gchg_fields"] = " ".join(fields)
            _set_float(values, "bus_voltage", fields, 0)
            _set_float(values, "charging_voltage", fields, 1)
            _set_float(values, "battery_cell_count", fields, 2)
            _set_float(values, "charging_current", fields, 3)

        gop = await _optional_request(session, "GOP")
        if gop:
            fields = parse_space_fields(gop)
            values["eybond_g_ascii_gop_fields"] = " ".join(fields)
            _set_float(values, "output_voltage", fields, 0)
            _set_float(values, "output_frequency", fields, 1)
            _set_float(values, "output_current", fields, 2)
            _set_float(values, "output_active_power", fields, 4)
            _set_float(values, "output_apparent_power", fields, 6)
            _set_float(values, "output_load_percentage", fields, 9)
            _set_scaled_float(values, "output_energy_today", fields, 12, divisor=100.0)
            _set_combined_scaled_counter(values, "output_energy_total", fields, 13, 14, divisor=100.0)

        ginv = await _optional_request(session, "GINV")
        if ginv:
            fields = parse_space_fields(ginv)
            _set_float(values, "inverter_voltage", fields, 0)
            _set_float(values, "inverter_frequency", fields, 1)

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

    return values


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


def _set_float_if_absent(
    values: dict[str, Any],
    key: str,
    fields: list[str],
    index: int,
) -> None:
    if key in values:
        return
    _set_float(values, key, fields, index)


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


def _clean_numeric_field(value: object) -> str:
    return str(value).strip().lstrip("#").rstrip(".")
