"""Experimental PI18 inverter driver over EyeBond transport."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..models import DetectedInverter, ProbeTarget
from ..payload.pi18 import (
    Pi18Error,
    Pi18Session,
    parse_current_time,
    parse_energy_counter,
    parse_firmware_versions,
    parse_protocol_id,
    parse_qflag,
    parse_qfws,
    parse_qmod,
    parse_qpigs,
    parse_qpiri,
    parse_serial_number,
)
from ..metadata.register_schema_loader import load_register_schema
from .base import InverterDriver


@dataclass(frozen=True, slots=True)
class Pi18CommandSpec:
    command: str
    parser: Callable[[str], dict[str, Any]]
    optional: bool = False


_PROBE_COMMAND_SPECS: tuple[Pi18CommandSpec, ...] = (
    Pi18CommandSpec(command="^P005PI", parser=parse_protocol_id),
    Pi18CommandSpec(command="^P005ID", parser=parse_serial_number),
    Pi18CommandSpec(command="^P007PIRI", parser=parse_qpiri),
    Pi18CommandSpec(command="^P006VFW", parser=parse_firmware_versions, optional=True),
)

_RUNTIME_COMMAND_SPECS: tuple[Pi18CommandSpec, ...] = (
    Pi18CommandSpec(command="^P005GS", parser=parse_qpigs),
    Pi18CommandSpec(command="^P006MOD", parser=parse_qmod),
    Pi18CommandSpec(command="^P007FLAG", parser=parse_qflag, optional=True),
    Pi18CommandSpec(command="^P005FWS", parser=parse_qfws, optional=True),
)

_SUPPORT_COMMANDS: tuple[str, ...] = tuple(
    dict.fromkeys([*(spec.command for spec in _PROBE_COMMAND_SPECS), *(spec.command for spec in _RUNTIME_COMMAND_SPECS)])
)


class Pi18Driver(InverterDriver):
    """Experimental read-only PI18 family driver."""

    key = "pi18"
    name = "PI18 / Experimental"
    register_schema_name = "pi18_ascii/base.json"
    probe_targets = (
        ProbeTarget(devcode=0x0994, collector_addr=0x01, device_addr=0),
        ProbeTarget(devcode=0x0994, collector_addr=0xFF, device_addr=0),
        ProbeTarget(devcode=0x0102, collector_addr=0xFF, device_addr=0),
    )

    @property
    def measurements(self):
        schema = self.register_schema_metadata
        return schema.measurement_descriptions if schema is not None else ()

    @property
    def binary_sensors(self):
        schema = self.register_schema_metadata
        return schema.binary_sensor_descriptions if schema is not None else ()

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        session = self._session(transport, target)
        try:
            values = await _async_collect_values(session, _PROBE_COMMAND_SPECS)
        except Pi18Error:
            return None

        if values.get("protocol_id") != "PI18":
            return None

        serial_number = values.get("serial_number", "")
        if len(serial_number) < 6:
            return None

        schema = load_register_schema(self.register_schema_name)
        values.update(_translate_config_enums(values, schema))
        model_name = _build_model_name(values)
        return DetectedInverter(
            driver_key=self.key,
            protocol_family="pi18",
            model_name=model_name,
            serial_number=serial_number,
            probe_target=target,
            details=values,
            register_schema_name=self.register_schema_name,
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
        session = self._session(transport, inverter.probe_target)
        values = await _async_collect_values(session, _RUNTIME_COMMAND_SPECS)
        values.update(_translate_runtime_enums(values, load_register_schema(inverter.register_schema_name or self.register_schema_name)))
        values.update(await _async_collect_energy_values(session))
        return values

    async def async_write_capability(self, transport, inverter: DetectedInverter, capability_key: str, value: Any) -> Any:
        raise ValueError(f"unsupported_capability:{self.key}:{capability_key}")

    async def async_capture_support_evidence(self, transport, inverter: DetectedInverter) -> dict[str, Any]:
        session = self._session(transport, inverter.probe_target)
        responses: dict[str, str] = {}
        failures: dict[str, str] = {}
        for command in _SUPPORT_COMMANDS:
            try:
                responses[command] = await session.request(command)
            except Exception as exc:
                failures[command] = str(exc)

        await _async_capture_energy_support(session, responses, failures)
        return {
            "capture_kind": "pi18_experimental_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "responses": responses,
            "failures": failures,
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> Pi18Session:
        return Pi18Session(
            transport,
            route=target.link_route,
        )


async def _async_collect_values(session: Pi18Session, specs: tuple[Pi18CommandSpec, ...]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for spec in specs:
        try:
            values.update(spec.parser(await session.request(spec.command)))
        except Pi18Error:
            if not spec.optional:
                raise
    return values


async def _async_collect_energy_values(session: Pi18Session) -> dict[str, Any]:
    values: dict[str, Any] = {}
    try:
        values.update(parse_energy_counter(await session.request("^P005ET"), key="pv_generation_sum"))
    except Pi18Error:
        return values

    try:
        clock_token = parse_current_time(await session.request("^P004T"))["clock_token"]
    except Pi18Error:
        return values

    dynamic_specs = (
        (f"^P009EY{clock_token[:4]}", "pv_generation_year"),
        (f"^P011EM{clock_token[:6]}", "pv_generation_month"),
        (f"^P013ED{clock_token[:8]}", "pv_generation_day"),
    )
    for command, key in dynamic_specs:
        try:
            values.update(parse_energy_counter(await session.request(command), key=key))
        except Pi18Error:
            continue
    return values


async def _async_capture_energy_support(session: Pi18Session, responses: dict[str, str], failures: dict[str, str]) -> None:
    for command in ("^P005ET", "^P004T"):
        try:
            responses[command] = await session.request(command)
        except Exception as exc:
            failures[command] = str(exc)
    current_time = responses.get("^P004T")
    if not current_time:
        return
    try:
        clock_token = parse_current_time(current_time)["clock_token"]
    except Pi18Error as exc:
        failures["^P004T"] = str(exc)
        return
    for command in (f"^P009EY{clock_token[:4]}", f"^P011EM{clock_token[:6]}", f"^P013ED{clock_token[:8]}"):
        try:
            responses[command] = await session.request(command)
        except Exception as exc:
            failures[command] = str(exc)


def _build_model_name(values: dict[str, Any]) -> str:
    rated_power = values.get("output_rating_active_power")
    if isinstance(rated_power, int) and rated_power > 0:
        return f"PI18 {rated_power}"
    return "PI18"


def _translate_config_enums(values: dict[str, Any], schema) -> dict[str, Any]:
    translated: dict[str, Any] = {}
    for value_key, code_key, enum_table in (
        ("battery_type", "battery_type_code", "battery_type_names"),
        ("input_voltage_range", "input_voltage_range_code", "input_voltage_range_names"),
        ("output_source_priority", "output_source_priority_code", "output_source_priority_names"),
        ("charger_source_priority", "charger_source_priority_code", "charger_source_priority_names"),
        ("machine_type", "machine_type_code", "machine_type_names"),
        ("topology", "topology_code", "topology_names"),
        ("output_mode", "output_mode_code", "output_mode_names"),
        ("solar_power_priority", "solar_power_priority_code", "solar_power_priority_names"),
    ):
        raw = values.get(code_key)
        if isinstance(raw, int):
            translated[value_key] = schema.enum_map_for(enum_table).get(raw, f"Unknown ({raw})")
    return translated


def _translate_runtime_enums(values: dict[str, Any], schema) -> dict[str, Any]:
    translated: dict[str, Any] = {}
    for value_key, code_key, enum_table in (
        ("operating_mode", "operating_mode_code", "operating_mode_names"),
        ("configuration_state", "configuration_state_code", "configuration_state_names"),
        ("mppt1_charger_status", "mppt1_charger_status_code", "charger_status_names"),
        ("mppt2_charger_status", "mppt2_charger_status_code", "charger_status_names"),
        ("load_connection", "load_connection_code", "load_connection_names"),
        ("battery_power_direction", "battery_power_direction_code", "power_direction_names"),
        ("dc_ac_power_direction", "dc_ac_power_direction_code", "dc_ac_power_direction_names"),
        ("line_power_direction", "line_power_direction_code", "line_power_direction_names"),
    ):
        raw = values.get(code_key)
        if isinstance(raw, int):
            translated[value_key] = schema.enum_map_for(enum_table).get(raw, f"Unknown ({raw})")
    return translated
