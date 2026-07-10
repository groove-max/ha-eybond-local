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
from ..metadata.compiled_detection_catalog import load_compiled_detection_catalog
from ..metadata.register_schema_loader import load_register_schema
from .base import InverterDriver
from .command_support import (
    apply_unsupported_diagnostics,
    command_skipped_as_unsupported,
    commit_cycle_failures,
    record_command_failure,
    record_command_success,
)
from .catalog_probe import (
    async_probe_ascii_catalog,
    catalog_model_name,
    evidence_providers_from_transport,
)


@dataclass(frozen=True, slots=True)
class Pi18CommandSpec:
    command: str
    parser: Callable[[str], dict[str, Any]]
    optional: bool = False


_RUNTIME_COMMAND_SPECS: tuple[Pi18CommandSpec, ...] = (
    Pi18CommandSpec(command="^P005GS", parser=parse_qpigs),
    Pi18CommandSpec(command="^P006MOD", parser=parse_qmod),
    Pi18CommandSpec(command="^P007FLAG", parser=parse_qflag, optional=True),
    Pi18CommandSpec(command="^P005FWS", parser=parse_qfws, optional=True),
)

_CATALOG_PARSERS = {
    "pi18.protocol_id": parse_protocol_id,
    "pi18.serial_number": parse_serial_number,
    "pi18.qpiri": parse_qpiri,
    "pi18.firmware_versions": parse_firmware_versions,
}


class Pi18Driver(InverterDriver):
    """Read-only PI18 family driver."""

    key = "pi18"
    name = "PI18 / ASCII"

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
    def profile_name(self) -> str:
        return _pi18_default_binding().profile_name

    @property
    def register_schema_name(self) -> str:
        return _pi18_default_binding().register_schema_name

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
            probe = await async_probe_ascii_catalog(
                protocol_key="pi18",
                session=session,
                parsers=_CATALOG_PARSERS,
                collector=getattr(transport, "collector_info", None),
                evidence_providers=evidence_providers_from_transport(transport),
            )
        except (Pi18Error, RuntimeError):
            return None
        if not probe.resolution.resolved:
            return None
        values = probe.values
        values["catalog_detection"] = probe.as_details()
        serial_number = values.get("serial_number", "")
        if len(serial_number) < 6:
            return None

        schema = load_register_schema(self.register_schema_name)
        values.update(_translate_config_enums(values, schema))
        model_name = catalog_model_name(
            protocol_key="pi18",
            resolution=probe.resolution,
            values=values,
        )
        surface = load_compiled_detection_catalog().surfaces[
            probe.resolution.surface_key
        ]
        return DetectedInverter(
            driver_key=self.key,
            protocol_family="pi18",
            model_name=model_name,
            variant_key=surface.variant_key,
            serial_number=serial_number,
            probe_target=target,
            details=values,
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
        session = self._session(transport, inverter.probe_target)
        values = await _async_collect_values(
            session,
            _RUNTIME_COMMAND_SPECS,
            runtime_state=runtime_state,
        )
        values.update(_translate_runtime_enums(values, load_register_schema(inverter.register_schema_name or self.register_schema_name)))
        values.update(
            await _async_collect_energy_values(session, runtime_state=runtime_state)
        )
        commit_cycle_failures(runtime_state)
        apply_unsupported_diagnostics(values, runtime_state)
        return values

    async def async_write_capability(self, transport, inverter: DetectedInverter, capability_key: str, value: Any) -> Any:
        raise ValueError(f"unsupported_capability:{self.key}:{capability_key}")

    async def async_capture_support_evidence(self, transport, inverter: DetectedInverter) -> dict[str, Any]:
        session = self._session(transport, inverter.probe_target)
        responses: dict[str, str] = {}
        failures: dict[str, str] = {}
        for command in _support_commands():
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


async def _async_collect_values(
    session: Pi18Session,
    specs: tuple[Pi18CommandSpec, ...],
    *,
    runtime_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for spec in specs:
        if spec.optional and command_skipped_as_unsupported(runtime_state, spec.command):
            continue
        try:
            values.update(spec.parser(await session.request(spec.command)))
        except Pi18Error:
            if not spec.optional:
                raise
            record_command_failure(runtime_state, spec.command)
        else:
            record_command_success(runtime_state, spec.command)
    return values


async def _async_collect_energy_values(
    session: Pi18Session,
    *,
    runtime_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {}

    async def _request(command: str, cache_key: str) -> str | None:
        # Dated commands are cached under a stable prefix key.
        if command_skipped_as_unsupported(runtime_state, cache_key):
            return None
        try:
            payload = await session.request(command)
        except Pi18Error:
            record_command_failure(runtime_state, cache_key)
            return None
        record_command_success(runtime_state, cache_key)
        return payload

    payload = await _request("^P005ET", "^P005ET")
    if payload is None:
        return values
    values.update(parse_energy_counter(payload, key="pv_generation_sum"))

    payload = await _request("^P004T", "^P004T")
    if payload is None:
        return values
    try:
        clock_token = parse_current_time(payload)["clock_token"]
    except Pi18Error:
        return values

    dynamic_specs = (
        (f"^P009EY{clock_token[:4]}", "^P009EY", "pv_generation_year"),
        (f"^P011EM{clock_token[:6]}", "^P011EM", "pv_generation_month"),
        (f"^P013ED{clock_token[:8]}", "^P013ED", "pv_generation_day"),
    )
    for command, cache_key, key in dynamic_specs:
        payload = await _request(command, cache_key)
        if payload is None:
            continue
        try:
            values.update(parse_energy_counter(payload, key=key))
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


def _pi18_default_binding():
    surfaces = tuple(
        surface
        for surface in load_compiled_detection_catalog().surfaces.values()
        if surface.driver_key == "pi18" and surface.default_for_driver
    )
    if len(surfaces) != 1:
        raise RuntimeError("missing_default_surface:pi18")
    return surfaces[0]


def _support_commands() -> tuple[str, ...]:
    protocol = load_compiled_detection_catalog().protocols["pi18"]
    return tuple(
        dict.fromkeys(
            [
                *(
                    action.command
                    for action in protocol.probe_actions
                    if action.kind == "ascii_command" and action.command
                ),
                *(spec.command for spec in _RUNTIME_COMMAND_SPECS),
            ]
        )
    )


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
