"""PI30-family inverter driver over EyeBond transport."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..models import (
    DetectedInverter,
    ProbeTarget,
    WriteCapability,
    decimals_for_divisor,
)
from ..payload.pi30 import (
    Pi30Error,
    Pi30Session,
    parse_energy_counter,
    parse_firmware_version,
    parse_model_number,
    parse_protocol_id,
    parse_q1,
    parse_qflag,
    parse_qmod,
    parse_qpigs,
    parse_qpiri,
    parse_qpiws,
    parse_qt_clock,
    parse_serial_number,
)
from ..metadata.pi_family import resolve_pi_identity, resolve_pi30_metadata_names
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from .base import InverterDriver


@dataclass(frozen=True, slots=True)
class Pi30CommandSpec:
    """One PI30 command and its parser."""

    command: str
    parser: Callable[[str], dict[str, Any]]
    optional: bool = False


@dataclass(frozen=True, slots=True)
class Pi30PollGroup:
    """One grouped PI30 polling class with its own cadence."""

    key: str
    specs: tuple[Pi30CommandSpec, ...] = ()
    include_energy: bool = False
    minimum_interval: float = 0.0
    interval_multiplier: float = 1.0

_PROBE_COMMAND_SPECS: tuple[Pi30CommandSpec, ...] = (
    Pi30CommandSpec(command="QPI", parser=parse_protocol_id),
    Pi30CommandSpec(command="QID", parser=parse_serial_number),
    Pi30CommandSpec(command="QPIRI", parser=parse_qpiri),
    Pi30CommandSpec(command="QMN", parser=parse_model_number, optional=True),
    Pi30CommandSpec(command="QFLAG", parser=parse_qflag, optional=True),
    Pi30CommandSpec(command="QMOD", parser=parse_qmod, optional=True),
    Pi30CommandSpec(command="QPIWS", parser=parse_qpiws, optional=True),
    Pi30CommandSpec(
        command="QVFW",
        parser=lambda payload: parse_firmware_version(payload, key="main_cpu_firmware_version"),
        optional=True,
    ),
    Pi30CommandSpec(
        command="QVFW2",
        parser=lambda payload: parse_firmware_version(payload, key="secondary_cpu_firmware_version"),
        optional=True,
    ),
    Pi30CommandSpec(
        command="QVFW3",
        parser=lambda payload: parse_firmware_version(payload, key="tertiary_cpu_firmware_version"),
        optional=True,
    ),
)

_RUNTIME_COMMAND_SPECS: tuple[Pi30CommandSpec, ...] = (
    Pi30CommandSpec(command="QPIGS", parser=parse_qpigs),
    Pi30CommandSpec(command="QMOD", parser=parse_qmod),
    Pi30CommandSpec(command="QPIWS", parser=parse_qpiws, optional=True),
    Pi30CommandSpec(command="Q1", parser=parse_q1, optional=True),
)

_FAST_RUNTIME_COMMAND_SPECS: tuple[Pi30CommandSpec, ...] = (
    Pi30CommandSpec(command="QPIGS", parser=parse_qpigs),
    Pi30CommandSpec(command="QMOD", parser=parse_qmod),
)

_MEDIUM_RUNTIME_COMMAND_SPECS: tuple[Pi30CommandSpec, ...] = (
    Pi30CommandSpec(command="QPIWS", parser=parse_qpiws, optional=True),
    Pi30CommandSpec(command="Q1", parser=parse_q1, optional=True),
)

_PI30_RUNTIME_GROUPS: tuple[Pi30PollGroup, ...] = (
    Pi30PollGroup(key="fast", specs=_FAST_RUNTIME_COMMAND_SPECS, minimum_interval=5.0, interval_multiplier=1.0),
    Pi30PollGroup(key="medium", specs=_MEDIUM_RUNTIME_COMMAND_SPECS, minimum_interval=30.0, interval_multiplier=3.0),
    Pi30PollGroup(key="slow", include_energy=True, minimum_interval=60.0, interval_multiplier=6.0),
)

_SUPPORT_COMMANDS: tuple[str, ...] = tuple(
    dict.fromkeys(
        [
            *(spec.command for spec in _PROBE_COMMAND_SPECS),
            *(spec.command for spec in _RUNTIME_COMMAND_SPECS),
        ]
    )
)

_PI30_BOOL_COMMANDS: dict[str, str] = {
    "buzzer_enabled": "A",
    "overload_bypass_enabled": "B",
    "power_saving_enabled": "J",
    "lcd_reset_to_default_enabled": "K",
    "overload_restart_enabled": "U",
    "over_temperature_restart_enabled": "V",
    "lcd_backlight_enabled": "X",
    "primary_source_interrupt_alarm_enabled": "Y",
    "record_fault_code_enabled": "Z",
}

_PI30_ENUM_COMMANDS: dict[str, str] = {
    "output_source_priority": "POP",
    "charger_source_priority": "PCP",
    "input_voltage_range": "PGR",
    "battery_type": "PBT",
}

_PI30_NUMERIC_COMMANDS: dict[str, str] = {
    "battery_recharge_voltage": "PBCV",
    "battery_redischarge_voltage": "PBDV",
    "battery_under_voltage": "PSDV",
    "battery_bulk_voltage": "PCVV",
    "battery_float_voltage": "PBFT",
}


class Pi30Driver(InverterDriver):
    """PI30 probe, runtime reader, and command-based controller."""

    key = "pi30"
    name = "PI30 / ASCII"
    profile_name = "pi30_ascii/models/default.json"
    register_schema_name = "pi30_ascii/models/default.json"
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

    @property
    def capability_groups(self):
        profile = self.profile_metadata
        return profile.groups if profile is not None else ()

    @property
    def write_capabilities(self):
        profile = self.profile_metadata
        return profile.capabilities if profile is not None else ()

    async def async_probe(self, transport, target: ProbeTarget) -> DetectedInverter | None:
        session = self._session(transport, target)
        try:
            config_values = await _async_collect_values(session, _PROBE_COMMAND_SPECS)
        except Pi30Error:
            return None

        protocol_id = config_values.get("protocol_id")
        if not isinstance(protocol_id, str):
            return None

        identity = resolve_pi_identity(protocol_id, config_values)
        if identity is None or identity.family_key != "pi30":
            return None

        model_name = identity.model_name
        metadata_names = resolve_pi30_metadata_names(
            config_values,
            model_name,
            default_profile_name=self.profile_name,
            default_register_schema_name=self.register_schema_name,
        )
        profile_name = metadata_names.profile_name
        schema_name = metadata_names.register_schema_name
        profile = load_driver_profile(profile_name)
        schema = load_register_schema(schema_name)
        config_values.update(_translate_config_enums(config_values, schema))
        serial_number = config_values.get("serial_number", "")
        if len(serial_number) < 6:
            return None

        return DetectedInverter(
            driver_key=self.key,
            protocol_family="pi30",
            model_name=model_name,
            variant_key=identity.variant_key,
            serial_number=serial_number,
            probe_target=target,
            details=config_values,
            profile_name=profile_name,
            register_schema_name=schema_name,
            capability_groups=profile.groups,
            capabilities=_build_pi30_capabilities(config_values, profile.capabilities),
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
        values = await _async_collect_runtime_values(
            session,
            runtime_state=runtime_state,
            poll_interval=poll_interval,
            now_monotonic=now_monotonic,
        )
        values.update(
            _translate_runtime_metadata(
                values,
                _schema_for_inverter(inverter, self.register_schema_name),
            )
        )
        return values

    async def async_write_capability(
        self,
        transport,
        inverter: DetectedInverter,
        capability_key: str,
        value: Any,
    ) -> Any:
        capability = _find_capability(capability_key, inverter.capabilities or self.write_capabilities)
        raw_value = _encode_capability_value(capability, value)
        command = _build_write_command(capability, raw_value)

        session = self._session(transport, inverter.probe_target)
        response = await session.request(command)
        if response != "ACK":
            raise RuntimeError(f"unexpected_write_response:{capability.key}:{response}")

        written_value = _decode_capability_value(capability, raw_value)
        inverter.details[capability.value_key] = written_value
        if capability.key != capability.value_key:
            inverter.details[capability.key] = written_value
        return written_value

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
            "capture_kind": "pi30_ascii_dump",
            "driver_key": self.key,
            "model_name": inverter.model_name,
            "serial_number": inverter.serial_number,
            "responses": responses,
            "failures": failures,
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> Pi30Session:
        return Pi30Session(
            transport,
            route=target.link_route,
        )


def _build_model_name(protocol_id: str, values: dict[str, Any]) -> str:
    from ..metadata.pi_family import build_pi_model_name

    return build_pi_model_name(protocol_id, values)


def _schema_for_inverter(
    inverter: DetectedInverter | None,
    fallback_schema_name: str,
):
    schema_name = fallback_schema_name
    if inverter is not None and inverter.register_schema_name:
        schema_name = inverter.register_schema_name
    return load_register_schema(schema_name)


def _resolve_pi30_schema_name(values: dict[str, Any], model_name: str) -> str:
    return resolve_pi30_metadata_names(
        values,
        model_name,
        default_profile_name=Pi30Driver.profile_name,
        default_register_schema_name=Pi30Driver.register_schema_name,
    ).register_schema_name


def _resolve_pi30_profile_name(values: dict[str, Any], model_name: str) -> str:
    return resolve_pi30_metadata_names(
        values,
        model_name,
        default_profile_name=Pi30Driver.profile_name,
        default_register_schema_name=Pi30Driver.register_schema_name,
    ).profile_name


async def _async_collect_values(
    session: Pi30Session,
    specs: tuple[Pi30CommandSpec, ...],
) -> dict[str, Any]:
    values: dict[str, Any] = {}

    for spec in specs:
        try:
            payload = await session.request(spec.command)
            values.update(spec.parser(payload))
        except Pi30Error:
            if not spec.optional:
                raise

    return values


def _group_interval(group: Pi30PollGroup, poll_interval: float) -> float:
    """Resolve one effective group interval from the base poll interval."""

    return max(group.minimum_interval, poll_interval * group.interval_multiplier)


def _should_poll_group(
    runtime_state: dict[str, Any],
    group: Pi30PollGroup,
    *,
    poll_interval: float,
    now_monotonic: float,
) -> bool:
    """Return whether one PI30 polling group is due."""

    group_times = runtime_state.setdefault("pi30_group_last_polled", {})
    last_polled = group_times.get(group.key)
    if last_polled is None:
        group_times[group.key] = now_monotonic
        return True
    if now_monotonic - float(last_polled) >= _group_interval(group, poll_interval):
        group_times[group.key] = now_monotonic
        return True
    return False


async def _async_collect_runtime_values(
    session: Pi30Session,
    *,
    runtime_state: dict[str, Any] | None,
    poll_interval: float | None,
    now_monotonic: float | None,
) -> dict[str, Any]:
    """Collect PI30 runtime values using grouped polling when configured."""

    if runtime_state is None or poll_interval is None or now_monotonic is None:
        values = await _async_collect_values(session, _RUNTIME_COMMAND_SPECS)
        values.update(await _async_collect_energy_values(session))
        return values

    values: dict[str, Any] = {}
    for group in _PI30_RUNTIME_GROUPS:
        if not _should_poll_group(
            runtime_state,
            group,
            poll_interval=poll_interval,
            now_monotonic=now_monotonic,
        ):
            continue
        if group.specs:
            values.update(await _async_collect_values(session, group.specs))
        if group.include_energy:
            values.update(await _async_collect_energy_values(session))
    return values


async def _async_collect_energy_values(session: Pi30Session) -> dict[str, Any]:
    values: dict[str, Any] = {}

    try:
        values.update(parse_energy_counter(await session.request("QET"), key="pv_generation_sum"))
    except Pi30Error:
        return values

    try:
        values.update(parse_energy_counter(await session.request("QLT"), key="ac_in_generation_sum"))
    except Pi30Error:
        pass

    try:
        clock_token = parse_qt_clock(await session.request("QT"))["clock_token"]
    except Pi30Error:
        return values

    dynamic_specs = (
        (f"QEY{clock_token[:4]}", "pv_generation_year"),
        (f"QEM{clock_token[:6]}", "pv_generation_month"),
        (f"QED{clock_token[:8]}", "pv_generation_day"),
        (f"QLY{clock_token[:4]}", "ac_in_generation_year"),
        (f"QLM{clock_token[:6]}", "ac_in_generation_month"),
        (f"QLD{clock_token[:8]}", "ac_in_generation_day"),
    )
    for command, key in dynamic_specs:
        try:
            values.update(parse_energy_counter(await session.request(command), key=key))
        except Pi30Error:
            continue

    return values


async def _async_capture_energy_support(
    session: Pi30Session,
    responses: dict[str, str],
    failures: dict[str, str],
) -> None:
    for command in ("QET", "QLT", "QT"):
        try:
            responses[command] = await session.request(command)
        except Exception as exc:
            failures[command] = str(exc)

    qt_payload = responses.get("QT")
    if not qt_payload:
        return

    try:
        clock_token = parse_qt_clock(qt_payload)["clock_token"]
    except Pi30Error as exc:
        failures["QT"] = str(exc)
        return

    for command in (
        f"QEY{clock_token[:4]}",
        f"QEM{clock_token[:6]}",
        f"QED{clock_token[:8]}",
        f"QLY{clock_token[:4]}",
        f"QLM{clock_token[:6]}",
        f"QLD{clock_token[:8]}",
    ):
        try:
            responses[command] = await session.request(command)
        except Exception as exc:
            failures[command] = str(exc)


def _translate_config_enums(values: dict[str, Any], schema) -> dict[str, Any]:
    translated: dict[str, Any] = {}

    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="battery_type",
        code_key="battery_type_code",
        enum_table="battery_type_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="input_voltage_range",
        code_key="input_voltage_range_code",
        enum_table="input_voltage_range_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="output_source_priority",
        code_key="output_source_priority_code",
        enum_table="output_source_priority_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="charger_source_priority",
        code_key="charger_source_priority_code",
        enum_table="charger_source_priority_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="machine_type",
        code_key="machine_type_code",
        enum_table="machine_type_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="topology",
        code_key="topology_code",
        enum_table="topology_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="output_mode",
        code_key="output_mode_code",
        enum_table="output_mode_names",
    )
    _translate_config_enum(
        translated,
        values,
        schema,
        value_key="operation_logic",
        code_key="operation_logic_code",
        enum_table="operation_logic_names",
    )

    return translated


def _translate_config_enum(
    translated: dict[str, Any],
    values: dict[str, Any],
    schema,
    *,
    value_key: str,
    code_key: str,
    enum_table: str,
) -> None:
    raw_code = values.get(code_key)
    if not isinstance(raw_code, int) or schema is None:
        return

    translated[value_key] = schema.enum_map_for(enum_table).get(raw_code, f"Unknown ({raw_code})")


def _translate_runtime_metadata(values: dict[str, Any], schema) -> dict[str, Any]:
    translated: dict[str, Any] = {}

    operating_mode_code = values.get("operating_mode_code")
    if isinstance(operating_mode_code, str) and schema is not None:
        translated["operating_mode"] = schema.enum_map_for("operating_mode_names").get(
            operating_mode_code,
            f"Unknown ({operating_mode_code})",
        )

    alarm_bits = values.get("alarm_bits_raw")
    if isinstance(alarm_bits, str):
        translated["alarm_status"] = _format_alarm_status(alarm_bits, schema)

    charge_state_code = values.get("inverter_charge_state_code")
    if isinstance(charge_state_code, int) and schema is not None:
        translated["inverter_charge_state"] = schema.enum_map_for("inverter_charge_state_names").get(
            charge_state_code,
            f"Unknown ({charge_state_code})",
        )

    return translated


def _format_alarm_status(alarm_bits: str, schema) -> str:
    if not any(bit == "1" for bit in alarm_bits):
        return "Ok"
    if schema is None:
        return "Unknown"

    labels = schema.bit_labels_for("alarm_status_names")
    active_labels = [
        label
        for index, label in labels.items()
        if index < len(alarm_bits) and alarm_bits[index] == "1"
    ]
    active_labels.extend(
        f"Unknown alarm bit {index}"
        for index, bit in enumerate(alarm_bits)
        if bit == "1" and index not in labels
    )
    return "; ".join(active_labels) if active_labels else "Ok"


def _build_pi30_capabilities(
    values: dict[str, Any],
    capabilities: tuple[WriteCapability, ...],
) -> tuple[WriteCapability, ...]:
    battery_rating_voltage = values.get("battery_rating_voltage")
    if not isinstance(battery_rating_voltage, (int, float)) or battery_rating_voltage <= 0:
        return capabilities

    scale = float(battery_rating_voltage) / 48.0
    scaled: list[WriteCapability] = []
    for capability in capabilities:
        if capability.key == "battery_recharge_voltage":
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=440, maximum=510)
            )
            continue
        if capability.key == "battery_redischarge_voltage":
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=0, maximum=580)
            )
            continue
        if capability.key == "battery_under_voltage":
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=400, maximum=480)
            )
            continue
        if capability.key in {"battery_bulk_voltage", "battery_float_voltage"}:
            scaled.append(
                _replace_voltage_range(capability, scale=scale, minimum=480, maximum=584)
            )
            continue
        scaled.append(capability)
    return tuple(scaled)


def _replace_voltage_range(
    capability: WriteCapability,
    *,
    scale: float,
    minimum: int,
    maximum: int,
) -> WriteCapability:
    raw_minimum = int(round(minimum * scale))
    raw_maximum = int(round(maximum * scale))
    return WriteCapability(
        key=capability.key,
        register=capability.register,
        value_kind=capability.value_kind,
        note=capability.note,
        tested=capability.tested,
        support_tier=capability.support_tier,
        support_notes=capability.support_notes,
        action_value=capability.action_value,
        divisor=capability.divisor,
        minimum=raw_minimum,
        maximum=raw_maximum,
        enum_map=capability.enum_map,
        choices=capability.choices,
        recommendations=capability.recommendations,
        title=capability.title,
        group=capability.group,
        order=capability.order,
        unit=capability.unit,
        device_class=capability.device_class,
        step=capability.step,
        enabled_default=capability.enabled_default,
        advanced=capability.advanced,
        requires_confirm=capability.requires_confirm,
        reboot_required=capability.reboot_required,
        read_key=capability.read_key,
        depends_on=capability.depends_on,
        affects=capability.affects,
        exclusive_with=capability.exclusive_with,
        change_summary=capability.change_summary,
        unsafe_while_running=capability.unsafe_while_running,
        safe_operating_modes=capability.safe_operating_modes,
        visible_if=capability.visible_if,
        editable_if=capability.editable_if,
    )


def _find_capability(
    capability_key: str,
    capabilities: tuple[WriteCapability, ...],
) -> WriteCapability:
    for capability in capabilities:
        if capability.key == capability_key:
            return capability
    raise ValueError(f"unsupported_capability:{capability_key}")


def _encode_capability_value(capability: WriteCapability, value: Any) -> int:
    if capability.value_kind == "bool":
        return _encode_bool_value(capability, value)
    if capability.value_kind == "enum":
        return _encode_enum_value(capability, value)
    if capability.value_kind == "scaled_u16":
        return _encode_scaled_u16_value(capability, value)
    if capability.value_kind == "u16":
        return _encode_u16_value(capability, value)
    raise ValueError(f"unsupported_value_kind:{capability.value_kind}")


def _decode_capability_value(capability: WriteCapability, raw_value: int) -> Any:
    enum_map = capability.enum_value_map
    if capability.value_kind == "bool":
        if enum_map:
            return enum_map.get(raw_value, bool(raw_value))
        return bool(raw_value)
    if enum_map:
        return enum_map.get(raw_value, f"Unknown ({raw_value})")
    if capability.divisor:
        return round(raw_value / capability.divisor, decimals_for_divisor(capability.divisor))
    return raw_value


def _encode_enum_value(capability: WriteCapability, value: Any) -> int:
    enum_map = capability.enum_value_map
    if isinstance(value, int):
        raw_value = value
    else:
        text = str(value).strip()
        if text.isdigit():
            raw_value = int(text)
        else:
            reverse_map = {label: key for key, label in enum_map.items()}
            if text not in reverse_map:
                raise ValueError(f"unsupported_enum_value:{capability.key}:{text}")
            raw_value = reverse_map[text]

    if raw_value not in enum_map:
        raise ValueError(f"unsupported_enum_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_bool_value(capability: WriteCapability, value: Any) -> int:
    if isinstance(value, bool):
        raw_value = 1 if value else 0
    elif isinstance(value, int):
        raw_value = value
    else:
        text = str(value).strip().lower()
        truthy = {"1", "true", "on", "yes", "enable", "enabled"}
        falsy = {"0", "false", "off", "no", "disable", "disabled"}
        if text in truthy:
            raw_value = 1
        elif text in falsy:
            raw_value = 0
        else:
            raise ValueError(f"unsupported_bool_value:{capability.key}:{value}")

    if raw_value not in {0, 1}:
        raise ValueError(f"unsupported_bool_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_scaled_u16_value(capability: WriteCapability, value: Any) -> int:
    if capability.divisor is None:
        raise ValueError(f"missing_divisor:{capability.key}")

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_numeric_value:{capability.key}:{value}") from exc

    raw_value = int(round(numeric * capability.divisor))
    _validate_range(capability, raw_value)
    return raw_value


def _encode_u16_value(capability: WriteCapability, value: Any) -> int:
    try:
        raw_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_integer_value:{capability.key}:{value}") from exc

    _validate_range(capability, raw_value)
    return raw_value


def _validate_range(capability: WriteCapability, raw_value: int) -> None:
    if capability.minimum is not None and raw_value < capability.minimum:
        raise ValueError(f"value_below_minimum:{capability.key}:{raw_value}")
    if capability.maximum is not None and raw_value > capability.maximum:
        raise ValueError(f"value_above_maximum:{capability.key}:{raw_value}")


def _build_write_command(capability: WriteCapability, raw_value: int) -> str:
    if capability.key in _PI30_BOOL_COMMANDS:
        prefix = "PE" if raw_value else "PD"
        return f"{prefix}{_PI30_BOOL_COMMANDS[capability.key]}"
    if capability.key in _PI30_ENUM_COMMANDS:
        return f"{_PI30_ENUM_COMMANDS[capability.key]}{raw_value:02d}"
    if capability.key in _PI30_NUMERIC_COMMANDS:
        return f"{_PI30_NUMERIC_COMMANDS[capability.key]}{_format_scaled_value(capability, raw_value)}"
    raise ValueError(f"unsupported_write_command:{capability.key}")


def _format_scaled_value(capability: WriteCapability, raw_value: int) -> str:
    if capability.divisor is None:
        return str(raw_value)
    return f"{raw_value / capability.divisor:.{decimals_for_divisor(capability.divisor)}f}"
