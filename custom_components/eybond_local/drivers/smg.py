"""SMG-family inverter driver over EyeBond transport + Modbus RTU."""

from __future__ import annotations

from typing import Any

from ..const import DEFAULT_COLLECTOR_ADDR, DEFAULT_MODBUS_DEVICE_ADDR
from ..models import (
    DetectedInverter,
    ProbeTarget,
    RegisterValueSpec,
    WriteCapability,
    decimals_for_divisor,
)
from ..payload.modbus import ModbusError, ModbusSession, to_signed_16
from ..metadata.model_binding_catalog_loader import (
    load_driver_model_binding_catalog,
    resolve_driver_model_binding,
)
from ..metadata.profile_loader import load_driver_profile
from ..metadata.register_schema_loader import load_register_schema
from .base import InverterDriver


_SMG_VARIANT_MODEL_NAMES = {
    "anenji_anj_11kw_48v_wifi_p": "Anenji ANJ-11KW-48V-WIFI-P",
}


class SmgModbusDriver(InverterDriver):
    """Bench-safe SMG probe and runtime reader."""

    key = "modbus_smg"
    name = "SMG / Modbus"
    probe_targets = (
        ProbeTarget(
            devcode=0x0001,
            collector_addr=DEFAULT_COLLECTOR_ADDR,
            device_addr=DEFAULT_MODBUS_DEVICE_ADDR,
        ),
    )

    @property
    def profile_name(self) -> str:
        return _smg_default_binding().profile_name

    @property
    def register_schema_name(self) -> str:
        return _smg_default_binding().register_schema_name

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

    @property
    def capability_presets(self):
        profile = self.profile_metadata
        return profile.presets if profile is not None else ()

    async def async_probe(
        self,
        transport,
        target: ProbeTarget,
    ) -> DetectedInverter | None:
        session = self._session(transport, target)
        for binding in _smg_bindings():
            try:
                schema = load_register_schema(binding.register_schema_name)
            except Exception:
                continue

            try:
                profile = load_driver_profile(binding.profile_name) if binding.profile_name else None
            except Exception:
                continue

            try:
                serial_block = await session.read_holding(
                    schema.block("serial").start,
                    schema.block("serial").count,
                )
                serial_number = _decode_ascii_words(serial_block)
                if len(serial_number) < 6:
                    continue

                live_block = await session.read_holding(
                    schema.block("live").start,
                    schema.block("live").count,
                )
                live_values = _decode_block(
                    schema.block("live").start,
                    live_block,
                    _specs_for_block(
                        schema.spec_set("live"),
                        schema.block("live").start,
                        schema.block("live").count,
                    ),
                )

                config_block = await session.read_holding(
                    schema.block("config").start,
                    schema.block("config").count,
                )
                config_values = _decode_block(
                    schema.block("config").start,
                    config_block,
                    _specs_for_block(
                        schema.spec_set("config"),
                        schema.block("config").start,
                        schema.block("config").count,
                    ),
                )
                config_values.update(await _read_optional_specs(session, schema.spec_set("aux_config")))
            except Exception:
                continue

            if not _is_valid_smg_probe(schema, live_values, config_values, binding.variant_key):
                continue

            rated_power = await _read_rated_power(session, schema)
            details = dict(config_values)
            if rated_power:
                details["rated_power"] = rated_power

            return DetectedInverter(
                driver_key=self.key,
                protocol_family="modbus_smg",
                model_name=_smg_model_name(binding.variant_key, rated_power),
                serial_number=serial_number,
                probe_target=target,
                variant_key=binding.variant_key,
                details=details,
                profile_name=binding.profile_name,
                register_schema_name=binding.register_schema_name,
                capability_groups=profile.groups if profile is not None else (),
                capabilities=profile.capabilities if profile is not None else (),
                capability_presets=profile.presets if profile is not None else (),
            )

        return None

    async def async_read_values(
        self,
        transport,
        inverter: DetectedInverter,
        *,
        runtime_state: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        now_monotonic: float | None = None,
    ) -> dict[str, Any]:
        schema = _schema_for_inverter(inverter, self.register_schema_name)
        if schema is None:
            return {}
        status_block_start = schema.block("status").start
        status_block_count = schema.block("status").count
        live_block_start = schema.block("live").start
        live_block_count = schema.block("live").count
        config_block_start = schema.block("config").start
        config_block_count = schema.block("config").count
        status_fields = _specs_for_block(schema.spec_set("status"), status_block_start, status_block_count)
        live_fields = _specs_for_block(schema.spec_set("live"), live_block_start, live_block_count)
        config_fields = _specs_for_block(schema.spec_set("config"), config_block_start, config_block_count)
        aux_config_fields = schema.spec_set("aux_config")
        fault_code_names = schema.bit_labels_for("fault_code_names")
        warning_code_names = schema.bit_labels_for("warning_code_names")

        session = self._session(transport, inverter.probe_target)

        status_block = await session.read_holding(status_block_start, status_block_count)
        status_values = _decode_block(status_block_start, status_block, status_fields)

        live_block = await session.read_holding(live_block_start, live_block_count)
        values = _decode_block(live_block_start, live_block, live_fields)
        values.update(status_values)

        config_block = await session.read_holding(config_block_start, config_block_count)
        values.update(_decode_block(config_block_start, config_block, config_fields))
        values.update(await _read_optional_specs(session, aux_config_fields))

        fault_descriptions = _decode_named_bits(
            values.get("fault_code"),
            fault_code_names,
            one_based=True,
            include_unknown=True,
            unknown_label_prefix="Unknown Fault Bit",
        )
        warning_descriptions = _decode_named_bits(
            values.get("warning_code"),
            warning_code_names,
            include_unknown=True,
            unknown_label_prefix="Unknown Warning Bit",
        )
        values["fault_count"] = len(fault_descriptions)
        values["warning_count"] = len(warning_descriptions)
        values["fault_descriptions"] = ", ".join(fault_descriptions) if fault_descriptions else "None"
        values["warning_descriptions"] = ", ".join(warning_descriptions) if warning_descriptions else "None"

        warning_code = values.get("warning_code")
        battery_present = not (isinstance(warning_code, int) and bool(warning_code & (1 << 9)))
        values["battery_connected"] = battery_present
        values["battery_connection_state"] = "Connected" if battery_present else "Not Connected"
        if not battery_present:
            values.pop("battery_voltage", None)
            values.pop("battery_percent", None)
            values.pop("battery_current", None)
            values.pop("battery_average_current", None)
            values.pop("battery_average_power", None)

        battery_percent = values.get("battery_percent")
        if isinstance(battery_percent, int) and not (0 <= battery_percent <= 100):
            values.pop("battery_percent", None)

        battery_voltage = values.get("battery_voltage")
        if isinstance(battery_voltage, (int, float)) and battery_voltage <= 0:
            values.pop("battery_voltage", None)

        values.update(_derive_runtime_states(values))
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

        session = self._session(transport, inverter.probe_target)
        await session.write_holding(capability.register, [raw_value])

        native_value = _decode_capability_value(capability, raw_value)
        inverter.details[capability.key] = native_value
        return native_value

    async def async_capture_support_evidence(
        self,
        transport,
        inverter: DetectedInverter,
    ) -> dict[str, Any]:
        """Capture raw SMG register evidence for support packages."""

        session = self._session(transport, inverter.probe_target)
        schema_name = inverter.register_schema_name or self.register_schema_name
        ranges = _support_capture_ranges(schema_name)
        captured_ranges: list[dict[str, Any]] = []
        fixture_ranges: list[dict[str, Any]] = []
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

            formatted = _format_support_range(start, values)
            captured_ranges.append(formatted)
            fixture_ranges.append(
                {
                    "start": start,
                    "count": count,
                    "values": list(values),
                }
            )

        return {
            "capture_kind": "modbus_register_dump",
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
            "fixture_ranges": fixture_ranges,
        }

    @staticmethod
    def _session(transport, target: ProbeTarget) -> ModbusSession:
        return ModbusSession(
            transport,
            route=target.link_route,
            slave_id=target.payload_address,
        )


def _decode_ascii_words(registers: list[int]) -> str:
    chars: list[str] = []
    for value in registers:
        for byte in ((value >> 8) & 0xFF, value & 0xFF):
            if byte in (0x00, 0xFF):
                continue
            char = chr(byte)
            if char.isalnum() or char in "-_/.":
                chars.append(char)
    return "".join(chars)


def _specs_for_block(
    specs: tuple[RegisterValueSpec, ...],
    start_register: int,
    register_count: int,
) -> tuple[RegisterValueSpec, ...]:
    block_end = start_register + register_count
    return tuple(
        spec
        for spec in specs
        if start_register <= spec.register and (spec.register + spec.word_count) <= block_end
    )


def _decode_block(
    start_register: int,
    values: list[int],
    specs: tuple[RegisterValueSpec, ...],
) -> dict[str, Any]:
    registers = {start_register + index: value for index, value in enumerate(values)}
    decoded: dict[str, Any] = {}
    for spec in specs:
        raw = _decode_raw_value(registers, spec)
        if spec.enum_map is not None:
            decoded[spec.key] = spec.enum_map.get(raw, f"Unknown ({raw})")
            continue
        if spec.divisor:
            scaled = raw / spec.divisor
            decoded[spec.key] = round(scaled, spec.decimals or 0)
            continue
        decoded[spec.key] = raw
    return decoded


def _decode_raw_value(registers: dict[int, int], spec: RegisterValueSpec) -> int:
    if spec.combine == "u32_high_first":
        high = registers[spec.register]
        low = registers[spec.register + 1]
        return (high << 16) | low

    value = registers[spec.register]
    if spec.signed:
        return to_signed_16(value)
    return value


async def _read_optional_specs(
    session: ModbusSession,
    specs: tuple[RegisterValueSpec, ...],
) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for spec in specs:
        try:
            raw_values = await session.read_holding(spec.register, spec.word_count)
        except Exception as exc:
            if not _is_optional_spec_error(exc):
                raise
            continue
        decoded.update(_decode_block(spec.register, raw_values, (spec,)))
    return decoded


def _is_optional_spec_error(exc: Exception) -> bool:
    """Return whether one optional-register read failure should be ignored."""

    if isinstance(exc, ModbusError):
        return True
    return str(exc).startswith("missing_register:")


def _decode_named_bits(
    raw_value: Any,
    names_by_bit: dict[int, str],
    *,
    one_based: bool = False,
    include_unknown: bool = False,
    unknown_label_prefix: str = "Unknown Bit",
) -> tuple[str, ...]:
    if not isinstance(raw_value, int):
        return ()

    active: list[str] = []
    if raw_value <= 0:
        return ()

    for bit_index in range(raw_value.bit_length()):
        if not raw_value & (1 << bit_index):
            continue

        lookup_bit = bit_index + 1 if one_based else bit_index
        label = names_by_bit.get(lookup_bit)
        if label is not None:
            active.append(label)
            continue
        if include_unknown:
            active.append(f"{unknown_label_prefix} {lookup_bit}")
    return tuple(active)


def _derive_runtime_states(values: dict[str, Any]) -> dict[str, Any]:
    """Derive human-readable runtime states from already decoded SMG values."""

    derived: dict[str, Any] = {}
    operating_mode = values.get("operating_mode")
    charge_source_priority = values.get("charge_source_priority")
    battery_connected = bool(values.get("battery_connected"))
    warning_descriptions = values.get("warning_descriptions")
    fault_descriptions = values.get("fault_descriptions")
    battery_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("battery",),
    )
    grid_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("mains", "grid"),
    )
    pv_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("pv",),
    )
    thermal_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("temperature", "fan"),
    )
    load_alarm_active = _alarm_matches(
        warning_descriptions,
        fault_descriptions,
        ("overload", "output short circuit", "output power derating"),
    )
    derived["configuration_safe_mode"] = values.get("operating_mode") in {
        "Power On",
        "Standby",
        "Fault",
    }
    derived["battery_equalization_enabled"] = values.get("battery_equalization_mode") == "On"
    utility_charging_allowed = values.get("charge_source_priority") in {
        "Utility Priority",
        "PV Priority",
        "PV and Utility",
        "PV Priority With Load Reserve",
    }
    derived["utility_charging_allowed"] = utility_charging_allowed
    derived["ac_charging_allowed"] = utility_charging_allowed
    derived["pv_only_charging"] = values.get("charge_source_priority") == "PV Only"
    derived["remote_control_enabled"] = values.get("turn_on_mode") in {
        "Local and Remote",
        "Remote Only",
    }
    derived["fault_active"] = bool(values.get("fault_count"))
    derived["warning_active"] = bool(values.get("warning_count"))
    derived["battery_protection_active"] = battery_alarm_active
    derived["grid_warning_active"] = grid_alarm_active
    derived["pv_warning_active"] = pv_alarm_active
    derived["thermal_warning_active"] = thermal_alarm_active
    derived["load_protection_active"] = load_alarm_active
    derived["grid_power_direction"] = _direction_from_power(
        values.get("grid_power"),
        positive_label="Importing",
        negative_label="Exporting",
    )
    derived["pv_producing"] = _is_active_power(values.get("pv_power"))
    derived["load_active"] = _is_active_power(values.get("output_power"))
    derived["battery_charging"] = _is_active_power(values.get("battery_average_power"))
    derived["battery_discharging"] = _is_active_power(values.get("battery_average_power"), negative=True)
    derived["battery_power_direction"] = _direction_from_power(
        values.get("battery_average_power"),
        positive_label="Charging",
        negative_label="Discharging",
    )
    derived["output_state"] = _active_state_from_power(
        values.get("output_power"),
        active_label="Supplying Load",
    )
    derived["pv_state"] = _pv_state(
        power=values.get("pv_power"),
        voltage=values.get("pv_voltage"),
    )
    derived["charging_source_state"] = _charging_source_state(
        pv_charging_power=values.get("pv_charging_power"),
        inverter_charging_power=values.get("inverter_charging_power"),
    )
    derived["charging_active"] = bool(
        _is_active_power(values.get("pv_charging_power"))
        or _is_active_power(values.get("inverter_charging_power"))
    )
    derived["charging_inactive"] = not derived["charging_active"]
    derived["charge_source_policy_state"] = charge_source_priority or "Unknown"
    derived["charging_settings_state"] = _charging_settings_state(
        charging_active=derived["charging_active"],
        charge_source_priority=charge_source_priority,
        utility_charging_allowed=derived["utility_charging_allowed"],
    )
    derived["battery_settings_state"] = _battery_settings_state(
        battery_connected=battery_connected,
        charging_active=derived["charging_active"],
        battery_equalization_enabled=derived["battery_equalization_enabled"],
    )
    derived["output_settings_state"] = _output_settings_state(
        configuration_safe_mode=derived["configuration_safe_mode"],
        operating_mode=operating_mode,
    )
    derived["operational_state"] = _operational_state(
        operating_mode=operating_mode,
        output_state=derived["output_state"],
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
    )
    derived["protection_state"] = _protection_state(
        warning_descriptions=warning_descriptions,
        fault_descriptions=fault_descriptions,
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
        battery_alarm_active=battery_alarm_active,
        grid_alarm_active=grid_alarm_active,
        pv_alarm_active=pv_alarm_active,
        thermal_alarm_active=thermal_alarm_active,
        load_alarm_active=load_alarm_active,
    )
    derived["alarm_context_state"] = _alarm_context_state(
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
        battery_alarm_active=battery_alarm_active,
        grid_alarm_active=grid_alarm_active,
        pv_alarm_active=pv_alarm_active,
        thermal_alarm_active=thermal_alarm_active,
        load_alarm_active=load_alarm_active,
    )
    derived["grid_assist_state"] = _grid_assist_state(
        operating_mode=operating_mode,
        grid_power_direction=derived["grid_power_direction"],
        charging_source_state=derived["charging_source_state"],
    )
    derived["load_supply_state"] = _load_supply_state(
        operating_mode=operating_mode,
        output_state=derived["output_state"],
        grid_power_direction=derived["grid_power_direction"],
        battery_discharging=derived["battery_discharging"],
        pv_producing=derived["pv_producing"],
    )
    derived["battery_role_state"] = _battery_role_state(
        battery_connected=battery_connected,
        battery_charging=derived["battery_charging"],
        battery_discharging=derived["battery_discharging"],
        charging_source_state=derived["charging_source_state"],
        load_active=derived["load_active"],
    )
    derived["pv_role_state"] = _pv_role_state(
        pv_state=derived["pv_state"],
        pv_producing=derived["pv_producing"],
        charging_source_state=derived["charging_source_state"],
        load_supply_state=derived["load_supply_state"],
    )
    derived["utility_role_state"] = _utility_role_state(
        operating_mode=operating_mode,
        grid_power_direction=derived["grid_power_direction"],
        charging_source_state=derived["charging_source_state"],
    )
    derived["site_mode_state"] = _site_mode_state(
        operating_mode=operating_mode,
        load_supply_state=derived["load_supply_state"],
        pv_role_state=derived["pv_role_state"],
        utility_role_state=derived["utility_role_state"],
        warning_active=derived["warning_active"],
        fault_active=derived["fault_active"],
    )
    derived["power_flow_summary"] = _power_flow_summary(
        load_supply_state=derived["load_supply_state"],
        battery_role_state=derived["battery_role_state"],
        pv_role_state=derived["pv_role_state"],
        utility_role_state=derived["utility_role_state"],
    )
    return derived


def _is_active_power(
    raw_value: Any,
    *,
    threshold: int = 20,
    negative: bool = False,
) -> bool:
    """Return whether one power reading indicates active flow."""

    if not isinstance(raw_value, (int, float)):
        return False
    if negative:
        return raw_value <= -threshold
    return raw_value >= threshold


def _direction_from_power(
    raw_value: Any,
    *,
    positive_label: str,
    negative_label: str,
    idle_label: str = "Idle",
    threshold: int = 20,
) -> str:
    """Return a direction label for a signed power value."""

    if not isinstance(raw_value, (int, float)):
        return "Unknown"
    if raw_value >= threshold:
        return positive_label
    if raw_value <= -threshold:
        return negative_label
    return idle_label


def _active_state_from_power(
    raw_value: Any,
    *,
    active_label: str,
    idle_label: str = "Idle",
    threshold: int = 20,
) -> str:
    """Return an activity label for one non-negative power reading."""

    if not isinstance(raw_value, (int, float)):
        return "Unknown"
    if raw_value >= threshold:
        return active_label
    return idle_label


def _pv_state(
    *,
    power: Any,
    voltage: Any,
) -> str:
    """Return a coarse PV state based on live SMG PV readings."""

    if isinstance(power, (int, float)) and power >= 20:
        return "Producing"
    if isinstance(voltage, (int, float)) and voltage >= 15:
        return "Available"
    if isinstance(voltage, (int, float)):
        return "Inactive"
    return "Unknown"


def _charging_source_state(
    *,
    pv_charging_power: Any,
    inverter_charging_power: Any,
    threshold: int = 20,
) -> str:
    """Return a human-readable charging source summary."""

    pv_active = isinstance(pv_charging_power, (int, float)) and pv_charging_power >= threshold
    inverter_active = (
        isinstance(inverter_charging_power, (int, float))
        and inverter_charging_power >= threshold
    )
    if pv_active and inverter_active:
        return "PV + Utility"
    if pv_active:
        return "PV"
    if inverter_active:
        return "Utility"
    if pv_charging_power is None and inverter_charging_power is None:
        return "Unknown"
    return "Idle"


def _charging_settings_state(
    *,
    charging_active: bool,
    charge_source_priority: Any,
    utility_charging_allowed: bool,
) -> str:
    """Summarize whether charging-related settings are currently practical to edit."""

    if charging_active:
        return "Locked While Charging"
    if charge_source_priority == "PV Only":
        return "PV-Only Policy Active"
    if utility_charging_allowed:
        return "Utility Charging Allowed"
    return "Idle"


def _battery_settings_state(
    *,
    battery_connected: bool,
    charging_active: bool,
    battery_equalization_enabled: bool,
) -> str:
    """Summarize the current editing context for battery settings."""

    if not battery_connected:
        return "Battery Disconnected"
    if charging_active:
        return "Locked While Charging"
    if not battery_equalization_enabled:
        return "Equalization Disabled"
    return "Editable"


def _output_settings_state(
    *,
    configuration_safe_mode: bool,
    operating_mode: Any,
) -> str:
    """Summarize whether output/system configuration changes are currently safe."""

    if configuration_safe_mode:
        return "Editable"
    if isinstance(operating_mode, str) and operating_mode:
        return f"Locked In {operating_mode}"
    return "Safe Mode Required"


def _operational_state(
    *,
    operating_mode: Any,
    output_state: Any,
    warning_active: bool,
    fault_active: bool,
) -> str:
    """Summarize the inverter's overall runtime state."""

    if fault_active:
        return "Fault"
    if warning_active:
        if operating_mode in {"Mains", "Off-Grid", "Bypass", "Charging"}:
            return "Running with Warnings"
        if isinstance(operating_mode, str) and operating_mode:
            return f"{operating_mode} with Warnings"
        return "Warnings Active"
    if output_state == "Supplying Load":
        return "Supplying Load"
    if isinstance(operating_mode, str) and operating_mode:
        return operating_mode
    return "Unknown"


def _protection_state(
    *,
    warning_descriptions: Any,
    fault_descriptions: Any,
    warning_active: bool,
    fault_active: bool,
    battery_alarm_active: bool,
    grid_alarm_active: bool,
    pv_alarm_active: bool,
    thermal_alarm_active: bool,
    load_alarm_active: bool,
) -> str:
    """Summarize alarm/protection context from decoded warning and fault states."""

    warnings_text = warning_descriptions if isinstance(warning_descriptions, str) else ""
    faults_text = fault_descriptions if isinstance(fault_descriptions, str) else ""
    if fault_active:
        if battery_alarm_active or "Battery" in faults_text:
            return "Battery Fault Protection"
        if pv_alarm_active:
            return "PV Fault Protection"
        if thermal_alarm_active:
            return "Thermal Fault Protection"
        if load_alarm_active:
            return "Load Fault Protection"
        return "Fault Protection Active"
    if battery_alarm_active or "Battery Low Voltage" in warnings_text or "Battery Discharged Below Recovery Point" in warnings_text:
        return "Battery Protection Active"
    if thermal_alarm_active or "Over Temperature" in warnings_text:
        return "Thermal Warning"
    if load_alarm_active or "Overload" in warnings_text or "Output Power Derating" in warnings_text:
        return "Load Protection Active"
    if pv_alarm_active:
        return "PV Input Warning"
    if grid_alarm_active:
        return "Grid Warning"
    if warning_active:
        return "Warning Active"
    return "Normal"


def _alarm_context_state(
    *,
    warning_active: bool,
    fault_active: bool,
    battery_alarm_active: bool,
    grid_alarm_active: bool,
    pv_alarm_active: bool,
    thermal_alarm_active: bool,
    load_alarm_active: bool,
) -> str:
    """Return one compact alarm category label for automation/UI use."""

    prefix = "Fault" if fault_active else "Warning" if warning_active else ""
    if not prefix:
        return "Normal"
    if battery_alarm_active:
        return f"Battery {prefix}"
    if grid_alarm_active:
        return f"Grid {prefix}"
    if pv_alarm_active:
        return f"PV {prefix}"
    if thermal_alarm_active:
        return f"Thermal {prefix}"
    if load_alarm_active:
        return f"Load {prefix}"
    return prefix


def _grid_assist_state(
    *,
    operating_mode: Any,
    grid_power_direction: Any,
    charging_source_state: Any,
) -> str:
    """Summarize the current role of the grid/utility path."""

    if operating_mode == "Bypass":
        return "Bypass Active"
    if charging_source_state in {"Utility", "PV + Utility"}:
        return "Utility Charging Active"
    if grid_power_direction == "Importing" or operating_mode == "Mains":
        return "Grid Assisting Load"
    if grid_power_direction == "Exporting":
        return "Exporting To Grid"
    if operating_mode == "Off-Grid":
        return "Grid Independent"
    if operating_mode == "Standby":
        return "Standby"
    return "Idle"


def _load_supply_state(
    *,
    operating_mode: Any,
    output_state: Any,
    grid_power_direction: Any,
    battery_discharging: bool,
    pv_producing: bool,
) -> str:
    """Summarize which sources appear to be feeding the load."""

    if output_state != "Supplying Load":
        return "Idle"
    if operating_mode == "Bypass":
        return "Utility Bypass"
    if operating_mode == "Mains":
        if pv_producing and battery_discharging:
            return "Utility + PV + Battery"
        if pv_producing:
            return "Utility + PV"
        if battery_discharging:
            return "Utility + Battery"
        return "Utility"
    if operating_mode == "Off-Grid":
        if pv_producing and battery_discharging:
            return "PV + Battery"
        if pv_producing:
            return "PV"
        if battery_discharging:
            return "Battery"
        return "Inverter"
    if grid_power_direction == "Importing":
        return "Utility"
    if pv_producing and battery_discharging:
        return "PV + Battery"
    if pv_producing:
        return "PV"
    if battery_discharging:
        return "Battery"
    return "Unknown"


def _battery_role_state(
    *,
    battery_connected: bool,
    battery_charging: bool,
    battery_discharging: bool,
    charging_source_state: Any,
    load_active: bool,
) -> str:
    """Summarize the current role of the battery pack."""

    if not battery_connected:
        return "Disconnected"
    if battery_charging:
        if charging_source_state == "PV":
            return "Charging from PV"
        if charging_source_state == "Utility":
            return "Charging from Utility"
        if charging_source_state == "PV + Utility":
            return "Charging from PV + Utility"
        return "Charging"
    if battery_discharging:
        if load_active:
            return "Supplying Load"
        return "Discharging"
    return "Idle"


def _pv_role_state(
    *,
    pv_state: Any,
    pv_producing: bool,
    charging_source_state: Any,
    load_supply_state: Any,
) -> str:
    """Summarize the current role of the PV input."""

    if pv_producing:
        supplies_load = isinstance(load_supply_state, str) and "PV" in load_supply_state
        if charging_source_state == "PV + Utility":
            return "Charging Battery with Utility Assist"
        if charging_source_state == "PV":
            if supplies_load:
                return "Supplying Load + Charging Battery"
            return "Charging Battery"
        if supplies_load:
            return "Supplying Load"
        return "Producing"
    if pv_state in {"Available", "Inactive"}:
        return pv_state
    return "Unknown"


def _utility_role_state(
    *,
    operating_mode: Any,
    grid_power_direction: Any,
    charging_source_state: Any,
) -> str:
    """Summarize the current role of the utility/grid path."""

    if operating_mode == "Bypass":
        return "Bypass Active"
    if charging_source_state in {"Utility", "PV + Utility"}:
        if grid_power_direction == "Importing":
            return "Supplying Load + Charging Battery"
        return "Charging Battery"
    if grid_power_direction == "Importing" or operating_mode == "Mains":
        return "Supplying Load"
    if grid_power_direction == "Exporting":
        return "Exporting"
    if operating_mode == "Off-Grid":
        return "Standby"
    if operating_mode == "Standby":
        return "Standby"
    return "Idle"


def _site_mode_state(
    *,
    operating_mode: Any,
    load_supply_state: Any,
    pv_role_state: Any,
    utility_role_state: Any,
    warning_active: bool,
    fault_active: bool,
) -> str:
    """Return one concise site-wide operating mode summary."""

    if fault_active:
        return "Fault"
    if operating_mode == "Bypass":
        return "Utility Bypass"
    if operating_mode == "Off-Grid":
        summary = "Off-Grid"
        if isinstance(load_supply_state, str) and load_supply_state not in {"Idle", "Unknown"}:
            summary += f" on {load_supply_state}"
        if pv_role_state == "Available":
            summary += ", PV Available"
        if warning_active:
            summary += " with Warnings"
        return summary
    if operating_mode == "Mains":
        summary = "Grid-Connected"
        if isinstance(load_supply_state, str) and load_supply_state not in {"Idle", "Unknown"}:
            summary += f" via {load_supply_state}"
        if utility_role_state == "Charging Battery":
            summary += ", Utility Charging"
        if warning_active:
            summary += " with Warnings"
        return summary
    if operating_mode == "Standby":
        return "Standby"
    if isinstance(operating_mode, str) and operating_mode:
        if warning_active:
            return f"{operating_mode} with Warnings"
        return operating_mode
    return "Unknown"


def _power_flow_summary(
    *,
    load_supply_state: Any,
    battery_role_state: Any,
    pv_role_state: Any,
    utility_role_state: Any,
) -> str:
    """Return one compact cross-source power-flow summary."""

    parts: list[str] = []
    if isinstance(load_supply_state, str) and load_supply_state not in {"Idle", "Unknown"}:
        parts.append(f"Load: {load_supply_state}")
    if isinstance(battery_role_state, str) and battery_role_state not in {"Idle", "Unknown", "Disconnected"}:
        parts.append(f"Battery: {battery_role_state}")
    if isinstance(pv_role_state, str) and pv_role_state not in {"Idle", "Unknown", "Inactive"}:
        parts.append(f"PV: {pv_role_state}")
    if isinstance(utility_role_state, str) and utility_role_state not in {"Idle", "Standby", "Unknown"}:
        parts.append(f"Utility: {utility_role_state}")
    if not parts:
        return "Idle"
    return " | ".join(parts)


def _alarm_matches(
    warning_descriptions: Any,
    fault_descriptions: Any,
    keywords: tuple[str, ...],
) -> bool:
    """Return whether any alarm text contains one of the given keywords."""

    haystacks = []
    if isinstance(warning_descriptions, str) and warning_descriptions != "None":
        haystacks.append(warning_descriptions.lower())
    if isinstance(fault_descriptions, str) and fault_descriptions != "None":
        haystacks.append(fault_descriptions.lower())
    if not haystacks:
        return False
    return any(keyword in haystack for keyword in keywords for haystack in haystacks)


def _find_capability(
    capability_key: str,
    capabilities: tuple[WriteCapability, ...],
) -> WriteCapability:
    for capability in capabilities:
        if capability.key == capability_key:
            return capability
    raise ValueError(f"unsupported_capability:{capability_key}")


def _encode_capability_value(capability: WriteCapability, value: Any) -> int:
    if capability.value_kind == "action":
        return _encode_action_value(capability, value)
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
    if capability.value_kind == "action":
        return raw_value
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
    enum_map = capability.enum_value_map
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
            reverse_map = {label.lower(): key for key, label in enum_map.items()}
            if text not in reverse_map:
                raise ValueError(f"unsupported_bool_value:{capability.key}:{value}")
            raw_value = reverse_map[text]

    if raw_value not in {0, 1}:
        raise ValueError(f"unsupported_bool_raw:{capability.key}:{raw_value}")
    return raw_value


def _encode_action_value(capability: WriteCapability, value: Any) -> int:
    if value is None:
        if capability.action_value is None:
            raise ValueError(f"missing_action_value:{capability.key}")
        return capability.action_value
    try:
        raw_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_action_value:{capability.key}:{value}") from exc
    if capability.action_value is not None and raw_value != capability.action_value:
        raise ValueError(f"unsupported_action_value:{capability.key}:{raw_value}")
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


_SMG_FAMILY_DISCOVERY_CAPTURE_RANGES: tuple[tuple[int, int], ...] = (
    (277, 5),
    (338, 16),
    (389, 3),
    (607, 1),
    (696, 8),
)

_SMG_SCHEMA_DISCOVERY_CAPTURE_RANGES: dict[str, tuple[tuple[int, int], ...]] = {
    "modbus_smg_anenji_anj_11kw_48v_wifi_p": (
        (326, 2),
        (338, 18),
        (376, 18),
        (414, 18),
        (677, 18),
        (707, 1),
        (709, 1),
        (858, 2),
    ),
}


def _support_capture_notes() -> tuple[str, ...]:
    return (
        "Includes supplemental SMG family discovery ranges for 11K-like variants: 277-281, 338-353, 389-391, 607, 696-703.",
    )


def _support_capture_ranges(schema_name: str | None = None) -> tuple[tuple[int, int], ...]:
    default_binding = _smg_default_binding()
    schema = load_register_schema(schema_name or default_binding.register_schema_name)
    planned: list[tuple[int, int]] = [
        (schema.block("status").start, schema.block("status").count),
        (schema.block("serial").start, schema.block("serial").count),
        (schema.block("live").start, schema.block("live").count),
        (schema.block("config").start, schema.block("config").count),
    ]
    planned.extend(
        (spec.register, spec.word_count)
        for spec in schema.spec_set("aux_config")
    )
    planned.extend(
        (register, 1)
        for register in sorted({value for value in schema.scalar_registers.values() if value > 0})
    )
    planned.extend(_SMG_FAMILY_DISCOVERY_CAPTURE_RANGES)
    planned.extend(_SMG_SCHEMA_DISCOVERY_CAPTURE_RANGES.get(schema.key, ()))
    return _merge_capture_ranges(planned)


def _schema_for_inverter(
    inverter: DetectedInverter | None,
    fallback_schema_name: str,
):
    schema_name = fallback_schema_name
    if inverter is not None and inverter.register_schema_name:
        schema_name = inverter.register_schema_name
    if not schema_name:
        return None
    return load_register_schema(schema_name)


def _smg_bindings():
    catalog = load_driver_model_binding_catalog()
    bindings = [binding for binding in catalog.bindings.values() if binding.driver_key == "modbus_smg"]
    return tuple(sorted(bindings, key=lambda binding: (binding.variant_key == "default", binding.variant_key)))


def _is_valid_smg_probe(
    schema,
    live_values: dict[str, Any],
    config_values: dict[str, Any],
    variant_key: str,
) -> bool:
    if live_values.get("operating_mode") not in schema.enum_map_for("mode_names").values():
        return False
    if config_values.get("output_rating_voltage", 0) <= 0:
        return False
    if config_values.get("output_rating_frequency", 0) <= 0:
        return False

    output_mode = config_values.get("output_mode")
    if isinstance(output_mode, str) and output_mode.startswith("Unknown"):
        return False

    output_source_priority = config_values.get("output_source_priority")
    if isinstance(output_source_priority, str) and output_source_priority.startswith("Unknown"):
        return False

    if variant_key == "anenji_anj_11kw_48v_wifi_p":
        if config_values.get("protocol_number") not in {3, 4, 5, 6}:
            return False

    return True


async def _read_rated_power(session: ModbusSession, schema) -> int:
    rated_power_register = schema.scalar_registers.get("rated_power_register", 0)
    if rated_power_register <= 0:
        return 0
    try:
        return (await session.read_holding(rated_power_register, 1))[0]
    except ModbusError:
        return 0


def _smg_model_name(variant_key: str, rated_power: int) -> str:
    if variant_key in _SMG_VARIANT_MODEL_NAMES:
        return _SMG_VARIANT_MODEL_NAMES[variant_key]
    return f"SMG {rated_power}" if rated_power else "SMG"


def _smg_default_binding():
    binding = resolve_driver_model_binding("modbus_smg")
    if binding is None:
        raise RuntimeError("missing_model_binding:modbus_smg")
    return binding


def _merge_capture_ranges(
    ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...],
) -> tuple[tuple[int, int], ...]:
    normalized = sorted(
        (
            (int(start), int(count))
            for start, count in ranges
            if count > 0
        ),
        key=lambda item: item[0],
    )
    if not normalized:
        return ()

    merged: list[tuple[int, int]] = []
    current_start, current_count = normalized[0]
    current_end = current_start + current_count

    for start, count in normalized[1:]:
        end = start + count
        if start <= current_end:
            current_end = max(current_end, end)
            current_count = current_end - current_start
            continue
        merged.append((current_start, current_count))
        current_start = start
        current_count = count
        current_end = end

    merged.append((current_start, current_count))
    return tuple(merged)


def _format_support_range(start: int, values: list[int]) -> dict[str, Any]:
    entries = []
    for offset, value in enumerate(values):
        entries.append(
            {
                "register": start + offset,
                "u16": value,
                "s16": to_signed_16(value),
                "hex": f"0x{value:04X}",
            }
        )
    return {
        "start": start,
        "count": len(values),
        "ascii": _decode_ascii_words(values),
        "words": list(values),
        "values": entries,
    }
