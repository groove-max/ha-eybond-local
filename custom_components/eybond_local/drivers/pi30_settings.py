"""Readback for known PI30 settings, including device-scoped learned commands.

Cloud option numbers are not inverter setting codes. Match the exact learned
commands to fresh QFLAG/QPIRI fields, never to translated labels or cached ACKs.
Unknown commands deliberately have no inferred readback.
"""

from decimal import Decimal
import re

from ..models import WriteCapability


BOOL_COMMANDS = {
    "buzzer_enabled": "A", "overload_bypass_enabled": "B",
    "power_saving_enabled": "J", "lcd_reset_to_default_enabled": "K",
    "overload_restart_enabled": "U", "over_temperature_restart_enabled": "V",
    "lcd_backlight_enabled": "X", "primary_source_interrupt_alarm_enabled": "Y",
    "record_fault_code_enabled": "Z",
}
ENUM_COMMANDS = {
    "output_source_priority": "POP", "charger_source_priority": "PCP",
    "input_voltage_range": "PGR", "battery_type": "PBT",
}
NUMERIC_COMMANDS = {
    "battery_recharge_voltage": "PBCV", "battery_redischarge_voltage": "PBDV",
    "battery_under_voltage": "PSDV", "battery_bulk_voltage": "PCVV",
    "battery_float_voltage": "PBFT",
}
_CURRENT_COMMANDS = {
    "max_charging_current": "MCHGC", "max_ac_charging_current": "MUCHGC",
}


def setting_readback(command: str) -> tuple[str, str, Decimal] | None:
    """Identify only existing supported setting grammars; never rewrite wire."""

    if re.fullmatch(r"P[ED][A-Za-z]", command):
        for key, flag in BOOL_COMMANDS.items():
            if command[2].upper() == flag:
                return "QFLAG", key, Decimal(command[1] == "E")
    for key, prefix in ENUM_COMMANDS.items():
        if re.fullmatch(prefix + r"[0-9]{2}", command):
            return "QPIRI", key + "_code", Decimal(command[len(prefix):])
    for key, prefix in {**NUMERIC_COMMANDS, **_CURRENT_COMMANDS}.items():
        if re.fullmatch(prefix + r"[0-9]+(?:\.[0-9]+)?", command):
            return "QPIRI", key, Decimal(command[len(prefix):])
    return None


def project_learned_settings(
    values: dict, capabilities: tuple[WriteCapability, ...], query: str,
) -> tuple[dict, frozenset[str]]:
    """Project this response only; invalidate unrepresented learned choices.

    Two provider choices mapping to the same native value are ambiguous and
    cannot establish the selected cloud choice. Mixed-setting command groups
    and unknown command families are not interpreted at all.
    """

    projected = {}
    removed = set()
    for cap in capabilities:
        if not cap.is_device_scoped_experimental or cap.value_key != cap.key:
            continue
        if cap.command_map:
            mapping = {raw: setting_readback(cmd) for raw, cmd in cap.command_map.items()}
            if any(item is None for item in mapping.values()):
                continue
            fields = {item[:2] for item in mapping.values()}
            if len(fields) != 1:
                continue
            response, key = next(iter(fields))
        elif cap.command and cap.command_precision is not None:
            reference = setting_readback(cap.command + "0")
            if reference is None or reference[1] not in {**NUMERIC_COMMANDS, **_CURRENT_COMMANDS}:
                continue
            response, key, _ = reference
            mapping = None
        else:
            continue
        if response != query:
            continue
        removed.add(cap.value_key)
        current = values.get(key)
        if type(current) not in (bool, int, float):
            continue
        native = Decimal(int(current)) if type(current) is bool else Decimal(str(current))
        if not native.is_finite():
            continue
        if mapping is not None:
            matches = [raw for raw, item in mapping.items() if item[2] == native]
            if len(matches) != 1:
                continue
            raw = matches[0]
        else:
            scaled = native * Decimal(cap.divisor or 1)
            if scaled != scaled.to_integral_value():
                continue
            raw = int(scaled)
        if cap.enum_value_map:
            result = cap.enum_value_map.get(raw)
        elif cap.value_kind == "bool":
            result = bool(raw)
        else:
            result = float(Decimal(raw) / Decimal(cap.divisor or 1))
        if result is not None:
            projected[cap.value_key] = result
            removed.discard(cap.value_key)
    return projected, frozenset(removed)
