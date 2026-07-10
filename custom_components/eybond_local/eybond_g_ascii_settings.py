"""Canonical settings metadata for the EyeBond G-ASCII protocol family."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ReadbackKind = Literal["enum_command", "feature_flag", "numeric_command", "output_voltage"]


@dataclass(frozen=True, slots=True)
class GAsciiSettingDefinition:
    """One canonical G-ASCII setting exposed by ValueCloud metadata."""

    field_id: str
    title: str
    choices: dict[str, str]
    read_key: str = ""
    readback_kind: ReadbackKind | None = None
    read_command: str = ""
    read_value_map: dict[str, str] | None = None
    feature_flag: str = ""
    requires_key: str = ""


G_ASCII_SETTINGS_BY_VALUECLOUD_FIELD: dict[str, GAsciiSettingDefinition] = {
    "cltd_inverter_remote_switch": GAsciiSettingDefinition(
        field_id="cltd_inverter_remote_switch",
        title="Remote inverter switch",
        choices={
            "0": "Turn on / cancel remote shutdown",
            "1": "Remote shutdown",
        },
    ),
    "cltd_set_output_priority": GAsciiSettingDefinition(
        field_id="cltd_set_output_priority",
        title="Output priority",
        choices={
            "12336": "Utility first",
            "12337": "PV first",
            "12338": "PV > Battery > Utility",
        },
        read_key="g_ascii_setting_output_priority",
        readback_kind="enum_command",
        read_command="OPR??",
        read_value_map={"00": "12336", "01": "12337", "02": "12338"},
    ),
    "cltd_battery_mode": GAsciiSettingDefinition(
        field_id="cltd_battery_mode",
        title="Battery type",
        choices={
            "48": "Lead-acid",
            "49": "Flooded / water-filled",
            "50": "Lithium",
            "51": "Custom",
        },
        read_key="g_ascii_setting_battery_type",
        readback_kind="enum_command",
        read_command="TBAT?",
        read_value_map={"0": "48", "1": "49", "2": "50", "3": "51"},
    ),
    "cltd_inverter_output_voltage": GAsciiSettingDefinition(
        field_id="cltd_inverter_output_voltage",
        title="Output voltage setting",
        choices={
            "100": "100 V display",
            "110": "110 V display",
            "115": "115 V display",
            "120": "120 V display",
            "127": "127 V display",
            "208": "208 V",
            "220": "220 V",
            "230": "230 V",
            "240": "240 V",
        },
        read_key="g_ascii_setting_output_voltage",
        readback_kind="output_voltage",
        read_command="V???",
    ),
    "cltd_set_output_mode": GAsciiSettingDefinition(
        field_id="cltd_set_output_mode",
        title="Output mode",
        choices={
            "12336": "Appliance mode",
            "12337": "UPS mode",
        },
        read_key="g_ascii_setting_output_mode",
        readback_kind="enum_command",
        read_command="OPM??",
        read_value_map={"00": "12336", "01": "12337"},
    ),
    "cltd_charging_priority": GAsciiSettingDefinition(
        field_id="cltd_charging_priority",
        title="Charging priority",
        choices={
            "12336": "Utility first",
            "12337": "PV first",
            "12338": "Utility + PV",
            "12339": "PV only",
        },
        read_key="g_ascii_setting_charging_priority",
        readback_kind="enum_command",
        read_command="CPR??",
        read_value_map={"00": "12336", "01": "12337", "02": "12338", "03": "12339"},
    ),
    "cltd_Communication_function": GAsciiSettingDefinition(
        field_id="cltd_Communication_function",
        title="BMS communication",
        choices={"68": "Disabled", "69": "Enabled"},
        read_key="g_ascii_setting_bms_communication",
        readback_kind="feature_flag",
        feature_flag="S",
    ),
    "cltd_energy_saving_mode": GAsciiSettingDefinition(
        field_id="cltd_energy_saving_mode",
        title="Battery energy-saving mode",
        choices={"68": "Disabled", "69": "Enabled"},
        read_key="g_ascii_setting_energy_saving_mode",
        readback_kind="feature_flag",
        feature_flag="I",
    ),
    "cltd_equalization_modet": GAsciiSettingDefinition(
        field_id="cltd_equalization_modet",
        title="Battery equalization mode",
        choices={"68": "Disabled", "69": "Enabled"},
        read_key="g_ascii_setting_equalization_mode",
        readback_kind="feature_flag",
        feature_flag="Q",
    ),
    "cltd_onnected_alarm": GAsciiSettingDefinition(
        field_id="cltd_onnected_alarm",
        title="Battery-not-connected alarm",
        choices={"68": "Disabled", "69": "Enabled"},
        read_key="g_ascii_setting_battery_not_connected_alarm",
        readback_kind="feature_flag",
        feature_flag="L",
    ),
    "cltd_inverter_soft_close": GAsciiSettingDefinition(
        field_id="cltd_inverter_soft_close",
        title="Inverter soft-start relay",
        choices={"68": "Disabled", "69": "Enabled"},
        read_key="g_ascii_setting_inverter_soft_start_relay",
        readback_kind="feature_flag",
        feature_flag="Y",
    ),
    "cltd_buzzer_silent": GAsciiSettingDefinition(
        field_id="cltd_buzzer_silent",
        title="Buzzer silent mode",
        choices={"68": "Disabled", "69": "Enabled"},
        read_key="g_ascii_setting_buzzer_silent_mode",
        readback_kind="feature_flag",
        feature_flag="O",
    ),
    "jsdsolar_max_charging_current": GAsciiSettingDefinition(
        field_id="jsdsolar_max_charging_current",
        title="Maximum charging current",
        choices={},
        read_key="g_ascii_setting_max_charging_current",
        readback_kind="numeric_command",
        read_command="CHGC???",
    ),
    "jsdsolar_max_utility_charging_current": GAsciiSettingDefinition(
        field_id="jsdsolar_max_utility_charging_current",
        title="Maximum utility charging current",
        choices={},
        read_key="g_ascii_setting_max_utility_charging_current",
        readback_kind="numeric_command",
        read_command="GCC???",
    ),
    "jsdsolar_charging_method": GAsciiSettingDefinition(
        field_id="jsdsolar_charging_method",
        title="Charging method",
        choices={"0": "Auto", "1": "Forced 2-stage", "2": "Forced 3-stage"},
        read_key="g_ascii_setting_charging_method",
        readback_kind="enum_command",
        read_command="CST??",
        read_value_map={"00": "0", "01": "1", "02": "2"},
    ),
    "jsdsolar_constant_voltage_charging_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_constant_voltage_charging_voltage",
        title="Constant voltage charging voltage",
        choices={},
        read_key="g_ascii_setting_constant_voltage_charging_voltage",
        readback_kind="numeric_command",
        read_command="TCCV????",
    ),
    "jsdsolar_float_charging_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_float_charging_voltage",
        title="Float charging voltage",
        choices={},
        read_key="g_ascii_setting_float_charging_voltage",
        readback_kind="numeric_command",
        read_command="TCFV????",
    ),
    "jsdsolar_equalization_charging_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_equalization_charging_voltage",
        title="Equalization charging voltage",
        choices={},
        read_key="g_ascii_setting_equalization_charging_voltage",
        readback_kind="numeric_command",
        read_command="TCQV????",
    ),
    "jsdsolar_constant_voltage_charging_time": GAsciiSettingDefinition(
        field_id="jsdsolar_constant_voltage_charging_time",
        title="Constant voltage charging time",
        choices={},
        read_key="g_ascii_setting_constant_voltage_charging_time",
        readback_kind="numeric_command",
        read_command="TCVT????",
    ),
    "jsdsolar_equalization_charging_time": GAsciiSettingDefinition(
        field_id="jsdsolar_equalization_charging_time",
        title="Equalization charging time",
        choices={},
        read_key="g_ascii_setting_equalization_charging_time",
        readback_kind="numeric_command",
        read_command="TCQT????",
    ),
    "jsdsolar_equalization_timeout": GAsciiSettingDefinition(
        field_id="jsdsolar_equalization_timeout",
        title="Equalization timeout",
        choices={},
        read_key="g_ascii_setting_equalization_timeout",
        readback_kind="numeric_command",
        read_command="TCQO????",
    ),
    "jsdsolar_equalization_interval": GAsciiSettingDefinition(
        field_id="jsdsolar_equalization_interval",
        title="Equalization interval",
        choices={},
        read_key="g_ascii_setting_equalization_interval",
        readback_kind="numeric_command",
        read_command="TCQI????",
    ),
    "jsdsolar_battery_discharge_cutoff_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_battery_discharge_cutoff_voltage",
        title="Battery discharge cut-off voltage",
        choices={},
        read_key="g_ascii_setting_battery_discharge_cutoff_voltage",
        readback_kind="numeric_command",
        read_command="EOD????",
    ),
    "jsdsolar_battery_discharge_alarm_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_battery_discharge_alarm_voltage",
        title="Battery discharge alarm voltage",
        choices={},
        read_key="g_ascii_setting_battery_discharge_alarm_voltage",
        readback_kind="numeric_command",
        read_command="TBLV????",
    ),
    "jsdsolar_low_power_discharge_time": GAsciiSettingDefinition(
        field_id="jsdsolar_low_power_discharge_time",
        title="Low-power discharge time",
        choices={},
        read_key="g_ascii_setting_low_power_discharge_time",
        readback_kind="numeric_command",
        read_command="LWDT????",
    ),
    "jsdsolar_battery_to_grid_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_battery_to_grid_voltage",
        title="Battery-to-grid voltage",
        choices={},
        read_key="g_ascii_setting_battery_to_grid_voltage",
        readback_kind="numeric_command",
        read_command="BTG????",
    ),
    "jsdsolar_grid_to_battery_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_grid_to_battery_voltage",
        title="Grid-to-battery voltage",
        choices={},
        read_key="g_ascii_setting_grid_to_battery_voltage",
        readback_kind="numeric_command",
        read_command="BTB????",
    ),
    "jsdsolar_battery_overvoltage_protection_voltage": GAsciiSettingDefinition(
        field_id="jsdsolar_battery_overvoltage_protection_voltage",
        title="Battery over-voltage protection voltage",
        choices={},
        read_key="g_ascii_setting_battery_overvoltage_protection_voltage",
        readback_kind="numeric_command",
        read_command="BTO????",
    ),
    "jsdsolar_grid_overvoltage_protection": GAsciiSettingDefinition(
        field_id="jsdsolar_grid_overvoltage_protection",
        title="Grid over-voltage protection",
        choices={},
        read_key="g_ascii_setting_grid_overvoltage_protection",
        readback_kind="numeric_command",
        read_command="OVP???",
    ),
    "jsdsolar_grid_undervoltage_protection": GAsciiSettingDefinition(
        field_id="jsdsolar_grid_undervoltage_protection",
        title="Grid under-voltage protection",
        choices={},
        read_key="g_ascii_setting_grid_undervoltage_protection",
        readback_kind="numeric_command",
        read_command="LVP???",
    ),
    "jsdsolar_constant_current_charging_max_time": GAsciiSettingDefinition(
        field_id="jsdsolar_constant_current_charging_max_time",
        title="Constant-current charging max time",
        choices={},
        read_key="g_ascii_setting_constant_current_charging_max_time",
        readback_kind="numeric_command",
        read_command="CI1??",
    ),
    "jsdsolar_bms_low_soc_shutdown": GAsciiSettingDefinition(
        field_id="jsdsolar_bms_low_soc_shutdown",
        title="BMS low SOC shutdown",
        choices={},
        read_key="g_ascii_setting_bms_low_soc_shutdown",
        readback_kind="numeric_command",
        read_command="BSOCU???",
        requires_key="g_ascii_bms_available",
    ),
    "jsdsolar_bms_low_soc_switch_to_grid": GAsciiSettingDefinition(
        field_id="jsdsolar_bms_low_soc_switch_to_grid",
        title="BMS low SOC switch to grid",
        choices={},
        read_key="g_ascii_setting_bms_low_soc_switch_to_grid",
        readback_kind="numeric_command",
        read_command="BSOCG???",
        requires_key="g_ascii_bms_available",
    ),
    "jsdsolar_bms_high_soc_switch_to_battery": GAsciiSettingDefinition(
        field_id="jsdsolar_bms_high_soc_switch_to_battery",
        title="BMS high SOC switch to battery",
        choices={},
        read_key="g_ascii_setting_bms_high_soc_switch_to_battery",
        readback_kind="numeric_command",
        read_command="BSOCB???",
        requires_key="g_ascii_bms_available",
    ),
}
