"""Measured battery-side DC power from qualified BMS voltage × currents.

MX2: this is a measurement (when currents are published), never an estimate.
Do not confuse with ``estimated_ac_load_power`` (load% × rated VA).
Sign: positive ⇒ net charging, negative ⇒ net discharging (HA convention).
"""

from __future__ import annotations


def battery_dc_power_values(values: dict[str, object]) -> dict[str, object]:
    """Return ``battery_power`` from BMS V × (Icharge − Idischarge), or omit."""
    voltage = values.get("bms_total_voltage")
    charge_i = values.get("bms_charging_current")
    discharge_i = values.get("bms_discharging_current")
    if not isinstance(voltage, (int, float)):
        return {}
    if not isinstance(charge_i, (int, float)) or not isinstance(discharge_i, (int, float)):
        return {}
    return {
        "battery_power": round(float(voltage) * (float(charge_i) - float(discharge_i)), 1),
    }
