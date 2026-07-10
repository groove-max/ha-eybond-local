"""Canonical telemetry aliases shared across inverter families."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import MeasurementDescription


_GRID_PRESENT_VOLTAGE_THRESHOLD = 20.0
_GRID_ABSENT_OPERATING_MODES = {"Battery", "Off-Grid"}


@dataclass(frozen=True, slots=True)
class CanonicalTelemetryVariant:
    """One driver-specific way to populate a canonical telemetry value."""

    driver_keys: tuple[str, ...]
    source_keys: tuple[str, ...]
    compute: str
    # Optional pack-level gate for multi-pack drivers (modbus_catalog): when
    # set, the variant only applies to inverters whose surface variant_key is
    # listed. Driver-level gating alone would silently impose one pack's
    # semantics (source-key meaning, sign conventions) on every future pack
    # served by the same generic driver.
    variant_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CanonicalTelemetryDescription:
    """One canonical telemetry sensor plus its driver-specific mappings."""

    description: MeasurementDescription
    variants: tuple[CanonicalTelemetryVariant, ...]


_CANONICAL_TELEMETRY: tuple[CanonicalTelemetryDescription, ...] = (
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="grid_voltage",
            name="Grid Voltage",
            unit="V",
            device_class="voltage",
            state_class="measurement",
            icon="mdi:transmission-tower",
            diagnostic=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi30",),
                source_keys=("input_voltage",),
                compute="passthrough",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("smartess_local",),
                source_keys=("ac_input_voltage",),
                compute="passthrough",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="grid_frequency",
            name="Grid Frequency",
            unit="Hz",
            device_class="frequency",
            state_class="measurement",
            icon="mdi:sine-wave",
            diagnostic=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi30",),
                source_keys=("input_frequency",),
                compute="passthrough",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("smartess_local",),
                source_keys=("ac_input_frequency",),
                compute="passthrough",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="output_power",
            name="Load Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:flash",
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi18", "pi30", "smartess_local", "eybond_g_ascii"),
                source_keys=("output_active_power",),
                compute="passthrough",
            ),
            # Solis hybrids report grid-side household load and EPS backup
            # load separately; total consumption is their sum.
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("solis_esinv",),
                source_keys=("household_load_power", "backup_load_power"),
                compute="sum",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="load_percent",
            name="Load Percent",
            unit="%",
            state_class="measurement",
            icon="mdi:gauge",
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi18",),
                source_keys=("output_load_percent",),
                compute="passthrough",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("eybond_g_ascii",),
                source_keys=("output_load_percentage",),
                compute="passthrough",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_voltage",
            name="PV Voltage",
            unit="V",
            device_class="voltage",
            state_class="measurement",
            icon="mdi:solar-power",
            diagnostic=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi18", "pi30", "smartess_local", "eybond_g_ascii"),
                source_keys=("pv_input_voltage",),
                compute="passthrough",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_current",
            name="PV Current",
            unit="A",
            device_class="current",
            state_class="measurement",
            icon="mdi:solar-power",
            diagnostic=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi30",),
                source_keys=("pv_input_current",),
                compute="passthrough",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_power",
            name="PV Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:solar-power",
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi18", "pi30", "smartess_local"),
                source_keys=("pv_input_power",),
                compute="passthrough",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("srne_modbus",),
                source_keys=("pv1_input_power", "pv2_input_power"),
                compute="sum",
            ),
            # Single-string SRNE models may not report the PV2 register.
            CanonicalTelemetryVariant(
                driver_keys=("srne_modbus",),
                source_keys=("pv1_input_power",),
                compute="passthrough",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("must_pv_ph18",),
                source_keys=("pv_charging_power",),
                compute="passthrough",
            ),
            # Aohai FSA has no PV power register; the vendor map derives it
            # as PV voltage x current, so the canonical value does the same.
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("aohai_fsa",),
                source_keys=("pv_input_voltage", "pv_input_current"),
                compute="multiply",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("growatt_spf",),
                source_keys=("pv1_input_power", "pv2_input_power"),
                compute="sum",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("deye_lv",),
                source_keys=(
                    "pv1_input_power",
                    "pv2_input_power",
                    "pv3_input_power",
                    "pv4_input_power",
                ),
                compute="sum",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_charging_power",
            name="PV Charging Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:solar-power",
        ),
        variants=(
            # Growatt SPF PV registers are charger-side readings ("PV charge
            # power"), so their sum is also the explicit PV charging power the
            # flow split trusts over derived headroom.
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("growatt_spf",),
                source_keys=("pv1_input_power", "pv2_input_power"),
                compute="sum",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="battery_percent",
            name="Battery Percent",
            unit="%",
            device_class="battery",
            state_class="measurement",
            icon="mdi:battery",
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("pi18", "eybond_g_ascii"),
                source_keys=("battery_capacity",),
                compute="passthrough",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="battery_power",
            name="Battery Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:battery-medium",
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=("modbus_smg",),
                source_keys=("battery_average_power",),
                compute="passthrough",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("pi18", "pi30"),
                source_keys=(
                    "battery_voltage",
                    "battery_charge_current",
                    "battery_discharge_current",
                ),
                compute="signed_delta_multiply",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("smartess_local",),
                source_keys=(
                    "battery_voltage",
                    "charging_current",
                    "battery_discharge_current",
                ),
                compute="signed_delta_multiply",
            ),
            CanonicalTelemetryVariant(
                driver_keys=("eybond_g_ascii",),
                source_keys=(
                    "battery_voltage",
                    "battery_current",
                    "eybond_g_ascii_operating_mode_code",
                ),
                compute="eybond_g_ascii_battery_power",
            ),
            # SRNE register 270 is signed battery charging power (charge > 0).
            CanonicalTelemetryVariant(
                driver_keys=("srne_modbus",),
                source_keys=("charge_power",),
                compute="passthrough",
            ),
            # MUST register 25273 is the vendor's "Batt power"; the schema
            # exposes it directly as battery_power. (25274 turned out to be
            # battery CURRENT — the third-party map mislabeled it
            # "Battery_Load", and a 0.3.0-beta.1 tester caught the sensor
            # showing amps as watts.)
            # Aohai FSA: BMS battery current is signed with charge > 0.
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("aohai_fsa",),
                source_keys=("battery_voltage", "battery_current"),
                compute="multiply",
            ),
            # Solis firmwares disagree on the sign of the battery-power
            # register, but the current-direction word is explicit — trust
            # the direction and the power magnitude.
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("solis_esinv",),
                source_keys=("battery_power_magnitude", "battery_current_direction"),
                compute="magnitude_with_direction",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="grid_power",
            name="Grid Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:transmission-tower",
        ),
        variants=(
            # Solis grid-port power is export-positive on the wire; the
            # canonical convention is import-positive.
            CanonicalTelemetryVariant(
                driver_keys=("modbus_catalog",),
                variant_keys=("solis_esinv",),
                source_keys=("grid_port_power",),
                compute="negate",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_to_home_power",
            name="PV to Home Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:solar-power",
            enabled_default=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=(
                    "modbus_smg",
                    "pi18",
                    "pi30",
                    "smartess_local",
                    "eybond_g_ascii",
                    "srne_modbus",
                    "must_pv_ph18",
                    "modbus_catalog",
                ),
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="flow_pv_to_home",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_to_battery_power",
            name="PV to Battery Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:solar-power-variant",
            enabled_default=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=(
                    "modbus_smg",
                    "pi18",
                    "pi30",
                    "smartess_local",
                    "eybond_g_ascii",
                    "srne_modbus",
                    "must_pv_ph18",
                    "modbus_catalog",
                ),
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="flow_pv_to_battery",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="pv_to_grid_power",
            name="PV to Grid Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:transmission-tower-export",
            enabled_default=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=(
                    "modbus_smg",
                    "pi18",
                    "pi30",
                    "smartess_local",
                    "eybond_g_ascii",
                    "srne_modbus",
                    "must_pv_ph18",
                    "modbus_catalog",
                ),
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="flow_pv_to_grid",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="battery_to_home_power",
            name="Battery to Home Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:battery-arrow-down",
            enabled_default=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=(
                    "modbus_smg",
                    "pi18",
                    "pi30",
                    "smartess_local",
                    "eybond_g_ascii",
                    "srne_modbus",
                    "must_pv_ph18",
                    "modbus_catalog",
                ),
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="flow_battery_to_home",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="grid_to_home_power",
            name="Grid to Home Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:transmission-tower-import",
            enabled_default=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=(
                    "modbus_smg",
                    "pi18",
                    "pi30",
                    "smartess_local",
                    "eybond_g_ascii",
                    "srne_modbus",
                    "must_pv_ph18",
                    "modbus_catalog",
                ),
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="flow_grid_to_home",
            ),
        ),
    ),
    CanonicalTelemetryDescription(
        description=MeasurementDescription(
            key="grid_to_battery_power",
            name="Grid to Battery Power",
            unit="W",
            device_class="power",
            state_class="measurement",
            icon="mdi:battery-charging",
            enabled_default=True,
        ),
        variants=(
            CanonicalTelemetryVariant(
                driver_keys=(
                    "modbus_smg",
                    "pi18",
                    "pi30",
                    "smartess_local",
                    "eybond_g_ascii",
                    "srne_modbus",
                    "must_pv_ph18",
                    "modbus_catalog",
                ),
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="flow_grid_to_battery",
            ),
        ),
    ),
)


def all_canonical_measurements() -> tuple[MeasurementDescription, ...]:
    """Return every canonical telemetry description exactly once."""

    return tuple(spec.description for spec in _CANONICAL_TELEMETRY)


def canonical_measurements_for_driver(
    driver_key: str | None,
    *,
    variant_key: str | None = None,
) -> tuple[MeasurementDescription, ...]:
    """Return canonical telemetry descriptions that a driver can populate.

    ``variant_key`` narrows pack-gated variants (multi-pack drivers such as
    ``modbus_catalog``): with a concrete variant, descriptions whose only
    matching variants belong to other packs are excluded, so those packs do
    not grow canonical entities that would never receive a value.  ``None``
    keeps the legacy driver-wide surface.
    """

    if not driver_key:
        return ()
    return tuple(
        spec.description
        for spec in _CANONICAL_TELEMETRY
        if any(
            driver_key in variant.driver_keys
            and (
                not variant.variant_keys
                or variant_key is None
                or variant_key in variant.variant_keys
            )
            for variant in spec.variants
        )
    )


def apply_canonical_measurements(
    driver_key: str | None,
    values: dict[str, Any],
    *,
    variant_key: str | None = None,
) -> dict[str, Any]:
    """Populate canonical telemetry aliases without overwriting native values."""

    if not driver_key:
        return values

    available_keys = set(values)
    for spec in _CANONICAL_TELEMETRY:
        canonical_key = spec.description.key
        if canonical_key in values:
            continue
        variant = _matching_variant(spec, driver_key, available_keys, variant_key=variant_key)
        if variant is None:
            continue
        value = _compute_variant(variant, values)
        if value is None:
            continue
        values[canonical_key] = value
        available_keys.add(canonical_key)
    return values


def _matching_variant(
    spec: CanonicalTelemetryDescription,
    driver_key: str,
    available_keys: set[str],
    *,
    variant_key: str | None = None,
) -> CanonicalTelemetryVariant | None:
    for variant in spec.variants:
        if driver_key not in variant.driver_keys:
            continue
        if variant.variant_keys and variant_key not in variant.variant_keys:
            continue
        if set(variant.source_keys).issubset(available_keys):
            return variant
    return None


def _compute_variant(
    variant: CanonicalTelemetryVariant,
    values: dict[str, Any],
) -> Any:
    if variant.compute == "passthrough":
        return values.get(variant.source_keys[0])

    if variant.compute == "multiply":
        product = 1.0
        for key in variant.source_keys:
            numeric = _numeric(values.get(key))
            if numeric is None:
                return None
            product *= numeric
        return round(product, 4)

    if variant.compute == "sum":
        total = 0.0
        for key in variant.source_keys:
            numeric = _numeric(values.get(key))
            if numeric is None:
                return None
            total += numeric
        return round(total, 4)

    if variant.compute == "signed_delta_multiply":
        voltage = values.get(variant.source_keys[0])
        charge_current = values.get(variant.source_keys[1])
        discharge_current = values.get(variant.source_keys[2])
        if not isinstance(voltage, (int, float)):
            return None
        charge = float(charge_current) if isinstance(charge_current, (int, float)) else 0.0
        discharge = float(discharge_current) if isinstance(discharge_current, (int, float)) else 0.0
        if charge == 0.0 and discharge == 0.0:
            return 0.0
        return round(float(voltage) * (charge - discharge), 4)

    if variant.compute == "eybond_g_ascii_battery_power":
        return _compute_eybond_g_ascii_battery_power(values, variant)

    if variant.compute == "negate":
        value = values.get(variant.source_keys[0])
        if not isinstance(value, (int, float)):
            return None
        return round(-float(value), 4)

    if variant.compute == "magnitude_with_direction":
        magnitude = values.get(variant.source_keys[0])
        direction = values.get(variant.source_keys[1])
        if not isinstance(magnitude, (int, float)) or direction is None:
            return None
        normalized = str(direction).strip().lower()
        sign = 1.0 if normalized in ("0", "charge", "charging") else -1.0
        return round(abs(float(magnitude)) * sign, 4)

    if variant.compute.startswith("flow_"):
        flows = _compute_flow_split(values)
        if flows is None:
            return None
        flow_key = variant.compute.removeprefix("flow_")
        return flows.get(flow_key)

    return None


def _compute_eybond_g_ascii_battery_power(
    values: dict[str, Any],
    variant: CanonicalTelemetryVariant,
) -> float | None:
    """Return signed battery power for EyeBond G-ASCII telemetry.

    The protocol exposes battery current as a magnitude.  The sign is inferred
    from GMOD/DCDC mode when it is known:

    * ``B`` = charging, positive power.
    * ``L`` = line/utility mode, positive battery current means charging.
    * ``0`` = discharging soft start, negative power.

    Some devices may eventually expose a signed current directly; preserve that
    sign if it appears instead of forcing it through the mode map.
    """

    voltage = values.get(variant.source_keys[0])
    current = values.get(variant.source_keys[1])
    mode = values.get(variant.source_keys[2])
    if not isinstance(voltage, (int, float)) or not isinstance(current, (int, float)):
        return None

    voltage_value = float(voltage)
    current_value = float(current)
    mode_code = str(mode).strip().upper() if mode is not None else ""
    if current_value == 0.0 and mode_code.startswith("B"):
        current_value = _positive_numeric(
            values.get("pv_charging_current"),
            values.get("charging_current"),
        )
    if current_value == 0.0:
        return 0.0
    if current_value < 0.0:
        return round(voltage_value * current_value, 4)

    if mode_code.startswith("B"):
        return round(voltage_value * current_value, 4)
    if mode_code in {"L", "4", "LINE", "UTILITY", "MAINS"}:
        return round(voltage_value * current_value, 4)
    if mode_code.startswith("0"):
        return round(-(voltage_value * current_value), 4)
    return None


def _compute_flow_split(values: dict[str, Any]) -> dict[str, float] | None:
    load = _non_negative_numeric(values.get("output_power"))
    pv = _non_negative_numeric(values.get("pv_power"))
    battery_power = _numeric(values.get("battery_power"))
    if load is None or pv is None or battery_power is None:
        return None

    battery_charge = max(0.0, battery_power)
    battery_discharge = max(0.0, -battery_power)
    grid_present = _grid_present(values)
    grid_power = _numeric(values.get("grid_power"))
    grid_import = max(0.0, grid_power) if grid_power is not None else None
    grid_export = max(0.0, -grid_power) if grid_power is not None else 0.0
    pv_charging_power = _non_negative_numeric(values.get("pv_charging_power"))
    pv_export_power = _non_negative_numeric(values.get("solar_feed_to_grid_power"))

    if grid_present is False:
        grid_import = 0.0 if grid_power is not None else None
        grid_export = 0.0
        pv_export_power = 0.0

    pv_to_home = min(load, pv)
    pv_remaining = max(0.0, pv - pv_to_home)

    if pv_charging_power is not None:
        # The charger's own PV-side measurement outranks the derived
        # headroom: units whose pv_power register under-reads (measured
        # 1718 W while the charger logged 1786 W into the battery and the
        # load drew 781 W) would otherwise have real PV charge clamped to
        # the phantom remainder and re-attributed to the grid — a PV-only
        # system accumulated 1.17 kWh of fake daily grid import that way.
        pv_to_battery = min(battery_charge, pv_charging_power)
    else:
        pv_to_battery = min(battery_charge, pv_remaining)
    pv_remaining = max(0.0, pv_remaining - pv_to_battery)

    desired_pv_to_grid = pv_export_power if pv_export_power is not None else grid_export
    pv_to_grid = min(pv_remaining, max(0.0, desired_pv_to_grid))

    battery_to_home = min(battery_discharge, max(0.0, load - pv_to_home))
    base_grid_to_home = max(0.0, load - pv_to_home - battery_to_home)
    grid_to_battery = 0.0 if grid_present is False else max(0.0, battery_charge - pv_to_battery)

    if grid_present is False:
        # When the grid is explicitly absent, residual power mismatch should
        # stay unattributed rather than being drawn as a fake grid import.
        grid_to_home = 0.0
    elif grid_import is None:
        grid_to_home = base_grid_to_home
    else:
        grid_to_home = max(base_grid_to_home, grid_import - grid_to_battery)

    return {
        "pv_to_home": round(pv_to_home, 4),
        "pv_to_battery": round(pv_to_battery, 4),
        "pv_to_grid": round(pv_to_grid, 4),
        "battery_to_home": round(battery_to_home, 4),
        "grid_to_home": round(grid_to_home, 4),
        "grid_to_battery": round(grid_to_battery, 4),
    }


def _numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _non_negative_numeric(value: Any) -> float | None:
    numeric = _numeric(value)
    if numeric is None:
        return None
    return max(0.0, numeric)


def _positive_numeric(*values: Any) -> float:
    for value in values:
        numeric = _numeric(value)
        if numeric is not None and numeric > 0.0:
            return numeric
    return 0.0


def _grid_present(values: dict[str, Any]) -> bool | None:
    grid_voltage = _numeric(values.get("grid_voltage"))
    if grid_voltage is not None:
        return grid_voltage >= _GRID_PRESENT_VOLTAGE_THRESHOLD

    operating_mode = values.get("operating_mode")
    if isinstance(operating_mode, str) and operating_mode in _GRID_ABSENT_OPERATING_MODES:
        return False

    return None
