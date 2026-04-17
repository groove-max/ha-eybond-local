"""Pure helpers for estimated energy sensors derived from live power values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DerivedEnergyVariant:
    """One way to compute instantaneous power for a derived energy sensor."""

    source_keys: tuple[str, ...]
    compute: str


@dataclass(frozen=True, slots=True)
class DerivedEnergyDescription:
    """Estimated energy sensor derived from existing runtime values."""

    key: str
    name: str
    icon: str
    variants: tuple[DerivedEnergyVariant, ...]
    expose_entity: bool = False
    enabled_default: bool = False


@dataclass(frozen=True, slots=True)
class DerivedEnergyCycleDescription:
    """Cycle-based helper sensor derived from one estimated energy sensor."""

    key: str
    name: str
    icon: str
    source_key: str
    cycle: str
    required_keys: tuple[str, ...] = ()
    enabled_default: bool = False


_DERIVED_ENERGY_DESCRIPTIONS: tuple[DerivedEnergyDescription, ...] = (
    DerivedEnergyDescription(
        key="estimated_output_energy",
        name="Estimated Output Energy",
        icon="mdi:flash",
        variants=(
            DerivedEnergyVariant(source_keys=("output_active_power",), compute="passthrough"),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_load_energy",
        name="Estimated Load Energy",
        icon="mdi:transmission-tower-import",
        variants=(
            DerivedEnergyVariant(source_keys=("output_power",), compute="passthrough"),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_pv_energy",
        name="Estimated PV Energy",
        icon="mdi:solar-power",
        variants=(
            DerivedEnergyVariant(source_keys=("pv_power",), compute="passthrough"),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_pv_to_home_energy",
        name="Estimated PV to Home Energy",
        icon="mdi:solar-power",
        variants=(
            DerivedEnergyVariant(
                source_keys=("output_power", "pv_power"),
                compute="split_pv_to_home",
            ),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_battery_to_home_energy",
        name="Estimated Battery to Home Energy",
        icon="mdi:battery-arrow-down",
        variants=(
            DerivedEnergyVariant(
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="split_battery_to_home",
            ),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_grid_to_home_energy",
        name="Estimated Grid to Home Energy",
        icon="mdi:transmission-tower-import",
        variants=(
            DerivedEnergyVariant(
                source_keys=("output_power", "pv_power", "battery_power"),
                compute="split_grid_to_home",
            ),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_grid_import_energy",
        name="Estimated Grid Import Energy",
        icon="mdi:transmission-tower-import",
        variants=(
            DerivedEnergyVariant(
                source_keys=("grid_to_home_power", "grid_to_battery_power"),
                compute="sum",
            ),
            DerivedEnergyVariant(
                source_keys=("grid_power",),
                compute="positive_passthrough",
            ),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_grid_export_energy",
        name="Estimated Grid Export Energy",
        icon="mdi:transmission-tower-export",
        variants=(
            DerivedEnergyVariant(
                source_keys=("pv_to_grid_power",),
                compute="positive_passthrough",
            ),
            DerivedEnergyVariant(
                source_keys=("solar_feed_to_grid_power",),
                compute="positive_passthrough",
            ),
            DerivedEnergyVariant(
                source_keys=("grid_power",),
                compute="negative_passthrough_abs",
            ),
        ),
        enabled_default=False,
    ),
    DerivedEnergyDescription(
        key="estimated_battery_charge_energy",
        name="Estimated Battery Charge Energy",
        icon="mdi:battery-charging",
        variants=(
            DerivedEnergyVariant(
                source_keys=("battery_power",),
                compute="positive_passthrough",
            ),
            DerivedEnergyVariant(
                source_keys=("battery_voltage", "battery_charge_current"),
                compute="multiply",
            ),
            DerivedEnergyVariant(
                source_keys=("battery_average_power",),
                compute="positive_passthrough",
            ),
        ),
        enabled_default=True,
    ),
    DerivedEnergyDescription(
        key="estimated_battery_discharge_energy",
        name="Estimated Battery Discharge Energy",
        icon="mdi:battery-arrow-down",
        variants=(
            DerivedEnergyVariant(
                source_keys=("battery_power",),
                compute="negative_passthrough_abs",
            ),
            DerivedEnergyVariant(
                source_keys=("battery_voltage", "battery_discharge_current"),
                compute="multiply",
            ),
            DerivedEnergyVariant(
                source_keys=("battery_average_power",),
                compute="negative_passthrough_abs",
            ),
        ),
        enabled_default=True,
    ),
)


_DERIVED_ENERGY_CYCLE_DESCRIPTIONS: tuple[DerivedEnergyCycleDescription, ...] = (
    DerivedEnergyCycleDescription(
        key="estimated_load_energy_daily",
        name="Estimated Load Energy Today",
        icon="mdi:home-lightning-bolt-outline",
        source_key="estimated_load_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_pv_energy_daily",
        name="Estimated PV Energy Today",
        icon="mdi:calendar-today",
        source_key="estimated_pv_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_battery_charge_energy_daily",
        name="Estimated Battery Charge Energy Today",
        icon="mdi:battery-charging",
        source_key="estimated_battery_charge_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_battery_discharge_energy_daily",
        name="Estimated Battery Discharge Energy Today",
        icon="mdi:battery-arrow-down",
        source_key="estimated_battery_discharge_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_pv_to_home_energy_daily",
        name="Estimated PV to Home Energy Today",
        icon="mdi:solar-power",
        source_key="estimated_pv_to_home_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_battery_to_home_energy_daily",
        name="Estimated Battery to Home Energy Today",
        icon="mdi:battery-arrow-down",
        source_key="estimated_battery_to_home_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_grid_to_home_energy_daily",
        name="Estimated Grid to Home Energy Today",
        icon="mdi:transmission-tower-import",
        source_key="estimated_grid_to_home_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_grid_import_energy_daily",
        name="Estimated Grid Import Energy Today",
        icon="mdi:transmission-tower-import",
        source_key="estimated_grid_import_energy",
        cycle="daily",
        enabled_default=True,
    ),
    DerivedEnergyCycleDescription(
        key="estimated_grid_export_energy_daily",
        name="Estimated Grid Export Energy Today",
        icon="mdi:transmission-tower-export",
        source_key="estimated_grid_export_energy",
        cycle="daily",
        required_keys=("solar_feed_to_grid_enabled",),
        enabled_default=True,
    ),
)

_GRID_EXPORT_DAILY_FALLBACK_KEYS = frozenset(
    {
        "grid_power",
        "pv_to_grid_power",
        "solar_feed_to_grid_power",
    }
)


def derived_energy_descriptions_for_keys(
    available_keys: set[str],
) -> tuple[DerivedEnergyDescription, ...]:
    """Return only derived sensors whose inputs exist for one driver."""

    descriptions: list[DerivedEnergyDescription] = []
    for description in _DERIVED_ENERGY_DESCRIPTIONS:
        if description.key == "estimated_output_energy" and "output_power" in available_keys:
            continue
        if _matching_variant(description, available_keys) is not None:
            descriptions.append(description)
    return tuple(descriptions)


def default_enabled_derived_energy_keys() -> set[str]:
    """Return derived energy keys that should be enabled by default."""

    keys = {
        description.key
        for description in _DERIVED_ENERGY_DESCRIPTIONS
        if description.expose_entity and description.enabled_default
    }
    keys.update(
        description.key
        for description in _DERIVED_ENERGY_CYCLE_DESCRIPTIONS
        if description.enabled_default
    )
    return keys


def derived_energy_cycle_descriptions_for_keys(
    available_keys: set[str],
) -> tuple[DerivedEnergyCycleDescription, ...]:
    """Return cycle helper sensors whose source and prerequisite keys exist."""

    return tuple(
        description
        for description in _DERIVED_ENERGY_CYCLE_DESCRIPTIONS
        if description.source_key in available_keys
        and _cycle_description_requirements_met(description, available_keys)
    )


def derived_energy_entity_descriptions_for_keys(
    available_keys: set[str],
) -> tuple[DerivedEnergyDescription, ...]:
    """Return only derived energy descriptions that should be exposed as entities."""

    return tuple(
        description
        for description in derived_energy_descriptions_for_keys(available_keys)
        if description.expose_entity
    )


def compute_derived_power(
    values: dict[str, Any],
    description: DerivedEnergyDescription,
) -> float | None:
    """Return instantaneous power in watts for one derived energy sensor."""

    variant = _matching_variant(description, set(values))
    if variant is None:
        return None

    if variant.compute == "passthrough":
        power = values.get(variant.source_keys[0])
        return float(power) if isinstance(power, (int, float)) else None

    if variant.compute == "positive_passthrough":
        power = values.get(variant.source_keys[0])
        if isinstance(power, (int, float)):
            return max(0.0, float(power))
        return None

    if variant.compute == "negative_passthrough_abs":
        power = values.get(variant.source_keys[0])
        if isinstance(power, (int, float)):
            return max(0.0, -float(power))
        return None

    if variant.compute == "multiply":
        left = values.get(variant.source_keys[0])
        right = values.get(variant.source_keys[1])
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return float(left) * float(right)
        return None

    if variant.compute == "sum":
        total = 0.0
        for key in variant.source_keys:
            value = values.get(key)
            if not isinstance(value, (int, float)):
                return None
            total += float(value)
        return total

    if variant.compute == "split_pv_to_home":
        split = _priority_split_power(values)
        return split["pv_to_home"] if split is not None else None

    if variant.compute == "split_battery_to_home":
        split = _priority_split_power(values)
        return split["battery_to_home"] if split is not None else None

    if variant.compute == "split_grid_to_home":
        split = _priority_split_power(values)
        return split["grid_to_home"] if split is not None else None

    return None


def _priority_split_power(values: dict[str, Any]) -> dict[str, float] | None:
    load = _non_negative_numeric(values.get("output_power"))
    pv_power = _non_negative_numeric(values.get("pv_power"))
    battery_power = _numeric(values.get("battery_power"))
    if load is None or pv_power is None or battery_power is None:
        return None

    pv_to_home = min(pv_power, load)
    remaining_after_pv = max(0.0, load - pv_to_home)

    battery_discharge = max(0.0, -battery_power)
    battery_to_home = min(battery_discharge, remaining_after_pv)
    remaining_after_battery = max(0.0, remaining_after_pv - battery_to_home)

    grid_power = _numeric(values.get("grid_power"))
    if grid_power is None:
        grid_to_home = remaining_after_battery
    else:
        grid_to_home = min(max(0.0, grid_power), remaining_after_battery)

    return {
        "pv_to_home": pv_to_home,
        "battery_to_home": battery_to_home,
        "grid_to_home": grid_to_home,
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


def _matching_variant(
    description: DerivedEnergyDescription,
    available_keys: set[str],
) -> DerivedEnergyVariant | None:
    """Return the first variant whose inputs are present."""

    for variant in description.variants:
        if set(variant.source_keys).issubset(available_keys):
            return variant
    return None


def _cycle_description_requirements_met(
    description: DerivedEnergyCycleDescription,
    available_keys: set[str],
) -> bool:
    required_keys = set(description.required_keys)
    if required_keys.issubset(available_keys):
        return True
    if description.key != "estimated_grid_export_energy_daily":
        return False
    return bool(_GRID_EXPORT_DAILY_FALLBACK_KEYS & available_keys)
