"""Helpers for device-scoped learned overlay activation state."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, TypeVar


DEVICE_SCOPED_OVERLAY_ACTIVATION_KEY = "device_scoped_overlay_activation"
LEARNED_READ_SENSOR_KEY_PREFIX = "learned_read_"

_T = TypeVar("_T")


def selected_read_sensor_keys_from_activation(
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
) -> frozenset[str]:
    """Return selected learned read sensor keys from activation state.

    Missing legacy selection is fail-closed to an empty set. Learned reads are
    route-sensitive and may only be exposed after an explicit, context-checked
    activation.
    """

    activation = device_scoped_overlay_activation(entry_data, entry_options)
    raw_keys = activation.get("selected_read_sensor_keys")
    if raw_keys is None:
        selected_sensors = activation.get("selected_read_sensors")
        if not isinstance(selected_sensors, (list, tuple)):
            return frozenset()
        raw_keys = [
            sensor.get("key") for sensor in selected_sensors if isinstance(sensor, Mapping)
        ]
    if not isinstance(raw_keys, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(key).strip() for key in raw_keys if str(key or "").strip())


def learned_read_sensor_allowed(
    key: str,
    *,
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
) -> bool:
    """Return whether one measurement key should be exposed at runtime."""

    normalized_key = str(key or "").strip()
    if not normalized_key.startswith(LEARNED_READ_SENSOR_KEY_PREFIX):
        return True
    selected_keys = selected_read_sensor_keys_from_activation(entry_data, entry_options)
    return normalized_key in selected_keys


def filter_learned_read_measurements_for_activation(
    measurements: Iterable[_T],
    *,
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
) -> tuple[_T, ...]:
    """Filter learned read measurement descriptions by activation selection."""

    return tuple(
        measurement
        for measurement in measurements
        if learned_read_sensor_allowed(
            str(getattr(measurement, "key", "") or ""),
            entry_data=entry_data,
            entry_options=entry_options,
        )
    )


def device_scoped_overlay_activation(
    entry_data: Mapping[str, Any] | None,
    entry_options: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    """Return the stored activation mapping, preferring options over data."""

    for container in (entry_options, entry_data):
        if not isinstance(container, Mapping):
            continue
        raw = container.get(DEVICE_SCOPED_OVERLAY_ACTIVATION_KEY)
        if isinstance(raw, Mapping):
            return raw
    return {}
