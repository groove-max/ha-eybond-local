"""User-declared inverter labels, never protocol or hardware identity evidence."""

from __future__ import annotations

from collections.abc import Mapping
import unicodedata

from .const import CONF_INVERTER_DISPLAY_MANUFACTURER, CONF_INVERTER_DISPLAY_MODEL


INVERTER_DISPLAY_FIELDS = (
    CONF_INVERTER_DISPLAY_MANUFACTURER,
    CONF_INVERTER_DISPLAY_MODEL,
)
MAX_DISPLAY_LABEL_LENGTH = 128


def normalize_display_label(value: object) -> str:
    """Validate a short, single-line label; blank means automatic display."""

    if not isinstance(value, str):
        raise ValueError("invalid_inverter_display_label")
    if any(
        unicodedata.category(char).startswith("C") or char in "\u2028\u2029"
        for char in value
    ):
        raise ValueError("invalid_inverter_display_label")
    value = value.strip()
    if len(value) > MAX_DISPLAY_LABEL_LENGTH:
        raise ValueError("invalid_inverter_display_label")
    return value


def inverter_display_labels(options: Mapping[str, object] | None) -> dict[str, str]:
    """Read valid saved overrides; malformed labels cannot break device setup."""

    labels = {}
    for key in INVERTER_DISPLAY_FIELDS:
        try:
            value = normalize_display_label((options or {}).get(key, ""))
        except ValueError:
            continue
        if value:
            labels[key] = value
    return labels


def with_inverter_display_labels(
    options: Mapping[str, object], values: Mapping[str, object],
) -> dict[str, object]:
    """Merge only presentation fields into the latest entry options."""

    result = dict(options)
    for key in INVERTER_DISPLAY_FIELDS:
        value = normalize_display_label(values.get(key, ""))
        result.pop(key, None)
        if value:
            result[key] = value
    return result


def inverter_display_diagnostics(options: Mapping[str, object]) -> dict[str, object]:
    """Keep owner-declared branding separate from detected technical metadata."""

    labels = inverter_display_labels(options)
    return {
        "source": "user" if labels else "automatic",
        "scope": "config_entry",
        "overrides": {
            field: labels[key]
            for field, key in (
                ("manufacturer", CONF_INVERTER_DISPLAY_MANUFACTURER),
                ("model", CONF_INVERTER_DISPLAY_MODEL),
            )
            if key in labels
        },
    }
