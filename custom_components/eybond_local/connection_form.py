"""Shared Home Assistant schema helpers for EyeBond connection forms.

Config-flow onboarding and options-flow runtime editing are separate lifecycle
owners, but both render and validate the same branch-declared connection
fields.  This module owns that presentation mapping so neither lifecycle has to
call methods on the other.
"""

from __future__ import annotations

import socket
from typing import Any

import voluptuous as vol
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
)

from .connection.branch_registry import get_connection_branch
from .connection.ui import ConnectionFormField
from .const import DRIVER_HINT_AUTO
from .drivers.registry import driver_options
from .flow_translation import selector_option_label

PORT_SELECTOR = NumberSelector(
    NumberSelectorConfig(min=1, max=65535, mode=NumberSelectorMode.BOX)
)
DISCOVERY_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=1,
        max=60,
        step=1,
        unit_of_measurement="s",
        mode=NumberSelectorMode.SLIDER,
    )
)
HEARTBEAT_INTERVAL_SELECTOR = NumberSelector(
    NumberSelectorConfig(
        min=5,
        max=3600,
        step=5,
        unit_of_measurement="s",
        mode=NumberSelectorMode.BOX,
    )
)
IP_TEXT_SELECTOR = TextSelector(TextSelectorConfig())

DRIVER_DISPLAY_LABELS: dict[str, str] = {
    DRIVER_HINT_AUTO: "Auto",
    "modbus_smg": "SMG / Modbus",
    "srne_modbus": "SRNE / Modbus",
    "must_pv_ph18": "MUST PV/PH18",
    "modbus_catalog": "Device Catalog / Modbus",
    "smartess_local": "SmartESS 0925 / Modbus",
    "pi30": "PI30",
    "eybond_g_ascii": "EyeBond G-ASCII",
    "pi18": "PI18",
}


def interface_selector(
    interface_options: list[dict[str, str]],
) -> SelectSelector:
    """Return a selector for known local interfaces."""

    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(value=item["ip"], label=item["label"])
                for item in interface_options
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def driver_selector(bundle: dict[str, Any] | None = None) -> SelectSelector:
    """Return the branch-aware driver selector."""

    return SelectSelector(
        SelectSelectorConfig(
            options=[
                SelectOptionDict(
                    value=option,
                    label=selector_option_label(
                        bundle,
                        "driver_hint",
                        option,
                        DRIVER_DISPLAY_LABELS.get(
                            option,
                            option.replace("_", " ").title(),
                        ),
                    ),
                )
                for option in driver_options()
            ],
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


def selector_for_connection_field(
    field: ConnectionFormField,
    *,
    server_ip_selector: SelectSelector | TextSelector,
    translation_bundle: dict[str, Any] | None,
):
    """Resolve one concrete selector from branch-declared field metadata."""

    if field.selector_kind == "server_ip":
        return server_ip_selector
    if field.selector_kind == "ip":
        return IP_TEXT_SELECTOR
    if field.selector_kind == "port":
        return PORT_SELECTOR
    if field.selector_kind == "optional_port":
        return IP_TEXT_SELECTOR
    if field.selector_kind == "discovery_interval":
        return DISCOVERY_INTERVAL_SELECTOR
    if field.selector_kind == "heartbeat_interval":
        return HEARTBEAT_INTERVAL_SELECTOR
    if field.selector_kind == "driver_hint":
        return driver_selector(translation_bundle)
    raise ValueError(f"unsupported_connection_selector:{field.selector_kind}")


def build_connection_fields_schema(
    connection_type: str,
    *,
    fields: tuple[ConnectionFormField, ...],
    values: dict[str, Any],
    server_ip_selector: SelectSelector | TextSelector,
    translation_bundle: dict[str, Any] | None,
) -> dict[Any, Any]:
    """Build one voluptuous mapping for branch-aware connection fields."""

    get_connection_branch(connection_type)
    schema: dict[Any, Any] = {}
    for field in fields:
        marker = vol.Required if field.required else vol.Optional
        schema[marker(field.key, default=values.get(field.key, ""))] = (
            selector_for_connection_field(
                field,
                server_ip_selector=server_ip_selector,
                translation_bundle=translation_bundle,
            )
        )
    return schema


def validate_connection_inputs(
    user_input: dict[str, Any],
    *,
    fields: tuple[ConnectionFormField, ...],
) -> dict[str, str]:
    """Validate branch-aware inputs from their declared validation metadata."""

    errors: dict[str, str] = {}
    for field in fields:
        raw_value = str(user_input.get(field.key, "") or "").strip()
        if field.validation_kind == "ipv4":
            if not raw_value:
                if field.required:
                    errors[field.key] = "invalid_ip"
                continue
            try:
                socket.inet_aton(raw_value)
            except OSError:
                errors[field.key] = "invalid_ip"
            continue
        if field.validation_kind == "port_optional":
            if not raw_value:
                continue
            if not raw_value.isdigit() or not 1 <= int(raw_value) <= 65535:
                errors[field.key] = "invalid_port"
    return errors


__all__ = [
    "DRIVER_DISPLAY_LABELS",
    "IP_TEXT_SELECTOR",
    "PORT_SELECTOR",
    "build_connection_fields_schema",
    "interface_selector",
    "selector_for_connection_field",
    "validate_connection_inputs",
]
