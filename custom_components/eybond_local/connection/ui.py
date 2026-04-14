"""Branch-aware connection setup UI metadata and default builders."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from ..const import (
    CONF_ADVERTISED_SERVER_IP,
    CONF_ADVERTISED_TCP_PORT,
    CONF_COLLECTOR_IP,
    CONF_DISCOVERY_INTERVAL,
    CONF_DISCOVERY_TARGET,
    CONF_DRIVER_HINT,
    CONF_HEARTBEAT_INTERVAL,
    CONF_SERVER_IP,
    CONF_TCP_PORT,
    CONF_UDP_PORT,
    DEFAULT_COLLECTOR_IP,
    DEFAULT_DISCOVERY_INTERVAL,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DRIVER_HINT_AUTO,
)


SelectorKind = Literal[
    "server_ip",
    "ip",
    "port",
    "optional_port",
    "discovery_interval",
    "heartbeat_interval",
    "driver_hint",
]

ValidationKind = Literal["none", "ipv4", "port_optional"]


@dataclass(frozen=True, slots=True)
class ConnectionFormField:
    """One connection-specific field used by onboarding/runtime forms."""

    key: str
    selector_kind: SelectorKind
    label: str
    required: bool = True
    validation_kind: ValidationKind = "none"


@dataclass(frozen=True, slots=True)
class ConnectionFormLayout:
    """Declarative layout for one connection branch."""

    auto_fields: tuple[ConnectionFormField, ...]
    manual_fields: tuple[ConnectionFormField, ...]
    manual_advanced_fields: tuple[ConnectionFormField, ...]
    runtime_fields: tuple[ConnectionFormField, ...]


@dataclass(frozen=True, slots=True)
class ConnectionDisplayMetadata:
    """Branch-aware display strings used by setup and options flows."""

    integration_name: str
    peer_label: str
    peer_label_plural: str
    pending_entry_title: str
    unconfirmed_inverter_label: str


EYBOND_CONNECTION_DISPLAY_METADATA = ConnectionDisplayMetadata(
    integration_name="EyeBond Local",
    peer_label="collector",
    peer_label_plural="collectors",
    pending_entry_title="EyeBond Setup Pending",
    unconfirmed_inverter_label="Unconfirmed inverter",
)


EYBOND_CONNECTION_FORM_LAYOUT = ConnectionFormLayout(
    auto_fields=(
        ConnectionFormField(CONF_SERVER_IP, "server_ip", "Home Assistant IP", validation_kind="ipv4"),
    ),
    manual_fields=(
        ConnectionFormField(CONF_SERVER_IP, "server_ip", "Home Assistant IP", validation_kind="ipv4"),
        ConnectionFormField(CONF_COLLECTOR_IP, "ip", "Collector IP", required=False, validation_kind="ipv4"),
        ConnectionFormField(CONF_DRIVER_HINT, "driver_hint", "Driver Hint"),
    ),
    manual_advanced_fields=(
        ConnectionFormField(CONF_TCP_PORT, "port", "TCP Port"),
        ConnectionFormField(
            CONF_ADVERTISED_SERVER_IP,
            "ip",
            "Advertised Callback IP",
            required=False,
            validation_kind="ipv4",
        ),
        ConnectionFormField(
            CONF_ADVERTISED_TCP_PORT,
            "optional_port",
            "Advertised Callback TCP Port",
            required=False,
            validation_kind="port_optional",
        ),
        ConnectionFormField(CONF_UDP_PORT, "port", "UDP Port"),
        ConnectionFormField(CONF_DISCOVERY_TARGET, "ip", "Discovery Target", validation_kind="ipv4"),
        ConnectionFormField(CONF_DISCOVERY_INTERVAL, "discovery_interval", "Discovery Interval"),
        ConnectionFormField(CONF_HEARTBEAT_INTERVAL, "heartbeat_interval", "Heartbeat Interval"),
    ),
    runtime_fields=(
        ConnectionFormField(CONF_SERVER_IP, "server_ip", "Home Assistant IP", validation_kind="ipv4"),
        ConnectionFormField(CONF_COLLECTOR_IP, "ip", "Collector IP", required=False, validation_kind="ipv4"),
        ConnectionFormField(CONF_TCP_PORT, "port", "TCP Port"),
        ConnectionFormField(
            CONF_ADVERTISED_SERVER_IP,
            "ip",
            "Advertised Callback IP",
            required=False,
            validation_kind="ipv4",
        ),
        ConnectionFormField(
            CONF_ADVERTISED_TCP_PORT,
            "optional_port",
            "Advertised Callback TCP Port",
            required=False,
            validation_kind="port_optional",
        ),
        ConnectionFormField(CONF_UDP_PORT, "port", "UDP Port"),
        ConnectionFormField(CONF_DISCOVERY_TARGET, "ip", "Discovery Target", validation_kind="ipv4"),
        ConnectionFormField(CONF_DISCOVERY_INTERVAL, "discovery_interval", "Discovery Interval"),
        ConnectionFormField(CONF_HEARTBEAT_INTERVAL, "heartbeat_interval", "Heartbeat Interval"),
        ConnectionFormField(CONF_DRIVER_HINT, "driver_hint", "Driver Hint"),
    ),
)


def _optional_port_default(value: object) -> str:
    """Return one optional TCP-port default suitable for a text selector."""

    if value in (None, ""):
        return ""
    return str(value)


def build_eybond_auto_values(
    *,
    server_ip: str,
    default_broadcast: str,
) -> dict[str, Any]:
    """Build EyeBond auto-scan defaults."""

    return {
        CONF_SERVER_IP: server_ip,
        CONF_TCP_PORT: DEFAULT_TCP_PORT,
        CONF_UDP_PORT: DEFAULT_UDP_PORT,
        CONF_DISCOVERY_TARGET: default_broadcast,
        CONF_DISCOVERY_INTERVAL: DEFAULT_DISCOVERY_INTERVAL,
        CONF_HEARTBEAT_INTERVAL: DEFAULT_HEARTBEAT_INTERVAL,
    }


def build_eybond_manual_base_values(
    *,
    server_ip: str,
    default_broadcast: str,
    stored_defaults: Mapping[str, object] | None = None,
    collector_ip: str = "",
    driver_hint: str = DRIVER_HINT_AUTO,
) -> dict[str, Any]:
    """Build EyeBond manual defaults before flow-specific overrides are applied."""

    stored = dict(stored_defaults or {})
    return {
        CONF_SERVER_IP: server_ip,
        CONF_COLLECTOR_IP: collector_ip or str(stored.get(CONF_COLLECTOR_IP, DEFAULT_COLLECTOR_IP)),
        CONF_TCP_PORT: int(stored.get(CONF_TCP_PORT, DEFAULT_TCP_PORT)),
        CONF_ADVERTISED_SERVER_IP: str(stored.get(CONF_ADVERTISED_SERVER_IP, "") or ""),
        CONF_ADVERTISED_TCP_PORT: _optional_port_default(stored.get(CONF_ADVERTISED_TCP_PORT, "")),
        CONF_UDP_PORT: int(stored.get(CONF_UDP_PORT, DEFAULT_UDP_PORT)),
        CONF_DISCOVERY_TARGET: str(stored.get(CONF_DISCOVERY_TARGET, default_broadcast)),
        CONF_DISCOVERY_INTERVAL: int(stored.get(CONF_DISCOVERY_INTERVAL, DEFAULT_DISCOVERY_INTERVAL)),
        CONF_HEARTBEAT_INTERVAL: int(stored.get(CONF_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL)),
        CONF_DRIVER_HINT: str(stored.get(CONF_DRIVER_HINT, driver_hint)),
    }


def build_eybond_runtime_option_values(
    *,
    data: Mapping[str, object],
    options: Mapping[str, object],
    default_server_ip: str,
    default_broadcast: str,
) -> dict[str, Any]:
    """Build EyeBond runtime option defaults."""

    return {
        CONF_SERVER_IP: str(options.get(CONF_SERVER_IP, data.get(CONF_SERVER_IP, default_server_ip))),
        CONF_COLLECTOR_IP: str(options.get(CONF_COLLECTOR_IP, data.get(CONF_COLLECTOR_IP, DEFAULT_COLLECTOR_IP))),
        CONF_TCP_PORT: int(options.get(CONF_TCP_PORT, data.get(CONF_TCP_PORT, DEFAULT_TCP_PORT))),
        CONF_ADVERTISED_SERVER_IP: str(
            options.get(
                CONF_ADVERTISED_SERVER_IP,
                data.get(CONF_ADVERTISED_SERVER_IP, ""),
            )
            or ""
        ),
        CONF_ADVERTISED_TCP_PORT: _optional_port_default(
            options.get(
                CONF_ADVERTISED_TCP_PORT,
                data.get(CONF_ADVERTISED_TCP_PORT, ""),
            )
        ),
        CONF_UDP_PORT: int(options.get(CONF_UDP_PORT, data.get(CONF_UDP_PORT, DEFAULT_UDP_PORT))),
        CONF_DISCOVERY_TARGET: str(
            options.get(
                CONF_DISCOVERY_TARGET,
                data.get(CONF_DISCOVERY_TARGET, default_broadcast),
            )
        ),
        CONF_DISCOVERY_INTERVAL: int(
            options.get(
                CONF_DISCOVERY_INTERVAL,
                data.get(CONF_DISCOVERY_INTERVAL, DEFAULT_DISCOVERY_INTERVAL),
            )
        ),
        CONF_HEARTBEAT_INTERVAL: int(
            options.get(
                CONF_HEARTBEAT_INTERVAL,
                data.get(CONF_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL),
            )
        ),
        CONF_DRIVER_HINT: str(options.get(CONF_DRIVER_HINT, data.get(CONF_DRIVER_HINT, DRIVER_HINT_AUTO))),
    }
