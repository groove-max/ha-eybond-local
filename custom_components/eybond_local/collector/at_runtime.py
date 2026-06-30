"""Read-only runtime queries for plain collector AT sessions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from .at import CollectorAtResponse
from .cloud_family import collector_cloud_family_observation_from_endpoint
from .signal import merge_collector_signal_values, normalize_signal_strength

# Literal prefix every virtual bridge (e.g. ESP EyeBond Collector) emits as the
# first token of its ``AT+VDTU?`` reply. Factory collectors return an error,
# empty value, or a reply without this prefix — absence means "factory/unknown".
VIRTUAL_BRIDGE_PREFIX = "esp-collector,"
VIRTUAL_BRIDGE_REBOOT_FEATURES = frozenset(
    {
        "reboot",
        "restart",
        "collector_reboot",
        "collector_restart",
    }
)


class CollectorAtQueryTransport(Protocol):
    """Minimal read-only collector AT transport contract."""

    async def async_query(self, command: str) -> CollectorAtResponse:
        ...


CollectorAtDecoder = Callable[[CollectorAtResponse], dict[str, object]]


@dataclass(frozen=True, slots=True)
class CollectorAtQueryDefinition:
    """One known read-only collector AT query."""

    command: str
    description: str
    decode: CollectorAtDecoder


def _decode_text_value(key: str) -> CollectorAtDecoder:
    def _decode(response: CollectorAtResponse) -> dict[str, object]:
        return {key: str(response.value or "").strip()}

    return _decode


def _decode_signal_strength(response: CollectorAtResponse) -> dict[str, object]:
    raw = str(response.value or "").strip()
    values: dict[str, object] = {
        "collector_signal_strength_raw": raw,
    }
    signal_strength, signal_source = normalize_signal_strength(raw, source="wifi_rssi")
    if signal_strength is not None:
        values["collector_signal_strength"] = signal_strength
        values["collector_signal_strength_source"] = signal_source
    return values


def _decode_collector_server_endpoint(response: CollectorAtResponse) -> dict[str, object]:
    endpoint = str(response.value or "").strip()
    values: dict[str, object] = {
        "collector_server_endpoint": endpoint,
    }
    observation = collector_cloud_family_observation_from_endpoint(endpoint)
    if observation.known:
        values["collector_cloud_family"] = observation.family
        values["collector_cloud_family_source"] = observation.source
        values["collector_cloud_family_confidence"] = observation.confidence
    return values


RUNTIME_COLLECTOR_AT_DEFINITIONS: tuple[CollectorAtQueryDefinition, ...] = (
    CollectorAtQueryDefinition("DTUPN", "Collector PN / serial.", _decode_text_value("collector_pn")),
    CollectorAtQueryDefinition(
        "ATVER",
        "AT interpreter / collector protocol version.",
        _decode_text_value("collector_protocol_version"),
    ),
    CollectorAtQueryDefinition(
        "ENUPMODE",
        "Collector upload mode flag.",
        _decode_text_value("collector_upload_mode"),
    ),
    CollectorAtQueryDefinition(
        "SYST",
        "Collector system time.",
        _decode_text_value("collector_system_time"),
    ),
    CollectorAtQueryDefinition("WFSS", "Collector Wi-Fi RSSI.", _decode_signal_strength),
    CollectorAtQueryDefinition(
        "UART",
        "Collector UART settings.",
        _decode_text_value("collector_serial_baudrate"),
    ),
    CollectorAtQueryDefinition(
        "DTUTYPE",
        "Collector model / type.",
        _decode_text_value("collector_type"),
    ),
    CollectorAtQueryDefinition(
        "FWVER",
        "Collector firmware version.",
        _decode_text_value("smartess_collector_version"),
    ),
    CollectorAtQueryDefinition(
        "CLDSRVHOST1",
        "Collector cloud callback endpoint.",
        _decode_collector_server_endpoint,
    ),
    CollectorAtQueryDefinition(
        "HTBT",
        "Collector cloud heartbeat value.",
        _decode_text_value("collector_cloud_heartbeat_value"),
    ),
    CollectorAtQueryDefinition(
        "LINK",
        "Collector link status from the newer communication path.",
        _decode_text_value("collector_link_status"),
    ),
    CollectorAtQueryDefinition(
        "INTPARA49",
        "Nearby Wi-Fi scan list reported by the collector.",
        _decode_text_value("collector_wifi_scan_list"),
    ),
    CollectorAtQueryDefinition(
        "VDTU",
        "Virtual-bridge identity probe (additive; factory collectors stay silent).",
        _decode_text_value("collector_vdtu_raw"),
    ),
)

_FACTORY_COLLECTOR_CLOUD_FAMILIES = frozenset(
    {
        "legacy_binary",
        "smartess_at",
        "valuecloud_at",
    }
)


@dataclass(frozen=True, slots=True)
class CollectorVirtualBridgeInfo:
    """Parsed identity of a virtual collector bridge (e.g. ESP EyeBond Collector)."""

    is_virtual_bridge: bool = False
    kind: str = ""
    version: str = ""
    features: tuple[str, ...] = ()
    attributes: tuple[tuple[str, str], ...] = ()


def collector_bridge_features_support_reboot(features: object) -> bool:
    """Return whether a parsed virtual-bridge feature set advertises restart support."""

    if isinstance(features, str):
        tokens = features.split(",")
    else:
        try:
            tokens = tuple(features or ())
        except TypeError:
            tokens = ()
    normalized = {
        str(token or "").strip().lower().replace("-", "_")
        for token in tokens
        if str(token or "").strip()
    }
    return bool(normalized & VIRTUAL_BRIDGE_REBOOT_FEATURES)


def parse_collector_vdtu(raw: object) -> CollectorVirtualBridgeInfo:
    """Parse one raw ``AT+VDTU`` value into virtual-bridge identity fields.

    The reply format is::

        esp-collector,<semver>;features=<csv>;uart=<...>;spacing_ms=<n>;queue=<n>

    Detection keys only on the leading ``esp-collector,`` token. This parser is
    pure and defensive: empty, truncated, or future-version input (including
    unknown ``features`` tokens) must never raise — it just returns whatever it
    can resolve, defaulting to "not a bridge".
    """

    text = str(raw or "").strip()
    if not text or not text.startswith(VIRTUAL_BRIDGE_PREFIX):
        return CollectorVirtualBridgeInfo()

    remainder = text[len(VIRTUAL_BRIDGE_PREFIX) :]
    segments = [segment.strip() for segment in remainder.split(";")]

    version = segments[0].strip() if segments else ""
    features: tuple[str, ...] = ()
    attributes: list[tuple[str, str]] = []
    for segment in segments[1:]:
        key, _, value = segment.partition("=")
        normalized_key = key.strip().lower()
        if not normalized_key:
            continue
        attributes.append((normalized_key, value.strip()))
        if normalized_key != "features":
            continue
        features = tuple(
            token.strip()
            for token in value.split(",")
            if token.strip()
        )

    return CollectorVirtualBridgeInfo(
        is_virtual_bridge=True,
        kind="esp-collector",
        version=version,
        features=features,
        attributes=tuple(attributes),
    )


async def query_runtime_collector_at_values(
    transport: CollectorAtQueryTransport,
    *,
    collector_cloud_family: str = "",
) -> dict[str, object]:
    """Read a safe read-only collector metadata set over the plain AT session."""

    values: dict[str, object] = {}
    normalized_family = str(collector_cloud_family or "").strip().lower()
    for definition in RUNTIME_COLLECTOR_AT_DEFINITIONS:
        if definition.command == "VDTU":
            effective_family = str(
                values.get("collector_cloud_family") or normalized_family
            ).strip().lower()
            if effective_family in _FACTORY_COLLECTOR_CLOUD_FAMILIES:
                continue
        try:
            response = await transport.async_query(definition.command)
        except Exception:
            continue
        merge_collector_signal_values(values, definition.decode(response))
    return values
