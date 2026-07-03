"""Shared SmartESS collector parameter registry and runtime decoders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..metadata.smartess_protocol_catalog_loader import load_smartess_protocol_catalog
from .signal import merge_collector_signal_values, normalize_signal_strength
from .smartess_local import CollectorQueryResponse, SmartEssLocalSession, resolve_protocol_descriptor


CollectorValueDecoder = Callable[[CollectorQueryResponse], dict[str, object]]


@dataclass(frozen=True, slots=True)
class CollectorParameterDefinition:
    """One known collector parameter and its semantic decode rules."""

    parameter: int
    name: str
    description: str
    risky_write: bool = False
    decode: CollectorValueDecoder | None = None


def _normalized_query_text(response: CollectorQueryResponse, *, max_len: int = 255) -> str:
    text = str(response.text or "").strip().strip("\x00")
    if not text:
        text = response.data.hex()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _decode_text_value(key: str, *, max_len: int = 255) -> CollectorValueDecoder:
    def _decode(response: CollectorQueryResponse) -> dict[str, object]:
        return {key: _normalized_query_text(response, max_len=max_len)}

    return _decode

def _decode_network_diagnostics(response: CollectorQueryResponse) -> dict[str, object]:
    text = _normalized_query_text(response)
    values: dict[str, object] = {
        "collector_network_diagnostics": text,
    }
    signal_strength, signal_source = normalize_signal_strength(text, source="wifi_rssi")
    if signal_strength is not None:
        values["collector_signal_strength"] = signal_strength
        values["collector_signal_strength_source"] = signal_source
    return values


def _decode_signal_strength(response: CollectorQueryResponse) -> dict[str, object]:
    text = _normalized_query_text(response)
    values: dict[str, object] = {
        "collector_signal_strength_raw": text,
    }
    signal_strength, signal_source = normalize_signal_strength(text, source="gprs_csq")
    if signal_strength is not None:
        values["collector_signal_strength"] = signal_strength
        values["collector_signal_strength_source"] = signal_source
    return values


def _decode_protocol_descriptor(response: CollectorQueryResponse) -> dict[str, object]:
    descriptor = resolve_protocol_descriptor(response)
    values: dict[str, object] = {
        "smartess_protocol_raw_id": descriptor.raw_id,
    }
    if descriptor.suffix:
        values["smartess_protocol_suffix"] = descriptor.suffix

    known_protocol = load_smartess_protocol_catalog().protocols.get(descriptor.asset_id)
    if known_protocol is not None:
        # Claim an asset id from parameter 14 only when the catalog knows it:
        # otherwise a raw serial-protocol config id would fight the asset id
        # the bound driver reports, flip-flopping the sensor every cycle.
        values["smartess_protocol_asset_id"] = descriptor.asset_id
        values["smartess_protocol_asset_name"] = descriptor.asset_name
        values["smartess_protocol_profile_key"] = known_protocol.profile_key
        if known_protocol.proto_name:
            values["smartess_protocol_name"] = known_protocol.proto_name
        elif descriptor.asset_name:
            values["smartess_protocol_name"] = descriptor.asset_name
        if len(known_protocol.device_addresses) == 1:
            values["smartess_device_address"] = known_protocol.device_addresses[0]
    return values


COLLECTOR_PARAMETER_DEFINITIONS: tuple[CollectorParameterDefinition, ...] = (
    CollectorParameterDefinition(1, "collector_type", "Collector type/category.", risky_write=True),
    CollectorParameterDefinition(2, "collector_pn", "Collector serial / PN.", risky_write=True, decode=_decode_text_value("collector_pn")),
    CollectorParameterDefinition(4, "protocol_version", "Collector protocol version.", decode=_decode_text_value("collector_protocol_version")),
    CollectorParameterDefinition(5, "firmware_version", "Collector firmware / ROM version.", decode=_decode_text_value("smartess_collector_version")),
    CollectorParameterDefinition(6, "hardware_version", "Collector hardware version.", decode=_decode_text_value("collector_hardware_version")),
    CollectorParameterDefinition(7, "production_date", "Collector production date."),
    CollectorParameterDefinition(11, "online_count", "Online device count."),
    CollectorParameterDefinition(12, "device_count", "Configured downstream device count."),
    CollectorParameterDefinition(13, "collect_frequency", "Collection / reporting frequency.", risky_write=True),
    CollectorParameterDefinition(14, "protocol_descriptor", "Protocol/profile descriptor such as 0912 or 0925.", risky_write=True, decode=_decode_protocol_descriptor),
    CollectorParameterDefinition(16, "local_ip_address", "Collector local IP address.", risky_write=True, decode=_decode_text_value("collector_local_ip_address")),
    CollectorParameterDefinition(21, "domain_address_1", "Primary cloud domain / server address.", risky_write=True, decode=_decode_text_value("collector_server_endpoint")),
    CollectorParameterDefinition(25, "timezone", "Collector timezone.", risky_write=True),
    CollectorParameterDefinition(29, "system_operation", "Apply / restart / system action trigger.", risky_write=True),
    CollectorParameterDefinition(30, "reboot_required", "Reboot / pending-apply status.", risky_write=True, decode=_decode_text_value("collector_reboot_required")),
    CollectorParameterDefinition(32, "transmission_mode", "RTU / URTU transmission mode.", risky_write=True, decode=_decode_text_value("collector_transmission_mode")),
    CollectorParameterDefinition(34, "serial_baudrate", "Serial port baudrate.", risky_write=True, decode=_decode_text_value("collector_serial_baudrate")),
    CollectorParameterDefinition(
        41,
        "router_ssid",
        "Connected upstream router SSID.",
        risky_write=True,
        decode=_decode_text_value("collector_ssid"),
    ),
    CollectorParameterDefinition(43, "router_password", "Configured upstream router password.", risky_write=True),
    CollectorParameterDefinition(46, "collector_ap_ssid", "Collector AP SSID.", risky_write=True),
    CollectorParameterDefinition(48, "network_diagnostics", "Network connection diagnostics.", risky_write=True, decode=_decode_network_diagnostics),
    CollectorParameterDefinition(49, "wifi_scan_list", "Nearby Wi-Fi scan results.", risky_write=True),
    CollectorParameterDefinition(55, "gprs_csq", "GPRS signal strength.", risky_write=True, decode=_decode_signal_strength),
    CollectorParameterDefinition(56, "gprs_ccid", "SIM CCID.", risky_write=True),
    CollectorParameterDefinition(58, "cpu_id", "CPU identifier.", risky_write=True),
    CollectorParameterDefinition(65, "sg_serial_number", "State-grid serial number.", risky_write=True),
)


COLLECTOR_PARAMETER_DEFINITION_BY_ID: dict[int, CollectorParameterDefinition] = {
    definition.parameter: definition for definition in COLLECTOR_PARAMETER_DEFINITIONS
}

KNOWN_PARAMETERS: dict[int, tuple[str, str]] = {
    definition.parameter: (definition.name, definition.description)
    for definition in COLLECTOR_PARAMETER_DEFINITIONS
}

RISKY_WRITE_PARAMETERS: set[int] = {
    definition.parameter for definition in COLLECTOR_PARAMETER_DEFINITIONS if definition.risky_write
}

RUNTIME_COLLECTOR_PARAMETERS: tuple[CollectorParameterDefinition, ...] = tuple(
    definition
    for definition in COLLECTOR_PARAMETER_DEFINITIONS
    if definition.parameter in {2, 4, 5, 6, 14, 16, 21, 30, 32, 34, 41, 48, 55}
)


async def query_runtime_collector_values(
    session: SmartEssLocalSession,
    *,
    parameters: tuple[CollectorParameterDefinition, ...] = RUNTIME_COLLECTOR_PARAMETERS,
) -> dict[str, object]:
    """Read a safe read-only collector runtime metadata set via FC=2."""

    values: dict[str, object] = {}
    for definition in parameters:
        if definition.decode is None:
            continue
        try:
            response = await session.query_collector(definition.parameter)
        except Exception:
            continue
        if response.code != 0:
            continue
        merge_collector_signal_values(values, definition.decode(response))
    return values