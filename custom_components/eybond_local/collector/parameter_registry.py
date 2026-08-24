"""Shared SmartESS collector parameter registry and runtime decoders."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Protocol

from ..metadata.smartess_protocol_catalog_loader import load_smartess_protocol_catalog
from .collector_wire import CollectorQueryResponse, CollectorWireError
from .metadata_result import (
    OUTCOME_COMMAND_ERROR,
    OUTCOME_EMPTY,
    OUTCOME_PARTIAL,
    OUTCOME_SUCCESS,
    OUTCOME_TRANSPORT_ERROR,
    CollectorMetadataChannelReadResult,
    present_metadata_values,
)
from .signal import merge_collector_signal_values, normalize_signal_strength
from .smartess_local import resolve_protocol_descriptor

# Delivery failures (link unusable) vs. a malformed response (command error) vs.
# a well-formed unsupported parameter (skipped): only the last two are collector
# facts; a delivery failure is a link fact.
_FRAMED_TRANSPORT_FAILURES = (
    asyncio.TimeoutError,
    TimeoutError,
    OSError,
    ConnectionError,
    EOFError,
    asyncio.IncompleteReadError,
    asyncio.LimitOverrunError,
)


class CollectorMetadataQuerySession(Protocol):
    """Minimal provider-neutral FC=2 read contract used by metadata polling."""

    async def query_collector(self, *parameters: int) -> CollectorQueryResponse:
        ...


CollectorValueDecoder = Callable[[CollectorQueryResponse], dict[str, object]]


@dataclass(frozen=True, slots=True)
class CollectorParameterDefinition:
    """One known collector parameter and its semantic decode rules."""

    parameter: int
    name: str
    description: str
    risky_write: bool = False
    sensitive_read: bool = False
    decode: CollectorValueDecoder | None = None
    semantic_fields: frozenset[str] = frozenset()


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
    CollectorParameterDefinition(2, "collector_pn", "Collector serial / PN.", risky_write=True, decode=_decode_text_value("collector_pn"), semantic_fields=frozenset({"collector_pn"})),
    CollectorParameterDefinition(4, "protocol_version", "Collector protocol version.", decode=_decode_text_value("collector_protocol_version"), semantic_fields=frozenset({"collector_protocol_version"})),
    CollectorParameterDefinition(5, "firmware_version", "Collector firmware / ROM version.", decode=_decode_text_value("smartess_collector_version"), semantic_fields=frozenset({"smartess_collector_version"})),
    CollectorParameterDefinition(6, "hardware_version", "Collector hardware version.", decode=_decode_text_value("collector_hardware_version"), semantic_fields=frozenset({"collector_hardware_version"})),
    CollectorParameterDefinition(7, "production_date", "Collector production date."),
    CollectorParameterDefinition(11, "online_count", "Online device count."),
    CollectorParameterDefinition(12, "device_count", "Configured downstream device count."),
    CollectorParameterDefinition(13, "collect_frequency", "Collection / reporting frequency.", risky_write=True),
    CollectorParameterDefinition(14, "protocol_descriptor", "Protocol/profile descriptor such as 0912 or 0925.", risky_write=True, decode=_decode_protocol_descriptor, semantic_fields=frozenset({"smartess_protocol_raw_id", "smartess_protocol_suffix", "smartess_protocol_asset_id", "smartess_protocol_asset_name", "smartess_protocol_profile_key", "smartess_protocol_name", "smartess_device_address"})),
    CollectorParameterDefinition(16, "local_ip_address", "Collector local IP address.", risky_write=True, decode=_decode_text_value("collector_local_ip_address"), semantic_fields=frozenset({"collector_local_ip_address"})),
    CollectorParameterDefinition(21, "domain_address_1", "Primary cloud domain / server address.", risky_write=True, decode=_decode_text_value("collector_server_endpoint"), semantic_fields=frozenset({"collector_server_endpoint"})),
    CollectorParameterDefinition(25, "timezone", "Collector timezone.", risky_write=True),
    CollectorParameterDefinition(29, "system_operation", "Apply / restart / system action trigger.", risky_write=True),
    CollectorParameterDefinition(30, "reboot_required", "Reboot / pending-apply status.", risky_write=True, decode=_decode_text_value("collector_reboot_required"), semantic_fields=frozenset({"collector_reboot_required"})),
    CollectorParameterDefinition(32, "transmission_mode", "RTU / URTU transmission mode.", risky_write=True, decode=_decode_text_value("collector_transmission_mode"), semantic_fields=frozenset({"collector_transmission_mode"})),
    CollectorParameterDefinition(34, "serial_baudrate", "Serial port baudrate.", risky_write=True, decode=_decode_text_value("collector_serial_baudrate"), semantic_fields=frozenset({"collector_serial_baudrate"})),
    CollectorParameterDefinition(
        41,
        "router_ssid",
        "Connected upstream router SSID.",
        risky_write=True,
        decode=_decode_text_value("collector_ssid"),
        semantic_fields=frozenset({"collector_ssid"}),
    ),
    CollectorParameterDefinition(
        43,
        "router_password",
        "Configured upstream router password.",
        risky_write=True,
        sensitive_read=True,
    ),
    CollectorParameterDefinition(46, "collector_ap_ssid", "Collector AP SSID.", risky_write=True),
    CollectorParameterDefinition(48, "network_diagnostics", "Network connection diagnostics.", risky_write=True, decode=_decode_network_diagnostics, semantic_fields=frozenset({"collector_network_diagnostics", "collector_signal_strength", "collector_signal_strength_source"})),
    CollectorParameterDefinition(49, "wifi_scan_list", "Nearby Wi-Fi scan results.", risky_write=True),
    CollectorParameterDefinition(55, "gprs_csq", "GPRS signal strength.", risky_write=True, decode=_decode_signal_strength, semantic_fields=frozenset({"collector_signal_strength_raw", "collector_signal_strength", "collector_signal_strength_source"})),
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

SENSITIVE_READ_PARAMETERS: set[int] = {
    definition.parameter
    for definition in COLLECTOR_PARAMETER_DEFINITIONS
    if definition.sensitive_read
}

RUNTIME_COLLECTOR_PARAMETERS: tuple[CollectorParameterDefinition, ...] = tuple(
    definition
    for definition in COLLECTOR_PARAMETER_DEFINITIONS
    if definition.parameter in {2, 4, 5, 6, 14, 16, 21, 30, 32, 34, 41, 48, 55}
)

RUNTIME_COLLECTOR_SEMANTIC_FIELDS = frozenset(
    field
    for definition in RUNTIME_COLLECTOR_PARAMETERS
    for field in definition.semantic_fields
)


async def read_runtime_collector_values(
    session: CollectorMetadataQuerySession,
    *,
    parameters: tuple[CollectorParameterDefinition, ...] = RUNTIME_COLLECTOR_PARAMETERS,
    excluded_semantic_fields: frozenset[str] = frozenset(),
) -> CollectorMetadataChannelReadResult:
    """Read the FC=2 read-only collector metadata set with a structured outcome.

    Outcome semantics:

    * a delivery failure (timeout/disconnect/OSError) -> ``transport_error``;
    * a malformed response -> ``command_error``;
    * a well-formed unsupported parameter (``code != 0``) is skipped, not a
      failure;
    * some parameters answered with metadata + a delivery/command error ->
      ``partial``; all clean with metadata -> ``success``; none -> ``empty``.
    """

    values: dict[str, object] = {}
    attempted = 0
    successful = 0
    failed = 0
    transport_failed = False
    command_failed = False
    safe_code = ""
    unsupported_semantic_fields: set[str] = set()
    for definition in parameters:
        if definition.sensitive_read:
            continue
        if definition.decode is None:
            continue
        if definition.semantic_fields and definition.semantic_fields.issubset(
            excluded_semantic_fields
        ):
            continue
        attempted += 1
        try:
            response = await session.query_collector(definition.parameter)
        except asyncio.CancelledError:
            raise
        except _FRAMED_TRANSPORT_FAILURES as exc:  # noqa: BLE001 - typed code only
            transport_failed = True
            failed += 1
            safe_code = safe_code or type(exc).__name__
            continue
        except CollectorWireError as exc:  # malformed / unparseable response
            command_failed = True
            failed += 1
            safe_code = safe_code or str(exc).split(":", 1)[0]
            continue
        except Exception:  # noqa: BLE001 - one bad parameter is skipped
            failed += 1
            continue
        if response.code != 0:
            # Well-formed "unsupported/not-set" -> skip, not a failure.
            unsupported_semantic_fields.update(definition.semantic_fields)
            continue
        decoded = present_metadata_values(
            {
                key: value
                for key, value in definition.decode(response).items()
                if key not in excluded_semantic_fields
            }
        )
        if decoded:
            merge_collector_signal_values(values, decoded)
            successful += 1
    has_metadata = successful > 0
    if has_metadata and not (transport_failed or command_failed):
        outcome = OUTCOME_SUCCESS
    elif has_metadata:
        outcome = OUTCOME_PARTIAL
    elif command_failed:
        outcome = OUTCOME_COMMAND_ERROR
    elif transport_failed:
        outcome = OUTCOME_TRANSPORT_ERROR
    else:
        outcome = OUTCOME_EMPTY
    return CollectorMetadataChannelReadResult(
        values=values if outcome in (OUTCOME_SUCCESS, OUTCOME_PARTIAL) else {},
        outcome=outcome,
        safe_error_code=safe_code,
        attempted_commands=attempted,
        successful_commands=successful,
        failed_commands=failed,
        unsupported_semantic_fields=frozenset(unsupported_semantic_fields),
    )
