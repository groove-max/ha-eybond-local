"""Read-only runtime queries for plain collector AT sessions."""

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from typing import Callable, Protocol

from .at import CollectorAtResponse
from .cloud_family import collector_cloud_family_observation_from_endpoint
from .signal import merge_collector_signal_values, normalize_signal_strength

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
)


async def query_runtime_collector_at_values(
    transport: CollectorAtQueryTransport,
    *,
    collector_cloud_family: str = "",
) -> dict[str, object]:
    """Read a safe read-only collector metadata set over the plain AT session."""

    values: dict[str, object] = {}
    for definition in RUNTIME_COLLECTOR_AT_DEFINITIONS:
        try:
            response = await transport.async_query(definition.command)
        except (asyncio.TimeoutError, TimeoutError):
            # A dead AT link times out for every command: this sweep is 12
            # commands, so marching on burns a full request timeout per
            # command (~60s per cycle). One timeout ends the sweep; the
            # values already collected are kept.
            break
        except Exception:
            continue
        merge_collector_signal_values(values, definition.decode(response))
    return values
