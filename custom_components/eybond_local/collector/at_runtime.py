"""Read-only runtime queries for plain collector AT sessions."""

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from typing import Callable, Protocol

from .at import CollectorAtResponse
from .cloud_family import collector_cloud_family_observation_from_endpoint
from .metadata_result import (
    OUTCOME_EMPTY,
    OUTCOME_PARTIAL,
    OUTCOME_SUCCESS,
    OUTCOME_TRANSPORT_ERROR,
    CollectorMetadataChannelReadResult,
    present_metadata_values,
)
from .signal import merge_collector_signal_values, normalize_signal_strength

# Delivery failures (as opposed to a command the collector simply did not
# answer): these mean the link is unusable, not that the commands are
# unsupported, so they must never accrue a dead-channel strike.
_AT_TRANSPORT_FAILURES = (
    asyncio.TimeoutError,
    TimeoutError,
    OSError,
    ConnectionError,
    EOFError,
    asyncio.IncompleteReadError,
    asyncio.LimitOverrunError,
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
    semantic_fields: frozenset[str] = frozenset()


def _decode_text_value(key: str) -> CollectorAtDecoder:
    def _decode(response: CollectorAtResponse) -> dict[str, object]:
        return {key: str(response.value or "").strip()}

    return _decode


def _decode_intpara_value(parameter: int, key: str) -> CollectorAtDecoder:
    """Decode one numbered ``INTPARA`` query without accepting a sibling reply.

    Factory collectors have been observed using both response shapes for
    ``AT+INTPARA<n>?``: ``AT+INTPARA<n>:value`` and
    ``AT+INTPARA:<n>,value``.  The generic AT transport intentionally carries
    either shape, so the parameter-specific decoder is the trust boundary that
    verifies the response belongs to the requested number.
    """

    expected_command = f"INTPARA{parameter}"
    expected_prefix = f"{parameter},"

    def _decode(response: CollectorAtResponse) -> dict[str, object]:
        if type(response.command) is not str or type(response.value) is not str:
            return {}
        command = response.command
        raw = response.value.strip()
        if command != command.strip().upper():
            return {}
        if command == "INTPARA":
            if not raw.startswith(expected_prefix):
                return {}
            raw = raw[len(expected_prefix) :].strip()
        elif command == expected_command:
            if raw.startswith(expected_prefix):
                raw = raw[len(expected_prefix) :].strip()
        else:
            return {}
        return {key: raw}

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
    CollectorAtQueryDefinition(
        "DTUPN",
        "Collector PN / serial.",
        _decode_text_value("collector_pn"),
        frozenset({"collector_pn"}),
    ),
    CollectorAtQueryDefinition(
        "ATVER",
        "AT interpreter / collector protocol version.",
        _decode_text_value("collector_protocol_version"),
        frozenset({"collector_protocol_version"}),
    ),
    CollectorAtQueryDefinition(
        "ENUPMODE",
        "Collector upload mode flag.",
        _decode_text_value("collector_upload_mode"),
        frozenset({"collector_upload_mode"}),
    ),
    CollectorAtQueryDefinition(
        "SYST",
        "Collector system time.",
        _decode_text_value("collector_system_time"),
        frozenset({"collector_system_time"}),
    ),
    CollectorAtQueryDefinition(
        "WFSS",
        "Collector Wi-Fi RSSI.",
        _decode_signal_strength,
        frozenset(
            {
                "collector_signal_strength_raw",
                "collector_signal_strength",
                "collector_signal_strength_source",
            }
        ),
    ),
    CollectorAtQueryDefinition(
        "UART",
        "Collector UART settings.",
        _decode_text_value("collector_serial_baudrate"),
        frozenset({"collector_serial_baudrate"}),
    ),
    CollectorAtQueryDefinition(
        "DTUTYPE",
        "Collector model / type.",
        _decode_text_value("collector_type"),
        frozenset({"collector_type"}),
    ),
    CollectorAtQueryDefinition(
        "FWVER",
        "Collector firmware version.",
        _decode_text_value("smartess_collector_version"),
        frozenset({"smartess_collector_version"}),
    ),
    CollectorAtQueryDefinition(
        "CLDSRVHOST1",
        "Collector cloud callback endpoint.",
        _decode_collector_server_endpoint,
        frozenset(
            {
                "collector_server_endpoint",
                "collector_cloud_family",
                "collector_cloud_family_source",
                "collector_cloud_family_confidence",
            }
        ),
    ),
    CollectorAtQueryDefinition(
        "HTBT",
        "Collector cloud heartbeat value.",
        _decode_text_value("collector_cloud_heartbeat_value"),
        frozenset({"collector_cloud_heartbeat_value"}),
    ),
    CollectorAtQueryDefinition(
        "LINK",
        "Collector link status from the newer communication path.",
        _decode_text_value("collector_link_status"),
        frozenset({"collector_link_status"}),
    ),
    CollectorAtQueryDefinition(
        "INTPARA49",
        "Nearby Wi-Fi scan list reported by the collector.",
        _decode_text_value("collector_wifi_scan_list"),
        frozenset({"collector_wifi_scan_list"}),
    ),
    # Keep optional numbered queries after the established metadata set. An
    # older collector that stays silent for this read still leaves every prior
    # value available as a fresh partial result.
    CollectorAtQueryDefinition(
        "INTPARA41",
        "Connected upstream Wi-Fi SSID reported by the collector.",
        _decode_intpara_value(41, "collector_ssid"),
        frozenset({"collector_ssid"}),
    ),
)

RUNTIME_COLLECTOR_AT_SEMANTIC_FIELDS = frozenset(
    field
    for definition in RUNTIME_COLLECTOR_AT_DEFINITIONS
    for field in definition.semantic_fields
)


async def read_runtime_collector_at_values(
    transport: CollectorAtQueryTransport,
    *,
    excluded_semantic_fields: frozenset[str] = frozenset(),
) -> CollectorMetadataChannelReadResult:
    """Read the read-only collector AT metadata set with a structured outcome.

    The cloud-family observation is derived from the collector's own endpoint
    reply (``CLDSRVHOST1``); it is never an input, so this reader takes no
    cloud-family argument and cloud family never selects the channel.

    Outcome semantics (dead-channel truth):

    * a timeout/disconnect BEFORE any metadata -> ``transport_error`` (no strike:
      the link is unusable, not the commands unsupported);
    * a timeout/disconnect AFTER some metadata -> ``partial`` (fresh, no strike);
      one timeout ends the sweep so a dead link costs one request timeout, not
      thirteen;
    * every command delivered but none carried metadata -> ``empty`` (a strike:
      the collector answered but does not support this channel);
    * an individual unsupported/rejected command is skipped, not fatal.

    The raw response/value is never placed in ``safe_error_code``.
    """

    values: dict[str, object] = {}
    attempted = 0
    successful = 0
    failed = 0
    timed_out = False
    transport_failed = False
    safe_code = ""
    for definition in RUNTIME_COLLECTOR_AT_DEFINITIONS:
        if definition.semantic_fields and definition.semantic_fields.issubset(
            excluded_semantic_fields
        ):
            continue
        attempted += 1
        try:
            response = await transport.async_query(definition.command)
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, TimeoutError):
            timed_out = True
            transport_failed = True
            safe_code = "at_response_timeout"
            break
        except _AT_TRANSPORT_FAILURES as exc:  # noqa: BLE001 - typed code only
            transport_failed = True
            safe_code = type(exc).__name__
            break
        except Exception:  # noqa: BLE001 - one unsupported command is skipped
            failed += 1
            continue
        decoded = {
            key: value
            for key, value in definition.decode(response).items()
            if key not in excluded_semantic_fields
        }
        present = present_metadata_values(decoded)
        if present:
            merge_collector_signal_values(values, present)
        if present:
            successful += 1
    has_metadata = successful > 0
    if transport_failed and not has_metadata:
        outcome = OUTCOME_TRANSPORT_ERROR
    elif transport_failed:
        outcome = OUTCOME_PARTIAL
    elif has_metadata:
        outcome = OUTCOME_SUCCESS
    else:
        outcome = OUTCOME_EMPTY
    # Only carry values forward on a fresh outcome; a delivered-but-blank sweep
    # (``empty``) is a dead-channel strike, not cache-worthy blank state.
    return CollectorMetadataChannelReadResult(
        values=values if outcome in (OUTCOME_SUCCESS, OUTCOME_PARTIAL) else {},
        outcome=outcome,
        safe_error_code=safe_code,
        attempted_commands=attempted,
        successful_commands=successful,
        failed_commands=failed,
        timed_out=timed_out,
    )
